//! Engines and infrastructure.
//! Owns: bounded HTTP/2 request and response body transfer; a peer's liveness, never a timer,
//! ends one.
//! Depends on: HTTP/2 streams, execution admission and the primitive boundary.
//! Must not know: transport registries, graph placement or runtime records.

use std::{future::poll_fn, io::Write as _};

use bytes::Bytes;
use error_stack::Report;
use h2::{RecvStream, SendStream, server};
use http::{Response, StatusCode, Version};
use meticulous::OptionExt as _;
use nervix_execution::{
    AdmissionError, BudgetedBuffer, ChargedBytes, ExecutionError, Executor, MemoryClass,
    Reservation,
};

use super::BODY_CHUNK_BYTES;
use crate::{
    PoolClass, RemoteOperationFailureKind, TransportError, operation::OPERATION_FAILURE_HEADER,
    request::RemoteRequestFailure, wire,
};

/// Read a refused opening under its body bound, retaining a reported operation outcome. Protocol
/// and transport errors do not carry an operation outcome.
pub(super) async fn read_error_response(
    executor: &Executor,
    class: PoolClass,
    limit: u64,
    response: Response<RecvStream>,
) -> Report<TransportError> {
    let status = response.status();
    let kind = match response.headers().get(OPERATION_FAILURE_HEADER) {
        Some(value) => {
            let text = match value.to_str() {
                Ok(text) => text,
                Err(error) => {
                    return TransportError::with_cause(Report::new(error), TransportError::Decode);
                }
            };
            match text.parse::<RemoteOperationFailureKind>() {
                Ok(kind) => Some(kind),
                Err(error) => {
                    return TransportError::with_cause(Report::new(error), TransportError::Decode);
                }
            }
        }
        None => None,
    };
    let declared = match declared_length(response.headers(), limit) {
        Ok(declared) => declared,
        Err(error) => return error,
    };
    let message = match read_body(executor, class.memory_class(), declared, response.into_body())
        .await
    {
        Ok(message) => message,
        Err(error) => return error,
    };
    let error = Report::new(TransportError::RemoteRejected {
        status: status.as_u16(),
        message: String::from_utf8_lossy(message.as_ref()).into_owned(),
    });
    match kind {
        Some(kind) => error.attach(kind),
        None => error,
    }
}

/// Answer an opening decode's admission refusal before the response stream is established.
/// Other decoder failures retain their transport report and its original failure behavior.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport answers receive admission at each stream opening"
    )
)]
pub(super) async fn send_decode_refusal(
    respond: &mut server::SendResponse<Bytes>,
    class: PoolClass,
    error: Report<TransportError>,
) -> Result<(), Report<TransportError>> {
    let admission = error.contains::<AdmissionError>()
        || matches!(
            error.downcast_ref::<ExecutionError>(),
            Some(ExecutionError::PoolClosed { .. })
        );
    if !admission {
        return Err(error);
    }
    let failure = RemoteRequestFailure::decode(error, class);
    send_static_error(respond, failure.status(), &failure.to_string()).await
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport services each admitted body and resolves its exact cancellation \
                  guard"
    )
)]
pub(super) async fn send_body(
    stream: &mut SendStream<Bytes>,
    body: ChargedBytes,
) -> Result<(), Report<TransportError>> {
    let mut offset = 0;
    while offset < body.len() {
        nervix_primitives::task::consume_budget().await;
        let remaining = body
            .len()
            .checked_sub(offset)
            .verified("the send offset never advances beyond the body");
        let wanted = remaining.min(BODY_CHUNK_BYTES);
        stream.reserve_capacity(wanted);
        let assigned = poll_fn(|context| stream.poll_capacity(context))
            .await
            .ok_or_else(|| {
                TransportError::Decode(
                    "HTTP/2 stream closed while assigning send capacity".to_string(),
                )
            })?
            .map_err(TransportError::from)?;
        let ready = assigned.min(wanted);
        if ready == 0 {
            continue;
        }
        let end = offset
            .checked_add(ready)
            .verified("assigned capacity is bounded by the remaining body");
        let chunk = body
            .slice(offset, end)
            .verified("the chunk bounds were checked against the body");
        offset = end;
        let end_stream = offset == body.len();
        stream
            .send_data(Bytes::from_owner(chunk), end_stream)
            .map_err(TransportError::from)?;
        if end_stream {
            break;
        }
    }
    stream.reserve_capacity(0);
    Ok(())
}

/// Resolves once the caller resets the exchange `respond` would answer, or the connection under
/// it fails. An answer that waits for a place on a full class waits only while this is pending,
/// because no answer can reach the caller afterwards.
pub(super) async fn exchange_reset(
    respond: &mut server::SendResponse<Bytes>,
) -> Result<(), Report<TransportError>> {
    match poll_fn(|context| respond.poll_reset(context)).await {
        Ok(_reason) => Ok(()),
        Err(error) => Err(Report::new(TransportError::from(error))),
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport performs this operation for each admitted frame or stream request"
    )
)]
pub(super) async fn send_response(
    mut respond: server::SendResponse<Bytes>,
    status: StatusCode,
    body: Option<ChargedBytes>,
) -> Result<(), Report<TransportError>> {
    let length = match &body {
        Some(body) => body.len(),
        None => 0,
    };
    let response = Response::builder()
        .status(status)
        .version(Version::HTTP_2)
        .header(http::header::CONTENT_LENGTH, length)
        .body(())
        .map_err(|error| TransportError::with_cause(Report::new(error), TransportError::Http))?;
    let end_stream = body.as_ref().is_none_or(ChargedBytes::is_empty);
    let mut stream = respond
        .send_response(response, end_stream)
        .map_err(TransportError::from)?;
    if let Some(body) = body
        && !body.is_empty()
    {
        send_body(&mut stream, body).await?;
    }
    Ok(())
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport performs this operation for each admitted frame or stream request"
    )
)]
pub(super) async fn send_static_error(
    respond: &mut server::SendResponse<Bytes>,
    status: StatusCode,
    message: &str,
) -> Result<(), Report<TransportError>> {
    send_error_response(respond, status, message, |response| response).await
}

/// Supplement the full diagnostic with its typed operation outcome. Successful streams retain
/// their framing, and sending an outcome adds no execution admission to the refusal path.
pub(super) async fn send_operation_error(
    respond: &mut server::SendResponse<Bytes>,
    kind: RemoteOperationFailureKind,
    message: &str,
) -> Result<(), Report<TransportError>> {
    let status = match kind {
        RemoteOperationFailureKind::Rejected => StatusCode::CONFLICT,
        RemoteOperationFailureKind::Unavailable => StatusCode::NOT_FOUND,
        RemoteOperationFailureKind::NotReady => StatusCode::TOO_EARLY,
        RemoteOperationFailureKind::Failed => StatusCode::INTERNAL_SERVER_ERROR,
    };
    send_error_response(respond, status, message, |response| {
        response.header(OPERATION_FAILURE_HEADER, kind.as_ref())
    })
    .await
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the private callers supply one synchronous HTTP response-header construction \
                  before the bounded body write"
    )
)]
async fn send_error_response<F>(
    respond: &mut server::SendResponse<Bytes>,
    status: StatusCode,
    message: &str,
    headers: F,
) -> Result<(), Report<TransportError>>
where
    F: FnOnce(http::response::Builder) -> http::response::Builder,
{
    let response = headers(
        Response::builder()
            .status(status)
            .version(Version::HTTP_2)
            .header(http::header::CONTENT_LENGTH, message.len()),
    )
    .body(())
    .map_err(|error| TransportError::with_cause(Report::new(error), TransportError::Http))?;
    let mut stream = respond
        .send_response(response, false)
        .map_err(TransportError::from)?;
    let body = Bytes::copy_from_slice(message.as_bytes());
    let mut offset = 0;
    while offset < body.len() {
        nervix_primitives::task::consume_budget().await;
        let remaining = body
            .len()
            .checked_sub(offset)
            .verified("the send offset never advances beyond the static error body");
        let wanted = remaining.min(BODY_CHUNK_BYTES);
        stream.reserve_capacity(wanted);
        let assigned = poll_fn(|context| stream.poll_capacity(context))
            .await
            .ok_or_else(|| {
                TransportError::Decode(
                    "HTTP/2 stream closed while assigning send capacity".to_string(),
                )
            })?
            .map_err(TransportError::from)?;
        let ready = assigned.min(wanted);
        if ready == 0 {
            continue;
        }
        let end = offset
            .checked_add(ready)
            .verified("assigned capacity is bounded by the remaining static error body");
        let chunk = body.slice(offset..end);
        offset = end;
        let end_stream = offset == body.len();
        stream
            .send_data(chunk, end_stream)
            .map_err(TransportError::from)?;
        if end_stream {
            break;
        }
    }
    stream.reserve_capacity(0);
    Ok(())
}

/// The length the sender declared for a body, which its receiver charges before reading any of it.
/// Every interconnect body declares its length, so a body without one, or declared longer than
/// `limit`, is malformed.
pub(super) fn declared_length(
    headers: &http::HeaderMap,
    limit: u64,
) -> Result<u64, Report<TransportError>> {
    let declared = headers
        .get(http::header::CONTENT_LENGTH)
        .ok_or_else(|| {
            Report::new(TransportError::Decode(
                "the body does not declare its length".to_string(),
            ))
        })?
        .to_str()
        .map_err(|error| TransportError::with_cause(Report::new(error), TransportError::Decode))?
        .parse::<u64>()
        .map_err(|error| TransportError::with_cause(Report::new(error), TransportError::Decode))?;
    if declared > limit {
        return Err(Report::new(TransportError::Decode(format!(
            "the body declares {declared} bytes, more than its limit of {limit}"
        ))));
    }
    Ok(declared)
}

/// Run `operation` until it completes or the peer resets the exchange it serves, as
/// [`wire::until_ended`] does. A receiver that waits for room to charge a request to, or for a
/// place to decode it, ends that wait with the request: `None` once the peer reset it.
pub(super) async fn until_reset<T>(
    respond: &mut server::SendResponse<Bytes>,
    operation: impl Future<Output = Result<T, Report<TransportError>>>,
) -> Result<Option<T>, Report<TransportError>> {
    let reset = async {
        poll_fn(|context| respond.poll_reset(context))
            .await
            .map_err(TransportError::from)?;
        Ok::<(), Report<TransportError>>(())
    };
    wire::until_ended(reset, operation).await
}

/// A body read whole, and the room its decoding takes, charged together with it.
pub(super) struct ReceivedBody {
    pub(super) bytes: ChargedBytes,
    /// Room for the decoded value, as large as the body.
    pub(super) decoding: Reservation,
}

/// Read a body of the `declared` length, charged exactly that much. The reader waits for the
/// charge while it holds nothing of `class`, so a full class holds the body back until room frees;
/// only a body the whole class could never hold fails. The caller's reset, or a deadline the
/// caller names, ends the wait.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport performs this operation for each admitted frame or stream request"
    )
)]
pub(super) async fn read_body(
    executor: &Executor,
    class: MemoryClass,
    declared: u64,
    body: RecvStream,
) -> Result<ChargedBytes, Report<TransportError>> {
    let charge = executor
        .reserve(class, declared)
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    read_declared(charge, declared, body).await
}

/// Read a body of the `declared` length together with the room its decoding needs, as one charge
/// the reader waits for while it holds nothing of `class`: no read or decoding then waits for room
/// behind the body it holds.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport performs this operation for each admitted frame or stream request"
    )
)]
pub(super) async fn read_body_to_decode(
    executor: &Executor,
    class: MemoryClass,
    declared: u64,
    body: RecvStream,
) -> Result<ReceivedBody, Report<TransportError>> {
    let whole = declared.checked_mul(2).ok_or_else(|| {
        Report::new(TransportError::Decode(format!(
            "a body of {declared} bytes and its decoding overflow a charge"
        )))
    })?;
    let charge = executor
        .reserve(class, whole)
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    let (input, decoding) = charge
        .split(declared)
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    let bytes = read_declared(input, declared, body).await?;
    Ok(ReceivedBody { bytes, decoding })
}

/// Read `body` into `charge`, which holds exactly its `declared` length, and fail when the body
/// carries more or fewer bytes than it declared.
async fn read_declared(
    charge: Reservation,
    declared: u64,
    body: RecvStream,
) -> Result<ChargedBytes, Report<TransportError>> {
    let mut buffer = BudgetedBuffer::with_limit(charge, declared);
    read_body_into(&mut buffer, body).await?;
    let received = u64::try_from(buffer.len())
        .map_err(|error| TransportError::with_cause(Report::new(error), TransportError::Decode))?;
    if received != declared {
        return Err(Report::new(TransportError::Decode(format!(
            "the body carried {received} of the {declared} bytes it declared"
        ))));
    }
    Ok(ChargedBytes::from_buffer(buffer))
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport performs this operation for each admitted frame or stream request"
    )
)]
pub(super) async fn read_body_into(
    buffer: &mut BudgetedBuffer,
    mut body: RecvStream,
) -> Result<(), Report<TransportError>> {
    loop {
        nervix_primitives::task::consume_budget().await;
        let Some(chunk) = next_body_chunk(&mut body).await? else {
            break;
        };
        buffer.write_all(&chunk).map_err(|error| {
            TransportError::with_cause(Report::new(error), TransportError::Decode)
        })?;
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(TransportError::from)?;
    }
    Ok(())
}

/// The next chunk of `body`, or `None` at its end. The read waits for its sender for as long as the
/// connection under it lives: a sender that is slow costs latency, and a connection that dies fails
/// the stream.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport performs this operation for each admitted frame or stream request"
    )
)]
async fn next_body_chunk(body: &mut RecvStream) -> Result<Option<Bytes>, Report<TransportError>> {
    let Some(chunk) = body.data().await else {
        return Ok(None);
    };
    let chunk = chunk.map_err(TransportError::from)?;
    Ok(Some(chunk))
}
