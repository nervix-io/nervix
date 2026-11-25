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
};

use super::BODY_CHUNK_BYTES;
use crate::{
    PoolClass, RemoteOperationFailureKind, TransportError, operation::OPERATION_FAILURE_HEADER,
    request::RemoteRequestFailure, wire::FreeRoom,
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
    let message = match read_body(executor, class.memory_class(), limit, response.into_body()).await
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
            Some(ExecutionError::QueueFull { .. } | ExecutionError::PoolClosed { .. })
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
    let response = Response::builder()
        .status(status)
        .version(Version::HTTP_2)
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
    let response = headers(Response::builder().status(status).version(Version::HTTP_2))
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
    limit: u64,
    body: RecvStream,
) -> Result<ChargedBytes, Report<TransportError>> {
    let reservation = executor
        .reserve(class, initial_body_charge(limit))
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    let mut buffer = BudgetedBuffer::with_limit(reservation, limit);
    read_body_into(&mut buffer, body).await?;
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

/// Read `body`, of at most `limit` bytes, charging it only to room `class` has free now.
///
/// [`read_body`] waits in its class's line while the class is full. A request whose answer is that
/// the class has no room must not wait on that class to be read, so this reader takes only free
/// room. A body it finds no room for is drained without being kept, which completes its sender's
/// request so that the request can still be answered.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport performs this operation for each admitted frame or stream request"
    )
)]
pub(super) async fn read_body_in_free_room(
    executor: &Executor,
    class: MemoryClass,
    limit: u64,
    mut body: RecvStream,
) -> Result<FreeRoom<ChargedBytes>, Report<TransportError>> {
    let reservation = match executor.try_reserve(class, initial_body_charge(limit)) {
        Ok(reservation) => reservation,
        Err(refusal) => {
            if let AdmissionError::BudgetExhausted { .. } = refusal.current_context() {
                drain_body(body).await?;
                return Ok(FreeRoom::NoRoom);
            }
            return Err(TransportError::with_cause(refusal, TransportError::Decode));
        }
    };
    let mut buffer = BudgetedBuffer::with_limit(reservation, limit);
    loop {
        nervix_primitives::task::consume_budget().await;
        let Some(chunk) = next_body_chunk(&mut body).await? else {
            break;
        };
        let grown = buffer.try_extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(TransportError::from)?;
        let Err(refusal) = grown else {
            continue;
        };
        if let Some(AdmissionError::BudgetExhausted { .. }) =
            refusal.downcast_ref::<AdmissionError>()
        {
            drop(buffer);
            drain_body(body).await?;
            return Ok(FreeRoom::NoRoom);
        }
        return Err(TransportError::with_cause(refusal, TransportError::Decode));
    }
    Ok(FreeRoom::Taken(ChargedBytes::from_buffer(buffer)))
}

/// Read the rest of `body` and keep none of it, releasing each chunk's flow-control capacity so
/// that its sender can complete the request.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport performs this operation for each admitted frame or stream request"
    )
)]
async fn drain_body(mut body: RecvStream) -> Result<(), Report<TransportError>> {
    loop {
        nervix_primitives::task::consume_budget().await;
        let Some(chunk) = next_body_chunk(&mut body).await? else {
            return Ok(());
        };
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(TransportError::from)?;
    }
}

/// The charge a body reader starts with: room for a small body, never more than the body's limit.
/// A larger body grows the charge as it arrives.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "body accounting starts with one 4 KiB granule and then charges bytes as they \
                  arrive"
    )
)]
const INITIAL_BODY_CHARGE: u64 = 4 * 1024;

fn initial_body_charge(limit: u64) -> u64 {
    limit.min(INITIAL_BODY_CHARGE)
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
