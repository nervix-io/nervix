//! Engines and infrastructure.
//! Owns: bounded HTTP/2 request and response body transfer and its progress deadlines.
//! Depends on: HTTP/2 streams, execution admission and the primitive boundary.
//! Must not know: transport registries, graph placement or runtime records.

use std::{future::poll_fn, io::Write as _, time::Duration};

use bytes::Bytes;
use error_stack::Report;
use h2::{RecvStream, SendStream, server};
use http::{Response, StatusCode, Version};
use meticulous::OptionExt as _;
use nervix_execution::{
    AdmissionError, BudgetedBuffer, ChargedBytes, ExecutionError, Executor, MemoryClass,
};
use nervix_primitives::time::timeout;

use super::BODY_CHUNK_BYTES;
use crate::{PoolClass, TransportError, request::RemoteRequestFailure, wire::FreeRoom};

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
    progress_timeout: Duration,
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
    send_static_error(
        respond,
        failure.status(),
        &failure.to_string(),
        progress_timeout,
    )
    .await
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
    progress_timeout: Duration,
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
        timeout(progress_timeout, send_body(&mut stream, body))
            .await
            .map_err(|_| TransportError::ProgressTimeout {
                timeout: progress_timeout,
            })??;
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
    progress_timeout: Duration,
) -> Result<(), Report<TransportError>> {
    let response = Response::builder()
        .status(status)
        .version(Version::HTTP_2)
        .body(())
        .map_err(|error| TransportError::with_cause(Report::new(error), TransportError::Http))?;
    let mut stream = respond
        .send_response(response, false)
        .map_err(TransportError::from)?;
    timeout(progress_timeout, async {
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
        Ok::<(), Report<TransportError>>(())
    })
    .await
    .map_err(|_| TransportError::ProgressTimeout {
        timeout: progress_timeout,
    })?
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
    progress_timeout: Duration,
    body: RecvStream,
) -> Result<ChargedBytes, Report<TransportError>> {
    let reservation = executor
        .reserve(class, initial_body_charge(limit))
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    let mut buffer = BudgetedBuffer::with_limit(reservation, limit);
    read_body_into(&mut buffer, progress_timeout, body).await?;
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
    progress_timeout: Duration,
    mut body: RecvStream,
) -> Result<(), Report<TransportError>> {
    loop {
        nervix_primitives::task::consume_budget().await;
        let Some(chunk) = next_body_chunk(&mut body, progress_timeout).await? else {
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
    progress_timeout: Duration,
    mut body: RecvStream,
) -> Result<FreeRoom<ChargedBytes>, Report<TransportError>> {
    let reservation = match executor.try_reserve(class, initial_body_charge(limit)) {
        Ok(reservation) => reservation,
        Err(refusal) => {
            if let AdmissionError::BudgetExhausted { .. } = refusal.current_context() {
                drain_body(progress_timeout, body).await?;
                return Ok(FreeRoom::NoRoom);
            }
            return Err(TransportError::with_cause(refusal, TransportError::Decode));
        }
    };
    let mut buffer = BudgetedBuffer::with_limit(reservation, limit);
    loop {
        nervix_primitives::task::consume_budget().await;
        let Some(chunk) = next_body_chunk(&mut body, progress_timeout).await? else {
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
            drain_body(progress_timeout, body).await?;
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
async fn drain_body(
    progress_timeout: Duration,
    mut body: RecvStream,
) -> Result<(), Report<TransportError>> {
    loop {
        nervix_primitives::task::consume_budget().await;
        let Some(chunk) = next_body_chunk(&mut body, progress_timeout).await? else {
            return Ok(());
        };
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(TransportError::from)?;
    }
}

/// The charge a body reader starts with: room for a small body, never more than the body's limit.
/// A larger body grows the charge as it arrives.
fn initial_body_charge(limit: u64) -> u64 {
    limit.min(4 * 1024)
}

/// The next chunk of `body`, or `None` at its end, failing when no chunk arrives for
/// `progress_timeout`.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the transport performs this operation for each admitted frame or stream request"
    )
)]
async fn next_body_chunk(
    body: &mut RecvStream,
    progress_timeout: Duration,
) -> Result<Option<Bytes>, Report<TransportError>> {
    let chunk = timeout(progress_timeout, body.data()).await.map_err(|_| {
        TransportError::ProgressTimeout {
            timeout: progress_timeout,
        }
    })?;
    let Some(chunk) = chunk else {
        return Ok(None);
    };
    let chunk = chunk.map_err(TransportError::from)?;
    Ok(Some(chunk))
}
