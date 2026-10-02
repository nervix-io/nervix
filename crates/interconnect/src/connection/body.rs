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
use nervix_execution::{BudgetedBuffer, ChargedBytes, Executor, MemoryClass};
use nervix_primitives::time::timeout;

use super::BODY_CHUNK_BYTES;
use crate::TransportError;

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
    let initial = limit.min(4 * 1024);
    let reservation = executor
        .reserve(class, initial)
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
        let chunk = timeout(progress_timeout, body.data()).await.map_err(|_| {
            TransportError::ProgressTimeout {
                timeout: progress_timeout,
            }
        })?;
        let Some(chunk) = chunk else {
            break;
        };
        let chunk = chunk.map_err(TransportError::from)?;
        buffer.write_all(&chunk).map_err(|error| {
            TransportError::with_cause(Report::new(error), TransportError::Decode)
        })?;
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(TransportError::from)?;
    }
    Ok(())
}
