//! Flow-controlled consumption of streamed interconnect responses.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Opening typed response streams, response-length enforcement, flow-control release,
//!   progress deadlines, and per-chunk memory admission.
//! - **Depends on.** The authenticated connection lease and bounded execution admission.
//! - **Must not know.** Resource contents, snapshot formats, or runtime graph behavior.

use std::{future::poll_fn, time::Duration};

use bytes::Bytes;
use error_stack::Report;
use futures_util::StreamExt as _;
use h2::{Reason, RecvStream, SendStream, server};
use http::{Response, StatusCode, Version};
use meticulous::OptionExt as _;
use nervix_execution::{ChargedBytes, Executor};
use nervix_models::ClusterNodeName;
use tokio::time::{Instant, timeout};

use super::{
    BODY_CHUNK_BYTES, RawRequest, STREAM_PATH, StreamLease, TransportState, read_body,
    send_static_error,
};
use crate::{
    PoolClass, RequestError, RequestSubquota, TransportError,
    request::{RequestAdmission, RequestEnvelope, StreamingResponse},
    wire,
};

/// A bounded, flow-controlled response body. Dropping it cancels the HTTP/2 stream and releases
/// its bulk slot; each returned chunk holds its own memory charge until the caller drops it.
pub struct IncomingByteStream {
    body: RecvStream,
    _lease: StreamLease,
    _request_admission: RequestAdmission,
    executor: Executor,
    class: PoolClass,
    node: ClusterNodeName,
    request: &'static str,
    progress_timeout: Duration,
    content_length: u64,
    received: u64,
    finished: bool,
}

pub(crate) struct OutboundByteStreamRequest {
    pub(crate) class: PoolClass,
    pub(crate) subquota: RequestSubquota,
    pub(crate) name: &'static str,
    pub(crate) body: ChargedBytes,
    pub(crate) timeout: Duration,
    pub(crate) admission: RequestAdmission,
}

impl IncomingByteStream {
    pub fn content_length(&self) -> u64 {
        self.content_length
    }

    pub async fn next_chunk(&mut self) -> Result<Option<ChargedBytes>, Report<RequestError>> {
        if self.finished {
            return Ok(None);
        }
        let next = timeout(self.progress_timeout, self.body.data()).await;
        let next = match next {
            Ok(next) => next,
            Err(_) => {
                self.finished = true;
                return Err(Report::new(RequestError::Stream {
                    node: self.node.clone(),
                    request: self.request,
                    reason: format!("response made no progress for {:?}", self.progress_timeout),
                }));
            }
        };
        let Some(next) = next else {
            self.finished = true;
            if self.received != self.content_length {
                return Err(Report::new(RequestError::StreamLengthMismatch {
                    node: self.node.clone(),
                    request: self.request,
                    declared: self.content_length,
                    received: self.received,
                }));
            }
            return Ok(None);
        };
        let chunk = next.map_err(|error| {
            self.finished = true;
            Report::new(RequestError::Stream {
                node: self.node.clone(),
                request: self.request,
                reason: error.to_string(),
            })
        })?;
        if chunk.is_empty() {
            self.finished = true;
            if !self.body.is_end_stream() {
                return Err(Report::new(RequestError::Stream {
                    node: self.node.clone(),
                    request: self.request,
                    reason: "received an empty non-terminal response chunk".to_string(),
                }));
            }
            if self.received != self.content_length {
                return Err(Report::new(RequestError::StreamLengthMismatch {
                    node: self.node.clone(),
                    request: self.request,
                    declared: self.content_length,
                    received: self.received,
                }));
            }
            return Ok(None);
        }
        let chunk_bytes = u64::try_from(chunk.len()).map_err(|error| {
            Report::new(RequestError::Stream {
                node: self.node.clone(),
                request: self.request,
                reason: error.to_string(),
            })
        })?;
        let received = self.received.checked_add(chunk_bytes).ok_or_else(|| {
            Report::new(RequestError::Stream {
                node: self.node.clone(),
                request: self.request,
                reason: "received byte count overflowed".to_string(),
            })
        })?;
        if received > self.content_length {
            self.finished = true;
            return Err(Report::new(RequestError::StreamLengthMismatch {
                node: self.node.clone(),
                request: self.request,
                declared: self.content_length,
                received,
            }));
        }
        let reservation = match self
            .executor
            .reserve(self.class.memory_class(), chunk_bytes)
            .await
        {
            Ok(reservation) => reservation,
            Err(error) => {
                self.body
                    .flow_control()
                    .release_capacity(chunk.len())
                    .map_err(|release_error| {
                        Report::new(RequestError::Stream {
                            node: self.node.clone(),
                            request: self.request,
                            reason: release_error.to_string(),
                        })
                    })?;
                self.finished = true;
                return Err(Report::new(RequestError::Stream {
                    node: self.node.clone(),
                    request: self.request,
                    reason: error.to_string(),
                }));
            }
        };
        let bytes = chunk.to_vec();
        self.body
            .flow_control()
            .release_capacity(chunk.len())
            .map_err(|error| {
                Report::new(RequestError::Stream {
                    node: self.node.clone(),
                    request: self.request,
                    reason: error.to_string(),
                })
            })?;
        self.received = received;
        Ok(Some(ChargedBytes::from_owned(bytes, reservation)))
    }
}

impl TransportState {
    pub(super) async fn handle_stream_request(
        &self,
        peer_node_id: ClusterNodeName,
        peer_advertised_host: String,
        class: PoolClass,
        body: RecvStream,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), Report<TransportError>> {
        let bytes = read_body(
            &self.executor,
            class.memory_class(),
            class.control_body_limit(&self.executor),
            self.options.progress_timeout,
            body,
        )
        .await?;
        let decoded = wire::decode_rkyv::<RequestEnvelope>(
            &self.executor,
            class.memory_class(),
            class.cpu_class(),
            bytes,
        )
        .await?;
        let (request, _reservation) = decoded.into_parts();
        if request.class != class {
            send_static_error(
                &mut respond,
                StatusCode::FORBIDDEN,
                "wrong pool class",
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }
        let handled = tokio::select! {
            handled = self.requests.handle_stream(
                &self.executor,
                peer_node_id,
                peer_advertised_host,
                request,
            ) => handled,
            reset = poll_fn(|context| respond.poll_reset(context)) => {
                reset.map_err(TransportError::from)?;
                return Ok(());
            }
        };
        let handled = match handled {
            Ok(handled) => handled,
            Err(error) => {
                send_static_error(
                    &mut respond,
                    StatusCode::BAD_REQUEST,
                    &error.to_string(),
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
        };
        let (response, _admission) = handled.into_parts();
        send_streaming_response(respond, response, self.options.progress_timeout).await
    }

    pub(crate) async fn open_byte_stream(
        &self,
        node_id: &ClusterNodeName,
        request: OutboundByteStreamRequest,
    ) -> Result<IncomingByteStream, Report<TransportError>> {
        let OutboundByteStreamRequest {
            class,
            subquota,
            name,
            body,
            timeout: timeout_duration,
            admission,
        } = request;
        let deadline = Instant::now()
            .checked_add(timeout_duration)
            .ok_or_else(|| TransportError::InvalidOptions {
                reason: "request deadline exceeds the monotonic clock range".to_string(),
            })?;
        let lease = self.lease(node_id, class, subquota, deadline).await?;
        let (body, content_length) = lease
            .connection
            .request_stream_raw(
                self,
                RawRequest {
                    path: STREAM_PATH,
                    body: Some(body),
                    response_class: class,
                    response_limit: class.control_body_limit(&self.executor),
                    timeout: timeout_duration,
                    headers: &[],
                },
            )
            .await?;
        Ok(IncomingByteStream {
            body,
            _lease: lease,
            _request_admission: admission,
            executor: self.executor.clone(),
            class,
            node: node_id.clone(),
            request: name,
            progress_timeout: self.options.progress_timeout,
            content_length,
            received: 0,
            finished: false,
        })
    }
}

async fn send_stream_chunk(
    stream: &mut SendStream<Bytes>,
    body: ChargedBytes,
) -> Result<(), Report<TransportError>> {
    let mut offset = 0;
    while offset < body.len() {
        tokio::task::consume_budget().await;
        let remaining = body
            .len()
            .checked_sub(offset)
            .verified("the send offset never advances beyond the streamed chunk");
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
            .verified("assigned capacity is bounded by the remaining streamed chunk");
        let chunk = body
            .slice(offset, end)
            .verified("the streamed chunk bounds were checked against its body");
        offset = end;
        stream
            .send_data(Bytes::from_owner(chunk), false)
            .map_err(TransportError::from)?;
    }
    stream.reserve_capacity(0);
    Ok(())
}

async fn send_streaming_response(
    mut respond: server::SendResponse<Bytes>,
    mut response: StreamingResponse,
    progress_timeout: Duration,
) -> Result<(), Report<TransportError>> {
    let headers = Response::builder()
        .status(StatusCode::OK)
        .version(Version::HTTP_2)
        .header(http::header::CONTENT_LENGTH, response.content_length)
        .body(())
        .map_err(|error| TransportError::Http(error.to_string()))?;
    let mut stream = respond
        .send_response(headers, false)
        .map_err(TransportError::from)?;
    let mut sent = 0_u64;
    loop {
        tokio::task::consume_budget().await;
        let next = match timeout(progress_timeout, response.chunks.next()).await {
            Ok(next) => next,
            Err(_) => {
                stream.send_reset(Reason::CANCEL);
                return Err(Report::new(TransportError::ProgressTimeout {
                    timeout: progress_timeout,
                }));
            }
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                stream.send_reset(Reason::INTERNAL_ERROR);
                return Err(Report::new(TransportError::Decode(error.to_string())));
            }
        };
        if chunk.is_empty() {
            stream.send_reset(Reason::INTERNAL_ERROR);
            return Err(Report::new(TransportError::Decode(
                "stream producer yielded an empty chunk".to_string(),
            )));
        }
        let chunk_bytes = u64::try_from(chunk.len())
            .map_err(|error| TransportError::Decode(error.to_string()))?;
        sent = sent.checked_add(chunk_bytes).ok_or_else(|| {
            TransportError::Decode("streamed response byte count overflowed".to_string())
        })?;
        if sent > response.content_length {
            stream.send_reset(Reason::INTERNAL_ERROR);
            return Err(Report::new(TransportError::Decode(
                "stream producer exceeded its declared content length".to_string(),
            )));
        }
        match timeout(progress_timeout, send_stream_chunk(&mut stream, chunk)).await {
            Ok(result) => result?,
            Err(_) => {
                stream.send_reset(Reason::CANCEL);
                return Err(Report::new(TransportError::ProgressTimeout {
                    timeout: progress_timeout,
                }));
            }
        }
    }
    if sent != response.content_length {
        stream.send_reset(Reason::INTERNAL_ERROR);
        return Err(Report::new(TransportError::Decode(format!(
            "stream producer declared {} bytes but produced {sent}",
            response.content_length
        ))));
    }
    stream
        .send_data(Bytes::new(), true)
        .map_err(TransportError::from)?;
    Ok(())
}
