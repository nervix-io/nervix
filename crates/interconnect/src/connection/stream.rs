//! Flow-controlled consumption of streamed interconnect responses.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Opening typed response streams, response-length enforcement, flow-control release,
//!   progress deadlines, and per-chunk memory admission.
//! - **Depends on.** The authenticated connection lease and bounded execution admission.
//! - **Must not know.** Resource contents, snapshot formats, or runtime graph behavior.

use std::time::Duration;

use error_stack::Report;
use h2::RecvStream;
use nervix_execution::{ChargedBytes, Executor};
use nervix_models::ClusterNodeName;
use tokio::time::{Instant, timeout};

use super::{RawRequest, STREAM_PATH, StreamLease, TransportState};
use crate::{PoolClass, RequestError, RequestSubquota, TransportError, request::RequestAdmission};

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
