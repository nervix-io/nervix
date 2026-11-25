//! Flow-controlled consumption of streamed interconnect responses.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Opening typed response streams, response-length enforcement, flow-control release,
//!   progress deadlines, and per-chunk memory admission.
//! - **Depends on.** The authenticated connection lease and bounded execution admission.
//! - **Must not know.** Resource contents, snapshot formats, or runtime graph behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::future::poll_fn;

use bytes::Bytes;
use error_stack::Report;
use futures_util::StreamExt as _;
use h2::{Reason, RecvStream, SendStream, server};
use http::{Request, Response, StatusCode, Version};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{BudgetedBuffer, ChargedBytes, Executor};
use nervix_models::ClusterNodeName;

use super::{
    BODY_CHUNK_BYTES, RawRequest, STREAM_PATH, StreamLease, TransportState, declared_length,
    read_body_to_decode, send_decode_refusal, send_static_error, until_reset,
};
use crate::{
    PoolClass, RequestError, TransportError,
    observation::TransferDirection,
    request::{
        RequestAdmission, StreamOpeningFailure, StreamPreconditionRefusal, StreamRequestEnvelope,
        StreamingResponse,
    },
    wire,
};

#[cfg(test)]
mod send_progress_tests;

/// A bounded, flow-controlled response body. Dropping it cancels the HTTP/2 stream and releases
/// its bulk slot; each returned chunk holds its own memory charge until the caller drops it.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained response stream consumes each admitted chunk under its progress \
                  deadline"
    )
)]
pub struct IncomingByteStream {
    body: RecvStream,
    lease: StreamLease,
    _request_admission: RequestAdmission,
    executor: Executor,
    class: PoolClass,
    node: ClusterNodeName,
    request: &'static str,
    content_length: u64,
    received: u64,
    finished: bool,
    /// What a chunk carried past the bytes [`IncomingByteStream::read_into`] last copied, and how
    /// far into it the stream has been read.
    pending: Option<(Bytes, usize)>,
}

pub(crate) struct OutboundByteStreamRequest {
    pub(crate) class: PoolClass,
    pub(crate) name: &'static str,
    pub(crate) body: ChargedBytes,
    /// What may end the opening besides the liveness of the peer: only a deadline its caller named.
    pub(crate) deadline: Option<crate::ExchangeDeadline>,
    pub(crate) admission: RequestAdmission,
    /// The stream the request leased before it encoded its payload.
    pub(crate) lease: StreamLease,
}

impl IncomingByteStream {
    pub fn content_length(&self) -> u64 {
        self.content_length
    }

    pub async fn next_chunk(&mut self) -> Result<Option<ChargedBytes>, Report<RequestError>> {
        if self.pending.is_some() {
            return Err(Report::new(RequestError::Stream {
                node: self.node.clone(),
                request: self.request,
                reason: "a stream read in exact lengths was read by chunk".to_string(),
            }));
        }
        let Some(chunk) = self.next_data().await? else {
            return Ok(None);
        };
        let chunk_bytes = u64::try_from(chunk.len())
            .verified("the chunk's length was measured when it was received");
        let reservation = match self
            .executor
            .reserve(self.class.memory_class(), chunk_bytes)
            .await
        {
            Ok(reservation) => reservation,
            Err(error) => {
                self.release(chunk.len())?;
                self.finished = true;
                return Err(RequestError::stream_with_cause(
                    error,
                    self.node.clone(),
                    self.request,
                ));
            }
        };
        let bytes = chunk.to_vec();
        self.release(chunk.len())?;
        self.lease.state.observations.bulk_transferred(
            self.class,
            TransferDirection::Received,
            chunk_bytes,
        );
        Ok(Some(ChargedBytes::from_owned(bytes, reservation)))
    }

    /// Copies exactly `length` bytes of the stream into `buffer`, whose room the caller already
    /// admitted, and takes no memory of its own: every chunk goes from the stream's receive window
    /// straight into the buffer, so the read never waits for memory while its caller holds the
    /// buffer. What a chunk carries past those bytes stays with the stream for the next read.
    pub async fn read_into(
        &mut self,
        buffer: &mut BudgetedBuffer,
        length: usize,
    ) -> Result<(), Report<RequestError>> {
        let mut left = length;
        while left > 0 {
            nervix_primitives::task::consume_budget().await;
            let (chunk, read) = match self.pending.take() {
                Some(pending) => pending,
                None => {
                    let Some(chunk) = self.next_data().await? else {
                        return Err(Report::new(RequestError::Stream {
                            node: self.node.clone(),
                            request: self.request,
                            reason: "the stream ended before the bytes its reader asked for"
                                .to_string(),
                        }));
                    };
                    (chunk, 0)
                }
            };
            let available = chunk
                .len()
                .checked_sub(read)
                .verified("a chunk is never read past its end");
            let piece = available.min(left);
            let end = read
                .checked_add(piece)
                .verified("a piece ends within its chunk");
            buffer
                .try_extend_from_slice(&chunk[read..end])
                .map_err(|error| {
                    RequestError::stream_with_cause(error, self.node.clone(), self.request)
                })?;
            left = left
                .checked_sub(piece)
                .verified("a piece is no longer than what is left");
            if end == chunk.len() {
                self.release(chunk.len())?;
                self.lease.state.observations.bulk_transferred(
                    self.class,
                    TransferDirection::Received,
                    u64::try_from(chunk.len())
                        .verified("the chunk's length was measured when it was received"),
                );
            } else {
                self.pending = Some((chunk, end));
            }
        }
        Ok(())
    }

    /// Whether the stream ends where its reader is, with no byte left unread; a stream that
    /// carries more is not consumed further.
    pub async fn ends(&mut self) -> Result<bool, Report<RequestError>> {
        if self.pending.is_some() {
            return Ok(false);
        }
        match self.next_data().await? {
            None => Ok(true),
            Some(chunk) => {
                self.pending = Some((chunk, 0));
                Ok(false)
            }
        }
    }

    /// Gives `bytes` of the stream's receive window back once they are consumed.
    fn release(&mut self, bytes: usize) -> Result<(), Report<RequestError>> {
        self.body
            .flow_control()
            .release_capacity(bytes)
            .map_err(|error| {
                RequestError::stream_with_cause(Report::new(error), self.node.clone(), self.request)
            })
    }

    /// The next chunk the stream received, checked against its bounds and counted, with its
    /// receive-window capacity not yet given back; `None` once the stream ended.
    async fn next_data(&mut self) -> Result<Option<Bytes>, Report<RequestError>> {
        if self.finished {
            return Ok(None);
        }
        // The stream waits for its producer for as long as the connection under it lives: a
        // producer that is slow costs latency, and a connection that dies fails the stream.
        let Some(next) = self.body.data().await else {
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
        let chunk = {
            let mapping_input = next;
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    self.finished = true;
                    RequestError::stream_with_cause(
                        Report::new(error),
                        self.node.clone(),
                        self.request,
                    )
                }),
            }
        }?;
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
            RequestError::stream_with_cause(Report::new(error), self.node.clone(), self.request)
        })?;
        let limit = self
            .lease
            .state
            .local_receive_bounds()
            .bulk_chunk_bytes
            .get();
        if chunk_bytes > limit {
            self.finished = true;
            return Err(Report::new(RequestError::StreamChunkBound {
                node: self.node.clone(),
                request: self.request,
                received: chunk_bytes,
                limit,
            }));
        }
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
        self.received = received;
        Ok(Some(chunk))
    }
}

impl TransportState {
    pub(super) async fn handle_stream_request(
        &self,
        peer_node_id: ClusterNodeName,
        peer_advertised_host: String,
        peer_process_epoch: u64,
        class: PoolClass,
        request: Request<RecvStream>,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), Report<TransportError>> {
        let declared =
            declared_length(request.headers(), class.control_body_limit(&self.executor))?;
        let reading = read_body_to_decode(
            &self.executor,
            class.memory_class(),
            declared,
            request.into_body(),
        );
        let received = match until_reset(&mut respond, reading).await {
            Ok(Some(received)) => received,
            Ok(None) => return Ok(()),
            Err(error) => {
                send_decode_refusal(&mut respond, class, error).await?;
                return Ok(());
            }
        };
        let decoded = until_reset(
            &mut respond,
            wire::decode_rkyv_within::<StreamRequestEnvelope>(
                &self.executor,
                class.cpu_class(),
                received.bytes,
                received.decoding,
            ),
        )
        .await;
        let decoded = match decoded {
            Ok(Some(decoded)) => decoded,
            Ok(None) => return Ok(()),
            Err(error) => {
                send_decode_refusal(&mut respond, class, error).await?;
                return Ok(());
            }
        };
        // The handler owns the request from here, and its own work takes charges of this class, so
        // the request's charge is given back before it runs: no handler waits for room behind the
        // request it serves. The connection's stream places bound the request meanwhile.
        let (request, reservation) = decoded.into_parts();
        drop(reservation);
        let receive = match self.targets.load().get(&peer_node_id) {
            Some(target) => match target.pool.capabilities.receive_bounds() {
                Some(committed) => committed.intersection(request.receive),
                None => request.receive,
            },
            None => request.receive,
        };
        let request = request.request;
        if request.class != class {
            send_static_error(&mut respond, StatusCode::FORBIDDEN, "wrong pool class").await?;
            return Ok(());
        }
        let handled = nervix_primitives::select! {
            handled = self.requests.handle_stream(
                &self.executor,
                peer_node_id,
                peer_advertised_host,
                peer_process_epoch,
                request,
            ) => handled,
            reset = poll_fn(|context| respond.poll_reset(context)) => {
                reset.map_err(TransportError::from)?;
                return Ok(());
            }
        };
        let handled = match handled {
            Ok(handled) => handled,
            Err(StreamOpeningFailure::Operation { kind, report }) => {
                super::body::send_operation_error(&mut respond, kind, &format!("{report:#}"))
                    .await?;
                return Ok(());
            }
            Err(StreamOpeningFailure::Request(error)) => {
                send_static_error(&mut respond, error.status(), &error.to_string()).await?;
                return Ok(());
            }
        };
        let (response, _admission) = handled.into_parts();
        self.send_streaming_response(class, respond, response, receive.bulk_chunk_bytes)
            .await
    }

    pub(crate) async fn open_byte_stream(
        &self,
        node_id: &ClusterNodeName,
        request: OutboundByteStreamRequest,
    ) -> Result<IncomingByteStream, Report<TransportError>> {
        let OutboundByteStreamRequest {
            class,
            name,
            body,
            deadline,
            admission,
            lease,
        } = request;
        let (body, content_length) = lease
            .connection
            .request_stream_raw(
                self,
                RawRequest {
                    path: STREAM_PATH,
                    body: Some(body),
                    response_class: class,
                    response_limit: class.control_body_limit(&self.executor),
                    deadline,
                    headers: &[],
                },
            )
            .await
            .map_err(StreamPreconditionRefusal::attach)?;
        Ok(IncomingByteStream {
            body,
            lease,
            _request_admission: admission,
            executor: self.executor.clone(),
            class,
            node: node_id.clone(),
            request: name,
            content_length,
            received: 0,
            finished: false,
            pending: None,
        })
    }
}

/// Writes one streamed chunk as the reader's flow-control window admits it.
///
/// A reader that grants no capacity is applying backpressure: the chunk waits for it for as long
/// as the connection under the stream lives, and a connection that dies fails the stream.
async fn send_stream_chunk(
    stream: &mut SendStream<Bytes>,
    body: ChargedBytes,
    chunk_limit: std::num::NonZeroU64,
) -> Result<(), Report<TransportError>> {
    let mut offset = 0;
    while offset < body.len() {
        nervix_primitives::task::consume_budget().await;
        let remaining = body
            .len()
            .checked_sub(offset)
            .verified("the send offset never advances beyond the streamed chunk");
        let chunk_limit = usize::try_from(chunk_limit.get()).unwrap_or(usize::MAX);
        let wanted = remaining.min(BODY_CHUNK_BYTES).min(chunk_limit);
        stream.reserve_capacity(wanted);
        let assigned = granted_send_capacity(stream).await?;
        let ready = assigned.min(wanted);
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

/// The next nonzero send capacity the reader grants `stream`. A notification of zero capacity is
/// no progress, so the wait continues through it.
async fn granted_send_capacity(
    stream: &mut SendStream<Bytes>,
) -> Result<usize, Report<TransportError>> {
    loop {
        let assigned = poll_fn(|context| stream.poll_capacity(context))
            .await
            .ok_or_else(|| {
                TransportError::Decode(
                    "HTTP/2 stream closed while assigning send capacity".to_string(),
                )
            })?
            .map_err(TransportError::from)?;
        if assigned > 0 {
            return Ok(assigned);
        }
        nervix_primitives::task::consume_budget().await;
    }
}

impl TransportState {
    /// Write one streamed response body, counting each chunk as it leaves so a long transfer is
    /// observable while it is running rather than only once it completes.
    async fn send_streaming_response(
        &self,
        class: PoolClass,
        mut respond: server::SendResponse<Bytes>,
        mut response: StreamingResponse,
        chunk_limit: std::num::NonZeroU64,
    ) -> Result<(), Report<TransportError>> {
        let headers = Response::builder()
            .status(StatusCode::OK)
            .version(Version::HTTP_2)
            .header(http::header::CONTENT_LENGTH, response.content_length)
            .body(())
            .map_err(|error| {
                TransportError::with_cause(Report::new(error), TransportError::Http)
            })?;
        let mut stream = respond
            .send_response(headers, false)
            .map_err(TransportError::from)?;
        let mut sent = 0_u64;
        loop {
            nervix_primitives::task::consume_budget().await;
            // The producer is this node's own handler: one that is slow costs its reader latency.
            let Some(chunk) = response.chunks.next().await else {
                break;
            };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    stream.send_reset(Reason::INTERNAL_ERROR);
                    return Err(TransportError::with_cause(error, TransportError::Decode));
                }
            };
            if chunk.is_empty() {
                stream.send_reset(Reason::INTERNAL_ERROR);
                return Err(Report::new(TransportError::Decode(
                    "stream producer yielded an empty chunk".to_string(),
                )));
            }
            let chunk_bytes = u64::try_from(chunk.len()).map_err(|error| {
                TransportError::with_cause(Report::new(error), TransportError::Decode)
            })?;
            sent = sent.checked_add(chunk_bytes).ok_or_else(|| {
                TransportError::Decode("streamed response byte count overflowed".to_string())
            })?;
            if sent > response.content_length {
                stream.send_reset(Reason::INTERNAL_ERROR);
                return Err(Report::new(TransportError::Decode(
                    "stream producer exceeded its declared content length".to_string(),
                )));
            }
            send_stream_chunk(&mut stream, chunk, chunk_limit).await?;
            self.observations
                .bulk_transferred(class, TransferDirection::Sent, chunk_bytes);
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
}
