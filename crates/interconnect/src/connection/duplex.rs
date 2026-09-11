//! Ordered bidirectional frame streams between two nodes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Opening one ordered frame stream per caller, frame length validation, per-frame
//!   memory admission, and the half-close that ends a stream.
//! - **Depends on.** The authenticated connection lease and bounded execution admission.
//! - **Must not know.** What the frames carry, or why a caller keeps a stream open.

use std::{future::poll_fn, marker::PhantomData, pin::Pin, time::Duration};

use arch_into::ArchInto as _;
use bytes::Bytes;
use error_stack::Report;
use futures_util::{Stream, StreamExt as _};
use h2::{Reason, RecvStream, SendStream, server};
use http::{Response, StatusCode, Version};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{ChargedBytes, Executor, MemoryClass, Reservation};
use nervix_models::ClusterNodeName;
use tokio::time::{Instant, timeout};
use triomphe::Arc;

use super::{
    BODY_CHUNK_BYTES, DUPLEX_PATH, RawDuplexRequest, StreamLease, TransportState,
    send_static_error,
};
use crate::{
    PoolClass, RequestError, TransportError,
    request::{
        InterconnectDuplexRequest, RequestAdmission, RequestEnvelope, RkyvMessage,
        StreamHandlerError,
    },
    wire,
};

/// The frame length prefix. One frame carries one encoded value and nothing else.
const FRAME_HEADER_BYTES: usize = 4;

/// How much of the next frame may already have arrived while the current one is still incomplete.
/// A peer that buries a frame boundary further than this has stopped following the framing.
const FRAME_CARRY_SLACK_BYTES: u64 = 64 * 1024;
/// What a reader charges before it has seen how large this stream's frames are.
const INITIAL_FRAME_CHARGE: u64 = 4 * 1024;

fn carry_limit(frame_limit: u64) -> u64 {
    frame_limit
        .checked_add(FRAME_CARRY_SLACK_BYTES)
        .assured("a configured frame limit leaves room inside a u64 for one arriving chunk")
}

/// Reads whole frames off one direction of a duplex stream.
///
/// An open stream that is simply idle is not a failure, so this applies no progress deadline of its
/// own: the caller that has work outstanding owns the deadline for its answer.
///
/// The charge follows what the reader actually buffers rather than the largest frame its class
/// allows. A stream carrying small frames therefore holds a small charge for its whole life, and
/// only a stream that really receives a large frame grows to it.
pub(crate) struct FrameReader {
    body: RecvStream,
    carry: Vec<u8>,
    frame_limit: u64,
    charge: Reservation,
    finished: bool,
}

impl FrameReader {
    pub(crate) async fn open(
        executor: &Executor,
        class: MemoryClass,
        frame_limit: u64,
        body: RecvStream,
    ) -> Result<Self, Report<TransportError>> {
        let charge = executor
            .reserve(class, INITIAL_FRAME_CHARGE.min(carry_limit(frame_limit)))
            .await
            .map_err(|error| Report::new(TransportError::Decode(error.to_string())))?;
        Ok(Self {
            body,
            carry: Vec::new(),
            frame_limit,
            charge,
            finished: false,
        })
    }

    /// The next complete frame, or `None` once the peer half-closed its direction.
    pub(crate) async fn next_frame(&mut self) -> Result<Option<Vec<u8>>, Report<TransportError>> {
        loop {
            tokio::task::consume_budget().await;
            if let Some(frame) = self.take_buffered_frame()? {
                return Ok(Some(frame));
            }
            if self.finished {
                if self.carry.is_empty() {
                    return Ok(None);
                }
                return Err(Report::new(TransportError::Decode(
                    "duplex stream ended part-way through a frame".to_string(),
                )));
            }
            let Some(chunk) = self.body.data().await else {
                self.finished = true;
                continue;
            };
            let chunk = chunk.map_err(TransportError::from)?;
            self.charge_for(chunk.len())?;
            self.body
                .flow_control()
                .release_capacity(chunk.len())
                .map_err(TransportError::from)?;
            self.carry.extend_from_slice(&chunk);
        }
    }

    /// Charge the room the arriving chunk needs before it is buffered.
    fn charge_for(&mut self, additional: usize) -> Result<(), Report<TransportError>> {
        let carried = u64::try_from(self.carry.len())
            .assured("supported targets have a pointer width no larger than u64");
        let additional = u64::try_from(additional)
            .assured("supported targets have a pointer width no larger than u64");
        let ceiling = carry_limit(self.frame_limit);
        let target = carried.checked_add(additional).ok_or_else(|| {
            Report::new(TransportError::Decode(
                "duplex frame exceeds u64 bytes".to_string(),
            ))
        })?;
        if target > ceiling {
            return Err(Report::new(TransportError::Decode(format!(
                "duplex peer buffered {target} bytes without completing a frame"
            ))));
        }
        if target <= self.charge.bytes() {
            return Ok(());
        }
        // Grow geometrically so a large frame takes the budget a logarithmic number of times, and
        // a stream carrying small frames never holds room it will not use.
        let doubled = self.charge.bytes().checked_mul(2).unwrap_or(ceiling);
        let charged = target.max(doubled).min(ceiling);
        self.charge
            .grow_to(charged)
            .map_err(|error| Report::new(TransportError::Decode(error.to_string())))?;
        let charged: usize = usize::try_from(charged)
            .assured("a charge bounded by the class limit fits the address space");
        let room = charged
            .checked_sub(self.carry.len())
            .verified("the new charge covers everything already carried");
        self.carry.reserve(room);
        Ok(())
    }

    fn take_buffered_frame(&mut self) -> Result<Option<Vec<u8>>, Report<TransportError>> {
        let Some(header) = self.carry.get(..FRAME_HEADER_BYTES) else {
            return Ok(None);
        };
        let mut length = [0_u8; FRAME_HEADER_BYTES];
        length.copy_from_slice(header);
        let length = u32::from_be_bytes(length);
        if u64::from(length) > self.frame_limit {
            return Err(Report::new(TransportError::Decode(format!(
                "duplex frame of {length} bytes exceeds the {} byte limit",
                self.frame_limit
            ))));
        }
        let length: usize = length.arch_into();
        let end = FRAME_HEADER_BYTES
            .checked_add(length)
            .assured("a frame length bounded by the class limit fits beside its header");
        let Some(frame) = self.carry.get(FRAME_HEADER_BYTES..end) else {
            return Ok(None);
        };
        let frame = frame.to_vec();
        self.carry.drain(..end);
        Ok(Some(frame))
    }
}

/// Writes whole frames into one direction of a duplex stream.
pub(crate) struct FrameWriter {
    stream: SendStream<Bytes>,
    progress_timeout: Duration,
}

/// The two directions of a duplex stream the peer has accepted.
pub(crate) struct OpenedDuplexStream {
    pub(crate) writer: FrameWriter,
    pub(crate) body: RecvStream,
}

impl FrameWriter {
    pub(crate) fn new(stream: SendStream<Bytes>, progress_timeout: Duration) -> Self {
        Self {
            stream,
            progress_timeout,
        }
    }

    pub(crate) async fn send_frame(
        &mut self,
        payload: ChargedBytes,
    ) -> Result<(), Report<TransportError>> {
        let length = u32::try_from(payload.len()).map_err(|_| {
            Report::new(TransportError::Encode(
                "duplex frame exceeds u32 bytes".to_string(),
            ))
        })?;
        let progress_timeout = self.progress_timeout;
        let send = async {
            self.send_all(Bytes::copy_from_slice(&length.to_be_bytes()))
                .await?;
            let mut offset = 0;
            while offset < payload.len() {
                tokio::task::consume_budget().await;
                let remaining = payload
                    .len()
                    .checked_sub(offset)
                    .verified("the send offset never advances beyond the frame payload");
                let end = offset
                    .checked_add(remaining.min(BODY_CHUNK_BYTES))
                    .verified("the next slice is bounded by the remaining frame payload");
                let chunk = payload
                    .slice(offset, end)
                    .verified("the slice bounds were checked against the frame payload");
                offset = end;
                self.send_all(Bytes::from_owner(chunk)).await?;
            }
            Ok::<(), Report<TransportError>>(())
        };
        match timeout(progress_timeout, send).await {
            Ok(result) => result,
            Err(_) => {
                self.stream.send_reset(Reason::CANCEL);
                Err(Report::new(TransportError::ProgressTimeout {
                    timeout: progress_timeout,
                }))
            }
        }
    }

    async fn send_all(&mut self, mut body: Bytes) -> Result<(), Report<TransportError>> {
        while !body.is_empty() {
            tokio::task::consume_budget().await;
            self.stream.reserve_capacity(body.len());
            let assigned = poll_fn(|context| self.stream.poll_capacity(context))
                .await
                .ok_or_else(|| {
                    Report::new(TransportError::Decode(
                        "HTTP/2 stream closed while assigning send capacity".to_string(),
                    ))
                })?
                .map_err(TransportError::from)?;
            let ready = assigned.min(body.len());
            if ready == 0 {
                continue;
            }
            self.stream
                .send_data(body.split_to(ready), false)
                .map_err(TransportError::from)?;
        }
        self.stream.reserve_capacity(0);
        Ok(())
    }

    /// Half-close this direction. The peer sees the frame sequence end here.
    pub(crate) fn finish(&mut self) -> Result<(), Report<TransportError>> {
        self.stream
            .send_data(Bytes::new(), true)
            .map_err(TransportError::from)?;
        Ok(())
    }
}

/// Both halves of one duplex stream hold the connection slot open until the last one is dropped.
struct DuplexHold {
    _lease: StreamLease,
    _admission: RequestAdmission,
}

/// The frames a caller submits, in the order it submits them.
pub struct DuplexSender<M: InterconnectDuplexRequest> {
    writer: FrameWriter,
    executor: Executor,
    node: ClusterNodeName,
    _hold: Arc<DuplexHold>,
    item: PhantomData<fn(M::Item)>,
}

impl<M: InterconnectDuplexRequest> DuplexSender<M> {
    /// Submit one frame. Frames arrive at the peer in submission order.
    ///
    /// Returns how many encoded bytes the frame carried, which is what a caller bounding its own
    /// outstanding work counts.
    pub async fn send(&mut self, item: M::Item) -> Result<u64, Report<RequestError>> {
        let (payload, reservation) = item
            .encode_rkyv(
                self.executor.clone(),
                M::CLASS,
                M::CLASS.payload_limit(&self.executor),
            )
            .await
            .map_err(|error| {
                Report::new(RequestError::Encode { request: M::NAME }).attach_printable(error)
            })?;
        let frame = ChargedBytes::from_owned(payload, reservation);
        let bytes = u64::try_from(frame.len())
            .assured("supported targets have a pointer width no larger than u64");
        self.writer.send_frame(frame).await.map_err(|error| {
            Report::new(RequestError::Stream {
                node: self.node.clone(),
                request: M::NAME,
                reason: error.to_string(),
            })
        })?;
        Ok(bytes)
    }

    /// Stop submitting. The peer finishes the frames it already has and then ends its own
    /// direction.
    pub fn finish(&mut self) -> Result<(), Report<RequestError>> {
        self.writer.finish().map_err(|error| {
            Report::new(RequestError::Stream {
                node: self.node.clone(),
                request: M::NAME,
                reason: error.to_string(),
            })
        })
    }
}

/// The answers a peer produced, in the order it produced them.
pub struct DuplexReceiver<M: InterconnectDuplexRequest> {
    reader: FrameReader,
    executor: Executor,
    node: ClusterNodeName,
    _hold: Arc<DuplexHold>,
    response: PhantomData<fn() -> M::Response>,
}

impl<M: InterconnectDuplexRequest> DuplexReceiver<M> {
    /// The next answer, or `None` once the peer ended its direction.
    ///
    /// This waits as long as the stream stays open. A caller with work outstanding owns the
    /// deadline for that work and applies it here.
    pub async fn next(&mut self) -> Result<Option<M::Response>, Report<RequestError>> {
        let frame = self.reader.next_frame().await.map_err(|error| {
            Report::new(RequestError::Stream {
                node: self.node.clone(),
                request: M::NAME,
                reason: error.to_string(),
            })
        })?;
        let Some(frame) = frame else {
            return Ok(None);
        };
        let (response, _reservation) =
            M::Response::decode_rkyv(self.executor.clone(), M::CLASS, frame)
                .await
                .map_err(|error| {
                    Report::new(RequestError::Decode { request: M::NAME }).attach_printable(error)
                })?;
        Ok(Some(response))
    }
}

/// The frames one peer sent after it opened a duplex stream.
pub struct DuplexItems<T> {
    reader: FrameReader,
    executor: Executor,
    class: PoolClass,
    request: &'static str,
    item: PhantomData<fn() -> T>,
}

impl<T> DuplexItems<T> {
    pub(crate) fn new(
        reader: FrameReader,
        executor: Executor,
        class: PoolClass,
        request: &'static str,
    ) -> Self {
        Self {
            reader,
            executor,
            class,
            request,
            item: PhantomData,
        }
    }
}

impl<T: RkyvMessage> DuplexItems<T> {
    /// The next frame, or `None` once the peer half-closed its direction.
    pub async fn next(&mut self) -> Result<Option<T>, Report<StreamHandlerError>> {
        let frame = self
            .reader
            .next_frame()
            .await
            .map_err(|error| Report::new(StreamHandlerError::new(error.to_string())))?;
        let Some(frame) = frame else {
            return Ok(None);
        };
        let (item, _reservation) = T::decode_rkyv(self.executor.clone(), self.class, frame)
            .await
            .map_err(|error| {
                Report::new(StreamHandlerError::new(format!(
                    "{} frame: {error}",
                    self.request
                )))
            })?;
        Ok(Some(item))
    }
}

/// A handler-owned sequence of answers, delivered in the order the handler produces them.
pub struct DuplexResponses<T> {
    items: Pin<Box<dyn Stream<Item = Result<T, Report<StreamHandlerError>>> + Send + 'static>>,
}

impl<T> DuplexResponses<T> {
    pub fn new<S>(items: S) -> Self
    where
        S: Stream<Item = Result<T, Report<StreamHandlerError>>> + Send + 'static,
    {
        Self {
            items: Box::pin(items),
        }
    }

    pub(crate) fn into_stream(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<T, Report<StreamHandlerError>>> + Send + 'static>> {
        self.items
    }
}

impl TransportState {
    pub(crate) async fn open_duplex_stream<M: InterconnectDuplexRequest>(
        &self,
        node_id: &ClusterNodeName,
        request: RawDuplexRequest,
    ) -> Result<(DuplexSender<M>, DuplexReceiver<M>), Report<TransportError>> {
        let RawDuplexRequest {
            class,
            subquota,
            body,
            timeout: setup_timeout,
            admission,
        } = request;
        let deadline = Instant::now()
            .checked_add(setup_timeout)
            .ok_or_else(|| TransportError::InvalidOptions {
                reason: "duplex setup deadline exceeds the monotonic clock range".to_string(),
            })?;
        let lease = self.lease(node_id, class, subquota, deadline).await?;
        let OpenedDuplexStream { writer, body } = lease
            .connection
            .open_duplex_raw(self, DUPLEX_PATH, class, body, setup_timeout)
            .await?;
        let frame_limit = class.payload_limit(&self.executor);
        let reader =
            FrameReader::open(&self.executor, class.memory_class(), frame_limit, body).await?;
        let hold = Arc::new(DuplexHold {
            _lease: lease,
            _admission: admission,
        });
        Ok((
            DuplexSender {
                writer,
                executor: self.executor.clone(),
                node: node_id.clone(),
                _hold: hold.clone(),
                item: PhantomData,
            },
            DuplexReceiver {
                reader,
                executor: self.executor.clone(),
                node: node_id.clone(),
                _hold: hold,
                response: PhantomData,
            },
        ))
    }

    pub(super) async fn handle_duplex_request(
        &self,
        peer_node_id: ClusterNodeName,
        peer_advertised_host: String,
        class: PoolClass,
        body: RecvStream,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), Report<TransportError>> {
        let frame_limit = class.control_body_limit(&self.executor);
        let mut reader =
            FrameReader::open(&self.executor, class.memory_class(), frame_limit, body).await?;
        let Some(opening) = reader.next_frame().await? else {
            send_static_error(
                &mut respond,
                StatusCode::BAD_REQUEST,
                "duplex stream ended before its opening frame",
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        };
        let opening = wire::decode_rkyv_payload::<RequestEnvelope>(
            &self.executor,
            class.memory_class(),
            class.cpu_class(),
            opening,
        )
        .await?;
        let (opening, _opening_reservation) = opening.into_parts();
        if opening.class != class {
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
            handled = self.requests.handle_duplex(
                &self.executor,
                peer_node_id,
                peer_advertised_host,
                opening,
                reader,
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
        let (mut responses, _admission) = handled.into_parts();
        let headers = Response::builder()
            .status(StatusCode::OK)
            .version(Version::HTTP_2)
            .body(())
            .map_err(|error| TransportError::Http(error.to_string()))?;
        let mut writer = FrameWriter::new(
            respond
                .send_response(headers, false)
                .map_err(TransportError::from)?,
            self.options.progress_timeout,
        );
        while let Some(frame) = responses.next().await {
            tokio::task::consume_budget().await;
            let frame = match frame {
                Ok(frame) => frame,
                Err(error) => {
                    writer.stream.send_reset(Reason::INTERNAL_ERROR);
                    return Err(Report::new(TransportError::Decode(error.to_string())));
                }
            };
            writer.send_frame(frame).await?;
        }
        writer.finish()?;
        Ok(())
    }
}
