//! The harness's raw producer session: one console WebSocket session that opens producers,
//! submits batches and reads their outcomes, speaking the FlatBuffers session protocol directly
//! as a browser client does.
//!
//! Outside the layer order: a test harness. Product code must not name it.
//!
//! - **Owns.** The session's request identities, a reader task that files every reply with the
//!   instant it arrived and every producer frame in arrival order, and the requests scenarios
//!   send, including batches no client library would submit.
//! - **Depends on.** The client wire contract and the console WebSocket endpoint, whose address
//!   carries the shared test credentials.
//! - **Must not know.** Server internals; everything it observes arrives through the public
//!   protocol.
//!
//! The reader runs for as long as the session lives, so the instant a reply is filed with is the
//! instant it arrived, whatever step happens to ask for it later.

use std::{collections::BTreeMap, io, num::NonZeroU64, time::Duration};

use futures_util::{SinkExt as _, StreamExt as _};
use nervix_client_wire::{
    ClientFrame, ClientMessage, ClientRequest, EncodedFrame, ProducerAdmissionChanged,
    ProducerEnded, ProducerId, ReplyBody, RequestId, ServerEvent, ServerFrame, ServerMessage,
    SessionLimits, TransferAssembly, VerifiedFrame,
};
use nervix_primitives::{
    sync::{Arc, Notify, blocking::Mutex, mpsc},
    task::AbortOnDropHandle,
    time::Instant,
};
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// How many frames the session queues for its transport before a send waits.
const OUTBOUND_FRAMES: usize = 64;

/// A reply and when the reader filed it.
#[derive(Debug, Clone)]
pub(crate) struct TimedReply {
    pub(crate) body: ReplyBody,
    pub(crate) arrived_at: Instant,
}

/// A frame about one producer the server sent without a request.
#[derive(Debug, Clone)]
pub(crate) enum ProducerFrame {
    Admission(ProducerAdmissionChanged),
    Ended(ProducerEnded),
}

impl ProducerFrame {
    pub(crate) fn producer(&self) -> ProducerId {
        match self {
            Self::Admission(changed) => changed.producer,
            Self::Ended(ended) => ended.producer,
        }
    }
}

/// A producer frame a step waited for, and how many frames about its producer have been
/// accounted for once it is.
#[derive(Debug, Clone)]
pub(crate) struct FoundProducerFrame {
    pub(crate) frame: ProducerFrame,
    pub(crate) seen: usize,
}

/// Everything the reader filed, and how the session ended once it did.
#[derive(Default)]
struct Filed {
    replies: BTreeMap<RequestId, TimedReply>,
    transfers: BTreeMap<RequestId, TransferAssembly>,
    /// Frames about producers, in the order they arrived.
    producer_frames: Vec<ProducerFrame>,
    ended: Option<String>,
}

/// What the reader shares with the steps that wait on it.
#[derive(Default)]
struct Inbox {
    filed: Mutex<Filed>,
    changed: Notify,
}

impl Inbox {
    /// Files one frame the server sent.
    fn file(&self, frame: &VerifiedFrame<ServerFrame>, limits: &SessionLimits) -> io::Result<()> {
        let arrived_at = Instant::now();
        let message = ServerMessage::decode(frame).map_err(io::Error::other)?;
        let mut filed = self.filed.lock();
        match message {
            ServerMessage::Reply(reply) => {
                filed.replies.insert(
                    reply.request_id,
                    TimedReply {
                        body: reply.body,
                        arrived_at,
                    },
                );
            }
            ServerMessage::TransferPart(part) => {
                let request_id = part.request_id();
                let assembly = filed
                    .transfers
                    .entry(request_id)
                    .or_insert_with(|| TransferAssembly::new(request_id, limits));
                assembly.append(&part).map_err(io::Error::other)?;
                if assembly.is_complete() {
                    let Some(assembly) = filed.transfers.remove(&request_id) else {
                        return Err(io::Error::other("the assembly was just appended to"));
                    };
                    let reply = assembly.finish().map_err(io::Error::other)?;
                    filed.replies.insert(
                        reply.request_id,
                        TimedReply {
                            body: reply.body,
                            arrived_at,
                        },
                    );
                }
            }
            ServerMessage::Event(ServerEvent::ProducerAdmissionChanged(changed)) => {
                filed
                    .producer_frames
                    .push(ProducerFrame::Admission(changed));
            }
            ServerMessage::Event(ServerEvent::ProducerEnded(ended)) => {
                filed.producer_frames.push(ProducerFrame::Ended(ended));
            }
            // Observation events are not this session's subject.
            ServerMessage::Event(_) => {}
        }
        drop(filed);
        self.changed.notify_waiters();
        Ok(())
    }

    fn end(&self, why: String) {
        let mut filed = self.filed.lock();
        if filed.ended.is_none() {
            filed.ended = Some(why);
        }
        drop(filed);
        self.changed.notify_waiters();
    }

    /// Waits until `found` finds what it looks for in what the reader filed.
    async fn wait_for<T>(
        &self,
        timeout: Duration,
        what: &str,
        mut found: impl FnMut(&Filed) -> Option<T>,
    ) -> io::Result<T> {
        let deadline = nervix_primitives::time::Instant::now() + timeout;
        loop {
            nervix_primitives::task::consume_budget().await;
            let changed = self.changed.notified();
            let mut changed = std::pin::pin!(changed);
            changed.as_mut().enable();
            {
                let filed = self.filed.lock();
                if let Some(value) = found(&filed) {
                    return Ok(value);
                }
                if let Some(why) = &filed.ended {
                    return Err(io::Error::other(format!(
                        "the session ended before {what}: {why}"
                    )));
                }
            }
            if nervix_primitives::time::timeout_at(deadline, changed)
                .await
                .is_err()
            {
                return Err(io::Error::other(format!(
                    "timed out after {timeout:?} waiting for {what}"
                )));
            }
        }
    }
}

/// A raw session a scenario opens producers on.
pub(crate) struct RawProducerSession {
    limits: SessionLimits,
    frames: mpsc::Sender<EncodedFrame<ClientFrame>>,
    next_request: NonZeroU64,
    inbox: Arc<Inbox>,
    _reader: AbortOnDropHandle<()>,
    _writer: AbortOnDropHandle<()>,
}

impl std::fmt::Debug for RawProducerSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RawProducerSession")
            .finish_non_exhaustive()
    }
}

impl RawProducerSession {
    /// Opens a console WebSocket session at `url`, which carries its credentials.
    pub(crate) async fn connect_websocket(url: &str) -> io::Result<Self> {
        let limits = SessionLimits::DEFAULT;
        let (socket, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(io::Error::other)?;
        let (mut sink, mut stream) = socket.split();
        let (frames, mut outgoing) = mpsc::channel::<EncodedFrame<ClientFrame>>(OUTBOUND_FRAMES);
        let inbox = Arc::new(Inbox::default());
        let writer_inbox = inbox.clone();
        let writer = nervix_primitives::task::spawn(async move {
            while let Some(frame) = outgoing.recv().await {
                nervix_primitives::task::consume_budget().await;
                let message = WsMessage::Binary(frame.into_bytes().to_vec());
                if let Err(error) = sink.send(message).await {
                    writer_inbox.end(format!("the WebSocket refused a frame: {error}"));
                    return;
                }
            }
        });
        let reader_inbox = inbox.clone();
        let reader = nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let Some(message) = stream.next().await else {
                    reader_inbox.end("the WebSocket ended".to_string());
                    return;
                };
                let payload = match message {
                    Ok(WsMessage::Binary(payload)) => payload,
                    Ok(WsMessage::Close(frame)) => {
                        reader_inbox.end(format!("the server closed the WebSocket: {frame:?}"));
                        return;
                    }
                    Ok(_) => continue,
                    Err(error) => {
                        reader_inbox.end(format!("the WebSocket failed: {error}"));
                        return;
                    }
                };
                let verified =
                    VerifiedFrame::<ServerFrame>::verify(bytes::Bytes::from(payload), &limits);
                let frame = match verified {
                    Ok(frame) => frame,
                    Err(error) => {
                        reader_inbox.end(format!("an unverifiable frame arrived: {error:?}"));
                        return;
                    }
                };
                if let Err(error) = reader_inbox.file(&frame, &limits) {
                    reader_inbox.end(format!("an unreadable frame arrived: {error}"));
                    return;
                }
            }
        });
        Ok(Self {
            limits,
            frames,
            next_request: NonZeroU64::MIN,
            inbox,
            _reader: AbortOnDropHandle::new(reader),
            _writer: AbortOnDropHandle::new(writer),
        })
    }

    /// Sends one request and returns its identity.
    pub(crate) async fn send(&mut self, request: ClientRequest) -> io::Result<RequestId> {
        let request_id = RequestId::new(self.next_request);
        self.next_request = self
            .next_request
            .checked_add(1)
            .ok_or_else(|| io::Error::other("the session ran out of request identities"))?;
        let message = ClientMessage {
            request_id,
            request,
        };
        let frame = message.encode(&self.limits).map_err(io::Error::other)?;
        self.frames
            .send(frame)
            .await
            .map_err(|_| io::Error::other("the session's transport stopped taking frames"))?;
        Ok(request_id)
    }

    /// Waits for the terminal reply of `request_id`.
    pub(crate) async fn reply(
        &self,
        request_id: RequestId,
        timeout: Duration,
    ) -> io::Result<TimedReply> {
        self.inbox
            .wait_for(
                timeout,
                &format!("the reply to request {request_id}"),
                |filed| filed.replies.get(&request_id).cloned(),
            )
            .await
    }

    /// Waits for the first frame about `producer`, after the `seen` frames about it a scenario
    /// already accounted for, that `wanted` accepts.
    pub(crate) async fn producer_frame(
        &self,
        producer: ProducerId,
        seen: usize,
        timeout: Duration,
        what: &str,
        wanted: impl Fn(&ProducerFrame) -> bool,
    ) -> io::Result<FoundProducerFrame> {
        self.inbox
            .wait_for(timeout, what, |filed| {
                let mut position = 0_usize;
                for frame in &filed.producer_frames {
                    if frame.producer() != producer {
                        continue;
                    }
                    position = position
                        .checked_add(1)
                        .expect("a session holds fewer than usize::MAX frames");
                    if position <= seen || !wanted(frame) {
                        continue;
                    }
                    return Some(FoundProducerFrame {
                        frame: frame.clone(),
                        seen: position,
                    });
                }
                None
            })
            .await
    }
}
