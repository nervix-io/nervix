//! A backup download and a restore stream, each carried over a console WebSocket of its own.
//!
//! Layer: edges.
//!
//! - **Owns.** Carrying one download or one restore stream over an upgraded console connection:
//!   one frame per binary message each way, ending a restore stream where the size its start
//!   declares ends, and the close that ends the connection.
//! - **Depends on.** The transport-independent download and restore owners, the client wire
//!   WebSocket codecs, and the console WebSocket's message classification.
//! - **Must not know.** How the connection was upgraded or authenticated, or what a download or a
//!   restore does with the frames it is given.
//!
//! A download reads its one request and then only sends. A data message after the request breaks
//! the call and closes the connection, and a close or a failed connection stops the download, which
//! releases only its own hold on the archive.
//!
//! A gRPC client ends a restore stream by half-closing it, but a WebSocket client that sends a
//! close can no longer read the reply. A restore stream over a console WebSocket therefore ends
//! with the chunk that completes the size its start declares, and the connection stays open for
//! the reply. A close or a failure before that chunk ends the call in transport, which changes
//! nothing; a frame the restore cannot read ends the stream at that frame, which the restore
//! refuses without reading further.
//!
//! When the node begins to stop, a download or a restore still running ends at once with a close
//! that says so, as a native gRPC call is cut when admission closes, so no client holds the drain
//! or the process open. A restore admitted with its whole archive goes on without its call.

use std::{
    fmt::Display,
    future::ready,
    num::NonZeroU64,
    pin::Pin,
    task::{Context, Poll},
};

use arch_into::ArchInto as _;
use futures_util::{Sink, SinkExt as _, Stream, StreamExt as _, stream};
use nervix_client_wire::{
    BackupDownloadFrame, EncodedFrame, FrameRoot, RestoreFrame, RestoreMessage, SessionLimits,
    VerifiedFrame,
    websocket::{ServerBackupDownloadWebSocketCodec, ServerRestoreWebSocketCodec},
};
use nervix_models::UserName;
use tokio_tungstenite::tungstenite::{
    self, Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
};
use tracing::debug;

use super::{
    download::DownloadFrameStream,
    websocket::{ConsoleMessage, Violation, close_frame},
};
use crate::application::session_service::SessionServiceImpl;

/// The close code for a call the client broke by sending a frame the call does not take.
const CLOSE_POLICY_VIOLATION: u16 = 1008;

/// The close code for an answer the server could not put into a frame.
const CLOSE_INTERNAL_ERROR: u16 = 1011;

/// Why a console call ended before its answer was complete.
#[derive(Debug)]
pub(in crate::application) enum ConsoleCallEnd {
    /// The client closed the connection.
    Closed,
    /// The connection failed.
    Failed,
    /// The client broke the call's framing.
    Violation { code: u16, reason: String },
    /// The node began to stop.
    ShuttingDown,
}

impl ConsoleCallEnd {
    /// What `message` gives a call: a frame, the end of the call, or nothing for a control
    /// message.
    fn item<Inbound: FrameRoot>(
        message: ConsoleMessage<Inbound>,
    ) -> Option<Result<VerifiedFrame<Inbound>, Self>> {
        match message {
            ConsoleMessage::Frame(frame) => Some(Ok(frame)),
            ConsoleMessage::Control => None,
            ConsoleMessage::Closed => Some(Err(Self::Closed)),
            ConsoleMessage::Failed => Some(Err(Self::Failed)),
            ConsoleMessage::Violation(violation) => Some(Err(Self::Violation {
                code: violation.code,
                reason: violation.reason,
            })),
        }
    }

    /// Ends the connection as this end calls for: it completes a close the client began, sends
    /// nothing on a failed connection, and otherwise closes with the code that says why.
    async fn finish<S, E>(self, sink: &mut S)
    where
        S: Sink<Message, Error = E> + Unpin,
        E: Display,
    {
        let frame = match self {
            Self::Closed => {
                if let Err(error) = sink.close().await {
                    debug!(error = %error, "a console connection failed while it closed");
                }
                return;
            }
            Self::Failed => return,
            Self::Violation { code, reason } => close_frame(Some(Violation { code, reason })),
            Self::ShuttingDown => CloseFrame {
                code: CloseCode::Away,
                reason: "the node is shutting down".into(),
            },
        };
        close(sink, frame).await;
    }
}

/// One step of a download that is sending its answer.
enum DownloadStep {
    /// The next frame of the answer, or `None` once every frame was sent.
    Frame(Option<EncodedFrame<BackupDownloadFrame>>),
    /// A message the client sent, or `None` once the connection ended.
    Received(Option<Result<Message, tungstenite::Error>>),
}

impl SessionServiceImpl {
    /// Serves one backup download for `user` over an upgraded console connection, then closes the
    /// connection.
    pub(in crate::application) async fn serve_console_download<S, E>(
        &self,
        user: UserName,
        limits: SessionLimits,
        connection: S,
    ) where
        S: Stream<Item = Result<Message, tungstenite::Error>>
            + Sink<Message, Error = E>
            + Unpin
            + Send
            + 'static,
        E: Display + Send + 'static,
    {
        let codec = ServerBackupDownloadWebSocketCodec::new(limits);
        let (mut sink, mut messages) = connection.split();
        let request = loop {
            nervix_primitives::task::consume_budget().await;
            let message = nervix_primitives::select! {
                biased;
                _ = self.inner.admission_shutdown.cancelled() => {
                    ConsoleCallEnd::ShuttingDown.finish(&mut sink).await;
                    return;
                }
                message = messages.next() => message,
            };
            let Some(message) = message else {
                return;
            };
            match ConsoleCallEnd::item(ConsoleMessage::read(&codec, message)) {
                Some(Ok(frame)) => break frame,
                None => {}
                Some(Err(end)) => {
                    end.finish(&mut sink).await;
                    return;
                }
            }
        };
        let frames = match self.serve_backup_download(user, &request, limits).await {
            Ok(frames) => frames.into_stream(),
            Err(error) => {
                debug!(error = %error, "a console download's answer does not fit a frame");
                let violation = Violation {
                    code: CLOSE_INTERNAL_ERROR,
                    reason: "the download's answer does not fit a frame".to_string(),
                };
                close(&mut sink, close_frame(Some(violation))).await;
                return;
            }
        };
        let ended = self
            .send_download_frames(&codec, &mut sink, &mut messages, frames)
            .await;
        match ended {
            Some(end) => end.finish(&mut sink).await,
            None => close(&mut sink, close_frame(None)).await,
        }
    }

    /// Sends every frame of a download's answer, while it watches for the client leaving or
    /// breaking the call and for the node stopping. `None` says every frame was sent.
    async fn send_download_frames<S, M, E>(
        &self,
        codec: &ServerBackupDownloadWebSocketCodec,
        sink: &mut S,
        messages: &mut M,
        mut frames: DownloadFrameStream,
    ) -> Option<ConsoleCallEnd>
    where
        S: Sink<Message, Error = E> + Unpin,
        M: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
        E: Display,
    {
        loop {
            nervix_primitives::task::consume_budget().await;
            // What the client sent is heard before another frame goes out, so its close stops the
            // download while frames remain and the archive stays retained for the next one.
            let step = nervix_primitives::select! {
                biased;
                _ = self.inner.admission_shutdown.cancelled() => {
                    return Some(ConsoleCallEnd::ShuttingDown);
                }
                message = messages.next() => DownloadStep::Received(message),
                frame = frames.next() => DownloadStep::Frame(frame),
            };
            match step {
                DownloadStep::Frame(Some(frame)) => {
                    let payload = Vec::from(codec.encode(frame));
                    // A client that stops reading holds the write, but never the node's stop.
                    let sent = nervix_primitives::select! {
                        biased;
                        _ = self.inner.admission_shutdown.cancelled() => {
                            return Some(ConsoleCallEnd::ShuttingDown);
                        }
                        sent = sink.send(Message::Binary(payload)) => sent,
                    };
                    if let Err(error) = sent {
                        debug!(error = %error, "a console download stopped taking frames");
                        return Some(ConsoleCallEnd::Failed);
                    }
                }
                DownloadStep::Frame(None) => return None,
                DownloadStep::Received(None) => return Some(ConsoleCallEnd::Failed),
                DownloadStep::Received(Some(message)) => {
                    match ConsoleCallEnd::item(ConsoleMessage::read(codec, message)) {
                        None => {}
                        Some(Ok(_)) => {
                            return Some(ConsoleCallEnd::Violation {
                                code: CLOSE_POLICY_VIOLATION,
                                reason: "a download carries exactly one request".to_string(),
                            });
                        }
                        Some(Err(end)) => return Some(end),
                    }
                }
            }
        }
    }

    /// Serves one restore stream for `user` over an upgraded console connection: answers it with
    /// its reply, then closes the connection.
    pub(in crate::application) async fn serve_console_restore<S, E>(
        &self,
        user: UserName,
        limits: SessionLimits,
        connection: S,
    ) where
        S: Stream<Item = Result<Message, tungstenite::Error>>
            + Sink<Message, Error = E>
            + Unpin
            + Send
            + 'static,
        E: Display + Send + 'static,
    {
        let codec = ServerRestoreWebSocketCodec::new(limits);
        let (mut sink, messages) = connection.split();
        let reader = codec.clone();
        // A connection that ends without a close message ended abnormally.
        let frames = messages
            .filter_map(move |message| {
                ready(ConsoleCallEnd::item(ConsoleMessage::read(&reader, message)))
            })
            .chain(stream::once(ready(Err(ConsoleCallEnd::Failed))));
        let frames = DeclaredRestoreStream::new(Box::pin(frames));
        // Stopping the node ends the call: staging an archive that had not fully arrived is
        // released, and a restore admitted with its whole archive goes on without its call.
        let answered = nervix_primitives::select! {
            biased;
            _ = self.inner.admission_shutdown.cancelled() => Err(ConsoleCallEnd::ShuttingDown),
            answered = self.serve_restore(user, frames) => answered,
        };
        let reply = match answered {
            Ok(reply) => reply,
            Err(end) => {
                end.finish(&mut sink).await;
                return;
            }
        };
        let frame = match reply.encode(&limits) {
            Ok(frame) => frame,
            Err(error) => {
                debug!(error = %error, "a console restore's reply does not fit a frame");
                let violation = Violation {
                    code: CLOSE_INTERNAL_ERROR,
                    reason: "the restore's reply does not fit a frame".to_string(),
                };
                close(&mut sink, close_frame(Some(violation))).await;
                return;
            }
        };
        let payload = Vec::from(codec.encode(frame));
        let sent = nervix_primitives::select! {
            biased;
            _ = self.inner.admission_shutdown.cancelled() => return,
            sent = sink.send(Message::Binary(payload)) => sent,
        };
        if let Err(error) = sent {
            debug!(error = %error, "a console restore's connection closed before its reply");
            return;
        }
        close(&mut sink, close_frame(None)).await;
    }
}

/// Closes a console connection with `frame`.
async fn close<S, E>(sink: &mut S, frame: CloseFrame<'static>)
where
    S: Sink<Message, Error = E> + Unpin,
    E: Display,
{
    if let Err(error) = sink.send(Message::Close(Some(frame))).await {
        debug!(error = %error, "a console connection closed before its close frame");
    }
}

/// How much of a restore stream over a WebSocket has arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeclaredProgress {
    /// The start has not arrived.
    AwaitingStart,
    /// The start declared the archive's size, and this many of its bytes have not arrived.
    Remaining(NonZeroU64),
    /// Every declared byte arrived, or a frame broke the stream: nothing more is read.
    Ended,
}

impl DeclaredProgress {
    /// Where the stream stands once `frame` arrived.
    fn after(self, frame: &VerifiedFrame<RestoreFrame>) -> Self {
        let message = match RestoreMessage::decode(frame) {
            Ok(message) => message,
            // The restore refuses a frame it cannot read, and reads nothing after it.
            Err(_) => return Self::Ended,
        };
        match (self, message) {
            (Self::AwaitingStart, RestoreMessage::Start(start)) => {
                Self::Remaining(start.archive.total_bytes)
            }
            (Self::Remaining(remaining), RestoreMessage::Chunk(chunk)) => {
                let length: u64 = chunk.bytes().len().arch_into();
                // A chunk past the declared size ends the stream too; the restore refuses it.
                let Some(left) = remaining.get().checked_sub(length) else {
                    return Self::Ended;
                };
                match NonZeroU64::new(left) {
                    Some(left) => Self::Remaining(left),
                    None => Self::Ended,
                }
            }
            // A stream that does not begin with its start, or carries a second one, breaks the
            // framing; the restore refuses it without reading further.
            (Self::AwaitingStart, RestoreMessage::Chunk(_))
            | (Self::Remaining(_), RestoreMessage::Start(_))
            | (Self::Ended, _) => Self::Ended,
        }
    }
}

/// A restore stream over a WebSocket, which ends with the chunk that completes the size its start
/// declares.
struct DeclaredRestoreStream<S> {
    frames: S,
    progress: DeclaredProgress,
}

impl<S> DeclaredRestoreStream<S> {
    fn new(frames: S) -> Self {
        Self {
            frames,
            progress: DeclaredProgress::AwaitingStart,
        }
    }
}

impl<S, E> Stream for DeclaredRestoreStream<S>
where
    S: Stream<Item = Result<VerifiedFrame<RestoreFrame>, E>> + Unpin,
{
    type Item = Result<VerifiedFrame<RestoreFrame>, E>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.progress == DeclaredProgress::Ended {
            return Poll::Ready(None);
        }
        let Poll::Ready(item) = self.frames.poll_next_unpin(context) else {
            return Poll::Pending;
        };
        let Some(item) = item else {
            self.progress = DeclaredProgress::Ended;
            return Poll::Ready(None);
        };
        self.progress = match &item {
            Ok(frame) => self.progress.after(frame),
            Err(_) => DeclaredProgress::Ended,
        };
        Poll::Ready(Some(item))
    }
}

#[cfg(test)]
mod tests;
