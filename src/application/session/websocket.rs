//! The session over the console WebSocket.
//!
//! Layer: edges.
//!
//! - **Owns.** Carrying one session's frames over an upgraded console connection: one frame per
//!   binary message each way, the message limit, and the close that ends the connection.
//! - **Depends on.** The session engine, the client wire WebSocket codec, and tungstenite.
//! - **Must not know.** How the connection was upgraded or authenticated, or what any request
//!   does.
//!
//! A text message, a message that is not a valid frame, and a message above the frame limit each
//! end the connection with the close code the codec assigns them: 1003, 1007 and 1009. A session
//! the server ends, and one the client closes, ends with a normal close.

use std::{borrow::Cow, future::ready};

use bytes::Bytes;
use error_stack::Report;
use futures_util::{Sink, SinkExt as _, Stream, StreamExt as _, stream};
use nervix_client_wire::{
    SessionLimits,
    websocket::{ServerWebSocketCodec, WebSocketData, WebSocketError},
};
use nervix_models::UserName;
use nervix_recovery::{Discarded as _, Reported as _};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::{
    self, Message,
    error::CapacityError,
    protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::{
    InboundFrame, SessionTransport,
    outbound::{self, SessionFrames},
};
use crate::application::session_service::SessionServiceImpl;

/// The close code for a message larger than the frame limit, which tungstenite refuses before it
/// reaches the codec.
const CLOSE_MESSAGE_TOO_BIG: u16 = 1009;

/// The WebSocket configuration of a console session: no message or frame larger than one session
/// frame is buffered.
pub(in crate::application) fn console_websocket_config(limits: &SessionLimits) -> WebSocketConfig {
    WebSocketConfig {
        max_message_size: Some(limits.frame_bytes()),
        max_frame_size: Some(limits.frame_bytes()),
        ..WebSocketConfig::default()
    }
}

/// Why the connection ends, when the client broke the framing.
struct Violation {
    code: u16,
    reason: String,
}

/// Where the reader records the violation that ended the connection, so the writer closes with
/// its code.
struct ViolationReport {
    sender: Option<oneshot::Sender<Violation>>,
}

impl ViolationReport {
    fn report(&mut self, violation: Violation) {
        if let Some(sender) = self.sender.take() {
            sender
                .send(violation)
                .discarded("a writer that already stopped has no close left to send");
        }
    }

    /// The frame a received message carries, or `None` for a control message.
    fn inbound_frame(
        &mut self,
        codec: &ServerWebSocketCodec,
        message: Result<Message, tungstenite::Error>,
    ) -> Option<InboundFrame> {
        let message = match message {
            Ok(message) => message,
            Err(tungstenite::Error::Capacity(CapacityError::MessageTooLong { size, max_size })) => {
                self.report(Violation {
                    code: CLOSE_MESSAGE_TOO_BIG,
                    reason: format!("a message of {size} bytes exceeds the {max_size}-byte limit"),
                });
                return Some(InboundFrame::Failed);
            }
            Err(error) => {
                debug!(error = %error, "a console session connection failed");
                return Some(InboundFrame::Failed);
            }
        };
        let data = match message {
            Message::Binary(payload) => WebSocketData::Binary(Bytes::from(payload)),
            Message::Text(_) => WebSocketData::Text,
            Message::Close(_) => return Some(InboundFrame::Closed),
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => return None,
        };
        match codec.decode(data) {
            Ok(frame) => Some(InboundFrame::Frame(frame)),
            Err(error) => {
                self.report(violation(&error));
                Some(InboundFrame::Failed)
            }
        }
    }
}

fn violation(error: &Report<WebSocketError>) -> Violation {
    Violation {
        code: WebSocketError::close_code(error),
        reason: error.current_context().to_string(),
    }
}

impl SessionServiceImpl {
    /// Serves one console session over an upgraded connection until it ends, then closes the
    /// connection.
    pub(in crate::application) async fn serve_console_session<S, E>(
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
        E: std::fmt::Display + Send + 'static,
    {
        let codec = ServerWebSocketCodec::new(limits);
        let (sink, messages) = connection.split();
        let (violation_sender, violation) = oneshot::channel();
        let mut violations = ViolationReport {
            sender: Some(violation_sender),
        };
        let reader_codec = codec.clone();
        // A connection that ends without a close message ended abnormally.
        let inbound = messages
            .filter_map(move |message| ready(violations.inbound_frame(&reader_codec, message)))
            .chain(stream::once(ready(InboundFrame::Failed)));
        let inbound = Box::pin(inbound);
        let (outbound, frames) = outbound::channel(CancellationToken::new());
        let writer = self
            .inner
            .service_tasks
            .spawn(write_frames(sink, codec, frames, violation));
        self.run_session(user, SessionTransport::Console, limits, inbound, outbound)
            .await;
        writer
            .await
            .reported("wait for a console session's writer to close its connection");
    }
}

/// Writes every frame the session queues, then closes the connection: with the code of the
/// violation that ended it, or normally.
async fn write_frames<S, E>(
    mut sink: S,
    codec: ServerWebSocketCodec,
    mut frames: SessionFrames,
    violation: oneshot::Receiver<Violation>,
) where
    S: Sink<Message, Error = E> + Unpin,
    E: std::fmt::Display,
{
    while let Some(frame) = frames.next().await {
        tokio::task::consume_budget().await;
        let payload = Vec::from(codec.encode(frame));
        if let Err(error) = sink.send(Message::Binary(payload)).await {
            debug!(error = %error, "a console session connection stopped taking frames");
            return;
        }
    }
    let close = match violation.await {
        Ok(violation) => CloseFrame {
            code: CloseCode::from(violation.code),
            reason: Cow::Owned(violation.reason),
        },
        Err(_) => CloseFrame {
            code: CloseCode::Normal,
            reason: Cow::Borrowed(""),
        },
    };
    if let Err(error) = sink.send(Message::Close(Some(close))).await {
        debug!(error = %error, "a console session connection closed before its close frame");
    }
}
