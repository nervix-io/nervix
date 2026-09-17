//! Frames over a WebSocket.
//!
//! Each binary message holds exactly one frame, and nothing else: no size prefix, and never two
//! frames or part of one. A text message is a protocol violation. Ping, pong and close messages
//! belong to the WebSocket connection and never carry a frame. The codec knows no WebSocket
//! library, so the browser and the server share it.

use std::{fmt, marker::PhantomData};

use bytes::Bytes;
use error_stack::Report;
use thiserror::Error;

use crate::{
    frame::{ClientFrame, EncodedFrame, FrameError, FrameRoot, ServerFrame, VerifiedFrame},
    limits::SessionLimits,
};

/// The close code for a message the protocol does not accept, such as a text message.
const CLOSE_UNSUPPORTED_DATA: u16 = 1003;

/// The close code for a message that is not a valid frame.
const CLOSE_INVALID_PAYLOAD: u16 = 1007;

/// The close code for a message larger than the frame limit.
const CLOSE_MESSAGE_TOO_BIG: u16 = 1009;

/// A data message received on a session WebSocket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketData {
    Binary(Bytes),
    Text,
}

/// Why a WebSocket message did not carry a frame. Every such message ends the connection.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WebSocketError {
    #[error("a text message is not a session frame")]
    TextMessage,
    #[error("a binary message is not a valid frame")]
    InvalidFrame,
}

impl WebSocketError {
    /// The close code the receiver ends the connection with.
    pub fn close_code(report: &Report<Self>) -> u16 {
        match report.current_context() {
            Self::TextMessage => CLOSE_UNSUPPORTED_DATA,
            Self::InvalidFrame => match report.downcast_ref::<FrameError>() {
                Some(FrameError::TooLarge { .. }) => CLOSE_MESSAGE_TOO_BIG,
                _ => CLOSE_INVALID_PAYLOAD,
            },
        }
    }
}

/// Encodes the frames one side sends and verifies the frames it receives.
pub struct WebSocketCodec<Outbound: FrameRoot, Inbound: FrameRoot> {
    limits: SessionLimits,
    frames: PhantomData<fn(Outbound) -> Inbound>,
}

/// The codec of a browser or other client: it sends client frames and receives server frames.
pub type ClientWebSocketCodec = WebSocketCodec<ClientFrame, ServerFrame>;

/// The codec of the server: it sends server frames and receives client frames.
pub type ServerWebSocketCodec = WebSocketCodec<ServerFrame, ClientFrame>;

impl<Outbound: FrameRoot, Inbound: FrameRoot> WebSocketCodec<Outbound, Inbound> {
    pub fn new(limits: SessionLimits) -> Self {
        Self {
            limits,
            frames: PhantomData,
        }
    }

    /// The largest message the codec accepts. Configure the WebSocket library's message limit
    /// with it, so an oversized message is refused before it is buffered.
    pub fn max_message_bytes(&self) -> usize {
        self.limits.frame_bytes()
    }

    /// The payload of the binary message that carries `frame`.
    pub fn encode(&self, frame: EncodedFrame<Outbound>) -> Bytes {
        frame.into_bytes()
    }

    /// Verifies a received data message as one frame.
    pub fn decode(
        &self,
        data: WebSocketData,
    ) -> Result<VerifiedFrame<Inbound>, Report<WebSocketError>> {
        let payload = match data {
            WebSocketData::Binary(payload) => payload,
            WebSocketData::Text => return Err(Report::new(WebSocketError::TextMessage)),
        };
        match VerifiedFrame::verify(payload, &self.limits) {
            Ok(frame) => Ok(frame),
            Err(error) => Err(error.change_context(WebSocketError::InvalidFrame)),
        }
    }
}

impl<Outbound: FrameRoot, Inbound: FrameRoot> Clone for WebSocketCodec<Outbound, Inbound> {
    fn clone(&self) -> Self {
        Self::new(self.limits)
    }
}

impl<Outbound: FrameRoot, Inbound: FrameRoot> fmt::Debug for WebSocketCodec<Outbound, Inbound> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketCodec")
            .field("outbound", &Outbound::NAME)
            .field("inbound", &Inbound::NAME)
            .field("limits", &self.limits)
            .finish()
    }
}
