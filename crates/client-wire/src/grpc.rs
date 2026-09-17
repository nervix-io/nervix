//! Frames over gRPC.
//!
//! The session is served as the gRPC service `nervix.session.Session`. Each gRPC message holds
//! exactly one frame, delimited by gRPC's own length prefix, so a frame carries no size prefix of
//! its own. The codec plugs into tonic directly; no generated gRPC code is involved.

use std::{fmt, marker::PhantomData};

use bytes::{Buf, BufMut, Bytes};
use tonic::{
    Status,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
};

use crate::{
    frame::{
        ClientFrame, EncodedFrame, FrameError, FrameRoot, ServerFrame, UploadFrame,
        UploadReplyFrame, VerifiedFrame,
    },
    limits::SessionLimits,
};

/// The gRPC service name.
pub const SERVICE_NAME: &str = "nervix.session.Session";

/// The bidirectional stream of client frames answered by server frames.
pub const EXCHANGE_PATH: &str = "/nervix.session.Session/Exchange";

/// The client stream of upload frames answered by one upload reply frame.
pub const UPLOAD_RESOURCE_PATH: &str = "/nervix.session.Session/UploadResource";

/// Frames below this size are copied out of tonic's read buffer.
///
/// Taking a frame without copying shares the read buffer's allocation, which grows to the largest
/// message the connection has carried. A small frame retained for long would keep that whole
/// allocation alive, so small frames get an allocation of their own; a large frame dominates its
/// allocation anyway and is taken without a copy.
const COPY_BELOW_BYTES: usize = 64 * 1024;

/// A tonic codec that sends frames of one root and receives frames of another.
pub struct FrameCodec<Outbound: FrameRoot, Inbound: FrameRoot> {
    limits: SessionLimits,
    frames: PhantomData<fn(Outbound) -> Inbound>,
}

/// The client side of the session exchange.
pub type ClientExchangeCodec = FrameCodec<ClientFrame, ServerFrame>;

/// The server side of the session exchange.
pub type ServerExchangeCodec = FrameCodec<ServerFrame, ClientFrame>;

/// The client side of a resource upload.
pub type ClientUploadCodec = FrameCodec<UploadFrame, UploadReplyFrame>;

/// The server side of a resource upload.
pub type ServerUploadCodec = FrameCodec<UploadReplyFrame, UploadFrame>;

impl<Outbound: FrameRoot, Inbound: FrameRoot> FrameCodec<Outbound, Inbound> {
    /// A codec holding frames to `limits`. Configure tonic's message size limits with
    /// [`SessionLimits::frame_bytes`] as well, so an oversized message is refused before tonic
    /// buffers it.
    pub fn new(limits: SessionLimits) -> Self {
        Self {
            limits,
            frames: PhantomData,
        }
    }
}

impl<Outbound: FrameRoot, Inbound: FrameRoot> Codec for FrameCodec<Outbound, Inbound> {
    type Encode = EncodedFrame<Outbound>;
    type Decode = VerifiedFrame<Inbound>;
    type Encoder = FrameEncoder<Outbound>;
    type Decoder = FrameDecoder<Inbound>;

    fn encoder(&mut self) -> Self::Encoder {
        FrameEncoder {
            frames: PhantomData,
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        FrameDecoder {
            limits: self.limits,
            frames: PhantomData,
        }
    }
}

impl<Outbound: FrameRoot, Inbound: FrameRoot> Clone for FrameCodec<Outbound, Inbound> {
    fn clone(&self) -> Self {
        Self::new(self.limits)
    }
}

impl<Outbound: FrameRoot, Inbound: FrameRoot> fmt::Debug for FrameCodec<Outbound, Inbound> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FrameCodec")
            .field("outbound", &Outbound::NAME)
            .field("inbound", &Inbound::NAME)
            .field("limits", &self.limits)
            .finish()
    }
}

/// Writes an encoded frame as one gRPC message.
#[derive(Debug)]
pub struct FrameEncoder<Outbound: FrameRoot> {
    frames: PhantomData<fn(Outbound)>,
}

impl<Outbound: FrameRoot> Encoder for FrameEncoder<Outbound> {
    type Item = EncodedFrame<Outbound>;
    type Error = Status;

    fn encode(&mut self, frame: Self::Item, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(frame.bytes());
        Ok(())
    }
}

/// Verifies each gRPC message as one frame.
///
/// A message above the frame limit ends the call with `RESOURCE_EXHAUSTED`, and a message that is
/// not a valid frame with `INTERNAL`, the status gRPC assigns to a message that fails to parse on
/// either side of a call.
#[derive(Debug)]
pub struct FrameDecoder<Inbound: FrameRoot> {
    limits: SessionLimits,
    frames: PhantomData<fn() -> Inbound>,
}

impl<Inbound: FrameRoot> Decoder for FrameDecoder<Inbound> {
    type Item = VerifiedFrame<Inbound>;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Status> {
        let length = source.remaining();
        let shared = source.copy_to_bytes(length);
        let bytes = if length < COPY_BELOW_BYTES {
            Bytes::copy_from_slice(&shared)
        } else {
            shared
        };
        match VerifiedFrame::verify(bytes, &self.limits) {
            Ok(frame) => Ok(Some(frame)),
            Err(error) => {
                let status = match error.current_context() {
                    FrameError::TooLarge { .. } => Status::resource_exhausted(error.to_string()),
                    FrameError::Truncated { .. }
                    | FrameError::WrongIdentifier { .. }
                    | FrameError::Invalid { .. } => Status::internal(error.to_string()),
                };
                Err(status)
            }
        }
    }
}
