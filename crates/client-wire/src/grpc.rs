//! Frames over gRPC.
//!
//! Layer: edges.
//! Owns: framing, verification and typed local rejection provenance for gRPC messages.
//! Depends on: the wire frames and limits, tonic, and primitive shared ownership.
//! Must not know: session recovery, execution outcomes or server runtime state.
//!
//! The session is served as the gRPC service `nervix.session.Session`. Each gRPC message holds
//! exactly one frame, delimited by gRPC's own length prefix, so a frame carries no size prefix of
//! its own. The codec plugs into tonic directly; no generated gRPC code is involved.

use std::{error::Error, fmt, marker::PhantomData};

use bytes::{Buf, BufMut, Bytes};
use error_stack::Report;
use nervix_primitives::sync::StdArc;
use tonic::{
    Status,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
};

use crate::{
    frame::{
        BackupDownloadFrame, BackupDownloadRequestFrame, ClientFrame, EncodedFrame, FrameError,
        FrameRoot, RestoreFrame, RestoreReplyFrame, ServerFrame, UploadFrame, UploadReplyFrame,
        VerifiedFrame,
    },
    limits::SessionLimits,
};

/// The gRPC service name.
pub const SERVICE_NAME: &str = "nervix.session.Session";

/// The bidirectional stream of client frames answered by server frames.
pub const EXCHANGE_PATH: &str = "/nervix.session.Session/Exchange";

/// The client stream of upload frames answered by one upload reply frame.
pub const UPLOAD_RESOURCE_PATH: &str = "/nervix.session.Session/UploadResource";

/// One backup download request answered by a server stream of download frames.
pub const DOWNLOAD_BACKUP_PATH: &str = "/nervix.session.Session/DownloadBackup";

/// The client stream of restore frames answered by one restore reply frame.
pub const RESTORE_BACKUP_PATH: &str = "/nervix.session.Session/RestoreBackup";

/// Frames below this size are copied out of tonic's read buffer.
///
/// Taking a frame without copying shares the read buffer's allocation, which grows to the largest
/// message the connection has carried. A small frame retained for long would keep that whole
/// allocation alive, so small frames get an allocation of their own; a large frame dominates its
/// allocation anyway and is taken without a copy.
const COPY_BELOW_BYTES: usize = 64 * 1024;

/// A local decoder rejection, retained as a tonic status's source.
///
/// Status codes alone cannot distinguish a local verification failure from a remote refusal.
/// This marker retains the complete verification report without exposing untrusted frame bytes.
#[derive(Debug)]
pub struct FrameRejection {
    report: Report<FrameError>,
}

impl FrameRejection {
    fn into_status(self) -> Status {
        let mut status = match self.report.current_context() {
            FrameError::TooLarge { .. } => Status::resource_exhausted(self.report.to_string()),
            FrameError::Truncated { .. }
            | FrameError::WrongIdentifier { .. }
            | FrameError::Invalid { .. } => Status::internal(self.report.to_string()),
        };
        status.set_source(StdArc::new(self));
        status
    }

    /// The complete frame-verification report, including its typed context and attachments.
    pub fn report(&self) -> &Report<FrameError> {
        &self.report
    }

    /// Finds local provenance in an error's source chain, without interpreting its text.
    pub fn find_in<'a>(mut error: &'a (dyn Error + 'static)) -> Option<&'a Self> {
        loop {
            if let Some(rejection) = error.downcast_ref::<Self>() {
                return Some(rejection);
            }
            error = error.source()?;
        }
    }
}

impl fmt::Display for FrameRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("local frame verification failed")
    }
}

impl Error for FrameRejection {}

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

/// The client side of a backup download.
pub type ClientBackupDownloadCodec = FrameCodec<BackupDownloadRequestFrame, BackupDownloadFrame>;

/// The server side of a backup download.
pub type ServerBackupDownloadCodec = FrameCodec<BackupDownloadFrame, BackupDownloadRequestFrame>;

/// The client side of a restore.
pub type ClientRestoreCodec = FrameCodec<RestoreFrame, RestoreReplyFrame>;

/// The server side of a restore.
pub type ServerRestoreCodec = FrameCodec<RestoreReplyFrame, RestoreFrame>;

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
            Err(error) => Err(FrameRejection { report: error }.into_status()),
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;

    use super::*;

    #[test]
    fn a_frame_rejection_retains_the_complete_verification_report() {
        #[derive(Debug, PartialEq, Eq)]
        struct Observation(u32);

        for error in [
            FrameError::Truncated {
                root: RestoreReplyFrame::NAME,
                actual: 3,
            },
            FrameError::TooLarge {
                root: RestoreReplyFrame::NAME,
                actual: 65,
                limit: 64,
            },
        ] {
            let expected_code = match error {
                FrameError::TooLarge { .. } => tonic::Code::ResourceExhausted,
                _ => tonic::Code::Internal,
            };
            let message = error.to_string();
            let status = FrameRejection {
                report: Report::new(error.clone()).attach(Observation(7)),
            }
            .into_status();
            let rejection = FrameRejection::find_in(&status)
                .assured("the codec's status keeps its local verification provenance");
            assert_eq!(rejection.report().current_context(), &error);
            assert_eq!(
                rejection.report().downcast_ref::<Observation>(),
                Some(&Observation(7))
            );
            assert_eq!(status.code(), expected_code);
            assert_eq!(status.message(), message);
            let report = Report::new(status);
            assert_eq!(format!("{report:#}").matches(&message).count(), 1);
        }
    }

    #[test]
    fn a_remote_status_has_no_local_frame_provenance() {
        assert!(
            FrameRejection::find_in(&Status::internal("local frame verification failed")).is_none()
        );
    }
}
