//! Frame ownership: the one place bytes become a trusted FlatBuffer.
//!
//! A [`VerifiedFrame`] owns immutable bytes that passed the size limit, the root's file identifier
//! and FlatBuffers verification exactly once, when the frame was constructed. Every view the
//! crate hands out borrows from such a frame, so no view reads unverified bytes and no read
//! verifies again.

use std::{borrow::Cow, fmt, marker::PhantomData};

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::{InvalidFlatbuffer, VerifierOptions};
use thiserror::Error;

use crate::{limits::SessionLimits, wire};

/// A root offset and a file identifier, which every frame starts with.
const FRAME_HEADER_BYTES: usize = 8;

/// The root table a frame carries, and the identifier that marks it.
///
/// The four roots are the only implementations.
pub trait FrameRoot: sealed::Sealed + 'static {
    /// The four-byte file identifier the schema declares for this root.
    const IDENTIFIER: &'static str;
    /// The root table's schema name.
    const NAME: &'static str;
}

/// The generated root table of each frame root. The module is private, so the generated types
/// never become nameable outside the crate.
mod sealed {
    use flatbuffers::{Follow, Verifiable};

    pub trait Sealed {
        type Table<'a>: Follow<'a, Inner = Self::Table<'a>> + Verifiable + 'a;
    }
}

macro_rules! frame_root {
    ($(#[$doc:meta])* $marker:ident => $table:ident, $identifier:literal) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $marker {}

        impl FrameRoot for $marker {
            const IDENTIFIER: &'static str = $identifier;
            const NAME: &'static str = stringify!($table);
        }

        impl sealed::Sealed for $marker {
            type Table<'a> = wire::$table<'a>;
        }
    };
}

frame_root!(
    /// A frame a client sends on a session: one request.
    ClientFrame => ClientMessage, "NXCM"
);
frame_root!(
    /// A frame a server sends on a session: a reply or an unsolicited message.
    ServerFrame => ServerMessage, "NXSM"
);
frame_root!(
    /// A frame of an upload stream: the start of the upload or one chunk of the archive.
    UploadFrame => UploadMessage, "NXUM"
);
frame_root!(
    /// The frame that answers an upload stream.
    UploadReplyFrame => UploadReply, "NXUR"
);

/// Why bytes were refused as a frame.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FrameError {
    #[error("a {root} frame of {actual} bytes exceeds the {limit}-byte limit")]
    TooLarge {
        root: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("a {root} frame of {actual} bytes cannot hold a root offset and identifier")]
    Truncated { root: &'static str, actual: usize },
    #[error("the frame does not carry the {root} identifier \"{expected}\"")]
    WrongIdentifier {
        root: &'static str,
        expected: &'static str,
    },
    #[error("the {root} frame failed verification: {violation}")]
    Invalid {
        root: &'static str,
        violation: FrameViolation,
    },
}

/// How a frame's FlatBuffer is malformed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FrameViolation {
    #[error("an offset or length points outside the frame")]
    OutOfBounds,
    #[error("a value is not aligned for its type")]
    Misaligned,
    #[error("required field `{field}` is absent")]
    MissingRequiredField { field: Cow<'static, str> },
    #[error("union `{field}` has a discriminant without a value, or a value without one")]
    InconsistentUnion { field: Cow<'static, str> },
    #[error("a string is not valid UTF-8")]
    InvalidUtf8,
    #[error("a string lacks its null terminator")]
    MissingNullTerminator,
    #[error("tables nest deeper than the limit of {limit}")]
    NestingTooDeep { limit: usize },
    #[error("the frame holds more tables than the limit of {limit}")]
    TooManyTables { limit: usize },
    #[error("the frame expands beyond the traversal limit of {limit} bytes")]
    ApparentSizeTooLarge { limit: usize },
}

impl FrameViolation {
    fn classify(error: InvalidFlatbuffer, options: &VerifierOptions) -> Self {
        match error {
            InvalidFlatbuffer::MissingRequiredField { required, .. } => {
                Self::MissingRequiredField { field: required }
            }
            InvalidFlatbuffer::InconsistentUnion { field, .. } => Self::InconsistentUnion { field },
            InvalidFlatbuffer::Utf8Error { .. } => Self::InvalidUtf8,
            InvalidFlatbuffer::MissingNullTerminator { .. } => Self::MissingNullTerminator,
            InvalidFlatbuffer::Unaligned { .. } => Self::Misaligned,
            InvalidFlatbuffer::RangeOutOfBounds { .. }
            | InvalidFlatbuffer::SignedOffsetOutOfBounds { .. } => Self::OutOfBounds,
            InvalidFlatbuffer::TooManyTables => Self::TooManyTables {
                limit: options.max_tables,
            },
            InvalidFlatbuffer::ApparentSizeTooLarge => Self::ApparentSizeTooLarge {
                limit: options.max_apparent_size,
            },
            InvalidFlatbuffer::DepthLimitReached => Self::NestingTooDeep {
                limit: options.max_depth,
            },
        }
    }
}

/// Bytes that passed frame verification for root `R`.
///
/// Cloning shares the bytes. A frame pins its whole buffer for as long as it, or any value
/// decoded from it that keeps it, is alive; [`VerifiedFrame::detached`] copies a frame out of a
/// larger shared buffer.
pub struct VerifiedFrame<R: FrameRoot> {
    bytes: Bytes,
    /// The limits the frame was verified under, which decoding it applies as well.
    limits: SessionLimits,
    root: PhantomData<fn() -> R>,
}

impl<R: FrameRoot> VerifiedFrame<R> {
    /// Verifies `bytes` as one frame no larger than the session frame limit.
    pub fn verify(bytes: Bytes, limits: &SessionLimits) -> Result<Self, Report<FrameError>> {
        Self::verify_within(bytes, limits.frame_bytes(), limits)
    }

    /// Verifies `bytes` as one frame no larger than `byte_limit`.
    pub(crate) fn verify_within(
        bytes: Bytes,
        byte_limit: usize,
        limits: &SessionLimits,
    ) -> Result<Self, Report<FrameError>> {
        if bytes.len() > byte_limit {
            return Err(Report::new(FrameError::TooLarge {
                root: R::NAME,
                actual: bytes.len(),
                limit: byte_limit,
            }));
        }
        if bytes.len() < FRAME_HEADER_BYTES {
            return Err(Report::new(FrameError::Truncated {
                root: R::NAME,
                actual: bytes.len(),
            }));
        }
        if !flatbuffers::buffer_has_identifier(&bytes, R::IDENTIFIER, false) {
            return Err(Report::new(FrameError::WrongIdentifier {
                root: R::NAME,
                expected: R::IDENTIFIER,
            }));
        }
        let options = limits.verifier_options(bytes.len());
        if let Err(error) = flatbuffers::root_with_opts::<R::Table<'_>>(&options, &bytes) {
            return Err(Report::new(FrameError::Invalid {
                root: R::NAME,
                violation: FrameViolation::classify(error, &options),
            }));
        }
        Ok(Self {
            bytes,
            limits: *limits,
            root: PhantomData,
        })
    }

    /// The limits the frame was verified under.
    pub fn limits(&self) -> &SessionLimits {
        &self.limits
    }

    /// The frame's bytes, for forwarding it unchanged.
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    /// The frame's length in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the frame is empty. A verified frame never is; this exists for API symmetry with
    /// `len`.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Copies the frame into an allocation of exactly its own size.
    ///
    /// A frame decoded from a transport's read buffer shares that buffer, which may be far larger
    /// than the frame. A receiver that retains a small frame for long copies it here so the rest
    /// of the buffer can be freed. The bytes are identical, so the copy needs no verification.
    pub fn detached(&self) -> Self {
        Self {
            bytes: Bytes::copy_from_slice(&self.bytes),
            limits: self.limits,
            root: PhantomData,
        }
    }
}

impl<R: FrameRoot> VerifiedFrame<R> {
    /// The root table.
    pub(crate) fn root(&self) -> R::Table<'_> {
        // SAFETY: `verify_within` is the only constructor. It ran the FlatBuffers verifier for
        // exactly this root table over these bytes, and `Bytes` is immutable, so every offset the
        // generated accessors follow stays in bounds and every required field is present.
        unsafe { flatbuffers::root_unchecked::<R::Table<'_>>(&self.bytes) }
    }
}

impl<R: FrameRoot> Clone for VerifiedFrame<R> {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            limits: self.limits,
            root: PhantomData,
        }
    }
}

impl<R: FrameRoot> fmt::Debug for VerifiedFrame<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedFrame")
            .field("root", &R::NAME)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

/// A finished frame this crate encoded for root `R`.
pub struct EncodedFrame<R: FrameRoot> {
    bytes: Bytes,
    root: PhantomData<fn() -> R>,
}

impl<R: FrameRoot> EncodedFrame<R> {
    pub(crate) fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            root: PhantomData,
        }
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    /// The frame's length in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the frame is empty. An encoded frame never is; this exists for API symmetry with
    /// `len`.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Verifies the frame as a receiver would.
    pub fn verify(self, limits: &SessionLimits) -> Result<VerifiedFrame<R>, Report<FrameError>> {
        VerifiedFrame::verify(self.bytes, limits)
    }
}

impl<R: FrameRoot> Clone for EncodedFrame<R> {
    fn clone(&self) -> Self {
        Self::new(self.bytes.clone())
    }
}

impl<R: FrameRoot> fmt::Debug for EncodedFrame<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedFrame")
            .field("root", &R::NAME)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}
