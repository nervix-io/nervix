//! The bounds every frame of a session is held to.
//!
//! One value carries them all, so a transport configures its own frame limit, the verifier, the
//! decoders that allocate, and the encoders that must never produce a frame a peer rejects from
//! the same numbers.

use std::num::NonZeroUsize;

use error_stack::Report;
use flatbuffers::VerifierOptions;
use meticulous::OptionExt as _;
use thiserror::Error;

/// The largest single frame by default. It matches the receive limit gRPC applies by default.
const DEFAULT_FRAME_BYTES: usize = 4 * 1024 * 1024;

/// The largest reply a transfer reassembles by default.
const DEFAULT_TRANSFER_BYTES: usize = 64 * 1024 * 1024;

/// The deepest nesting of tables by default. The deepest structure of the schema that does not
/// recurse, a node coverage in an execution step's affected topology inside an inspection reply,
/// nests 13 tables; the rest of the budget is for nested list values and list field types.
const DEFAULT_NESTING_DEPTH: usize = 64;

/// The most entries a single vector may hold by default.
const DEFAULT_COLLECTION_ENTRIES: usize = 1 << 18;

/// The longest string by default.
const DEFAULT_STRING_BYTES: usize = DEFAULT_FRAME_BYTES;

/// A frame must hold a transfer part with a useful chunk, and a limit below this cannot.
pub(crate) const MIN_FRAME_BYTES: usize = 1024;

/// The largest frame any limit admits.
///
/// FlatBuffers offsets are 32-bit, and the verifier's apparent-size budget, which is this bound
/// times [`APPARENT_SIZE_FACTOR`], must stay representable in a 32-bit `usize` for browser builds.
const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

/// The fewest nesting levels any limit admits: enough for every structure of the schema that does
/// not recurse, which a test verifies under this limit.
pub(crate) const MIN_NESTING_DEPTH: usize = 16;

/// The most nesting levels any limit admits.
///
/// The verifier and the decoders recurse once per level, so the depth bounds the stack a hostile
/// frame can make a receiver use. A test verifies and decodes a frame nested this deep on a test
/// thread's stack in a debug build.
pub(crate) const MAX_NESTING_DEPTH: usize = 128;

/// The smallest table occupies its four-byte vtable offset, so a frame of `n` bytes cannot hold
/// more than `n / MIN_TABLE_BYTES` tables.
const MIN_TABLE_BYTES: usize = 4;

/// How much larger than the frame the verifier lets its traversal grow.
///
/// Tables share vtables, so a well-formed frame's traversal visits more bytes than the frame
/// holds: an empty table visited through its offset counts about two and a half times its own
/// size. Anything beyond this factor is a frame whose offsets alias one another to expand the
/// work a receiver does.
const APPARENT_SIZE_FACTOR: usize = 8;

const _: () = {
    assert!(MIN_FRAME_BYTES <= DEFAULT_FRAME_BYTES);
    assert!(DEFAULT_FRAME_BYTES <= DEFAULT_TRANSFER_BYTES);
    assert!(DEFAULT_TRANSFER_BYTES <= MAX_FRAME_BYTES);
    assert!(DEFAULT_STRING_BYTES <= DEFAULT_TRANSFER_BYTES);
    assert!(MIN_NESTING_DEPTH <= DEFAULT_NESTING_DEPTH);
    assert!(DEFAULT_NESTING_DEPTH <= MAX_NESTING_DEPTH);
    assert!(DEFAULT_COLLECTION_ENTRIES > 0);
    // The verifier counts its apparent size in `usize`. This evaluates on the target being built,
    // so a browser build proves the budget fits its 32-bit `usize`.
    assert!(MAX_FRAME_BYTES.checked_mul(APPARENT_SIZE_FACTOR).is_some());
};

/// Why a set of limits was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LimitsError {
    #[error("a frame limit of {frame_bytes} bytes is outside {minimum}..={maximum}")]
    FrameBytesOutOfRange {
        frame_bytes: usize,
        minimum: usize,
        maximum: usize,
    },
    #[error(
        "a transfer limit of {transfer_bytes} bytes is outside the frame limit {frame_bytes} to \
         {maximum}"
    )]
    TransferBytesOutOfRange {
        transfer_bytes: usize,
        frame_bytes: usize,
        maximum: usize,
    },
    #[error("a nesting limit of {nesting_depth} is outside {minimum}..={maximum}")]
    NestingDepthOutOfRange {
        nesting_depth: usize,
        minimum: usize,
        maximum: usize,
    },
    #[error("a string limit of {string_bytes} bytes exceeds the transfer limit {transfer_bytes}")]
    StringBytesAboveTransfer {
        string_bytes: usize,
        transfer_bytes: usize,
    },
}

/// The limits a session is configured with, before they are checked against one another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionLimitSettings {
    /// The largest frame a transport carries, in bytes.
    pub frame_bytes: NonZeroUsize,
    /// The largest reply a transfer reassembles, in bytes.
    pub transfer_bytes: NonZeroUsize,
    /// The deepest nesting of tables, counting the frame's root table as the first level.
    pub nesting_depth: NonZeroUsize,
    /// The most entries one vector may hold.
    pub collection_entries: NonZeroUsize,
    /// The longest string, in bytes.
    pub string_bytes: NonZeroUsize,
}

/// Checked session limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionLimits {
    settings: SessionLimitSettings,
}

impl SessionLimits {
    /// The limits a session uses unless its transport configures others.
    pub const DEFAULT: Self = Self {
        settings: SessionLimitSettings {
            frame_bytes: non_zero(DEFAULT_FRAME_BYTES),
            transfer_bytes: non_zero(DEFAULT_TRANSFER_BYTES),
            nesting_depth: non_zero(DEFAULT_NESTING_DEPTH),
            collection_entries: non_zero(DEFAULT_COLLECTION_ENTRIES),
            string_bytes: non_zero(DEFAULT_STRING_BYTES),
        },
    };

    /// The largest frame a transport carries, in bytes.
    pub const fn frame_bytes(&self) -> usize {
        self.settings.frame_bytes.get()
    }

    /// The largest reply a transfer reassembles, in bytes.
    pub const fn transfer_bytes(&self) -> usize {
        self.settings.transfer_bytes.get()
    }

    /// The deepest nesting of tables.
    pub const fn nesting_depth(&self) -> usize {
        self.settings.nesting_depth.get()
    }

    /// The most entries one vector may hold.
    pub const fn collection_entries(&self) -> usize {
        self.settings.collection_entries.get()
    }

    /// The longest string, in bytes.
    pub const fn string_bytes(&self) -> usize {
        self.settings.string_bytes.get()
    }

    /// The verifier options for a frame of `frame_len` bytes, which verification has already held
    /// to a limit of at most [`MAX_FRAME_BYTES`].
    ///
    /// The table and traversal budgets scale with the frame itself rather than with the limit, so
    /// a small frame whose offsets alias one another cannot make a receiver walk far more than its
    /// own size.
    pub(crate) fn verifier_options(&self, frame_len: usize) -> VerifierOptions {
        let max_tables = frame_len / MIN_TABLE_BYTES;
        let max_apparent_size = frame_len.checked_mul(APPARENT_SIZE_FACTOR).assured(
            "a verified frame is at most MAX_FRAME_BYTES long, whose apparent-size product a \
             const assertion proves fits usize on the target being built",
        );
        VerifierOptions {
            max_depth: self.nesting_depth(),
            max_tables,
            max_apparent_size,
            ignore_missing_null_terminator: false,
        }
    }
}

impl Default for SessionLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl TryFrom<SessionLimitSettings> for SessionLimits {
    type Error = Report<LimitsError>;

    fn try_from(settings: SessionLimitSettings) -> Result<Self, Self::Error> {
        let frame_bytes = settings.frame_bytes.get();
        if !(MIN_FRAME_BYTES..=MAX_FRAME_BYTES).contains(&frame_bytes) {
            return Err(Report::new(LimitsError::FrameBytesOutOfRange {
                frame_bytes,
                minimum: MIN_FRAME_BYTES,
                maximum: MAX_FRAME_BYTES,
            }));
        }
        let transfer_bytes = settings.transfer_bytes.get();
        if !(frame_bytes..=MAX_FRAME_BYTES).contains(&transfer_bytes) {
            return Err(Report::new(LimitsError::TransferBytesOutOfRange {
                transfer_bytes,
                frame_bytes,
                maximum: MAX_FRAME_BYTES,
            }));
        }
        let nesting_depth = settings.nesting_depth.get();
        if !(MIN_NESTING_DEPTH..=MAX_NESTING_DEPTH).contains(&nesting_depth) {
            return Err(Report::new(LimitsError::NestingDepthOutOfRange {
                nesting_depth,
                minimum: MIN_NESTING_DEPTH,
                maximum: MAX_NESTING_DEPTH,
            }));
        }
        let string_bytes = settings.string_bytes.get();
        if string_bytes > transfer_bytes {
            return Err(Report::new(LimitsError::StringBytesAboveTransfer {
                string_bytes,
                transfer_bytes,
            }));
        }
        Ok(Self { settings })
    }
}

const fn non_zero(value: usize) -> NonZeroUsize {
    match NonZeroUsize::new(value) {
        Some(value) => value,
        None => panic!("a default session limit is zero"),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    fn size(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).assured("the test passes a non-zero size")
    }

    fn settings() -> SessionLimitSettings {
        SessionLimits::DEFAULT.settings
    }

    #[test]
    fn default_limits_pass_their_own_checks() {
        let checked = SessionLimits::try_from(settings());
        assert_eq!(checked.ok(), Some(SessionLimits::DEFAULT));
    }

    #[test]
    fn frame_limit_boundaries_are_inclusive() {
        let smallest = SessionLimitSettings {
            frame_bytes: size(MIN_FRAME_BYTES),
            ..settings()
        };
        assert!(SessionLimits::try_from(smallest).is_ok());

        let too_small = SessionLimitSettings {
            frame_bytes: size(MIN_FRAME_BYTES - 1),
            ..settings()
        };
        let error = SessionLimits::try_from(too_small).expect_err("below the minimum");
        assert!(matches!(
            error.current_context(),
            LimitsError::FrameBytesOutOfRange { frame_bytes, .. } if *frame_bytes == MIN_FRAME_BYTES - 1
        ));

        let largest = SessionLimitSettings {
            frame_bytes: size(MAX_FRAME_BYTES),
            transfer_bytes: size(MAX_FRAME_BYTES),
            ..settings()
        };
        assert!(SessionLimits::try_from(largest).is_ok());

        let too_large = SessionLimitSettings {
            frame_bytes: size(MAX_FRAME_BYTES + 1),
            transfer_bytes: size(MAX_FRAME_BYTES + 1),
            ..settings()
        };
        assert!(matches!(
            SessionLimits::try_from(too_large)
                .expect_err("above the maximum")
                .current_context(),
            LimitsError::FrameBytesOutOfRange { .. }
        ));
    }

    #[test]
    fn transfer_limit_cannot_undercut_the_frame_limit() {
        let undercut = SessionLimitSettings {
            transfer_bytes: size(DEFAULT_FRAME_BYTES - 1),
            ..settings()
        };
        assert!(matches!(
            SessionLimits::try_from(undercut)
                .expect_err("below the frame limit")
                .current_context(),
            LimitsError::TransferBytesOutOfRange { .. }
        ));

        let equal = SessionLimitSettings {
            transfer_bytes: size(DEFAULT_FRAME_BYTES),
            string_bytes: size(DEFAULT_FRAME_BYTES),
            ..settings()
        };
        assert!(SessionLimits::try_from(equal).is_ok());
    }

    #[test]
    fn nesting_and_string_limits_are_checked() {
        for depth in [MIN_NESTING_DEPTH, MAX_NESTING_DEPTH] {
            let within = SessionLimitSettings {
                nesting_depth: size(depth),
                ..settings()
            };
            assert!(SessionLimits::try_from(within).is_ok());
        }
        for depth in [MIN_NESTING_DEPTH - 1, MAX_NESTING_DEPTH + 1] {
            let outside = SessionLimitSettings {
                nesting_depth: size(depth),
                ..settings()
            };
            let error = SessionLimits::try_from(outside).expect_err("outside the nesting range");
            assert!(matches!(
                error.current_context(),
                LimitsError::NestingDepthOutOfRange { nesting_depth, .. } if *nesting_depth == depth
            ));
        }

        let long_strings = SessionLimitSettings {
            string_bytes: size(DEFAULT_TRANSFER_BYTES + 1),
            ..settings()
        };
        assert!(matches!(
            SessionLimits::try_from(long_strings)
                .expect_err("above the transfer limit")
                .current_context(),
            LimitsError::StringBytesAboveTransfer { .. }
        ));
    }

    #[test]
    fn verifier_options_scale_with_the_frame_length() {
        let options = SessionLimits::DEFAULT.verifier_options(DEFAULT_FRAME_BYTES);
        assert_eq!(options.max_depth, DEFAULT_NESTING_DEPTH);
        assert_eq!(options.max_tables, DEFAULT_FRAME_BYTES / MIN_TABLE_BYTES);
        assert_eq!(
            options.max_apparent_size,
            DEFAULT_FRAME_BYTES * APPARENT_SIZE_FACTOR
        );
        assert!(!options.ignore_missing_null_terminator);
    }
}
