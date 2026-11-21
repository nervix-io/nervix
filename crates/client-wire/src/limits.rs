//! The bounds every frame of a session is held to.
//!
//! One value carries them all, so a transport configures its own frame limit, the verifier, the
//! decoders that allocate, and the encoders that must never produce a frame a peer rejects from
//! the same numbers.
//!
//! The session-limit settings own every default and bound: `SessionFrameBytes`,
//! `SessionTransferBytes`, `SessionNestingDepth`, `SessionCollectionEntries` and
//! `SessionStringBytes`. This crate reads each one from its definition and relies on them as
//! follows:
//!
//! - The default frame limit matches the receive limit gRPC applies by default.
//! - The smallest frame limit holds the session opening and a transfer part with a useful chunk,
//!   so every envelope this crate reserves beside a chunk, a batch or rows stays below it.
//! - The largest frame limit keeps FlatBuffers' 32-bit offsets in range, and keeps the verifier's
//!   apparent-size budget, the frame length times `APPARENT_SIZE_FACTOR`, representable in the
//!   32-bit `usize` of browser builds.
//! - The deepest structure of the schema that does not recurse, a node coverage in an execution
//!   step's affected topology inside an inspection reply, nests 13 tables. The default nesting
//!   limit leaves the rest for nested list values and list field types.
//! - The smallest nesting limit holds every structure of the schema that does not recurse, which a
//!   test verifies under that limit.
//! - The verifier and the decoders recurse once per level, so the largest nesting limit bounds the
//!   stack a hostile frame can make a receiver use. A test verifies and decodes a frame nested that
//!   deep on a test thread's stack in a debug build.

use std::num::{NonZeroU64, NonZeroUsize};

use error_stack::Report;
use flatbuffers::VerifierOptions;
use meticulous::{OptionExt as _, ResultExt as _};
pub use nervix_models::SessionBoundsError as LimitsError;
use nervix_models::{
    SessionBoundValues, SessionBounds, SessionCollectionEntries, SessionFrameBytes,
    SessionNestingDepth, SessionStringBytes, SessionTransferBytes,
};

/// How many requests one session may have in flight, counting both of its lanes. A server refuses
/// a request beyond it with `TooManyRequestsInFlight` rather than queue it, so a client flooding one
/// session cannot grow what the server holds for it, and it also bounds the queue of ordered
/// requests waiting for their lane. A client that wants every request served holds the rest until
/// a reply frees a place.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the client session protocol (rule C-6) allows 64 requests in flight per session"
    )
)]
pub const MAX_IN_FLIGHT_REQUESTS: usize = 64;

/// The smallest table occupies its four-byte vtable offset, so a frame of `n` bytes cannot hold
/// more than `n / MIN_TABLE_BYTES` tables.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "every FlatBuffers table starts with its 4-byte vtable offset, and the client \
                  session protocol (rule F-3) limits a frame to its length divided by 4 tables"
    )
)]
const MIN_TABLE_BYTES: usize = flatbuffers::SIZE_SOFFSET;

/// How much larger than the frame the verifier lets its traversal grow.
///
/// Tables share vtables, so a well-formed frame's traversal visits more bytes than the frame
/// holds: an empty table visited through its offset counts about two and a half times its own
/// size. Anything beyond this factor is a frame whose offsets alias one another to expand the
/// work a receiver does.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the client session protocol (rule F-3) limits the verifier's apparent size to 8 \
                  times the frame length"
    )
)]
const APPARENT_SIZE_FACTOR: usize = 8;

const _: () = {
    assert!(SessionFrameBytes::MINIMUM <= SessionFrameBytes::DEFAULT);
    assert!(SessionFrameBytes::DEFAULT <= SessionTransferBytes::DEFAULT);
    assert!(SessionTransferBytes::DEFAULT <= SessionFrameBytes::MAXIMUM);
    assert!(SessionStringBytes::DEFAULT <= SessionTransferBytes::DEFAULT);
    assert!(SessionNestingDepth::MINIMUM <= SessionNestingDepth::DEFAULT);
    assert!(SessionNestingDepth::DEFAULT <= SessionNestingDepth::MAXIMUM);
    assert!(SessionCollectionEntries::DEFAULT > 0);
    // The verifier counts its apparent size in `usize`. This evaluates on the target being built,
    // so a browser build proves the budget fits its 32-bit `usize`.
    assert!(
        SessionFrameBytes::MAXIMUM
            .checked_mul(APPARENT_SIZE_FACTOR)
            .is_some()
    );
};

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
    /// The fixed envelope bound before the current session's limits are announced. It holds
    /// the numeric opening and the download request's bounded execution reference.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the session opening envelope fits the protocol's minimum frame"
        )
    )]
    pub const OPENING: Self = Self {
        settings: SessionLimitSettings {
            frame_bytes: non_zero(SessionFrameBytes::MINIMUM),
            transfer_bytes: non_zero(SessionFrameBytes::MINIMUM),
            nesting_depth: non_zero(SessionNestingDepth::MINIMUM),
            collection_entries: non_zero(SessionFrameBytes::MINIMUM),
            string_bytes: non_zero(SessionFrameBytes::MINIMUM),
        },
    };

    /// The limits a session uses unless its transport configures others: the default of each
    /// session-limit setting.
    pub const DEFAULT: Self = Self {
        settings: SessionLimitSettings {
            frame_bytes: non_zero(SessionFrameBytes::DEFAULT),
            transfer_bytes: non_zero(SessionTransferBytes::DEFAULT),
            nesting_depth: non_zero(SessionNestingDepth::DEFAULT),
            collection_entries: non_zero(SessionCollectionEntries::DEFAULT),
            string_bytes: non_zero(SessionStringBytes::DEFAULT),
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
    /// to a limit of at most `SessionFrameBytes::MAXIMUM`.
    ///
    /// The table and traversal budgets scale with the frame itself rather than with the limit, so
    /// a small frame whose offsets alias one another cannot make a receiver walk far more than its
    /// own size.
    pub(crate) fn verifier_options(&self, frame_len: usize) -> VerifierOptions {
        let max_tables = frame_len / MIN_TABLE_BYTES;
        let max_apparent_size = frame_len.checked_mul(APPARENT_SIZE_FACTOR).assured(
            "a verified frame is at most SessionFrameBytes::MAXIMUM long, whose apparent-size \
             product a const assertion proves fits usize on the target being built",
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
        let bounds = SessionBounds::resolve(SessionBoundValues {
            frame_bytes: Some(semantic_size(settings.frame_bytes)),
            transfer_bytes: Some(semantic_size(settings.transfer_bytes)),
            nesting_depth: Some(semantic_size(settings.nesting_depth)),
            collection_entries: Some(semantic_size(settings.collection_entries)),
            string_bytes: Some(semantic_size(settings.string_bytes)),
        })?;
        Ok(Self::from(&bounds))
    }
}

impl From<&SessionBounds> for SessionLimits {
    fn from(bounds: &SessionBounds) -> Self {
        Self {
            settings: SessionLimitSettings {
                frame_bytes: native_size(bounds.frame_bytes()),
                transfer_bytes: native_size(bounds.transfer_bytes()),
                nesting_depth: native_size(bounds.nesting_depth()),
                collection_entries: native_size(bounds.collection_entries()),
                string_bytes: native_size(bounds.string_bytes()),
            },
        }
    }
}

fn semantic_size(value: NonZeroUsize) -> NonZeroU64 {
    let value = u64::try_from(value.get()).assured("supported native and browser sizes fit u64");
    NonZeroU64::new(value).assured("the source size is nonzero")
}

fn native_size(value: NonZeroU64) -> NonZeroUsize {
    let value = usize::try_from(value.get())
        .assured("the session definition validates platform representation");
    NonZeroUsize::new(value).assured("the validated semantic size is nonzero")
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
            frame_bytes: size(SessionFrameBytes::MINIMUM),
            ..settings()
        };
        assert!(SessionLimits::try_from(smallest).is_ok());

        let too_small = SessionLimitSettings {
            frame_bytes: size(SessionFrameBytes::MINIMUM - 1),
            ..settings()
        };
        let error = SessionLimits::try_from(too_small).expect_err("below the minimum");
        assert!(matches!(
            error.current_context(),
            LimitsError::FrameBytesOutOfRange { frame_bytes, .. } if *frame_bytes == SessionFrameBytes::MINIMUM - 1
        ));

        let largest = SessionLimitSettings {
            frame_bytes: size(SessionFrameBytes::MAXIMUM),
            transfer_bytes: size(SessionFrameBytes::MAXIMUM),
            ..settings()
        };
        assert!(SessionLimits::try_from(largest).is_ok());

        let too_large = SessionLimitSettings {
            frame_bytes: size(SessionFrameBytes::MAXIMUM + 1),
            transfer_bytes: size(SessionFrameBytes::MAXIMUM + 1),
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
            transfer_bytes: size(SessionFrameBytes::DEFAULT - 1),
            ..settings()
        };
        assert!(matches!(
            SessionLimits::try_from(undercut)
                .expect_err("below the frame limit")
                .current_context(),
            LimitsError::TransferBytesOutOfRange { .. }
        ));

        let equal = SessionLimitSettings {
            transfer_bytes: size(SessionFrameBytes::DEFAULT),
            string_bytes: size(SessionFrameBytes::DEFAULT),
            ..settings()
        };
        assert!(SessionLimits::try_from(equal).is_ok());
    }

    #[test]
    fn nesting_and_string_limits_are_checked() {
        for depth in [SessionNestingDepth::MINIMUM, SessionNestingDepth::MAXIMUM] {
            let within = SessionLimitSettings {
                nesting_depth: size(depth),
                ..settings()
            };
            assert!(SessionLimits::try_from(within).is_ok());
        }
        for depth in [
            SessionNestingDepth::MINIMUM - 1,
            SessionNestingDepth::MAXIMUM + 1,
        ] {
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
            string_bytes: size(SessionTransferBytes::DEFAULT + 1),
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
        let options = SessionLimits::DEFAULT.verifier_options(SessionFrameBytes::DEFAULT);
        assert_eq!(options.max_depth, SessionNestingDepth::DEFAULT);
        // Rule F-3 of the Client Implementation Manual: a table limit of the frame length divided
        // by 4 and an apparent-size limit of 8 times the frame length.
        assert_eq!(options.max_tables, SessionFrameBytes::DEFAULT / 4);
        assert_eq!(options.max_apparent_size, SessionFrameBytes::DEFAULT * 8);
        assert!(!options.ignore_missing_null_terminator);
    }
}
