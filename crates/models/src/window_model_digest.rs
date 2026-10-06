//! The identity of the window processor model a window's retained state was built under.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The digest of one window processor model's canonical NSPL.
//! - **Depends on.** Canonical NSPL rendering and BLAKE3.
//! - **Must not know.** How a window keeps, archives or restores its state.

use std::fmt;

use crate::{CanonicalNsplError, CreateWindowProcessor};

/// The BLAKE3 digest of a window processor model's canonical NSPL.
///
/// A window's retained rows, argument columns and histogram state mean what the model that built
/// them says, so state is carried to a model only when the digests are equal. Digests are compared
/// only for equality; no byte pattern of one means anything of its own.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct WindowModelDigest([u8; 32]);

impl WindowModelDigest {
    /// The digest whose bytes are `digest`.
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// The digest's bytes, as an archive record encodes them.
    pub const fn as_digest(&self) -> &[u8; 32] {
        &self.0
    }
}

impl CreateWindowProcessor {
    /// The digest of this model's canonical NSPL, which changes with any change to the window's
    /// inputs, bounds, state limit, branch, routes or aggregates.
    pub fn model_digest(&self) -> error_stack::Result<WindowModelDigest, CanonicalNsplError> {
        let text = self.to_canonical_nspl()?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/window-processor-model");
        hasher.update(text.as_bytes());
        Ok(WindowModelDigest(*hasher.finalize().as_bytes()))
    }
}

/// A digest reads as lowercase hexadecimal, the form `b3sum` prints.
impl fmt::Display for WindowModelDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for WindowModelDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "WindowModelDigest({self})")
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use meticulous::{OptionExt as _, ResultExt as _};

    use crate::{
        AckMode, BranchSelection, CreateWindowProcessor, FlushPolicy, ProcessorInputs,
        ProcessorOutput, ProcessorOutputs, WindowBound, WindowStateLimit,
    };

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).assured("the test name is valid")
    }

    fn window() -> CreateWindowProcessor {
        CreateWindowProcessor {
            name: named("latency_window"),
            from: ProcessorInputs::single(named("metrics")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                named("summaries"),
                FlushPolicy::Immediate,
            )]),
            branched_by: BranchSelection::unbranched(),
            width: WindowBound {
                messages: Some(2),
                duration: None,
            },
            step: WindowBound {
                messages: Some(1),
                duration: None,
            },
            state_limit: WindowStateLimit::Unbounded,
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }
    }

    #[test]
    fn equal_models_share_a_digest_and_any_change_moves_it() {
        let base = window();
        let digest = base.model_digest().assured("the test window renders");
        assert_eq!(
            window().model_digest().assured("an equal window renders"),
            digest
        );
        let mut wider = window();
        wider.width.messages = Some(3);
        let mut limited = window();
        limited.state_limit =
            WindowStateLimit::MaxBytes(NonZeroU64::new(1024).assured("1024 is positive"));
        let mut detached = window();
        detached.mode = AckMode::Detached;
        for changed in [wider, limited, detached] {
            assert_ne!(
                changed.model_digest().assured("a changed window renders"),
                digest
            );
        }
        assert_eq!(format!("{digest}").len(), 64);
        assert!(format!("{digest:?}").starts_with("WindowModelDigest("));
        assert_eq!(
            super::WindowModelDigest::from_digest(*digest.as_digest()),
            digest
        );
    }
}
