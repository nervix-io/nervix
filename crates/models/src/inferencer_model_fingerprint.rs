//! Layer: vocabulary.
//! Owns: the exact identity of a pinned inferencer model and its STATE declarations.
//! Depends on: serialization and byte values.
//! Must not know: hashing policy, native tensors, placement or runtime persistence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns typed inferencer model identity")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Equality identity computed once by the decision layer; every digest is an ordinary identity.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct InferencerModelFingerprint([u8; 32]);

impl InferencerModelFingerprint {
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }
    pub const fn as_digest(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for InferencerModelFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InferencerModelFingerprint(")?;
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        f.write_str(")")
    }
}
