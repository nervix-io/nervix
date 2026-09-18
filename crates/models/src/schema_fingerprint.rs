//! The identity of the schemas one node's records and runtime state are laid out by.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The fingerprint the registry computes over the schema models one node depends on.
//! - **Depends on.** Serialization crates only.
//! - **Must not know.** How the registry walks a graph to compute a fingerprint, or how a runtime
//!   keys the state it stores by one.

use std::fmt;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// The digest the registry computes over every schema model one node's records are laid out by.
///
/// Fingerprints are compared only for equality. No byte pattern of one means anything of its own,
/// so none stands for "no schema": runtime state whose encoding depends on no schema names no
/// fingerprint at all rather than a reserved one.
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
pub struct SchemaFingerprint([u8; 32]);

impl SchemaFingerprint {
    /// The fingerprint whose digest is `digest`.
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// The digest this fingerprint is, as a storage key or another digest encodes it.
    pub const fn as_digest(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A fingerprint reads as its hexadecimal digest, so a placement in a diagnostic stays legible.
impl fmt::Debug for SchemaFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SchemaFingerprint(")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        formatter.write_str(")")
    }
}
