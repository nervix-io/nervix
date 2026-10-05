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

    /// The materialized state identity for this exact schema and domain start.
    pub fn materialized_at(self, start_version: u64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/materialized-state/start-version");
        hasher.update(self.as_digest());
        hasher.update(&start_version.to_be_bytes());
        Self::from_digest(*hasher.finalize().as_bytes())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_identity_binds_the_exact_schema_and_start_generation() {
        let schema = SchemaFingerprint::from_digest([7; 32]);
        let archived = schema.materialized_at(37);
        assert_eq!(
            archived,
            SchemaFingerprint::from_digest([7; 32]).materialized_at(37)
        );
        assert_ne!(archived, schema.materialized_at(38));
        assert_ne!(
            archived,
            SchemaFingerprint::from_digest([8; 32]).materialized_at(37)
        );
        assert_ne!(schema.materialized_at(0), schema.materialized_at(u64::MAX));
    }
}
