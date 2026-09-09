//! Layer: vocabulary.
//! Owns: the typed identity of one process lifetime for a named cluster node.
//! May depend on: primitive integers and serialization traits.
//! Must not know: language parsing, scheduling decisions, runtime state, or cluster transport.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Identifies one process lifetime of a named cluster node.
///
/// A node name survives restarts. Its incarnation changes on every restart so an operation can
/// reject acknowledgements or durable preparation produced by a different process lifetime.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
)]
pub struct ClusterNodeIncarnation(u64);

impl ClusterNodeIncarnation {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}
