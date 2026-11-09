//! The resolved capacity claim of one concrete node run.
//!
//! Layer: vocabulary.
//! - **Owns.** CPU and memory capacity, peer receive bounds and the incarnation-bound claim.
//! - **Depends on.** Node identities and serialization primitives.
//! - **Must not know.** Detection, consensus, transport or placement policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::ClusterNodeIdentity;

/// Positive limits the receiver enforces. They bound work; they never change cluster acceptance.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct PeerReceiveBounds {
    pub relay_batch_bytes: NonZeroU64,
    pub relay_decoded_bytes: NonZeroU64,
    pub bulk_chunk_bytes: NonZeroU64,
    pub bulk_window_bytes: NonZeroU64,
}

impl PeerReceiveBounds {
    /// The common safe bound, retaining each receiver's independent dimensions.
    pub fn intersection(self, other: Self) -> Self {
        Self {
            relay_batch_bytes: self.relay_batch_bytes.min(other.relay_batch_bytes),
            relay_decoded_bytes: self.relay_decoded_bytes.min(other.relay_decoded_bytes),
            bulk_chunk_bytes: self.bulk_chunk_bytes.min(other.bulk_chunk_bytes),
            bulk_window_bytes: self.bulk_window_bytes.min(other.bulk_window_bytes),
        }
    }
}

/// What one node can carry. Unknown host measurements stay explicit.
///
/// This is also the sealed class of node-local capacity settings. Further sizing derivations
/// belong here and receive their policy inputs explicitly.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct NodeCapabilities {
    pub cpu_count: Option<NonZeroU64>,
    pub memory_bytes: Option<NonZeroU64>,
    pub receive: PeerReceiveBounds,
}

impl NodeCapabilities {
    /// The limits a peer may use for this receiver after its claim commits.
    pub const fn peer_receive_bounds(&self) -> PeerReceiveBounds {
        self.receive
    }
}

/// One record per node name, replaced by a newer run and retained after departure until DROP NODE.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeCapabilityRecord {
    pub identity: ClusterNodeIdentity,
    pub capabilities: NodeCapabilities,
}
