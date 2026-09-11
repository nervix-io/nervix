//! The internode operations one sealed runtime snapshot is described and transferred by.
//!
//! Layer: control plane.
//!
//! - **Owns.** The request and response shapes that describe a sealed generation and open a
//!   bounded stream of its bytes.
//! - **Depends on.** The placement vocabulary and the authenticated interconnect request contract.
//! - **Must not know.** Where a snapshot is stored, what it contains, or how a receiver installs
//!   it.
//!
//! Describing and transferring are separate operations on purpose. A requester that already holds
//! the current generation learns so from the description alone, and the owner neither scans its
//! state nor encodes anything for it. Only a requester that needs bytes opens the bulk stream, and
//! that stream carries no application framing beside the sealed container itself.

use std::time::Duration;

use nervix_interconnect::{
    InterconnectRequest, InterconnectStreamRequest, PoolClass, RequestSubquota,
    StatePlacementEnvelope,
};
use rkyv::{Archive, Deserialize, Serialize};

/// Ask the node that owns a runtime state to seal a generation newer than `after_revision`.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub(crate) struct DescribeStateSnapshot {
    pub(crate) placement: StatePlacementEnvelope,
    pub(crate) after_revision: Option<u64>,
}

impl InterconnectRequest for DescribeStateSnapshot {
    type Response = DescribedStateSnapshot;

    const NAME: &'static str = "describe_state_snapshot";
    const CLASS: PoolClass = PoolClass::Commands;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

/// What the owner sealed, or why it sealed nothing.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DescribedStateSnapshot {
    /// The requester already holds the owner's current revision. Nothing was scanned or encoded.
    Current,
    /// A generation is sealed and its bytes are ready to be fetched.
    Sealed(SealedSnapshotEnvelope),
    /// The owner cannot serve this state, and says why.
    Unavailable(String),
}

/// Everything a receiver checks a transfer against before it accepts one byte of it.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SealedSnapshotEnvelope {
    pub(crate) length: u64,
    pub(crate) digest: [u8; 32],
    pub(crate) schema_fingerprint: [u8; 32],
    pub(crate) revision: u64,
    pub(crate) fence: u64,
    pub(crate) branch_generation: u64,
}

/// Open a bounded stream of one sealed generation's bytes.
///
/// The revision names the exact generation the requester was described; an owner that has moved on
/// refuses rather than substituting a different one.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub(crate) struct FetchStateSnapshot {
    pub(crate) placement: StatePlacementEnvelope,
    pub(crate) revision: u64,
}

impl InterconnectStreamRequest for FetchStateSnapshot {
    const NAME: &'static str = "fetch_state_snapshot";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(30);
}
