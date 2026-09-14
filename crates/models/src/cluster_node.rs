//! Stable cluster-node and per-process coordination identities.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** Identities for concrete node runs and their coordination operations.
//! - **Depends on.** Validated node names and serialization primitives.
//! - **Must not know.** Gossip membership, consensus, transport authentication or runtime tasks.

use std::fmt;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::ClusterNodeName;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
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

impl fmt::Display for ClusterNodeIncarnation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ClusterNodeIdentity {
    node_id: ClusterNodeName,
    incarnation: ClusterNodeIncarnation,
}

impl ClusterNodeIdentity {
    pub const fn new(node_id: ClusterNodeName, incarnation: ClusterNodeIncarnation) -> Self {
        Self {
            node_id,
            incarnation,
        }
    }

    pub const fn node_id(&self) -> &ClusterNodeName {
        &self.node_id
    }

    pub const fn incarnation(&self) -> ClusterNodeIncarnation {
        self.incarnation
    }
}

impl fmt::Display for ClusterNodeIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}#{}", self.node_id, self.incarnation)
    }
}

/// The identity of one coordination operation issued by one running node process.
///
/// The node and process epoch keep equal local sequences from colliding across coordinators or
/// restarts. The interconnect binds those two fields to the authenticated sending connection.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct CoordinationIdentity {
    coordinator: ClusterNodeName,
    process_epoch: u64,
    sequence: u64,
}

impl CoordinationIdentity {
    pub const fn new(coordinator: ClusterNodeName, process_epoch: u64, sequence: u64) -> Self {
        Self {
            coordinator,
            process_epoch,
            sequence,
        }
    }

    pub const fn coordinator(&self) -> &ClusterNodeName {
        &self.coordinator
    }

    pub const fn process_epoch(&self) -> u64 {
        self.process_epoch
    }

    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl fmt::Display for CoordinationIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}@{}:{}",
            self.coordinator, self.process_epoch, self.sequence
        )
    }
}
