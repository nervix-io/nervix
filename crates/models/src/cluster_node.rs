//! Stable cluster-node identities used by committed control-plane state.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The identity of one concrete run of a named cluster node.
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
