//! Committed observations and grouping of external partitions.
//!
//! Layer: vocabulary.
//! - **Owns.** Source configuration fingerprints, observed partition sets and inherited layouts.
//! - **Depends on.** Typed ingestor and branch names, partition counts and execution-unit identity.
//! - **Must not know.** Broker clients, observation tasks, registry decisions or record handling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{
    BranchName, IngestorName, PartitionEpochNumber, PartitionSeed, ResolvedBranchPartitions, Unit,
};

/// A non-security digest identifying the exact source configuration an observation inspected.
/// Non-security BLAKE3: identifies the typed source plan for observation freshness only.
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
pub struct PartitionSourceFingerprint([u8; 32]);

impl PartitionSourceFingerprint {
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    pub const fn digest(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A transport partition coordinate, independent of a concrete key's fields.
#[derive(
    Debug,
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
pub struct ExternalPartition(u32);

impl ExternalPartition {
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    pub const fn index(self) -> u32 {
        self.0
    }

    pub fn seed(self) -> PartitionSeed {
        PartitionSeed::from_le_bytes(u64::from(self.0).to_le_bytes())
    }
}

/// The immutable external coordinate and committed branch epoch that admitted one key.
#[derive(
    Debug,
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
pub struct InheritedPartitionOrigin {
    pub partition: ExternalPartition,
    pub epoch: PartitionEpochNumber,
    /// The group that admitted this record, retained for lifetime operations after regrouping.
    pub unit: Unit,
}

/// The complete enumerable set a broker reported. Partitioned topics name the consecutive
/// coordinates from zero to their count; an unpartitioned topic has one external coordinate.
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
pub enum ExternalPartitionSet {
    Unpartitioned,
    Partitioned(NonZeroU32),
}

impl ExternalPartitionSet {
    pub const fn count(self) -> NonZeroU32 {
        match self {
            Self::Unpartitioned => NonZeroU32::MIN,
            Self::Partitioned(count) => count,
        }
    }

    pub const fn contains(self, partition: ExternalPartition) -> bool {
        partition.index() < self.count().get()
    }
}

/// One source's latest observation. An absent set means the configured external object is
/// unavailable; an absent map entry means it has not been observed for this configuration yet.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SourcePartitionObservation {
    pub configuration: PartitionSourceFingerprint,
    pub partitions: Option<ExternalPartitionSet>,
    pub epoch: u64,
}

/// The grouping committed for one inherited branch. The modulo rule is represented once rather
/// than storing a repeated assignment vector for every source and every intake instance.
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
pub enum InheritedPartitionLayout {
    Identity(NonZeroU16),
    Grouped {
        source_count: NonZeroU32,
        groups: NonZeroU16,
    },
}

impl InheritedPartitionLayout {
    pub fn source_count(self) -> NonZeroU32 {
        match self {
            Self::Identity(count) => NonZeroU32::from(count),
            Self::Grouped { source_count, .. } => source_count,
        }
    }

    pub const fn partitions(self) -> ResolvedBranchPartitions {
        ResolvedBranchPartitions::Inherited(match self {
            Self::Identity(count) => count,
            Self::Grouped { groups, .. } => groups,
        })
    }

    pub fn unit_of(self, partition: ExternalPartition) -> Option<Unit> {
        if partition.index() >= self.source_count().get() {
            return None;
        }
        Some(self.partitions().unit_of(partition.seed()))
    }

    /// Declared groups that receive no external partition, in increasing order.
    pub fn idle_partitions(self) -> std::ops::Range<u32> {
        self.source_count().get()..u32::from(self.partitions().count().get())
    }
}

/// The domain-owned topology snapshot shared by every co-partitioned producer.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct PartitionTopology {
    pub sources: BTreeMap<IngestorName, SourcePartitionObservation>,
    pub branches: BTreeMap<BranchName, InheritedPartitionLayout>,
    pub suspended: BTreeSet<IngestorName>,
}

/// A captured topology and its state-bearing epochs, independent of node placement. Restoring
/// this coordinate before discovery lets state retain its real layout while brokers are offline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTopologySnapshot {
    pub topology: PartitionTopology,
    pub epochs: BTreeMap<BranchName, crate::BranchPartitionEpoch>,
    pub holding_units: BTreeMap<BranchName, BTreeSet<Unit>>,
    /// Installed PER PARTITION intakes retain their native positions while discovery is pending.
    pub intake_units: BTreeMap<IngestorName, NonZeroU16>,
}

/// The exact inverse grouping one reader executes, projected from a committed observation.
/// No broker-specific assignment vector is stored beside the shared topology.
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
pub struct ExternalPartitionAssignment {
    pub partitions: ExternalPartitionSet,
    pub readers: NonZeroU64,
    pub epoch: u64,
}

impl ExternalPartitionAssignment {
    pub fn contains(self, reader: u64, partition: ExternalPartition) -> bool {
        reader < self.readers.get()
            && self.partitions.contains(partition)
            && u64::from(partition.index()) % self.readers.get() == reader
    }
}

/// The committed external coordinates a connector may read. A shared subscription reads the
/// complete set; a partition-owned subscription reads its instance's inverse grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalPartitionRead {
    Assigned(ExternalPartitionAssignment),
    All(ExternalPartitionSet),
}

impl ExternalPartitionRead {
    pub fn contains(self, reader: u64, partition: ExternalPartition) -> bool {
        match self {
            Self::Assigned(assignment) => assignment.contains(reader, partition),
            Self::All(partitions) => partitions.contains(partition),
        }
    }
}
