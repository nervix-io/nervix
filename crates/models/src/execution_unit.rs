//! Compact identities for independently placed executions.
//!
//! Layer: vocabulary.
//! - **Owns.** Execution-unit identity, semantic inspection and its validated external encoding.
//! - **Depends on.** Integer, archive, serialization and typed-error primitives.
//! - **Must not know.** Placement, allocation, runtime tasks or connector implementations.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::fmt;

use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const PARTITION_END: u64 = 1u64 << u32::BITS;
const INSTANCE_START: u64 = PARTITION_END + 1;

/// A semantic view of an execution unit, rather than a second identity or storage key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnitKind {
    Single,
    Partition(u32),
    Instance(u64),
}

#[derive(Debug, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "unit construction returns a pure validation refusal before placement or storage"
    )
)]
#[error("instance index {index} exceeds the execution-unit identity range")]
pub struct UnitIndexError {
    pub index: u64,
}

/// One independently placed execution of a logical node.
///
/// The private rank stores all single and partition units and every instance index a target can
/// materialize in eight bytes, with four-byte archive alignment. Semantic JSON stays tagged by
/// kind. Instance declarations retain their full `u64` count; allocation-layout validation rejects
/// unaddressable counts before their units are materialized.
#[derive(
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
#[serde(into = "UnitKind", try_from = "UnitKind")]
pub struct Unit {
    // Field order also gives semantic ordering: Single, partitions, then instances.
    upper: u32,
    lower: u32,
}

impl Unit {
    pub const SINGLE: Self = Self { upper: 0, lower: 0 };
    pub const FIRST_INSTANCE: Self = Self::from_rank(INSTANCE_START);
    pub const MAX_INSTANCE_INDEX: u64 = u64::MAX - INSTANCE_START;

    pub const fn partition(index: u32) -> Self {
        // The rank is index + 1. Its low-word overflow is retained in the upper word.
        let (lower, carry) = index.overflowing_add(1);
        Self {
            upper: if carry { 1 } else { 0 },
            lower,
        }
    }

    pub fn try_instance(index: u64) -> Result<Self, UnitIndexError> {
        index
            .checked_add(INSTANCE_START)
            .map(Self::from_rank)
            .ok_or(UnitIndexError { index })
    }

    pub fn kind(self) -> UnitKind {
        let rank = (u64::from(self.upper) << u32::BITS) | u64::from(self.lower);
        match rank {
            0 => UnitKind::Single,
            1..=PARTITION_END => UnitKind::Partition(
                u32::try_from(
                    rank.checked_sub(1)
                        .verified("a partition rank is at least one"),
                )
                .verified("partition ranks minus one are at most u32::MAX"),
            ),
            _ => UnitKind::Instance(
                rank.checked_sub(INSTANCE_START)
                    .verified("instance ranks follow the complete single and partition range"),
            ),
        }
    }

    const fn from_rank(rank: u64) -> Self {
        let [a, b, c, d, e, f, g, h] = rank.to_be_bytes();
        Self {
            upper: u32::from_be_bytes([a, b, c, d]),
            lower: u32::from_be_bytes([e, f, g, h]),
        }
    }

    /// The status field that names a unit, or nothing for a single execution.
    pub fn status_field(self) -> String {
        match self.kind() {
            UnitKind::Single => String::new(),
            UnitKind::Partition(partition) => format!(" unit=partition:{partition}"),
            UnitKind::Instance(instance) => format!(" unit=instance:{instance}"),
        }
    }

    /// The operator-facing suffix after the node name.
    pub fn label_suffix(self) -> String {
        match self.kind() {
            UnitKind::Single => String::new(),
            UnitKind::Partition(partition) => format!(" partition {partition}"),
            UnitKind::Instance(instance) => format!(" instance {instance}"),
        }
    }
}

impl From<Unit> for UnitKind {
    fn from(unit: Unit) -> Self {
        unit.kind()
    }
}

impl TryFrom<UnitKind> for Unit {
    type Error = UnitIndexError;

    fn try_from(kind: UnitKind) -> Result<Self, Self::Error> {
        match kind {
            UnitKind::Single => Ok(Self::SINGLE),
            UnitKind::Partition(index) => Ok(Self::partition(index)),
            UnitKind::Instance(index) => Self::try_instance(index),
        }
    }
}

impl fmt::Debug for Unit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(formatter)
    }
}
