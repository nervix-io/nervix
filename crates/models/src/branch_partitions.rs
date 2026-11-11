//! Branch partition declarations and the concrete layouts resolved before execution.
//!
//! Layer: vocabulary.
//! - **Owns.** Direct and inherited declarations, resolved nonzero layouts.
//! - **Depends on.** Execution unit identities, scalar counts and archive/serialization traits.
//! - **Must not know.** Grammar, external brokers, registry decisions or runtime records.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the partition vocabulary declared above")
)]

use std::num::NonZeroU16;

use meticulous::ResultExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{PartitionSeed, Unit};

/// The partitioning a branch declaration requests. Inherited counts and their external
/// grouping are resolved from a committed topology before execution units are started.
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
pub enum BranchPartitions {
    Direct(NonZeroU16),
    Inherited,
    InheritedGrouped(NonZeroU16),
}

impl BranchPartitions {
    pub const ONE: Self = Self::Direct(NonZeroU16::MIN);

    pub const fn declared_count(self) -> Option<NonZeroU16> {
        match self {
            Self::Direct(count) | Self::InheritedGrouped(count) => Some(count),
            Self::Inherited => None,
        }
    }

    pub const fn is_inherited(self) -> bool {
        matches!(self, Self::Inherited | Self::InheritedGrouped(_))
    }

    /// Direct declarations need no external observation to resolve their layout.
    pub const fn direct_layout(self) -> Option<ResolvedBranchPartitions> {
        match self {
            Self::Direct(count) => Some(ResolvedBranchPartitions::Direct(count)),
            Self::Inherited | Self::InheritedGrouped(_) => None,
        }
    }
}

impl std::fmt::Display for BranchPartitions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Direct(count) => write!(formatter, "{count}"),
            Self::Inherited => formatter.write_str("INHERITED"),
            Self::InheritedGrouped(count) => write!(formatter, "{count} INHERITED"),
        }
    }
}

/// A resolved nonzero partition layout. Direct layouts use a key fingerprint seed; inherited
/// layouts use the external partition coordinate. Every relay of the branch shares this layout.
/// An inherited declaration awaiting discovery is not a resolved layout.
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
pub enum ResolvedBranchPartitions {
    Direct(NonZeroU16),
    Inherited(NonZeroU16),
}

impl ResolvedBranchPartitions {
    pub const ONE: Self = Self::Direct(NonZeroU16::MIN);

    pub const fn is_inherited(self) -> bool {
        matches!(self, Self::Inherited(_))
    }

    /// The resolved number of partitions.
    pub const fn count(self) -> NonZeroU16 {
        match self {
            Self::Direct(count) | Self::Inherited(count) => count,
        }
    }

    /// Whether the branch is divided into several partitions.
    pub const fn is_partitioned(self) -> bool {
        self.count().get() > 1
    }

    /// The units a relay of this branch is scheduled as: the one `Single` unit every graph without
    /// partitions has for a branch of one partition, and one `Partition` unit per partition
    /// otherwise, in partition order.
    pub fn units(self) -> Vec<Unit> {
        if !self.is_partitioned() {
            return vec![Unit::SINGLE];
        }
        let count = self.count().get();
        (0..count)
            .map(|partition| Unit::partition(u32::from(partition)))
            .collect()
    }

    /// Whether a node this branch branches is scheduled as `unit` under this count: the `Single`
    /// unit for one partition, and `Partition(0)` through `Partition(n - 1)` for n partitions.
    pub fn schedules(self, unit: Unit) -> bool {
        match unit.kind() {
            crate::UnitKind::Single => !self.is_partitioned(),
            crate::UnitKind::Partition(index) => {
                self.is_partitioned() && index < u32::from(self.count().get())
            }
            crate::UnitKind::Instance(_) => false,
        }
    }

    /// The units of a relay of this branch that instance `instance` of an emitter with `instances`
    /// instances serves: every partition `p` with `p mod instances = instance`, and the `Single`
    /// unit of a branch of one partition, which instance 0 serves. An unbranched relay serves as a
    /// branch of one partition.
    pub fn units_served_by(self, instance: u64, instances: std::num::NonZeroU64) -> Vec<Unit> {
        let mut served = Vec::new();
        for (ordinal, unit) in (0_u64..).zip(self.units()) {
            if ordinal % instances.get() == instance {
                served.push(unit);
            }
        }
        served
    }

    /// The unit that owns a key whose seed is `seed`: the `Single` unit of a branch of one
    /// partition, and `Partition(seed mod n)` of a branch of n partitions.
    pub fn unit_of(self, seed: PartitionSeed) -> Unit {
        if !self.is_partitioned() {
            return Unit::SINGLE;
        }
        let count = u64::from(self.count().get());
        let partition = seed.get() % count;
        Unit::partition(
            u32::try_from(partition)
                .assured("a remainder modulo a 16-bit count is below that count"),
        )
    }
}
