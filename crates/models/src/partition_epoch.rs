//! The partition epochs a domain schedule records for its branches.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** A branch's partition epoch once its partition count first changed: the epoch's
//!   number and count and, while keys placed under the previous count may still lie outside their
//!   partition under the current one, the transition from that previous count together with the
//!   units it keeps scheduled for those keys.
//! - **Depends on.** Branch partitions and execution-unit identity.
//! - **Must not know.** How a key moves between units, how a unit's keys are counted, or when a
//!   transition retires.
//!
//! This is replicated control-plane state, which belongs to consensus rather than to the
//! vocabulary: the crate root names that contradiction.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{fmt, num::NonZeroU64};

use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{BranchPartitions, Unit};

/// The number of a branch's partition epoch: one for the count the branch was created with, and one
/// more for each committed change of that count.
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
#[serde(transparent)]
pub struct PartitionEpochNumber(NonZeroU64);

impl PartitionEpochNumber {
    /// The epoch of the count a branch was created with.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// The epoch numbered `number`, which is never zero.
    pub const fn new(number: NonZeroU64) -> Self {
        Self(number)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// The epoch the next committed change of the count begins.
    #[must_use]
    pub fn next(self) -> Self {
        Self(
            self.0
                .checked_add(1)
                .assured("a domain commits fewer than 2^64 changes of one branch's count"),
        )
    }
}

impl fmt::Display for PartitionEpochNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// The partition epoch of a branch whose count changed at least once since it was created.
///
/// A branch the schedule records no epoch for keeps the count it was created with, in epoch one,
/// and every key of it lies in its partition.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct BranchPartitionEpoch {
    pub number: PartitionEpochNumber,
    /// The count this epoch places keys by.
    pub partitions: BranchPartitions,
    /// Present while keys placed under the previous count may still lie outside their partition
    /// under this one.
    pub transition: Option<PartitionTransition>,
}

/// The change from the count keys were placed by before an epoch began.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PartitionTransition {
    pub previous: BranchPartitions,
}

impl BranchPartitionEpoch {
    /// The epoch a committed change from `from` to `to` begins after `current`: the branch's
    /// recorded epoch, or none while it keeps the count it was created with.
    pub fn changed(current: Option<&Self>, from: BranchPartitions, to: BranchPartitions) -> Self {
        let number = match current {
            Some(current) => current.number.next(),
            None => PartitionEpochNumber::FIRST.next(),
        };
        Self {
            number,
            partitions: to,
            transition: Some(PartitionTransition { previous: from }),
        }
    }

    /// The epoch a committed change to `to` begins after `current` for a branch none of whose
    /// entities keeps state per key, so no key has anything to move and no transition is needed.
    pub fn settled_change(current: Option<&Self>, to: BranchPartitions) -> Self {
        let number = match current {
            Some(current) => current.number.next(),
            None => PartitionEpochNumber::FIRST.next(),
        };
        Self {
            number,
            partitions: to,
            transition: None,
        }
    }

    /// The units every node the branch branches keeps scheduled beyond the current count's units,
    /// because they may still hold keys placed under the previous count. A growing count keeps
    /// none, since every unit of the previous count is a unit of the current one.
    pub fn holding_units(&self) -> Vec<Unit> {
        let Some(transition) = &self.transition else {
            return Vec::new();
        };
        let mut holding = Vec::new();
        for unit in transition.previous.units() {
            if !self.partitions.schedules(unit) {
                holding.push(unit);
            }
        }
        holding
    }

    /// This epoch once no unit holds a key outside its partition any more.
    #[must_use]
    pub fn retired(&self) -> Self {
        Self {
            number: self.number,
            partitions: self.partitions,
            transition: None,
        }
    }
}
