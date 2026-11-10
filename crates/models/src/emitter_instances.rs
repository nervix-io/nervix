//! How many instances an emitter runs, and which emitter entities can run more than one.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The `INSTANCES <count> | INSTANCES PER PARTITION` declaration an emitter statement
//!   writes, the instancing capability each emitter entity declares, the reason a single-instance
//!   entity gives for refusing more, and the typed refusal that reason produces.
//! - **Depends on.** The emitter entity kinds, which name the entity that refuses.
//! - **Must not know.** How instances are validated against their inputs, placed, planned or run.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{
    fmt::{Display, Formatter},
    num::NonZeroU64,
};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::EmitterEntityKind;

/// `INSTANCES <count>` or `INSTANCES PER PARTITION`: the instances an emitter runs. A statement
/// that writes no `INSTANCES` clause declares [`EmitterInstances::ONE`].
///
/// Instance `i` of `n` serves the input partitions `p` with `p mod n = i`, so one key lives in one
/// partition and one instance.
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
pub enum EmitterInstances {
    /// A fixed number of instances.
    Count(NonZeroU64),
    /// One instance per partition of the branch every input relay shares.
    PerPartition,
}

impl EmitterInstances {
    /// The single instance an emitter runs when its statement writes no `INSTANCES` clause.
    pub const ONE: Self = Self::Count(NonZeroU64::MIN);

    /// Whether this is the single instance a statement declares by writing no clause.
    pub fn is_one(self) -> bool {
        self == Self::ONE
    }
}

impl Display for EmitterInstances {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count(count) => count.fmt(f),
            Self::PerPartition => f.write_str("PER PARTITION"),
        }
    }
}

/// Whether an emitter entity can run more than one instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterInstancingCapability {
    /// It runs exactly one instance, for the declared reason, and has no `INSTANCES` clause.
    Single { reason: SingleEmitterReason },
    /// It accepts `INSTANCES <count>` and `INSTANCES PER PARTITION`.
    Instances,
}

impl EmitterInstancingCapability {
    /// What an emitter of `entity` says when a statement writes `INSTANCES`, where it runs one
    /// instance.
    pub const fn refusal(self, entity: EmitterEntityKind) -> Option<SingleInstanceRefusal> {
        match self {
            Self::Single { reason } => Some(SingleInstanceRefusal { entity, reason }),
            Self::Instances => None,
        }
    }
}

/// Why an emitter entity runs one instance. It reads as what the entity's emitters do instead:
/// "CLIENT CONSUMER emitters deliver through one consumer stream".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
pub enum SingleEmitterReason {
    /// Application sessions consume the emitter's rows through one consumer stream.
    #[strum(to_string = "deliver through one consumer stream")]
    OneConsumerStream,
    /// One writer stages the emitter's rows and commits the table's snapshots.
    #[strum(to_string = "commit through one table writer")]
    OneTableWriter,
    /// The broker keeps one session per client identifier, so a second connection with the same
    /// identifier takes the first one's session over.
    #[strum(to_string = "publish through one client identifier")]
    OneClientIdentifier,
}

/// A statement wrote `INSTANCES` for an emitter entity that runs one instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "an instancing declaration the entity cannot run is a pure validation refusal \
                  before placement or storage"
    )
)]
#[error("{entity} emitters {reason}, so they refuse INSTANCES")]
pub struct SingleInstanceRefusal {
    pub entity: EmitterEntityKind,
    pub reason: SingleEmitterReason,
}
