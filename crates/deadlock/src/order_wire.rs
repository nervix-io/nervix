//! The current wire shape of potential order, multiplicity and engineer review.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Validated lossless encoding of order evidence and review.
//! - **Depends on.** Primitive order types and the evidence wire's scalar encodings.
//! - **Must not know.** Detector configuration, source owners or the workload.

use error_stack::Report;
use nervix_primitives::deadlock::{
    BlockedAttempt, BoundedText, LockLifetime, MAX_ORDER_EDGES, MAX_ORDER_WITNESSES, OrderEdge,
    OrderLock, OrderOutOfBounds, OrderWitness, PotentialCycle, TrackedThreadId, WaitedLock,
};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    EvidenceError, EvidenceOutOfBounds, PotentialTriage, ProofBasis, TriageProof,
    wire::{AttemptWire, TextWire, WaitedLockWire, nonzero, system_time, unix_nanos},
};

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct OrderCycleWire {
    pub(crate) detected_at_unix_nanos: u64,
    pub(crate) edges: Vec<OrderEdgeWire>,
    pub(crate) omitted_edges: u64,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct OrderEdgeWire {
    pub(crate) before: OrderLockWire,
    pub(crate) after: OrderLockWire,
    pub(crate) witnesses: Vec<WitnessWire>,
    pub(crate) omitted_witnesses: u64,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct OrderLockWire {
    pub(crate) lock: WaitedLockWire,
    pub(crate) lifetime: LifetimeWire,
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum LifetimeWire {
    Live,
    Ended,
    Unrecorded,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct WitnessWire {
    pub(crate) thread: u64,
    pub(crate) name: Option<TextWire>,
    pub(crate) held: AttemptWire,
    pub(crate) requested: AttemptWire,
    pub(crate) attempts: u64,
    pub(crate) held_count: u64,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum TriageWire {
    Unreviewed,
    Reviewed(ProofWire),
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct ProofWire {
    pub(crate) basis: BasisWire,
    pub(crate) reason: TextWire,
    pub(crate) regression: TextWire,
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum BasisWire {
    Correction,
    NonOverlap,
    SharedReaders,
    Lifecycle,
}

impl TryFrom<&PotentialCycle> for OrderCycleWire {
    type Error = Report<EvidenceError>;
    fn try_from(cycle: &PotentialCycle) -> Result<Self, Self::Error> {
        Ok(Self {
            detected_at_unix_nanos: unix_nanos(cycle.detected_at())?,
            edges: cycle.edges().iter().map(OrderEdgeWire::from).collect(),
            omitted_edges: cycle.omitted_edges(),
        })
    }
}

impl From<&OrderEdge> for OrderEdgeWire {
    fn from(edge: &OrderEdge) -> Self {
        Self {
            before: OrderLockWire::from(&edge.before),
            after: OrderLockWire::from(&edge.after),
            witnesses: edge.witnesses().iter().map(WitnessWire::from).collect(),
            omitted_witnesses: edge.omitted_witnesses(),
        }
    }
}

impl From<&OrderLock> for OrderLockWire {
    fn from(lock: &OrderLock) -> Self {
        let lifetime = match lock.lifetime {
            LockLifetime::Live => LifetimeWire::Live,
            LockLifetime::Ended => LifetimeWire::Ended,
            LockLifetime::Unrecorded => LifetimeWire::Unrecorded,
        };
        Self {
            lock: WaitedLockWire::from(&WaitedLock {
                id: lock.id,
                site: lock.site.clone(),
            }),
            lifetime,
        }
    }
}

impl From<&OrderWitness> for WitnessWire {
    fn from(witness: &OrderWitness) -> Self {
        Self {
            thread: witness.thread.get().get(),
            name: witness.name.as_ref().map(TextWire::from),
            held: AttemptWire::from(&witness.held),
            requested: AttemptWire::from(&witness.requested),
            attempts: witness.attempts.get(),
            held_count: witness.held_count.get(),
        }
    }
}

impl TryFrom<OrderCycleWire> for PotentialCycle {
    type Error = EvidenceOutOfBounds;
    fn try_from(cycle: OrderCycleWire) -> Result<Self, Self::Error> {
        if cycle.edges.len() > MAX_ORDER_EDGES {
            return Err(EvidenceOutOfBounds::Order(OrderOutOfBounds::TooManyEdges));
        }
        let mut edges = Vec::with_capacity(cycle.edges.len());
        for edge in cycle.edges {
            edges.push(OrderEdge::try_from(edge)?);
        }
        Self::new(
            system_time(cycle.detected_at_unix_nanos),
            edges,
            cycle.omitted_edges,
        )
        .map_err(EvidenceOutOfBounds::Order)
    }
}

impl TryFrom<OrderEdgeWire> for OrderEdge {
    type Error = EvidenceOutOfBounds;
    fn try_from(edge: OrderEdgeWire) -> Result<Self, Self::Error> {
        if edge.witnesses.len() > MAX_ORDER_WITNESSES {
            return Err(EvidenceOutOfBounds::Order(
                OrderOutOfBounds::TooManyWitnesses,
            ));
        }
        let mut witnesses = Vec::with_capacity(edge.witnesses.len());
        for witness in edge.witnesses {
            witnesses.push(OrderWitness::try_from(witness)?);
        }
        Self::new(
            OrderLock::try_from(edge.before)?,
            OrderLock::try_from(edge.after)?,
            witnesses,
            edge.omitted_witnesses,
        )
        .map_err(EvidenceOutOfBounds::Order)
    }
}

impl TryFrom<OrderLockWire> for OrderLock {
    type Error = EvidenceOutOfBounds;
    fn try_from(wire: OrderLockWire) -> Result<Self, Self::Error> {
        let lock = WaitedLock::try_from(wire.lock)?;
        let lifetime = match wire.lifetime {
            LifetimeWire::Live => LockLifetime::Live,
            LifetimeWire::Ended => LockLifetime::Ended,
            LifetimeWire::Unrecorded => LockLifetime::Unrecorded,
        };
        Ok(Self {
            id: lock.id,
            site: lock.site,
            lifetime,
        })
    }
}

impl TryFrom<WitnessWire> for OrderWitness {
    type Error = EvidenceOutOfBounds;
    fn try_from(witness: WitnessWire) -> Result<Self, Self::Error> {
        let name = match witness.name {
            Some(name) => Some(BoundedText::try_from(name)?),
            None => None,
        };
        Ok(Self {
            thread: TrackedThreadId::new(nonzero(witness.thread)?),
            name,
            held: BlockedAttempt::try_from(witness.held)?,
            requested: BlockedAttempt::try_from(witness.requested)?,
            attempts: nonzero(witness.attempts)?,
            held_count: nonzero(witness.held_count)?,
        })
    }
}

impl From<&PotentialTriage> for TriageWire {
    fn from(triage: &PotentialTriage) -> Self {
        match triage {
            PotentialTriage::Unreviewed => Self::Unreviewed,
            PotentialTriage::Reviewed(proof) => {
                let basis = match proof.basis() {
                    ProofBasis::Correction => BasisWire::Correction,
                    ProofBasis::NonOverlap => BasisWire::NonOverlap,
                    ProofBasis::SharedReaders => BasisWire::SharedReaders,
                    ProofBasis::Lifecycle => BasisWire::Lifecycle,
                };
                Self::Reviewed(ProofWire {
                    basis,
                    reason: TextWire::from(proof.reason()),
                    regression: TextWire::from(proof.regression()),
                })
            }
        }
    }
}

impl TryFrom<TriageWire> for PotentialTriage {
    type Error = EvidenceOutOfBounds;
    fn try_from(triage: TriageWire) -> Result<Self, Self::Error> {
        match triage {
            TriageWire::Unreviewed => Ok(Self::Unreviewed),
            TriageWire::Reviewed(proof) => {
                let basis = match proof.basis {
                    BasisWire::Correction => ProofBasis::Correction,
                    BasisWire::NonOverlap => ProofBasis::NonOverlap,
                    BasisWire::SharedReaders => ProofBasis::SharedReaders,
                    BasisWire::Lifecycle => ProofBasis::Lifecycle,
                };
                Ok(Self::Reviewed(TriageProof::from_parts(
                    basis,
                    BoundedText::try_from(proof.reason)?,
                    BoundedText::try_from(proof.regression)?,
                )?))
            }
        }
    }
}
