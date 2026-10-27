//! Fault controls at the coordinator's entity drain observation.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Domain-scoped forced entity drain timeouts and the drain each one refuses once it
//!   met one.
//! - **Depends on.** Vocabulary identities, the durable domain mutation owner, the gate purpose and
//!   the shared test injector.
//! - **Must not know.** Entity gates, transactions, command recovery or the runtime.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns test drain controls and the drain each one refuses"
    )
)]

use ahash::HashSet;
use nervix_consensus::DomainMutationOwner;
use nervix_interconnect::EntityGatePurpose;
use nervix_models::{DomainName, NodeRef, RelayName};
use nervix_primitives::collections::dash_map::Entry;

use super::FaultInjection;

/// One entity drain a coordinator observes: what it waits for, and the durable execution that owns
/// its domain's mutation at that observation, if any. An operation that runs again under the same
/// execution reference observes the same drain again.
pub(crate) struct ObservedEntityDrain<'a> {
    pub(crate) owner: Option<&'a DomainMutationOwner>,
    pub(crate) purpose: EntityGatePurpose,
    pub(crate) relays: &'a [RelayName],
    pub(crate) affected_entities: &'a [NodeRef],
}

/// The drain a forced timeout refused, which it keeps refusing whenever its execution observes
/// that drain again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RefusedEntityDrain {
    owner: DomainMutationOwner,
    purpose: EntityGatePurpose,
    relays: HashSet<RelayName>,
    affected_entities: HashSet<NodeRef>,
}

impl RefusedEntityDrain {
    fn new(owner: &DomainMutationOwner, drain: &ObservedEntityDrain<'_>) -> Self {
        Self {
            owner: owner.clone(),
            purpose: drain.purpose,
            relays: drain.relays.iter().cloned().collect(),
            affected_entities: drain.affected_entities.iter().cloned().collect(),
        }
    }

    /// Whether `drain` waits for what this refused drain waited for.
    fn waits_like(&self, drain: &ObservedEntityDrain<'_>) -> bool {
        let relays = drain.relays.iter().cloned().collect::<HashSet<_>>();
        let affected_entities = drain
            .affected_entities
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        self.purpose == drain.purpose
            && self.relays == relays
            && self.affected_entities == affected_entities
    }
}

/// A forced timeout of one pending entity drain in a domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ForcedEntityDrainTimeout {
    /// No drain observation has met the timeout yet.
    Armed,
    /// A drain observation met the timeout while a durable execution owned the domain's mutation,
    /// so the timeout refuses that drain whenever the execution observes it, until the execution no
    /// longer owns the domain.
    Refusing(RefusedEntityDrain),
}

/// What one entity drain observation makes of a forced timeout.
enum ForcedEntityDrainObservation {
    /// The drain is refused, and the timeout refuses it whenever its execution observes it again.
    Claimed(RefusedEntityDrain),
    /// The drain is the one the timeout refused before, observed again by the same execution.
    RefusedAgain,
    /// The drain is refused once: no durable execution owns the domain to observe it again.
    RefusedOnce,
    /// The drain is another one of the refused drain's execution, which the timeout leaves alone.
    OtherDrain,
    /// The drain is not refused: the refused drain's execution no longer owns the domain.
    Ended,
}

impl ForcedEntityDrainTimeout {
    fn observe(&self, drain: &ObservedEntityDrain<'_>) -> ForcedEntityDrainObservation {
        match (self, drain.owner) {
            (Self::Armed, Some(owner)) => {
                ForcedEntityDrainObservation::Claimed(RefusedEntityDrain::new(owner, drain))
            }
            (Self::Armed, None) => ForcedEntityDrainObservation::RefusedOnce,
            (Self::Refusing(refused), Some(owner))
                if refused.owner == *owner && refused.waits_like(drain) =>
            {
                ForcedEntityDrainObservation::RefusedAgain
            }
            (Self::Refusing(refused), Some(owner)) if refused.owner == *owner => {
                ForcedEntityDrainObservation::OtherDrain
            }
            // The refused drain's execution reached its terminal outcome: another execution owns
            // the domain now, or none does.
            (Self::Refusing(_), Some(_) | None) => ForcedEntityDrainObservation::Ended,
        }
    }
}

impl FaultInjection {
    /// Forces the next pending entity drain in `domain` to report its normal timeout outcome after
    /// one complete status observation, so timeout recovery tests exercise that outcome without
    /// making successful cluster work race a short duration.
    ///
    /// When a durable execution owns the domain's mutation at the refused drain, the timeout
    /// refuses the same drain whenever that execution observes it again, until another execution or
    /// none owns the domain. A command whose refusal was never recorded runs again under its
    /// execution reference, on a new leader or after a retry, and meets the same refusal, while
    /// its other drains, such as those of the other units a node drain moves, are left alone. A
    /// drain that no execution owns meets the timeout once.
    pub fn force_next_entity_drain_timeout(&self, domain: DomainName) {
        self.inner
            .forced_entity_drain_timeouts
            .insert(domain, ForcedEntityDrainTimeout::Armed);
    }

    /// Whether a forced timeout waits for or refuses an entity drain in `domain`, so that a
    /// coordinator looks the domain's mutation owner up only then.
    pub(crate) fn entity_drain_timeout_is_forced(&self, domain: &DomainName) -> bool {
        self.inner.forced_entity_drain_timeouts.contains_key(domain)
    }

    /// Whether `drain`, which a coordinator observes in `domain`, meets a forced timeout. Consumed
    /// only at the coordinator's drain-observation seam.
    pub(crate) fn take_forced_entity_drain_timeout(
        &self,
        domain: &DomainName,
        drain: &ObservedEntityDrain<'_>,
    ) -> bool {
        let Entry::Occupied(mut timeout) = self
            .inner
            .forced_entity_drain_timeouts
            .entry(domain.clone())
        else {
            return false;
        };
        let observation = timeout.get().observe(drain);
        match observation {
            ForcedEntityDrainObservation::Claimed(refused) => {
                timeout.insert(ForcedEntityDrainTimeout::Refusing(refused));
                true
            }
            ForcedEntityDrainObservation::RefusedAgain => true,
            ForcedEntityDrainObservation::RefusedOnce => {
                timeout.remove();
                true
            }
            ForcedEntityDrainObservation::OtherDrain => false,
            ForcedEntityDrainObservation::Ended => {
                timeout.remove();
                false
            }
        }
    }
}
