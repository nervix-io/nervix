//! The relay memory a scenario takes from one node, for public remote relay delivery scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The nodes armed to take their free relay memory and the memory each fill holds.
//! - **Depends on.** Vocabulary identities and the node's execution budgets.
//! - **Must not know.** Relays, deliveries, acknowledgement watches or the interconnect protocol.

use meticulous::OptionExt as _;
use nervix_execution::{Executor, MemoryClass, Reservation};
use nervix_models::ClusterNodeName;

use super::FaultInjection;

/// The relay memory a scenario fills on one node as the node charges acknowledgement watches.
#[derive(Debug)]
pub(super) enum RelayMemoryFill {
    /// The node's next charge of acknowledgement watches first takes all of its free relay memory.
    Armed,
    /// The relay memory the fill took, held until the scenario releases it.
    Holding {
        // Held for as long as the fill lasts and never read: the charge is the reservation's
        // existence, not its value.
        _memory: Reservation,
    },
}

impl FaultInjection {
    /// Arms `node_id` to take all of its free relay memory just before it charges the
    /// acknowledgement watches of the next delivery another node sends it, and to hold that memory
    /// until the scenario releases it.
    pub fn fill_relay_memory_before_next_acknowledgement_watches_on(
        &self,
        node_id: ClusterNodeName,
    ) {
        self.inner
            .relay_memory_fills
            .insert(node_id, RelayMemoryFill::Armed);
    }

    /// Returns the relay memory an armed fill took on `node_id`, and disarms a fill that has not
    /// taken any yet.
    pub fn release_relay_memory_fill_on(&self, node_id: &ClusterNodeName) {
        self.inner.relay_memory_fills.remove(node_id);
    }

    /// Whether the fill armed on `node_id` holds its relay memory.
    pub fn relay_memory_fill_holds_on(&self, node_id: &ClusterNodeName) -> bool {
        let Some(fill) = self.inner.relay_memory_fills.get(node_id) else {
            return false;
        };
        matches!(fill.value(), RelayMemoryFill::Holding { .. })
    }

    /// Takes all of `node_id`'s free relay memory, once, when a scenario armed the fill.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario fills a node's relay memory only before the first watches the \
                      node charges after the fill was armed",
            key = "one test node's armed relay memory fill",
            bound = "one keyed entry and one charge of the free room; no guard across await"
        )
    )]
    pub(crate) fn fill_relay_memory_before_acknowledgement_watches_if_armed(
        &self,
        node_id: &ClusterNodeName,
        executor: &Executor,
    ) {
        let Some(mut fill) = self.inner.relay_memory_fills.get_mut(node_id) else {
            return;
        };
        if let RelayMemoryFill::Holding { .. } = fill.value() {
            return;
        }
        // The relay work running beside the fill can take memory between the read and the
        // charge, so the fill charges again what is free then.
        let reservation = loop {
            let memory = executor.memory(MemoryClass::Relay);
            let room = memory
                .capacity_bytes
                .checked_sub(memory.reserved_bytes)
                .assured("a budget never reserves more than its capacity");
            if let Ok(reservation) = executor.try_reserve(MemoryClass::Relay, room) {
                break reservation;
            }
        };
        *fill.value_mut() = RelayMemoryFill::Holding {
            _memory: reservation,
        };
    }
}
