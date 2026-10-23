//! The relay memory a scenario takes from one node, for public remote relay delivery scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The nodes armed to take their free relay memory, the point in its relay work at
//!   which each takes it, the execution budgets the nodes register to be filled at once, and the
//!   memory each fill holds.
//! - **Depends on.** Vocabulary identities and the node's execution budgets.
//! - **Must not know.** Relays, deliveries, acknowledgement watches, record storage or the
//!   interconnect protocol.

use meticulous::OptionExt as _;
use nervix_execution::{Executor, MemoryClass, Reservation};
use nervix_models::ClusterNodeName;

use super::FaultInjection;

/// The relay memory a scenario fills on one node.
#[derive(Debug)]
pub(super) enum RelayMemoryFill {
    /// The node takes all of its free relay memory when its relay work next reaches this point.
    Armed(RelayMemoryFillPoint),
    /// The relay memory the fill took, held until the scenario releases it.
    Holding {
        // Held for as long as the fill lasts and never read: the charge is the reservation's
        // existence, not its value.
        _memory: Reservation,
    },
}

/// Where in its relay work a node takes the free relay memory an armed fill names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelayMemoryFillPoint {
    /// Just before the node charges the acknowledgement watches of the next delivery another node
    /// sends it.
    AcknowledgementWatches,
    /// Just before the node stores the acknowledgement records of the next delivery it sends
    /// another node.
    RecordStorage,
}

impl FaultInjection {
    /// Lets the scenario fill the relay memory of `node_id` at once. A node registers the
    /// execution budgets its relay work charges when it joins its cluster.
    pub(crate) fn register_relay_memory_owner(
        &self,
        node_id: &ClusterNodeName,
        executor: &Executor,
    ) {
        self.inner
            .relay_memory_owners
            .insert(node_id.clone(), executor.clone());
    }

    /// Arms `node_id` to take all of its free relay memory just before it charges the
    /// acknowledgement watches of the next delivery another node sends it, and to hold that memory
    /// until the scenario releases it.
    pub fn fill_relay_memory_before_next_acknowledgement_watches_on(
        &self,
        node_id: ClusterNodeName,
    ) {
        self.inner.relay_memory_fills.insert(
            node_id,
            RelayMemoryFill::Armed(RelayMemoryFillPoint::AcknowledgementWatches),
        );
    }

    /// Arms `node_id` to take all of its free relay memory just before it stores the
    /// acknowledgement records of the next delivery it sends another node, and to hold that
    /// memory until the scenario releases it.
    pub fn fill_relay_memory_before_next_record_storage_on(&self, node_id: ClusterNodeName) {
        self.inner.relay_memory_fills.insert(
            node_id,
            RelayMemoryFill::Armed(RelayMemoryFillPoint::RecordStorage),
        );
    }

    /// Takes all of the free relay memory of `node_id` now, except one operation's decode scratch
    /// room, and holds it until the scenario releases it.
    ///
    /// The room left lets the node read and answer the relay grant requests it receives, and is
    /// smaller than any grant: a grant reserves a delivery's body, the largest decoded batch and
    /// that same scratch room together, so every grant another node asks this one for is refused
    /// while the fill holds.
    pub fn fill_relay_memory_leaving_room_for_no_grant_on(&self, node_id: &ClusterNodeName) {
        let executor = self
            .inner
            .relay_memory_owners
            .get(node_id)
            .verified(
                "a node registers its relay memory as it joins the cluster the scenario started",
            )
            .value()
            .clone();
        let left = executor.limits().relay_scratch_bytes.as_u64();
        let memory = take_free_relay_memory(&executor, left);
        self.inner.relay_memory_fills.insert(
            node_id.clone(),
            RelayMemoryFill::Holding { _memory: memory },
        );
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

    /// Takes all of `node_id`'s free relay memory, once, when a scenario armed the fill before the
    /// node charges acknowledgement watches.
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
        self.fill_relay_memory_at(
            node_id,
            executor,
            RelayMemoryFillPoint::AcknowledgementWatches,
        );
    }

    /// Takes all of `node_id`'s free relay memory, once, when a scenario armed the fill before the
    /// node stores the acknowledgement records of a delivery it sends.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario fills a node's relay memory only before the first record \
                      storage the node charges after the fill was armed",
            key = "one test node's armed relay memory fill",
            bound = "one keyed entry and one charge of the free room; no guard across await"
        )
    )]
    pub(crate) fn fill_relay_memory_before_record_storage_if_armed(
        &self,
        node_id: &ClusterNodeName,
        executor: &Executor,
    ) {
        self.fill_relay_memory_at(node_id, executor, RelayMemoryFillPoint::RecordStorage);
    }

    fn fill_relay_memory_at(
        &self,
        node_id: &ClusterNodeName,
        executor: &Executor,
        point: RelayMemoryFillPoint,
    ) {
        let Some(mut fill) = self.inner.relay_memory_fills.get_mut(node_id) else {
            return;
        };
        let RelayMemoryFill::Armed(armed) = fill.value() else {
            return;
        };
        if *armed != point {
            return;
        }
        let memory = take_free_relay_memory(executor, 0);
        *fill.value_mut() = RelayMemoryFill::Holding { _memory: memory };
    }
}

/// Charges all of `executor`'s free relay memory except `left` bytes of it.
fn take_free_relay_memory(executor: &Executor, left: u64) -> Reservation {
    // The relay work running beside the fill can take memory between the read and the charge, so
    // the fill charges again what is free then.
    loop {
        let memory = executor.memory(MemoryClass::Relay);
        let room = memory
            .capacity_bytes
            .checked_sub(memory.reserved_bytes)
            .assured("a budget never reserves more than its capacity");
        // Less free room than the fill leaves is taken as none: leaving that room is the fill's
        // whole meaning, so the difference saturates at zero rather than failing.
        let taken = room.saturating_sub(left);
        if let Ok(reservation) = executor.try_reserve(MemoryClass::Relay, taken) {
            return reservation;
        }
    }
}
