//! Node-local refusals of subscription openings for public restoration scenarios.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** The one subscription opening a scenario arms a node to refuse as unavailable,
//!   and clearing an unused refusal before another host-suite test begins.
//! - **Depends on.** The fault harness's shared seam registry and typed node identities.
//! - **Must not know.** How a session opens a subscription, or how a client restores one.
//!
//! A scenario proves the refusal happened through what its client observes, so a delivered
//! refusal leaves nothing behind.

use nervix_models::ClusterNodeName;

use super::FaultInjection;

impl FaultInjection {
    /// Makes `node_id` refuse the next subscription it is asked to open as unavailable.
    pub fn refuse_next_subscription_opening_on(&self, node_id: ClusterNodeName) {
        self.inner.refused_subscription_openings.insert(node_id, ());
    }

    /// Clears an unused refusal on `node_id` so it cannot affect a later test.
    pub fn clear_subscription_opening_refusal_on(&self, node_id: &ClusterNodeName) {
        self.inner.refused_subscription_openings.remove(node_id);
    }

    /// Takes the refusal armed on `node_id`, when one is still armed.
    pub(crate) fn take_subscription_opening_refusal(&self, node_id: &ClusterNodeName) -> bool {
        self.inner
            .refused_subscription_openings
            .remove(node_id)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn clearing_unused_refusals_leaves_later_tests_unaffected_and_other_nodes_armed() {
        let faults = FaultInjection::default();
        let chosen = ClusterNodeName::parse("node-1").assured("a valid literal node name");
        let other = ClusterNodeName::parse("node-2").assured("a valid literal node name");
        faults.refuse_next_subscription_opening_on(chosen.clone());
        faults.refuse_next_subscription_opening_on(other.clone());
        faults.clear_subscription_opening_refusal_on(&chosen);
        assert!(!faults.take_subscription_opening_refusal(&chosen));
        assert!(faults.take_subscription_opening_refusal(&other));
        assert!(!faults.take_subscription_opening_refusal(&other));
        faults.clear_subscription_opening_refusal_on(&chosen);
        faults.refuse_next_subscription_opening_on(chosen.clone());
        assert!(faults.take_subscription_opening_refusal(&chosen));
    }
}
