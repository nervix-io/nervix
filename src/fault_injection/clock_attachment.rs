//! Node-local clock attachment refusals for public host-suite restoration scenarios.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Arming one clock attachment refusal per node and clearing an unused refusal
//!   before a later host test can attach another clock.
//! - **Depends on.** The fault harness's shared seam registry and typed node identities.
//! - **Must not know.** Session transactions, clock installation or client restoration policy.

use nervix_models::ClusterNodeName;

use super::FaultInjection;

impl FaultInjection {
    /// Makes `node_id` refuse its next clock attachment as in-transaction.
    pub fn refuse_next_clock_attachment_on(&self, node_id: ClusterNodeName) {
        self.inner.refused_clock_attachments.insert(node_id, ());
    }

    /// Clears an unused refusal so it cannot affect another host test.
    pub fn clear_clock_attachment_refusal_on(&self, node_id: &ClusterNodeName) {
        self.inner.refused_clock_attachments.remove(node_id);
    }

    /// Takes the next refusal on this node, releasing its entry before a reply is sent.
    pub(crate) fn take_clock_attachment_refusal(&self, node_id: &ClusterNodeName) -> bool {
        self.inner
            .refused_clock_attachments
            .remove(node_id)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn clearing_unused_refusals_keeps_other_nodes_armed_and_taken_refusals_are_one_shot() {
        let faults = FaultInjection::default();
        let chosen = ClusterNodeName::parse("node-1").assured("a valid literal node name");
        let other = ClusterNodeName::parse("node-2").assured("a valid literal node name");
        faults.refuse_next_clock_attachment_on(chosen.clone());
        faults.refuse_next_clock_attachment_on(other.clone());
        faults.clear_clock_attachment_refusal_on(&chosen);
        assert!(!faults.take_clock_attachment_refusal(&chosen));
        assert!(faults.take_clock_attachment_refusal(&other));
        assert!(!faults.take_clock_attachment_refusal(&other));
        faults.clear_clock_attachment_refusal_on(&chosen);
        faults.refuse_next_clock_attachment_on(chosen.clone());
        assert!(faults.take_clock_attachment_refusal(&chosen));
        assert!(!faults.take_clock_attachment_refusal(&chosen));
    }
}
