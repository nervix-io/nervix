//! Node-local refusals of subscription interest visibility checks for public refusal scenarios.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** The one refusal of a subscription interest visibility check a scenario arms on a
//!   node, and whether that node delivered it.
//! - **Depends on.** The fault harness's shared seam registry and typed node identities.
//! - **Must not know.** Gossip publication, interest leases, or how a refusal reaches a client.

use nervix_models::ClusterNodeName;

use super::FaultInjection;

/// Whether a node still owes the visibility refusal a scenario armed, or delivered it.
#[derive(Debug, Clone, Copy)]
pub(super) enum InterestVisibilityRefusal {
    Armed,
    Delivered,
}

impl InterestVisibilityRefusal {
    fn is_delivered(self) -> bool {
        matches!(self, Self::Delivered)
    }
}

impl FaultInjection {
    /// Makes `node_id` refuse the next subscription interest visibility check it answers.
    pub fn refuse_interest_visibility_on(&self, node_id: ClusterNodeName) {
        self.inner
            .refused_interest_visibility
            .insert(node_id, InterestVisibilityRefusal::Armed);
    }

    /// Whether `node_id` refused the visibility check a scenario armed it to refuse.
    pub fn interest_visibility_refusal_delivered(&self, node_id: &ClusterNodeName) -> bool {
        self.inner
            .refused_interest_visibility
            .get(node_id)
            .is_some_and(|refusal| refusal.is_delivered())
    }

    /// Takes the refusal armed on `node_id`, recording its delivery, when one is still armed.
    pub(crate) fn take_interest_visibility_refusal(&self, node_id: &ClusterNodeName) -> bool {
        let Some(mut refusal) = self.inner.refused_interest_visibility.get_mut(node_id) else {
            return false;
        };
        let InterestVisibilityRefusal::Armed = *refusal else {
            return false;
        };
        *refusal = InterestVisibilityRefusal::Delivered;
        true
    }
}
