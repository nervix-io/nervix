//! A link that loses the gate engagement requests an ownership-handoff coordinator sends one node,
//! for public handoff scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The coordinator-to-node links whose ownership-handoff gate engagement requests are
//!   lost, and how many requests each link lost.
//! - **Depends on.** Vocabulary identities.
//! - **Must not know.** Which nodes a gate holds, drain planning or handoff policy.

use meticulous::OptionExt as _;
use nervix_models::ClusterNodeName;

use super::FaultInjection;

/// The direction a lost engagement request travels: from the node coordinating a handoff to a
/// node it asks to hold the handoff's gate.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) struct HandoffGateLink {
    coordinator: ClusterNodeName,
    participant: ClusterNodeName,
}

impl FaultInjection {
    /// Loses every ownership-handoff gate engagement request `coordinator` sends `participant` from
    /// now on, as a link that drops them does: none arrives, and each attempt waits out its reply
    /// timeout.
    pub fn lose_handoff_gate_engagements(
        &self,
        coordinator: ClusterNodeName,
        participant: ClusterNodeName,
    ) {
        let link = HandoffGateLink {
            coordinator,
            participant,
        };
        self.inner.lost_handoff_gate_engagements.insert(link, 0);
    }

    /// How many ownership-handoff gate engagement requests the link from `coordinator` to
    /// `participant` lost.
    pub fn lost_handoff_gate_engagements(
        &self,
        coordinator: &ClusterNodeName,
        participant: &ClusterNodeName,
    ) -> usize {
        let link = HandoffGateLink {
            coordinator: coordinator.clone(),
            participant: participant.clone(),
        };
        let Some(lost) = self.inner.lost_handoff_gate_engagements.get(&link) else {
            return 0;
        };
        *lost
    }

    /// Whether the link from `coordinator` to `participant` loses the engagement request about to
    /// be sent over it. A lost request is counted.
    pub(crate) fn loses_handoff_gate_engagement(
        &self,
        coordinator: &ClusterNodeName,
        participant: &ClusterNodeName,
    ) -> bool {
        let link = HandoffGateLink {
            coordinator: coordinator.clone(),
            participant: participant.clone(),
        };
        let Some(mut lost) = self.inner.lost_handoff_gate_engagements.get_mut(&link) else {
            return false;
        };
        *lost = lost.checked_add(1).assured(
            "each lost request waits out a reply timeout within one handoff's preparation \
             deadline, so a scenario counts far fewer than usize::MAX of them",
        );
        true
    }
}
