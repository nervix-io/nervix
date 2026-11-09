//! A bootstrap test arrangement that submits two independently generated identity candidates.
//!
//! Layer: test harness.
//! - **Owns.** Arming competing bootstrap proposals and observing their completion.
//! - **Depends on.** The fault harness's node-scoped registry and typed node names.
//! - **Must not know.** Private keys, candidate values, consensus storage or winning order.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the competing-bootstrap fixture")
)]

use nervix_models::ClusterNodeName;

use super::FaultInjection;

#[derive(Debug, Clone, Copy)]
pub(super) enum BootstrapProposals {
    Requested,
    Completed,
}

impl FaultInjection {
    pub fn request_competing_cluster_identity_proposals(&self, node: ClusterNodeName) {
        self.inner
            .consensus_probes
            .entry(node)
            .or_default()
            .cluster_identity_proposals = Some(BootstrapProposals::Requested);
    }

    pub(crate) fn competing_cluster_identity_proposals_requested(
        &self,
        node: &ClusterNodeName,
    ) -> bool {
        self.inner.consensus_probes.get(node).is_some_and(|state| {
            matches!(
                state.cluster_identity_proposals,
                Some(BootstrapProposals::Requested)
            )
        })
    }

    pub(crate) fn complete_competing_cluster_identity_proposals(&self, node: &ClusterNodeName) {
        self.inner
            .consensus_probes
            .entry(node.clone())
            .or_default()
            .cluster_identity_proposals = Some(BootstrapProposals::Completed);
    }

    pub fn competing_cluster_identity_proposals_completed(&self, node: &ClusterNodeName) -> bool {
        self.inner.consensus_probes.get(node).is_some_and(|state| {
            matches!(
                state.cluster_identity_proposals,
                Some(BootstrapProposals::Completed)
            )
        })
    }
}
