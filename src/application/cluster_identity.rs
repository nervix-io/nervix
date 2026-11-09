//! Bootstrap orchestration for the cluster's permanent signing identity.
//!
//! Layer: control plane.
//! - **Owns.** Proposing an identity when the local leader observes an uninitialized cluster.
//! - **Depends on.** Consensus identity operations and the domain-clock owner's UTC sampling.
//! - **Must not know.** Private keys, their encoding, licensing policy or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns identity bootstrap orchestration")
)]

use nervix_consensus::{ConsensusError, Proposer};

#[cfg(feature = "testing")]
use crate::fault_injection::FaultInjection;

pub(super) async fn seed(
    proposer: &Proposer,
    #[cfg(feature = "testing")] fault: &FaultInjection,
) -> error_stack::Result<(), ConsensusError> {
    if proposer.cluster_identity().is_some()
        || proposer.current_leader().await.as_ref() != Some(proposer.local_node_id())
    {
        return Ok(());
    }
    let created_at = super::domain_clock::current_timestamp();
    #[cfg(feature = "testing")]
    if fault.competing_cluster_identity_proposals_requested(proposer.local_node_id()) {
        let (first, second) = futures_util::future::join(
            proposer.install_cluster_identity(created_at),
            proposer.install_cluster_identity(created_at),
        )
        .await;
        first?;
        second?;
        fault.complete_competing_cluster_identity_proposals(proposer.local_node_id());
        return Ok(());
    }
    proposer.install_cluster_identity(created_at).await
}
