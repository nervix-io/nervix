//! Publication of this node's resolved, immutable capacity claim after membership admission.
//!
//! Layer: control plane.
//! - **Owns.** Retrying this run's announcement until Raft confirms its immutable claim.
//! - **Depends on.** The consensus proposer, local run identity and resolved vocabulary values.
//! - **Must not know.** Detection, scheduling scores or relay acknowledgement ownership.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns local capability announcement")
)]

use nervix_consensus::Proposer;
use nervix_models::{NodeCapabilities, NodeCapabilityRecord};

pub(super) async fn announce(
    consensus: &Proposer,
    cluster: &crate::cluster::ClusterHandle,
    capabilities: NodeCapabilities,
) -> bool {
    let identity = cluster.local_node_identity().await;
    let record = NodeCapabilityRecord {
        identity,
        capabilities,
    };
    if consensus
        .node_capability(record.identity.node_id())
        .as_ref()
        == Some(&record)
    {
        return true;
    }
    if !consensus
        .stored_membership_nodes()
        .contains_key(record.identity.node_id())
    {
        return false;
    }
    match consensus.announce_node_capabilities(record).await {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(error = %format_args!("{error:#}"), "node capability announcement will retry");
            false
        }
    }
}

/// Capacity publication progresses independently of a leader's domain reconciliation work.
pub(super) fn spawn(
    consensus: Proposer,
    cluster: nervix_primitives::sync::Arc<crate::cluster::ClusterHandle>,
    interconnect: nervix_interconnect::Transport,
    capabilities: NodeCapabilities,
    shutdown: nervix_primitives::sync::CancellationToken,
) -> nervix_primitives::task::JoinHandle<()> {
    nervix_primitives::task::spawn(async move {
        let mut announced = false;
        let mut cadence = nervix_primitives::time::interval(
            super::runtime_admission::RUNTIME_STATE_RETRY_INTERVAL,
        );
        loop {
            nervix_primitives::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = cadence.tick() => {},
            }
            interconnect.publish_peer_capabilities(&consensus.node_capabilities());
            if !announced {
                let Some(committed) = shutdown
                    .run_until_cancelled(announce(&consensus, &cluster, capabilities))
                    .await
                else {
                    return;
                };
                // A successful reply already confirms Raft's durable commit even while this
                // follower's applied view is catching up; this run's immutable claim is done.
                announced = committed;
            }
        }
    })
}
