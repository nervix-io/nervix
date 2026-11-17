//! Publication of this node's immutable record after membership admission: its resolved capacity
//! claim, the properties it detected at startup and the tags it was provisioned with.
//!
//! Layer: control plane.
//! - **Owns.** Retrying this run's announcement until Raft confirms its immutable record, and
//!   handing the committed capacity claims to the transport.
//! - **Depends on.** The consensus proposer, local run identity and resolved vocabulary values.
//! - **Must not know.** Detection, scheduling scores or relay acknowledgement ownership.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns local node record announcement")
)]

use nervix_consensus::Proposer;
use nervix_models::{NodeCapabilities, NodeCapabilityRecord, NodeProperties, NodeRecord, NodeTags};

/// What this run states about itself beside who it is: resolved once at startup and announced
/// unchanged for as long as the run lasts.
#[derive(Debug, Clone)]
pub(super) struct LocalNodeFacts {
    capabilities: NodeCapabilities,
    properties: NodeProperties,
    tags: NodeTags,
}

impl LocalNodeFacts {
    pub(super) fn new(
        capabilities: NodeCapabilities,
        properties: NodeProperties,
        tags: NodeTags,
    ) -> Self {
        Self {
            capabilities,
            properties,
            tags,
        }
    }
}

pub(super) async fn announce(
    consensus: &Proposer,
    cluster: &crate::cluster::ClusterHandle,
    facts: &LocalNodeFacts,
) -> bool {
    let identity = cluster.local_node_identity().await;
    let record = NodeRecord {
        claim: NodeCapabilityRecord {
            identity,
            capabilities: facts.capabilities,
        },
        properties: facts.properties.clone(),
        tags: facts.tags.clone(),
    };
    let committed = consensus.node_record(record.identity().node_id());
    if let Some(committed) = committed
        && committed.record == record
    {
        return true;
    }
    if !consensus
        .stored_membership_nodes()
        .contains_key(record.identity().node_id())
    {
        return false;
    }
    match consensus.announce_node_record(record).await {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(error = %format_args!("{error:#}"), "node record announcement will retry");
            false
        }
    }
}

/// Record publication progresses independently of a leader's domain reconciliation work.
pub(super) fn spawn(
    consensus: Proposer,
    cluster: nervix_primitives::sync::Arc<crate::cluster::ClusterHandle>,
    interconnect: nervix_interconnect::Transport,
    facts: LocalNodeFacts,
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
            interconnect.publish_peer_capabilities(&consensus.node_capability_claims());
            if !announced {
                let Some(committed) = shutdown
                    .run_until_cancelled(announce(&consensus, &cluster, &facts))
                    .await
                else {
                    return;
                };
                // A successful reply already confirms Raft's durable commit even while this
                // follower's applied view is catching up; this run's immutable record is done.
                announced = committed;
            }
        }
    })
}
