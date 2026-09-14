//! Completion of authoritative control-plane effects on every live node.
//!
//! Layer: control plane.
//!
//! - **Owns.** The all-live-node visibility barrier for a fixed authoritative revision.
//! - **Depends on.** Consensus for the committed revision and cluster gossip for incarnation-aware
//!   application acknowledgements.
//! - **Must not know.** Session transports, NSPL syntax, runtime tasks, or data-plane payloads.

use error_stack::Report;
use nervix_consensus::Observer;
use nervix_models::{ClusterNodeIdentity, ClusterNodeName};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use triomphe::Arc;

use super::{
    scheduling::RUNTIME_REVISION_READINESS_PROPAGATION_BOUND, session_service::SessionServiceImpl,
};
use crate::cluster::ClusterHandle;

pub(in crate::application) fn spawn_authoritative_revision_reporting(
    cluster: Arc<ClusterHandle>,
    consensus: Observer,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let mut applied_revision = consensus.subscribe_applied();
    tokio::spawn(async move {
        loop {
            tokio::task::consume_budget().await;
            let revision = *applied_revision.borrow_and_update();
            cluster.set_local_authoritative_revision(revision).await;
            tokio::select! {
                _ = shutdown.cancelled() => break,
                changed = applied_revision.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }
    })
}

#[derive(Debug, Error)]
pub(in crate::application) enum CompletionError {
    #[error(
        "timed out waiting for authoritative revision {revision} to become visible on nodes \
         {pending_nodes:?}"
    )]
    Visibility {
        revision: u64,
        pending_nodes: Vec<ClusterNodeName>,
    },
    #[error(
        "cannot represent an authoritative visibility deadline from node-unavailability timeout \
         {node_unavailability_timeout:?} and propagation bound {propagation_bound:?}"
    )]
    DeadlineOverflow {
        node_unavailability_timeout: tokio::time::Duration,
        propagation_bound: tokio::time::Duration,
    },
}

impl SessionServiceImpl {
    pub(in crate::application) async fn wait_for_authoritative_visibility(
        &self,
    ) -> Result<(), Report<CompletionError>> {
        let revision = self.inner.consensus.current_revision().await;
        self.wait_for_authoritative_revision(revision).await
    }

    pub(in crate::application) async fn wait_for_authoritative_revision(
        &self,
        revision: u64,
    ) -> Result<(), Report<CompletionError>> {
        let mut cluster_state = self.inner.cluster.subscribe_state_changes().await;
        self.inner
            .cluster
            .set_local_authoritative_revision(revision)
            .await;

        let node_unavailability_timeout = self.inner.cluster.node_unavailability_timeout();
        let Some(wait_budget) =
            node_unavailability_timeout.checked_add(RUNTIME_REVISION_READINESS_PROPAGATION_BOUND)
        else {
            return Err(Report::new(CompletionError::DeadlineOverflow {
                node_unavailability_timeout,
                propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
            }));
        };
        let Some(deadline) = tokio::time::Instant::now().checked_add(wait_budget) else {
            return Err(Report::new(CompletionError::DeadlineOverflow {
                node_unavailability_timeout,
                propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
            }));
        };

        let local_identity = ClusterNodeIdentity::new(
            self.inner.consensus.local_node_id().clone(),
            self.inner.cluster.local_incarnation(),
        );
        let mut deadline_elapsed = false;
        loop {
            tokio::task::consume_budget().await;
            let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
            tokio::pin!(cluster_change);
            let gossip = self.inner.cluster.availability_state().await;
            let mut expected_nodes = gossip.live_identities();
            expected_nodes.insert(local_identity.clone());
            let visible_nodes = self
                .inner
                .cluster
                .nodes_at_authoritative_revision(revision)
                .await;
            let pending_nodes = expected_nodes
                .difference(&visible_nodes)
                .map(|identity| identity.node_id().clone())
                .collect::<Vec<_>>();
            if pending_nodes.is_empty() {
                return Ok(());
            }
            if deadline_elapsed {
                return Err(Report::new(CompletionError::Visibility {
                    revision,
                    pending_nodes,
                }));
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    deadline_elapsed = true;
                }
                _ = &mut cluster_change => {}
            }
        }
    }

    pub(in crate::application) async fn wait_for_runtime_revision(
        &self,
        revision: u64,
    ) -> Result<(), Report<crate::runtime::RuntimeError>> {
        let mut cluster_state = self.inner.cluster.subscribe_state_changes().await;
        let node_unavailability_timeout = self.inner.cluster.node_unavailability_timeout();
        let Some(wait_budget) =
            node_unavailability_timeout.checked_add(RUNTIME_REVISION_READINESS_PROPAGATION_BOUND)
        else {
            return Err(Report::new(
                crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                    node_unavailability_timeout,
                    readiness_propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
                },
            ));
        };
        let Some(deadline) = tokio::time::Instant::now().checked_add(wait_budget) else {
            return Err(Report::new(
                crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                    node_unavailability_timeout,
                    readiness_propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
                },
            ));
        };

        let local_identity = ClusterNodeIdentity::new(
            self.inner.consensus.local_node_id().clone(),
            self.inner.cluster.local_incarnation(),
        );
        let mut deadline_elapsed = false;
        loop {
            tokio::task::consume_budget().await;
            let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
            tokio::pin!(cluster_change);
            let gossip = self.inner.cluster.availability_state().await;
            let mut expected_nodes = gossip.live_identities();
            expected_nodes.insert(local_identity.clone());
            let ready_nodes = self
                .inner
                .cluster
                .nodes_ready_for_runtime_revision(revision)
                .await;
            let pending_nodes = expected_nodes
                .difference(&ready_nodes)
                .map(|identity| identity.node_id().clone())
                .collect::<Vec<_>>();
            if pending_nodes.is_empty() {
                return Ok(());
            }
            if deadline_elapsed {
                return Err(Report::new(
                    crate::runtime::RuntimeError::RuntimeRevisionReadiness {
                        revision,
                        pending_nodes,
                    },
                ));
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    deadline_elapsed = true;
                }
                _ = &mut cluster_change => {}
            }
        }
    }
}
