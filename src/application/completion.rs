//! Completion of authoritative control-plane effects on every live node.
//!
//! Layer: control plane.
//!
//! - **Owns.** The all-live-node barriers for a fixed revision: authoritative visibility, runtime
//!   preparation and readiness, and HTTPS listener installation.
//! - **Depends on.** Consensus for the committed revision, cluster availability, and authenticated
//!   incarnation-aware progress observations.
//! - **Must not know.** Session transports, NSPL syntax, runtime tasks, or data-plane payloads.

use std::collections::BTreeSet;

use error_stack::Report;
use futures_util::{StreamExt as _, stream::FuturesUnordered};
use nervix_consensus::Observer;
use nervix_interconnect::{
    ApplicationRevisionRequest, ApplicationRevisionResponse, HandlerRegistrationError,
    HttpsListenerInstallation, HttpsListenerInstallationRequest, Transport,
};
use nervix_models::{ClusterNodeIdentity, ClusterNodeName};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use triomphe::Arc;

use super::{
    scheduling::RUNTIME_REVISION_READINESS_PROPAGATION_BOUND, session_service::SessionServiceImpl,
    tls::HttpsListenerCertificates,
};
use crate::cluster::ClusterHandle;

const APPLICATION_REVISION_PROBE_RETRY: tokio::time::Duration =
    tokio::time::Duration::from_millis(250);

pub(in crate::application) fn register_application_revision_handler(
    cluster: Arc<ClusterHandle>,
    interconnect: &Transport,
) -> Result<(), Report<HandlerRegistrationError>> {
    interconnect.register_handler::<ApplicationRevisionRequest, _, _>(move |_context, _request| {
        let cluster = cluster.clone();
        async move { cluster.local_application_revision().await }
    })
}

#[derive(Clone, Copy)]
pub(in crate::application) enum ApplicationRevisionPhase {
    Authoritative,
    RuntimePrepared,
    RuntimeReady,
}

impl ApplicationRevisionPhase {
    fn is_complete(self, application: &ApplicationRevisionResponse, revision: u64) -> bool {
        let completed_revision = match self {
            Self::Authoritative => application.authoritative,
            Self::RuntimePrepared => application.runtime_prepared,
            Self::RuntimeReady => application.runtime_ready,
        };
        completed_revision >= revision
    }

    async fn gossiped_nodes(
        self,
        cluster: &ClusterHandle,
        revision: u64,
    ) -> BTreeSet<ClusterNodeIdentity> {
        match self {
            Self::Authoritative => cluster.nodes_at_authoritative_revision(revision).await,
            Self::RuntimePrepared => cluster.nodes_prepared_for_runtime_revision(revision).await,
            Self::RuntimeReady => cluster.nodes_ready_for_runtime_revision(revision).await,
        }
    }
}

pub(in crate::application) struct ApplicationRevisionTimeout {
    pub(in crate::application) pending_nodes: Vec<ClusterNodeName>,
}

async fn probe_application_revisions(
    interconnect: &Transport,
    pending_nodes: &BTreeSet<ClusterNodeIdentity>,
    local_identity: &ClusterNodeIdentity,
    revision: u64,
    phase: ApplicationRevisionPhase,
) -> BTreeSet<ClusterNodeIdentity> {
    let mut probes = FuturesUnordered::new();
    for expected_identity in pending_nodes {
        if expected_identity == local_identity {
            continue;
        }
        let expected_identity = expected_identity.clone();
        let interconnect = interconnect.clone();
        probes.push(async move {
            let response = match interconnect
                .request(expected_identity.node_id(), ApplicationRevisionRequest)
                .await
            {
                Ok(response) => response,
                Err(_) => return None,
            };
            if response.identity != expected_identity || !phase.is_complete(&response, revision) {
                return None;
            }
            Some(expected_identity)
        });
    }

    let mut completed_nodes = BTreeSet::new();
    while let Some(completed_node) = probes.next().await {
        tokio::task::consume_budget().await;
        if let Some(completed_node) = completed_node {
            completed_nodes.insert(completed_node);
        }
    }
    completed_nodes
}

/// What one live process incarnation answered about its HTTPS listener.
struct ProbedHttpsListener {
    identity: ClusterNodeIdentity,
    installation: HttpsListenerInstallation,
}

/// Asks every expected incarnation where its HTTPS listener stands against `revision`. The local
/// incarnation answers in process; a remote incarnation that cannot be reached, or that answers
/// under another identity, is still pending.
async fn probe_https_listeners(
    interconnect: &Transport,
    certificates: &HttpsListenerCertificates,
    expected_nodes: &BTreeSet<ClusterNodeIdentity>,
    local_identity: &ClusterNodeIdentity,
    revision: u64,
) -> Vec<ProbedHttpsListener> {
    let mut probes = FuturesUnordered::new();
    for expected_identity in expected_nodes {
        let expected_identity = expected_identity.clone();
        let is_local = &expected_identity == local_identity;
        let interconnect = interconnect.clone();
        let certificates = certificates.clone();
        probes.push(async move {
            if is_local {
                let installation = certificates.installation(revision).await;
                return ProbedHttpsListener {
                    identity: expected_identity,
                    installation,
                };
            }
            let response = interconnect
                .request(
                    expected_identity.node_id(),
                    HttpsListenerInstallationRequest { revision },
                )
                .await;
            let installation = match response {
                Ok(response) if response.identity == expected_identity => response.installation,
                Ok(_) | Err(_) => HttpsListenerInstallation::Pending,
            };
            ProbedHttpsListener {
                identity: expected_identity,
                installation,
            }
        });
    }

    let mut probed = Vec::new();
    while let Some(listener) = probes.next().await {
        tokio::task::consume_budget().await;
        probed.push(listener);
    }
    probed
}

pub(in crate::application) async fn wait_for_application_revision(
    cluster: &ClusterHandle,
    interconnect: &Transport,
    revision: u64,
    phase: ApplicationRevisionPhase,
    deadline: tokio::time::Instant,
) -> Result<(), ApplicationRevisionTimeout> {
    let local_identity = cluster.local_node_identity().await;
    let mut directly_completed_nodes = BTreeSet::new();
    let mut deadline_elapsed = false;

    loop {
        tokio::task::consume_budget().await;
        let mut cluster_state = cluster.subscribe_state_changes().await;
        let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
        tokio::pin!(cluster_change);
        let gossip = cluster.availability_state().await;
        let mut expected_nodes = gossip.live_identities();
        expected_nodes.insert(local_identity.clone());
        directly_completed_nodes.retain(|identity| expected_nodes.contains(identity));

        let mut completed_nodes = phase.gossiped_nodes(cluster, revision).await;
        completed_nodes.extend(directly_completed_nodes.iter().cloned());
        let pending_nodes = expected_nodes
            .difference(&completed_nodes)
            .cloned()
            .collect::<BTreeSet<_>>();
        if pending_nodes.is_empty() {
            return Ok(());
        }
        if deadline_elapsed {
            return Err(ApplicationRevisionTimeout {
                pending_nodes: pending_nodes
                    .into_iter()
                    .map(|identity| identity.node_id().clone())
                    .collect(),
            });
        }

        let probes = probe_application_revisions(
            interconnect,
            &pending_nodes,
            &local_identity,
            revision,
            phase,
        );
        tokio::pin!(probes);
        let completed_before_probe = directly_completed_nodes.len();
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                deadline_elapsed = true;
            }
            observed_nodes = &mut probes => {
                directly_completed_nodes.extend(observed_nodes);
                if directly_completed_nodes.len() == completed_before_probe {
                    tokio::select! {
                        biased;
                        _ = tokio::time::sleep_until(deadline) => {
                            deadline_elapsed = true;
                        }
                        _ = &mut cluster_change => {}
                        _ = tokio::time::sleep(APPLICATION_REVISION_PROBE_RETRY) => {}
                    }
                }
            }
        }
    }
}

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
        "cannot represent a completion deadline from node-unavailability timeout \
         {node_unavailability_timeout:?} and propagation bound {propagation_bound:?}"
    )]
    DeadlineOverflow {
        node_unavailability_timeout: tokio::time::Duration,
        propagation_bound: tokio::time::Duration,
    },
    /// The reason is the failing node's own description, because the installation ran inside that
    /// node's listener.
    #[error(
        "failed to install the HTTPS listener TLS configuration on node '{node}' for runtime \
         revision {revision}: {reason}"
    )]
    HttpsListenerInstallation {
        node: ClusterNodeName,
        revision: u64,
        reason: String,
    },
    #[error(
        "timed out waiting for the HTTPS listener TLS configuration of runtime revision \
         {revision} on nodes {pending_nodes:?}"
    )]
    HttpsListenerPending {
        revision: u64,
        pending_nodes: Vec<ClusterNodeName>,
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

        wait_for_application_revision(
            &self.inner.cluster,
            &self.inner.interconnect,
            revision,
            ApplicationRevisionPhase::Authoritative,
            deadline,
        )
        .await
        .map_err(|timeout| {
            Report::new(CompletionError::Visibility {
                revision,
                pending_nodes: timeout.pending_nodes,
            })
        })
    }

    /// Waits until the HTTPS listener of every live process incarnation installed the TLS VHOSTs
    /// of `revision` or of a later runtime revision. The first incarnation that reports a failed
    /// installation ends the wait with that failure.
    pub(in crate::application) async fn wait_for_https_listener_installation(
        &self,
        revision: u64,
    ) -> Result<(), Report<CompletionError>> {
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
        let local_identity = self.inner.cluster.local_node_identity().await;

        loop {
            tokio::task::consume_budget().await;
            let gossip = self.inner.cluster.availability_state().await;
            let mut expected_nodes = gossip.live_identities();
            expected_nodes.insert(local_identity.clone());
            let probed = probe_https_listeners(
                &self.inner.interconnect,
                &self.inner.https_certificates,
                &expected_nodes,
                &local_identity,
                revision,
            )
            .await;

            let mut pending_nodes = Vec::new();
            for listener in probed {
                match listener.installation {
                    HttpsListenerInstallation::Installed { .. } => {}
                    HttpsListenerInstallation::Failed {
                        revision: failed_revision,
                        reason,
                    } => {
                        return Err(Report::new(CompletionError::HttpsListenerInstallation {
                            node: listener.identity.node_id().clone(),
                            revision: failed_revision,
                            reason,
                        }));
                    }
                    HttpsListenerInstallation::Pending => {
                        pending_nodes.push(listener.identity.node_id().clone());
                    }
                }
            }
            if pending_nodes.is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                pending_nodes.sort();
                return Err(Report::new(CompletionError::HttpsListenerPending {
                    revision,
                    pending_nodes,
                }));
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {}
                _ = tokio::time::sleep(APPLICATION_REVISION_PROBE_RETRY) => {}
            }
        }
    }

    pub(in crate::application) async fn wait_for_runtime_revision(
        &self,
        revision: u64,
    ) -> Result<(), Report<crate::runtime::RuntimeError>> {
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

        wait_for_application_revision(
            &self.inner.cluster,
            &self.inner.interconnect,
            revision,
            ApplicationRevisionPhase::RuntimeReady,
            deadline,
        )
        .await
        .map_err(|timeout| {
            Report::new(crate::runtime::RuntimeError::RuntimeRevisionReadiness {
                revision,
                pending_nodes: timeout.pending_nodes,
            })
        })
    }
}
