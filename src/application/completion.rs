//! Completion of authoritative control-plane effects on every live node, or on the live nodes an
//! effect names.
//!
//! Layer: control plane.
//!
//! - **Owns.** The barriers for a fixed revision over every live node or over named live nodes:
//!   authoritative visibility, runtime preparation and readiness, and HTTPS listener installation.
//! - **Depends on.** Consensus for the committed revision, cluster availability, and the
//!   incarnation-bound application progress each node records, reports and answers queries for.
//! - **Must not know.** Session transports, Plexus syntax, data-plane tasks, or payloads.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeSet;

use error_stack::Report;
use futures_util::{StreamExt as _, stream::FuturesUnordered};
use meticulous::ResultExt as _;
use nervix_consensus::{LeaderTenure, Observer};
use nervix_interconnect::{
    ApplicationCompletionPeersRequest, ApplicationCompletionPeersResponse, ApplicationProgress,
    ApplicationProgressPhase, ApplicationRevisionRequest, HandlerRegistrationError,
    HttpsListenerInstallation, HttpsListenerInstallationRequest, Transport,
};
use nervix_models::{ClusterNodeIdentity, ClusterNodeName};
use nervix_primitives::{sync::Arc, task::AbortOnDropHandle};
use nervix_recovery::Reported as _;
use thiserror::Error;

use super::{
    scheduling::RUNTIME_REVISION_READINESS_PROPAGATION_BOUND, session_service::SessionServiceImpl,
    tls::HttpsListenerCertificates,
};
use crate::cluster::ClusterHandle;

mod progress;

#[cfg(test)]
pub(in crate::application) use progress::record_authoritative_progress;
pub(in crate::application) use progress::{
    ApplicationProgressState, register_application_progress_report_handler,
    spawn_application_progress_reporting, spawn_authoritative_progress_recording,
};

const APPLICATION_REVISION_PROBE_RETRY: std::time::Duration = std::time::Duration::from_millis(250);

/// Registers the handler that answers this incarnation's progress record to a revision query.
pub(in crate::application) fn register_application_revision_handler(
    progress: ApplicationProgressState,
    interconnect: &Transport,
    #[cfg(feature = "testing")] fault_injection: crate::FaultInjection,
) -> Result<(), Report<HandlerRegistrationError>> {
    interconnect.register_handler::<ApplicationRevisionRequest, _, _>(move |_context, _request| {
        let progress = progress.clone();
        #[cfg(feature = "testing")]
        let fault_injection = fault_injection.clone();
        async move {
            let response = progress.local();
            #[cfg(feature = "testing")]
            fault_injection
                .pause_application_revision_response_if_armed(response.identity.node_id())
                .await;
            response
        }
    })
}

/// Registers the handler that answers a follower's barrier with this leader's participants and the
/// progress it holds for them.
pub(in crate::application) fn register_completion_peers_handler(
    cluster: Arc<ClusterHandle>,
    consensus: Observer,
    progress: ApplicationProgressState,
    interconnect: &Transport,
) -> Result<(), Report<HandlerRegistrationError>> {
    interconnect.register_handler::<ApplicationCompletionPeersRequest, _, _>(
        move |_context, _request| {
            let cluster = cluster.clone();
            let consensus = consensus.clone();
            let progress = progress.clone();
            async move {
                let tenure = consensus.current_leader_tenure()?;
                let leader = progress.identity();
                if tenure.leader_id() != leader.node_id() {
                    return None;
                }
                let mut peers = cluster.availability_state().await.live_identities();
                peers.insert(leader.clone());
                if consensus.current_leader_tenure().as_ref() != Some(&tenure) {
                    return None;
                }
                let held = progress.progress_of(&peers);
                Some(ApplicationCompletionPeersResponse {
                    leader,
                    term: tenure.term(),
                    peers: peers.into_iter().collect(),
                    progress: held,
                })
            }
        },
    )
}

/// The participants of one barrier wake and the progress the leader held for them.
#[derive(Debug, PartialEq, Eq)]
struct CompletionParticipants {
    peers: BTreeSet<ClusterNodeIdentity>,
    /// The leader's own record and the latest report each other peer sent the leader.
    progress: Vec<ApplicationProgress>,
}

/// One leader observation: the tenure it was made in and the participants it named.
struct CompletionView {
    tenure: LeaderTenure,
    participants: CompletionParticipants,
}

/// Accepts a leader's answer only from the leader and term the barrier observed, with the leader's
/// own incarnation among its peers, and adds the local incarnation. Progress for an incarnation the
/// answer does not name is dropped.
fn accepted_completion_participants(
    response: ApplicationCompletionPeersResponse,
    expected_leader: &ClusterNodeName,
    expected_term: u64,
    local_identity: &ClusterNodeIdentity,
) -> Option<CompletionParticipants> {
    if response.leader.node_id() != expected_leader || response.term != expected_term {
        return None;
    }
    let mut peers = response.peers.into_iter().collect::<BTreeSet<_>>();
    if !peers.contains(&response.leader) {
        return None;
    }
    peers.insert(local_identity.clone());
    let mut progress = Vec::new();
    for held in response.progress {
        if peers.contains(&held.identity) {
            progress.push(held);
        }
    }
    Some(CompletionParticipants { peers, progress })
}

/// All completion participants come from one leader health observation. A follower cannot retire
/// a peer merely because its own interconnect is impaired, or keep waiting for a peer the leader
/// has already removed from the active schedule. The local incarnation always participates. The
/// leader also answers the progress it holds for the participants, so a follower's barrier sees
/// the reports the other followers sent the leader.
async fn completion_view(
    cluster: &ClusterHandle,
    consensus: &Observer,
    interconnect: &Transport,
    progress: &ApplicationProgressState,
    local_identity: &ClusterNodeIdentity,
) -> Option<CompletionView> {
    let tenure = consensus.current_leader_tenure()?;
    if tenure.leader_id() == local_identity.node_id() {
        let mut peers = cluster.availability_state().await.live_identities();
        peers.insert(local_identity.clone());
        if consensus.current_leader_tenure().as_ref() != Some(&tenure) {
            return None;
        }
        let held = progress.progress_of(&peers);
        return Some(CompletionView {
            tenure,
            participants: CompletionParticipants {
                peers,
                progress: held,
            },
        });
    }

    let response = match interconnect
        .request(tenure.leader_id(), ApplicationCompletionPeersRequest)
        .await
    {
        Ok(Some(response)) => response,
        Ok(None) => return None,
        Err(error) => {
            Err::<(), _>(error).reported("observe completion peers");
            return None;
        }
    };
    if consensus.current_leader_tenure().as_ref() != Some(&tenure) {
        return None;
    }
    let participants = accepted_completion_participants(
        response,
        tenure.leader_id(),
        tenure.term(),
        local_identity,
    )?;
    Some(CompletionView {
        tenure,
        participants,
    })
}

#[derive(Clone, Copy)]
pub(in crate::application) enum ApplicationRevisionPhase {
    Authoritative,
    RuntimePrepared,
    RuntimeReady,
}

impl ApplicationRevisionPhase {
    /// The phase of a progress record this barrier phase reads.
    const fn progress_phase(self) -> ApplicationProgressPhase {
        match self {
            Self::Authoritative => ApplicationProgressPhase::Authoritative,
            Self::RuntimePrepared => ApplicationProgressPhase::RuntimePrepared,
            Self::RuntimeReady => ApplicationProgressPhase::RuntimeReady,
        }
    }

    /// Whether `progress` reached `revision`, or a later revision, in this phase. A phase the
    /// incarnation has not reached completes no revision.
    fn is_complete(self, progress: &ApplicationProgress, revision: u64) -> bool {
        let Some(reached) = progress.reached(self.progress_phase()) else {
            return false;
        };
        reached >= revision
    }
}

/// The live process incarnations a revision barrier waits for, of those the leader's effective
/// availability view requires.
#[derive(Clone, Copy)]
pub(in crate::application) enum ApplicationRevisionParticipants<'a> {
    /// Every live incarnation, the local one included.
    EveryLiveNode,
    /// The live incarnations of these nodes alone. The local incarnation takes part only when its
    /// node is named.
    LiveAmong(&'a BTreeSet<ClusterNodeName>),
}

impl ApplicationRevisionParticipants<'_> {
    /// Keeps, of the incarnations the leader's view requires, those this barrier waits for.
    fn retain(self, expected_nodes: &mut BTreeSet<ClusterNodeIdentity>) {
        if let Self::LiveAmong(nodes) = self {
            expected_nodes.retain(|identity| nodes.contains(identity.node_id()));
        }
    }
}

pub(in crate::application) struct ApplicationRevisionTimeout {
    pub(in crate::application) pending_nodes: Vec<ClusterNodeName>,
}

async fn probe_application_revisions(
    interconnect: Transport,
    pending_nodes: BTreeSet<ClusterNodeIdentity>,
    local_identity: ClusterNodeIdentity,
    revision: u64,
    phase: ApplicationRevisionPhase,
) -> BTreeSet<ClusterNodeIdentity> {
    let mut probes = FuturesUnordered::new();
    for expected_identity in pending_nodes {
        if expected_identity == local_identity {
            continue;
        }
        let interconnect = interconnect.clone();
        probes.push(async move {
            let response = match interconnect
                .request(expected_identity.node_id(), ApplicationRevisionRequest)
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    Err::<(), _>(error).reported("observe application revision");
                    return None;
                }
            };
            if response.identity != expected_identity || !phase.is_complete(&response, revision) {
                return None;
            }
            Some(expected_identity)
        });
    }

    let mut completed_nodes = BTreeSet::new();
    while let Some(completed_node) = probes.next().await {
        nervix_primitives::task::consume_budget().await;
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
                Ok(_) => HttpsListenerInstallation::Pending,
                Err(error) => {
                    Err::<(), _>(error).reported("observe HTTPS listener installation");
                    HttpsListenerInstallation::Pending
                }
            };
            ProbedHttpsListener {
                identity: expected_identity,
                installation,
            }
        });
    }

    let mut probed = Vec::new();
    while let Some(listener) = probes.next().await {
        nervix_primitives::task::consume_budget().await;
        probed.push(listener);
    }
    probed
}

/// What a revision barrier on one node reads: its cluster view, consensus, transport and the
/// application progress it records and holds.
#[derive(Clone, Copy)]
pub(in crate::application) struct BarrierNode<'a> {
    pub(in crate::application) cluster: &'a ClusterHandle,
    pub(in crate::application) consensus: &'a Observer,
    pub(in crate::application) interconnect: &'a Transport,
    pub(in crate::application) progress: &'a ApplicationProgressState,
}

/// Waits until every required live incarnation reached `revision` in `phase`, or `deadline`.
///
/// A required incarnation completes through its own record on this node, through the progress the
/// leader holds for it, or through a direct query. The barrier registers for progress changes
/// before it reads the progress, so a report or an advance that arrives in between wakes it.
pub(in crate::application) async fn wait_for_application_revision(
    barrier_node: BarrierNode<'_>,
    revision: u64,
    phase: ApplicationRevisionPhase,
    participants: ApplicationRevisionParticipants<'_>,
    deadline: nervix_primitives::time::Instant,
) -> Result<(), ApplicationRevisionTimeout> {
    let BarrierNode {
        cluster,
        consensus,
        interconnect,
        progress,
    } = barrier_node;
    let local_identity = progress.identity();
    let mut observer = progress.observer();
    let mut directly_completed_nodes = BTreeSet::new();
    let mut deadline_elapsed = false;
    let mut pending_probes = None;

    loop {
        nervix_primitives::task::consume_budget().await;
        let mut cluster_state = cluster.subscribe_state_changes().await;
        let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
        tokio::pin!(cluster_change);
        let local_progress = observer.observe();
        let Some(view) =
            completion_view(cluster, consensus, interconnect, progress, &local_identity).await
        else {
            if deadline_elapsed || nervix_primitives::time::Instant::now() >= deadline {
                let pending_node = match consensus.current_leader_tenure() {
                    Some(tenure) => tenure.leader_id().clone(),
                    None => local_identity.node_id().clone(),
                };
                return Err(ApplicationRevisionTimeout {
                    pending_nodes: vec![pending_node],
                });
            }
            nervix_primitives::select! {
                biased;
                _ = nervix_primitives::time::sleep_until(deadline) => deadline_elapsed = true,
                _ = &mut cluster_change => {},
                () = observer.changed() => {},
                _ = nervix_primitives::time::sleep(APPLICATION_REVISION_PROBE_RETRY) => {},
            }
            continue;
        };
        let CompletionView {
            tenure,
            participants:
                CompletionParticipants {
                    peers: mut expected_nodes,
                    progress: held_progress,
                },
        } = view;
        participants.retain(&mut expected_nodes);
        directly_completed_nodes.retain(|identity| expected_nodes.contains(identity));

        let mut completed_nodes = BTreeSet::new();
        if phase.is_complete(&local_progress, revision) {
            completed_nodes.insert(local_identity.clone());
        }
        for held in &held_progress {
            if held.identity != local_identity && phase.is_complete(held, revision) {
                completed_nodes.insert(held.identity.clone());
            }
        }
        completed_nodes.extend(directly_completed_nodes.iter().cloned());
        let pending_nodes = expected_nodes
            .difference(&completed_nodes)
            .cloned()
            .collect::<BTreeSet<_>>();
        if pending_nodes.is_empty() {
            if consensus.current_leader_tenure().as_ref() == Some(&tenure) {
                return Ok(());
            }
            continue;
        }
        if deadline_elapsed {
            return Err(ApplicationRevisionTimeout {
                pending_nodes: pending_nodes
                    .into_iter()
                    .map(|identity| identity.node_id().clone())
                    .collect(),
            });
        }

        // Poll the batch independently of leader-view requests: an unpolled probe may already
        // own the executor permit a view request needs. The barrier owns and cancels the batch.
        let probes = pending_probes.get_or_insert_with(|| {
            AbortOnDropHandle::new(nervix_primitives::task::spawn(probe_application_revisions(
                interconnect.clone(),
                pending_nodes,
                local_identity.clone(),
                revision,
                phase,
            )))
        });
        let completed_before_probe = directly_completed_nodes.len();
        // A follower learns the other followers' reports only from the leader's view, so it reads
        // that view again on the retry cadence while its queries are outstanding.
        nervix_primitives::select! {
            biased;
            _ = nervix_primitives::time::sleep_until(deadline) => {
                deadline_elapsed = true;
            }
            observed_nodes = probes => {
                pending_probes = None;
                let observed_nodes = match observed_nodes {
                    Ok(nodes) => nodes,
                    Err(failure) => {
                        let panic = failure.try_into_panic().assured(
                            "the barrier owns the probe task's only abort handle, so nothing \
                             cancelled it while the barrier awaited it",
                        );
                        std::panic::resume_unwind(panic)
                    }
                };
                directly_completed_nodes.extend(observed_nodes);
                if directly_completed_nodes.len() == completed_before_probe {
                    nervix_primitives::select! {
                        biased;
                        _ = nervix_primitives::time::sleep_until(deadline) => {
                            deadline_elapsed = true;
                        }
                        _ = &mut cluster_change => {}
                        () = observer.changed() => {}
                        _ = nervix_primitives::time::sleep(APPLICATION_REVISION_PROBE_RETRY) => {}
                    }
                }
            }
            _ = &mut cluster_change => {}
            () = observer.changed() => {}
            _ = nervix_primitives::time::sleep(APPLICATION_REVISION_PROBE_RETRY) => {}
        }
    }
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
        node_unavailability_timeout: std::time::Duration,
        propagation_bound: std::time::Duration,
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
    /// When a completion barrier started now stops waiting: a node that cannot answer leaves the
    /// live set within the node-unavailability timeout, and a live node answers within the
    /// propagation bound.
    fn completion_deadline(
        &self,
    ) -> Result<nervix_primitives::time::Instant, Report<CompletionError>> {
        let node_unavailability_timeout = self.inner.cluster.node_unavailability_timeout();
        let overflow = CompletionError::DeadlineOverflow {
            node_unavailability_timeout,
            propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
        };
        let Some(wait_budget) =
            node_unavailability_timeout.checked_add(RUNTIME_REVISION_READINESS_PROPAGATION_BOUND)
        else {
            return Err(Report::new(overflow));
        };
        let Some(deadline) = nervix_primitives::time::Instant::now().checked_add(wait_budget)
        else {
            return Err(Report::new(overflow));
        };
        Ok(deadline)
    }

    pub(in crate::application) async fn wait_for_authoritative_visibility(
        &self,
    ) -> Result<(), Report<CompletionError>> {
        let revision = self.inner.consensus.current_revision().await;
        self.wait_for_authoritative_revision(revision).await
    }

    /// What this node's revision barriers read.
    pub(in crate::application) fn barrier_node(&self) -> BarrierNode<'_> {
        BarrierNode {
            cluster: &self.inner.cluster,
            consensus: &self.inner.consensus,
            interconnect: &self.inner.interconnect,
            progress: &self.inner.application_progress,
        }
    }

    /// Waits until every live incarnation applied `revision`, a revision this node has applied.
    pub(in crate::application) async fn wait_for_authoritative_revision(
        &self,
        revision: u64,
    ) -> Result<(), Report<CompletionError>> {
        let deadline = self.completion_deadline()?;

        wait_for_application_revision(
            self.barrier_node(),
            revision,
            ApplicationRevisionPhase::Authoritative,
            ApplicationRevisionParticipants::EveryLiveNode,
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
        let deadline = self.completion_deadline()?;
        let local_identity = self.inner.application_progress.identity();

        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(CompletionView {
                tenure,
                participants:
                    CompletionParticipants {
                        peers: expected_nodes,
                        ..
                    },
            }) = completion_view(
                &self.inner.cluster,
                &self.inner.consensus,
                &self.inner.interconnect,
                &self.inner.application_progress,
                &local_identity,
            )
            .await
            else {
                if nervix_primitives::time::Instant::now() >= deadline {
                    let pending_node = match self.inner.consensus.current_leader_tenure() {
                        Some(tenure) => tenure.leader_id().clone(),
                        None => local_identity.node_id().clone(),
                    };
                    return Err(Report::new(CompletionError::HttpsListenerPending {
                        revision,
                        pending_nodes: vec![pending_node],
                    }));
                }
                nervix_primitives::time::sleep(APPLICATION_REVISION_PROBE_RETRY).await;
                continue;
            };
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
                if self.inner.consensus.current_leader_tenure().as_ref() == Some(&tenure) {
                    return Ok(());
                }
                continue;
            }
            if nervix_primitives::time::Instant::now() >= deadline {
                pending_nodes.sort();
                return Err(Report::new(CompletionError::HttpsListenerPending {
                    revision,
                    pending_nodes,
                }));
            }
            nervix_primitives::select! {
                biased;
                _ = nervix_primitives::time::sleep_until(deadline) => {}
                _ = nervix_primitives::time::sleep(APPLICATION_REVISION_PROBE_RETRY) => {}
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
        let Some(deadline) = nervix_primitives::time::Instant::now().checked_add(wait_budget)
        else {
            return Err(Report::new(
                crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                    node_unavailability_timeout,
                    readiness_propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
                },
            ));
        };

        wait_for_application_revision(
            self.barrier_node(),
            revision,
            ApplicationRevisionPhase::RuntimeReady,
            ApplicationRevisionParticipants::EveryLiveNode,
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

#[cfg(feature = "testing")]
impl SessionServiceImpl {
    /// Answers the scenario's application progress queries from this node. A query for another
    /// node goes over the interconnect, as this node's revision barriers query it; a query for this
    /// node answers what its revision-query handler answers.
    pub(in crate::application) fn register_application_progress_test_queries(
        &self,
        fault_injection: &crate::fault_injection::FaultInjection,
        shutdown: nervix_primitives::sync::CancellationToken,
    ) {
        use nervix_recovery::NoReceiver as _;

        let local_node = self.inner.consensus.local_node_id().clone();
        let mut queries = fault_injection.register_application_progress_queries(local_node.clone());
        let service = self.clone();
        self.inner.service_tasks.spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let query = nervix_primitives::select! {
                    _ = shutdown.cancelled() => break,
                    query = queries.recv() => query,
                };
                let Some(query) = query else {
                    break;
                };
                let answer = service
                    .answer_application_progress_test_query(&local_node, query.target)
                    .await;
                query
                    .response
                    .send(answer)
                    .means_peer_left("application progress test query");
            }
        });
    }

    async fn answer_application_progress_test_query(
        &self,
        local_node: &ClusterNodeName,
        target: ClusterNodeName,
    ) -> error_stack::Result<
        ApplicationProgress,
        crate::fault_injection::ApplicationProgressQueryError,
    > {
        use error_stack::ResultExt as _;

        if &target == local_node {
            return Ok(self.inner.application_progress.local());
        }
        let failed = crate::fault_injection::ApplicationProgressQueryError::Query {
            querier: local_node.clone(),
            target: target.clone(),
        };
        self.inner
            .interconnect
            .request(&target, ApplicationRevisionRequest)
            .await
            .change_context(failed)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use meticulous::ResultExt as _;
    use nervix_interconnect::{
        ApplicationCompletionPeersResponse, ApplicationProgress, ApplicationProgressPhase,
    };
    use nervix_models::{
        ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, ClusterSchedule,
    };

    use super::{
        super::test_fixtures::{TestService, build_test_service},
        ApplicationRevisionParticipants, ApplicationRevisionPhase, CompletionParticipants,
        accepted_completion_participants, wait_for_application_revision,
    };

    #[test]
    fn completion_peers_require_the_current_leader_and_include_the_local_incarnation() {
        let leader = ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-1").assured("the test node name is valid"),
            ClusterNodeIncarnation::new(7),
        );
        let local = ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-2").assured("the test node name is valid"),
            ClusterNodeIncarnation::new(9),
        );
        let retired = ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-3").assured("the test node name is valid"),
            ClusterNodeIncarnation::new(4),
        );
        let leader_progress = ApplicationProgress {
            runtime_prepared: Some(6),
            ..ApplicationProgress::new(leader.clone())
        };
        let response = ApplicationCompletionPeersResponse {
            leader: leader.clone(),
            term: 12,
            peers: vec![leader.clone()],
            progress: vec![
                leader_progress.clone(),
                ApplicationProgress::new(retired.clone()),
            ],
        };
        assert_eq!(
            accepted_completion_participants(response.clone(), leader.node_id(), 12, &local),
            Some(CompletionParticipants {
                peers: BTreeSet::from([leader.clone(), local.clone()]),
                progress: vec![leader_progress],
            }),
            "the leader's held progress for an incarnation it does not name is dropped",
        );
        assert_eq!(
            accepted_completion_participants(response.clone(), leader.node_id(), 13, &local),
            None,
            "another Raft term cannot choose this barrier's participants",
        );
        assert_eq!(
            accepted_completion_participants(response.clone(), local.node_id(), 12, &local),
            None,
            "another Raft leader cannot choose this barrier's participants",
        );
        assert_eq!(
            accepted_completion_participants(
                ApplicationCompletionPeersResponse {
                    peers: Vec::new(),
                    ..response
                },
                leader.node_id(),
                12,
                &local,
            ),
            None,
            "the leader must include its own current incarnation",
        );
    }

    #[nervix_primitives::test]
    async fn a_barrier_among_named_nodes_waits_for_them_and_for_no_other_live_node() {
        let bound = Duration::from_secs(30);
        let TestService { service, path, .. } =
            nervix_primitives::time::timeout(bound, build_test_service(false))
                .await
                .assured("the revision barrier fixture starts within its test deadline");
        let barrier_node = service.barrier_node();
        let local = service.inner.application_progress.identity();
        let named = BTreeSet::from([local.node_id().clone()]);
        let elsewhere = BTreeSet::from([
            ClusterNodeName::parse("node-elsewhere").assured("the test node name is valid")
        ]);
        // The only live node has prepared no revision this far ahead, and every barrier below is
        // already past its deadline, so a barrier that waits for that node ends pending on it.
        let revision = 1_000;
        let elapsed = nervix_primitives::time::Instant::now();

        let unnamed = wait_for_application_revision(
            barrier_node,
            revision,
            ApplicationRevisionPhase::RuntimePrepared,
            ApplicationRevisionParticipants::LiveAmong(&elsewhere),
            elapsed,
        )
        .await;
        assert!(
            unnamed.is_ok(),
            "a live node the barrier does not name cannot hold it"
        );

        let Err(named_timeout) = wait_for_application_revision(
            barrier_node,
            revision,
            ApplicationRevisionPhase::RuntimePrepared,
            ApplicationRevisionParticipants::LiveAmong(&named),
            elapsed,
        )
        .await
        else {
            panic!("a named live node that has not prepared the revision must hold the barrier");
        };
        assert_eq!(named_timeout.pending_nodes, vec![local.node_id().clone()]);

        let Err(every_timeout) = wait_for_application_revision(
            barrier_node,
            revision,
            ApplicationRevisionPhase::RuntimePrepared,
            ApplicationRevisionParticipants::EveryLiveNode,
            elapsed,
        )
        .await
        else {
            panic!("a barrier over every live node must wait for the local node");
        };
        assert_eq!(every_timeout.pending_nodes, vec![local.node_id().clone()]);

        service
            .inner
            .application_progress
            .advance(ApplicationProgressPhase::RuntimePrepared, revision);
        let prepared = wait_for_application_revision(
            barrier_node,
            revision,
            ApplicationRevisionPhase::RuntimePrepared,
            ApplicationRevisionParticipants::LiveAmong(&named),
            elapsed,
        )
        .await;
        assert!(
            prepared.is_ok(),
            "the named node prepared the revision, so the barrier completes"
        );

        drop(service);
        std::fs::remove_dir_all(&path).expect("the test database directory is removed");
    }

    #[nervix_primitives::test]
    async fn the_https_listener_barrier_waits_until_the_listener_installs_the_revision() {
        let bound = Duration::from_secs(30);
        let TestService { service, path, .. } =
            nervix_primitives::time::timeout(bound, build_test_service(false))
                .await
                .assured("the listener barrier fixture starts within its test deadline");
        let certificates = service.inner.https_certificates.clone();
        let mut barrier = Box::pin(service.wait_for_https_listener_installation(4));

        assert!(
            futures_util::poll!(barrier.as_mut()).is_pending(),
            "no listener installed revision 4 yet"
        );
        nervix_primitives::time::timeout(
            bound,
            certificates.install(4, &ClusterSchedule::default()),
        )
        .await
        .assured("the listener installs within its test deadline")
        .expect("a runtime state without TLS VHOSTs installs");
        nervix_primitives::time::timeout(bound, barrier)
            .await
            .assured("the listener barrier completes within its test deadline")
            .expect("the barrier completes once the listener installed revision 4");

        drop(service);
        std::fs::remove_dir_all(&path).expect("the test database directory is removed");
    }
}
