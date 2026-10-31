//! The authority that advances a domain's logical time, and the node that runs it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Choosing a clock authority per domain, running the clock task, and delivering its
//!   progress to every node.
//! - **Depends on.** Consensus for the recorded authority and typed interconnect progress requests.
//! - **Must not know.** What a domain does with the time it is given.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::{collections::BTreeSet, time::Duration};

use ahash::{HashMap, HashMapExt};
use futures_util::{StreamExt, stream::FuturesUnordered};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{ConsensusError, ConsensusRuntimeState};
use nervix_interconnect::DomainClockProgressRequest;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeName, DomainClockAdvancement, DomainClockAuthority,
    DomainClockPeriod, DomainClockProgress, DomainClockState, DomainName, DomainPace, DomainStatus,
    DomainTick, Timestamp,
};
use nervix_primitives::{
    sync::{CancellationToken, watch},
    time::{Instant, sleep},
};
use tracing::{debug, warn};

use super::{background_task::BackgroundTask, session_service::SessionServiceImpl};
use crate::{
    domain_clock_authority::DomainClockAuthorityCandidates, runtime::CommittedClockInstallation,
    task_shutdown::JoinShutdown,
};

const DOMAIN_CLOCK_PROGRESS_RETRY_BACKOFF: Duration = Duration::from_millis(200);
/// How long a leader waits before proposing again an authority reassignment that did not commit.
/// A newly elected leader refuses writes until its quorum acknowledges it, so the first proposal
/// it makes on taking over can fail while the next one commits.
const DOMAIN_CLOCK_AUTHORITY_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Whether the leader's latest reconciliation pass committed every authority its selection
/// requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DomainClockAuthorityReconciliation {
    /// Every committed authority matches the selection, or this node does not lead.
    Settled,
    /// A reassignment did not commit, so the leader proposes again after the retry interval.
    Owed,
}

impl crate::runtime::Runtime {
    fn report_domain_clock_failure(
        &self,
        domain: &DomainName,
        operation: &str,
        error: &error_stack::Report<nervix_models::DomainClockError>,
        log_message: &str,
    ) {
        self.report_error(format!(
            "domain clock {operation} for '{}' failed: {error:#}",
            domain.as_str(),
        ));
        warn!(domain = domain.as_str(), error = %format_args!("{error:#}"), "{log_message}");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DomainClockTaskSpec {
    clock: DomainClockState,
    period: DomainClockPeriod,
    generation: u64,
    authority_revision: nervix_models::DomainClockAuthorityRevision,
    authority: ClusterNodeIdentity,
}

impl DomainClockTaskSpec {
    /// The physical interval represented by one logical period, rounded up by the same clock
    /// arithmetic used for all logical deadlines.
    fn minimum_tick_wall_spacing(
        &self,
    ) -> error_stack::Result<Duration, nervix_models::DomainClockError> {
        self.clock
            .wall_duration_for_logical_delta(self.period.as_duration())
    }
}

pub(in crate::application) struct DomainClockTask {
    spec: DomainClockTaskSpec,
    pub(in crate::application) task: BackgroundTask,
}

/// One target's latest replaceable progress report and its physical delivery loop.
///
/// The watch channel retains one value regardless of tick rate or connection delay. A new report
/// replaces the pending value while the previous request is in flight, and the transport's
/// progress subquota bounds requests across every domain and peer on the node.
struct DomainClockProgressDelivery {
    latest: watch::Sender<Option<DomainClockProgressRequest>>,
    task: BackgroundTask,
}

impl DomainClockProgressDelivery {
    fn start(
        service: SessionServiceImpl,
        target: ClusterNodeIdentity,
        shutdown: &CancellationToken,
    ) -> Self {
        let (latest, receiver) = watch::channel(None);
        let token = shutdown.child_token();
        let task_token = token.clone();
        let handle = nervix_primitives::task::spawn(async move {
            Self::run(service, target, receiver, task_token).await;
        });
        Self {
            latest,
            task: BackgroundTask {
                cancel: token,
                handle,
            },
        }
    }

    fn publish(&self, request: DomainClockProgressRequest) {
        self.latest.send_replace(Some(request));
    }

    async fn stop(self) {
        self.task.stop().await;
    }

    async fn run(
        service: SessionServiceImpl,
        target: ClusterNodeIdentity,
        mut latest: watch::Receiver<Option<DomainClockProgressRequest>>,
        shutdown: CancellationToken,
    ) {
        let mut retry_pending = false;
        loop {
            nervix_primitives::task::consume_budget().await;
            if !retry_pending {
                let changed = nervix_primitives::select! {
                    _ = shutdown.cancelled() => return,
                    changed = latest.changed() => changed,
                };
                if changed.is_err() {
                    return;
                }
            }

            let Some(request) = latest.borrow_and_update().clone() else {
                retry_pending = false;
                continue;
            };
            let domain_id = request.domain_id.clone();
            let result = nervix_primitives::select! {
                _ = shutdown.cancelled() => return,
                result = service.inner.interconnect.request(target.node_id(), request) => result,
            };
            match result {
                Ok(()) => {
                    retry_pending = false;
                }
                Err(error) => {
                    debug!(
                        domain = domain_id.as_str(),
                        node = %target,
                        error = %format_args!("{error:#}"),
                        "failed to deliver domain clock progress"
                    );
                    nervix_primitives::select! {
                        _ = shutdown.cancelled() => return,
                        _ = sleep(DOMAIN_CLOCK_PROGRESS_RETRY_BACKOFF) => {}
                    }
                    retry_pending = true;
                }
            }
        }
    }
}

#[derive(Default)]
struct DomainClockProgressDeliveries {
    targets: HashMap<ClusterNodeIdentity, DomainClockProgressDelivery>,
}

impl DomainClockProgressDeliveries {
    async fn reconcile(
        &mut self,
        service: &SessionServiceImpl,
        spec: &DomainClockTaskSpec,
        clock_revision: u64,
        latest: Option<&DomainClockProgress>,
        shutdown: &CancellationToken,
        domain_id: &DomainName,
    ) {
        let gossip = service.inner.cluster.availability_state().await;
        // A peer advertises the revision once its domain clocks, authority fences included, match
        // it. It installs a reassigned fence ahead of its graph installation, so a peer whose graph
        // installation waits on a node that left still receives progress.
        let installed = service
            .inner
            .cluster
            .nodes_with_domain_clocks_installed(clock_revision)
            .await;
        let mut desired = gossip
            .live_identities()
            .intersection(&installed)
            .cloned()
            .collect::<BTreeSet<_>>();
        desired.remove(&spec.authority);

        let departed = self
            .targets
            .keys()
            .filter(|target| !desired.contains(*target))
            .cloned()
            .collect::<Vec<_>>();
        for target in departed {
            nervix_primitives::task::consume_budget().await;
            if let Some(delivery) = self.targets.remove(&target) {
                delivery.stop().await;
            }
        }

        for target in desired {
            nervix_primitives::task::consume_budget().await;
            if self.targets.contains_key(&target) {
                continue;
            }
            let delivery =
                DomainClockProgressDelivery::start(service.clone(), target.clone(), shutdown);
            if let Some(progress) = latest {
                delivery.publish(DomainClockProgressRequest {
                    domain_id: domain_id.clone(),
                    progress: progress.clone(),
                });
            }
            self.targets.insert(target, delivery);
        }
    }

    fn publish(&self, domain_id: &DomainName, progress: &DomainClockProgress) {
        for delivery in self.targets.values() {
            delivery.publish(DomainClockProgressRequest {
                domain_id: domain_id.clone(),
                progress: progress.clone(),
            });
        }
    }

    async fn stop_all(self) {
        for delivery in self.targets.into_values() {
            nervix_primitives::task::consume_budget().await;
            delivery.stop().await;
        }
    }
}

/// Producers whose authority was revoked while they finish an in-flight fenced delivery.
///
/// Installing the committed replacement cannot wait for transport or a test-held delivery from
/// the previous producer. Its immutable specification still carries the superseded fence, and a
/// successor for the same domain starts only after every retiring local task has finished.
#[derive(Default)]
pub(in crate::application) struct DomainClockRetirements {
    tasks: HashMap<DomainName, Vec<DomainClockTask>>,
}

impl DomainClockRetirements {
    pub(in crate::application) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn contains(&self, domain: &DomainName) -> bool {
        self.tasks.contains_key(domain)
    }

    fn retire(&mut self, domain: DomainName, task: DomainClockTask) {
        task.task.request_stop();
        self.tasks.entry(domain).or_default().push(task);
    }

    pub(in crate::application) async fn join_next(&mut self) {
        let mut completions = self
            .tasks
            .iter_mut()
            .flat_map(|(domain, tasks)| {
                tasks.iter_mut().enumerate().map(move |(index, task)| {
                    let domain = domain.clone();
                    async move {
                        (&mut task.task.handle)
                            .join_after_shutdown("retiring domain clock")
                            .await;
                        (domain, index)
                    }
                })
            })
            .collect::<FuturesUnordered<_>>();
        let completed = completions
            .next()
            .await
            .assured("join_next is called only while a clock task is retiring");
        drop(completions);
        let Some(tasks) = self.tasks.get_mut(&completed.0) else {
            return;
        };
        tasks.swap_remove(completed.1);
        if tasks.is_empty() {
            self.tasks.remove(&completed.0);
        }
    }

    pub(in crate::application) async fn stop_all(self) {
        for tasks in self.tasks.into_values() {
            nervix_primitives::task::consume_budget().await;
            for task in tasks {
                nervix_primitives::task::consume_budget().await;
                task.task.stop().await;
            }
        }
    }
}

pub(in crate::application) async fn reconcile_domain_clock_tasks(
    service: &SessionServiceImpl,
    shutdown: &CancellationToken,
    state: &ConsensusRuntimeState,
    tasks: &mut HashMap<DomainName, DomainClockTask>,
    retirements: &mut DomainClockRetirements,
) {
    let local_identity = service.inner.cluster.local_node_identity().await;

    let mut desired = HashMap::<DomainName, DomainClockTaskSpec>::new();
    for (domain_id, domain) in &state.domains {
        nervix_primitives::task::consume_budget().await;
        if matches!(domain.status, DomainStatus::Stopped)
            || matches!(domain.config.pace, DomainPace::Unpaced)
        {
            continue;
        }
        let Some(clock) = domain.clock.clone() else {
            continue;
        };
        let Some(authority) = state.domain_clock_authorities.get(domain_id) else {
            continue;
        };
        if !service.inner.runtime.has_domain_clock_authority(
            domain_id,
            domain.start_version,
            authority,
        ) {
            continue;
        }
        let Some(owner) = authority.owner() else {
            continue;
        };
        if owner != &local_identity {
            continue;
        }
        let DomainPace::Paced { period, .. } = domain.config.pace else {
            continue;
        };
        desired.insert(
            domain_id.clone(),
            DomainClockTaskSpec {
                clock,
                period,
                generation: domain.start_version,
                authority_revision: authority.revision(),
                authority: owner.clone(),
            },
        );
    }

    let existing = tasks.keys().cloned().collect::<Vec<_>>();
    for domain_id in existing {
        let unchanged = match (tasks.get(&domain_id), desired.get(&domain_id)) {
            (Some(task), Some(spec)) => &task.spec == spec,
            _ => false,
        };
        if !unchanged && let Some(task) = tasks.remove(&domain_id) {
            retirements.retire(domain_id, task);
        }
    }

    for (domain_id, spec) in desired {
        nervix_primitives::task::consume_budget().await;
        if tasks.contains_key(&domain_id) || retirements.contains(&domain_id) {
            continue;
        }
        let token = shutdown.child_token();
        let task_service = service.clone();
        let task_domain_id = domain_id.clone();
        let task_token = token.clone();
        let task_spec = spec.clone();
        let clock_revision = state.revision;
        let handle = nervix_primitives::task::spawn(async move {
            run_domain_clock(
                task_service,
                task_domain_id,
                task_spec,
                clock_revision,
                task_token,
            )
            .await;
        });
        tasks.insert(
            domain_id,
            DomainClockTask {
                spec,
                task: BackgroundTask {
                    cancel: token,
                    handle,
                },
            },
        );
    }
}

pub(in crate::application) async fn run_domain_clock_authority_reconciliation(
    service: SessionServiceImpl,
    shutdown: CancellationToken,
) {
    let mut topology_changes = service.inner.consensus.subscribe_topology();
    let mut domain_changes = service.inner.consensus.subscribe_domains();
    let mut cluster_state = service.inner.cluster.subscribe_state_changes().await;
    let mut reconciliation = DomainClockAuthorityReconciliation::Settled;

    loop {
        nervix_primitives::task::consume_budget().await;
        // Prepare the deadline-bearing wait before reading the effective cluster view. If the
        // monotonic deadline elapses while reconciliation is running, the prepared sleep remains
        // ready and causes a second predicate evaluation instead of losing that transition.
        let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
        tokio::pin!(cluster_change);
        let leads = service.inner.consensus.current_leader().await.as_ref()
            == Some(service.inner.consensus.local_node_id());
        if leads {
            reconciliation = service
                .reconcile_domain_clock_authorities(reconciliation)
                .await;
        } else {
            reconciliation = DomainClockAuthorityReconciliation::Settled;
        }
        // A refused proposal changes nothing this loop observes, so a reassignment that did not
        // commit is proposed again after the retry interval rather than at the next change.
        let retry = sleep(DOMAIN_CLOCK_AUTHORITY_RETRY_INTERVAL);
        tokio::pin!(retry);
        nervix_primitives::select! {
            _ = shutdown.cancelled() => break,
            open = topology_changes.changed() => {
                if !open {
                    break;
                }
            }
            changed = domain_changes.changed() => changed.assured(
                "the consensus store retains its domain sender for the server lifetime",
            ),
            _ = &mut cluster_change => {}
            _ = &mut retry, if reconciliation == DomainClockAuthorityReconciliation::Owed => {}
        }
    }
}

async fn run_domain_clock(
    service: SessionServiceImpl,
    domain_id: DomainName,
    spec: DomainClockTaskSpec,
    clock_revision: u64,
    shutdown: CancellationToken,
) {
    let mut cluster_state = service.inner.cluster.subscribe_state_changes().await;
    // Reconciliation checked this node's installed mapping and authority fence before starting
    // the producer. Another node's preparation or graph activation must not delay its progress.

    // A valid rate may make the next physical interval unrepresentable. Tick one is already due
    // at START and must still be emitted before scheduling a later interval can fail.
    let mut minimum_tick_wall_spacing = None;
    let mut next_tick_id = 1;
    let mut last_emitted_at: Option<Instant> = None;
    let mut latest_progress = None;
    let mut deliveries = DomainClockProgressDeliveries::default();
    loop {
        nervix_primitives::task::consume_budget().await;
        if shutdown.is_cancelled() {
            break;
        }
        deliveries
            .reconcile(
                &service,
                &spec,
                clock_revision,
                latest_progress.as_ref(),
                &shutdown,
                &domain_id,
            )
            .await;
        let wall_time = current_timestamp();
        let due = match spec
            .clock
            .due_advancement(spec.period, next_tick_id, wall_time)
        {
            Ok(due) => due,
            Err(error) => {
                service.inner.runtime.report_domain_clock_failure(
                    &domain_id,
                    "projection",
                    &error,
                    "domain clock arithmetic failed",
                );
                break;
            }
        };
        if let Some(advancement) = due {
            if let Some(last) = last_emitted_at {
                let spacing = match minimum_tick_wall_spacing {
                    Some(spacing) => spacing,
                    None => match spec.minimum_tick_wall_spacing() {
                        Ok(spacing) => {
                            minimum_tick_wall_spacing = Some(spacing);
                            spacing
                        }
                        Err(error) => {
                            service.inner.runtime.report_domain_clock_failure(
                                &domain_id,
                                "cadence",
                                &error,
                                "domain clock cadence failed",
                            );
                            break;
                        }
                    },
                };
                if let Some(remaining) = spacing.checked_sub(last.elapsed())
                    && !remaining.is_zero()
                {
                    let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
                    tokio::pin!(cluster_change);
                    nervix_primitives::select! {
                        _ = shutdown.cancelled() => break,
                        _ = sleep(remaining) => {},
                        _ = &mut cluster_change => {},
                    }
                    continue;
                }
            }
            let progress = emit_domain_clock_progress(
                &service,
                &domain_id,
                &spec,
                &mut next_tick_id,
                advancement,
                &deliveries,
                &shutdown,
            )
            .await;
            let Some(progress) = progress else {
                break;
            };
            last_emitted_at = Some(Instant::now());
            latest_progress = Some(progress);
            continue;
        }
        let next_boundary = match spec.clock.tick_boundary(spec.period, next_tick_id) {
            Ok(boundary) => boundary,
            Err(error) => {
                service.inner.runtime.report_domain_clock_failure(
                    &domain_id,
                    "boundary",
                    &error,
                    "domain clock boundary arithmetic failed",
                );
                break;
            }
        };
        let reached_logical = match spec.clock.logical_time_at(wall_time) {
            Ok(reached) => reached,
            Err(error) => {
                service.inner.runtime.report_domain_clock_failure(
                    &domain_id,
                    "projection",
                    &error,
                    "domain clock projection failed",
                );
                break;
            }
        };
        let wait = match spec
            .clock
            .wall_duration_until(reached_logical, next_boundary.logical_timestamp())
        {
            Ok(wait) => wait,
            Err(error) => {
                service.inner.runtime.report_domain_clock_failure(
                    &domain_id,
                    "rate conversion",
                    &error,
                    "domain clock rate conversion failed",
                );
                break;
            }
        };
        // `due_advancement` returned `None`, so this boundary is strictly in the logical future.
        // The model converts that positive delta with ceiling and a one-nanosecond minimum.
        let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
        tokio::pin!(cluster_change);
        nervix_primitives::select! {
            _ = shutdown.cancelled() => break,
            _ = sleep(wait) => {}
            _ = &mut cluster_change => {}
        }
    }
    deliveries.stop_all().await;
}

async fn emit_domain_clock_progress(
    service: &SessionServiceImpl,
    domain_id: &DomainName,
    spec: &DomainClockTaskSpec,
    next_tick_id: &mut u64,
    advancement: DomainClockAdvancement,
    deliveries: &DomainClockProgressDeliveries,
    shutdown: &CancellationToken,
) -> Option<DomainClockProgress> {
    #[cfg(feature = "testing")]
    let progress_was_paused = service
        .inner
        .runtime
        .pause_domain_clock_progress_if_armed(
            domain_id,
            service.inner.consensus.local_node_id(),
            shutdown,
        )
        .await;
    if shutdown.is_cancelled() {
        return None;
    }
    let wall_clock = current_timestamp();
    let boundary = advancement.boundary();
    let tick = DomainTick {
        tick_id: boundary.tick_id(),
        logical_timestamp: boundary.logical_timestamp(),
        wall_clock,
        period: spec.period,
    };
    *next_tick_id = advancement.next_tick_id();
    let progress = DomainClockProgress {
        generation: spec.generation,
        authority_revision: spec.authority_revision,
        authority: spec.authority.clone(),
        tick,
    };
    service.handle_domain_clock_progress(
        spec.authority.node_id(),
        DomainClockProgressRequest {
            domain_id: domain_id.clone(),
            progress: progress.clone(),
        },
    );
    deliveries.publish(domain_id, &progress);
    #[cfg(feature = "testing")]
    if progress_was_paused {
        service.inner.runtime.mark_domain_clock_progress_delivered(
            domain_id,
            service.inner.consensus.local_node_id(),
        );
    }
    Some(progress)
}

pub(in crate::application) fn current_timestamp() -> Timestamp {
    Timestamp::now()
}

pub(in crate::application) fn subtract_timestamp_duration(
    timestamp: Timestamp,
    duration: Duration,
) -> Timestamp {
    // A skew window extends to the first representable instant when its lower edge would precede
    // the timestamp model's range.
    timestamp
        .checked_sub(duration)
        .unwrap_or_else(|_| Timestamp::from_unix_nanos(i64::MIN))
}

impl SessionServiceImpl {
    async fn domain_clock_authority_candidates(&self) -> DomainClockAuthorityCandidates {
        let gossip = self.inner.cluster.availability_state().await;
        let live_identities = gossip.live_identities();
        let live_node_ids = live_identities
            .iter()
            .map(|identity| identity.node_id().clone())
            .collect::<Vec<_>>();
        let voters = self
            .inner
            .consensus
            .live_voter_ids(live_node_ids)
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();
        DomainClockAuthorityCandidates::new(
            live_identities
                .into_iter()
                .filter(|identity| voters.contains(identity.node_id())),
        )
    }

    pub(in crate::application) async fn selected_domain_clock_authority(
        &self,
        domain_id: &DomainName,
    ) -> Option<ClusterNodeIdentity> {
        self.domain_clock_authority_candidates()
            .await
            .owner_for(domain_id)
    }

    /// Installs the authority fences of the committed `state` ahead of graph installation, then
    /// advertises its runtime revision once every domain clock on this node matches it.
    pub(in crate::application) async fn install_committed_domain_clocks(
        &self,
        state: &ConsensusRuntimeState,
    ) {
        let installation = self.inner.runtime.install_committed_clock_authorities(
            state.revision,
            &state.domains,
            &state.domain_clock_authorities,
        );
        if let CommittedClockInstallation::Complete = installation {
            self.inner
                .cluster
                .set_local_domain_clock_revision_installed(state.revision)
                .await;
        }
    }

    async fn reconcile_domain_clock_authorities(
        &self,
        previous: DomainClockAuthorityReconciliation,
    ) -> DomainClockAuthorityReconciliation {
        let state = self.inner.consensus.current_runtime_state().await;
        let candidates = self.domain_clock_authority_candidates().await;
        let mut reconciliation = DomainClockAuthorityReconciliation::Settled;
        for (domain_id, domain) in state.domains {
            nervix_primitives::task::consume_budget().await;
            if matches!(domain.config.pace, DomainPace::Unpaced)
                || matches!(domain.status, DomainStatus::Stopped)
            {
                continue;
            }
            let expected = state
                .domain_clock_authorities
                .get(&domain_id)
                .cloned()
                .unwrap_or_else(DomainClockAuthority::initial);
            let owner = candidates.owner_for(&domain_id);
            if expected.owner() == owner.as_ref() {
                continue;
            }
            let proposal = self
                .propose_domain_clock_authority(
                    domain_id.clone(),
                    domain.start_version,
                    expected,
                    owner,
                )
                .await;
            let Err(error) = proposal else {
                continue;
            };
            reconciliation = DomainClockAuthorityReconciliation::Owed;
            // The retry recurs every interval while the failure lasts, so only the first failure
            // after a settled pass warns.
            match previous {
                DomainClockAuthorityReconciliation::Settled => warn!(
                    domain = domain_id.as_str(),
                    error = %format_args!("{error:#}"),
                    "failed to reconcile committed domain-clock authority; retrying"
                ),
                DomainClockAuthorityReconciliation::Owed => debug!(
                    domain = domain_id.as_str(),
                    error = %format_args!("{error:#}"),
                    "domain-clock authority reconciliation is still failing; retrying"
                ),
            }
        }
        reconciliation
    }

    /// Proposes one compare-and-set reassignment of a domain's committed clock authority.
    async fn propose_domain_clock_authority(
        &self,
        domain_id: DomainName,
        expected_start_version: u64,
        expected: DomainClockAuthority,
        owner: Option<ClusterNodeIdentity>,
    ) -> error_stack::Result<(), ConsensusError> {
        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .take_domain_clock_authority_proposal_rejection(self.inner.consensus.local_node_id())
        {
            return Err(error_stack::Report::new(ConsensusError::LeadershipLost {
                observed: nervix_consensus::LeadershipObservation::NoLeader,
            }));
        }
        self.inner
            .consensus
            .reconcile_domain_clock_authority(domain_id, expected_start_version, expected, owner)
            .await
    }

    pub(in crate::application) fn handle_domain_clock_progress(
        &self,
        authenticated_node: &ClusterNodeName,
        request: DomainClockProgressRequest,
    ) {
        if let Err(error) = self.inner.runtime.handle_domain_clock_progress(
            &request.domain_id,
            authenticated_node,
            &request.progress,
        ) {
            self.broadcast_error(format!(
                "failed to apply domain clock progress for '{}': {error:#}",
                request.domain_id.as_str(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{Runtime, RuntimeEvent, report_observer::ReportLogObserver};

    #[nervix_primitives::test]
    async fn runtime_report_chain_clock_events_and_logs() {
        let runtime = Runtime::default();
        let mut events = runtime.subscribe_events();
        let domain = DomainName::parse("orders").assured("the fixture domain name is valid");
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(i64::MAX),
            nervix_models::DomainTimeRate::ONE,
        );
        let period = DomainClockPeriod::try_from(Duration::from_nanos(1))
            .assured("the fixture clock has a positive period");
        let Err(error) = clock.tick_boundary(period, 2) else {
            panic!("the second tick exceeds the timestamp range");
        };
        let chain = "domain clock tick boundary leaves the signed Unix-nanosecond range: \
                     timestamp arithmetic leaves the signed Unix-nanosecond range";
        let mut logs = ReportLogObserver::new();
        for (operation, message) in [
            ("projection", "domain clock arithmetic failed"),
            ("cadence", "domain clock cadence failed"),
            ("boundary", "domain clock boundary arithmetic failed"),
            ("projection", "domain clock projection failed"),
            ("rate conversion", "domain clock rate conversion failed"),
        ] {
            logs.observe(async {
                runtime.report_domain_clock_failure(&domain, operation, &error, message);
            })
            .await;
            let RuntimeEvent::Error(event) =
                nervix_primitives::time::timeout(Duration::from_secs(1), events.recv())
                    .await
                    .assured("the clock failure is queued before its observation deadline")
                    .assured("each clock failure publishes its report");
            assert_eq!(
                event,
                format!("domain clock {operation} for 'orders' failed: {chain}")
            );
            assert_eq!(logs.next(message).fields["error"], chain);
        }
    }

    /// The committed authority of `domain` once `owner` holds it at a revision newer than `after`,
    /// or nothing if that does not happen within ten seconds.
    #[cfg(feature = "testing")]
    async fn committed_owner_after(
        service: &SessionServiceImpl,
        domain: &DomainName,
        owner: &ClusterNodeIdentity,
        after: nervix_models::DomainClockAuthorityRevision,
        domain_changes: &mut watch::Receiver<u64>,
    ) -> Option<DomainClockAuthority> {
        let committed = nervix_primitives::time::timeout(Duration::from_secs(10), async {
            loop {
                nervix_primitives::task::consume_budget().await;
                let state = service.inner.consensus.current_runtime_state().await;
                if let Some(authority) = state.domain_clock_authorities.get(domain)
                    && authority.owner() == Some(owner)
                    && authority.revision().get() > after.get()
                {
                    return authority.clone();
                }
                domain_changes
                    .changed()
                    .await
                    .assured("the consensus store retains its domain sender for its lifetime");
            }
        })
        .await;
        committed.ok()
    }

    /// A leader whose proposal Raft refuses, as it refuses a write before a newly elected leader's
    /// quorum acknowledges it, proposes again without waiting for any other cluster change.
    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn leader_retries_an_authority_reassignment_its_leadership_refused() {
        use nervix_models::{ClusterNodeIncarnation, DomainStartPoint, DomainTimeRate};
        use nervix_recovery::Discarded as _;

        use crate::application::test_fixtures::{
            TestService, build_test_service_with_fault_injection,
        };

        let fault_injection = crate::FaultInjection::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_fault_injection(true, fault_injection.clone()).await;
        let domain = DomainName::parse("default").assured("the test domain name is valid");
        let departed = ClusterNodeIdentity::new(
            ClusterNodeName::parse("departed").assured("the fixture node name is valid"),
            ClusterNodeIncarnation::new(1),
        );
        let origin = Timestamp::from_unix_nanos(0);
        service
            .inner
            .consensus
            .start_domain(
                domain.clone(),
                DomainStartPoint::At {
                    timestamp: origin,
                    time_rate: DomainTimeRate::ONE,
                },
                Some(DomainClockState::new(
                    current_timestamp(),
                    origin,
                    DomainTimeRate::ONE,
                )),
                Some(departed.clone()),
                None,
            )
            .await
            .assured("the single-node leader commits the start it proposes");
        // Gossip publishes the local node once after it starts. Waiting for that publication leaves
        // no cluster change after the refusal, so only a retry can reassign the authority.
        let mut live_nodes = service.inner.cluster.subscribe_live_node_states().await;
        nervix_primitives::time::timeout(Duration::from_secs(10), async {
            loop {
                nervix_primitives::task::consume_budget().await;
                if !live_nodes.borrow_and_update().is_empty() {
                    return;
                }
                live_nodes
                    .changed()
                    .await
                    .assured("the cluster handle retains its gossip sender for its lifetime");
            }
        })
        .await
        .assured("gossip publishes the local node within its first rounds");
        let local = service.inner.cluster.local_node_identity().await;
        let mut domain_changes = service.inner.consensus.subscribe_domains();
        let shutdown = CancellationToken::new();
        let reconciliation = nervix_primitives::task::spawn(
            run_domain_clock_authority_reconciliation(service.clone(), shutdown.clone()),
        );
        // The running leader first replaces the departed authority with itself. Its first passes
        // also consume the changes it observed when it subscribed.
        let settled = committed_owner_after(
            &service,
            &domain,
            &local,
            nervix_models::DomainClockAuthorityRevision::INITIAL,
            &mut domain_changes,
        )
        .await
        .assured("the running leader replaces the departed authority with itself");

        // A reassignment back to the departed node is the last change the loop observes. Raft
        // then refuses the leader's next proposal, so only a retry can restore its authority.
        fault_injection.reject_next_domain_clock_authority_proposal_on(local.node_id().clone());
        let state = service.inner.consensus.current_runtime_state().await;
        let start_version = state
            .domains
            .get(&domain)
            .assured("the started domain remains committed")
            .start_version;
        service
            .inner
            .consensus
            .reconcile_domain_clock_authority(
                domain.clone(),
                start_version,
                settled.clone(),
                Some(departed.clone()),
            )
            .await
            .assured("the single-node leader commits the reassignment it proposes");
        let departed_revision = settled
            .revision()
            .checked_next()
            .assured("the fixture authority revision is far below its bound");
        let restored = committed_owner_after(
            &service,
            &domain,
            &local,
            departed_revision,
            &mut domain_changes,
        )
        .await;
        shutdown.cancel();
        reconciliation
            .await
            .assured("the reconciliation ends without panicking once its shutdown is cancelled");

        assert!(
            restored.is_some(),
            "the leader never reassigned the authority after Raft refused its proposal"
        );
        assert!(
            !fault_injection.domain_clock_authority_proposal_rejection_is_armed(local.node_id()),
            "the refusal was never consumed, so the test did not exercise a refused proposal"
        );
        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }
}
