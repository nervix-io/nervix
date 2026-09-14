//! The authority that advances a domain's logical time, and the node that runs it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Choosing a clock authority per domain, running the clock task, and delivering its
//!   progress to every node.
//! - **Depends on.** Consensus for the recorded authority and typed interconnect progress requests.
//! - **Must not know.** What a domain does with the time it is given.

use std::collections::BTreeSet;

use ahash::{HashMap, HashMapExt};
use futures_util::{StreamExt, stream::FuturesUnordered};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::DomainClockProgressRequest;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeName, DomainClockAdvancement, DomainClockAuthority,
    DomainClockPeriod, DomainClockProgress, DomainClockState, DomainName, DomainPace, DomainStatus,
    DomainTick, Timestamp,
};
use tokio::{
    sync::watch,
    time::{Duration, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::{background_task::BackgroundTask, session_service::SessionServiceImpl};
use crate::{domain_clock_authority::DomainClockAuthorityCandidates, task_shutdown::JoinShutdown};

const DOMAIN_CLOCK_PROGRESS_RETRY_BACKOFF: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, PartialEq, Eq)]
struct DomainClockTaskSpec {
    clock: DomainClockState,
    period: DomainClockPeriod,
    generation: u64,
    authority_revision: nervix_models::DomainClockAuthorityRevision,
    authority: ClusterNodeIdentity,
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
        let handle = tokio::spawn(async move {
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
            tokio::task::consume_budget().await;
            if !retry_pending {
                let changed = tokio::select! {
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
            let result = tokio::select! {
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
                        error = %error,
                        "failed to deliver domain clock progress"
                    );
                    tokio::select! {
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
        minimum_runtime_revision: u64,
        latest: Option<&DomainClockProgress>,
        shutdown: &CancellationToken,
        domain_id: &DomainName,
    ) {
        let gossip = service.inner.cluster.availability_state().await;
        let ready = service
            .inner
            .cluster
            .nodes_ready_for_runtime_revision(minimum_runtime_revision)
            .await;
        let mut desired = gossip
            .live_identities()
            .intersection(&ready)
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
            tokio::task::consume_budget().await;
            if let Some(delivery) = self.targets.remove(&target) {
                delivery.stop().await;
            }
        }

        for target in desired {
            tokio::task::consume_budget().await;
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
            tokio::task::consume_budget().await;
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
            tokio::task::consume_budget().await;
            for task in tasks {
                tokio::task::consume_budget().await;
                task.task.stop().await;
            }
        }
    }
}

pub(in crate::application) async fn reconcile_domain_clock_tasks(
    service: &SessionServiceImpl,
    shutdown: &CancellationToken,
    tasks: &mut HashMap<DomainName, DomainClockTask>,
    retirements: &mut DomainClockRetirements,
) {
    let state = service.inner.consensus.current_runtime_state().await;
    let local_identity = service.inner.cluster.local_node_identity().await;

    let mut desired = HashMap::<DomainName, DomainClockTaskSpec>::new();
    for (domain_id, domain) in &state.domains {
        tokio::task::consume_budget().await;
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
        tokio::task::consume_budget().await;
        if tasks.contains_key(&domain_id) || retirements.contains(&domain_id) {
            continue;
        }
        let token = shutdown.child_token();
        let task_service = service.clone();
        let task_domain_id = domain_id.clone();
        let task_token = token.clone();
        let task_spec = spec.clone();
        let minimum_runtime_revision = state.revision;
        let handle = tokio::spawn(async move {
            run_domain_clock(
                task_service,
                task_domain_id,
                task_spec,
                minimum_runtime_revision,
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

    loop {
        tokio::task::consume_budget().await;
        // Prepare the deadline-bearing wait before reading the effective cluster view. If the
        // monotonic deadline elapses while reconciliation is running, the prepared sleep remains
        // ready and causes a second predicate evaluation instead of losing that transition.
        let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
        tokio::pin!(cluster_change);
        if service.inner.consensus.current_leader().await.as_ref()
            == Some(service.inner.consensus.local_node_id())
        {
            service.reconcile_domain_clock_authorities().await;
        }
        tokio::select! {
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
        }
    }
}

async fn run_domain_clock(
    service: SessionServiceImpl,
    domain_id: DomainName,
    spec: DomainClockTaskSpec,
    minimum_runtime_revision: u64,
    shutdown: CancellationToken,
) {
    let mut cluster_state = service.inner.cluster.subscribe_state_changes().await;
    loop {
        tokio::task::consume_budget().await;
        let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
        tokio::pin!(cluster_change);
        let live_targets = service
            .inner
            .cluster
            .availability_state()
            .await
            .live_identities();
        let ready_targets = service
            .inner
            .cluster
            .nodes_ready_for_runtime_revision(minimum_runtime_revision)
            .await;
        if !live_targets.is_empty() && live_targets.is_subset(&ready_targets) {
            break;
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = &mut cluster_change => {}
        }
    }

    let mut next_tick_id = 1;
    let mut latest_progress = None;
    let mut deliveries = DomainClockProgressDeliveries::default();
    loop {
        tokio::task::consume_budget().await;
        if shutdown.is_cancelled() {
            break;
        }
        deliveries
            .reconcile(
                &service,
                &spec,
                minimum_runtime_revision,
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
                service.inner.runtime.report_error(format!(
                    "domain clock projection for '{}' failed: {error}",
                    domain_id.as_str()
                ));
                warn!(
                    domain = domain_id.as_str(),
                    error = %error,
                    "domain clock arithmetic failed"
                );
                break;
            }
        };
        if let Some(advancement) = due {
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
            latest_progress = Some(progress);
            continue;
        }
        let next_boundary = match spec.clock.tick_boundary(spec.period, next_tick_id) {
            Ok(boundary) => boundary,
            Err(error) => {
                service.inner.runtime.report_error(format!(
                    "domain clock boundary for '{}' failed: {error}",
                    domain_id.as_str()
                ));
                warn!(
                    domain = domain_id.as_str(),
                    error = %error,
                    "domain clock boundary arithmetic failed"
                );
                break;
            }
        };
        let reached_logical = match spec.clock.logical_time_at(wall_time) {
            Ok(reached) => reached,
            Err(error) => {
                service.inner.runtime.report_error(format!(
                    "domain clock projection for '{}' failed: {error}",
                    domain_id.as_str()
                ));
                warn!(domain = domain_id.as_str(), error = %error, "domain clock projection failed");
                break;
            }
        };
        let wait = match spec
            .clock
            .wall_duration_until(reached_logical, next_boundary.logical_timestamp())
        {
            Ok(wait) => wait,
            Err(error) => {
                service.inner.runtime.report_error(format!(
                    "domain clock rate conversion for '{}' failed: {error}",
                    domain_id.as_str()
                ));
                warn!(domain = domain_id.as_str(), error = %error, "domain clock rate conversion failed");
                break;
            }
        };
        // `due_advancement` returned `None`, so this boundary is strictly in the logical future.
        // The model converts that positive delta with ceiling and a one-nanosecond minimum.
        let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
        tokio::pin!(cluster_change);
        tokio::select! {
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

    async fn reconcile_domain_clock_authorities(&self) {
        let state = self.inner.consensus.current_runtime_state().await;
        let candidates = self.domain_clock_authority_candidates().await;
        for (domain_id, domain) in state.domains {
            tokio::task::consume_budget().await;
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
            if let Err(error) = self
                .inner
                .consensus
                .reconcile_domain_clock_authority(
                    domain_id.clone(),
                    domain.start_version,
                    expected,
                    owner,
                )
                .await
            {
                warn!(
                    domain = domain_id.as_str(),
                    error = %error,
                    "failed to reconcile committed domain-clock authority"
                );
            }
        }
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
                "failed to apply domain clock progress for '{}': {error}",
                request.domain_id.as_str(),
            ));
        }
    }
}
