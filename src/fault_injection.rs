//! Fault controls exposed to the server test harness.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** The shared state used to arm deterministic server failures, pauses, timing
//!   overrides, listener-address overrides, and test scheduling.
//! - **Depends on.** Vocabulary identities, Tokio notification primitives, and the decision
//!   layer's test scheduler mode.
//! - **Must not know.** Product configuration, NSPL syntax, connector implementations, or runtime
//!   ownership beyond the typed values needed to select a seam.

use std::{
    net::{IpAddr, SocketAddr},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

use ahash::RandomState;
use dashmap::DashMap;
use meticulous::ResultExt as _;
use nervix_execution::{CpuClass, Executor, MemoryClass};
use nervix_models::{ClusterNodeName, DomainName, EmitterName, IngestorName};
use nervix_recovery::{Discarded as _, NoReceiver as _};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{broadcast, watch};
use triomphe::Arc;

use crate::registry::SchedulerMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmitterFaultMode {
    Fail,
    Stall,
}

/// One cloneable handle for every fault and override a server test can inject.
///
/// Every clone points at the same state, so a Cucumber world can arm a seam after handing the
/// handle to every node without keeping parallel injector objects in sync.
#[derive(Clone, Debug)]
pub struct FaultInjection {
    inner: Arc<FaultInjectionState>,
}

#[derive(Debug)]
struct FaultInjectionState {
    emitter_faults: DashMap<String, EmitterFaultMode, RandomState>,
    failed_ingestors: DashMap<String, (), RandomState>,
    unavailable_otel_clients: DashMap<String, (), RandomState>,
    failed_schedule_publications: DashMap<String, (), RandomState>,
    /// One-shot, domain-scoped drain failures consumed after a pending status is observed.
    forced_entity_drain_timeouts: DashMap<DomainName, (), RandomState>,
    transaction_binding_drops: DashMap<ClusterNodeName, (), RandomState>,
    consensus_probes: DashMap<ClusterNodeName, ConsensusProbe, RandomState>,
    bulk_executions: DashMap<ClusterNodeName, NodeBulkExecution, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    command_pauses: DashMap<CommandPausePoint, Arc<TestPause>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    entity_gate_pauses: DashMap<String, Arc<TestPause>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    remote_relay_admission_pauses:
        DashMap<RemoteRelayAdmissionPauseKey, Arc<TestPause>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    ownership_handoff_preparation_pauses: DashMap<String, Arc<TestPause>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    domain_clock_progress_pauses:
        DashMap<DomainClockProgressPausePoint, Arc<TestPause>, RandomState>,
    /// Wall time already elapsed when the next newly supplied mapping for a domain starts.
    domain_clock_initial_elapsed: DashMap<DomainName, Duration, RandomState>,
    state_replica_polling_paused: AtomicBool,
    syslog_ingestor_bind_ips: DashMap<ClusterNodeName, IpAddr, RandomState>,
    branch_instance_expiration_scan_interval: RwLock<Option<Duration>>,
    domain_drain_timeout: RwLock<Option<Duration>>,
    entity_gate_deadline: RwLock<Option<Duration>>,
    scheduler_mode: RwLock<SchedulerMode>,
    leadership_transfers: broadcast::Sender<LeadershipTransferRequest>,
}

struct ConsensusProbe {
    observer: nervix_consensus::Observer,
    fault: nervix_consensus::StorageFault,
}

impl std::fmt::Debug for ConsensusProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusProbe").finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct NodeBulkExecution {
    executor: Executor,
    /// Occupying jobs outlive the map guard while they run, so their release senders are shared.
    holders: Arc<Mutex<Vec<std::sync::mpsc::Sender<()>>>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct TestPauseState {
    reached: bool,
    released: bool,
    delivered: bool,
}

#[derive(Debug)]
struct TestPause {
    state: watch::Sender<TestPauseState>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RemoteRelayAdmissionPauseKey {
    domain: String,
    branch: Option<String>,
}

/// The command boundary a test controls without racing an election against a request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CommandPausePoint {
    Admission(ClusterNodeName),
    TransactionCommit {
        node_id: ClusterNodeName,
        completed_statements: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DomainClockProgressPausePoint {
    domain: String,
    node: Option<ClusterNodeName>,
}

#[derive(Clone, Debug)]
pub(crate) struct LeadershipTransferRequest {
    pub from_node_id: ClusterNodeName,
    pub to_node_id: ClusterNodeName,
}

impl Default for FaultInjection {
    fn default() -> Self {
        let (leadership_transfers, _) = broadcast::channel(16);
        Self {
            inner: Arc::new(FaultInjectionState {
                emitter_faults: DashMap::default(),
                failed_ingestors: DashMap::default(),
                unavailable_otel_clients: DashMap::default(),
                failed_schedule_publications: DashMap::default(),
                forced_entity_drain_timeouts: DashMap::default(),
                transaction_binding_drops: DashMap::default(),
                consensus_probes: DashMap::default(),
                bulk_executions: DashMap::default(),
                command_pauses: DashMap::default(),
                entity_gate_pauses: DashMap::default(),
                remote_relay_admission_pauses: DashMap::default(),
                ownership_handoff_preparation_pauses: DashMap::default(),
                domain_clock_progress_pauses: DashMap::default(),
                domain_clock_initial_elapsed: DashMap::default(),
                state_replica_polling_paused: AtomicBool::new(false),
                syslog_ingestor_bind_ips: DashMap::default(),
                branch_instance_expiration_scan_interval: RwLock::new(None),
                domain_drain_timeout: RwLock::new(None),
                entity_gate_deadline: RwLock::new(None),
                scheduler_mode: RwLock::new(SchedulerMode::default()),
                leadership_transfers,
            }),
        }
    }
}

impl FaultInjection {
    pub fn unregister_consensus(&self, node: &ClusterNodeName) {
        self.inner.consensus_probes.remove(node);
    }

    pub(crate) fn register_consensus(
        &self,
        node: ClusterNodeName,
        consensus: &nervix_consensus::Consensus,
    ) {
        self.inner.consensus_probes.insert(
            node,
            ConsensusProbe {
                observer: consensus.observer(),
                fault: consensus.storage_fault(),
            },
        );
    }

    pub fn consensus_observer(&self, node: &ClusterNodeName) -> nervix_consensus::Observer {
        use meticulous::OptionExt as _;
        self.inner
            .consensus_probes
            .get(node)
            .verified("the harness started this node before observing consensus")
            .observer
            .clone()
    }

    pub fn fail_consensus_storage(
        &self,
        node: &ClusterNodeName,
        operation: String,
        boundary: nervix_consensus::StorageBoundary,
    ) {
        use meticulous::OptionExt as _;
        self.inner
            .consensus_probes
            .get(node)
            .verified("the harness started this node before injecting storage failure")
            .fault
            .fail_next(operation, boundary);
    }

    pub fn fail_emitter(&self, emitter: &str) {
        self.inner
            .emitter_faults
            .insert(emitter.to_ascii_lowercase(), EmitterFaultMode::Fail);
    }

    pub fn stall_emitter(&self, emitter: &str) {
        self.inner
            .emitter_faults
            .insert(emitter.to_ascii_lowercase(), EmitterFaultMode::Stall);
    }

    pub fn clear_emitter_fault(&self, emitter: &str) {
        self.inner
            .emitter_faults
            .remove(&emitter.to_ascii_lowercase());
    }

    pub fn clear_all_emitter_faults(&self) {
        self.inner.emitter_faults.clear();
    }

    pub fn fail_ingestor(&self, ingestor: &str) {
        self.inner
            .failed_ingestors
            .insert(ingestor.to_ascii_lowercase(), ());
    }

    pub fn clear_ingestor_fault(&self, ingestor: &str) {
        self.inner
            .failed_ingestors
            .remove(&ingestor.to_ascii_lowercase());
    }

    pub fn fail_otel_client_unavailable(&self, emitter: &str) {
        self.inner
            .unavailable_otel_clients
            .insert(emitter.to_ascii_lowercase(), ());
    }

    pub fn clear_otel_client_fault(&self, emitter: &str) {
        self.inner
            .unavailable_otel_clients
            .remove(&emitter.to_ascii_lowercase());
    }

    /// Fails the next schedule publication for a domain so a test can observe recovery after the
    /// committed model mutation's new schedule does not reach the cluster.
    pub fn fail_next_schedule_publication(&self, domain: &str) {
        self.inner
            .failed_schedule_publications
            .insert(domain.to_ascii_lowercase(), ());
    }

    /// Forces the next pending entity drain in `domain` to report its normal timeout outcome
    /// after collecting one complete status observation. The one-shot seam lets timeout recovery
    /// tests exercise that outcome without making successful cluster work race a short duration.
    pub fn force_next_entity_drain_timeout(&self, domain: DomainName) {
        self.inner.forced_entity_drain_timeouts.insert(domain, ());
    }

    pub fn drop_transaction_bindings_on(&self, node_id: ClusterNodeName) {
        self.inner.transaction_binding_drops.insert(node_id, ());
    }

    /// Fill every bulk worker on `node_id` and return once every occupying job is running.
    pub async fn occupy_bulk_execution(&self, node_id: &ClusterNodeName) {
        let node = self
            .inner
            .bulk_executions
            .get(node_id)
            .unwrap_or_else(|| panic!("node '{node_id}' has not registered its executor"));
        let executor = node.executor.clone();
        let holders = node.holders.clone();
        drop(node);
        let workers = executor.snapshot().bulk_cpu.workers;
        let started = Arc::new(AtomicUsize::new(0));
        for _ in 0..workers {
            let reservation = executor
                .try_reserve(MemoryClass::Bulk, 0)
                .unwrap_or_else(|error| {
                    panic!("bulk admission must accept a zero charge: {error}")
                });
            let (holder, held) = std::sync::mpsc::channel();
            holders.lock().push(holder);
            let executor = executor.clone();
            let started = started.clone();
            tokio::spawn(async move {
                executor
                    .run_cpu(
                        CpuClass::Bulk,
                        reservation,
                        move |_charge, _cancellation| {
                            started.fetch_add(1, Ordering::AcqRel);
                            // Park until the scenario drops the holder so occupancy consumes no
                            // CPU. Nothing is ever sent, so the disconnect is the wake-up rather
                            // than a failure.
                            held.recv().discarded(
                                "dropping the holder is how the scenario releases this worker",
                            );
                        },
                    )
                    .await
                    .discarded(
                        "this job exists to hold a worker until the scenario releases it; its \
                         outcome is not what the scenario reads",
                    );
            });
        }
        while started.load(Ordering::Acquire) < workers {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    }

    pub fn release_bulk_execution(&self, node_id: &ClusterNodeName) {
        self.inner
            .bulk_executions
            .get(node_id)
            .unwrap_or_else(|| panic!("node '{node_id}' has not registered its executor"))
            .holders
            .lock()
            .clear();
    }

    pub fn pause_command_admission_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::Admission(node_id));
    }

    pub async fn wait_for_command_admission_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::Admission(node_id.clone()))
            .await;
    }

    pub fn release_command_admission_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::Admission(node_id.clone()));
    }

    pub fn pause_transaction_commit_after(
        &self,
        node_id: ClusterNodeName,
        completed_statements: usize,
    ) {
        self.arm_command_pause(CommandPausePoint::TransactionCommit {
            node_id,
            completed_statements,
        });
    }

    pub async fn wait_for_transaction_commit_pause(
        &self,
        node_id: &ClusterNodeName,
        completed_statements: usize,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::TransactionCommit {
            node_id: node_id.clone(),
            completed_statements,
        })
        .await;
    }

    pub fn release_transaction_commit_pause(
        &self,
        node_id: &ClusterNodeName,
        completed_statements: usize,
    ) {
        self.release_command_pause(&CommandPausePoint::TransactionCommit {
            node_id: node_id.clone(),
            completed_statements,
        });
    }

    pub fn pause_entity_gate(&self, domain: impl Into<String>) {
        self.inner.entity_gate_pauses.insert(
            domain.into().to_ascii_lowercase(),
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_entity_gate_pause(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self.entity_gate_pause(&key);
        pause.wait_until_reached().await;
    }

    pub fn release_entity_gate_pause(&self, domain: &str) {
        let pause = self.entity_gate_pause(&domain.to_ascii_lowercase());
        pause.release();
    }

    pub fn pause_remote_relay_admission(&self, domain: impl Into<String>) {
        self.pause_remote_relay_admission_for_branch(domain, None);
    }

    pub fn pause_remote_relay_admission_for_branch(
        &self,
        domain: impl Into<String>,
        branch: Option<String>,
    ) {
        self.inner.remote_relay_admission_pauses.insert(
            RemoteRelayAdmissionPauseKey {
                domain: domain.into().to_ascii_lowercase(),
                branch,
            },
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_remote_relay_admission_pause(&self, domain: &str) {
        self.wait_for_remote_relay_admission_pause_for_branch(domain, None)
            .await;
    }

    pub async fn wait_for_remote_relay_admission_pause_for_branch(
        &self,
        domain: &str,
        branch: Option<&str>,
    ) {
        let key = RemoteRelayAdmissionPauseKey {
            domain: domain.to_ascii_lowercase(),
            branch: branch.map(str::to_string),
        };
        let pause = self.remote_relay_admission_pause(&key);
        pause.wait_until_reached().await;
    }

    pub fn release_remote_relay_admission_pause(&self, domain: &str) {
        self.release_remote_relay_admission_pause_for_branch(domain, None);
    }

    pub fn release_remote_relay_admission_pause_for_branch(
        &self,
        domain: &str,
        branch: Option<&str>,
    ) {
        let key = RemoteRelayAdmissionPauseKey {
            domain: domain.to_ascii_lowercase(),
            branch: branch.map(str::to_string),
        };
        let pause = self.remote_relay_admission_pause(&key);
        pause.release();
    }

    pub fn pause_ownership_handoff_after_preparation(&self, domain: impl Into<String>) {
        self.inner.ownership_handoff_preparation_pauses.insert(
            domain.into().to_ascii_lowercase(),
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_ownership_handoff_preparation_pause(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self.ownership_handoff_preparation_pause(&key);
        pause.wait_until_reached().await;
    }

    pub fn release_ownership_handoff_preparation_pause(&self, domain: &str) {
        let pause = self.ownership_handoff_preparation_pause(&domain.to_ascii_lowercase());
        pause.release();
    }

    pub fn pause_domain_clock_progress(&self, domain: impl Into<String>) {
        self.inner.domain_clock_progress_pauses.insert(
            DomainClockProgressPausePoint {
                domain: domain.into().to_ascii_lowercase(),
                node: None,
            },
            Arc::new(TestPause::default()),
        );
    }

    pub fn set_domain_clock_initial_elapsed(&self, domain: DomainName, elapsed: Duration) {
        self.inner
            .domain_clock_initial_elapsed
            .insert(domain, elapsed);
    }

    pub(crate) fn take_domain_clock_initial_elapsed(
        &self,
        domain: &DomainName,
    ) -> Option<Duration> {
        let (_, elapsed) = self.inner.domain_clock_initial_elapsed.remove(domain)?;
        Some(elapsed)
    }

    pub fn pause_domain_clock_progress_on(&self, domain: impl Into<String>, node: ClusterNodeName) {
        self.inner.domain_clock_progress_pauses.insert(
            DomainClockProgressPausePoint {
                domain: domain.into().to_ascii_lowercase(),
                node: Some(node),
            },
            Arc::new(TestPause::default()),
        );
    }

    pub fn pause_state_replica_polling(&self) {
        self.inner
            .state_replica_polling_paused
            .store(true, Ordering::Release);
    }

    pub async fn wait_for_domain_clock_progress_pause(&self, domain: &str) {
        let point = DomainClockProgressPausePoint {
            domain: domain.to_ascii_lowercase(),
            node: None,
        };
        let pause = self.domain_clock_progress_pause(&point);
        pause.wait_until_reached().await;
    }

    pub async fn wait_for_domain_clock_progress_pause_on(
        &self,
        domain: &str,
        node: &ClusterNodeName,
    ) {
        let point = DomainClockProgressPausePoint {
            domain: domain.to_ascii_lowercase(),
            node: Some(node.clone()),
        };
        let pause = self.domain_clock_progress_pause(&point);
        pause.wait_until_reached().await;
    }

    pub async fn release_domain_clock_progress(&self, domain: &str) {
        let point = DomainClockProgressPausePoint {
            domain: domain.to_ascii_lowercase(),
            node: None,
        };
        let pause = self.domain_clock_progress_pause(&point);
        pause.release();
        pause.wait_until_delivered().await;
        self.inner.domain_clock_progress_pauses.remove(&point);
    }

    pub async fn release_domain_clock_progress_on(&self, domain: &str, node: &ClusterNodeName) {
        let point = DomainClockProgressPausePoint {
            domain: domain.to_ascii_lowercase(),
            node: Some(node.clone()),
        };
        let pause = self.domain_clock_progress_pause(&point);
        pause.release();
        pause.wait_until_delivered().await;
        self.inner.domain_clock_progress_pauses.remove(&point);
    }

    pub fn release_all_domain_clock_progress(&self) {
        for pause in &self.inner.domain_clock_progress_pauses {
            pause.release();
        }
        self.inner.domain_clock_progress_pauses.clear();
    }

    pub fn set_syslog_ingestor_bind_ip(&self, node_id: ClusterNodeName, host: IpAddr) {
        self.inner.syslog_ingestor_bind_ips.insert(node_id, host);
    }

    pub fn set_branch_instance_expiration_scan_interval(&self, interval: Duration) {
        *self.inner.branch_instance_expiration_scan_interval.write() = Some(interval);
    }

    pub fn set_domain_drain_timeout(&self, timeout: Duration) {
        *self.inner.domain_drain_timeout.write() = Some(timeout);
    }

    pub fn set_entity_gate_deadline(&self, deadline: Duration) {
        *self.inner.entity_gate_deadline.write() = Some(deadline);
    }

    pub fn set_scheduler_mode(&self, mode: SchedulerMode) {
        *self.inner.scheduler_mode.write() = mode;
    }

    pub fn request_leadership_transfer(
        &self,
        from_node_id: ClusterNodeName,
        to_node_id: ClusterNodeName,
    ) {
        self.inner
            .leadership_transfers
            .send(LeadershipTransferRequest {
                from_node_id,
                to_node_id,
            })
            .means_shutdown("leadership transfer watcher");
    }

    pub(crate) fn emitter_should_fail(&self, emitter: &EmitterName) -> bool {
        self.inner
            .emitter_faults
            .get(&emitter.as_str().to_ascii_lowercase())
            .is_some_and(|mode| *mode == EmitterFaultMode::Fail)
    }

    pub(crate) fn emitter_should_stall(&self, emitter: &EmitterName) -> bool {
        self.inner
            .emitter_faults
            .get(&emitter.as_str().to_ascii_lowercase())
            .is_some_and(|mode| *mode == EmitterFaultMode::Stall)
    }

    pub(crate) fn ingestor_is_failed(&self, ingestor: &IngestorName) -> bool {
        self.inner
            .failed_ingestors
            .contains_key(&ingestor.as_str().to_ascii_lowercase())
    }

    pub(crate) fn otel_client_is_unavailable(&self, emitter: &EmitterName) -> bool {
        self.inner
            .unavailable_otel_clients
            .contains_key(&emitter.as_str().to_ascii_lowercase())
    }

    /// Consumes an armed fault so the rollback publication can still reach the cluster.
    pub(crate) fn take_armed_schedule_publication_fault(&self, domain: &DomainName) -> bool {
        self.inner
            .failed_schedule_publications
            .remove(&domain.as_str().to_ascii_lowercase())
            .is_some()
    }

    /// Consumes the domain-scoped timeout only at the coordinator's drain-observation seam.
    pub(crate) fn take_forced_entity_drain_timeout(&self, domain: &DomainName) -> bool {
        self.inner
            .forced_entity_drain_timeouts
            .remove(domain)
            .is_some()
    }

    /// Consumes an armed drop for `node_id` when the node handles its next transaction command.
    pub(crate) fn take_armed_transaction_binding_drop(&self, node_id: &ClusterNodeName) -> bool {
        self.inner
            .transaction_binding_drops
            .remove(node_id)
            .is_some()
    }

    /// Record the executor whose bulk workers a scenario may fill.
    pub(crate) fn register_bulk_executor(&self, node_id: ClusterNodeName, executor: Executor) {
        self.inner.bulk_executions.insert(
            node_id,
            NodeBulkExecution {
                executor,
                holders: Arc::default(),
            },
        );
    }

    pub(crate) async fn pause_transaction_commit_after_progress_if_armed(
        &self,
        node_id: &ClusterNodeName,
        completed_statements: usize,
    ) {
        self.pause_command_if_armed(CommandPausePoint::TransactionCommit {
            node_id: node_id.clone(),
            completed_statements,
        })
        .await;
    }

    pub(crate) async fn pause_command_admission_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::Admission(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_entity_gate_if_armed(&self, domain: &DomainName) {
        let key = domain.as_str().to_ascii_lowercase();
        let Some(pause) = self
            .inner
            .entity_gate_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.entity_gate_pauses.remove(&key);
    }

    pub(crate) async fn pause_remote_relay_admission_if_armed(
        &self,
        domain: &DomainName,
        branch: Option<&str>,
    ) {
        let exact_key = RemoteRelayAdmissionPauseKey {
            domain: domain.as_str().to_ascii_lowercase(),
            branch: branch.map(str::to_string),
        };
        let exact_pause = self
            .inner
            .remote_relay_admission_pauses
            .get(&exact_key)
            .map(|pause| pause.value().clone());
        let (key, pause) = if let Some(pause) = exact_pause {
            (exact_key, pause)
        } else if exact_key.branch.is_some() {
            let domain_key = RemoteRelayAdmissionPauseKey {
                domain: exact_key.domain,
                branch: None,
            };
            let Some(pause) = self
                .inner
                .remote_relay_admission_pauses
                .get(&domain_key)
                .map(|pause| pause.value().clone())
            else {
                return;
            };
            (domain_key, pause)
        } else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.remote_relay_admission_pauses.remove(&key);
    }

    pub(crate) async fn pause_ownership_handoff_after_preparation_if_armed(
        &self,
        domain: &DomainName,
    ) {
        let key = domain.as_str().to_ascii_lowercase();
        let Some(pause) = self
            .inner
            .ownership_handoff_preparation_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.ownership_handoff_preparation_pauses.remove(&key);
    }

    pub(crate) async fn pause_domain_clock_progress_if_armed(
        &self,
        domain: &DomainName,
        node: &ClusterNodeName,
    ) -> bool {
        let exact = DomainClockProgressPausePoint {
            domain: domain.as_str().to_ascii_lowercase(),
            node: Some(node.clone()),
        };
        let any_node = DomainClockProgressPausePoint {
            domain: domain.as_str().to_ascii_lowercase(),
            node: None,
        };
        let pause = if let Some(pause) = self.inner.domain_clock_progress_pauses.get(&exact) {
            pause.value().clone()
        } else if let Some(pause) = self.inner.domain_clock_progress_pauses.get(&any_node) {
            pause.value().clone()
        } else {
            return false;
        };
        pause.reach();
        pause.wait_until_released().await;
        true
    }

    pub(crate) fn mark_domain_clock_progress_delivered(
        &self,
        domain: &DomainName,
        node: &ClusterNodeName,
    ) {
        let exact = DomainClockProgressPausePoint {
            domain: domain.as_str().to_ascii_lowercase(),
            node: Some(node.clone()),
        };
        let any_node = DomainClockProgressPausePoint {
            domain: domain.as_str().to_ascii_lowercase(),
            node: None,
        };
        if let Some(pause) = self
            .inner
            .domain_clock_progress_pauses
            .get(&exact)
            .or_else(|| self.inner.domain_clock_progress_pauses.get(&any_node))
        {
            pause.mark_delivered();
        }
    }

    pub(crate) fn syslog_ingestor_bind_addr(
        &self,
        node_id: &ClusterNodeName,
        configured: &str,
    ) -> String {
        let Some(host) = self
            .inner
            .syslog_ingestor_bind_ips
            .get(node_id)
            .map(|host| *host.value())
        else {
            return configured.to_string();
        };
        let Ok(mut addr) = configured.parse::<SocketAddr>() else {
            return configured.to_string();
        };
        addr.set_ip(host);
        addr.to_string()
    }

    pub(crate) fn branch_instance_expiration_scan_interval(&self) -> Option<Duration> {
        *self.inner.branch_instance_expiration_scan_interval.read()
    }

    #[doc(hidden)]
    pub fn domain_drain_timeout(&self) -> Option<Duration> {
        *self.inner.domain_drain_timeout.read()
    }

    pub(crate) fn state_replica_polling_is_paused(&self) -> bool {
        self.inner
            .state_replica_polling_paused
            .load(Ordering::Acquire)
    }

    pub(crate) fn entity_gate_deadline(&self) -> Option<Duration> {
        *self.inner.entity_gate_deadline.read()
    }

    pub(crate) fn scheduler_mode(&self) -> SchedulerMode {
        *self.inner.scheduler_mode.read()
    }

    pub(crate) fn subscribe_leadership_transfers(
        &self,
    ) -> broadcast::Receiver<LeadershipTransferRequest> {
        self.inner.leadership_transfers.subscribe()
    }

    fn arm_command_pause(&self, point: CommandPausePoint) {
        self.inner
            .command_pauses
            .insert(point, Arc::new(TestPause::default()));
    }

    fn command_pause(&self, point: &CommandPausePoint) -> Arc<TestPause> {
        let Some(pause) = self.inner.command_pauses.get(point) else {
            panic!("command pause at {point:?} is not armed");
        };
        pause.value().clone()
    }

    fn release_command_pause(&self, point: &CommandPausePoint) {
        let pause = self.command_pause(point);
        pause.release();
    }

    async fn wait_for_command_pause(&self, point: &CommandPausePoint) {
        let pause = self.command_pause(point);
        pause.wait_until_reached().await;
    }

    async fn pause_command_if_armed(&self, point: CommandPausePoint) {
        let Some(pause) = self
            .inner
            .command_pauses
            .get(&point)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.command_pauses.remove(&point);
    }

    fn entity_gate_pause(&self, key: &str) -> Arc<TestPause> {
        let Some(pause) = self.inner.entity_gate_pauses.get(key) else {
            panic!("entity gate pause for domain '{key}' is not armed");
        };
        pause.value().clone()
    }

    fn remote_relay_admission_pause(&self, key: &RemoteRelayAdmissionPauseKey) -> Arc<TestPause> {
        let Some(pause) = self.inner.remote_relay_admission_pauses.get(key) else {
            panic!(
                "remote relay admission pause for domain '{}' and branch {:?} is not armed",
                key.domain, key.branch
            );
        };
        pause.value().clone()
    }

    fn ownership_handoff_preparation_pause(&self, key: &str) -> Arc<TestPause> {
        let Some(pause) = self.inner.ownership_handoff_preparation_pauses.get(key) else {
            panic!("ownership handoff preparation pause for domain '{key}' is not armed");
        };
        pause.value().clone()
    }

    fn domain_clock_progress_pause(&self, point: &DomainClockProgressPausePoint) -> Arc<TestPause> {
        let Some(pause) = self.inner.domain_clock_progress_pauses.get(point) else {
            panic!("domain clock progress pause for '{point:?}' is not armed");
        };
        pause.value().clone()
    }
}

impl Default for TestPause {
    fn default() -> Self {
        Self {
            state: watch::channel(TestPauseState::default()).0,
        }
    }
}

impl TestPause {
    fn reach(&self) {
        self.state.send_modify(|state| state.reached = true);
    }

    async fn wait_until_reached(&self) {
        self.state
            .subscribe()
            .wait_for(|state| state.reached)
            .await
            .assured("the pause owns its state sender for the full wait");
    }

    async fn wait_until_released(&self) {
        self.state
            .subscribe()
            .wait_for(|state| state.released)
            .await
            .assured("the pause owns its state sender for the full wait");
    }

    async fn wait_until_delivered(&self) {
        self.state
            .subscribe()
            .wait_for(|state| state.delivered)
            .await
            .assured("the pause owns its state sender for the full wait");
    }

    fn release(&self) {
        self.state.send_modify(|state| state.released = true);
    }

    fn mark_delivered(&self) {
        self.state.send_modify(|state| state.delivered = true);
    }
}
