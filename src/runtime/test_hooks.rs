// Only the seams the `testing` feature compiles in use these.
#[cfg(feature = "testing")]
use std::net::SocketAddr;
use std::{
    net::IpAddr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

use ahash::RandomState;
use dashmap::DashMap;
use nervix_execution::{CpuClass, Executor, MemoryClass};
#[cfg(feature = "testing")]
use nervix_models::DomainName;
use nervix_models::{ClusterNodeName, EmitterName, IngestorName};
use tokio::sync::{Notify, broadcast};
use triomphe::Arc;

#[derive(Debug, Default)]
pub struct EmitterFaultInjector {
    emitters: DashMap<String, EmitterFaultMode, RandomState>,
}

#[derive(Debug, Default)]
pub struct IngestorFaultInjector {
    ingestors: DashMap<String, (), RandomState>,
}

#[derive(Debug, Default)]
pub struct OtelClientFaultInjector {
    unavailable_emitters: DashMap<String, (), RandomState>,
}

/// Fails the next schedule publication for a domain so tests can observe how a committed model
/// mutation recovers when the new schedule never reaches the cluster.
#[derive(Debug, Default)]
pub struct SchedulePublicationFaultInjector {
    domains: DashMap<String, (), RandomState>,
}

#[derive(Debug, Default)]
pub(crate) struct SyslogIngestorBindAddressOverrides {
    hosts: DashMap<ClusterNodeName, IpAddr, RandomState>,
}

/// Fills every bulk worker on a node and holds them until a scenario releases them, so a scenario
/// can prove that management work is admitted while bulk work is not.
///
/// Each node registers its executor here as it starts, and the scenario submits the occupying jobs
/// itself. Registering a handle costs one map insert and is the same in every build, so the node's
/// own startup path does not differ between them.
#[derive(Debug, Default)]
pub struct BulkExecutionOccupancy {
    nodes: DashMap<ClusterNodeName, NodeBulkExecution, RandomState>,
}

#[derive(Debug)]
struct NodeBulkExecution {
    executor: Executor,
    /// One holder per occupying job. Dropping them releases every job at once, and while they are
    /// held each job parks rather than spinning, so an occupied class costs no CPU.
    holders: Arc<parking_lot::Mutex<Vec<std::sync::mpsc::Sender<()>>>>,
}

/// Drops a node's leader-local transaction session bindings on its next transaction command, so
/// tests can reproduce the soft state a node does not have after a leadership change.
#[derive(Debug, Default)]
pub struct TransactionBindingDropInjector {
    nodes: DashMap<ClusterNodeName, (), RandomState>,
}

#[derive(Debug, Default)]
pub(crate) struct CommandPauseInjector {
    pauses: DashMap<CommandPausePoint, Arc<CommandPause>, RandomState>,
}

#[derive(Debug, Default)]
struct EntityGatePauseInjector {
    pauses: DashMap<String, Arc<EntityGatePause>, RandomState>,
}

#[derive(Debug, Default)]
struct DomainClockProgressPauseInjector {
    pauses: DashMap<String, Arc<DomainClockProgressPause>, RandomState>,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimePauseInjectors {
    entity_gates: EntityGatePauseInjector,
    domain_clock_progress: DomainClockProgressPauseInjector,
}

#[derive(Debug, Default)]
struct CommandPause {
    reached: AtomicBool,
    released: AtomicBool,
    reached_notify: Notify,
    release_notify: Notify,
}

#[derive(Debug, Default)]
struct EntityGatePause {
    reached: AtomicBool,
    released: AtomicBool,
    reached_notify: Notify,
    release_notify: Notify,
}

#[derive(Debug, Default)]
struct DomainClockProgressPause {
    reached: AtomicBool,
    released: AtomicBool,
    delivered: AtomicBool,
    reached_notify: Notify,
    release_notify: Notify,
    delivered_notify: Notify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EmitterFaultMode {
    Fail,
    Stall,
}

#[derive(Clone, Debug)]
pub struct RuntimeTestHooks {
    pub emitter_faults: Arc<EmitterFaultInjector>,
    pub ingestor_faults: Arc<IngestorFaultInjector>,
    pub otel_client_faults: Arc<OtelClientFaultInjector>,
    pub schedule_publication_faults: Arc<SchedulePublicationFaultInjector>,
    pub transaction_binding_drops: Arc<TransactionBindingDropInjector>,
    pub bulk_execution_occupancy: Arc<BulkExecutionOccupancy>,
    pub(crate) command_pauses: Arc<CommandPauseInjector>,
    pub(crate) runtime_pauses: Arc<RuntimePauseInjectors>,
    pub(crate) syslog_ingestor_bind_address_overrides: Arc<SyslogIngestorBindAddressOverrides>,
    pub branch_instance_expiration_scan_interval: Option<Duration>,
    pub domain_drain_timeout: Option<Duration>,
    pub entity_gate_deadline: Option<Duration>,
    pub leadership_transfers: broadcast::Sender<LeadershipTransferRequest>,
}

#[derive(Clone, Debug)]
pub struct LeadershipTransferRequest {
    pub from_node_id: ClusterNodeName,
    pub to_node_id: ClusterNodeName,
}

impl Default for RuntimeTestHooks {
    fn default() -> Self {
        let (leadership_transfers, _) = broadcast::channel(16);
        Self {
            emitter_faults: Arc::default(),
            ingestor_faults: Arc::default(),
            otel_client_faults: Arc::default(),
            schedule_publication_faults: Arc::default(),
            transaction_binding_drops: Arc::default(),
            bulk_execution_occupancy: Arc::default(),
            command_pauses: Arc::default(),
            runtime_pauses: Arc::default(),
            syslog_ingestor_bind_address_overrides: Arc::default(),
            branch_instance_expiration_scan_interval: None,
            domain_drain_timeout: None,
            entity_gate_deadline: None,
            leadership_transfers,
        }
    }
}

impl RuntimeTestHooks {
    pub fn pause_command_admission_on(&self, node_id: ClusterNodeName) {
        self.command_pauses
            .arm(CommandPausePoint::Admission(node_id));
    }

    pub async fn wait_for_command_admission_pause(&self, node_id: &ClusterNodeName) {
        self.command_pauses
            .wait_for_pause(&CommandPausePoint::Admission(node_id.clone()))
            .await;
    }

    pub fn release_command_admission_pause(&self, node_id: &ClusterNodeName) {
        self.command_pauses
            .release(&CommandPausePoint::Admission(node_id.clone()));
    }

    pub fn set_syslog_ingestor_bind_ip(&self, node_id: ClusterNodeName, host: IpAddr) {
        self.syslog_ingestor_bind_address_overrides
            .hosts
            .insert(node_id, host);
    }

    pub fn request_leadership_transfer(
        &self,
        from_node_id: ClusterNodeName,
        to_node_id: ClusterNodeName,
    ) {
        let _ = self.leadership_transfers.send(LeadershipTransferRequest {
            from_node_id,
            to_node_id,
        });
    }

    pub fn drop_transaction_bindings_on(&self, node_id: ClusterNodeName) {
        self.transaction_binding_drops.nodes.insert(node_id, ());
    }

    pub fn pause_transaction_commit_after(
        &self,
        node_id: ClusterNodeName,
        completed_statements: usize,
    ) {
        self.command_pauses
            .arm(CommandPausePoint::TransactionCommit {
                node_id,
                completed_statements,
            });
    }

    pub fn pause_entity_gate(&self, domain: impl Into<String>) {
        self.runtime_pauses.entity_gates.pauses.insert(
            domain.into().to_ascii_lowercase(),
            Arc::new(EntityGatePause::default()),
        );
    }

    pub async fn wait_for_entity_gate_pause(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self
            .runtime_pauses
            .entity_gates
            .pauses
            .get(&key)
            .unwrap_or_else(|| panic!("entity gate pause for domain '{domain}' is not armed"))
            .clone();
        while !pause.reached.load(Ordering::Acquire) {
            tokio::task::consume_budget().await;
            let notified = pause.reached_notify.notified();
            if pause.reached.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    pub fn release_entity_gate_pause(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self
            .runtime_pauses
            .entity_gates
            .pauses
            .get(&key)
            .unwrap_or_else(|| panic!("entity gate pause for domain '{domain}' is not armed"))
            .clone();
        pause.released.store(true, Ordering::Release);
        pause.release_notify.notify_waiters();
    }

    pub fn pause_domain_clock_progress(&self, domain: impl Into<String>) {
        self.runtime_pauses
            .domain_clock_progress
            .arm(domain.into().to_ascii_lowercase());
    }

    pub async fn wait_for_domain_clock_progress_pause(&self, domain: &str) {
        self.runtime_pauses
            .domain_clock_progress
            .wait_for_pause(&domain.to_ascii_lowercase())
            .await;
    }

    pub async fn release_domain_clock_progress(&self, domain: &str) {
        self.runtime_pauses
            .domain_clock_progress
            .release_and_wait(&domain.to_ascii_lowercase())
            .await;
    }

    pub fn release_all_domain_clock_progress(&self) {
        self.runtime_pauses.domain_clock_progress.release_all();
    }

    pub async fn wait_for_transaction_commit_pause(
        &self,
        node_id: &ClusterNodeName,
        completed_statements: usize,
    ) {
        self.command_pauses
            .wait_for_pause(&CommandPausePoint::TransactionCommit {
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
        self.command_pauses
            .release(&CommandPausePoint::TransactionCommit {
                node_id: node_id.clone(),
                completed_statements,
            });
    }
}

#[cfg(feature = "testing")]
impl SyslogIngestorBindAddressOverrides {
    pub(crate) fn resolve(&self, node_id: &ClusterNodeName, configured: &str) -> String {
        let Some(host) = self.hosts.get(node_id).map(|host| *host.value()) else {
            return configured.to_string();
        };
        let Ok(mut addr) = configured.parse::<SocketAddr>() else {
            return configured.to_string();
        };
        addr.set_ip(host);
        addr.to_string()
    }
}

impl TransactionBindingDropInjector {
    /// Consumes an armed drop for `node_id`, returning whether the node should forget its
    /// transaction session bindings now.
    #[cfg(feature = "testing")]
    pub(crate) fn take(&self, node_id: &ClusterNodeName) -> bool {
        self.nodes.remove(node_id).is_some()
    }
}

/// The command boundary a test controls without racing an election against a request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum CommandPausePoint {
    Admission(ClusterNodeName),
    TransactionCommit {
        node_id: ClusterNodeName,
        completed_statements: usize,
    },
}

impl CommandPauseInjector {
    fn arm(&self, point: CommandPausePoint) {
        self.pauses.insert(point, Arc::new(CommandPause::default()));
    }

    fn release(&self, point: &CommandPausePoint) {
        let pause = self
            .pauses
            .get(point)
            .unwrap_or_else(|| panic!("command pause at {point:?} is not armed"))
            .clone();
        pause.released.store(true, Ordering::Release);
        pause.release_notify.notify_waiters();
    }

    async fn wait_for_pause(&self, point: &CommandPausePoint) {
        let pause = self
            .pauses
            .get(point)
            .unwrap_or_else(|| panic!("command pause at {point:?} is not armed"))
            .clone();
        while !pause.reached.load(Ordering::Acquire) {
            tokio::task::consume_budget().await;
            let notified = pause.reached_notify.notified();
            if pause.reached.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_if_armed(&self, key: CommandPausePoint) {
        let Some(pause) = self.pauses.get(&key).map(|pause| pause.clone()) else {
            return;
        };
        pause.reached.store(true, Ordering::Release);
        pause.reached_notify.notify_waiters();
        while !pause.released.load(Ordering::Acquire) {
            tokio::task::consume_budget().await;
            let notified = pause.release_notify.notified();
            if pause.released.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        self.pauses.remove(&key);
    }
}

impl BulkExecutionOccupancy {
    /// Record the executor whose bulk workers a scenario may fill.
    pub(crate) fn register(&self, node_id: ClusterNodeName, executor: Executor) {
        self.nodes.insert(
            node_id,
            NodeBulkExecution {
                executor,
                holders: Arc::default(),
            },
        );
    }

    /// Fill every bulk worker on `node_id` and return once they are all actually running.
    pub async fn occupy(&self, node_id: &ClusterNodeName) {
        let node = self
            .nodes
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
                let _ = executor
                    .run_cpu(
                        CpuClass::Bulk,
                        reservation,
                        move |_charge, _cancellation| {
                            started.fetch_add(1, Ordering::AcqRel);
                            // Parks until the scenario drops the holder, so the worker is occupied
                            // without burning the CPU the scenario is measuring.
                            let _ = held.recv();
                        },
                    )
                    .await;
            });
        }
        while started.load(Ordering::Acquire) < workers {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    }

    pub fn release(&self, node_id: &ClusterNodeName) {
        self.nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("node '{node_id}' has not registered its executor"))
            .holders
            .lock()
            .clear();
    }
}

impl EntityGatePauseInjector {
    #[cfg(feature = "testing")]
    pub(crate) async fn pause_if_armed(&self, domain: &DomainName) {
        let key = domain.as_str().to_ascii_lowercase();
        let Some(pause) = self.pauses.get(&key).map(|pause| pause.clone()) else {
            return;
        };
        pause.reached.store(true, Ordering::Release);
        pause.reached_notify.notify_waiters();
        while !pause.released.load(Ordering::Acquire) {
            tokio::task::consume_budget().await;
            let notified = pause.release_notify.notified();
            if pause.released.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        self.pauses.remove(&key);
    }
}

#[cfg(feature = "testing")]
impl RuntimePauseInjectors {
    pub(crate) async fn pause_entity_gate_if_armed(&self, domain: &DomainName) {
        self.entity_gates.pause_if_armed(domain).await;
    }

    pub(crate) async fn pause_domain_clock_progress_if_armed(&self, domain: &DomainName) -> bool {
        self.domain_clock_progress.pause_if_armed(domain).await
    }

    pub(crate) fn mark_domain_clock_progress_delivered(&self, domain: &DomainName) {
        self.domain_clock_progress.mark_delivered(domain);
    }
}

impl DomainClockProgressPause {
    async fn wait_until_reached(&self) {
        while !self.reached.load(Ordering::Acquire) {
            tokio::task::consume_budget().await;
            let notified = self.reached_notify.notified();
            if self.reached.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    #[cfg(feature = "testing")]
    async fn wait_until_released(&self) {
        while !self.released.load(Ordering::Acquire) {
            tokio::task::consume_budget().await;
            let notified = self.release_notify.notified();
            if self.released.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    async fn wait_until_delivered(&self) {
        while !self.delivered.load(Ordering::Acquire) {
            tokio::task::consume_budget().await;
            let notified = self.delivered_notify.notified();
            if self.delivered.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_waiters();
    }

    #[cfg(feature = "testing")]
    fn mark_delivered(&self) {
        self.delivered.store(true, Ordering::Release);
        self.delivered_notify.notify_waiters();
    }
}

impl DomainClockProgressPauseInjector {
    fn arm(&self, domain: String) {
        self.pauses
            .insert(domain, Arc::new(DomainClockProgressPause::default()));
    }

    async fn wait_for_pause(&self, domain: &str) {
        let pause = self
            .pauses
            .get(domain)
            .unwrap_or_else(|| panic!("domain clock progress pause for '{domain}' is not armed"))
            .clone();
        pause.wait_until_reached().await;
    }

    async fn release_and_wait(&self, domain: &str) {
        let pause = self
            .pauses
            .get(domain)
            .unwrap_or_else(|| panic!("domain clock progress pause for '{domain}' is not armed"))
            .clone();
        pause.release();
        pause.wait_until_delivered().await;
        self.pauses.remove(domain);
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_if_armed(&self, domain: &DomainName) -> bool {
        let key = domain.as_str().to_ascii_lowercase();
        let Some(pause) = self.pauses.get(&key).map(|pause| pause.clone()) else {
            return false;
        };
        pause.reached.store(true, Ordering::Release);
        pause.reached_notify.notify_waiters();
        pause.wait_until_released().await;
        true
    }

    #[cfg(feature = "testing")]
    pub(crate) fn mark_delivered(&self, domain: &DomainName) {
        let key = domain.as_str().to_ascii_lowercase();
        if let Some(pause) = self.pauses.get(&key) {
            pause.mark_delivered();
        }
    }

    fn release_all(&self) {
        for pause in &self.pauses {
            pause.release();
        }
        self.pauses.clear();
    }
}

impl IngestorFaultInjector {
    pub fn fail_ingestor(&self, ingestor: &str) {
        self.ingestors.insert(ingestor.to_ascii_lowercase(), ());
    }

    pub fn clear_ingestor(&self, ingestor: &str) {
        self.ingestors.remove(&ingestor.to_ascii_lowercase());
    }

    pub(super) fn is_failed(&self, ingestor: &IngestorName) -> bool {
        self.ingestors
            .contains_key(&ingestor.as_str().to_ascii_lowercase())
    }
}

impl SchedulePublicationFaultInjector {
    pub fn fail_next_publication(&self, domain: &str) {
        self.domains.insert(domain.to_ascii_lowercase(), ());
    }

    /// Consumes an armed fault so the rollback republication that follows a failed publication can
    /// still reach the cluster.
    #[cfg(feature = "testing")]
    pub(crate) fn take_armed_fault(&self, domain: &DomainName) -> bool {
        self.domains
            .remove(&domain.as_str().to_ascii_lowercase())
            .is_some()
    }
}

impl OtelClientFaultInjector {
    pub fn fail_unavailable(&self, emitter: &str) {
        self.unavailable_emitters
            .insert(emitter.to_ascii_lowercase(), ());
    }

    pub fn clear_emitter(&self, emitter: &str) {
        self.unavailable_emitters
            .remove(&emitter.to_ascii_lowercase());
    }

    pub(super) fn is_unavailable(&self, emitter: &EmitterName) -> bool {
        self.unavailable_emitters
            .contains_key(&emitter.as_str().to_ascii_lowercase())
    }
}

impl EmitterFaultInjector {
    pub fn fail_emitter(&self, emitter: &str) {
        self.emitters
            .insert(emitter.to_ascii_lowercase(), EmitterFaultMode::Fail);
    }

    pub fn stall_emitter(&self, emitter: &str) {
        self.emitters
            .insert(emitter.to_ascii_lowercase(), EmitterFaultMode::Stall);
    }

    pub fn clear_emitter(&self, emitter: &str) {
        self.emitters.remove(&emitter.to_ascii_lowercase());
    }

    pub fn clear_all(&self) {
        self.emitters.clear();
    }

    pub(super) fn fault_mode(&self, emitter: &EmitterName) -> Option<EmitterFaultMode> {
        self.emitters
            .get(&emitter.as_str().to_ascii_lowercase())
            .map(|mode| *mode)
    }
}
