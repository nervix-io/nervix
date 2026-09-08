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
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use ahash::RandomState;
use dashmap::DashMap;
use nervix_models::{ClusterNodeName, DomainName, EmitterName, IngestorName};
use parking_lot::RwLock;
use tokio::sync::{Notify, broadcast};
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
    transaction_binding_drops: DashMap<ClusterNodeName, (), RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    command_pauses: DashMap<CommandPausePoint, Arc<CommandPause>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    entity_gate_pauses: DashMap<String, Arc<EntityGatePause>, RandomState>,
    syslog_ingestor_bind_ips: DashMap<ClusterNodeName, IpAddr, RandomState>,
    branch_instance_expiration_scan_interval: RwLock<Option<Duration>>,
    domain_drain_timeout: RwLock<Option<Duration>>,
    entity_gate_deadline: RwLock<Option<Duration>>,
    scheduler_mode: RwLock<SchedulerMode>,
    leadership_transfers: broadcast::Sender<LeadershipTransferRequest>,
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

/// The command boundary a test controls without racing an election against a request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CommandPausePoint {
    Admission(ClusterNodeName),
    TransactionCommit {
        node_id: ClusterNodeName,
        completed_statements: usize,
    },
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
                transaction_binding_drops: DashMap::default(),
                command_pauses: DashMap::default(),
                entity_gate_pauses: DashMap::default(),
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

    pub fn drop_transaction_bindings_on(&self, node_id: ClusterNodeName) {
        self.inner.transaction_binding_drops.insert(node_id, ());
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
            Arc::new(EntityGatePause::default()),
        );
    }

    pub async fn wait_for_entity_gate_pause(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self.entity_gate_pause(&key);
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
        let pause = self.entity_gate_pause(&domain.to_ascii_lowercase());
        pause.released.store(true, Ordering::Release);
        pause.release_notify.notify_waiters();
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
        let _ = self
            .inner
            .leadership_transfers
            .send(LeadershipTransferRequest {
                from_node_id,
                to_node_id,
            });
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

    /// Consumes an armed drop for `node_id` when the node handles its next transaction command.
    pub(crate) fn take_armed_transaction_binding_drop(&self, node_id: &ClusterNodeName) -> bool {
        self.inner
            .transaction_binding_drops
            .remove(node_id)
            .is_some()
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
        self.inner.entity_gate_pauses.remove(&key);
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

    pub(crate) fn domain_drain_timeout(&self) -> Option<Duration> {
        *self.inner.domain_drain_timeout.read()
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
            .insert(point, Arc::new(CommandPause::default()));
    }

    fn command_pause(&self, point: &CommandPausePoint) -> Arc<CommandPause> {
        let Some(pause) = self.inner.command_pauses.get(point) else {
            panic!("command pause at {point:?} is not armed");
        };
        pause.value().clone()
    }

    fn release_command_pause(&self, point: &CommandPausePoint) {
        let pause = self.command_pause(point);
        pause.released.store(true, Ordering::Release);
        pause.release_notify.notify_waiters();
    }

    async fn wait_for_command_pause(&self, point: &CommandPausePoint) {
        let pause = self.command_pause(point);
        while !pause.reached.load(Ordering::Acquire) {
            tokio::task::consume_budget().await;
            let notified = pause.reached_notify.notified();
            if pause.reached.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
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
        self.inner.command_pauses.remove(&point);
    }

    fn entity_gate_pause(&self, key: &str) -> Arc<EntityGatePause> {
        let Some(pause) = self.inner.entity_gate_pauses.get(key) else {
            panic!("entity gate pause for domain '{key}' is not armed");
        };
        pause.value().clone()
    }
}
