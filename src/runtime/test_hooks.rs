use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use ahash::RandomState;
use dashmap::DashMap;
#[cfg(feature = "testing")]
use nervix_models::Domain;
use nervix_models::Identifier;
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

/// Drops a node's leader-local transaction session bindings on its next transaction command, so
/// tests can reproduce the soft state a node does not have after a leadership change.
#[derive(Debug, Default)]
pub struct TransactionBindingDropInjector {
    nodes: DashMap<String, (), RandomState>,
}

#[derive(Debug, Default)]
pub(crate) struct TransactionCommitPauseInjector {
    pauses: DashMap<(String, usize), Arc<TransactionCommitPause>, RandomState>,
}

#[derive(Debug, Default)]
pub(crate) struct EntityGatePauseInjector {
    pauses: DashMap<String, Arc<EntityGatePause>, RandomState>,
}

#[derive(Debug, Default)]
struct TransactionCommitPause {
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
    pub(crate) transaction_commit_pauses: Arc<TransactionCommitPauseInjector>,
    pub(crate) entity_gate_pauses: Arc<EntityGatePauseInjector>,
    pub branch_instance_expiration_scan_interval: Option<Duration>,
    pub domain_drain_timeout: Option<Duration>,
    pub entity_gate_deadline: Option<Duration>,
    pub leadership_transfers: broadcast::Sender<LeadershipTransferRequest>,
}

#[derive(Clone, Debug)]
pub struct LeadershipTransferRequest {
    pub from_node_id: String,
    pub to_node_id: String,
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
            transaction_commit_pauses: Arc::default(),
            entity_gate_pauses: Arc::default(),
            branch_instance_expiration_scan_interval: None,
            domain_drain_timeout: None,
            entity_gate_deadline: None,
            leadership_transfers,
        }
    }
}

impl RuntimeTestHooks {
    pub fn request_leadership_transfer(&self, from_node_id: String, to_node_id: String) {
        let _ = self.leadership_transfers.send(LeadershipTransferRequest {
            from_node_id,
            to_node_id,
        });
    }

    pub fn drop_transaction_bindings_on(&self, node_id: impl Into<String>) {
        self.transaction_binding_drops
            .nodes
            .insert(node_id.into(), ());
    }

    pub fn pause_transaction_commit_after(
        &self,
        node_id: impl Into<String>,
        completed_statements: usize,
    ) {
        self.transaction_commit_pauses.pauses.insert(
            (node_id.into(), completed_statements),
            Arc::new(TransactionCommitPause::default()),
        );
    }

    pub fn pause_entity_gate(&self, domain: impl Into<String>) {
        self.entity_gate_pauses.pauses.insert(
            domain.into().to_ascii_lowercase(),
            Arc::new(EntityGatePause::default()),
        );
    }

    pub async fn wait_for_entity_gate_pause(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self
            .entity_gate_pauses
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
            .entity_gate_pauses
            .pauses
            .get(&key)
            .unwrap_or_else(|| panic!("entity gate pause for domain '{domain}' is not armed"))
            .clone();
        pause.released.store(true, Ordering::Release);
        pause.release_notify.notify_waiters();
    }

    pub async fn wait_for_transaction_commit_pause(
        &self,
        node_id: &str,
        completed_statements: usize,
    ) {
        let key = (node_id.to_string(), completed_statements);
        let pause = self
            .transaction_commit_pauses
            .pauses
            .get(&key)
            .unwrap_or_else(|| {
                panic!(
                    "transaction commit pause for node '{node_id}' after {completed_statements} \
                     statements is not armed"
                )
            })
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

    pub fn release_transaction_commit_pause(&self, node_id: &str, completed_statements: usize) {
        let key = (node_id.to_string(), completed_statements);
        let pause = self
            .transaction_commit_pauses
            .pauses
            .get(&key)
            .unwrap_or_else(|| {
                panic!(
                    "transaction commit pause for node '{node_id}' after {completed_statements} \
                     statements is not armed"
                )
            })
            .clone();
        pause.released.store(true, Ordering::Release);
        pause.release_notify.notify_waiters();
    }
}

impl TransactionBindingDropInjector {
    /// Consumes an armed drop for `node_id`, returning whether the node should forget its
    /// transaction session bindings now.
    #[cfg(feature = "testing")]
    pub(crate) fn take(&self, node_id: &str) -> bool {
        self.nodes.remove(node_id).is_some()
    }
}

impl TransactionCommitPauseInjector {
    #[cfg(feature = "testing")]
    pub(crate) async fn pause_if_armed(&self, node_id: &str, completed_statements: usize) {
        let key = (node_id.to_string(), completed_statements);
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

impl EntityGatePauseInjector {
    #[cfg(feature = "testing")]
    pub(crate) async fn pause_if_armed(&self, domain: &Domain) {
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

impl IngestorFaultInjector {
    pub fn fail_ingestor(&self, ingestor: &str) {
        self.ingestors.insert(ingestor.to_ascii_lowercase(), ());
    }

    pub fn clear_ingestor(&self, ingestor: &str) {
        self.ingestors.remove(&ingestor.to_ascii_lowercase());
    }

    pub(super) fn is_failed(&self, ingestor: &Identifier) -> bool {
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
    pub(crate) fn take_armed_fault(&self, domain: &Domain) -> bool {
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

    pub(super) fn is_unavailable(&self, emitter: &Identifier) -> bool {
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

    pub(super) fn fault_mode(&self, emitter: &Identifier) -> Option<EmitterFaultMode> {
        self.emitters
            .get(&emitter.as_str().to_ascii_lowercase())
            .map(|mode| *mode)
    }
}
