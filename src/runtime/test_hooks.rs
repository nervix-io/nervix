use std::{sync::Arc, time::Duration};

use ahash::RandomState;
use dashmap::DashMap;
use nervix_models::Identifier;
use tokio::sync::broadcast;

#[derive(Debug, Default)]
pub struct EmitterFaultInjector {
    emitters: DashMap<String, EmitterFaultMode, RandomState>,
}

#[derive(Debug, Default)]
pub struct IngestorFaultInjector {
    ingestors: DashMap<String, (), RandomState>,
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
    pub branch_instance_expiration_scan_interval: Option<Duration>,
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
            branch_instance_expiration_scan_interval: None,
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
