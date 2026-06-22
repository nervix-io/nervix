use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ahash::RandomState;
use dashmap::DashMap;
use tokio::sync::Notify;

use super::{PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement};

#[derive(Debug)]
pub(super) struct ReplicatedWasmProcessorState {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) required_replica_acks: usize,
    pub(super) replica_nodes: Vec<String>,
    snapshot: parking_lot::Mutex<Vec<u8>>,
    pub(super) current_lsm: AtomicU64,
    pub(super) last_persisted_lsm: AtomicU64,
    pub(super) dirty: AtomicBool,
    pub(super) replica_progress: DashMap<String, u64, RandomState>,
    pub(super) replication_notify: Notify,
}

impl ReplicatedWasmProcessorState {
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let mut current_lsm = 0;
        let mut last_persisted_lsm = 0;
        let mut snapshot = Vec::new();
        if let Some(initial) = initial {
            current_lsm = initial.lsm;
            last_persisted_lsm = initial.lsm;
            snapshot = initial.payload;
        }
        Ok(Self {
            placement,
            required_replica_acks,
            replica_nodes,
            snapshot: parking_lot::Mutex::new(snapshot),
            current_lsm: AtomicU64::new(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            dirty: AtomicBool::new(false),
            replica_progress: DashMap::default(),
            replication_notify: Notify::new(),
        })
    }

    pub(super) fn restore_guest_state(&self) -> Option<Vec<u8>> {
        let snapshot = self.snapshot.lock().clone();
        (!snapshot.is_empty()).then_some(snapshot)
    }

    pub(super) fn replace_guest_state(
        &self,
        guest_state: Vec<u8>,
    ) -> Result<(u64, Vec<u8>), RuntimePersistenceError> {
        let payload = guest_state.clone();
        *self.snapshot.lock() = guest_state;
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        Ok((lsm, payload))
    }

    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        let snapshot = self.snapshot.lock().clone();
        Ok(PersistedRuntimeStateEntry {
            lsm: self.current_lsm.load(Ordering::SeqCst),
            payload: snapshot,
        })
    }

    pub(super) fn mark_replica_progress(&self, node_id: &str, lsm: u64) {
        self.replica_progress.insert(node_id.to_string(), lsm);
        self.replication_notify.notify_waiters();
    }

    pub(super) fn replica_quorum_satisfied(&self, lsm: u64) -> bool {
        self.replica_nodes
            .iter()
            .filter(|node_id| {
                self.replica_progress
                    .get(node_id.as_str())
                    .is_some_and(|observed| *observed >= lsm)
            })
            .count()
            >= self.required_replica_acks
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{Domain, Identifier, ModelKind};

    use super::*;
    use crate::{
        runtime::{BranchKey, RuntimeStateKind},
        runtime_schema::RuntimeValue,
    };

    fn placement() -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: Domain::parse("test").expect("valid domain"),
            state: RuntimeStateKind::WasmProcessor,
            kind: ModelKind::WasmProcessor,
            identifier: Identifier::parse("filter").expect("valid identifier"),
            branch_key: BranchKey::from_fields([(
                Identifier::parse("tenant").expect("valid identifier"),
                RuntimeValue::String("acme".to_string()),
            )])
            .expect("test branch key must be non-empty")
            .into(),
        }
    }

    #[test]
    fn wasm_processor_state_tracks_replica_quorum() {
        let state = ReplicatedWasmProcessorState::new(
            placement(),
            vec!["node-2".to_string(), "node-3".to_string()],
            2,
            None,
        )
        .expect("state should initialize");
        let (lsm, payload) = state
            .replace_guest_state(vec![1, 2, 3])
            .expect("guest state should persist");

        assert_eq!(payload, vec![1, 2, 3]);
        assert!(!state.replica_quorum_satisfied(lsm));
        state.mark_replica_progress("node-2", lsm);
        assert!(!state.replica_quorum_satisfied(lsm));
        state.mark_replica_progress("node-3", lsm);
        assert!(state.replica_quorum_satisfied(lsm));
    }

    #[test]
    fn wasm_processor_state_restores_raw_guest_bytes() {
        let initial = PersistedRuntimeStateEntry {
            lsm: 7,
            payload: vec![9, 8, 7],
        };
        let state = ReplicatedWasmProcessorState::new(placement(), Vec::new(), 0, Some(initial))
            .expect("state should initialize from persisted payload");

        assert_eq!(state.restore_guest_state(), Some(vec![9, 8, 7]));
        assert_eq!(state.current_lsm.load(Ordering::SeqCst), 7);
    }
}
