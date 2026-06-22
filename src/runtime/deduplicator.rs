use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use ahash::RandomState;
use dashmap::DashMap;
use indexmap::IndexMap;
use nervix_models::{Identifier, Timestamp};
use nervix_vm::CompiledProgram as VmCompiledProgram;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::sync::Notify;

use super::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
    checked_add_duration_to_timestamp, compile_key_projection_program,
    split_reorder_by_expressions,
};

#[derive(Debug, Clone)]
pub(super) struct CompiledDeduplicatorKeyProgram {
    pub(super) program: VmCompiledProgram,
    pub(super) key_column_offset: usize,
    pub(super) key_count: usize,
}

#[derive(Debug)]
pub(super) struct ReplicatedDeduplicatorState {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) required_replica_acks: usize,
    pub(super) replica_nodes: Vec<String>,
    recent_keys: parking_lot::Mutex<IndexMap<String, Timestamp, RandomState>>,
    pub(super) current_lsm: AtomicU64,
    pub(super) last_persisted_lsm: AtomicU64,
    pub(super) dirty: AtomicBool,
    pub(super) replica_progress: DashMap<String, u64, RandomState>,
    pub(super) replication_notify: Notify,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct DeduplicatorSnapshot {
    entries: Vec<DeduplicatorEntrySnapshot>,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct DeduplicatorEntrySnapshot {
    key: String,
    seen_at: Timestamp,
}

pub(super) fn compile_deduplicator_key_program(
    processor: &Identifier,
    input_relay: &Identifier,
    deduplicate_on: &str,
    input_schema: Arc<arrow_schema::Schema>,
) -> Result<CompiledDeduplicatorKeyProgram, String> {
    let expressions = split_reorder_by_expressions(deduplicate_on);
    if expressions.is_empty() {
        return Err(format!(
            "deduplicator '{}' requires at least one DEDUPLICATE ON expression",
            processor.as_str()
        ));
    }
    let compiled = compile_key_projection_program(
        "deduplicator",
        processor,
        "DEDUPLICATE ON",
        input_relay,
        &expressions,
        input_schema,
    )?;
    Ok(CompiledDeduplicatorKeyProgram {
        key_column_offset: 0,
        key_count: expressions.len(),
        program: compiled,
    })
}

impl ReplicatedDeduplicatorState {
    fn prune_expired_recent_keys(
        recent_keys: &mut IndexMap<String, Timestamp, RandomState>,
        now: Timestamp,
        max_time: Duration,
    ) {
        while recent_keys
            .get_index(0)
            .map(|(_, seen_at)| checked_add_duration_to_timestamp(*seen_at, max_time) <= now)
            .unwrap_or(false)
        {
            recent_keys.shift_remove_index(0);
        }
    }

    pub(super) fn new(
        placement: RuntimeStatePlacement,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let mut recent_keys = IndexMap::with_hasher(RandomState::default());
        let mut current_lsm = 0;
        let mut last_persisted_lsm = 0;
        if let Some(initial) = initial {
            current_lsm = initial.lsm;
            last_persisted_lsm = initial.lsm;
            recent_keys = decode_deduplicator_snapshot(&initial.payload)?;
        }
        Ok(Self {
            placement,
            required_replica_acks,
            replica_nodes,
            recent_keys: parking_lot::Mutex::new(recent_keys),
            current_lsm: AtomicU64::new(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            dirty: AtomicBool::new(false),
            replica_progress: DashMap::default(),
            replication_notify: Notify::new(),
        })
    }

    pub(super) fn apply_new_key(
        &self,
        key: String,
        seen_at: Timestamp,
        max_time: Duration,
    ) -> Result<Option<(u64, Vec<u8>)>, RuntimePersistenceError> {
        let mut recent_keys = self.recent_keys.lock();
        Self::prune_expired_recent_keys(&mut recent_keys, seen_at, max_time);
        if recent_keys.contains_key(&key) {
            return Ok(None);
        }
        recent_keys.insert(key, seen_at);
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        Ok(Some((lsm, encode_deduplicator_snapshot(&recent_keys)?)))
    }

    pub(super) fn remove_reserved_keys(&self, keys: &[String]) {
        if keys.is_empty() {
            return;
        }
        let mut recent_keys = self.recent_keys.lock();
        for key in keys {
            recent_keys.shift_remove(key);
        }
        self.current_lsm.fetch_add(1, Ordering::SeqCst);
        self.dirty.store(true, Ordering::SeqCst);
    }

    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        let recent_keys = self.recent_keys.lock();
        Ok(PersistedRuntimeStateEntry {
            lsm: self.current_lsm.load(Ordering::SeqCst),
            payload: encode_deduplicator_snapshot(&recent_keys)?,
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

fn encode_deduplicator_snapshot(
    recent_keys: &IndexMap<String, Timestamp, RandomState>,
) -> Result<Vec<u8>, RuntimePersistenceError> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&DeduplicatorSnapshot {
        entries: recent_keys
            .iter()
            .map(|(key, seen_at)| DeduplicatorEntrySnapshot {
                key: key.clone(),
                seen_at: *seen_at,
            })
            .collect(),
    })
    .map(|bytes| bytes.to_vec())
    .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))
}

fn decode_deduplicator_snapshot(
    payload: &[u8],
) -> Result<IndexMap<String, Timestamp, RandomState>, RuntimePersistenceError> {
    let snapshot = rkyv::from_bytes::<DeduplicatorSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
    let mut recent_keys = IndexMap::with_hasher(RandomState::default());
    for entry in snapshot.entries {
        recent_keys.insert(entry.key, entry.seen_at);
    }
    Ok(recent_keys)
}
