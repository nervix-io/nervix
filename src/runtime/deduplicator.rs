use std::{
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use ahash::RandomState;
use dashmap::DashMap;
use indexmap::IndexMap;
use nervix_models::{Expression, Identifier, Timestamp};
use nervix_vm::CompiledProgram as VmCompiledProgram;
use ordered_float::OrderedFloat;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::sync::Notify;
use triomphe::Arc;

use super::{
    PersistedRuntimeStateEntry, ReorderKeyPart, RuntimePersistenceError, RuntimeStatePlacement,
    UdfExecutor, checked_add_duration_to_timestamp, compile_key_projection_program,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct DeduplicatorKey(Vec<ReorderKeyPart>);

impl DeduplicatorKey {
    pub(super) fn new(parts: Vec<ReorderKeyPart>) -> Self {
        Self(parts)
    }
}

#[derive(Debug, Clone)]
pub(super) struct CompiledDeduplicatorKeyProgram {
    pub(super) program: Arc<VmCompiledProgram>,
    pub(super) key_column_offset: usize,
    pub(super) key_count: usize,
}

#[derive(Debug)]
pub(super) struct ReplicatedDeduplicatorState {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) required_replica_acks: usize,
    pub(super) replica_nodes: Vec<String>,
    recent_keys: parking_lot::Mutex<IndexMap<DeduplicatorKey, Timestamp, RandomState>>,
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
    key: DeduplicatorKeySnapshot,
    seen_at: Timestamp,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct DeduplicatorKeySnapshot(Vec<DeduplicatorKeyPartSnapshot>);

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
enum DeduplicatorKeyPartSnapshot {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(u64),
    Utf8(String),
    Datetime(i64),
}

impl From<&DeduplicatorKey> for DeduplicatorKeySnapshot {
    fn from(key: &DeduplicatorKey) -> Self {
        Self(
            key.0
                .iter()
                .map(|part| match part {
                    ReorderKeyPart::Null => DeduplicatorKeyPartSnapshot::Null,
                    ReorderKeyPart::Boolean(value) => DeduplicatorKeyPartSnapshot::Boolean(*value),
                    ReorderKeyPart::Int64(value) => DeduplicatorKeyPartSnapshot::Int64(*value),
                    ReorderKeyPart::UInt64(value) => DeduplicatorKeyPartSnapshot::UInt64(*value),
                    ReorderKeyPart::Float64(value) => {
                        DeduplicatorKeyPartSnapshot::Float64(value.into_inner().to_bits())
                    }
                    ReorderKeyPart::Utf8(value) => DeduplicatorKeyPartSnapshot::Utf8(value.clone()),
                    ReorderKeyPart::Datetime(value) => {
                        DeduplicatorKeyPartSnapshot::Datetime(*value)
                    }
                })
                .collect(),
        )
    }
}

impl From<DeduplicatorKeySnapshot> for DeduplicatorKey {
    fn from(key: DeduplicatorKeySnapshot) -> Self {
        Self(
            key.0
                .into_iter()
                .map(|part| match part {
                    DeduplicatorKeyPartSnapshot::Null => ReorderKeyPart::Null,
                    DeduplicatorKeyPartSnapshot::Boolean(value) => ReorderKeyPart::Boolean(value),
                    DeduplicatorKeyPartSnapshot::Int64(value) => ReorderKeyPart::Int64(value),
                    DeduplicatorKeyPartSnapshot::UInt64(value) => ReorderKeyPart::UInt64(value),
                    DeduplicatorKeyPartSnapshot::Float64(value) => {
                        ReorderKeyPart::Float64(OrderedFloat(f64::from_bits(value)))
                    }
                    DeduplicatorKeyPartSnapshot::Utf8(value) => ReorderKeyPart::Utf8(value),
                    DeduplicatorKeyPartSnapshot::Datetime(value) => ReorderKeyPart::Datetime(value),
                })
                .collect(),
        )
    }
}

pub(super) fn compile_deduplicator_key_program(
    processor: &Identifier,
    input_relays: &[Identifier],
    deduplicate_on: &[Expression],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledDeduplicatorKeyProgram, String> {
    if deduplicate_on.is_empty() {
        return Err(format!(
            "deduplicator '{}' requires at least one DEDUPLICATE ON expression",
            processor.as_str()
        ));
    }
    let compiled = compile_key_projection_program(
        "deduplicator",
        processor,
        "DEDUPLICATE ON",
        input_relays,
        deduplicate_on,
        input_schema,
        udfs,
    )?;
    Ok(CompiledDeduplicatorKeyProgram {
        key_column_offset: 0,
        key_count: deduplicate_on.len(),
        program: Arc::new(compiled),
    })
}

impl ReplicatedDeduplicatorState {
    fn prune_expired_recent_keys(
        recent_keys: &mut IndexMap<DeduplicatorKey, Timestamp, RandomState>,
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
        key: DeduplicatorKey,
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

    pub(super) fn remove_reserved_keys(&self, keys: &[DeduplicatorKey]) {
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
            schema_fingerprint: self.placement.schema_fingerprint,
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
    recent_keys: &IndexMap<DeduplicatorKey, Timestamp, RandomState>,
) -> Result<Vec<u8>, RuntimePersistenceError> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&DeduplicatorSnapshot {
        entries: recent_keys
            .iter()
            .map(|(key, seen_at)| DeduplicatorEntrySnapshot {
                key: DeduplicatorKeySnapshot::from(key),
                seen_at: *seen_at,
            })
            .collect(),
    })
    .map(|bytes| bytes.to_vec())
    .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))
}

fn decode_deduplicator_snapshot(
    payload: &[u8],
) -> Result<IndexMap<DeduplicatorKey, Timestamp, RandomState>, RuntimePersistenceError> {
    let snapshot = rkyv::from_bytes::<DeduplicatorSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
    let mut recent_keys = IndexMap::with_hasher(RandomState::default());
    for entry in snapshot.entries {
        recent_keys.insert(entry.key.into(), entry.seen_at);
    }
    Ok(recent_keys)
}

#[cfg(test)]
mod tests {
    use ahash::RandomState;
    use indexmap::IndexMap;
    use nervix_models::Timestamp;
    use ordered_float::OrderedFloat;

    use super::{
        DeduplicatorKey, ReorderKeyPart, decode_deduplicator_snapshot, encode_deduplicator_snapshot,
    };

    #[test]
    fn typed_deduplicator_keys_round_trip_without_string_collisions() {
        let numeric = DeduplicatorKey::new(vec![
            ReorderKeyPart::UInt64(1),
            ReorderKeyPart::Float64(OrderedFloat(1.5)),
            ReorderKeyPart::Null,
        ]);
        let text = DeduplicatorKey::new(vec![
            ReorderKeyPart::Utf8("1".to_string()),
            ReorderKeyPart::Float64(OrderedFloat(1.5)),
            ReorderKeyPart::Null,
        ]);
        assert_ne!(numeric, text);

        let mut keys = IndexMap::with_hasher(RandomState::default());
        keys.insert(numeric.clone(), Timestamp::from_unix_nanos(10));
        keys.insert(text.clone(), Timestamp::from_unix_nanos(20));

        let encoded = encode_deduplicator_snapshot(&keys).expect("snapshot must encode");
        let decoded = decode_deduplicator_snapshot(&encoded).expect("snapshot must decode");

        assert_eq!(decoded.get(&numeric), Some(&Timestamp::from_unix_nanos(10)));
        assert_eq!(decoded.get(&text), Some(&Timestamp::from_unix_nanos(20)));
    }
}
