use std::{sync::Arc as StdArc, time::Duration};

use error_stack::{Report, ResultExt as _};
use nervix_expiry_map::ExpiryMap;
use nervix_models::{Expression, ModelName, RelayName, Timestamp};
use nervix_vm::CompiledProgram as VmCompiledProgram;
use ordered_float::OrderedFloat;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use triomphe::Arc;

use super::{
    KeyProjectionKind, PersistedRuntimeStateEntry, ProcessorCompileError, ReorderKeyPart,
    RuntimePersistenceError, RuntimeStatePlacement, UdfExecutor, checked_add_duration_to_timestamp,
    compile_key_projection_program,
    published_generation::{Generation, PublishedGenerations},
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

/// What one deduplicator branch keeps beyond the branch task that processes it.
///
/// The keys belong to that task's [`DeduplicatorKeyspace`], which reserves and releases them without
/// a lock. Everything outside the task reads the keys it last published here: the snapshot task
/// persists them, replicas and ownership handoff receive them, and the next task for the same
/// branch restores its keyspace from them.
#[derive(Debug)]
pub(super) struct ReplicatedDeduplicatorState {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) generations: PublishedGenerations<Vec<PublishedDeduplicatorKey>>,
}

/// One key a deduplicator branch held when its task published the keyspace, shared with that
/// keyspace rather than copied from it.
#[derive(Debug)]
pub(super) struct PublishedDeduplicatorKey {
    key: Arc<DeduplicatorKey>,
    seen_at: Timestamp,
}

/// The keys one deduplicator branch admitted within its `MAX TIME`, owned by the branch task.
#[derive(Debug)]
pub(super) struct DeduplicatorKeyspace {
    state: Arc<ReplicatedDeduplicatorState>,
    recent_keys: ExpiryMap<DeduplicatorKey, Timestamp>,
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
    processor: &ModelName,
    input_relays: &[RelayName],
    deduplicate_on: &[Expression],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> error_stack::Result<CompiledDeduplicatorKeyProgram, ProcessorCompileError> {
    if deduplicate_on.is_empty() {
        return Err(Report::new(
            ProcessorCompileError::DeduplicatorWithoutKeys {
                processor: processor.clone(),
            },
        ));
    }
    let compiled = compile_key_projection_program(
        KeyProjectionKind::Deduplicator,
        processor,
        input_relays,
        deduplicate_on,
        input_schema,
        udfs,
    )
    .change_context_lazy(|| ProcessorCompileError::DeduplicatorKeyProgram {
        processor: processor.clone(),
    })?;
    Ok(CompiledDeduplicatorKeyProgram {
        key_column_offset: 0,
        key_count: deduplicate_on.len(),
        program: Arc::new(compiled),
    })
}

impl ReplicatedDeduplicatorState {
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let generations = match initial {
            Some(initial) => {
                let recent_keys = decode_deduplicator_snapshot(&initial.payload)?;
                PublishedGenerations::restored(initial.lsm, Self::published_keys(&recent_keys))
            }
            None => PublishedGenerations::restored(0, Vec::new()),
        };
        Ok(Self {
            placement,
            generations,
        })
    }

    /// Build the keyspace a branch task owns from the keys published last.
    pub(super) fn keyspace(state: &Arc<Self>) -> DeduplicatorKeyspace {
        let published = state.generations.load();
        let mut recent_keys = ExpiryMap::new();
        for published_key in &published.value {
            // Published keys come from one keyspace or one decoded snapshot, and each of those holds
            // a key once, so every key is new to the keyspace rebuilt from them.
            recent_keys.insert_shared(published_key.key.clone(), published_key.seen_at);
        }
        DeduplicatorKeyspace {
            state: state.clone(),
            recent_keys,
        }
    }

    /// The keys a keyspace holds, oldest first, each shared with that keyspace.
    fn published_keys(
        recent_keys: &ExpiryMap<DeduplicatorKey, Timestamp>,
    ) -> Vec<PublishedDeduplicatorKey> {
        let mut published = Vec::with_capacity(recent_keys.len());
        for (key, seen_at) in recent_keys.iter_shared() {
            published.push(PublishedDeduplicatorKey {
                key: key.clone(),
                seen_at: *seen_at,
            });
        }
        published
    }

    /// Encode the keys published last, stamped with the revision they stand at.
    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, Report<RuntimePersistenceError>> {
        let published = self.generations.load();
        self.snapshot_of(&published)
    }

    /// Encode the keys published last when their revision is after `after_lsm`. A requester that
    /// already holds that revision costs no encode.
    pub(super) fn snapshot_after(
        &self,
        after_lsm: Option<u64>,
    ) -> Result<Option<PersistedRuntimeStateEntry>, Report<RuntimePersistenceError>> {
        let Some(published) = self.generations.load_after(after_lsm) else {
            return Ok(None);
        };
        Ok(Some(self.snapshot_of(&published)?))
    }

    fn snapshot_of(
        &self,
        published: &Generation<Vec<PublishedDeduplicatorKey>>,
    ) -> Result<PersistedRuntimeStateEntry, Report<RuntimePersistenceError>> {
        Ok(PersistedRuntimeStateEntry {
            lsm: published.revision,
            payload: encode_deduplicator_snapshot(&published.value)?,
        })
    }
}

impl DeduplicatorKeyspace {
    pub(super) fn state(&self) -> &Arc<ReplicatedDeduplicatorState> {
        &self.state
    }

    fn prune_expired(&mut self, now: Timestamp, max_time: Duration) {
        while let Some((_, seen_at)) = self.recent_keys.oldest() {
            if checked_add_duration_to_timestamp(*seen_at, max_time) > now {
                break;
            }
            self.recent_keys.remove_oldest();
        }
    }

    pub(super) fn reserve_new_key(
        &mut self,
        key: DeduplicatorKey,
        seen_at: Timestamp,
        max_time: Duration,
    ) -> bool {
        self.prune_expired(seen_at, max_time);
        if !self.recent_keys.insert(key, seen_at) {
            return false;
        }
        self.state.generations.mark_live_dirty();
        true
    }

    pub(super) fn remove_reserved_keys(&mut self, keys: &[DeduplicatorKey]) {
        if keys.is_empty() {
            return;
        }
        for key in keys {
            self.recent_keys.remove(key);
        }
        self.state.generations.mark_live_dirty();
    }

    /// Publish the keys this branch holds when they changed after the last publication.
    ///
    /// Publishing shares every key with the keyspace instead of copying it, and nothing outside this
    /// task reads the keyspace itself, so a reservation never waits for a snapshot or a replica.
    pub(super) fn publish(&self) {
        if !self.state.generations.is_live_dirty() {
            return;
        }
        let published = ReplicatedDeduplicatorState::published_keys(&self.recent_keys);
        self.state.generations.publish(published);
    }
}

fn encode_deduplicator_snapshot(
    keys: &[PublishedDeduplicatorKey],
) -> Result<Vec<u8>, RuntimePersistenceError> {
    let mut entries = Vec::with_capacity(keys.len());
    for published in keys {
        entries.push(DeduplicatorEntrySnapshot {
            key: DeduplicatorKeySnapshot::from(published.key.as_ref()),
            seen_at: published.seen_at,
        });
    }
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&DeduplicatorSnapshot { entries })
        .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
    Ok(bytes.to_vec())
}

fn decode_deduplicator_snapshot(
    payload: &[u8],
) -> Result<ExpiryMap<DeduplicatorKey, Timestamp>, RuntimePersistenceError> {
    let snapshot = rkyv::from_bytes::<DeduplicatorSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
    let mut recent_keys = ExpiryMap::new();
    for entry in snapshot.entries {
        if !recent_keys.insert(entry.key.into(), entry.seen_at) {
            return Err(RuntimePersistenceError::DecodeState(
                "deduplicator snapshot contains a duplicate key".to_string(),
            ));
        }
    }
    Ok(recent_keys)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use meticulous::ResultExt as _;
    use nervix_expiry_map::ExpiryMap;
    use nervix_models::{DomainName, ModelKind, ModelName, SchemaFingerprint, Timestamp};
    use ordered_float::OrderedFloat;
    use triomphe::Arc;

    use super::{
        DeduplicatorKey, ReorderKeyPart, ReplicatedDeduplicatorState, RuntimeStatePlacement,
        decode_deduplicator_snapshot, encode_deduplicator_snapshot,
    };
    use crate::runtime::RuntimeState;

    const MAX_TIME: Duration = Duration::from_secs(600);

    fn empty_state() -> Arc<ReplicatedDeduplicatorState> {
        let placement = RuntimeStatePlacement {
            domain: DomainName::parse("test").assured("the domain name is well formed"),
            state: RuntimeState::Deduplicator {
                schema: SchemaFingerprint::from_digest([7; 32]),
            },
            kind: ModelKind::Deduplicator,
            identifier: ModelName::parse("dedup_orders").assured("the identifier is well formed"),
            branch_key: None,
        };
        Arc::new(
            ReplicatedDeduplicatorState::new(placement, None)
                .assured("an empty deduplicator state has nothing to decode"),
        )
    }

    fn key(value: &str) -> DeduplicatorKey {
        DeduplicatorKey::new(vec![ReorderKeyPart::Utf8(value.to_string())])
    }

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

        let mut keys = ExpiryMap::new();
        keys.insert(numeric.clone(), Timestamp::from_unix_nanos(10));
        keys.insert(text.clone(), Timestamp::from_unix_nanos(20));

        let encoded =
            encode_deduplicator_snapshot(&ReplicatedDeduplicatorState::published_keys(&keys))
                .expect("snapshot must encode");
        let decoded = decode_deduplicator_snapshot(&encoded).expect("snapshot must decode");

        assert_eq!(decoded.get(&numeric), Some(&Timestamp::from_unix_nanos(10)));
        assert_eq!(decoded.get(&text), Some(&Timestamp::from_unix_nanos(20)));
    }

    #[test]
    fn pruning_removes_the_expired_prefix_without_disturbing_newer_keys() {
        let first = key("first");
        let second = key("second");
        let third = key("third");
        let mut keyspace = ReplicatedDeduplicatorState::keyspace(&empty_state());
        assert!(
            keyspace
                .recent_keys
                .insert(first.clone(), Timestamp::from_unix_nanos(10))
        );
        assert!(
            keyspace
                .recent_keys
                .insert(second.clone(), Timestamp::from_unix_nanos(20))
        );
        assert!(
            keyspace
                .recent_keys
                .insert(third.clone(), Timestamp::from_unix_nanos(30))
        );

        keyspace.prune_expired(Timestamp::from_unix_nanos(25), Duration::from_nanos(10));

        assert!(!keyspace.recent_keys.contains_key(&first));
        assert!(keyspace.recent_keys.contains_key(&second));
        assert!(keyspace.recent_keys.contains_key(&third));
        assert_eq!(
            keyspace.recent_keys.oldest(),
            Some((&second, &Timestamp::from_unix_nanos(20)))
        );
    }

    /// The snapshot task, replicas and ownership handoff read the keys a branch published for as
    /// long as encoding them takes. The branch keeps reserving keys meanwhile, and the keys they
    /// read stay the generation they loaded.
    #[test]
    fn a_key_reservation_proceeds_while_a_snapshot_reads_the_published_keys() {
        let state = empty_state();
        let mut keyspace = ReplicatedDeduplicatorState::keyspace(&state);
        assert!(keyspace.reserve_new_key(key("txn-1"), Timestamp::from_unix_nanos(1), MAX_TIME));
        keyspace.publish();
        let snapshot_read = state.generations.load();

        assert!(keyspace.reserve_new_key(key("txn-2"), Timestamp::from_unix_nanos(2), MAX_TIME));
        keyspace.publish();

        assert_eq!(snapshot_read.revision, 1);
        assert_eq!(
            snapshot_read.value.len(),
            1,
            "a later reservation changed the keys a snapshot was reading"
        );
        let latest = state.generations.load();
        assert_eq!(latest.revision, 2);
        assert_eq!(latest.value.len(), 2);
        assert!(
            Arc::ptr_eq(&snapshot_read.value[0].key, &latest.value[0].key),
            "publishing copied a key the keyspace already shared"
        );
    }

    /// A branch task publishes its keys when it stops, and the next task for the branch restores
    /// them, so a key reserved before the stop is still a duplicate after it.
    #[test]
    fn the_next_keyspace_for_a_branch_restores_the_published_keys() {
        let state = empty_state();
        let mut stopping = ReplicatedDeduplicatorState::keyspace(&state);
        assert!(stopping.reserve_new_key(key("txn-1"), Timestamp::from_unix_nanos(1), MAX_TIME));
        assert!(stopping.reserve_new_key(key("txn-2"), Timestamp::from_unix_nanos(2), MAX_TIME));
        stopping.remove_reserved_keys(&[key("txn-2")]);
        stopping.publish();
        let published_revision = state.generations.load().revision;
        stopping.publish();
        assert_eq!(
            state.generations.load().revision,
            published_revision,
            "publishing an unchanged keyspace stamped a new revision"
        );
        drop(stopping);

        let mut next = ReplicatedDeduplicatorState::keyspace(&state);
        assert!(!next.reserve_new_key(key("txn-1"), Timestamp::from_unix_nanos(3), MAX_TIME));
        assert!(next.reserve_new_key(key("txn-2"), Timestamp::from_unix_nanos(3), MAX_TIME));
    }

    #[test]
    fn deduplicator_key_program_requires_deduplicate_on_expressions() {
        let Err(error) = super::compile_deduplicator_key_program(
            &ModelName::parse("dedup_orders").assured("the identifier is well formed"),
            &[],
            &[],
            std::sync::Arc::new(arrow_schema::Schema::empty()),
            None,
        ) else {
            panic!("a deduplicator without DEDUPLICATE ON expressions must not compile");
        };
        assert_eq!(
            error.current_context().to_string(),
            "deduplicator 'dedup_orders' requires at least one DEDUPLICATE ON expression"
        );
    }
}
