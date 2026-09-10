//! Ordered durable Raft storage and atomic publication of immutable state revisions.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Log/vote persistence, semantic record batches, recovery and observer publication.
//! - **Depends on.** The bounded executor, Fjall, and the replicated vocabulary.
//! - **Must not know.** Network transport, graph execution or application lifecycle policy.

use std::{
    io::{self, Cursor},
    ops::{Bound, RangeBounds},
    sync::atomic::{AtomicBool, Ordering},
};

use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use futures_util::StreamExt as _;
use nervix_execution::{Executor, MemoryClass, Reservation, StorageClass};
use openraft::{
    Snapshot, SnapshotMeta, StoredMembership,
    entry::{EntryPayload, RaftPayload},
    storage::{
        IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
    },
    type_config::alias::EntryOf,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use triomphe::Arc;

use crate::{
    AppliedConsensusCommand, LogIdOf, SnapshotOf, StateMachineChanges, StateMachineData,
    StoredMembershipOf, StoredSnapshotData, TypeConfig, VoteOf, apply_consensus_command,
    durable_batch::{DurableBatch, StorageFailure},
    read_key,
    records::{Records, ResourceRecords, ScheduleRecords},
    storage_decode,
    storage_fault::{StorageBoundary, StorageFault},
};

const KEY_METADATA: &[u8] = b"metadata";
const KEY_SNAPSHOT: &[u8] = b"snapshot";
const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_LAST_PURGED: &[u8] = b"last_purged";
const KEYSPACE_LOGS: &str = "raft_logs";
const KEYSPACE_META: &str = "raft_meta";
const KEYSPACE_STATE_MACHINE: &str = "raft_state_machine";
const KEYSPACE_SNAPSHOT: &str = "raft_snapshot";
const KEYSPACE_NAMES: [&str; 4] = [
    KEYSPACE_LOGS,
    KEYSPACE_META,
    KEYSPACE_STATE_MACHINE,
    KEYSPACE_SNAPSHOT,
];

#[derive(Debug, Serialize, Deserialize)]
enum StateEncoding {
    SemanticRecords,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateMetadata {
    encoding: StateEncoding,
    last_applied_log_id: Option<LogIdOf>,
    last_membership: Arc<StoredMembershipOf>,
    runtime_revision: u64,
}

impl From<&StateMachineData> for StateMetadata {
    fn from(state: &StateMachineData) -> Self {
        Self {
            encoding: StateEncoding::SemanticRecords,
            last_applied_log_id: state.last_applied_log_id.clone(),
            last_membership: state.last_membership.clone(),
            runtime_revision: state.runtime_revision,
        }
    }
}

impl StateMachineData {
    fn load(sm: &Keyspace, metadata: StateMetadata) -> io::Result<Self> {
        let StateMetadata {
            encoding: StateEncoding::SemanticRecords,
            last_applied_log_id,
            last_membership,
            runtime_revision,
        } = metadata;
        Ok(Self {
            last_applied_log_id,
            last_membership,
            runtime_revision,
            schedule: ScheduleRecords {
                domains: Records::load(b's', sm)?,
            },
            domains: Records::load(b'd', sm)?,
            domain_clock_authorities: Records::load(b'a', sm)?,
            users: Records::load(b'u', sm)?,
            resources: ResourceRecords {
                counters: Records::load(b'c', sm)?,
                versions: Records::load(b'v', sm)?,
                replicas: Records::load(b'r', sm)?,
            },
            cordoned_node_ids: Records::load(b'n', sm)?,
            transactions: Records::load(b't', sm)?,
        })
    }

    fn write_changes(
        &self,
        preceding: &Self,
        batch: &mut DurableBatch<'_>,
        sm: &Keyspace,
    ) -> io::Result<()> {
        self.schedule
            .domains
            .write_changes(&preceding.schedule.domains, b's', batch, sm)?;
        self.domains
            .write_changes(&preceding.domains, b'd', batch, sm)?;
        self.domain_clock_authorities.write_changes(
            &preceding.domain_clock_authorities,
            b'a',
            batch,
            sm,
        )?;
        self.users
            .write_changes(&preceding.users, b'u', batch, sm)?;
        self.resources
            .counters
            .write_changes(&preceding.resources.counters, b'c', batch, sm)?;
        self.resources
            .versions
            .write_changes(&preceding.resources.versions, b'v', batch, sm)?;
        self.resources
            .replicas
            .write_changes(&preceding.resources.replicas, b'r', batch, sm)?;
        self.cordoned_node_ids
            .write_changes(&preceding.cordoned_node_ids, b'n', batch, sm)?;
        self.transactions
            .write_changes(&preceding.transactions, b't', batch, sm)?;
        batch.insert(sm, KEY_METADATA, &StateMetadata::from(self))
    }
}

pub(super) struct StoreState {
    db: Database,
    pub(super) executor: Executor,
    pub(super) faults: StorageFault,
    failed: AtomicBool,
    logs: Keyspace,
    meta: Keyspace,
    sm: Keyspace,
    snapshot: Keyspace,
    pub(super) state_machine: RwLock<StateMachineData>,
    current_snapshot: RwLock<Option<Arc<StoredSnapshotData>>>,
    pub(super) schedule_tx: watch::Sender<u64>,
    pub(super) domain_tx: watch::Sender<u64>,
    pub(super) resource_tx: watch::Sender<u64>,
    pub(super) transaction_tx: watch::Sender<u64>,
}

#[derive(Clone)]
pub(super) struct StoreInner {
    shared: Arc<StoreState>,
}

impl std::ops::Deref for StoreInner {
    type Target = StoreState;
    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl StoreInner {
    pub(super) fn state(&self) -> StateMachineData {
        self.state_machine.read().clone()
    }

    async fn run<T: Send + 'static>(
        &self,
        class: MemoryClass,
        operation: impl FnOnce(&Self, &Reservation) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let reservation = Self::reserve(&self.executor, class).await?;
        let inner = self.clone();
        self.executor
            .run_storage(
                StorageClass::Consensus,
                reservation,
                move |reservation, cancellation| {
                    cancellation.check().map_err(io::Error::other)?;
                    operation(&inner, &reservation)
                },
            )
            .await
            .map_err(io::Error::other)?
    }

    async fn reserve(executor: &Executor, class: MemoryClass) -> io::Result<Reservation> {
        let unit = match class {
            MemoryClass::Management => executor.limits().management_event_bytes.as_u64(),
            MemoryClass::Bulk => executor.snapshot().bulk_memory.capacity_bytes / 2,
            MemoryClass::Commands | MemoryClass::Relay => executor.limits().command_bytes.as_u64(),
        };
        let bytes = match class {
            MemoryClass::Bulk => unit,
            _ => unit
                .checked_mul(4)
                .ok_or_else(|| io::Error::other(StorageFailure::Capacity))?,
        };
        executor
            .reserve(class, bytes)
            .await
            .map_err(io::Error::other)
    }

    fn commit(&self, operation: &str, batch: DurableBatch<'_>) -> io::Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other(StorageFailure::Stopped));
        }
        let result = (|| {
            self.faults
                .check(operation, StorageBoundary::BeforeCommit)?;
            batch.commit(&self.db)?;
            self.faults.check(operation, StorageBoundary::AfterSync)
        })();
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }

    fn publish(&self, state: StateMachineData, changes: &AppliedConsensusCommand) {
        let revision = match &state.last_applied_log_id {
            Some(id) => id.index,
            None => 0,
        };
        // Swap one coherent revision; destruction and notifications run after releasing the lock.
        let preceding = std::mem::replace(&mut *self.state_machine.write(), state);
        drop(preceding);
        if changes.schedule_changed {
            self.schedule_tx.send_replace(revision);
        }
        if changes.domains_changed {
            self.domain_tx.send_replace(revision);
        }
        if changes.resources_changed {
            self.resource_tx.send_replace(revision);
        }
        if changes.transactions_changed {
            self.transaction_tx.send_replace(revision);
        }
    }

    fn apply_entry(
        &self,
        entry: EntryOf<TypeConfig>,
        reservation: &Reservation,
    ) -> io::Result<crate::ConsensusResponse> {
        let preceding = self.state();
        let mut state = preceding.clone();
        state.last_applied_log_id = Some(entry.log_id.clone());
        if let Some(membership) = entry.get_membership() {
            state.last_membership = Arc::new(StoredMembership::new(
                Some(entry.log_id.clone()),
                membership,
            ));
        }
        let (operation, applied) = match &entry.payload {
            EntryPayload::Normal(command) => {
                let applied = apply_consensus_command(&mut state, command);
                state.record_runtime_revision(entry.log_id.index, &applied);
                (command.to_string(), applied)
            }
            _ => (
                "apply".to_owned(),
                AppliedConsensusCommand::applied(StateMachineChanges::default()),
            ),
        };
        let mut batch = DurableBatch::new(reservation)?;
        state.write_changes(&preceding, &mut batch, &self.sm)?;
        self.commit(&operation, batch)?;
        self.publish(state, &applied);
        Ok(applied.response)
    }

    fn log_key(index: u64) -> Vec<u8> {
        index.to_be_bytes().to_vec()
    }

    fn log_bounds<R: RangeBounds<u64>>(range: R) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        let convert = |bound: Bound<&u64>| match bound {
            Bound::Included(index) => Bound::Included(Self::log_key(*index)),
            Bound::Excluded(index) => Bound::Excluded(Self::log_key(*index)),
            Bound::Unbounded => Bound::Unbounded,
        };
        (convert(range.start_bound()), convert(range.end_bound()))
    }

    fn read_optional_log_id(&self, key: &[u8]) -> io::Result<Option<LogIdOf>> {
        match read_key::<Option<LogIdOf>>(&self.meta, key)? {
            Some(value) => Ok(value),
            None => Ok(None),
        }
    }
}

#[derive(Clone)]
pub(super) struct FjallStore {
    pub(super) inner: StoreInner,
}

impl FjallStore {
    pub(super) async fn from_database(db: Database, executor: Executor) -> io::Result<Self> {
        let reservation = StoreInner::reserve(&executor, MemoryClass::Commands).await?;
        let store_executor = executor.clone();
        executor
            .run_storage(
                StorageClass::Consensus,
                reservation,
                move |reservation, _| {
                    for name in db.list_keyspace_names() {
                        // This namespace contains exactly these four storage-owned keyspaces.
                        if name.starts_with("raft_") && !KEYSPACE_NAMES.contains(&name.as_ref()) {
                            return Err(io::Error::other(StorageFailure::InvalidState));
                        }
                    }
                    let logs = db
                        .keyspace(KEYSPACE_LOGS, KeyspaceCreateOptions::default)
                        .map_err(io::Error::other)?;
                    let meta = db
                        .keyspace(KEYSPACE_META, KeyspaceCreateOptions::default)
                        .map_err(io::Error::other)?;
                    let sm = db
                        .keyspace(KEYSPACE_STATE_MACHINE, KeyspaceCreateOptions::default)
                        .map_err(io::Error::other)?;
                    let snapshot = db
                        .keyspace(KEYSPACE_SNAPSHOT, KeyspaceCreateOptions::default)
                        .map_err(io::Error::other)?;
                    let state_machine = match read_key(&sm, KEY_METADATA)? {
                        Some(metadata) => StateMachineData::load(&sm, metadata)?,
                        None => {
                            if !sm.is_empty().map_err(io::Error::other)?
                                || !logs.is_empty().map_err(io::Error::other)?
                                || !meta.is_empty().map_err(io::Error::other)?
                                || !snapshot.is_empty().map_err(io::Error::other)?
                            {
                                return Err(io::Error::other(StorageFailure::InvalidState));
                            }
                            let state = StateMachineData::default();
                            let mut batch = DurableBatch::new(&reservation)?;
                            batch.insert(&sm, KEY_METADATA, &StateMetadata::from(&state))?;
                            batch.commit(&db)?;
                            state
                        }
                    };
                    let current_snapshot =
                        read_key::<StoredSnapshotData>(&snapshot, KEY_SNAPSHOT)?.map(Arc::new);
                    let revision = match &state_machine.last_applied_log_id {
                        Some(id) => id.index,
                        None => 0,
                    };
                    Ok(Self {
                        inner: StoreInner {
                            shared: Arc::new(StoreState {
                                db,
                                executor: store_executor,
                                faults: StorageFault::default(),
                                failed: AtomicBool::new(false),
                                logs,
                                meta,
                                sm,
                                snapshot,
                                state_machine: RwLock::new(state_machine),
                                current_snapshot: RwLock::new(current_snapshot),
                                schedule_tx: watch::channel(revision).0,
                                domain_tx: watch::channel(revision).0,
                                resource_tx: watch::channel(revision).0,
                                transaction_tx: watch::channel(revision).0,
                            }),
                        },
                    })
                },
            )
            .await
            .map_err(io::Error::other)?
    }

    pub(super) async fn has_raft_state(&self) -> io::Result<bool> {
        self.inner
            .run(MemoryClass::Management, |inner, _| {
                Ok(read_key::<VoteOf>(&inner.meta, KEY_VOTE)?.is_some()
                    || !inner.logs.is_empty().map_err(io::Error::other)?)
            })
            .await
    }

    /// Join storage work whose async caller was cancelled after its blocking job began.
    ///
    /// Raft is stopped before this barrier is submitted, so no new storage operation can be
    /// admitted behind it. The consensus storage class has one ordered worker; reaching this
    /// no-op therefore proves every earlier blocking job has returned and released its store
    /// handle.
    pub(super) async fn wait_for_idle(&self) -> io::Result<()> {
        self.inner.run(MemoryClass::Management, |_, _| Ok(())).await
    }

    fn log_reader(&self) -> FjallLogReader {
        FjallLogReader {
            inner: self.inner.clone(),
        }
    }
}

pub(super) struct FjallLogReader {
    inner: StoreInner,
}

impl RaftLogReader<TypeConfig> for FjallLogReader {
    async fn try_get_log_entries<
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<EntryOf<TypeConfig>>> {
        let bounds = StoreInner::log_bounds(range);
        self.inner
            .run(MemoryClass::Commands, move |inner, reservation| {
                let mut entries = Vec::new();
                let mut bytes = 0u64;
                for item in inner.logs.range(bounds) {
                    let (_, value) = item.into_inner().map_err(io::Error::other)?;
                    let length = u64::try_from(value.len()).map_err(io::Error::other)?;
                    bytes = bytes
                        .checked_add(length)
                        .ok_or_else(|| io::Error::other(StorageFailure::Capacity))?;
                    if bytes > reservation.bytes() / 2 {
                        return Err(io::Error::other(StorageFailure::Capacity));
                    }
                    entries.push(storage_decode(&value)?);
                }
                Ok(entries)
            })
            .await
    }

    async fn read_vote(&mut self) -> io::Result<Option<VoteOf>> {
        self.inner
            .run(MemoryClass::Management, |inner, _| {
                read_key(&inner.meta, KEY_VOTE)
            })
            .await
    }
}

impl RaftLogStorage<TypeConfig> for FjallStore {
    type LogReader = FjallLogReader;
    async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
        self.inner
            .run(MemoryClass::Management, |inner, _| {
                let last_purged_log_id = inner.read_optional_log_id(KEY_LAST_PURGED)?;
                let last_log_id = match inner.logs.iter().next_back() {
                    Some(item) => {
                        let (_, value) = item.into_inner().map_err(io::Error::other)?;
                        Some(storage_decode::<EntryOf<TypeConfig>>(&value)?.log_id)
                    }
                    None => last_purged_log_id.clone(),
                };
                Ok(LogState {
                    last_purged_log_id,
                    last_log_id,
                })
            })
            .await
    }
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.log_reader()
    }
    async fn save_vote(&mut self, vote: &VoteOf) -> io::Result<()> {
        let vote = vote.clone();
        self.inner
            .run(MemoryClass::Management, move |inner, reservation| {
                let mut batch = DurableBatch::new(reservation)?;
                batch.insert(&inner.meta, KEY_VOTE, &vote)?;
                inner.commit("vote", batch)
            })
            .await
    }
    async fn save_committed(&mut self, committed: Option<LogIdOf>) -> io::Result<()> {
        self.inner
            .run(MemoryClass::Management, move |inner, reservation| {
                let mut batch = DurableBatch::new(reservation)?;
                batch.insert(&inner.meta, KEY_COMMITTED, &committed)?;
                inner.commit("committed", batch)
            })
            .await
    }
    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf>> {
        self.inner
            .run(MemoryClass::Management, |inner, _| {
                inner.read_optional_log_id(KEY_COMMITTED)
            })
            .await
    }
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> io::Result<()>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        // Each ready entry is one bounded atomic write. A vote already queued gets the next
        // worker turn; this append never gathers newly arriving work in front of it.
        for entry in entries {
            tokio::task::consume_budget().await;
            let result = self
                .inner
                .run(MemoryClass::Commands, move |inner, reservation| {
                    let mut batch = DurableBatch::new(reservation)?;
                    batch.insert(
                        &inner.logs,
                        &StoreInner::log_key(entry.log_id.index),
                        &entry,
                    )?;
                    inner.commit("append", batch)
                })
                .await;
            if let Err(error) = result {
                self.inner.failed.store(true, Ordering::Release);
                callback.io_completed(Err(io::Error::other(StorageFailure::Stopped)));
                return Err(error);
            }
        }
        callback.io_completed(Ok(()));
        Ok(())
    }
    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf>) -> io::Result<()> {
        let start = match last_log_id {
            Some(id) => Bound::Excluded(StoreInner::log_key(id.index)),
            None => Bound::Unbounded,
        };
        self.inner
            .run(MemoryClass::Commands, move |inner, reservation| {
                let mut batch = DurableBatch::new(reservation)?;
                for item in inner.logs.range((start, Bound::Unbounded)) {
                    batch.remove(&inner.logs, &item.key().map_err(io::Error::other)?)?;
                }
                inner.commit("truncate", batch)
            })
            .await
    }
    async fn purge(&mut self, log_id: LogIdOf) -> io::Result<()> {
        self.inner
            .run(MemoryClass::Commands, move |inner, reservation| {
                let mut batch = DurableBatch::new(reservation)?;
                for item in inner.logs.range(..=StoreInner::log_key(log_id.index)) {
                    batch.remove(&inner.logs, &item.key().map_err(io::Error::other)?)?;
                }
                batch.insert(&inner.meta, KEY_LAST_PURGED, &Some(log_id))?;
                inner.commit("purge", batch)
            })
            .await
    }
}

impl RaftStateMachine<TypeConfig> for FjallStore {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;
    async fn applied_state(&mut self) -> io::Result<(Option<LogIdOf>, StoredMembershipOf)> {
        let state = self.inner.state();
        Ok((
            state.last_applied_log_id,
            state.last_membership.as_ref().clone(),
        ))
    }
    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: futures_util::Stream<Item = io::Result<openraft::storage::EntryResponder<TypeConfig>>>
            + Unpin
            + openraft::OptionalSend,
    {
        while let Some(item) = entries.next().await {
            tokio::task::consume_budget().await;
            let (entry, responder) = item?;
            let response = self
                .inner
                .run(MemoryClass::Commands, move |inner, reservation| {
                    inner.apply_entry(entry, reservation)
                })
                .await?;
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<
            openraft::type_config::alias::CommittedLeaderIdOf<TypeConfig>,
            crate::ClusterNodeName,
            crate::Node,
        >,
        snapshot: Cursor<Vec<u8>>,
    ) -> io::Result<()> {
        let meta = meta.clone();
        self.inner
            .run(MemoryClass::Bulk, move |inner, reservation| {
                let bytes = snapshot.into_inner();
                let state: StateMachineData = storage_decode(&bytes)?;
                if state.last_applied_log_id != meta.last_log_id
                    || state.last_membership.as_ref() != &meta.last_membership
                {
                    return Err(io::Error::other(StorageFailure::InvalidState));
                }
                let mut batch = DurableBatch::new(reservation)?;
                state.write_changes(&inner.state(), &mut batch, &inner.sm)?;
                let stored = Arc::new(StoredSnapshotData { meta, data: bytes });
                batch.insert(&inner.snapshot, KEY_SNAPSHOT, stored.as_ref())?;
                inner.commit("install_snapshot", batch)?;
                inner.publish(
                    state,
                    &AppliedConsensusCommand::applied(StateMachineChanges {
                        schedule_changed: true,
                        domains_changed: true,
                        resources_changed: true,
                        transactions_changed: true,
                    }),
                );
                *inner.current_snapshot.write() = Some(stored);
                Ok(())
            })
            .await
    }
    async fn get_current_snapshot(&mut self) -> io::Result<Option<SnapshotOf>> {
        self.inner
            .run(MemoryClass::Bulk, |inner, _| {
                let snapshot = inner.current_snapshot.read().clone();
                Ok(snapshot.map(|stored| Snapshot {
                    meta: stored.meta.clone(),
                    snapshot: Cursor::new(stored.data.clone()),
                }))
            })
            .await
    }
}

impl RaftSnapshotBuilder<TypeConfig> for FjallStore {
    type SnapshotData = Cursor<Vec<u8>>;
    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf> {
        self.inner
            .run(MemoryClass::Bulk, |inner, reservation| {
                let state = inner.state();
                let meta = SnapshotMeta {
                    last_log_id: state.last_applied_log_id.clone(),
                    last_membership: state.last_membership.as_ref().clone(),
                };
                let data = DurableBatch::encode(&state, reservation.bytes() / 4)?;
                let stored = Arc::new(StoredSnapshotData {
                    meta: meta.clone(),
                    data: data.clone(),
                });
                let mut batch = DurableBatch::new(reservation)?;
                batch.insert(&inner.snapshot, KEY_SNAPSHOT, stored.as_ref())?;
                inner.commit("snapshot", batch)?;
                *inner.current_snapshot.write() = Some(stored);
                Ok(Snapshot {
                    meta,
                    snapshot: Cursor::new(data),
                })
            })
            .await
    }
}

#[cfg(test)]
mod tests;
