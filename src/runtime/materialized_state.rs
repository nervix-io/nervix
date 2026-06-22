use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ahash::RandomState;
use dashmap::DashMap;
use nervix_models::{ModelKind, RemoteRuntimeField, RemoteRuntimeRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::sync::Notify;

use super::{
    BranchKey, PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
    materialized_record_is_newer,
};
use crate::{
    metrics::{RuntimeMetrics, RuntimeMetricsSnapshot},
    runtime_schema::RuntimeRecord,
};

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct MaterializedRelayEntrySnapshot {
    key: Option<Vec<RemoteRuntimeField>>,
    record: RemoteRuntimeRecord,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct MaterializedRelaySnapshot {
    entries: Vec<MaterializedRelayEntrySnapshot>,
    metrics: RuntimeMetricsSnapshot,
}

#[derive(Debug)]
pub(super) struct ReplicatedMaterializedRelayState {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) required_replica_acks: usize,
    pub(super) primary_node: Option<String>,
    pub(super) physical_node_id: String,
    pub(super) replica_nodes: Vec<String>,
    pub(super) entries: DashMap<Option<BranchKey>, RemoteRuntimeRecord, RandomState>,
    pub(super) current_lsm: AtomicU64,
    pub(super) last_persisted_lsm: AtomicU64,
    pub(super) dirty: AtomicBool,
    pub(super) replica_progress: DashMap<String, u64, RandomState>,
    pub(super) replication_notify: Notify,
}

impl ReplicatedMaterializedRelayState {
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        primary_node: Option<String>,
        physical_node_id: String,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
        metrics: &RuntimeMetrics,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let entries = DashMap::default();
        let mut current_lsm = 0;
        let mut last_persisted_lsm = 0;
        if let Some(initial) = initial {
            current_lsm = initial.lsm;
            last_persisted_lsm = initial.lsm;
            let (snapshot_entries, snapshot_metrics) =
                decode_materialized_stream_snapshot_with_metrics(&initial.payload)?;
            for (key, record) in snapshot_entries {
                entries.insert(key, record);
            }
            metrics.apply_branch_target_snapshot(
                placement.concrete_branch_key(),
                &placement.domain,
                ModelKind::Relay,
                &placement.identifier,
                &physical_node_id,
                snapshot_metrics,
            );
        }
        Ok(Self {
            placement,
            required_replica_acks,
            primary_node,
            physical_node_id,
            replica_nodes,
            entries,
            current_lsm: AtomicU64::new(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            dirty: AtomicBool::new(false),
            replica_progress: DashMap::default(),
            replication_notify: Notify::new(),
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

    pub(super) fn metrics_snapshot(&self, metrics: &RuntimeMetrics) -> RuntimeMetricsSnapshot {
        metrics.snapshot_branch_target(
            self.placement.concrete_branch_key(),
            &self.placement.domain,
            ModelKind::Relay,
            &self.placement.identifier,
            &self.physical_node_id,
        )
    }

    pub(super) fn update_last_by_timestamp(
        &self,
        metrics: &RuntimeMetrics,
        key: &Option<BranchKey>,
        record: &RuntimeRecord,
    ) -> Result<Option<(u64, Vec<u8>)>, RuntimePersistenceError> {
        let candidate = record.to_remote();
        let should_update = if let Some(existing) = self.entries.get(key) {
            materialized_record_is_newer(&existing.metadata, &candidate.metadata)
        } else {
            true
        };
        if !should_update {
            return Ok(None);
        }
        self.entries.insert(key.clone(), candidate);
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        Ok(Some((
            lsm,
            encode_materialized_stream_snapshot(&self.entries, self.metrics_snapshot(metrics))?,
        )))
    }

    pub(super) fn remove_key(
        &self,
        metrics: &RuntimeMetrics,
        key: &Option<BranchKey>,
    ) -> Result<Option<(u64, Vec<u8>)>, RuntimePersistenceError> {
        if self.entries.remove(key).is_none() {
            return Ok(None);
        }
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        Ok(Some((
            lsm,
            encode_materialized_stream_snapshot(&self.entries, self.metrics_snapshot(metrics))?,
        )))
    }
}

pub(super) fn encode_materialized_stream_snapshot_entries(
    entries: &[(Option<BranchKey>, RemoteRuntimeRecord)],
    metrics: RuntimeMetricsSnapshot,
) -> Result<Vec<u8>, RuntimePersistenceError> {
    let mut snapshot_entries = entries
        .iter()
        .map(|(key, record)| MaterializedRelayEntrySnapshot {
            key: BranchKey::to_remote_key(key),
            record: record.clone(),
        })
        .collect::<Vec<_>>();
    snapshot_entries
        .sort_by(|left, right| snapshot_key_sort(&left.key).cmp(&snapshot_key_sort(&right.key)));
    rkyv::to_bytes::<rkyv::rancor::Error>(&MaterializedRelaySnapshot {
        entries: snapshot_entries,
        metrics,
    })
    .map(|bytes| bytes.to_vec())
    .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))
}

pub(super) fn decode_materialized_stream_snapshot(
    payload: &[u8],
) -> Result<Vec<(Option<BranchKey>, RemoteRuntimeRecord)>, RuntimePersistenceError> {
    decode_materialized_stream_snapshot_with_metrics(payload).map(|(entries, _)| entries)
}

fn encode_materialized_stream_snapshot(
    entries: &DashMap<Option<BranchKey>, RemoteRuntimeRecord, RandomState>,
    metrics: RuntimeMetricsSnapshot,
) -> Result<Vec<u8>, RuntimePersistenceError> {
    let mut snapshot_entries = entries
        .iter()
        .map(|entry| MaterializedRelayEntrySnapshot {
            key: BranchKey::to_remote_key(entry.key()),
            record: entry.value().clone(),
        })
        .collect::<Vec<_>>();
    snapshot_entries
        .sort_by(|left, right| snapshot_key_sort(&left.key).cmp(&snapshot_key_sort(&right.key)));
    rkyv::to_bytes::<rkyv::rancor::Error>(&MaterializedRelaySnapshot {
        entries: snapshot_entries,
        metrics,
    })
    .map(|bytes| bytes.to_vec())
    .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))
}

fn decode_materialized_stream_snapshot_with_metrics(
    payload: &[u8],
) -> Result<
    (
        Vec<(Option<BranchKey>, RemoteRuntimeRecord)>,
        RuntimeMetricsSnapshot,
    ),
    RuntimePersistenceError,
> {
    let snapshot = rkyv::from_bytes::<MaterializedRelaySnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
    let entries = snapshot
        .entries
        .into_iter()
        .map(|entry| {
            BranchKey::from_remote_key(entry.key)
                .map(|key| (key, entry.record))
                .map_err(RuntimePersistenceError::DecodeState)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((entries, snapshot.metrics))
}

fn snapshot_key_sort(key: &Option<Vec<RemoteRuntimeField>>) -> String {
    let Some(fields) = key else {
        return String::new();
    };
    fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>()
        .join("\0")
}
