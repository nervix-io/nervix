use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ahash::RandomState;
use dashmap::DashMap;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::sync::Notify;

use super::{PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement};
use crate::metrics::{RuntimeMetrics, RuntimeMetricsSnapshot};

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct BranchAggregatedRuntimeStateSnapshot {
    pub(super) metrics: RuntimeMetricsSnapshot,
}

#[derive(Debug)]
pub(super) struct ReplicatedBranchAggregatedState {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) primary_node: Option<String>,
    pub(super) physical_node_id: String,
    pub(super) current_lsm: AtomicU64,
    pub(super) last_persisted_lsm: AtomicU64,
    pub(super) dirty: AtomicBool,
    pub(super) replica_progress: DashMap<String, u64, RandomState>,
    pub(super) replication_notify: Notify,
}

impl ReplicatedBranchAggregatedState {
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        primary_node: Option<String>,
        physical_node_id: String,
        _replica_nodes: Vec<String>,
        _required_replica_acks: usize,
        metrics: &RuntimeMetrics,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let mut current_lsm = 0;
        let mut last_persisted_lsm = 0;
        if let Some(initial) = initial {
            current_lsm = initial.lsm;
            last_persisted_lsm = initial.lsm;
            let snapshot = decode_branch_aggregated_snapshot(&initial.payload)?;
            metrics.apply_global_target_snapshot(
                &placement.domain,
                placement.kind,
                &placement.identifier,
                &physical_node_id,
                snapshot.metrics,
            );
        }
        Ok(Self {
            placement,
            primary_node,
            physical_node_id,
            current_lsm: AtomicU64::new(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            dirty: AtomicBool::new(false),
            replica_progress: DashMap::default(),
            replication_notify: Notify::new(),
        })
    }

    pub(super) fn mark_metrics_updated(&self) -> u64 {
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        lsm
    }

    pub(super) fn latest_snapshot(
        &self,
        metrics: &RuntimeMetrics,
    ) -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        let snapshot = BranchAggregatedRuntimeStateSnapshot {
            metrics: metrics.snapshot_global_target(
                &self.placement.domain,
                self.placement.kind,
                &self.placement.identifier,
                &self.physical_node_id,
            ),
        };
        Ok(PersistedRuntimeStateEntry {
            lsm: self.current_lsm.load(Ordering::SeqCst),
            payload: encode_branch_aggregated_snapshot(&snapshot)?,
        })
    }

    pub(super) fn apply_snapshot(
        &self,
        metrics: &RuntimeMetrics,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), RuntimePersistenceError> {
        let snapshot = decode_branch_aggregated_snapshot(payload)?;
        metrics.apply_global_target_snapshot(
            &self.placement.domain,
            self.placement.kind,
            &self.placement.identifier,
            &self.physical_node_id,
            snapshot.metrics,
        );
        self.current_lsm.store(lsm, Ordering::SeqCst);
        self.dirty.store(true, Ordering::SeqCst);
        self.replication_notify.notify_waiters();
        Ok(())
    }

    pub(super) fn restore_persisted_snapshot(
        &self,
        metrics: &RuntimeMetrics,
        snapshot: PersistedRuntimeStateEntry,
    ) -> Result<(), RuntimePersistenceError> {
        let current_lsm = self.current_lsm.load(Ordering::SeqCst);
        if snapshot.lsm <= current_lsm
            && metrics.has_global_target_measurements(
                &self.placement.domain,
                self.placement.kind,
                &self.placement.identifier,
            )
        {
            return Ok(());
        }
        let decoded = decode_branch_aggregated_snapshot(&snapshot.payload)?;
        metrics.apply_global_target_snapshot(
            &self.placement.domain,
            self.placement.kind,
            &self.placement.identifier,
            &self.physical_node_id,
            decoded.metrics,
        );
        self.current_lsm.store(snapshot.lsm, Ordering::SeqCst);
        self.last_persisted_lsm
            .store(snapshot.lsm, Ordering::SeqCst);
        self.dirty.store(false, Ordering::SeqCst);
        self.replication_notify.notify_waiters();
        Ok(())
    }

    pub(super) fn mark_replica_progress(&self, node_id: &str, lsm: u64) {
        self.replica_progress.insert(node_id.to_string(), lsm);
        self.replication_notify.notify_waiters();
    }
}

pub(super) fn encode_branch_aggregated_snapshot(
    snapshot: &BranchAggregatedRuntimeStateSnapshot,
) -> Result<Vec<u8>, RuntimePersistenceError> {
    rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)
        .map(|bytes| bytes.to_vec())
        .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))
}

pub(super) fn decode_branch_aggregated_snapshot(
    payload: &[u8],
) -> Result<BranchAggregatedRuntimeStateSnapshot, RuntimePersistenceError> {
    rkyv::from_bytes::<BranchAggregatedRuntimeStateSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))
}
