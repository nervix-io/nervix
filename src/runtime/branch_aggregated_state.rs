//! Layer: data plane.
//! Owns: one metric placement's dirty revision, exact checkpoint bytes and replication progress.
//! May depend on: metric snapshots, checkpoint replication and typed runtime state placements.
//! Must not know: transport, graph scheduling or control-plane orchestration.

use error_stack::ResultExt as _;
use nervix_checkpoint_replication::CheckpointReplication;
use nervix_models::ClusterNodeName;
use nervix_primitives::sync::atomic::{AtomicU64, Ordering};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use super::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
    StateReplicationRoles, lsm_sequence::LsmSequence,
};
use crate::metrics::{RuntimeMetrics, RuntimeMetricsSnapshot};

#[cfg(test)]
mod tests {
    use nervix_models::ModelKind;

    use super::*;
    use crate::{
        metrics::NodeBatchMetricsSpec,
        runtime::{
            RuntimeState,
            test_fixtures::{domain, named},
        },
    };

    #[test]
    fn a_metrics_revision_keeps_its_exact_bytes_until_the_next_dirty_mark() {
        let metrics = RuntimeMetrics::default();
        let node = ClusterNodeName::parse("node-1").expect("valid node");
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeState::BranchAggregated,
            kind: ModelKind::Ingestor,
            identifier: named("source"),
            branch_key: None,
        };
        let state = ReplicatedBranchAggregatedState::new(
            placement.clone(),
            Some(node.clone()),
            node.clone(),
            &metrics,
            None,
        )
        .expect("the metric placement initializes");
        let relay = named("events");
        let batch = metrics.resolve_node_batch_metrics(NodeBatchMetricsSpec {
            domain: &placement.domain,
            kind: placement.kind,
            node: &placement.identifier,
            relay: &relay,
            physical_node_id: Some(&node),
            direction: "sent",
            branch: None,
        });
        batch.observe(2, 64, None);
        state.mark_metrics_updated();
        let described = state
            .latest_snapshot(&metrics)
            .expect("capture the descriptor bytes");
        // A batch records its metrics before publishing the dirty mark. A request reaching
        // that interval must still stream the bytes already chosen for the current revision.
        batch.observe(3, 96, None);
        let streamed = state
            .latest_snapshot(&metrics)
            .expect("capture the stream bytes");
        assert_eq!(streamed.lsm, described.lsm);
        assert_eq!(streamed.payload, described.payload);
        state.mark_metrics_updated();
        let next = state
            .latest_snapshot(&metrics)
            .expect("capture the next metric revision");
        assert!(next.lsm > described.lsm);
        assert_ne!(next.payload, described.payload);
        let restored_metrics = RuntimeMetrics::default();
        let restored = ReplicatedBranchAggregatedState::new(
            placement,
            Some(node.clone()),
            node,
            &restored_metrics,
            Some(next.clone()),
        )
        .expect("restore the chosen checkpoint");
        let captured = restored
            .latest_snapshot(&restored_metrics)
            .expect("capture restored bytes");
        assert_eq!(captured.lsm, next.lsm);
        assert_eq!(captured.payload, next.payload);
        assert!(
            restored_metrics
                .describe_global_target(
                    &restored.placement.domain,
                    "INGESTOR",
                    &restored.placement.identifier,
                )
                .iter()
                .any(|line| line.contains("messages_total sent relay=events")
                    && line.contains("total=5"))
        );
        let applied_lsm = next.lsm + 1;
        restored
            .apply_snapshot(&restored_metrics, applied_lsm, &described.payload)
            .expect("a replica installs the selected bytes");
        let applied = restored
            .latest_snapshot(&restored_metrics)
            .expect("capture replica bytes");
        assert_eq!(applied.lsm, applied_lsm);
        assert_eq!(applied.payload, described.payload);
        let persisted = PersistedRuntimeStateEntry {
            lsm: applied_lsm + 1,
            payload: next.payload,
        };
        restored
            .restore_persisted_snapshot(&restored_metrics, persisted.clone())
            .expect("recovery installs the selected persisted bytes");
        let recovered = restored
            .latest_snapshot(&restored_metrics)
            .expect("capture recovered bytes");
        assert_eq!(recovered.lsm, persisted.lsm);
        assert_eq!(recovered.payload, persisted.payload);
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct BranchAggregatedRuntimeStateSnapshot {
    pub(super) metrics: RuntimeMetricsSnapshot,
}

#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        key = "one branch state placement",
        bound = "one installed role set, one encoded metric checkpoint and synchronous replacement",
        reason = "assignment roles and one exact metric checkpoint belong to the retained state"
    )
)]
pub(super) struct ReplicatedBranchAggregatedState {
    pub(super) placement: RuntimeStatePlacement,
    roles: nervix_primitives::sync::blocking::RwLock<StateReplicationRoles>,
    pub(super) physical_node_id: ClusterNodeName,
    pub(super) current_lsm: LsmSequence,
    pub(super) last_persisted_lsm: AtomicU64,
    /// A revision selects its bytes once. Elapsed metric fields cannot change its descriptor,
    /// stream or persisted payload when another request captures that same revision.
    snapshot: nervix_primitives::sync::blocking::Mutex<Option<PersistedRuntimeStateEntry>>,
    /// What each replica reported holding and the offer of the newest snapshot to them while this
    /// node aggregates the metrics, and the owner's announcements while it replicates them.
    replication: CheckpointReplication,
}

impl ReplicatedBranchAggregatedState {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "branch aggregate snapshot work executes outside per-record metric \
                      accumulation"
        )
    )]
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        primary_node: Option<ClusterNodeName>,
        physical_node_id: ClusterNodeName,
        metrics: &RuntimeMetrics,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> error_stack::Result<Self, RuntimePersistenceError> {
        let mut current_lsm = 0;
        let mut last_persisted_lsm = 0;
        if let Some(initial) = initial.as_ref() {
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
            roles: nervix_primitives::sync::blocking::RwLock::new(StateReplicationRoles::owned_by(
                primary_node,
            )),
            physical_node_id,
            current_lsm: LsmSequence::restored(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            snapshot: nervix_primitives::sync::blocking::Mutex::new(initial),
            replication: CheckpointReplication::new(),
        })
    }

    pub(super) fn primary_node(&self) -> Option<ClusterNodeName> {
        self.roles.read().primary_node.clone()
    }

    pub(super) fn rebind_roles(&self, roles: StateReplicationRoles) {
        *self.roles.write() = roles;
    }

    pub(super) fn replication(&self) -> &CheckpointReplication {
        &self.replication
    }

    /// The snapshot a flush persists now, or `None` when its revision is no newer than the last one
    /// persisted.
    ///
    /// The decision compares revisions, and a snapshot reads its revision before the metrics, so
    /// an update a flush's snapshot missed has a newer revision than the one it persisted and the
    /// next flush persists it.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "branch aggregate snapshot work executes outside per-record metric \
                      accumulation"
        )
    )]
    pub(super) fn snapshot_to_persist(
        &self,
        metrics: &RuntimeMetrics,
    ) -> error_stack::Result<Option<PersistedRuntimeStateEntry>, RuntimePersistenceError> {
        let persisted = self.last_persisted_lsm.load(Ordering::SeqCst);
        if self.current_lsm.current() <= persisted {
            return Ok(None);
        }
        let snapshot = self.latest_snapshot(metrics)?;
        if snapshot.lsm <= persisted {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }

    /// Records that the snapshot at revision `lsm` is persisted.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "branch aggregate snapshot work executes outside per-record metric \
                      accumulation"
        )
    )]
    pub(super) fn persisted(&self, lsm: u64) {
        self.last_persisted_lsm.fetch_max(lsm, Ordering::SeqCst);
    }

    pub(super) fn mark_metrics_updated(&self) -> u64 {
        self.current_lsm.advance()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "branch aggregate snapshot work executes outside per-record metric \
                      accumulation"
        )
    )]
    pub(super) fn latest_snapshot(
        &self,
        metrics: &RuntimeMetrics,
    ) -> error_stack::Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        // Capture and installation serialize at this placement's cold snapshot boundary. No
        // metric recording takes this guard, and it never crosses an await.
        let mut captured = self.snapshot.lock();
        // The revision is read first, so an update whose metrics this snapshot misses has a newer
        // revision than the one the snapshot is stamped with.
        let lsm = self.current_lsm.current();
        if let Some(snapshot) = captured.as_ref()
            && snapshot.lsm == lsm
        {
            return Ok(snapshot.clone());
        }
        let snapshot = BranchAggregatedRuntimeStateSnapshot {
            metrics: metrics.snapshot_global_target(
                &self.placement.domain,
                self.placement.kind,
                &self.placement.identifier,
                &self.physical_node_id,
            ),
        };
        let snapshot = PersistedRuntimeStateEntry {
            lsm,
            payload: encode_branch_aggregated_snapshot(&snapshot)?,
        };
        *captured = Some(snapshot.clone());
        Ok(snapshot)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "branch aggregate snapshot work executes outside per-record metric \
                      accumulation"
        )
    )]
    pub(super) fn apply_snapshot(
        &self,
        metrics: &RuntimeMetrics,
        lsm: u64,
        payload: &[u8],
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let mut captured = self.snapshot.lock();
        let snapshot = decode_branch_aggregated_snapshot(payload)?;
        metrics.apply_global_target_snapshot(
            &self.placement.domain,
            self.placement.kind,
            &self.placement.identifier,
            &self.physical_node_id,
            snapshot.metrics,
        );
        self.current_lsm.adopt(lsm);
        *captured = Some(PersistedRuntimeStateEntry {
            lsm,
            payload: payload.to_vec(),
        });
        Ok(())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "branch aggregate snapshot work executes outside per-record metric \
                      accumulation"
        )
    )]
    pub(super) fn restore_persisted_snapshot(
        &self,
        metrics: &RuntimeMetrics,
        snapshot: PersistedRuntimeStateEntry,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let mut captured = self.snapshot.lock();
        let current_lsm = self.current_lsm.current();
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
        self.current_lsm.adopt(snapshot.lsm);
        self.last_persisted_lsm
            .store(snapshot.lsm, Ordering::SeqCst);
        *captured = Some(snapshot);
        Ok(())
    }
}

pub(super) fn encode_branch_aggregated_snapshot(
    snapshot: &BranchAggregatedRuntimeStateSnapshot,
) -> error_stack::Result<Vec<u8>, RuntimePersistenceError> {
    rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)
        .map(|bytes| bytes.to_vec())
        .change_context(RuntimePersistenceError::EncodeState)
}

pub(super) fn decode_branch_aggregated_snapshot(
    payload: &[u8],
) -> error_stack::Result<BranchAggregatedRuntimeStateSnapshot, RuntimePersistenceError> {
    rkyv::from_bytes::<BranchAggregatedRuntimeStateSnapshot, rkyv::rancor::Error>(payload)
        .change_context(RuntimePersistenceError::DecodeState)
}

/// A generated metrics snapshot stores through `stored`, the checkpoint envelope a node keeps it
/// in, and restores a snapshot that encodes to exactly the stored bytes. The snapshot holds plain
/// numbers, text and lists, so equal encodings are equal values, floats compared by their bits.
#[cfg(test)]
pub(in crate::runtime) fn assert_generated_metrics_survive(
    arbitrary: &mut nervix_arbitrary::Arbitrary<'_>,
    stored: impl FnOnce(Vec<u8>) -> Vec<u8>,
) {
    use meticulous::ResultExt as _;

    let snapshot = BranchAggregatedRuntimeStateSnapshot {
        metrics: RuntimeMetricsSnapshot::generated(arbitrary),
    };
    let payload = encode_branch_aggregated_snapshot(&snapshot)
        .assured("a bounded generated metrics snapshot encodes");
    let restored = decode_branch_aggregated_snapshot(&stored(payload.clone()))
        .assured("a stored metrics snapshot decodes from its own encoding");
    let restored =
        encode_branch_aggregated_snapshot(&restored).assured("a decoded metrics snapshot encodes");
    assert_eq!(restored, payload);
}

/// Arbitrary bytes read as a stored metrics snapshot either fail with the typed decode failure or
/// restore a snapshot that stores back unchanged.
#[cfg(test)]
pub(in crate::runtime) fn assert_metrics_payload_decodes_typed(payload: &[u8]) {
    use meticulous::ResultExt as _;

    match decode_branch_aggregated_snapshot(payload) {
        Ok(snapshot) => {
            let encoded = encode_branch_aggregated_snapshot(&snapshot)
                .assured("a decoded metrics snapshot encodes");
            let again = decode_branch_aggregated_snapshot(&encoded)
                .assured("a re-encoded metrics snapshot decodes");
            let again = encode_branch_aggregated_snapshot(&again)
                .assured("a decoded metrics snapshot encodes");
            assert_eq!(again, encoded);
        }
        Err(error) => assert!(
            matches!(
                error.current_context(),
                RuntimePersistenceError::DecodeState
            ),
            "{error:?}"
        ),
    }
}
