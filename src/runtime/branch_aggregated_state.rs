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
        bound = "one installed primary/replica role set and synchronous role replacement",
        reason = "assignment roles belong to the retained replicated state"
    )
)]
pub(super) struct ReplicatedBranchAggregatedState {
    pub(super) placement: RuntimeStatePlacement,
    roles: nervix_primitives::sync::blocking::RwLock<StateReplicationRoles>,
    pub(super) physical_node_id: ClusterNodeName,
    pub(super) current_lsm: LsmSequence,
    pub(super) last_persisted_lsm: AtomicU64,
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
            roles: nervix_primitives::sync::blocking::RwLock::new(StateReplicationRoles::owned_by(
                primary_node,
            )),
            physical_node_id,
            current_lsm: LsmSequence::restored(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
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
        // The revision is read first, so an update whose metrics this snapshot misses has a newer
        // revision than the one the snapshot is stamped with.
        let lsm = self.current_lsm.current();
        let snapshot = BranchAggregatedRuntimeStateSnapshot {
            metrics: metrics.snapshot_global_target(
                &self.placement.domain,
                self.placement.kind,
                &self.placement.identifier,
                &self.physical_node_id,
            ),
        };
        Ok(PersistedRuntimeStateEntry {
            lsm,
            payload: encode_branch_aggregated_snapshot(&snapshot)?,
        })
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
        let snapshot = decode_branch_aggregated_snapshot(payload)?;
        metrics.apply_global_target_snapshot(
            &self.placement.domain,
            self.placement.kind,
            &self.placement.identifier,
            &self.physical_node_id,
            snapshot.metrics,
        );
        self.current_lsm.adopt(lsm);
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
