//! The guest-state checkpoints of one WASM processor branch, on the node that executes the branch.
//!
//! Layer: data plane.
//! - **Owns.** The committed and published checkpoints of one branch, the revisions they are
//!   stamped with, the typed stages a checkpoint passes through, the boundary each checkpoint has
//!   to reach, the progress of the latest checkpoint, and the durable progress each replica
//!   reports.
//! - **Depends on.** Runtime-state placements, the revision sequence and cluster node names.
//! - **Must not know.** How a checkpoint reaches stable storage or a replica, guest execution, or
//!   which acknowledgements a completed checkpoint releases.

use std::{collections::BTreeSet, num::NonZeroU64, sync::Arc as StdArc};

use ahash::RandomState;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::sync::{ArcSwap, ArcSwapOption, DashMap};
use nervix_interconnect::RuntimeState;
use nervix_models::{
    ClusterNodeName, WasmCheckpointInspection, WasmCheckpointStage, WasmStateGeneration,
};
use tokio::sync::Notify;

use super::{PersistedRuntimeStateEntry, RuntimeStatePlacement, lsm_sequence::LsmSequence};

/// One branch's guest-state checkpoints.
///
/// A checkpoint is captured, written to this node's stable storage, and confirmed by the replicas
/// its boundary names, in that order and one at a time per branch. Only a checkpoint that reached
/// its boundary becomes the committed checkpoint a new guest instance restores. A checkpoint that
/// fails leaves the committed checkpoint where it was.
#[derive(Debug)]
pub(super) struct ReplicatedWasmProcessorState {
    pub(super) placement: RuntimeStatePlacement,
    /// The last checkpoint that reached its boundary. Replaced as one pointer, so nothing that
    /// reads it waits for the branch task or makes it copy the guest buffer.
    committed: ArcSwap<WasmGuestState>,
    /// The newest checkpoint on this node's stable storage, which is what replicas synchronize. It
    /// runs ahead of the committed checkpoint while a checkpoint waits for its replicas, and stays
    /// ahead when that wait fails.
    published: ArcSwap<WasmGuestState>,
    /// Allocates the revision each capture is stamped with. A revision is never reused, including
    /// the revision of a checkpoint that failed, because a replica or this node's storage may still
    /// hold it.
    current_lsm: LsmSequence,
    /// The latest checkpoint's non-sensitive revision and boundary, published as one read-only
    /// observation. Absence means this lifetime has not captured a checkpoint on this owner.
    latest: ArcSwapOption<ObservedWasmCheckpoint>,
    /// The highest revision each replica reported holding on its stable storage.
    replica_progress: DashMap<ClusterNodeName, u64, RandomState>,
    /// Wakes a checkpoint waiting for its replicas when one of them reports progress.
    replication_notify: Notify,
}

/// The bytes a WASM processor guest returned when its state was saved, and the revision of that
/// save.
#[derive(Debug)]
pub(super) struct WasmGuestState {
    revision: u64,
    bytes: Vec<u8>,
}

/// Saved guest state a new guest instance restores, and the revision it was saved at.
pub(super) struct RestorableGuestState<'a> {
    pub(super) revision: u64,
    pub(super) bytes: &'a [u8],
}

/// Where the latest checkpoint of one branch stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WasmCheckpointProgress {
    /// The latest checkpoint reached its boundary, so the committed checkpoint is current.
    Committed,
    /// The guest saved its state, and the state is being written to this node's stable storage.
    Captured,
    /// The state is on this node's stable storage and waits for the replicas its boundary names.
    LocallyDurable,
    /// The latest checkpoint did not reach its boundary, so the previous committed checkpoint
    /// stays current.
    Failed,
}

#[derive(Debug, Clone)]
struct ObservedWasmCheckpoint {
    revision: NonZeroU64,
    boundary: WasmCheckpointBoundary,
    progress: WasmCheckpointProgress,
}

/// The boundary a checkpoint has to reach before the success acknowledgements it covers are
/// released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WasmCheckpointBoundary {
    /// The schedule assigns the processor no replicas, so this node's stable storage completes the
    /// checkpoint.
    LocalStorage,
    /// The checkpoint also has to be on the stable storage of every one of these replicas.
    Replicas(WasmCheckpointReplicas),
}

/// The replicas one checkpoint waits for. There is always at least one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WasmCheckpointReplicas(BTreeSet<ClusterNodeName>);

/// A checkpoint promised to a number of replicas that the schedule no longer assigns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WasmCheckpointReplicasShrunk {
    pub(super) required: usize,
    pub(super) assigned: usize,
}

impl WasmCheckpointBoundary {
    /// The boundary of a checkpoint taken while the schedule assigns `replicas` to the processor.
    pub(super) fn assigned(replicas: BTreeSet<ClusterNodeName>) -> Self {
        if replicas.is_empty() {
            return Self::LocalStorage;
        }
        Self::Replicas(WasmCheckpointReplicas(replicas))
    }
}

impl WasmCheckpointReplicas {
    pub(super) fn nodes(&self) -> &BTreeSet<ClusterNodeName> {
        &self.0
    }

    /// The replicas this checkpoint waits for once the schedule assigns `assigned`.
    ///
    /// The replicas follow the schedule, so a replaced replica is replaced in the wait as well. The
    /// number of replicas never falls: a checkpoint promised to some number of replicas is refused
    /// rather than completed with fewer.
    pub(super) fn followed(
        self,
        assigned: WasmCheckpointBoundary,
    ) -> Result<Self, WasmCheckpointReplicasShrunk> {
        let assigned = match assigned {
            WasmCheckpointBoundary::LocalStorage => {
                return Err(WasmCheckpointReplicasShrunk {
                    required: self.0.len(),
                    assigned: 0,
                });
            }
            WasmCheckpointBoundary::Replicas(assigned) => assigned,
        };
        if assigned.0.len() < self.0.len() {
            return Err(WasmCheckpointReplicasShrunk {
                required: self.0.len(),
                assigned: assigned.0.len(),
            });
        }
        Ok(assigned)
    }
}

/// Guest state the guest has just saved, stamped with its revision, before any of it is on stable
/// storage.
#[derive(Debug)]
pub(super) struct CapturedWasmCheckpoint {
    saved: StdArc<WasmGuestState>,
    boundary: WasmCheckpointBoundary,
}

/// A captured checkpoint that is on this node's stable storage.
#[derive(Debug)]
pub(super) struct LocallyDurableWasmCheckpoint {
    saved: StdArc<WasmGuestState>,
    boundary: WasmCheckpointBoundary,
}

/// A checkpoint that reached its boundary: on this node's stable storage, and on the stable storage
/// of every replica its boundary names. Committing one is what lets the acknowledgements it covers
/// be released.
#[derive(Debug)]
pub(super) struct CompletedWasmCheckpoint {
    saved: StdArc<WasmGuestState>,
}

impl CapturedWasmCheckpoint {
    pub(super) fn revision(&self) -> u64 {
        self.saved.revision
    }

    /// The saved buffer, shared rather than copied, for the storage job that writes it.
    pub(super) fn saved(&self) -> StdArc<WasmGuestState> {
        self.saved.clone()
    }
}

impl LocallyDurableWasmCheckpoint {
    pub(super) fn revision(&self) -> u64 {
        self.saved.revision
    }

    pub(super) fn boundary(&self) -> &WasmCheckpointBoundary {
        &self.boundary
    }

    /// This checkpoint, once the caller has established that it reached its boundary: on this
    /// node's stable storage when its boundary is local storage, and confirmed by every replica
    /// otherwise.
    pub(super) fn completed(self) -> CompletedWasmCheckpoint {
        CompletedWasmCheckpoint { saved: self.saved }
    }
}

impl<'a> RestorableGuestState<'a> {
    /// The guest state a transferred snapshot carries, or `None` when the guest had saved nothing.
    pub(super) fn of_snapshot(snapshot: &'a PersistedRuntimeStateEntry) -> Option<Self> {
        if snapshot.payload.is_empty() {
            return None;
        }
        Some(Self {
            revision: snapshot.lsm,
            bytes: &snapshot.payload,
        })
    }
}

impl WasmGuestState {
    pub(super) fn revision(&self) -> u64 {
        self.revision
    }

    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The state a new guest instance restores from, or `None` while the guest has saved nothing.
    pub(super) fn restorable(&self) -> Option<RestorableGuestState<'_>> {
        if self.bytes.is_empty() {
            return None;
        }
        Some(RestorableGuestState {
            revision: self.revision,
            bytes: &self.bytes,
        })
    }

    fn snapshot(&self) -> PersistedRuntimeStateEntry {
        PersistedRuntimeStateEntry {
            lsm: self.revision,
            payload: self.bytes.clone(),
        }
    }
}

impl ReplicatedWasmProcessorState {
    /// The checkpoints of a branch whose committed checkpoint is `initial`: what this node's
    /// storage holds for the branch or an ownership transfer handed over, or nothing for a branch
    /// that has never saved state.
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Self {
        let mut committed = WasmGuestState {
            revision: 0,
            bytes: Vec::new(),
        };
        if let Some(initial) = initial {
            committed = WasmGuestState {
                revision: initial.lsm,
                bytes: initial.payload,
            };
        }
        let committed = StdArc::new(committed);
        Self {
            placement,
            current_lsm: LsmSequence::restored(committed.revision),
            published: ArcSwap::from(committed.clone()),
            committed: ArcSwap::from(committed),
            latest: ArcSwapOption::empty(),
            replica_progress: DashMap::default(),
            replication_notify: Notify::new(),
        }
    }

    /// The committed checkpoint, which a new guest instance restores from.
    pub(super) fn restore_guest_state(&self) -> StdArc<WasmGuestState> {
        self.committed.load_full()
    }

    /// The guest-state lifetime these checkpoints belong to.
    pub(super) fn generation(&self) -> WasmStateGeneration {
        let named = match self.placement.state {
            RuntimeState::WasmProcessor { generation, .. } => Some(generation),
            _ => None,
        };
        named.assured("a WASM processor's state is only ever placed as WASM processor state")
    }

    /// The revision of the committed checkpoint.
    #[cfg(test)]
    pub(super) fn committed_revision(&self) -> u64 {
        self.committed.load().revision
    }

    /// Where the latest checkpoint of this branch stands.
    #[cfg(test)]
    pub(super) fn progress(&self) -> WasmCheckpointProgress {
        match self.latest.load().as_deref() {
            Some(observed) => observed.progress,
            None => WasmCheckpointProgress::Committed,
        }
    }

    fn record_progress(&self, progress: WasmCheckpointProgress) {
        let observed = self
            .latest
            .load_full()
            .assured("checkpoint progress changes only after capture published its observation");
        self.latest.store(Some(StdArc::new(ObservedWasmCheckpoint {
            progress,
            ..(*observed).clone()
        })));
    }

    /// A read-only observation of the latest checkpoint and its completed predecessor. Replica
    /// reports are read as progress facts; this does not synchronize or advance a checkpoint.
    pub(super) fn inspection(&self) -> WasmCheckpointInspection {
        let committed = self.committed.load_full();
        let committed_revision = NonZeroU64::new(committed.revision);
        let latest = self.latest.load_full();
        let Some(latest) = latest else {
            return WasmCheckpointInspection {
                branch: self
                    .placement
                    .branch_key
                    .as_ref()
                    .map(|key| key.fingerprint()),
                generation: self.generation(),
                committed_revision,
                latest_revision: committed_revision,
                stage: match committed_revision {
                    Some(_) => WasmCheckpointStage::LocallyDurable,
                    None => WasmCheckpointStage::Empty,
                },
                required_replicas: None,
                confirmed_replicas: None,
            };
        };
        let (required_replicas, confirmed_replicas) = match &latest.boundary {
            WasmCheckpointBoundary::LocalStorage => (Some(0), Some(0)),
            WasmCheckpointBoundary::Replicas(replicas) => {
                let required = u32::try_from(replicas.nodes().len())
                    .assured("a cluster cannot contain more than u32::MAX nodes");
                let awaiting = self.replicas_awaiting(replicas, latest.revision.get());
                let confirmed = required
                    .checked_sub(u32::try_from(awaiting.len()).assured(
                        "the awaiting set is a subset of the u32-bounded required replicas",
                    ))
                    .assured("the awaiting set cannot exceed the required replicas");
                (Some(required), Some(confirmed))
            }
        };
        let stage = match latest.progress {
            WasmCheckpointProgress::Committed => WasmCheckpointStage::ReplicaConfirmed,
            WasmCheckpointProgress::Captured => WasmCheckpointStage::Captured,
            WasmCheckpointProgress::LocallyDurable => WasmCheckpointStage::LocallyDurable,
            WasmCheckpointProgress::Failed => WasmCheckpointStage::Failed,
        };
        WasmCheckpointInspection {
            branch: self
                .placement
                .branch_key
                .as_ref()
                .map(|key| key.fingerprint()),
            generation: self.generation(),
            committed_revision,
            latest_revision: Some(latest.revision),
            stage,
            required_replicas,
            confirmed_replicas,
        }
    }

    /// Stamp `bytes`, what the guest returned from a save, with the next revision. Keeping the
    /// capture is one pointer, not a copy of the guest buffer.
    pub(super) fn capture(
        &self,
        bytes: Vec<u8>,
        boundary: WasmCheckpointBoundary,
    ) -> CapturedWasmCheckpoint {
        let saved = StdArc::new(WasmGuestState {
            revision: self.current_lsm.advance(),
            bytes,
        });
        let revision = NonZeroU64::new(saved.revision)
            .assured("a captured checkpoint advances the revision beyond zero");
        self.latest.store(Some(StdArc::new(ObservedWasmCheckpoint {
            revision,
            boundary: boundary.clone(),
            progress: WasmCheckpointProgress::Captured,
        })));
        CapturedWasmCheckpoint { saved, boundary }
    }

    /// Record that `captured` is on this node's stable storage, which is what lets replicas fetch
    /// it.
    pub(super) fn record_locally_durable(
        &self,
        captured: CapturedWasmCheckpoint,
    ) -> LocallyDurableWasmCheckpoint {
        let CapturedWasmCheckpoint { saved, boundary } = captured;
        self.published.store(saved.clone());
        self.record_progress(WasmCheckpointProgress::LocallyDurable);
        LocallyDurableWasmCheckpoint { saved, boundary }
    }

    /// Make `completed` the committed checkpoint, which a new guest instance restores from.
    pub(super) fn commit(&self, completed: CompletedWasmCheckpoint) {
        self.committed.store(completed.saved);
        self.record_progress(WasmCheckpointProgress::Committed);
    }

    /// Record that the latest checkpoint did not reach its boundary. The committed checkpoint
    /// stays where it was.
    pub(super) fn record_failed(&self) {
        self.record_progress(WasmCheckpointProgress::Failed);
    }

    /// The committed checkpoint, as an ownership handoff transfers it.
    pub(super) fn latest_snapshot(&self) -> PersistedRuntimeStateEntry {
        self.committed.load_full().snapshot()
    }

    /// The newest checkpoint on this node's stable storage when its revision is after `after_lsm`,
    /// as a replica synchronizes it.
    pub(super) fn snapshot_after(
        &self,
        after_lsm: Option<u64>,
    ) -> Option<PersistedRuntimeStateEntry> {
        let published = self.published.load_full();
        if let Some(after_lsm) = after_lsm
            && published.revision <= after_lsm
        {
            return None;
        }
        Some(published.snapshot())
    }

    /// Record that `node` reported holding revision `lsm` on its stable storage.
    ///
    /// A replica reports its progress in order, and an older report never lowers what is recorded.
    /// Two reports of one replica racing here can at worst record the lower revision, which only
    /// delays a confirmation until that replica's next report: nothing is ever recorded above what
    /// a replica reported.
    pub(super) fn mark_replica_progress(&self, node: &ClusterNodeName, lsm: u64) {
        let recorded = self.replica_progress.get(node).map(|held| *held);
        let reported = match recorded {
            Some(recorded) => recorded.max(lsm),
            None => lsm,
        };
        self.replica_progress.insert(node.clone(), reported);
        self.replication_notify.notify_waiters();
    }

    /// The replicas of `replicas` that have not reported holding revision `lsm`.
    pub(super) fn replicas_awaiting(
        &self,
        replicas: &WasmCheckpointReplicas,
        lsm: u64,
    ) -> BTreeSet<ClusterNodeName> {
        let mut awaiting = BTreeSet::new();
        for replica in replicas.nodes() {
            let held = self.replica_progress.get(replica).map(|held| *held);
            let holds = match held {
                Some(held) => held >= lsm,
                None => false,
            };
            if !holds {
                awaiting.insert(replica.clone());
            }
        }
        awaiting
    }

    /// Signals every replica report. A waiter registers for the next signal before it reads the
    /// progress it waits for, so a report that lands in between is not missed.
    pub(super) fn replica_progress_signal(&self) -> &Notify {
        &self.replication_notify
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        DomainName, FieldName, ModelKind, ModelName, SchemaFingerprint, WasmStateGeneration,
    };

    use super::*;
    use crate::{
        runtime::{BranchKey, RuntimeState},
        runtime_schema::RuntimeValue,
    };

    fn placement() -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("test").expect("valid domain"),
            state: RuntimeState::WasmProcessor {
                schema: SchemaFingerprint::from_digest([7; 32]),
                generation: WasmStateGeneration::FIRST,
            },
            kind: ModelKind::WasmProcessor,
            identifier: ModelName::parse("filter").expect("valid identifier"),
            branch_key: BranchKey::from_fields([(
                FieldName::parse("tenant").expect("valid identifier"),
                RuntimeValue::String("acme".to_string()),
            )])
            .expect("test branch key must be non-empty")
            .into(),
        }
    }

    fn node(name: &str) -> ClusterNodeName {
        ClusterNodeName::parse(name).expect("valid name")
    }

    fn replicas(names: &[&str]) -> WasmCheckpointBoundary {
        WasmCheckpointBoundary::assigned(names.iter().map(|name| node(name)).collect())
    }

    fn replica_set(names: &[&str]) -> WasmCheckpointReplicas {
        let WasmCheckpointBoundary::Replicas(replicas) = replicas(names) else {
            panic!("a non-empty assignment names replicas");
        };
        replicas
    }

    #[test]
    fn a_checkpoint_waits_for_every_replica_it_names() {
        let state = ReplicatedWasmProcessorState::new(placement(), None);
        let replicas = replica_set(&["node-2", "node-3"]);
        let captured = state.capture(
            vec![1, 2, 3],
            WasmCheckpointBoundary::Replicas(replicas.clone()),
        );
        let lsm = captured.revision();

        assert_eq!(
            state.replicas_awaiting(&replicas, lsm),
            BTreeSet::from([node("node-2"), node("node-3")])
        );
        state.mark_replica_progress(&node("node-2"), lsm);
        assert_eq!(
            state.replicas_awaiting(&replicas, lsm),
            BTreeSet::from([node("node-3")])
        );
        state.mark_replica_progress(&node("node-3"), lsm);
        assert!(state.replicas_awaiting(&replicas, lsm).is_empty());

        let older = lsm
            .checked_sub(1)
            .expect("the first capture is revision one");
        state.mark_replica_progress(&node("node-3"), older);
        assert!(
            state.replicas_awaiting(&replicas, lsm).is_empty(),
            "an older report must not lower the progress a replica already reported"
        );
    }

    #[test]
    fn a_checkpoint_restores_raw_guest_bytes() {
        let initial = PersistedRuntimeStateEntry {
            lsm: 7,
            payload: vec![9, 8, 7],
        };
        let state = ReplicatedWasmProcessorState::new(placement(), Some(initial));

        let saved = state.restore_guest_state();
        let restorable = saved
            .restorable()
            .expect("a persisted payload must be restorable");
        assert_eq!(restorable.bytes, [9_u8, 8, 7].as_slice());
        assert_eq!(restorable.revision, 7);
    }

    /// Guest state is saved after every callback. Keeping it for persistence, replication and the
    /// next guest instance must not copy the whole guest buffer each time.
    #[test]
    fn a_capture_keeps_the_saved_buffer_without_copying_it() {
        let state = ReplicatedWasmProcessorState::new(placement(), None);
        let guest_state = vec![7_u8; 4_096];
        let saved_buffer = guest_state.as_ptr();
        let captured = state.capture(guest_state, WasmCheckpointBoundary::LocalStorage);
        assert_eq!(
            captured.saved().bytes().as_ptr(),
            saved_buffer,
            "capturing guest state copied the whole guest buffer"
        );

        let durable = state.record_locally_durable(captured);
        state.commit(durable.completed());
        assert_eq!(
            state.restore_guest_state().bytes().as_ptr(),
            saved_buffer,
            "committing guest state copied the whole guest buffer"
        );
    }

    /// Only a checkpoint that reached its boundary is what a new guest instance restores. A capture
    /// on its way, and one that failed, leave the committed checkpoint in place, and a failed
    /// revision is never stamped on a later capture.
    #[test]
    fn only_a_completed_checkpoint_becomes_the_committed_checkpoint() {
        let state = ReplicatedWasmProcessorState::new(placement(), None);
        assert_eq!(state.inspection().stage, WasmCheckpointStage::Empty);
        let first = state.capture(vec![1], WasmCheckpointBoundary::LocalStorage);
        assert_eq!(state.progress(), WasmCheckpointProgress::Captured);
        let captured = state.inspection();
        assert_eq!(captured.stage, WasmCheckpointStage::Captured);
        assert_eq!(captured.committed_revision, None);
        assert_eq!(captured.required_replicas, Some(0));
        assert_eq!(captured.confirmed_replicas, Some(0));
        let first = state.record_locally_durable(first);
        assert_eq!(state.progress(), WasmCheckpointProgress::LocallyDurable);
        assert_eq!(
            state.inspection().stage,
            WasmCheckpointStage::LocallyDurable
        );
        state.commit(first.completed());
        assert_eq!(state.progress(), WasmCheckpointProgress::Committed);
        assert_eq!(
            state.inspection().stage,
            WasmCheckpointStage::ReplicaConfirmed
        );
        assert_eq!(state.committed_revision(), 1);

        let failed = state.capture(vec![2], replicas(&["node-2"]));
        let failed = state.record_locally_durable(failed);
        let awaiting = state.inspection();
        assert_eq!(awaiting.stage, WasmCheckpointStage::LocallyDurable);
        assert_eq!(awaiting.required_replicas, Some(1));
        assert_eq!(awaiting.confirmed_replicas, Some(0));
        assert_eq!(failed.revision(), 2);
        state.record_failed();
        assert_eq!(state.progress(), WasmCheckpointProgress::Failed);
        let failed_inspection = state.inspection();
        assert_eq!(failed_inspection.stage, WasmCheckpointStage::Failed);
        assert_eq!(failed_inspection.committed_revision, NonZeroU64::new(1));
        assert_eq!(state.committed_revision(), 1);
        assert_eq!(state.restore_guest_state().bytes(), [1_u8].as_slice());
        assert_eq!(
            state.snapshot_after(Some(1)).map(|snapshot| snapshot.lsm),
            Some(2),
            "a replica keeps being offered the newest checkpoint on this node's storage"
        );
        assert_eq!(state.latest_snapshot().lsm, 1);

        let retried = state.capture(vec![3], WasmCheckpointBoundary::LocalStorage);
        assert_eq!(retried.revision(), 3);
    }

    #[test]
    fn inspection_counts_only_replica_reports_for_the_current_revision() {
        let state = ReplicatedWasmProcessorState::new(placement(), None);
        let captured = state.capture(vec![1], replicas(&["node-2", "node-3"]));
        let durable = state.record_locally_durable(captured);
        state.mark_replica_progress(&node("node-2"), 1);
        let inspection = state.inspection();
        assert_eq!(inspection.required_replicas, Some(2));
        assert_eq!(inspection.confirmed_replicas, Some(1));
        state.mark_replica_progress(&node("node-3"), 1);
        assert_eq!(state.inspection().confirmed_replicas, Some(2));
        state.commit(durable.completed());
        assert_eq!(
            state.inspection().stage,
            WasmCheckpointStage::ReplicaConfirmed
        );

        let next = state.capture(vec![2], replicas(&["node-2", "node-3"]));
        assert_eq!(state.inspection().confirmed_replicas, Some(0));
        state.record_locally_durable(next);
        assert_eq!(state.inspection().committed_revision, NonZeroU64::new(1));
    }

    #[test]
    fn a_checkpoint_follows_replaced_replicas_but_never_completes_with_fewer() {
        let promised = replica_set(&["node-2"]);

        let replaced = promised
            .clone()
            .followed(replicas(&["node-3"]))
            .expect("a replaced replica is followed");
        assert_eq!(replaced, replica_set(&["node-3"]));

        assert_eq!(
            promised
                .clone()
                .followed(WasmCheckpointBoundary::LocalStorage),
            Err(WasmCheckpointReplicasShrunk {
                required: 1,
                assigned: 0,
            })
        );
        assert_eq!(
            replica_set(&["node-2", "node-3"]).followed(replicas(&["node-3"])),
            Err(WasmCheckpointReplicasShrunk {
                required: 2,
                assigned: 1,
            })
        );
        assert_eq!(
            promised.followed(replicas(&["node-2", "node-3"])),
            Ok(replica_set(&["node-2", "node-3"]))
        );
    }
}
