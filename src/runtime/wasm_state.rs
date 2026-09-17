use std::sync::{
    Arc as StdArc,
    atomic::{AtomicU64, Ordering},
};

use ahash::RandomState;
use nervix_execution::sync::{ArcSwap, DashMap};
use nervix_models::ClusterNodeName;
use tokio::sync::Notify;

use super::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
    lsm_sequence::LsmSequence,
};

#[derive(Debug)]
pub(super) struct ReplicatedWasmProcessorState {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) required_replica_acks: usize,
    pub(super) replica_nodes: Vec<ClusterNodeName>,
    /// Replaced after every save, so keeping a save is one pointer replacement and nothing that
    /// reads the saved state waits for the branch task or makes it copy the guest buffer.
    saved: ArcSwap<WasmGuestState>,
    /// Allocates the revision each save is stamped with.
    current_lsm: LsmSequence,
    last_persisted_lsm: AtomicU64,
    pub(super) replica_progress: DashMap<String, u64, RandomState>,
    pub(super) replication_notify: Notify,
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

    fn snapshot(&self, placement: &RuntimeStatePlacement) -> PersistedRuntimeStateEntry {
        PersistedRuntimeStateEntry {
            lsm: self.revision,
            schema_fingerprint: placement.schema_fingerprint,
            payload: self.bytes.clone(),
        }
    }
}

impl ReplicatedWasmProcessorState {
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        replica_nodes: Vec<ClusterNodeName>,
        required_replica_acks: usize,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let mut saved = WasmGuestState {
            revision: 0,
            bytes: Vec::new(),
        };
        if let Some(initial) = initial {
            saved = WasmGuestState {
                revision: initial.lsm,
                bytes: initial.payload,
            };
        }
        Ok(Self {
            placement,
            required_replica_acks,
            replica_nodes,
            current_lsm: LsmSequence::restored(saved.revision),
            last_persisted_lsm: AtomicU64::new(saved.revision),
            saved: ArcSwap::from_pointee(saved),
            replica_progress: DashMap::default(),
            replication_notify: Notify::new(),
        })
    }

    /// The guest state saved last, which a new guest instance restores from.
    pub(super) fn restore_guest_state(&self) -> StdArc<WasmGuestState> {
        self.saved.load_full()
    }

    /// Keep `bytes`, what the guest returned from a save, as the guest state saved last.
    pub(super) fn replace_guest_state(&self, bytes: Vec<u8>) -> StdArc<WasmGuestState> {
        let saved = StdArc::new(WasmGuestState {
            revision: self.current_lsm.advance(),
            bytes,
        });
        self.saved.store(saved.clone());
        saved
    }

    /// The revision of the guest state saved last.
    pub(super) fn saved_revision(&self) -> u64 {
        self.saved.load().revision
    }

    /// Record that the guest state saved at `revision` is persisted. Persisting an older save
    /// afterwards never moves this back.
    pub(super) fn record_persisted(&self, revision: u64) {
        self.last_persisted_lsm
            .fetch_max(revision, Ordering::SeqCst);
    }

    /// Whether the guest state saved last is newer than the guest state persisted last.
    pub(super) fn is_dirty(&self) -> bool {
        self.saved_revision() > self.last_persisted_lsm.load(Ordering::SeqCst)
    }

    pub(super) fn latest_snapshot(&self) -> PersistedRuntimeStateEntry {
        self.saved.load_full().snapshot(&self.placement)
    }

    /// The guest state saved last when its revision is after `after_lsm`.
    pub(super) fn snapshot_after(
        &self,
        after_lsm: Option<u64>,
    ) -> Option<PersistedRuntimeStateEntry> {
        let saved = self.saved.load_full();
        if let Some(after_lsm) = after_lsm
            && saved.revision <= after_lsm
        {
            return None;
        }
        Some(saved.snapshot(&self.placement))
    }

    pub(super) fn mark_replica_progress(&self, node_id: &ClusterNodeName, lsm: u64) {
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
    use nervix_models::{DomainName, FieldName, ModelKind, ModelName};

    use super::*;
    use crate::{
        runtime::{BranchKey, RuntimeStateKind},
        runtime_schema::RuntimeValue,
    };

    fn placement() -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("test").expect("valid domain"),
            state: RuntimeStateKind::WasmProcessor,
            kind: ModelKind::WasmProcessor,
            identifier: ModelName::parse("filter").expect("valid identifier"),
            schema_fingerprint: [0; 32],
            branch_key: BranchKey::from_fields([(
                FieldName::parse("tenant").expect("valid identifier"),
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
            vec![
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
            2,
            None,
        )
        .expect("state should initialize");
        let saved = state.replace_guest_state(vec![1, 2, 3]);
        let lsm = saved.revision();

        assert_eq!(saved.bytes(), [1_u8, 2, 3].as_slice());
        assert!(!state.replica_quorum_satisfied(lsm));
        state.mark_replica_progress(&ClusterNodeName::parse("node-2").expect("valid name"), lsm);
        assert!(!state.replica_quorum_satisfied(lsm));
        state.mark_replica_progress(&ClusterNodeName::parse("node-3").expect("valid name"), lsm);
        assert!(state.replica_quorum_satisfied(lsm));
    }

    #[test]
    fn wasm_processor_state_restores_raw_guest_bytes() {
        let initial = PersistedRuntimeStateEntry {
            lsm: 7,
            schema_fingerprint: placement().schema_fingerprint,
            payload: vec![9, 8, 7],
        };
        let state = ReplicatedWasmProcessorState::new(placement(), Vec::new(), 0, Some(initial))
            .expect("state should initialize from persisted payload");

        let saved = state.restore_guest_state();
        let restorable = saved
            .restorable()
            .expect("a persisted payload must be restorable");
        assert_eq!(restorable.bytes, [9_u8, 8, 7].as_slice());
        assert_eq!(restorable.revision, 7);
    }

    /// Guest state is saved after every batch. Keeping it for persistence, replication and the next
    /// guest instance must not copy the whole guest buffer each time.
    #[test]
    fn saving_guest_state_keeps_the_saved_buffer_without_copying_it() {
        let state = ReplicatedWasmProcessorState::new(placement(), Vec::new(), 0, None)
            .expect("state should initialize");
        let guest_state = vec![7_u8; 4_096];
        let saved_buffer = guest_state.as_ptr();
        let saved = state.replace_guest_state(guest_state);

        assert_eq!(
            saved.bytes().as_ptr(),
            saved_buffer,
            "saving guest state copied the whole guest buffer"
        );
        assert!(
            StdArc::ptr_eq(&saved, &state.restore_guest_state()),
            "a new guest instance would not restore the state that was just saved"
        );
    }
}
