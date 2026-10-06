//! Production replacement and purge boundaries within a selected restored generation.
//!
//! Layer: test harness.
//! - **Owns.** Replica, ownership handoff, forced recovery and queued writer regressions.
//! - **Depends on.** The production storage owners and one complete restored checkpoint.
//! - **Must not know.** Graph scheduling, record delivery or archive compatibility.

use super::*;

#[test]
pub(in super::super) fn replica_and_ownership_replacement_release_their_restored_chunks() {
    for replacement in [
        Replacement::Replica,
        Replacement::Handoff,
        Replacement::Recovery,
    ] {
        let directory = tempfile::tempdir().assured("the test directory opens");
        let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let checkpoint = placement("orders");
        let payload = vec![7; RESTORE_STATE_CHUNK_BYTES + 3];
        publish(&store, &checkpoint, &payload);
        replacement.apply(&store, &checkpoint);
        let swept = store
            .reclaim_restore_staging(|_, _| false, || Ok(()))
            .assured("replacement maintenance completes");
        assert!(swept.reclaimed_bytes > u64::try_from(payload.len()).verified("test payload fits"));
        assert_eq!(
            store.checkpoint_chunks.len().assured("chunk count reads"),
            0
        );
        assert_eq!(
            store
                .latest_snapshot(&checkpoint)
                .assured("current state reads")
                .assured("replacement exists"),
            PersistedRuntimeStateEntry {
                lsm: 4,
                payload: b"replacement".to_vec()
            }
        );
    }
}

enum Replacement {
    Replica,
    Handoff,
    Recovery,
}

impl Replacement {
    fn apply(self, store: &RuntimeStateStore, placement: &RuntimeStatePlacement) {
        let snapshot = PersistedRuntimeStateEntry {
            lsm: 4,
            payload: b"replacement".to_vec(),
        };
        let coordinator = ClusterNodeName::parse("leader").assured("the test coordinator is valid");
        let source = ClusterNodeName::parse("source").assured("the test source is valid");
        let destination =
            ClusterNodeName::parse("destination").assured("the test destination is valid");
        let entity = DomainNodeRef::node_in(
            placement.domain.clone(),
            placement.kind,
            placement.identifier.clone(),
        );
        match self {
            Self::Replica => {
                let installed = store
                    .latest_snapshot_writer()
                    .install_replica_if_newer(placement, snapshot.clone())
                    .assured("the ordinary replica path installs");
                assert_eq!(installed, Some(snapshot));
            }
            Self::Handoff => {
                let coordination = CoordinationIdentity::new(coordinator, 22, 1);
                let transition = RuntimeStateHandoffTransition {
                    coordination: &coordination,
                    operation_id: "handoff",
                    source: &source,
                    destination: &destination,
                    source_incarnation: ClusterNodeIncarnation::new(31),
                    destination_incarnation: ClusterNodeIncarnation::new(32),
                    entity: &entity,
                    base_schedule_fingerprint: [4; 32],
                    target_schedule_fingerprint: [5; 32],
                };
                let checkpoints = [(placement.clone(), snapshot)];
                store
                    .persist_handoff_preparation(&transition, &checkpoints)
                    .assured("handoff preparation persists");
                store
                    .activate_handoff_preparation(&transition, &checkpoints)
                    .assured("handoff activation replaces the selected checkpoint");
            }
            Self::Recovery => {
                let transition = ForcedRuntimeStateRecoveryTransition {
                    operation_id: "recovery",
                    source: &source,
                    destination: &destination,
                    destination_incarnation: ClusterNodeIncarnation::new(32),
                    entity: &entity,
                    target_schedule_fingerprint: [5; 32],
                };
                store
                    .persist_forced_recovery_preparation(
                        &transition,
                        &[(placement.clone(), snapshot)],
                    )
                    .assured("forced recovery preparation persists");
                assert!(
                    store
                        .activate_forced_recovery(
                            &transition,
                            ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                            None
                        )
                        .assured("forced recovery replaces the selected checkpoint")
                        .is_some()
                );
            }
        }
    }
}

#[test]
pub(in super::super) fn queued_writers_and_publication_retries_keep_only_current_references() {
    let directory = tempfile::tempdir().assured("the test directory opens");
    let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let checkpoint = placement("orders");
    let queued = store.latest_snapshot_writer();
    publish(&store, &checkpoint, b"selected");
    queued
        .write_latest_snapshot(&checkpoint, 99, b"queued-before-publication")
        .assured("the queued writer finishes fenced");
    store
        .reclaim_restore_staging(|_, _| false, || Ok(()))
        .assured("the live checkpoint is retained");
    assert_eq!(
        store
            .latest_snapshot(&checkpoint)
            .assured("selected state reads")
            .assured("selected state exists")
            .payload,
        b"selected"
    );
    let queued = store.latest_snapshot_writer();
    store
        .purge_entity(
            &checkpoint.domain,
            checkpoint.state.kind(),
            checkpoint.kind,
            checkpoint.identifier.clone(),
        )
        .assured("the selected entity purges");
    store
        .reclaim_restore_staging(|_, _| false, || Ok(()))
        .assured("maintenance reclaims before the queued writer runs");
    queued
        .write_latest_snapshot(&checkpoint, 4, b"queued-after-purge")
        .assured("the writer stays in its current namespace");
    store
        .reclaim_restore_staging(|_, _| false, || Ok(()))
        .assured("a later sweep preserves the inline checkpoint");
    assert_eq!(
        store
            .latest_snapshot(&checkpoint)
            .assured("current state reads")
            .assured("current state exists")
            .payload,
        b"queued-after-purge"
    );
    assert_eq!(
        store.checkpoint_chunks.len().assured("chunk count reads"),
        0
    );
}

#[test]
pub(in super::super) fn retiring_a_scheduled_identity_reclaims_only_its_checkpoint_chunks() {
    let directory = tempfile::tempdir().assured("the test directory opens");
    let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let checkpoint = placement("orders");
    publish(&store, &checkpoint, b"retired");
    store
        .purge_stale_state_identities(&checkpoint.domain, &HashMap::default())
        .assured("the production schedule purge retires its checkpoint");
    let swept = store
        .reclaim_restore_staging(|_, _| true, || Ok(()))
        .assured("active chunks follow their headers independently of execution retention");
    assert!(swept.reclaimed_bytes > 0);
    assert_eq!(
        store.checkpoint_chunks.len().assured("chunk count reads"),
        0
    );
    assert!(
        store
            .latest_snapshot(&checkpoint)
            .assured("the retired checkpoint reads")
            .is_none()
    );
}
