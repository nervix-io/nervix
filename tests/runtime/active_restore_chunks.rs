//! Reclamation of checkpoint chunks after production writes and purges.
//!
//! Layer: test harness.
//! - **Owns.** Current chunk liveness, complete snapshot readers and interruption regressions.
//! - **Depends on.** The production state store, its maintenance owner and Fjall snapshots.
//! - **Must not know.** Archive encodings, graph execution or consensus retention policy.

use nervix_interconnect::backup::RestoreStateInventory;

use super::{
    tests::{authority, placement, stage, store},
    *,
};

#[path = "active_restore_writers.rs"]
mod writers;

#[path = "active_restore_liveness.rs"]
mod liveness;

#[cfg(feature = "deloxide")]
pub(super) fn diagnostic_active_chunk_reclamation() {
    ordinary_replacement_reclaims_chunks_and_keeps_complete_snapshot_readers();
    entity_purge_reclaims_chunks_after_reopen();
    active_reclamation_retains_future_installations_and_exact_publication_retries();
    cancellation_during_active_deletion_preserves_snapshot_and_resumes_after_restart();
    writers::replica_and_ownership_replacement_release_their_restored_chunks();
    writers::queued_writers_and_publication_retries_keep_only_current_references();
    writers::retiring_a_scheduled_identity_reclaims_only_its_checkpoint_chunks();
    liveness::selected_segmented_revisions_are_retained_while_other_chunk_sets_are_reclaimed();
    liveness::segmented_replacement_reclaims_incomplete_revisions_in_every_active_namespace();
    liveness::large_inline_replacement_needs_no_large_caller_owned_header_copy();
    liveness::malformed_active_chunks_and_headers_fail_before_deletion();
}

fn publish(store: &RuntimeStateStore, placement: &RuntimeStatePlacement, payload: &[u8]) {
    stage(store, 19, placement, payload);
    store
        .publish_restored_state(
            &placement.domain,
            &authority(19),
            RestoreStateInventory {
                checkpoints: 1,
                payload_bytes: u64::try_from(payload.len()).verified("test payload fits u64"),
            },
            || Ok(()),
        )
        .assured("the complete checkpoint generation publishes");
}

#[test]
pub(super) fn ordinary_replacement_reclaims_chunks_and_keeps_complete_snapshot_readers() {
    let directory = tempfile::tempdir().assured("the test directory opens");
    let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let checkpoint = placement("orders");
    let payload = vec![7; RESTORE_STATE_CHUNK_BYTES * 3 + 13];
    publish(&store, &checkpoint, &payload);
    store
        .checkpoint_chunks
        .rotate_memtable_and_wait()
        .assured("published chunks flush to SST files");
    let published_disk_bytes = store.checkpoint_chunks.disk_space();
    let reader = store.db.snapshot();
    let key = StateNamespace::Restored(19)
        .key(&checkpoint)
        .assured("placement fits");
    let header = reader
        .get(&store.latest, &key)
        .assured("snapshot reads")
        .assured("header exists");
    store
        .persist_latest_snapshot(&checkpoint, 4, b"resumed")
        .assured("ordinary checkpoint replaces the restored header");
    let swept = store
        .reclaim_restore_staging(|_, _| false, || Ok(()))
        .assured("maintenance completes");
    assert!(swept.reclaimed_bytes > u64::try_from(payload.len()).verified("test payload fits"));
    assert_eq!(
        store.checkpoint_chunks.len().assured("chunk count reads"),
        0
    );
    assert_eq!(
        read_checkpoint(&reader, &store.checkpoint_chunks, &key, &header)
            .assured("the retained snapshot reads deleted chunks")
            .payload,
        payload
    );
    assert_eq!(
        store
            .latest_snapshot(&checkpoint)
            .assured("current checkpoint reads")
            .assured("current checkpoint exists"),
        PersistedRuntimeStateEntry {
            lsm: 4,
            payload: b"resumed".to_vec()
        }
    );
    println!(
        "active chunk storage: published_sst_bytes={published_disk_bytes}, \
         after_logical_deletion_sst_bytes={}, retained_snapshot_payload_bytes={}, \
         reclaimed_key_value_bytes={}",
        store.checkpoint_chunks.disk_space(),
        payload.len(),
        swept.reclaimed_bytes
    );
    drop(reader);
    store
        .checkpoint_chunks
        .rotate_memtable_and_wait()
        .assured("deletion tombstones flush");
    store
        .checkpoint_chunks
        .major_compact()
        .assured("the measurement compacts after the reader releases its view");
    println!(
        "active chunk storage after snapshot release and explicit compaction: \
         current_level_sst_bytes={}",
        store.checkpoint_chunks.disk_space()
    );
}

#[test]
pub(super) fn entity_purge_reclaims_chunks_after_reopen() {
    let directory = tempfile::tempdir().assured("the test directory opens");
    let checkpoint = placement("orders");
    let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let payload = vec![9; RESTORE_STATE_CHUNK_BYTES * 2];
    publish(&store, &checkpoint, &payload);
    store
        .purge_entity(
            &checkpoint.domain,
            checkpoint.state.kind(),
            checkpoint.kind,
            checkpoint.identifier.clone(),
        )
        .assured("the production entity purge completes");
    store
        .db
        .persist(PersistMode::SyncAll)
        .assured("the purge is durable");
    drop(store);
    let reopened = self::store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let swept = reopened
        .reclaim_restore_staging(|_, _| false, || Ok(()))
        .assured("startup maintenance reclaims purged chunks");
    assert!(swept.reclaimed_bytes > u64::try_from(payload.len()).verified("test payload fits"));
    assert_eq!(
        reopened
            .checkpoint_chunks
            .len()
            .assured("chunk count reads"),
        0
    );
    assert!(
        reopened
            .latest_snapshot(&checkpoint)
            .assured("the purged checkpoint reads")
            .is_none()
    );
    assert_eq!(
        backup::active_namespace(
            &reopened.db.snapshot(),
            &reopened.restore_publications,
            &checkpoint.domain
        )
        .assured("the durable pointer reads"),
        StateNamespace::Restored(19)
    );
}

#[test]
pub(super) fn active_reclamation_retains_future_installations_and_exact_publication_retries() {
    let directory = tempfile::tempdir().assured("the test directory opens");
    let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let checkpoint = placement("orders");
    publish(&store, &checkpoint, b"restored");
    stage(&store, 20, &checkpoint, b"future");
    store
        .persist_latest_snapshot(&checkpoint, 3, b"same-revision-inline")
        .assured("an ordinary same-revision checkpoint replaces the segmented header");
    store
        .reclaim_restore_staging(|_, _| true, || Ok(()))
        .assured("even conservative consensus retention reclaims unreferenced active chunks");
    let future_key = StateNamespace::Restored(20)
        .key(&checkpoint)
        .assured("the future placement fits");
    let view = store.db.snapshot();
    let header = view
        .get(&store.latest, &future_key)
        .assured("future state reads")
        .assured("future header is retained");
    assert_eq!(
        read_checkpoint(&view, &store.checkpoint_chunks, &future_key, &header)
            .assured("the future installation remains complete")
            .payload,
        b"future"
    );
    stage(&store, 19, &checkpoint, b"restored");
    store
        .publish_restored_state(
            &checkpoint.domain,
            &authority(19),
            RestoreStateInventory {
                checkpoints: 1,
                payload_bytes: 8,
            },
            || Ok(()),
        )
        .assured("exact publication retry preserves resumed state");
    assert_eq!(
        store
            .latest_snapshot(&checkpoint)
            .assured("current checkpoint reads")
            .assured("current checkpoint exists")
            .payload,
        b"same-revision-inline"
    );
    assert_eq!(
        store
            .checkpoint_chunks
            .len()
            .assured("only the future chunks remain"),
        1
    );
}

#[test]
pub(super) fn cancellation_during_active_deletion_preserves_snapshot_and_resumes_after_restart() {
    let directory = tempfile::tempdir().assured("the test directory opens");
    let checkpoint = placement("orders");
    let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let payload = vec![5; RESTORE_STATE_CHUNK_BYTES * 1200];
    publish(&store, &checkpoint, &payload);
    let reader = store.db.snapshot();
    let key = StateNamespace::Restored(19)
        .key(&checkpoint)
        .assured("placement fits");
    let header = reader
        .get(&store.latest, &key)
        .assured("snapshot reads")
        .assured("header exists");
    store
        .persist_latest_snapshot(&checkpoint, 4, b"resumed")
        .assured("the ordinary header replacement completes");
    let mut checks = 0;
    let Err(error) = store.reclaim_restore_staging(
        |_, _| false,
        || {
            checks += 1;
            if checks % 64 == 0 {
                let remaining = store
                    .checkpoint_chunks
                    .len()
                    .assured("current chunks are measurable");
                if remaining > 0 && remaining < 1200 {
                    return Err(Report::new(RuntimePersistenceError::Cancelled));
                }
            }
            Ok(())
        },
    ) else {
        panic!("cancellation interrupts active cleanup after a committed batch");
    };
    assert!(matches!(
        error.current_context(),
        RuntimePersistenceError::Cancelled
    ));
    let remaining = store
        .checkpoint_chunks
        .len()
        .assured("remaining chunks read");
    assert!(remaining > 0 && remaining < 1200);
    assert_eq!(
        read_checkpoint(&reader, &store.checkpoint_chunks, &key, &header)
            .assured("the retained reader still has the whole checkpoint")
            .payload,
        payload
    );
    drop(reader);
    store
        .db
        .persist(PersistMode::SyncAll)
        .assured("partial cleanup is durable");
    drop(store);
    let reopened = self::store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let swept = reopened
        .reclaim_restore_staging(|_, _| false, || Ok(()))
        .assured("restart completes remaining active cleanup");
    assert_eq!(
        reopened
            .checkpoint_chunks
            .len()
            .assured("chunk count reads"),
        0
    );
    assert!(swept.reclaimed_bytes > 0);
    assert_eq!(
        reopened
            .latest_snapshot(&checkpoint)
            .assured("current checkpoint reads")
            .assured("current checkpoint exists")
            .payload,
        b"resumed"
    );
}
