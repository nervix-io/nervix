//! Bounded selection of live checkpoint segments from current storage views.
//!
//! Layer: test harness.
//! - **Owns.** Current revision selection, bounded caller copies and malformed-key regressions.
//! - **Depends on.** Current checkpoint encodings, the production store and its sweeper.
//! - **Must not know.** Archive compatibility, external transports or graph decisions.

use super::*;

#[test]
pub(in super::super) fn segmented_replacement_reclaims_incomplete_revisions_in_every_active_namespace()
 {
    use std::io::Read;

    for namespace in [StateNamespace::Initial, StateNamespace::Restored(19)] {
        let directory = tempfile::tempdir().assured("the test directory opens");
        let mut checkpoint = placement("orders");
        checkpoint.kind = ModelKind::Relay;
        checkpoint.state = RuntimeState::MaterializedRelay {
            schema: nervix_models::SchemaFingerprint::from_digest([7; 32]).materialized_at(37),
        };
        let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let previous = vec![7; RESTORE_STATE_CHUNK_BYTES * 3 + 13];
        if namespace == StateNamespace::Initial {
            publish_periodic(&store, &checkpoint, 3, &previous);
        } else {
            publish(&store, &checkpoint, &previous);
        }
        let key = namespace.key(&checkpoint).assured("placement fits");
        let reader = store.db.snapshot();
        let header = reader
            .get(&store.latest, &key)
            .assured("snapshot reads")
            .assured("selected header exists");
        let selected = vec![9; RESTORE_STATE_CHUNK_BYTES * 2 + 17];
        publish_periodic(&store, &checkpoint, 4, &selected);
        let writer = store.checkpoint_stream_writer();
        let metadata = CheckpointMetadata {
            lsm: 5,
            length: u64::try_from(selected.len()).verified("payload fits"),
            digest: [0; 32],
        };
        assert!(
            writer
                .publish_checkpoint_stream(&checkpoint, metadata, selected.as_slice(), || Ok(()))
                .is_err()
        );
        let mut checks = 0;
        assert!(
            writer
                .publish_checkpoint_stream(&checkpoint, metadata, selected.as_slice(), || {
                    checks += 1;
                    if checks == 3 {
                        Err(Report::new(RuntimePersistenceError::RestoreRead))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        writer
            .publish_checkpoint_stream(
                &checkpoint,
                CheckpointMetadata {
                    lsm: 4,
                    length: u64::try_from(selected.len()).verified("payload fits"),
                    digest: *blake3::hash(&selected).as_bytes(),
                },
                &[][..],
                || Ok(()),
            )
            .assured("an exact retry preserves the complete selected revision");
        let swept = store
            .reclaim_restore_staging(|_, _| true, || Ok(()))
            .assured("maintenance reclaims unselected revisions in the active namespace");
        assert!(swept.reclaimed_bytes > u64::try_from(previous.len()).verified("payload fits"));
        assert_eq!(
            store.checkpoint_chunks.len().assured("chunk count reads"),
            3
        );
        assert_eq!(
            read_checkpoint(&reader, &store.checkpoint_chunks, &key, &header)
                .assured("the retained reader has the preceding complete revision")
                .payload,
            previous
        );
        assert_eq!(
            store
                .latest_snapshot(&checkpoint)
                .assured("selected checkpoint reads")
                .assured("selected checkpoint exists")
                .payload,
            selected
        );
        drop(reader);
        drop(writer);
        drop(store);
        let reopened = super::store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        assert_eq!(
            reopened
                .reclaim_restore_staging(|_, _| true, || Ok(()))
                .assured("reopen retains selected chunks")
                .reclaimed_bytes,
            0
        );
        let mut retained = reopened
            .checkpoint_reader(&checkpoint)
            .assured("materialized reader opens")
            .assured("selected materialized checkpoint exists");
        reopened
            .purge_entity(
                &checkpoint.domain,
                checkpoint.state.kind(),
                checkpoint.kind,
                checkpoint.identifier.clone(),
            )
            .assured("the materialized entity purges");
        assert!(
            reopened
                .reclaim_restore_staging(|_, _| true, || Ok(()))
                .assured("purged periodic segments reclaim")
                .reclaimed_bytes
                > u64::try_from(selected.len()).verified("payload fits")
        );
        assert_eq!(
            reopened
                .checkpoint_chunks
                .len()
                .assured("chunk count reads"),
            0
        );
        let mut retained_bytes = Vec::new();
        retained
            .read_to_end(&mut retained_bytes)
            .assured("the pinned materialized reader finishes after purge and reclamation");
        assert_eq!(retained_bytes, selected);
    }
}

fn publish_periodic(
    store: &RuntimeStateStore,
    placement: &RuntimeStatePlacement,
    lsm: u64,
    payload: &[u8],
) {
    store
        .checkpoint_stream_writer()
        .publish_checkpoint_stream(
            placement,
            CheckpointMetadata {
                lsm,
                length: u64::try_from(payload.len()).verified("payload length fits u64"),
                digest: *blake3::hash(payload).as_bytes(),
            },
            payload,
            || Ok(()),
        )
        .assured("the production periodic stream durably publishes");
}

#[test]
pub(in super::super) fn selected_segmented_revisions_are_retained_while_other_chunk_sets_are_reclaimed()
 {
    let directory = tempfile::tempdir().assured("the test directory opens");
    let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let checkpoint = placement("orders");
    let mut other = checkpoint.clone();
    other.identifier = ModelName::parse("another").assured("the test model name is valid");
    let payload = vec![7; RESTORE_STATE_CHUNK_BYTES * 256];
    stage(&store, 19, &checkpoint, &payload);
    stage(&store, 19, &other, b"other");
    store
        .publish_restored_state(
            &checkpoint.domain,
            &authority(19),
            RestoreStateInventory {
                checkpoints: 2,
                payload_bytes: u64::try_from(payload.len()).verified("payload fits") + 5,
            },
            || Ok(()),
        )
        .assured("two complete checkpoints publish");
    let key = StateNamespace::Restored(19)
        .key(&checkpoint)
        .assured("placement fits");
    let mut stale_chunk = chunk_prefix(&key, 2);
    stale_chunk.extend_from_slice(&0_u64.to_be_bytes());
    store
        .checkpoint_chunks
        .insert(&stale_chunk, b"unreferenced")
        .assured("an unreferenced revision is present");
    store
        .purge_entity(
            &other.domain,
            other.state.kind(),
            other.kind,
            other.identifier.clone(),
        )
        .assured("only the other entity purges");
    let mut units = 0;
    let swept = store
        .reclaim_restore_staging(
            |_, _| true,
            || {
                units += 1;
                Ok(())
            },
        )
        .assured("only unreferenced active chunks are reclaimed");
    assert!(
        units < 64,
        "maintenance skips 256 referenced chunks after checking their header once"
    );
    assert_eq!(
        store.checkpoint_chunks.len().assured("chunk count reads"),
        256
    );
    assert!(swept.reclaimed_bytes > 17);
    assert_eq!(
        store
            .latest_snapshot(&checkpoint)
            .assured("selected state reads")
            .assured("selected state exists")
            .payload,
        payload
    );
    assert!(
        store
            .latest_snapshot(&other)
            .assured("purged state reads")
            .is_none()
    );
}

#[test]
pub(in super::super) fn large_inline_replacement_needs_no_large_caller_owned_header_copy() {
    let directory = tempfile::tempdir().assured("the test directory opens");
    let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
    let checkpoint = placement("orders");
    publish(&store, &checkpoint, b"restored");
    let current = vec![9; RESTORE_STATE_CHUNK_BYTES * 64];
    store
        .persist_latest_snapshot(&checkpoint, 4, &current)
        .assured("a large native inline checkpoint replaces restored segments");
    store
        .latest
        .rotate_memtable_and_wait()
        .assured("the large current header flushes to the database");
    let swept = store
        .reclaim_restore_staging(|_, _| false, || Ok(()))
        .assured("maintenance handles a header larger than its own reservation");
    assert!(swept.reclaimed_bytes > 8);
    assert_eq!(
        store.checkpoint_chunks.len().assured("chunk count reads"),
        0
    );
    assert_eq!(
        store
            .latest_snapshot(&checkpoint)
            .assured("large current checkpoint reads")
            .assured("current checkpoint exists")
            .payload,
        current
    );
}

#[test]
pub(in super::super) fn malformed_active_chunks_and_headers_fail_before_deletion() {
    for malformed_header in [false, true] {
        let directory = tempfile::tempdir().assured("the test directory opens");
        let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let checkpoint = placement("orders");
        publish(&store, &checkpoint, b"selected");
        let key = StateNamespace::Restored(19)
            .key(&checkpoint)
            .assured("placement fits");
        if malformed_header {
            store
                .latest
                .insert(&key, b"corrupt")
                .assured("the current header is truncated");
        } else {
            let mut malformed_chunk = chunk_prefix(&key, 3);
            malformed_chunk.extend_from_slice(&[0; 7]);
            store
                .checkpoint_chunks
                .insert(malformed_chunk, b"bad-offset")
                .assured("a current chunk has a truncated coordinate");
        }
        let before = store.checkpoint_chunks.len().assured("chunk count reads");
        let Err(error) = store.reclaim_restore_staging(|_, _| false, || Ok(())) else {
            panic!("malformed current chunk storage must report its typed failure");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::InvalidCheckpointChunks
                | RuntimePersistenceError::DecodeState(_)
        ));
        assert_eq!(
            store.checkpoint_chunks.len().assured("chunk count reads"),
            before
        );
    }
}
