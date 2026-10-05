//! Bounded publication of one native checkpoint within its selected domain namespace.
//!
//! Layer: infrastructure.
//! - **Owns.** Streaming immutable checkpoint chunks, verifying their digest and durably replacing
//!   the checkpoint header after its data is durable.
//! - **Depends on.** The state store's namespace fence, installation barrier and chunk encoding.
//! - **Must not know.** Archive layouts, graph plans or restore consensus authority.

use std::io::Read;

use super::{generation::*, *};

/// A periodic writer pins its namespace before entering the executor's wait queue.
pub(in crate::runtime) struct CheckpointStreamWriter {
    writer: LatestSnapshotWriter,
    chunks: Keyspace,
}

impl RuntimeStateStore {
    pub(in crate::runtime) fn checkpoint_stream_writer(&self) -> CheckpointStreamWriter {
        CheckpointStreamWriter {
            writer: self.latest_snapshot_writer(),
            chunks: self.checkpoint_chunks.clone(),
        }
    }
}

impl CheckpointStreamWriter {
    /// The selected namespace is retained before waiting for the writer barrier. A publication
    /// replacing it makes this write inert. Every storage batch holds at most one 64 KiB chunk;
    /// readers retain the database view that selected their header through all chunk reads.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the admitted storage owner supplies an immutable file reader and checks \
                      cancellation between bounded chunks"
        )
    )]
    pub(in crate::runtime) fn publish_checkpoint_stream(
        &self,
        placement: &RuntimeStatePlacement,
        metadata: CheckpointMetadata,
        mut reader: impl Read,
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let writer = &self.writer;
        writer.with_installation(|| {
            check()?;
            let namespace = backup::active_namespace(
                &writer.namespace_view,
                &writer.restore_publications,
                &placement.domain,
            )?;
            if namespace
                != backup::active_namespace(
                    &writer.db.snapshot(),
                    &writer.restore_publications,
                    &placement.domain,
                )?
                || writer
                    .latest_lsm(placement)?
                    .is_some_and(|revision| revision >= metadata.lsm)
            {
                return Ok(());
            }
            let placement_key = namespace.key(placement)?;
            let prefix = chunk_prefix(&placement_key, metadata.lsm);
            let mut buffer = vec![0; RESTORE_STATE_CHUNK_BYTES];
            let mut offset = 0_u64;
            let mut hasher = blake3::Hasher::new();
            while offset < metadata.length {
                check()?;
                let size = usize::try_from(
                    (metadata.length - offset)
                        .min(u64::try_from(buffer.len()).verified("bounded buffer fits")),
                )
                .verified("a bounded chunk fits the address space");
                reader
                    .read_exact(&mut buffer[..size])
                    .change_context(RuntimePersistenceError::RestoreRead)?;
                hasher.update(&buffer[..size]);
                let mut key = prefix.clone();
                key.extend_from_slice(&offset.to_be_bytes());
                let mut batch = writer.db.batch();
                batch.insert(&self.chunks, key, &buffer[..size]);
                batch
                    .commit()
                    .map_err(|_| RuntimePersistenceError::WriteValue)?;
                offset = offset
                    .checked_add(u64::try_from(size).verified("bounded chunk fits"))
                    .ok_or(RuntimePersistenceError::InvalidCheckpointChunks)?;
            }
            check()?;
            let mut trailing = [0];
            if hasher.finalize().as_bytes() != &metadata.digest
                || reader
                    .read(&mut trailing)
                    .change_context(RuntimePersistenceError::RestoreRead)?
                    != 0
            {
                return Err(Report::new(
                    RuntimePersistenceError::InvalidCheckpointChunks,
                ));
            }
            writer.persist(PersistMode::SyncAll)?;
            let mut batch = writer.db.batch();
            batch.insert(
                &writer.latest,
                placement_key.clone(),
                StoredCheckpoint::Segmented(metadata).encode()?,
            );
            batch.insert(
                &writer.lsm_index,
                index_key(&placement_key, metadata.lsm),
                placement_key,
            );
            batch
                .commit()
                .map_err(|_| RuntimePersistenceError::WriteValue)?;
            writer.persist(PersistMode::SyncAll)
        })
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::io::Cursor;

    use super::*;

    fn metadata(revision: u64, bytes: &[u8]) -> CheckpointMetadata {
        CheckpointMetadata {
            lsm: revision,
            length: u64::try_from(bytes.len()).verified("fixture fits"),
            digest: *blake3::hash(bytes).as_bytes(),
        }
    }

    #[test]
    pub(in crate::runtime::state_store) fn materialized_periodic_streams_preserve_readers_retries_and_failed_publications()
     {
        let root = tempfile::tempdir().assured("store directory opens");
        let store = RuntimeStateStore::from_database(
            Database::builder(root.path())
                .open()
                .assured("database opens"),
            Executor::default(),
            super::super::DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
        .assured("store opens");
        let placement = RuntimeStatePlacement {
            domain: DomainName::parse("orders").assured("domain parses"),
            kind: nervix_models::ModelKind::Relay,
            identifier: nervix_models::ModelName::parse("state").assured("relay parses"),
            state: RuntimeState::MaterializedRelay {
                schema: nervix_models::SchemaFingerprint::from_digest([7; 32]).materialized_at(37),
            },
            branch_key: None,
        };
        let initial = vec![7; 2 * RESTORE_STATE_CHUNK_BYTES + 17];
        store
            .checkpoint_stream_writer()
            .publish_checkpoint_stream(
                &placement,
                metadata(1, &initial),
                Cursor::new(&initial),
                || Ok(()),
            )
            .assured("a multi-chunk periodic snapshot publishes");
        let mut pinned = store
            .checkpoint_reader(&placement)
            .assured("reader opens")
            .assured("the published snapshot exists");
        let next = vec![13; RESTORE_STATE_CHUNK_BYTES + 31];
        let queued = store.checkpoint_stream_writer();
        store
            .checkpoint_stream_writer()
            .publish_checkpoint_stream(
                &placement,
                metadata(2, &next),
                Cursor::new(&next),
                || Ok(()),
            )
            .assured("a newer periodic snapshot publishes");
        queued
            .publish_checkpoint_stream(
                &placement,
                metadata(1, &initial),
                Cursor::new(&initial),
                || Ok(()),
            )
            .assured("an older queued writer is inert");
        store
            .checkpoint_stream_writer()
            .publish_checkpoint_stream(&placement, metadata(2, &next), Cursor::new(&[]), || Ok(()))
            .assured("an exact retry needs no data");
        let mut actual = Vec::new();
        pinned
            .read_to_end(&mut actual)
            .assured("the pinned reader finishes");
        assert_eq!(actual, initial);
        for (bytes, descriptor) in [
            (next[..3].to_vec(), metadata(3, &next)),
            (vec![1; next.len()], metadata(3, &next)),
            ([next.as_slice(), &[1]].concat(), metadata(3, &next)),
        ] {
            assert!(
                store
                    .checkpoint_stream_writer()
                    .publish_checkpoint_stream(
                        &placement,
                        descriptor,
                        Cursor::new(bytes),
                        || Ok(()),
                    )
                    .is_err()
            );
        }
        let mut checks = 0;
        assert!(
            store
                .checkpoint_stream_writer()
                .publish_checkpoint_stream(
                    &placement,
                    metadata(3, &next),
                    Cursor::new(&next),
                    || {
                        checks += 1;
                        if checks == 3 {
                            Err(Report::new(RuntimePersistenceError::RestoreRead))
                        } else {
                            Ok(())
                        }
                    },
                )
                .is_err()
        );
        let mut actual = Vec::new();
        store
            .checkpoint_reader(&placement)
            .assured("reader opens")
            .assured("checkpoint exists")
            .read_to_end(&mut actual)
            .assured("published data reads");
        assert_eq!(actual, next);
        let queued_namespace = store.checkpoint_stream_writer();
        let authority = nervix_models::RestoreStateAuthority {
            leader: nervix_models::ClusterNodeName::parse("restored-1").assured("node parses"),
            term: 4,
            execution: nervix_models::CommandExecutionReference::parse("periodic-reader")
                .assured("reference parses"),
            mutation_revision: 11,
            generation: 19,
        };
        let next = vec![17; 100];
        store
            .stage_restored_checkpoint(
                &authority,
                &placement,
                metadata(1, &next),
                Cursor::new(&next),
                || Ok(()),
            )
            .assured("a complete restore generation stages");
        store
            .publish_restored_state(
                &placement.domain,
                &authority,
                nervix_interconnect::backup::RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: 100,
                },
                || Ok(()),
            )
            .assured("the restore generation publishes");
        queued_namespace
            .publish_checkpoint_stream(&placement, metadata(99, &initial), Cursor::new(&[]), || {
                Ok(())
            })
            .assured("a writer selected before restore cannot overwrite the new namespace");
        drop(pinned);
        drop(queued);
        drop(queued_namespace);
        drop(store);
        let store = RuntimeStateStore::from_database(
            Database::builder(root.path())
                .open()
                .assured("database reopens"),
            Executor::default(),
            super::super::DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
        .assured("store reopens");
        let mut actual = Vec::new();
        store
            .checkpoint_reader(&placement)
            .assured("reader opens")
            .assured("checkpoint exists")
            .read_to_end(&mut actual)
            .assured("durable data reads");
        assert_eq!(actual, next);
    }
}
