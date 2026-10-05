//! Reclamation and accounting of unpublished restore generations.
//!
//! Layer: infrastructure.
//! - **Owns.** Bounded namespace deletion, current key/value accounting and checkpoint admission.
//! - **Depends on.** The installation barrier, one Fjall view and a caller's retained generations.
//! - **Must not know.** Consensus state, command retention policy or domain activation gates.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "restore accounting and reclamation run as admitted storage maintenance"
    )
)]

use super::{generation::*, *};
use crate::metrics::RestoreStagingObservation;

pub(crate) const DEFAULT_RESTORE_STAGING_MAX_BYTES: u64 = 128 * 1024 * 1024 * 1024;

impl RuntimeStateStore {
    /// Measure unpublished key and value sizes from one view, seeking past selected namespaces.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the admitted storage caller checks cancellation between cursor entries"
        )
    )]
    pub(in crate::runtime) fn restore_staging_observation(
        &self,
        check: &mut impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<RestoreStagingObservation, RuntimePersistenceError> {
        let view = self.db.snapshot();
        let mut observation = RestoreStagingObservation {
            limit_bytes: self.restore_staging_max_bytes,
            ..RestoreStagingObservation::default()
        };
        for (keyspace, receipts) in [
            (&self.latest, false),
            (&self.lsm_index, false),
            (&self.checkpoint_chunks, false),
            (&self.restore_staging, true),
        ] {
            observation.disk_bytes = observation
                .disk_bytes
                .checked_add(keyspace.disk_space())
                .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
            let mut cursor = Vec::new();
            while let Some((namespace, domain, next)) = next_namespace(&view, keyspace, &cursor)? {
                check()?;
                cursor = next;
                let active = backup::active_namespace(&view, &self.restore_publications, &domain)?;
                if namespace == StateNamespace::Initial || (!receipts && namespace == active) {
                    continue;
                }
                for item in view.prefix(keyspace, namespace.prefix(&domain)) {
                    check()?;
                    let key = item
                        .key()
                        .change_context(RuntimePersistenceError::ReadValue)?;
                    physical_namespace(&key)?;
                    let value_bytes = view
                        .size_of(keyspace, &key)
                        .change_context(RuntimePersistenceError::ReadValue)?;
                    let Some(value_bytes) = value_bytes else {
                        return Err(Report::new(RuntimePersistenceError::ReadValue));
                    };
                    let bytes = u64::try_from(key.len())
                        .verified("database keys fit u64")
                        .checked_add(u64::from(value_bytes))
                        .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
                    observation.staged_bytes = observation
                        .staged_bytes
                        .checked_add(bytes)
                        .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
                    observation.staged_keys = observation
                        .staged_keys
                        .checked_add(1)
                        .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
                }
            }
        }
        observation.disk_bytes = observation
            .disk_bytes
            .checked_add(self.restore_publications.disk_space())
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        Ok(observation)
    }

    fn restore_key_domain(tail: &[u8]) -> error_stack::Result<DomainName, RuntimePersistenceError> {
        let Some(end) = tail.iter().position(|byte| *byte == 0) else {
            return Err(Report::new(RuntimePersistenceError::InvalidStorageFormat));
        };
        let name = std::str::from_utf8(&tail[..end])
            .change_context(RuntimePersistenceError::InvalidStorageFormat)?;
        DomainName::decode(name).change_context(RuntimePersistenceError::InvalidStorageFormat)
    }

    /// The caller retains the applied consensus revision while this operation retains the
    /// installation barrier. The same lock order fences every queued installer and publisher.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the lifecycle caller supplies an immutable retention view \
                                   and cancellation; local callbacks remain analyzed")
    )]
    pub(in crate::runtime) fn reclaim_restore_staging(
        &self,
        retains: impl Fn(&DomainName, u64) -> bool,
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<RestoreStagingObservation, RuntimePersistenceError> {
        self.latest_snapshot_writer().with_installation(|| {
            check()?;
            let before = self.restore_staging_observation(&mut check)?;
            let view = self.db.snapshot();
            for (keyspace, receipts) in [
                (&self.latest, false),
                (&self.lsm_index, false),
                (&self.checkpoint_chunks, false),
                (&self.restore_staging, true),
            ] {
                let mut cursor = Vec::new();
                while let Some((namespace, domain, next)) =
                    next_namespace(&view, keyspace, &cursor)?
                {
                    check()?;
                    cursor = next;
                    let StateNamespace::Restored(generation) = namespace else {
                        continue;
                    };
                    let active =
                        backup::active_namespace(&view, &self.restore_publications, &domain)?;
                    let reclaim = if namespace == active {
                        receipts
                    } else {
                        !retains(&domain, generation)
                    };
                    if reclaim {
                        remove_bounded(
                            &self.db,
                            keyspace,
                            &namespace.prefix(&domain),
                            |_| Ok(true),
                            &mut check,
                        )?;
                    }
                }
            }
            // Release the iterator view before measuring the remaining live keys. Existing
            // readers retain their own complete snapshots through Fjall's retention mechanism.
            drop(view);
            let mut after = self.restore_staging_observation(&mut check)?;
            after.reclaimed_bytes = before
                .staged_bytes
                .checked_sub(after.staged_bytes)
                .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
            if after.reclaimed_bytes > 0 {
                self.db
                    .persist(PersistMode::SyncAll)
                    .change_context(RuntimePersistenceError::Synchronize)?;
            }
            Ok(after)
        })
    }

    pub(super) fn admit_restore_checkpoint(
        &self,
        placement_key: &[u8],
        metadata: CheckpointMetadata,
        header_bytes: usize,
        receipt_bytes: usize,
        check: &mut impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let key_bytes = u64::try_from(placement_key.len()).verified("bounded keys fit u64");
        let chunk_key_bytes = key_bytes
            .checked_add(18)
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        let chunks = metadata.length.div_ceil(
            u64::try_from(RESTORE_STATE_CHUNK_BYTES).verified("the chunk policy fits u64"),
        );
        let chunk_keys = chunks
            .checked_mul(chunk_key_bytes)
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        // One header, one receipt, and an index whose value is the placement key.
        let metadata_keys = key_bytes
            .checked_mul(4)
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        let metadata_keys = metadata_keys
            .checked_add(9)
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        let encoded_metadata = u64::try_from(header_bytes)
            .verified("bounded header fits u64")
            .checked_add(u64::try_from(receipt_bytes).verified("bounded receipt fits u64"))
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        let requested = metadata
            .length
            .checked_add(chunk_keys)
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        let requested = requested
            .checked_add(metadata_keys)
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        let requested = requested
            .checked_add(encoded_metadata)
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        let used = self.restore_staging_observation(check)?.staged_bytes;
        let total = used
            .checked_add(requested)
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        if total > self.restore_staging_max_bytes {
            return Err(Report::new(RuntimePersistenceError::RestoreStagingQuota {
                limit: self.restore_staging_max_bytes,
                used,
                requested,
            }));
        }
        Ok(())
    }
}

/// The required trailing zero separator gives every physical namespace a bounded exclusive end.
fn next_namespace(
    view: &fjall::Snapshot,
    keyspace: &Keyspace,
    cursor: &[u8],
) -> error_stack::Result<Option<(StateNamespace, DomainName, Vec<u8>)>, RuntimePersistenceError> {
    let Some(item) = view
        .range::<&[u8], _>(
            keyspace,
            (
                std::ops::Bound::Included(cursor),
                std::ops::Bound::Unbounded,
            ),
        )
        .next()
    else {
        return Ok(None);
    };
    let key = item
        .key()
        .change_context(RuntimePersistenceError::ReadValue)?;
    let (namespace, tail) = physical_namespace(&key)?;
    let domain = RuntimeStateStore::restore_key_domain(tail)?;
    let mut next = namespace.prefix(&domain);
    *next
        .last_mut()
        .verified("a constructed physical namespace ends with its required zero separator") = 1;
    Ok(Some((namespace, domain, next)))
}

#[cfg(test)]
pub(in crate::runtime::state_store) mod tests {
    use nervix_interconnect::backup::RestoreStateInventory;

    use super::*;

    fn store(path: &std::path::Path, limit: u64) -> RuntimeStateStore {
        RuntimeStateStore::from_database(
            Database::builder(path)
                .open()
                .assured("the test database opens"),
            Executor::default(),
            limit,
        )
        .assured("the current store opens")
    }

    fn placement(domain: &str) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse(domain).assured("the test domain is valid"),
            kind: ModelKind::Ingestor,
            identifier: ModelName::parse("source").assured("the test name is valid"),
            state: RuntimeState::KafkaOffset,
            branch_key: None,
        }
    }

    fn authority(generation: u64) -> nervix_models::RestoreStateAuthority {
        nervix_models::RestoreStateAuthority {
            leader: ClusterNodeName::parse("node-1").assured("the test node is valid"),
            term: 4,
            execution: nervix_models::CommandExecutionReference::parse("restore-attempt")
                .assured("the test execution is valid"),
            mutation_revision: 11,
            generation,
        }
    }

    fn stage(
        store: &RuntimeStateStore,
        generation: u64,
        placement: &RuntimeStatePlacement,
        payload: &[u8],
    ) {
        store
            .stage_restored_checkpoint(
                &authority(generation),
                placement,
                CheckpointMetadata {
                    lsm: 3,
                    length: u64::try_from(payload.len()).verified("test payload fits u64"),
                    digest: *blake3::hash(payload).as_bytes(),
                },
                payload,
                || Ok(()),
            )
            .assured("the checkpoint stages");
    }

    fn observe(store: &RuntimeStateStore) -> RestoreStagingObservation {
        store
            .restore_staging_observation(&mut || Ok(()))
            .assured("storage is measurable")
    }

    #[test]
    fn quota_measures_all_checkpoint_keys_and_exact_retry_reuses_its_allowance() {
        let directory = tempfile::tempdir().assured("the test directory opens");
        let mut store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let checkpoint = placement("orders");
        let payload = vec![7; RESTORE_STATE_CHUNK_BYTES * 2 + 13];
        stage(&store, 19, &checkpoint, &payload);
        let usage = observe(&store);
        assert_eq!(
            usage.staged_keys, 6,
            "three chunks, header, index and receipt"
        );
        assert!(usage.staged_bytes > u64::try_from(payload.len()).verified("test payload fits"));
        store.restore_staging_max_bytes = usage.staged_bytes;
        stage(&store, 19, &checkpoint, &payload);
        assert_eq!(observe(&store).staged_bytes, usage.staged_bytes);
        let other = placement("another");
        let Err(error) = store.stage_restored_checkpoint(
            &authority(20),
            &other,
            CheckpointMetadata {
                lsm: 1,
                length: 1,
                digest: *blake3::hash(b"x").as_bytes(),
            },
            &b"x"[..],
            || Ok(()),
        ) else {
            panic!("another checkpoint exceeds the exact allowance");
        };
        assert!(
            matches!(error.current_context(), RuntimePersistenceError::RestoreStagingQuota {
            limit, used, requested,
        } if *limit == usage.staged_bytes && *used == usage.staged_bytes && *requested > 1)
        );
        assert_eq!(observe(&store).staged_bytes, usage.staged_bytes);
        let reclaimed = store
            .reclaim_restore_staging(|_, _| false, || Ok(()))
            .assured("the abandoned checkpoint is reclaimed");
        assert_eq!(reclaimed.reclaimed_bytes, usage.staged_bytes);
        assert_eq!(reclaimed.staged_keys, 0);
        stage(&store, 20, &other, b"x");
    }

    #[test]
    fn partial_chunks_without_a_receipt_are_accounted_and_reclaimed_after_reopen() {
        let directory = tempfile::tempdir().assured("the test directory opens");
        let checkpoint = placement("orders");
        let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let payload = vec![9; RESTORE_STATE_CHUNK_BYTES * 2];
        let Err(error) = store.stage_restored_checkpoint(
            &authority(19),
            &checkpoint,
            CheckpointMetadata {
                lsm: 3,
                length: u64::try_from(payload.len()).verified("test payload fits"),
                digest: *blake3::hash(&payload).as_bytes(),
            },
            &payload[..RESTORE_STATE_CHUNK_BYTES],
            || Ok(()),
        ) else {
            panic!("the reader ends before the declared checkpoint length");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::RestoreRead
        ));
        let usage = observe(&store);
        assert_eq!(usage.staged_keys, 1);
        assert!(
            usage.staged_bytes > u64::try_from(RESTORE_STATE_CHUNK_BYTES).verified("chunk fits")
        );
        assert_eq!(
            store.restore_staging.len().assured("receipt count reads"),
            0
        );
        store
            .db
            .persist(PersistMode::SyncAll)
            .assured("partial chunks are durable");
        drop(store);
        let reopened = self::store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        assert_eq!(observe(&reopened).staged_bytes, usage.staged_bytes);
        let swept = reopened
            .reclaim_restore_staging(|_, _| false, || Ok(()))
            .assured("restart maintenance reclaims incomplete chunks");
        assert_eq!(swept.staged_bytes, 0);
        assert_eq!(swept.reclaimed_bytes, usage.staged_bytes);
    }

    #[test]
    fn reclamation_preserves_publication_live_generations_and_snapshot_readers() {
        let directory = tempfile::tempdir().assured("the test directory opens");
        let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let checkpoint = placement("orders");
        let initial = placement("another");
        store
            .persist_latest_snapshot(&initial, 1, b"initial")
            .assured("initial state persists");
        stage(&store, 19, &checkpoint, b"published");
        store
            .publish_restored_state(
                &checkpoint.domain,
                &authority(19),
                RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: 9,
                },
                || Ok(()),
            )
            .assured("the complete generation publishes");
        stage(&store, 20, &checkpoint, b"abandoned");
        stage(&store, 21, &checkpoint, b"applying");
        stage(&store, 22, &checkpoint, b"ahead");
        let reader = store.db.snapshot();
        let key = StateNamespace::Restored(20)
            .key(&checkpoint)
            .assured("placement fits");
        let header = reader
            .get(&store.latest, &key)
            .assured("snapshot reads")
            .assured("the abandoned generation was staged");
        let before = observe(&store);
        let retained = store
            .reclaim_restore_staging(
                |domain, generation| domain == &checkpoint.domain && generation >= 21,
                || Ok(()),
            )
            .assured("maintenance reclaims only the abandoned generation");
        assert!(retained.reclaimed_bytes > 0);
        assert!(retained.staged_bytes > 0 && retained.staged_bytes < before.staged_bytes);
        assert_eq!(
            read_checkpoint(&reader, &store.checkpoint_chunks, &key, &header)
                .assured("the retained snapshot reads its deleted chunks")
                .payload,
            b"abandoned"
        );
        store
            .publish_restored_state(
                &checkpoint.domain,
                &authority(21),
                RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: 8,
                },
                || Ok(()),
            )
            .assured("the active retained attempt still publishes");
        let completed = store
            .reclaim_restore_staging(|_, _| false, || Ok(()))
            .assured("the future attempt becomes abandoned");
        assert_eq!(completed.staged_bytes, 0);
        assert_eq!(
            store
                .latest_snapshot(&checkpoint)
                .assured("published state reads")
                .assured("published state exists")
                .payload,
            b"applying"
        );
        assert_eq!(
            store
                .latest_snapshot(&initial)
                .assured("initial state reads")
                .assured("initial state exists")
                .payload,
            b"initial"
        );
    }

    #[test]
    fn interrupted_bounded_reclamation_resumes_from_remaining_keys_after_restart() {
        let directory = tempfile::tempdir().assured("the test directory opens");
        let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let checkpoint = placement("orders");
        let key = StateNamespace::Restored(19)
            .key(&checkpoint)
            .assured("placement fits");
        let mut batch = store.db.batch();
        for offset in 0_u64..2400 {
            let mut chunk = chunk_prefix(&key, 3);
            chunk.extend_from_slice(&offset.to_be_bytes());
            batch.insert(&store.checkpoint_chunks, chunk, b"payload");
        }
        batch
            .commit()
            .assured("incomplete checkpoint chunks persist");
        let snapshot = store.db.snapshot();
        let usage = observe(&store);
        let mut checks = 0;
        let Err(error) = store.reclaim_restore_staging(
            |_, _| false,
            || {
                checks += 1;
                if checks % 128 == 0 {
                    let remaining = store.checkpoint_chunks.len().assured("chunk count reads");
                    if remaining > 0 && remaining < 2400 {
                        return Err(Report::new(RuntimePersistenceError::Cancelled));
                    }
                }
                Ok(())
            },
        ) else {
            panic!("cancellation interrupts after a committed bounded batch");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::Cancelled
        ));
        let remaining = observe(&store);
        assert!(remaining.staged_keys > 0 && remaining.staged_keys < 2400);
        assert_eq!(snapshot.iter(&store.checkpoint_chunks).count(), 2400);
        drop(snapshot);
        store
            .db
            .persist(PersistMode::SyncAll)
            .assured("partial deletion persists");
        drop(store);
        let reopened = self::store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let completed = reopened
            .reclaim_restore_staging(|_, _| false, || Ok(()))
            .assured("restart resumes bounded deletion");
        assert_eq!(completed.staged_bytes, 0);
        assert_eq!(completed.reclaimed_bytes, remaining.staged_bytes);
        assert!(completed.reclaimed_bytes < usage.staged_bytes);
    }

    #[test]
    fn cancelled_maintenance_and_unrepresentable_admission_preserve_the_store() {
        let directory = tempfile::tempdir().assured("the test directory opens");
        let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let checkpoint = placement("orders");
        stage(&store, 19, &checkpoint, b"retained");
        let before = observe(&store);
        let Err(error) = store.reclaim_restore_staging(
            |_, _| false,
            || Err(Report::new(RuntimePersistenceError::Cancelled)),
        ) else {
            panic!("a cancelled sweep mutates nothing");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::Cancelled
        ));
        assert_eq!(observe(&store).staged_bytes, before.staged_bytes);
        let key = StateNamespace::Restored(20)
            .key(&checkpoint)
            .assured("placement fits");
        let Err(error) = store.admit_restore_checkpoint(
            &key,
            CheckpointMetadata {
                lsm: 1,
                length: u64::MAX,
                digest: [3; 32],
            },
            64,
            64,
            &mut || Ok(()),
        ) else {
            panic!("an unrepresentable storage charge fails");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::RestoreStagingSize
        ));
        assert_eq!(observe(&store).staged_bytes, before.staged_bytes);
    }

    #[cfg(feature = "deloxide")]
    pub(in crate::runtime::state_store) fn diagnostic_restore_reclamation() {
        quota_measures_all_checkpoint_keys_and_exact_retry_reuses_its_allowance();
        partial_chunks_without_a_receipt_are_accounted_and_reclaimed_after_reopen();
        reclamation_preserves_publication_live_generations_and_snapshot_readers();
        interrupted_bounded_reclamation_resumes_from_remaining_keys_after_restart();
        cancelled_maintenance_and_unrepresentable_admission_preserve_the_store();
        maintenance_work_tracks_namespaces_in_a_published_store();
    }

    #[test]
    fn malformed_namespace_names_fail_accounting_with_their_typed_cause() {
        for byte in [b'!', 0xff, b'O'] {
            let directory = tempfile::tempdir().assured("the test directory opens");
            let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
            let mut key = StateNamespace::Restored(19)
                .key(&placement("orders"))
                .assured("the current placement encodes");
            let tail_offset = key.len()
                - physical_namespace(&key)
                    .assured("the current namespace decodes")
                    .1
                    .len();
            key[0] = byte;
            key[tail_offset] = byte;
            store
                .checkpoint_chunks
                .insert(key, b"payload")
                .assured("the fixture corrupts the required namespace name");
            let Err(error) = store.reclaim_restore_staging(|_, _| false, || Ok(())) else {
                panic!("accounting must validate the required namespace name");
            };
            assert!(matches!(
                error.current_context(),
                RuntimePersistenceError::InvalidStorageFormat
            ));
            if byte == 0xff {
                assert!(error.contains::<std::str::Utf8Error>());
            } else {
                assert!(error.contains::<nervix_models::NameError>());
            }
            assert_eq!(
                store.checkpoint_chunks.len().assured("chunk count reads"),
                1,
                "a failed validation preserves the stored data for diagnosis"
            );
        }
    }

    #[test]
    fn maintenance_work_tracks_namespaces_in_a_published_store() {
        let directory = tempfile::tempdir().assured("the test directory opens");
        let store = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        let checkpoint = placement("orders");
        let payload = vec![7; RESTORE_STATE_CHUNK_BYTES * 256];
        stage(&store, 19, &checkpoint, &payload);
        store
            .publish_restored_state(
                &checkpoint.domain,
                &authority(19),
                RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: u64::try_from(payload.len()).verified("test payload fits"),
                },
                || Ok(()),
            )
            .assured("the large complete generation publishes");
        let mut units = 0;
        let swept = store
            .reclaim_restore_staging(
                |_, _| false,
                || {
                    units += 1;
                    Ok(())
                },
            )
            .assured("idle maintenance seeks across the selected namespaces");
        assert!(
            units < 64,
            "maintenance visits namespaces while 256 published chunks stay selected"
        );
        assert_eq!(swept.staged_bytes, 0);
        assert_eq!(
            store
                .latest_snapshot(&checkpoint)
                .assured("published state reads")
                .assured("published state exists")
                .payload,
            payload
        );
    }
}
