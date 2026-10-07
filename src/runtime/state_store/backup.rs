//! The store operations a backup cut and a restore installation use for one domain.
//!
//! Layer: infrastructure.
//! - **Owns.** Reading one published generation, streaming restore checkpoints into an invisible
//!   namespace and durably selecting its complete inventory.
//! - **Depends on.** Typed placement encoding, bounded generation storage and Fjall snapshots.
//! - **Must not know.** Archive records, consensus authority grants or client restore options.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "backup views and bounded restore storage jobs run as admitted lifecycle work"
    )
)]

use std::io::Read;

use nervix_interconnect::backup::RestoreStateInventory;
use nervix_models::RestoreStateAuthority;

use super::{generation::*, *};

pub(in crate::runtime) struct BackupCheckpointView {
    pub(in crate::runtime) checkpoints: Vec<(StoredPlacement, PersistedRuntimeStateEntry)>,
    pub(in crate::runtime) materialized:
        Vec<(StoredPlacement, checkpoint_reader::CheckpointReader)>,
}

#[derive(Debug, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
struct StagedRestoreCheckpoint {
    authority: RestoreStateAuthority,
    metadata: CheckpointMetadata,
}

#[derive(Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
struct RestorePublication {
    authority: RestoreStateAuthority,
    inventory: RestoreStateInventory,
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(reason = "the storage owner supplies one immutable Fjall snapshot view")
)]
fn publication(
    view: &impl Readable,
    publications: &Keyspace,
    domain: &DomainName,
) -> error_stack::Result<Option<RestorePublication>, RuntimePersistenceError> {
    let Some(raw) = view
        .get(publications, domain.as_str().as_bytes())
        .map_err(|_| RuntimePersistenceError::ReadValue)?
    else {
        return Ok(None);
    };
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(raw.len());
    aligned.extend_from_slice(&raw);
    let record = rkyv::from_bytes::<RestorePublication, rkyv::rancor::Error>(&aligned)
        .change_context(RuntimePersistenceError::DecodeState)?;
    Ok(Some(record))
}

pub(super) fn active_namespace(
    view: &impl Readable,
    publications: &Keyspace,
    domain: &DomainName,
) -> error_stack::Result<StateNamespace, RuntimePersistenceError> {
    match publication(view, publications, domain)? {
        Some(record) => Ok(StateNamespace::Restored(record.authority.generation)),
        None => Ok(StateNamespace::Initial),
    }
}

impl RuntimeStateStore {
    fn check_restore_authority(
        &self,
        domain: &DomainName,
        authority: &RestoreStateAuthority,
    ) -> error_stack::Result<Option<RestorePublication>, RuntimePersistenceError> {
        let published = publication(&self.db.snapshot(), &self.restore_publications, domain)?;
        if let Some(record) = &published
            && record.authority != *authority
            && record.authority.generation >= authority.generation
        {
            return Err(Report::new(RuntimePersistenceError::RestoreGeneration {
                requested: authority.generation,
                published: record.authority.generation,
            }));
        }
        Ok(published)
    }

    /// The completed receipt and checkpoint header are committed only after every bounded chunk
    /// has arrived and its digest matches. The active namespace remains unchanged throughout.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the admitted storage owner supplies a synchronous file or \
                                   slice reader and checks cancellation between bounded chunks")
    )]
    pub(in crate::runtime) fn stage_restored_checkpoint(
        &self,
        authority: &RestoreStateAuthority,
        placement: &RuntimeStatePlacement,
        metadata: CheckpointMetadata,
        mut reader: impl Read,
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        self.latest_snapshot_writer().with_installation(|| {
            check()?;
            if self
                .check_restore_authority(&placement.domain, authority)?
                .is_some_and(|record| record.authority == *authority)
            {
                return Ok(());
            }
            let CheckpointMetadata {
                lsm,
                length,
                digest,
            } = metadata;
            let checkpoint = StoredCheckpoint::Segmented(metadata);
            let placement_key = StateNamespace::Restored(authority.generation).key(placement)?;
            let header = checkpoint.encode()?;
            let receipt = StagedRestoreCheckpoint {
                authority: authority.clone(),
                metadata,
            };
            let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&receipt)
                .change_context(RuntimePersistenceError::EncodeState)?;
            if encoded.len() > RESTORE_STATE_CHUNK_BYTES {
                return Err(Report::new(
                    RuntimePersistenceError::CheckpointPlacementTooLarge,
                ));
            }
            let receipt_key = placement_key.clone();
            // A failed retry cannot leave the receipt of a different byte stream marked complete.
            self.restore_staging
                .remove(&receipt_key)
                .map_err(|_| RuntimePersistenceError::WriteValue)?;
            let mut all_chunks = placement_key.clone();
            all_chunks.push(0);
            self.latest
                .remove(&placement_key)
                .map_err(|_| RuntimePersistenceError::WriteValue)?;
            remove_bounded(
                &self.db,
                &self.lsm_index,
                &all_chunks,
                |_| Ok(true),
                &mut check,
            )?;
            remove_bounded(
                &self.db,
                &self.checkpoint_chunks,
                &all_chunks,
                |_| Ok(true),
                &mut check,
            )?;
            self.admit_restore_checkpoint(
                &placement_key,
                metadata,
                header.len(),
                encoded.len(),
                &mut check,
            )?;
            let prefix = chunk_prefix(&placement_key, lsm);
            let mut buffer = vec![0; RESTORE_STATE_CHUNK_BYTES];
            let mut offset = 0_u64;
            let mut hasher = blake3::Hasher::new();
            while offset < length {
                check()?;
                let size = usize::try_from(
                    (length - offset)
                        .min(u64::try_from(buffer.len()).verified("bounded buffer fits")),
                )
                .verified("a bounded chunk fits the address space");
                reader
                    .read_exact(&mut buffer[..size])
                    .change_context(RuntimePersistenceError::RestoreRead)?;
                hasher.update(&buffer[..size]);
                let mut key = prefix.clone();
                key.extend_from_slice(&offset.to_be_bytes());
                let mut batch = self.db.batch();
                batch.insert(&self.checkpoint_chunks, key, &buffer[..size]);
                batch
                    .commit()
                    .map_err(|_| RuntimePersistenceError::WriteValue)?;
                offset = offset
                    .checked_add(u64::try_from(size).verified("bounded chunk fits"))
                    .ok_or(RuntimePersistenceError::InvalidCheckpointChunks)?;
            }
            check()?;
            if hasher.finalize().as_bytes() != &digest {
                return Err(Report::new(
                    RuntimePersistenceError::InvalidCheckpointChunks,
                ));
            }
            let mut batch = self.db.batch();
            batch.insert(&self.latest, placement_key.clone(), header);
            batch.insert(
                &self.lsm_index,
                index_key(&placement_key, lsm),
                placement_key,
            );
            batch.insert(&self.restore_staging, receipt_key, encoded.to_vec());
            batch
                .commit()
                .map_err(|_| Report::new(RuntimePersistenceError::WriteValue))
        })
    }

    fn validate_restore_inventory(
        &self,
        domain: &DomainName,
        authority: &RestoreStateAuthority,
        expected: RestoreStateInventory,
        check: &mut impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let view = self.db.snapshot();
        let mut inventory = RestoreStateInventory::default();
        let namespace = StateNamespace::Restored(authority.generation);
        for item in view.prefix(&self.restore_staging, namespace.prefix(domain)) {
            check()?;
            let (key, raw) = item
                .into_inner()
                .map_err(|_| RuntimePersistenceError::ReadValue)?;
            if raw.len() > RESTORE_STATE_CHUNK_BYTES {
                return Err(Report::new(
                    RuntimePersistenceError::CheckpointPlacementTooLarge,
                ));
            }
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(raw.len());
            aligned.extend_from_slice(&raw);
            let receipt =
                rkyv::from_bytes::<StagedRestoreCheckpoint, rkyv::rancor::Error>(&aligned)
                    .change_context(RuntimePersistenceError::DecodeState)?;
            if receipt.authority != *authority {
                return Err(Report::new(
                    RuntimePersistenceError::InvalidCheckpointChunks,
                ));
            }
            let (stored_namespace, _) = physical_placement(&key)?;
            if stored_namespace != namespace {
                return Err(Report::new(
                    RuntimePersistenceError::InvalidCheckpointChunks,
                ));
            }
            let placement_key = key;
            let Some(raw) = view
                .get(&self.latest, &placement_key)
                .map_err(|_| RuntimePersistenceError::ReadValue)?
            else {
                return Err(Report::new(
                    RuntimePersistenceError::InvalidCheckpointChunks,
                ));
            };
            let checkpoint = StoredCheckpoint::decode(&raw)?;
            if checkpoint != StoredCheckpoint::Segmented(receipt.metadata) {
                return Err(Report::new(
                    RuntimePersistenceError::InvalidCheckpointChunks,
                ));
            }
            visit_chunks(
                &view,
                &self.checkpoint_chunks,
                &placement_key,
                &checkpoint,
                |_| check(),
            )?;
            inventory.checkpoints = inventory
                .checkpoints
                .checked_add(1)
                .ok_or(RuntimePersistenceError::ReadValue)?;
            inventory.payload_bytes = inventory
                .payload_bytes
                .checked_add(receipt.metadata.length)
                .ok_or(RuntimePersistenceError::ReadValue)?;
        }
        let mut headers = 0_u64;
        for item in view.prefix(&self.latest, namespace.prefix(domain)) {
            check()?;
            item.key().map_err(|_| RuntimePersistenceError::ReadValue)?;
            headers = headers
                .checked_add(1)
                .ok_or(RuntimePersistenceError::ReadValue)?;
        }
        if inventory != expected || headers != expected.checkpoints {
            return Err(StoredStateIssue::StagedInventoryDiffers.decode_failure());
        }
        Ok(())
    }

    /// Namespace data is synchronized before selecting it. The single pointer record is then
    /// synchronized before cleanup or runtime handle clearing. Retried publication selects no new
    /// data, and repeats durability and bounded cleanup under exactly the same authority.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the admitted lifecycle storage caller checks cancellation \
                                   between bounded publication units")
    )]
    pub(in crate::runtime) fn publish_restored_state(
        &self,
        domain: &DomainName,
        authority: &RestoreStateAuthority,
        expected: RestoreStateInventory,
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        self.latest_snapshot_writer().with_installation(|| {
            check()?;
            match self.check_restore_authority(domain, authority)? {
                Some(record) if record.authority == *authority => {
                    if record.inventory != expected {
                        return Err(Report::new(
                            RuntimePersistenceError::InvalidCheckpointChunks,
                        ));
                    }
                }
                _ => {
                    self.validate_restore_inventory(domain, authority, expected, &mut check)?;
                    self.db
                        .persist(PersistMode::SyncAll)
                        .map_err(|_| RuntimePersistenceError::Synchronize)?;
                    check()?;
                    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&RestorePublication {
                        authority: authority.clone(),
                        inventory: expected,
                    })
                    .change_context(RuntimePersistenceError::EncodeState)?;
                    let mut batch = self.db.batch();
                    batch.insert(
                        &self.restore_publications,
                        domain.as_str().as_bytes(),
                        encoded.to_vec(),
                    );
                    batch
                        .commit()
                        .map_err(|_| RuntimePersistenceError::WriteValue)?;
                }
            }
            // A cancellation or failure after pointer commit leaves START closed. Retry makes
            // that same publication durable before any handle can attach to it.
            self.db
                .persist(PersistMode::SyncAll)
                .map_err(|_| RuntimePersistenceError::Synchronize)?;
            check()?;
            self.clean_published_restore(domain, authority.generation, &mut check)
        })
    }

    fn clean_published_restore(
        &self,
        domain: &DomainName,
        generation: u64,
        check: &mut impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let prefix = domain_prefix(domain);
        for keyspace in [&self.latest, &self.lsm_index, &self.checkpoint_chunks] {
            remove_bounded(
                &self.db,
                keyspace,
                &prefix,
                |key| {
                    let (namespace, _) = physical_namespace(key)?;
                    Ok(namespace.obsolete_at(generation))
                },
                check,
            )?;
        }
        remove_bounded(
            &self.db,
            &self.restore_staging,
            &prefix,
            |key| {
                let (namespace, _) = physical_namespace(key)?;
                match namespace {
                    StateNamespace::Restored(value) => Ok(value <= generation),
                    StateNamespace::Initial => {
                        Err(Report::new(RuntimePersistenceError::InvalidStorageFormat))
                    }
                }
            },
            check,
        )?;
        Ok(())
    }

    /// The active pointer, checkpoint headers and materialized/guest chunks share this one view.
    pub(in crate::runtime) fn snapshot_backup_domain(
        &self,
        domain: &DomainName,
        kinds: &[RuntimeStateKind],
    ) -> error_stack::Result<BackupCheckpointView, RuntimePersistenceError> {
        let view = self.db.snapshot();
        let namespace = active_namespace(&view, &self.restore_publications, domain)?;
        let mut entries = Vec::new();
        let mut materialized = Vec::new();
        for item in view.prefix(&self.latest, namespace.prefix(domain)) {
            let (key, raw) = item
                .into_inner()
                .map_err(|_| RuntimePersistenceError::ReadValue)?;
            let (_, stored) = physical_placement(&key)?;
            // The runtime supplies the fixed three- or four-kind backup selection.
            if !kinds.contains(&stored.state.kind()) {
                continue;
            }
            if stored.state.kind() == RuntimeStateKind::MaterializedRelay {
                let reader = self.checkpoint_reader_at(view.clone(), &key, &raw)?;
                materialized.push((stored, reader));
                continue;
            }
            let entry = read_checkpoint(&view, &self.checkpoint_chunks, &key, &raw)?;
            entries.push((stored, entry));
        }
        Ok(BackupCheckpointView {
            checkpoints: entries,
            materialized,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One fresh diagnostic test process installs the detector before constructing any store or
    /// executor locks, then exercises the same production owners as the ordinary regressions.
    #[cfg(feature = "deloxide")]
    #[test]
    fn deloxide_restore_storage() {
        let directory = std::env::var_os("NERVIX_DEADLOCK_EVIDENCE")
            .map(nervix_deadlock::EvidenceDirectory::new);
        nervix_deadlock::DiagnosticRun::start(directory)
            .assured("the diagnostic storage process starts its detector once");
        bolero_restore_staging_and_publication_records_round_trip();
        incomplete_and_corrupt_staging_preserve_the_complete_published_state();
        a_durable_generation_is_retryable_and_fences_stale_installations_after_reopen();
        domain_view_keeps_current_state_kinds_and_empty_restore_leaves_other_domains();
        cancellation_on_both_sides_of_publication_preserves_complete_snapshot_views();
        a_queued_checkpoint_writer_is_fenced_by_its_selected_namespace();
        super::super::checkpoint_reader::tests::a_materialized_reader_retains_its_generation_across_publication_and_queued_writes();
        super::super::checkpoint_reader::tests::current_checkpoint_readers_validate_chunks_lengths_and_digests();
        super::super::checkpoint_reader::tests::inline_and_empty_current_checkpoints_read_without_a_segment_allocation();
        super::super::checkpoint_stream::tests::materialized_periodic_streams_preserve_readers_retries_and_failed_publications();
        chunk_corruption_cannot_select_an_incomplete_generation();
        cleanup_commits_bounded_batches_across_many_checkpoint_keys();
        interrupted_cleanup_retains_future_staging_and_retries_the_published_generation();
        a_guest_larger_than_the_bulk_budget_stages_and_publishes_in_one_fixed_reservation();
        a_missing_or_corrupt_required_format_marker_fails_to_open_the_current_store();
        malformed_current_namespace_keys_and_oversize_placements_fail_at_the_storage_boundary();
        super::super::tests::diagnostic_installation_writers_and_purges();
        super::super::maintenance::tests::diagnostic_restore_reclamation();
    }

    #[derive(Debug, bolero::TypeGenerator)]
    struct StoredRestoreCase {
        term: u64,
        revision: u64,
        generation: u64,
        byte: u8,
        length: u8,
    }

    #[test]
    fn bolero_restore_staging_and_publication_records_round_trip() {
        bolero::check!()
            .with_iterations(128)
            .with_max_len(128)
            .with_type::<StoredRestoreCase>()
            .for_each(|case| {
                let mut authority = authority();
                authority.term = case.term.max(1);
                authority.mutation_revision = case.revision.max(1);
                authority.generation = case.generation.max(1);
                let staged = StagedRestoreCheckpoint {
                    authority: authority.clone(),
                    metadata: CheckpointMetadata {
                        lsm: case.revision,
                        length: u64::from(case.length),
                        digest: *blake3::hash(&vec![case.byte; usize::from(case.length)])
                            .as_bytes(),
                    },
                };
                let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&staged)
                    .assured("the staged current checkpoint encodes");
                assert_eq!(
                    rkyv::from_bytes::<StagedRestoreCheckpoint, rkyv::rancor::Error>(&bytes)
                        .assured("the staged current checkpoint decodes"),
                    staged
                );
                let mut stored_placement = placement(
                    "orders",
                    RuntimeState::WasmProcessor {
                        schema: SchemaFingerprint::from_digest([case.byte; 32]),
                        generation: WasmStateGeneration::try_from(case.generation.max(1))
                            .assured("a positive guest generation is valid"),
                    },
                );
                if case.length % 2 == 0 {
                    stored_placement.branch_key = Some(
                        BranchKey::from_fields([(
                            nervix_models::FieldName::parse("tenant")
                                .assured("the field name is valid"),
                            crate::runtime::RuntimeValue::String(format!("tenant-{}", case.byte)),
                        )])
                        .assured("the typed branch key is nonempty"),
                    );
                }
                for namespace in [
                    StateNamespace::Initial,
                    StateNamespace::Restored(authority.generation),
                ] {
                    let key = namespace
                        .key(&stored_placement)
                        .assured("the placement encoding is bounded");
                    let (decoded_namespace, decoded_placement) =
                        physical_placement(&key).assured("the current namespace key decodes");
                    assert_eq!(decoded_namespace, namespace);
                    let (_, logical_key) = physical_namespace(&key)
                        .assured("the physical key retains its complete logical placement");
                    assert_eq!(logical_key, stored_placement.as_storage_key());
                    assert_eq!(decoded_placement.state, stored_placement.state);
                    assert_eq!(decoded_placement.kind, stored_placement.kind);
                    assert_eq!(decoded_placement.identifier, stored_placement.identifier);
                    assert_eq!(
                        decoded_placement.branch,
                        stored_placement
                            .branch_key
                            .as_ref()
                            .map(BranchKey::fingerprint)
                    );
                }
                for checkpoint in [
                    StoredCheckpoint::Inline(PersistedRuntimeStateEntry {
                        lsm: case.revision,
                        payload: vec![case.byte; usize::from(case.length)],
                    }),
                    StoredCheckpoint::Segmented(CheckpointMetadata {
                        lsm: case.revision,
                        length: u64::from(case.length),
                        digest: staged.metadata.digest,
                    }),
                ] {
                    let bytes = checkpoint
                        .encode()
                        .assured("the current checkpoint shape encodes");
                    let mut unaligned = vec![0];
                    unaligned.extend_from_slice(&bytes);
                    assert_eq!(
                        StoredCheckpoint::decode(&unaligned[1..])
                            .assured("the current checkpoint decodes from an unaligned view"),
                        checkpoint
                    );
                }
                let published = RestorePublication {
                    authority,
                    inventory: RestoreStateInventory {
                        checkpoints: case.revision,
                        payload_bytes: u64::from(case.length),
                    },
                };
                let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&published)
                    .assured("the current publication encodes");
                assert_eq!(
                    rkyv::from_bytes::<RestorePublication, rkyv::rancor::Error>(&bytes)
                        .assured("the current publication decodes"),
                    published
                );
            });
    }

    impl RuntimeStateStore {
        fn stage_checkpoint(
            &self,
            authority: &RestoreStateAuthority,
            placement: &RuntimeStatePlacement,
            revision: u64,
            payload: &[u8],
        ) -> error_stack::Result<(), RuntimePersistenceError> {
            self.stage_restored_checkpoint(
                authority,
                placement,
                CheckpointMetadata {
                    lsm: revision,
                    length: u64::try_from(payload.len()).verified("test payload fits"),
                    digest: *blake3::hash(payload).as_bytes(),
                },
                payload,
                || Ok(()),
            )
        }

        fn publish_checkpoint_set(
            &self,
            domain: &DomainName,
            authority: &RestoreStateAuthority,
            inventory: RestoreStateInventory,
        ) -> error_stack::Result<(), RuntimePersistenceError> {
            self.publish_restored_state(domain, authority, inventory, || Ok(()))
        }
    }

    fn authority() -> RestoreStateAuthority {
        RestoreStateAuthority {
            leader: ClusterNodeName::parse("node-1").assured("node is valid"),
            term: 4,
            execution: nervix_models::CommandExecutionReference::parse("restore-attempt")
                .assured("reference is valid"),
            mutation_revision: 11,
            generation: 19,
        }
    }

    fn placement(domain: &str, state: RuntimeState) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse(domain).assured("test domain is valid"),
            kind: if state.kind() == RuntimeStateKind::KafkaOffset {
                ModelKind::Ingestor
            } else {
                ModelKind::WasmProcessor
            },
            identifier: ModelName::parse("source").assured("test entity is valid"),
            state,
            branch_key: None,
        }
    }

    fn open_store(path: &std::path::Path) -> RuntimeStateStore {
        RuntimeStateStore::from_database(
            Database::builder(path).open().assured("database opens"),
            Executor::default(),
            crate::runtime::DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
        .assured("state store opens")
    }

    #[test]
    fn incomplete_and_corrupt_staging_preserve_the_complete_published_state() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let store = open_store(dir.path());
        let domain = DomainName::parse("orders").assured("domain is valid");
        let offsets = placement("orders", RuntimeState::KafkaOffset);
        let guest = placement(
            "orders",
            RuntimeState::WasmProcessor {
                schema: SchemaFingerprint::from_digest([7; 32]),
                generation: WasmStateGeneration::FIRST,
            },
        );
        store
            .persist_latest_snapshot(&offsets, 90, b"previous-offset")
            .assured("offset persists");
        store
            .persist_latest_snapshot(&guest, 91, b"previous-guest")
            .assured("guest persists");
        let authority = authority();
        let expected = RestoreStateInventory {
            checkpoints: 2,
            payload_bytes: 19,
        };
        store
            .stage_checkpoint(&authority, &offsets, 10, b"new-offset")
            .assured("offset stages");
        assert!(
            store
                .publish_checkpoint_set(&domain, &authority, expected)
                .is_err()
        );
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("offset reads")
                .assured("offset exists")
                .payload,
            b"previous-offset"
        );
        assert_eq!(
            store
                .latest_snapshot(&guest)
                .assured("guest reads")
                .assured("guest exists")
                .payload,
            b"previous-guest"
        );
        let key = StateNamespace::Restored(authority.generation)
            .key(&guest)
            .assured("the placement encoding is bounded");
        store
            .restore_staging
            .insert(&key, b"invalid-checkpoint")
            .assured("damaged checkpoint stages");
        assert!(
            store
                .publish_checkpoint_set(&domain, &authority, expected)
                .is_err()
        );
        assert_eq!(
            store
                .latest_snapshot(&guest)
                .assured("guest reads")
                .assured("guest exists")
                .lsm,
            91
        );
        store
            .stage_checkpoint(&authority, &guest, 11, b"new-guest")
            .assured("guest stages");
        let previous = store.db.snapshot();
        store
            .publish_checkpoint_set(&domain, &authority, expected)
            .assured("complete generation publishes");
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("offset reads")
                .assured("offset exists")
                .payload,
            b"new-offset"
        );
        assert_eq!(
            store
                .latest_snapshot(&guest)
                .assured("guest reads")
                .assured("guest exists")
                .lsm,
            11
        );
        let prior_guest = previous
            .get(
                &store.latest,
                StateNamespace::Initial
                    .key(&guest)
                    .assured("the placement encoding is bounded"),
            )
            .assured("previous snapshot reads")
            .assured("previous guest exists");
        assert_eq!(
            read_checkpoint(
                &previous,
                &store.checkpoint_chunks,
                &StateNamespace::Initial
                    .key(&guest)
                    .assured("the placement encoding is bounded"),
                &prior_guest
            )
            .assured("previous guest decodes")
            .payload,
            b"previous-guest"
        );
        assert_eq!(store.lsm_index.prefix(domain_prefix(&domain)).count(), 2);
    }

    #[test]
    fn a_durable_generation_is_retryable_and_fences_stale_installations_after_reopen() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let store = open_store(dir.path());
        let domain = DomainName::parse("orders").assured("domain is valid");
        let offsets = placement("orders", RuntimeState::KafkaOffset);
        let authority = authority();
        let expected = RestoreStateInventory {
            checkpoints: 1,
            payload_bytes: 6,
        };
        store
            .stage_checkpoint(&authority, &offsets, 10, b"offset")
            .assured("offset stages");
        store
            .publish_checkpoint_set(&domain, &authority, expected)
            .assured("generation publishes");
        drop(store);
        let store = open_store(dir.path());
        store
            .publish_checkpoint_set(&domain, &authority, expected)
            .assured("publication retries after reopen");
        assert!(
            store
                .publish_checkpoint_set(&domain, &authority, RestoreStateInventory::default())
                .is_err()
        );
        store
            .stage_checkpoint(&authority, &offsets, 1, b"late-chunk")
            .assured("published staging retry has no effect");
        assert_eq!(
            store.restore_staging.len().assured("staging count reads"),
            0
        );
        let mut newer = authority.clone();
        newer.term += 1;
        newer.generation += 1;
        store
            .stage_checkpoint(&newer, &offsets, 12, b"new-offset")
            .assured("new generation stages");
        store
            .publish_checkpoint_set(
                &domain,
                &newer,
                RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: 10,
                },
            )
            .assured("new generation publishes");
        let Err(error) = store.stage_checkpoint(&authority, &offsets, 20, b"stale") else {
            panic!("stale staging must be fenced");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::RestoreGeneration { .. }
        ));
        let Err(error) = store.publish_checkpoint_set(&domain, &authority, expected) else {
            panic!("stale publication must be fenced");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::RestoreGeneration { .. }
        ));
        let mut competing = newer.clone();
        competing.leader = ClusterNodeName::parse("node-2").assured("node is valid");
        assert!(
            store
                .publish_checkpoint_set(&domain, &competing, expected)
                .is_err()
        );
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("offset reads")
                .assured("offset exists")
                .payload,
            b"new-offset"
        );
    }

    #[test]
    fn domain_view_keeps_current_state_kinds_and_empty_restore_leaves_other_domains() {
        let dir = tempfile::tempdir().assured("temporary state directory opens");
        let db = Database::builder(dir.path())
            .open()
            .assured("state database opens");
        let store = RuntimeStateStore::from_database(
            db,
            Executor::default(),
            crate::runtime::DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
        .assured("runtime state store opens");
        let domain = DomainName::parse("orders").assured("test domain is valid");
        let schema = SchemaFingerprint::from_digest([4; 32]);
        let kafka = placement("orders", RuntimeState::KafkaOffset);
        let lifecycle = placement("orders", RuntimeState::BranchLru { schema });
        let wasm = placement(
            "orders",
            RuntimeState::WasmProcessor {
                schema,
                generation: WasmStateGeneration::FIRST,
            },
        );
        let unrelated_kind = placement("orders", RuntimeState::Deduplicator { schema });
        let materialized = placement(
            "orders",
            RuntimeState::MaterializedRelay {
                schema: schema.materialized_at(1),
            },
        );
        let other_domain = placement("billing", RuntimeState::KafkaOffset);
        for (placement, lsm) in [
            (&kafka, 5),
            (&lifecycle, 3),
            (&wasm, 7),
            (&unrelated_kind, 9),
            (&materialized, 13),
            (&other_domain, 11),
        ] {
            store
                .persist_latest_snapshot(placement, lsm, b"current")
                .assured("checkpoint persists");
        }
        store
            .persist_latest_snapshot(&kafka, 2, b"late-periodic-write")
            .assured("an earlier periodic write completes");
        let view = store
            .snapshot_backup_domain(
                &domain,
                &[
                    RuntimeStateKind::WasmProcessor,
                    RuntimeStateKind::KafkaOffset,
                    RuntimeStateKind::BranchLru,
                    RuntimeStateKind::MaterializedRelay,
                ],
            )
            .assured("one database view opens");
        assert_eq!(view.materialized.len(), 1);
        let mut read = view.checkpoints;
        read.sort_by_key(|(placement, _)| u8::from(placement.state.kind()));
        assert_eq!(read.len(), 3);
        assert!(read.iter().any(|(placement, entry)| placement.state.kind()
            == RuntimeStateKind::KafkaOffset
            && entry.lsm == 5
            && entry.payload == b"current"));
        assert!(
            read.iter()
                .any(|(placement, _)| placement.state.kind() == RuntimeStateKind::BranchLru)
        );
        assert!(
            read.iter()
                .any(|(placement, _)| placement.state.kind() == RuntimeStateKind::WasmProcessor)
        );
        store
            .publish_checkpoint_set(&domain, &authority(), RestoreStateInventory::default())
            .assured("restored target purges");
        assert!(
            store
                .snapshot_backup_domain(
                    &domain,
                    &[
                        RuntimeStateKind::WasmProcessor,
                        RuntimeStateKind::KafkaOffset,
                        RuntimeStateKind::BranchLru,
                        RuntimeStateKind::MaterializedRelay
                    ]
                )
                .assured("purged domain view opens")
                .checkpoints
                .is_empty()
        );
        let (captured, mut reader) = view
            .materialized
            .into_iter()
            .next()
            .assured("the materialized checkpoint retains its database view");
        assert_eq!(captured.identifier, materialized.identifier);
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut bytes)
            .assured("publication cleanup cannot change the selected materialized checkpoint");
        assert_eq!(bytes, b"current");
        assert_eq!(
            store
                .latest_snapshot(&other_domain)
                .assured("unrelated domain remains readable")
                .assured("unrelated checkpoint remains")
                .lsm,
            11
        );
    }

    #[test]
    fn cancellation_on_both_sides_of_publication_preserves_complete_snapshot_views() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let store = open_store(dir.path());
        let domain = DomainName::parse("orders").assured("domain is valid");
        let offsets = placement("orders", RuntimeState::KafkaOffset);
        store
            .persist_latest_snapshot(&offsets, 90, b"previous")
            .assured("checkpoint persists");
        let previous = store.db.snapshot();
        let authority = authority();
        store
            .stage_checkpoint(&authority, &offsets, 10, b"complete")
            .assured("checkpoint stages");
        let expected = RestoreStateInventory {
            checkpoints: 1,
            payload_bytes: 8,
        };
        let mut checks = 0;
        let result = store.publish_restored_state(&domain, &authority, expected, || {
            checks += 1;
            if checks == 3 {
                return Err(Report::new(RuntimePersistenceError::Cancelled));
            }
            Ok(())
        });
        let Err(error) = result else {
            panic!("validation must be cancelled");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::Cancelled
        ));
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("checkpoint reads")
                .assured("checkpoint exists")
                .payload,
            b"previous"
        );
        let result = store.publish_restored_state(&domain, &authority, expected, || {
            if active_namespace(&store.db.snapshot(), &store.restore_publications, &domain)?
                == StateNamespace::Restored(authority.generation)
            {
                return Err(Report::new(RuntimePersistenceError::Cancelled));
            }
            Ok(())
        });
        let Err(error) = result else {
            panic!("work must be cancelled after durable selection");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::Cancelled
        ));
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("published checkpoint reads")
                .assured("checkpoint exists")
                .payload,
            b"complete"
        );
        store
            .publish_checkpoint_set(&domain, &authority, expected)
            .assured("the same authority retries cleanup");
        let raw = previous
            .get(
                &store.latest,
                StateNamespace::Initial
                    .key(&offsets)
                    .assured("the placement encoding is bounded"),
            )
            .assured("prior view reads")
            .assured("prior checkpoint exists");
        assert_eq!(
            read_checkpoint(
                &previous,
                &store.checkpoint_chunks,
                &StateNamespace::Initial
                    .key(&offsets)
                    .assured("the placement encoding is bounded"),
                &raw
            )
            .assured("prior complete view decodes")
            .payload,
            b"previous"
        );
        drop(previous);
        drop(store);
        let store = open_store(dir.path());
        store
            .publish_checkpoint_set(&domain, &authority, expected)
            .assured("durable selection retries after restart");
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("checkpoint reads after restart")
                .assured("checkpoint exists")
                .payload,
            b"complete"
        );
    }

    #[test]
    fn a_queued_checkpoint_writer_is_fenced_by_its_selected_namespace() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let store = open_store(dir.path());
        let domain = DomainName::parse("orders").assured("domain is valid");
        let offsets = placement("orders", RuntimeState::KafkaOffset);
        let queued_writer = store.latest_snapshot_writer();
        let authority = authority();
        store
            .stage_checkpoint(&authority, &offsets, 10, b"complete")
            .assured("checkpoint stages");
        store
            .publish_checkpoint_set(
                &domain,
                &authority,
                RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: 8,
                },
            )
            .assured("generation publishes");
        queued_writer
            .write_latest_snapshot(&offsets, 99, b"queued")
            .assured("the queued writer finishes against its own namespace");
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("selected checkpoint reads")
                .assured("checkpoint exists")
                .payload,
            b"complete"
        );
        store
            .persist_latest_snapshot(&offsets, 11, b"live")
            .assured("a current writer advances the selected namespace");
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("current checkpoint reads")
                .assured("checkpoint exists"),
            PersistedRuntimeStateEntry {
                lsm: 11,
                payload: b"live".to_vec()
            }
        );
    }

    #[test]
    fn chunk_corruption_cannot_select_an_incomplete_generation() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let store = open_store(dir.path());
        let domain = DomainName::parse("orders").assured("domain is valid");
        let offsets = placement("orders", RuntimeState::KafkaOffset);
        store
            .persist_latest_snapshot(&offsets, 90, b"previous")
            .assured("checkpoint persists");
        let authority = authority();
        store
            .stage_checkpoint(&authority, &offsets, 10, b"complete")
            .assured("checkpoint stages");
        let mut key = chunk_prefix(
            &StateNamespace::Restored(authority.generation)
                .key(&offsets)
                .assured("the placement encoding is bounded"),
            10,
        );
        key.extend_from_slice(&0_u64.to_be_bytes());
        store
            .checkpoint_chunks
            .insert(&key, b"altered!")
            .assured("one current chunk is corrupted");
        let Err(error) = store.publish_checkpoint_set(
            &domain,
            &authority,
            RestoreStateInventory {
                checkpoints: 1,
                payload_bytes: 8,
            },
        ) else {
            panic!("a corrupted chunk cannot publish");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::InvalidCheckpointChunks
        ));
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("selected checkpoint reads")
                .assured("checkpoint exists")
                .payload,
            b"previous"
        );
        store
            .stage_checkpoint(&authority, &offsets, 10, b"complete")
            .assured("the checkpoint can be restaged exactly");
        store
            .publish_checkpoint_set(
                &domain,
                &authority,
                RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: 8,
                },
            )
            .assured("the verified generation publishes");
        assert_eq!(
            store
                .latest_snapshot(&offsets)
                .assured("selected checkpoint reads")
                .assured("checkpoint exists")
                .payload,
            b"complete"
        );
    }

    #[test]
    fn cleanup_commits_bounded_batches_across_many_checkpoint_keys() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let store = open_store(dir.path());
        let domain = DomainName::parse("orders").assured("domain is valid");
        let authority = authority();
        for index in 0..600 {
            let mut checkpoint = placement("orders", RuntimeState::KafkaOffset);
            checkpoint.identifier = ModelName::parse(&format!(
                "restore_checkpoint_source_{index:04}_bounded_cleanup"
            ))
            .assured("entity name is valid");
            store
                .stage_checkpoint(&authority, &checkpoint, 1, b"x")
                .assured("one bounded checkpoint stages");
        }
        let (allocations, result) = alloc_count::alloc_count!({
            store.publish_checkpoint_set(
                &domain,
                &authority,
                RestoreStateInventory {
                    checkpoints: 600,
                    payload_bytes: 600,
                },
            )
        });
        result.assured("the complete set publishes");
        eprintln!(
            "restore generation measurement: checkpoints=600 \
             publication_allocations={allocations:?}"
        );
        let held = store.db.snapshot();
        let mut next = authority.clone();
        next.generation += 1;
        let (allocations, result) = alloc_count::alloc_count!({
            store.publish_checkpoint_set(&domain, &next, RestoreStateInventory::default())
        });
        result.assured("empty replacement cleans every obsolete checkpoint in bounded batches");
        eprintln!(
            "restore generation measurement: checkpoints=600 cleanup_allocations={allocations:?}"
        );
        assert!(
            store
                .snapshot_backup_domain(
                    &domain,
                    &[
                        RuntimeStateKind::WasmProcessor,
                        RuntimeStateKind::KafkaOffset,
                        RuntimeStateKind::BranchLru,
                        RuntimeStateKind::MaterializedRelay
                    ]
                )
                .assured("selected empty set reads")
                .checkpoints
                .is_empty()
        );
        assert_eq!(
            held.prefix(
                &store.latest,
                StateNamespace::Restored(authority.generation).prefix(&domain)
            )
            .count(),
            600
        );
        assert_eq!(
            store
                .checkpoint_chunks
                .len()
                .assured("current chunks count reads"),
            0
        );
        assert_eq!(
            store
                .restore_staging
                .len()
                .assured("current receipts count reads"),
            0
        );
    }

    #[test]
    fn interrupted_cleanup_retains_future_staging_and_retries_the_published_generation() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let store = open_store(dir.path());
        let domain = DomainName::parse("orders").assured("domain is valid");
        let first = authority();
        for index in 0..600 {
            let mut checkpoint = placement("orders", RuntimeState::KafkaOffset);
            checkpoint.identifier = ModelName::parse(&format!(
                "restore_checkpoint_source_{index:04}_bounded_cleanup"
            ))
            .assured("the source name is valid");
            store
                .stage_checkpoint(&first, &checkpoint, 1, b"saved")
                .assured("each checkpoint stages");
        }
        store
            .publish_checkpoint_set(
                &domain,
                &first,
                RestoreStateInventory {
                    checkpoints: 600,
                    payload_bytes: 3000,
                },
            )
            .assured("the first complete generation publishes");
        let previous = store.db.snapshot();
        let previous_prefix = StateNamespace::Restored(first.generation).prefix(&domain);
        let second = RestoreStateAuthority {
            generation: first.generation + 1,
            ..first.clone()
        };
        let future = RestoreStateAuthority {
            generation: second.generation + 1,
            ..second.clone()
        };
        let checkpoint = placement("orders", RuntimeState::KafkaOffset);
        store
            .stage_checkpoint(&future, &checkpoint, 2, b"future")
            .assured("the future attempt stages before cleanup");
        let future_key = StateNamespace::Restored(future.generation)
            .key(&checkpoint)
            .assured("the placement encoding is bounded");
        let Err(error) = store.publish_restored_state(
            &domain,
            &second,
            RestoreStateInventory::default(),
            || {
                let remaining = store.latest.prefix(&previous_prefix).count();
                if remaining > 0 && remaining < 600 {
                    return Err(Report::new(RuntimePersistenceError::Cancelled));
                }
                Ok(())
            },
        ) else {
            panic!("cleanup stops after its first committed deletion batch");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::Cancelled
        ));
        let remaining = store.latest.prefix(&previous_prefix).count();
        assert!(
            remaining > 0 && remaining < 600,
            "cleanup committed one bounded batch"
        );
        assert_eq!(
            previous.prefix(&store.latest, &previous_prefix).count(),
            600
        );
        assert!(
            store
                .latest_snapshot(&checkpoint)
                .assured("the selected generation reads")
                .is_none()
        );
        assert!(
            store
                .restore_staging
                .contains_key(&future_key)
                .assured("future staging reads")
        );
        store
            .publish_checkpoint_set(&domain, &second, RestoreStateInventory::default())
            .assured("exact publication retry completes cleanup");
        assert_eq!(store.latest.prefix(&previous_prefix).count(), 0);
        assert!(
            store
                .restore_staging
                .contains_key(&future_key)
                .assured("future staging remains readable")
        );
        store
            .publish_checkpoint_set(
                &domain,
                &future,
                RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: 6,
                },
            )
            .assured("the preserved future attempt publishes its complete checkpoint");
        assert_eq!(
            store
                .latest_snapshot(&checkpoint)
                .assured("the checkpoint reads")
                .assured("the future checkpoint is selected")
                .payload,
            b"future"
        );
    }

    #[nervix_primitives::test]
    async fn a_guest_larger_than_the_bulk_budget_stages_and_publishes_in_one_fixed_reservation() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let store = Arc::new(open_store(dir.path()));
        let executor = store.executor.clone();
        assert_eq!(
            executor.snapshot().bulk_memory.capacity_bytes,
            32 * 1024 * 1024
        );
        let length = 40 * 1024 * 1024_u64;
        let buffer = vec![7; RESTORE_STATE_CHUNK_BYTES];
        let mut hasher = blake3::Hasher::new();
        for _ in 0..length / u64::try_from(buffer.len()).verified("bounded buffer size fits") {
            hasher.update(&buffer);
        }
        let digest = *hasher.finalize().as_bytes();
        let guest = placement(
            "orders",
            RuntimeState::WasmProcessor {
                schema: SchemaFingerprint::from_digest([7; 32]),
                generation: WasmStateGeneration::FIRST,
            },
        );
        let domain = guest.domain.clone();
        let authority = authority();
        let work_store = store.clone();
        let measured_executor = executor.clone();
        let reservation = executor
            .reserve(MemoryClass::Bulk, RESTORE_STATE_WORKING_BYTES)
            .await
            .assured("fixed work fits the default budget");
        executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    assert_eq!(
                        measured_executor.snapshot().bulk_memory.reserved_bytes,
                        RESTORE_STATE_WORKING_BYTES
                    );
                    let began = nervix_primitives::time::Instant::now();
                    let (allocations, result) = alloc_count::alloc_count!({
                        work_store.stage_restored_checkpoint(
                            &authority,
                            &guest,
                            CheckpointMetadata {
                                lsm: 10,
                                length,
                                digest,
                            },
                            std::io::repeat(7).take(length),
                            || {
                                cancellation
                                    .check()
                                    .change_context(RuntimePersistenceError::Cancelled)
                            },
                        )
                    });
                    result.assured("a large guest stages without a full-payload allocation");
                    eprintln!(
                        "restore generation measurement: payload_bytes={length} \
                         staging_elapsed={:?} staging_allocations={allocations:?} \
                         reserved_bytes={RESTORE_STATE_WORKING_BYTES}",
                        began.elapsed()
                    );
                    let began = nervix_primitives::time::Instant::now();
                    let (allocations, result) = alloc_count::alloc_count!({
                        work_store.publish_restored_state(
                            &domain,
                            &authority,
                            RestoreStateInventory {
                                checkpoints: 1,
                                payload_bytes: length,
                            },
                            || {
                                cancellation
                                    .check()
                                    .change_context(RuntimePersistenceError::Cancelled)
                            },
                        )
                    });
                    result
                        .assured("the complete large generation publishes in the same reservation");
                    eprintln!(
                        "restore generation measurement: payload_bytes={length} \
                         publication_elapsed={:?} publication_allocations={allocations:?} \
                         reserved_bytes={RESTORE_STATE_WORKING_BYTES}",
                        began.elapsed()
                    );
                },
            )
            .await
            .assured("the admitted storage job completes");
        assert_eq!(store.executor.snapshot().bulk_memory.reserved_bytes, 0);
        assert_eq!(store.executor.snapshot().bulk_memory.refused, 0);
    }

    #[test]
    fn a_missing_or_corrupt_required_format_marker_fails_to_open_the_current_store() {
        for damaged in [None, Some(b"invalid-format".as_slice())] {
            let dir = tempfile::tempdir().assured("state directory opens");
            let store = open_store(dir.path());
            let offsets = placement("orders", RuntimeState::KafkaOffset);
            store
                .persist_latest_snapshot(&offsets, 1, b"current")
                .assured("a current checkpoint persists");
            let format = store
                .db
                .keyspace("runtime_state_format", KeyspaceCreateOptions::default)
                .assured("the required format keyspace opens");
            match damaged {
                Some(value) => format
                    .insert(b"encoding", value)
                    .assured("the marker is corrupted"),
                None => format.remove(b"encoding").assured("the marker is missing"),
            }
            store
                .db
                .persist(PersistMode::SyncAll)
                .assured("the damaged current marker persists");
            drop(format);
            drop(store);
            let result = RuntimeStateStore::from_database(
                Database::builder(dir.path())
                    .open()
                    .assured("database opens"),
                Executor::default(),
                crate::runtime::DEFAULT_RESTORE_STAGING_MAX_BYTES,
            );
            let Err(error) = result else {
                panic!("a damaged current format marker must fail to open");
            };
            assert!(matches!(
                error.current_context(),
                RuntimePersistenceError::InvalidStorageFormat
            ));
            assert!(
                error
                    .to_string()
                    .contains("recreate the node state directory")
            );
        }
    }

    #[test]
    fn malformed_current_namespace_keys_and_oversize_placements_fail_at_the_storage_boundary() {
        let offsets = placement("orders", RuntimeState::KafkaOffset);
        let key = StateNamespace::Restored(19)
            .key(&offsets)
            .assured("the placement encoding is bounded");
        for length in 0..generation::domain_prefix(&offsets.domain).len() + 10 {
            assert!(physical_namespace(&key[..length]).is_err());
        }
        let mut damaged = key.clone();
        damaged[generation::domain_prefix(&offsets.domain).len()] = b'?';
        let Err(error) = physical_namespace(&damaged) else {
            panic!("the namespace marker must be valid");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::InvalidStorageFormat
        ));
        let mut damaged = key;
        let index = generation::domain_prefix(&offsets.domain).len() + 10;
        damaged[index] = b'?';
        assert!(physical_placement(&damaged).is_err());
        let Err(error) = check_placement_bound(&vec![b'x'; 64 * 1024]) else {
            panic!("placement encoding must fit the bounded metadata reservation");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::CheckpointPlacementTooLarge
        ));
        let dir = tempfile::tempdir().assured("state directory opens");
        let store = open_store(dir.path());
        let mut oversized = offsets;
        oversized.branch_key = Some(
            BranchKey::from_fields([(
                nervix_models::FieldName::parse("tenant").assured("the field name is valid"),
                crate::runtime::RuntimeValue::String("x".repeat(64 * 1024)),
            )])
            .assured("the typed branch key is nonempty"),
        );
        let Err(error) = store.persist_latest_snapshot(&oversized, 1, b"checkpoint") else {
            panic!("the checkpoint writer enforces its physical placement bound");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::CheckpointPlacementTooLarge
        ));
    }
}
