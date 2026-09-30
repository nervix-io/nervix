//! The store operations a backup cut and a restore installation use for one domain.
//!
//! Layer: infrastructure.
//! - **Owns.** Reading the archive-owned state kinds from one database view, staging complete
//!   restore installations and atomically publishing their checkpoints.
//! - **Depends on.** The runtime state store's typed placement encoding and Fjall snapshot.
//! - **Must not know.** Archive records, how consensus grants installation authority, or client
//!   restore options.

use nervix_interconnect::backup::RestoreStateInventory;
use nervix_models::RestoreStateAuthority;

use super::*;

#[derive(Debug, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
struct StagedRestoreCheckpoint {
    authority: RestoreStateAuthority,
    placement: nervix_interconnect::StatePlacementEnvelope,
    snapshot: PersistedRuntimeStateEntry,
}

#[derive(Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
struct RestorePublication {
    authority: RestoreStateAuthority,
    inventory: RestoreStateInventory,
}

fn domain_prefix(domain: &DomainName) -> Vec<u8> {
    let mut prefix = domain.as_str().as_bytes().to_vec();
    prefix.push(0);
    prefix
}

fn restore_prefix(domain: &DomainName, authority: &RestoreStateAuthority) -> Vec<u8> {
    let mut prefix = domain_prefix(domain);
    prefix.extend_from_slice(&authority.generation.to_be_bytes());
    prefix.push(0);
    prefix
}

impl RuntimeStateStore {
    fn restore_publication(
        &self,
        domain: &DomainName,
    ) -> error_stack::Result<Option<RestorePublication>, RuntimePersistenceError> {
        let Some(raw) = self
            .restore_publications
            .get(domain.as_str().as_bytes())
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        else {
            return Ok(None);
        };
        let publication = rkyv::from_bytes::<RestorePublication, rkyv::rancor::Error>(&raw)
            .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
        Ok(Some(publication))
    }

    /// Staging has no effect on the checkpoints a runtime can read.
    pub(in crate::runtime) fn stage_restored_checkpoint(
        &self,
        authority: &RestoreStateAuthority,
        placement: &RuntimeStatePlacement,
        revision: u64,
        payload: &[u8],
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        self.latest_snapshot_writer().with_installation(|| {
            if let Some(published) = self.restore_publication(&placement.domain)? {
                if published.authority == *authority {
                    return Ok(());
                }
                if published.authority.generation >= authority.generation {
                    return Err(Report::new(RuntimePersistenceError::RestoreGeneration {
                        requested: authority.generation,
                        published: published.authority.generation,
                    }));
                }
            }
            let checkpoint = StagedRestoreCheckpoint {
                authority: authority.clone(),
                placement: placement.to_remote(),
                snapshot: PersistedRuntimeStateEntry {
                    lsm: revision,
                    payload: payload.to_vec(),
                },
            };
            let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
                .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
            let mut key = restore_prefix(&placement.domain, authority);
            key.extend_from_slice(&placement.as_storage_key());
            self.restore_staging
                .insert(key, encoded.to_vec())
                .map_err(|_| Report::new(RuntimePersistenceError::WriteValue))
        })
    }

    /// Validates every staged checkpoint before atomically replacing the entire published set.
    /// A successful generation is durable and retryable without staging its checkpoints again.
    pub(in crate::runtime) fn publish_restored_state(
        &self,
        domain: &DomainName,
        authority: &RestoreStateAuthority,
        expected: RestoreStateInventory,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        self.latest_snapshot_writer().with_installation(|| {
            if let Some(published) = self.restore_publication(domain)? {
                if published.authority == *authority {
                    if published.inventory != expected {
                        return Err(Report::new(RuntimePersistenceError::DecodeState(
                            "restored state inventory differs from the published generation"
                                .to_string(),
                        )));
                    }
                    self.db
                        .persist(PersistMode::SyncAll)
                        .map_err(|_| RuntimePersistenceError::WriteValue)?;
                    return Ok(());
                }
                if published.authority.generation >= authority.generation {
                    return Err(Report::new(RuntimePersistenceError::RestoreGeneration {
                        requested: authority.generation,
                        published: published.authority.generation,
                    }));
                }
            }
            let prefix = restore_prefix(domain, authority);
            let mut inventory = RestoreStateInventory::default();
            let mut batch = self.db.batch();
            for keyspace in [&self.latest, &self.lsm_index] {
                for item in keyspace.prefix(domain_prefix(domain)) {
                    let key = item.key().map_err(|_| RuntimePersistenceError::ReadValue)?;
                    batch.remove(keyspace, key);
                }
            }
            for item in self.restore_staging.prefix(prefix) {
                let (key, raw) = item
                    .into_inner()
                    .map_err(|_| RuntimePersistenceError::ReadValue)?;
                let checkpoint =
                    rkyv::from_bytes::<StagedRestoreCheckpoint, rkyv::rancor::Error>(&raw)
                        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
                if checkpoint.authority != *authority || checkpoint.placement.domain != *domain {
                    return Err(Report::new(RuntimePersistenceError::DecodeState(
                        "staged state belongs to another restore installation".to_string(),
                    )));
                }
                let placement = RuntimeStatePlacement::from_remote(checkpoint.placement)
                    .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
                inventory.checkpoints = inventory
                    .checkpoints
                    .checked_add(1)
                    .ok_or(RuntimePersistenceError::ReadValue)?;
                inventory.payload_bytes = inventory
                    .payload_bytes
                    .checked_add(
                        u64::try_from(checkpoint.snapshot.payload.len())
                            .map_err(|_| RuntimePersistenceError::ReadValue)?,
                    )
                    .ok_or(RuntimePersistenceError::ReadValue)?;
                let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint.snapshot)
                    .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
                let placement_key = placement.as_storage_key();
                batch.insert(&self.latest, placement_key.clone(), encoded.to_vec());
                batch.insert(
                    &self.lsm_index,
                    placement.as_lsm_index_key(checkpoint.snapshot.lsm),
                    placement_key,
                );
                batch.remove(&self.restore_staging, key);
            }
            if inventory != expected {
                return Err(Report::new(RuntimePersistenceError::DecodeState(
                    "restored state installation is incomplete: staged inventory differs"
                        .to_string(),
                )));
            }
            // Also discard abandoned attempts; they can never publish through a newer authority.
            for item in self.restore_staging.prefix(domain_prefix(domain)) {
                batch.remove(
                    &self.restore_staging,
                    item.key().map_err(|_| RuntimePersistenceError::ReadValue)?,
                );
            }
            let published = rkyv::to_bytes::<rkyv::rancor::Error>(&RestorePublication {
                authority: authority.clone(),
                inventory,
            })
            .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
            batch.insert(
                &self.restore_publications,
                domain.as_str().as_bytes(),
                published.to_vec(),
            );
            batch
                .commit()
                .map_err(|_| RuntimePersistenceError::WriteValue)?;
            self.db
                .persist(PersistMode::SyncAll)
                .map_err(|_| RuntimePersistenceError::WriteValue)?;
            Ok(())
        })
    }

    /// Reads the three backup-owned runtime state kinds from one cross-keyspace database view.
    /// The caller closes the domain publication generation and forces owner publication before
    /// opening this view. Unrelated domains may keep writing concurrently.
    pub(in crate::runtime) fn snapshot_backup_domain(
        &self,
        domain: &DomainName,
    ) -> error_stack::Result<
        Vec<(StoredPlacement, PersistedRuntimeStateEntry)>,
        RuntimePersistenceError,
    > {
        let snapshot = self.db.snapshot();
        let mut prefix = domain.as_str().as_bytes().to_vec();
        prefix.push(0);
        let mut entries = Vec::new();
        for item in snapshot.prefix(&self.latest, prefix) {
            let (key, raw) = item
                .into_inner()
                .map_err(|_| Report::new(RuntimePersistenceError::ReadValue))?;
            let stored = stored_placement(key.as_ref())?;
            if !matches!(
                stored.state.kind(),
                RuntimeStateKind::WasmProcessor
                    | RuntimeStateKind::KafkaOffset
                    | RuntimeStateKind::BranchLru
            ) {
                continue;
            }
            let entry = PersistedRuntimeStateEntry::decode(raw.as_ref())?;
            entries.push((stored, entry));
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                    placement: placement(
                        "orders",
                        RuntimeState::WasmProcessor {
                            schema: SchemaFingerprint::from_digest([case.byte; 32]),
                            generation: WasmStateGeneration::try_from(case.generation.max(1))
                                .assured("a positive state generation is valid"),
                        },
                    )
                    .to_remote(),
                    snapshot: PersistedRuntimeStateEntry {
                        lsm: case.revision,
                        payload: vec![case.byte; usize::from(case.length)],
                    },
                };
                let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&staged)
                    .assured("the staged current checkpoint encodes");
                assert_eq!(
                    rkyv::from_bytes::<StagedRestoreCheckpoint, rkyv::rancor::Error>(&bytes)
                        .assured("the staged current checkpoint decodes"),
                    staged
                );
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
            .stage_restored_checkpoint(&authority, &offsets, 10, b"new-offset")
            .assured("offset stages");
        assert!(
            store
                .publish_restored_state(&domain, &authority, expected)
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
        let mut key = restore_prefix(&domain, &authority);
        key.extend_from_slice(&guest.as_storage_key());
        store
            .restore_staging
            .insert(&key, b"invalid-checkpoint")
            .assured("damaged checkpoint stages");
        assert!(
            store
                .publish_restored_state(&domain, &authority, expected)
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
            .stage_restored_checkpoint(&authority, &guest, 11, b"new-guest")
            .assured("guest stages");
        let previous = store.db.snapshot();
        store
            .publish_restored_state(&domain, &authority, expected)
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
            .get(&store.latest, guest.as_storage_key())
            .assured("previous snapshot reads")
            .assured("previous guest exists");
        assert_eq!(
            PersistedRuntimeStateEntry::decode(&prior_guest)
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
            .stage_restored_checkpoint(&authority, &offsets, 10, b"offset")
            .assured("offset stages");
        store
            .publish_restored_state(&domain, &authority, expected)
            .assured("generation publishes");
        drop(store);
        let store = open_store(dir.path());
        store
            .publish_restored_state(&domain, &authority, expected)
            .assured("publication retries after reopen");
        assert!(
            store
                .publish_restored_state(&domain, &authority, RestoreStateInventory::default())
                .is_err()
        );
        store
            .stage_restored_checkpoint(&authority, &offsets, 1, b"late-chunk")
            .assured("published staging retry has no effect");
        assert_eq!(
            store.restore_staging.len().assured("staging count reads"),
            0
        );
        let mut newer = authority.clone();
        newer.term += 1;
        newer.generation += 1;
        store
            .stage_restored_checkpoint(&newer, &offsets, 12, b"new-offset")
            .assured("new generation stages");
        store
            .publish_restored_state(
                &domain,
                &newer,
                RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: 10,
                },
            )
            .assured("new generation publishes");
        let Err(error) = store.stage_restored_checkpoint(&authority, &offsets, 20, b"stale") else {
            panic!("stale staging must be fenced");
        };
        assert!(matches!(
            error.current_context(),
            RuntimePersistenceError::RestoreGeneration { .. }
        ));
        let Err(error) = store.publish_restored_state(&domain, &authority, expected) else {
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
                .publish_restored_state(&domain, &competing, expected)
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
        let store = RuntimeStateStore::from_database(db, Executor::default())
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
        let other_domain = placement("billing", RuntimeState::KafkaOffset);
        for (placement, lsm) in [
            (&kafka, 5),
            (&lifecycle, 3),
            (&wasm, 7),
            (&unrelated_kind, 9),
            (&other_domain, 11),
        ] {
            store
                .persist_latest_snapshot(placement, lsm, b"current")
                .assured("checkpoint persists");
        }
        store
            .persist_latest_snapshot(&kafka, 2, b"late-periodic-write")
            .assured("an earlier periodic write completes");
        let mut read = store
            .snapshot_backup_domain(&domain)
            .assured("one database view opens");
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
            .publish_restored_state(&domain, &authority(), RestoreStateInventory::default())
            .assured("restored target purges");
        assert!(
            store
                .snapshot_backup_domain(&domain)
                .assured("purged domain view opens")
                .is_empty()
        );
        assert_eq!(
            store
                .latest_snapshot(&other_domain)
                .assured("unrelated domain remains readable")
                .assured("unrelated checkpoint remains")
                .lsm,
            11
        );
    }
}
