//! The store operations a backup cut and a restore installation use for one domain.
//!
//! Layer: infrastructure.
//! - **Owns.** Reading the archive-owned state kinds from one database view and purging the
//!   restored domain's previous checkpoints.
//! - **Depends on.** The runtime state store's typed placement encoding and Fjall snapshot.
//! - **Must not know.** Archive records, consensus revisions, or client restore options.

use super::*;

impl RuntimeStateStore {
    pub(in crate::runtime) fn purge_domain(
        &self,
        domain: &DomainName,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let mut domain_prefix = domain.as_str().as_bytes().to_vec();
        domain_prefix.push(0);
        let latest_keys = self
            .latest
            .prefix(domain_prefix.clone())
            .map(|item| {
                item.key()
                    .map(|key| key.as_ref().to_vec())
                    .map_err(|_| Report::new(RuntimePersistenceError::ReadValue))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let lsm_keys = self
            .lsm_index
            .prefix(domain_prefix)
            .map(|item| {
                item.key()
                    .map(|key| key.as_ref().to_vec())
                    .map_err(|_| Report::new(RuntimePersistenceError::ReadValue))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if latest_keys.is_empty() && lsm_keys.is_empty() {
            return Ok(());
        }

        let mut batch = self.db.batch();
        for key in latest_keys {
            batch.remove(&self.latest, key);
        }
        for key in lsm_keys {
            batch.remove(&self.lsm_index, key);
        }
        batch
            .commit()
            .map_err(|_| Report::new(RuntimePersistenceError::WriteValue))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|_| Report::new(RuntimePersistenceError::WriteValue))
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

    #[test]
    fn domain_view_keeps_current_state_kinds_and_purge_leaves_other_domains() {
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
            .purge_domain(&domain)
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
