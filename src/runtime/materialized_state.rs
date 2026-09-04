use std::sync::{
    Arc as StdArc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use ahash::RandomState;
use dashmap::DashMap;
use nervix_models::{RemoteRuntimeField, RemoteRuntimeRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use super::{
    BranchKey, PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
    StateReplicationRoles,
};
use crate::runtime_schema::{RuntimeRow, RuntimeValue};

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct MaterializedRelayEntrySnapshot {
    key: Option<Vec<RemoteRuntimeField>>,
    record: RemoteRuntimeRecord,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct MaterializedRelaySnapshot {
    entries: Vec<MaterializedRelayEntrySnapshot>,
}

#[derive(Debug)]
pub(super) struct ReplicatedMaterializedRelayState {
    pub(super) placement: RuntimeStatePlacement,
    schema: StdArc<arrow_schema::Schema>,
    roles: parking_lot::RwLock<StateReplicationRoles>,
    pub(super) entries: DashMap<Option<BranchKey>, RuntimeRow, RandomState>,
    pub(super) current_lsm: AtomicU64,
    pub(super) last_persisted_lsm: AtomicU64,
    pub(super) dirty: AtomicBool,
}

impl ReplicatedMaterializedRelayState {
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        schema: StdArc<arrow_schema::Schema>,
        primary_node: Option<String>,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let entries = DashMap::default();
        let mut current_lsm = 0;
        let mut last_persisted_lsm = 0;
        if let Some(initial) = initial {
            current_lsm = initial.lsm;
            last_persisted_lsm = initial.lsm;
            let snapshot_entries = decode_materialized_stream_snapshot(&initial.payload)?;
            for (key, record) in snapshot_entries {
                entries.insert(
                    key,
                    RuntimeRow::from_remote(schema.clone(), record)
                        .map_err(RuntimePersistenceError::DecodeState)?,
                );
            }
        }
        Ok(Self {
            placement,
            schema,
            roles: parking_lot::RwLock::new(StateReplicationRoles::owned_by(primary_node)),
            entries,
            current_lsm: AtomicU64::new(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            dirty: AtomicBool::new(false),
        })
    }

    pub(super) fn primary_node(&self) -> Option<String> {
        self.roles.read().primary_node.clone()
    }

    pub(super) fn rebind_roles(&self, roles: StateReplicationRoles) {
        *self.roles.write() = roles;
    }

    pub(super) fn apply_snapshot(
        &self,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), RuntimePersistenceError> {
        let entries = decode_materialized_stream_snapshot(payload)?;
        self.entries.clear();
        for (key, record) in entries {
            self.entries.insert(
                key,
                RuntimeRow::from_remote(self.schema.clone(), record)
                    .map_err(RuntimePersistenceError::DecodeState)?,
            );
        }
        self.current_lsm.store(lsm, Ordering::SeqCst);
        self.dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        Ok(PersistedRuntimeStateEntry {
            lsm: self.current_lsm.load(Ordering::SeqCst),
            schema_fingerprint: self.placement.schema_fingerprint,
            payload: encode_materialized_stream_snapshot(&self.entries)?,
        })
    }

    pub(super) fn update_last_by_timestamp(
        &self,
        key: &Option<BranchKey>,
        record: &RuntimeRow,
    ) -> Option<u64> {
        let should_update = if let Some(existing) = self.entries.get(key) {
            record.metadata().is_newer_than(existing.metadata())
        } else {
            true
        };
        if !should_update {
            return None;
        }
        self.entries.insert(key.clone(), record.clone());
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        Some(lsm)
    }

    pub(super) fn remove_key(&self, key: &Option<BranchKey>) -> Option<u64> {
        self.entries.remove(key)?;
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        Some(lsm)
    }

    pub(super) fn values_at(
        &self,
        key: &Option<BranchKey>,
        column_indices: impl IntoIterator<Item = usize>,
    ) -> Result<Option<Vec<Option<RuntimeValue>>>, String> {
        let Some(record) = self.entries.get(key) else {
            return Ok(None);
        };
        column_indices
            .into_iter()
            .map(|column_index| record.value_at(column_index))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }
}

pub(super) fn encode_materialized_stream_snapshot_entries(
    entries: &[(Option<BranchKey>, RemoteRuntimeRecord)],
) -> Result<Vec<u8>, RuntimePersistenceError> {
    let mut snapshot_entries = entries
        .iter()
        .map(|(key, record)| MaterializedRelayEntrySnapshot {
            key: BranchKey::to_remote_key(key),
            record: record.clone(),
        })
        .collect::<Vec<_>>();
    snapshot_entries.sort_by_key(|entry| snapshot_key_sort(&entry.key));
    rkyv::to_bytes::<rkyv::rancor::Error>(&MaterializedRelaySnapshot {
        entries: snapshot_entries,
    })
    .map(|bytes| bytes.to_vec())
    .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))
}

pub(super) fn decode_materialized_stream_snapshot(
    payload: &[u8],
) -> Result<Vec<(Option<BranchKey>, RemoteRuntimeRecord)>, RuntimePersistenceError> {
    let snapshot = rkyv::from_bytes::<MaterializedRelaySnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
    snapshot
        .entries
        .into_iter()
        .map(|entry| {
            BranchKey::from_remote_key(entry.key)
                .map(|key| (key, entry.record))
                .map_err(RuntimePersistenceError::DecodeState)
        })
        .collect()
}

fn encode_materialized_stream_snapshot(
    entries: &DashMap<Option<BranchKey>, RuntimeRow, RandomState>,
) -> Result<Vec<u8>, RuntimePersistenceError> {
    let mut snapshot_entries = entries
        .iter()
        .map(|entry| {
            Ok(MaterializedRelayEntrySnapshot {
                key: BranchKey::to_remote_key(entry.key()),
                record: entry
                    .value()
                    .to_remote()
                    .map_err(RuntimePersistenceError::EncodeState)?,
            })
        })
        .collect::<Result<Vec<_>, RuntimePersistenceError>>()?;
    snapshot_entries.sort_by_key(|entry| snapshot_key_sort(&entry.key));
    rkyv::to_bytes::<rkyv::rancor::Error>(&MaterializedRelaySnapshot {
        entries: snapshot_entries,
    })
    .map(|bytes| bytes.to_vec())
    .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))
}

fn snapshot_key_sort(key: &Option<Vec<RemoteRuntimeField>>) -> String {
    let Some(fields) = key else {
        return String::new();
    };
    fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>()
        .join("\0")
}

#[cfg(test)]
mod tests {
    use nervix_models::{Domain, Identifier, ModelKind};

    use super::*;
    use crate::runtime_schema::{RuntimeValue, test_runtime_row};

    #[test]
    fn unbranched_materialized_state_snapshot_restores_entries() {
        let domain = Domain::parse("default").expect("valid domain");
        let relay = Identifier::parse("notifications").expect("valid relay");
        let placement = RuntimeStatePlacement {
            domain,
            state: super::super::RuntimeStateKind::MaterializedRelay,
            kind: ModelKind::Relay,
            identifier: relay,
            schema_fingerprint: [0; 32],
            branch_key: None,
        };
        let record = test_runtime_row([(
            "value".to_string(),
            RuntimeValue::String("ready".to_string()),
        )]);
        let schema = record.arrow_schema();
        let state =
            ReplicatedMaterializedRelayState::new(placement.clone(), schema.clone(), None, None)
                .expect("unbranched materialized state should build");

        let lsm = state
            .update_last_by_timestamp(&None, &record)
            .expect("the first record should update state");
        let payload = state
            .latest_snapshot()
            .expect("unbranched materialized state should snapshot")
            .payload;
        let restored = ReplicatedMaterializedRelayState::new(
            placement,
            schema,
            None,
            Some(PersistedRuntimeStateEntry {
                lsm,
                schema_fingerprint: [0; 32],
                payload,
            }),
        )
        .expect("unbranched materialized state should restore");

        assert_eq!(
            restored
                .entries
                .get(&None)
                .expect("restored record should exist")
                .value()
                .value("value")
                .expect("restored field should load"),
            Some(RuntimeValue::String("ready".to_string()))
        );
    }

    #[test]
    fn materialized_state_reads_selected_arrow_columns_by_index() {
        let record = test_runtime_row([
            (
                "status".to_string(),
                RuntimeValue::String("ready".to_string()),
            ),
            ("score".to_string(), RuntimeValue::I64(42)),
        ]);
        let state = ReplicatedMaterializedRelayState::new(
            RuntimeStatePlacement {
                domain: Domain::parse("default").expect("valid domain"),
                state: super::super::RuntimeStateKind::MaterializedRelay,
                kind: ModelKind::Relay,
                identifier: Identifier::parse("profiles").expect("valid relay"),
                schema_fingerprint: [0; 32],
                branch_key: None,
            },
            record.arrow_schema(),
            None,
            None,
        )
        .expect("materialized state should build");
        assert!(state.update_last_by_timestamp(&None, &record).is_some());

        assert_eq!(
            state
                .values_at(&None, [1, 0])
                .expect("selected fields should load")
                .expect("the materialized record should exist"),
            vec![
                Some(RuntimeValue::I64(42)),
                Some(RuntimeValue::String("ready".to_string())),
            ]
        );
    }
}
