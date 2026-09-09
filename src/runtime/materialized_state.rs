use std::sync::{
    Arc as StdArc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use ahash::RandomState;
use dashmap::DashMap;
use error_stack::Report;
use nervix_models::{ClusterNodeName, RemoteRuntimeField, RemoteRuntimeRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use triomphe::Arc;

use super::{
    BranchKey, PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStateOperationError,
    RuntimeStatePlacement, StateAssignmentAuthority, StateAssignmentToken, StateCapability,
    StateReplicationRoles, lsm_sequence::LsmSequence,
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
    placement: RuntimeStatePlacement,
    schema: StdArc<arrow_schema::Schema>,
    assignment: StateAssignmentAuthority,
    entries: DashMap<Option<BranchKey>, RuntimeRow, RandomState>,
    current_lsm: LsmSequence,
    last_persisted_lsm: AtomicU64,
    dirty: AtomicBool,
}

/// Read-only access to materialized records and snapshots.
#[derive(Debug, Clone)]
pub struct MaterializedRelayStateRead {
    state: Arc<ReplicatedMaterializedRelayState>,
}

/// Authoritative materialization access for one concrete assignment generation.
#[derive(Debug, Clone)]
pub struct MaterializedRelayStateOriginator {
    read: MaterializedRelayStateRead,
    assignment: StateAssignmentToken,
}

/// Replica snapshot installation access for one concrete assignment generation.
#[derive(Debug, Clone)]
pub struct MaterializedRelaySnapshotInstaller {
    read: MaterializedRelayStateRead,
    assignment: StateAssignmentToken,
}

/// Local snapshot persistence access shared by owners and replicas.
#[derive(Debug, Clone)]
pub(super) struct MaterializedRelayStatePersistence {
    read: MaterializedRelayStateRead,
}

#[derive(Debug)]
pub(super) struct MaterializedRelayStateAssignment {
    pub(super) originator: Option<MaterializedRelayStateOriginator>,
    pub(super) installer: Option<MaterializedRelaySnapshotInstaller>,
    pub(super) persistence: MaterializedRelayStatePersistence,
}

impl ReplicatedMaterializedRelayState {
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        schema: StdArc<arrow_schema::Schema>,
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
            assignment: StateAssignmentAuthority::default(),
            entries,
            current_lsm: LsmSequence::restored(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            dirty: AtomicBool::new(false),
        })
    }

    pub(super) fn bind(
        state: &Arc<Self>,
        roles: StateReplicationRoles,
        local_node: Option<&ClusterNodeName>,
    ) -> MaterializedRelayStateAssignment {
        let binding = state.assignment.rebind(roles, local_node);
        let read = MaterializedRelayStateRead {
            state: state.clone(),
        };
        MaterializedRelayStateAssignment {
            originator: binding
                .token_for(StateCapability::Originate)
                .map(|assignment| MaterializedRelayStateOriginator {
                    read: read.clone(),
                    assignment,
                }),
            installer: binding
                .token_for(StateCapability::InstallSnapshot)
                .map(|assignment| MaterializedRelaySnapshotInstaller {
                    read: read.clone(),
                    assignment,
                }),
            persistence: MaterializedRelayStatePersistence { read: read.clone() },
        }
    }

    pub(super) fn read(state: &Arc<Self>) -> MaterializedRelayStateRead {
        MaterializedRelayStateRead {
            state: state.clone(),
        }
    }

    pub(super) fn current_installer(
        state: &Arc<Self>,
    ) -> Option<MaterializedRelaySnapshotInstaller> {
        let assignment = state
            .assignment
            .current_binding()
            .token_for(StateCapability::InstallSnapshot)?;
        Some(MaterializedRelaySnapshotInstaller {
            read: Self::read(state),
            assignment,
        })
    }
}

impl MaterializedRelayStateRead {
    pub(super) fn placement(&self) -> &RuntimeStatePlacement {
        &self.state.placement
    }

    pub(super) fn current_lsm(&self) -> u64 {
        self.state.current_lsm.current()
    }

    pub(super) fn primary_node(&self) -> Option<ClusterNodeName> {
        self.state.assignment.roles().primary_node
    }

    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        self.state.assignment.serialize(|| {
            Ok(PersistedRuntimeStateEntry {
                lsm: self.state.current_lsm.current(),
                schema_fingerprint: self.state.placement.schema_fingerprint,
                payload: encode_materialized_stream_snapshot(&self.state.entries)?,
            })
        })
    }

    pub(super) fn values_at(
        &self,
        key: &Option<BranchKey>,
        column_indices: impl IntoIterator<Item = usize>,
    ) -> Result<Option<Vec<Option<RuntimeValue>>>, String> {
        let Some(record) = self.state.entries.get(key) else {
            return Ok(None);
        };
        column_indices
            .into_iter()
            .map(|column_index| record.value_at(column_index))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub(super) fn remote_entries(
        &self,
    ) -> Result<Vec<(Option<BranchKey>, RemoteRuntimeRecord)>, Report<RuntimePersistenceError>>
    {
        self.state
            .entries
            .iter()
            .map(|entry| {
                entry
                    .value()
                    .to_remote()
                    .map(|record| (entry.key().clone(), record))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Report::new(RuntimePersistenceError::EncodeState(error)))
    }

    pub(super) fn remote_entry(
        &self,
        key: &Option<BranchKey>,
    ) -> Result<Option<(Option<BranchKey>, RemoteRuntimeRecord)>, Report<RuntimePersistenceError>>
    {
        self.state
            .entries
            .get(key)
            .map(|record| record.to_remote().map(|record| (key.clone(), record)))
            .transpose()
            .map_err(|error| Report::new(RuntimePersistenceError::EncodeState(error)))
    }

    pub(super) fn restored_branch_watermarks(
        &self,
    ) -> Vec<(Option<BranchKey>, nervix_models::Timestamp)> {
        self.state
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.key().clone(),
                    entry.value().metadata().ingested_at_high_watermark(),
                )
            })
            .collect()
    }
}

impl MaterializedRelayStateOriginator {
    pub(super) fn read(&self) -> &MaterializedRelayStateRead {
        &self.read
    }

    pub(super) fn update_last_by_timestamp(
        &self,
        key: &Option<BranchKey>,
        record: &RuntimeRow,
    ) -> Result<Option<u64>, Report<super::StateAuthorityError>> {
        self.read
            .state
            .assignment
            .authorize(self.assignment, StateCapability::Originate, || {
                let should_update = if let Some(existing) = self.read.state.entries.get(key) {
                    record.metadata().is_newer_than(existing.metadata())
                } else {
                    true
                };
                if !should_update {
                    return None;
                }
                self.read.state.entries.insert(key.clone(), record.clone());
                let lsm = self.read.state.current_lsm.advance();
                self.read.state.dirty.store(true, Ordering::SeqCst);
                Some(lsm)
            })
    }

    pub(super) fn remove_key(
        &self,
        key: &Option<BranchKey>,
    ) -> Result<Option<u64>, Report<super::StateAuthorityError>> {
        self.read
            .state
            .assignment
            .authorize(self.assignment, StateCapability::Originate, || {
                self.read.state.entries.remove(key)?;
                let lsm = self.read.state.current_lsm.advance();
                self.read.state.dirty.store(true, Ordering::SeqCst);
                Some(lsm)
            })
    }
}

impl MaterializedRelaySnapshotInstaller {
    pub(super) fn read(&self) -> &MaterializedRelayStateRead {
        &self.read
    }

    pub(super) fn install_snapshot(
        &self,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), RuntimeStateOperationError> {
        let entries = decode_materialized_stream_snapshot(payload)?;
        let decoded = entries
            .into_iter()
            .map(|(key, record)| {
                RuntimeRow::from_remote(self.read.state.schema.clone(), record)
                    .map(|record| (key, record))
                    .map_err(RuntimePersistenceError::DecodeState)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.read.state.assignment.authorize(
            self.assignment,
            StateCapability::InstallSnapshot,
            || {
                self.read.state.entries.clear();
                for (key, record) in decoded {
                    self.read.state.entries.insert(key, record);
                }
                self.read.state.current_lsm.adopt(lsm);
                self.read.state.dirty.store(true, Ordering::SeqCst);
            },
        )?;
        Ok(())
    }
}

impl MaterializedRelayStatePersistence {
    pub(super) fn read(&self) -> &MaterializedRelayStateRead {
        &self.read
    }

    pub(super) fn take_dirty(&self) -> bool {
        self.read.state.dirty.swap(false, Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn is_dirty(&self) -> bool {
        self.read.state.dirty.load(Ordering::SeqCst)
    }

    pub(super) fn restore_dirty(&self) {
        self.read.state.dirty.store(true, Ordering::SeqCst);
    }

    pub(super) fn last_persisted_lsm(&self) -> u64 {
        self.read.state.last_persisted_lsm.load(Ordering::SeqCst)
    }

    pub(super) fn record_persisted(&self, lsm: u64) {
        self.read.state.assignment.serialize(|| {
            self.read
                .state
                .last_persisted_lsm
                .fetch_max(lsm, Ordering::SeqCst);
            if self.read.state.current_lsm.current() <= lsm {
                self.read.state.dirty.store(false, Ordering::SeqCst);
            }
        });
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
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{DomainName, ModelKind, ModelName, RelayName};

    use super::*;
    use crate::runtime_schema::{RuntimeValue, test_runtime_row};

    #[test]
    fn unbranched_materialized_state_snapshot_restores_entries() {
        let domain = DomainName::parse("default")
            .assured("the test domain name satisfies the domain grammar");
        let relay = RelayName::parse("notifications")
            .assured("the test relay name satisfies the relay grammar");
        let placement = RuntimeStatePlacement {
            domain,
            state: super::super::RuntimeStateKind::MaterializedRelay,
            kind: ModelKind::Relay,
            identifier: ModelName::from(&relay),
            schema_fingerprint: [0; 32],
            branch_key: None,
        };
        let record = test_runtime_row([(
            "value".to_string(),
            RuntimeValue::String("ready".to_string()),
        )]);
        let schema = record.arrow_schema();
        let state = Arc::new(
            ReplicatedMaterializedRelayState::new(placement.clone(), schema.clone(), None)
                .assured("unbranched materialized state should build"),
        );
        let mut assignment = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::owned_by(None),
            None,
        );
        let originator = assignment
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");

        let lsm = originator
            .update_last_by_timestamp(&None, &record)
            .assured("the assignment remains authoritative")
            .assured("the first record should update state");
        let payload = originator
            .read()
            .latest_snapshot()
            .assured("unbranched materialized state should snapshot")
            .payload;
        let restored = ReplicatedMaterializedRelayState::new(
            placement,
            schema,
            Some(PersistedRuntimeStateEntry {
                lsm,
                schema_fingerprint: [0; 32],
                payload,
            }),
        )
        .assured("unbranched materialized state should restore");
        let restored = Arc::new(restored);
        let read = ReplicatedMaterializedRelayState::read(&restored);

        assert_eq!(
            read.values_at(&None, [0])
                .assured("restored field should load")
                .assured("restored record should exist"),
            vec![Some(RuntimeValue::String("ready".to_string()))]
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
        let state = Arc::new(
            ReplicatedMaterializedRelayState::new(
                RuntimeStatePlacement {
                    domain: DomainName::parse("default")
                        .assured("the test domain name satisfies the domain grammar"),
                    state: super::super::RuntimeStateKind::MaterializedRelay,
                    kind: ModelKind::Relay,
                    identifier: ModelName::parse("profiles")
                        .assured("the test model name satisfies the model-name grammar"),
                    schema_fingerprint: [0; 32],
                    branch_key: None,
                },
                record.arrow_schema(),
                None,
            )
            .assured("materialized state should build"),
        );
        let mut assignment = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::owned_by(None),
            None,
        );
        let originator = assignment
            .originator
            .take()
            .assured("branch-local state is authoritative in this process");
        assert!(
            originator
                .update_last_by_timestamp(&None, &record)
                .assured("the assignment remains authoritative")
                .is_some()
        );

        assert_eq!(
            originator
                .read()
                .values_at(&None, [1, 0])
                .assured("selected fields should load")
                .assured("the materialized record should exist"),
            vec![
                Some(RuntimeValue::I64(42)),
                Some(RuntimeValue::String("ready".to_string())),
            ]
        );
    }
}
