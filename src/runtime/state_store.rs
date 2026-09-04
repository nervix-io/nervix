use std::str::FromStr;

use ahash::HashMap;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
pub(crate) use nervix_interconnect::RuntimeStateKind;
use nervix_models::{Domain, Identifier, ModelKind};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;

use super::BranchKey;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RuntimeStatePlacement {
    pub(crate) domain: Domain,
    pub(crate) state: RuntimeStateKind,
    pub(crate) kind: ModelKind,
    pub(crate) identifier: Identifier,
    pub(crate) schema_fingerprint: [u8; 32],
    pub(crate) branch_key: Option<BranchKey>,
}

/// Which cluster nodes currently own and replicate one runtime state. Ownership moves while the
/// state itself lives on, so a replicated state keeps its roles as rebindable configuration rather
/// than as a construction-time constant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StateReplicationRoles {
    pub(crate) primary_node: Option<String>,
    pub(crate) replica_nodes: Vec<String>,
    pub(crate) required_replica_acks: usize,
}

impl StateReplicationRoles {
    pub(crate) fn new(
        primary_node: Option<String>,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
    ) -> Self {
        Self {
            primary_node,
            replica_nodes,
            required_replica_acks,
        }
    }

    pub(crate) fn owned_by(primary_node: Option<String>) -> Self {
        Self {
            primary_node,
            replica_nodes: Vec::new(),
            required_replica_acks: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct PersistedRuntimeStateEntry {
    pub lsm: u64,
    pub schema_fingerprint: [u8; 32],
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum RuntimePersistenceError {
    #[error("failed to open runtime state keyspace")]
    OpenKeyspace,
    #[error("failed to read runtime state value")]
    ReadValue,
    #[error("failed to write runtime state value")]
    WriteValue,
    #[error("failed to encode runtime state: {0}")]
    EncodeState(String),
    #[error("failed to decode runtime state: {0}")]
    DecodeState(String),
    #[error("persisted runtime state for {kind} '{identifier}' has a stale schema fingerprint")]
    SchemaFingerprintMismatch {
        kind: &'static str,
        identifier: String,
    },
}

pub struct RuntimeStateStore {
    db: Database,
    latest: Keyspace,
    lsm_index: Keyspace,
}

impl RuntimeStatePlacement {
    pub fn as_storage_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(self.domain.as_str().as_bytes());
        key.push(0);
        key.push(self.state as u8);
        key.push(0);
        key.extend_from_slice(self.kind.as_str().as_bytes());
        key.push(0);
        key.extend_from_slice(self.identifier.as_str().as_bytes());
        key.push(0);
        key.extend_from_slice(&self.schema_fingerprint);
        key.push(0);
        match self.branch_key.as_ref() {
            Some(branch_key) => {
                key.push(1);
                key.extend_from_slice(branch_key.as_str().as_bytes());
            }
            None => key.push(0),
        }
        key
    }

    fn as_lsm_index_key(&self, lsm: u64) -> Vec<u8> {
        let mut key = self.as_storage_key();
        key.push(0);
        key.extend_from_slice(&lsm.to_be_bytes());
        key
    }

    pub(crate) fn to_remote(&self) -> nervix_interconnect::StatePlacementEnvelope {
        nervix_interconnect::StatePlacementEnvelope {
            domain: self.domain.clone(),
            state: self.state,
            kind: self.kind,
            identifier: self.identifier.clone(),
            schema_fingerprint: self.schema_fingerprint,
            branch_key: BranchKey::to_remote_key(&self.branch_key),
        }
    }

    pub(crate) fn from_remote(
        placement: nervix_interconnect::StatePlacementEnvelope,
    ) -> Result<Self, String> {
        Ok(Self {
            domain: placement.domain,
            state: placement.state,
            kind: placement.kind,
            identifier: placement.identifier,
            schema_fingerprint: placement.schema_fingerprint,
            branch_key: BranchKey::from_remote_key(placement.branch_key)?,
        })
    }

    pub(in crate::runtime) fn concrete_branch_key(&self) -> &str {
        self.branch_key
            .as_ref()
            .map(BranchKey::as_str)
            .expect("concrete runtime state must carry a branch key")
    }
}

impl RuntimeStateStore {
    pub fn from_database(db: Database) -> Result<Self, RuntimePersistenceError> {
        let latest = db
            .keyspace("runtime_state_latest", KeyspaceCreateOptions::default)
            .map_err(|_| RuntimePersistenceError::OpenKeyspace)?;
        let lsm_index = db
            .keyspace("runtime_state_lsm", KeyspaceCreateOptions::default)
            .map_err(|_| RuntimePersistenceError::OpenKeyspace)?;
        Ok(Self {
            db,
            latest,
            lsm_index,
        })
    }

    pub fn persist_latest_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), RuntimePersistenceError> {
        let entry = PersistedRuntimeStateEntry {
            lsm,
            schema_fingerprint: placement.schema_fingerprint,
            payload: payload.to_vec(),
        };
        let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&entry)
            .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
        let placement_key = placement.as_storage_key();
        self.latest
            .insert(placement_key.clone(), encoded.to_vec())
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        self.lsm_index
            .insert(placement.as_lsm_index_key(lsm), placement_key)
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        self.db
            .persist(PersistMode::Buffer)
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        Ok(())
    }

    pub fn latest_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Result<Option<PersistedRuntimeStateEntry>, RuntimePersistenceError> {
        let Some(raw) = self
            .latest
            .get(placement.as_storage_key())
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        else {
            return Ok(None);
        };
        let archived = rkyv::access::<
            <PersistedRuntimeStateEntry as Archive>::Archived,
            rkyv::rancor::Error,
        >(raw.as_ref())
        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
        if archived.schema_fingerprint != placement.schema_fingerprint {
            return Err(RuntimePersistenceError::SchemaFingerprintMismatch {
                kind: placement.kind.as_str(),
                identifier: placement.identifier.as_str().to_string(),
            });
        }
        Ok(Some(PersistedRuntimeStateEntry {
            lsm: archived.lsm.into(),
            schema_fingerprint: archived.schema_fingerprint,
            payload: archived.payload.as_slice().to_vec(),
        }))
    }

    pub fn purge_domain(&self, domain: &Domain) -> Result<(), RuntimePersistenceError> {
        let mut domain_prefix = domain.as_str().as_bytes().to_vec();
        domain_prefix.push(0);
        let latest_keys = self
            .latest
            .prefix(domain_prefix.clone())
            .map(|item| {
                item.key()
                    .map(|key| key.as_ref().to_vec())
                    .map_err(|_| RuntimePersistenceError::ReadValue)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let lsm_keys = self
            .lsm_index
            .prefix(domain_prefix)
            .map(|item| {
                item.key()
                    .map(|key| key.as_ref().to_vec())
                    .map_err(|_| RuntimePersistenceError::ReadValue)
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
            .map_err(|_| RuntimePersistenceError::WriteValue)
    }

    pub fn purge_entity(
        &self,
        domain: &Domain,
        state: RuntimeStateKind,
        kind: ModelKind,
        identifier: &Identifier,
    ) -> Result<(), RuntimePersistenceError> {
        let mut prefix = domain.as_str().as_bytes().to_vec();
        prefix.push(0);
        prefix.push(state as u8);
        prefix.push(0);
        prefix.extend_from_slice(kind.as_str().as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(identifier.as_str().as_bytes());
        prefix.push(0);
        let latest_keys = self
            .latest
            .prefix(prefix)
            .map(|item| {
                item.key()
                    .map(|key| key.as_ref().to_vec())
                    .map_err(|_| RuntimePersistenceError::ReadValue)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if latest_keys.is_empty() {
            return Ok(());
        }
        let mut lsm_keys = Vec::new();
        for latest_key in &latest_keys {
            let mut lsm_prefix = latest_key.clone();
            lsm_prefix.push(0);
            lsm_keys.extend(
                self.lsm_index
                    .prefix(lsm_prefix)
                    .map(|item| {
                        item.key()
                            .map(|key| key.as_ref().to_vec())
                            .map_err(|_| RuntimePersistenceError::ReadValue)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
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
            .map_err(|_| RuntimePersistenceError::WriteValue)
    }

    pub fn purge_stale_schema_fingerprints(
        &self,
        domain: &Domain,
        current: &HashMap<(ModelKind, Identifier), [u8; 32]>,
    ) -> Result<(), RuntimePersistenceError> {
        let mut domain_prefix = domain.as_str().as_bytes().to_vec();
        domain_prefix.push(0);
        let stale_latest_keys = self
            .latest
            .prefix(domain_prefix)
            .map(|item| {
                item.key()
                    .map(|key| key.as_ref().to_vec())
                    .map_err(|_| RuntimePersistenceError::ReadValue)
            })
            .filter_map(|item| match item {
                Ok(key) => match stored_placement_schema(&key) {
                    Ok((state, kind, identifier, fingerprint)) => {
                        let expected =
                            current
                                .get(&(kind, identifier))
                                .copied()
                                .map(|fingerprint| {
                                    if let RuntimeStateKind::BranchAggregated
                                    | RuntimeStateKind::KafkaOffset = state
                                    {
                                        [0; 32]
                                    } else {
                                        fingerprint
                                    }
                                });
                        (expected != Some(fingerprint)).then_some(Ok(key))
                    }
                    Err(error) => Some(Err(error)),
                },
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if stale_latest_keys.is_empty() {
            return Ok(());
        }

        let mut stale_lsm_keys = Vec::new();
        for latest_key in &stale_latest_keys {
            let mut lsm_prefix = latest_key.clone();
            lsm_prefix.push(0);
            stale_lsm_keys.extend(
                self.lsm_index
                    .prefix(lsm_prefix)
                    .map(|item| {
                        item.key()
                            .map(|key| key.as_ref().to_vec())
                            .map_err(|_| RuntimePersistenceError::ReadValue)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        let mut batch = self.db.batch();
        for key in stale_latest_keys {
            batch.remove(&self.latest, key);
        }
        for key in stale_lsm_keys {
            batch.remove(&self.lsm_index, key);
        }
        batch
            .commit()
            .map_err(|_| RuntimePersistenceError::WriteValue)
    }
}

fn stored_placement_schema(
    key: &[u8],
) -> Result<(RuntimeStateKind, ModelKind, Identifier, [u8; 32]), RuntimePersistenceError> {
    let domain_end = key.iter().position(|byte| *byte == 0).ok_or_else(|| {
        RuntimePersistenceError::DecodeState(
            "runtime state key has no domain separator".to_string(),
        )
    })?;
    let state_offset = domain_end.saturating_add(1);
    let state = key
        .get(state_offset)
        .and_then(|state| match state {
            0 => Some(RuntimeStateKind::BranchAggregated),
            1 => Some(RuntimeStateKind::Correlator),
            2 => Some(RuntimeStateKind::Deduplicator),
            3 => Some(RuntimeStateKind::KafkaOffset),
            4 => Some(RuntimeStateKind::MaterializedRelay),
            5 => Some(RuntimeStateKind::WasmProcessor),
            6 => Some(RuntimeStateKind::WindowProcessor),
            7 => Some(RuntimeStateKind::BranchLru),
            _ => None,
        })
        .ok_or_else(|| {
            RuntimePersistenceError::DecodeState(
                "runtime state key has an invalid state kind".to_string(),
            )
        })?;
    let kind_start = state_offset.saturating_add(2);
    let kind_end = key[kind_start..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| kind_start.saturating_add(offset))
        .ok_or_else(|| {
            RuntimePersistenceError::DecodeState(
                "runtime state key has no model-kind separator".to_string(),
            )
        })?;
    let kind = std::str::from_utf8(&key[kind_start..kind_end])
        .ok()
        .and_then(|kind| ModelKind::from_str(kind).ok())
        .ok_or_else(|| {
            RuntimePersistenceError::DecodeState(
                "runtime state key has an invalid model kind".to_string(),
            )
        })?;
    let identifier_start = kind_end.saturating_add(1);
    let identifier_end = key[identifier_start..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| identifier_start.saturating_add(offset))
        .ok_or_else(|| {
            RuntimePersistenceError::DecodeState(
                "runtime state key has no identifier separator".to_string(),
            )
        })?;
    let identifier = std::str::from_utf8(&key[identifier_start..identifier_end])
        .ok()
        .and_then(|identifier| Identifier::parse(identifier).ok())
        .ok_or_else(|| {
            RuntimePersistenceError::DecodeState(
                "runtime state key has an invalid identifier".to_string(),
            )
        })?;
    let fingerprint_start = identifier_end.saturating_add(1);
    let fingerprint = key
        .get(fingerprint_start..fingerprint_start.saturating_add(32))
        .ok_or_else(|| {
            RuntimePersistenceError::DecodeState(
                "runtime state key has a truncated schema fingerprint".to_string(),
            )
        })?;
    let mut schema_fingerprint = [0; 32];
    schema_fingerprint.copy_from_slice(fingerprint);
    Ok((state, kind, identifier, schema_fingerprint))
}
