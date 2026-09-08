use std::str::FromStr;

use ahash::HashMap;
use error_stack::Report;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use meticulous::OptionExt as _;
pub(crate) use nervix_interconnect::RuntimeStateKind;
use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName, NodeRef};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;

use super::BranchKey;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RuntimeStatePlacement {
    pub(crate) domain: DomainName,
    pub(crate) state: RuntimeStateKind,
    pub(crate) kind: ModelKind,
    pub(crate) identifier: ModelName,
    pub(crate) schema_fingerprint: [u8; 32],
    pub(crate) branch_key: Option<BranchKey>,
}

/// Which cluster nodes currently own and replicate one runtime state. Ownership moves while the
/// state itself lives on, so a replicated state keeps its roles as rebindable configuration rather
/// than as a construction-time constant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StateReplicationRoles {
    pub(crate) primary_node: Option<ClusterNodeName>,
    pub(crate) replica_nodes: Vec<ClusterNodeName>,
    pub(crate) required_replica_acks: usize,
}

impl StateReplicationRoles {
    pub(crate) fn new(
        primary_node: Option<ClusterNodeName>,
        replica_nodes: Vec<ClusterNodeName>,
        required_replica_acks: usize,
    ) -> Self {
        Self {
            primary_node,
            replica_nodes,
            required_replica_acks,
        }
    }

    pub(crate) fn owned_by(primary_node: Option<ClusterNodeName>) -> Self {
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
        key.push(u8::from(self.state));
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
            .verified("concrete state is only built for a branch that has a key")
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

    pub fn purge_domain(&self, domain: &DomainName) -> Result<(), RuntimePersistenceError> {
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
        domain: &DomainName,
        state: RuntimeStateKind,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Result<(), RuntimePersistenceError> {
        let identifier = identifier.into();
        let mut prefix = domain.as_str().as_bytes().to_vec();
        prefix.push(0);
        prefix.push(u8::from(state));
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
        domain: &DomainName,
        current: &HashMap<NodeRef, [u8; 32]>,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let mut domain_prefix = domain.as_str().as_bytes().to_vec();
        domain_prefix.push(0);
        let mut stale_latest_keys = Vec::new();
        for item in self.latest.prefix(domain_prefix) {
            let key = item
                .key()
                .map(|key| key.as_ref().to_vec())
                .map_err(|_| RuntimePersistenceError::ReadValue)?;
            let stored = stored_placement_schema(&key)?;
            let mut expected = current
                .get(&NodeRef {
                    kind: stored.kind,
                    identifier: stored.identifier,
                })
                .copied();
            if expected.is_some()
                && let RuntimeStateKind::BranchAggregated | RuntimeStateKind::KafkaOffset =
                    stored.state
            {
                expected = Some([0; 32]);
            }
            if expected != Some(stored.schema_fingerprint) {
                stale_latest_keys.push(key);
            }
        }
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
            .map_err(|_| Report::new(RuntimePersistenceError::WriteValue))
    }
}

/// The placement a stored runtime-state key encodes: which kind of state it is, which model owns
/// it, and the schema fingerprint the state was written under.
struct StoredPlacementSchema {
    state: RuntimeStateKind,
    kind: ModelKind,
    identifier: ModelName,
    schema_fingerprint: [u8; 32],
}

fn stored_placement_schema(
    key: &[u8],
) -> Result<StoredPlacementSchema, Report<RuntimePersistenceError>> {
    let domain_end = key.iter().position(|byte| *byte == 0).ok_or_else(|| {
        RuntimePersistenceError::DecodeState(
            "runtime state key has no domain separator".to_string(),
        )
    })?;
    let state_offset = domain_end
        .checked_add(1)
        .verified("the separator position is an index into this key");
    let state = key
        .get(state_offset)
        .copied()
        .and_then(RuntimeStateKind::from_repr);
    let Some(state) = state else {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has an invalid state kind".to_string(),
        )));
    };
    let kind_start = state_offset
        .checked_add(2)
        .verified("the state-kind byte position is an index into this key");
    let kind_end = key
        .get(kind_start..)
        .and_then(|rest| rest.iter().position(|byte| *byte == 0));
    let kind_end = kind_end.and_then(|offset| kind_start.checked_add(offset));
    let Some(kind_end) = kind_end else {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has no model-kind separator".to_string(),
        )));
    };
    let kind = std::str::from_utf8(&key[kind_start..kind_end]).map_err(|_| {
        RuntimePersistenceError::DecodeState(
            "runtime state key has an invalid model kind".to_string(),
        )
    })?;
    let kind = ModelKind::from_str(kind).map_err(|_| {
        RuntimePersistenceError::DecodeState(
            "runtime state key has an invalid model kind".to_string(),
        )
    })?;
    let identifier_start = kind_end
        .checked_add(1)
        .verified("the model-kind separator position is an index into this key");
    let identifier_end = key
        .get(identifier_start..)
        .and_then(|rest| rest.iter().position(|byte| *byte == 0));
    let identifier_end = identifier_end.and_then(|offset| identifier_start.checked_add(offset));
    let Some(identifier_end) = identifier_end else {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has no identifier separator".to_string(),
        )));
    };
    let identifier = std::str::from_utf8(&key[identifier_start..identifier_end]).map_err(|_| {
        RuntimePersistenceError::DecodeState(
            "runtime state key has an invalid identifier".to_string(),
        )
    })?;
    let identifier = ModelName::parse(identifier).map_err(|_| {
        RuntimePersistenceError::DecodeState(
            "runtime state key has an invalid identifier".to_string(),
        )
    })?;
    let fingerprint_start = identifier_end
        .checked_add(1)
        .verified("the identifier separator position is an index into this key");
    let fingerprint = fingerprint_start
        .checked_add(32)
        .and_then(|fingerprint_end| key.get(fingerprint_start..fingerprint_end));
    let Some(fingerprint) = fingerprint else {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has a truncated schema fingerprint".to_string(),
        )));
    };
    let mut schema_fingerprint = [0; 32];
    schema_fingerprint.copy_from_slice(fingerprint);
    Ok(StoredPlacementSchema {
        state,
        kind,
        identifier,
        schema_fingerprint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stored key is bytes read back from the database, so decoding one that ends inside the
    /// state-kind prefix must report a decode error rather than index past the key.
    #[test]
    fn truncated_state_key_reports_a_decode_error() {
        let mut key = b"acme".to_vec();
        key.push(0);
        key.push(u8::from(RuntimeStateKind::Deduplicator));

        let error = stored_placement_schema(&key)
            .err()
            .expect("a key that ends inside the state-kind prefix must not decode");

        assert!(
            matches!(error.current_context(), RuntimePersistenceError::DecodeState(message)
                if message.contains("model-kind separator")),
            "unexpected error for a truncated state key: {error:?}"
        );
    }
}
