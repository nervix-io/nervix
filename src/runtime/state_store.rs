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
    pub(crate) branch_key: Option<BranchKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct PersistedRuntimeStateEntry {
    pub lsm: u64,
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
        Ok(Some(PersistedRuntimeStateEntry {
            lsm: archived.lsm.into(),
            payload: archived.payload.as_slice().to_vec(),
        }))
    }
}
