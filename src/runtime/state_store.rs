use std::{collections::BTreeSet, str::FromStr};

use ahash::HashMap;
use error_stack::Report;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use meticulous::OptionExt as _;
pub(crate) use nervix_interconnect::RuntimeStateKind;
use nervix_models::{
    ClusterNodeIncarnation, ClusterNodeName, DomainName, DomainNodeRef, ModelKind, ModelName,
    NodeRef,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;

use super::BranchKey;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RuntimeStatePlacement {
    pub(in crate::runtime) domain: DomainName,
    pub(crate) state: RuntimeStateKind,
    pub(crate) kind: ModelKind,
    pub(crate) identifier: ModelName,
    pub(in crate::runtime) schema_fingerprint: [u8; 32],
    pub(in crate::runtime) branch_key: Option<BranchKey>,
}

/// Which cluster nodes currently own and replicate one runtime state. Ownership moves while the
/// state itself lives on, so a replicated state keeps its roles as rebindable configuration rather
/// than as a construction-time constant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::runtime) struct StateReplicationRoles {
    pub(in crate::runtime) primary_node: Option<ClusterNodeName>,
    pub(in crate::runtime) replica_nodes: BTreeSet<ClusterNodeName>,
    pub(in crate::runtime) required_replica_acks: usize,
}

impl StateReplicationRoles {
    pub(in crate::runtime) fn new(
        primary_node: Option<ClusterNodeName>,
        replica_nodes: Vec<ClusterNodeName>,
        required_replica_acks: usize,
    ) -> Self {
        Self {
            primary_node,
            replica_nodes: replica_nodes.into_iter().collect(),
            required_replica_acks,
        }
    }

    pub(in crate::runtime) fn owned_by(primary_node: Option<ClusterNodeName>) -> Self {
        Self {
            primary_node,
            replica_nodes: BTreeSet::new(),
            required_replica_acks: 0,
        }
    }

    fn local_capability(&self, local_node: Option<&ClusterNodeName>) -> StateCapability {
        if self.primary_node.is_none() && self.replica_nodes.is_empty() {
            return StateCapability::Originate;
        }
        if self.primary_node.as_ref() == local_node {
            return StateCapability::Originate;
        }
        if local_node.is_some_and(|node| self.replica_nodes.contains(node)) {
            return StateCapability::InstallSnapshot;
        }
        StateCapability::Read
    }
}

/// The operation one assignment grants over a shared runtime state. A capability token carries
/// this discriminator as well as its generation, so possessing a generation number alone never
/// authorizes an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub(in crate::runtime) enum StateCapability {
    Read,
    Originate,
    InstallSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct StateAssignmentToken {
    generation: u64,
    capability: StateCapability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct StateAssignmentBinding {
    generation: u64,
    capability: StateCapability,
}

impl StateAssignmentBinding {
    pub(in crate::runtime) fn token_for(
        self,
        capability: StateCapability,
    ) -> Option<StateAssignmentToken> {
        (self.capability == capability).then_some(StateAssignmentToken {
            generation: self.generation,
            capability,
        })
    }

    /// The ownership fence this binding acts under. A capture records it so a snapshot sealed
    /// under a superseded assignment is refused instead of installed.
    pub(in crate::runtime) fn fence(self) -> u64 {
        self.generation
    }
}

#[derive(Debug)]
struct StateAssignment {
    generation: u64,
    roles: StateReplicationRoles,
    local_capability: StateCapability,
}

/// Serializes assignment rebinding with every authoritative mutation and replica installation.
/// The state value outlives individual assignments; short-lived capability handles carry the
/// token returned by `rebind` and must validate it inside this lock at the operation boundary.
#[derive(Debug)]
pub(in crate::runtime) struct StateAssignmentAuthority {
    assignment: parking_lot::Mutex<StateAssignment>,
}

impl Default for StateAssignmentAuthority {
    fn default() -> Self {
        Self {
            assignment: parking_lot::Mutex::new(StateAssignment {
                generation: 0,
                roles: StateReplicationRoles::default(),
                local_capability: StateCapability::Read,
            }),
        }
    }
}

impl StateAssignmentAuthority {
    pub(in crate::runtime) fn rebind(
        &self,
        roles: StateReplicationRoles,
        local_node: Option<&ClusterNodeName>,
    ) -> StateAssignmentBinding {
        let mut assignment = self.assignment.lock();
        assignment.generation = assignment
            .generation
            .checked_add(1)
            .assured("one process cannot apply 2^64 assignments to one runtime state");
        assignment.local_capability = roles.local_capability(local_node);
        assignment.roles = roles;
        StateAssignmentBinding {
            generation: assignment.generation,
            capability: assignment.local_capability,
        }
    }

    pub(in crate::runtime) fn current_binding(&self) -> StateAssignmentBinding {
        let assignment = self.assignment.lock();
        StateAssignmentBinding {
            generation: assignment.generation,
            capability: assignment.local_capability,
        }
    }

    pub(in crate::runtime) fn roles(&self) -> StateReplicationRoles {
        self.assignment.lock().roles.clone()
    }

    pub(in crate::runtime) fn serialize<T>(&self, action: impl FnOnce() -> T) -> T {
        self.serialize_with(|_| action())
    }

    /// Run `action` under the barrier with the assignment in force while it runs.
    ///
    /// A capture that must record which assignment it observed reads the fence in the same
    /// critical section as the contents, so the two cannot describe different moments.
    pub(in crate::runtime) fn serialize_with<T>(
        &self,
        action: impl FnOnce(StateAssignmentBinding) -> T,
    ) -> T {
        let assignment = self.assignment.lock();
        action(StateAssignmentBinding {
            generation: assignment.generation,
            capability: assignment.local_capability,
        })
    }

    pub(in crate::runtime) fn authorize<T>(
        &self,
        token: StateAssignmentToken,
        required: StateCapability,
        action: impl FnOnce() -> T,
    ) -> Result<T, Report<StateAuthorityError>> {
        let assignment = self.assignment.lock();
        if token.generation != assignment.generation
            || token.capability != required
            || assignment.local_capability != required
        {
            return Err(Report::new(StateAuthorityError {
                operation: required,
            }));
        }
        Ok(action())
    }
}

#[derive(Debug, Clone, Copy, Error)]
#[error("runtime state assignment no longer grants {} authority", operation.as_ref())]
pub(in crate::runtime) struct StateAuthorityError {
    operation: StateCapability,
}

#[derive(Debug, Error)]
pub(in crate::runtime) enum RuntimeStateOperationError {
    #[error(transparent)]
    Authority(#[from] StateAuthorityError),
    #[error(transparent)]
    Persistence(#[from] RuntimePersistenceError),
    #[error("runtime state checkpoint failed: {0}")]
    Checkpoint(String),
    #[error("runtime state replication failed: {0}")]
    Replication(String),
}

pub(in crate::runtime) type RuntimeStateResult<T> = Result<T, Report<RuntimeStateOperationError>>;

impl RuntimeStateOperationError {
    pub(in crate::runtime) fn checkpoint(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Checkpoint(reason.into()))
    }

    pub(in crate::runtime) fn replication(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Replication(reason.into()))
    }

    pub(in crate::runtime) fn persistence(error: RuntimePersistenceError) -> Report<Self> {
        Report::new(Self::Persistence(error))
    }
}

impl From<Report<StateAuthorityError>> for RuntimeStateOperationError {
    fn from(error: Report<StateAuthorityError>) -> Self {
        Self::Authority(*error.current_context())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct PersistedRuntimeStateEntry {
    pub(crate) lsm: u64,
    pub(crate) schema_fingerprint: [u8; 32],
    pub(crate) payload: Vec<u8>,
}

impl PersistedRuntimeStateEntry {
    pub(in crate::runtime) fn is_after(&self, after_lsm: Option<u64>) -> bool {
        match after_lsm {
            Some(after_lsm) => self.lsm > after_lsm,
            None => true,
        }
    }
}

#[derive(Debug, Clone, Error)]
pub(crate) enum RuntimePersistenceError {
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
    #[error("prepared ownership handoff state is unavailable")]
    MissingHandoffPreparation,
    #[error("prepared ownership handoff state does not match the acknowledged checkpoint")]
    HandoffPreparationMismatch,
    #[error("local node incarnation is unavailable while activating recovered runtime state")]
    MissingNodeIncarnation,
}

pub(in crate::runtime) struct RuntimeStateStore {
    db: Database,
    latest: Keyspace,
    lsm_index: Keyspace,
    handoff_preparations: Keyspace,
    handoff_activations: Keyspace,
    forced_recovery_preparations: Keyspace,
    forced_recovery_activations: Keyspace,
    replica_installs: parking_lot::Mutex<()>,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
struct StoredHandoffCheckpoint {
    placement: nervix_interconnect::StatePlacementEnvelope,
    snapshot: PersistedRuntimeStateEntry,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
struct StoredHandoffPreparation {
    operation_id: String,
    source: ClusterNodeName,
    destination: ClusterNodeName,
    source_incarnation: ClusterNodeIncarnation,
    destination_incarnation: ClusterNodeIncarnation,
    domain: DomainName,
    kind: ModelKind,
    identifier: ModelName,
    base_schedule_fingerprint: [u8; 32],
    target_schedule_fingerprint: [u8; 32],
    checkpoints: Vec<StoredHandoffCheckpoint>,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
struct StoredForcedRecoveryPreparation {
    operation_id: String,
    source: ClusterNodeName,
    destination: ClusterNodeName,
    destination_incarnation: ClusterNodeIncarnation,
    domain: DomainName,
    kind: ModelKind,
    identifier: ModelName,
    target_schedule_fingerprint: [u8; 32],
    checkpoints: Vec<StoredHandoffCheckpoint>,
}

#[derive(Debug)]
struct PersistedForcedRecoveryPreparation {
    operation_id: String,
    source: ClusterNodeName,
    destination: ClusterNodeName,
    destination_incarnation: ClusterNodeIncarnation,
    domain: DomainName,
    kind: ModelKind,
    identifier: ModelName,
    target_schedule_fingerprint: [u8; 32],
    checkpoints: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
}

impl PersistedForcedRecoveryPreparation {
    fn matches(&self, transition: &ForcedRuntimeStateRecoveryTransition<'_>) -> bool {
        self.operation_id == transition.operation_id
            && self.source == *transition.source
            && self.destination == *transition.destination
            && self.destination_incarnation == transition.destination_incarnation
            && self.domain == transition.entity.domain
            && self.kind == transition.entity.kind()
            && self.identifier == *transition.entity.identifier()
            && self.target_schedule_fingerprint == transition.target_schedule_fingerprint
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct PersistedRuntimeStateHandoffPreparation {
    pub(in crate::runtime) operation_id: String,
    pub(in crate::runtime) source: ClusterNodeName,
    pub(in crate::runtime) destination: ClusterNodeName,
    pub(in crate::runtime) source_incarnation: ClusterNodeIncarnation,
    pub(in crate::runtime) destination_incarnation: ClusterNodeIncarnation,
    pub(in crate::runtime) domain: DomainName,
    pub(in crate::runtime) kind: ModelKind,
    pub(in crate::runtime) identifier: ModelName,
    pub(in crate::runtime) base_schedule_fingerprint: [u8; 32],
    pub(in crate::runtime) target_schedule_fingerprint: [u8; 32],
    pub(in crate::runtime) checkpoints: Vec<(
        nervix_interconnect::StatePlacementEnvelope,
        PersistedRuntimeStateEntry,
    )>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RuntimeStateHandoffTransition<'a> {
    pub(in crate::runtime) operation_id: &'a str,
    pub(in crate::runtime) source: &'a ClusterNodeName,
    pub(in crate::runtime) destination: &'a ClusterNodeName,
    pub(in crate::runtime) source_incarnation: ClusterNodeIncarnation,
    pub(in crate::runtime) destination_incarnation: ClusterNodeIncarnation,
    pub(in crate::runtime) entity: &'a DomainNodeRef,
    pub(in crate::runtime) base_schedule_fingerprint: [u8; 32],
    pub(in crate::runtime) target_schedule_fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ForcedRuntimeStateRecoveryTransition<'a> {
    pub(in crate::runtime) operation_id: &'a str,
    pub(in crate::runtime) source: &'a ClusterNodeName,
    pub(in crate::runtime) destination: &'a ClusterNodeName,
    pub(in crate::runtime) destination_incarnation: ClusterNodeIncarnation,
    pub(in crate::runtime) entity: &'a DomainNodeRef,
    pub(in crate::runtime) target_schedule_fingerprint: [u8; 32],
}

impl RuntimeStatePlacement {
    pub(in crate::runtime) fn as_storage_key(&self) -> Vec<u8> {
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

    pub(in crate::runtime) fn to_remote(&self) -> nervix_interconnect::StatePlacementEnvelope {
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
    pub(in crate::runtime) fn from_database(db: Database) -> Result<Self, RuntimePersistenceError> {
        let latest = db
            .keyspace("runtime_state_latest", KeyspaceCreateOptions::default)
            .map_err(|_| RuntimePersistenceError::OpenKeyspace)?;
        let lsm_index = db
            .keyspace("runtime_state_lsm", KeyspaceCreateOptions::default)
            .map_err(|_| RuntimePersistenceError::OpenKeyspace)?;
        let handoff_preparations = db
            .keyspace(
                "runtime_state_handoff_preparations",
                KeyspaceCreateOptions::default,
            )
            .map_err(|_| RuntimePersistenceError::OpenKeyspace)?;
        let handoff_activations = db
            .keyspace(
                "runtime_state_handoff_activations",
                KeyspaceCreateOptions::default,
            )
            .map_err(|_| RuntimePersistenceError::OpenKeyspace)?;
        let forced_recovery_preparations = db
            .keyspace(
                "runtime_state_forced_recovery_preparations",
                KeyspaceCreateOptions::default,
            )
            .map_err(|_| RuntimePersistenceError::OpenKeyspace)?;
        let forced_recovery_activations = db
            .keyspace(
                "runtime_state_forced_recovery_activations",
                KeyspaceCreateOptions::default,
            )
            .map_err(|_| RuntimePersistenceError::OpenKeyspace)?;
        Ok(Self {
            db,
            latest,
            lsm_index,
            handoff_preparations,
            handoff_activations,
            forced_recovery_preparations,
            forced_recovery_activations,
            replica_installs: parking_lot::Mutex::new(()),
        })
    }

    fn handoff_preparation_key(
        operation_id: &str,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Vec<u8> {
        let mut key = operation_id.as_bytes().to_vec();
        key.push(0);
        key.extend_from_slice(domain.as_str().as_bytes());
        key.push(0);
        key.extend_from_slice(kind.as_str().as_bytes());
        key.push(0);
        key.extend_from_slice(identifier.as_str().as_bytes());
        key
    }

    fn forced_recovery_key(
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Vec<u8> {
        let mut key = domain.as_str().as_bytes().to_vec();
        key.push(0);
        key.extend_from_slice(kind.as_str().as_bytes());
        key.push(0);
        key.extend_from_slice(identifier.as_str().as_bytes());
        key
    }

    fn encode_handoff_preparation(
        transition: &RuntimeStateHandoffTransition<'_>,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> Result<Vec<u8>, Report<RuntimePersistenceError>> {
        let stored = StoredHandoffPreparation {
            operation_id: transition.operation_id.to_string(),
            source: transition.source.clone(),
            destination: transition.destination.clone(),
            source_incarnation: transition.source_incarnation,
            destination_incarnation: transition.destination_incarnation,
            domain: transition.entity.domain.clone(),
            kind: transition.entity.kind(),
            identifier: transition.entity.identifier().clone(),
            base_schedule_fingerprint: transition.base_schedule_fingerprint,
            target_schedule_fingerprint: transition.target_schedule_fingerprint,
            checkpoints: checkpoints
                .iter()
                .map(|(placement, snapshot)| StoredHandoffCheckpoint {
                    placement: placement.to_remote(),
                    snapshot: snapshot.clone(),
                })
                .collect(),
        };
        Ok(rkyv::to_bytes::<rkyv::rancor::Error>(&stored)
            .map(|bytes| bytes.to_vec())
            .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?)
    }

    fn decode_handoff_preparation(
        raw: &[u8],
    ) -> Result<PersistedRuntimeStateHandoffPreparation, Report<RuntimePersistenceError>> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(raw.len());
        aligned.extend_from_slice(raw);
        let stored = rkyv::from_bytes::<StoredHandoffPreparation, rkyv::rancor::Error>(&aligned)
            .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
        Ok(PersistedRuntimeStateHandoffPreparation {
            operation_id: stored.operation_id,
            source: stored.source,
            destination: stored.destination,
            source_incarnation: stored.source_incarnation,
            destination_incarnation: stored.destination_incarnation,
            domain: stored.domain,
            kind: stored.kind,
            identifier: stored.identifier,
            base_schedule_fingerprint: stored.base_schedule_fingerprint,
            target_schedule_fingerprint: stored.target_schedule_fingerprint,
            checkpoints: stored
                .checkpoints
                .into_iter()
                .map(|checkpoint| (checkpoint.placement, checkpoint.snapshot))
                .collect(),
        })
    }

    fn encode_forced_recovery_preparation(
        transition: &ForcedRuntimeStateRecoveryTransition<'_>,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> Result<Vec<u8>, Report<RuntimePersistenceError>> {
        let stored = StoredForcedRecoveryPreparation {
            operation_id: transition.operation_id.to_string(),
            source: transition.source.clone(),
            destination: transition.destination.clone(),
            destination_incarnation: transition.destination_incarnation,
            domain: transition.entity.domain.clone(),
            kind: transition.entity.kind(),
            identifier: transition.entity.identifier().clone(),
            target_schedule_fingerprint: transition.target_schedule_fingerprint,
            checkpoints: checkpoints
                .iter()
                .map(|(placement, snapshot)| StoredHandoffCheckpoint {
                    placement: placement.to_remote(),
                    snapshot: snapshot.clone(),
                })
                .collect(),
        };
        Ok(rkyv::to_bytes::<rkyv::rancor::Error>(&stored)
            .map(|bytes| bytes.to_vec())
            .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?)
    }

    fn decode_forced_recovery_preparation(
        raw: &[u8],
    ) -> Result<PersistedForcedRecoveryPreparation, Report<RuntimePersistenceError>> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(raw.len());
        aligned.extend_from_slice(raw);
        let stored =
            rkyv::from_bytes::<StoredForcedRecoveryPreparation, rkyv::rancor::Error>(&aligned)
                .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
        let checkpoints = stored
            .checkpoints
            .into_iter()
            .map(|checkpoint| {
                RuntimeStatePlacement::from_remote(checkpoint.placement)
                    .map(|placement| (placement, checkpoint.snapshot))
                    .map_err(RuntimePersistenceError::DecodeState)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PersistedForcedRecoveryPreparation {
            operation_id: stored.operation_id,
            source: stored.source,
            destination: stored.destination,
            destination_incarnation: stored.destination_incarnation,
            domain: stored.domain,
            kind: stored.kind,
            identifier: stored.identifier,
            target_schedule_fingerprint: stored.target_schedule_fingerprint,
            checkpoints,
        })
    }

    pub(in crate::runtime) fn persist_handoff_preparation(
        &self,
        transition: &RuntimeStateHandoffTransition<'_>,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let key = Self::handoff_preparation_key(
            transition.operation_id,
            &transition.entity.domain,
            transition.entity.kind(),
            transition.entity.identifier(),
        );
        let encoded = Self::encode_handoff_preparation(transition, checkpoints)?;
        self.handoff_preparations
            .insert(key, encoded)
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        Ok(())
    }

    pub(in crate::runtime) fn persist_forced_recovery_preparation(
        &self,
        transition: &ForcedRuntimeStateRecoveryTransition<'_>,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let key = Self::forced_recovery_key(
            &transition.entity.domain,
            transition.entity.kind(),
            transition.entity.identifier(),
        );
        if let Some(raw) = self
            .forced_recovery_activations
            .get(&key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        {
            let activated = Self::decode_forced_recovery_preparation(raw.as_ref())?;
            if activated.matches(transition) {
                return Ok(());
            }
        }
        let encoded = Self::encode_forced_recovery_preparation(transition, checkpoints)?;
        let mut batch = self.db.batch();
        batch.remove(&self.forced_recovery_activations, key.clone());
        batch.insert(&self.forced_recovery_preparations, key, encoded);
        batch
            .commit()
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        Ok(())
    }

    pub(in crate::runtime) fn activate_forced_recovery(
        &self,
        transition: &ForcedRuntimeStateRecoveryTransition<'_>,
    ) -> Result<
        Option<Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>>,
        Report<RuntimePersistenceError>,
    > {
        let key = Self::forced_recovery_key(
            &transition.entity.domain,
            transition.entity.kind(),
            transition.entity.identifier(),
        );
        if let Some(raw) = self
            .forced_recovery_activations
            .get(&key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        {
            let activated = Self::decode_forced_recovery_preparation(raw.as_ref())?;
            if activated.matches(transition) {
                return Ok(None);
            }
        }

        let prepared = match self
            .forced_recovery_preparations
            .get(&key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        {
            Some(raw) => Some(Self::decode_forced_recovery_preparation(raw.as_ref())?),
            None => None,
        };
        let checkpoints = match prepared {
            Some(prepared) if prepared.matches(transition) => prepared.checkpoints,
            Some(_) | None => Vec::new(),
        };
        self.replace_entity_snapshots(
            &transition.entity.domain,
            transition.entity.kind(),
            transition.entity.identifier(),
            &checkpoints,
        )?;
        let encoded = Self::encode_forced_recovery_preparation(transition, &checkpoints)?;
        let mut batch = self.db.batch();
        batch.remove(&self.forced_recovery_preparations, key.clone());
        batch.insert(&self.forced_recovery_activations, key, encoded);
        batch
            .commit()
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        Ok(Some(checkpoints))
    }

    pub(in crate::runtime) fn discard_handoff_preparation(
        &self,
        operation_id: &str,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let key = Self::handoff_preparation_key(operation_id, domain, kind, identifier);
        let mut batch = self.db.batch();
        batch.remove(&self.handoff_preparations, key.clone());
        batch.remove(&self.handoff_activations, key);
        batch
            .commit()
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        Ok(())
    }

    pub(in crate::runtime) fn handoff_preparations(
        &self,
    ) -> Result<Vec<PersistedRuntimeStateHandoffPreparation>, Report<RuntimePersistenceError>> {
        let preparations = self
            .handoff_preparations
            .iter()
            .map(|item| {
                let value = item
                    .value()
                    .map_err(|_| RuntimePersistenceError::ReadValue)?;
                Self::decode_handoff_preparation(value.as_ref())
            })
            .collect::<Result<Vec<_>, Report<RuntimePersistenceError>>>()?;
        Ok(preparations)
    }

    pub(in crate::runtime) fn handoff_activation(
        &self,
        operation_id: &str,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Result<Option<PersistedRuntimeStateHandoffPreparation>, Report<RuntimePersistenceError>>
    {
        let key = Self::handoff_preparation_key(operation_id, domain, kind, identifier);
        let Some(raw) = self
            .handoff_activations
            .get(key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        else {
            return Ok(None);
        };
        let preparation = Self::decode_handoff_preparation(raw.as_ref())?;
        Ok(Some(preparation))
    }

    pub(in crate::runtime) fn activate_handoff_preparation(
        &self,
        transition: &RuntimeStateHandoffTransition<'_>,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let preparation_key = Self::handoff_preparation_key(
            transition.operation_id,
            &transition.entity.domain,
            transition.entity.kind(),
            transition.entity.identifier(),
        );
        let expected = Self::encode_handoff_preparation(transition, checkpoints)?;
        let stored = self
            .handoff_preparations
            .get(&preparation_key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?
            .ok_or(RuntimePersistenceError::MissingHandoffPreparation)?;
        if stored.as_ref() != expected {
            return Err(Report::new(
                RuntimePersistenceError::HandoffPreparationMismatch,
            ));
        }

        let mut domain_prefix = transition.entity.domain.as_str().as_bytes().to_vec();
        domain_prefix.push(0);
        let latest_keys = self
            .latest
            .prefix(domain_prefix)
            .map(|item| {
                let key = item
                    .key()
                    .map(|key| key.as_ref().to_vec())
                    .map_err(|_| RuntimePersistenceError::ReadValue)?;
                let stored = stored_placement_schema(&key)
                    .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
                Ok((stored.kind == transition.entity.kind()
                    && stored.identifier == *transition.entity.identifier())
                .then_some(key))
            })
            .collect::<Result<Vec<_>, RuntimePersistenceError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
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
        for (placement, snapshot) in checkpoints {
            let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)
                .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
            let placement_key = placement.as_storage_key();
            batch.insert(&self.latest, placement_key.clone(), encoded.to_vec());
            batch.insert(
                &self.lsm_index,
                placement.as_lsm_index_key(snapshot.lsm),
                placement_key,
            );
        }
        batch.remove(&self.handoff_preparations, preparation_key);
        batch.insert(
            &self.handoff_activations,
            Self::handoff_preparation_key(
                transition.operation_id,
                &transition.entity.domain,
                transition.entity.kind(),
                transition.entity.identifier(),
            ),
            expected,
        );
        batch
            .commit()
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        Ok(())
    }

    /// Publish one sealed snapshot generation and make it durable before returning.
    ///
    /// The container and the manifest that names it are one stored value, so a reader either finds
    /// the whole generation or the one before it. Returning only after the durability barrier is
    /// what makes that true across a restart: a generation this call reported as published is on
    /// disk, and one it did not is not referenced by anything.
    pub(in crate::runtime) fn publish_sealed_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        revision: u64,
        payload: &[u8],
    ) -> Result<(), RuntimePersistenceError> {
        self.write_latest_snapshot(placement, revision, payload)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|_| RuntimePersistenceError::WriteValue)
    }

    pub(in crate::runtime) fn persist_latest_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), RuntimePersistenceError> {
        self.write_latest_snapshot(placement, lsm, payload)?;
        self.db
            .persist(PersistMode::Buffer)
            .map_err(|_| RuntimePersistenceError::WriteValue)
    }

    fn write_latest_snapshot(
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
        Ok(())
    }

    pub(in crate::runtime) fn replace_entity_snapshots(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let mut domain_prefix = domain.as_str().as_bytes().to_vec();
        domain_prefix.push(0);
        let latest_keys = self
            .latest
            .prefix(domain_prefix)
            .map(|item| {
                let key = item
                    .key()
                    .map(|key| key.as_ref().to_vec())
                    .map_err(|_| RuntimePersistenceError::ReadValue)?;
                let stored = stored_placement_schema(&key)
                    .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
                Ok((stored.kind == kind && stored.identifier == *identifier).then_some(key))
            })
            .collect::<Result<Vec<_>, RuntimePersistenceError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
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
        for (placement, snapshot) in checkpoints {
            let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)
                .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
            let placement_key = placement.as_storage_key();
            batch.insert(&self.latest, placement_key.clone(), encoded.to_vec());
            batch.insert(
                &self.lsm_index,
                placement.as_lsm_index_key(snapshot.lsm),
                placement_key,
            );
        }
        batch
            .commit()
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
        Ok(())
    }

    pub(in crate::runtime) fn persist_replica_snapshot_if_newer(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: &PersistedRuntimeStateEntry,
    ) -> Result<bool, Report<RuntimePersistenceError>> {
        let _install = self.replica_installs.lock();
        if self
            .latest_snapshot(placement)?
            .is_some_and(|current| current.lsm >= snapshot.lsm)
        {
            return Ok(false);
        }
        self.persist_latest_snapshot(placement, snapshot.lsm, &snapshot.payload)?;
        Ok(true)
    }

    pub(in crate::runtime) fn latest_snapshot(
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

    #[cfg(test)]
    pub(in crate::runtime) fn purge_domain(
        &self,
        domain: &DomainName,
    ) -> Result<(), RuntimePersistenceError> {
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

    pub(in crate::runtime) fn purge_entity(
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

    pub(in crate::runtime) fn purge_stale_schema_fingerprints(
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

    #[test]
    fn forced_recovery_preparation_survives_reopen_and_activation_is_idempotent() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let domain = DomainName::parse("testing").expect("valid domain name");
        let identifier = ModelName::parse("moving_state").expect("valid model name");
        let source = ClusterNodeName::parse("node-1").expect("valid cluster node name");
        let destination = ClusterNodeName::parse("node-2").expect("valid cluster node name");
        let destination_incarnation = ClusterNodeIncarnation::new(42);
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeStateKind::MaterializedRelay,
            kind: ModelKind::Relay,
            identifier: identifier.clone(),
            schema_fingerprint: [7; 32],
            branch_key: None,
        };
        let payload = crate::runtime::empty_sealed_container(placement.schema_fingerprint)
            .expect("an empty materialized generation should seal");
        let prepared = PersistedRuntimeStateEntry {
            lsm: 5,
            schema_fingerprint: placement.schema_fingerprint,
            payload: payload.clone(),
        };
        let operation_id = "handoff-operation";
        let target_schedule_fingerprint = [9; 32];
        let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, identifier.clone());
        let transition = ForcedRuntimeStateRecoveryTransition {
            operation_id,
            source: &source,
            destination: &destination,
            destination_incarnation,
            entity: &entity,
            target_schedule_fingerprint,
        };

        {
            let db = Database::builder(dir.path())
                .open()
                .expect("database should open");
            let store = RuntimeStateStore::from_database(db).expect("state store should open");
            store
                .persist_latest_snapshot(&placement, 4, &payload)
                .expect("earlier state should persist");
            store
                .persist_forced_recovery_preparation(
                    &transition,
                    &[(placement.clone(), prepared.clone())],
                )
                .expect("forced recovery preparation should persist");
        }

        {
            let db = Database::builder(dir.path())
                .open()
                .expect("database should reopen");
            let store = RuntimeStateStore::from_database(db).expect("state store should reopen");
            let activated = store
                .activate_forced_recovery(&transition)
                .expect("persisted forced recovery should activate")
                .expect("the first activation should apply its checkpoint");
            assert_eq!(activated, vec![(placement.clone(), prepared)]);
            store
                .persist_latest_snapshot(&placement, 6, &payload)
                .expect("post-activation state should persist");
        }

        {
            let db = Database::builder(dir.path())
                .open()
                .expect("database should reopen again");
            let store = RuntimeStateStore::from_database(db).expect("state store should reopen");
            let activated = store
                .activate_forced_recovery(&transition)
                .expect("repeated forced recovery activation should be readable");
            assert!(activated.is_none());
            let current = store
                .latest_snapshot(&placement)
                .expect("current state should load")
                .expect("post-activation state should remain");
            assert_eq!(current.lsm, 6);
        }
    }

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
