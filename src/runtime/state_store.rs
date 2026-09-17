#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::{collections::BTreeSet, fmt, str::FromStr, sync::Arc as StdArc};

use ahash::HashMap;
use error_stack::Report;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::sync::{ArcSwap, Guard};
pub(crate) use nervix_interconnect::RuntimeStateKind;
use nervix_models::{
    ClusterNodeIncarnation, ClusterNodeName, CoordinationIdentity, DomainName, DomainNodeRef,
    ModelKind, ModelName, NodeRef,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

impl fmt::Display for RuntimeStatePlacement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let branch_scope = if self.branch_key.is_some() {
            "branch-local"
        } else {
            "unbranched"
        };
        write!(
            formatter,
            "{branch_scope} {} state for {} '{}' in domain '{}'",
            self.state.as_str(),
            self.kind.as_str(),
            self.identifier.as_str(),
            self.domain.as_str()
        )
    }
}

#[derive(Debug, Error)]
#[error(
    "runtime state placement for {state} state of {kind} '{identifier}' in domain '{domain}' \
     carries an invalid branch key",
    state = .state.as_str(),
    kind = .kind.as_str()
)]
pub(crate) struct RuntimeStatePlacementError {
    pub(crate) domain: DomainName,
    pub(crate) state: RuntimeStateKind,
    pub(crate) kind: ModelKind,
    pub(crate) identifier: ModelName,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr, strum::FromRepr)]
#[strum(serialize_all = "snake_case")]
#[repr(u8)]
pub(in crate::runtime) enum StateCapability {
    Read = 0,
    Originate = 1,
    InstallSnapshot = 2,
}

impl StateCapability {
    /// The tag a packed binding stores this capability as, which is its declared discriminant.
    const fn tag(self) -> u8 {
        match self {
            Self::Read => 0,
            Self::Originate => 1,
            Self::InstallSnapshot => 2,
        }
    }
}

/// The low bits of a packed binding that hold its capability tag; the generation fills the rest.
const CAPABILITY_TAG_BITS: u32 = 2;
/// How many tags the capability bits hold, which is the factor a generation is scaled by to leave
/// room for them.
const CAPABILITY_TAG_SLOTS: u64 = 1 << CAPABILITY_TAG_BITS;
/// The bits of a packed binding that hold its capability tag.
const CAPABILITY_TAG_MASK: u8 = (1 << CAPABILITY_TAG_BITS) - 1;

// A packed binding is read back through the declared discriminant, so every tag must be that
// discriminant and fit in the capability bits. The unassigned binding packs to zero because it is
// generation zero with the read tag.
const _: () = {
    assert!(matches!(
        StateCapability::from_repr(StateCapability::Read.tag()),
        Some(StateCapability::Read)
    ));
    assert!(matches!(
        StateCapability::from_repr(StateCapability::Originate.tag()),
        Some(StateCapability::Originate)
    ));
    assert!(matches!(
        StateCapability::from_repr(StateCapability::InstallSnapshot.tag()),
        Some(StateCapability::InstallSnapshot)
    ));
    assert!(StateCapability::Read.tag() <= CAPABILITY_TAG_MASK);
    assert!(StateCapability::Originate.tag() <= CAPABILITY_TAG_MASK);
    assert!(StateCapability::InstallSnapshot.tag() <= CAPABILITY_TAG_MASK);
    assert!(StateCapability::Read.tag() == 0);
};

/// How many times a rebind spins on the operations it waits for before it yields its thread.
const ADMISSION_SPINS_BEFORE_YIELD: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct StateAssignmentToken {
    binding: StateAssignmentBinding,
}

/// One assignment of a runtime state: its generation and the capability it grants this node, packed
/// into the single word the authority publishes, so the two are always read together.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct StateAssignmentBinding {
    packed: u64,
}

impl StateAssignmentBinding {
    /// The binding before a state is first assigned: generation zero, granting only reads.
    const UNASSIGNED: Self = Self { packed: 0 };

    pub(in crate::runtime) fn token_for(
        self,
        capability: StateCapability,
    ) -> Option<StateAssignmentToken> {
        self.grants(capability)
            .then_some(StateAssignmentToken { binding: self })
    }

    /// The ownership fence this binding acts under. A capture records it so a snapshot sealed
    /// under a superseded assignment is refused instead of installed.
    pub(in crate::runtime) fn fence(self) -> u64 {
        self.generation()
    }

    fn generation(self) -> u64 {
        self.packed >> CAPABILITY_TAG_BITS
    }

    fn capability(self) -> StateCapability {
        let tag = u8::try_from(self.packed & u64::from(CAPABILITY_TAG_MASK))
            .assured("the capability mask keeps a tag within one byte");
        StateCapability::from_repr(tag)
            .assured("a binding is only ever packed from a declared capability")
    }

    fn grants(self, capability: StateCapability) -> bool {
        (self.packed & u64::from(CAPABILITY_TAG_MASK)) == u64::from(capability.tag())
    }

    /// The binding that supersedes this one, granting `capability`.
    fn successor(self, capability: StateCapability) -> Self {
        let generation = self
            .generation()
            .checked_add(1)
            .assured("a packed generation is below 2^62, so the next one fits in a u64");
        let scaled = generation
            .checked_mul(CAPABILITY_TAG_SLOTS)
            .assured("one process cannot apply 2^62 assignments to one runtime state");
        Self {
            packed: scaled | u64::from(capability.tag()),
        }
    }
}

impl std::fmt::Debug for StateAssignmentBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StateAssignmentBinding")
            .field("generation", &self.generation())
            .field("capability", &self.capability())
            .finish()
    }
}

/// Operations admitted under an assignment that have not finished, counted separately for even and
/// odd generations.
///
/// A rebind waits only for the generation it supersedes. The generation it publishes has the other
/// parity, so operations admitted under the new assignment never delay it.
#[derive(Debug, Default)]
struct StateAdmissions {
    even_generation: AtomicUsize,
    odd_generation: AtomicUsize,
}

impl StateAdmissions {
    fn of_generation(&self, generation: u64) -> &AtomicUsize {
        if generation.is_multiple_of(2) {
            &self.even_generation
        } else {
            &self.odd_generation
        }
    }

    fn admit(&self, generation: u64) -> StateAdmission<'_> {
        let admitted = self.of_generation(generation);
        admitted
            .fetch_add(1, Ordering::SeqCst)
            .checked_add(1)
            .assured(
                "every admitted operation occupies a running stack frame, so fewer than \
                 usize::MAX are admitted at once",
            );
        StateAdmission { admitted }
    }

    /// Wait until every operation admitted under `generation` has finished.
    ///
    /// Admitted operations are synchronous and never take the barrier, so the wait lasts only as
    /// long as the operations already running. The spin yields through the execution crate, whose
    /// yield a deterministic scheduler sees, so that scheduler runs the operations this waits for
    /// instead of the spin.
    fn wait_until_finished(&self, generation: u64) {
        let admitted = self.of_generation(generation);
        let mut spins = 0_u32;
        while admitted.load(Ordering::SeqCst) != 0 {
            if spins < ADMISSION_SPINS_BEFORE_YIELD {
                spins = spins
                    .checked_add(1)
                    .verified("the loop only spins while below ADMISSION_SPINS_BEFORE_YIELD");
                std::hint::spin_loop();
            } else {
                nervix_execution::sync::yield_now();
            }
        }
    }
}

/// One admitted operation. Releasing it on drop keeps the count exact when the operation panics, so
/// a rebind never waits for an operation that already unwound.
struct StateAdmission<'a> {
    admitted: &'a AtomicUsize,
}

impl Drop for StateAdmission<'_> {
    fn drop(&mut self) {
        self.admitted
            .fetch_sub(1, Ordering::SeqCst)
            .checked_sub(1)
            .verified("an admission releases only the count it added");
    }
}

/// Fences every operation over one runtime state to the assignment it was granted under.
///
/// The assignment in force is one packed word and its replication roles are a published snapshot,
/// so checking a capability is a load and a compare and neither is ever read under a lock. The
/// state value outlives individual assignments; short-lived capability handles carry the token
/// returned by `rebind`.
///
/// Operations that replace or restructure the whole state, and captures that must describe one
/// consistent generation, serialize on the barrier. A per-message operation never takes it: it is
/// admitted instead, counting itself in before it compares the binding and out when it finishes,
/// while `rebind` publishes the new binding under the barrier and then waits for the operations
/// admitted under the one it replaced. An admitted operation therefore either observes the new
/// binding and is refused, or finishes before `rebind` returns.
#[derive(Debug)]
pub(in crate::runtime) struct StateAssignmentAuthority {
    binding: AtomicU64,
    roles: ArcSwap<StateReplicationRoles>,
    admissions: StateAdmissions,
    barrier: parking_lot::Mutex<()>,
}

impl Default for StateAssignmentAuthority {
    fn default() -> Self {
        Self {
            binding: AtomicU64::new(StateAssignmentBinding::UNASSIGNED.packed),
            roles: ArcSwap::from_pointee(StateReplicationRoles::default()),
            admissions: StateAdmissions::default(),
            barrier: parking_lot::Mutex::new(()),
        }
    }
}

impl StateAssignmentAuthority {
    pub(in crate::runtime) fn rebind(
        &self,
        roles: StateReplicationRoles,
        local_node: Option<&ClusterNodeName>,
    ) -> StateAssignmentBinding {
        let _barrier = self.barrier.lock();
        let superseded = self.current_binding();
        let binding = superseded.successor(roles.local_capability(local_node));
        self.roles.store(StdArc::new(roles));
        self.binding.store(binding.packed, Ordering::SeqCst);
        self.admissions.wait_until_finished(superseded.generation());
        binding
    }

    pub(in crate::runtime) fn current_binding(&self) -> StateAssignmentBinding {
        StateAssignmentBinding {
            packed: self.binding.load(Ordering::SeqCst),
        }
    }

    /// The replication roles of the assignment in force, as `rebind` last published them.
    pub(in crate::runtime) fn roles(&self) -> Guard<StdArc<StateReplicationRoles>> {
        self.roles.load()
    }

    pub(in crate::runtime) fn serialize<T>(&self, action: impl FnOnce() -> T) -> T {
        self.serialize_with(|_| action())
    }

    /// Run `action` under the barrier with the assignment in force while it runs.
    ///
    /// A capture that must record which assignment it observed reads the fence in the same
    /// critical section as the contents, so the two cannot describe different moments: `rebind`
    /// publishes under the same barrier and keeps it until every operation admitted under the
    /// assignment it replaced has finished.
    pub(in crate::runtime) fn serialize_with<T>(
        &self,
        action: impl FnOnce(StateAssignmentBinding) -> T,
    ) -> T {
        let _barrier = self.barrier.lock();
        action(self.current_binding())
    }

    /// Run a per-message operation under the assignment `token` was granted, without the barrier.
    ///
    /// The operation is counted as admitted before the binding is compared, which is what lets
    /// `rebind` wait for it, so `action` must not rebind this authority. Operations admitted under
    /// the same assignment run beside each other and beside exclusive operations and captures.
    pub(in crate::runtime) fn authorize<T>(
        &self,
        token: StateAssignmentToken,
        required: StateCapability,
        action: impl FnOnce() -> T,
    ) -> Result<T, Report<StateAuthorityError>> {
        if !token.binding.grants(required) {
            return Err(Report::new(StateAuthorityError {
                operation: required,
            }));
        }
        let _admission = self.admissions.admit(token.binding.generation());
        if self.binding.load(Ordering::SeqCst) != token.binding.packed {
            return Err(Report::new(StateAuthorityError {
                operation: required,
            }));
        }
        Ok(action())
    }

    /// Run an operation that replaces or restructures the whole state under the barrier.
    ///
    /// It excludes `rebind`, captures and every other exclusive operation, and no operation
    /// admitted under an earlier assignment is still running while it holds the barrier.
    pub(in crate::runtime) fn authorize_exclusive<T>(
        &self,
        token: StateAssignmentToken,
        required: StateCapability,
        action: impl FnOnce() -> T,
    ) -> Result<T, Report<StateAuthorityError>> {
        let _barrier = self.barrier.lock();
        if !token.binding.grants(required)
            || self.binding.load(Ordering::SeqCst) != token.binding.packed
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
    #[error("prepared forced ownership recovery state is unavailable")]
    MissingForcedRecoveryPreparation,
    #[error(
        "prepared forced ownership recovery state does not match the active process and schedule"
    )]
    ForcedRecoveryPreparationMismatch,
    #[error("forced ownership recovery decision does not match the scheduled entity state")]
    InvalidForcedRecoveryDecision,
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
    forced_recovery_completions: Keyspace,
    replica_installs: parking_lot::Mutex<()>,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
struct StoredHandoffCheckpoint {
    placement: nervix_interconnect::StatePlacementEnvelope,
    snapshot: PersistedRuntimeStateEntry,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
struct StoredHandoffPreparation {
    coordination: CoordinationIdentity,
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
    recovery: ForcedRuntimeStateRecoveryIdentity,
    destination_incarnation: ClusterNodeIncarnation,
    target_schedule_fingerprint: [u8; 32],
    checkpoints: Vec<StoredHandoffCheckpoint>,
}

#[derive(Debug)]
struct PersistedForcedRecoveryPreparation {
    recovery: ForcedRuntimeStateRecoveryIdentity,
    destination_incarnation: ClusterNodeIncarnation,
    target_schedule_fingerprint: [u8; 32],
    checkpoints: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
}

impl PersistedForcedRecoveryPreparation {
    fn accepts(&self, transition: &ForcedRuntimeStateRecoveryTransition<'_>) -> bool {
        self.recovery.matches(transition)
            && self.destination_incarnation == transition.destination_incarnation
            && self.target_schedule_fingerprint == transition.target_schedule_fingerprint
    }
}

/// The durable identity of one forced ownership recovery. Process incarnation and complete-schedule
/// fingerprint fence the preparation that may be activated, but neither identifies whether this
/// ownership transition already completed.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(in crate::runtime) struct ForcedRuntimeStateRecoveryIdentity {
    operation_id: String,
    source: ClusterNodeName,
    destination: ClusterNodeName,
    domain: DomainName,
    entity: NodeRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ForcedRuntimeStateRecoveryAuthorization {
    PreparedCheckpoints,
    RecreateState,
}

impl ForcedRuntimeStateRecoveryAuthorization {
    pub(in crate::runtime) fn recreates_without_preparation(self) -> bool {
        self == Self::RecreateState
    }
}

impl ForcedRuntimeStateRecoveryIdentity {
    pub(in crate::runtime) fn matches(
        &self,
        transition: &ForcedRuntimeStateRecoveryTransition<'_>,
    ) -> bool {
        self.operation_id == transition.operation_id
            && self.source == *transition.source
            && self.destination == *transition.destination
            && self.domain == transition.entity.domain
            && self.entity == transition.entity.node
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct PersistedRuntimeStateHandoffPreparation {
    pub(in crate::runtime) coordination: CoordinationIdentity,
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
    pub(in crate::runtime) coordination: &'a CoordinationIdentity,
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

impl ForcedRuntimeStateRecoveryTransition<'_> {
    pub(in crate::runtime) fn identity(&self) -> ForcedRuntimeStateRecoveryIdentity {
        ForcedRuntimeStateRecoveryIdentity {
            operation_id: self.operation_id.to_string(),
            source: self.source.clone(),
            destination: self.destination.clone(),
            domain: self.entity.domain.clone(),
            entity: self.entity.node.clone(),
        }
    }
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
    ) -> error_stack::Result<Self, RuntimeStatePlacementError> {
        let branch_key = BranchKey::from_remote_key(placement.branch_key).map_err(|reason| {
            Report::new(RuntimeStatePlacementError {
                domain: placement.domain.clone(),
                state: placement.state,
                kind: placement.kind,
                identifier: placement.identifier.clone(),
            })
            .attach_printable(reason)
        })?;
        Ok(Self {
            domain: placement.domain,
            state: placement.state,
            kind: placement.kind,
            identifier: placement.identifier,
            schema_fingerprint: placement.schema_fingerprint,
            branch_key,
        })
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
        let forced_recovery_completions = db
            .keyspace(
                "runtime_state_forced_recovery_completions",
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
            forced_recovery_completions,
            replica_installs: parking_lot::Mutex::new(()),
        })
    }

    fn handoff_preparation_key(
        coordination: &CoordinationIdentity,
        operation_id: &str,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Vec<u8> {
        let mut key = coordination.coordinator().as_str().as_bytes().to_vec();
        key.push(0);
        key.extend_from_slice(&coordination.process_epoch().to_be_bytes());
        key.extend_from_slice(&coordination.sequence().to_be_bytes());
        key.push(0);
        key.extend_from_slice(operation_id.as_bytes());
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
            coordination: transition.coordination.clone(),
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
            coordination: stored.coordination,
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
            recovery: transition.identity(),
            destination_incarnation: transition.destination_incarnation,
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
                    .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PersistedForcedRecoveryPreparation {
            recovery: stored.recovery,
            destination_incarnation: stored.destination_incarnation,
            target_schedule_fingerprint: stored.target_schedule_fingerprint,
            checkpoints,
        })
    }

    fn encode_forced_recovery_completion(
        transition: &ForcedRuntimeStateRecoveryTransition<'_>,
    ) -> Result<Vec<u8>, Report<RuntimePersistenceError>> {
        Ok(
            rkyv::to_bytes::<rkyv::rancor::Error>(&transition.identity())
                .map(|bytes| bytes.to_vec())
                .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?,
        )
    }

    fn decode_forced_recovery_completion(
        raw: &[u8],
    ) -> Result<ForcedRuntimeStateRecoveryIdentity, Report<RuntimePersistenceError>> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(raw.len());
        aligned.extend_from_slice(raw);
        rkyv::from_bytes::<ForcedRuntimeStateRecoveryIdentity, rkyv::rancor::Error>(&aligned)
            .map_err(|error| Report::new(RuntimePersistenceError::DecodeState(error.to_string())))
    }

    pub(in crate::runtime) fn persist_handoff_preparation(
        &self,
        transition: &RuntimeStateHandoffTransition<'_>,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let key = Self::handoff_preparation_key(
            transition.coordination,
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

    pub(in crate::runtime) fn replace_handoff_preparation(
        &self,
        replaced: &RuntimeStateHandoffTransition<'_>,
        replacement: &RuntimeStateHandoffTransition<'_>,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let replaced_key = Self::handoff_preparation_key(
            replaced.coordination,
            replaced.operation_id,
            &replaced.entity.domain,
            replaced.entity.kind(),
            replaced.entity.identifier(),
        );
        let replacement_key = Self::handoff_preparation_key(
            replacement.coordination,
            replacement.operation_id,
            &replacement.entity.domain,
            replacement.entity.kind(),
            replacement.entity.identifier(),
        );
        let encoded = Self::encode_handoff_preparation(replacement, checkpoints)?;
        let mut batch = self.db.batch();
        batch.remove(&self.handoff_preparations, replaced_key.clone());
        batch.remove(&self.handoff_activations, replaced_key);
        batch.insert(&self.handoff_preparations, replacement_key, encoded);
        batch
            .commit()
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
            .forced_recovery_completions
            .get(&key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        {
            let completed = Self::decode_forced_recovery_completion(raw.as_ref())?;
            if completed.matches(transition) {
                return Ok(());
            }
        }
        let encoded = Self::encode_forced_recovery_preparation(transition, checkpoints)?;
        let mut batch = self.db.batch();
        batch.remove(&self.forced_recovery_completions, key.clone());
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
        authorization: ForcedRuntimeStateRecoveryAuthorization,
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
            .forced_recovery_completions
            .get(&key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        {
            let completed = Self::decode_forced_recovery_completion(raw.as_ref())?;
            if completed.matches(transition) {
                return Ok(None);
            }
        }

        let prepared = self
            .forced_recovery_preparations
            .get(&key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?;
        let checkpoints = match prepared {
            Some(prepared) => {
                let prepared = Self::decode_forced_recovery_preparation(prepared.as_ref())?;
                if prepared.accepts(transition) {
                    prepared.checkpoints
                } else if authorization.recreates_without_preparation() {
                    Vec::new()
                } else {
                    return Err(Report::new(
                        RuntimePersistenceError::ForcedRecoveryPreparationMismatch,
                    ));
                }
            }
            None if authorization.recreates_without_preparation() => Vec::new(),
            None => {
                return Err(Report::new(
                    RuntimePersistenceError::MissingForcedRecoveryPreparation,
                ));
            }
        };
        self.replace_entity_snapshots(
            &transition.entity.domain,
            transition.entity.kind(),
            transition.entity.identifier(),
            &checkpoints,
        )?;
        let encoded = Self::encode_forced_recovery_completion(transition)?;
        let mut batch = self.db.batch();
        batch.remove(&self.forced_recovery_preparations, key.clone());
        batch.insert(&self.forced_recovery_completions, key, encoded);
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
        coordination: &CoordinationIdentity,
        operation_id: &str,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let key =
            Self::handoff_preparation_key(coordination, operation_id, domain, kind, identifier);
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
        coordination: &CoordinationIdentity,
        operation_id: &str,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Result<Option<PersistedRuntimeStateHandoffPreparation>, Report<RuntimePersistenceError>>
    {
        let key =
            Self::handoff_preparation_key(coordination, operation_id, domain, kind, identifier);
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
            transition.coordination,
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
                transition.coordination,
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

    fn replace_entity_snapshots(
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
    fn ownership_handoff_preparation_retains_exact_coordination_identity_across_reopen() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let domain = DomainName::parse("testing").expect("valid domain name");
        let identifier = ModelName::parse("moving_state").expect("valid model name");
        let coordinator = ClusterNodeName::parse("leader-a").expect("valid coordinator name");
        let source = ClusterNodeName::parse("node-1").expect("valid cluster node name");
        let destination = ClusterNodeName::parse("node-2").expect("valid cluster node name");
        let coordination = CoordinationIdentity::new(coordinator.clone(), 22, 1);
        let prior_incarnation = CoordinationIdentity::new(coordinator, 21, 1);
        let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, identifier.clone());
        let operation_id = "handoff-operation";
        let transition = RuntimeStateHandoffTransition {
            coordination: &coordination,
            operation_id,
            source: &source,
            destination: &destination,
            source_incarnation: ClusterNodeIncarnation::new(31),
            destination_incarnation: ClusterNodeIncarnation::new(32),
            entity: &entity,
            base_schedule_fingerprint: [4; 32],
            target_schedule_fingerprint: [5; 32],
        };

        {
            let db = Database::builder(dir.path())
                .open()
                .expect("database should open");
            let store = RuntimeStateStore::from_database(db).expect("state store should open");
            store
                .persist_handoff_preparation(&transition, &[])
                .expect("ownership handoff preparation should persist");
        }

        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen");
        let store = RuntimeStateStore::from_database(db).expect("state store should reopen");
        let preparations = store
            .handoff_preparations()
            .expect("persisted handoff preparation should load");
        assert_eq!(preparations.len(), 1);
        assert_eq!(preparations[0].coordination, coordination);

        store
            .activate_handoff_preparation(&transition, &[])
            .expect("the exact preparation should activate");
        assert!(
            store
                .handoff_activation(
                    &prior_incarnation,
                    operation_id,
                    &domain,
                    ModelKind::Relay,
                    &identifier,
                )
                .expect("a stale activation lookup should be readable")
                .is_none()
        );
        store
            .discard_handoff_preparation(
                &prior_incarnation,
                operation_id,
                &domain,
                ModelKind::Relay,
                &identifier,
            )
            .expect("stale cleanup should remain idempotent");
        assert!(
            store
                .handoff_activation(
                    &coordination,
                    operation_id,
                    &domain,
                    ModelKind::Relay,
                    &identifier,
                )
                .expect("the exact activation should remain readable")
                .is_some()
        );
        store
            .discard_handoff_preparation(
                &coordination,
                operation_id,
                &domain,
                ModelKind::Relay,
                &identifier,
            )
            .expect("the exact coordination identity should discard its activation");
        assert!(
            store
                .handoff_activation(
                    &coordination,
                    operation_id,
                    &domain,
                    ModelKind::Relay,
                    &identifier,
                )
                .expect("discarded activation lookup should be readable")
                .is_none()
        );
    }

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
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                )
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
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                )
                .expect("repeated forced recovery activation should be readable");
            assert!(activated.is_none());
            let current = store
                .latest_snapshot(&placement)
                .expect("current state should load")
                .expect("post-activation state should remain");
            assert_eq!(current.lsm, 6);
        }
    }

    #[test]
    fn forced_recovery_preserves_checkpoint_after_incarnation_change() {
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
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                )
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
            let transition = ForcedRuntimeStateRecoveryTransition {
                destination_incarnation: ClusterNodeIncarnation::new(43),
                ..transition
            };
            store
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                )
                .expect("repeated forced recovery activation should be readable");
            let current = store
                .latest_snapshot(&placement)
                .expect("current state should load")
                .expect("post-activation state should remain");
            assert_eq!(current.lsm, 6);
        }
    }

    #[test]
    fn forced_recovery_preserves_checkpoint_after_fingerprint_change() {
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
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                )
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
            let transition = ForcedRuntimeStateRecoveryTransition {
                target_schedule_fingerprint: [10; 32],
                ..transition
            };
            store
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                )
                .expect("repeated forced recovery activation should be readable");
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

    #[test]
    fn an_operation_under_a_replaced_assignment_is_refused() {
        let authority = StateAssignmentAuthority::default();
        let replaced = authority
            .rebind(StateReplicationRoles::owned_by(None), None)
            .token_for(StateCapability::Originate)
            .assured("a state without roles is originated locally");
        let current = authority
            .rebind(StateReplicationRoles::owned_by(None), None)
            .token_for(StateCapability::Originate)
            .assured("a state without roles is originated locally");

        assert!(
            authority
                .authorize(replaced, StateCapability::Originate, || ())
                .is_err()
        );
        assert!(
            authority
                .authorize_exclusive(replaced, StateCapability::Originate, || ())
                .is_err()
        );
        assert!(
            authority
                .authorize(current, StateCapability::Originate, || ())
                .is_ok()
        );
        assert!(
            authority
                .authorize_exclusive(current, StateCapability::Originate, || ())
                .is_ok()
        );
        assert!(
            authority
                .authorize(current, StateCapability::InstallSnapshot, || ())
                .is_err()
        );
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::ClusterNodeName;
    use nervix_recovery::Discarded as _;
    use shuttle::{
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            mpsc,
        },
        thread,
    };
    use triomphe::Arc;

    use super::{
        StateAssignmentAuthority, StateAssignmentToken, StateCapability, StateReplicationRoles,
    };
    use crate::shuttle_test::check_interleavings;

    const MODEL_THREAD_JOINS: &str =
        "Shuttle fails the whole execution when a model thread panics, so no join observes one";

    /// Operation threads in the model that rebinds repeatedly.
    const OPERATION_THREADS: usize = 2;
    /// Originating threads in the model that installs snapshots.
    const ORIGINATOR_THREADS: usize = 2;
    /// Operations or captures each operating or capturing thread performs.
    const OPERATIONS_PER_THREAD: usize = 4;
    /// Rebinds after the first assignment. The assignments run from generation one to four, so each
    /// admission counter serves two generations while operations are being admitted.
    const REBINDS: usize = 3;

    /// What the operations and rebinds of one fence model execution observe of each other.
    struct FenceModel {
        authority: StateAssignmentAuthority,
        /// The token of the first assignment. Operations keep submitting it after rebinds supersede
        /// it, so stale admissions reach the counter its generation shares with later ones.
        first: StateAssignmentToken,
        /// The generation published by the latest rebind that has returned.
        returned_generation: AtomicU64,
        /// One slot per operation thread, holding the generation of the operation it is running.
        running_generations: Vec<RunningGeneration>,
    }

    /// The generation one operation thread runs its admitted operation under, as the single atomic
    /// a rebinding thread reads while that operation runs.
    ///
    /// No operation runs under generation zero, which is the unassigned binding and grants none, so
    /// the slot spends zero on running nothing and answers with the generation only while one runs.
    #[derive(Default)]
    struct RunningGeneration(AtomicU64);

    impl RunningGeneration {
        fn enter(&self, generation: u64) {
            self.0.store(generation, Ordering::SeqCst);
        }

        fn leave(&self) {
            self.0.store(0, Ordering::SeqCst);
        }

        /// The generation of the operation running in this slot, while one runs.
        fn running(&self) -> Option<u64> {
            match self.0.load(Ordering::SeqCst) {
                0 => None,
                generation => Some(generation),
            }
        }
    }

    impl FenceModel {
        fn new(operation_threads: usize) -> Self {
            let authority = StateAssignmentAuthority::default();
            let first = authority
                .rebind(StateReplicationRoles::owned_by(None), None)
                .token_for(StateCapability::Originate)
                .assured("a state without roles is originated locally");
            let mut running_generations = Vec::with_capacity(operation_threads);
            for _ in 0..operation_threads {
                running_generations.push(RunningGeneration::default());
            }
            Self {
                authority,
                first,
                returned_generation: AtomicU64::new(first.binding.fence()),
                running_generations,
            }
        }

        /// Submit operations from `operation_thread`, alternating a token for the assignment in
        /// force with the first token.
        fn submit_alternating(&self, operation_thread: usize) {
            for operation in 0..OPERATIONS_PER_THREAD {
                if operation.is_multiple_of(2) {
                    let current = self
                        .authority
                        .current_binding()
                        .token_for(StateCapability::Originate)
                        .assured("every assignment of a state without roles is originated locally");
                    self.submit(operation_thread, current);
                } else {
                    self.submit(operation_thread, self.first);
                }
            }
        }

        /// Submit one operation under `token` from `operation_thread`. It either observes a
        /// superseding binding and never runs, or finishes before a rebind superseding its
        /// generation returns.
        fn submit(&self, operation_thread: usize, token: StateAssignmentToken) {
            let running_generation = self
                .running_generations
                .get(operation_thread)
                .assured("the model holds a slot for every operation thread it spawns");
            let generation = token.binding.fence();
            self.authority
                .authorize(token, StateCapability::Originate, || {
                    running_generation.enter(generation);
                    let returned = self.returned_generation.load(Ordering::SeqCst);
                    assert!(
                        returned <= generation,
                        "an operation admitted under generation {generation} was still running \
                         after the rebind publishing generation {returned} returned"
                    );
                    running_generation.leave();
                })
                .discarded("a refused operation observed a superseding binding and never ran");
        }

        /// Rebind once and assert that no operation admitted under a superseded generation is still
        /// running when the rebind has returned.
        fn rebind(&self) {
            let published = self
                .authority
                .rebind(StateReplicationRoles::owned_by(None), None)
                .fence();
            self.returned_generation.store(published, Ordering::SeqCst);
            for running_generation in &self.running_generations {
                let Some(running) = running_generation.running() else {
                    continue;
                };
                assert!(
                    running >= published,
                    "the rebind publishing generation {published} returned while an operation \
                     admitted under generation {running} was still running"
                );
            }
        }
    }

    /// What the originations, installations and captures of one installation model execution
    /// observe of each other.
    ///
    /// Each of them marks itself running, yields in place of the work it does, and only then looks
    /// for the others, so a scheduler that prefers another thread runs it inside that work.
    struct InstallationModel {
        authority: StateAssignmentAuthority,
        local: ClusterNodeName,
        peer: ClusterNodeName,
        /// The token of the first assignment, which makes the local node the owner.
        first: StateAssignmentToken,
        originations: AtomicUsize,
        installing: AtomicBool,
        capturing: AtomicBool,
    }

    impl InstallationModel {
        /// A model whose first assignment makes the local node the owner.
        fn new() -> Self {
            let local = ClusterNodeName::parse("node-1")
                .assured("the test node name satisfies the cluster-node grammar");
            let peer = ClusterNodeName::parse("node-2")
                .assured("the test node name satisfies the cluster-node grammar");
            let authority = StateAssignmentAuthority::default();
            let first = authority
                .rebind(
                    StateReplicationRoles::owned_by(Some(local.clone())),
                    Some(&local),
                )
                .token_for(StateCapability::Originate)
                .assured("the local node owns a state whose primary it is");
            Self {
                authority,
                local,
                peer,
                first,
                originations: AtomicUsize::new(0),
                installing: AtomicBool::new(false),
                capturing: AtomicBool::new(false),
            }
        }

        /// Originate repeatedly, alternating admitted and exclusive originations, each under the
        /// latest assignment this thread saw make the local node the owner. That assignment may
        /// since have made the local node a replica, as it does for a task still holding an
        /// originator a rebind replaced.
        fn originate_repeatedly(&self) {
            let mut token = self.first;
            for operation in 0..OPERATIONS_PER_THREAD {
                if let Some(current) = self
                    .authority
                    .current_binding()
                    .token_for(StateCapability::Originate)
                {
                    token = current;
                }
                self.originate(token, !operation.is_multiple_of(2));
            }
        }

        /// Originate under `token`, admitted beside other operations or exclusively.
        fn originate(&self, token: StateAssignmentToken, exclusive: bool) {
            let origination = || {
                self.originations.fetch_add(1, Ordering::SeqCst);
                thread::yield_now();
                assert!(
                    !self.installing.load(Ordering::SeqCst),
                    "an origination ran while a snapshot installation replaced the state"
                );
                self.originations.fetch_sub(1, Ordering::SeqCst);
            };
            let outcome = if exclusive {
                self.authority
                    .authorize_exclusive(token, StateCapability::Originate, origination)
            } else {
                self.authority
                    .authorize(token, StateCapability::Originate, origination)
            };
            outcome.discarded("a refused origination observed a superseding binding and never ran");
        }

        /// Install a snapshot under `token`, which a rebind granted the local node as a replica.
        fn install(&self, token: StateAssignmentToken) {
            self.authority
                .authorize_exclusive(token, StateCapability::InstallSnapshot, || {
                    self.installing.store(true, Ordering::SeqCst);
                    thread::yield_now();
                    assert_eq!(
                        self.originations.load(Ordering::SeqCst),
                        0,
                        "a snapshot installation replaced the state while an origination ran"
                    );
                    assert!(
                        !self.capturing.load(Ordering::SeqCst),
                        "a snapshot installation replaced the state while a capture read it"
                    );
                    self.installing.store(false, Ordering::SeqCst);
                })
                .discarded("a refused installation observed a superseding binding and never ran");
        }

        /// Capture under the barrier, which keeps the assignment the capture observed in force
        /// until the capture finishes.
        fn capture(&self) {
            self.authority.serialize_with(|observed| {
                self.capturing.store(true, Ordering::SeqCst);
                thread::yield_now();
                assert!(
                    !self.installing.load(Ordering::SeqCst),
                    "a capture read the state while a snapshot installation replaced it"
                );
                assert_eq!(
                    self.authority.current_binding(),
                    observed,
                    "the assignment changed while a capture held the barrier"
                );
                self.capturing.store(false, Ordering::SeqCst);
            });
        }

        /// Rebind the local node to replicate the state or to own it, returning the installation
        /// token a replica assignment grants.
        fn rebind(&self, replicate: bool) -> Option<StateAssignmentToken> {
            let roles = if replicate {
                StateReplicationRoles::new(Some(self.peer.clone()), vec![self.local.clone()], 1)
            } else {
                StateReplicationRoles::owned_by(Some(self.local.clone()))
            };
            self.authority
                .rebind(roles, Some(&self.local))
                .token_for(StateCapability::InstallSnapshot)
        }
    }

    /// One operation admitted under the first assignment races one rebind.
    fn one_operation_against_one_rebind() {
        let model = Arc::new(FenceModel::new(1));
        let operation = thread::spawn({
            let model = model.clone();
            move || model.submit(0, model.first)
        });
        // A depth-first search runs the lowest-numbered runnable thread first and ignores yields,
        // so the rebind that spins on the operation is spawned after it.
        let rebinding = thread::spawn(move || model.rebind());
        operation.join().assured(MODEL_THREAD_JOINS);
        rebinding.join().assured(MODEL_THREAD_JOINS);
    }

    /// Operations with fresh and stale tokens race three rebinds.
    fn operations_against_three_rebinds() {
        let model = Arc::new(FenceModel::new(OPERATION_THREADS));
        let mut operations = Vec::with_capacity(OPERATION_THREADS);
        for operation_thread in 0..OPERATION_THREADS {
            let model = model.clone();
            operations.push(thread::spawn(move || {
                model.submit_alternating(operation_thread);
            }));
        }
        // Spawned after the operations it spins on, for the depth-first search.
        let rebinding = thread::spawn(move || {
            for _ in 0..REBINDS {
                model.rebind();
            }
        });
        for operation in operations {
            operation.join().assured(MODEL_THREAD_JOINS);
        }
        rebinding.join().assured(MODEL_THREAD_JOINS);
    }

    /// Originations and captures race rebinds that move the local node between owning and
    /// replicating the state, and each replica assignment hands its token to an installing thread,
    /// as binding a replicated state hands its installer to the task that installs snapshots.
    fn installations_originations_and_captures_against_three_rebinds() {
        let model = Arc::new(InstallationModel::new());
        let mut originators = Vec::with_capacity(ORIGINATOR_THREADS);
        for _ in 0..ORIGINATOR_THREADS {
            let model = model.clone();
            originators.push(thread::spawn(move || model.originate_repeatedly()));
        }
        let capturer = thread::spawn({
            let model = model.clone();
            move || {
                for _ in 0..OPERATIONS_PER_THREAD {
                    model.capture();
                }
            }
        });
        let (installations_tx, installations_rx) = mpsc::channel();
        let installer = thread::spawn({
            let model = model.clone();
            move || {
                while let Ok(token) = installations_rx.recv() {
                    model.install(token);
                }
            }
        });
        // Spawned after the originations it spins on, for the depth-first search.
        let rebinding = thread::spawn(move || {
            for rebind in 0..REBINDS {
                let Some(installation) = model.rebind(rebind.is_multiple_of(2)) else {
                    continue;
                };
                installations_tx
                    .send(installation)
                    .assured("the installing thread receives until this sender is dropped");
            }
        });
        for originator in originators {
            originator.join().assured(MODEL_THREAD_JOINS);
        }
        capturer.join().assured(MODEL_THREAD_JOINS);
        rebinding.join().assured(MODEL_THREAD_JOINS);
        installer.join().assured(MODEL_THREAD_JOINS);
    }

    /// A rebind that returned while an operation admitted under the assignment it replaced was
    /// still running would let that stale operation land after the new assignment took over. The
    /// rebind spins until the operation finishes and yields through the execution crate while it
    /// does, so a PCT schedule that ranks the rebind above the operation still runs the operation
    /// rather than spinning until the step bound fails it.
    #[test]
    fn shuttle_a_rebind_yields_until_the_operation_admitted_under_its_replaced_binding_finishes() {
        check_interleavings(one_operation_against_one_rebind);
    }

    /// Rebinds serialize on the barrier, and each waits only for the admission counter of the
    /// generation it supersedes, a counter that generation shares with the generations two apart.
    /// Operations keep submitting the first token too, so stale admissions arrive at the counter a
    /// later rebind waits on, and still no operation admitted under a superseded generation may run
    /// once the rebind superseding it has returned.
    #[test]
    fn shuttle_no_operation_admitted_under_a_superseded_binding_outlives_its_superseding_rebind() {
        check_interleavings(operations_against_three_rebinds);
    }

    /// Installing a snapshot replaces the whole state, so no origination, admitted or exclusive,
    /// and no capture may run beside it, and the assignment a capture observed stays in force for
    /// the whole capture.
    #[test]
    fn shuttle_snapshot_installation_never_overlaps_origination_or_a_capture() {
        check_interleavings(installations_originations_and_captures_against_three_rebinds);
    }
}
