#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::{collections::BTreeSet, fmt, str::FromStr, sync::Arc as StdArc};

use ahash::HashMap;
use error_stack::{Report, ResultExt as _};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{
    Executor, MemoryClass, StorageClass,
    sync::{ArcSwap, Guard},
};
pub(crate) use nervix_interconnect::{RuntimeState, RuntimeStateKind, StateSchema};
use nervix_models::{
    BranchKeyFingerprint, ClusterNodeIncarnation, ClusterNodeName, CoordinationIdentity,
    DomainName, DomainNodeRef, ModelKind, ModelName, NodeRef, SchemaFingerprint,
    WasmStateGeneration, WasmStateGenerations,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use thiserror::Error;
use triomphe::Arc;

use super::{BranchKey, WasmGuestState};

mod durability;

use durability::DurabilityBarrier;

/// The byte that opens the generation segment of a WASM guest state key. A key without it was not
/// written in the current shape and fails to decode instead of addressing the current lifetime.
const STATE_GENERATION_KEY_MARKER: u8 = b'g';

/// The byte that opens the schema fingerprint segment of a schema-bound state key. Every key of
/// schema-bound state carries it and no other key does, so a key without it fails to decode as
/// schema-bound state instead of addressing state laid out by some schema.
const STATE_SCHEMA_KEY_MARKER: u8 = b's';

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RuntimeStatePlacement {
    pub(in crate::runtime) domain: DomainName,
    pub(crate) state: RuntimeState,
    pub(crate) kind: ModelKind,
    pub(crate) identifier: ModelName,
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
            "{branch_scope} {} state",
            self.state.kind().as_str()
        )?;
        if let RuntimeState::WasmProcessor { generation, .. } = self.state {
            write!(formatter, " generation {generation}")?;
        }
        write!(
            formatter,
            " for {} '{}' in domain '{}'",
            self.kind.as_str(),
            self.identifier.as_str(),
            self.domain.as_str()
        )
    }
}

/// What the committed schedule keys one node's runtime state by: the fingerprint of the schemas its
/// schema-bound state is laid out by and, for a WASM processor, the generation of every branch's
/// guest state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct ScheduledStateIdentity {
    pub(in crate::runtime) schema_fingerprint: SchemaFingerprint,
    pub(in crate::runtime) wasm_state_generations: Option<WasmStateGenerations>,
}

impl ScheduledStateIdentity {
    /// Whether `state` for `branch` is the state this identity currently names.
    ///
    /// State that depends on no schema is current for as long as its node is scheduled. Every other
    /// kind is current only under this identity's schema fingerprint, and WASM guest state only in
    /// the generation this identity names for its branch, so a snapshot of an earlier lifetime is
    /// never current, whatever revision it carries.
    pub(in crate::runtime) fn names(
        &self,
        state: RuntimeState,
        branch: Option<&BranchKeyFingerprint>,
    ) -> bool {
        if let StateSchema::Fingerprinted(schema) = state.schema()
            && schema != self.schema_fingerprint
        {
            return false;
        }
        let RuntimeState::WasmProcessor { generation, .. } = state else {
            return true;
        };
        let Some(generations) = self.wasm_state_generations.as_ref() else {
            return false;
        };
        generations.of_branch(branch) == generation
    }

    /// The state of `kind` for `branch` in the lifetime this identity names, or `None` for WASM
    /// guest state when this identity names no guest-state generation.
    pub(in crate::runtime) fn state_of(
        &self,
        kind: RuntimeStateKind,
        branch: Option<&BranchKeyFingerprint>,
    ) -> Option<RuntimeState> {
        let schema = self.schema_fingerprint;
        let state = match kind {
            RuntimeStateKind::BranchAggregated => RuntimeState::BranchAggregated,
            RuntimeStateKind::KafkaOffset => RuntimeState::KafkaOffset,
            RuntimeStateKind::Correlator => RuntimeState::Correlator { schema },
            RuntimeStateKind::Deduplicator => RuntimeState::Deduplicator { schema },
            RuntimeStateKind::MaterializedRelay => RuntimeState::MaterializedRelay { schema },
            RuntimeStateKind::WasmProcessor => {
                let generations = self.wasm_state_generations.as_ref()?;
                RuntimeState::WasmProcessor {
                    schema,
                    generation: generations.of_branch(branch),
                }
            }
            RuntimeStateKind::WindowProcessor => RuntimeState::WindowProcessor { schema },
            RuntimeStateKind::BranchLru => RuntimeState::BranchLru { schema },
        };
        Some(state)
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

/// Why runtime state cannot be placed in the identity the committed schedule publishes for its node.
#[derive(Debug, Error)]
pub(in crate::runtime) enum StateIdentityError {
    #[error(
        "{} '{}' in domain '{}' has no published schema fingerprint",
        .kind.as_str(),
        .identifier.as_str(),
        .domain.as_str()
    )]
    SchemaFingerprintUnpublished {
        domain: DomainName,
        kind: ModelKind,
        identifier: ModelName,
    },
    #[error(
        "{} '{}' in domain '{}' has no published guest-state generation",
        .kind.as_str(),
        .identifier.as_str(),
        .domain.as_str()
    )]
    GenerationUnpublished {
        domain: DomainName,
        kind: ModelKind,
        identifier: ModelName,
    },
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
    #[error(
        "runtime state of {} '{}' cannot be placed in its published identity",
        .kind.as_str(),
        .identifier.as_str()
    )]
    StateIdentity {
        kind: ModelKind,
        identifier: ModelName,
    },
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

/// One checkpoint of the state a placement names. It is stored under that placement and carried
/// beside it, and the placement alone says what the payload is laid out by.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct PersistedRuntimeStateEntry {
    pub(crate) lsm: u64,
    pub(crate) payload: Vec<u8>,
}

impl PersistedRuntimeStateEntry {
    pub(in crate::runtime) fn is_after(&self, after_lsm: Option<u64>) -> bool {
        match after_lsm {
            Some(after_lsm) => self.lsm > after_lsm,
            None => true,
        }
    }

    fn decode(raw: &[u8]) -> error_stack::Result<Self, RuntimePersistenceError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(raw.len());
        aligned.extend_from_slice(raw);
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(&aligned)
            .map_err(|error| Report::new(RuntimePersistenceError::DecodeState(error.to_string())))
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
    #[error("runtime state storage admission failed")]
    StorageAdmission,
    #[error("runtime state storage job did not complete")]
    StorageExecution,
    #[error("failed to synchronize runtime state to stable storage")]
    Synchronize,
}

pub(in crate::runtime) struct RuntimeStateStore {
    db: Database,
    latest: Keyspace,
    lsm_index: Keyspace,
    handoff_preparations: Keyspace,
    handoff_activations: Keyspace,
    forced_recovery_preparations: Keyspace,
    forced_recovery_completions: Keyspace,
    /// Held by every replica installation, which compares with the stored snapshot before it
    /// replaces it. The storage job that installs a replica holds its own handle.
    replica_installs: Arc<parking_lot::Mutex<()>>,
    /// Makes applied writes durable, one synchronization for every writer waiting at once. The
    /// storage job that synchronizes holds its own handle.
    durability: Arc<DurabilityBarrier>,
    /// Runs the writes that must not occupy an async worker.
    executor: Executor,
}

/// The handles one latest-snapshot write needs, cloned into the storage job that performs it.
struct LatestSnapshotWriter {
    db: Database,
    latest: Keyspace,
    lsm_index: Keyspace,
    replica_installs: Arc<parking_lot::Mutex<()>>,
}

impl LatestSnapshotWriter {
    fn write_latest_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        lsm: u64,
        payload: &[u8],
    ) -> error_stack::Result<(), RuntimePersistenceError> {
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
        Ok(())
    }

    fn persist(&self, mode: PersistMode) -> error_stack::Result<(), RuntimePersistenceError> {
        self.db
            .persist(mode)
            .map_err(|_| Report::new(RuntimePersistenceError::WriteValue))
    }

    fn latest_lsm(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> error_stack::Result<Option<u64>, RuntimePersistenceError> {
        let Some(raw) = self
            .latest
            .get(placement.as_storage_key())
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        else {
            return Ok(None);
        };
        Ok(Some(PersistedRuntimeStateEntry::decode(raw.as_ref())?.lsm))
    }

    /// Replace the stored snapshot of `placement` with `snapshot` unless the stored one is as new,
    /// and hand back the snapshot it installed.
    fn install_replica_if_newer(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
    ) -> error_stack::Result<Option<PersistedRuntimeStateEntry>, RuntimePersistenceError> {
        let _install = self.replica_installs.lock();
        if let Some(stored_lsm) = self.latest_lsm(placement)?
            && stored_lsm >= snapshot.lsm
        {
            return Ok(None);
        }
        self.write_latest_snapshot(placement, snapshot.lsm, &snapshot.payload)?;
        Ok(Some(snapshot))
    }
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
        key.push(u8::from(self.state.kind()));
        key.push(0);
        key.extend_from_slice(self.kind.as_str().as_bytes());
        key.push(0);
        key.extend_from_slice(self.identifier.as_str().as_bytes());
        key.push(0);
        if let StateSchema::Fingerprinted(schema) = self.state.schema() {
            key.push(STATE_SCHEMA_KEY_MARKER);
            key.extend_from_slice(schema.as_digest());
            key.push(0);
        }
        if let RuntimeState::WasmProcessor { generation, .. } = self.state {
            key.push(STATE_GENERATION_KEY_MARKER);
            key.extend_from_slice(&u64::from(generation).to_be_bytes());
            key.push(0);
        }
        match self.branch_key.as_ref() {
            Some(branch_key) => {
                key.push(1);
                key.extend_from_slice(branch_key.as_str().as_bytes());
            }
            None => key.push(0),
        }
        key
    }

    /// This placement in the lifetimes a committed schedule publishes for its node. WASM guest
    /// state moves to the generation `generations` names for its branch; every other kind of state
    /// has a single lifetime and keeps its placement.
    pub(in crate::runtime) fn published_in(
        self,
        generations: Option<&WasmStateGenerations>,
    ) -> Self {
        let RuntimeState::WasmProcessor { schema, .. } = self.state else {
            return self;
        };
        let Some(generations) = generations else {
            return self;
        };
        let branch = self.branch_key.as_ref().map(BranchKey::fingerprint);
        Self {
            state: RuntimeState::WasmProcessor {
                schema,
                generation: generations.of_branch(branch.as_ref()),
            },
            ..self
        }
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
            branch_key: BranchKey::to_remote_key(&self.branch_key),
        }
    }

    pub(crate) fn from_remote(
        placement: nervix_interconnect::StatePlacementEnvelope,
    ) -> error_stack::Result<Self, RuntimeStatePlacementError> {
        let branch_key = BranchKey::from_remote_key(placement.branch_key).map_err(|reason| {
            Report::new(RuntimeStatePlacementError {
                domain: placement.domain.clone(),
                state: placement.state.kind(),
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
            branch_key,
        })
    }
}

impl RuntimeStateStore {
    pub(in crate::runtime) fn from_database(
        db: Database,
        executor: Executor,
    ) -> Result<Self, RuntimePersistenceError> {
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
            replica_installs: Arc::new(parking_lot::Mutex::new(())),
            durability: Arc::new(DurabilityBarrier::new()),
            executor,
        })
    }

    fn latest_snapshot_writer(&self) -> LatestSnapshotWriter {
        LatestSnapshotWriter {
            db: self.db.clone(),
            latest: self.latest.clone(),
            lsm_index: self.lsm_index.clone(),
            replica_installs: self.replica_installs.clone(),
        }
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

    /// Activate the forced recovery `transition` names and publish its checkpoints in the lifetimes
    /// `generations` names, which is the committed schedule that accepted the recovery.
    pub(in crate::runtime) fn activate_forced_recovery(
        &self,
        transition: &ForcedRuntimeStateRecoveryTransition<'_>,
        authorization: ForcedRuntimeStateRecoveryAuthorization,
        generations: Option<&WasmStateGenerations>,
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
        let checkpoints = checkpoints
            .into_iter()
            .map(|(placement, snapshot)| (placement.published_in(generations), snapshot))
            .collect::<Vec<_>>();
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
                let stored = stored_placement(&key)
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
        let writer = self.latest_snapshot_writer();
        writer
            .write_latest_snapshot(placement, revision, payload)
            .map_err(|error| error.current_context().clone())?;
        writer
            .persist(PersistMode::SyncAll)
            .map_err(|error| error.current_context().clone())
    }

    pub(in crate::runtime) fn persist_latest_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), RuntimePersistenceError> {
        let writer = self.latest_snapshot_writer();
        writer
            .write_latest_snapshot(placement, lsm, payload)
            .map_err(|error| error.current_context().clone())?;
        writer
            .persist(PersistMode::Buffer)
            .map_err(|error| error.current_context().clone())
    }

    /// Write the guest state a WASM processor branch checkpointed under `placement`, and return
    /// once it is on stable storage.
    ///
    /// A branch checkpoints after every guest callback, so the write runs on the storage workers
    /// instead of the async worker driving the branch, and shares the saved buffer rather than
    /// receiving a copy of it. Its synchronization is shared with every other durable write in
    /// flight on this node.
    pub(in crate::runtime) async fn persist_wasm_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        saved: StdArc<WasmGuestState>,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let writer = self.latest_snapshot_writer();
        let placement = placement.clone();
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(RuntimePersistenceError::StorageAdmission)?;
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, _cancellation| {
                    writer.write_latest_snapshot(&placement, saved.revision(), saved.bytes())
                },
            )
            .await
            .change_context(RuntimePersistenceError::StorageExecution)??;
        self.synchronize().await
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
                let stored = stored_placement(&key)
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

    /// Persist a replicated checkpoint unless the stored one is at least as new, and hand back the
    /// checkpoint when it was written, once it is on stable storage. The comparison and the write
    /// run together on the storage workers; the synchronization is shared with every other durable
    /// write in flight on this node.
    pub(in crate::runtime) async fn persist_replica_snapshot_if_newer(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
    ) -> error_stack::Result<Option<PersistedRuntimeStateEntry>, RuntimePersistenceError> {
        let writer = self.latest_snapshot_writer();
        let placement = placement.clone();
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(RuntimePersistenceError::StorageAdmission)?;
        let installed = self
            .executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, _cancellation| writer.install_replica_if_newer(&placement, snapshot),
            )
            .await
            .change_context(RuntimePersistenceError::StorageExecution)??;
        if installed.is_some() {
            self.synchronize().await?;
        }
        Ok(installed)
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
        let decoded = PersistedRuntimeStateEntry::decode(raw.as_ref())
            .map_err(|error| error.current_context().clone())?;
        Ok(Some(decoded))
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

    /// Remove every stored state of `domain` that `current` no longer names: state of a node the
    /// schedule dropped, schema-bound state written under another schema fingerprint, and WASM guest
    /// state of a generation the committed schedule has moved past. State that depends on no schema
    /// stays for as long as its node is scheduled.
    pub(in crate::runtime) fn purge_stale_state_identities(
        &self,
        domain: &DomainName,
        current: &HashMap<NodeRef, ScheduledStateIdentity>,
    ) -> Result<(), Report<RuntimePersistenceError>> {
        let mut domain_prefix = domain.as_str().as_bytes().to_vec();
        domain_prefix.push(0);
        let mut stale_latest_keys = Vec::new();
        for item in self.latest.prefix(domain_prefix) {
            let key = item
                .key()
                .map(|key| key.as_ref().to_vec())
                .map_err(|_| RuntimePersistenceError::ReadValue)?;
            let stored = stored_placement(&key)?;
            let node = NodeRef {
                kind: stored.kind,
                identifier: stored.identifier,
            };
            let is_current = match current.get(&node) {
                Some(identity) => identity.names(stored.state, stored.branch.as_ref()),
                None => false,
            };
            if !is_current {
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

/// The placement a stored runtime-state key encodes: which state and lifetime it is, including the
/// schema fingerprint of schema-bound state, which model owns it, and the branch it belongs to.
struct StoredPlacement {
    state: RuntimeState,
    kind: ModelKind,
    identifier: ModelName,
    branch: Option<BranchKeyFingerprint>,
}

fn stored_placement(key: &[u8]) -> Result<StoredPlacement, Report<RuntimePersistenceError>> {
    let domain_end = key.iter().position(|byte| *byte == 0).ok_or_else(|| {
        RuntimePersistenceError::DecodeState(
            "runtime state key has no domain separator".to_string(),
        )
    })?;
    let state_offset = domain_end
        .checked_add(1)
        .verified("the separator position is an index into this key");
    let state_kind = key
        .get(state_offset)
        .copied()
        .and_then(RuntimeStateKind::from_repr);
    let Some(state_kind) = state_kind else {
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
    let lifetime_start = identifier_end
        .checked_add(1)
        .verified("the identifier separator position is an index into this key");
    let StoredRuntimeState {
        state,
        branch_start,
    } = stored_runtime_state(key, state_kind, lifetime_start)?;
    let branch = match key.get(branch_start) {
        Some(0) => {
            let scope_end = branch_start
                .checked_add(1)
                .verified("the branch flag read above is inside this key");
            if key.len() != scope_end {
                return Err(Report::new(RuntimePersistenceError::DecodeState(
                    "runtime state key continues after its unbranched scope".to_string(),
                )));
            }
            None
        }
        Some(1) => {
            let text_start = branch_start
                .checked_add(1)
                .verified("the branch flag read above is inside this key");
            let text = std::str::from_utf8(&key[text_start..]).map_err(|_| {
                RuntimePersistenceError::DecodeState(
                    "runtime state key has an invalid branch key".to_string(),
                )
            })?;
            Some(BranchKey::fingerprint_of_canonical_text(text))
        }
        _ => {
            return Err(Report::new(RuntimePersistenceError::DecodeState(
                "runtime state key has an invalid branch scope".to_string(),
            )));
        }
    };
    Ok(StoredPlacement {
        state,
        kind,
        identifier,
        branch,
    })
}

/// The runtime state a stored key names, and where the key's branch scope begins after it.
struct StoredRuntimeState {
    state: RuntimeState,
    branch_start: usize,
}

/// The runtime state of `kind` a key names from `start`: nothing more for state that depends on no
/// schema, the schema fingerprint of every other kind, and after it the generation of WASM guest
/// state.
fn stored_runtime_state(
    key: &[u8],
    kind: RuntimeStateKind,
    start: usize,
) -> Result<StoredRuntimeState, Report<RuntimePersistenceError>> {
    let stored = match kind {
        RuntimeStateKind::BranchAggregated => StoredRuntimeState {
            state: RuntimeState::BranchAggregated,
            branch_start: start,
        },
        RuntimeStateKind::KafkaOffset => StoredRuntimeState {
            state: RuntimeState::KafkaOffset,
            branch_start: start,
        },
        RuntimeStateKind::Correlator => {
            let (schema, branch_start) = stored_schema_fingerprint(key, start)?;
            StoredRuntimeState {
                state: RuntimeState::Correlator { schema },
                branch_start,
            }
        }
        RuntimeStateKind::Deduplicator => {
            let (schema, branch_start) = stored_schema_fingerprint(key, start)?;
            StoredRuntimeState {
                state: RuntimeState::Deduplicator { schema },
                branch_start,
            }
        }
        RuntimeStateKind::MaterializedRelay => {
            let (schema, branch_start) = stored_schema_fingerprint(key, start)?;
            StoredRuntimeState {
                state: RuntimeState::MaterializedRelay { schema },
                branch_start,
            }
        }
        RuntimeStateKind::WasmProcessor => {
            let (schema, generation_start) = stored_schema_fingerprint(key, start)?;
            let (generation, branch_start) = stored_state_generation(key, generation_start)?;
            StoredRuntimeState {
                state: RuntimeState::WasmProcessor { schema, generation },
                branch_start,
            }
        }
        RuntimeStateKind::WindowProcessor => {
            let (schema, branch_start) = stored_schema_fingerprint(key, start)?;
            StoredRuntimeState {
                state: RuntimeState::WindowProcessor { schema },
                branch_start,
            }
        }
        RuntimeStateKind::BranchLru => {
            let (schema, branch_start) = stored_schema_fingerprint(key, start)?;
            StoredRuntimeState {
                state: RuntimeState::BranchLru { schema },
                branch_start,
            }
        }
    };
    Ok(stored)
}

/// The schema fingerprint segment a key of schema-bound state carries at `start`, and where the
/// segment after it begins.
fn stored_schema_fingerprint(
    key: &[u8],
    start: usize,
) -> Result<(SchemaFingerprint, usize), Report<RuntimePersistenceError>> {
    if key.get(start) != Some(&STATE_SCHEMA_KEY_MARKER) {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has no schema fingerprint".to_string(),
        )));
    }
    let fingerprint_start = start
        .checked_add(1)
        .verified("the schema marker read above is inside this key");
    let fingerprint_end = fingerprint_start
        .checked_add(32)
        .assured("a key index is below isize::MAX, so 32 more bytes stay within usize");
    let Some(fingerprint) = key.get(fingerprint_start..fingerprint_end) else {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has a truncated schema fingerprint".to_string(),
        )));
    };
    let fingerprint = <[u8; 32]>::try_from(fingerprint)
        .verified("the fingerprint slice above is exactly 32 bytes long");
    if key.get(fingerprint_end) != Some(&0) {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has no schema fingerprint separator".to_string(),
        )));
    }
    let next = fingerprint_end
        .checked_add(1)
        .verified("the separator read above is inside this key");
    Ok((SchemaFingerprint::from_digest(fingerprint), next))
}

/// The generation segment a WASM guest state key carries at `start`, and where its branch scope
/// begins after it.
fn stored_state_generation(
    key: &[u8],
    start: usize,
) -> Result<(WasmStateGeneration, usize), Report<RuntimePersistenceError>> {
    if key.get(start) != Some(&STATE_GENERATION_KEY_MARKER) {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has no state generation".to_string(),
        )));
    }
    let generation_start = start
        .checked_add(1)
        .verified("the generation marker read above is inside this key");
    let generation_end = generation_start
        .checked_add(8)
        .assured("a key index is below isize::MAX, so eight more bytes stay within usize");
    let Some(generation) = key.get(generation_start..generation_end) else {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has a truncated state generation".to_string(),
        )));
    };
    let generation = <[u8; 8]>::try_from(generation)
        .verified("the generation slice above is exactly eight bytes long");
    let generation =
        WasmStateGeneration::try_from(u64::from_be_bytes(generation)).map_err(|_| {
            RuntimePersistenceError::DecodeState(
                "runtime state key has an invalid state generation".to_string(),
            )
        })?;
    if key.get(generation_end) != Some(&0) {
        return Err(Report::new(RuntimePersistenceError::DecodeState(
            "runtime state key has no state generation separator".to_string(),
        )));
    }
    let branch_start = generation_end
        .checked_add(1)
        .verified("the generation separator read above is inside this key");
    Ok((generation, branch_start))
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should open");
            store
                .persist_handoff_preparation(&transition, &[])
                .expect("ownership handoff preparation should persist");
        }

        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should reopen");
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
            state: RuntimeState::MaterializedRelay {
                schema: SchemaFingerprint::from_digest([7; 32]),
            },
            kind: ModelKind::Relay,
            identifier: identifier.clone(),
            branch_key: None,
        };
        let payload = crate::runtime::empty_sealed_container()
            .expect("an empty materialized generation should seal");
        let prepared = PersistedRuntimeStateEntry {
            lsm: 5,
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should open");
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should reopen");
            let activated = store
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should reopen");
            let activated = store
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
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
            state: RuntimeState::MaterializedRelay {
                schema: SchemaFingerprint::from_digest([7; 32]),
            },
            kind: ModelKind::Relay,
            identifier: identifier.clone(),
            branch_key: None,
        };
        let payload = crate::runtime::empty_sealed_container()
            .expect("an empty materialized generation should seal");
        let prepared = PersistedRuntimeStateEntry {
            lsm: 5,
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should open");
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should reopen");
            let activated = store
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should reopen");
            let transition = ForcedRuntimeStateRecoveryTransition {
                destination_incarnation: ClusterNodeIncarnation::new(43),
                ..transition
            };
            store
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
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
            state: RuntimeState::MaterializedRelay {
                schema: SchemaFingerprint::from_digest([7; 32]),
            },
            kind: ModelKind::Relay,
            identifier: identifier.clone(),
            branch_key: None,
        };
        let payload = crate::runtime::empty_sealed_container()
            .expect("an empty materialized generation should seal");
        let prepared = PersistedRuntimeStateEntry {
            lsm: 5,
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should open");
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should reopen");
            let activated = store
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
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
            let store = RuntimeStateStore::from_database(db, Executor::default())
                .expect("state store should reopen");
            let transition = ForcedRuntimeStateRecoveryTransition {
                target_schedule_fingerprint: [10; 32],
                ..transition
            };
            store
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
                )
                .expect("repeated forced recovery activation should be readable");
            let current = store
                .latest_snapshot(&placement)
                .expect("current state should load")
                .expect("post-activation state should remain");
            assert_eq!(current.lsm, 6);
        }
    }

    fn tenant_branch(tenant: &str) -> BranchKey {
        BranchKey::from_fields([(
            nervix_models::FieldName::parse("tenant").expect("valid field name"),
            crate::runtime_schema::RuntimeValue::String(tenant.to_string()),
        )])
        .expect("a tenant branch key is not empty")
    }

    fn generation(value: u64) -> WasmStateGeneration {
        WasmStateGeneration::try_from(value).expect("test generations are non-zero")
    }

    fn guest_schema() -> SchemaFingerprint {
        SchemaFingerprint::from_digest([4; 32])
    }

    fn wasm_guest_placement(tenant: &str, value: u64) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("testing").expect("valid domain name"),
            state: RuntimeState::WasmProcessor {
                schema: guest_schema(),
                generation: generation(value),
            },
            kind: ModelKind::WasmProcessor,
            identifier: ModelName::parse("counting_guest").expect("valid model name"),
            branch_key: Some(tenant_branch(tenant)),
        }
    }

    pub(super) fn open_store(dir: &tempfile::TempDir) -> RuntimeStateStore {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        RuntimeStateStore::from_database(db, Executor::default()).expect("state store should open")
    }

    fn current_identity(
        generations: WasmStateGenerations,
    ) -> HashMap<NodeRef, ScheduledStateIdentity> {
        HashMap::from_iter([(
            NodeRef::new(
                ModelKind::WasmProcessor,
                ModelName::parse("counting_guest").expect("valid model name"),
            ),
            ScheduledStateIdentity {
                schema_fingerprint: guest_schema(),
                wasm_state_generations: Some(generations),
            },
        )])
    }

    /// A snapshot saved in an earlier generation keeps its own key, so no revision it carries, however
    /// high, can replace or stand in for the guest state of the generation that succeeded it.
    #[test]
    fn an_earlier_generation_never_addresses_the_current_guest_state() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let store = open_store(&dir);
        let earlier = wasm_guest_placement("acme", 1);
        let current = wasm_guest_placement("acme", 2);

        store
            .persist_latest_snapshot(&current, 1, b"current")
            .expect("current guest state should persist");
        store
            .persist_latest_snapshot(&earlier, 9, b"earlier")
            .expect("a late save of the earlier generation writes only its own key");

        let restored = store
            .latest_snapshot(&current)
            .expect("current guest state should load")
            .expect("current guest state should remain");
        assert_eq!(
            (restored.lsm, restored.payload.as_slice()),
            (1, b"current".as_slice())
        );
        let decoded =
            stored_placement(&current.as_storage_key()).expect("a current-shape WASM key decodes");
        assert_eq!(decoded.state, current.state);
        assert_eq!(decoded.branch, Some(tenant_branch("acme").fingerprint()));
    }

    /// Purging a domain against its committed identities removes exactly the guest state of the
    /// generations a transition replaced: a concrete-branch transition leaves every other branch in
    /// place, and a transition of every branch fences all of them at once, including branches that
    /// exist only as persisted state. The purge survives a restart of the store.
    #[test]
    fn purging_removes_only_guest_state_of_superseded_generations() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let mut generations = WasmStateGenerations::first();
        {
            let store = open_store(&dir);
            for (tenant, value) in [("acme", 1), ("beta", 1)] {
                store
                    .persist_latest_snapshot(&wasm_guest_placement(tenant, value), 3, b"state")
                    .expect("first-generation guest state should persist");
            }
            generations.begin_branch(tenant_branch("acme").fingerprint());
            store
                .persist_latest_snapshot(&wasm_guest_placement("acme", 2), 1, b"reset")
                .expect("the transitioned branch saves in its new generation");
            store
                .purge_stale_state_identities(
                    &DomainName::parse("testing").expect("valid domain name"),
                    &current_identity(generations.clone()),
                )
                .expect("stale guest state should purge");
        }

        let store = open_store(&dir);
        let loaded = |tenant: &str, value: u64| {
            store
                .latest_snapshot(&wasm_guest_placement(tenant, value))
                .expect("guest state should load")
                .map(|snapshot| snapshot.lsm)
        };
        assert_eq!(loaded("acme", 1), None);
        assert_eq!(loaded("acme", 2), Some(1));
        assert_eq!(loaded("beta", 1), Some(3));

        let every_branch = generations.begin_every_branch();
        store
            .purge_stale_state_identities(
                &DomainName::parse("testing").expect("valid domain name"),
                &current_identity(generations),
            )
            .expect("stale guest state should purge");
        assert_eq!(every_branch, generation(3));
        assert_eq!(loaded("acme", 2), None);
        assert_eq!(loaded("beta", 1), None);
    }

    #[test]
    fn persisted_runtime_state_decodes_from_unaligned_storage() {
        let expected = PersistedRuntimeStateEntry {
            lsm: 7,
            payload: vec![1, 2, 3],
        };
        let encoded =
            rkyv::to_bytes::<rkyv::rancor::Error>(&expected).expect("runtime state should encode");
        let mut unaligned = vec![0];
        unaligned.extend_from_slice(&encoded);

        let decoded = PersistedRuntimeStateEntry::decode(&unaligned[1..])
            .expect("runtime state should decode from an unaligned database buffer");

        assert_eq!(decoded, expected);
    }

    /// Guest checkpoints and replica installations go through the store's storage workers and
    /// return only once a synchronization covered them. A replica installation hands back the
    /// checkpoint it wrote, and refuses one that is not newer than the checkpoint already stored for
    /// that generation.
    #[tokio::test]
    async fn guest_checkpoints_and_replica_installs_return_once_synchronized() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let store = open_store(&dir);
        let placement = wasm_guest_placement("acme", 1);
        let guest = super::super::ReplicatedWasmProcessorState::new(placement.clone(), None);
        let captured = guest.capture(
            vec![1, 2, 3],
            super::super::WasmCheckpointBoundary::LocalStorage,
        );

        store
            .persist_wasm_checkpoint(&placement, captured.saved())
            .await
            .expect("the guest checkpoint should reach stable storage");
        assert_eq!(store.durability.rounds(), 1);
        let stored = store
            .latest_snapshot(&placement)
            .expect("guest state should load")
            .expect("the checkpointed guest state is stored");
        assert_eq!((stored.lsm, stored.payload), (1, vec![1, 2, 3]));

        let older = PersistedRuntimeStateEntry {
            lsm: 1,
            payload: vec![9],
        };
        assert_eq!(
            store
                .persist_replica_snapshot_if_newer(&placement, older)
                .await
                .expect("the replica installation should run"),
            None
        );
        let newer = PersistedRuntimeStateEntry {
            lsm: 5,
            payload: vec![7],
        };
        let installed = store
            .persist_replica_snapshot_if_newer(&placement, newer.clone())
            .await
            .expect("the replica installation should run");
        assert_eq!(installed, Some(newer));
        assert_eq!(
            store.durability.rounds(),
            2,
            "only the installation that wrote a checkpoint synchronizes"
        );
    }

    /// Activating a forced recovery publishes the checkpoints it staged in the generation the
    /// committed schedule names, and replaying the same recovery publishes nothing again.
    #[test]
    fn forced_recovery_publishes_staged_guest_state_in_the_committed_generation() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let store = open_store(&dir);
        let staged = wasm_guest_placement("acme", 1);
        let snapshot = PersistedRuntimeStateEntry {
            lsm: 6,
            payload: vec![2],
        };
        let entity = DomainNodeRef::node_in(
            staged.domain.clone(),
            ModelKind::WasmProcessor,
            staged.identifier.clone(),
        );
        let source = ClusterNodeName::parse("node-1").expect("valid cluster node name");
        let destination = ClusterNodeName::parse("node-2").expect("valid cluster node name");
        let transition = ForcedRuntimeStateRecoveryTransition {
            operation_id: "owner-replacement",
            source: &source,
            destination: &destination,
            destination_incarnation: ClusterNodeIncarnation::new(7),
            entity: &entity,
            target_schedule_fingerprint: [8; 32],
        };
        store
            .persist_forced_recovery_preparation(&transition, &[(staged.clone(), snapshot.clone())])
            .expect("the recovery preparation should persist");
        let mut committed = WasmStateGenerations::first();
        committed.begin_every_branch();

        let activated = store
            .activate_forced_recovery(
                &transition,
                ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                Some(&committed),
            )
            .expect("the recovery should activate")
            .expect("the first activation publishes its checkpoints");

        let published = wasm_guest_placement("acme", 2);
        assert_eq!(activated, vec![(published.clone(), snapshot)]);
        assert_eq!(
            store
                .latest_snapshot(&published)
                .expect("guest state should load")
                .map(|stored| stored.lsm),
            Some(6)
        );
        assert_eq!(
            store
                .latest_snapshot(&staged)
                .expect("guest state should load"),
            None
        );
        assert_eq!(
            store
                .activate_forced_recovery(
                    &transition,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    Some(&committed),
                )
                .expect("a replayed activation should be readable"),
            None
        );
    }

    /// A stored key is bytes read back from the database, so decoding one that ends inside the
    /// state-kind prefix must report a decode error rather than index past the key.
    #[test]
    fn truncated_state_key_reports_a_decode_error() {
        let mut key = b"acme".to_vec();
        key.push(0);
        key.push(u8::from(RuntimeStateKind::Deduplicator));

        let error = stored_placement(&key)
            .err()
            .expect("a key that ends inside the state-kind prefix must not decode");

        assert!(
            matches!(error.current_context(), RuntimePersistenceError::DecodeState(message)
                if message.contains("model-kind separator")),
            "unexpected error for a truncated state key: {error:?}"
        );
    }

    fn orders_placement(
        state: RuntimeState,
        branch_key: Option<BranchKey>,
    ) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("testing").expect("valid domain name"),
            state,
            kind: ModelKind::Deduplicator,
            identifier: ModelName::parse("orders").expect("valid model name"),
            branch_key,
        }
    }

    /// Every runtime state keeps its whole identity in its storage key: state that depends on no
    /// schema is keyed by its kind alone, schema-bound state also by its schema fingerprint, and
    /// WASM guest state also by its generation, for unbranched execution and a concrete branch.
    #[test]
    fn every_runtime_state_round_trips_through_its_storage_key() {
        let schema = SchemaFingerprint::from_digest([3; 32]);
        let states = [
            RuntimeState::BranchAggregated,
            RuntimeState::KafkaOffset,
            RuntimeState::Correlator { schema },
            RuntimeState::Deduplicator { schema },
            RuntimeState::MaterializedRelay { schema },
            RuntimeState::WasmProcessor {
                schema,
                generation: generation(2),
            },
            RuntimeState::WindowProcessor { schema },
            RuntimeState::BranchLru { schema },
        ];
        for state in states {
            for branch_key in [None, Some(tenant_branch("acme"))] {
                let placement = orders_placement(state, branch_key.clone());

                let decoded = stored_placement(&placement.as_storage_key())
                    .expect("a current-shape runtime state key decodes");

                assert_eq!(decoded.state, state);
                assert_eq!(decoded.kind, placement.kind);
                assert_eq!(decoded.identifier, placement.identifier);
                assert_eq!(
                    decoded.branch,
                    branch_key.as_ref().map(BranchKey::fingerprint)
                );
            }
        }
    }

    /// A schema change starts a new lifetime for schema-bound state and leaves state that depends on
    /// no schema where it is: purging against the new identity removes only the checkpoints written
    /// under the replaced fingerprint, and everything that remains loads again after a restart.
    #[test]
    fn a_schema_change_replaces_only_schema_bound_state() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let replaced = SchemaFingerprint::from_digest([1; 32]);
        let current = SchemaFingerprint::from_digest([2; 32]);
        let independent = [RuntimeState::BranchAggregated, RuntimeState::KafkaOffset];
        let replaced_branch = orders_placement(
            RuntimeState::Deduplicator { schema: replaced },
            Some(tenant_branch("acme")),
        );
        let current_branch = orders_placement(
            RuntimeState::Deduplicator { schema: current },
            Some(tenant_branch("beta")),
        );
        {
            let store = open_store(&dir);
            for state in independent {
                store
                    .persist_latest_snapshot(&orders_placement(state, None), 3, b"kept")
                    .expect("schema-independent state should persist");
            }
            store
                .persist_latest_snapshot(&replaced_branch, 4, b"replaced")
                .expect("state of the replaced schema should persist");
            store
                .persist_latest_snapshot(&current_branch, 5, b"current")
                .expect("state of the current schema should persist");
            store
                .purge_stale_state_identities(
                    &current_branch.domain,
                    &HashMap::from_iter([(
                        NodeRef::new(ModelKind::Deduplicator, current_branch.identifier.clone()),
                        ScheduledStateIdentity {
                            schema_fingerprint: current,
                            wasm_state_generations: None,
                        },
                    )]),
                )
                .expect("state of the replaced schema should purge");
        }

        let store = open_store(&dir);
        let loaded = |placement: &RuntimeStatePlacement| {
            store
                .latest_snapshot(placement)
                .expect("runtime state should load")
        };
        for state in independent {
            assert_eq!(
                loaded(&orders_placement(state, None)),
                Some(PersistedRuntimeStateEntry {
                    lsm: 3,
                    payload: b"kept".to_vec(),
                })
            );
        }
        assert_eq!(loaded(&replaced_branch), None);
        assert_eq!(
            loaded(&current_branch),
            Some(PersistedRuntimeStateEntry {
                lsm: 5,
                payload: b"current".to_vec(),
            })
        );
    }

    /// Schema-bound state is addressed only through its whole fingerprint, so a key that ends
    /// inside the fingerprint reports a decode error instead of naming some other state.
    #[test]
    fn a_key_that_ends_inside_its_schema_fingerprint_does_not_decode() {
        let placement = orders_placement(
            RuntimeState::Deduplicator {
                schema: SchemaFingerprint::from_digest([3; 32]),
            },
            None,
        );
        let mut key = placement.as_storage_key();
        let inside_fingerprint = key
            .len()
            .checked_sub(20)
            .expect("the key is longer than the tail of its fingerprint");
        key.truncate(inside_fingerprint);

        let error = stored_placement(&key)
            .err()
            .expect("a key that ends inside its schema fingerprint must not decode");

        assert!(
            matches!(error.current_context(), RuntimePersistenceError::DecodeState(message)
                if message.contains("truncated schema fingerprint")),
            "unexpected error for a truncated schema fingerprint: {error:?}"
        );
    }

    /// An unbranched key ends with its scope, so bytes after it are refused rather than ignored.
    #[test]
    fn an_unbranched_key_that_continues_after_its_scope_does_not_decode() {
        let mut key = orders_placement(RuntimeState::KafkaOffset, None).as_storage_key();
        key.push(7);

        let error = stored_placement(&key)
            .err()
            .expect("an unbranched key with trailing bytes must not decode");

        assert!(
            matches!(error.current_context(), RuntimePersistenceError::DecodeState(message)
                if message.contains("continues after its unbranched scope")),
            "unexpected error for trailing key bytes: {error:?}"
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
#[path = "state_store_shuttle_tests.rs"]
mod shuttle_tests;
