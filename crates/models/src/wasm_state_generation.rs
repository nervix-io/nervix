//! Durable lifetimes of WASM processor guest state.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The generation that names one lifetime of a WASM processor branch's guest state, and
//!   the rule by which the generations of one processor advance.
//! - **Depends on.** The non-sensitive concrete branch identity the impact vocabulary defines.
//! - **Must not know.** Where a snapshot is stored, how replicas synchronize it, or which operation
//!   publishes a transition.

use std::{collections::BTreeMap, fmt, fmt::Write as _, num::NonZeroU64};

use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;

use crate::{BranchKeyFingerprint, CommandExecutionReference};

/// The branch-local guest state one coordinated reset replaces.
///
/// A branched processor names either one concrete branch or every branch. An unbranched processor
/// uses the explicit `Unbranched` variant so absence never doubles as an all-branches request.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[rkyv(derive(PartialEq, Eq, PartialOrd, Ord))]
pub enum WasmStateResetScope {
    Unbranched,
    Branch(BranchKeyFingerprint),
    AllBranches,
}

impl WasmStateResetScope {
    /// A bounded diagnostic label that does not include a branch value or fingerprint.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Unbranched => "unbranched",
            Self::Branch(_) => "branch",
            Self::AllBranches => "all branches",
        }
    }

    /// Whether this scope selects the supplied concrete-branch fingerprint. `None` is the explicit
    /// unbranched execution.
    pub fn contains(&self, branch: Option<&BranchKeyFingerprint>) -> bool {
        match (self, branch) {
            (Self::Unbranched, None) => true,
            (Self::Branch(selected), Some(branch)) => selected == branch,
            (Self::AllBranches, Some(_)) => true,
            _ => false,
        }
    }

    /// The coordinated reset one recovery attempt of this scope at `generation` drives.
    ///
    /// The reference is derived from the identity alone, so every node that reaches the same
    /// refused lifetime asks for the very same reset. A branch contributes its opaque fingerprint
    /// rather than its key, because a reference is not a place for payload values.
    pub fn recovery_request(&self, generation: WasmStateGeneration) -> CommandExecutionReference {
        let mut selector = String::new();
        match self {
            Self::Unbranched => selector.push_str("unbranched"),
            Self::AllBranches => selector.push_str("all-branches"),
            Self::Branch(branch) => {
                for byte in branch.fingerprint() {
                    write!(selector, "{byte:02x}").assured(
                        "writing to a String cannot fail, and every byte renders as two hex digits",
                    );
                }
            }
        }
        CommandExecutionReference::parse(format!("wasm-recovery.{generation}.{selector}")).assured(
            "a generation renders as decimal digits and a selector as hex or a hyphenated word, \
             which are reference characters, and the longest of them is well under the limit",
        )
    }
}

/// How far the durable reset transition has progressed.
///
/// `Publishing` already names the new generation, but its initial guest checkpoint has not yet
/// reached the storage boundary assigned to the processor. Runtime admission for the scope stays
/// fenced until the same request reaches `Ready`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum WasmStateResetPhase {
    Publishing,
    Ready,
}

/// The operation that requested a coordinated guest-state lifetime change.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum WasmStateResetReason {
    Operator,
    Transaction,
    Guest,
    RejectedSnapshot,
}

/// The latest coordinated reset published for one scheduled WASM processor.
///
/// The request reference makes a lost response idempotent. Reapplying `Publishing` continues the
/// same generation and durability work; it never starts another lifetime. `Ready` is published
/// only after every initial checkpoint required for the selected scope is durable.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct WasmStateReset {
    request: CommandExecutionReference,
    scope: WasmStateResetScope,
    phase: WasmStateResetPhase,
    reason: WasmStateResetReason,
}

impl WasmStateReset {
    pub fn publishing(
        request: CommandExecutionReference,
        scope: WasmStateResetScope,
        reason: WasmStateResetReason,
    ) -> Self {
        Self {
            request,
            scope,
            phase: WasmStateResetPhase::Publishing,
            reason,
        }
    }

    pub fn request(&self) -> &CommandExecutionReference {
        &self.request
    }

    pub fn scope(&self) -> &WasmStateResetScope {
        &self.scope
    }

    pub const fn phase(&self) -> WasmStateResetPhase {
        self.phase
    }

    pub const fn reason(&self) -> WasmStateResetReason {
        self.reason
    }

    pub fn mark_ready(&mut self) {
        self.phase = WasmStateResetPhase::Ready;
    }
}

/// Why a guest refused the saved snapshot it was asked to restore.
///
/// The guest ABI carries the same two verdicts as return codes. This is their durable form: the
/// data plane converts a verdict into it once, at the boundary where a refused restore becomes a
/// control-plane decision, so nothing outside that boundary reads an ABI code.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::Display,
)]
pub enum WasmSavedStateRejection {
    /// The saved bytes are not a snapshot envelope the guest can decode.
    #[strum(to_string = "snapshot envelope rejected")]
    SnapshotEnvelope,
    /// The guest decoded the envelope and refuses the application state it carries.
    #[strum(to_string = "application state rejected")]
    ApplicationState,
}

/// What became of the one recovery attempt a refused lifetime is worth.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::Display,
)]
pub enum WasmStateRecoveryOutcome {
    /// The attempt is admitted and its coordinated reset has not reported an outcome yet. A leader
    /// or owner change resumes this attempt rather than admitting another one.
    #[strum(to_string = "attempted")]
    Attempted,
    /// The reset published a fresh lifetime and the branch resumed on it.
    #[strum(to_string = "recovered")]
    Recovered,
    /// The attempt ended without a usable fresh lifetime. The budget is spent, and the same refused
    /// generation is never discarded again.
    #[strum(to_string = "failed")]
    Failed,
}

/// The one recovery attempt one scope's refused guest-state lifetime is worth.
///
/// The identity is the scope together with the generation whose snapshot was refused, which is what
/// makes the budget survive a process restart and an owner change: both read the same committed
/// schedule and find the same spent attempt. The reset request is derived from that identity, so a
/// resumed attempt drives the very same coordinated reset instead of starting a second one.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmStateRecovery {
    generation: WasmStateGeneration,
    rejection: WasmSavedStateRejection,
    request: CommandExecutionReference,
    outcome: WasmStateRecoveryOutcome,
}

impl WasmStateRecovery {
    /// The generation whose saved snapshot the guest refused.
    pub const fn generation(&self) -> WasmStateGeneration {
        self.generation
    }

    /// The guest's verdict on the refused snapshot.
    pub const fn rejection(&self) -> WasmSavedStateRejection {
        self.rejection
    }

    /// The coordinated reset this attempt drives.
    pub const fn request(&self) -> &CommandExecutionReference {
        &self.request
    }

    /// What the attempt achieved.
    pub const fn outcome(&self) -> WasmStateRecoveryOutcome {
        self.outcome
    }
}

/// What admitting a recovery attempt for one refused lifetime decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmStateRecoveryAdmission {
    /// No attempt has been made for this refused lifetime, and this one is now recorded.
    Admitted(CommandExecutionReference),
    /// An attempt for this refused lifetime is already recorded and unresolved. It is resumed
    /// under the request it was admitted with.
    Resumed(CommandExecutionReference),
    /// This refused lifetime has already been replaced, so there is nothing left to discard.
    AlreadyRecovered,
    /// This refused lifetime spent its attempt without producing a usable one.
    Exhausted,
}

/// The recovery attempts spent on the refused lifetimes of one WASM processor.
///
/// One scope holds at most one attempt, for the newest of its lifetimes that was refused. A scope
/// whose later lifetime is refused replaces the entry, because reaching that lifetime required a
/// successful fresh start and is a new failure rather than a retry of the previous one.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmStateRecoveries {
    #[serde(with = "scope_recoveries")]
    scopes: BTreeMap<WasmStateResetScope, WasmStateRecovery>,
}

mod scope_recoveries {
    use super::*;

    #[derive(Serialize)]
    struct ScopeRecoveryRef<'a> {
        scope: &'a WasmStateResetScope,
        recovery: &'a WasmStateRecovery,
    }

    #[derive(Deserialize)]
    struct ScopeRecovery {
        scope: WasmStateResetScope,
        recovery: WasmStateRecovery,
    }

    pub(super) fn serialize<S>(
        scopes: &BTreeMap<WasmStateResetScope, WasmStateRecovery>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        scopes
            .iter()
            .map(|(scope, recovery)| ScopeRecoveryRef { scope, recovery })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<WasmStateResetScope, WasmStateRecovery>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<ScopeRecovery>::deserialize(deserializer)?;
        let mut scopes = BTreeMap::new();
        for entry in entries {
            if scopes.insert(entry.scope, entry.recovery).is_some() {
                return Err(D::Error::custom(
                    "WASM state recoveries contain a duplicate scope",
                ));
            }
        }
        Ok(scopes)
    }
}

impl WasmStateRecoveries {
    /// The recoveries of a WASM processor no refused lifetime has been reported for.
    pub fn empty() -> Self {
        Self {
            scopes: BTreeMap::new(),
        }
    }

    /// The attempt recorded for `scope`, whichever of its lifetimes it belongs to.
    pub fn of_scope(&self, scope: &WasmStateResetScope) -> Option<&WasmStateRecovery> {
        self.scopes.get(scope)
    }

    /// Every scope that has spent an attempt, with the attempt it spent.
    pub fn iter(&self) -> impl Iterator<Item = (&WasmStateResetScope, &WasmStateRecovery)> {
        self.scopes.iter()
    }

    pub fn len(&self) -> usize {
        self.scopes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }

    /// Decide what a refused `generation` of `scope` is entitled to, and record an admitted attempt.
    ///
    /// `request` names the coordinated reset an admitted attempt drives. It is ignored when an
    /// earlier attempt for this same refused lifetime already decided the outcome, so a caller that
    /// derives it from the identity and a caller that resumes reach the same reset.
    pub fn admit(
        &mut self,
        scope: WasmStateResetScope,
        generation: WasmStateGeneration,
        rejection: WasmSavedStateRejection,
        request: CommandExecutionReference,
    ) -> WasmStateRecoveryAdmission {
        if let Some(recorded) = self.scopes.get(&scope)
            && recorded.generation == generation
        {
            return match recorded.outcome {
                WasmStateRecoveryOutcome::Attempted => {
                    WasmStateRecoveryAdmission::Resumed(recorded.request.clone())
                }
                WasmStateRecoveryOutcome::Recovered => WasmStateRecoveryAdmission::AlreadyRecovered,
                WasmStateRecoveryOutcome::Failed => WasmStateRecoveryAdmission::Exhausted,
            };
        }
        self.scopes.insert(
            scope,
            WasmStateRecovery {
                generation,
                rejection,
                request: request.clone(),
                outcome: WasmStateRecoveryOutcome::Attempted,
            },
        );
        WasmStateRecoveryAdmission::Admitted(request)
    }

    /// Record what the admitted attempt of `scope` achieved.
    ///
    /// Returns `false` when this scope holds no unresolved attempt under `request`, which is how a
    /// report that arrives after the attempt was already settled leaves the record alone.
    pub fn settle(
        &mut self,
        scope: &WasmStateResetScope,
        request: &CommandExecutionReference,
        outcome: WasmStateRecoveryOutcome,
    ) -> bool {
        let Some(recorded) = self.scopes.get_mut(scope) else {
            return false;
        };
        if &recorded.request != request || recorded.outcome != WasmStateRecoveryOutcome::Attempted {
            return false;
        }
        recorded.outcome = outcome;
        true
    }
}

/// One lifetime of a WASM processor branch's guest state.
///
/// A saved snapshot belongs to the generation that was current when the guest state was saved.
/// Nervix saves, installs, serves, restores, and recovers a snapshot only while its generation is the
/// one the committed schedule names for that branch, so a snapshot of an earlier lifetime never
/// becomes current again, whatever revision it was saved at.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct WasmStateGeneration(NonZeroU64);

/// A stored or transferred generation number that no lifetime can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("WASM state generation {0} is not a valid generation number")]
pub struct InvalidWasmStateGeneration(pub u64);

impl WasmStateGeneration {
    /// The generation every branch of a newly created WASM processor starts in.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// The generation published after this one.
    fn successor(self) -> Self {
        let next = self.0.checked_add(1).assured(
            "a generation advances only through a committed schedule publication, and a cluster \
             cannot commit 2^64 of them",
        );
        Self(next)
    }
}

impl From<WasmStateGeneration> for u64 {
    fn from(generation: WasmStateGeneration) -> Self {
        generation.0.get()
    }
}

impl TryFrom<u64> for WasmStateGeneration {
    type Error = InvalidWasmStateGeneration;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match NonZeroU64::new(value) {
            Some(generation) => Ok(Self(generation)),
            None => Err(InvalidWasmStateGeneration(value)),
        }
    }
}

impl fmt::Display for WasmStateGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// The generation the committed schedule names for every branch of one WASM processor.
///
/// Every generation this value has ever handed out is below the one it hands out next, so a
/// branch's generation only moves forward. Starting a new lifetime for every branch replaces all of
/// them at one publication point, including branches that exist only as persisted snapshots, while
/// starting a new lifetime for one concrete branch leaves every other branch in the lifetime it had.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmStateGenerations {
    /// The generation of every branch that has not started a lifetime of its own since.
    every_branch: WasmStateGeneration,
    /// The concrete branches that started a lifetime of their own after `every_branch` was
    /// published. Each generation here is later than `every_branch`.
    #[serde(with = "branch_generations")]
    branches: BTreeMap<BranchKeyFingerprint, WasmStateGeneration>,
}

mod branch_generations {
    use super::*;

    #[derive(Serialize)]
    struct BranchGenerationRef<'a> {
        branch: &'a BranchKeyFingerprint,
        generation: WasmStateGeneration,
    }

    #[derive(Deserialize)]
    struct BranchGeneration {
        branch: BranchKeyFingerprint,
        generation: WasmStateGeneration,
    }

    pub(super) fn serialize<S>(
        branches: &BTreeMap<BranchKeyFingerprint, WasmStateGeneration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        branches
            .iter()
            .map(|(branch, generation)| BranchGenerationRef {
                branch,
                generation: *generation,
            })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<BranchKeyFingerprint, WasmStateGeneration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<BranchGeneration>::deserialize(deserializer)?;
        let mut branches = BTreeMap::new();
        for entry in entries {
            if branches.insert(entry.branch, entry.generation).is_some() {
                return Err(D::Error::custom(
                    "WASM state generations contain a duplicate concrete branch",
                ));
            }
        }
        Ok(branches)
    }
}

impl WasmStateGenerations {
    /// The generations of a WASM processor that was just created: every branch starts its first
    /// lifetime.
    pub fn first() -> Self {
        Self {
            every_branch: WasmStateGeneration::FIRST,
            branches: BTreeMap::new(),
        }
    }

    /// The generation the guest state of `branch` belongs to. `None` names the processor's
    /// unbranched execution.
    pub fn of_branch(&self, branch: Option<&BranchKeyFingerprint>) -> WasmStateGeneration {
        let Some(branch) = branch else {
            return self.every_branch;
        };
        match self.branches.get(branch) {
            Some(generation) => *generation,
            None => self.every_branch,
        }
    }

    /// The generation of a branch that has not been reset separately.
    pub const fn default_generation(&self) -> WasmStateGeneration {
        self.every_branch
    }

    /// The generation named by a coordinated reset's exact scope.
    pub fn of_reset_scope(&self, scope: &WasmStateResetScope) -> WasmStateGeneration {
        match scope {
            WasmStateResetScope::Unbranched | WasmStateResetScope::AllBranches => self.every_branch,
            WasmStateResetScope::Branch(branch) => self.of_branch(Some(branch)),
        }
    }

    /// Start a new lifetime for every branch at once and return its generation.
    pub fn begin_every_branch(&mut self) -> WasmStateGeneration {
        let generation = self.latest().successor();
        self.every_branch = generation;
        self.branches.clear();
        generation
    }

    /// Start a new lifetime for one concrete branch and return its generation. Every other branch
    /// keeps the generation it had.
    pub fn begin_branch(&mut self, branch: BranchKeyFingerprint) -> WasmStateGeneration {
        let generation = self.latest().successor();
        self.branches.insert(branch, generation);
        generation
    }

    /// Start the lifetime selected by one coordinated reset.
    pub fn begin_reset(&mut self, scope: &WasmStateResetScope) -> WasmStateGeneration {
        match scope {
            WasmStateResetScope::Unbranched | WasmStateResetScope::AllBranches => {
                self.begin_every_branch()
            }
            WasmStateResetScope::Branch(branch) => self.begin_branch(*branch),
        }
    }

    /// The most recent generation handed out. Transitions are control-plane decisions, so the
    /// branches that carry a generation of their own are bounded by the branch transitions published
    /// since the last transition of every branch.
    fn latest(&self) -> WasmStateGeneration {
        let mut latest = self.every_branch;
        for generation in self.branches.values() {
            if *generation > latest {
                latest = *generation;
            }
        }
        latest
    }
}

#[cfg(test)]
mod tests {
    use nonzero_ext::nonzero;

    use super::*;
    use crate::{
        AckMode, BranchSelection, CreateRelay, CreateWasmProcessor, GeneralErrorPolicy, Model,
        ProcessorInputs, ProcessorOutputs, ScheduledNode, SchemaFingerprint,
        WasmCheckpointInspection, WasmCheckpointStage, WasmProcessorLimits, WasmStateInspection,
        WasmStateResetReadiness,
    };

    fn branch(byte: u8) -> BranchKeyFingerprint {
        BranchKeyFingerprint::new([byte; 32])
    }

    fn wasm_processor(resource_version: u64, max_fuel: NonZeroU64) -> ScheduledNode {
        ScheduledNode::new(
            Model::WasmProcessor(CreateWasmProcessor {
                name: "guest".try_into().expect("valid processor name"),
                from: ProcessorInputs::single("input".try_into().expect("valid relay name")),
                output_routes: ProcessorOutputs::single(
                    "output".try_into().expect("valid relay name"),
                ),
                branched_by: BranchSelection::unbranched(),
                resource: "guest_bundle".try_into().expect("valid resource name"),
                resource_version,
                file: "processors/guest.wasm".to_string(),
                limits: WasmProcessorLimits {
                    max_fuel,
                    max_memory_bytes: nonzero!(67_108_864u64),
                },
                global_error_policy: GeneralErrorPolicy::Log,
                rejected_state_policy: Default::default(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            }),
            SchemaFingerprint::from_digest([1; 32]),
        )
    }

    fn generation(value: u64) -> WasmStateGeneration {
        WasmStateGeneration::try_from(value).expect("test generations are non-zero")
    }

    #[test]
    fn a_new_processor_starts_every_branch_in_the_first_generation() {
        let generations = WasmStateGenerations::first();

        assert_eq!(generations.of_branch(None), WasmStateGeneration::FIRST);
        assert_eq!(
            generations.of_branch(Some(&branch(1))),
            WasmStateGeneration::FIRST
        );
    }

    #[test]
    fn a_branch_transition_leaves_every_other_branch_in_its_lifetime() {
        let mut generations = WasmStateGenerations::first();

        let alpha = generations.begin_branch(branch(1));
        let beta = generations.begin_branch(branch(2));
        let alpha_again = generations.begin_branch(branch(1));

        assert_eq!(alpha, generation(2));
        assert_eq!(beta, generation(3));
        assert_eq!(alpha_again, generation(4));
        assert_eq!(generations.of_branch(Some(&branch(1))), generation(4));
        assert_eq!(generations.of_branch(Some(&branch(2))), generation(3));
        assert_eq!(
            generations.of_branch(Some(&branch(3))),
            WasmStateGeneration::FIRST
        );
        assert_eq!(generations.of_branch(None), WasmStateGeneration::FIRST);
    }

    #[test]
    fn a_transition_of_every_branch_supersedes_every_branch_of_its_own() {
        let mut generations = WasmStateGenerations::first();
        generations.begin_branch(branch(1));
        generations.begin_branch(branch(2));

        let every_branch = generations.begin_every_branch();

        assert_eq!(every_branch, generation(4));
        for fingerprint in [branch(1), branch(2), branch(3)] {
            assert_eq!(generations.of_branch(Some(&fingerprint)), every_branch);
        }
        assert_eq!(generations.of_branch(None), every_branch);
        assert_eq!(generations.begin_branch(branch(1)), generation(5));
    }

    #[test]
    fn the_same_transition_from_the_same_generations_publishes_the_same_generation() {
        let mut published = WasmStateGenerations::first();
        published.begin_branch(branch(7));
        let mut replanned = published.clone();

        published.begin_every_branch();
        replanned.begin_every_branch();

        assert_eq!(published, replanned);
    }

    #[test]
    fn a_coordinated_branch_reset_advances_once_and_becomes_ready_for_the_same_request() {
        let mut processor = wasm_processor(1, nonzero!(1_000u64));
        let request = CommandExecutionReference::parse("reset-alpha")
            .assured("reset-alpha uses only permitted reference characters");
        let other_request = CommandExecutionReference::parse("reset-beta")
            .expect("the reset reference must be valid");
        let scope = WasmStateResetScope::Branch(branch(1));

        assert!(processor.begin_wasm_state_reset(
            request.clone(),
            scope,
            WasmStateResetReason::Operator,
        ));
        assert!(!processor.begin_wasm_state_reset(
            request.clone(),
            scope,
            WasmStateResetReason::Operator,
        ));
        assert_eq!(
            processor
                .wasm_state_generations()
                .expect("a WASM processor carries generations")
                .of_branch(Some(&branch(1))),
            generation(2)
        );
        assert_eq!(
            processor
                .wasm_state_generations()
                .expect("a WASM processor carries generations")
                .of_branch(Some(&branch(2))),
            WasmStateGeneration::FIRST
        );
        assert_eq!(
            processor
                .wasm_state_reset()
                .expect("the reset was published")
                .phase(),
            WasmStateResetPhase::Publishing
        );

        assert!(!processor.complete_wasm_state_reset(&other_request));
        assert!(processor.complete_wasm_state_reset(&request));
        assert!(!processor.complete_wasm_state_reset(&request));
        assert_eq!(
            processor
                .wasm_state_reset()
                .expect("the reset remains durable after readiness")
                .phase(),
            WasmStateResetPhase::Ready
        );
    }

    #[test]
    fn inspection_fences_previous_generations_and_distinguishes_durable_from_usable() {
        let mut processor = wasm_processor(1, nonzero!(1_000u64));
        let request = CommandExecutionReference::parse("reset-alpha")
            .assured("reset-alpha uses only permitted reference characters");
        let scope = WasmStateResetScope::Branch(branch(1));
        assert!(processor.begin_wasm_state_reset(
            request.clone(),
            scope,
            WasmStateResetReason::Operator,
        ));
        let resetting = WasmStateInspection::of_scheduled(&processor, Vec::new())
            .assured("the test scheduled a WASM processor with state generations");
        assert_eq!(
            resetting.reset_readiness,
            Some(WasmStateResetReadiness::Resetting)
        );
        let checkpoint = |branch, generation, stage| WasmCheckpointInspection {
            branch: Some(branch),
            generation,
            committed_revision: NonZeroU64::new(1),
            latest_revision: NonZeroU64::new(1),
            stage,
            required_replicas: Some(1),
            confirmed_replicas: Some(1),
        };
        let inspection = WasmStateInspection::of_scheduled(
            &processor,
            vec![
                checkpoint(
                    branch(1),
                    WasmStateGeneration::FIRST,
                    WasmCheckpointStage::Failed,
                ),
                checkpoint(
                    branch(1),
                    generation(2),
                    WasmCheckpointStage::ReplicaConfirmed,
                ),
                checkpoint(
                    branch(2),
                    WasmStateGeneration::FIRST,
                    WasmCheckpointStage::Captured,
                ),
            ],
        )
        .assured("the test scheduled a WASM processor with state generations");
        assert_eq!(
            inspection.reset_readiness,
            Some(WasmStateResetReadiness::AwaitingUsableExecution)
        );
        assert_eq!(inspection.checkpoint_counts.total, 2);
        assert_eq!(inspection.checkpoint_counts.failed, 0);
        assert_eq!(inspection.checkpoint_counts.replica_confirmed, 1);
        assert_eq!(inspection.checkpoint_counts.captured, 1);

        assert!(processor.complete_wasm_state_reset(&request));
        let ready = WasmStateInspection::of_scheduled(&processor, inspection.checkpoints)
            .expect("the same processor remains inspectable");
        assert_eq!(ready.reset_readiness, Some(WasmStateResetReadiness::Ready));
    }

    #[test]
    fn inspection_retains_a_failed_restore_without_inventing_checkpoint_success() {
        let mut processor = wasm_processor(1, nonzero!(1_000u64));
        let scope = WasmStateResetScope::Branch(branch(1));
        let admitted = processor
            .admit_wasm_state_recovery(
                scope,
                WasmStateGeneration::FIRST,
                WasmSavedStateRejection::ApplicationState,
            )
            .assured("the test scheduled a WASM processor with recovery state");
        let WasmStateRecoveryAdmission::Admitted(request) = admitted else {
            panic!("the first refusal must admit recovery: {admitted:?}");
        };
        assert!(processor.settle_wasm_state_recovery(
            &scope,
            &request,
            WasmStateRecoveryOutcome::Failed,
        ));
        let inspection = WasmStateInspection::of_scheduled(&processor, Vec::new())
            .assured("the test scheduled a WASM processor with state generations");
        assert_eq!(inspection.reset, None);
        assert_eq!(inspection.reset_readiness, None);
        assert_eq!(inspection.checkpoint_counts.total, 0);
        assert_eq!(inspection.recoveries.len(), 1);
        assert_eq!(inspection.recoveries[0].scope, scope);
        assert_eq!(
            inspection.recoveries[0].rejection,
            WasmSavedStateRejection::ApplicationState
        );
        assert_eq!(
            inspection.recoveries[0].outcome,
            WasmStateRecoveryOutcome::Failed
        );
    }

    #[test]
    fn inspection_counts_every_checkpoint_stage_and_pending_replica() {
        let processor = wasm_processor(1, nonzero!(1_000u64));
        let checkpoint =
            |number, stage, required_replicas, confirmed_replicas| WasmCheckpointInspection {
                branch: Some(branch(number)),
                generation: WasmStateGeneration::FIRST,
                committed_revision: NonZeroU64::new(1),
                latest_revision: NonZeroU64::new(1),
                stage,
                required_replicas,
                confirmed_replicas,
            };
        let inspection = WasmStateInspection::of_scheduled(
            &processor,
            vec![
                checkpoint(1, WasmCheckpointStage::Empty, None, None),
                checkpoint(2, WasmCheckpointStage::Captured, Some(2), Some(0)),
                checkpoint(3, WasmCheckpointStage::LocallyDurable, Some(2), Some(1)),
                checkpoint(4, WasmCheckpointStage::ReplicaConfirmed, Some(2), Some(2)),
                checkpoint(5, WasmCheckpointStage::Failed, Some(2), Some(0)),
            ],
        )
        .assured("the test scheduled a WASM processor with state generations");

        assert_eq!(inspection.checkpoint_counts.total, 5);
        assert_eq!(inspection.checkpoint_counts.empty, 1);
        assert_eq!(inspection.checkpoint_counts.captured, 1);
        assert_eq!(inspection.checkpoint_counts.locally_durable, 1);
        assert_eq!(inspection.checkpoint_counts.awaiting_replicas, 1);
        assert_eq!(inspection.checkpoint_counts.replica_confirmed, 1);
        assert_eq!(inspection.checkpoint_counts.failed, 1);
        assert_eq!(inspection.omitted_checkpoints, 0);
    }

    #[test]
    fn a_refused_lifetime_spends_exactly_one_recovery_attempt() {
        let mut processor = wasm_processor(1, nonzero!(1_000u64));
        let scope = WasmStateResetScope::Branch(branch(1));
        let refused = WasmStateGeneration::FIRST;

        let admitted = processor
            .admit_wasm_state_recovery(scope, refused, WasmSavedStateRejection::ApplicationState)
            .expect("a WASM processor records recoveries");
        let WasmStateRecoveryAdmission::Admitted(request) = admitted else {
            panic!("the first report of a refused lifetime must be admitted: {admitted:?}");
        };
        assert_eq!(request, scope.recovery_request(refused));

        // A second report of the same refused lifetime, from a new owner or after a restart,
        // resumes the attempt that is already recorded rather than starting another one.
        assert_eq!(
            processor.admit_wasm_state_recovery(
                scope,
                refused,
                WasmSavedStateRejection::ApplicationState
            ),
            Some(WasmStateRecoveryAdmission::Resumed(request.clone()))
        );

        assert!(processor.settle_wasm_state_recovery(
            &scope,
            &request,
            WasmStateRecoveryOutcome::Failed
        ));
        assert!(!processor.settle_wasm_state_recovery(
            &scope,
            &request,
            WasmStateRecoveryOutcome::Recovered
        ));
        assert_eq!(
            processor.admit_wasm_state_recovery(
                scope,
                refused,
                WasmSavedStateRejection::ApplicationState
            ),
            Some(WasmStateRecoveryAdmission::Exhausted)
        );

        // Every other branch keeps its own budget, and a later lifetime of the same branch is a
        // new failure rather than a retry of the one that already spent its attempt.
        let sibling = WasmStateResetScope::Branch(branch(2));
        assert!(matches!(
            processor.admit_wasm_state_recovery(
                sibling,
                refused,
                WasmSavedStateRejection::SnapshotEnvelope
            ),
            Some(WasmStateRecoveryAdmission::Admitted(_))
        ));
        assert!(matches!(
            processor.admit_wasm_state_recovery(
                scope,
                generation(4),
                WasmSavedStateRejection::ApplicationState
            ),
            Some(WasmStateRecoveryAdmission::Admitted(_))
        ));
    }

    #[test]
    fn a_recovered_lifetime_answers_a_later_report_of_the_same_refusal() {
        let mut processor = wasm_processor(1, nonzero!(1_000u64));
        let scope = WasmStateResetScope::Unbranched;
        let refused = WasmStateGeneration::FIRST;
        let request = scope.recovery_request(refused);

        processor
            .admit_wasm_state_recovery(scope, refused, WasmSavedStateRejection::SnapshotEnvelope)
            .expect("a WASM processor records recoveries");
        assert!(processor.settle_wasm_state_recovery(
            &scope,
            &request,
            WasmStateRecoveryOutcome::Recovered
        ));

        assert_eq!(
            processor.admit_wasm_state_recovery(
                scope,
                refused,
                WasmSavedStateRejection::SnapshotEnvelope
            ),
            Some(WasmStateRecoveryAdmission::AlreadyRecovered)
        );
        let recorded = processor
            .wasm_state_recoveries()
            .expect("a WASM processor carries recoveries")
            .of_scope(&scope)
            .expect("the attempt is recorded under its scope");
        assert_eq!(recorded.generation(), refused);
        assert_eq!(
            recorded.rejection(),
            WasmSavedStateRejection::SnapshotEnvelope
        );
        assert_eq!(recorded.outcome(), WasmStateRecoveryOutcome::Recovered);
    }

    #[test]
    fn a_recovery_request_names_its_scope_and_generation_without_a_branch_key() {
        let unbranched = WasmStateResetScope::Unbranched.recovery_request(generation(4));
        assert_eq!(unbranched.as_str(), "wasm-recovery.4.unbranched");

        let branched = WasmStateResetScope::Branch(branch(0xab)).recovery_request(generation(12));
        assert_eq!(
            branched.as_str(),
            format!("wasm-recovery.12.{}", "ab".repeat(32))
        );
    }

    #[test]
    fn recovery_attempts_have_a_json_safe_stored_shape() {
        let mut processor = wasm_processor(1, nonzero!(1_000u64));
        processor
            .admit_wasm_state_recovery(
                WasmStateResetScope::Branch(branch(3)),
                generation(2),
                WasmSavedStateRejection::ApplicationState,
            )
            .expect("a WASM processor records recoveries");
        let recoveries = processor
            .wasm_state_recoveries()
            .expect("a WASM processor carries recoveries");

        let encoded = serde_json::to_vec(recoveries)
            .expect("recovery attempts must have a JSON representation");
        let decoded = serde_json::from_slice::<WasmStateRecoveries>(&encoded)
            .expect("the current recovery shape must decode");

        assert_eq!(&decoded, recoveries);
    }

    #[test]
    fn concrete_branch_generations_have_a_json_safe_stored_shape() {
        let mut generations = WasmStateGenerations::first();
        generations.begin_branch(branch(7));

        let encoded = serde_json::to_vec(&generations)
            .expect("concrete branch generations must have a JSON representation");
        let decoded = serde_json::from_slice::<WasmStateGenerations>(&encoded)
            .expect("the current branch generation shape must decode");

        assert_eq!(decoded, generations);
    }

    #[test]
    fn only_a_scheduled_wasm_processor_carries_state_generations() {
        let relay = ScheduledNode::new(
            Model::Relay(CreateRelay {
                name: "events".try_into().expect("valid relay name"),
                schema: "event".try_into().expect("valid schema name"),
                buffer: nonzero!(1usize),
                branching: crate::RelayBranching::unbranched(),
                materialized_state: None,
            }),
            SchemaFingerprint::from_digest([1; 32]),
        )
        .with_resolved_branching(Some(crate::ResolvedBranching::unbranched()));

        assert_eq!(relay.wasm_state_generations(), None);
        assert_eq!(
            wasm_processor(1, nonzero!(1_000u64)).wasm_state_generations(),
            Some(&WasmStateGenerations::first())
        );
    }

    #[test]
    fn a_rescheduled_wasm_processor_continues_its_published_lifetimes() {
        let mut existing = wasm_processor(1, nonzero!(1_000u64));
        existing.begin_wasm_branch_state_generation(branch(1));
        let mut rescheduled = wasm_processor(1, nonzero!(1_000u64));

        rescheduled.continue_wasm_state_generations_of(&existing);

        assert_eq!(
            rescheduled.wasm_state_generations(),
            existing.wasm_state_generations()
        );
    }

    #[test]
    fn a_limits_change_keeps_the_lifetime_and_a_binding_change_starts_a_new_one() {
        let mut existing = wasm_processor(1, nonzero!(1_000u64));
        let request = CommandExecutionReference::parse("reset-alpha")
            .assured("reset-alpha uses only permitted reference characters");
        assert!(existing.begin_wasm_state_reset(
            request.clone(),
            WasmStateResetScope::Branch(branch(1)),
            WasmStateResetReason::Operator,
        ));
        assert!(existing.complete_wasm_state_reset(&request));
        let mut relimited = wasm_processor(1, nonzero!(2_000u64));
        let mut rebound = wasm_processor(2, nonzero!(1_000u64));

        relimited.continue_wasm_state_generations_of(&existing);
        rebound.continue_wasm_state_generations_of(&existing);

        assert_eq!(
            relimited.wasm_state_generations(),
            existing.wasm_state_generations()
        );
        assert_eq!(relimited.wasm_state_reset(), existing.wasm_state_reset());
        assert_eq!(rebound.wasm_state_reset(), None);
        let rebound = rebound
            .wasm_state_generations()
            .expect("a WASM processor carries generations");
        assert_eq!(rebound.of_branch(Some(&branch(1))), generation(3));
        assert_eq!(rebound.of_branch(Some(&branch(2))), generation(3));
    }

    #[test]
    fn a_zero_generation_number_is_rejected() {
        assert_eq!(
            WasmStateGeneration::try_from(0),
            Err(InvalidWasmStateGeneration(0))
        );
        assert_eq!(u64::from(generation(9)), 9);
    }
}
