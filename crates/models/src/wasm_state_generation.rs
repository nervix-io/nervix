//! Durable lifetimes of WASM processor guest state.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The generation that names one lifetime of a WASM processor branch's guest state, and
//!   the rule by which the generations of one processor advance.
//! - **Depends on.** The non-sensitive concrete branch identity the impact vocabulary defines.
//! - **Must not know.** Where a snapshot is stored, how replicas synchronize it, or which operation
//!   publishes a transition.

use std::{collections::BTreeMap, fmt, num::NonZeroU64};

use meticulous::OptionExt as _;
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
pub enum WasmStateResetScope {
    Unbranched,
    Branch(BranchKeyFingerprint),
    AllBranches,
}

impl WasmStateResetScope {
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
)]
pub enum WasmStateResetPhase {
    Publishing,
    Ready,
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
}

impl WasmStateReset {
    pub fn publishing(request: CommandExecutionReference, scope: WasmStateResetScope) -> Self {
        Self {
            request,
            scope,
            phase: WasmStateResetPhase::Publishing,
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

    pub fn mark_ready(&mut self) {
        self.phase = WasmStateResetPhase::Ready;
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
        ProcessorInputs, ProcessorOutputs, ScheduledNode, SchemaFingerprint, WasmProcessorLimits,
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
            .expect("the reset reference must be valid");
        let other_request = CommandExecutionReference::parse("reset-beta")
            .expect("the reset reference must be valid");
        let scope = WasmStateResetScope::Branch(branch(1));

        assert!(processor.begin_wasm_state_reset(request.clone(), scope));
        assert!(!processor.begin_wasm_state_reset(request.clone(), scope));
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
        );

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
        existing.begin_wasm_branch_state_generation(branch(1));
        let mut relimited = wasm_processor(1, nonzero!(2_000u64));
        let mut rebound = wasm_processor(2, nonzero!(1_000u64));

        relimited.continue_wasm_state_generations_of(&existing);
        rebound.continue_wasm_state_generations_of(&existing);

        assert_eq!(
            relimited.wasm_state_generations(),
            existing.wasm_state_generations()
        );
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
