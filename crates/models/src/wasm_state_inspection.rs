//! Read-only, non-sensitive facts about one scheduled WASM processor's guest state.
//!
//! Layer: vocabulary.
//! - **Owns.** The typed checkpoint, reset and recovery facts an operator may inspect, their
//!   complete counts over a branch selection and the ordered branch entries a bounded read pages.
//! - **Depends on.** WASM state lifetimes, resource bindings, inspection selectors and opaque
//!   branch fingerprints.
//! - **Must not know.** Guest bytes, checkpoint storage, synchronization, owner routing or
//!   presentation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{collections::BTreeMap, num::NonZeroU64};

use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{
    BranchKeyFingerprint, CommandExecutionReference, ResourceName, ScheduledNode, ScheduledUnit,
    StateInspectionBranchSelection, WasmSavedStateRejection, WasmStateGeneration,
    WasmStateRecoveryOutcome, WasmStateReset, WasmStateResetPhase, WasmStateResetScope,
};

/// The latest checkpoint's progress through its durability boundary.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum WasmCheckpointStage {
    /// This branch has not captured guest state in its current lifetime.
    Empty,
    Captured,
    LocallyDurable,
    ReplicaConfirmed,
    Failed,
}

/// One branch's checkpoint progress in its current lifetime. Revisions are absent until the guest
/// has saved a checkpoint, and replica counts until the checkpoint names its durability boundary.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct WasmCheckpointState {
    pub generation: WasmStateGeneration,
    pub committed_revision: Option<NonZeroU64>,
    pub latest_revision: Option<NonZeroU64>,
    pub stage: WasmCheckpointStage,
    pub required_replicas: Option<u32>,
    pub confirmed_replicas: Option<u32>,
}

/// One branch's checkpoint progress as its owning runtime samples it. A fingerprint is a
/// fixed-size opaque identity, never a branch value; `None` is the unbranched execution.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmCheckpointInspection {
    pub branch: Option<BranchKeyFingerprint>,
    pub state: WasmCheckpointState,
}

/// Counts every selected checkpoint of the current lifetimes, including those beyond the returned
/// page of branch entries.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct WasmCheckpointCounts {
    #[rkyv(with = crate::CountAsU64)]
    pub total: usize,
    #[rkyv(with = crate::CountAsU64)]
    pub empty: usize,
    #[rkyv(with = crate::CountAsU64)]
    pub captured: usize,
    #[rkyv(with = crate::CountAsU64)]
    pub locally_durable: usize,
    #[rkyv(with = crate::CountAsU64)]
    pub awaiting_replicas: usize,
    #[rkyv(with = crate::CountAsU64)]
    pub replica_confirmed: usize,
    #[rkyv(with = crate::CountAsU64)]
    pub failed: usize,
}

fn tally(count: &mut usize) {
    *count = count.checked_add(1).verified(
        "each counted fact is one entry of an in-memory map or list, so the count fits usize",
    );
}

impl WasmCheckpointCounts {
    fn count(&mut self, checkpoint: &WasmCheckpointState) {
        tally(&mut self.total);
        match checkpoint.stage {
            WasmCheckpointStage::Empty => tally(&mut self.empty),
            WasmCheckpointStage::Captured => tally(&mut self.captured),
            WasmCheckpointStage::LocallyDurable => {
                tally(&mut self.locally_durable);
                if let (Some(required), Some(confirmed)) =
                    (checkpoint.required_replicas, checkpoint.confirmed_replicas)
                    && confirmed < required
                {
                    tally(&mut self.awaiting_replicas);
                }
            }
            WasmCheckpointStage::ReplicaConfirmed => tally(&mut self.replica_confirmed),
            WasmCheckpointStage::Failed => tally(&mut self.failed),
        }
    }
}

/// Counts every selected rejected-state recovery by what its attempt achieved.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct WasmRecoveryCounts {
    #[rkyv(with = crate::CountAsU64)]
    pub attempted: usize,
    #[rkyv(with = crate::CountAsU64)]
    pub recovered: usize,
    #[rkyv(with = crate::CountAsU64)]
    pub failed: usize,
}

impl WasmRecoveryCounts {
    fn count(&mut self, recovery: &WasmRecoveryInspection) {
        match recovery.outcome {
            WasmStateRecoveryOutcome::Attempted => tally(&mut self.attempted),
            WasmStateRecoveryOutcome::Recovered => tally(&mut self.recovered),
            WasmStateRecoveryOutcome::Failed => tally(&mut self.failed),
        }
    }

    /// Whether any recovery is recorded among the counted scopes.
    pub const fn is_empty(&self) -> bool {
        self.attempted == 0 && self.recovered == 0 && self.failed == 0
    }
}

/// The retained result of one refused state lifetime of a branch.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmRecoveryInspection {
    /// The generation whose saved snapshot the guest refused.
    pub generation: WasmStateGeneration,
    pub rejection: WasmSavedStateRejection,
    /// The coordinated reset the attempt drives.
    pub request: CommandExecutionReference,
    pub outcome: WasmStateRecoveryOutcome,
}

/// The reset's admission boundary as observed by a read. `Ready` describes the committed
/// schedule; a caller that needs execution confirmation must still observe the running owner.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum WasmStateResetReadiness {
    Resetting,
    AwaitingUsableExecution,
    Ready,
}

/// The latest reset, the generation it published for its selected scope and its readiness.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmStateResetInspection {
    pub reset: WasmStateReset,
    pub generation: WasmStateGeneration,
    pub readiness: WasmStateResetReadiness,
}

/// The identity a branch entry is ordered and resumed by. `None` is the unbranched execution,
/// which orders before every branch fingerprint.
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
pub struct WasmStateEntryKey(pub Option<BranchKeyFingerprint>);

/// One selected branch's guest-state facts: its checkpoint in the current lifetime and the
/// recovery recorded for a refused lifetime. A branch is listed when it has at least one of them.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmBranchStateInspection {
    pub branch: Option<BranchKeyFingerprint>,
    pub checkpoint: Option<WasmCheckpointState>,
    pub recovery: Option<WasmRecoveryInspection>,
}

impl WasmBranchStateInspection {
    /// The identity this entry is ordered and resumed by.
    pub const fn key(&self) -> WasmStateEntryKey {
        WasmStateEntryKey(self.branch)
    }

    /// Whether the entry reports anything; an entry of neither fact describes no state.
    pub const fn has_state(&self) -> bool {
        self.checkpoint.is_some() || self.recovery.is_some()
    }
}

/// One execution unit's guest-state lifetimes, complete counts over a branch selection and at most
/// one page of its selected branch entries, in entry order.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmUnitStateInspection {
    pub default_generation: WasmStateGeneration,
    pub reset: Option<WasmStateResetInspection>,
    /// Every selected branch entry, including those beyond `branches`.
    #[rkyv(with = crate::CountAsU64)]
    pub entries: usize,
    pub checkpoints: WasmCheckpointCounts,
    pub recoveries: WasmRecoveryCounts,
    pub branches: Vec<WasmBranchStateInspection>,
}

/// One unit's page and whether selected entries remain after its last entry.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmUnitStatePage {
    pub state: WasmUnitStateInspection,
    pub more: bool,
}

/// The pinned module a WASM processor's guest state belongs to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmStateBinding {
    pub resource: ResourceName,
    pub version: u64,
    pub file: String,
}

impl WasmStateBinding {
    /// The binding a scheduled WASM processor pins; `None` for any other scheduled node.
    pub fn of_scheduled(scheduled: &ScheduledNode) -> Option<Self> {
        let processor = scheduled.wasm_processor()?;
        Some(Self {
            resource: processor.resource.clone(),
            version: processor.resource_version,
            file: processor.file.clone(),
        })
    }
}

/// Why one unit's guest-state facts cannot be read from its schedule entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "a schedule entry that cannot be read as WASM guest state is a typed refusal the \
                  inspection reports; nothing is partially read"
    )
)]
pub enum WasmStateInspectionError {
    #[error("the scheduled unit records no WASM guest-state lifetimes")]
    NoLifetimes,
    #[error(
        "a rejected-state recovery is recorded for every branch at once, which no single \
         execution's refusal records"
    )]
    AllBranchesRecovery,
}

/// One unit's complete selection in entry order, before a page is taken from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmUnitStateSelection {
    default_generation: WasmStateGeneration,
    reset: Option<WasmStateResetInspection>,
    checkpoints: WasmCheckpointCounts,
    recoveries: WasmRecoveryCounts,
    entries: Vec<WasmBranchStateInspection>,
}

impl WasmUnitStateSelection {
    /// The facts of a unit's committed lifetimes and the checkpoints its owner sampled, over the
    /// branches `selection` admits.
    ///
    /// Only checkpoints of the lifetime the schedule names for their branch are current, so a
    /// lagging owner or an in-flight rebind is fenced out of this read. The readiness of the
    /// latest reset is judged from every current checkpoint of the unit, whatever the selection.
    pub fn of_unit(
        scheduled: &ScheduledUnit,
        checkpoints: impl IntoIterator<Item = WasmCheckpointInspection>,
        selection: StateInspectionBranchSelection,
    ) -> Result<Self, WasmStateInspectionError> {
        let Some(generations) = scheduled.wasm_state_generations.as_ref() else {
            return Err(WasmStateInspectionError::NoLifetimes);
        };
        let Some(recorded) = scheduled.wasm_state_recoveries.as_ref() else {
            return Err(WasmStateInspectionError::NoLifetimes);
        };
        let reset = scheduled.wasm_state_reset.as_ref();
        let reset_generation = reset.map(|reset| generations.of_reset_scope(reset.scope()));
        let mut entries = BTreeMap::<WasmStateEntryKey, WasmBranchStateInspection>::new();
        let mut checkpoint_counts = WasmCheckpointCounts::default();
        let mut reset_checkpoint_confirmed = false;
        for checkpoint in checkpoints {
            let branch = checkpoint.branch.as_ref();
            if generations.of_branch(branch) != checkpoint.state.generation {
                continue;
            }
            if let (Some(reset), Some(generation)) = (reset, reset_generation) {
                reset_checkpoint_confirmed |= reset.scope() != &WasmStateResetScope::AllBranches
                    && reset.scope().contains(branch)
                    && checkpoint.state.generation == generation
                    && checkpoint.state.stage == WasmCheckpointStage::ReplicaConfirmed;
            }
            if !selection.admits(checkpoint.branch.as_ref()) {
                continue;
            }
            checkpoint_counts.count(&checkpoint.state);
            entries.insert(
                WasmStateEntryKey(checkpoint.branch),
                WasmBranchStateInspection {
                    branch: checkpoint.branch,
                    checkpoint: Some(checkpoint.state),
                    recovery: None,
                },
            );
        }
        let mut recovery_counts = WasmRecoveryCounts::default();
        for (scope, recovery) in recorded.iter() {
            let branch = match scope {
                WasmStateResetScope::Unbranched => None,
                WasmStateResetScope::Branch(branch) => Some(*branch),
                WasmStateResetScope::AllBranches => {
                    return Err(WasmStateInspectionError::AllBranchesRecovery);
                }
            };
            if !selection.admits(branch.as_ref()) {
                continue;
            }
            let recovery = WasmRecoveryInspection {
                generation: recovery.generation(),
                rejection: recovery.rejection(),
                request: recovery.request().clone(),
                outcome: recovery.outcome(),
            };
            recovery_counts.count(&recovery);
            let entry =
                entries
                    .entry(WasmStateEntryKey(branch))
                    .or_insert(WasmBranchStateInspection {
                        branch,
                        checkpoint: None,
                        recovery: None,
                    });
            entry.recovery = Some(recovery);
        }
        let reset = match (reset, reset_generation) {
            (Some(reset), Some(generation)) => {
                let readiness = match reset.phase() {
                    WasmStateResetPhase::Ready => WasmStateResetReadiness::Ready,
                    WasmStateResetPhase::Publishing if reset_checkpoint_confirmed => {
                        WasmStateResetReadiness::AwaitingUsableExecution
                    }
                    WasmStateResetPhase::Publishing => WasmStateResetReadiness::Resetting,
                };
                Some(WasmStateResetInspection {
                    reset: reset.clone(),
                    generation,
                    readiness,
                })
            }
            _ => None,
        };
        Ok(Self {
            default_generation: generations.default_generation(),
            reset,
            checkpoints: checkpoint_counts,
            recoveries: recovery_counts,
            entries: entries.into_values().collect(),
        })
    }

    /// The generation of every branch that has not started a lifetime of its own.
    pub const fn default_generation(&self) -> WasmStateGeneration {
        self.default_generation
    }

    /// The latest reset with its readiness, when the unit has one.
    pub fn reset(&self) -> Option<&WasmStateResetInspection> {
        self.reset.as_ref()
    }

    /// Every selected entry in entry order.
    pub fn entries(&self) -> &[WasmBranchStateInspection] {
        &self.entries
    }

    /// The unit's facts with at most `limit` of the selected entries that follow `after`.
    pub fn page(self, after: Option<WasmStateEntryKey>, limit: usize) -> WasmUnitStatePage {
        let total = self.entries.len();
        let start = match after {
            Some(after) => self.entries.partition_point(|entry| entry.key() <= after),
            None => 0,
        };
        let mut branches = self.entries;
        let mut tail = branches.split_off(start);
        let remaining = tail.len();
        tail.truncate(limit);
        let more = remaining > tail.len();
        WasmUnitStatePage {
            state: WasmUnitStateInspection {
                default_generation: self.default_generation,
                reset: self.reset,
                entries: total,
                checkpoints: self.checkpoints,
                recoveries: self.recoveries,
                branches: tail,
            },
            more,
        }
    }
}
