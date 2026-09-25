//! Read-only, non-sensitive facts about one scheduled WASM processor's guest state.
//!
//! Layer: vocabulary.
//! - **Owns.** The typed checkpoint, reset and recovery facts an operator may inspect.
//! - **Depends on.** WASM state lifetimes, resource bindings and opaque branch fingerprints.
//! - **Must not know.** Guest bytes, checkpoint storage, synchronization or presentation.

use std::num::NonZeroU64;

use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{
    BranchKeyFingerprint, CommandExecutionReference, ResourceName, ScheduledNode,
    WasmSavedStateRejection, WasmStateGeneration, WasmStateRecoveryOutcome, WasmStateReset,
    WasmStateResetPhase, WasmStateResetScope,
};

const MAX_REPORTED_SCOPES: usize = 128;

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

/// One branch's checkpoint status. A fingerprint is a fixed-size opaque identity, never a branch
/// value. Revisions are absent until the guest has saved a checkpoint.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmCheckpointInspection {
    pub branch: Option<BranchKeyFingerprint>,
    pub generation: WasmStateGeneration,
    pub committed_revision: Option<NonZeroU64>,
    pub latest_revision: Option<NonZeroU64>,
    pub stage: WasmCheckpointStage,
    pub required_replicas: Option<u32>,
    pub confirmed_replicas: Option<u32>,
}

/// Counts every current branch, including those omitted from the bounded detail list.
#[derive(
    Debug,
    Clone,
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
    pub total: usize,
    pub empty: usize,
    pub captured: usize,
    pub locally_durable: usize,
    pub awaiting_replicas: usize,
    pub replica_confirmed: usize,
    pub failed: usize,
}

impl WasmCheckpointCounts {
    fn tally(count: &mut usize) {
        *count = count.checked_add(1).verified(
            "each counted checkpoint exists in the in-memory state map, so the count fits usize",
        );
    }

    fn count(&mut self, checkpoint: &WasmCheckpointInspection) {
        Self::tally(&mut self.total);
        match checkpoint.stage {
            WasmCheckpointStage::Empty => Self::tally(&mut self.empty),
            WasmCheckpointStage::Captured => Self::tally(&mut self.captured),
            WasmCheckpointStage::LocallyDurable => {
                Self::tally(&mut self.locally_durable);
                if let (Some(required), Some(confirmed)) =
                    (checkpoint.required_replicas, checkpoint.confirmed_replicas)
                    && confirmed < required
                {
                    Self::tally(&mut self.awaiting_replicas);
                }
            }
            WasmCheckpointStage::ReplicaConfirmed => Self::tally(&mut self.replica_confirmed),
            WasmCheckpointStage::Failed => Self::tally(&mut self.failed),
        }
    }
}

/// The retained result of one refused state lifetime, identified without exposing branch values.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmRecoveryInspection {
    pub scope: WasmStateResetScope,
    pub generation: WasmStateGeneration,
    pub rejection: WasmSavedStateRejection,
    pub request: CommandExecutionReference,
    pub outcome: WasmStateRecoveryOutcome,
}

/// The latest reset together with the generation it published for its selected scope.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmStateResetInspection {
    pub reset: WasmStateReset,
    pub generation: WasmStateGeneration,
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

/// One read of the committed binding and lifetime state beside the owning runtime's checkpoint
/// progress. The checkpoint list contains only active concrete branches of the current binding.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WasmStateInspection {
    pub resource: ResourceName,
    pub resource_version: u64,
    pub file: String,
    pub default_generation: WasmStateGeneration,
    pub reset: Option<WasmStateResetInspection>,
    pub reset_readiness: Option<WasmStateResetReadiness>,
    pub recoveries: Vec<WasmRecoveryInspection>,
    pub omitted_recoveries: usize,
    pub checkpoint_counts: WasmCheckpointCounts,
    pub checkpoints: Vec<WasmCheckpointInspection>,
    pub omitted_checkpoints: usize,
}

impl WasmStateInspection {
    /// Project one committed schedule entry with the owner's independently sampled checkpoint
    /// facts. The schedule generation fences a lagging owner or an in-flight rebind out of this
    /// read. The fixed report bound keeps a branch churn from expanding a diagnostic without bound.
    pub fn of_scheduled(
        scheduled: &ScheduledNode,
        checkpoints: Vec<WasmCheckpointInspection>,
    ) -> Option<Self> {
        let processor = scheduled.wasm_processor()?;
        let generations = scheduled.wasm_state_generations()?;
        let recorded = scheduled.wasm_state_recoveries()?;
        let mut recoveries = Vec::new();
        for (scope, recovery) in recorded.iter().take(MAX_REPORTED_SCOPES) {
            recoveries.push(WasmRecoveryInspection {
                scope: *scope,
                generation: recovery.generation(),
                rejection: recovery.rejection(),
                request: recovery.request().clone(),
                outcome: recovery.outcome(),
            });
        }
        let omitted_recoveries = recorded
            .len()
            .checked_sub(recoveries.len())
            .verified("the reported recoveries are a prefix of the recorded recoveries");
        let mut current = Vec::new();
        let mut checkpoint_counts = WasmCheckpointCounts::default();
        let reset = scheduled
            .wasm_state_reset()
            .map(|reset| WasmStateResetInspection {
                reset: reset.clone(),
                generation: generations.of_reset_scope(reset.scope()),
            });
        let mut selected_checkpoint_confirmed = false;
        for checkpoint in checkpoints {
            if generations.of_branch(checkpoint.branch.as_ref()) == checkpoint.generation {
                if let Some(reset) = &reset {
                    selected_checkpoint_confirmed |= reset.reset.scope()
                        != &WasmStateResetScope::AllBranches
                        && reset.reset.scope().contains(checkpoint.branch.as_ref())
                        && checkpoint.generation == reset.generation
                        && checkpoint.stage == WasmCheckpointStage::ReplicaConfirmed;
                }
                checkpoint_counts.count(&checkpoint);
                current.push(checkpoint);
            }
        }
        current.sort_by_key(|checkpoint| checkpoint.branch);
        let omitted_checkpoints = if current.len() > MAX_REPORTED_SCOPES {
            current
                .len()
                .checked_sub(MAX_REPORTED_SCOPES)
                .verified("the checkpoint count was checked above against the report limit")
        } else {
            0
        };
        current.truncate(MAX_REPORTED_SCOPES);
        let reset_readiness = reset.as_ref().map(|reset| match reset.reset.phase() {
            WasmStateResetPhase::Ready => WasmStateResetReadiness::Ready,
            WasmStateResetPhase::Publishing if selected_checkpoint_confirmed => {
                WasmStateResetReadiness::AwaitingUsableExecution
            }
            WasmStateResetPhase::Publishing => WasmStateResetReadiness::Resetting,
        });
        Some(Self {
            resource: processor.resource.clone(),
            resource_version: processor.resource_version,
            file: processor.file.clone(),
            default_generation: generations.default_generation(),
            reset,
            reset_readiness,
            recoveries,
            omitted_recoveries,
            checkpoint_counts,
            checkpoints: current,
            omitted_checkpoints,
        })
    }
}
