//! Structured WASM inspection exercises every current checkpoint, reset and recovery state.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current frame cases and their complete representation assertions.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays, service dispatch or live external systems.

use meticulous::ResultExt as _;
use nervix_models::{
    BranchKeyFingerprint, WasmCheckpointCounts, WasmCheckpointInspection, WasmCheckpointStage,
    WasmRecoveryInspection, WasmSavedStateRejection, WasmStateGeneration, WasmStateInspection,
    WasmStateRecoveryOutcome, WasmStateReset, WasmStateResetInspection, WasmStateResetReadiness,
    WasmStateResetReason, WasmStateResetScope,
};

use super::WireValues;

impl WireValues<'_> {
    fn wasm_scope(&mut self) -> WasmStateResetScope {
        let fingerprint = BranchKeyFingerprint::new(self.digest());
        self.arbitrary.entropy().pick([
            WasmStateResetScope::Unbranched,
            WasmStateResetScope::Branch(fingerprint),
            WasmStateResetScope::AllBranches,
        ])
    }

    fn wasm_generation(&mut self) -> WasmStateGeneration {
        WasmStateGeneration::try_from(self.arbitrary.positive_u64().get())
            .assured("a positive generation")
    }

    pub(super) fn wasm_state(&mut self) -> WasmStateInspection {
        let mut checkpoints = Vec::new();
        for stage in [
            WasmCheckpointStage::Empty,
            WasmCheckpointStage::Captured,
            WasmCheckpointStage::LocallyDurable,
            WasmCheckpointStage::ReplicaConfirmed,
            WasmCheckpointStage::Failed,
        ] {
            let replicas = if self.arbitrary.entropy().flag() {
                Some(self.count32().get())
            } else {
                None
            };
            checkpoints.push(WasmCheckpointInspection {
                branch: if self.arbitrary.entropy().flag() {
                    Some(BranchKeyFingerprint::new(self.digest()))
                } else {
                    None
                },
                generation: self.wasm_generation(),
                committed_revision: if self.arbitrary.entropy().flag() {
                    Some(self.arbitrary.positive_u64())
                } else {
                    None
                },
                latest_revision: if self.arbitrary.entropy().flag() {
                    Some(self.arbitrary.positive_u64())
                } else {
                    None
                },
                stage,
                required_replicas: if stage == WasmCheckpointStage::LocallyDurable {
                    Some(2)
                } else {
                    replicas
                },
                confirmed_replicas: if stage == WasmCheckpointStage::LocallyDurable {
                    Some(1)
                } else {
                    replicas
                },
            });
        }
        let mut recoveries = Vec::new();
        for outcome in [
            WasmStateRecoveryOutcome::Attempted,
            WasmStateRecoveryOutcome::Recovered,
            WasmStateRecoveryOutcome::Failed,
        ] {
            for rejection in [
                WasmSavedStateRejection::SnapshotEnvelope,
                WasmSavedStateRejection::ApplicationState,
            ] {
                recoveries.push(WasmRecoveryInspection {
                    scope: self.wasm_scope(),
                    generation: self.wasm_generation(),
                    rejection,
                    request: self.reference(),
                    outcome,
                });
            }
        }
        let reset = if self.arbitrary.entropy().flag() {
            let request = self.reference();
            let scope = self.wasm_scope();
            let reason = self.arbitrary.entropy().pick([
                WasmStateResetReason::Operator,
                WasmStateResetReason::Transaction,
                WasmStateResetReason::Guest,
                WasmStateResetReason::RejectedSnapshot,
            ]);
            let mut reset = WasmStateReset::publishing(request, scope, reason);
            if self.arbitrary.entropy().flag() {
                reset.mark_ready();
            }
            Some(WasmStateResetInspection {
                reset,
                generation: self.wasm_generation(),
            })
        } else {
            None
        };
        let reset_readiness = if reset.is_some() {
            Some(self.arbitrary.entropy().pick([
                WasmStateResetReadiness::Resetting,
                WasmStateResetReadiness::AwaitingUsableExecution,
                WasmStateResetReadiness::Ready,
            ]))
        } else {
            None
        };
        let omitted_checkpoints = self.arbitrary.entropy().count(usize::MAX - 5);
        WasmStateInspection {
            resource: self.arbitrary.name(),
            resource_version: self.arbitrary.positive_u64().get(),
            file: self.arbitrary.string(),
            default_generation: self.wasm_generation(),
            reset,
            reset_readiness,
            recoveries,
            omitted_recoveries: self.arbitrary.entropy().count(usize::MAX - 6),
            checkpoint_counts: WasmCheckpointCounts {
                total: 5 + omitted_checkpoints,
                empty: 1 + omitted_checkpoints,
                captured: 1,
                locally_durable: 1,
                awaiting_replicas: 1,
                replica_confirmed: 1,
                failed: 1,
            },
            checkpoints,
            omitted_checkpoints,
        }
    }
}
