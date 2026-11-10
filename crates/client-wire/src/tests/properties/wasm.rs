//! Typed WASM state inspections exercise every current checkpoint, reset and recovery state.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current envelope cases with WASM tables and their complete values.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime checkpoint storage, service dispatch or live external systems.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_models::{
    BranchKeyFingerprint, ClusterNodeIdentity, ClusterNodeIncarnation, DomainNodeRef, ModelKind,
    StateInspectionCounts, StateInspectionOrigin, StateInspectionStage, WasmBranchStateInspection,
    WasmCheckpointCounts, WasmCheckpointStage, WasmCheckpointState, WasmRecoveryCounts,
    WasmRecoveryInspection, WasmSavedStateRejection, WasmStateBinding, WasmStateGeneration,
    WasmStateRecoveryOutcome, WasmStateReset, WasmStateResetInspection, WasmStateResetReadiness,
    WasmStateResetReason, WasmStateResetScope, WasmUnitStateInspection,
};

use super::WireValues;
use crate::{StateInspection, StateInspectionPage, StateInspectionResult, StateInspectionUnit};

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

    fn wasm_count(&mut self) -> usize {
        self.arbitrary.entropy().count(usize::MAX)
    }

    fn wasm_checkpoint(&mut self, stage: WasmCheckpointStage) -> WasmCheckpointState {
        let replicas = if self.arbitrary.entropy().flag() {
            Some(self.count32().get())
        } else {
            None
        };
        WasmCheckpointState {
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
            required_replicas: replicas,
            confirmed_replicas: replicas,
        }
    }

    fn wasm_recovery(
        &mut self,
        outcome: WasmStateRecoveryOutcome,
        rejection: WasmSavedStateRejection,
    ) -> WasmRecoveryInspection {
        WasmRecoveryInspection {
            generation: self.wasm_generation(),
            rejection,
            request: self.reference(),
            outcome,
        }
    }

    /// Branch entries in strictly increasing branch order that reach every checkpoint stage,
    /// every recovery outcome and rejection, and every combination of the two facts.
    fn wasm_entries(&mut self) -> Vec<WasmBranchStateInspection> {
        let stages = [
            WasmCheckpointStage::Empty,
            WasmCheckpointStage::Captured,
            WasmCheckpointStage::LocallyDurable,
            WasmCheckpointStage::ReplicaConfirmed,
            WasmCheckpointStage::Failed,
        ];
        let recoveries = [
            (
                WasmStateRecoveryOutcome::Attempted,
                WasmSavedStateRejection::SnapshotEnvelope,
            ),
            (
                WasmStateRecoveryOutcome::Recovered,
                WasmSavedStateRejection::ApplicationState,
            ),
            (
                WasmStateRecoveryOutcome::Failed,
                WasmSavedStateRejection::ApplicationState,
            ),
        ];
        let mut entries = Vec::new();
        if self.arbitrary.entropy().flag() {
            let checkpoint = self.wasm_checkpoint(WasmCheckpointStage::ReplicaConfirmed);
            entries.push(WasmBranchStateInspection {
                branch: None,
                checkpoint: Some(checkpoint),
                recovery: None,
            });
        }
        for (index, stage) in stages.into_iter().enumerate() {
            let mut fingerprint: [u8; 32] = self.digest();
            fingerprint[0] = u8::try_from(index).assured("five stages index below 256");
            let checkpoint = self.wasm_checkpoint(stage);
            let (outcome, rejection) = recoveries[index % recoveries.len()];
            let recovery = if self.arbitrary.entropy().flag() {
                Some(self.wasm_recovery(outcome, rejection))
            } else {
                None
            };
            entries.push(WasmBranchStateInspection {
                branch: Some(BranchKeyFingerprint::new(fingerprint)),
                checkpoint: Some(checkpoint),
                recovery,
            });
        }
        for (index, (outcome, rejection)) in recoveries.into_iter().enumerate() {
            let mut fingerprint: [u8; 32] = self.digest();
            fingerprint[0] = 0x80 | u8::try_from(index).assured("three outcomes index below 128");
            let recovery = self.wasm_recovery(outcome, rejection);
            entries.push(WasmBranchStateInspection {
                branch: Some(BranchKeyFingerprint::new(fingerprint)),
                checkpoint: None,
                recovery: Some(recovery),
            });
        }
        entries
    }

    fn wasm_unit_state(&mut self) -> WasmUnitStateInspection {
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
                readiness: self.arbitrary.entropy().pick([
                    WasmStateResetReadiness::Resetting,
                    WasmStateResetReadiness::AwaitingUsableExecution,
                    WasmStateResetReadiness::Ready,
                ]),
            })
        } else {
            None
        };
        let branches = self.wasm_entries();
        let omitted = self.arbitrary.entropy().count(usize::MAX - branches.len());
        WasmUnitStateInspection {
            default_generation: self.wasm_generation(),
            reset,
            entries: branches.len() + omitted,
            checkpoints: WasmCheckpointCounts {
                total: self.wasm_count(),
                empty: self.wasm_count(),
                captured: self.wasm_count(),
                locally_durable: self.wasm_count(),
                awaiting_replicas: self.wasm_count(),
                replica_confirmed: self.wasm_count(),
                failed: self.wasm_count(),
            },
            recoveries: WasmRecoveryCounts {
                attempted: self.wasm_count(),
                recovered: self.wasm_count(),
                failed: self.wasm_count(),
            },
            branches,
        }
    }

    /// A WASM processor's state read over one or two units, with its pinned binding.
    pub(super) fn wasm_state(&mut self) -> StateInspection {
        let units = if self.arbitrary.entropy().flag() {
            vec![
                nervix_models::Unit::partition(0),
                nervix_models::Unit::partition(1),
            ]
        } else {
            vec![nervix_models::Unit::SINGLE]
        };
        let counts = StateInspectionCounts {
            branches: self.arbitrary.entropy().any_u64(),
            rows: 0,
            bytes: 0,
        };
        let mut inspected = Vec::with_capacity(units.len());
        for unit in units {
            let owner = self.arbitrary.name::<nervix_models::ClusterNodeName>();
            inspected.push(StateInspectionUnit {
                unit,
                owner: owner.clone(),
                replicas: vec![self.arbitrary.name()],
                origin: StateInspectionOrigin {
                    node: ClusterNodeIdentity::new(
                        owner,
                        ClusterNodeIncarnation::new(self.arbitrary.positive_u64().get()),
                    ),
                    fingerprint: self.digest(),
                    generation: self.arbitrary.positive_u64().get(),
                    revision: self.arbitrary.entropy().any_u64(),
                    stage: StateInspectionStage::Live,
                },
                counts,
                sections: Vec::new(),
                wasm: Some(self.wasm_unit_state()),
            });
        }
        StateInspection {
            entity: DomainNodeRef::node_in(
                self.arbitrary.name(),
                ModelKind::WasmProcessor,
                self.arbitrary.name::<nervix_models::WasmProcessorName>(),
            )
            .into(),
            result: StateInspectionResult::Page(StateInspectionPage {
                counts,
                unit_count: u64::try_from(inspected.len()).assured("two units fit u64"),
                units: inspected,
                continuation: if self.arbitrary.entropy().flag() {
                    Some(self.arbitrary.string())
                } else {
                    None
                },
                wasm_binding: Some(WasmStateBinding {
                    resource: self.arbitrary.name(),
                    version: self.arbitrary.positive_u64().get(),
                    file: self.arbitrary.string(),
                }),
            }),
        }
    }
}
