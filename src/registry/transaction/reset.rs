//! Pure ordered planning for a WASM guest-state reset.
//!
//! Layer: decisions.
//! - **Owns.** Validation and the branch-exact impact of one scheduled reset step.
//! - **Depends on.** Captured Models, schedules, and transaction impact vocabulary.
//! - **Must not know.** Runtime branch tasks, consensus proposals, or interconnect commands.

use error_stack::Report;
use nervix_models::{
    AttributedGateBoundary, AttributedImpactNode, CanonicalImpactSet, CommandExecutionReference,
    ConcreteBranchCoverage, DomainName, DomainSchedule, DomainStatus, ImpactAttribution,
    ImpactEffects, ImpactGateBoundary, ImpactNodeCoverage, ModelIndex, ModelKind, NodeRef,
    OperationImpactReason, PauseRequirement, QuiesceSubgraph, ResetWasmState, ResolvedBranching,
    StatePurge, StateResetImpact, TransactionOperation, WasmStateResetPhase, WasmStateResetScope,
};

use super::TransactionPlanningError;
use crate::registry::Registry;

pub(super) struct PlannedWasmReset {
    pub(super) operation: TransactionOperation,
    pub(super) reasons: Vec<OperationImpactReason>,
    pub(super) effects: ImpactEffects,
    pub(super) pause: PauseRequirement,
    pub(super) schedule: DomainSchedule,
}

impl Registry {
    pub(super) fn plan_wasm_state_reset(
        reset: &ResetWasmState,
        domain: &DomainName,
        status: DomainStatus,
        models: &ModelIndex,
        current_schedule: Option<&DomainSchedule>,
        request: &CommandExecutionReference,
        attribution: &ImpactAttribution,
    ) -> error_stack::Result<PlannedWasmReset, TransactionPlanningError> {
        if &reset.domain != domain {
            return Err(Report::new(TransactionPlanningError::ResetDomainMismatch {
                domain: domain.clone(),
                requested: reset.domain.clone(),
            }));
        }
        if status != DomainStatus::Running {
            return Err(Report::new(
                TransactionPlanningError::ResetProcessorNotRunning {
                    domain: domain.clone(),
                    processor: reset.processor.clone(),
                },
            ));
        }
        let entity = NodeRef::new(ModelKind::WasmProcessor, &reset.processor);
        if models.get(&entity).is_none() {
            return Err(Report::new(
                TransactionPlanningError::ResetProcessorNotFound {
                    domain: domain.clone(),
                    processor: reset.processor.clone(),
                },
            ));
        }
        let Some(mut schedule) = current_schedule.cloned() else {
            return Err(Report::new(
                TransactionPlanningError::ResetProcessorNotRunning {
                    domain: domain.clone(),
                    processor: reset.processor.clone(),
                },
            ));
        };
        let Some(node) = schedule.nodes.get_mut(&entity) else {
            return Err(Report::new(
                TransactionPlanningError::ResetProcessorNotRunning {
                    domain: domain.clone(),
                    processor: reset.processor.clone(),
                },
            ));
        };
        let Some(wasm) = node.wasm_processor() else {
            return Err(Report::new(
                TransactionPlanningError::ResetProcessorNotFound {
                    domain: domain.clone(),
                    processor: reset.processor.clone(),
                },
            ));
        };
        if node.execution_node().is_none() || node.wasm_state_generations().is_none() {
            return Err(Report::new(
                TransactionPlanningError::ResetProcessorNotRunning {
                    domain: domain.clone(),
                    processor: reset.processor.clone(),
                },
            ));
        }
        let relays = wasm.from.relays().to_vec();
        let branching = node.resolved_branching.as_ref().ok_or_else(|| {
            Report::new(TransactionPlanningError::ResetProcessorNotRunning {
                domain: domain.clone(),
                processor: reset.processor.clone(),
            })
        })?;
        let selection = reset.scope.resolve(branching).map_err(|error| {
            Report::new(TransactionPlanningError::ResetInvalidScope {
                processor: reset.processor.clone(),
                error,
            })
        })?;
        let scope = selection.scope();
        if let Some(current) = node.wasm_state_reset()
            && current.phase() == WasmStateResetPhase::Publishing
            && (current.request() != request || current.scope() != &scope)
        {
            return Err(Report::new(TransactionPlanningError::ResetInProgress {
                processor: reset.processor.clone(),
            }));
        }
        let coverage = match (branching, scope) {
            (ResolvedBranching::Unbranched, WasmStateResetScope::Unbranched) => {
                ConcreteBranchCoverage::Unbranched
            }
            (ResolvedBranching::Branched { branch, .. }, WasmStateResetScope::AllBranches) => {
                ConcreteBranchCoverage::AllOfBranch {
                    branch: branch.clone(),
                }
            }
            (
                ResolvedBranching::Branched { branch, .. },
                WasmStateResetScope::Branch(fingerprint),
            ) => ConcreteBranchCoverage::selected(branch.clone(), [fingerprint]).map_err(
                |error| Report::new(TransactionPlanningError::InvalidImpactReport { error }),
            )?,
            _ => {
                return Err(Report::new(TransactionPlanningError::ResetInvalidScope {
                    processor: reset.processor.clone(),
                    error: Report::new(nervix_models::ResetWasmStateSelectionError::InvalidScope),
                }));
            }
        };
        let covered = ImpactNodeCoverage::execution(entity.clone(), coverage.clone());
        let gate_boundaries = relays.into_iter().map(|relay| AttributedGateBoundary {
            boundary: ImpactGateBoundary {
                relay,
                branches: coverage.clone(),
            },
            attribution: attribution.clone(),
        });
        let pause = PauseRequirement::Subgraph {
            scope: QuiesceSubgraph::new(
                domain.clone(),
                [AttributedImpactNode {
                    coverage: covered.clone(),
                    attribution: attribution.clone(),
                }],
                gate_boundaries,
            ),
        };
        let effects = ImpactEffects {
            state_resets: CanonicalImpactSet::new([StateResetImpact {
                node: covered,
                state: StatePurge::WasmGuestState,
                attribution: attribution.clone(),
            }]),
            ..ImpactEffects::default()
        };
        if node.begin_wasm_state_reset(request.clone(), scope) {
            node.complete_wasm_state_reset(request);
        } else if node
            .wasm_state_reset()
            .is_none_or(|current| current.request() != request || current.scope() != &scope)
        {
            return Err(Report::new(
                TransactionPlanningError::ResetProcessorNotRunning {
                    domain: domain.clone(),
                    processor: reset.processor.clone(),
                },
            ));
        } else {
            node.complete_wasm_state_reset(request);
        }
        Ok(PlannedWasmReset {
            operation: TransactionOperation::ResetWasmState {
                domain: domain.clone(),
                processor: reset.processor.clone(),
            },
            reasons: vec![OperationImpactReason::WasmStateReset { node: entity }],
            effects,
            pause,
            schedule,
        })
    }
}
