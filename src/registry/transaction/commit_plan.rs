//! Conversion from a complete registry transaction decision into its durable commit plan.
//!
//! Layer: decisions.
//!
//! - **Owns.** Projection of planned steps, model transitions and entity gates for admission.
//! - **Depends on.** The parent transaction planner and public transaction-plan vocabulary.
//! - **Must not know.** Consensus storage, runtime execution, sessions, Tokio or locks.

use std::collections::{BTreeMap, BTreeSet};

use meticulous::OptionExt as _;
use nervix_models::{
    DomainSchedule, OwnershipMoveImpact, OwnershipStateRecoveryOutcome, OwnershipTransition,
    TransactionCommitPlan, TransactionCommitPlanStep, TransactionCommitStepKind,
    TransactionEntityGatePlan, TransactionModelTransition, TransactionOperationNumber,
    TransactionPreviewIdentity, TransactionResolvedDomainStart,
};

use super::{PlannedTransaction, PlannedTransactionStepKind};
use crate::registry::{EntityGatePlan, PlannedMutations};

impl PlannedTransaction {
    pub(crate) fn commit_plan(
        &self,
        transaction_id: String,
        ownership_transition_ids: &BTreeMap<TransactionOperationNumber, String>,
        resolved_starts: &BTreeMap<TransactionOperationNumber, TransactionResolvedDomainStart>,
    ) -> TransactionCommitPlan {
        let steps = self
            .steps
            .iter()
            .map(|step| TransactionCommitPlanStep {
                impact: step.impact.clone(),
                kind: step.kind.commit_step_kind(
                    step.impact.operations().first(),
                    step.impact.planned().effects.ownership_moves.as_slice(),
                    ownership_transition_ids,
                    resolved_starts,
                ),
            })
            .collect();
        TransactionCommitPlan {
            preview: TransactionPreviewIdentity {
                transaction_id,
                position: self.position,
                planning_basis: self.basis,
            },
            steps,
        }
    }
}

impl PlannedTransactionStepKind {
    fn commit_step_kind(
        &self,
        operation: TransactionOperationNumber,
        ownership_moves: &[OwnershipMoveImpact],
        ownership_transition_ids: &BTreeMap<TransactionOperationNumber, String>,
        resolved_starts: &BTreeMap<TransactionOperationNumber, TransactionResolvedDomainStart>,
    ) -> TransactionCommitStepKind {
        let ownership_transition_id = ownership_transition_ids.get(&operation).map(String::as_str);
        match self {
            Self::Models { plan } => {
                let planned = plan
                    .planned
                    .as_ref()
                    .verified("a complete commit plan cannot contain an incomplete model run");
                TransactionCommitStepKind::Models {
                    transitions: planned.model_transitions(),
                    schedule: Self::committed_schedule(
                        plan.schedule.clone(),
                        ownership_moves,
                        ownership_transition_id,
                    ),
                    no_op_operations: plan.no_op_operations.iter().copied().collect(),
                    model_gate: plan.model_gate.commit_gate_plan(),
                    ownership_gate: plan.ownership_gate.commit_gate_plan(),
                }
            }
            Self::AlterDomain { plan } => TransactionCommitStepKind::AlterDomain {
                next: Box::new(plan.next.clone()),
                schedule: Self::committed_schedule(
                    plan.schedule.clone(),
                    ownership_moves,
                    ownership_transition_id,
                ),
                ownership_gate: plan.ownership_gate.commit_gate_plan(),
            },
            Self::StartDomain { .. } => {
                let resolved = resolved_starts.get(&operation).cloned().verified(
                    "commit preparation resolves every start step in this same complete plan",
                );
                TransactionCommitStepKind::StartDomain { resolved }
            }
            Self::StopDomain => TransactionCommitStepKind::StopDomain,
            Self::CreateResource {
                resource,
                already_existed,
            } => TransactionCommitStepKind::CreateResource {
                resource: resource.clone(),
                already_existed: *already_existed,
            },
        }
    }

    fn committed_schedule(
        schedule: Option<DomainSchedule>,
        ownership_moves: &[OwnershipMoveImpact],
        transition_id: Option<&str>,
    ) -> Option<Box<DomainSchedule>> {
        let mut schedule = schedule?;
        if ownership_moves.is_empty() {
            return Some(Box::new(schedule));
        }
        let transition_id = transition_id.verified(
            "commit preparation assigns one transition id to every step with ownership moves",
        );
        for moved in ownership_moves {
            let node = schedule
                .nodes
                .get_mut(&moved.node.node)
                .verified("an admitted ownership move names a node in its exact target schedule");
            node.ownership_transition = Some(OwnershipTransition {
                id: transition_id.to_string(),
                source: moved.source.clone(),
                destination: moved.destination.clone(),
                state_recovery: OwnershipStateRecoveryOutcome::Complete,
                resets: Vec::new(),
            });
        }
        Some(Box::new(schedule))
    }
}

impl PlannedMutations {
    fn model_transitions(&self) -> Vec<TransactionModelTransition> {
        let nodes = self
            .base_models
            .nodes()
            .chain(self.domain_state.models.nodes())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut transitions = Vec::new();
        for node in nodes {
            match (
                self.base_models.get(&node),
                self.domain_state.models.get(&node),
            ) {
                (None, Some(after)) => transitions.push(TransactionModelTransition::Create {
                    model: Box::new(after.clone()),
                }),
                (Some(before), Some(after)) if before != after => {
                    transitions.push(TransactionModelTransition::Replace {
                        before: Box::new(before.clone()),
                        after: Box::new(after.clone()),
                    });
                }
                (Some(before), None) => transitions.push(TransactionModelTransition::Drop {
                    model: Box::new(before.clone()),
                }),
                (Some(_), Some(_)) | (None, None) => {}
            }
        }
        transitions
    }
}

impl EntityGatePlan {
    fn commit_gate_plan(&self) -> TransactionEntityGatePlan {
        TransactionEntityGatePlan {
            affected_entities: self.affected_entities().to_vec(),
            relays: self.relays().to_vec(),
        }
    }
}
