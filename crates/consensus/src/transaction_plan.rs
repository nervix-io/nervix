//! Keyed persistence for admitted transaction commit plans.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Durable sectioning and exact retrieval of an admitted decision with its inputs.
//! - **Depends on.** Replicated transaction plan vocabulary and keyed consensus records.
//! - **Must not know.** Planning, runtime execution, parsing or presentation.

use std::io;

use error_stack::Report;
use fjall::Keyspace;
#[cfg(test)]
use meticulous::ResultExt as _;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeName, DomainName, DomainStatus, TransactionCommitPlan,
    TransactionCommitPlanStep, TransactionCommitStepKind, TransactionOperationNumber,
    TransactionPreviewIdentity,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    DomainPlanningInputs, PlanningInputSet, TransactionStepEffect, durable_batch::DurableBatch,
    records::Records,
};

const COMMIT_PLAN_HEADER_TAG: u8 = b'P';
const COMMIT_PLAN_STEP_TAG: u8 = b'p';

/// Volatile node liveness and incarnation inputs consumed by the admitted schedule decisions.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionScheduleEligibility {
    domain: DomainName,
    voters: PlanningInputSet<ClusterNodeName>,
    live_identities: PlanningInputSet<ClusterNodeIdentity>,
    placement_candidate_identities: PlanningInputSet<ClusterNodeIdentity>,
}

impl TransactionScheduleEligibility {
    pub fn new(
        domain: DomainName,
        voters: Vec<ClusterNodeName>,
        live_identities: Vec<ClusterNodeIdentity>,
        placement_candidate_identities: Vec<ClusterNodeIdentity>,
    ) -> Self {
        Self {
            domain,
            voters: voters.into_iter().collect(),
            live_identities: live_identities.into_iter().collect(),
            placement_candidate_identities: placement_candidate_identities.into_iter().collect(),
        }
    }

    pub fn domain(&self) -> &DomainName {
        &self.domain
    }

    pub fn voters(&self) -> &[ClusterNodeName] {
        self.voters.as_slice()
    }

    pub fn live_identities(&self) -> &[ClusterNodeIdentity] {
        self.live_identities.as_slice()
    }

    pub fn placement_candidate_identities(&self) -> &[ClusterNodeIdentity] {
        self.placement_candidate_identities.as_slice()
    }
}

/// The complete decision and captured inputs proposed at commit admission.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionCommitAdmissionPlan {
    decision: TransactionCommitPlan,
    inputs: Vec<DomainPlanningInputs>,
    eligibility: TransactionScheduleEligibility,
}

impl TransactionCommitAdmissionPlan {
    pub fn capture(
        decision: TransactionCommitPlan,
        first_inputs: DomainPlanningInputs,
        eligibility: TransactionScheduleEligibility,
    ) -> error_stack::Result<Self, TransactionCommitPlanBuildError> {
        if first_inputs.domain() != eligibility.domain() {
            return Err(Report::new(TransactionCommitPlanBuildError::DomainMismatch));
        }
        let mut expected = first_inputs;
        let mut inputs = Vec::with_capacity(decision.steps.len());
        for step in &decision.steps {
            inputs.push(expected.clone());
            expected = Self::after_step(expected, step)?;
        }
        Ok(Self {
            decision,
            inputs,
            eligibility,
        })
    }

    pub fn decision(&self) -> &TransactionCommitPlan {
        &self.decision
    }

    pub fn inputs(&self) -> &[DomainPlanningInputs] {
        &self.inputs
    }

    pub fn eligibility(&self) -> &TransactionScheduleEligibility {
        &self.eligibility
    }

    fn after_step(
        inputs: DomainPlanningInputs,
        step: &TransactionCommitPlanStep,
    ) -> error_stack::Result<DomainPlanningInputs, TransactionCommitPlanBuildError> {
        match &step.kind {
            TransactionCommitStepKind::Models { schedule, .. } => {
                Ok(inputs.after_schedule(schedule.as_deref().cloned()))
            }
            TransactionCommitStepKind::AlterDomain { next, schedule, .. } => {
                Ok(inputs.after_domain_update((**next).clone(), schedule.as_deref().cloned()))
            }
            TransactionCommitStepKind::StartDomain { resolved } => {
                let operation = step.impact.operations().first();
                let mut domain = inputs.state().cloned().ok_or_else(|| {
                    Report::new(TransactionCommitPlanBuildError::MissingDomainState { operation })
                })?;
                domain.status = DomainStatus::Running;
                domain.start_version = domain.start_version.checked_add(1).ok_or_else(|| {
                    Report::new(
                        TransactionCommitPlanBuildError::DomainStartGenerationOverflow {
                            operation,
                        },
                    )
                })?;
                domain.last_start = resolved.start.clone();
                domain.clock = resolved.clock.clone();
                let schedule = inputs.schedule().cloned();
                Ok(inputs.after_domain_update(domain, schedule))
            }
            TransactionCommitStepKind::StopDomain => {
                let operation = step.impact.operations().first();
                let mut domain = inputs.state().cloned().ok_or_else(|| {
                    Report::new(TransactionCommitPlanBuildError::MissingDomainState { operation })
                })?;
                domain.status = DomainStatus::Stopped;
                domain.clock = None;
                let schedule = inputs.schedule().cloned();
                Ok(inputs.after_domain_update(domain, schedule))
            }
            TransactionCommitStepKind::CreateResource {
                resource,
                already_existed,
            } => {
                if *already_existed {
                    Ok(inputs)
                } else {
                    Ok(inputs.after_resource_catalog(resource.clone()))
                }
            }
        }
    }

    pub(crate) fn is_consistent(&self) -> bool {
        let Some(first_inputs) = self.inputs.first().cloned() else {
            return self.decision.steps.is_empty();
        };
        Self::capture(
            self.decision.clone(),
            first_inputs,
            self.eligibility.clone(),
        )
        .is_ok_and(|rebuilt| rebuilt == *self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransactionCommitPlanBuildError {
    #[error("transaction plan inputs and schedule eligibility belong to different domains")]
    DomainMismatch,
    #[error("transaction operation {operation} requires captured domain state")]
    MissingDomainState {
        operation: TransactionOperationNumber,
    },
    #[error("transaction operation {operation} overflows the domain start generation")]
    DomainStartGenerationOverflow {
        operation: TransactionOperationNumber,
    },
}

/// One recovered step with every value execution may consume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenTransactionCommitStep {
    pub decision: TransactionCommitPlanStep,
    pub inputs: DomainPlanningInputs,
    pub eligibility: TransactionScheduleEligibility,
}

impl FrozenTransactionCommitStep {
    fn effect_inputs(&self) -> DomainPlanningInputs {
        if matches!(
            &self.decision.kind,
            TransactionCommitStepKind::Models { .. }
        ) && self
            .decision
            .impact
            .planned()
            .pause
            .level()
            .requires_domain_pause()
        {
            self.inputs.clone().after_domain_pause()
        } else {
            self.inputs.clone()
        }
    }

    pub(crate) fn matches_effect(
        &self,
        success: bool,
        effect: Option<&TransactionStepEffect>,
    ) -> bool {
        if !success {
            return effect.is_none();
        }
        let expected_inputs = self.effect_inputs();
        match (&self.decision.kind, effect) {
            (
                TransactionCommitStepKind::Models { schedule, .. },
                Some(TransactionStepEffect::ReplaceDomainSchedule {
                    inputs,
                    schedule: applied,
                }),
            ) => **inputs == expected_inputs && applied.as_deref() == schedule.as_deref(),
            (
                TransactionCommitStepKind::AlterDomain { next, schedule, .. },
                Some(TransactionStepEffect::PutDomainAndSchedule {
                    inputs,
                    domain,
                    schedule: applied,
                }),
            ) => {
                **inputs == expected_inputs
                    && domain.as_ref() == next.as_ref()
                    && applied.as_deref() == schedule.as_deref()
            }
            (
                TransactionCommitStepKind::StartDomain { resolved },
                Some(TransactionStepEffect::StartDomain {
                    inputs,
                    start,
                    clock,
                    authority,
                }),
            ) => {
                **inputs == expected_inputs
                    && start == &resolved.start
                    && clock == &resolved.clock
                    && authority == &resolved.authority
            }
            (
                TransactionCommitStepKind::StopDomain,
                Some(TransactionStepEffect::StopDomain { inputs }),
            ) => **inputs == expected_inputs,
            (
                TransactionCommitStepKind::CreateResource {
                    resource,
                    already_existed: false,
                },
                Some(TransactionStepEffect::CreateResourceCatalog { inputs, identifier }),
            ) => **inputs == expected_inputs && identifier == resource,
            (TransactionCommitStepKind::Models { transitions, .. }, None) => transitions.is_empty(),
            (TransactionCommitStepKind::AlterDomain { next, schedule, .. }, None) => {
                self.inputs.state() == Some(next.as_ref())
                    && self.inputs.schedule() == schedule.as_deref()
            }
            (
                TransactionCommitStepKind::CreateResource {
                    already_existed: true,
                    ..
                },
                None,
            ) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct TransactionCommitPlanStepKey {
    transaction_id: String,
    index: usize,
}

impl TransactionCommitPlanStepKey {
    fn new(transaction_id: &str, index: usize) -> Self {
        Self {
            transaction_id: transaction_id.to_string(),
            index,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
struct TransactionCommitPlanRecordHeader {
    preview: TransactionPreviewIdentity,
    step_count: usize,
    eligibility: TransactionScheduleEligibility,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
struct TransactionCommitPlanRecordStep {
    decision: TransactionCommitPlanStep,
    inputs: DomainPlanningInputs,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TransactionCommitPlanRecords {
    headers: Records<String, TransactionCommitPlanRecordHeader>,
    steps: Records<TransactionCommitPlanStepKey, TransactionCommitPlanRecordStep>,
}

impl TransactionCommitPlanRecords {
    pub(crate) fn load(keyspace: &Keyspace) -> io::Result<Self> {
        Ok(Self {
            headers: Records::load(COMMIT_PLAN_HEADER_TAG, keyspace)?,
            steps: Records::load(COMMIT_PLAN_STEP_TAG, keyspace)?,
        })
    }

    pub(crate) fn write_changes(
        &self,
        preceding: &Self,
        batch: &mut DurableBatch<'_>,
        keyspace: &Keyspace,
    ) -> io::Result<()> {
        self.headers
            .write_changes(&preceding.headers, COMMIT_PLAN_HEADER_TAG, batch, keyspace)?;
        self.steps
            .write_changes(&preceding.steps, COMMIT_PLAN_STEP_TAG, batch, keyspace)
    }

    pub(crate) fn insert(
        &mut self,
        plan: &TransactionCommitAdmissionPlan,
    ) -> error_stack::Result<(), TransactionCommitPlanStoreError> {
        let decision = plan.decision();
        if decision.steps.len() != plan.inputs().len() {
            return Err(Report::new(TransactionCommitPlanStoreError::InputCount {
                steps: decision.steps.len(),
                inputs: plan.inputs().len(),
            }));
        }
        if !plan.is_consistent() {
            return Err(Report::new(TransactionCommitPlanStoreError::InvalidPlan));
        }
        let transaction_id = &decision.preview.transaction_id;
        let header = TransactionCommitPlanRecordHeader {
            preview: decision.preview.clone(),
            step_count: decision.steps.len(),
            eligibility: plan.eligibility().clone(),
        };
        if self
            .headers
            .get(transaction_id)
            .is_some_and(|existing| existing != &header)
        {
            return Err(Report::new(
                TransactionCommitPlanStoreError::ConflictingPlan,
            ));
        }
        for (index, (decision, inputs)) in decision.steps.iter().zip(plan.inputs()).enumerate() {
            let key = TransactionCommitPlanStepKey::new(transaction_id, index);
            let step = TransactionCommitPlanRecordStep {
                decision: decision.clone(),
                inputs: inputs.clone(),
            };
            if self
                .steps
                .get(&key)
                .is_some_and(|existing| existing != &step)
            {
                return Err(Report::new(
                    TransactionCommitPlanStoreError::ConflictingPlan,
                ));
            }
        }

        self.headers.insert(transaction_id.clone(), header);
        for (index, (decision, inputs)) in decision
            .steps
            .iter()
            .cloned()
            .zip(plan.inputs().iter().cloned())
            .enumerate()
        {
            self.steps.insert(
                TransactionCommitPlanStepKey::new(transaction_id, index),
                TransactionCommitPlanRecordStep { decision, inputs },
            );
        }
        Ok(())
    }

    pub(crate) fn matches_admission(&self, plan: &TransactionCommitAdmissionPlan) -> bool {
        let decision = plan.decision();
        let transaction_id = &decision.preview.transaction_id;
        let header = TransactionCommitPlanRecordHeader {
            preview: decision.preview.clone(),
            step_count: decision.steps.len(),
            eligibility: plan.eligibility().clone(),
        };
        if self.headers.get(transaction_id) != Some(&header)
            || decision.steps.len() != plan.inputs().len()
        {
            return false;
        }
        decision
            .steps
            .iter()
            .zip(plan.inputs())
            .enumerate()
            .all(|(index, (decision, inputs))| {
                self.steps
                    .get(&TransactionCommitPlanStepKey::new(transaction_id, index))
                    == Some(&TransactionCommitPlanRecordStep {
                        decision: decision.clone(),
                        inputs: inputs.clone(),
                    })
            })
    }

    pub(crate) fn step(
        &self,
        transaction_id: &str,
        index: usize,
    ) -> error_stack::Result<FrozenTransactionCommitStep, TransactionCommitPlanReadError> {
        let header = self
            .headers
            .get(transaction_id)
            .ok_or_else(|| Report::new(TransactionCommitPlanReadError::MissingHeader))?;
        if index >= header.step_count {
            return Err(Report::new(TransactionCommitPlanReadError::MissingStep {
                index,
            }));
        }
        let step = self
            .steps
            .get(&TransactionCommitPlanStepKey::new(transaction_id, index))
            .ok_or_else(|| Report::new(TransactionCommitPlanReadError::MissingStep { index }))?;
        Ok(FrozenTransactionCommitStep {
            decision: step.decision.clone(),
            inputs: step.inputs.clone(),
            eligibility: header.eligibility.clone(),
        })
    }

    pub(crate) fn remove(&mut self, transaction_id: &str) {
        self.headers.remove(transaction_id);
        self.steps
            .retain(|key, _| key.transaction_id != transaction_id);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransactionCommitPlanStoreError {
    #[error("transaction commit plan has {steps} step(s) and {inputs} captured input set(s)")]
    InputCount { steps: usize, inputs: usize },
    #[error("transaction commit plan inputs do not follow its admitted decisions")]
    InvalidPlan,
    #[error("transaction commit plan conflicts with retained plan content")]
    ConflictingPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransactionCommitPlanReadError {
    #[error("transaction commit plan header is missing")]
    MissingHeader,
    #[error("transaction commit plan is missing step {index}")]
    MissingStep { index: usize },
}

#[cfg(test)]
pub(crate) fn test_admission_plan(
    state: &crate::StateMachineData,
    domain: &DomainName,
    decision: TransactionCommitPlan,
) -> TransactionCommitAdmissionPlan {
    let input = state.domain_planning_inputs(domain);
    let eligibility = TransactionScheduleEligibility::new(
        domain.clone(),
        input.topology().voters().to_vec(),
        Vec::new(),
        Vec::new(),
    );
    TransactionCommitAdmissionPlan::capture(decision, input, eligibility)
        .assured("the test plan can advance from its captured domain inputs")
}

#[cfg(test)]
mod tests {
    use nervix_models::{DomainName, ResourceName};

    use super::*;
    use crate::{StateMachineData, TransactionCommitStepKind, transaction::test_commit_plan};

    fn admission_plan(
        transaction_id: &str,
        operation_count: usize,
    ) -> TransactionCommitAdmissionPlan {
        let decision = test_commit_plan(transaction_id, operation_count);
        let domain =
            DomainName::parse("tenant").assured("the test domain is an identifier-shaped literal");
        let state = StateMachineData::default();
        test_admission_plan(&state, &domain, decision)
    }

    #[test]
    fn plan_steps_are_exact_idempotent_and_removed_as_one_transaction() {
        let plan = admission_plan("tx", 2);
        let mut records = TransactionCommitPlanRecords::default();
        records
            .insert(&plan)
            .assured("the test plan has matching captured inputs");
        records
            .insert(&plan)
            .assured("replaying the identical plan is idempotent");

        assert_eq!(
            records
                .step("tx", 0)
                .assured("the first retained step exists")
                .decision,
            plan.decision.steps[0]
        );
        assert_eq!(
            records
                .step("tx", 1)
                .assured("the second retained step exists")
                .decision,
            plan.decision.steps[1]
        );
        let Err(error) = records.step("tx", 2) else {
            panic!("the plan has only two steps");
        };
        assert_eq!(
            error.current_context(),
            &TransactionCommitPlanReadError::MissingStep { index: 2 }
        );

        let mut conflicting = plan.clone();
        conflicting.decision.steps[1].kind = TransactionCommitStepKind::CreateResource {
            resource: ResourceName::parse("different")
                .assured("the test resource is an identifier-shaped literal"),
            already_existed: false,
        };
        let conflicting = TransactionCommitAdmissionPlan::capture(
            conflicting.decision,
            plan.inputs[0].clone(),
            plan.eligibility.clone(),
        )
        .assured("the conflicting decision has a structurally valid input sequence");
        let preceding = records.clone();
        let Err(error) = records.insert(&conflicting) else {
            panic!("the retained address must reject different plan content");
        };
        assert_eq!(
            error.current_context(),
            &TransactionCommitPlanStoreError::ConflictingPlan
        );
        assert_eq!(records, preceding);

        records.remove("tx");
        let Err(error) = records.step("tx", 0) else {
            panic!("removing a transaction removes its plan header");
        };
        assert_eq!(
            error.current_context(),
            &TransactionCommitPlanReadError::MissingHeader
        );
    }
}
