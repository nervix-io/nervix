//! Ordered transaction planning over one immutable control-plane snapshot.
//!
//! Layer: decisions.
//!
//! - **Owns.** Commit-step segmentation, prefix simulation and effective transaction impact.
//! - **Depends on.** Captured vocabulary state and registry mutation/graph decisions.
//! - **Must not know.** Consensus, Tokio, locks, sessions, persistence or runtime execution.

use std::collections::{BTreeMap, BTreeSet};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ActualExecutionStepImpact, CanonicalImpactSet, ConfigurationImpact, ConfigurationTransition,
    DomainLifecycleAction, DomainLifecycleImpact, DomainName, DomainSchedule,
    DomainState as ControlDomainState, DomainStatus, ExecutionStepImpactReport, ImpactAttribution,
    ImpactDiagnostic, ImpactDiagnosticKind, ImpactEffects, ImpactPlanningBasis,
    ImpactReportCompleteness, ImpactReportError, ModelChangeAspect, ModelIndex, NodeRef,
    OperationImpactReason, OperationImpactReport, OwnershipMoveImpact, PauseRequirement,
    PlacementPolicy, PlannedExecutionStepImpact, QuiesceLevel, QuiesceSubgraph,
    ResourceCatalogAction, ResourceCatalogImpact, ResourceName, Statement, TransactionImpactReport,
    TransactionOperation, TransactionOperationNumber, TransactionOperationRange,
    TransactionPosition,
};
use thiserror::Error;

use crate::registry::{ActiveGraph, PlannedMutations, Registry, RegistryError, RegistryMutation};

/// Every mutable control-plane input one planning pass may inspect.
#[derive(Debug, Clone)]
pub(crate) struct TransactionPlanningSnapshot {
    pub(crate) domain: ControlDomainState,
    pub(crate) models: ModelIndex,
    pub(crate) resources: BTreeSet<ResourceName>,
    pub(crate) schedule: Option<DomainSchedule>,
    pub(crate) basis: ImpactPlanningBasis,
}

/// The schedule decision supplied to the ordered planner from the same captured topology inputs.
#[derive(Debug, Clone)]
pub(crate) struct TransactionScheduleDecision {
    pub(crate) schedule: Option<DomainSchedule>,
    pub(crate) ownership_moves: CanonicalImpactSet<OwnershipMoveImpact>,
}

/// One atomic execution step and the exact decision commit must consume for it.
#[derive(Debug, Clone)]
pub(crate) struct PlannedTransactionStep {
    pub(crate) impact: ExecutionStepImpactReport,
    pub(crate) kind: PlannedTransactionStepKind,
}

#[derive(Debug, Clone)]
pub(crate) enum PlannedTransactionStepKind {
    Models {
        plan: Box<PlannedModelTransactionStep>,
    },
    AlterDomain {
        plan: Box<PlannedAlterDomainTransactionStep>,
    },
    StartDomain {
        previous: ControlDomainState,
    },
    StopDomain {
        previous: ControlDomainState,
    },
    CreateResource {
        resource: ResourceName,
        already_existed: bool,
    },
}

/// The captured registry and schedule decisions for one atomic model run.
#[derive(Debug, Clone)]
pub(crate) struct PlannedModelTransactionStep {
    pub(crate) planned: Option<PlannedMutations>,
    pub(crate) expected_schedule: Option<DomainSchedule>,
    pub(crate) schedule: Option<DomainSchedule>,
    pub(crate) no_op_operations: BTreeSet<TransactionOperationNumber>,
}

/// The captured state transition and schedule decision for one domain alteration.
#[derive(Debug, Clone)]
pub(crate) struct PlannedAlterDomainTransactionStep {
    pub(crate) previous: ControlDomainState,
    pub(crate) next: ControlDomainState,
    pub(crate) expected_schedule: Option<DomainSchedule>,
    pub(crate) schedule: Option<DomainSchedule>,
}

/// A complete ordered decision. Admission can project its public semantic report; commit consumes
/// the same step records directly.
#[derive(Debug, Clone)]
pub(crate) struct PlannedTransaction {
    domain: DomainName,
    first_operation_index: usize,
    position: TransactionPosition,
    basis: ImpactPlanningBasis,
    completeness: ImpactReportCompleteness,
    operations: Vec<OperationImpactReport>,
    steps: Vec<PlannedTransactionStep>,
}

impl PlannedTransaction {
    pub(crate) fn steps(&self) -> &[PlannedTransactionStep] {
        &self.steps
    }

    pub(crate) fn first_step(&self) -> Option<&PlannedTransactionStep> {
        self.steps.first()
    }

    pub(crate) fn report(
        &self,
    ) -> Result<TransactionImpactReport, Report<TransactionPlanningError>> {
        if self.first_operation_index != 0 {
            return Err(Report::new(
                TransactionPlanningError::PartialPlanHasNoTransactionReport {
                    first_operation: self
                        .first_operation_index
                        .checked_add(1)
                        .assured("a partial plan starts at an addressable transaction index"),
                },
            ));
        }
        let execution_steps = self.steps.iter().map(|step| step.impact.clone()).collect();
        TransactionImpactReport::new(
            self.domain.clone(),
            self.position,
            self.basis,
            self.completeness.clone(),
            self.operations.clone(),
            execution_steps,
        )
        .map_err(|error| Report::new(TransactionPlanningError::InvalidImpactReport { error }))
    }
}

#[derive(Debug, Error)]
pub(crate) enum TransactionPlanningError {
    #[error("domain '{domain}' does not exist")]
    DomainNotFound { domain: DomainName },
    #[error("domain '{domain}' is paused by a model alteration")]
    DomainPaused { domain: DomainName },
    #[error("domain '{domain}' already has a model alteration in progress")]
    ConcurrentDomainAlter { domain: DomainName },
    #[error("domain '{domain}' is already running")]
    DomainAlreadyRunning { domain: DomainName },
    #[error("domain '{domain}' is already stopped")]
    DomainAlreadyStopped { domain: DomainName },
    #[error("domain '{domain}' start generation overflowed")]
    DomainStartGenerationOverflow { domain: DomainName },
    #[error("resource '{resource}' already exists")]
    ResourceAlreadyExists { resource: ResourceName },
    #[error("transaction operation {operation} is not valid transaction content")]
    InvalidOperation {
        operation: TransactionOperationNumber,
    },
    #[error("transaction operation {operation} failed model preflight: {error}")]
    ModelPreflight {
        operation: TransactionOperationNumber,
        error: Report<RegistryError>,
    },
    #[error("transaction operation {operation} failed external model validation")]
    ExternalModelValidation {
        operation: TransactionOperationNumber,
    },
    #[error("transaction operation {operation} failed UDF preparation")]
    UdfPreparation {
        operation: TransactionOperationNumber,
    },
    #[error("transaction impact identities are invalid: {error}")]
    InvalidImpactReport { error: Report<ImpactReportError> },
    #[error("failed to encode the transaction planning basis")]
    PlanningBasisEncoding,
    #[error("a plan beginning at operation {first_operation} has no whole-transaction report")]
    PartialPlanHasNoTransactionReport { first_operation: usize },
}

struct ModelContribution {
    reasons: Vec<OperationImpactReason>,
    effects: ImpactEffects,
    touched_nodes: BTreeSet<NodeRef>,
}

struct PendingOperationImpact {
    number: TransactionOperationNumber,
    operation: TransactionOperation,
    reasons: Vec<OperationImpactReason>,
    contribution: ImpactEffects,
}

struct ModelRunPlan {
    operations: Vec<PendingOperationImpact>,
    step: PlannedTransactionStep,
    candidate_models: ModelIndex,
    schedule: Option<DomainSchedule>,
}

struct ModelRunPlanningInput<'a> {
    domain: &'a DomainName,
    domain_state: &'a ControlDomainState,
    base_models: ModelIndex,
    current_schedule: Option<DomainSchedule>,
    statements: &'a [Statement],
    first_operation_index: usize,
    allow_incomplete: bool,
}

impl Registry {
    /// Plans `statements` from `first_operation_index` in their written order. The callback is a
    /// synchronous schedule decision over topology inputs captured beside `snapshot`.
    pub(crate) fn plan_transaction<F>(
        snapshot: TransactionPlanningSnapshot,
        statements: &[Statement],
        first_operation_index: usize,
        allow_incomplete_final_model_run: bool,
        mut schedule: F,
    ) -> Result<PlannedTransaction, Report<TransactionPlanningError>>
    where
        F: FnMut(
            Option<ActiveGraph>,
            PlacementPolicy,
            Option<&DomainSchedule>,
            &ImpactAttribution,
        ) -> TransactionScheduleDecision,
    {
        let domain = snapshot.domain.id.clone();
        let mut domain_state = snapshot.domain;
        let mut models = snapshot.models;
        let mut resources = snapshot.resources;
        let mut current_schedule = snapshot.schedule;
        let mut operations = Vec::with_capacity(statements.len());
        let mut steps = Vec::new();
        let mut operation_offset = 0usize;

        while operation_offset < statements.len() {
            let absolute_index = first_operation_index
                .checked_add(operation_offset)
                .assured("a statement slice cannot extend beyond the transaction that owns it");
            let statement = statements
                .get(operation_offset)
                .verified("the loop condition keeps its operation offset in the statement slice");
            if statement.is_model_mutation() {
                let mut end = operation_offset;
                while statements
                    .get(end)
                    .is_some_and(Statement::is_model_mutation)
                {
                    end = end
                        .checked_add(1)
                        .assured("a transaction statement count is below usize::MAX");
                }
                let is_final_run = end == statements.len();
                let allow_incomplete = allow_incomplete_final_model_run && is_final_run;
                let run_statements = statements
                    .get(operation_offset..end)
                    .verified("the model run bounds were found in this statement slice");
                let input = ModelRunPlanningInput {
                    domain: &domain,
                    domain_state: &domain_state,
                    base_models: models,
                    current_schedule,
                    statements: run_statements,
                    first_operation_index: absolute_index,
                    allow_incomplete,
                };
                let run = Self::plan_model_run(input, &mut schedule)?;
                models = run.candidate_models;
                current_schedule = run.schedule;
                let range = run.step.impact.operations();
                operations.extend(run.operations.into_iter().map(|operation| {
                    operation_report(
                        operation,
                        range,
                        run.step.impact.planned().completeness.clone(),
                    )
                }));
                steps.push(run.step);
                operation_offset = end;
                continue;
            }

            let number =
                TransactionOperationNumber::from_index(absolute_index).map_err(|error| {
                    Report::new(TransactionPlanningError::InvalidImpactReport { error })
                })?;
            let range = TransactionOperationRange::from_index_and_count(absolute_index, 1)
                .map_err(|error| {
                    Report::new(TransactionPlanningError::InvalidImpactReport { error })
                })?;
            let attribution = ImpactAttribution::single(number);
            let operation;
            let reasons;
            let contribution;
            let kind;
            let pause;
            match statement {
                Statement::AlterDomain(alter) => {
                    ensure_domain_not_paused(&domain_state)?;
                    operation = TransactionOperation::AlterDomain {
                        domain: domain.clone(),
                    };
                    let previous = domain_state.clone();
                    let changed = previous.config.placement != alter.policy;
                    domain_state.config.placement = alter.policy;
                    let expected_schedule = current_schedule.clone();
                    let schedule_decision = if changed {
                        let graph =
                            crate::registry::domain_state::DomainState::build(&domain, &models)
                                .map_err(|error| {
                                    Report::new(TransactionPlanningError::ModelPreflight {
                                        operation: number,
                                        error,
                                    })
                                })?
                                .graph;
                        schedule(
                            (graph.node_count() > 0).then_some(graph),
                            alter.policy,
                            current_schedule.as_ref(),
                            &attribution,
                        )
                    } else {
                        TransactionScheduleDecision {
                            schedule: current_schedule.clone(),
                            ownership_moves: CanonicalImpactSet::default(),
                        }
                    };
                    let ownership_moves = schedule_decision.ownership_moves;
                    pause = effective_pause(
                        &domain,
                        &domain_state.status,
                        if ownership_moves.is_empty() {
                            QuiesceLevel::Dynamic
                        } else {
                            QuiesceLevel::EntityPause
                        },
                    );
                    reasons = if changed {
                        vec![OperationImpactReason::DomainPlacement]
                    } else {
                        Vec::new()
                    };
                    contribution = ImpactEffects {
                        ownership_moves: ownership_moves.clone(),
                        ..ImpactEffects::default()
                    };
                    current_schedule = schedule_decision.schedule.clone();
                    kind = PlannedTransactionStepKind::AlterDomain {
                        plan: Box::new(PlannedAlterDomainTransactionStep {
                            previous,
                            next: domain_state.clone(),
                            expected_schedule,
                            schedule: schedule_decision.schedule,
                        }),
                    };
                }
                Statement::StartDomain(start) => {
                    ensure_domain_not_paused(&domain_state)?;
                    if let DomainStatus::Running = domain_state.status {
                        return Err(Report::new(
                            TransactionPlanningError::DomainAlreadyRunning {
                                domain: domain.clone(),
                            },
                        ));
                    }
                    operation = TransactionOperation::StartDomain {
                        domain: domain.clone(),
                    };
                    let previous = domain_state.clone();
                    domain_state.status = DomainStatus::Running;
                    domain_state.last_start = start.start.clone();
                    domain_state.start_version =
                        domain_state.start_version.checked_add(1).ok_or_else(|| {
                            Report::new(TransactionPlanningError::DomainStartGenerationOverflow {
                                domain: domain.clone(),
                            })
                        })?;
                    reasons = vec![OperationImpactReason::DomainStart];
                    contribution = ImpactEffects {
                        lifecycle: CanonicalImpactSet::new([DomainLifecycleImpact {
                            domain: domain.clone(),
                            action: DomainLifecycleAction::Start,
                            attribution,
                        }]),
                        ..ImpactEffects::default()
                    };
                    kind = PlannedTransactionStepKind::StartDomain { previous };
                    pause = PauseRequirement::NoPause;
                }
                Statement::StopDomain(_) => {
                    ensure_domain_not_paused(&domain_state)?;
                    if let DomainStatus::Stopped = domain_state.status {
                        return Err(Report::new(
                            TransactionPlanningError::DomainAlreadyStopped {
                                domain: domain.clone(),
                            },
                        ));
                    }
                    operation = TransactionOperation::StopDomain {
                        domain: domain.clone(),
                    };
                    let previous = domain_state.clone();
                    domain_state.status = DomainStatus::Stopped;
                    domain_state.clock = None;
                    reasons = vec![OperationImpactReason::DomainStop];
                    contribution = ImpactEffects {
                        lifecycle: CanonicalImpactSet::new([DomainLifecycleImpact {
                            domain: domain.clone(),
                            action: DomainLifecycleAction::Stop,
                            attribution,
                        }]),
                        ..ImpactEffects::default()
                    };
                    kind = PlannedTransactionStepKind::StopDomain { previous };
                    pause = PauseRequirement::NoPause;
                }
                Statement::CreateResource(create) => {
                    operation = TransactionOperation::CreateResource {
                        domain: domain.clone(),
                        resource: create.body.identifier.clone(),
                    };
                    let already_existed = !resources.insert(create.body.identifier.clone());
                    if already_existed && !create.if_not_exists {
                        return Err(Report::new(
                            TransactionPlanningError::ResourceAlreadyExists {
                                resource: create.body.identifier.clone(),
                            },
                        ));
                    }
                    if already_existed {
                        reasons = Vec::new();
                        contribution = ImpactEffects::default();
                    } else {
                        reasons = vec![OperationImpactReason::ResourceCatalog {
                            resource: create.body.identifier.clone(),
                        }];
                        contribution = ImpactEffects {
                            resource_catalog: CanonicalImpactSet::new([ResourceCatalogImpact {
                                resource: create.body.identifier.clone(),
                                action: ResourceCatalogAction::Create,
                                attribution,
                            }]),
                            ..ImpactEffects::default()
                        };
                    }
                    kind = PlannedTransactionStepKind::CreateResource {
                        resource: create.body.identifier.clone(),
                        already_existed,
                    };
                    pause = PauseRequirement::NoPause;
                }
                _ => {
                    return Err(Report::new(TransactionPlanningError::InvalidOperation {
                        operation: number,
                    }));
                }
            }
            let completeness = completeness_for_pause(number, &pause, Vec::new());
            operations.push(OperationImpactReport {
                number,
                operation,
                execution_step: range,
                completeness: completeness.clone(),
                reasons,
                contribution: contribution.clone(),
            });
            steps.push(PlannedTransactionStep {
                impact: ExecutionStepImpactReport::new(
                    range,
                    PlannedExecutionStepImpact {
                        completeness,
                        pause,
                        effects: contribution,
                    },
                    ActualExecutionStepImpact::unattempted(),
                ),
                kind,
            });
            operation_offset = operation_offset
                .checked_add(1)
                .assured("a transaction statement count is below usize::MAX");
        }

        let accepted_operations = first_operation_index
            .checked_add(statements.len())
            .assured("a transaction prefix is bounded by its configured statement limit");
        let completeness = combined_completeness(&steps);
        Ok(PlannedTransaction {
            domain,
            first_operation_index,
            position: TransactionPosition::new(accepted_operations),
            basis: snapshot.basis,
            completeness,
            operations,
            steps,
        })
    }

    fn plan_model_run<F>(
        input: ModelRunPlanningInput<'_>,
        schedule: &mut F,
    ) -> Result<ModelRunPlan, Report<TransactionPlanningError>>
    where
        F: FnMut(
            Option<ActiveGraph>,
            PlacementPolicy,
            Option<&DomainSchedule>,
            &ImpactAttribution,
        ) -> TransactionScheduleDecision,
    {
        let ModelRunPlanningInput {
            domain,
            domain_state,
            base_models,
            current_schedule,
            statements,
            first_operation_index,
            allow_incomplete,
        } = input;
        ensure_domain_not_paused(domain_state)?;
        let range = TransactionOperationRange::from_index_and_count(
            first_operation_index,
            statements.len(),
        )
        .map_err(|error| Report::new(TransactionPlanningError::InvalidImpactReport { error }))?;
        let mut prefix_models = base_models.clone();
        let mut mutations = Vec::new();
        let mut no_op_operations = BTreeSet::new();
        let mut pending_operations = Vec::with_capacity(statements.len());
        let mut touched_by_node = BTreeMap::<NodeRef, Vec<TransactionOperationNumber>>::new();

        for (run_index, statement) in statements.iter().enumerate() {
            let absolute_index = first_operation_index
                .checked_add(run_index)
                .assured("a model run is a range of this transaction");
            let number =
                TransactionOperationNumber::from_index(absolute_index).map_err(|error| {
                    Report::new(TransactionPlanningError::InvalidImpactReport { error })
                })?;
            let operation = model_operation(domain, statement).map_err(|_| {
                Report::new(TransactionPlanningError::InvalidOperation { operation: number })
            })?;
            let is_if_not_exists_noop = match statement {
                Statement::Create(create) if create.if_not_exists => {
                    prefix_models.contains(&create.body.node_ref())
                }
                _ => false,
            };
            let contribution = if is_if_not_exists_noop {
                no_op_operations.insert(number);
                ModelContribution {
                    reasons: Vec::new(),
                    effects: ImpactEffects::default(),
                    touched_nodes: BTreeSet::new(),
                }
            } else {
                let mutation = RegistryMutation::try_from(statement).map_err(|_| {
                    Report::new(TransactionPlanningError::InvalidOperation { operation: number })
                })?;
                let before = prefix_models.clone();
                mutation.fold_into_models(&mut prefix_models);
                let contribution = model_contribution(&before, &prefix_models, number);
                mutations.push(mutation);
                contribution
            };
            for node in &contribution.touched_nodes {
                touched_by_node
                    .entry(node.clone())
                    .or_default()
                    .push(number);
            }
            pending_operations.push(PendingOperationImpact {
                number,
                operation,
                reasons: contribution.reasons,
                contribution: contribution.effects,
            });
        }

        let preflight = Self::preflight_transaction_mutations_against(
            domain,
            base_models.clone(),
            &mutations,
            allow_incomplete,
        )
        .map_err(|error| {
            Report::new(TransactionPlanningError::ModelPreflight {
                operation: range.first(),
                error,
            })
        })?;
        let candidate_models = preflight.candidate_models().clone();
        let incomplete_reason = preflight.incomplete_reason().map(ToOwned::to_owned);
        let attribution = ImpactAttribution::for_range(range);
        let step_effects = model_step_effects(
            &base_models,
            &candidate_models,
            &touched_by_node,
            &attribution,
        );
        let mut diagnostics = Vec::new();
        if let Some(reason) = &incomplete_reason {
            diagnostics.push(ImpactDiagnostic {
                kind: ImpactDiagnosticKind::Planning,
                operation: Some(range.first()),
                message: reason.to_string(),
            });
        }
        if !step_effects.changed_configuration.is_empty() {
            diagnostics.push(ImpactDiagnostic {
                kind: ImpactDiagnosticKind::Topology,
                operation: Some(range.first()),
                message: "the exact affected topology and derived runtime effects are not planned \
                          yet"
                .to_string(),
            });
        }

        let expected_schedule = current_schedule.clone();
        let (planned, next_schedule, ownership_moves, base_level) = match preflight.planned {
            Some(planned) => {
                let base_level = planned.quiesce().level();
                if planned.is_noop() {
                    (
                        Some(planned),
                        current_schedule,
                        CanonicalImpactSet::default(),
                        QuiesceLevel::Dynamic,
                    )
                } else {
                    let decision = schedule(
                        planned.candidate_graph(),
                        domain_state.config.placement,
                        current_schedule.as_ref(),
                        &attribution,
                    );
                    (
                        Some(planned),
                        decision.schedule,
                        decision.ownership_moves,
                        base_level,
                    )
                }
            }
            None => (
                None,
                current_schedule,
                CanonicalImpactSet::default(),
                QuiesceLevel::Dynamic,
            ),
        };
        let effective_level = if ownership_moves.is_empty() {
            base_level
        } else {
            base_level.max(QuiesceLevel::EntityPause)
        };
        let pause = if incomplete_reason.is_some() {
            PauseRequirement::NoPause
        } else {
            effective_pause(domain, &domain_state.status, effective_level)
        };
        let completeness = completeness_for_pause(range.first(), &pause, diagnostics);
        let mut effects = step_effects;
        effects.ownership_moves = ownership_moves;
        let impact = ExecutionStepImpactReport::new(
            range,
            PlannedExecutionStepImpact {
                completeness,
                pause,
                effects,
            },
            ActualExecutionStepImpact::unattempted(),
        );
        Ok(ModelRunPlan {
            operations: pending_operations,
            step: PlannedTransactionStep {
                impact,
                kind: PlannedTransactionStepKind::Models {
                    plan: Box::new(PlannedModelTransactionStep {
                        planned,
                        expected_schedule,
                        schedule: next_schedule.clone(),
                        no_op_operations,
                    }),
                },
            },
            candidate_models,
            schedule: next_schedule,
        })
    }
}

fn ensure_domain_not_paused(
    domain: &ControlDomainState,
) -> Result<(), Report<TransactionPlanningError>> {
    if let DomainStatus::Paused = domain.status {
        return Err(Report::new(TransactionPlanningError::DomainPaused {
            domain: domain.id.clone(),
        }));
    }
    Ok(())
}

fn model_operation(
    domain: &DomainName,
    statement: &Statement,
) -> Result<TransactionOperation, crate::registry::mutation::RegistryMutationConversionError> {
    let mutation = RegistryMutation::try_from(statement)?;
    let operation = match statement {
        Statement::Create(_) => TransactionOperation::CreateConfiguration {
            domain: domain.clone(),
            node: mutation.target_key(),
        },
        Statement::Drop(_) => TransactionOperation::DropConfiguration {
            domain: domain.clone(),
            node: mutation.target_key(),
        },
        _ => TransactionOperation::AlterConfiguration {
            domain: domain.clone(),
            node: mutation.target_key(),
        },
    };
    Ok(operation)
}

fn model_contribution(
    before: &ModelIndex,
    after: &ModelIndex,
    operation: TransactionOperationNumber,
) -> ModelContribution {
    let attribution = ImpactAttribution::single(operation);
    let mut reasons = Vec::new();
    let mut changed_configuration = Vec::new();
    let mut touched_nodes = BTreeSet::new();
    for node in model_keys(before, after) {
        let base = before.get(&node);
        let candidate = after.get(&node);
        match (base, candidate) {
            (None, Some(_)) => {
                reasons.push(OperationImpactReason::Configuration {
                    node: node.clone(),
                    aspect: ModelChangeAspect::EntityCreated,
                });
                changed_configuration.push(ConfigurationImpact {
                    transition: ConfigurationTransition::Created { node: node.clone() },
                    attribution: attribution.clone(),
                });
                touched_nodes.insert(node);
            }
            (Some(_), None) => {
                reasons.push(OperationImpactReason::Configuration {
                    node: node.clone(),
                    aspect: ModelChangeAspect::EntityDropped,
                });
                changed_configuration.push(ConfigurationImpact {
                    transition: ConfigurationTransition::Dropped { node: node.clone() },
                    attribution: attribution.clone(),
                });
                touched_nodes.insert(node);
            }
            (Some(base), Some(candidate)) if base != candidate => {
                for aspect in base.change_aspects_against(candidate).aspects() {
                    reasons.push(OperationImpactReason::Configuration {
                        node: node.clone(),
                        aspect: *aspect,
                    });
                }
                changed_configuration.push(ConfigurationImpact {
                    transition: ConfigurationTransition::Changed { node: node.clone() },
                    attribution: attribution.clone(),
                });
                touched_nodes.insert(node);
            }
            (Some(_), Some(_)) | (None, None) => {}
        }
    }
    ModelContribution {
        reasons,
        effects: ImpactEffects {
            changed_configuration: CanonicalImpactSet::new(changed_configuration),
            ..ImpactEffects::default()
        },
        touched_nodes,
    }
}

fn model_step_effects(
    before: &ModelIndex,
    after: &ModelIndex,
    touched_by_node: &BTreeMap<NodeRef, Vec<TransactionOperationNumber>>,
    fallback_attribution: &ImpactAttribution,
) -> ImpactEffects {
    let mut changed_configuration = Vec::new();
    for node in model_keys(before, after) {
        let base = before.get(&node);
        let candidate = after.get(&node);
        let transition = match (base, candidate) {
            (None, Some(_)) => Some(ConfigurationTransition::Created { node: node.clone() }),
            (Some(_), None) => Some(ConfigurationTransition::Dropped { node: node.clone() }),
            (Some(base), Some(candidate)) if base != candidate => {
                Some(ConfigurationTransition::Changed { node: node.clone() })
            }
            (Some(_), Some(_)) | (None, None) => None,
        };
        let Some(transition) = transition else {
            continue;
        };
        let attribution = match touched_by_node.get(&node) {
            Some(operations) => ImpactAttribution::new(operations.iter().copied())
                .assured("every changed node was touched by at least one operation"),
            None => fallback_attribution.clone(),
        };
        changed_configuration.push(ConfigurationImpact {
            transition,
            attribution,
        });
    }
    ImpactEffects {
        changed_configuration: CanonicalImpactSet::new(changed_configuration),
        ..ImpactEffects::default()
    }
}

fn model_keys(before: &ModelIndex, after: &ModelIndex) -> BTreeSet<NodeRef> {
    before.nodes().chain(after.nodes()).cloned().collect()
}

fn effective_pause(
    domain: &DomainName,
    status: &DomainStatus,
    level: QuiesceLevel,
) -> PauseRequirement {
    if !matches!(status, DomainStatus::Running) {
        return PauseRequirement::NoPause;
    }
    match level {
        QuiesceLevel::Dynamic => PauseRequirement::NoPause,
        QuiesceLevel::EntityPause => PauseRequirement::Subgraph {
            scope: QuiesceSubgraph::new(domain.clone(), [], []),
        },
        QuiesceLevel::DomainPause => PauseRequirement::Domain {
            domain: domain.clone(),
        },
    }
}

fn completeness_for_pause(
    operation: TransactionOperationNumber,
    pause: &PauseRequirement,
    mut diagnostics: Vec<ImpactDiagnostic>,
) -> ImpactReportCompleteness {
    if let PauseRequirement::Subgraph { .. } = pause {
        diagnostics.push(ImpactDiagnostic {
            kind: ImpactDiagnosticKind::Topology,
            operation: Some(operation),
            message: "the exact subgraph and gate boundaries are not planned yet".to_string(),
        });
    }
    if diagnostics.is_empty() {
        ImpactReportCompleteness::Complete
    } else {
        ImpactReportCompleteness::incomplete(diagnostics)
            .assured("an incomplete transaction step has at least one diagnostic")
    }
}

fn combined_completeness(steps: &[PlannedTransactionStep]) -> ImpactReportCompleteness {
    let diagnostics = steps
        .iter()
        .flat_map(|step| {
            step.impact
                .planned()
                .completeness
                .diagnostics()
                .iter()
                .cloned()
        })
        .collect::<Vec<_>>();
    if diagnostics.is_empty() {
        ImpactReportCompleteness::Complete
    } else {
        ImpactReportCompleteness::incomplete(diagnostics)
            .assured("the combined incomplete report has diagnostics from an incomplete step")
    }
}

fn operation_report(
    operation: PendingOperationImpact,
    execution_step: TransactionOperationRange,
    completeness: ImpactReportCompleteness,
) -> OperationImpactReport {
    OperationImpactReport {
        number: operation.number,
        operation: operation.operation,
        execution_step,
        completeness,
        reasons: operation.reasons,
        contribution: operation.contribution,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nervix_models::{
        AlterSchema, AlterSchemaOperation, CreateResource, CreateStatement, DomainConfig,
        DomainPace, DomainStartPoint, DropModel, FieldName, ImpactPlanningBasis, Model, ModelKind,
        ParseAsType, ResourceName, SchemaField, Statement,
    };

    use super::*;
    use crate::registry::test_fixtures::{named, relay, schema};

    fn domain_state(status: DomainStatus) -> ControlDomainState {
        ControlDomainState {
            id: named("default"),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                placement: PlacementPolicy::Neutral,
            },
            status,
            start_version: 0,
            last_start: DomainStartPoint::Resume,
            clock: None,
        }
    }

    fn snapshot(
        status: DomainStatus,
        models: impl IntoIterator<Item = Model>,
    ) -> TransactionPlanningSnapshot {
        TransactionPlanningSnapshot {
            domain: domain_state(status),
            models: models.into_iter().collect(),
            resources: BTreeSet::new(),
            schedule: None,
            basis: ImpactPlanningBasis::new([7; 32]),
        }
    }

    fn preserve_schedule(
        _graph: Option<ActiveGraph>,
        _placement: PlacementPolicy,
        current: Option<&DomainSchedule>,
        _attribution: &ImpactAttribution,
    ) -> TransactionScheduleDecision {
        TransactionScheduleDecision {
            schedule: current.cloned(),
            ownership_moves: CanonicalImpactSet::default(),
        }
    }

    fn add_note() -> Statement {
        Statement::AlterSchema(AlterSchema {
            schema: named("events"),
            operations: vec![AlterSchemaOperation::AddField {
                field: SchemaField {
                    name: FieldName::parse("note")
                        .assured("the fixture field is an identifier-shaped literal"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            }],
        })
    }

    fn drop_note() -> Statement {
        Statement::AlterSchema(AlterSchema {
            schema: named("events"),
            operations: vec![AlterSchemaOperation::DropField {
                field: named("note"),
            }],
        })
    }

    #[test]
    fn an_incomplete_model_run_cannot_be_repaired_after_a_resource_boundary() {
        let statements = vec![
            Statement::Create(CreateStatement::new(
                Box::new(relay("events", "missing_schema")),
                false,
            )),
            Statement::CreateResource(CreateStatement::new(
                CreateResource {
                    identifier: ResourceName::parse("weights")
                        .assured("the resource fixture is an identifier-shaped literal"),
                },
                false,
            )),
            Statement::Create(CreateStatement::new(
                Box::new(schema("missing_schema")),
                false,
            )),
        ];

        let error = Registry::plan_transaction(
            snapshot(DomainStatus::Running, []),
            &statements,
            0,
            true,
            preserve_schedule,
        )
        .expect_err("a later run cannot repair the invalid earlier run");
        assert!(matches!(
            error.current_context(),
            TransactionPlanningError::ModelPreflight { operation, .. }
                if operation.get() == 1
        ));
    }

    #[test]
    fn drop_and_recreate_classifies_the_model_run_from_base_to_final() {
        let mut replacement = schema("events");
        let Model::Schema(replacement_schema) = &mut replacement else {
            unreachable!("the schema fixture constructs a schema model");
        };
        replacement_schema.fields.push(SchemaField {
            name: named("note"),
            ty: ParseAsType::String,
            optional: true,
            sensitive: false,
        });
        let statements = vec![
            Statement::Drop(DropModel {
                kind: ModelKind::Schema,
                name: named("events"),
            }),
            Statement::Create(CreateStatement::new(Box::new(replacement), false)),
        ];

        let plan = Registry::plan_transaction(
            snapshot(DomainStatus::Running, [schema("events")]),
            &statements,
            0,
            false,
            preserve_schedule,
        )
        .assured("the final replacement schema is valid");
        let report = plan
            .report()
            .assured("the complete plan has a valid report");
        assert_eq!(report.execution_steps().len(), 1);
        assert_eq!(report.summary().level(), QuiesceLevel::DomainPause);
        let changes = report.execution_steps()[0]
            .planned()
            .effects
            .changed_configuration
            .as_slice();
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            changes[0].transition,
            ConfigurationTransition::Changed { .. }
        ));
        assert!(matches!(
            report.operations()[0]
                .contribution
                .changed_configuration
                .as_slice()[0]
                .transition,
            ConfigurationTransition::Dropped { .. }
        ));
        assert!(matches!(
            report.operations()[1]
                .contribution
                .changed_configuration
                .as_slice()[0]
                .transition,
            ConfigurationTransition::Created { .. }
        ));
    }

    #[test]
    fn cancelling_alters_make_the_atomic_run_a_noop() {
        let plan = Registry::plan_transaction(
            snapshot(DomainStatus::Running, [schema("events")]),
            &[add_note(), drop_note()],
            0,
            false,
            preserve_schedule,
        )
        .assured("the two valid alterations cancel");

        let step = plan
            .first_step()
            .verified("the two alterations form one model run");
        assert_eq!(step.impact.planned().pause.level(), QuiesceLevel::Dynamic);
        assert!(
            step.impact
                .planned()
                .effects
                .changed_configuration
                .is_empty()
        );
        let PlannedTransactionStepKind::Models { plan } = &step.kind else {
            unreachable!("the alterations produce a complete model plan");
        };
        let Some(planned) = &plan.planned else {
            unreachable!("the alterations produce a complete model plan");
        };
        assert!(planned.is_noop());
    }

    #[test]
    fn ownership_moves_raise_a_running_step_to_entity_pause() {
        let moved = relay("events", "event_schema").node_ref();
        let source = nervix_models::ClusterNodeName::parse("node-a")
            .assured("the source node fixture is an identifier-shaped literal");
        let destination = nervix_models::ClusterNodeName::parse("node-b")
            .assured("the destination node fixture is an identifier-shaped literal");
        let statement = Statement::Create(CreateStatement::new(
            Box::new(relay("events", "event_schema")),
            false,
        ));
        let plan = Registry::plan_transaction(
            snapshot(DomainStatus::Running, [schema("event_schema")]),
            &[statement],
            0,
            false,
            move |_graph, _placement, current, attribution| TransactionScheduleDecision {
                schedule: current.cloned(),
                ownership_moves: CanonicalImpactSet::new([OwnershipMoveImpact {
                    node: nervix_models::ImpactNodeCoverage::all_executions(moved.clone()),
                    source: source.clone(),
                    destination: destination.clone(),
                    attribution: attribution.clone(),
                }]),
            },
        )
        .assured("the model creation and captured schedule decision are valid");

        let report = plan
            .report()
            .assured("the complete plan has a valid report");
        assert_eq!(report.summary().level(), QuiesceLevel::EntityPause);
        assert_eq!(
            report.execution_steps()[0]
                .planned()
                .effects
                .ownership_moves
                .len(),
            1
        );
    }

    #[test]
    fn lifecycle_steps_change_the_effective_pause_of_later_model_runs() {
        let plan = Registry::plan_transaction(
            snapshot(DomainStatus::Stopped, [schema("events")]),
            &[
                add_note(),
                Statement::StartDomain(nervix_models::StartDomain {
                    start: DomainStartPoint::Resume,
                }),
                drop_note(),
            ],
            0,
            false,
            preserve_schedule,
        )
        .assured("the ordered lifecycle and model runs are valid");

        assert_eq!(plan.steps().len(), 3);
        assert_eq!(
            plan.steps()[0].impact.planned().pause.level(),
            QuiesceLevel::Dynamic
        );
        assert_eq!(
            plan.steps()[2].impact.planned().pause.level(),
            QuiesceLevel::DomainPause
        );
    }

    #[test]
    fn if_not_exists_uses_the_ordered_prefix_and_keeps_operation_positions() {
        let statements = [
            Statement::Create(CreateStatement::new(Box::new(schema("events")), false)),
            Statement::Create(CreateStatement::new(Box::new(schema("events")), true)),
        ];
        let plan = Registry::plan_transaction(
            snapshot(DomainStatus::Running, []),
            &statements,
            0,
            false,
            preserve_schedule,
        )
        .assured("the second create is an ordered no-op");

        let step = plan.first_step().verified("the creates form one model run");
        assert_eq!(step.impact.operations().first().get(), 1);
        assert_eq!(step.impact.operations().last().get(), 2);
        let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
            unreachable!("the creates produce a model plan");
        };
        assert_eq!(
            &model_plan.no_op_operations,
            &BTreeSet::from([TransactionOperationNumber::from_index(1)
                .assured("the second fixture operation is addressable")])
        );
    }
}
