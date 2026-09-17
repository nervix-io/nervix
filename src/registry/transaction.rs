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
    ActivationAction, ActivationImpact, ActualExecutionStepImpact, AffectedTopology,
    AttributedGateBoundary, AttributedImpactNode, CanonicalImpactSet, ConfigurationImpact,
    ConfigurationTransition, DomainLifecycleAction, DomainLifecycleImpact, DomainName,
    DomainSchedule, DomainState as ControlDomainState, DomainStatus, DropModel, DynamicModelUpdate,
    ExecutionStepImpactReport, ForceFlushImpact, ImpactAttribution, ImpactDiagnostic,
    ImpactDiagnosticKind, ImpactEffects, ImpactNodeCoverage, ImpactPlanningBasis,
    ImpactReportCompleteness, ImpactReportError, ImpactTopology, ImpactTopologyEdge,
    ModelChangeAspect, ModelIndex, NodeRef, OperationImpactReason, OperationImpactReport,
    OwnershipMoveImpact, PauseRequirement, PlacementPolicy, PlannedExecutionStepImpact,
    QuiesceLevel, QuiesceSubgraph, RebuildImpact, RebuildReason, RequestedResourceVersion,
    ResourceBindingImpact, ResourceCatalogAction, ResourceCatalogImpact, ResourceName,
    ResourceUploads, ResourceVersionResolutionError, StateResetImpact, Statement,
    TransactionImpactReport, TransactionOperation, TransactionOperationNumber,
    TransactionOperationRange, TransactionPosition,
};
use thiserror::Error;

use super::graph::{UnattributedImpactEdge, UnattributedImpactTopology};
use crate::registry::{
    ActiveGraph, EntityGatePlan, PlannedMutations, Registry, RegistryError, RegistryMutation,
    ScheduleDelta, entity_pause_relays_for_schedule, gate_boundary, scheduled_impact_coverage,
};

/// Every mutable control-plane input one planning pass may inspect.
#[derive(Debug, Clone)]
pub(crate) struct TransactionPlanningSnapshot {
    pub(crate) domain: ControlDomainState,
    pub(crate) models: ModelIndex,
    pub(crate) resources: BTreeSet<ResourceName>,
    /// The upload outcomes of the planned domain. Every resource version a statement writes is
    /// resolved against these, so `LATEST` names the highest version completed when this snapshot
    /// was captured.
    pub(crate) resource_uploads: ResourceUploads,
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
    pub(crate) model_gate: EntityGatePlan,
    pub(crate) ownership_gate: EntityGatePlan,
}

/// The captured state transition and schedule decision for one domain alteration.
#[derive(Debug, Clone)]
pub(crate) struct PlannedAlterDomainTransactionStep {
    pub(crate) previous: ControlDomainState,
    pub(crate) next: ControlDomainState,
    pub(crate) expected_schedule: Option<DomainSchedule>,
    pub(crate) schedule: Option<DomainSchedule>,
    pub(crate) ownership_gate: EntityGatePlan,
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

    pub(crate) fn operations(&self) -> &[OperationImpactReport] {
        &self.operations
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
    #[error("resource '{resource}' does not exist")]
    ResourceNotFound { resource: ResourceName },
    #[error(
        "{kind} '{name}' does not exist in domain '{domain}'",
        kind = node.kind.keyword_phrase(),
        name = node.identifier.as_str(),
        domain = domain.as_str()
    )]
    RebindMemberNotFound { domain: DomainName, node: NodeRef },
    #[error(
        "{kind} '{name}' does not bind resource '{resource}'",
        kind = node.kind.keyword_phrase(),
        name = node.identifier.as_str(),
        resource = resource.as_str()
    )]
    RebindMemberDoesNotBind {
        node: NodeRef,
        resource: ResourceName,
    },
    #[error("transaction operation {operation} is not valid transaction content")]
    InvalidOperation {
        operation: TransactionOperationNumber,
    },
    #[error("transaction operation {operation} failed resource version resolution: {error}")]
    ResourceVersion {
        operation: TransactionOperationNumber,
        error: ResourceVersionResolutionError,
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
    resources: &'a BTreeSet<ResourceName>,
    resource_uploads: &'a ResourceUploads,
    base_models: ModelIndex,
    current_schedule: Option<DomainSchedule>,
    statements: &'a [Statement],
    first_operation_index: usize,
    allow_incomplete: bool,
}

struct PlannedResourceRebind {
    operation: TransactionOperation,
    contribution: ModelContribution,
    mutations: Vec<RegistryMutation>,
    bindings: Vec<ResourceBindingImpact>,
}

#[derive(Default)]
struct ImpactTopologyBuilder {
    nodes: BTreeMap<ImpactNodeCoverage, BTreeSet<TransactionOperationNumber>>,
    edges: BTreeMap<UnattributedImpactEdge, BTreeSet<TransactionOperationNumber>>,
}

impl ImpactTopologyBuilder {
    fn add(&mut self, topology: UnattributedImpactTopology, attribution: &ImpactAttribution) {
        for node in topology.nodes {
            self.nodes
                .entry(node)
                .or_default()
                .extend(attribution.operations().iter().copied());
        }
        for edge in topology.edges {
            self.edges
                .entry(edge)
                .or_default()
                .extend(attribution.operations().iter().copied());
        }
    }

    fn into_topology(self) -> ImpactTopology {
        let nodes = self
            .nodes
            .into_iter()
            .map(|(coverage, operations)| AttributedImpactNode {
                coverage,
                attribution: impact_attribution(operations),
            });
        let edges = self
            .edges
            .into_iter()
            .map(|(edge, operations)| ImpactTopologyEdge {
                source: edge.source,
                target: edge.target,
                kind: edge.kind,
                attribution: impact_attribution(operations),
            });
        ImpactTopology {
            nodes: CanonicalImpactSet::new(nodes),
            edges: CanonicalImpactSet::new(edges),
        }
    }
}

struct CompletedModelImpact {
    effects: ImpactEffects,
    subgraph: QuiesceSubgraph,
    model_gate: EntityGatePlan,
    ownership_gate: EntityGatePlan,
}

struct ModelImpactInput<'a> {
    domain: &'a DomainName,
    before: &'a ModelIndex,
    after: &'a ModelIndex,
    touched_by_node: &'a BTreeMap<NodeRef, Vec<TransactionOperationNumber>>,
    fallback_attribution: &'a ImpactAttribution,
    planned: Option<&'a PlannedMutations>,
    current_schedule: Option<&'a DomainSchedule>,
    next_schedule: Option<&'a DomainSchedule>,
    ownership_moves: &'a CanonicalImpactSet<OwnershipMoveImpact>,
    schedule_delta: &'a ScheduleDelta,
    running: bool,
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
        let resource_uploads = snapshot.resource_uploads;
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
                    resources: &resources,
                    resource_uploads: &resource_uploads,
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
                    let active_graph = if changed {
                        Some(
                            crate::registry::domain_state::DomainState::build(&domain, &models)
                                .map_err(|error| {
                                    Report::new(TransactionPlanningError::ModelPreflight {
                                        operation: number,
                                        error,
                                    })
                                })?
                                .graph,
                        )
                    } else {
                        None
                    };
                    let schedule_decision = if let Some(graph) = &active_graph {
                        schedule(
                            (graph.node_count() > 0).then_some(graph.clone()),
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
                    let completed = complete_placement_impact(
                        &domain,
                        active_graph.as_ref(),
                        expected_schedule.as_ref(),
                        schedule_decision.schedule.as_ref(),
                        &ownership_moves,
                        &attribution,
                        matches!(domain_state.status, DomainStatus::Running),
                    );
                    pause = effective_pause(
                        &domain,
                        &domain_state.status,
                        if ownership_moves.is_empty() {
                            QuiesceLevel::Dynamic
                        } else {
                            QuiesceLevel::EntityPause
                        },
                        completed.subgraph,
                    );
                    reasons = if changed {
                        vec![OperationImpactReason::DomainPlacement]
                    } else {
                        Vec::new()
                    };
                    contribution = completed.effects;
                    current_schedule = schedule_decision.schedule.clone();
                    kind = PlannedTransactionStepKind::AlterDomain {
                        plan: Box::new(PlannedAlterDomainTransactionStep {
                            previous,
                            next: domain_state.clone(),
                            expected_schedule,
                            schedule: schedule_decision.schedule,
                            ownership_gate: completed.ownership_gate,
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
            let completeness = completeness_for_pause(Vec::new());
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
            resources,
            resource_uploads,
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
        // The resource versions the latest create of each node bound. Dropping the node removes
        // its entry, so what remains after the run describes the models the run leaves behind.
        let mut bindings_by_node = BTreeMap::<NodeRef, Vec<ResourceBindingImpact>>::new();

        for (run_index, statement) in statements.iter().enumerate() {
            let absolute_index = first_operation_index
                .checked_add(run_index)
                .assured("a model run is a range of this transaction");
            let number =
                TransactionOperationNumber::from_index(absolute_index).map_err(|error| {
                    Report::new(TransactionPlanningError::InvalidImpactReport { error })
                })?;
            if let Statement::RebindResource(rebind) = statement {
                let planned = plan_resource_rebind(
                    domain,
                    resources,
                    resource_uploads,
                    &mut prefix_models,
                    rebind,
                    number,
                )?;
                if planned.contribution.touched_nodes.is_empty() {
                    no_op_operations.insert(number);
                }
                for binding in &planned.bindings {
                    bindings_by_node.insert(binding.node.clone(), vec![binding.clone()]);
                }
                for node in &planned.contribution.touched_nodes {
                    touched_by_node
                        .entry(node.clone())
                        .or_default()
                        .push(number);
                }
                mutations.extend(planned.mutations);
                pending_operations.push(PendingOperationImpact {
                    number,
                    operation: planned.operation,
                    reasons: planned.contribution.reasons,
                    contribution: planned.contribution.effects,
                });
                continue;
            }

            let operation = model_operation(domain, statement, number)?;
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
                let written = RegistryMutation::try_from(statement).map_err(|_| {
                    Report::new(TransactionPlanningError::InvalidOperation { operation: number })
                })?;
                let target = written.target_key();
                let pinned = pin_written_resource_versions(PinnedResourceVersionsInput {
                    domain,
                    resource_uploads,
                    target: &target,
                    operation: number,
                    written,
                })?;
                match &pinned.mutation {
                    RegistryMutation::Create(_) => {
                        bindings_by_node.insert(target, pinned.bindings.clone());
                    }
                    RegistryMutation::Drop(_) => {
                        bindings_by_node.remove(&target);
                    }
                    _ => {}
                }
                let before = prefix_models.clone();
                pinned.mutation.fold_into_models(&mut prefix_models);
                let mut contribution = model_contribution(&before, &prefix_models, number);
                contribution.effects.resource_bindings = CanonicalImpactSet::new(pinned.bindings);
                mutations.push(pinned.mutation);
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
        let mut diagnostics = Vec::new();
        if let Some(reason) = &incomplete_reason {
            diagnostics.push(ImpactDiagnostic {
                kind: ImpactDiagnosticKind::Planning,
                operation: Some(range.first()),
                message: reason.to_string(),
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
        let schedule_delta =
            ScheduleDelta::between(expected_schedule.as_ref(), next_schedule.as_ref());
        let ownership_level = if ownership_moves.is_empty() {
            QuiesceLevel::Dynamic
        } else {
            QuiesceLevel::EntityPause
        };
        let effective_level = base_level
            .max(ownership_level)
            .max(schedule_delta.quiesce_level());
        let completed = complete_model_impact(ModelImpactInput {
            domain,
            before: &base_models,
            after: &candidate_models,
            touched_by_node: &touched_by_node,
            fallback_attribution: &attribution,
            planned: planned.as_ref(),
            current_schedule: expected_schedule.as_ref(),
            next_schedule: next_schedule.as_ref(),
            ownership_moves: &ownership_moves,
            schedule_delta: &schedule_delta,
            running: matches!(domain_state.status, DomainStatus::Running),
        });
        let pause = if incomplete_reason.is_some() {
            PauseRequirement::NoPause
        } else {
            effective_pause(
                domain,
                &domain_state.status,
                effective_level,
                completed.subgraph.clone(),
            )
        };
        let completeness = completeness_for_pause(diagnostics);
        let mut step_bindings = Vec::new();
        for (node, bindings) in bindings_by_node {
            if candidate_models.contains(&node) {
                step_bindings.extend(bindings);
            }
        }
        let mut effects = completed.effects;
        effects.resource_bindings = CanonicalImpactSet::new(step_bindings);
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
                        model_gate: completed.model_gate,
                        ownership_gate: completed.ownership_gate,
                    }),
                },
            },
            candidate_models,
            schedule: next_schedule,
        })
    }
}

/// A written mutation together with everything needed to pin the resource versions it names.
struct PinnedResourceVersionsInput<'a> {
    domain: &'a DomainName,
    resource_uploads: &'a ResourceUploads,
    target: &'a NodeRef,
    operation: TransactionOperationNumber,
    written: RegistryMutation<RequestedResourceVersion>,
}

/// A mutation whose resource versions are pinned, with one binding per version it names.
struct PinnedResourceVersions {
    mutation: RegistryMutation,
    bindings: Vec<ResourceBindingImpact>,
}

/// Resolves every resource version a statement wrote against the captured upload outcomes of its
/// domain. An explicit number has to be a completed version, and `LATEST` becomes the highest
/// completed one.
fn pin_written_resource_versions(
    input: PinnedResourceVersionsInput<'_>,
) -> Result<PinnedResourceVersions, Report<TransactionPlanningError>> {
    let PinnedResourceVersionsInput {
        domain,
        resource_uploads,
        target,
        operation,
        written,
    } = input;
    let mut bindings = Vec::new();
    let pinned = written.pin_resource_versions(|resource, requested| {
        let id = resource_uploads.resolve_completed_version(domain, resource, requested)?;
        bindings.push(ResourceBindingImpact {
            node: target.clone(),
            resource: resource.clone(),
            requested,
            version: id.version,
            attribution: ImpactAttribution::single(operation),
        });
        Ok::<u64, Report<ResourceVersionResolutionError>>(id.version)
    });
    let mutation = match pinned {
        Ok(mutation) => mutation,
        Err(error) => {
            let resolution = error.current_context().clone();
            return Err(
                error.change_context(TransactionPlanningError::ResourceVersion {
                    operation,
                    error: resolution,
                }),
            );
        }
    };
    Ok(PinnedResourceVersions { mutation, bindings })
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

fn plan_resource_rebind(
    domain: &DomainName,
    resources: &BTreeSet<ResourceName>,
    resource_uploads: &ResourceUploads,
    prefix_models: &mut ModelIndex,
    rebind: &nervix_models::RebindResource,
    operation: TransactionOperationNumber,
) -> Result<PlannedResourceRebind, Report<TransactionPlanningError>> {
    if !resources.contains(&rebind.resource) {
        return Err(Report::new(TransactionPlanningError::ResourceNotFound {
            resource: rebind.resource.clone(),
        }));
    }
    let resolved = resource_uploads
        .resolve_completed_version(domain, &rebind.resource, rebind.version)
        .map_err(|error| {
            let resolution = error.current_context().clone();
            error.change_context(TransactionPlanningError::ResourceVersion {
                operation,
                error: resolution,
            })
        })?;
    let selected: BTreeSet<NodeRef> = match &rebind.selection {
        nervix_models::RebindResourceSelection::Members(members) => {
            members.iter().cloned().collect()
        }
        nervix_models::RebindResourceSelection::All => prefix_models
            .iter()
            .filter_map(|(node, model)| {
                model
                    .resource_version(&rebind.resource)
                    .map(|_| node.clone())
            })
            .collect(),
    };
    let before = prefix_models.clone();
    let mut reasons = Vec::with_capacity(selected.len());
    let mut bindings = Vec::with_capacity(selected.len());
    let mut mutations = Vec::new();

    for node in selected {
        let model = prefix_models.get(&node).ok_or_else(|| {
            Report::new(TransactionPlanningError::RebindMemberNotFound {
                domain: domain.clone(),
                node: node.clone(),
            })
        })?;
        let rebound = model
            .rebind_resource(&rebind.resource, resolved.version)
            .ok_or_else(|| {
                Report::new(TransactionPlanningError::RebindMemberDoesNotBind {
                    node: node.clone(),
                    resource: rebind.resource.clone(),
                })
            })?;
        reasons.push(OperationImpactReason::ResourceRebinding {
            node: node.clone(),
            resource: rebind.resource.clone(),
            from_version: rebound.previous_version,
            to_version: resolved.version,
        });
        bindings.push(ResourceBindingImpact {
            node: node.clone(),
            resource: rebind.resource.clone(),
            requested: rebind.version,
            version: resolved.version,
            attribution: ImpactAttribution::single(operation),
        });
        if rebound.previous_version == resolved.version {
            continue;
        }
        mutations.push(RegistryMutation::Drop(DropModel {
            kind: node.kind,
            name: node.identifier.clone(),
        }));
        mutations.push(RegistryMutation::Create(Box::new(rebound.model.clone())));
        prefix_models.insert(rebound.model);
    }

    let mut contribution = model_contribution(&before, prefix_models, operation);
    contribution.reasons = reasons;
    contribution.effects.resource_bindings = CanonicalImpactSet::new(bindings.clone());
    Ok(PlannedResourceRebind {
        operation: TransactionOperation::RebindResource {
            domain: domain.clone(),
            resource: rebind.resource.clone(),
            requested: rebind.version,
            version: resolved.version,
        },
        contribution,
        mutations,
        bindings,
    })
}

fn model_operation(
    domain: &DomainName,
    statement: &Statement,
    operation: TransactionOperationNumber,
) -> Result<TransactionOperation, Report<TransactionPlanningError>> {
    let mutation = RegistryMutation::<RequestedResourceVersion>::try_from(statement)
        .map_err(|_| Report::new(TransactionPlanningError::InvalidOperation { operation }))?;
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

fn complete_model_impact(input: ModelImpactInput<'_>) -> CompletedModelImpact {
    let ModelImpactInput {
        domain,
        before,
        after,
        touched_by_node,
        fallback_attribution,
        planned,
        current_schedule,
        next_schedule,
        ownership_moves,
        schedule_delta,
        running,
    } = input;
    let mut effects = model_step_effects(before, after, touched_by_node, fallback_attribution);
    effects.ownership_moves = ownership_moves.clone();

    let mut before_topology = ImpactTopologyBuilder::default();
    let mut after_topology = ImpactTopologyBuilder::default();
    let mut pause_nodes =
        BTreeMap::<ImpactNodeCoverage, BTreeSet<TransactionOperationNumber>>::new();
    let mut pause_entity_operations =
        BTreeMap::<NodeRef, BTreeSet<TransactionOperationNumber>>::new();
    let mut state_resets = Vec::new();

    if let Some(planned) = planned {
        for change in planned.quiesce().changed() {
            let attribution = attribution_for_node(
                &change.node,
                touched_by_node,
                fallback_attribution,
                ownership_moves,
            );
            let before_impact = impact_for_change(planned.base_graph(), &change.node, change.level);
            let after_impact =
                impact_for_change(planned.resulting_graph(), &change.node, change.level);
            if change.level.requires_entity_pause() {
                add_pause_nodes(
                    &mut pause_nodes,
                    &mut pause_entity_operations,
                    &before_impact,
                    &attribution,
                );
                add_pause_nodes(
                    &mut pause_nodes,
                    &mut pause_entity_operations,
                    &after_impact,
                    &attribution,
                );
            }
            before_topology.add(
                planned
                    .base_graph()
                    .with_configuration_dependencies(&before_impact),
                &attribution,
            );
            after_topology.add(
                planned
                    .resulting_graph()
                    .with_configuration_dependencies(&after_impact),
                &attribution,
            );

            let coverage = coverage_for_node(planned.resulting_graph(), &change.node)
                .or_else(|| coverage_for_node(planned.base_graph(), &change.node));
            if let Some(coverage) = coverage {
                state_resets.extend(change.state_resets.iter().copied().map(|state| {
                    StateResetImpact {
                        node: coverage.clone(),
                        state,
                        attribution: attribution.clone(),
                    }
                }));
            }
        }
    }

    let mut activations = Vec::new();
    let mut rebuilds = Vec::new();
    if let Some(planned) = planned {
        add_schedule_impact(
            schedule_delta,
            planned,
            touched_by_node,
            fallback_attribution,
            ownership_moves,
            &mut before_topology,
            &mut after_topology,
            &mut activations,
            &mut rebuilds,
        );
    }

    let mut model_gate_entities = BTreeSet::new();
    let mut model_gate_roots = BTreeSet::new();
    if let Some(planned) = planned
        && planned.quiesce().level().requires_entity_pause()
    {
        model_gate_entities.extend(planned.quiesce().affected_entities().iter().cloned());
        model_gate_roots.extend(
            planned
                .quiesce()
                .changed()
                .iter()
                .filter(|change| change.level.requires_entity_pause())
                .map(|change| change.node.clone()),
        );
    }
    let schedule_gate_entities = schedule_delta.entity_gate_entities();
    model_gate_entities.extend(schedule_gate_entities.iter().cloned());
    model_gate_roots.extend(schedule_gate_entities);
    let model_gate = if running
        && schedule_delta.quiesce_level() != QuiesceLevel::DomainPause
        && !model_gate_entities.is_empty()
    {
        EntityGatePlan::for_model_change(current_schedule, model_gate_entities, model_gate_roots)
    } else {
        EntityGatePlan::default()
    };
    add_model_gate_pause_nodes(
        &model_gate,
        current_schedule,
        fallback_attribution,
        &mut pause_nodes,
        &mut pause_entity_operations,
    );
    let moved_entities = ownership_moves
        .as_slice()
        .iter()
        .map(|moved| moved.node.node.clone())
        .collect::<Vec<_>>();
    let ownership_gate =
        EntityGatePlan::for_ownership_handoff(current_schedule, next_schedule, moved_entities);
    if running {
        add_ownership_pause_nodes(
            &ownership_gate,
            current_schedule,
            next_schedule,
            ownership_moves,
            fallback_attribution,
            &mut pause_nodes,
            &mut pause_entity_operations,
        );
    }

    let no_ownership_gate = EntityGatePlan::default();
    let reported_ownership_gate = if running {
        &ownership_gate
    } else {
        &no_ownership_gate
    };
    let gate_boundaries = attributed_gate_boundaries(
        &model_gate,
        reported_ownership_gate,
        current_schedule,
        next_schedule,
        &pause_entity_operations,
        fallback_attribution,
    );
    let attributed_nodes =
        pause_nodes
            .into_iter()
            .map(|(coverage, operations)| AttributedImpactNode {
                coverage,
                attribution: impact_attribution(operations),
            });
    let subgraph = QuiesceSubgraph::new(domain.clone(), attributed_nodes, gate_boundaries);

    let should_force_flush = running
        && (planned.is_some_and(|planned| planned.quiesce().level() != QuiesceLevel::Dynamic)
            || !ownership_moves.is_empty()
            || matches!(
                schedule_delta,
                ScheduleDelta::EntitySwap { .. } | ScheduleDelta::Rebuild
            ));
    if should_force_flush && let Some(planned) = planned {
        let force_flushes = planned
            .base_graph()
            .whole_impact()
            .nodes
            .into_iter()
            .filter(|coverage| coverage.branches.is_some())
            .map(|node| ForceFlushImpact {
                node,
                attribution: fallback_attribution.clone(),
            });
        effects.force_flushes = CanonicalImpactSet::new(force_flushes);
    }

    effects.topology = AffectedTopology {
        before: before_topology.into_topology(),
        after: after_topology.into_topology(),
    };
    effects.activations = CanonicalImpactSet::new(activations);
    effects.rebuilds = CanonicalImpactSet::new(rebuilds);
    effects.state_resets = CanonicalImpactSet::new(state_resets);
    CompletedModelImpact {
        effects,
        subgraph,
        model_gate,
        ownership_gate,
    }
}

fn complete_placement_impact(
    domain: &DomainName,
    graph: Option<&ActiveGraph>,
    current_schedule: Option<&DomainSchedule>,
    next_schedule: Option<&DomainSchedule>,
    ownership_moves: &CanonicalImpactSet<OwnershipMoveImpact>,
    attribution: &ImpactAttribution,
    running: bool,
) -> CompletedModelImpact {
    let moved_entities = ownership_moves
        .as_slice()
        .iter()
        .map(|moved| moved.node.node.clone());
    let ownership_gate =
        EntityGatePlan::for_ownership_handoff(current_schedule, next_schedule, moved_entities);
    let mut pause_nodes =
        BTreeMap::<ImpactNodeCoverage, BTreeSet<TransactionOperationNumber>>::new();
    let mut pause_entity_operations =
        BTreeMap::<NodeRef, BTreeSet<TransactionOperationNumber>>::new();
    if running {
        add_ownership_pause_nodes(
            &ownership_gate,
            current_schedule,
            next_schedule,
            ownership_moves,
            attribution,
            &mut pause_nodes,
            &mut pause_entity_operations,
        );
    }
    let no_ownership_gate = EntityGatePlan::default();
    let reported_ownership_gate = if running {
        &ownership_gate
    } else {
        &no_ownership_gate
    };
    let gate_boundaries = attributed_gate_boundaries(
        &EntityGatePlan::default(),
        reported_ownership_gate,
        current_schedule,
        next_schedule,
        &pause_entity_operations,
        attribution,
    );
    let subgraph = QuiesceSubgraph::new(
        domain.clone(),
        pause_nodes
            .into_iter()
            .map(|(coverage, operations)| AttributedImpactNode {
                coverage,
                attribution: impact_attribution(operations),
            }),
        gate_boundaries,
    );

    let mut effects = ImpactEffects {
        ownership_moves: ownership_moves.clone(),
        ..ImpactEffects::default()
    };
    if let Some(graph) = graph {
        let affected = ownership_gate
            .affected_entities()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let affected = graph.nodes_impact(&affected);
        let topology = graph.with_configuration_dependencies(&affected);
        let mut before = ImpactTopologyBuilder::default();
        before.add(topology.clone(), attribution);
        let mut after = ImpactTopologyBuilder::default();
        after.add(topology, attribution);
        effects.topology = AffectedTopology {
            before: before.into_topology(),
            after: after.into_topology(),
        };
        if running && !ownership_moves.is_empty() {
            effects.force_flushes = CanonicalImpactSet::new(
                graph
                    .whole_impact()
                    .nodes
                    .into_iter()
                    .filter(|coverage| coverage.branches.is_some())
                    .map(|node| ForceFlushImpact {
                        node,
                        attribution: attribution.clone(),
                    }),
            );
        }
    }

    let mut activations = Vec::new();
    let mut rebuilds = Vec::new();
    for moved in ownership_moves {
        let before = scheduled_coverage(current_schedule, &moved.node.node);
        let after = scheduled_coverage(next_schedule, &moved.node.node);
        if let Some(node) = before {
            activations.push(ActivationImpact {
                node,
                action: ActivationAction::Deactivate,
                attribution: moved.attribution.clone(),
            });
        }
        if let Some(node) = after {
            activations.push(ActivationImpact {
                node: node.clone(),
                action: ActivationAction::Activate,
                attribution: moved.attribution.clone(),
            });
            rebuilds.push(RebuildImpact {
                node,
                reason: RebuildReason::Ownership,
                attribution: moved.attribution.clone(),
            });
        }
    }
    effects.activations = CanonicalImpactSet::new(activations);
    effects.rebuilds = CanonicalImpactSet::new(rebuilds);
    CompletedModelImpact {
        effects,
        subgraph,
        model_gate: EntityGatePlan::default(),
        ownership_gate,
    }
}

fn scheduled_coverage(
    schedule: Option<&DomainSchedule>,
    entity: &NodeRef,
) -> Option<ImpactNodeCoverage> {
    let schedule = schedule?;
    let node = schedule.nodes.get(entity)?;
    Some(scheduled_impact_coverage(node))
}

fn impact_for_change(
    graph: &ActiveGraph,
    node: &NodeRef,
    level: QuiesceLevel,
) -> UnattributedImpactTopology {
    match level {
        QuiesceLevel::Dynamic => graph.node_impact(node),
        QuiesceLevel::EntityPause => graph.downstream_impact(node),
        QuiesceLevel::DomainPause => graph.whole_impact(),
    }
}

fn add_pause_nodes(
    nodes: &mut BTreeMap<ImpactNodeCoverage, BTreeSet<TransactionOperationNumber>>,
    by_entity: &mut BTreeMap<NodeRef, BTreeSet<TransactionOperationNumber>>,
    topology: &UnattributedImpactTopology,
    attribution: &ImpactAttribution,
) {
    for coverage in topology
        .nodes
        .iter()
        .filter(|coverage| coverage.branches.is_some())
    {
        let operations = attribution.operations().iter().copied();
        nodes
            .entry(coverage.clone())
            .or_default()
            .extend(operations.clone());
        by_entity
            .entry(coverage.node.clone())
            .or_default()
            .extend(operations);
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the schedule effect projection writes each independent report role into its owner"
)]
fn add_schedule_impact(
    delta: &ScheduleDelta,
    planned: &PlannedMutations,
    touched_by_node: &BTreeMap<NodeRef, Vec<TransactionOperationNumber>>,
    fallback_attribution: &ImpactAttribution,
    ownership_moves: &CanonicalImpactSet<OwnershipMoveImpact>,
    before_topology: &mut ImpactTopologyBuilder,
    after_topology: &mut ImpactTopologyBuilder,
    activations: &mut Vec<ActivationImpact>,
    rebuilds: &mut Vec<RebuildImpact>,
) {
    match delta {
        ScheduleDelta::Unchanged => {}
        ScheduleDelta::Dynamic(updates) => {
            for node in dynamic_update_nodes(updates) {
                let attribution = attribution_for_node(
                    &node,
                    touched_by_node,
                    fallback_attribution,
                    ownership_moves,
                );
                let before_impact = planned.base_graph().node_impact(&node);
                before_topology.add(
                    planned
                        .base_graph()
                        .with_configuration_dependencies(&before_impact),
                    &attribution,
                );
                let after_impact = planned.resulting_graph().node_impact(&node);
                if let Some(coverage) = after_impact.nodes.iter().next().cloned() {
                    activations.push(ActivationImpact {
                        node: coverage,
                        action: ActivationAction::Activate,
                        attribution: attribution.clone(),
                    });
                }
                after_topology.add(
                    planned
                        .resulting_graph()
                        .with_configuration_dependencies(&after_impact),
                    &attribution,
                );
            }
        }
        ScheduleDelta::EntitySwap {
            entities,
            reassignments,
            dynamic_updates,
        } => {
            for (nodes, reason) in [
                (entities.as_slice(), RebuildReason::Configuration),
                (reassignments.as_slice(), RebuildReason::Ownership),
            ] {
                for node in nodes {
                    let attribution = attribution_for_node(
                        node,
                        touched_by_node,
                        fallback_attribution,
                        ownership_moves,
                    );
                    let before_impact = planned.base_graph().node_impact(node);
                    let after_impact = planned.resulting_graph().node_impact(node);
                    add_activation_pair(
                        &before_impact,
                        &after_impact,
                        reason,
                        &attribution,
                        activations,
                        rebuilds,
                    );
                    before_topology.add(
                        planned
                            .base_graph()
                            .with_configuration_dependencies(&before_impact),
                        &attribution,
                    );
                    after_topology.add(
                        planned
                            .resulting_graph()
                            .with_configuration_dependencies(&after_impact),
                        &attribution,
                    );
                }
            }
            for node in dynamic_update_nodes(dynamic_updates) {
                if entities.contains(&node) || reassignments.contains(&node) {
                    continue;
                }
                let attribution = attribution_for_node(
                    &node,
                    touched_by_node,
                    fallback_attribution,
                    ownership_moves,
                );
                let after_impact = planned.resulting_graph().node_impact(&node);
                if let Some(coverage) = after_impact.nodes.iter().next().cloned() {
                    activations.push(ActivationImpact {
                        node: coverage,
                        action: ActivationAction::Activate,
                        attribution: attribution.clone(),
                    });
                }
                let before_impact = planned.base_graph().node_impact(&node);
                before_topology.add(
                    planned
                        .base_graph()
                        .with_configuration_dependencies(&before_impact),
                    &attribution,
                );
                after_topology.add(
                    planned
                        .resulting_graph()
                        .with_configuration_dependencies(&after_impact),
                    &attribution,
                );
            }
        }
        ScheduleDelta::Rebuild => {
            let before_impact = planned.base_graph().whole_impact();
            let after_impact = planned.resulting_graph().whole_impact();
            for coverage in before_impact
                .nodes
                .iter()
                .filter(|coverage| coverage.branches.is_some())
            {
                activations.push(ActivationImpact {
                    node: coverage.clone(),
                    action: ActivationAction::Deactivate,
                    attribution: fallback_attribution.clone(),
                });
            }
            for coverage in after_impact
                .nodes
                .iter()
                .filter(|coverage| coverage.branches.is_some())
            {
                activations.push(ActivationImpact {
                    node: coverage.clone(),
                    action: ActivationAction::Activate,
                    attribution: fallback_attribution.clone(),
                });
                rebuilds.push(RebuildImpact {
                    node: coverage.clone(),
                    reason: RebuildReason::Configuration,
                    attribution: fallback_attribution.clone(),
                });
                if !ownership_moves.is_empty() {
                    rebuilds.push(RebuildImpact {
                        node: coverage.clone(),
                        reason: RebuildReason::Ownership,
                        attribution: merged_ownership_attribution(
                            ownership_moves,
                            fallback_attribution,
                        ),
                    });
                }
            }
            before_topology.add(before_impact, fallback_attribution);
            after_topology.add(after_impact, fallback_attribution);
        }
    }
}

fn add_activation_pair(
    before: &UnattributedImpactTopology,
    after: &UnattributedImpactTopology,
    reason: RebuildReason,
    attribution: &ImpactAttribution,
    activations: &mut Vec<ActivationImpact>,
    rebuilds: &mut Vec<RebuildImpact>,
) {
    if let Some(coverage) = before.nodes.iter().next().cloned() {
        activations.push(ActivationImpact {
            node: coverage,
            action: ActivationAction::Deactivate,
            attribution: attribution.clone(),
        });
    }
    if let Some(coverage) = after.nodes.iter().next().cloned() {
        activations.push(ActivationImpact {
            node: coverage.clone(),
            action: ActivationAction::Activate,
            attribution: attribution.clone(),
        });
        rebuilds.push(RebuildImpact {
            node: coverage,
            reason,
            attribution: attribution.clone(),
        });
    }
}

fn dynamic_update_nodes(updates: &[DynamicModelUpdate]) -> BTreeSet<NodeRef> {
    updates
        .iter()
        .map(|update| match update {
            DynamicModelUpdate::RelayCapacity { relay, .. } => {
                NodeRef::new(nervix_models::ModelKind::Relay, relay.clone())
            }
            DynamicModelUpdate::Processor { kind, processor } => {
                NodeRef::new(*kind, processor.clone())
            }
            DynamicModelUpdate::Emitter { emitter, .. } => {
                NodeRef::new(nervix_models::ModelKind::Emitter, emitter.clone())
            }
        })
        .collect()
}

fn add_ownership_pause_nodes(
    gate: &EntityGatePlan,
    current: Option<&DomainSchedule>,
    desired: Option<&DomainSchedule>,
    ownership_moves: &CanonicalImpactSet<OwnershipMoveImpact>,
    fallback_attribution: &ImpactAttribution,
    nodes: &mut BTreeMap<ImpactNodeCoverage, BTreeSet<TransactionOperationNumber>>,
    by_entity: &mut BTreeMap<NodeRef, BTreeSet<TransactionOperationNumber>>,
) {
    let attribution = merged_ownership_attribution(ownership_moves, fallback_attribution);
    for entity in gate.affected_entities() {
        let moved = ownership_moves
            .as_slice()
            .iter()
            .find(|moved| &moved.node.node == entity);
        let entity_attribution = match moved {
            Some(moved) => &moved.attribution,
            None => &attribution,
        };
        by_entity
            .entry(entity.clone())
            .or_default()
            .extend(entity_attribution.operations().iter().copied());
        for schedule in [current, desired].into_iter().flatten() {
            let Some(scheduled) = schedule.nodes.get(entity) else {
                continue;
            };
            nodes
                .entry(scheduled_impact_coverage(scheduled))
                .or_default()
                .extend(entity_attribution.operations().iter().copied());
        }
    }
}

fn add_model_gate_pause_nodes(
    gate: &EntityGatePlan,
    current: Option<&DomainSchedule>,
    fallback_attribution: &ImpactAttribution,
    nodes: &mut BTreeMap<ImpactNodeCoverage, BTreeSet<TransactionOperationNumber>>,
    by_entity: &mut BTreeMap<NodeRef, BTreeSet<TransactionOperationNumber>>,
) {
    let Some(schedule) = current else {
        return;
    };
    for entity in gate.affected_entities() {
        let operations = by_entity
            .get(entity)
            .cloned()
            .unwrap_or_else(|| fallback_attribution.operations().iter().copied().collect());
        by_entity
            .entry(entity.clone())
            .or_default()
            .extend(operations.iter().copied());
        let Some(scheduled) = schedule.nodes.get(entity) else {
            continue;
        };
        nodes
            .entry(scheduled_impact_coverage(scheduled))
            .or_default()
            .extend(operations);
    }
}

fn attributed_gate_boundaries(
    model_gate: &EntityGatePlan,
    ownership_gate: &EntityGatePlan,
    current: Option<&DomainSchedule>,
    desired: Option<&DomainSchedule>,
    entity_operations: &BTreeMap<NodeRef, BTreeSet<TransactionOperationNumber>>,
    fallback_attribution: &ImpactAttribution,
) -> Vec<AttributedGateBoundary> {
    if current.is_none() && desired.is_none() {
        return Vec::new();
    }
    let mut boundaries =
        BTreeMap::<nervix_models::ImpactGateBoundary, BTreeSet<TransactionOperationNumber>>::new();
    for (gate, schedules) in [
        (model_gate, [current, desired]),
        (ownership_gate, [current, None]),
    ] {
        for entity in gate.affected_entities() {
            let operations = entity_operations
                .get(entity)
                .cloned()
                .unwrap_or_else(|| fallback_attribution.operations().iter().copied().collect());
            for schedule in schedules.into_iter().flatten() {
                for relay in
                    entity_pause_relays_for_schedule(schedule, std::slice::from_ref(entity))
                {
                    if !gate.relays().contains(&relay) {
                        continue;
                    }
                    let boundary = gate_boundary_for_schedules(current, desired, &relay);
                    let Some(boundary) = boundary else {
                        continue;
                    };
                    boundaries
                        .entry(boundary)
                        .or_default()
                        .extend(operations.iter().copied());
                }
            }
        }
    }
    boundaries
        .into_iter()
        .map(|(boundary, operations)| AttributedGateBoundary {
            boundary,
            attribution: impact_attribution(operations),
        })
        .collect()
}

fn gate_boundary_for_schedules(
    current: Option<&DomainSchedule>,
    desired: Option<&DomainSchedule>,
    relay: &nervix_models::RelayName,
) -> Option<nervix_models::ImpactGateBoundary> {
    if let Some(schedule) = current
        && let Some(boundary) = gate_boundary(schedule, relay)
    {
        return Some(boundary);
    }
    if let Some(schedule) = desired {
        return gate_boundary(schedule, relay);
    }
    None
}

fn coverage_for_node(graph: &ActiveGraph, node: &NodeRef) -> Option<ImpactNodeCoverage> {
    graph.node_impact(node).nodes.into_iter().next()
}

fn attribution_for_node(
    node: &NodeRef,
    touched_by_node: &BTreeMap<NodeRef, Vec<TransactionOperationNumber>>,
    fallback: &ImpactAttribution,
    ownership_moves: &CanonicalImpactSet<OwnershipMoveImpact>,
) -> ImpactAttribution {
    if let Some(operations) = touched_by_node.get(node) {
        return ImpactAttribution::new(operations.iter().copied())
            .assured("every touched node has at least one contributing operation");
    }
    if let Some(moved) = ownership_moves
        .as_slice()
        .iter()
        .find(|moved| &moved.node.node == node)
    {
        return moved.attribution.clone();
    }
    fallback.clone()
}

fn merged_ownership_attribution(
    ownership_moves: &CanonicalImpactSet<OwnershipMoveImpact>,
    fallback: &ImpactAttribution,
) -> ImpactAttribution {
    let operations = ownership_moves
        .as_slice()
        .iter()
        .flat_map(|moved| moved.attribution.operations().iter().copied())
        .collect::<BTreeSet<_>>();
    if operations.is_empty() {
        fallback.clone()
    } else {
        impact_attribution(operations)
    }
}

fn impact_attribution(
    operations: impl IntoIterator<Item = TransactionOperationNumber>,
) -> ImpactAttribution {
    ImpactAttribution::new(operations)
        .assured("every attributed impact item has at least one contributing operation")
}

fn model_keys(before: &ModelIndex, after: &ModelIndex) -> BTreeSet<NodeRef> {
    before.nodes().chain(after.nodes()).cloned().collect()
}

fn effective_pause(
    domain: &DomainName,
    status: &DomainStatus,
    level: QuiesceLevel,
    subgraph: QuiesceSubgraph,
) -> PauseRequirement {
    if !matches!(status, DomainStatus::Running) {
        return PauseRequirement::NoPause;
    }
    match level {
        QuiesceLevel::Dynamic => PauseRequirement::NoPause,
        QuiesceLevel::EntityPause => PauseRequirement::Subgraph { scope: subgraph },
        QuiesceLevel::DomainPause => PauseRequirement::Domain {
            domain: domain.clone(),
        },
    }
}

fn completeness_for_pause(diagnostics: Vec<ImpactDiagnostic>) -> ImpactReportCompleteness {
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
mod rebind_tests;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nervix_models::{
        AckMode, AlterJunction, AlterProcessorOperation, AlterRelay, AlterRelayOperation,
        AlterSchema, AlterSchemaOperation, BranchSelection, ClusterNodeName,
        ConcreteBranchCoverage, CreateResource, CreateStatement, CreateVhost, DomainConfig,
        DomainPace, DomainStartPoint, DropModel, FieldName, ImpactEdgeKind, ImpactPlanningBasis,
        Model, ModelKind, OutputBranch, ParseAsType, ResourceId, ResourceName, ResourceUpload,
        ResourceUploadIdentity, ResourceUploadKey, ResourceUploadState, SchemaField, Statement,
        UserName, VhostTlsResource,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::registry::test_fixtures::{
        client_model, codec, ingestor, junction, named, relay, schema, wire_schema,
    };

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

    pub(super) fn snapshot(
        status: DomainStatus,
        models: impl IntoIterator<Item = Model>,
    ) -> TransactionPlanningSnapshot {
        TransactionPlanningSnapshot {
            domain: domain_state(status),
            models: models.into_iter().collect(),
            resources: BTreeSet::new(),
            resource_uploads: ResourceUploads::default(),
            schedule: None,
            basis: ImpactPlanningBasis::new([7; 32]),
        }
    }

    fn scheduled_snapshot(
        status: DomainStatus,
        models: impl IntoIterator<Item = Model>,
    ) -> TransactionPlanningSnapshot {
        let domain = named("default");
        let models = models.into_iter().collect::<ModelIndex>();
        let graph = crate::registry::domain_state::DomainState::build(&domain, &models)
            .assured("the transaction test graph is valid")
            .graph;
        let node = ClusterNodeName::parse("node-a")
            .assured("the scheduler fixture node is an identifier-shaped literal");
        let schedule = graph.schedule_for_domain(&domain, &[node], 0, PlacementPolicy::Neutral);
        TransactionPlanningSnapshot {
            domain: domain_state(status),
            models,
            resources: BTreeSet::new(),
            resource_uploads: ResourceUploads::default(),
            schedule: Some(schedule),
            basis: ImpactPlanningBasis::new([7; 32]),
        }
    }

    fn unbranched_junction(name: &str, input: &str, output: &str) -> Model {
        let mut model = junction(name, &[input], output);
        let Model::Junction(junction) = &mut model else {
            unreachable!("the junction fixture constructs a junction model");
        };
        junction.branched_by = BranchSelection::unbranched();
        for route in &mut junction.output_routes.routes {
            route.branch = Some(OutputBranch::Unbranched);
        }
        model
    }

    pub(super) fn node_ref(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(kind, named::<nervix_models::ModelName>(name))
    }

    pub(super) fn preserve_schedule(
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
                Box::new(relay("events", "missing_schema").into()),
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
                Box::new(schema("missing_schema").into()),
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
            Statement::Create(CreateStatement::new(Box::new(replacement.into()), false)),
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
            Box::new(relay("events", "event_schema").into()),
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
    fn stopped_schedule_move_keeps_the_execution_gate_without_reporting_a_pause() {
        let domain = named("default");
        let moved = node_ref(ModelKind::Relay, "events");
        let expected_moved = moved.clone();
        let source = ClusterNodeName::parse("node-a")
            .assured("the source node fixture is an identifier-shaped literal");
        let destination = ClusterNodeName::parse("node-b")
            .assured("the destination node fixture is an identifier-shaped literal");
        let statement = Statement::AlterRelay(AlterRelay {
            relay: named("events"),
            operations: vec![AlterRelayOperation::SetCapacity {
                capacity: nonzero!(32usize),
            }],
        });
        let plan = Registry::plan_transaction(
            scheduled_snapshot(
                DomainStatus::Stopped,
                [schema("event_schema"), relay("events", "event_schema")],
            ),
            &[statement],
            0,
            false,
            move |graph, placement, _current, attribution| TransactionScheduleDecision {
                schedule: graph.map(|graph| {
                    graph.schedule_for_domain(
                        &domain,
                        std::slice::from_ref(&destination),
                        0,
                        placement,
                    )
                }),
                ownership_moves: CanonicalImpactSet::new([OwnershipMoveImpact {
                    node: ImpactNodeCoverage::all_executions(moved.clone()),
                    source: source.clone(),
                    destination: destination.clone(),
                    attribution: attribution.clone(),
                }]),
            },
        )
        .assured("the stopped relay change has a captured ownership transition");
        let step = plan
            .first_step()
            .verified("the relay alteration produces one execution step");
        assert_eq!(step.impact.planned().pause, PauseRequirement::NoPause);
        let PlannedTransactionStepKind::Models { plan } = &step.kind else {
            unreachable!("the relay alteration produces a model step");
        };
        assert_eq!(plan.ownership_gate.affected_entities(), &[expected_moved]);
        assert_eq!(plan.ownership_gate.relays(), &[named("events")]);
    }

    #[test]
    fn entity_pause_report_and_execution_share_the_exact_affected_graph() {
        let domain = named("default");
        let cluster_node = ClusterNodeName::parse("node-a")
            .assured("the scheduler fixture node is an identifier-shaped literal");
        let models = [
            schema("event_schema"),
            wire_schema("event_wire"),
            codec("event_codec", "event_schema"),
            client_model("input_client"),
            client_model("disjoint_client"),
            relay("input", "event_schema"),
            relay("middle", "event_schema"),
            relay("output", "event_schema"),
            relay("disjoint_input", "event_schema"),
            relay("disjoint_output", "event_schema"),
            ingestor("input_ingestor", "input", "event_codec", "input_client"),
            ingestor(
                "disjoint_ingestor",
                "disjoint_input",
                "event_codec",
                "disjoint_client",
            ),
            unbranched_junction("changed", "input", "middle"),
            unbranched_junction("downstream", "middle", "output"),
            unbranched_junction("disjoint", "disjoint_input", "disjoint_output"),
        ];
        let statements = [
            Statement::AlterJunction(AlterJunction {
                junction: named("changed"),
                operations: vec![AlterProcessorOperation::SetMode {
                    mode: AckMode::Detached,
                }],
            }),
            Statement::AlterRelay(AlterRelay {
                relay: named("disjoint_input"),
                operations: vec![AlterRelayOperation::SetCapacity {
                    capacity: nonzero!(32usize),
                }],
            }),
        ];
        let plan = Registry::plan_transaction(
            scheduled_snapshot(DomainStatus::Running, models),
            &statements,
            0,
            false,
            move |graph, placement, _current, _attribution| TransactionScheduleDecision {
                schedule: graph.map(|graph| {
                    graph.schedule_for_domain(
                        &domain,
                        std::slice::from_ref(&cluster_node),
                        0,
                        placement,
                    )
                }),
                ownership_moves: CanonicalImpactSet::default(),
            },
        )
        .assured("the junction mode change has a complete scheduled plan");
        let step = plan
            .first_step()
            .verified("the single alteration produces one execution step");
        assert!(step.impact.planned().completeness.is_complete());
        let PauseRequirement::Subgraph { scope } = &step.impact.planned().pause else {
            unreachable!("changing junction acknowledgement mode requires an entity pause");
        };
        let expected_nodes = BTreeSet::from([
            node_ref(ModelKind::Junction, "changed"),
            node_ref(ModelKind::Relay, "middle"),
            node_ref(ModelKind::Junction, "downstream"),
            node_ref(ModelKind::Relay, "output"),
        ]);
        let paused_nodes = scope
            .nodes()
            .iter()
            .map(|node| {
                assert_eq!(
                    node.coverage.branches,
                    Some(ConcreteBranchCoverage::Unbranched)
                );
                node.coverage.node.clone()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(paused_nodes, expected_nodes);
        assert!(!paused_nodes.contains(&node_ref(ModelKind::Ingestor, "input_ingestor")));
        assert!(!paused_nodes.contains(&node_ref(ModelKind::Ingestor, "disjoint_ingestor")));
        assert!(!paused_nodes.contains(&node_ref(ModelKind::Relay, "disjoint_input")));
        let expected_relays = BTreeSet::from([named("input")]);
        let gate_relays = scope
            .gate_boundaries()
            .iter()
            .map(|gate| gate.boundary.relay.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(gate_relays, expected_relays);

        let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
            unreachable!("the alteration produces a model step");
        };
        assert_eq!(
            model_plan
                .model_gate
                .affected_entities()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            expected_nodes
        );
        assert_eq!(
            model_plan
                .model_gate
                .relays()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            expected_relays
        );

        let effects = &step.impact.planned().effects;
        let topology_nodes = effects
            .topology
            .before
            .nodes
            .as_slice()
            .iter()
            .map(|node| node.coverage.node.clone())
            .collect::<BTreeSet<_>>();
        assert!(topology_nodes.contains(&node_ref(ModelKind::Relay, "input")));
        assert!(!topology_nodes.contains(&node_ref(ModelKind::Junction, "disjoint")));
        let input_edge_kinds = effects
            .topology
            .before
            .edges
            .as_slice()
            .iter()
            .filter(|edge| {
                edge.source.node == node_ref(ModelKind::Relay, "input")
                    && edge.target.node == node_ref(ModelKind::Junction, "changed")
            })
            .map(|edge| edge.kind)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            input_edge_kinds,
            BTreeSet::from([
                ImpactEdgeKind::ConfigurationDependency,
                ImpactEdgeKind::Dataflow,
            ])
        );
        let force_flushed_nodes = effects
            .force_flushes
            .as_slice()
            .iter()
            .map(|flush| flush.node.node.clone())
            .collect::<BTreeSet<_>>();
        assert!(force_flushed_nodes.contains(&node_ref(ModelKind::Junction, "disjoint")));
    }

    #[test]
    fn schedule_reassignment_raises_a_dynamic_change_to_an_entity_pause() {
        let domain = named("default");
        let destination = ClusterNodeName::parse("node-b")
            .assured("the scheduler fixture node is an identifier-shaped literal");
        let statement = Statement::AlterRelay(AlterRelay {
            relay: named("events"),
            operations: vec![AlterRelayOperation::SetCapacity {
                capacity: nonzero!(32usize),
            }],
        });
        let plan = Registry::plan_transaction(
            scheduled_snapshot(
                DomainStatus::Running,
                [schema("event_schema"), relay("events", "event_schema")],
            ),
            &[statement],
            0,
            false,
            move |graph, placement, _current, _attribution| TransactionScheduleDecision {
                schedule: graph.map(|graph| {
                    graph.schedule_for_domain(
                        &domain,
                        std::slice::from_ref(&destination),
                        0,
                        placement,
                    )
                }),
                ownership_moves: CanonicalImpactSet::default(),
            },
        )
        .assured("the dynamic relay change has a captured reassignment");
        let step = plan
            .first_step()
            .verified("the relay alteration produces one execution step");
        let PauseRequirement::Subgraph { scope } = &step.impact.planned().pause else {
            unreachable!("a scheduled entity swap requires an entity pause");
        };
        let relay = node_ref(ModelKind::Relay, "events");
        assert_eq!(
            scope
                .nodes()
                .iter()
                .map(|node| node.coverage.node.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([relay.clone()])
        );
        assert_eq!(
            scope
                .gate_boundaries()
                .iter()
                .map(|gate| gate.boundary.relay.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([named("events")])
        );
        let PlannedTransactionStepKind::Models { plan } = &step.kind else {
            unreachable!("the relay alteration produces a model step");
        };
        assert_eq!(plan.model_gate.affected_entities(), &[relay]);
    }

    #[test]
    fn schedule_rebuild_raises_entity_creation_to_a_domain_pause() {
        let domain = named("default");
        let cluster_node = ClusterNodeName::parse("node-a")
            .assured("the scheduler fixture node is an identifier-shaped literal");
        let statement = Statement::Create(CreateStatement::new(
            Box::new(relay("events", "event_schema").into()),
            false,
        ));
        let plan = Registry::plan_transaction(
            scheduled_snapshot(DomainStatus::Running, [schema("event_schema")]),
            &[statement],
            0,
            false,
            move |graph, placement, _current, _attribution| TransactionScheduleDecision {
                schedule: graph.map(|graph| {
                    graph.schedule_for_domain(
                        &domain,
                        std::slice::from_ref(&cluster_node),
                        0,
                        placement,
                    )
                }),
                ownership_moves: CanonicalImpactSet::default(),
            },
        )
        .assured("the relay creation has a captured schedule rebuild");
        let step = plan
            .first_step()
            .verified("the relay creation produces one execution step");
        assert_eq!(
            step.impact.planned().pause,
            PauseRequirement::Domain {
                domain: named("default")
            }
        );
        assert!(
            step.impact
                .planned()
                .effects
                .rebuilds
                .as_slice()
                .iter()
                .any(|rebuild| rebuild.node.node == node_ref(ModelKind::Relay, "events"))
        );
        let PlannedTransactionStepKind::Models { plan } = &step.kind else {
            unreachable!("the relay creation produces a model step");
        };
        assert!(plan.model_gate.affected_entities().is_empty());
    }

    #[test]
    fn drop_recreate_retains_both_sides_of_a_rewired_graph() {
        let domain = named("default");
        let cluster_node = ClusterNodeName::parse("node-a")
            .assured("the scheduler fixture node is an identifier-shaped literal");
        let replacement = unbranched_junction("changed", "input", "new_output");
        let statements = [
            Statement::Drop(DropModel {
                kind: ModelKind::Junction,
                name: named("changed"),
            }),
            Statement::Create(CreateStatement::new(Box::new(replacement.into()), false)),
        ];
        let plan = Registry::plan_transaction(
            scheduled_snapshot(
                DomainStatus::Running,
                [
                    schema("event_schema"),
                    relay("input", "event_schema"),
                    relay("old_output", "event_schema"),
                    relay("new_output", "event_schema"),
                    unbranched_junction("changed", "input", "old_output"),
                ],
            ),
            &statements,
            0,
            false,
            move |graph, placement, _current, _attribution| TransactionScheduleDecision {
                schedule: graph.map(|graph| {
                    graph.schedule_for_domain(
                        &domain,
                        std::slice::from_ref(&cluster_node),
                        0,
                        placement,
                    )
                }),
                ownership_moves: CanonicalImpactSet::default(),
            },
        )
        .assured("the junction replacement leaves a valid graph");
        let effects = &plan
            .first_step()
            .verified("the replacement produces one execution step")
            .impact
            .planned()
            .effects;
        let changed = node_ref(ModelKind::Junction, "changed");
        let old_output = node_ref(ModelKind::Relay, "old_output");
        let new_output = node_ref(ModelKind::Relay, "new_output");
        assert!(effects.topology.before.edges.as_slice().iter().any(|edge| {
            edge.source.node == changed
                && edge.target.node == old_output
                && edge.kind == ImpactEdgeKind::Dataflow
                && edge.attribution.operations()
                    == [
                        TransactionOperationNumber::from_index(0)
                            .assured("the first fixture operation is addressable"),
                        TransactionOperationNumber::from_index(1)
                            .assured("the second fixture operation is addressable"),
                    ]
        }));
        assert!(effects.topology.after.edges.as_slice().iter().any(|edge| {
            edge.source.node == changed
                && edge.target.node == new_output
                && edge.kind == ImpactEdgeKind::Dataflow
        }));
        assert!(
            !effects
                .topology
                .before
                .nodes
                .as_slice()
                .iter()
                .any(|node| { node.coverage.node == new_output })
        );
        assert!(
            !effects
                .topology
                .after
                .nodes
                .as_slice()
                .iter()
                .any(|node| { node.coverage.node == old_output })
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
            Statement::Create(CreateStatement::new(
                Box::new(schema("events").into()),
                false,
            )),
            Statement::Create(CreateStatement::new(
                Box::new(schema("events").into()),
                true,
            )),
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

    /// The outcome of one fixture upload of the `tls_bundle` resource.
    pub(super) enum FixtureUploadOutcome {
        Completed,
        Applying,
    }

    pub(super) fn tls_bundle_uploads(uploads: &[(u64, FixtureUploadOutcome)]) -> ResourceUploads {
        let domain = named::<DomainName>("default");
        let mut records = Vec::new();
        for (version, outcome) in uploads {
            let root_checksum = format!("root-{version}");
            let state = match outcome {
                FixtureUploadOutcome::Completed => ResourceUploadState::Completed {
                    root_checksum,
                    outcome_revision: *version,
                },
                FixtureUploadOutcome::Applying => ResourceUploadState::Applying { root_checksum },
            };
            records.push(ResourceUpload {
                key: ResourceUploadKey::new(
                    UserName::parse("default")
                        .assured("the fixture owner is an identifier-shaped literal"),
                    domain.clone(),
                    named("tls_bundle"),
                    ResourceUploadIdentity::parse(format!("upload-{version}"))
                        .assured("fixture upload identities use accepted characters"),
                ),
                version: *version,
                state,
            });
        }
        ResourceUploads::try_from_uploads(records)
            .assured("fixture uploads have unique identities and versions")
    }

    fn create_tls_vhost(version: RequestedResourceVersion, if_not_exists: bool) -> Statement {
        Statement::Create(CreateStatement::new(
            Box::new(Model::Vhost(CreateVhost {
                name: named("edge"),
                hostnames: vec!["edge.example.com".to_string()],
                tls: Some(VhostTlsResource {
                    resource: named("tls_bundle"),
                    version,
                }),
            })),
            if_not_exists,
        ))
    }

    pub(super) fn stored_tls_vhost(name: &str, version: u64) -> Model {
        Model::Vhost(CreateVhost {
            name: named(name),
            hostnames: vec![format!("{name}.example.com")],
            tls: Some(VhostTlsResource {
                resource: named("tls_bundle"),
                version,
            }),
        })
    }

    fn operation(number: usize) -> TransactionOperationNumber {
        TransactionOperationNumber::from_index(
            number
                .checked_sub(1)
                .assured("fixture operation numbers are one-based"),
        )
        .assured("fixture operation numbers are addressable")
    }

    #[test]
    fn latest_binds_the_highest_version_completed_in_the_captured_uploads() {
        let mut snapshot = snapshot(DomainStatus::Stopped, []);
        snapshot.resource_uploads = tls_bundle_uploads(&[
            (1, FixtureUploadOutcome::Completed),
            (2, FixtureUploadOutcome::Completed),
            (3, FixtureUploadOutcome::Applying),
        ]);
        let statements = vec![create_tls_vhost(RequestedResourceVersion::Latest, false)];

        let plan = Registry::plan_transaction(snapshot, &statements, 0, false, preserve_schedule)
            .assured("the latest completed version is bindable");

        let binding = ResourceBindingImpact {
            node: node_ref(ModelKind::Vhost, "edge"),
            resource: named("tls_bundle"),
            requested: RequestedResourceVersion::Latest,
            version: 2,
            attribution: ImpactAttribution::single(operation(1)),
        };
        let step = plan.first_step().verified("the create forms one model run");
        assert_eq!(
            step.impact.planned().effects.resource_bindings.as_slice(),
            std::slice::from_ref(&binding)
        );
        let report = plan
            .report()
            .assured("a plan from the first operation has a whole-transaction report");
        assert_eq!(
            report.operations()[0]
                .contribution
                .resource_bindings
                .as_slice(),
            std::slice::from_ref(&binding)
        );
        let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
            unreachable!("the create produces a model plan");
        };
        let planned = model_plan
            .planned
            .as_ref()
            .verified("a complete model run carries its planned mutations");
        let [Model::Vhost(vhost)] = planned.changed_models().as_slice() else {
            panic!("the plan persists exactly the created VHOST");
        };
        let tls = vhost.tls.as_ref().verified("the created VHOST binds TLS");
        assert_eq!(tls.version, 2);
    }

    #[test]
    fn an_incomplete_version_fails_the_operation_that_names_it() {
        let mut snapshot = snapshot(DomainStatus::Stopped, []);
        snapshot.resource_uploads = tls_bundle_uploads(&[
            (1, FixtureUploadOutcome::Completed),
            (2, FixtureUploadOutcome::Applying),
        ]);
        let statements = vec![
            Statement::Create(CreateStatement::new(
                Box::new(schema("events").into()),
                false,
            )),
            create_tls_vhost(RequestedResourceVersion::Number(2), false),
        ];

        let error = Registry::plan_transaction(snapshot, &statements, 0, false, preserve_schedule)
            .expect_err("an applying version is not bindable");

        assert!(matches!(
            error.current_context(),
            TransactionPlanningError::ResourceVersion {
                operation,
                error: ResourceVersionResolutionError::NotCompleted(id),
            } if operation.get() == 2
                && id == &ResourceId::new(named("default"), named("tls_bundle"), 2)
        ));
        assert!(
            error
                .to_string()
                .contains("resource 'tls_bundle@2' is not a completed version in domain 'default'")
        );
    }

    #[test]
    fn an_existing_model_skipped_by_if_not_exists_resolves_no_version() {
        let existing: Model = Model::Vhost(CreateVhost {
            name: named("edge"),
            hostnames: vec!["edge.example.com".to_string()],
            tls: Some(VhostTlsResource {
                resource: named("tls_bundle"),
                version: 1,
            }),
        });
        let statements = vec![create_tls_vhost(RequestedResourceVersion::Latest, true)];

        let plan = Registry::plan_transaction(
            snapshot(DomainStatus::Stopped, [existing]),
            &statements,
            0,
            false,
            preserve_schedule,
        )
        .assured("an IF NOT EXISTS no-op does not need a completed version");

        let step = plan.first_step().verified("the create forms one model run");
        assert!(step.impact.planned().effects.resource_bindings.is_empty());
        let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
            unreachable!("the create produces a model plan");
        };
        assert!(model_plan.no_op_operations.contains(&operation(1)));
    }

    #[test]
    fn a_binding_dropped_later_in_the_run_is_not_reported_by_its_step() {
        let mut snapshot = snapshot(DomainStatus::Stopped, []);
        snapshot.resource_uploads = tls_bundle_uploads(&[(1, FixtureUploadOutcome::Completed)]);
        let statements = vec![
            create_tls_vhost(RequestedResourceVersion::Latest, false),
            Statement::Drop(DropModel {
                kind: ModelKind::Vhost,
                name: named("edge"),
            }),
        ];

        let plan = Registry::plan_transaction(snapshot, &statements, 0, false, preserve_schedule)
            .assured("creating and dropping a VHOST is a valid run");

        let step = plan
            .first_step()
            .verified("both statements form one model run");
        assert!(step.impact.planned().effects.resource_bindings.is_empty());
        let report = plan
            .report()
            .assured("a plan from the first operation has a whole-transaction report");
        assert_eq!(
            report.operations()[0].contribution.resource_bindings.len(),
            1,
            "the create still reports the version it resolved"
        );
    }
}
