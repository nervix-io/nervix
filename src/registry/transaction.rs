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
    AttributedGateBoundary, AttributedImpactNode, CanonicalImpactSet, CommandExecutionReference,
    ConfigurationImpact, ConfigurationTransition, DomainLifecycleAction, DomainLifecycleImpact,
    DomainName, DomainSchedule, DomainState as ControlDomainState, DomainStatus,
    DynamicModelUpdate, ExecutionStepImpactReport, ForceFlushImpact, ImpactAttribution,
    ImpactDiagnostic, ImpactDiagnosticKind, ImpactEffects, ImpactNodeCoverage, ImpactPlanningBasis,
    ImpactReportCompleteness, ImpactReportError, ImpactTopology, ImpactTopologyEdge,
    ModelChangeAspect, ModelIndex, NodeRef, OperationImpactReason, OperationImpactReport,
    OwnershipMoveImpact, PauseRequirement, PlacementPolicy, PlannedExecutionStepImpact,
    QuiesceLevel, QuiesceSubgraph, RebuildImpact, RebuildReason, RequestedResourceVersion,
    ResetWasmState, ResetWasmStateSelectionError, ResourceBindingImpact, ResourceCatalogAction,
    ResourceCatalogImpact, ResourceName, ResourceUploads, ResourceVersionResolutionError,
    StateResetImpact, Statement, TransactionCommitPlanStep, TransactionCommitStepKind,
    TransactionImpactReport, TransactionOperation, TransactionOperationNumber,
    TransactionOperationRange, TransactionPosition,
};
use thiserror::Error;

use super::graph::{UnattributedImpactEdge, UnattributedImpactTopology};
use crate::registry::{
    ActiveGraph, EntityGatePlan, PlannedMutations, Registry, RegistryError, RegistryMutation,
    ScheduleDelta, entity_pause_relays_for_schedule, gate_boundary, scheduled_impact_coverage,
};

mod commit_plan;
mod rebind;
mod reset;

use rebind::plan_resource_rebind;

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
    pub(crate) operation_references: Vec<CommandExecutionReference>,
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
    StopDomain,
    CreateResource {
        resource: ResourceName,
        already_existed: bool,
    },
    ResetWasmState {
        reset: ResetWasmState,
        request: CommandExecutionReference,
        schedule: DomainSchedule,
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

    #[cfg(test)]
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
    #[error(
        "WASM state reset names domain '{requested}', but the transaction is bound to '{domain}'"
    )]
    ResetDomainMismatch {
        domain: DomainName,
        requested: DomainName,
    },
    #[error("WASM processor '{processor}' does not exist in domain '{domain}'")]
    ResetProcessorNotFound {
        domain: DomainName,
        processor: nervix_models::WasmProcessorName,
    },
    #[error("WASM processor '{processor}' has no active execution in domain '{domain}'")]
    ResetProcessorNotRunning {
        domain: DomainName,
        processor: nervix_models::WasmProcessorName,
    },
    #[error("WASM processor '{processor}' reset has invalid branch scope: {error}")]
    ResetInvalidScope {
        processor: nervix_models::WasmProcessorName,
        error: Report<ResetWasmStateSelectionError>,
    },
    #[error("WASM processor '{processor}' is publishing another reset")]
    ResetInProgress {
        processor: nervix_models::WasmProcessorName,
    },
    #[error("transaction operation {operation} has no durable reset request identity")]
    ResetIdentityMissing {
        operation: TransactionOperationNumber,
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

impl TransactionPlanningError {
    pub(crate) const fn operation(&self) -> Option<TransactionOperationNumber> {
        match self {
            Self::InvalidOperation { operation }
            | Self::ResourceVersion { operation, .. }
            | Self::ModelPreflight { operation, .. }
            | Self::ExternalModelValidation { operation }
            | Self::UdfPreparation { operation } => Some(*operation),
            Self::ResetIdentityMissing { operation } => Some(*operation),
            Self::DomainNotFound { .. }
            | Self::DomainPaused { .. }
            | Self::ConcurrentDomainAlter { .. }
            | Self::DomainAlreadyRunning { .. }
            | Self::DomainAlreadyStopped { .. }
            | Self::DomainStartGenerationOverflow { .. }
            | Self::ResourceAlreadyExists { .. }
            | Self::ResourceNotFound { .. }
            | Self::RebindMemberNotFound { .. }
            | Self::RebindMemberDoesNotBind { .. }
            | Self::ResetDomainMismatch { .. }
            | Self::ResetProcessorNotFound { .. }
            | Self::ResetProcessorNotRunning { .. }
            | Self::ResetInvalidScope { .. }
            | Self::ResetInProgress { .. }
            | Self::InvalidImpactReport { .. }
            | Self::PlanningBasisEncoding
            | Self::PartialPlanHasNoTransactionReport { .. } => None,
        }
    }
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
    pub(crate) fn restore_transaction_commit_step(
        domain: &DomainName,
        current_models: ModelIndex,
        previous: ControlDomainState,
        expected_schedule: Option<DomainSchedule>,
        step: TransactionCommitPlanStep,
    ) -> Result<PlannedTransactionStep, Report<TransactionPlanningError>> {
        let operation = step.impact.operations().first();
        let kind = match step.kind {
            TransactionCommitStepKind::Models {
                transitions,
                schedule,
                no_op_operations,
                model_gate,
                ownership_gate,
            } => {
                let operation_count = step.impact.operations().operation_count().get();
                let planned = Self::restore_transaction_model_plan(
                    domain,
                    current_models,
                    &transitions,
                    operation_count,
                )
                .map_err(|error| {
                    Report::new(TransactionPlanningError::ModelPreflight { operation, error })
                })?;
                PlannedTransactionStepKind::Models {
                    plan: Box::new(PlannedModelTransactionStep {
                        planned: Some(planned),
                        expected_schedule,
                        schedule: schedule.map(|schedule| *schedule),
                        no_op_operations: no_op_operations.into_iter().collect(),
                        model_gate: EntityGatePlan::from_commit_plan(model_gate),
                        ownership_gate: EntityGatePlan::from_commit_plan(ownership_gate),
                    }),
                }
            }
            TransactionCommitStepKind::AlterDomain {
                next,
                schedule,
                ownership_gate,
            } => PlannedTransactionStepKind::AlterDomain {
                plan: Box::new(PlannedAlterDomainTransactionStep {
                    previous,
                    next: *next,
                    expected_schedule,
                    schedule: schedule.map(|schedule| *schedule),
                    ownership_gate: EntityGatePlan::from_commit_plan(ownership_gate),
                }),
            },
            TransactionCommitStepKind::StartDomain { .. } => {
                PlannedTransactionStepKind::StartDomain { previous }
            }
            TransactionCommitStepKind::StopDomain => PlannedTransactionStepKind::StopDomain,
            TransactionCommitStepKind::CreateResource {
                resource,
                already_existed,
            } => PlannedTransactionStepKind::CreateResource {
                resource,
                already_existed,
            },
            TransactionCommitStepKind::ResetWasmState {
                reset,
                request,
                schedule,
            } => PlannedTransactionStepKind::ResetWasmState {
                reset: *reset,
                request,
                schedule: *schedule,
            },
        };
        Ok(PlannedTransactionStep {
            impact: step.impact,
            kind,
        })
    }

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
        let operation_references = snapshot.operation_references;
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
                    kind = PlannedTransactionStepKind::StopDomain;
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
                Statement::ResetWasmState(reset) => {
                    ensure_domain_not_paused(&domain_state)?;
                    let request = operation_references.get(absolute_index).ok_or_else(|| {
                        Report::new(TransactionPlanningError::ResetIdentityMissing {
                            operation: number,
                        })
                    })?;
                    let planned = Self::plan_wasm_state_reset(
                        reset,
                        &domain,
                        domain_state.status.clone(),
                        &models,
                        current_schedule.as_ref(),
                        request,
                        &attribution,
                    )?;
                    operation = planned.operation;
                    reasons = planned.reasons;
                    contribution = planned.effects;
                    pause = planned.pause;
                    current_schedule = Some(planned.schedule.clone());
                    kind = PlannedTransactionStepKind::ResetWasmState {
                        reset: reset.clone(),
                        request: request.clone(),
                        schedule: planned.schedule,
                    };
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
            for (node, action) in dynamic_update_activations(updates) {
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
                        action,
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
            for (node, action) in dynamic_update_activations(dynamic_updates) {
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
                        action,
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

/// Every node a set of dynamic updates changes in place, with the activation each one reports.
fn dynamic_update_activations(
    updates: &[DynamicModelUpdate],
) -> BTreeMap<NodeRef, ActivationAction> {
    updates
        .iter()
        .map(|update| (update.node(), update.activation()))
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
mod tests;
