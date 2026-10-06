//! Transaction-impact reports, the previews that identify them, and the commit plans admitted for
//! them: the records consensus keeps about what a transaction changes.
//!
//! A report divides its operations into execution steps as the planner does: a run of consecutive
//! Model operations is one atomic step, and every other operation is a step of its own. Every
//! impact item is attributed to operations of the step that holds it, and an operation's own
//! contribution to that operation alone. A configuration-only node carries no branch coverage and
//! an execution node always carries one. An operation's completeness is its step's planned
//! completeness, and the report's completeness lists the diagnostics of every step planned
//! incompletely. Each report therefore passes the vocabulary's own validation, and a commit plan
//! pairs every step of a report commit admission froze with the decision its operations call for.

use std::{collections::BTreeSet, num::NonZeroUsize};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ActivationAction, ActivationImpact, ActualExecutionStepImpact, ActualQuiescence,
    AffectedTopology, AttributedGateBoundary, AttributedImpactNode, BranchKeyFingerprint,
    CanonicalImpactSet, ClusterNodeIdentity, ClusterNodeIncarnation, CommandExecutionReference,
    ConcreteBranchCoverage, ConfigurationImpact, ConfigurationTransition, DomainClockState,
    DomainConfig, DomainLifecycleAction, DomainLifecycleImpact, DomainName, DomainPace,
    DomainSchedule, DomainStartPoint, DomainState, DomainStatus, ExecutionStepImpactReport,
    ExecutionStepOutcome, ForceFlushImpact, ImpactAttribution, ImpactDiagnostic,
    ImpactDiagnosticKind, ImpactEdgeKind, ImpactEffects, ImpactGateBoundary, ImpactNodeCoverage,
    ImpactPlanningBasis, ImpactReportCompleteness, ImpactTopology, ImpactTopologyEdge,
    ModelChangeAspect, ModelKind, ModelName, NodeRef, OperationImpactReason, OperationImpactReport,
    OwnershipMoveImpact, PauseRequirement, PlacementGroupSchedule, PlacementPolicy,
    PlannedExecutionStepImpact, QuiesceSubgraph, QuiescenceOutcome, RebuildImpact, RebuildReason,
    RelayName, RequestedResourceVersion, ResetWasmState, ResourceBindingImpact,
    ResourceCatalogAction, ResourceCatalogImpact, ScheduledNode, SchemaFingerprint, StatePurge,
    StateResetImpact, Statement, Timestamp, TransactionCommitPlan, TransactionCommitPlanStep,
    TransactionCommitStepKind, TransactionEntityGatePlan, TransactionImpactReport,
    TransactionModelTransition, TransactionOperation, TransactionOperationNumber,
    TransactionOperationRange, TransactionPosition, TransactionPreviewIdentity,
    TransactionResolvedDomainStart, WasmProcessorName,
};

use crate::{Arbitrary, ModelVariant, StatementVariant};

/// The most items a generated collection holds: the operations of a transaction, the reasons of
/// one operation, the items of one effect family, the nodes and edges of one side of a topology,
/// and the attempts, transitions and entries one step records.
const ITEMS: usize = 3;

/// The last millisecond the 48-bit timestamp of a UUIDv7 holds.
const UUID_MILLISECONDS: u64 = 0xffff_ffff_ffff;

/// Every kind of node an execution graph runs. An impact covers such a node by its concrete
/// executions.
const EXECUTION_KINDS: [ModelKind; 13] = [
    ModelKind::Generator,
    ModelKind::Inferencer,
    ModelKind::WasmProcessor,
    ModelKind::Ingestor,
    ModelKind::Reingestor,
    ModelKind::Relay,
    ModelKind::Lookup,
    ModelKind::Junction,
    ModelKind::Deduplicator,
    ModelKind::Correlator,
    ModelKind::Reorderer,
    ModelKind::WindowProcessor,
    ModelKind::Emitter,
];

/// Every kind of node that only configures others. An impact names such a node without
/// executions.
const CONFIGURATION_KINDS: [ModelKind; 12] = [
    ModelKind::Schema,
    ModelKind::WireJsonSchema,
    ModelKind::WireCborSchema,
    ModelKind::WireAvroSchema,
    ModelKind::Codec,
    ModelKind::Client,
    ModelKind::Vhost,
    ModelKind::Branch,
    ModelKind::Endpoint,
    ModelKind::SignalingProtocol,
    ModelKind::Placement,
    ModelKind::Udf,
];

/// The Model families of the execution kinds, which are the families a schedule places.
const EXECUTION_FAMILIES: [ModelVariant; 13] = [
    ModelVariant::Generator,
    ModelVariant::Inferencer,
    ModelVariant::WasmProcessor,
    ModelVariant::Ingestor,
    ModelVariant::Reingestor,
    ModelVariant::Relay,
    ModelVariant::Lookup,
    ModelVariant::Junction,
    ModelVariant::Deduplicator,
    ModelVariant::Correlator,
    ModelVariant::Reorderer,
    ModelVariant::WindowProcessor,
    ModelVariant::Emitter,
];

/// Every aspect of a Model change, in declaration order. The vocabulary declares no iterator over
/// the aspects, so the tests below read every archived aspect back to keep this list complete.
const MODEL_CHANGE_ASPECTS: [ModelChangeAspect; 76] = [
    ModelChangeAspect::RelayCapacity,
    ModelChangeAspect::RelaySchema,
    ModelChangeAspect::RelayBranching,
    ModelChangeAspect::RelayMaterializedState,
    ModelChangeAspect::ProcessorFilter,
    ModelChangeAspect::ProcessorInputWhere,
    ModelChangeAspect::ProcessorRouteConstruction,
    ModelChangeAspect::ProcessorRouteFlushPolicy,
    ModelChangeAspect::ProcessorCollectPolicy,
    ModelChangeAspect::ProcessorMessageErrorPolicy,
    ModelChangeAspect::ProcessorErrorRouteTargets,
    ModelChangeAspect::ProcessorInputs,
    ModelChangeAspect::ProcessorRoutes,
    ModelChangeAspect::ProcessorMode,
    ModelChangeAspect::ProcessorBranching,
    ModelChangeAspect::ProcessorMaterializedState,
    ModelChangeAspect::DeduplicatorKeyspace,
    ModelChangeAspect::DeduplicatorMaxTime,
    ModelChangeAspect::ReordererOrdering,
    ModelChangeAspect::ReordererMaxTime,
    ModelChangeAspect::EmitterInput,
    ModelChangeAspect::EmitterInputWhere,
    ModelChangeAspect::EmitterSink,
    ModelChangeAspect::EmitterClient,
    ModelChangeAspect::EmitterCodec,
    ModelChangeAspect::EmitterCollectPolicy,
    ModelChangeAspect::EmitterMode,
    ModelChangeAspect::EmitterPublishingMode,
    ModelChangeAspect::EmitterFlushPolicy,
    ModelChangeAspect::EmitterBatchPolicy,
    ModelChangeAspect::EmitterConstruction,
    ModelChangeAspect::EmitterErrorPolicies,
    ModelChangeAspect::EmitterMaterializedState,
    ModelChangeAspect::IngestorSource,
    ModelChangeAspect::IngestorCodec,
    ModelChangeAspect::IngestorTimestamp,
    ModelChangeAspect::IngestorFilter,
    ModelChangeAspect::IngestorRoutes,
    ModelChangeAspect::IngestorGeneralError,
    ModelChangeAspect::ReingestorInputs,
    ModelChangeAspect::ReingestorRoutes,
    ModelChangeAspect::ReingestorMode,
    ModelChangeAspect::ReingestorFilter,
    ModelChangeAspect::ReingestorMaterializedState,
    ModelChangeAspect::GeneratorMaterializedState,
    ModelChangeAspect::GeneratorCadence,
    ModelChangeAspect::GeneratorBranching,
    ModelChangeAspect::GeneratorRoutes,
    ModelChangeAspect::CorrelatorCorrelation,
    ModelChangeAspect::CorrelatorMatchPolicy,
    ModelChangeAspect::CorrelatorMaxTime,
    ModelChangeAspect::CorrelatorTimeoutPolicy,
    ModelChangeAspect::WindowBounds,
    ModelChangeAspect::InferencerBinding,
    ModelChangeAspect::InferencerTensors,
    ModelChangeAspect::WasmBinding,
    ModelChangeAspect::WasmLimits,
    ModelChangeAspect::WasmGlobalError,
    ModelChangeAspect::WasmRejectedState,
    ModelChangeAspect::SchemaDefinition,
    ModelChangeAspect::WireSchemaDefinition,
    ModelChangeAspect::CodecDefinition,
    ModelChangeAspect::ClientConfig,
    ModelChangeAspect::VhostHostnames,
    ModelChangeAspect::VhostTls,
    ModelChangeAspect::VhostTlsVersion,
    ModelChangeAspect::EndpointDefinition,
    ModelChangeAspect::SignalingProtocolDefinition,
    ModelChangeAspect::LookupDefinition,
    ModelChangeAspect::BranchSchema,
    ModelChangeAspect::BranchLifecycle,
    ModelChangeAspect::UdfDefinition,
    ModelChangeAspect::PlacementDefinition,
    ModelChangeAspect::EntityReplaced,
    ModelChangeAspect::EntityCreated,
    ModelChangeAspect::EntityDropped,
];

/// Every runtime state a Model change can make meaningless, in declaration order.
const STATE_PURGES: [StatePurge; 6] = [
    StatePurge::DeduplicatorKeyspace,
    StatePurge::ReordererBuffer,
    StatePurge::WindowAccumulator,
    StatePurge::CorrelationBuffer,
    StatePurge::InferencerWarmState,
    StatePurge::WasmGuestState,
];

/// Every relation an edge of affected topology states, in declaration order.
const EDGE_KINDS: [ImpactEdgeKind; 5] = [
    ImpactEdgeKind::ConfigurationDependency,
    ImpactEdgeKind::Dataflow,
    ImpactEdgeKind::MessageError,
    ImpactEdgeKind::CorrelationTimeout,
    ImpactEdgeKind::MaterializedState,
];

/// Every class of report diagnostic, in declaration order.
const DIAGNOSTIC_KINDS: [ImpactDiagnosticKind; 7] = [
    ImpactDiagnosticKind::Planning,
    ImpactDiagnosticKind::Topology,
    ImpactDiagnosticKind::Quiescence,
    ImpactDiagnosticKind::Ownership,
    ImpactDiagnosticKind::Activation,
    ImpactDiagnosticKind::Application,
    ImpactDiagnosticKind::Recovery,
];

/// How far the transaction a generated report describes has progressed.
#[derive(Debug, Clone, Copy)]
enum ReportStage {
    /// Any planning outcome and any recorded execution: a report as inspection reads it at any
    /// point of its transaction's life.
    Any,
    /// Every step planned completely and none attempted yet: the preview commit admission freezes
    /// into a commit plan.
    Admitted,
}

/// Where the planner divides a transaction's operations into execution steps.
trait StepBoundary {
    /// Whether the operation belongs to a run of consecutive Model operations, which executes as
    /// one atomic step, rather than executing as a step of its own.
    fn joins_model_run(&self) -> bool;
}

impl StepBoundary for TransactionOperation {
    fn joins_model_run(&self) -> bool {
        match self {
            Self::CreateConfiguration { .. }
            | Self::AlterConfiguration { .. }
            | Self::DropConfiguration { .. }
            | Self::RebindResource { .. } => true,
            Self::AlterDomain { .. }
            | Self::StartDomain { .. }
            | Self::StopDomain { .. }
            | Self::CreateResource { .. }
            | Self::ResetWasmState { .. } => false,
        }
    }
}

impl Arbitrary<'_> {
    /// One through three items, for a collection that holds at least one.
    fn some_items(&mut self) -> usize {
        let bound = NonZeroUsize::new(ITEMS).assured("the item bound is positive");
        self.entropy.positive_count(bound)
    }

    /// Up to three items, each built by `item`, held in the canonical order a report keeps.
    fn impact_set<T: Ord>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> T,
    ) -> CanonicalImpactSet<T> {
        let count = self.entropy.count(ITEMS);
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(item(self));
        }
        CanonicalImpactSet::new(items)
    }

    /// A transaction identity as the control plane mints one: the lowercase text of a UUIDv7, with
    /// any 48-bit millisecond and any random bits below its version and variant bits.
    pub fn transaction_id(&mut self) -> String {
        self.uuid_v7_text(UUID_MILLISECONDS)
    }

    /// The fingerprint of the control-plane inputs a report was planned from.
    pub fn impact_planning_basis(&mut self) -> ImpactPlanningBasis {
        ImpactPlanningBasis::new(self.digest())
    }

    /// Any accepted-operation position a `usize` holds, landing on the extremes as often as
    /// elsewhere.
    pub fn transaction_position(&mut self) -> TransactionPosition {
        let widest = u64::try_from(usize::MAX).assured("supported targets address at most 64 bits");
        let accepted = self.entropy.boundary_biased(0..=widest);
        let accepted = usize::try_from(accepted).verified("the draw is at most usize::MAX");
        TransactionPosition::new(accepted)
    }

    /// The identity of a whole-transaction preview: a transaction, any accepted position and any
    /// planning basis.
    pub fn transaction_preview_identity(&mut self) -> TransactionPreviewIdentity {
        let transaction_id = self.transaction_id();
        let position = self.transaction_position();
        let planning_basis = self.impact_planning_basis();
        TransactionPreviewIdentity {
            transaction_id,
            position,
            planning_basis,
        }
    }

    /// One through three consecutive operations beginning anywhere, up to the last operation
    /// number a `usize` holds.
    pub fn transaction_operation_range(&mut self) -> TransactionOperationRange {
        let count = self.some_items();
        let widest = u64::try_from(usize::MAX).assured("supported targets address at most 64 bits");
        let operations = u64::try_from(count).assured("supported targets address at most 64 bits");
        let latest_first = widest
            .checked_sub(operations)
            .assured("a range holds at most three operations, far fewer than usize::MAX");
        let first_index = self.entropy.boundary_biased(0..=latest_first);
        let first_index = usize::try_from(first_index).verified("the draw is below usize::MAX");
        TransactionOperationRange::from_index_and_count(first_index, count)
            .verified("the first index leaves room for every operation of the range")
    }

    /// One operation of `operations`, landing on either end as often as between them.
    pub(crate) fn operation_within(
        &mut self,
        operations: TransactionOperationRange,
    ) -> TransactionOperationNumber {
        let first = u64::try_from(operations.first().get())
            .assured("supported targets address at most 64 bits");
        let last = u64::try_from(operations.last().get())
            .assured("supported targets address at most 64 bits");
        let drawn = self.entropy.boundary_biased(first..=last);
        let drawn = usize::try_from(drawn).verified("the draw lies between two operation numbers");
        let drawn = NonZeroUsize::new(drawn).verified("the draw is at least the first operation");
        TransactionOperationNumber::new(drawn)
    }

    /// One through three operations of `operations` that contributed to an impact item.
    pub fn impact_attribution(
        &mut self,
        operations: TransactionOperationRange,
    ) -> ImpactAttribution {
        let count = self.some_items();
        let mut contributors = Vec::with_capacity(count);
        for _ in 0..count {
            contributors.push(self.operation_within(operations));
        }
        ImpactAttribution::new(contributors).assured("at least one operation was drawn above")
    }

    /// Any node of any kind.
    fn any_node_ref(&mut self) -> NodeRef {
        let kind = self.model_kind();
        NodeRef::new(kind, self.name::<ModelName>())
    }

    /// A node of a kind an execution graph runs.
    fn execution_node_ref(&mut self) -> NodeRef {
        let kind = self.entropy.pick(EXECUTION_KINDS);
        NodeRef::new(kind, self.name::<ModelName>())
    }

    /// Which concrete executions of a node an impact covers: the unbranched one, every one, every
    /// key of a declared branch, or one through three fingerprinted keys of it.
    pub fn concrete_branch_coverage(&mut self) -> ConcreteBranchCoverage {
        match self.entropy.byte() % 4 {
            0 => ConcreteBranchCoverage::Unbranched,
            1 => ConcreteBranchCoverage::All,
            2 => ConcreteBranchCoverage::AllOfBranch {
                branch: self.name(),
            },
            _ => {
                let branch = self.name();
                let count = self.some_items();
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    keys.push(BranchKeyFingerprint::new(self.digest()));
                }
                ConcreteBranchCoverage::selected(branch, keys)
                    .assured("at least one key was drawn above")
            }
        }
    }

    /// An execution node with the concrete executions an impact covers.
    pub fn execution_node_coverage(&mut self) -> ImpactNodeCoverage {
        let node = self.execution_node_ref();
        let branches = self.concrete_branch_coverage();
        ImpactNodeCoverage::execution(node, branches)
    }

    /// A configuration-only node, which an impact names without executions.
    fn configuration_node_coverage(&mut self) -> ImpactNodeCoverage {
        let kind = self.entropy.pick(CONFIGURATION_KINDS);
        let node = NodeRef::new(kind, self.name::<ModelName>());
        ImpactNodeCoverage::configuration(node)
    }

    /// Any node of affected topology: a configuration-only node, or an execution node with the
    /// concrete executions an impact covers.
    pub fn impact_node_coverage(&mut self) -> ImpactNodeCoverage {
        if self.entropy.flag() {
            self.execution_node_coverage()
        } else {
            self.configuration_node_coverage()
        }
    }

    /// A diagnostic of any class, naming an operation of `operations` or none, with any message.
    pub fn impact_diagnostic(&mut self, operations: TransactionOperationRange) -> ImpactDiagnostic {
        let kind = self.entropy.pick(DIAGNOSTIC_KINDS);
        let operation = if self.entropy.flag() {
            Some(self.operation_within(operations))
        } else {
            None
        };
        let message = self.string();
        ImpactDiagnostic {
            kind,
            operation,
            message,
        }
    }

    /// Complete, or incomplete with one through three diagnostics about `operations`.
    pub fn impact_report_completeness(
        &mut self,
        operations: TransactionOperationRange,
    ) -> ImpactReportCompleteness {
        if !self.entropy.flag() {
            return ImpactReportCompleteness::Complete;
        }
        let count = self.some_items();
        let mut diagnostics = Vec::with_capacity(count);
        for _ in 0..count {
            diagnostics.push(self.impact_diagnostic(operations));
        }
        ImpactReportCompleteness::incomplete(diagnostics)
            .assured("at least one diagnostic was drawn above")
    }

    /// A named subgraph of `domain` to pause: one through three execution nodes and up to
    /// three relay gates, each attributed to operations of `operations`.
    pub fn quiesce_subgraph(
        &mut self,
        domain: &DomainName,
        operations: TransactionOperationRange,
    ) -> QuiesceSubgraph {
        let node_count = self.some_items();
        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            let coverage = self.execution_node_coverage();
            let attribution = self.impact_attribution(operations);
            nodes.push(AttributedImpactNode {
                coverage,
                attribution,
            });
        }
        let gate_count = self.entropy.count(ITEMS);
        let mut gate_boundaries = Vec::with_capacity(gate_count);
        for _ in 0..gate_count {
            let relay = self.name::<RelayName>();
            let branches = self.concrete_branch_coverage();
            let attribution = self.impact_attribution(operations);
            gate_boundaries.push(AttributedGateBoundary {
                boundary: ImpactGateBoundary { relay, branches },
                attribution,
            });
        }
        QuiesceSubgraph::new(domain.clone(), nodes, gate_boundaries)
    }

    /// The coordination a step over `operations` of `domain` requires: none, a named subgraph,
    /// or the whole domain.
    pub fn pause_requirement(
        &mut self,
        domain: &DomainName,
        operations: TransactionOperationRange,
    ) -> PauseRequirement {
        match self.entropy.byte() % 3 {
            0 => PauseRequirement::NoPause,
            1 => PauseRequirement::Subgraph {
                scope: self.quiesce_subgraph(domain, operations),
            },
            _ => PauseRequirement::Domain {
                domain: domain.clone(),
            },
        }
    }

    /// One side of affected topology: up to three distinct nodes, and up to three edges
    /// of any relation between them, each attributed to operations of `operations`.
    pub fn impact_topology(&mut self, operations: TransactionOperationRange) -> ImpactTopology {
        let node_count = self.entropy.count(ITEMS);
        let mut distinct = BTreeSet::new();
        for _ in 0..node_count {
            distinct.insert(self.impact_node_coverage());
        }
        let coverages = distinct.into_iter().collect::<Vec<_>>();
        let mut nodes = Vec::with_capacity(coverages.len());
        for coverage in &coverages {
            let attribution = self.impact_attribution(operations);
            nodes.push(AttributedImpactNode {
                coverage: coverage.clone(),
                attribution,
            });
        }
        let mut edges = Vec::new();
        if let Some(choices) = NonZeroUsize::new(coverages.len()) {
            let edge_count = self.entropy.count(ITEMS);
            for _ in 0..edge_count {
                let source = self.entropy.index(choices);
                let target = self.entropy.index(choices);
                let kind = self.entropy.pick(EDGE_KINDS);
                let attribution = self.impact_attribution(operations);
                let source = coverages
                    .get(source)
                    .verified("an index is below the node count");
                let target = coverages
                    .get(target)
                    .verified("an index is below the node count");
                edges.push(ImpactTopologyEdge {
                    source: source.clone(),
                    target: target.clone(),
                    kind,
                    attribution,
                });
            }
        }
        ImpactTopology {
            nodes: CanonicalImpactSet::new(nodes),
            edges: CanonicalImpactSet::new(edges),
        }
    }

    /// A node of any kind created, changed or dropped.
    fn configuration_impact(
        &mut self,
        operations: TransactionOperationRange,
    ) -> ConfigurationImpact {
        let node = self.any_node_ref();
        let transition = match self.entropy.byte() % 3 {
            0 => ConfigurationTransition::Created { node },
            1 => ConfigurationTransition::Changed { node },
            _ => ConfigurationTransition::Dropped { node },
        };
        let attribution = self.impact_attribution(operations);
        ConfigurationImpact {
            transition,
            attribution,
        }
    }

    /// Every execution of a node moving from one cluster node to another.
    fn ownership_move_impact(
        &mut self,
        operations: TransactionOperationRange,
    ) -> OwnershipMoveImpact {
        let node = ImpactNodeCoverage::all_executions(self.execution_node_ref());
        let source = self.cluster_node();
        let destination = self.cluster_node();
        let attribution = self.impact_attribution(operations);
        OwnershipMoveImpact {
            node,
            source,
            destination,
            attribution,
        }
    }

    /// `domain` starting or stopping.
    fn domain_lifecycle_impact(
        &mut self,
        domain: &DomainName,
        operations: TransactionOperationRange,
    ) -> DomainLifecycleImpact {
        let action = self
            .entropy
            .pick([DomainLifecycleAction::Start, DomainLifecycleAction::Stop]);
        let attribution = self.impact_attribution(operations);
        DomainLifecycleImpact {
            domain: domain.clone(),
            action,
            attribution,
        }
    }

    /// An execution node activating or deactivating, or a VHOST whose HTTPS listener installs a new
    /// certificate on every live node.
    fn activation_impact(&mut self, operations: TransactionOperationRange) -> ActivationImpact {
        let action = self.entropy.pick([
            ActivationAction::Activate,
            ActivationAction::Deactivate,
            ActivationAction::RefreshHttpsListener,
        ]);
        let node = match action {
            ActivationAction::Activate | ActivationAction::Deactivate => {
                self.execution_node_coverage()
            }
            ActivationAction::RefreshHttpsListener => {
                let vhost = NodeRef::new(ModelKind::Vhost, self.name::<ModelName>());
                ImpactNodeCoverage::configuration(vhost)
            }
        };
        let attribution = self.impact_attribution(operations);
        ActivationImpact {
            node,
            action,
            attribution,
        }
    }

    /// An execution node rebuilt for its configuration, its ownership or a recovery.
    fn rebuild_impact(&mut self, operations: TransactionOperationRange) -> RebuildImpact {
        let node = self.execution_node_coverage();
        let reason = self.entropy.pick([
            RebuildReason::Configuration,
            RebuildReason::Ownership,
            RebuildReason::Recovery,
        ]);
        let attribution = self.impact_attribution(operations);
        RebuildImpact {
            node,
            reason,
            attribution,
        }
    }

    /// Runtime state of an execution node a change makes meaningless.
    fn state_reset_impact(&mut self, operations: TransactionOperationRange) -> StateResetImpact {
        let node = self.execution_node_coverage();
        let state = self.entropy.pick(STATE_PURGES);
        let attribution = self.impact_attribution(operations);
        StateResetImpact {
            node,
            state,
            attribution,
        }
    }

    /// An execution node whose current work is flushed when a gate engages.
    fn force_flush_impact(&mut self, operations: TransactionOperationRange) -> ForceFlushImpact {
        let node = self.execution_node_coverage();
        let attribution = self.impact_attribution(operations);
        ForceFlushImpact { node, attribution }
    }

    /// A resource entering the catalog.
    fn resource_catalog_impact(
        &mut self,
        operations: TransactionOperationRange,
    ) -> ResourceCatalogImpact {
        let resource = self.name();
        let attribution = self.impact_attribution(operations);
        ResourceCatalogImpact {
            resource,
            action: ResourceCatalogAction::Create,
            attribution,
        }
    }

    /// The version a resource version request binds: the number it names, or for `LATEST` any
    /// number it resolved to.
    fn bound_version(&mut self, requested: RequestedResourceVersion) -> u64 {
        match requested {
            RequestedResourceVersion::Number(number) => number,
            RequestedResourceVersion::Latest => self.entropy.any_u64(),
        }
    }

    /// A node that binds a resource version, the version it asked for, and the version it binds.
    fn resource_binding_impact(
        &mut self,
        operations: TransactionOperationRange,
    ) -> ResourceBindingImpact {
        let node = self.resource_binding_ref();
        let resource = self.name();
        let requested = self.requested_version();
        let version = self.bound_version(requested);
        let attribution = self.impact_attribution(operations);
        ResourceBindingImpact {
            node,
            resource,
            requested,
            version,
            attribution,
        }
    }

    /// Effects of every family, up to three items each, attributed to operations of
    /// `operations`: configuration transitions, the topology before and after, ownership moves,
    /// lifecycle changes of `domain`, activations, rebuilds, state resets, force flushes, and
    /// resource catalog and binding changes.
    pub fn impact_effects(
        &mut self,
        domain: &DomainName,
        operations: TransactionOperationRange,
    ) -> ImpactEffects {
        let changed_configuration =
            self.impact_set(|arbitrary| arbitrary.configuration_impact(operations));
        let before = self.impact_topology(operations);
        let after = self.impact_topology(operations);
        let ownership_moves =
            self.impact_set(|arbitrary| arbitrary.ownership_move_impact(operations));
        let lifecycle =
            self.impact_set(|arbitrary| arbitrary.domain_lifecycle_impact(domain, operations));
        let activations = self.impact_set(|arbitrary| arbitrary.activation_impact(operations));
        let rebuilds = self.impact_set(|arbitrary| arbitrary.rebuild_impact(operations));
        let state_resets = self.impact_set(|arbitrary| arbitrary.state_reset_impact(operations));
        let force_flushes = self.impact_set(|arbitrary| arbitrary.force_flush_impact(operations));
        let resource_catalog =
            self.impact_set(|arbitrary| arbitrary.resource_catalog_impact(operations));
        let resource_bindings =
            self.impact_set(|arbitrary| arbitrary.resource_binding_impact(operations));
        ImpactEffects {
            changed_configuration,
            topology: AffectedTopology { before, after },
            ownership_moves,
            lifecycle,
            activations,
            rebuilds,
            state_resets,
            force_flushes,
            resource_catalog,
            resource_bindings,
        }
    }

    /// The outcomes one pause attempt records in order: requested, then still pending, confirmed
    /// and possibly failing to drain, failed before engaging, or uncertain; an engaged or uncertain
    /// attempt may then be released. Every diagnostic names an operation of `operations` or none.
    fn quiescence_outcomes(
        &mut self,
        operations: TransactionOperationRange,
    ) -> Vec<QuiescenceOutcome> {
        let mut outcomes = vec![QuiescenceOutcome::Requested];
        match self.entropy.byte() % 4 {
            0 => {}
            1 => {
                outcomes.push(QuiescenceOutcome::Confirmed);
                if self.entropy.flag() {
                    let diagnostic = self.impact_diagnostic(operations);
                    outcomes.push(QuiescenceOutcome::Failed { diagnostic });
                }
                if self.entropy.flag() {
                    outcomes.push(QuiescenceOutcome::Released);
                }
            }
            2 => {
                let diagnostic = self.impact_diagnostic(operations);
                outcomes.push(QuiescenceOutcome::Failed { diagnostic });
            }
            _ => {
                let diagnostic = self.impact_diagnostic(operations);
                outcomes.push(QuiescenceOutcome::Uncertain { diagnostic });
                if self.entropy.flag() {
                    outcomes.push(QuiescenceOutcome::Released);
                }
            }
        }
        outcomes
    }

    /// One pause attempt of a step over `operations`: the whole of `domain` or a named subgraph of
    /// it, with the outcomes the attempt recorded.
    pub fn actual_quiescence(
        &mut self,
        domain: &DomainName,
        operations: TransactionOperationRange,
    ) -> ActualQuiescence {
        let requirement = if self.entropy.flag() {
            PauseRequirement::Subgraph {
                scope: self.quiesce_subgraph(domain, operations),
            }
        } else {
            PauseRequirement::Domain {
                domain: domain.clone(),
            }
        };
        let outcomes = self.quiescence_outcomes(operations);
        ActualQuiescence {
            requirement,
            outcomes,
        }
    }

    /// What a step over `operations` of `domain` actually did: up to three pause attempts, and
    /// an outcome of unattempted, applying, applied or failed. A step that began applying records
    /// the effects it applied; an unattempted one has recorded none yet.
    pub fn actual_execution_step_impact_of(
        &mut self,
        domain: &DomainName,
        operations: TransactionOperationRange,
    ) -> ActualExecutionStepImpact {
        let attempts = self.entropy.count(ITEMS);
        let mut quiescence = Vec::with_capacity(attempts);
        for _ in 0..attempts {
            quiescence.push(self.actual_quiescence(domain, operations));
        }
        let outcome = match self.entropy.byte() % 4 {
            0 => ExecutionStepOutcome::Unattempted,
            1 => ExecutionStepOutcome::Applying,
            2 => ExecutionStepOutcome::Applied,
            _ => ExecutionStepOutcome::Failed {
                diagnostic: self.impact_diagnostic(operations),
            },
        };
        let effects = match &outcome {
            ExecutionStepOutcome::Unattempted => ImpactEffects::default(),
            ExecutionStepOutcome::Applying
            | ExecutionStepOutcome::Applied
            | ExecutionStepOutcome::Failed { .. } => self.impact_effects(domain, operations),
        };
        ActualExecutionStepImpact {
            outcome,
            quiescence,
            effects,
        }
    }

    /// What a step over any operations of any domain actually did, as
    /// [`Self::actual_execution_step_impact_of`] builds it.
    pub fn actual_execution_step_impact(&mut self) -> ActualExecutionStepImpact {
        let domain = self.name::<DomainName>();
        let operations = self.transaction_operation_range();
        self.actual_execution_step_impact_of(&domain, operations)
    }

    /// One step over `operations` of `domain` as a report at `stage` records it.
    fn staged_execution_step(
        &mut self,
        domain: &DomainName,
        operations: TransactionOperationRange,
        stage: ReportStage,
    ) -> ExecutionStepImpactReport {
        let completeness = match stage {
            ReportStage::Any => self.impact_report_completeness(operations),
            ReportStage::Admitted => ImpactReportCompleteness::Complete,
        };
        let pause = self.pause_requirement(domain, operations);
        let effects = self.impact_effects(domain, operations);
        let planned = PlannedExecutionStepImpact {
            completeness,
            pause,
            effects,
        };
        let actual = match stage {
            ReportStage::Any => self.actual_execution_step_impact_of(domain, operations),
            ReportStage::Admitted => ActualExecutionStepImpact::unattempted(),
        };
        ExecutionStepImpactReport::new(operations, planned, actual)
    }

    /// One step over `operations` of `domain`: any planned completeness, pause and effects, and
    /// anything it actually did, every item attributed to operations of `operations`.
    pub fn execution_step_impact_report(
        &mut self,
        domain: &DomainName,
        operations: TransactionOperationRange,
    ) -> ExecutionStepImpactReport {
        self.staged_execution_step(domain, operations, ReportStage::Any)
    }

    /// Any aspect of a Model change.
    pub fn model_change_aspect(&mut self) -> ModelChangeAspect {
        self.entropy.pick(MODEL_CHANGE_ASPECTS)
    }

    /// An operation of `domain` of any kind. A configuration operation names a node of any kind,
    /// and a resource rebinding binds the version it names, or any version for `LATEST`.
    pub fn transaction_operation(&mut self, domain: &DomainName) -> TransactionOperation {
        let domain = domain.clone();
        match self.entropy.byte() % 9 {
            0 => TransactionOperation::StopDomain { domain },
            1 => TransactionOperation::StartDomain { domain },
            2 => TransactionOperation::AlterDomain { domain },
            3 => TransactionOperation::CreateResource {
                domain,
                resource: self.name(),
            },
            4 => TransactionOperation::ResetWasmState {
                domain,
                processor: self.name(),
            },
            5 => TransactionOperation::CreateConfiguration {
                domain,
                node: self.any_node_ref(),
            },
            6 => TransactionOperation::AlterConfiguration {
                domain,
                node: self.any_node_ref(),
            },
            7 => TransactionOperation::DropConfiguration {
                domain,
                node: self.any_node_ref(),
            },
            _ => {
                let resource = self.name();
                let requested = self.requested_version();
                let version = self.bound_version(requested);
                TransactionOperation::RebindResource {
                    domain,
                    resource,
                    requested,
                    version,
                }
            }
        }
    }

    /// The ordered reasons `operation` contributes, each about what the operation names: up to
    /// three changed aspects of a configuration operation's node, up to three nodes a
    /// rebinding moves to its version, the placement an `ALTER DOMAIN` changes unless it left the
    /// policy as it was, the start or stop of the domain, the resource a `CREATE RESOURCE` adds
    /// unless it already existed, and the guest state a reset replaces.
    pub fn operation_impact_reasons(
        &mut self,
        operation: &TransactionOperation,
    ) -> Vec<OperationImpactReason> {
        match operation {
            TransactionOperation::CreateConfiguration { node, .. }
            | TransactionOperation::AlterConfiguration { node, .. }
            | TransactionOperation::DropConfiguration { node, .. } => {
                let count = self.entropy.count(ITEMS);
                let mut reasons = Vec::with_capacity(count);
                for _ in 0..count {
                    let aspect = self.model_change_aspect();
                    reasons.push(OperationImpactReason::Configuration {
                        node: node.clone(),
                        aspect,
                    });
                }
                reasons
            }
            TransactionOperation::RebindResource {
                resource, version, ..
            } => {
                let count = self.entropy.count(ITEMS);
                let mut reasons = Vec::with_capacity(count);
                for _ in 0..count {
                    let node = self.resource_binding_ref();
                    let from_version = self.entropy.any_u64();
                    reasons.push(OperationImpactReason::ResourceRebinding {
                        node,
                        resource: resource.clone(),
                        from_version,
                        to_version: *version,
                    });
                }
                reasons
            }
            TransactionOperation::AlterDomain { .. } => {
                if self.entropy.flag() {
                    vec![OperationImpactReason::DomainPlacement]
                } else {
                    Vec::new()
                }
            }
            TransactionOperation::StartDomain { .. } => vec![OperationImpactReason::DomainStart],
            TransactionOperation::StopDomain { .. } => vec![OperationImpactReason::DomainStop],
            TransactionOperation::CreateResource { resource, .. } => {
                if self.entropy.flag() {
                    vec![OperationImpactReason::ResourceCatalog {
                        resource: resource.clone(),
                    }]
                } else {
                    Vec::new()
                }
            }
            TransactionOperation::ResetWasmState { processor, .. } => {
                vec![OperationImpactReason::WasmStateReset {
                    node: NodeRef::new(ModelKind::WasmProcessor, processor),
                }]
            }
        }
    }

    /// What operation `number` contributes inside `step`: its reasons, and effects attributed to it
    /// alone. Its completeness is the step's planned completeness.
    fn operation_impact_report(
        &mut self,
        number: TransactionOperationNumber,
        operation: TransactionOperation,
        step: &ExecutionStepImpactReport,
    ) -> OperationImpactReport {
        let reasons = self.operation_impact_reasons(&operation);
        let own = TransactionOperationRange::new(number, number)
            .assured("a range of one operation ends where it begins");
        let contribution = self.impact_effects(operation.domain(), own);
        OperationImpactReport {
            number,
            operation,
            execution_step: step.operations(),
            completeness: step.planned().completeness.clone(),
            reasons,
            contribution,
        }
    }

    /// A report of `domain` at `stage`: up to three operations of any kind, divided into the
    /// execution steps the planner would execute them in.
    fn staged_impact_report(
        &mut self,
        domain: DomainName,
        stage: ReportStage,
    ) -> TransactionImpactReport {
        let accepted = self.entropy.count(ITEMS);
        let planning_basis = self.impact_planning_basis();
        let mut drawn = Vec::with_capacity(accepted);
        for _ in 0..accepted {
            drawn.push(self.transaction_operation(&domain));
        }
        let mut operations = Vec::with_capacity(accepted);
        let mut execution_steps = Vec::new();
        let mut diagnostics = Vec::new();
        let mut first_index = 0_usize;
        while let Some(first) = drawn.get(first_index) {
            let mut end = first_index
                .checked_add(1)
                .assured("a report holds at most three operations");
            if first.joins_model_run() {
                while let Some(next) = drawn.get(end)
                    && next.joins_model_run()
                {
                    end = end
                        .checked_add(1)
                        .assured("a report holds at most three operations");
                }
            }
            let count = end
                .checked_sub(first_index)
                .verified("a step ends after the operation it begins with");
            let range = TransactionOperationRange::from_index_and_count(first_index, count)
                .assured("a step of at most three operations is addressable");
            let step = self.staged_execution_step(&domain, range, stage);
            diagnostics.extend(step.planned().completeness.diagnostics().iter().cloned());
            let executed = drawn
                .get(first_index..end)
                .verified("the step's bounds were found in the drawn operations");
            for (number, operation) in range.operations().zip(executed) {
                let report = self.operation_impact_report(number, operation.clone(), &step);
                operations.push(report);
            }
            execution_steps.push(step);
            first_index = end;
        }
        let completeness = if diagnostics.is_empty() {
            ImpactReportCompleteness::Complete
        } else {
            ImpactReportCompleteness::incomplete(diagnostics)
                .verified("the diagnostics were found non-empty above")
        };
        TransactionImpactReport::new(
            domain,
            TransactionPosition::new(accepted),
            planning_basis,
            completeness,
            operations,
            execution_steps,
        )
        .assured(
            "operations are numbered from one, each step continues where the previous one ended, \
             and every operation and pause names the report's domain",
        )
    }

    /// A report of `domain` at any point of its transaction's life: up to three operations of
    /// any kind divided into execution steps, any planning basis, every step with any planned
    /// impact and anything it actually did.
    pub fn transaction_impact_report(&mut self, domain: DomainName) -> TransactionImpactReport {
        self.staged_impact_report(domain, ReportStage::Any)
    }

    /// A report of `domain` as commit admission freezes it: every step planned completely and none
    /// attempted yet.
    pub fn admitted_impact_report(&mut self, domain: DomainName) -> TransactionImpactReport {
        self.staged_impact_report(domain, ReportStage::Admitted)
    }

    /// A stored Model created, replaced by another of its family, or dropped.
    pub fn transaction_model_transition(&mut self) -> TransactionModelTransition {
        match self.entropy.byte() % 3 {
            0 => TransactionModelTransition::Create {
                model: Box::new(self.pinned_model()),
            },
            1 => {
                let before = self.pinned_model();
                let family = ModelVariant::of(&before);
                let requested = self.model_of(family);
                let after = self.pinned(requested);
                TransactionModelTransition::Replace {
                    before: Box::new(before),
                    after: Box::new(after),
                }
            }
            _ => TransactionModelTransition::Drop {
                model: Box::new(self.pinned_model()),
            },
        }
    }

    /// The gates an entity pause engages: up to three execution nodes and up to three
    /// relays, each sorted and named once.
    pub fn transaction_entity_gate_plan(&mut self) -> TransactionEntityGatePlan {
        let entity_count = self.entropy.count(ITEMS);
        let mut affected_entities = BTreeSet::new();
        for _ in 0..entity_count {
            affected_entities.insert(self.execution_node_ref());
        }
        let relay_count = self.entropy.count(ITEMS);
        let mut relays = BTreeSet::new();
        for _ in 0..relay_count {
            relays.insert(self.name::<RelayName>());
        }
        TransactionEntityGatePlan {
            affected_entities: affected_entities.into_iter().collect(),
            relays: relays.into_iter().collect(),
        }
    }

    /// The start a `START DOMAIN` statement requests: where the domain stopped, now, or a logical
    /// instant, each at any positive finite time rate.
    fn requested_start(&mut self) -> DomainStartPoint {
        let statement = self.statement_of(StatementVariant::StartDomain);
        let start = match statement {
            Statement::StartDomain(start) => Some(start.start),
            _ => None,
        };
        start.assured("statement_of builds the statement form it is asked for")
    }

    /// The placement policy an `ALTER DOMAIN` statement sets.
    fn altered_placement(&mut self) -> PlacementPolicy {
        let statement = self.statement_of(StatementVariant::AlterDomain);
        let policy = match statement {
            Statement::AlterDomain(alter) => Some(alter.policy),
            _ => None,
        };
        policy.assured("statement_of builds the statement form it is asked for")
    }

    /// The reset a `RESET WASM PROCESSOR ... STATE` statement requests for `processor` of `domain`:
    /// its unbranched state, every branch, or the branch its fields select.
    fn requested_reset(
        &mut self,
        domain: &DomainName,
        processor: &WasmProcessorName,
    ) -> ResetWasmState {
        let statement = self.statement_of(StatementVariant::ResetWasmState);
        let scope = match statement {
            Statement::ResetWasmState(reset) => Some(reset.scope),
            _ => None,
        };
        let scope = scope.assured("statement_of builds the statement form it is asked for");
        ResetWasmState {
            domain: domain.clone(),
            processor: processor.clone(),
            scope,
        }
    }

    /// The execution reference a reset request carries: a UUIDv7, as a client mints one.
    fn reset_request(&mut self) -> CommandExecutionReference {
        let text = self.transaction_id();
        CommandExecutionReference::parse(text)
            .assured("a UUID's text holds only hexadecimal digits and hyphens")
    }

    /// The node process that holds a paced domain's clock authority: any cluster node, in any
    /// incarnation.
    fn clock_authority(&mut self) -> ClusterNodeIdentity {
        let node = self.cluster_node();
        let incarnation = ClusterNodeIncarnation::new(self.entropy.any_u64());
        ClusterNodeIdentity::new(node, incarnation)
    }

    /// The clock mapping of a domain started at `start`: any wall instant it began at, and the
    /// logical start and rate `start` resolves to there.
    fn clock_mapping(&mut self, start: &DomainStartPoint) -> DomainClockState {
        let wall_started_at = Timestamp::from_unix_nanos(self.entropy.any_i64());
        let (logical_start, time_rate) = start.resolve_at(wall_started_at);
        DomainClockState::new(wall_started_at, logical_start, time_rate)
    }

    /// The clock start and authority commit admission resolves for a `START DOMAIN`: a start that
    /// asked for now becomes the logical instant it began at, and a paced domain records the clock
    /// mapping it began with and the authority it selected, while an unpaced one records neither.
    pub fn transaction_resolved_domain_start(&mut self) -> TransactionResolvedDomainStart {
        let requested = self.requested_start();
        let wall_started_at = Timestamp::from_unix_nanos(self.entropy.any_i64());
        let (logical_start, time_rate) = requested.resolve_at(wall_started_at);
        let start = match requested {
            DomainStartPoint::Now { .. } => DomainStartPoint::At {
                timestamp: logical_start,
                time_rate,
            },
            DomainStartPoint::Resume | DomainStartPoint::At { .. } => requested,
        };
        if !self.entropy.flag() {
            return TransactionResolvedDomainStart {
                start,
                clock: None,
                authority: None,
            };
        }
        let clock = DomainClockState::new(wall_started_at, logical_start, time_rate);
        let authority = self.clock_authority();
        TransactionResolvedDomainStart {
            start,
            clock: Some(clock),
            authority: Some(authority),
        }
    }

    /// The state an `ALTER DOMAIN` step installs for `domain`: any pace, the policy the statement
    /// sets, and a stopped domain or a running one that has started at least once. A running paced
    /// domain carries the clock mapping its last start began with.
    fn altered_domain_state(&mut self, domain: &DomainName) -> DomainState {
        let pace = if self.entropy.flag() {
            let period = self.clock_period();
            let skew = self.clock_skew();
            DomainPace::Paced { period, skew }
        } else {
            DomainPace::Unpaced
        };
        let placement = self.altered_placement();
        let running = self.entropy.flag();
        let status = if running {
            DomainStatus::Running
        } else {
            DomainStatus::Stopped
        };
        let start_version = if running {
            self.entropy.boundary_biased(1..=u64::MAX)
        } else {
            self.entropy.any_u64()
        };
        let last_start = self.requested_start();
        let clock = if running && pace.is_paced() {
            Some(self.clock_mapping(&last_start))
        } else {
            None
        };
        DomainState {
            id: domain.clone(),
            config: DomainConfig { pace, placement },
            status,
            start_version,
            last_start,
            clock,
        }
    }

    /// One entry of a published schedule: a stored Model of an execution family, its schema
    /// fingerprint, and up to three distinct cluster nodes it is assigned to, one of which may
    /// be its primary.
    fn schedule_entry(&mut self) -> ScheduledNode {
        let family = self.entropy.pick(EXECUTION_FAMILIES);
        let requested = self.model_of(family);
        let model = self.pinned(requested);
        let fingerprint = SchemaFingerprint::from_digest(self.digest());
        let assigned_count = self.entropy.count(ITEMS);
        let mut distinct = BTreeSet::new();
        for _ in 0..assigned_count {
            distinct.insert(self.cluster_node());
        }
        let assigned = distinct.into_iter().collect::<Vec<_>>();
        let mut primary = None;
        if let Some(placements) = NonZeroUsize::new(assigned.len())
            && self.entropy.flag()
        {
            let chosen = self.entropy.index(placements);
            primary = assigned.get(chosen).cloned();
        }
        ScheduledNode::new(model, fingerprint).placed_on(primary, assigned)
    }

    /// A placement group of a schedule: any of the scheduled `identities`, and the cluster node it
    /// shares, or none.
    fn schedule_group(&mut self, identities: &[NodeRef]) -> PlacementGroupSchedule {
        let mut members = Vec::new();
        for identity in identities {
            if self.entropy.flag() {
                members.push(identity.clone());
            }
        }
        let primary_node = if self.entropy.flag() {
            Some(self.cluster_node())
        } else {
            None
        };
        PlacementGroupSchedule {
            members,
            primary_node,
        }
    }

    /// A schedule a step publishes for `domain`: up to three entries and up to three
    /// placement groups of them.
    fn published_schedule(&mut self, domain: &DomainName) -> DomainSchedule {
        let entry_count = self.entropy.count(ITEMS);
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(self.schedule_entry());
        }
        let mut schedule = DomainSchedule::new(domain.clone(), entries, Vec::new());
        let identities = schedule.nodes.keys().cloned().collect::<Vec<_>>();
        let group_count = self.entropy.count(ITEMS);
        for _ in 0..group_count {
            let group = self.schedule_group(&identities);
            schedule.placement_groups.push(group);
        }
        schedule
    }

    /// Up to three operations of `operations` that changed nothing, ascending and named once.
    fn no_op_operations(
        &mut self,
        operations: TransactionOperationRange,
    ) -> Vec<TransactionOperationNumber> {
        let count = self.entropy.count(ITEMS);
        let mut numbers = BTreeSet::new();
        for _ in 0..count {
            numbers.insert(self.operation_within(operations));
        }
        numbers.into_iter().collect()
    }

    /// The decision for a Model run over `operations` of `domain`: up to three Model
    /// transitions, the schedule it publishes or none, the operations that changed nothing, and the
    /// gates its Model and ownership changes engage.
    fn models_commit_step(
        &mut self,
        domain: &DomainName,
        operations: TransactionOperationRange,
    ) -> TransactionCommitStepKind {
        let transition_count = self.entropy.count(ITEMS);
        let mut transitions = Vec::with_capacity(transition_count);
        for _ in 0..transition_count {
            transitions.push(self.transaction_model_transition());
        }
        let schedule = if self.entropy.flag() {
            Some(Box::new(self.published_schedule(domain)))
        } else {
            None
        };
        let no_op_operations = self.no_op_operations(operations);
        let model_gate = self.transaction_entity_gate_plan();
        let ownership_gate = self.transaction_entity_gate_plan();
        TransactionCommitStepKind::Models {
            transitions,
            schedule,
            no_op_operations,
            model_gate,
            ownership_gate,
        }
    }

    /// The decision for an `ALTER DOMAIN` of `domain`: the state it installs, the schedule it
    /// publishes or none, and the gates its ownership moves engage.
    fn alter_domain_commit_step(&mut self, domain: &DomainName) -> TransactionCommitStepKind {
        let next = self.altered_domain_state(domain);
        let schedule = if self.entropy.flag() {
            Some(Box::new(self.published_schedule(domain)))
        } else {
            None
        };
        let ownership_gate = self.transaction_entity_gate_plan();
        TransactionCommitStepKind::AlterDomain {
            next: Box::new(next),
            schedule,
            ownership_gate,
        }
    }

    /// The decision for a reset of `processor` of `domain`: the reset requested, the reference of
    /// its request, and the schedule it publishes.
    fn reset_commit_step(
        &mut self,
        domain: &DomainName,
        processor: &WasmProcessorName,
    ) -> TransactionCommitStepKind {
        let reset = self.requested_reset(domain, processor);
        let request = self.reset_request();
        let schedule = self.published_schedule(domain);
        TransactionCommitStepKind::ResetWasmState {
            reset: Box::new(reset),
            request,
            schedule: Box::new(schedule),
        }
    }

    /// The decision commit admission records for the step over `operations` that begins with
    /// `operation`: a Model run for a Model operation, and otherwise the decision of the
    /// operation's own kind. Every value it holds names the operation's domain, and a resource or
    /// reset decision names the operation's resource or processor.
    pub fn transaction_commit_step_kind(
        &mut self,
        operation: &TransactionOperation,
        operations: TransactionOperationRange,
    ) -> TransactionCommitStepKind {
        match operation {
            TransactionOperation::CreateConfiguration { domain, .. }
            | TransactionOperation::AlterConfiguration { domain, .. }
            | TransactionOperation::DropConfiguration { domain, .. }
            | TransactionOperation::RebindResource { domain, .. } => {
                self.models_commit_step(domain, operations)
            }
            TransactionOperation::AlterDomain { domain } => self.alter_domain_commit_step(domain),
            TransactionOperation::StartDomain { .. } => TransactionCommitStepKind::StartDomain {
                resolved: self.transaction_resolved_domain_start(),
            },
            TransactionOperation::StopDomain { .. } => TransactionCommitStepKind::StopDomain,
            TransactionOperation::CreateResource { resource, .. } => {
                let already_existed = self.entropy.flag();
                TransactionCommitStepKind::CreateResource {
                    resource: resource.clone(),
                    already_existed,
                }
            }
            TransactionOperation::ResetWasmState { domain, processor } => {
                self.reset_commit_step(domain, processor)
            }
        }
    }

    /// The plan admitted for `report`, which is a report as commit admission freezes it, such as
    /// one [`Self::admitted_impact_report`] builds: a preview of a new transaction at the report's
    /// position and planning basis, and every step of the report paired with the decision its
    /// first operation calls for.
    pub fn transaction_commit_plan_for(
        &mut self,
        report: &TransactionImpactReport,
    ) -> TransactionCommitPlan {
        let transaction_id = self.transaction_id();
        let preview = TransactionPreviewIdentity {
            transaction_id,
            position: report.position(),
            planning_basis: report.planning_basis(),
        };
        let mut steps = Vec::with_capacity(report.execution_steps().len());
        for impact in report.execution_steps() {
            let operations = impact.operations();
            let first = report
                .operations()
                .get(operations.first_index())
                .assured("a valid report holds every operation its steps execute");
            let kind = self.transaction_commit_step_kind(&first.operation, operations);
            steps.push(TransactionCommitPlanStep {
                impact: impact.clone(),
                kind,
            });
        }
        TransactionCommitPlan { preview, steps }
    }

    /// The plan admitted for a transaction of `domain`, built for a report
    /// [`Self::admitted_impact_report`] builds.
    pub fn transaction_commit_plan(&mut self, domain: DomainName) -> TransactionCommitPlan {
        let report = self.admitted_impact_report(domain);
        self.transaction_commit_plan_for(&report)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        ActivationAction, ActualExecutionStepImpact, ConcreteBranchCoverage,
        ConfigurationTransition, DomainLifecycleAction, DomainName, DomainSchedule,
        DomainStartPoint, DomainState, DomainStatus, ExecutionStepImpactReport,
        ExecutionStepOutcome, ImpactAttribution, ImpactDiagnostic, ImpactDiagnosticKind,
        ImpactEdgeKind, ImpactEffects, ImpactNodeCoverage, ImpactReportCompleteness,
        ImpactTopology, ModelChangeAspect, ModelKind, OperationImpactReason, OperationImpactReport,
        PauseRequirement, QuiesceSubgraph, QuiescenceOutcome, RebuildReason,
        RequestedResourceVersion, ResourceCatalogAction, StatePurge, TransactionCommitPlan,
        TransactionCommitStepKind, TransactionEntityGatePlan, TransactionImpactReport,
        TransactionModelTransition, TransactionOperation, TransactionOperationNumber,
        TransactionOperationRange, TransactionResolvedDomainStart,
    };
    use rkyv::{
        Archive, Deserialize,
        api::high::{HighDeserializer, HighValidator},
        bytecheck::CheckBytes,
        rancor::Error,
    };
    use strum::IntoEnumIterator as _;

    use super::{
        CONFIGURATION_KINDS, DIAGNOSTIC_KINDS, EDGE_KINDS, EXECUTION_FAMILIES, EXECUTION_KINDS,
        MODEL_CHANGE_ASPECTS, STATE_PURGES, StepBoundary as _,
    };
    use crate::{Arbitrary, Domain, ModelVariant};

    /// How many seeds each check draws its values from.
    const SEEDS: u64 = 256;

    /// How many bytes each value is drawn from: enough that the first steps of a transaction of
    /// three operations are built before the bytes run out.
    const BYTES: usize = 8192;

    /// Every variant, family and shape the generators promise to reach, as [`Walk`] names them.
    const EXPECTED: [&str; 107] = [
        "TransactionImpactReport::empty",
        "TransactionOperationRange::single",
        "TransactionOperationRange::several",
        "TransactionOperation::CreateConfiguration",
        "TransactionOperation::AlterConfiguration",
        "TransactionOperation::DropConfiguration",
        "TransactionOperation::AlterDomain",
        "TransactionOperation::StartDomain",
        "TransactionOperation::StopDomain",
        "TransactionOperation::CreateResource",
        "TransactionOperation::RebindResource",
        "TransactionOperation::ResetWasmState",
        "OperationImpactReason::Configuration",
        "OperationImpactReason::DomainPlacement",
        "OperationImpactReason::DomainStart",
        "OperationImpactReason::DomainStop",
        "OperationImpactReason::ResourceCatalog",
        "OperationImpactReason::ResourceRebinding",
        "OperationImpactReason::WasmStateReset",
        "ImpactReportCompleteness::Complete",
        "ImpactReportCompleteness::Incomplete",
        "ImpactDiagnosticKind::Planning",
        "ImpactDiagnosticKind::Topology",
        "ImpactDiagnosticKind::Quiescence",
        "ImpactDiagnosticKind::Ownership",
        "ImpactDiagnosticKind::Activation",
        "ImpactDiagnosticKind::Application",
        "ImpactDiagnosticKind::Recovery",
        "ImpactDiagnostic::operation",
        "ImpactDiagnostic::no_operation",
        "PauseRequirement::NoPause",
        "PauseRequirement::Subgraph",
        "PauseRequirement::Domain",
        "QuiesceSubgraph::gate_boundaries",
        "ConcreteBranchCoverage::All",
        "ConcreteBranchCoverage::Unbranched",
        "ConcreteBranchCoverage::AllOfBranch",
        "ConcreteBranchCoverage::Selected",
        "ImpactNodeCoverage::configuration",
        "ImpactNodeCoverage::execution",
        "ImpactTopology::nodes",
        "ImpactTopology::edges",
        "ImpactEdgeKind::ConfigurationDependency",
        "ImpactEdgeKind::Dataflow",
        "ImpactEdgeKind::MessageError",
        "ImpactEdgeKind::CorrelationTimeout",
        "ImpactEdgeKind::MaterializedState",
        "ImpactEffects::changed_configuration",
        "ImpactEffects::topology_before",
        "ImpactEffects::topology_after",
        "ImpactEffects::ownership_moves",
        "ImpactEffects::lifecycle",
        "ImpactEffects::activations",
        "ImpactEffects::rebuilds",
        "ImpactEffects::state_resets",
        "ImpactEffects::force_flushes",
        "ImpactEffects::resource_catalog",
        "ImpactEffects::resource_bindings",
        "ConfigurationTransition::Created",
        "ConfigurationTransition::Changed",
        "ConfigurationTransition::Dropped",
        "DomainLifecycleAction::Start",
        "DomainLifecycleAction::Stop",
        "ActivationAction::Activate",
        "ActivationAction::Deactivate",
        "ActivationAction::RefreshHttpsListener",
        "RebuildReason::Configuration",
        "RebuildReason::Ownership",
        "RebuildReason::Recovery",
        "StatePurge::DeduplicatorKeyspace",
        "StatePurge::ReordererBuffer",
        "StatePurge::WindowAccumulator",
        "StatePurge::CorrelationBuffer",
        "StatePurge::InferencerWarmState",
        "StatePurge::WasmGuestState",
        "ResourceCatalogAction::Create",
        "RequestedResourceVersion::Number",
        "RequestedResourceVersion::Latest",
        "ActualExecutionStepImpact::quiescence",
        "QuiescenceOutcome::Requested",
        "QuiescenceOutcome::Confirmed",
        "QuiescenceOutcome::Failed",
        "QuiescenceOutcome::Uncertain",
        "QuiescenceOutcome::Released",
        "ExecutionStepOutcome::Unattempted",
        "ExecutionStepOutcome::Applying",
        "ExecutionStepOutcome::Applied",
        "ExecutionStepOutcome::Failed",
        "TransactionCommitStepKind::Models",
        "TransactionCommitStepKind::AlterDomain",
        "TransactionCommitStepKind::StartDomain",
        "TransactionCommitStepKind::StopDomain",
        "TransactionCommitStepKind::CreateResource",
        "TransactionCommitStepKind::ResetWasmState",
        "TransactionModelTransition::Create",
        "TransactionModelTransition::Replace",
        "TransactionModelTransition::Drop",
        "TransactionEntityGatePlan::affected_entities",
        "TransactionEntityGatePlan::relays",
        "DomainSchedule::entries",
        "DomainSchedule::placement_groups",
        "DomainStatus::Running",
        "DomainStatus::Stopped",
        "TransactionResolvedDomainStart::Resume",
        "TransactionResolvedDomainStart::At",
        "TransactionResolvedDomainStart::paced",
        "TransactionResolvedDomainStart::unpaced",
    ];

    /// Bytes that differ from seed to seed and from stream to stream. SplitMix64 spreads them
    /// without the short period of a linear sequence, so the choices a report reads late still
    /// vary between seeds.
    fn seeded_bytes(seed: u64, stream: u64) -> Vec<u8> {
        let mut state = seed ^ (stream << 32);
        let mut bytes = Vec::with_capacity(BYTES);
        while bytes.len() < BYTES {
            // SplitMix64 is defined over wrapping arithmetic: wrapping is the mixer's meaning.
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut mixed = state;
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            mixed ^= mixed >> 31;
            bytes.extend_from_slice(&mixed.to_le_bytes());
        }
        bytes
    }

    /// Every variant of `T` its archive holds. A fieldless enum archives as one byte holding the
    /// variant's declaration index, and a checked read refuses every byte past its last variant,
    /// so this witnesses every variant of an enum that declares no iterator.
    fn archived_variants<T>() -> Vec<T>
    where
        T: Archive,
        T::Archived:
            for<'a> CheckBytes<HighValidator<'a, Error>> + Deserialize<T, HighDeserializer<Error>>,
    {
        let mut variants = Vec::new();
        for tag in 0..=u8::MAX {
            let archived = [tag];
            if let Ok(variant) = rkyv::from_bytes::<T, Error>(&archived) {
                variants.push(variant);
            }
        }
        variants
    }

    /// `text` is the lowercase text of a UUIDv7.
    fn assert_uuid_v7_shaped(text: &str) {
        assert_eq!(text.len(), 36, "{text}");
        for (index, byte) in text.bytes().enumerate() {
            match index {
                8 | 13 | 18 | 23 => assert_eq!(byte, b'-', "{text}"),
                14 => assert_eq!(byte, b'7', "{text}"),
                19 => assert!(matches!(byte, b'8' | b'9' | b'a' | b'b'), "{text}"),
                _ => assert!(
                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
                    "{text}"
                ),
            }
        }
    }

    /// The one operation `number`, as a range.
    fn only(number: TransactionOperationNumber) -> TransactionOperationRange {
        TransactionOperationRange::new(number, number)
            .assured("a range of one operation ends where it begins")
    }

    /// Every operation `report` accepted, or its first operation when it accepted none, which no
    /// diagnostic of it can name.
    fn accepted_by(report: &TransactionImpactReport) -> TransactionOperationRange {
        let count = report.position().accepted_operations().max(1);
        TransactionOperationRange::from_index_and_count(0, count)
            .assured("a generated report accepts at most three operations")
    }

    /// Walks generated values, asserting the rules their generators promise and recording every
    /// variant, family and shape they hold.
    #[derive(Debug, Default)]
    struct Walk {
        seen: BTreeSet<&'static str>,
    }

    impl Walk {
        fn see(&mut self, variant: &'static str) {
            self.seen.insert(variant);
        }

        /// The attribution names only operations of `within`, and the vocabulary rebuilds it.
        fn attribution(
            &mut self,
            attribution: &ImpactAttribution,
            within: TransactionOperationRange,
        ) {
            for operation in attribution.operations() {
                assert!(
                    within.contains(*operation),
                    "{operation} is outside {within:?}"
                );
            }
            let rebuilt = ImpactAttribution::new(attribution.operations().iter().copied())
                .assured("a generated attribution names an operation");
            assert_eq!(&rebuilt, attribution);
        }

        fn branches(&mut self, branches: &ConcreteBranchCoverage) {
            match branches {
                ConcreteBranchCoverage::All => self.see("ConcreteBranchCoverage::All"),
                ConcreteBranchCoverage::Unbranched => {
                    self.see("ConcreteBranchCoverage::Unbranched");
                }
                ConcreteBranchCoverage::AllOfBranch { .. } => {
                    self.see("ConcreteBranchCoverage::AllOfBranch");
                }
                ConcreteBranchCoverage::Selected { branch, keys } => {
                    self.see("ConcreteBranchCoverage::Selected");
                    let rebuilt =
                        ConcreteBranchCoverage::selected(branch.clone(), keys.as_slice().to_vec())
                            .assured("a generated selection names a key");
                    assert_eq!(&rebuilt, branches);
                }
            }
        }

        /// A configuration-only kind carries no coverage, and an execution kind always does.
        fn coverage(&mut self, coverage: &ImpactNodeCoverage) {
            match &coverage.branches {
                None => {
                    self.see("ImpactNodeCoverage::configuration");
                    assert!(
                        CONFIGURATION_KINDS.contains(&coverage.node.kind),
                        "{coverage:?}"
                    );
                }
                Some(branches) => {
                    self.see("ImpactNodeCoverage::execution");
                    assert!(
                        EXECUTION_KINDS.contains(&coverage.node.kind),
                        "{coverage:?}"
                    );
                    self.branches(branches);
                }
            }
        }

        fn execution(&mut self, coverage: &ImpactNodeCoverage) {
            assert!(
                coverage.branches.is_some(),
                "{coverage:?} covers no executions"
            );
            self.coverage(coverage);
        }

        fn diagnostic(&mut self, diagnostic: &ImpactDiagnostic, within: TransactionOperationRange) {
            match diagnostic.kind {
                ImpactDiagnosticKind::Planning => self.see("ImpactDiagnosticKind::Planning"),
                ImpactDiagnosticKind::Topology => self.see("ImpactDiagnosticKind::Topology"),
                ImpactDiagnosticKind::Quiescence => self.see("ImpactDiagnosticKind::Quiescence"),
                ImpactDiagnosticKind::Ownership => self.see("ImpactDiagnosticKind::Ownership"),
                ImpactDiagnosticKind::Activation => self.see("ImpactDiagnosticKind::Activation"),
                ImpactDiagnosticKind::Application => {
                    self.see("ImpactDiagnosticKind::Application");
                }
                ImpactDiagnosticKind::Recovery => self.see("ImpactDiagnosticKind::Recovery"),
            }
            match diagnostic.operation {
                Some(operation) => {
                    self.see("ImpactDiagnostic::operation");
                    assert!(
                        within.contains(operation),
                        "{operation} is outside {within:?}"
                    );
                }
                None => self.see("ImpactDiagnostic::no_operation"),
            }
        }

        fn completeness(
            &mut self,
            completeness: &ImpactReportCompleteness,
            within: TransactionOperationRange,
        ) {
            match completeness {
                ImpactReportCompleteness::Complete => {
                    self.see("ImpactReportCompleteness::Complete");
                }
                ImpactReportCompleteness::Incomplete { diagnostics } => {
                    self.see("ImpactReportCompleteness::Incomplete");
                    let rebuilt = ImpactReportCompleteness::incomplete(diagnostics.clone())
                        .assured("a generated incomplete report holds a diagnostic");
                    assert_eq!(&rebuilt, completeness);
                    for diagnostic in diagnostics {
                        self.diagnostic(diagnostic, within);
                    }
                }
            }
        }

        /// The requirement names `domain`, and a subgraph is the one the vocabulary rebuilds from
        /// its execution nodes and gates, each attributed within `within`.
        fn pause(
            &mut self,
            pause: &PauseRequirement,
            domain: &DomainName,
            within: TransactionOperationRange,
        ) {
            match pause {
                PauseRequirement::NoPause => self.see("PauseRequirement::NoPause"),
                PauseRequirement::Subgraph { scope } => {
                    self.see("PauseRequirement::Subgraph");
                    assert_eq!(scope.domain(), domain);
                    assert!(!scope.nodes().is_empty(), "{scope:?} pauses no node");
                    let rebuilt = QuiesceSubgraph::new(
                        domain.clone(),
                        scope.nodes().to_vec(),
                        scope.gate_boundaries().to_vec(),
                    );
                    assert_eq!(&rebuilt, scope);
                    for node in scope.nodes() {
                        self.execution(&node.coverage);
                        self.attribution(&node.attribution, within);
                    }
                    if !scope.gate_boundaries().is_empty() {
                        self.see("QuiesceSubgraph::gate_boundaries");
                    }
                    for gate in scope.gate_boundaries() {
                        self.branches(&gate.boundary.branches);
                        self.attribution(&gate.attribution, within);
                    }
                }
                PauseRequirement::Domain { domain: paused } => {
                    self.see("PauseRequirement::Domain");
                    assert_eq!(paused, domain);
                }
            }
        }

        /// Nodes are distinct, and every edge joins two nodes of the same side.
        fn topology(&mut self, topology: &ImpactTopology, within: TransactionOperationRange) {
            let mut coverages = BTreeSet::new();
            for node in &topology.nodes {
                self.see("ImpactTopology::nodes");
                self.coverage(&node.coverage);
                self.attribution(&node.attribution, within);
                assert!(
                    coverages.insert(&node.coverage),
                    "{:?} repeats",
                    node.coverage
                );
            }
            for edge in &topology.edges {
                self.see("ImpactTopology::edges");
                assert!(coverages.contains(&edge.source), "{edge:?}");
                assert!(coverages.contains(&edge.target), "{edge:?}");
                match edge.kind {
                    ImpactEdgeKind::ConfigurationDependency => {
                        self.see("ImpactEdgeKind::ConfigurationDependency");
                    }
                    ImpactEdgeKind::Dataflow => self.see("ImpactEdgeKind::Dataflow"),
                    ImpactEdgeKind::MessageError => self.see("ImpactEdgeKind::MessageError"),
                    ImpactEdgeKind::CorrelationTimeout => {
                        self.see("ImpactEdgeKind::CorrelationTimeout");
                    }
                    ImpactEdgeKind::MaterializedState => {
                        self.see("ImpactEdgeKind::MaterializedState");
                    }
                }
                self.attribution(&edge.attribution, within);
            }
        }

        fn requested_version(&mut self, requested: RequestedResourceVersion, version: u64) {
            match requested {
                RequestedResourceVersion::Number(number) => {
                    self.see("RequestedResourceVersion::Number");
                    assert_eq!(number, version, "a numbered request binds its number");
                }
                RequestedResourceVersion::Latest => self.see("RequestedResourceVersion::Latest"),
            }
        }

        /// Every item is attributed within `within`, a lifecycle change names `domain`, and every
        /// node but a refreshed VHOST is an execution node.
        fn effects(
            &mut self,
            effects: &ImpactEffects,
            domain: &DomainName,
            within: TransactionOperationRange,
        ) {
            for change in &effects.changed_configuration {
                self.see("ImpactEffects::changed_configuration");
                match &change.transition {
                    ConfigurationTransition::Created { .. } => {
                        self.see("ConfigurationTransition::Created");
                    }
                    ConfigurationTransition::Changed { .. } => {
                        self.see("ConfigurationTransition::Changed");
                    }
                    ConfigurationTransition::Dropped { .. } => {
                        self.see("ConfigurationTransition::Dropped");
                    }
                }
                self.attribution(&change.attribution, within);
            }
            if !effects.topology.before.is_empty() {
                self.see("ImpactEffects::topology_before");
            }
            if !effects.topology.after.is_empty() {
                self.see("ImpactEffects::topology_after");
            }
            self.topology(&effects.topology.before, within);
            self.topology(&effects.topology.after, within);
            for moved in &effects.ownership_moves {
                self.see("ImpactEffects::ownership_moves");
                assert_eq!(moved.node.branches, Some(ConcreteBranchCoverage::All));
                self.execution(&moved.node);
                self.attribution(&moved.attribution, within);
            }
            for lifecycle in &effects.lifecycle {
                self.see("ImpactEffects::lifecycle");
                assert_eq!(&lifecycle.domain, domain);
                match lifecycle.action {
                    DomainLifecycleAction::Start => self.see("DomainLifecycleAction::Start"),
                    DomainLifecycleAction::Stop => self.see("DomainLifecycleAction::Stop"),
                }
                self.attribution(&lifecycle.attribution, within);
            }
            for activation in &effects.activations {
                self.see("ImpactEffects::activations");
                match activation.action {
                    ActivationAction::Activate => {
                        self.see("ActivationAction::Activate");
                        self.execution(&activation.node);
                    }
                    ActivationAction::Deactivate => {
                        self.see("ActivationAction::Deactivate");
                        self.execution(&activation.node);
                    }
                    ActivationAction::RefreshHttpsListener => {
                        self.see("ActivationAction::RefreshHttpsListener");
                        assert_eq!(activation.node.node.kind, ModelKind::Vhost);
                        self.coverage(&activation.node);
                    }
                }
                self.attribution(&activation.attribution, within);
            }
            for rebuild in &effects.rebuilds {
                self.see("ImpactEffects::rebuilds");
                match rebuild.reason {
                    RebuildReason::Configuration => self.see("RebuildReason::Configuration"),
                    RebuildReason::Ownership => self.see("RebuildReason::Ownership"),
                    RebuildReason::Recovery => self.see("RebuildReason::Recovery"),
                }
                self.execution(&rebuild.node);
                self.attribution(&rebuild.attribution, within);
            }
            for reset in &effects.state_resets {
                self.see("ImpactEffects::state_resets");
                match reset.state {
                    StatePurge::DeduplicatorKeyspace => {
                        self.see("StatePurge::DeduplicatorKeyspace");
                    }
                    StatePurge::ReordererBuffer => self.see("StatePurge::ReordererBuffer"),
                    StatePurge::WindowAccumulator => self.see("StatePurge::WindowAccumulator"),
                    StatePurge::CorrelationBuffer => self.see("StatePurge::CorrelationBuffer"),
                    StatePurge::InferencerWarmState => {
                        self.see("StatePurge::InferencerWarmState");
                    }
                    StatePurge::WasmGuestState => self.see("StatePurge::WasmGuestState"),
                }
                self.execution(&reset.node);
                self.attribution(&reset.attribution, within);
            }
            for flush in &effects.force_flushes {
                self.see("ImpactEffects::force_flushes");
                self.execution(&flush.node);
                self.attribution(&flush.attribution, within);
            }
            for catalog in &effects.resource_catalog {
                self.see("ImpactEffects::resource_catalog");
                match catalog.action {
                    ResourceCatalogAction::Create => self.see("ResourceCatalogAction::Create"),
                }
                self.attribution(&catalog.attribution, within);
            }
            for binding in &effects.resource_bindings {
                self.see("ImpactEffects::resource_bindings");
                self.requested_version(binding.requested, binding.version);
                self.attribution(&binding.attribution, within);
            }
        }

        /// Every pause attempt engages a scope of `domain` and begins with its request, and an
        /// unattempted step has recorded no effect.
        fn actual(
            &mut self,
            actual: &ActualExecutionStepImpact,
            domain: &DomainName,
            within: TransactionOperationRange,
        ) {
            for engagement in &actual.quiescence {
                self.see("ActualExecutionStepImpact::quiescence");
                assert_ne!(engagement.requirement, PauseRequirement::NoPause);
                self.pause(&engagement.requirement, domain, within);
                assert_eq!(
                    engagement.outcomes.first(),
                    Some(&QuiescenceOutcome::Requested)
                );
                for outcome in &engagement.outcomes {
                    match outcome {
                        QuiescenceOutcome::Requested => self.see("QuiescenceOutcome::Requested"),
                        QuiescenceOutcome::Confirmed => self.see("QuiescenceOutcome::Confirmed"),
                        QuiescenceOutcome::Failed { diagnostic } => {
                            self.see("QuiescenceOutcome::Failed");
                            self.diagnostic(diagnostic, within);
                        }
                        QuiescenceOutcome::Uncertain { diagnostic } => {
                            self.see("QuiescenceOutcome::Uncertain");
                            self.diagnostic(diagnostic, within);
                        }
                        QuiescenceOutcome::Released => self.see("QuiescenceOutcome::Released"),
                    }
                }
            }
            match &actual.outcome {
                ExecutionStepOutcome::Unattempted => {
                    self.see("ExecutionStepOutcome::Unattempted");
                    assert_eq!(actual.effects, ImpactEffects::default());
                }
                ExecutionStepOutcome::Applying => self.see("ExecutionStepOutcome::Applying"),
                ExecutionStepOutcome::Applied => self.see("ExecutionStepOutcome::Applied"),
                ExecutionStepOutcome::Failed { diagnostic } => {
                    self.see("ExecutionStepOutcome::Failed");
                    self.diagnostic(diagnostic, within);
                }
            }
            self.effects(&actual.effects, domain, within);
        }

        /// Everything the step planned and did is attributed to its own operations.
        fn step(&mut self, step: &ExecutionStepImpactReport, domain: &DomainName) {
            let within = step.operations();
            if within.operation_count().get() == 1 {
                self.see("TransactionOperationRange::single");
            } else {
                self.see("TransactionOperationRange::several");
            }
            let planned = step.planned();
            self.completeness(&planned.completeness, within);
            self.pause(&planned.pause, domain, within);
            self.effects(&planned.effects, domain, within);
            self.actual(step.actual(), domain, within);
        }

        /// The operation lies in `step` and shares its completeness, its contribution is
        /// attributed to it alone, and every reason is about what the operation names.
        fn operation(
            &mut self,
            operation: &OperationImpactReport,
            step: &ExecutionStepImpactReport,
        ) {
            assert!(step.operations().contains(operation.number));
            assert_eq!(operation.execution_step, step.operations());
            assert_eq!(&operation.completeness, &step.planned().completeness);
            match &operation.operation {
                TransactionOperation::CreateConfiguration { .. } => {
                    self.see("TransactionOperation::CreateConfiguration");
                }
                TransactionOperation::AlterConfiguration { .. } => {
                    self.see("TransactionOperation::AlterConfiguration");
                }
                TransactionOperation::DropConfiguration { .. } => {
                    self.see("TransactionOperation::DropConfiguration");
                }
                TransactionOperation::AlterDomain { .. } => {
                    self.see("TransactionOperation::AlterDomain");
                }
                TransactionOperation::StartDomain { .. } => {
                    self.see("TransactionOperation::StartDomain");
                }
                TransactionOperation::StopDomain { .. } => {
                    self.see("TransactionOperation::StopDomain");
                }
                TransactionOperation::CreateResource { .. } => {
                    self.see("TransactionOperation::CreateResource");
                }
                TransactionOperation::RebindResource {
                    requested, version, ..
                } => {
                    self.see("TransactionOperation::RebindResource");
                    self.requested_version(*requested, *version);
                }
                TransactionOperation::ResetWasmState { .. } => {
                    self.see("TransactionOperation::ResetWasmState");
                }
            }
            for reason in &operation.reasons {
                self.reason(reason, &operation.operation);
            }
            let own = only(operation.number);
            self.effects(&operation.contribution, operation.operation.domain(), own);
        }

        fn reason(&mut self, reason: &OperationImpactReason, operation: &TransactionOperation) {
            match (reason, operation) {
                (
                    OperationImpactReason::Configuration { node, .. },
                    TransactionOperation::CreateConfiguration { node: named, .. }
                    | TransactionOperation::AlterConfiguration { node: named, .. }
                    | TransactionOperation::DropConfiguration { node: named, .. },
                ) => {
                    self.see("OperationImpactReason::Configuration");
                    assert_eq!(node, named);
                }
                (
                    OperationImpactReason::ResourceRebinding {
                        resource,
                        to_version,
                        ..
                    },
                    TransactionOperation::RebindResource {
                        resource: named,
                        version,
                        ..
                    },
                ) => {
                    self.see("OperationImpactReason::ResourceRebinding");
                    assert_eq!(resource, named);
                    assert_eq!(to_version, version);
                }
                (
                    OperationImpactReason::DomainPlacement,
                    TransactionOperation::AlterDomain { .. },
                ) => self.see("OperationImpactReason::DomainPlacement"),
                (OperationImpactReason::DomainStart, TransactionOperation::StartDomain { .. }) => {
                    self.see("OperationImpactReason::DomainStart");
                }
                (OperationImpactReason::DomainStop, TransactionOperation::StopDomain { .. }) => {
                    self.see("OperationImpactReason::DomainStop");
                }
                (
                    OperationImpactReason::ResourceCatalog { resource },
                    TransactionOperation::CreateResource {
                        resource: named, ..
                    },
                ) => {
                    self.see("OperationImpactReason::ResourceCatalog");
                    assert_eq!(resource, named);
                }
                (
                    OperationImpactReason::WasmStateReset { node },
                    TransactionOperation::ResetWasmState { processor, .. },
                ) => {
                    self.see("OperationImpactReason::WasmStateReset");
                    assert_eq!(node.kind, ModelKind::WasmProcessor);
                    assert_eq!(node.identifier.as_str(), processor.as_str());
                }
                (reason, operation) => panic!("{operation:?} contributed {reason:?}"),
            }
        }

        /// The vocabulary rebuilds the report from its parts, its steps divide its operations as
        /// the planner does, and its completeness lists the diagnostics of every step it planned
        /// incompletely.
        fn report(&mut self, report: &TransactionImpactReport) {
            let rebuilt = TransactionImpactReport::new(
                report.domain().clone(),
                report.position(),
                report.planning_basis(),
                report.completeness().clone(),
                report.operations().to_vec(),
                report.execution_steps().to_vec(),
            )
            .assured("the vocabulary accepts a generated report");
            assert_eq!(&rebuilt, report);
            if report.position().accepted_operations() == 0 {
                self.see("TransactionImpactReport::empty");
            }
            let mut diagnostics = Vec::new();
            let mut follows_model_run = false;
            for step in report.execution_steps() {
                self.step(step, report.domain());
                diagnostics.extend(step.planned().completeness.diagnostics().iter().cloned());
                let range = step.operations();
                let executed = report
                    .operations()
                    .get(range.first_index()..range.end_index())
                    .assured("a valid report holds every operation its steps execute");
                let first = executed.first().assured("a step executes an operation");
                let model_run = first.operation.joins_model_run();
                assert!(
                    !(follows_model_run && model_run),
                    "two Model runs are adjacent"
                );
                for operation in executed {
                    assert_eq!(operation.operation.joins_model_run(), model_run);
                    self.operation(operation, step);
                }
                if !model_run {
                    assert_eq!(
                        executed.len(),
                        1,
                        "a step outside a Model run is one operation"
                    );
                }
                follows_model_run = model_run;
            }
            assert_eq!(report.completeness().diagnostics(), diagnostics.as_slice());
            self.completeness(report.completeness(), accepted_by(report));
        }

        fn transition(&mut self, transition: &TransactionModelTransition) {
            match transition {
                TransactionModelTransition::Create { .. } => {
                    self.see("TransactionModelTransition::Create");
                }
                TransactionModelTransition::Replace { before, after } => {
                    self.see("TransactionModelTransition::Replace");
                    assert_eq!(ModelVariant::of(before), ModelVariant::of(after));
                }
                TransactionModelTransition::Drop { .. } => {
                    self.see("TransactionModelTransition::Drop");
                }
            }
        }

        /// Gated entities are execution nodes, and entities and relays are sorted and distinct.
        fn gates(&mut self, gates: &TransactionEntityGatePlan) {
            if !gates.affected_entities.is_empty() {
                self.see("TransactionEntityGatePlan::affected_entities");
            }
            if !gates.relays.is_empty() {
                self.see("TransactionEntityGatePlan::relays");
            }
            assert!(
                gates
                    .affected_entities
                    .is_sorted_by(|left, right| left < right)
            );
            assert!(gates.relays.is_sorted_by(|left, right| left < right));
            for entity in &gates.affected_entities {
                assert!(EXECUTION_KINDS.contains(&entity.kind), "{entity:?}");
            }
        }

        /// The schedule is of `domain`, places execution nodes on distinct cluster nodes with any
        /// primary among them, and groups only nodes it places.
        fn schedule(&mut self, schedule: &DomainSchedule, domain: &DomainName) {
            assert_eq!(&schedule.domain, domain);
            if !schedule.nodes.is_empty() {
                self.see("DomainSchedule::entries");
            }
            if !schedule.placement_groups.is_empty() {
                self.see("DomainSchedule::placement_groups");
            }
            for (identity, node) in &schedule.nodes {
                assert_eq!(identity, &node.identity());
                assert!(EXECUTION_KINDS.contains(&node.kind()), "{identity:?}");
                assert!(node.assigned_nodes.is_sorted_by(|left, right| left < right));
                if let Some(primary) = node.primary_node() {
                    assert!(node.is_assigned_to(primary), "{identity:?}");
                }
            }
            for group in &schedule.placement_groups {
                for member in &group.members {
                    assert!(schedule.nodes.contains_key(member), "{member:?}");
                }
            }
        }

        /// The state is of `domain`, stopped or running; a running one has started, and only a
        /// running paced one carries a clock mapping.
        fn altered_state(&mut self, next: &DomainState, domain: &DomainName) {
            assert_eq!(&next.id, domain);
            let running = match next.status {
                DomainStatus::Running => {
                    self.see("DomainStatus::Running");
                    assert!(next.start_version >= 1);
                    true
                }
                DomainStatus::Stopped => {
                    self.see("DomainStatus::Stopped");
                    false
                }
                DomainStatus::Paused => panic!("an ALTER DOMAIN never installs a paused domain"),
            };
            assert_eq!(next.clock.is_some(), running && next.config.pace.is_paced());
        }

        /// A resolved start never reads the clock again, and a paced one maps the clock from the
        /// start it records.
        fn resolved_start(&mut self, resolved: &TransactionResolvedDomainStart) {
            match &resolved.start {
                DomainStartPoint::Resume => self.see("TransactionResolvedDomainStart::Resume"),
                DomainStartPoint::At { .. } => self.see("TransactionResolvedDomainStart::At"),
                DomainStartPoint::Now { .. } => panic!("a resolved start still asks for now"),
            }
            assert_eq!(resolved.clock.is_some(), resolved.authority.is_some());
            match &resolved.clock {
                Some(clock) => {
                    self.see("TransactionResolvedDomainStart::paced");
                    let wall_started_at = clock.wall_started_at();
                    let (logical_start, time_rate) = resolved.start.resolve_at(wall_started_at);
                    assert_eq!(clock.logical_start(), logical_start);
                    assert_eq!(clock.time_rate(), time_rate);
                }
                None => self.see("TransactionResolvedDomainStart::unpaced"),
            }
        }

        /// The plan previews `report` and pairs each of its steps with the decision the step's
        /// first operation calls for, every value naming the report's domain.
        fn plan(&mut self, plan: &TransactionCommitPlan, report: &TransactionImpactReport) {
            assert_uuid_v7_shaped(&plan.preview.transaction_id);
            assert_eq!(plan.preview.position, report.position());
            assert_eq!(plan.preview.planning_basis, report.planning_basis());
            assert_eq!(plan.steps.len(), report.execution_steps().len());
            let domain = report.domain();
            for (step, impact) in plan.steps.iter().zip(report.execution_steps()) {
                assert_eq!(&step.impact, impact);
                assert!(impact.planned().completeness.is_complete());
                assert_eq!(impact.actual(), &ActualExecutionStepImpact::unattempted());
                let within = impact.operations();
                let first = report
                    .operations()
                    .get(within.first_index())
                    .assured("a valid report holds every operation its steps execute");
                match (&step.kind, &first.operation) {
                    (
                        TransactionCommitStepKind::Models {
                            transitions,
                            schedule,
                            no_op_operations,
                            model_gate,
                            ownership_gate,
                        },
                        operation,
                    ) => {
                        self.see("TransactionCommitStepKind::Models");
                        assert!(operation.joins_model_run(), "{operation:?}");
                        for transition in transitions {
                            self.transition(transition);
                        }
                        if let Some(schedule) = schedule {
                            self.schedule(schedule, domain);
                        }
                        assert!(no_op_operations.is_sorted_by(|left, right| left < right));
                        for number in no_op_operations {
                            assert!(within.contains(*number), "{number} is outside {within:?}");
                        }
                        self.gates(model_gate);
                        self.gates(ownership_gate);
                    }
                    (
                        TransactionCommitStepKind::AlterDomain {
                            next,
                            schedule,
                            ownership_gate,
                        },
                        TransactionOperation::AlterDomain { .. },
                    ) => {
                        self.see("TransactionCommitStepKind::AlterDomain");
                        self.altered_state(next, domain);
                        if let Some(schedule) = schedule {
                            self.schedule(schedule, domain);
                        }
                        self.gates(ownership_gate);
                    }
                    (
                        TransactionCommitStepKind::StartDomain { resolved },
                        TransactionOperation::StartDomain { .. },
                    ) => {
                        self.see("TransactionCommitStepKind::StartDomain");
                        self.resolved_start(resolved);
                    }
                    (
                        TransactionCommitStepKind::StopDomain,
                        TransactionOperation::StopDomain { .. },
                    ) => self.see("TransactionCommitStepKind::StopDomain"),
                    (
                        TransactionCommitStepKind::CreateResource { resource, .. },
                        TransactionOperation::CreateResource {
                            resource: named, ..
                        },
                    ) => {
                        self.see("TransactionCommitStepKind::CreateResource");
                        assert_eq!(resource, named);
                    }
                    (
                        TransactionCommitStepKind::ResetWasmState {
                            reset, schedule, ..
                        },
                        TransactionOperation::ResetWasmState { processor, .. },
                    ) => {
                        self.see("TransactionCommitStepKind::ResetWasmState");
                        assert_eq!(&reset.domain, domain);
                        assert_eq!(&reset.processor, processor);
                        self.schedule(schedule, domain);
                    }
                    (kind, operation) => panic!("{operation:?} was paired with {kind:?}"),
                }
            }
        }
    }

    #[test]
    fn every_variant_is_reached_across_seeds() {
        let mut walk = Walk::default();
        for seed in 0..SEEDS {
            let report_bytes = seeded_bytes(seed, 0);
            let mut arbitrary = Arbitrary::new(&report_bytes, Domain::Vocabulary);
            let domain = arbitrary.name::<DomainName>();
            let report = arbitrary.transaction_impact_report(domain.clone());
            walk.report(&report);

            let admitted_bytes = seeded_bytes(seed, 1);
            let admitted =
                Arbitrary::new(&admitted_bytes, Domain::Vocabulary).admitted_impact_report(domain);
            walk.report(&admitted);
            let plan_bytes = seeded_bytes(seed, 2);
            let plan = Arbitrary::new(&plan_bytes, Domain::Vocabulary)
                .transaction_commit_plan_for(&admitted);
            walk.plan(&plan, &admitted);
        }
        let expected = BTreeSet::from(EXPECTED);
        let missing = expected.difference(&walk.seen).collect::<Vec<_>>();
        assert!(missing.is_empty(), "never generated: {missing:?}");
        let unexpected = walk.seen.difference(&expected).collect::<Vec<_>>();
        assert!(
            unexpected.is_empty(),
            "generated but not expected: {unexpected:?}"
        );
    }

    #[test]
    fn reports_steps_and_plans_follow_the_vocabulary_rules_in_both_domains() {
        for values in [Domain::Nspl, Domain::Vocabulary] {
            for seed in 0..SEEDS {
                let bytes = seeded_bytes(seed, 3);
                let mut arbitrary = Arbitrary::new(&bytes, values);
                let mut walk = Walk::default();
                let domain = arbitrary.name::<DomainName>();
                let report = arbitrary.transaction_impact_report(domain.clone());
                walk.report(&report);

                let operations = arbitrary.transaction_operation_range();
                let step = arbitrary.execution_step_impact_report(&domain, operations);
                assert_eq!(step.operations(), operations);
                walk.step(&step, &domain);

                let actual = arbitrary.actual_execution_step_impact();
                let mut paused = BTreeSet::new();
                for engagement in &actual.quiescence {
                    let paused_domain = engagement
                        .requirement
                        .domain()
                        .assured("a pause attempt engages a scope of a domain");
                    paused.insert(paused_domain.clone());
                }
                assert!(paused.len() <= 1, "one step pauses one domain: {paused:?}");

                let admitted = arbitrary.admitted_impact_report(domain);
                walk.report(&admitted);
                let plan = arbitrary.transaction_commit_plan_for(&admitted);
                walk.plan(&plan, &admitted);

                let preview = arbitrary.transaction_preview_identity();
                assert_uuid_v7_shaped(&preview.transaction_id);
            }
        }
    }

    #[test]
    fn reports_steps_and_plans_survive_their_archives() {
        for seed in 0..SEEDS {
            let bytes = seeded_bytes(seed, 4);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let domain = arbitrary.name::<DomainName>();

            let report = arbitrary.transaction_impact_report(domain.clone());
            let archived =
                rkyv::to_bytes::<Error>(&report).assured("every report field has an archive");
            let restored = rkyv::from_bytes::<TransactionImpactReport, Error>(&archived)
                .assured("the archive was written from a report");
            assert_eq!(restored, report);

            let operations = arbitrary.transaction_operation_range();
            let step = arbitrary.execution_step_impact_report(&domain, operations);
            let archived =
                rkyv::to_bytes::<Error>(&step).assured("every step field has an archive");
            let restored = rkyv::from_bytes::<ExecutionStepImpactReport, Error>(&archived)
                .assured("the archive was written from a step");
            assert_eq!(restored, step);

            let plan = arbitrary.transaction_commit_plan(domain);
            let archived =
                rkyv::to_bytes::<Error>(&plan).assured("every plan field has an archive");
            let restored = rkyv::from_bytes::<TransactionCommitPlan, Error>(&archived)
                .assured("the archive was written from a plan");
            assert_eq!(restored, plan);
        }
    }

    #[test]
    fn exhausted_bytes_build_an_empty_transaction() {
        let mut arbitrary = Arbitrary::new(&[], Domain::Vocabulary);
        let domain = arbitrary.name::<DomainName>();
        assert_eq!(domain.as_str(), "a");
        let report = arbitrary.transaction_impact_report(domain.clone());
        assert_eq!(report.position().accepted_operations(), 0);
        assert!(report.operations().is_empty());
        assert!(report.execution_steps().is_empty());
        assert!(report.completeness().is_complete());
        assert_eq!(report.summary().pause(), &PauseRequirement::NoPause);
        let plan = arbitrary.transaction_commit_plan(domain);
        assert!(plan.steps.is_empty());
        assert_eq!(
            plan.preview.transaction_id,
            "00000000-0000-7000-8000-000000000000"
        );
        assert_eq!(
            arbitrary.actual_execution_step_impact(),
            ActualExecutionStepImpact::unattempted()
        );
    }

    #[test]
    fn variant_lists_name_every_variant_in_declaration_order() {
        assert_eq!(
            archived_variants::<ModelChangeAspect>(),
            MODEL_CHANGE_ASPECTS.to_vec()
        );
        assert_eq!(archived_variants::<StatePurge>(), STATE_PURGES.to_vec());
        assert_eq!(archived_variants::<ImpactEdgeKind>(), EDGE_KINDS.to_vec());
        assert_eq!(
            archived_variants::<ImpactDiagnosticKind>(),
            DIAGNOSTIC_KINDS.to_vec()
        );
    }

    #[test]
    fn every_model_kind_is_an_execution_or_a_configuration_kind() {
        let mut listed = BTreeSet::new();
        for kind in EXECUTION_KINDS.into_iter().chain(CONFIGURATION_KINDS) {
            assert!(listed.insert(kind.as_str()), "{kind:?} is listed twice");
        }
        let declared = ModelKind::iter()
            .map(ModelKind::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(listed, declared);

        let bytes = seeded_bytes(0, 5);
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        for family in ModelVariant::ALL {
            let model = arbitrary.model_of(family);
            assert_eq!(
                EXECUTION_KINDS.contains(&model.kind()),
                EXECUTION_FAMILIES.contains(&family),
                "{family:?}"
            );
        }
    }

    #[test]
    fn every_listed_aspect_is_drawn() {
        let mut drawn = BTreeSet::new();
        for seed in 0..SEEDS {
            let bytes = seeded_bytes(seed, 6);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            for _ in 0..16 {
                drawn.insert(arbitrary.model_change_aspect());
            }
        }
        assert_eq!(drawn, BTreeSet::from(MODEL_CHANGE_ASPECTS));
    }
}
