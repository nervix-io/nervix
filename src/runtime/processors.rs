use std::{
    collections::VecDeque,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc as StdArc,
    time::Duration,
};

use ahash::{HashMap, HashSet};
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    AckMode, Assignment, AssignmentTarget, BranchName, CorrelationTimeoutAction,
    CorrelationTimeoutPolicy, CorrelatorMatchPolicy, ErrorPolicies, FieldName, FlushPolicy,
    InferencerTensorDeclaration, InferencerTensorMapping, MessageErrorPolicy, ModelKind, ModelName,
    RelayName, ResourceName, RouteConstruction, StructuredMessageError, Timestamp, WindowBound,
};
use nervix_roto::UdfExecutor;
use nervix_vm::{
    CompileBinding as VmCompileBinding, CompileOptions as VmCompileOptions,
    CompiledProgram as VmCompiledProgram, OutputMode as VmOutputMode,
    SchemaSensitivity as VmSchemaSensitivity, SemanticScopePolicy,
    compile_program_with_options_for_bindings_with_sensitivity as compile_vm_program,
    lower_route_construction,
    window::{CompiledWindowRoute, WindowAggregateProgram, WindowRouteSchemas},
};
use nervix_wasm::CompiledWasmProcessor;
use ordered_float::OrderedFloat;
use triomphe::Arc;

use super::{
    BranchBufferDeadline, BranchBufferTimer, BranchBufferTimingResult, BranchKey, BranchRuntime,
    CompiledBranchProgram, CompiledDeduplicatorKeyProgram, CompiledProgramWithMaterializedInterest,
    DeduplicatorKeyspace, DomainClock, DomainExecutionSnapshot, PendingMaterializedBatch,
    RelayBoundaryServices, RelayMessage, RelayRecordBatch, RelayRegistry,
    ReplicatedWasmProcessorState, ReplicatedWindowProcessorState, RuntimeFlushPolicy,
    RuntimeInputCollectPolicy, RuntimeInputCollector, SharedActiveGraph, WasmGuestStateResetFence,
    WasmLiveInstance, WindowAccumulatorPlan, WindowProcessorState, branch_key_display,
    inferencer::OnnxInferencerSession, relay_batch::RelayRecordBatchError,
};
use crate::{
    registry::ActiveGraph,
    runtime_ack::AckSet,
    runtime_schema::{
        CompiledSchema, RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeValue, arrow_data_type,
    },
};

pub(super) type WasmAckMap = HashMap<u64, WasmAckContext>;

#[derive(Debug, Clone)]
pub(super) struct WasmAckContext {
    pub(super) acks: AckSet,
    pub(super) metadata: RuntimeRecordMetadata,
    pub(super) input_batch: Arc<RuntimeRecordBatch>,
    pub(super) input_row: usize,
}

#[derive(Debug, Clone)]
pub(super) struct BranchedIngestorSpec {
    pub(super) kind: ModelKind,
    pub(super) identifier: ModelName,
    pub(super) root_relay: RelayName,
    pub(super) branch: Option<BranchName>,
    pub(super) branch_ttl: Option<String>,
    pub(super) branch_max_instances: Option<NonZeroU64>,
    pub(super) output_ack_boundary: BranchInstanceAckBoundary,
    pub(super) output_flush_policy: FlushPolicy,
    pub(super) error_policies: ErrorPolicies,
}

#[derive(Debug, Clone)]
pub(super) struct BranchedProcessorNodeSpec {
    pub(super) spec: BranchedProcessorSpec,
    pub(super) branch: Option<BranchName>,
    pub(super) branch_ttl: Option<String>,
    pub(super) branch_max_instances: Option<NonZeroU64>,
    pub(super) wasm_state_reset: Option<nervix_models::WasmStateReset>,
}

#[derive(Debug, Clone)]
pub(super) struct BranchedNodeSpecs {
    pub(super) entrypoints: Vec<BranchedIngestorSpec>,
    pub(super) processors: Vec<BranchedProcessorNodeSpec>,
}

impl BranchedNodeSpecs {
    pub(super) fn processor(
        &self,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Option<&BranchedProcessorNodeSpec> {
        self.processors
            .iter()
            .find(|node| node.spec.kind == kind && &node.spec.processor == identifier)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BranchInstanceAckBoundary {
    Preserve,
    Reingestor(AckMode),
}

#[derive(Debug, Clone)]
pub(super) struct BranchedProcessorSpec {
    pub(super) kind: ModelKind,
    pub(super) processor: ModelName,
    pub(super) input_relays: Vec<RelayName>,
    pub(super) input_collect_policies: HashMap<RelayName, nervix_models::InputCollectPolicy>,
    pub(super) mode: AckMode,
    pub(super) error_policies: ErrorPolicies,
    pub(super) from_where: HashMap<RelayName, nervix_models::Expression>,
    pub(super) filter_where: Option<nervix_models::Expression>,
    pub(super) materialized_state: Vec<nervix_models::MaterializedStateDependency>,
    pub(super) operation: BranchedProcessorOperationSpec,
}

#[derive(Debug, Clone)]
pub(super) enum BranchedProcessorOperationSpec {
    Deduplicator {
        output_routes: BranchedProcessorOutputsSpec,
        deduplicate_on: Vec<nervix_models::Expression>,
        max_time: String,
    },
    WindowProcessor {
        output_routes: BranchedProcessorOutputsSpec,
        width: WindowBound,
        step: WindowBound,
    },
    Reorderer {
        output_routes: BranchedProcessorOutputsSpec,
        order_by: Vec<nervix_models::Expression>,
        max_time: String,
    },
    Correlator {
        output_routes: BranchedProcessorOutputsSpec,
        left_relays: Vec<RelayName>,
        right_relays: Vec<RelayName>,
        correlate_where: nervix_models::Expression,
        match_policy: CorrelatorMatchPolicy,
        max_time: String,
        timeout_policy: CorrelationTimeoutPolicy,
    },
    Junction {
        output_routes: BranchedProcessorOutputsSpec,
    },
    Inferencer {
        output_routes: BranchedProcessorOutputsSpec,
        resource: ResourceName,
        resource_version: u64,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        output_schema: Vec<InferencerTensorDeclaration>,
    },
    WasmProcessor {
        output_routes: BranchedProcessorOutputsSpec,
        resource: ResourceName,
        resource_version: u64,
        file: String,
        limits: nervix_models::WasmProcessorLimits,
    },
}

#[derive(Debug, Clone)]
pub(super) struct BranchedProcessorOutputsSpec {
    pub(super) routes: Vec<BranchedProcessorOutputSpec>,
}

impl BranchedProcessorOutputsSpec {
    pub(super) fn outputs(&self) -> impl Iterator<Item = &BranchedProcessorOutputSpec> {
        self.routes.iter()
    }
}

#[derive(Debug, Clone)]
pub(super) struct BranchedProcessorOutputSpec {
    pub(super) relay: RelayName,
    pub(super) construction: nervix_models::RouteConstruction,
    pub(super) flush_policy: Option<FlushPolicy>,
    pub(super) message_error_policy: MessageErrorPolicy,
}

impl BranchedProcessorSpec {
    pub(super) fn output_relays(&self) -> HashSet<RelayName> {
        let mut relays = HashSet::default();
        match &self.operation {
            BranchedProcessorOperationSpec::Deduplicator { output_routes, .. }
            | BranchedProcessorOperationSpec::Reorderer { output_routes, .. }
            | BranchedProcessorOperationSpec::WindowProcessor { output_routes, .. }
            | BranchedProcessorOperationSpec::Junction { output_routes, .. }
            | BranchedProcessorOperationSpec::Inferencer { output_routes, .. }
            | BranchedProcessorOperationSpec::WasmProcessor { output_routes, .. } => {
                relays.extend(output_routes.outputs().map(|output| output.relay.clone()));
            }
            BranchedProcessorOperationSpec::Correlator {
                output_routes,
                timeout_policy,
                ..
            } => {
                relays.extend(output_routes.outputs().map(|output| output.relay.clone()));
                if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.left {
                    relays.insert(relay.clone());
                }
                if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.right {
                    relays.insert(relay.clone());
                }
            }
        }
        relays
    }

    pub(super) fn relay_ids(&self) -> HashSet<RelayName> {
        let mut relays = self.output_relays();
        relays.extend(self.input_relays.iter().cloned());
        relays
    }
}

#[derive(Debug, Clone)]
pub(super) struct BranchInstanceTemplate {
    pub(super) source_kind: ModelKind,
    pub(super) source: RelayName,
    pub(super) root_relay: RelayName,
    pub(super) branch: Option<BranchName>,
    pub(super) branch_ttl: Option<Duration>,
    pub(super) branch_max_instances: Option<NonZeroUsize>,
    pub(super) error_policies: ErrorPolicies,
    pub(super) relays: HashMap<RelayName, RelayProcessorRelayTemplate>,
    pub(super) processors: HashMap<ModelName, RelayProcessorTemplate>,
    pub(super) wasm_state_reset: Option<nervix_models::WasmStateReset>,
}

#[derive(Debug, Clone)]
pub(super) struct IngestorRouteTemplate {
    pub(super) branch: BranchInstanceTemplate,
    pub(super) ack_boundary: BranchInstanceAckBoundary,
    pub(super) flush_policy: RuntimeFlushPolicy,
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorRelayTemplate {
    pub(super) registry: RelayRegistry,
    pub(super) services: Arc<RelayBoundaryServices>,
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorTemplate {
    pub(super) kind: ModelKind,
    pub(super) processor: ModelName,
    pub(super) input_relays: Vec<RelayName>,
    pub(super) input_collect_policies: HashMap<RelayName, RuntimeInputCollectPolicy>,
    pub(super) error_policies: ErrorPolicies,
    pub(super) from_where: HashMap<RelayName, nervix_models::Expression>,
    pub(super) filter_where: Option<nervix_models::Expression>,
    pub(super) materialized_state: Vec<nervix_models::MaterializedStateDependency>,
    pub(super) operation: RelayProcessorOperationTemplate,
}

#[derive(Debug, Clone)]
pub(super) enum RelayProcessorOperationTemplate {
    Deduplicator {
        output_routes: RelayProcessorOutputsTemplate,
        deduplicate_on: Vec<nervix_models::Expression>,
        max_time: Duration,
    },
    WindowProcessor {
        output_routes: RelayProcessorOutputsTemplate,
        width_messages: Option<usize>,
        step_messages: Option<usize>,
        width_duration: Option<Duration>,
        step_duration: Option<Duration>,
        /// The routes' combined aggregate program, which decides whether a changed processor can
        /// keep its live window.
        aggregate: WindowAggregateProgram,
        plan: WindowAccumulatorPlan,
        compiled_aggregates: Vec<CompiledWindowAggregateProgram>,
    },
    Reorderer {
        output_routes: RelayProcessorOutputsTemplate,
        order_by: Vec<nervix_models::Expression>,
        max_time: Duration,
    },
    Correlator {
        output_routes: RelayProcessorOutputsTemplate,
        left_relays: Vec<RelayName>,
        right_relays: Vec<RelayName>,
        correlate_where: nervix_models::Expression,
        match_policy: CorrelatorMatchPolicy,
        max_time: Duration,
        timeout_policy: CorrelationTimeoutPolicy,
    },
    Junction {
        output_routes: RelayProcessorOutputsTemplate,
    },
    Inferencer {
        output_routes: RelayProcessorOutputsTemplate,
        resource: ResourceName,
        resource_version: u64,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        output_schema: Vec<InferencerTensorDeclaration>,
        compiled_input_program: CompiledInferencerInputProgram,
    },
    WasmProcessor {
        output_routes: RelayProcessorOutputsTemplate,
        resource: ResourceName,
        resource_version: u64,
        file: String,
        limits: nervix_models::WasmProcessorLimits,
        compiled: Option<WasmCompiledBranchProcessor>,
    },
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputsTemplate {
    pub(super) routes: Vec<RelayProcessorOutputTemplate>,
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputTemplate {
    pub(super) output_relay: RelayName,
    pub(super) construction: nervix_models::RouteConstruction,
    pub(super) flush_policy: Option<RuntimeFlushPolicy>,
    pub(super) message_error_policy: MessageErrorPolicy,
}

#[derive(Debug)]
pub(super) struct RelayProcessorNode {
    pub(super) kind: ModelKind,
    pub(super) processor: ModelName,
    pub(super) input_relays: Vec<RelayName>,
    pub(super) input_collectors: HashMap<RelayName, RuntimeInputCollector>,
    pub(super) error_policies: ErrorPolicies,
    pub(super) from_where: HashMap<RelayName, nervix_models::Expression>,
    pub(super) compiled_from_where: HashMap<RelayName, CompiledProgramWithMaterializedInterest>,
    pub(super) filter_where: Option<nervix_models::Expression>,
    pub(super) materialized_state: Vec<nervix_models::MaterializedStateDependency>,
    pub(super) pending_materialized: VecDeque<PendingMaterializedBatch>,
    pub(super) compiled_filter_where: HashMap<RelayName, CompiledProgramWithMaterializedInterest>,
    pub(super) operation: RelayProcessorOperationNode,
    pub(super) last_graph: Option<StdArc<ActiveGraph>>,
    pub(super) applied_generation: u64,
}

#[derive(Debug)]
pub(super) enum RelayProcessorOperationNode {
    Deduplicator {
        output_routes: RelayProcessorOutputsNode,
        deduplicate_on: Vec<nervix_models::Expression>,
        max_time: Duration,
        compiled_key_program: Option<Box<CompiledDeduplicatorKeyProgram>>,
        keyspace: DeduplicatorKeyspace,
    },
    WindowProcessor {
        output_routes: RelayProcessorOutputsNode,
        width_messages: Option<usize>,
        step_messages: Option<usize>,
        width_duration: Option<Duration>,
        step_duration: Option<Duration>,
        aggregate: WindowAggregateProgram,
        plan: WindowAccumulatorPlan,
        compiled_aggregates: Vec<CompiledWindowAggregateProgram>,
        state: WindowProcessorState,
        replicated_state: Arc<ReplicatedWindowProcessorState>,
    },
    Reorderer {
        output_routes: RelayProcessorOutputsNode,
        order_by: Vec<nervix_models::Expression>,
        max_time: Duration,
        compiled_program: Option<Box<CompiledReordererProgram>>,
        output_buffers: Vec<ReordererOutputBuffer>,
        arrival_sequence: u64,
    },
    Correlator {
        output_routes: RelayProcessorOutputsNode,
        left_relays: Vec<RelayName>,
        right_relays: Vec<RelayName>,
        correlate_where: nervix_models::Expression,
        match_policy: CorrelatorMatchPolicy,
        max_time: Duration,
        timeout_policy: CorrelationTimeoutPolicy,
        compiled_where_program: Option<Box<CompiledCorrelatorWhereProgram>>,
        compiled_output_programs: Vec<Option<Box<CompiledCorrelatorOutputProgram>>>,
        state: CorrelatorBranchState,
    },
    Junction {
        output_routes: RelayProcessorOutputsNode,
    },
    Inferencer {
        output_routes: RelayProcessorOutputsNode,
        resource: ResourceName,
        resource_version: u64,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        output_schema: Vec<InferencerTensorDeclaration>,
        compiled_input_program: CompiledInferencerInputProgram,
        output_buffers: Vec<InferencerOutputBuffer>,
        session: Option<OnnxInferencerSession>,
    },
    WasmProcessor {
        output_routes: RelayProcessorOutputsNode,
        resource: ResourceName,
        resource_version: u64,
        file: String,
        limits: nervix_models::WasmProcessorLimits,
        compiled: Option<WasmCompiledBranchProcessor>,
        instance: Option<Box<WasmLiveInstance>>,
        replicated_state: Arc<ReplicatedWasmProcessorState>,
        ack_map: WasmAckMap,
        next_ack_token: u64,
        pending: Vec<RelayRecordBatch>,
        /// Closed once this branch's guest asks for a new state lifetime, and open for as long as
        /// the branch runs in the one it has.
        state_reset: WasmGuestStateResetFence,
    },
}

/// Every way compiling the VM programs a stateful processor runs fails. The programs belong to the
/// processor family rather than to one processor, so every processor's compilation reports here.
#[derive(Debug, thiserror::Error)]
pub(super) enum ProcessorCompileError {
    #[error("inferencer '{}' tensor name '{tensor}' is not a valid field", .processor.as_str())]
    InferencerTensorField {
        processor: ModelName,
        tensor: String,
    },
    #[error("inferencer '{}' INPUTS mapping is invalid", .processor.as_str())]
    InferencerInputsMapping { processor: ModelName },
    #[error("inferencer '{}' INPUTS compile failed", .processor.as_str())]
    InferencerInputsCompile { processor: ModelName },
    #[error("window aggregate output relay '{}' has no runtime schema", .relay.as_str())]
    WindowOutputSchema { relay: RelayName },
    #[error("window aggregate requires at least one input relay")]
    WindowWithoutInput,
    #[error("window aggregate input relay '{}' has no runtime schema", .relay.as_str())]
    WindowInputSchema { relay: RelayName },
    #[error("window aggregate route to '{}' failed to compile", .relay.as_str())]
    WindowRoute { relay: RelayName },
    #[error(
        "deduplicator '{}' requires at least one DEDUPLICATE ON expression",
        .processor.as_str()
    )]
    DeduplicatorWithoutKeys { processor: ModelName },
    #[error("deduplicator '{}' DEDUPLICATE ON program is invalid", .processor.as_str())]
    DeduplicatorKeyProgram { processor: ModelName },
    #[error("correlator '{}' CORRELATE WHERE is invalid", .processor.as_str())]
    CorrelateWhereInvalid { processor: ModelName },
    #[error("correlator '{}' requires both LEFT and RIGHT inputs", .processor.as_str())]
    CorrelatorSides { processor: ModelName },
    #[error("correlator '{}' CORRELATE WHERE compile failed", .processor.as_str())]
    CorrelateWhereCompile { processor: ModelName },
    #[error(
        "correlator '{}' TO output '{}' is invalid",
        .processor.as_str(),
        .relay.as_str()
    )]
    CorrelatorOutputInvalid {
        processor: ModelName,
        relay: RelayName,
    },
    #[error(
        "correlator '{}' TO output '{}' must contain SET assignments and may contain WHERE",
        .processor.as_str(),
        .relay.as_str()
    )]
    CorrelatorOutputShape {
        processor: ModelName,
        relay: RelayName,
    },
    #[error(
        "correlator '{}' TO output '{}' compile failed",
        .processor.as_str(),
        .relay.as_str()
    )]
    CorrelatorOutputCompile {
        processor: ModelName,
        relay: RelayName,
    },
}

/// Every way resolving a stateful processor's materialized dependencies for one branch fails.
#[derive(Debug, thiserror::Error)]
pub(super) enum ProcessorMaterializedError {
    #[error("failed to read the domain routing for branch '{}'", branch_key_display(.branch))]
    DomainRouting { branch: Option<BranchKey> },
    #[error(
        "failed to resolve materialized dependencies in branch '{}'",
        branch_key_display(.branch)
    )]
    Resolve { branch: Option<BranchKey> },
    #[error(
        "{} '{}' requires materialized state that was evicted from branch '{}' after the batch was \
         admitted",
        .node_kind.as_str(),
        .processor.as_str(),
        branch_key_display(.branch)
    )]
    EvictedRequiredSkip {
        node_kind: ModelKind,
        processor: ModelName,
        branch: Option<BranchKey>,
    },
    #[error(
        "{} '{}' awaits materialized state that was evicted from branch '{}' after the batch was \
         admitted",
        .node_kind.as_str(),
        .processor.as_str(),
        branch_key_display(.branch)
    )]
    EvictedRequiredWait {
        node_kind: ModelKind,
        processor: ModelName,
        branch: Option<BranchKey>,
    },
}

/// A stateful processor failed to publish the live state its branch task owns.
#[derive(Debug, thiserror::Error)]
#[error("failed to publish processor live state for branch '{}'", branch_key_display(.branch))]
pub(super) struct ProcessorLiveStateError {
    pub(super) branch: Option<BranchKey>,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledInferencerInputProgram {
    pub(super) program: Arc<VmCompiledProgram>,
}

impl CompiledInferencerInputProgram {
    pub(super) fn compile(
        processor: &ModelName,
        mappings: &[InferencerTensorMapping],
        input_schema: &CompiledSchema,
        udfs: Option<&UdfExecutor>,
    ) -> error_stack::Result<Self, ProcessorCompileError> {
        let mut assignments = Vec::with_capacity(mappings.len());
        for mapping in mappings {
            let target = FieldName::parse(&mapping.tensor).change_context_lazy(|| {
                ProcessorCompileError::InferencerTensorField {
                    processor: processor.clone(),
                    tensor: mapping.tensor.clone(),
                }
            })?;
            assignments.push(Assignment {
                target: AssignmentTarget::bare(target),
                value: mapping.expression.clone(),
            });
        }
        let parsed = lower_route_construction(
            &RouteConstruction {
                assignments,
                ..RouteConstruction::default()
            },
            SemanticScopePolicy::read_write("input", "mapped_input"),
        )
        .change_context_lazy(|| ProcessorCompileError::InferencerInputsMapping {
            processor: processor.clone(),
        })?;
        let output_schema = StdArc::new(arrow_schema::Schema::new(
            mappings
                .iter()
                .map(|mapping| {
                    arrow_schema::Field::new(
                        &mapping.tensor,
                        arrow_data_type(&mapping.schema.message_type()),
                        false,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let input_sensitivity = input_schema.vm_sensitivity();
        let output_sensitivity = VmSchemaSensitivity::from_sensitive_fields(
            mappings
                .iter()
                .filter(|mapping| {
                    super::expression_reads_sensitive_source(
                        &mapping.expression,
                        &input_sensitivity,
                    )
                })
                .map(|mapping| mapping.tensor.clone()),
        );
        let program = compile_vm_program(
            &parsed,
            output_schema.clone(),
            output_sensitivity.clone(),
            [
                VmCompileBinding::readonly("input", input_schema.arrow_schema())
                    .with_sensitivity(input_sensitivity),
                VmCompileBinding::writeonly("mapped_input", output_schema)
                    .with_sensitivity(output_sensitivity),
            ],
            super::runtime_udf_compile_options(
                udfs,
                VmCompileOptions {
                    output_mode: VmOutputMode::ExplicitOnly,
                    ..VmCompileOptions::default()
                },
            ),
        )
        .change_context_lazy(|| ProcessorCompileError::InferencerInputsCompile {
            processor: processor.clone(),
        })?;
        Ok(Self {
            program: Arc::new(program),
        })
    }
}

impl RelayProcessorOperationNode {
    pub(super) fn output_routes(&self) -> &RelayProcessorOutputsNode {
        match self {
            Self::Deduplicator { output_routes, .. }
            | Self::WindowProcessor { output_routes, .. }
            | Self::Reorderer { output_routes, .. }
            | Self::Correlator { output_routes, .. }
            | Self::Junction { output_routes, .. }
            | Self::Inferencer { output_routes, .. }
            | Self::WasmProcessor { output_routes, .. } => output_routes,
        }
    }

    pub(super) fn output_routes_mut(&mut self) -> &mut RelayProcessorOutputsNode {
        match self {
            Self::Deduplicator { output_routes, .. }
            | Self::WindowProcessor { output_routes, .. }
            | Self::Reorderer { output_routes, .. }
            | Self::Correlator { output_routes, .. }
            | Self::Junction { output_routes, .. }
            | Self::Inferencer { output_routes, .. }
            | Self::WasmProcessor { output_routes, .. } => output_routes,
        }
    }
}

/// One window route's aggregate program compiled for its relays, placed among the window's
/// demands.
#[derive(Debug, Clone)]
pub(super) struct CompiledWindowAggregateProgram {
    pub(super) route: CompiledWindowRoute,
    /// For each field of the route's output schema, in schema order, the index of the route
    /// assignment that initializes it.
    pub(super) field_assignments: Vec<Option<usize>>,
    /// How many demands the routes written before this one declared.
    pub(super) demand_offset: usize,
}

impl CompiledWindowAggregateProgram {
    pub(super) fn compile(
        aggregate: &WindowAggregateProgram,
        input_relays: &[RelayName],
        output_relay: &RelayName,
        relay_schemas: &HashMap<RelayName, Arc<CompiledSchema>>,
        udfs: Option<&UdfExecutor>,
    ) -> error_stack::Result<Self, ProcessorCompileError> {
        let output_schema = relay_schemas.get(output_relay).ok_or_else(|| {
            Report::new(ProcessorCompileError::WindowOutputSchema {
                relay: output_relay.clone(),
            })
        })?;
        let input_relay = input_relays
            .first()
            .ok_or_else(|| Report::new(ProcessorCompileError::WindowWithoutInput))?;
        let input_schema = relay_schemas.get(input_relay).ok_or_else(|| {
            Report::new(ProcessorCompileError::WindowInputSchema {
                relay: input_relay.clone(),
            })
        })?;
        let input_arrow_schema = input_schema.arrow_schema();
        let input_sensitivity = input_schema.vm_sensitivity();
        let output_arrow_schema = output_schema.arrow_schema();
        let output_sensitivity = output_schema.vm_sensitivity();
        let route = CompiledWindowRoute::compile(
            aggregate,
            WindowRouteSchemas {
                input: &input_arrow_schema,
                input_sensitivity: &input_sensitivity,
                readable: &[],
                output: &output_arrow_schema,
                output_sensitivity: &output_sensitivity,
            },
            &super::runtime_udf_compile_options(udfs, VmCompileOptions::default()),
        )
        .change_context_lazy(|| ProcessorCompileError::WindowRoute {
            relay: output_relay.clone(),
        })?;
        let mut assignment_by_field = HashMap::default();
        for (index, assignment) in route.assignments.iter().enumerate() {
            assignment_by_field.insert(assignment.field.as_str(), index);
        }
        let field_assignments = output_arrow_schema
            .fields()
            .iter()
            .map(|field| assignment_by_field.get(field.name().as_str()).copied())
            .collect();
        Ok(Self {
            route,
            field_assignments,
            demand_offset: 0,
        })
    }

    pub(super) fn with_demand_offset(mut self, demand_offset: usize) -> Self {
        self.demand_offset = demand_offset;
        self
    }
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputsNode {
    pub(super) routes: Vec<RelayProcessorOutputNode>,
}

impl RelayProcessorOutputsNode {
    pub(super) fn base_relay(&self) -> Option<RelayName> {
        self.routes.first().map(|output| output.relay.clone())
    }

    pub(super) fn buffer_deadlines(&self) -> Vec<BranchBufferDeadline> {
        self.routes
            .iter()
            .filter_map(|output| output.flush_timer.deadline())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputNode {
    pub(super) relay: RelayName,
    pub(super) construction: nervix_models::RouteConstruction,
    pub(super) branch: Option<nervix_models::OutputBranch>,
    pub(super) flush_policy: Option<RuntimeFlushPolicy>,
    pub(super) message_error_policy: MessageErrorPolicy,
    pub(super) pending: Vec<RelayRecordBatch>,
    pub(super) flush_timer: BranchBufferTimer,
    pub(super) compiled_program: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) compiled_branch_program: Option<CompiledBranchProgram>,
}

impl RelayProcessorOutputNode {
    pub(super) fn schedule_input_flush(
        &mut self,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
        pending_bytes: u64,
    ) -> BranchBufferTimingResult<Option<bool>> {
        let Some(policy) = self.flush_policy else {
            return Ok(None);
        };
        self.flush_timer.arm_flush(policy, clock, snapshot)?;
        Ok(Some(
            self.flush_timer.is_due(clock, snapshot)?
                || policy.size_boundary_reached(pending_bytes),
        ))
    }

    pub(super) fn flush_deadline_due(
        &self,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
    ) -> BranchBufferTimingResult<bool> {
        self.flush_timer.is_due(clock, snapshot)
    }

    pub(super) fn clear_flush_timer(&mut self) {
        self.flush_timer.clear();
    }

    pub(super) fn enqueue(
        &mut self,
        batch: RelayRecordBatch,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
    ) -> BranchBufferTimingResult<bool> {
        self.pending.push(batch);
        let Some(policy) = self.flush_policy else {
            return Ok(true);
        };
        self.flush_timer.arm_flush(policy, clock, snapshot)?;
        Ok(self.flush_timer.is_due(clock, snapshot)?
            || policy.size_boundary_reached(
                self.pending
                    .iter()
                    .map(RelayRecordBatch::estimated_bytes)
                    .sum::<u64>(),
            ))
    }

    pub(super) fn flush_due(
        &self,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
    ) -> BranchBufferTimingResult<bool> {
        if self.pending.is_empty() {
            return Ok(false);
        }
        self.flush_timer.is_due(clock, snapshot)
    }

    pub(super) fn take_pending(&mut self) -> Vec<RelayRecordBatch> {
        self.flush_timer.clear();
        std::mem::take(&mut self.pending)
    }
}

#[derive(Debug, Clone)]
pub(super) struct CompiledReordererProgram {
    pub(super) program: Arc<VmCompiledProgram>,
    pub(super) key_column_offset: usize,
    pub(super) key_count: usize,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledCorrelatorWhereProgram {
    pub(super) program: Arc<VmCompiledProgram>,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledCorrelatorOutputProgram {
    pub(super) program: CompiledProgramWithMaterializedInterest,
}

#[derive(Debug)]
pub(super) struct ReordererRowOrder {
    pub(super) key: Vec<ReorderKeyPart>,
    pub(super) arrival_sequence: u64,
}

#[derive(Debug)]
struct ReordererPendingBatch {
    pub(super) received_at: Timestamp,
    pub(super) row_order: Arc<Vec<ReordererRowOrder>>,
    pub(super) batch: RelayRecordBatch,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ReordererOutputBatchError {
    #[error("reorderer buffered {arrow_rows} Arrow rows with {ordering_keys} ordering keys")]
    OrderingKeyCount {
        arrow_rows: usize,
        ordering_keys: usize,
    },
    #[error("reorderer buffered row count overflowed usize")]
    RowCountOverflow,
    #[error("cannot order an empty reorderer output buffer")]
    Empty,
    #[error("failed to concatenate buffered reorderer batches: {report}")]
    Concatenate {
        report: Report<RelayRecordBatchError>,
    },
    #[error("failed to reorder the buffered relay batch: {report}")]
    Reorder {
        report: Report<RelayRecordBatchError>,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub(super) struct ReordererOutputBatchFailure {
    #[source]
    pub(super) error: ReordererOutputBatchError,
    pub(super) batches: Vec<RelayRecordBatch>,
}

impl ReordererOutputBatchFailure {
    /// Fail a flush while handing every buffered batch back to the caller. The caller resolves the
    /// ACKs those batches carry, so a failure that dropped one would strand them.
    fn retaining(
        error: ReordererOutputBatchError,
        pending: Vec<ReordererPendingBatch>,
    ) -> Box<Self> {
        Box::new(Self {
            error,
            batches: pending.into_iter().map(|pending| pending.batch).collect(),
        })
    }
}

#[derive(Debug, Default)]
pub(super) struct ReordererOutputBuffer {
    pending: Vec<ReordererPendingBatch>,
    estimated_bytes: u64,
}

impl ReordererOutputBuffer {
    pub(super) fn push(
        &mut self,
        batch: RelayRecordBatch,
        row_order: Arc<Vec<ReordererRowOrder>>,
        received_at: Timestamp,
    ) {
        self.estimated_bytes = self
            .estimated_bytes
            .checked_add(batch.estimated_bytes())
            .assured("both counts estimate bytes of batches this node already holds in memory");
        self.pending.push(ReordererPendingBatch {
            received_at,
            row_order,
            batch,
        });
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn first_received_at(&self) -> Option<Timestamp> {
        self.pending.first().map(|pending| pending.received_at)
    }

    pub(super) fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    pub(super) fn acks(&self) -> impl Iterator<Item = &AckSet> {
        self.pending
            .iter()
            .flat_map(|pending| pending.batch.acks.iter())
    }

    pub(super) fn clear(&mut self) {
        self.pending.clear();
        self.estimated_bytes = 0;
    }

    pub(super) fn take_ordered_batch(
        &mut self,
    ) -> Result<RelayRecordBatch, Box<ReordererOutputBatchFailure>> {
        // The buffer empties here whether or not ordering succeeds, so every failure below hands
        // the drained batches back and their ACKs stay owned by exactly one place.
        self.estimated_bytes = 0;
        let pending = std::mem::take(&mut self.pending);

        // Every buffered batch must carry one ordering key per Arrow row. Walking the buffer in
        // order makes the first batch that disagrees the failure the caller reports, and counts
        // the rows the ordered output will hold on the way.
        let mut row_count = 0_usize;
        for index in 0..pending.len() {
            let buffered = &pending[index];
            let arrow_rows = buffered.batch.batch.batch().num_rows();
            let ordering_keys = buffered.row_order.len();
            if ordering_keys != arrow_rows {
                let error = ReordererOutputBatchError::OrderingKeyCount {
                    arrow_rows,
                    ordering_keys,
                };
                return Err(ReordererOutputBatchFailure::retaining(error, pending));
            }
            let Some(total) = row_count.checked_add(arrow_rows) else {
                let error = ReordererOutputBatchError::RowCountOverflow;
                return Err(ReordererOutputBatchFailure::retaining(error, pending));
            };
            row_count = total;
        }
        if row_count == 0 {
            let error = ReordererOutputBatchError::Empty;
            return Err(ReordererOutputBatchFailure::retaining(error, pending));
        }

        // One permutation for the whole flush, expressed in the row space the concatenation below
        // produces.
        let permutation = Self::ordering_permutation(&pending, row_count);

        let batches = pending
            .into_iter()
            .map(|pending| pending.batch)
            .collect::<Vec<_>>();
        let concatenated = match RelayRecordBatch::concat_preserving(batches) {
            Ok(concatenated) => concatenated,
            Err(failure) => {
                let failure = *failure;
                return Err(Box::new(ReordererOutputBatchFailure {
                    error: ReordererOutputBatchError::Concatenate {
                        report: failure.error,
                    },
                    batches: failure.preserved,
                }));
            }
        };

        // Applying the permutation moves the Arrow columns and every sidecar together, so a
        // failure here returns the single batch that now carries all the buffered ACKs.
        match concatenated.into_reordered(&permutation) {
            Ok(ordered) => Ok(ordered),
            Err(failure) => {
                let failure = *failure;
                Err(Box::new(ReordererOutputBatchFailure {
                    error: ReordererOutputBatchError::Reorder {
                        report: failure.error,
                    },
                    batches: vec![failure.batch],
                }))
            }
        }
    }

    /// The permutation the flush applies, given as the concatenated row each ordered row reads
    /// from. `row_count` is the row total the validation above counted for the same buffer.
    fn ordering_permutation(pending: &[ReordererPendingBatch], row_count: usize) -> Vec<usize> {
        /// One buffered row waiting to be placed: the row concatenation gives it, and the key and
        /// arrival sequence that decide where the ordered output puts it.
        struct BufferedRow<'a> {
            row: usize,
            order: &'a ReordererRowOrder,
        }

        // Rows are addressed in the concatenated row space, where buffer order decides each
        // batch's offset exactly as the concatenation lays the batches out. Every offset stays
        // below `row_count`, which the count above already proved fits in `usize`.
        let mut rows = Vec::with_capacity(row_count);
        let mut offset = 0_usize;
        for buffered in pending {
            for (row_in_batch, order) in buffered.row_order.iter().enumerate() {
                rows.push(BufferedRow {
                    row: offset + row_in_batch,
                    order,
                });
            }
            offset += buffered.row_order.len();
        }

        // Ordering key first, then arrival sequence, then the concatenated row itself, so rows
        // that agree on both keep the order the buffer received them in.
        rows.sort_by(|left, right| {
            left.order
                .key
                .cmp(&right.order.key)
                .then(
                    left.order
                        .arrival_sequence
                        .cmp(&right.order.arrival_sequence),
                )
                .then(left.row.cmp(&right.row))
        });

        rows.into_iter().map(|buffered| buffered.row).collect()
    }
}

#[derive(Debug, Default)]
pub(super) struct InferencerOutputBuffer {
    pub(super) pending: Vec<RelayRecordBatch>,
    estimated_bytes: u64,
}

impl InferencerOutputBuffer {
    pub(super) fn push(&mut self, batch: RelayRecordBatch) {
        self.estimated_bytes = self
            .estimated_bytes
            .checked_add(batch.estimated_bytes())
            .assured("both counts estimate bytes of batches this node already holds in memory");
        self.pending.push(batch);
    }

    pub(super) fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    pub(super) fn clear(&mut self) {
        self.pending.clear();
        self.estimated_bytes = 0;
    }

    pub(super) fn take_pending(&mut self) -> Vec<RelayRecordBatch> {
        self.estimated_bytes = 0;
        std::mem::take(&mut self.pending)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum ReorderKeyPart {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(OrderedFloat<f64>),
    Utf8(String),
    Datetime(i64),
}

#[derive(Debug, Default)]
pub(super) struct CorrelatorBranchState {
    pub(super) pending_left: Vec<CorrelatorPendingMessage>,
    pub(super) pending_right: Vec<CorrelatorPendingMessage>,
}

#[derive(Debug)]
pub(super) struct CorrelatorPendingMessage {
    pub(super) received_at: Timestamp,
    pub(super) message: RelayMessage,
    pub(super) materialized_state: Arc<HashMap<String, RuntimeValue>>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct WindowBounds {
    pub(super) width_messages: Option<usize>,
    pub(super) step_messages: Option<usize>,
    pub(super) width_duration: Option<Duration>,
    pub(super) step_duration: Option<Duration>,
}

pub(super) struct WindowFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) materialized_state: &'a [nervix_models::MaterializedStateDependency],
    pub(super) execution_now: Timestamp,
}

pub(super) struct JunctionFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) input_relays: &'a [RelayName],
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    /// Resolved when the junction admitted this batch; a junction flushes within the same
    /// execution, so its routes read that snapshot rather than resolving again.
    pub(super) materialized_values: &'a HashMap<String, RuntimeValue>,
    pub(super) execution_now: Timestamp,
}

pub(super) struct InferencerFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) resource: &'a ResourceName,
    pub(super) resource_version: u64,
    pub(super) file: &'a str,
    pub(super) inputs: &'a [InferencerTensorMapping],
    pub(super) output_schema: &'a [InferencerTensorDeclaration],
    pub(super) compiled_input_program: &'a CompiledInferencerInputProgram,
    pub(super) input_relays: &'a [RelayName],
    pub(super) session: &'a mut Option<OnnxInferencerSession>,
    pub(super) materialized_state: &'a [nervix_models::MaterializedStateDependency],
    pub(super) execution_now: Timestamp,
}

pub(super) struct WasmFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) input_relays: &'a [RelayName],
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) resource: &'a ResourceName,
    pub(super) resource_version: u64,
    pub(super) file: &'a str,
    pub(super) limits: nervix_models::WasmProcessorLimits,
    pub(super) replicated_state: &'a ReplicatedWasmProcessorState,
    pub(super) execution_now: Timestamp,
}

/// The guest module of the one resource version a WASM processor pins, compiled once and shared
/// by every branch instance of the processor.
#[derive(Clone)]
pub(super) struct WasmCompiledBranchProcessor {
    pub(super) compiled: Arc<CompiledWasmProcessor>,
}

impl std::fmt::Debug for WasmCompiledBranchProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmCompiledBranchProcessor")
            .finish_non_exhaustive()
    }
}

pub(super) struct PlannedMessageError {
    pub(super) message: RelayMessage,
    pub(super) error: StructuredMessageError,
    pub(super) partial_output: Option<RuntimeRecordBatch>,
    pub(super) materialized_state: HashMap<String, RuntimeValue>,
    pub(super) execution_now: Timestamp,
}

#[derive(thiserror::Error)]
#[error("{reason}")]
pub(super) struct PlannedGeneralError {
    pub(super) acks: Vec<AckSet>,
    pub(super) reason: String,
}

pub(super) type PlannedGeneralResult<T> = error_stack::Result<T, PlannedGeneralError>;

impl std::fmt::Debug for PlannedGeneralError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlannedGeneralError")
            .field("ack_count", &self.acks.len())
            .field("reason", &self.reason)
            .finish()
    }
}

pub(super) struct FilterMapPlan {
    pub(super) batch: Option<RelayRecordBatch>,
    pub(super) message_errors: Vec<PlannedMessageError>,
}
