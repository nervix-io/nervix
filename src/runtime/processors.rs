use std::{sync::Arc as StdArc, time::Duration};

use ahash::{HashMap, HashSet};
use nervix_models::{
    AckMode, BranchValueMapping, CorrelationTimeoutAction, CorrelationTimeoutPolicy,
    CorrelatorMatchPolicy, ErrorPolicies, Identifier, InferencerTensorDeclaration,
    InferencerTensorMapping, MessageErrorPolicy, ModelKind, Timestamp, WindowBound,
};
use nervix_nspl::{
    vm_program::{FieldRef, Program as VmProgram, SpannedNode},
    window_processor::aggregate::{WindowAggregateExpr, WindowAggregateProgram},
};
use nervix_vm::{
    CompileBinding as VmCompileBinding, CompileOptions as VmCompileOptions,
    CompiledProgram as VmCompiledProgram, InstructionKind as VmInstructionKind,
    OutputMode as VmOutputMode, SchemaSensitivity as VmSchemaSensitivity,
    compile_program_with_options_for_bindings_with_sensitivity as compile_vm_program,
};
use nervix_wasm::{CompiledWasmProcessor, WasmBranchInstance};
use ordered_float::OrderedFloat;
use registry::ActiveGraph;
use triomphe::Arc;

use super::{
    BranchRuntime, CompiledDeduplicatorKeyProgram, CompiledProgramWithMaterializedInterest,
    RelayBoundaryServices, RelayMessage, RelayRecordBatch, RelayRegistry,
    ReplicatedDeduplicatorState, ReplicatedWasmProcessorState, ReplicatedWindowProcessorState,
    RuntimeFlushPolicy, SharedActiveGraph, WindowProcessorState, inferencer::OnnxInferencerSession,
};
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{CompiledSchema, RuntimeRecord, RuntimeRecordBatch, RuntimeRecordMetadata},
};

pub(super) type WasmAckMap = HashMap<u64, WasmAckContext>;

#[derive(Debug, Clone)]
pub(super) struct WasmAckContext {
    pub(super) acks: AckSet,
    pub(super) metadata: RuntimeRecordMetadata,
    pub(super) record: RuntimeRecord,
    pub(super) input_batch: Arc<RuntimeRecordBatch>,
    pub(super) input_row: usize,
}

#[derive(Debug, Clone)]
pub(super) struct BranchedIngestorSpec {
    pub(super) kind: ModelKind,
    pub(super) identifier: Identifier,
    pub(super) root_relay: Identifier,
    pub(super) branch_ttl: Option<String>,
    pub(super) branch_max_instances: Option<u64>,
    pub(super) entrypoint_branch_mappings: Vec<BranchValueMapping>,
    pub(super) entrypoint_ack_boundary: BranchInstanceAckBoundary,
    pub(super) entrypoint_flush_each: String,
    pub(super) entrypoint_max_batch_size: Option<String>,
    pub(super) error_policies: ErrorPolicies,
    pub(super) processors: Vec<BranchedProcessorSpec>,
    pub(super) roots: Vec<BranchedProcessorSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BranchInstanceAckBoundary {
    Preserve,
    Reingestor(AckMode),
}

#[derive(Debug, Clone)]
pub(super) struct BranchedProcessorSpec {
    pub(super) kind: ModelKind,
    pub(super) processor: Identifier,
    pub(super) input_relays: Vec<Identifier>,
    pub(super) mode: AckMode,
    pub(super) error_policies: ErrorPolicies,
    pub(super) from_where: HashMap<Identifier, String>,
    pub(super) filter_where: Option<String>,
    pub(super) operation: BranchedProcessorOperationSpec,
}

#[derive(Debug, Clone)]
pub(super) enum BranchedProcessorOperationSpec {
    Deduplicator {
        output_routes: BranchedProcessorOutputsSpec,
        deduplicate_on: String,
        max_time: String,
    },
    WindowProcessor {
        output_routes: BranchedProcessorOutputsSpec,
        width: WindowBound,
        step: WindowBound,
        aggregate: String,
    },
    Reorderer {
        output_routes: BranchedProcessorOutputsSpec,
        order_by: String,
        max_time: String,
    },
    Correlator {
        output_routes: BranchedProcessorOutputsSpec,
        left_relays: Vec<Identifier>,
        right_relays: Vec<Identifier>,
        correlate_where: String,
        match_policy: CorrelatorMatchPolicy,
        max_time: String,
        timeout_policy: CorrelationTimeoutPolicy,
    },
    Junction {
        output_routes: BranchedProcessorOutputsSpec,
    },
    Inferencer {
        output_routes: BranchedProcessorOutputsSpec,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        output_schema: Vec<InferencerTensorDeclaration>,
    },
    WasmProcessor {
        output_routes: BranchedProcessorOutputsSpec,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
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

    pub(super) fn outputs_mut(&mut self) -> impl Iterator<Item = &mut BranchedProcessorOutputSpec> {
        self.routes.iter_mut()
    }
}

#[derive(Debug, Clone)]
pub(super) struct BranchedProcessorOutputSpec {
    pub(super) relay: Identifier,
    pub(super) filter_map: Option<String>,
    pub(super) flush_each: Option<String>,
    pub(super) max_batch_size: Option<String>,
    pub(super) message_error_policy: MessageErrorPolicy,
    pub(super) children: Vec<BranchedProcessorSpec>,
}

impl BranchedIngestorSpec {
    pub(super) fn relay_ids(&self) -> HashSet<Identifier> {
        let mut relays = HashSet::default();
        relays.insert(self.root_relay.clone());

        fn collect(nodes: &[BranchedProcessorSpec], relays: &mut HashSet<Identifier>) {
            for node in nodes {
                relays.extend(node.input_relays.iter().cloned());
                match &node.operation {
                    BranchedProcessorOperationSpec::Deduplicator { output_routes, .. }
                    | BranchedProcessorOperationSpec::Reorderer { output_routes, .. }
                    | BranchedProcessorOperationSpec::WindowProcessor { output_routes, .. }
                    | BranchedProcessorOperationSpec::Junction { output_routes, .. }
                    | BranchedProcessorOperationSpec::Inferencer { output_routes, .. }
                    | BranchedProcessorOperationSpec::WasmProcessor { output_routes, .. } => {
                        for output in output_routes.outputs() {
                            relays.insert(output.relay.clone());
                            collect(&output.children, relays);
                        }
                    }
                    BranchedProcessorOperationSpec::Correlator {
                        output_routes,
                        timeout_policy,
                        ..
                    } => {
                        for output in output_routes.outputs() {
                            relays.insert(output.relay.clone());
                            collect(&output.children, relays);
                        }
                        if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.left {
                            relays.insert(relay.clone());
                        }
                        if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.right {
                            relays.insert(relay.clone());
                        }
                    }
                }
            }
        }

        collect(&self.roots, &mut relays);
        relays
    }

    pub(super) fn contains_stream(&self, relay: &Identifier) -> bool {
        if &self.root_relay == relay {
            return true;
        }

        fn contains(nodes: &[BranchedProcessorSpec], relay: &Identifier) -> bool {
            nodes.iter().any(|node| match &node.operation {
                BranchedProcessorOperationSpec::Deduplicator { output_routes, .. }
                | BranchedProcessorOperationSpec::WindowProcessor { output_routes, .. }
                | BranchedProcessorOperationSpec::Reorderer { output_routes, .. }
                | BranchedProcessorOperationSpec::Correlator { output_routes, .. }
                | BranchedProcessorOperationSpec::Junction { output_routes, .. }
                | BranchedProcessorOperationSpec::Inferencer { output_routes, .. }
                | BranchedProcessorOperationSpec::WasmProcessor { output_routes, .. } => {
                    output_routes
                        .outputs()
                        .any(|output| &output.relay == relay || contains(&output.children, relay))
                }
            })
        }

        contains(&self.roots, relay)
    }
}

#[derive(Debug, Clone)]
pub(super) struct BranchInstanceTemplate {
    pub(super) source_kind: ModelKind,
    pub(super) source: Identifier,
    pub(super) root_relay: Identifier,
    pub(super) branch_ttl: Option<Duration>,
    pub(super) branch_max_instances: Option<usize>,
    pub(super) entrypoint_schema: Arc<CompiledSchema>,
    pub(super) entrypoint_branch_mappings: Vec<BranchValueMapping>,
    pub(super) entrypoint_ack_boundary: BranchInstanceAckBoundary,
    pub(super) entrypoint_flush_each: RuntimeFlushPolicy,
    pub(super) error_policies: ErrorPolicies,
    pub(super) relays: HashMap<Identifier, RelayProcessorRelayTemplate>,
    pub(super) materialized_streams: HashSet<Identifier>,
    pub(super) processors: HashMap<Identifier, RelayProcessorTemplate>,
    pub(super) processors_by_input: HashMap<Identifier, Vec<Identifier>>,
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorRelayTemplate {
    pub(super) registry: RelayRegistry,
    pub(super) services: Arc<RelayBoundaryServices>,
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorTemplate {
    pub(super) kind: ModelKind,
    pub(super) processor: Identifier,
    pub(super) input_relays: Vec<Identifier>,
    pub(super) mode: AckMode,
    pub(super) error_policies: ErrorPolicies,
    pub(super) from_where: HashMap<Identifier, String>,
    pub(super) filter_where: Option<String>,
    pub(super) operation: RelayProcessorOperationTemplate,
}

#[derive(Debug, Clone)]
pub(super) enum RelayProcessorOperationTemplate {
    Deduplicator {
        output_routes: RelayProcessorOutputsTemplate,
        deduplicate_on: String,
        max_time: Duration,
    },
    WindowProcessor {
        output_routes: RelayProcessorOutputsTemplate,
        width_messages: Option<usize>,
        step_messages: Option<usize>,
        width_duration: Option<Duration>,
        step_duration: Option<Duration>,
        aggregate: WindowAggregateProgram,
        compiled_aggregate: CompiledWindowAggregateProgram,
    },
    Reorderer {
        output_routes: RelayProcessorOutputsTemplate,
        order_by: String,
        max_time: Duration,
    },
    Correlator {
        output_routes: RelayProcessorOutputsTemplate,
        left_relays: Vec<Identifier>,
        right_relays: Vec<Identifier>,
        correlate_where: String,
        match_policy: CorrelatorMatchPolicy,
        max_time: Duration,
        timeout_policy: CorrelationTimeoutPolicy,
    },
    Junction {
        output_routes: RelayProcessorOutputsTemplate,
    },
    Inferencer {
        output_routes: RelayProcessorOutputsTemplate,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        output_schema: Vec<InferencerTensorDeclaration>,
    },
    WasmProcessor {
        output_routes: RelayProcessorOutputsTemplate,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        compiled: Option<WasmCompiledBranchProcessor>,
    },
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputsTemplate {
    pub(super) routes: Vec<RelayProcessorOutputTemplate>,
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputTemplate {
    pub(super) output_relay: Identifier,
    pub(super) filter_map: Option<String>,
    pub(super) flush_policy: Option<RuntimeFlushPolicy>,
    pub(super) message_error_policy: MessageErrorPolicy,
}

#[derive(Debug)]
pub(super) struct RelayProcessorNode {
    pub(super) kind: ModelKind,
    pub(super) processor: Identifier,
    pub(super) input_relays: Vec<Identifier>,
    pub(super) mode: AckMode,
    pub(super) error_policies: ErrorPolicies,
    pub(super) from_where: HashMap<Identifier, String>,
    pub(super) compiled_from_where: HashMap<Identifier, CompiledProgramWithMaterializedInterest>,
    pub(super) filter_where: Option<String>,
    pub(super) compiled_filter_where: HashMap<Identifier, CompiledProgramWithMaterializedInterest>,
    pub(super) operation: RelayProcessorOperationNode,
    pub(super) last_graph: Option<StdArc<ActiveGraph>>,
    pub(super) generation: u64,
}

#[derive(Debug)]
pub(super) enum RelayProcessorOperationNode {
    Deduplicator {
        output_routes: RelayProcessorOutputsNode,
        deduplicate_on: String,
        max_time: Duration,
        compiled_key_program: Option<Box<CompiledDeduplicatorKeyProgram>>,
        state: Arc<ReplicatedDeduplicatorState>,
    },
    WindowProcessor {
        output_routes: RelayProcessorOutputsNode,
        width_messages: Option<usize>,
        step_messages: Option<usize>,
        width_duration: Option<Duration>,
        step_duration: Option<Duration>,
        aggregate: WindowAggregateProgram,
        compiled_aggregate: CompiledWindowAggregateProgram,
        state: WindowProcessorState,
        replicated_state: Arc<ReplicatedWindowProcessorState>,
    },
    Reorderer {
        output_routes: RelayProcessorOutputsNode,
        order_by: String,
        max_time: Duration,
        compiled_program: Option<Box<CompiledReordererProgram>>,
        output_buffers: Vec<ReordererOutputBuffer>,
        arrival_sequence: u64,
    },
    Correlator {
        output_routes: RelayProcessorOutputsNode,
        left_relays: Vec<Identifier>,
        right_relays: Vec<Identifier>,
        correlate_where: String,
        match_policy: CorrelatorMatchPolicy,
        max_time: Duration,
        timeout_policy: CorrelationTimeoutPolicy,
        compiled_where_program: Option<Box<CompiledCorrelatorWhereProgram>>,
        compiled_output_programs: Vec<Option<Box<CompiledCorrelatorOutputProgram>>>,
        state: SharedCorrelatorBranchState,
    },
    Junction {
        output_routes: RelayProcessorOutputsNode,
    },
    Inferencer {
        output_routes: RelayProcessorOutputsNode,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        output_schema: Vec<InferencerTensorDeclaration>,
        output_buffers: Vec<InferencerOutputBuffer>,
        session: Option<OnnxInferencerSession>,
    },
    WasmProcessor {
        output_routes: RelayProcessorOutputsNode,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        compiled: Option<WasmCompiledBranchProcessor>,
        instance: Option<Box<WasmBranchInstance>>,
        replicated_state: Arc<ReplicatedWasmProcessorState>,
        ack_map: WasmAckMap,
        next_ack_token: u64,
        pending: Vec<RelayRecordBatch>,
    },
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

#[derive(Debug, Clone)]
pub(super) struct CompiledWindowAggregateProgram {
    pub(super) assignments: Vec<CompiledWindowAggregateAssignment>,
    pub(super) demand_types: Vec<arrow_schema::DataType>,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledWindowAggregateAssignment {
    pub(super) target: FieldRef,
    pub(super) value: CompiledWindowAggregateExpr,
}

#[derive(Debug, Clone)]
pub(super) enum CompiledWindowAggregateExpr {
    Scalar(VmCompiledProgram),
    Array {
        items: Vec<CompiledWindowAggregateExpr>,
        fixed_size: bool,
    },
}

impl CompiledWindowAggregateProgram {
    pub(super) fn compile(
        aggregate: &WindowAggregateProgram,
        input_relays: &[Identifier],
        output_relay: &Identifier,
        relay_schemas: &HashMap<Identifier, Arc<CompiledSchema>>,
    ) -> Result<Self, String> {
        let output_schema = relay_schemas.get(output_relay).ok_or_else(|| {
            format!(
                "window aggregate output relay '{}' has no runtime schema",
                output_relay.as_str()
            )
        })?;
        let bindings = input_relays
            .iter()
            .map(|relay| {
                let schema = relay_schemas.get(relay).ok_or_else(|| {
                    format!(
                        "window aggregate input relay '{}' has no runtime schema",
                        relay.as_str()
                    )
                })?;
                Ok(
                    VmCompileBinding::readonly(relay.as_str(), schema.arrow_schema())
                        .with_sensitivity(schema.vm_sensitivity()),
                )
            })
            .collect::<Result<Vec<_>, String>>()?;
        let assignments = aggregate
            .assignments
            .iter()
            .map(|assignment| {
                let target_field = output_schema
                    .arrow_schema()
                    .field_with_name(&assignment.target.field)
                    .cloned()
                    .map_err(|_| {
                        format!(
                            "window aggregate output schema is missing field '{}'",
                            assignment.target.field
                        )
                    })?;
                let target_sensitive = output_schema
                    .vm_sensitivity()
                    .is_sensitive(&assignment.target.field);
                Ok(CompiledWindowAggregateAssignment {
                    target: assignment.target.clone(),
                    value: Self::compile_expr(
                        &assignment.value.inner,
                        &assignment.target,
                        target_field.data_type(),
                        target_sensitive,
                        &bindings,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut demand_types = vec![None; aggregate.demands().len()];
        for assignment in &assignments {
            assignment.value.collect_demand_types(&mut demand_types)?;
        }
        let demand_types = demand_types
            .into_iter()
            .enumerate()
            .map(|(id, data_type)| {
                data_type.ok_or_else(|| {
                    format!("window aggregate demand {id} has no compiled invocation")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            assignments,
            demand_types,
        })
    }

    fn compile_expr(
        expr: &WindowAggregateExpr,
        target: &FieldRef,
        target_type: &arrow_schema::DataType,
        target_sensitive: bool,
        bindings: &[VmCompileBinding],
    ) -> Result<CompiledWindowAggregateExpr, String> {
        match expr {
            WindowAggregateExpr::Scalar(expr) => {
                let output_schema =
                    StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                        &target.field,
                        target_type.clone(),
                        false,
                    )]));
                let output_sensitivity = if target_sensitive {
                    VmSchemaSensitivity::from_sensitive_fields([target.field.clone()])
                } else {
                    VmSchemaSensitivity::default()
                };
                let mut compile_bindings = bindings.to_vec();
                compile_bindings.push(VmCompileBinding::writeonly(
                    target.relay.clone(),
                    output_schema.clone(),
                ));
                let program = SpannedNode {
                    inner: VmProgram {
                        filter: None,
                        branch_filters: Vec::new(),
                        set: vec![(target.clone(), expr.clone())],
                        unset: Vec::new(),
                        invoke: Vec::new(),
                    },
                    span: expr.span,
                };
                compile_vm_program(
                    &program,
                    output_schema,
                    output_sensitivity,
                    compile_bindings,
                    VmCompileOptions {
                        output_mode: VmOutputMode::ExplicitOnly,
                        ..VmCompileOptions::default()
                    },
                )
                .map(CompiledWindowAggregateExpr::Scalar)
                .map_err(|error| format!("window aggregate VM compile failed: {}", error.message))
            }
            WindowAggregateExpr::Array(items) => {
                let (element_type, fixed_size) = match target_type {
                    arrow_schema::DataType::FixedSizeList(field, _) => (field.data_type(), true),
                    arrow_schema::DataType::List(field) => (field.data_type(), false),
                    other => {
                        return Err(format!(
                            "window aggregate array cannot be assigned to {other:?} field '{}'",
                            target.field
                        ));
                    }
                };
                Ok(CompiledWindowAggregateExpr::Array {
                    items: items
                        .iter()
                        .map(|item| {
                            Self::compile_expr(
                                &item.inner,
                                target,
                                element_type,
                                target_sensitive,
                                bindings,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    fixed_size,
                })
            }
        }
    }
}

impl CompiledWindowAggregateExpr {
    fn collect_demand_types(
        &self,
        demand_types: &mut [Option<arrow_schema::DataType>],
    ) -> Result<(), String> {
        match self {
            Self::Scalar(program) => {
                for instruction in &program.instructions {
                    if let VmInstructionKind::Inject {
                        function: nervix_nspl::vm_program::FunctionName::WindowAggregate(invocation),
                        output_type,
                        ..
                    } = &instruction.kind
                    {
                        let Some(existing) = demand_types.get_mut(invocation.demand_id) else {
                            return Err(format!(
                                "compiled window aggregate references unknown demand {}",
                                invocation.demand_id
                            ));
                        };
                        if existing
                            .as_ref()
                            .is_some_and(|existing| existing != output_type)
                        {
                            return Err(format!(
                                "window aggregate demand {} has incompatible output types",
                                invocation.demand_id
                            ));
                        }
                        *existing = Some(output_type.clone());
                    }
                }
                Ok(())
            }
            Self::Array { items, .. } => {
                for item in items {
                    item.collect_demand_types(demand_types)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputsNode {
    pub(super) routes: Vec<RelayProcessorOutputNode>,
}

impl RelayProcessorOutputsNode {
    pub(super) fn base_relay(&self) -> Option<Identifier> {
        self.routes.first().map(|output| output.relay.clone())
    }

    pub(super) fn next_flush(&self) -> Option<Timestamp> {
        self.routes
            .iter()
            .filter_map(|output| output.next_flush)
            .min()
    }
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputNode {
    pub(super) relay: Identifier,
    pub(super) filter_map: Option<String>,
    pub(super) flush_policy: Option<RuntimeFlushPolicy>,
    pub(super) message_error_policy: MessageErrorPolicy,
    pub(super) pending: Vec<RelayRecordBatch>,
    pub(super) next_flush: Option<Timestamp>,
    pub(super) compiled_program: Option<CompiledProgramWithMaterializedInterest>,
}

impl RelayProcessorOutputNode {
    pub(super) fn schedule_input_flush(
        &mut self,
        now: Timestamp,
        pending_bytes: u64,
    ) -> Option<bool> {
        match self.flush_policy? {
            RuntimeFlushPolicy::Immediate => Some(true),
            RuntimeFlushPolicy::Each {
                interval,
                max_batch_size,
            } => {
                let deadline = self
                    .next_flush
                    .get_or_insert_with(|| super::checked_add_duration_to_timestamp(now, interval));
                Some(*deadline <= now || pending_bytes >= max_batch_size)
            }
        }
    }

    pub(super) fn flush_deadline_due(&self, now: Timestamp) -> bool {
        self.next_flush.is_some_and(|deadline| deadline <= now)
    }

    pub(super) fn force_flush_at(&mut self, now: Timestamp) {
        self.next_flush = Some(now);
    }

    pub(super) fn clear_flush_deadline(&mut self) {
        self.next_flush = None;
    }

    pub(super) fn enqueue(&mut self, batch: RelayRecordBatch, now: Timestamp) -> bool {
        self.pending.push(batch);
        match self.flush_policy {
            None | Some(RuntimeFlushPolicy::Immediate) => true,
            Some(RuntimeFlushPolicy::Each {
                interval,
                max_batch_size,
            }) => {
                let deadline = self
                    .next_flush
                    .get_or_insert_with(|| super::checked_add_duration_to_timestamp(now, interval));
                *deadline <= now
                    || self
                        .pending
                        .iter()
                        .map(RelayRecordBatch::estimated_bytes)
                        .sum::<u64>()
                        >= max_batch_size
            }
        }
    }

    pub(super) fn flush_due(&self, now: Timestamp) -> bool {
        !self.pending.is_empty() && self.next_flush.is_some_and(|deadline| deadline <= now)
    }

    pub(super) fn take_pending(&mut self) -> Vec<RelayRecordBatch> {
        self.next_flush = None;
        std::mem::take(&mut self.pending)
    }
}

#[derive(Debug, Clone)]
pub(super) struct CompiledReordererProgram {
    pub(super) program: VmCompiledProgram,
    pub(super) key_column_offset: usize,
    pub(super) key_count: usize,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledCorrelatorWhereProgram {
    pub(super) program: VmCompiledProgram,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledCorrelatorOutputProgram {
    pub(super) program: VmCompiledProgram,
}

#[derive(Debug)]
pub(super) struct ReordererPendingMessage {
    pub(super) key: Vec<ReorderKeyPart>,
    pub(super) arrival_sequence: u64,
    pub(super) received_at: Timestamp,
    pub(super) message: RelayMessage,
}

#[derive(Debug, Default)]
pub(super) struct ReordererOutputBuffer {
    pub(super) pending: Vec<ReordererPendingMessage>,
    pub(super) estimated_bytes: u64,
}

impl ReordererOutputBuffer {
    pub(super) fn clear(&mut self) {
        self.pending.clear();
        self.estimated_bytes = 0;
    }

    pub(super) fn take_pending(&mut self) -> Vec<ReordererPendingMessage> {
        self.estimated_bytes = 0;
        std::mem::take(&mut self.pending)
    }
}

#[derive(Debug, Default)]
pub(super) struct InferencerOutputBuffer {
    pub(super) pending: Vec<RelayRecordBatch>,
    estimated_bytes: u64,
}

impl InferencerOutputBuffer {
    pub(super) fn push(&mut self, batch: RelayRecordBatch) {
        self.estimated_bytes = self.estimated_bytes.saturating_add(batch.estimated_bytes());
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ReorderKeyPart {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(OrderedFloat<f64>),
    Utf8(String),
    Datetime(i64),
}

pub(super) type SharedCorrelatorBranchState = Arc<parking_lot::Mutex<CorrelatorBranchState>>;

#[derive(Debug, Default)]
pub(super) struct CorrelatorBranchState {
    pub(super) pending_left: Vec<CorrelatorPendingMessage>,
    pub(super) pending_right: Vec<CorrelatorPendingMessage>,
}

#[derive(Debug)]
pub(super) struct CorrelatorPendingMessage {
    pub(super) received_at: Timestamp,
    pub(super) message: RelayMessage,
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
    pub(super) node_kind: &'a str,
    pub(super) processor: &'a Identifier,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
}

pub(super) struct JunctionFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: &'a str,
    pub(super) processor: &'a Identifier,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) input_relays: &'a [Identifier],
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
}

pub(super) struct InferencerFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: &'a str,
    pub(super) processor: &'a Identifier,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) resource: &'a Identifier,
    pub(super) resource_version: Option<u64>,
    pub(super) file: &'a str,
    pub(super) inputs: &'a [InferencerTensorMapping],
    pub(super) output_schema: &'a [InferencerTensorDeclaration],
    pub(super) input_relays: &'a [Identifier],
    pub(super) session: &'a mut Option<OnnxInferencerSession>,
}

pub(super) struct WasmFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: &'a str,
    pub(super) processor: &'a Identifier,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) input_relays: &'a [Identifier],
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) resource: &'a Identifier,
    pub(super) resource_version: Option<u64>,
    pub(super) file: &'a str,
    pub(super) replicated_state: &'a ReplicatedWasmProcessorState,
}

#[derive(Clone)]
pub(super) struct WasmCompiledBranchProcessor {
    pub(super) version: u64,
    pub(super) compiled: Arc<CompiledWasmProcessor>,
}

impl std::fmt::Debug for WasmCompiledBranchProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmCompiledBranchProcessor")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

pub(super) struct PlannedMessageError {
    pub(super) message: RelayMessage,
    pub(super) reason: String,
}

pub(super) struct PlannedGeneralError {
    pub(super) acks: Vec<AckSet>,
    pub(super) reason: String,
}

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
