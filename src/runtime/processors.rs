use std::{sync::Arc, time::Duration};

use ahash::{HashMap, HashSet};
use nervix_models::{
    AckMode, BranchValueMapping, CorrelationTimeoutAction, CorrelationTimeoutPolicy,
    CorrelatorMatchPolicy, ErrorPolicies, Identifier, InferencerTensorMapping, ModelKind,
    Timestamp, WindowBound,
};
use nervix_nspl::window_processor::aggregate::WindowAggregateProgram;
use nervix_vm::CompiledProgram as VmCompiledProgram;
use nervix_wasm::{CompiledWasmProcessor, WasmBranchInstance};
use ordered_float::OrderedFloat;
use registry::ActiveGraph;

use super::{
    BranchRuntime, CompiledDeduplicatorKeyProgram, CompiledProgramWithMaterializedInterest,
    RelayBoundaryServices, RelayMessage, RelayRecordBatch, RelayRegistry,
    ReplicatedDeduplicatorState, ReplicatedWasmProcessorState, ReplicatedWindowProcessorState,
    RuntimeFlushPolicy, SharedActiveGraph, WindowProcessorState,
};
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{CompiledSchema, RuntimeRecord, RuntimeRecordMetadata},
};

pub(super) type WasmAckMap = HashMap<u64, WasmAckContext>;

#[derive(Debug, Clone)]
pub(super) struct WasmAckContext {
    pub(super) acks: AckSet,
    pub(super) metadata: RuntimeRecordMetadata,
    pub(super) record: RuntimeRecord,
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
        flush_each: String,
        max_batch_size: Option<String>,
    },
    Correlator {
        output_routes: BranchedProcessorOutputsSpec,
        left_relays: Vec<Identifier>,
        right_relays: Vec<Identifier>,
        correlate_where: String,
        match_policy: CorrelatorMatchPolicy,
        output_assignments: String,
        max_time: String,
        flush_each: String,
        max_batch_size: Option<String>,
        timeout_policy: CorrelationTimeoutPolicy,
    },
    Junction {
        output_routes: BranchedProcessorOutputsSpec,
        flush_each: String,
        max_batch_size: Option<String>,
    },
    Inferencer {
        output_routes: BranchedProcessorOutputsSpec,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        outputs: Vec<InferencerTensorMapping>,
        flush_each: String,
        max_batch_size: Option<String>,
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
    },
    Reorderer {
        output_routes: RelayProcessorOutputsTemplate,
        order_by: String,
        max_time: Duration,
        flush_each: RuntimeFlushPolicy,
    },
    Correlator {
        output_routes: RelayProcessorOutputsTemplate,
        left_relays: Vec<Identifier>,
        right_relays: Vec<Identifier>,
        correlate_where: String,
        match_policy: CorrelatorMatchPolicy,
        output_assignments: String,
        max_time: Duration,
        flush_each: RuntimeFlushPolicy,
        timeout_policy: CorrelationTimeoutPolicy,
    },
    Junction {
        output_routes: RelayProcessorOutputsTemplate,
        flush_each: RuntimeFlushPolicy,
    },
    Inferencer {
        output_routes: RelayProcessorOutputsTemplate,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        outputs: Vec<InferencerTensorMapping>,
        flush_each: RuntimeFlushPolicy,
    },
    WasmProcessor {
        output_routes: RelayProcessorOutputsTemplate,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
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
    pub(super) last_graph: Option<Arc<ActiveGraph>>,
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
        state: WindowProcessorState,
        replicated_state: Arc<ReplicatedWindowProcessorState>,
    },
    Reorderer {
        output_routes: RelayProcessorOutputsNode,
        order_by: String,
        max_time: Duration,
        flush_each: RuntimeFlushPolicy,
        compiled_program: Option<Box<CompiledReordererProgram>>,
        pending: Vec<ReordererPendingMessage>,
        arrival_sequence: u64,
        next_flush: Option<Timestamp>,
    },
    Correlator {
        output_routes: RelayProcessorOutputsNode,
        left_relays: Vec<Identifier>,
        right_relays: Vec<Identifier>,
        correlate_where: String,
        match_policy: CorrelatorMatchPolicy,
        output_assignments: String,
        max_time: Duration,
        flush_each: RuntimeFlushPolicy,
        timeout_policy: CorrelationTimeoutPolicy,
        compiled_where_program: Option<Box<CompiledCorrelatorWhereProgram>>,
        compiled_output_program: Option<Box<CompiledCorrelatorOutputProgram>>,
        state: SharedCorrelatorBranchState,
    },
    Junction {
        output_routes: RelayProcessorOutputsNode,
        flush_each: RuntimeFlushPolicy,
        pending: Vec<RelayRecordBatch>,
        next_flush: Option<Timestamp>,
    },
    Inferencer {
        output_routes: RelayProcessorOutputsNode,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        outputs: Vec<InferencerTensorMapping>,
        flush_each: RuntimeFlushPolicy,
        pending: Vec<RelayRecordBatch>,
        next_flush: Option<Timestamp>,
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

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputsNode {
    pub(super) routes: Vec<RelayProcessorOutputNode>,
}

impl RelayProcessorOutputsNode {
    pub(super) fn base_relay(&self) -> Option<Identifier> {
        self.routes.first().map(|output| output.relay.clone())
    }
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputNode {
    pub(super) relay: Identifier,
    pub(super) filter_map: Option<String>,
    pub(super) compiled_program: Option<CompiledProgramWithMaterializedInterest>,
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
    pub(super) output_pending: Vec<RelayMessage>,
    pub(super) next_flush: Option<Timestamp>,
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
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: &'a str,
    pub(super) processor: &'a Identifier,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) resource: &'a Identifier,
    pub(super) resource_version: Option<u64>,
    pub(super) file: &'a str,
    pub(super) inputs: &'a [InferencerTensorMapping],
    pub(super) outputs: &'a [InferencerTensorMapping],
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

pub(super) struct WasmCompiledBranchProcessor {
    pub(super) version: u64,
    pub(super) compiled: CompiledWasmProcessor,
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
