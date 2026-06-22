use std::{sync::Arc, time::Duration};

use ahash::{HashMap, HashSet};
use nervix_models::{
    AckMode, CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
    ErrorPolicies, Identifier, InferencerTensorMapping, ModelKind, ParameterValueMapping,
    RouterMatchPolicy, Timestamp, WindowBound,
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
pub(super) struct ParameterizedIngestorSpec {
    pub(super) kind: ModelKind,
    pub(super) identifier: Identifier,
    pub(super) root_relay: Identifier,
    pub(super) branch_ttl: Option<String>,
    pub(super) entrypoint_parameter_mappings: Vec<ParameterValueMapping>,
    pub(super) entrypoint_ack_boundary: ParametrizerAckBoundary,
    pub(super) entrypoint_flush_each: String,
    pub(super) entrypoint_max_batch_size: Option<String>,
    pub(super) error_policies: ErrorPolicies,
    pub(super) processors: Vec<ParameterizedProcessorSpec>,
    pub(super) roots: Vec<ParameterizedProcessorSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParametrizerAckBoundary {
    Preserve,
    Reingestor(AckMode),
}

#[derive(Debug, Clone)]
pub(super) struct ParameterizedProcessorSpec {
    pub(super) kind: ModelKind,
    pub(super) processor: Identifier,
    pub(super) input_relay: Identifier,
    pub(super) input_relays: Vec<Identifier>,
    pub(super) mode: AckMode,
    pub(super) error_policies: ErrorPolicies,
    pub(super) operation: ParameterizedProcessorOperationSpec,
}

#[derive(Debug, Clone)]
pub(super) enum ParameterizedProcessorOperationSpec {
    Deduplicator {
        output: ParameterizedProcessorOutputSpec,
        deduplicate_on: String,
        max_time: String,
        filter_map: Option<String>,
    },
    Router {
        filter_map: Option<String>,
        match_policy: RouterMatchPolicy,
        routes: Vec<ParameterizedRouterRouteSpec>,
        default_output: ParameterizedProcessorOutputSpec,
    },
    WindowProcessor {
        output: ParameterizedProcessorOutputSpec,
        width: WindowBound,
        step: WindowBound,
        aggregate: String,
    },
    Reorderer {
        output: ParameterizedProcessorOutputSpec,
        order_by: String,
        max_time: String,
        flush_each: String,
        max_batch_size: Option<String>,
        filter_map: Option<String>,
    },
    Correlator {
        output: ParameterizedProcessorOutputSpec,
        left_relay: Identifier,
        right_relay: Identifier,
        left_on: Vec<String>,
        right_on: Vec<String>,
        match_policy: CorrelatorMatchPolicy,
        output_assignments: String,
        max_time: String,
        flush_each: String,
        max_batch_size: Option<String>,
        timeout_policy: CorrelationTimeoutPolicy,
    },
    Unifier {
        output: ParameterizedProcessorOutputSpec,
        flush_each: String,
        max_batch_size: Option<String>,
        filter_map: Option<String>,
    },
    Inferencer {
        output: ParameterizedProcessorOutputSpec,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        outputs: Vec<InferencerTensorMapping>,
        flush_each: String,
        max_batch_size: Option<String>,
        filter_map: Option<String>,
    },
    WasmProcessor {
        output: ParameterizedProcessorOutputSpec,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
    },
}

#[derive(Debug, Clone)]
pub(super) struct ParameterizedProcessorOutputSpec {
    pub(super) relay: Identifier,
    pub(super) children: Vec<ParameterizedProcessorSpec>,
}

#[derive(Debug, Clone)]
pub(super) struct ParameterizedRouterRouteSpec {
    pub(super) condition: String,
    pub(super) output: ParameterizedProcessorOutputSpec,
}

impl ParameterizedIngestorSpec {
    pub(super) fn relay_ids(&self) -> HashSet<Identifier> {
        let mut relays = HashSet::default();
        relays.insert(self.root_relay.clone());

        fn collect(nodes: &[ParameterizedProcessorSpec], relays: &mut HashSet<Identifier>) {
            for node in nodes {
                relays.extend(node.input_relays.iter().cloned());
                match &node.operation {
                    ParameterizedProcessorOperationSpec::Deduplicator { output, .. }
                    | ParameterizedProcessorOperationSpec::Reorderer { output, .. }
                    | ParameterizedProcessorOperationSpec::WindowProcessor { output, .. }
                    | ParameterizedProcessorOperationSpec::Unifier { output, .. }
                    | ParameterizedProcessorOperationSpec::Inferencer { output, .. }
                    | ParameterizedProcessorOperationSpec::WasmProcessor { output, .. } => {
                        relays.insert(output.relay.clone());
                        collect(&output.children, relays);
                    }
                    ParameterizedProcessorOperationSpec::Correlator {
                        output,
                        timeout_policy,
                        ..
                    } => {
                        relays.insert(output.relay.clone());
                        if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.left {
                            relays.insert(relay.clone());
                        }
                        if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.right {
                            relays.insert(relay.clone());
                        }
                        collect(&output.children, relays);
                    }
                    ParameterizedProcessorOperationSpec::Router {
                        routes,
                        default_output,
                        ..
                    } => {
                        for route in routes {
                            relays.insert(route.output.relay.clone());
                            collect(&route.output.children, relays);
                        }
                        relays.insert(default_output.relay.clone());
                        collect(&default_output.children, relays);
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

        fn contains(nodes: &[ParameterizedProcessorSpec], relay: &Identifier) -> bool {
            nodes.iter().any(|node| match &node.operation {
                ParameterizedProcessorOperationSpec::Deduplicator { output, .. } => {
                    &output.relay == relay || contains(&output.children, relay)
                }
                ParameterizedProcessorOperationSpec::Router {
                    routes,
                    default_output,
                    ..
                } => {
                    routes.iter().any(|route| {
                        &route.output.relay == relay || contains(&route.output.children, relay)
                    }) || &default_output.relay == relay
                        || contains(&default_output.children, relay)
                }
                ParameterizedProcessorOperationSpec::WindowProcessor { output, .. } => {
                    &output.relay == relay || contains(&output.children, relay)
                }
                ParameterizedProcessorOperationSpec::Reorderer { output, .. } => {
                    &output.relay == relay || contains(&output.children, relay)
                }
                ParameterizedProcessorOperationSpec::Correlator { output, .. } => {
                    &output.relay == relay || contains(&output.children, relay)
                }
                ParameterizedProcessorOperationSpec::Unifier { output, .. } => {
                    &output.relay == relay || contains(&output.children, relay)
                }
                ParameterizedProcessorOperationSpec::Inferencer { output, .. } => {
                    &output.relay == relay || contains(&output.children, relay)
                }
                ParameterizedProcessorOperationSpec::WasmProcessor { output, .. } => {
                    &output.relay == relay || contains(&output.children, relay)
                }
            })
        }

        contains(&self.roots, relay)
    }
}

#[derive(Debug, Clone)]
pub(super) struct ParametrizerTemplate {
    pub(super) source_kind: ModelKind,
    pub(super) source: Identifier,
    pub(super) root_relay: Identifier,
    pub(super) branch_ttl: Option<Duration>,
    pub(super) entrypoint_schema: Arc<CompiledSchema>,
    pub(super) entrypoint_parameter_mappings: Vec<ParameterValueMapping>,
    pub(super) entrypoint_ack_boundary: ParametrizerAckBoundary,
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
    pub(super) input_relay: Identifier,
    pub(super) mode: AckMode,
    pub(super) error_policies: ErrorPolicies,
    pub(super) operation: RelayProcessorOperationTemplate,
}

#[derive(Debug, Clone)]
pub(super) enum RelayProcessorOperationTemplate {
    Deduplicator {
        output: RelayProcessorOutputTemplate,
        deduplicate_on: String,
        max_time: Duration,
        filter_map: Option<String>,
    },
    Router {
        filter_map: Option<String>,
        match_policy: RouterMatchPolicy,
        routes: Vec<StreamProcessorRouterRouteTemplate>,
        default_output: RelayProcessorOutputTemplate,
    },
    WindowProcessor {
        output: RelayProcessorOutputTemplate,
        width_messages: Option<usize>,
        step_messages: Option<usize>,
        width_duration: Option<Duration>,
        step_duration: Option<Duration>,
        aggregate: WindowAggregateProgram,
    },
    Reorderer {
        output: RelayProcessorOutputTemplate,
        order_by: String,
        max_time: Duration,
        flush_each: RuntimeFlushPolicy,
        filter_map: Option<String>,
    },
    Correlator {
        output: RelayProcessorOutputTemplate,
        left_relay: Identifier,
        right_relay: Identifier,
        left_on: Vec<String>,
        right_on: Vec<String>,
        match_policy: CorrelatorMatchPolicy,
        output_assignments: String,
        max_time: Duration,
        flush_each: RuntimeFlushPolicy,
        timeout_policy: CorrelationTimeoutPolicy,
    },
    Unifier {
        output: RelayProcessorOutputTemplate,
        flush_each: RuntimeFlushPolicy,
        filter_map: Option<String>,
    },
    Inferencer {
        output: RelayProcessorOutputTemplate,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        outputs: Vec<InferencerTensorMapping>,
        flush_each: RuntimeFlushPolicy,
        filter_map: Option<String>,
    },
    WasmProcessor {
        output: RelayProcessorOutputTemplate,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
    },
}

#[derive(Debug, Clone)]
pub(super) struct RelayProcessorOutputTemplate {
    pub(super) output_relay: Identifier,
}

#[derive(Debug)]
pub(super) struct RelayProcessorNode {
    pub(super) kind: ModelKind,
    pub(super) processor: Identifier,
    pub(super) input_relay: Identifier,
    pub(super) mode: AckMode,
    pub(super) error_policies: ErrorPolicies,
    pub(super) operation: RelayProcessorOperationNode,
    pub(super) last_graph: Option<Arc<ActiveGraph>>,
    pub(super) generation: u64,
}

#[derive(Debug)]
pub(super) enum RelayProcessorOperationNode {
    Deduplicator {
        output: RelayProcessorOutputNode,
        deduplicate_on: String,
        max_time: Duration,
        filter_map: Option<String>,
        compiled_key_program: Option<Box<CompiledDeduplicatorKeyProgram>>,
        compiled_program: Option<CompiledProgramWithMaterializedInterest>,
        state: Arc<ReplicatedDeduplicatorState>,
    },
    Router {
        filter_map: Option<String>,
        match_policy: RouterMatchPolicy,
        routes: Vec<RelayProcessorRouterRouteNode>,
        default_output: RelayProcessorOutputNode,
        compiled_program: Option<CompiledProgramWithMaterializedInterest>,
    },
    WindowProcessor {
        output: RelayProcessorOutputNode,
        width_messages: Option<usize>,
        step_messages: Option<usize>,
        width_duration: Option<Duration>,
        step_duration: Option<Duration>,
        aggregate: WindowAggregateProgram,
        state: WindowProcessorState,
        replicated_state: Arc<ReplicatedWindowProcessorState>,
    },
    Reorderer {
        output: RelayProcessorOutputNode,
        order_by: String,
        max_time: Duration,
        flush_each: RuntimeFlushPolicy,
        filter_map: Option<String>,
        compiled_program: Option<Box<CompiledReordererProgram>>,
        compiled_filter_map: Option<CompiledProgramWithMaterializedInterest>,
        pending: Vec<ReordererPendingMessage>,
        arrival_sequence: u64,
        next_flush: Option<Timestamp>,
    },
    Correlator {
        output: RelayProcessorOutputNode,
        left_relay: Identifier,
        right_relay: Identifier,
        left_on: Vec<String>,
        right_on: Vec<String>,
        match_policy: CorrelatorMatchPolicy,
        output_assignments: String,
        max_time: Duration,
        flush_each: RuntimeFlushPolicy,
        timeout_policy: CorrelationTimeoutPolicy,
        compiled_left_key_program: Option<Box<CompiledCorrelatorKeyProgram>>,
        compiled_right_key_program: Option<Box<CompiledCorrelatorKeyProgram>>,
        compiled_output_program: Option<Box<CompiledCorrelatorOutputProgram>>,
        state: SharedCorrelatorBranchState,
    },
    Unifier {
        output: RelayProcessorOutputNode,
        flush_each: RuntimeFlushPolicy,
        filter_map: Option<String>,
        compiled_program: Option<CompiledProgramWithMaterializedInterest>,
        pending: Vec<RelayRecordBatch>,
        next_flush: Option<Timestamp>,
    },
    Inferencer {
        output: RelayProcessorOutputNode,
        resource: Identifier,
        resource_version: Option<u64>,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        outputs: Vec<InferencerTensorMapping>,
        flush_each: RuntimeFlushPolicy,
        filter_map: Option<String>,
        compiled_program: Option<CompiledProgramWithMaterializedInterest>,
        pending: Vec<RelayRecordBatch>,
        next_flush: Option<Timestamp>,
    },
    WasmProcessor {
        output: RelayProcessorOutputNode,
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

#[derive(Debug)]
pub(super) struct RelayProcessorOutputNode {
    pub(super) relay: Identifier,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledReordererProgram {
    pub(super) program: VmCompiledProgram,
    pub(super) key_column_offset: usize,
    pub(super) key_count: usize,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledCorrelatorKeyProgram {
    pub(super) program: VmCompiledProgram,
    pub(super) key_column_offset: usize,
    pub(super) key_count: usize,
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
    pub(super) pending: HashMap<String, CorrelatorPendingSlot>,
    pub(super) output_pending: Vec<RelayMessage>,
    pub(super) next_flush: Option<Timestamp>,
}

#[derive(Debug, Default)]
pub(super) struct CorrelatorPendingSlot {
    pub(super) left: Option<CorrelatorPendingMessage>,
    pub(super) right: Option<CorrelatorPendingMessage>,
}

impl CorrelatorPendingSlot {
    pub(super) fn is_empty(&self) -> bool {
        self.left.is_none() && self.right.is_none()
    }
}

#[derive(Debug)]
pub(super) struct CorrelatorPendingMessage {
    pub(super) received_at: Timestamp,
    pub(super) message: RelayMessage,
}

#[derive(Debug, Clone)]
pub(super) struct StreamProcessorRouterRouteTemplate {
    pub(super) condition: String,
    pub(super) output: RelayProcessorOutputTemplate,
}

#[derive(Debug)]
pub(super) struct RelayProcessorRouterRouteNode {
    pub(super) condition: String,
    pub(super) output: RelayProcessorOutputNode,
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
    pub(super) output: &'a RelayProcessorOutputNode,
    pub(super) output_schema: &'a Arc<CompiledSchema>,
}

pub(super) struct UnifierFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: &'a str,
    pub(super) processor: &'a Identifier,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) input_relay: &'a Identifier,
    pub(super) output: &'a RelayProcessorOutputNode,
}

pub(super) struct InferencerFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: &'a str,
    pub(super) processor: &'a Identifier,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) input_relay: &'a Identifier,
    pub(super) output: &'a RelayProcessorOutputNode,
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
    pub(super) input_relay: &'a Identifier,
    pub(super) output: &'a RelayProcessorOutputNode,
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

pub(super) struct ParameterizedRouterBatchContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) processor: &'a Identifier,
    pub(super) node_kind: &'a str,
    pub(super) error_policies: &'a ErrorPolicies,
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
    pub(super) messages: Vec<RelayMessage>,
    pub(super) message_errors: Vec<PlannedMessageError>,
}

pub(super) struct RouterPlan {
    pub(super) route_messages: Vec<Vec<RelayMessage>>,
    pub(super) default_messages: Vec<RelayMessage>,
    pub(super) message_errors: Vec<PlannedMessageError>,
    pub(super) general_errors: Vec<PlannedGeneralError>,
}
