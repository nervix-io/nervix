//! What one node executes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Relay boundaries and their fan-out, branch-local processor tasks, ingestor and
//!   emitter connectors, compiled programs, materialized state, deduplication, reordering, windows,
//!   correlation, generators, in-flight acknowledgement tracking, node-owned state persistence and
//!   replication, and the entities that bind listening ports.
//! - **Depends on.** The engines — the VM, the UDF and WASM hosts, codecs, the interconnect, the
//!   state and resource stores — the vocabulary, and the registry's `ActiveGraph` as its input.
//! - **Must not know.** NSPL text, transactions, the gRPC surface or consensus. It is told what to
//!   run and runs it.
//!
//! This module breaks its own contract: it reads Models directly rather than consuming a planned
//! execution. A planner between the control state and the runtime closes that boundary;
//! `just ratchet` counts what is left.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use ahash::{HashMap, HashMapExt, HashSet, RandomState};
use arc_swap::{ArcSwap, ArcSwapOption};
use arch_into::ArchInto as _;
use arrow_array::{
    Array, ArrayRef, BooleanArray, ListArray, RecordBatch, RecordBatchOptions, StringArray,
    UInt64Array,
    builder::{
        ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
        Int8Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
        TimestampNanosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
        make_builder,
    },
    new_empty_array, new_null_array,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::DataType as ArrowDataType;
use arrow_select::{
    concat::concat as concat_arrow_arrays, filter::filter as filter_arrow_array,
    take::take as take_arrow_array,
};
use chrono::{TimeZone, Utc};
use dashmap::DashMap;
use error_stack::Report;
use fjall::Database;
use futures_util::{future::BoxFuture, stream::FuturesUnordered};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use nervix_execution::{ChargedBytes, Executor};
use nervix_interconnect::{
    EntityGatePurpose, Envelope, InterconnectRequest, RelayAdmission, RelayAdmissionDecision,
    RelayAdmissionStatus, RelayCancellationGuard, RelayDelivery, RelayPayload, RelayPayloadKind,
    Transport,
};
use nervix_models::{
    AckMode, Assignment, BranchName, ClickHouseValueMapping, ClientConfigEntry, ClientName,
    ClientPoolBounds, ClusterNodeIncarnation, ClusterNodeName, ClusterSchedule, CodecName,
    CodecWireFormat, CorrelationTimeoutAction, CorrelatorMatchPolicy, CreateClientAzureBlob,
    CreateClientGcs, CreateClientIcebergRest, CreateClientKafka, CreateClientMqtt,
    CreateClientNats, CreateClientOtel, CreateClientPulsar, CreateClientRabbitMq,
    CreateClientRedis, CreateClientS3, CreateClientSentry, CreateClientSqs, CreateClientSyslog,
    CreateClientZeroMq, CreateCodec, CreateEmitter, CreateGenerator, CreateIngestor, CreateLookup,
    CreateReingestor, CreateRelay, CreateSignalingProtocol, CreateUdf, DomainClockAuthority,
    DomainConfig, DomainName, DomainNodeRef, DomainSchedule, DomainState, EmitSink,
    EmitterAckWindow, EmitterName, EmitterPublishingMode, EndpointName, EndpointType,
    ErrorPolicies, FieldName, FieldPath, FlushPolicy, GeneralErrorPolicy, GeneratorName,
    IcebergCatalog, IcebergStorageBackend, IcebergValueMapping, InferencerExecutionMode,
    InferencerTensorDeclaration, IngestQuiesceMode, IngestQuiesceOverflow, IngestSource,
    IngestTimestampSource, IngestorName, KafkaIngestMode, KafkaOffsetMode, KafkaPartitionSchedule,
    Literal as ModelLiteral, LookupName, MaterializedStatePolicy, MessageErrorCode,
    MessageErrorOperation, MessageErrorPolicy, Model, ModelIndex, ModelKind, ModelName,
    MongoDbConflictAction, MongoDbValueMapping, MqttIngestMode, MqttQos, MqttSession,
    MySqlConflictAction, MySqlValueMapping, NodeRef, OtelAggregationTemporality, OtelMetric,
    OtelMetricKind, OtelScope, OtelSignal, OtelValueMapping, OutputBranch, OwnershipStateComponent,
    OwnershipStateRecoveryOutcome, OwnershipStateReset, OwnershipStateResetCause,
    PostgresConflictAction, PostgresValueMapping, ProcessorOutput, PulsarIngestMode,
    RabbitMqIngestMode, RelayName, RemoteAckOutcome, RemoteAckRegistration, RemoteAckResolution,
    RemoteRuntimeField, ResourceId, ResourceName, ResourceVersionStatus, RetryPolicy,
    RouteConstruction, ScheduledModel, ScheduledNode, ScheduledNodes, SignalingProtocolName,
    SignalingWireFormat, SqsFifoGroup, SqsIngestMode, StructuredMessageError, SubscriptionName,
    Timestamp,
};
#[cfg(test)]
use nervix_models::{CreateClientHttp, CreateClientPrometheus, CreateClientWebsockets};
use nervix_recovery::{Discarded as _, NoReceiver as _, Reported as _};
use nervix_roto::UdfExecutor;
#[cfg(test)]
use nervix_vm::SPAWN_BLOCKING_ROW_THRESHOLD as VM_SPAWN_BLOCKING_ROW_THRESHOLD;
use nervix_vm::{
    CompileBinding as VmCompileBinding, CompileNamespace as VmCompileNamespace,
    CompileOptions as VmCompileOptions, CompiledProgram as VmCompiledProgram,
    ExecutionContext as VmExecutionContext, FunctionInjector as VmFunctionInjector,
    OutputMode as VmOutputMode, SchemaSensitivity as VmSchemaSensitivity, SemanticNamespaces,
    TypedArray as VmTypedArray, TypedBatch as VmTypedBatch, UdfSignatures as VmUdfSignatures,
    compile_program_with_options_for_bindings_with_sensitivity as compile_vm_program_with_options_for_bindings_with_sensitivity,
    execute_program_with_selection_in_context,
    infer_set_expr_types_for_bindings_with_udfs as infer_vm_set_expr_types_for_bindings_with_udfs,
    lower_branch_construction, lower_finalized_output_filter, lower_generated_route,
    lower_route_construction, lower_set_only_route, lower_transforming_route,
    program::{
        CaseArm, Expr, FunctionName, InternalFieldNamespace, InternalFieldRef, Literal,
        Span as VmSpan, SpannedExpr,
    },
    window::{
        WindowAggregateDemand, WindowAggregateFunction, WindowAggregateProgram,
        WindowAggregateStorageKind, lower_window_assignments,
    },
};
use nervix_wasm::{
    WasmAckSidecar, WasmAckToken, WasmAckTokenSet, WasmBranchInit, WasmEnvelope,
    WasmOutputColumnRef, WasmOutputRow, WasmRoutedOutput, WasmRuntime, WasmRuntimeConfig,
};
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use sorted_vec::SortedSet;
use tempfile::TempDir;
use thiserror::Error;
use tokio::{
    io::AsyncBufReadExt,
    sync::{Mutex, Notify, broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Duration, Instant, sleep, sleep_until},
};
use tokio_stream::StreamExt;
use tokio_util::{
    sync::CancellationToken,
    task::{AbortOnDropHandle, TaskTracker},
};
use tracing::{debug, error, info, trace, warn};
use triomphe::Arc;
use upon::Engine as TemplateEngine;

const OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL: Duration = Duration::from_millis(25);

#[cfg(test)]
use crate::runtime_schema::test_runtime_row;
use crate::{
    ConfiguredFaultInjection, cluster,
    metrics::{
        BranchEvictionReason, IngestorQuiesceMetricLabels, NodeBatchObservation,
        NodeLatencyObservation, NodeWithoutRelayObservation, RelayBatchObservation,
        RelayBufferObservation, RuntimeMetrics, RuntimeMetricsSnapshot,
    },
    registry::{ActiveGraph, RuntimeChange, RuntimeChanges},
    resource::ResourceStore,
    runtime_ack::{
        AckCompletion, AckOutcome, AckProgress, AckRequiredWaitGuard, AckRootTracker, AckSet,
    },
    runtime_schema::{
        CodecError, CompiledCodec, CompiledSchema, ProtobufDescriptorPool, RuntimeRecordBatch,
        RuntimeRecordBatchBuilder, RuntimeRecordMetadata, RuntimeRow, RuntimeValue,
        RuntimeValueColumn, compile_codec_with_protobuf, compile_schema, decode_with_codec,
        parse_as_type_from_arrow, runtime_value_arrow_array, runtime_value_from_arrow_array,
    },
    task_shutdown::JoinShutdown as _,
};

mod branch_aggregated_state;
mod branch_buffering;
mod branch_instance_registry;
mod branch_key;
mod branch_lru_state;
mod branch_runtime;
mod client_config;
mod correlator;
mod deduplicator;
mod domain_clock;
mod domain_execution;
mod domain_rebuild;
mod domain_wire_schemas;
mod emitter_supervision;
mod emitters;
mod endpoint;
mod entity_gate;
mod filter_map;
mod force_flush;
mod generator;
mod http_client;
mod inferencer;
mod inferencer_output;
mod ingest_group;
mod ingest_metadata;
mod ingestion_time;
mod ingestor_quiesce;
mod ingestor_start;
mod ingestor_start_plan;
mod ingestors;
mod kafka_offset_state;
mod lookup_hash_map;
mod lsm_sequence;
mod materialized_read;
mod materialized_state;
mod message_error;
mod message_error_delivery;
mod node_settings;
mod observability;
mod ownership_handoff_error;
mod physical_time;
mod planning;
mod processor_branch_task;
mod processor_output;
mod processor_template;
mod processors;
mod reconnect_backoff;
mod reingestor;
mod relay_batch;
mod relay_boundary;
mod relay_channel;
mod relay_interaction;
#[cfg(feature = "benchmarks")]
#[doc(hidden)]
pub mod relay_interaction_benchmark;
mod relay_processor_node;
mod remote_dispatch;
mod reorderer;
mod resources;
mod runtime_lifecycle;
mod schedule_apply;
mod schedule_delta;
mod scheduled_node;
mod service_url;
mod shared_clients;
mod state_replication;
mod state_store;
mod syslog;
#[cfg(test)]
mod test_fixtures;

use branch_aggregated_state::{
    BranchAggregatedRuntimeStateSnapshot, ReplicatedBranchAggregatedState,
    decode_branch_aggregated_snapshot, encode_branch_aggregated_snapshot,
};
use branch_buffering::{
    BranchBufferDeadline, BranchBufferTimer, BranchBufferTimingError, BranchBufferTimingResult,
    RuntimeFlushPolicy, RuntimeInputCollectPolicy, RuntimeInputCollector, RuntimeWake,
    wait_for_branch_buffer_deadlines,
};
use branch_instance_registry::BranchInstanceRegistry;
use branch_lru_state::{decode_branch_lru_snapshot, encode_branch_lru_snapshot};
use client_config::{client_tls_paths, read_tls_file, render_client_config_template};
use deduplicator::{
    CompiledDeduplicatorKeyProgram, DeduplicatorKey, ReplicatedDeduplicatorState,
    compile_deduplicator_key_program,
};
use force_flush::{DomainForceFlush, DomainForceFlushCompletion, DomainForceFlushParticipant};
use http_client::HttpClientConfig;
pub(crate) use ingestors::kafka::KafkaIngestor;
use kafka_offset_state::{
    KafkaOffsetSnapshotInstaller, KafkaOffsetStateAssignment, KafkaOffsetStateOriginator,
    KafkaOffsetStatePersistence, KafkaOffsetStateRead, KafkaTopicPartition,
    ReplicatedKafkaOffsetState,
};
use materialized_state::{
    MaterializedRelaySnapshotInstaller, MaterializedRelayStateAssignment,
    MaterializedRelayStateOriginator, MaterializedRelayStatePersistence,
    MaterializedRelayStateRead, ReplicatedMaterializedRelayState,
    decode_materialized_stream_snapshot, encode_materialized_stream_snapshot_entries,
};

/// Opaque runtime-state handle types exposed only so compile-fail tests can prove that forbidden
/// operations are absent from each capability.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod state_capability_compile_tests {
    pub use super::{
        kafka_offset_state::{
            KafkaOffsetSnapshotInstaller, KafkaOffsetStateOriginator, KafkaOffsetStateRead,
        },
        materialized_state::{
            MaterializedRelaySnapshotInstaller, MaterializedRelayStateOriginator,
            MaterializedRelayStateRead,
        },
    };
}

/// Opaque clock and deadline capabilities exposed only so compile-fail tests can prove that
/// logical and physical deadlines cannot be interchanged.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod clock_capability_compile_tests {
    pub use super::{
        domain_clock::{DomainClock, LogicalDeadline},
        physical_time::{PhysicalDeadline, PhysicalDeadlineCapability},
    };
}
use message_error_delivery::{
    MessageErrorDelivery, MessageErrorRouteKey, MessageErrorRouteRuntime, MessageErrorRouteTarget,
    matching_message_error_output,
};
#[cfg(test)]
use planning::PlannedModel;
use planning::{
    branched_node_specs_from_active_graph, branched_node_specs_from_scheduled_nodes,
    format_branched_by, materialize_ingestor_route_template,
    materialize_processor_instance_template, processor_template_for_graph_node,
};
use processors::{
    BranchInstanceAckBoundary, BranchInstanceTemplate, BranchedIngestorSpec, BranchedNodeSpecs,
    BranchedProcessorNodeSpec, BranchedProcessorOperationSpec, BranchedProcessorOutputSpec,
    BranchedProcessorOutputsSpec, BranchedProcessorSpec, CompiledCorrelatorOutputProgram,
    CompiledCorrelatorWhereProgram, CompiledInferencerInputProgram, CompiledReordererProgram,
    CompiledWindowAggregateExpr, CompiledWindowAggregateProgram, CorrelatorBranchState,
    CorrelatorPendingMessage, FilterMapPlan, InferencerFlushContext, InferencerOutputBuffer,
    IngestorRouteTemplate, JunctionFlushContext, PlannedGeneralError, PlannedMessageError,
    RelayProcessorNode, RelayProcessorOperationNode, RelayProcessorOperationTemplate,
    RelayProcessorOutputNode, RelayProcessorOutputTemplate, RelayProcessorOutputsNode,
    RelayProcessorOutputsTemplate, RelayProcessorRelayTemplate, RelayProcessorTemplate,
    ReorderKeyPart, ReordererOutputBuffer, ReordererRowOrder, WasmAckContext, WasmAckMap,
    WasmCompiledBranchProcessor, WasmFlushContext, WindowBounds, WindowFlushContext,
};
pub use relay_batch::RelayMessage;
pub(crate) use relay_batch::RelayRecordBatch;
use relay_batch::build_stream_record_batch_preserving_acks;
type RelayDispatchResult = Result<(), Box<RelayRecordBatch>>;

/// Why an ingestor that keeps running discards the summary a flush returns.
///
/// See [`Runtime::flush_ingest_collector`], which routes every failure through the ingestor's
/// error policy before returning that summary.
const INGEST_FLUSH_FAILURES_ARE_HANDLED: &str =
    "the ingestor's error policy already handled every failure this flush produced";
pub(crate) use relay_channel::{
    RelayBroadcast, RelayDispatchGate, RelayDispatchGateLease,
    RelayReceiver as RelaySubscriptionReceiver,
};
use relay_interaction::{
    RelayInteraction, RelayInteractionCommand, RelayInteractionError, RelayInteractionEvent,
    RelayInteractionInput,
};
pub(crate) type RelaySubscriptionRecvError = async_broadcast::RecvError;
use std::str::FromStr;

pub(crate) use branch_key::BranchKey;
use branch_key::branch_key_display;
use branch_runtime::{
    BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL, BranchRuntime, IngestorRouteRuntime,
    MaterializedBatchWaitContext, PendingMaterializedBatch, branch_lru_placement,
    flush_branch_junction, internal_processor_error_policies, output_error_policies,
    persist_branch_instance_lru_snapshot, wall_duration_until_domain_deadline,
};
pub(crate) use client_config::{ClientResourceMounts, ResolvedClientConfig};
use client_config::{
    ParsedRetryPolicy, client_config_entries, client_config_value, next_retry_delay,
    optional_bool_client_config_value, optional_client_config_value,
};
use correlator::{
    CorrelatorMatchedBatch, CorrelatorOutputCompileContext, CorrelatorOutputContext,
    CorrelatorSide, CorrelatorTimeoutContext, compile_correlator_where_program,
    correlate_incoming_message, enqueue_correlator_output, evaluate_correlator_output_batch,
    handle_correlator_timeout_action,
};
use domain_clock::{
    DomainCadenceOccurrence, DomainCadenceStart, DomainClock, DomainClockAccessResult,
    DomainClockLifecycle, DomainExecutionSnapshot, LogicalDeadline,
    checked_add_duration_to_timestamp, current_timestamp,
};
pub(crate) use domain_execution::LookupRuntime;
use domain_execution::{
    DomainExecution, DomainResourceKey, ObservedDomainTick, RuntimeDomainState,
};
use domain_rebuild::{branch_relays_from_branched_specs, relay_branching_schema_for_runtime};
use domain_wire_schemas::DomainWireSchemas;
use emitter_supervision::{
    EmitterRetryKind, EmitterRetryStatus, EmitterTaskCommand, ScheduledEmitterTask,
    clear_emitter_stop_signal,
};
pub use endpoint::EndpointDispatchOutcome;
use endpoint::{
    EndpointIngestBinding, EndpointRoute, HttpRouteKey, RoutedEndpoint, RoutedEndpointsByDomain,
};
pub(crate) use entity_gate::EntityGateLease;
use entity_gate::{
    ActiveDomainAlter, BranchQuiesceGauges, DomainActivityGuard, EntityAlterHold,
    EntityGateHoldKey, NodeQuiesceCounters, NodeQuiesceWorkGuard,
};
pub use entity_gate::{
    DomainDrainStatus, EmitterPublishingDrainState, EmitterPublishingDrainStatus,
    EntityDrainStatus, EntityGateHold,
};
pub(in crate::runtime) use filter_map::evaluate_sqs_fifo_group_program;
pub(crate) use filter_map::execute_filter_map_on_record;
use filter_map::{
    FilterMapBatchInputs, FilterMapOutcomeInputs, InferencerFilterMapTensors, VmUninitializedInput,
    append_filter_map_nested_value, evaluate_filter_map_on_batch, evaluate_output_branch_program,
    execute_filter_map_program_on_batch, expression_reads_sensitive_source,
    plan_emitter_filter_map_batch, plan_filter_map_messages,
};
use generator::{GeneratorTaskRouteSpec, GeneratorTaskSpec};
use inferencer_output::flush_branch_inferencer_output;
pub(crate) use ingest_group::INGEST_GROUP_MAX_ROWS;
use ingest_group::{
    BranchedEntrypointInput, IngestGroupDispatch, IngestRouteCollector, IngestorDependencies,
    IngestorRouteRuntimes, RawIngestDispatch, branched_branch_filter_blocking,
    branched_branch_plan_blocking, branched_entrypoint_batch_from_inputs_blocking,
    decode_ingested_payload,
};
use ingest_metadata::{
    BRANCH_NAMESPACE, INGEST_METADATA_NAMESPACE, IngestHeaderFunctionInjector,
    IngestMetadataBuilders, emit_sink_supports_headers, ingest_source_supports_headers,
};
pub(crate) use ingest_metadata::{
    IngestFilterMapMetadata, IngestMessageHeaders, IngestMetadataKind, IngestMetadataRow,
    NoIngestHeaders, RetainedIngestHeaders,
};
pub use ingestor_quiesce::IngestorQuiesceCounters;
pub(crate) use ingestor_quiesce::{
    BufferedIngestMetadata, BufferedIngestPayload, IngestorQuiesceCause, IngestorQuiesceControl,
    IngestorQuiesceIntake,
};
use ingestor_quiesce::{
    DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL, IngestorReadiness, IngestorRuntime,
    RuntimeReconnectStatus,
};
use ingestor_start_plan::*;
use lookup_hash_map::{
    LookupHashMapCall, LookupHashMapCallKey, collect_program_field_refs,
    compile_lookup_hash_map_calls, rewrite_lookup_hash_map_program,
};
use message_error::{
    MessageErrorCompileSchemas, MessageErrorFailure, MessageErrorHandling,
    MessageErrorSourceContext, SingleRecordFilterMapOutcome, captured_partial_output,
    invalid_output_fields, operation_for_filter_label, planned_structured_message_error,
    structured_message_error, vm_partial_output_row_to_runtime_batch,
};
use nervix_models::{
    CreateAvroWireSchema, CreateCborWireSchema, CreateJsonWireSchema, DeduplicatorName,
    ReingestorName, ResolvedCodecWireFormat, SchemaName, WireSchemaLookup, WireSchemaName,
};
pub use observability::{
    DataflowNodeTransientState, IngestorDescribe, KafkaDomainOffsetDescribe, LocalLookupDescription,
};
pub(crate) use ownership_handoff_error::{OwnershipHandoffError, OwnershipHandoffResult};
use processor_branch_task::{
    PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, ProcessorBranchHandoff, ProcessorNodeCommand,
    SpawnedSnapshotTask, WindowProcessorSnapshotRequest,
};
pub(in crate::runtime) use processor_branch_task::{
    ProcessorRuntimeContext, spawn_processor_node_runtime,
    spawn_processor_node_runtime_with_handoffs,
};
use processor_output::{
    PendingProcessorOutputBatch, PendingProcessorOutputMessageError, ProcessorMaterializedState,
    ProcessorOutputBatchScope, ProcessorOutputDispatchContext, ProcessorOutputFilterSource,
    dispatch_processor_output, dispatch_processor_outputs, flush_all_processor_outputs,
    flush_due_processor_outputs, pending_output_batches_by_key, processor_output_input_sensitivity,
};
use processor_template::{
    MaterializedDependencyResolution, ProcessorInputFilterKind, wasm_guest_call_schemas,
    wasm_instance_next_deadline,
};
use rdkafka::consumer::StreamConsumer;
pub(in crate::runtime) use reconnect_backoff::RuntimeReconnectBackoff;
use reingestor::ReingestorInputSpec;
pub(crate) use relay_boundary::scheduled_relay_owner_nodes;
use relay_boundary::{
    ConcreteRelayRuntime, ConcreteRelayRuntimeBuild, ExpiringRelayState, RelayBoundaryBuilder,
    RelayBoundaryFanout, RelayBoundaryFanoutMap, RelayBoundaryServices, RelayOutboundSlot,
    RelayOwnerTask, RelayRegistry, RelayRetention, RelayRuntimeFanIn, RelayStateTask,
    RelayStateTaskSpec, RemoteRuntimeConsumer, addressable_count,
};
use remote_dispatch::{REMOTE_ACK_ALIVE_INTERVAL, RemoteDispatchRegistry, RemoteDispatcher};
use reorderer::{ReordererFlushContext, flush_branch_reorderer_output, reorder_key_part};
use schedule_delta::ScheduleDelta;
use scheduled_node::{
    EmitterTaskBuildDeps, EmitterTaskDeps, ExecutionBuildDeps, ScheduledNodePlacement,
    ScheduledNodeTask,
};
use service_url::ServiceUrl;
pub(in crate::runtime) use shared_clients::{
    OpenClientError, SharedClientError, SharedClientLease,
};
pub(crate) use state_replication::StateSyncAck;
use state_replication::{
    ActivatedRuntimeStateHandoff, DEFAULT_STATE_REPLICATION_POLL_INTERVAL,
    DEFAULT_STATE_SNAPSHOT_INTERVAL, PendingStateCheckpointAnnouncement, PendingStateReplicaSync,
    PreparedForcedRuntimeStateRecovery, PreparedRuntimeStateHandoff, PreparedRuntimeStateSnapshot,
};
pub(crate) use state_store::{
    ForcedRuntimeStateRecoveryTransition, PersistedRuntimeStateEntry, RuntimePersistenceError,
    RuntimeStateHandoffTransition, RuntimeStateKind, RuntimeStateOperationError,
    RuntimeStatePlacement, RuntimeStateResult, RuntimeStateStore, StateAssignmentAuthority,
    StateAssignmentToken, StateAuthorityError, StateCapability, StateReplicationRoles,
};
#[cfg(test)]
use test_fixtures::{
    OptionalTestField, TWO_ITEM_TEST_CHANNEL_CAPACITY, TestIngestHeaders, batch_value,
    branch_model, branched_by, compile_window_aggregate_for_test, concrete_branch_key,
    construction, domain, execute_filter_map_for_test, expression, ingest_metadata_for_test,
    install_unpaced_test_domain, junction_branch_template, key_label, named, nonzero_capacity,
    paced_domain_state, processor_branched_by, quiesce_test_batch, row_value, scheduled_model,
    string_branch_key, test_domain_clock, test_domain_clock_authority,
    test_ingestor_quiesce_control, test_optional_schema, test_relay_boundary_services, test_schema,
    u32_branch_key, unpaced_domain_state, validate_wasm_test_output_groups,
    validate_wasm_test_outputs, vm_input_from_test_rows, wait_for_persisted_runtime_state_lsm,
    wasm_generated_pool, wasm_guest_column, wasm_guest_stream, wasm_input_acks,
    wasm_input_for_records, wasm_input_for_values, wasm_test_generated_output, wasm_test_output,
    window_aggregate, window_inputs, window_outputs, with_inherit_all,
};
use tls::RustlsClientConfigSource;
pub(crate) use vm_compile::{
    CompiledBranchProgram, CompiledDomainUdfs, CompiledEmitterFilterMapProgram,
    CompiledProgramWithMaterializedInterest, EmitterHeaders, MaterializedFieldInterest,
    MaterializedLookupKeyMode, MaterializedProgramInterest, RuntimeMaterializedRelaySpec,
    RuntimeVmCompileContext, compile_emitter_filter_map_program, compile_key_projection_program,
    compile_session_filter_map_program, compile_sqs_fifo_group_program,
};
use vm_compile::{
    CompiledMessageErrorSites, GeneratorSetProgramSchemas, OutputNamespaceInput,
    RuntimeCompileTarget, RuntimeFilterScope, RuntimeVmSchema, RuntimeVmSchemaPair,
    compile_expression_filter_program, compile_generator_set_program,
    compile_ingestor_filter_map_program, compile_message_error_set_program,
    compile_output_branch_program, compile_processor_output_filter_map_program,
    compile_processor_output_program, compile_reorderer_program, compile_scoped_filter_program,
    compile_wasm_output_filter_map_program, compiled_message_error_sites,
    evaluate_constant_expression_vm, materialized_stream_specs_for_graph,
    referenced_materialized_stream_bindings, relay_branch_schema_for_runtime,
    relay_schema_for_runtime, runtime_udf_compile_options, runtime_udf_signatures,
};
use vm_input::{
    SharedBatchColumns, VmInputProjectionSources, compute_lookup_hash_map_columns,
    project_vm_input_batch, relay_state_snapshot_from_side_inputs, runtime_value_type_name,
    runtime_values_input_column, vm_output_value, vm_typed_batch_selected_rows_to_runtime_batch,
    vm_typed_batch_to_runtime_batch,
};
use wasm_output::{
    WasmOutputContext, checkpoint_wasm_guest_state, dispatch_wasm_output_envelopes,
    persist_wasm_guest_state,
};
use wasm_processor::flush_branch_wasm_processor;
use wasm_state::ReplicatedWasmProcessorState;
pub use websocket_signaling::CompiledSignalingProtocol;
pub(crate) use websocket_signaling::{
    SignalingDataSink, SignalingProtobufDescriptors, WebsocketSignalingSession,
};
use window_processor::{
    WindowAggregateInput, WindowProcessorState, evaluate_window_aggregate_inputs,
    flush_ready_window_processor, message_timestamp, snapshot_window_processor_live_state,
    window_next_deadline, window_width_met,
};
use window_state::{
    LinearHistogramDelayedRemovalSnapshot, ReplicatedWindowProcessorState,
    WindowAggregateAccumulatorSnapshot, WindowEntrySnapshot, WindowProcessorStateSnapshot,
    WindowSequenceValueSnapshot, WindowSortedCountSnapshot,
};

#[cfg(test)]
const STUPID_CHANNEL_CAPACITY_REMOVE_ME: NonZeroUsize = NonZeroUsize::MIN;

/// Default deadline for draining one runtime branch during a domain or node transition.
pub const DEFAULT_DOMAIN_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// Includes the grace a branch task receives after its configured drain deadline.
pub const fn branch_task_stop_timeout(domain_drain_timeout: Duration) -> Duration {
    // Saturation is the meaning: a drain timeout configured near `Duration::MAX` already asks to
    // wait for as long as the process runs, and no grace can extend that further.
    domain_drain_timeout.saturating_add(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE)
}

/// How many runtime events the bus holds for a receiver that has fallen behind. A receiver that
/// exceeds it is told how many it missed rather than being left to believe it saw everything.
const RUNTIME_EVENT_CAPACITY: usize = 256;

pub const DEFAULT_TEMP_DIR: &str = "/tmp";

type SharedActiveGraph = StdArc<ArcSwapOption<ActiveGraph>>;

#[cfg(not(feature = "testing"))]
impl ConfiguredFaultInjection {
    fn emitter_should_fail(&self, _emitter: &EmitterName) -> bool {
        false
    }

    fn emitter_should_stall(&self, _emitter: &EmitterName) -> bool {
        false
    }

    fn ingestor_is_failed(&self, _ingestor: &IngestorName) -> bool {
        false
    }

    fn otel_client_is_unavailable(&self, _emitter: &EmitterName) -> bool {
        false
    }

    fn syslog_ingestor_bind_addr(&self, _node_id: &ClusterNodeName, configured: &str) -> String {
        configured.to_string()
    }

    fn state_replica_polling_is_paused(&self) -> bool {
        false
    }

    fn branch_instance_expiration_scan_interval(&self) -> Option<Duration> {
        None
    }

    fn domain_drain_timeout(&self) -> Option<Duration> {
        None
    }

    fn entity_gate_deadline(&self) -> Option<Duration> {
        None
    }

    async fn pause_remote_relay_admission_if_armed(
        &self,
        _domain: &DomainName,
        _branch: Option<&str>,
    ) {
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("ingestor '{ingestor}' in domain '{domain}' is already running")]
    IngestorAlreadyRunning { domain: String, ingestor: String },
    #[error("ingestor '{ingestor}' in domain '{domain}' is not running")]
    IngestorNotRunning { domain: String, ingestor: String },
    #[error("failed to initialize ingestor '{ingestor}' in domain '{domain}': {reason}")]
    StartIngestor {
        domain: String,
        ingestor: String,
        reason: String,
    },
    #[error("codec '{codec}' in domain '{domain}' is not instantiated")]
    CodecNotInstantiated { domain: String, codec: String },
    #[error("relay '{relay}' in domain '{domain}' is not instantiated")]
    RelayNotInstantiated { domain: String, relay: String },
    #[error("failed to build domain execution for '{domain}': {reason}")]
    BuildDomainExecution { domain: String, reason: String },
    #[error(
        "timed out waiting for runtime revision {revision} to become ready on nodes \
         {pending_nodes:?}"
    )]
    RuntimeRevisionReadiness {
        revision: u64,
        pending_nodes: Vec<ClusterNodeName>,
    },
    #[error(
        "cannot represent a runtime revision readiness deadline from node-unavailability timeout \
         {node_unavailability_timeout:?} and readiness propagation bound \
         {readiness_propagation_bound:?}"
    )]
    RuntimeRevisionReadinessDeadlineOverflow {
        node_unavailability_timeout: Duration,
        readiness_propagation_bound: Duration,
    },
    #[error("failed to decode remote relay '{relay}' in domain '{domain}': {reason}")]
    DecodeRemoteRelay {
        domain: String,
        relay: String,
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    Error(String),
}

/// The node's runtime event bus, and the one way a connector failure becomes observable.
///
/// A failure that arrives here has already been recovered from: the connector reconnects, retries,
/// or hands the message to its route's error policy, and the node keeps serving. What is left is
/// to make that recovery visible, which is why publishing goes through [`Self::report_error`]
/// rather than through the sender directly. Dropping the send result at each call site would leave
/// the recovery silent, and a recovery nobody can observe is indistinguishable from data loss.
#[derive(Clone)]
pub(crate) struct RuntimeEvents {
    sender: broadcast::Sender<RuntimeEvent>,
}

impl RuntimeEvents {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(RUNTIME_EVENT_CAPACITY);
        Self { sender }
    }

    /// Report a failure the node recovered from. Callers name the entity and domain in `message`,
    /// because this bus carries the report to readers that have no other way to tell them apart.
    ///
    /// The event reaches the sessions attached to this node and, through the fan-out task the node
    /// starts with, every peer. That task holds its subscription for as long as the node serves, so
    /// a send that finds no receiver means the node is still starting or has already torn the task
    /// down. Nothing is left to observe the event in that window, so it is logged at `warn`
    /// instead. A delivered event is traced at `debug`, because the observers are the report and
    /// many of these failures are per-message.
    pub(crate) fn report_error(&self, message: impl Into<String>) {
        let message = message.into();
        debug!(error = %message, "reported runtime error to observers");
        if let Err(broadcast::error::SendError(RuntimeEvent::Error(message))) =
            self.sender.send(RuntimeEvent::Error(message))
        {
            warn!(error = %message, "runtime error raised while no observer is attached");
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.sender.subscribe()
    }
}

/// The handle every task, ingestor, emitter, and connector carries. It is one `Arc` over the
/// node's state, so passing the runtime into a spawned task costs a single refcount rather than
/// one per piece of state the node owns.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<RuntimeInner>,
}

/// Everything one Nervix node owns for as long as it runs. These fields are reached only through
/// a `Runtime` handle and therefore hold their values directly. The few that keep an `Arc` of
/// their own have a second owner that outlives the handle's borrow, and each names that owner.
struct RuntimeInner {
    /// Also held by the entity gate's deadline task, which releases an expired lease long after
    /// the call that engaged it returned.
    ingestors: Arc<DashMap<DomainNodeRef, IngestorRuntime, RandomState>>,
    /// Also held by the entity gate's deadline task, alongside `ingestors`.
    ingestor_quiescence: Arc<DashMap<DomainNodeRef, Arc<IngestorQuiesceControl>, RandomState>>,
    ingestors_paused_for_memory_pressure: AtomicBool,
    ingestor_transient_errors: DashMap<DomainNodeRef, String, RandomState>,
    ingestor_reconnect_backoffs: DashMap<DomainNodeRef, RuntimeReconnectStatus, RandomState>,
    ingestor_readiness: DashMap<DomainNodeRef, IngestorReadiness, RandomState>,
    emitter_transient_errors: DashMap<DomainNodeRef, String, RandomState>,
    emitter_retry_statuses: DashMap<DomainNodeRef, EmitterRetryStatus, RandomState>,
    emitter_confirmation_waits: DashMap<DomainNodeRef, Arc<AtomicUsize>, RandomState>,
    /// One connector instance per named client on this node, keyed by the client it belongs to and
    /// held open by the emitters and ingestors leasing it.
    shared_clients: DashMap<DomainNodeRef, shared_clients::SharedClientSlot, RandomState>,
    /// The graph nodes that have asked a shared client for a connection and not yet been given
    /// one, keyed by the waiting node rather than the client it waits on.
    pool_waits: DashMap<DomainNodeRef, shared_clients::PoolWait, RandomState>,
    executions: DashMap<DomainName, DomainExecution, RandomState>,
    message_error_routes: DashMap<MessageErrorRouteKey, Arc<MessageErrorRouteRuntime>, RandomState>,
    compiled_domain_udfs: DashMap<DomainName, CompiledDomainUdfs, RandomState>,
    schedule_apply_lock: Mutex<()>,
    applied_cluster_revision: AtomicU64,
    domain_instantiation_errors: DashMap<DomainName, String, RandomState>,
    domains: DashMap<DomainName, RuntimeDomainState, RandomState>,
    domain_status_changed: watch::Sender<u64>,
    in_flight_by_domain: DashMap<DomainName, Arc<AckRootTracker>, RandomState>,
    in_flight_by_ingestor: DashMap<DomainNodeRef, Arc<AckRootTracker>, RandomState>,
    generator_activity_by_domain: DashMap<DomainName, Arc<AtomicUsize>, RandomState>,
    emitter_buffers: DashMap<DomainNodeRef, Arc<AtomicUsize>, RandomState>,
    force_flush_by_domain: DashMap<DomainName, Arc<DomainForceFlush>, RandomState>,
    node_quiesce_counters: DashMap<DomainNodeRef, Arc<NodeQuiesceCounters>, RandomState>,
    /// Also held by the entity gate's deadline task, alongside `ingestors`.
    entity_gate_holds: Arc<DashMap<EntityGateHoldKey, EntityAlterHold, RandomState>>,
    /// Also held by the entity gate deadline task so a failed handoff resumes state timers when
    /// its lease expires.
    frozen_ownership_handoff_entities: Arc<DashMap<DomainNodeRef, (), RandomState>>,
    /// Also held by branch tasks waiting for a handoff freeze to end.
    ownership_handoff_freeze_changed: Arc<Notify>,
    /// Also held by every outstanding `DomainAlterGuard`, which clears its entry on drop.
    active_domain_alters: Arc<DashMap<DomainName, ActiveDomainAlter, RandomState>>,
    state_schema_fingerprints: DashMap<DomainNodeRef, [u8; 32], RandomState>,
    domain_graphs: DashMap<DomainName, SharedActiveGraph, RandomState>,
    endpoint_bindings: DashMap<HttpRouteKey, Vec<EndpointIngestBinding>, RandomState>,
    /// Instantiated endpoint routes keyed by the host and path an inbound request carries, so
    /// request routing never scans domain executions or their configured routes.
    routed_endpoints: DashMap<HttpRouteKey, RoutedEndpointsByDomain, RandomState>,
    relay_boundary_fanouts: RelayBoundaryFanoutMap,
    events: RuntimeEvents,
    /// The test harness keeps another handle to the same injected state and arms it while this
    /// node runs. Normal builds store a zero-sized marker here.
    fault_injection: ConfiguredFaultInjection,
    resource_store: RwLock<Option<Arc<ResourceStore>>>,
    resource_versions: RwLock<ResourceVersionStatus>,
    remote_dispatcher: RwLock<Option<Arc<RemoteDispatcher>>>,
    /// Also held by the attached `RemoteDispatcher`, which must allocate correlation ids from the
    /// same registry the runtime resolves incoming acknowledgements against.
    remote_dispatch: Arc<RemoteDispatchRegistry>,
    /// Cancels remote acknowledgement watchers once this runtime has drained its domain tasks.
    remote_ack_watcher_shutdown: CancellationToken,
    /// Owns acknowledgement progress tasks so none can retain an interconnect after shutdown.
    remote_ack_watcher_tasks: TaskTracker,
    state_checkpoint_notifications: DashMap<RuntimeStatePlacement, Arc<Notify>, RandomState>,
    pending_state_replica_syncs:
        DashMap<RuntimeStatePlacement, PendingStateReplicaSync, RandomState>,
    pending_state_checkpoint_announcements:
        DashMap<RuntimeStatePlacement, PendingStateCheckpointAnnouncement, RandomState>,
    /// Owns replica synchronization and checkpoint announcement work that outlives the event that
    /// scheduled it.
    state_replication_tasks: TaskTracker,
    passive_runtime_state_snapshots:
        DashMap<RuntimeStatePlacement, PersistedRuntimeStateEntry, RandomState>,
    replicated_branch_lru_snapshots:
        DashMap<RuntimeStatePlacement, PersistedRuntimeStateEntry, RandomState>,
    prepared_runtime_state_handoffs:
        DashMap<DomainNodeRef, PreparedRuntimeStateHandoff, RandomState>,
    activated_runtime_state_handoffs:
        DashMap<DomainNodeRef, ActivatedRuntimeStateHandoff, RandomState>,
    prepared_forced_runtime_state_recoveries:
        DashMap<DomainNodeRef, PreparedForcedRuntimeStateRecovery, RandomState>,
    prepared_runtime_state_snapshots:
        DashMap<RuntimeStatePlacement, PreparedRuntimeStateSnapshot, RandomState>,
    expiring_stream_states: DashMap<RuntimeStatePlacement, Arc<ExpiringRelayState>, RandomState>,
    latest_resource_versions: DashMap<DomainResourceKey, u64, RandomState>,
    replicated_deduplicator_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedDeduplicatorState>, RandomState>,
    replicated_kafka_offset_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedKafkaOffsetState>, RandomState>,
    replicated_materialized_stream_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedMaterializedRelayState>, RandomState>,
    relay_state_epochs: DashMap<DomainName, Arc<AtomicU64>, RandomState>,
    materialized_state_changed: Notify,
    replicated_window_processor_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedWindowProcessorState>, RandomState>,
    replicated_wasm_processor_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedWasmProcessorState>, RandomState>,
    replicated_branch_aggregated_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedBranchAggregatedState>, RandomState>,
    wasm_runtime: WasmRuntime,
    branch_instance_expiration_scan_interval: Duration,
    state_store: Option<Arc<RuntimeStateStore>>,
    state_snapshot_interval: Duration,
    state_replication_poll_interval: Duration,
    domain_drain_timeout: Duration,
    entity_gate_deadline: Duration,
    temp_dir: PathBuf,
    /// The node's bounded execution and transient-memory admission. Every variable-size encode,
    /// decode, validation and hash the runtime performs is submitted through it, so none of them
    /// occupies an async worker and none of them allocates before it is charged.
    executor: Executor,
    metrics: RuntimeMetrics,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

mod tls;
mod vm_compile;
mod vm_input;
mod wasm_output;
mod wasm_processor;
mod wasm_state;
mod websocket_signaling;
mod window_processor;
mod window_state;
