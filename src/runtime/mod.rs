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
mod error;
mod events;
mod fault_injection;
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
mod materialized_snapshot;
mod materialized_state;
mod message_error;
mod message_error_delivery;
mod node;
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
mod snapshot_staging;

mod schedule_delta;
mod scheduled_node;
mod service_url;
mod shared_clients;
mod state_replication;
mod state_snapshot_exchange;
mod state_snapshot_transfer;
mod state_store;
mod syslog;
#[cfg(test)]
mod test_fixtures;

use std::str::FromStr;

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
use branch_key::branch_key_display;
use branch_lru_state::{decode_branch_lru_snapshot, encode_branch_lru_snapshot};
use branch_runtime::{
    BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL, BranchRuntime, IngestorRouteRuntime,
    MaterializedBatchWaitContext, PendingMaterializedBatch, branch_lru_placement,
    flush_branch_junction, internal_processor_error_policies, output_error_policies,
    persist_branch_instance_lru_snapshot,
};
use client_config::{
    ParsedRetryPolicy, client_config_entries, client_config_value, client_tls_paths,
    next_retry_delay, optional_bool_client_config_value, optional_client_config_value,
    read_tls_file, render_client_config_template,
};
use correlator::{
    CorrelatorMatchedBatch, CorrelatorOutputCompileContext, CorrelatorOutputContext,
    CorrelatorSide, CorrelatorTimeoutContext, compile_correlator_where_program,
    correlate_incoming_message, enqueue_correlator_output, evaluate_correlator_output_batch,
    handle_correlator_timeout_action,
};
use deduplicator::{
    CompiledDeduplicatorKeyProgram, DeduplicatorKey, ReplicatedDeduplicatorState,
    compile_deduplicator_key_program,
};
use domain_clock::{
    DomainCadenceOccurrence, DomainCadenceStart, DomainClock, DomainClockAccessResult,
    DomainClockLifecycle, LogicalDeadline, checked_add_duration_to_timestamp, current_timestamp,
    wait_for_branch_deadline,
};
use domain_execution::{
    DomainExecution, DomainResourceKey, ObservedDomainTick, RuntimeDomainState,
};
use domain_rebuild::{branch_relays_from_branched_specs, relay_branching_schema_for_runtime};
use domain_wire_schemas::DomainWireSchemas;
use emitter_supervision::{
    EmitterRetryKind, EmitterRetryStatus, EmitterTaskCommand, ScheduledEmitterTask,
    clear_emitter_stop_signal,
};
use endpoint::{
    EndpointIngestBinding, EndpointRoute, HttpRouteKey, RoutedEndpoint, RoutedEndpointsByDomain,
};
pub(in crate::runtime) use entity_gate::OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL;
use entity_gate::{
    ActiveDomainAlter, BranchQuiesceGauges, DomainActivityGuard, EntityAlterHold,
    EntityGateHoldKey, NodeQuiesceCounters, NodeQuiesceWorkGuard,
};
pub(in crate::runtime) use events::RuntimeEvents;
pub(in crate::runtime) use filter_map::evaluate_sqs_fifo_group_program;
use filter_map::{
    FilterMapBatchInputs, FilterMapOutcomeInputs, InferencerFilterMapTensors, VmUninitializedInput,
    append_filter_map_nested_value, evaluate_filter_map_on_batch, evaluate_output_branch_program,
    execute_filter_map_program_on_batch, expression_reads_sensitive_source,
    plan_emitter_filter_map_batch, plan_filter_map_messages,
};
use force_flush::{DomainForceFlush, DomainForceFlushCompletion, DomainForceFlushParticipant};
use generator::{GeneratorTaskRouteSpec, GeneratorTaskSpec};
use http_client::HttpClientConfig;
use inferencer_output::flush_branch_inferencer_output;
use ingest_group::{
    BranchedEntrypointInput, IngestGroupDispatch, IngestRouteCollector, IngestorDependencies,
    IngestorRouteRuntimes, RawIngestDispatch, branched_branch_filter_blocking,
    branched_branch_plan_blocking, branched_entrypoint_batch_from_inputs_blocking,
    decode_ingested_payload,
};
pub(in crate::runtime) use ingest_group::{
    INGEST_FLUSH_FAILURES_ARE_HANDLED, INGEST_GROUP_MAX_ROWS,
};
use ingest_metadata::{
    BRANCH_NAMESPACE, INGEST_METADATA_NAMESPACE, IngestHeaderFunctionInjector,
    IngestMetadataBuilders, emit_sink_supports_headers, ingest_source_supports_headers,
};
pub(in crate::runtime) use ingest_metadata::{
    IngestMetadataKind, IngestMetadataRow, NoIngestHeaders,
};
pub(in crate::runtime) use ingestor_quiesce::{
    BufferedIngestMetadata, BufferedIngestPayload, IngestorQuiesceCause, IngestorQuiesceControl,
    IngestorQuiesceIntake,
};
use ingestor_quiesce::{
    DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL, IngestorReadiness, IngestorRuntime,
    RuntimeReconnectStatus,
};
use ingestor_start_plan::*;
use kafka_offset_state::{
    KafkaOffsetSnapshotInstaller, KafkaOffsetStateAssignment, KafkaOffsetStateOriginator,
    KafkaOffsetStatePersistence, KafkaOffsetStateRead, KafkaTopicPartition,
    ReplicatedKafkaOffsetState,
};
use lookup_hash_map::{
    LookupHashMapCall, LookupHashMapCallKey, collect_program_field_refs,
    compile_lookup_hash_map_calls, rewrite_lookup_hash_map_program,
};
use materialized_snapshot::{
    MaterializedGenerationRecord, RestoredMaterializedSnapshot, SealedSource,
    empty_sealed_container, inspect_sealed_container,
};
use materialized_state::{
    MaterializedRelaySnapshotInstaller, MaterializedRelayStateAssignment,
    MaterializedRelayStateOriginator, MaterializedRelayStatePersistence,
    ReplicatedMaterializedRelayState,
};
use message_error::{
    MessageErrorCompileSchemas, MessageErrorFailure, MessageErrorHandling,
    MessageErrorSourceContext, SingleRecordFilterMapOutcome, captured_partial_output,
    invalid_output_fields, operation_for_filter_label, planned_structured_message_error,
    structured_message_error, vm_partial_output_row_to_runtime_batch,
};
use message_error_delivery::{
    MessageErrorDelivery, MessageErrorRouteKey, MessageErrorRouteRuntime, MessageErrorRouteTarget,
    matching_message_error_output,
};
use nervix_models::{
    CreateAvroWireSchema, CreateCborWireSchema, CreateJsonWireSchema, DeduplicatorName,
    ReingestorName, ResolvedCodecWireFormat, SchemaName, WireSchemaLookup, WireSchemaName,
};
pub(in crate::runtime) use node::{RuntimeInner, SharedActiveGraph};
#[cfg(test)]
use planning::PlannedModel;
use planning::{
    branched_node_specs_from_active_graph, branched_node_specs_from_scheduled_nodes,
    format_branched_by, materialize_ingestor_route_template,
    materialize_processor_instance_template, processor_template_for_graph_node,
};
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
use rdkafka::consumer::StreamConsumer;
pub(in crate::runtime) use reconnect_backoff::RuntimeReconnectBackoff;
use reingestor::ReingestorInputSpec;
pub(in crate::runtime) use relay_batch::RelayDispatchResult;
use relay_batch::build_stream_record_batch_preserving_acks;
use relay_boundary::{
    ConcreteRelayRuntime, ConcreteRelayRuntimeBuild, ExpiringRelayState, RelayBoundaryBuilder,
    RelayBoundaryFanout, RelayBoundaryFanoutMap, RelayBoundaryServices, RelayOutboundSlot,
    RelayOwnerTask, RelayRegistry, RelayRetention, RelayRuntimeFanIn, RelayStateTask,
    RelayStateTaskSpec, RemoteRuntimeConsumer, addressable_count,
};
pub(in crate::runtime) use relay_channel::{RelayDispatchGate, RelayDispatchGateLease};
use relay_interaction::{
    RelayInteraction, RelayInteractionCommand, RelayInteractionError, RelayInteractionEvent,
    RelayInteractionInput,
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
use snapshot_staging::{SnapshotStaging, SnapshotStagingLimits};
use state_replication::{
    ActivatedRuntimeStateHandoff, DEFAULT_STATE_REPLICATION_POLL_INTERVAL,
    DEFAULT_STATE_SNAPSHOT_INTERVAL, PendingStateCheckpointAnnouncement, PendingStateReplicaSync,
    PreparedForcedRuntimeStateRecovery, PreparedRuntimeStateHandoff, PreparedRuntimeStateSnapshot,
};
pub(in crate::runtime) use state_store::{
    ForcedRuntimeStateRecoveryTransition, RuntimeStateHandoffTransition, RuntimeStateKind,
    RuntimeStateOperationError, RuntimeStateResult, RuntimeStateStore, StateAssignmentAuthority,
    StateAssignmentToken, StateAuthorityError, StateCapability, StateReplicationRoles,
};
#[cfg(test)]
pub(in crate::runtime) use test_fixtures::STUPID_CHANNEL_CAPACITY_REMOVE_ME;
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
pub(in crate::runtime) use vm_compile::{
    CompiledBranchProgram, CompiledEmitterFilterMapProgram, EmitterHeaders,
    MaterializedFieldInterest, MaterializedLookupKeyMode, compile_emitter_filter_map_program,
    compile_key_projection_program, compile_sqs_fifo_group_program,
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
pub(in crate::runtime) use websocket_signaling::SignalingProtobufDescriptors;
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

mod tls;
mod vm_compile;
mod vm_input;
mod wasm_output;
mod wasm_processor;
mod wasm_state;
mod websocket_signaling;
mod window_processor;
mod window_state;

pub(crate) use branch_key::BranchKey;
pub(crate) use client_config::{ClientResourceMounts, ResolvedClientConfig};
pub(crate) use domain_clock::DomainExecutionSnapshot;
pub(crate) use domain_execution::LookupRuntime;
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

/// What the data plane exposes. Everything else this module and its submodules declare is
/// `pub(in crate::runtime)` or narrower, so the layers above reach the runtime only through the
/// names below.
pub use entity_gate::{DEFAULT_DOMAIN_DRAIN_TIMEOUT, branch_task_stop_timeout};
pub(crate) use entity_gate::{
    EmitterPublishingDrainState, EmitterPublishingDrainStatus, EntityGateLease,
};
pub(crate) use error::RuntimeError;
pub(crate) use events::RuntimeEvent;
pub(crate) use filter_map::execute_filter_map_on_record;
pub(crate) use ingest_metadata::{
    IngestFilterMapMetadata, IngestMessageHeaders, RetainedIngestHeaders,
};
pub(crate) use ingestor_quiesce::IngestorQuiesceCounters;
pub(crate) use ingestors::kafka::KafkaIngestor;
pub(crate) use materialized_state::MaterializedRecordReport;
pub use node::{DEFAULT_TEMP_DIR, Runtime};
pub(crate) use observability::{IngestorDescribe, KafkaDomainOffsetDescribe};
pub(crate) use ownership_handoff_error::{OwnershipHandoffError, OwnershipHandoffResult};
pub(crate) use relay_batch::{RelayMessage, RelayRecordBatch};
pub(crate) use relay_boundary::scheduled_relay_owner_nodes;
pub(crate) use relay_channel::{
    RelayBroadcast, RelayReceiver as RelaySubscriptionReceiver, RelaySubscriptionRecvError,
};
pub(crate) use state_replication::StateSyncAck;
pub(crate) use state_snapshot_transfer::{
    DescribeStateSnapshot, DescribedStateSnapshot, FetchStateSnapshot,
};
pub(crate) use state_store::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
};
pub(crate) use vm_compile::{
    CompiledDomainUdfs, CompiledProgramWithMaterializedInterest, MaterializedProgramInterest,
    RuntimeMaterializedRelaySpec, RuntimeVmCompileContext, compile_session_filter_map_program,
};
pub(crate) use websocket_signaling::{
    CompiledSignalingProtocol, SignalingDataSink, WebsocketSignalingSession,
};
