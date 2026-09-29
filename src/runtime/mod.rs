//! What one node executes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Relay boundaries and their fan-out, branch-local processor tasks, the host that
//!   drives ingestor and emitter connectors, compiled programs, materialized state, deduplication,
//!   reordering, windows, correlation, generators, in-flight acknowledgement tracking, node-owned
//!   state persistence and replication, and the entities that bind listening ports.
//! - **Depends on.** The engines — the VM, the UDF and WASM hosts, codecs, the connector crates,
//!   the interconnect, the state and resource stores — vocabulary and typed execution plans.
//! - **Must not know.** NSPL text, transactions, the gRPC surface or consensus. It is told what to
//!   run and runs it.
//!
//! Schedule application, processor tasks, entrypoints, emitters and message-error delivery consume
//! complete typed revisions and prepared plans. This module also holds connector host composition.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::Arc as StdArc,
};

use ahash::{HashMap, HashMapExt, HashSet, RandomState};
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
#[cfg(test)]
use chrono::TimeZone as _;
use error_stack::Report;
use fjall::Database;
use futures_util::{future::BoxFuture, stream::FuturesUnordered};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use nervix_dns::DnsResolver;
use nervix_execution::{ChargedBytes, Executor};
use nervix_interconnect::{
    EntityGatePurpose, Envelope, InterconnectRequest, RelayAdmission, RelayAdmissionDecision,
    RelayAdmissionStatus, RelayCancellationGuard, RelayDelivery, RelayPayload, RelayPayloadKind,
    Transport, WasmStateResetTarget,
};
use nervix_models::{
    AckMode, AtomicTimestamp, BranchKeyFingerprint, BranchName, ClientConfigEntry, ClientName,
    ClientProducerEndReason, ClientResourceMount, ClusterNodeIncarnation, ClusterNodeName,
    CodecName, CommandExecutionReference, CoordinationIdentity, CorrelationTimeoutAction,
    CorrelatorMatchPolicy, DomainClockAuthority, DomainConfig, DomainName, DomainNodeRef,
    DomainState, EmitterName, EndpointName, EndpointType, ErrorPolicies, FieldName, FieldPath,
    FlushPolicy, GeneralErrorPolicy, GeneratorName, InferencerExecutionMode,
    InferencerTensorDeclaration, IngestQuiesceMode, IngestQuiesceOverflow, IngestTimestampSource,
    IngestorName, KafkaPartitionSchedule, Literal as ModelLiteral, LookupName,
    MaterializedStatePolicy, MessageErrorCode, MessageErrorOperation, MessageErrorPolicy,
    ModelKind, ModelName, NodeRef, OwnershipStateComponent, OwnershipStateRecoveryOutcome,
    OwnershipStateReset, OwnershipStateResetCause, ParseAsType, RelayName, RemoteAckOutcome,
    RemoteAckRegistration, RemoteAckResolution, RemoteRuntimeField, ResolvedBranching, ResourceId,
    ResourceName, RetryPolicy, RouteConstruction, SchemaFingerprint, SignalingProtocolName,
    SignalingWireFormat, StructuredMessageError, SubscriptionName, Timestamp,
    WasmCheckpointInspection, WasmRejectedStatePolicy, WasmSavedStateRejection,
    WasmStateGeneration, WasmStateResetScope,
};
#[cfg(test)]
use nervix_models::{
    ClusterSchedule, CreateIngestor, CreateReingestor, CreateRelay, DomainSchedule, IngestSource,
    Model, OutputBranch, ScheduledNode, ScheduledNodes,
};
#[cfg(test)]
use nervix_models::{
    CreateClientHttp, CreateClientPrometheus, CreateClientRabbitMq, CreateEmitter,
    EmitterPublishingMode,
};
use nervix_primitives::{
    collections::DashMap,
    publication::{ArcSwap, ArcSwapOption, Cache},
    stream::StreamExt,
    sync::{
        CancellationToken, Mutex, Notify,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        broadcast, mpsc, oneshot, watch,
    },
    task::{AbortOnDropHandle, JoinHandle, TaskTracker},
};
use nervix_recovery::{Discarded as _, NoReceiver as _};
use nervix_roto::{UdfExecutor, UdfProgram};
#[cfg(test)]
use nervix_vm::SPAWN_BLOCKING_ROW_THRESHOLD as VM_SPAWN_BLOCKING_ROW_THRESHOLD;
use nervix_vm::{
    CompileBinding as VmCompileBinding, CompileNamespace as VmCompileNamespace,
    CompileOptions as VmCompileOptions, CompiledPredicate as VmCompiledPredicate,
    CompiledProgram as VmCompiledProgram, ExecutionContext as VmExecutionContext,
    FunctionInjector as VmFunctionInjector, OutputMode as VmOutputMode,
    PredicateCompileOptions as VmPredicateCompileOptions, SchemaSensitivity as VmSchemaSensitivity,
    SemanticScopePolicy, TypedArray as VmTypedArray, TypedBatch as VmTypedBatch,
    UdfSignatures as VmUdfSignatures,
    compile_predicate_with_options_for_bindings as compile_vm_predicate_with_options_for_bindings,
    compile_program_with_options_for_bindings_with_sensitivity as compile_vm_program_with_options_for_bindings_with_sensitivity,
    execute_predicate_in_context as execute_vm_predicate_in_context,
    execute_program_with_selection_in_context,
    infer_set_expr_types_for_bindings_with_udfs as infer_vm_set_expr_types_for_bindings_with_udfs,
    lower_finalized_output_filter, lower_generated_route, lower_route_construction,
    lower_transforming_route,
    program::{
        CaseArm, Expr, FunctionName, InternalFieldNamespace, InternalFieldRef, Literal,
        Span as VmSpan, SpannedExpr,
    },
    window::{
        CompiledWindowDemand, CompiledWindowExpr, CompiledWindowRoute, WINDOW_ARGUMENT_NAMESPACE,
        WindowAggregateFunction, WindowAggregateInvocation, WindowAggregateProgram,
        WindowAggregateStorageKind, WindowArgumentColumn, WindowArguments,
        WindowLinearHistogramConfig, WindowPaneLayout, WindowSketchConfig,
        lower_window_assignments,
    },
};
use nervix_wasm::{
    WasmAckSidecar, WasmAckToken, WasmAckTokenSet, WasmBranchInit, WasmEnvelope,
    WasmOutputColumnRef, WasmOutputRow, WasmRoutedOutput, WasmRuntime, WasmRuntimeConfig,
};
use ordered_float::OrderedFloat;
use sorted_vec::SortedSet;
use thiserror::Error;
use tokio::{
    io::AsyncBufReadExt,
    time::{Duration, Instant, sleep, sleep_until},
};
use tracing::{debug, error, info, trace, warn};
use triomphe::Arc;
use upon::Engine as TemplateEngine;

#[cfg(test)]
use crate::registry::ActiveGraph;
#[cfg(test)]
use crate::registry::MessageErrorRouteSpec;
#[cfg(test)]
use crate::runtime_schema::test_runtime_row;
use crate::{
    ConfiguredFaultInjection, cluster,
    emitter_execution_plan::{EmitterExecutionPlan, EmitterOrderingGroupPlan, EmitterRoutePlan},
    emitter_start_plan::*,
    metrics::{
        BatchMetricsHandle, BranchEvictionReason, ClientIngestorSeries,
        IngestorQuiesceMetricLabels, MessageMetricsHandle, NodeBatchMetricsSpec,
        NodeInputMetricsHandle, RelayMetricRecorders, RelayMetricsHandle, RuntimeMetrics,
        RuntimeMetricsSnapshot,
    },
    registry::{
        BranchInstanceAckBoundary, BranchedNodeSpecs, BranchedProcessorNodeSpec,
        BranchedProcessorOperationSpec, BranchedProcessorOutputSpec, BranchedProcessorOutputsSpec,
        BranchedProcessorSpec, ClientIngestorStartPlan, DomainActivationPlan,
        DynamicExecutionUpdate, EndpointIngestorStartPlan, EntitySwapExecution, EntrypointPlans,
        ExecutionDelta, ExecutionNode, ExecutionRevision, GeneratorExecutionPlan,
        GeneratorRoutePlan, HttpIngestorStartPlan, IngestorInputPlan, IngestorSpec,
        IngestorStartPlan, KafkaDomainOffsetPlacement, KafkaIngestorStartPlan, KafkaOffsetPlan,
        LookupResourcePlan, LoweredConstruction, MessageErrorCompileSchemas, MessageErrorRouteKey,
        MessageErrorRouteSpecs, MqttIngestorStartPlan, NatsIngestorStartPlan,
        PlannedClusterRevision, PlannedCodec, PlannedCodecWireFormat, PlannedEntryRoute,
        PlannedRouteBranch, PlannedSignalingProtocol, PrometheusIngestorStartPlan,
        PulsarIngestorStartPlan, RabbitMqIngestorStartPlan, RedisPubSubIngestorStartPlan,
        ReingestorInputPlan, ReingestorPlan, SourceStartPlan, SqsIngestorStartPlan,
        SyslogIngestorStartPlan, TransportInputPlan, WasmModulePlan, WebsocketsIngestorStartPlan,
        ZeroMqIngestorStartPlan,
    },
    resource::ResourceStore,
    runtime_ack::{
        AckCompletion, AckOutcome, AckProgress, AckRequiredWaitGuard, AckRootTracker, AckSet,
    },
    runtime_schema::{
        CodecError, CompiledCodec, CompiledSchema, JsonDecoder, ProtobufCodecDescriptors,
        ProtobufDescriptorPool, RuntimeProjectionComponent, RuntimeRecordBatch,
        RuntimeRecordBatchBuilder, RuntimeRecordMetadata, RuntimeRow, RuntimeSchemaError,
        RuntimeSchemaOperation, RuntimeValue, RuntimeValueColumn, RuntimeValueLocation,
        RuntimeVmOperation, compile_codec_spec_with_protobuf, compile_schema, decode_with_codec,
        parse_as_type_from_arrow, runtime_value_from_arrow_array,
    },
    task_shutdown::JoinShutdown as _,
};

mod branch_aggregated_state;
mod branch_buffering;
mod branch_instance_registry;
mod branch_key;
mod branch_lru_state;
mod branch_runtime;
mod client_ingestor;
mod correlator;
mod deduplicator;
mod domain_clock;
mod domain_execution;
mod domain_rebuild;
mod emitter_batch_packing;
mod emitter_buffer;
mod emitter_encoding;
mod emitter_http_requests;
mod emitter_ordering_group;
mod emitter_publishing;
mod emitter_record_writes;
mod emitter_retry;
mod emitter_sinks;
mod emitter_supervision;
mod emitter_task;
mod emitter_values;
mod endpoint;
mod entity_gate;
mod entrypoint_routes;
mod error;
mod events;
mod fault_injection;
mod filter_map;
mod force_flush;
mod forced_recovery_decision;
mod generator;
mod http_request_fields;
mod inferencer;
mod inferencer_output;
mod ingest_group;
mod ingest_metadata;
mod ingestion_time;
mod ingestor_quiesce;
mod ingestor_start;
mod ingestors;
mod kafka_offset_state;
mod local_drain;
mod lookup_hash_map;
mod lsm_sequence;
mod materialized_read;
mod materialized_snapshot;
mod materialized_state;
mod message_error;
mod message_error_delivery;
mod message_error_plan;
mod node;
mod node_settings;
mod observability;
mod ownership_handoff_error;
mod planning;
mod pooled_sink_clients;
mod processor_branch_task;
mod processor_output;
mod processor_plan_binding;
mod processor_template;
mod processors;
mod published_generation;
mod reconnect_backoff;
mod record_metadata;
mod reingestor;
mod relay_batch;
mod relay_boundary;
mod relay_channel;
mod relay_interaction;
#[cfg(feature = "benchmarks")]
#[doc(hidden)]
pub mod relay_interaction_benchmark;
mod relay_processor_node;
mod relay_subscription;
mod remote_dispatch;
mod reorderer;
mod resources;
mod runtime_lifecycle;
mod schedule_apply;
mod snapshot_staging;

mod scheduled_node;
mod shared_clients;
mod state_replication;
mod state_snapshot_exchange;
mod state_snapshot_transfer;
mod state_store;
mod subscription_predicate;
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
use branch_instance_registry::{
    BranchInstanceRegistry, BranchInstanceSnapshotEntry, GetOrCreateBranchInstance,
};
use branch_key::branch_key_display;
use branch_lru_state::{
    BranchLruSnapshotError, decode_branch_lru_snapshot, encode_branch_lru_snapshot,
};
use branch_runtime::{
    BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL, BranchRuntime, BranchRuntimeMetrics,
    IngestorRouteRuntime, MaterializedBatchWaitContext, MaterializedDomainHandles,
    PendingMaterializedBatch, branch_lru_placement, flush_branch_junction,
    internal_processor_error_policies, persist_branch_instance_lru_snapshot,
    publish_branch_instance_lru_snapshot,
};
pub(crate) use client_ingestor::{
    ClientIngestorGauges, ClientProducerEvent, ClientProducerEvents, ClientProducerHandle,
    ClientProducerOpenRequest, ClientProducerReservation, ClientProducerRetention,
    ClientSubmissionId, OpenedClientProducer,
};
use correlator::{
    CorrelatorMatchedBatch, CorrelatorOutputCompileContext, CorrelatorOutputContext,
    CorrelatorSide, CorrelatorTimeoutContext, compile_correlator_where_program,
    correlate_incoming_message, enqueue_correlator_output, evaluate_correlator_output_batch,
    handle_correlator_timeout_action,
};
use deduplicator::{
    CompiledDeduplicatorKeyProgram, DeduplicatorKey, DeduplicatorKeyspace,
    ReplicatedDeduplicatorState, compile_deduplicator_key_program,
};
use domain_clock::{
    DomainCadenceOccurrence, DomainCadenceStart, DomainClock, DomainClockAccessResult,
    DomainClockLifecycle, LogicalDeadline, checked_add_duration_to_timestamp,
    wait_for_branch_deadline,
};
#[cfg(test)]
use domain_execution::DomainRouting;
use domain_execution::{
    DomainExecution, DomainRoutingError, DomainRoutingSnapshot, ObservedDomainTick,
    RuntimeDomainState,
};
pub(crate) use domain_execution::{DomainRoutingCache, SharedDomainRouting};
use domain_rebuild::branch_relays_from_plans;
use emitter_buffer::{
    DeliveredAcknowledgements, EmitterBatchBuffer, EmitterBufferedMessages, EmitterPublication,
    EmitterPublishBatch, PublishReport, RowToPack,
};
use emitter_encoding::EncodedRecordSink;
use emitter_ordering_group::{CompiledOrderingGroup, OrderingGroupError, OrderingGroups};
use emitter_publishing::{
    EmitterPublishBatchOwner, EmitterPublishControl, EmitterPublishFailure, EmitterPublishResult,
    EmitterSink, EmitterSinkState, RejectedEmitterRecord, RejectedRecordInput,
    await_emitter_confirmation, emitter_unavailable_reason, finish_rejected_records,
    sink_publish_failure,
};
use emitter_record_writes::{
    CheckedPreparation, EncodedPayload, PreparedHttpRequest, PreparedPayload, PreparedPayloads,
    PreparedWrite, RowAnswers, RowPreparationViolation, RowRecords, RowRequestBody,
};
use emitter_retry::{
    EmitterAcknowledgements, EmitterRetryDeferral, EmitterRetrySchedule, RETRY_ACK_ALIVE_EACH,
    emitter_retry_delay,
};
use emitter_sinks::EmitterSinkStarter;
use emitter_supervision::{
    EmitterRetryKind, EmitterRetryStatus, EmitterTaskCommand, ScheduledEmitterTask,
    clear_emitter_stop_signal,
};
use emitter_task::{
    EmitterRuntimeError, EmitterRuntimeResult, EmitterSinkContext, emitter_error_message,
    emitter_init_error, emitter_publish_error_is_retryable, emitter_report,
};
use emitter_values::{
    MappedRequestSink, MappedRowSink, MappedValuesProjection, MappedValuesProjectionInit,
};
use endpoint::{
    EndpointIngestBinding, EndpointRoute, HttpRouteKey, RoutedEndpoint, RoutedEndpointsByDomain,
};
pub(in crate::runtime) use entity_gate::OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL;
use entity_gate::{
    ActiveDomainAlter, BranchQuiesceGauges, DomainActivityGuard, EntityGateOperation,
    NodeQuiesceCounters, NodeQuiesceWorkGuard, OutputBufferQuiesceGauge,
    OwnershipHandoffFreezeWatch,
};
pub(crate) use entrypoint_routes::EntrypointBindingError;
use entrypoint_routes::{
    BoundEntryRoute, BoundIngestorRoutes, BoundReingestorInput, BoundReingestorRoute,
    BoundRouteBranch, RelayRuntimeHandles,
};
pub(in crate::runtime) use events::RuntimeEvents;
use filter_map::{
    ExecutedFilterMap, FilterMapBatchInputs, FilterMapOutcomeInputs, InferencerFilterMapTensors,
    VmUninitializedInput, append_filter_map_nested_value, evaluate_filter_map_on_batch,
    evaluate_output_branch_program, execute_filter_map_program_on_batch,
    expression_reads_sensitive_source, plan_emitter_filter_map_batch, plan_filter_map_messages,
};
use force_flush::{
    DomainForceFlush, DomainForceFlushCompletion, DomainForceFlushParticipant,
    IngestorAckRootTrackers,
};
use generator::GeneratorTaskSpec;
use http_request_fields::{
    AcceptedHttpRequests, AdmittedHttpRequests, CompiledHttpRequestFields, HttpRequestFields,
    HttpRequestInput, HttpRequestSchemas, SourceRecords,
};
use inferencer_output::flush_branch_inferencer_output;
pub(in crate::runtime) use ingest_group::INGEST_GROUP_MAX_ROWS;
use ingest_group::{
    BoundIngestor, BoundIngestorInput, BranchedEntrypointInput, ClientBatchDispatch,
    IngestGroupDispatch, IngestRouteCollector, IngestorDependencies, IngestorRouteRuntimes,
    RawIngestDispatch, branched_branch_filter_blocking, branched_branch_plan_blocking,
    branched_entrypoint_batch_from_inputs_blocking, decode_ingested_payload,
};
pub(in crate::runtime) use ingest_metadata::IngestMetadataKind;
use ingest_metadata::{
    BRANCH_NAMESPACE, INGEST_METADATA_NAMESPACE, IngestHeaderFunctionInjector,
    IngestMetadataBuilders,
};
pub(in crate::runtime) use ingestor_quiesce::{
    BufferedIngestMetadata, BufferedIngestPayload, IngestorQuiesceCause, IngestorQuiesceControl,
    IngestorQuiesceIntake,
};
use ingestor_quiesce::{
    DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL, IngestorReadiness, RuntimeReconnectStatus,
};
use ingestor_start::IngestorRuntime;
use kafka_offset_state::{
    KafkaOffsetSnapshotInstaller, KafkaOffsetStateAssignment, KafkaOffsetStateOriginator,
    KafkaOffsetStatePersistence, KafkaOffsetStateRead, ReplicatedKafkaOffsetState,
};
use local_drain::LocalIntake;
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
    MessageErrorFailure, MessageErrorHandling, MessageErrorSourceContext,
    SingleRecordFilterMapOutcome, captured_partial_output, finalized_partial_output,
    invalid_output_fields, planned_structured_message_error, structured_message_error,
    vm_partial_output_row_to_runtime_batch,
};
use message_error_delivery::{
    MessageErrorDelivery, MessageErrorRouteRuntime, MessageErrorRouteTarget,
};
use message_error_plan::{
    BoundMessageErrorRoute, BoundMessageErrorRoutes, MessageErrorRouteBindingContext,
};
use nervix_connector_kafka::KafkaOffsetPosition;
use nervix_models::{DeduplicatorName, ReingestorName};
pub(in crate::runtime) use node::RuntimeInner;
use planning::{
    ProcessorPlanBindingContext, bind_published_processor_plans,
    materialize_ingestor_route_template, parse_branch_flush_policy, parse_input_collect_policy,
};
use processor_branch_task::{
    PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, ProcessorBranchHandoff, ProcessorNodeCommand,
    ProcessorSnapshotRequest, SpawnedSnapshotTask,
};
pub(in crate::runtime) use processor_branch_task::{
    ProcessorRuntimeContext, spawn_processor_node_runtime,
    spawn_processor_node_runtime_with_handoffs,
};
use processor_output::{
    PendingProcessorOutputBatch, PendingProcessorOutputMessageError, ProcessorMaterializedState,
    ProcessorOutputBatchScope, ProcessorOutputDispatchContext, dispatch_processor_output,
    dispatch_processor_outputs, flush_all_processor_outputs, flush_due_processor_outputs,
    pending_output_batches_by_key,
};
use processor_template::{
    MaterializedDependencyResolution, ProcessorInputFilterKind, ProcessorTemplateError,
    wasm_guest_call_schemas, wasm_instance_next_deadline,
};
use processors::{
    BranchInstanceTemplate, CompiledCorrelatorOutputProgram, CompiledCorrelatorWhereProgram,
    CompiledInferencerInputProgram, CompiledReordererProgram, CompiledWindowAggregateProgram,
    CorrelatorBranchState, CorrelatorPendingMessage, FilterMapPlan, InferencerFlushContext,
    InferencerOutputBuffer, IngestorRouteTemplate, JunctionFlushContext, PlannedGeneralError,
    PlannedGeneralResult, PlannedMessageError, ProcessorCompileError, ProcessorLiveStateError,
    ProcessorMaterializedError, ProcessorPlanRevision, PublishedProcessorPlan, RelayProcessorNode,
    RelayProcessorOperationNode, RelayProcessorOperationTemplate, RelayProcessorOutputNode,
    RelayProcessorOutputTemplate, RelayProcessorOutputsNode, RelayProcessorOutputsTemplate,
    RelayProcessorRelayTemplate, RelayProcessorTemplate, ReorderKeyPart, ReordererOutputBuffer,
    ReordererRowOrder, WasmAckContext, WasmAckMap, WasmCompiledBranchProcessor, WasmFlushContext,
    WindowBounds, WindowFlushContext,
};
pub(in crate::runtime) use reconnect_backoff::{AcknowledgementKeepalive, RuntimeReconnectBackoff};
use record_metadata::RecordMetadataColumns;
use reingestor::{PlannedReingestorInput, ReingestorInputConsumer, ReingestorRuntimes};
pub(in crate::runtime) use relay_batch::RelayDispatchResult;
use relay_batch::build_stream_record_batch_preserving_acks;
use relay_boundary::{
    BranchRelayDispatchGateLease, ConcreteRelayRuntime, ConcreteRelayRuntimeBuild,
    ExpiringRelayState, RelayBoundaryBuilder, RelayBoundaryFanout, RelayBoundaryFanoutMap,
    RelayBoundaryServices, RelayOutboundSlot, RelayOwnerTask, RelayRegistry, RelayRetention,
    RelayRuntimeFanIn, RelayStateTask, RelayStateTaskSpec, RemoteRuntimeConsumer,
    addressable_count,
};
pub(in crate::runtime) use relay_channel::{
    OwnedRelayDispatchPermit, RelayDispatchGate, RelayDispatchGateLease, RelayTryRecv,
};
use relay_interaction::{
    RelayInteraction, RelayInteractionCommand, RelayInteractionError, RelayInteractionEvent,
    RelayInteractionInput,
};
use remote_dispatch::{REMOTE_ACK_ALIVE_INTERVAL, RemoteDispatchRegistry, RemoteDispatcher};
use reorderer::{ReordererFlushContext, flush_branch_reorderer_output, reorder_key_part};
use schedule_apply::ScheduleApplication;
use scheduled_node::{
    EmitterTaskBuildDeps, EmitterTaskDeps, ExecutionBuildDeps, PlacedNodeState,
    ScheduledNodePlacement, ScheduledNodeTask,
};
pub(in crate::runtime) use shared_clients::{SharedClientError, SharedClientLease};
use snapshot_staging::{SnapshotStaging, SnapshotStagingLimits};
pub(crate) use snapshot_staging::{
    SnapshotStagingError, StagedArtifact, StagedArtifactReader, StagedSnapshotWriter,
};
use state_replication::{
    ActivatedRuntimeStateHandoff, DEFAULT_STATE_REPLICATION_POLL_INTERVAL,
    DEFAULT_STATE_SNAPSHOT_INTERVAL, PendingStateCheckpointAnnouncement, PendingStateReplicaSync,
    PreparedForcedRuntimeStateRecovery, PreparedRuntimeStateHandoff, PreparedRuntimeStateSnapshot,
    PublishedBranchState,
};
pub(in crate::runtime) use state_store::{
    ForcedRuntimeStateRecoveryAuthorization, ForcedRuntimeStateRecoveryIdentity,
    ForcedRuntimeStateRecoveryTransition, RuntimeState, RuntimeStateHandoffTransition,
    RuntimeStateKind, RuntimeStateOperationError, RuntimeStateResult, RuntimeStateStore,
    ScheduledStateIdentity, StateAssignmentAuthority, StateAssignmentToken, StateAuthorityError,
    StateCapability, StateIdentityError, StateReplicationRoles,
};
#[cfg(test)]
pub(in crate::runtime) use test_fixtures::STUPID_CHANNEL_CAPACITY_REMOVE_ME;
#[cfg(test)]
use test_fixtures::{
    EntrypointTestDomain, OptionalTestField, TWO_ITEM_TEST_CHANNEL_CAPACITY, TestIngestHeaders,
    attach_loopback_cluster, batch_value, bind_ingestor_route_for_test, branch_model, branched_by,
    concrete_branch_key, construction, domain, execute_filter_map_for_test, expression,
    ingest_metadata_for_test, install_test_domain_execution, install_unpaced_test_domain,
    junction_branch_template, key_label, named, nonzero_capacity, paced_domain_state,
    planned_entrypoints_for_test, processor_branched_by, publish_state_identity,
    quiesce_test_batch, row_value, scheduled_model, string_branch_key, test_branching,
    test_domain_clock, test_domain_clock_authority, test_execution_revision,
    test_ingestor_quiesce_control, test_named_branching, test_optional_schema,
    test_relay_boundary_services, test_schema, u32_branch_key, unbranched_subscription_definition,
    unpaced_domain_state, validate_wasm_test_output_groups, validate_wasm_test_outputs,
    vm_input_from_test_rows, wait_for_persisted_runtime_state_lsm, wasm_generated_pool,
    wasm_guest_column, wasm_guest_stream, wasm_input_acks, wasm_input_for_records,
    wasm_input_for_values, wasm_test_generated_output, wasm_test_output, window_aggregate,
    window_outputs, window_plan, with_inherit_all,
};
pub(in crate::runtime) use vm_compile::{
    CompiledBranchProgram, CompiledEmitterFilterMapProgram, EmitterHeaders, KeyProjectionKind,
    MaterializedFieldInterest, MaterializedLookupKeyMode, compile_emitter_filter_map_program,
    compile_key_projection_program,
};
use vm_compile::{
    CompiledMessageErrorSite, CompiledMessageErrorSites, GeneratorSetProgramSchemas,
    OutputNamespaceInput, RouteProgram, RuntimeCompileTarget, RuntimeFilterScope, RuntimeVmSchema,
    RuntimeVmSchemaPair, bind_ingestor_filter_map_program, bind_output_branch_program,
    bind_processor_output_filter_map_program, bind_scoped_filter_program,
    collect_expression_field_paths, compile_emitter_filter_map_part,
    compile_finalized_output_filter_program, compile_generator_set_program,
    compile_message_error_set_program, compile_processor_output_filter_map_program,
    compile_reorderer_program, compile_scoped_filter_program,
    compile_wasm_output_filter_map_program, compiled_message_error_sites,
    evaluate_constant_expression_vm, referenced_materialized_stream_bindings,
    relay_schema_for_routing, relay_schema_for_runtime, runtime_udf_compile_options,
    runtime_udf_signatures,
};
use vm_input::{
    SharedBatchColumns, VmInputProjectionSources, compute_lookup_hash_map_columns,
    project_vm_input_batch, relay_state_snapshot_from_side_inputs, runtime_values_input_column,
    vm_output_value, vm_typed_batch_selected_rows_to_runtime_batch,
    vm_typed_batch_to_runtime_batch,
};
use wasm_checkpoint::{
    WASM_CHECKPOINT_DEADLINE, WasmCallbackReporting, WasmCheckpointHolds,
    checkpoint_wasm_guest_state, wasm_callback_decided_tokens,
};
pub(crate) use wasm_guest_state_reset::GuestWasmStateResetRequest;
use wasm_guest_state_reset::{
    PendingGuestWasmStateResets, WasmGuestStateResetContext, WasmGuestStateResetFence,
    refuse_fenced_wasm_branch_input, request_wasm_guest_state_reset,
};
use wasm_output::{WasmMaterializedOutput, WasmOutputContext, dispatch_wasm_output_envelopes};
pub(crate) use wasm_processor::WasmInstanceError;
use wasm_processor::{
    WasmBranchModule, WasmLiveInstance, WasmModuleFile, flush_branch_wasm_processor,
};
use wasm_state::{
    CapturedWasmCheckpoint, CompletedWasmCheckpoint, LocallyDurableWasmCheckpoint,
    ReplicatedWasmProcessorState, RestorableGuestState, WasmCheckpointBoundary, WasmGuestState,
};
use wasm_state_recovery::RaisedWasmStateRecoveries;
pub(crate) use wasm_state_recovery::WasmStateRecoveryRequest;
pub(crate) use wasm_state_reset::WasmStateResetPreparation;
use wasm_state_reset::{
    PreparedWasmStateReset, PreparedWasmStateResetBranch, WasmStateResetRuntimeError,
};
use window_accumulator::{
    RetainedWindowRows, WindowAccumulator, WindowAccumulatorPlan, WindowArgumentColumns, WindowRow,
};
use window_processor::{
    WindowAdmission, WindowProcessorError, WindowProcessorState, evaluate_window_arguments,
    flush_ready_window_processor, message_timestamp, snapshot_window_processor_live_state,
    window_next_deadline,
};
use window_state::{
    LinearHistogramDelayedRemovalSnapshot, ReplicatedWindowProcessorState,
    WindowAccumulatorSnapshot, WindowEntrySnapshot, WindowProcessorStateSnapshot,
};

#[cfg(test)]
use crate::registry::{PlannedModel, branched_node_specs_from_models};

mod vm_compile;
mod vm_input;
mod wasm_checkpoint;
#[cfg(feature = "benchmarks")]
#[doc(hidden)]
pub mod wasm_checkpoint_benchmark;
mod wasm_guest_state_reset;
mod wasm_output;
mod wasm_processor;
mod wasm_state;
mod wasm_state_recovery;
mod wasm_state_reset;
mod window_accumulator;
mod window_processor;
mod window_state;

#[doc(hidden)]
pub use branch_key::BranchKey;
pub(crate) use domain_clock::{DomainClockObserver, DomainExecutionSnapshot};
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

/// Opaque logical clock and deadline capabilities exposed only so compile-fail tests can prove
/// that they cannot be interchanged with the physical deadlines of the connector contract.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod clock_capability_compile_tests {
    pub use super::domain_clock::{DomainClock, LogicalDeadline};
}

/// The opaque subscription predicate exposed only so compile-fail tests can prove that general VM
/// programs and predicates compiled with a different input context cannot be substituted.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod subscription_predicate_capability_compile_tests {
    pub use super::subscription_predicate::CompiledSubscriptionPredicate;
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
pub(crate) use ingest_metadata::IngestFilterMapMetadata;
pub(crate) use ingestor_quiesce::IngestorQuiesceCounters;
pub(crate) use local_drain::LocalGraphDrainOutcome;
pub(crate) use materialized_state::MaterializedRecordReport;
pub use node::{DEFAULT_TEMP_DIR, Runtime};
pub(crate) use observability::{IngestorDescribe, KafkaDomainOffsetDescribe};
pub(crate) use ownership_handoff_error::{OwnershipHandoffError, OwnershipHandoffResult};
pub(crate) use relay_batch::{RelayMessage, RelayRecordBatch};
pub(crate) use relay_channel::{RelayBroadcast, RelayReceiver as RelaySubscriptionReceiver};
pub(crate) use relay_subscription::RelaySubscriptionDefinition;
use relay_subscription::{RelaySubscriptionRefusal, RelaySubscriptions};
pub(crate) use state_replication::StateSyncAck;
pub(crate) use state_snapshot_transfer::{DescribeStateSnapshot, FetchStateSnapshot};
pub(crate) use state_store::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
};
pub(crate) use subscription_predicate::{
    CompiledSubscriptionPredicate, SubscriptionPredicateCompileContext,
    compile_subscription_predicate, execute_subscription_predicate_on_record,
};
pub(crate) use vm_compile::{
    CompiledDomainUdfs, CompiledProgramWithMaterializedInterest, MaterializedProgramInterest,
    RuntimeMaterializedRelaySpec, RuntimeVmCompileContext,
};
