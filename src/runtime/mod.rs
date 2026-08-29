use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use ahash::{HashMap, HashMapExt, HashSet, RandomState};
use arc_swap::{ArcSwap, ArcSwapOption};
use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array,
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
use chrono::{TimeDelta, TimeZone, Utc};
use dashmap::DashMap;
use fjall::Database;
use futures_util::stream::FuturesUnordered;
use nervix_interconnect::{
    Envelope, RelayPayload, RelayPayloadKind, Transport, TransportMode as InterconnectTransportMode,
};
use nervix_models::{
    AckMode, Assignment, ClickHouseValueMapping, ClientConfigEntry, ClusterSchedule,
    CodecWireFormat, CorrelationTimeoutAction, CorrelatorMatchPolicy, CreateClientAzureBlob,
    CreateClientGcs, CreateClientHttp, CreateClientIcebergRest, CreateClientKafka,
    CreateClientMqtt, CreateClientNats, CreateClientOtel, CreateClientPrometheus,
    CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis, CreateClientS3,
    CreateClientSentry, CreateClientSqs, CreateClientWebsockets, CreateClientZeroMq, CreateCodec,
    CreateEmitter, CreateEndpoint, CreateGenerator, CreateIngestor, CreateLookup, CreateReingestor,
    CreateRelay, CreateSignalingProtocol, CreateUdf, Domain, DomainConfig, DomainPace,
    DomainSchedule, DomainState, DomainTick, EmitSink, EmitterAckWindow, EmitterPublishingMode,
    EndpointType, ErrorPolicies, FieldPath, GeneralErrorPolicy, IcebergCatalog,
    IcebergStorageBackend, IcebergValueMapping, Identifier, InferencerExecutionMode,
    InferencerTensorDeclaration, InferencerTensorMapping, IngestQuiesceMode, IngestQuiesceOverflow,
    IngestSource, IngestTimestampSource, KafkaIngestMode, KafkaOffsetMode, KafkaPartitionSchedule,
    Literal as ModelLiteral, MaterializedStatePolicy, MessageErrorCode, MessageErrorOperation,
    MessageErrorPolicy, Model, ModelKind, MongoDbConflictAction, MongoDbValueMapping,
    MqttIngestMode, MqttQos, MqttSession, MySqlConflictAction, MySqlValueMapping,
    OtelAggregationTemporality, OtelMetric, OtelMetricKind, OtelScope, OtelSignal,
    OtelValueMapping, OutputBranch, PostgresConflictAction, PostgresValueMapping, ProcessorOutput,
    PulsarIngestMode, RabbitMqIngestMode, RemoteAckOutcome, RemoteAckRegistration,
    RemoteAckResolution, RemoteRuntimeField, ResourceId, ResourceVersionStatus, RetryPolicy,
    RouteConstruction, ScheduledNode, SignalingWireFormat, SqsFifoGroup, SqsIngestMode,
    StructuredMessageError, Timestamp, WireSchemaDefinition,
};
use nervix_nspl::{
    vm_program::{
        CaseArm, Expr, FunctionName, InternalFieldNamespace, InternalFieldRef, Literal,
        SemanticNamespaces, Span as VmSpan, SpannedExpr, lower_branch_construction,
        lower_finalized_output_filter, lower_generated_route, lower_route_construction,
        lower_set_only_route, lower_transforming_route,
    },
    window_processor::aggregate::{
        WindowAggregateDemand, WindowAggregateFunction, WindowAggregateProgram,
        WindowAggregateStorageKind, lower_window_assignments,
    },
};
use nervix_roto::UdfExecutor;
#[cfg(test)]
use nervix_vm::SPAWN_BLOCKING_ROW_THRESHOLD as VM_SPAWN_BLOCKING_ROW_THRESHOLD;
use nervix_vm::{
    CompileBinding as VmCompileBinding, CompileNamespace as VmCompileNamespace,
    CompileOptions as VmCompileOptions, CompiledProgram as VmCompiledProgram,
    ExecutionContext as VmExecutionContext, FunctionInjector as VmFunctionInjector,
    OutputMode as VmOutputMode, SchemaSensitivity as VmSchemaSensitivity,
    TypedArray as VmTypedArray, TypedBatch as VmTypedBatch,
    compile_program_with_options_for_bindings_with_sensitivity as compile_vm_program_with_options_for_bindings_with_sensitivity,
    execute_program_with_selection_in_context,
    infer_set_expr_types_for_bindings_with_udfs as infer_vm_set_expr_types_for_bindings_with_udfs,
};
use nervix_wasm::{
    DomainClock as WasmDomainClock, WasmAckSidecar, WasmAckToken, WasmAckTokenSet, WasmBranchInit,
    WasmEnvelope, WasmOutputColumnRef, WasmOutputRow, WasmRoutedOutput, WasmRuntime,
    WasmRuntimeConfig,
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
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, info, trace, warn};
use triomphe::Arc;
use upon::Engine as TemplateEngine;

#[cfg(test)]
use crate::runtime_schema::test_runtime_row;
use crate::{
    cluster,
    metrics::{
        BranchEvictionReason, IngestorQuiesceMetricLabels, NodeBatchObservation,
        NodeLatencyObservation, NodeWithoutRelayObservation, RelayBatchObservation,
        RelayBufferObservation, RuntimeMetrics,
    },
    registry::{ActiveGraph, RegistryEntity, RuntimeChange, RuntimeChanges},
    resource::ResourceStore,
    runtime_ack::{AckCompletion, AckOutcome, AckProgress, AckRootTracker, AckSet},
    runtime_schema::{
        CodecError, CompiledCodec, CompiledSchema, ProtobufDescriptorPool, RuntimeRecordBatch,
        RuntimeRecordMetadata, RuntimeRow, RuntimeValue, compile_codec_with_protobuf,
        compile_schema, decode_with_codec, decode_with_codec_owned, parse_as_type_from_arrow,
        runtime_value_arrow_array, runtime_value_from_arrow_array,
    },
};

mod branch_aggregated_state;
mod branch_instance_registry;
mod branch_lru_state;
mod client_config;
mod deduplicator;
mod emitters;
mod force_flush;
mod http_client;
mod inferencer;
mod ingestors;
mod kafka_offset_state;
mod materialized_state;
mod message_error_delivery;
mod planning;
mod processors;
mod relay_batch;
mod relay_channel;
mod relay_interaction;
#[cfg(feature = "benchmarks")]
#[doc(hidden)]
pub mod relay_interaction_benchmark;
mod runtime_impl;
mod schedule_delta;
mod service_url;
mod state_store;
mod test_hooks;
mod tls;
mod wasm_state;
mod websocket_signaling;
mod window_state;

#[cfg(test)]
use branch_aggregated_state::{
    BranchAggregatedRuntimeStateSnapshot, encode_branch_aggregated_snapshot,
};
use branch_aggregated_state::{ReplicatedBranchAggregatedState, decode_branch_aggregated_snapshot};
use branch_instance_registry::BranchInstanceRegistry;
use branch_lru_state::{decode_branch_lru_snapshot, encode_branch_lru_snapshot};
use client_config::{client_tls_paths, read_tls_file, render_client_config_template};
use deduplicator::{
    CompiledDeduplicatorKeyProgram, ReplicatedDeduplicatorState, compile_deduplicator_key_program,
};
use force_flush::{DomainForceFlush, DomainForceFlushCompletion, DomainForceFlushParticipant};
use http_client::HttpClientConfig;
pub(crate) use ingestors::kafka::KafkaIngestor;
use kafka_offset_state::ReplicatedKafkaOffsetState;
use materialized_state::{
    ReplicatedMaterializedRelayState, decode_materialized_stream_snapshot,
    encode_materialized_stream_snapshot_entries,
};
use message_error_delivery::{
    MessageErrorDelivery, MessageErrorRouteKey, MessageErrorRouteRuntime, MessageErrorRouteTarget,
    matching_message_error_output,
};
use planning::{
    branched_node_specs_from_active_graph, branched_node_specs_from_models,
    branched_node_specs_from_scheduled_nodes, format_branched_by,
    materialize_ingestor_route_template, materialize_processor_instance_template,
    processor_template_for_graph_node,
};
use processors::{
    BranchInstanceAckBoundary, BranchInstanceTemplate, BranchedIngestorSpec, BranchedNodeSpecs,
    BranchedProcessorNodeSpec, BranchedProcessorOperationSpec, BranchedProcessorOutputSpec,
    BranchedProcessorOutputsSpec, BranchedProcessorSpec, CompiledCorrelatorOutputProgram,
    CompiledCorrelatorWhereProgram, CompiledReordererProgram, CompiledWindowAggregateExpr,
    CompiledWindowAggregateProgram, CorrelatorBranchState, CorrelatorPendingMessage, FilterMapPlan,
    InferencerFlushContext, InferencerOutputBuffer, IngestorRouteTemplate, JunctionFlushContext,
    PlannedGeneralError, PlannedMessageError, RelayProcessorNode, RelayProcessorOperationNode,
    RelayProcessorOperationTemplate, RelayProcessorOutputNode, RelayProcessorOutputTemplate,
    RelayProcessorOutputsNode, RelayProcessorOutputsTemplate, RelayProcessorRelayTemplate,
    RelayProcessorTemplate, ReorderKeyPart, ReordererOutputBuffer, ReordererPendingMessage,
    RuntimeInputCollector, WasmAckContext, WasmAckMap, WasmCompiledBranchProcessor,
    WasmFlushContext, WindowBounds, WindowFlushContext,
};
pub use relay_batch::RelayMessage;
pub(crate) use relay_batch::RelayRecordBatch;
use relay_batch::build_stream_record_batch_preserving_acks;
type RelayDispatchResult = Result<(), Box<RelayRecordBatch>>;
pub(crate) use relay_channel::{
    RelayBroadcast, RelayDispatchGate, RelayDispatchGateLease,
    RelayReceiver as RelaySubscriptionReceiver,
};
use relay_interaction::{
    RelayInteraction, RelayInteractionCommand, RelayInteractionEvent, RelayInteractionInput,
    RuntimeInputCollectPolicy,
};
pub(crate) type RelaySubscriptionRecvError = async_broadcast::RecvError;
use service_url::ServiceUrl;
pub(crate) use state_store::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStateKind, RuntimeStatePlacement,
    RuntimeStateStore,
};
use test_hooks::EmitterFaultMode;
pub use test_hooks::{
    EmitterFaultInjector, IngestorFaultInjector, OtelClientFaultInjector, RuntimeTestHooks,
    SchedulePublicationFaultInjector,
};
use tls::RustlsClientConfigSource;
use wasm_state::ReplicatedWasmProcessorState;
pub use websocket_signaling::CompiledSignalingProtocol;
pub(crate) use websocket_signaling::{
    SignalingDataSink, SignalingProtobufDescriptors, WebsocketSignalingSession,
};
use window_state::{
    LinearHistogramDelayedRemovalSnapshot, ReplicatedWindowProcessorState,
    WindowAggregateAccumulatorSnapshot, WindowEntrySnapshot, WindowProcessorStateSnapshot,
    WindowSequenceValueSnapshot, WindowSortedCountSnapshot,
};

#[cfg(test)]
const STUPID_CHANNEL_CAPACITY_REMOVE_ME: usize = 1;
/// Chosen operational bound for how many decoded source rows accumulate before an
/// ingest group executes and becomes one Arrow batch per (relay, branch key). This is
/// intentionally independent of an NSPL route's flush policy.
pub(crate) const INGEST_GROUP_MAX_ROWS: usize = 1024;
/// Chosen operational bound for how long a partial source group waits when the source
/// goes quiet. This is intentionally independent of an NSPL route's flush policy.
pub(crate) const INGEST_GROUP_IDLE_FLUSH: Duration = Duration::from_millis(5);
const RELAY_BUFFER_DIRECTION_CONCRETE: &str = "concrete";
const BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_STATE_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_STATE_REPLICATION_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_DOMAIN_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);
pub const DEFAULT_TEMP_DIR: &str = "/tmp";
const DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL: Duration = Duration::from_secs(1);
const REMOTE_RELAY_INSTANTIATION_WAIT: Duration = Duration::from_secs(5);
const REMOTE_RELAY_INSTANTIATION_POLL: Duration = Duration::from_millis(25);
const REMOTE_ACK_ALIVE_INTERVAL: Duration = Duration::from_millis(100);
const INGEST_METADATA_NAMESPACE: &str = "metadata";
const BRANCH_NAMESPACE: &str = "branch";

pub(crate) type IngestHeaders = Vec<(String, String)>;

type SharedActiveGraph = StdArc<ArcSwapOption<ActiveGraph>>;
type PendingStateSyncSender = oneshot::Sender<Result<Option<PersistedRuntimeStateEntry>, String>>;

#[derive(Debug, Clone, Copy)]
struct ParsedRetryPolicy {
    backoff: Duration,
    max_backoff: Duration,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ResolvedClientConfig {
    pub(crate) entries: Vec<nervix_models::ClientConfigEntry>,
    pub(crate) mounts: Option<Arc<ClientResourceMounts>>,
}

#[derive(Debug)]
pub(crate) struct ClientResourceMounts {
    _root: TempDir,
    _aliases: BTreeMap<String, PathBuf>,
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
        pending_nodes: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RuntimeKey {
    domain: Domain,
    identifier: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RuntimeStateSchemaKey {
    domain: Domain,
    kind: ModelKind,
    identifier: Identifier,
}

impl RuntimeStateSchemaKey {
    fn new(domain: Domain, kind: ModelKind, identifier: Identifier) -> Self {
        Self {
            domain,
            kind,
            identifier,
        }
    }
}

impl RuntimeKey {
    fn new(domain: Domain, identifier: Identifier) -> Self {
        Self { domain, identifier }
    }
}

enum IngestorRuntime {
    Background {
        shutdown: watch::Sender<bool>,
        branched: Vec<Arc<IngestorRouteRuntime>>,
        tasks: Vec<JoinHandle<()>>,
    },
    Endpoint {
        route_keys: Vec<HttpRouteKey>,
        branched: Vec<Arc<IngestorRouteRuntime>>,
        shutdown: watch::Sender<bool>,
        tasks: Vec<JoinHandle<()>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IngestorQuiesceCause {
    EntityHold,
    DomainPause,
    MemoryPressure,
}

impl IngestorQuiesceCause {
    fn as_str(self) -> &'static str {
        match self {
            Self::EntityHold => "entity hold",
            Self::DomainPause => "domain pause",
            Self::MemoryPressure => "memory pressure",
        }
    }
}

#[derive(Debug, Default)]
struct IngestorQuiesceReasons {
    entity_holds: usize,
    domain_pause: bool,
    memory_pressure: bool,
}

impl IngestorQuiesceReasons {
    fn active(&self) -> Option<IngestorQuiesceCause> {
        if self.memory_pressure {
            Some(IngestorQuiesceCause::MemoryPressure)
        } else if self.domain_pause {
            Some(IngestorQuiesceCause::DomainPause)
        } else if self.entity_holds > 0 {
            Some(IngestorQuiesceCause::EntityHold)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct IngestorQuiesceModes {
    active: IngestQuiesceMode,
    pending: Option<IngestQuiesceMode>,
    active_supported_by_source: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct BufferedIngestPayload {
    payloads: Vec<Vec<u8>>,
    metadata: Vec<IngestFilterMapMetadata>,
}

impl BufferedIngestPayload {
    pub(crate) fn new(payload: &[u8], metadata: IngestFilterMapMetadata) -> Self {
        Self {
            payloads: vec![payload.to_vec()],
            metadata: vec![metadata],
        }
    }

    pub(crate) fn batch(entries: Vec<(Vec<u8>, IngestFilterMapMetadata)>) -> Self {
        let (payloads, metadata) = entries.into_iter().unzip();
        Self { payloads, metadata }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        self.payloads
            .first()
            .expect("buffered ingest payload must contain at least one source payload")
    }

    pub(crate) fn metadata(&self) -> &IngestFilterMapMetadata {
        self.metadata
            .first()
            .expect("buffered ingest payload must contain matching ingest metadata")
    }

    fn entries(&self) -> impl Iterator<Item = (&[u8], &IngestFilterMapMetadata)> {
        self.payloads
            .iter()
            .zip(&self.metadata)
            .map(|(payload, metadata)| (payload.as_slice(), metadata))
    }

    fn byte_len(&self) -> usize {
        self.payloads.iter().map(Vec::len).sum()
    }
}

#[derive(Debug, Default)]
struct IngestorQuiesceBuffer {
    payloads: VecDeque<BufferedIngestPayload>,
    bytes: usize,
}

#[derive(Debug)]
pub(crate) enum IngestorQuiesceIntake {
    Dispatch(BufferedIngestPayload),
    Buffered,
    Dropped,
    Rejected { retry_after: Option<Duration> },
}

#[derive(Debug)]
pub(crate) struct IngestorQuiesceControl {
    modes: RwLock<IngestorQuiesceModes>,
    reasons: RwLock<IngestorQuiesceReasons>,
    buffers: parking_lot::Mutex<HashMap<u64, IngestorQuiesceBuffer>>,
    changed: Notify,
    buffered_records: AtomicUsize,
    buffered_bytes: AtomicUsize,
    dropped_total: AtomicU64,
    rejected_total: AtomicU64,
    metrics: RuntimeMetrics,
    metric_labels: IngestorQuiesceMetricLabels,
}

impl IngestorQuiesceControl {
    fn new(
        mode: IngestQuiesceMode,
        metrics: RuntimeMetrics,
        metric_labels: IngestorQuiesceMetricLabels,
    ) -> Self {
        Self {
            modes: RwLock::new(IngestorQuiesceModes {
                active: mode,
                pending: None,
                active_supported_by_source: true,
            }),
            reasons: RwLock::new(IngestorQuiesceReasons::default()),
            buffers: parking_lot::Mutex::new(HashMap::default()),
            changed: Notify::new(),
            buffered_records: AtomicUsize::new(0),
            buffered_bytes: AtomicUsize::new(0),
            dropped_total: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
            metrics,
            metric_labels,
        }
    }

    fn sync_buffered_metrics(&self) {
        self.metrics.set_ingestor_quiesce_buffered(
            &self.metric_labels,
            self.buffered_records.load(Ordering::Relaxed),
            self.buffered_bytes.load(Ordering::Relaxed),
        );
    }

    fn record_dropped(&self, count: u64) {
        self.dropped_total.fetch_add(count, Ordering::Relaxed);
        self.metrics
            .increment_ingestor_quiesce_dropped(&self.metric_labels, count);
    }

    fn record_rejected(&self, count: u64) {
        self.rejected_total.fetch_add(count, Ordering::Relaxed);
        self.metrics
            .increment_ingestor_quiesce_rejected(&self.metric_labels, count);
    }

    fn update_declared_source(&self, source: &IngestSource) {
        let quiesced = self.reasons.read().active().is_some();
        let mut modes = self.modes.write();
        if quiesced {
            modes.active_supported_by_source = source.supports_quiesce(&modes.active);
            modes.pending = Some(source.quiesce().clone());
        } else {
            modes.active = source.quiesce().clone();
            modes.pending = None;
            modes.active_supported_by_source = true;
        }
        drop(modes);
        self.changed.notify_waiters();
    }

    fn engage(&self, cause: IngestorQuiesceCause) {
        let mut reasons = self.reasons.write();
        match cause {
            IngestorQuiesceCause::EntityHold => {
                reasons.entity_holds = reasons.entity_holds.saturating_add(1);
            }
            IngestorQuiesceCause::DomainPause => reasons.domain_pause = true,
            IngestorQuiesceCause::MemoryPressure => reasons.memory_pressure = true,
        }
        drop(reasons);
        self.changed.notify_waiters();
    }

    fn release(&self, cause: IngestorQuiesceCause) {
        let now_active = {
            let mut reasons = self.reasons.write();
            match cause {
                IngestorQuiesceCause::EntityHold => {
                    reasons.entity_holds = reasons.entity_holds.saturating_sub(1);
                }
                IngestorQuiesceCause::DomainPause => reasons.domain_pause = false,
                IngestorQuiesceCause::MemoryPressure => reasons.memory_pressure = false,
            }
            reasons.active()
        };
        if now_active.is_none() {
            let mut modes = self.modes.write();
            if let Some(pending) = modes.pending.take() {
                modes.active = pending;
            }
            modes.active_supported_by_source = true;
        }
        self.changed.notify_waiters();
    }

    pub(crate) fn cause(&self) -> Option<IngestorQuiesceCause> {
        self.reasons.read().active()
    }

    pub(crate) fn is_quiesced(&self) -> bool {
        self.cause().is_some()
    }

    pub(crate) fn mode(&self) -> IngestQuiesceMode {
        self.modes.read().active.clone()
    }

    fn active_mode_is_supported(&self) -> bool {
        self.modes.read().active_supported_by_source
    }

    pub(crate) fn should_suspend_intake(&self) -> bool {
        self.is_quiesced()
            && (!self.active_mode_is_supported()
                || matches!(self.mode(), IngestQuiesceMode::Suspend))
    }

    pub(crate) fn should_skip_poll(&self) -> bool {
        match self.cause() {
            Some(IngestorQuiesceCause::MemoryPressure) => true,
            Some(_) if !self.active_mode_is_supported() => true,
            Some(_) => matches!(self.mode(), IngestQuiesceMode::Suspend),
            None => false,
        }
    }

    pub(crate) async fn wait_until_not_suspended(&self) {
        loop {
            if !self.should_suspend_intake() {
                return;
            }
            let changed = self.changed.notified();
            if !self.should_suspend_intake() {
                return;
            }
            changed.await;
        }
    }

    pub(crate) async fn wait_for_change(&self) {
        self.changed.notified().await;
    }

    pub(crate) fn intake(
        &self,
        instance: u64,
        payload: BufferedIngestPayload,
        endpoint: bool,
    ) -> IngestorQuiesceIntake {
        let Some(cause) = self.cause() else {
            return IngestorQuiesceIntake::Dispatch(payload);
        };
        let mode = self.mode();
        if !self.active_mode_is_supported() {
            if endpoint {
                self.record_rejected(1);
                return IngestorQuiesceIntake::Rejected { retry_after: None };
            }
            self.record_dropped(1);
            return IngestorQuiesceIntake::Dropped;
        }
        if cause == IngestorQuiesceCause::MemoryPressure {
            if endpoint {
                self.record_rejected(1);
                return IngestorQuiesceIntake::Rejected {
                    retry_after: match mode {
                        IngestQuiesceMode::Reject { retry_after } => {
                            humantime::parse_duration(&retry_after).ok()
                        }
                        _ => None,
                    },
                };
            }
            self.record_dropped(1);
            return IngestorQuiesceIntake::Dropped;
        }

        match mode {
            IngestQuiesceMode::Suspend => IngestorQuiesceIntake::Dispatch(payload),
            IngestQuiesceMode::Drop => {
                self.record_dropped(1);
                IngestorQuiesceIntake::Dropped
            }
            IngestQuiesceMode::Reject { retry_after } => {
                self.record_rejected(1);
                IngestorQuiesceIntake::Rejected {
                    retry_after: humantime::parse_duration(&retry_after).ok(),
                }
            }
            IngestQuiesceMode::EndpointBuffer { max_size } => {
                let max_size = quiesce_max_size_bytes(&max_size);
                let payload_bytes = payload.byte_len();
                let mut buffers = self.buffers.lock();
                let buffer = buffers.entry(instance).or_default();
                if payload_bytes > max_size || buffer.bytes.saturating_add(payload_bytes) > max_size
                {
                    self.record_rejected(1);
                    return IngestorQuiesceIntake::Rejected { retry_after: None };
                }
                buffer.bytes = buffer.bytes.saturating_add(payload_bytes);
                buffer.payloads.push_back(payload);
                self.buffered_records.fetch_add(1, Ordering::Relaxed);
                self.buffered_bytes
                    .fetch_add(payload_bytes, Ordering::Relaxed);
                self.sync_buffered_metrics();
                IngestorQuiesceIntake::Buffered
            }
            IngestQuiesceMode::Buffer { max_size, overflow } => {
                let max_size = quiesce_max_size_bytes(&max_size);
                let payload_bytes = payload.byte_len();
                let mut buffers = self.buffers.lock();
                let buffer = buffers.entry(instance).or_default();
                if payload_bytes > max_size {
                    self.record_dropped(1);
                    return IngestorQuiesceIntake::Dropped;
                }
                if overflow == IngestQuiesceOverflow::DropNewest
                    && buffer.bytes.saturating_add(payload_bytes) > max_size
                {
                    self.record_dropped(1);
                    return IngestorQuiesceIntake::Dropped;
                }
                while buffer.bytes.saturating_add(payload_bytes) > max_size {
                    let Some(dropped) = buffer.payloads.pop_front() else {
                        break;
                    };
                    buffer.bytes = buffer.bytes.saturating_sub(dropped.byte_len());
                    self.buffered_records.fetch_sub(1, Ordering::Relaxed);
                    self.buffered_bytes
                        .fetch_sub(dropped.byte_len(), Ordering::Relaxed);
                    self.record_dropped(1);
                }
                buffer.bytes = buffer.bytes.saturating_add(payload_bytes);
                buffer.payloads.push_back(payload);
                self.buffered_records.fetch_add(1, Ordering::Relaxed);
                self.buffered_bytes
                    .fetch_add(payload_bytes, Ordering::Relaxed);
                self.sync_buffered_metrics();
                IngestorQuiesceIntake::Buffered
            }
        }
    }

    fn endpoint_admission(&self) -> Result<(), Option<Duration>> {
        let Some(cause) = self.cause() else {
            return Ok(());
        };
        let mode = self.mode();
        if !self.active_mode_is_supported() {
            self.record_rejected(1);
            return Err(None);
        }
        if cause != IngestorQuiesceCause::MemoryPressure
            && matches!(mode, IngestQuiesceMode::EndpointBuffer { .. })
        {
            return Ok(());
        }
        self.record_rejected(1);
        Err(match mode {
            IngestQuiesceMode::Reject { retry_after } => {
                humantime::parse_duration(&retry_after).ok()
            }
            _ => None,
        })
    }

    pub(crate) fn pop_buffered(&self, instance: u64) -> Option<BufferedIngestPayload> {
        if self.is_quiesced() {
            return None;
        }
        let mut buffers = self.buffers.lock();
        let buffer = buffers.get_mut(&instance)?;
        let payload = buffer.payloads.pop_front()?;
        buffer.bytes = buffer.bytes.saturating_sub(payload.byte_len());
        self.buffered_records.fetch_sub(1, Ordering::Relaxed);
        self.buffered_bytes
            .fetch_sub(payload.byte_len(), Ordering::Relaxed);
        self.sync_buffered_metrics();
        Some(payload)
    }

    fn counters(&self) -> IngestorQuiesceCounters {
        IngestorQuiesceCounters {
            buffered_records: self.buffered_records.load(Ordering::Relaxed),
            buffered_bytes: self.buffered_bytes.load(Ordering::Relaxed),
            dropped_total: self.dropped_total.load(Ordering::Relaxed),
            rejected_total: self.rejected_total.load(Ordering::Relaxed),
        }
    }

    fn terminate(&self) {
        let dropped = {
            let mut buffers = self.buffers.lock();
            let dropped = buffers
                .values()
                .map(|buffer| buffer.payloads.len())
                .sum::<usize>();
            buffers.clear();
            dropped
        };
        self.buffered_records.store(0, Ordering::Relaxed);
        self.buffered_bytes.store(0, Ordering::Relaxed);
        self.sync_buffered_metrics();
        self.record_dropped(u64::try_from(dropped).unwrap_or(u64::MAX));
    }
}

fn quiesce_max_size_bytes(value: &str) -> usize {
    value
        .parse::<ubyte::ByteUnit>()
        .ok()
        .and_then(|size| usize::try_from(size.as_u64()).ok())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestorQuiesceCounters {
    pub buffered_records: usize,
    pub buffered_bytes: usize,
    pub dropped_total: u64,
    pub rejected_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BranchKey {
    fields: BTreeMap<Identifier, RuntimeValue>,
    json: String,
}

impl BranchKey {
    pub(crate) fn field_value(&self, name: &str) -> Option<&RuntimeValue> {
        self.fields
            .iter()
            .find_map(|(field, value)| (field.as_str() == name).then_some(value))
    }

    pub(crate) fn from_fields(
        fields: impl IntoIterator<Item = (Identifier, RuntimeValue)>,
    ) -> Result<Self, String> {
        let fields = fields.into_iter().collect::<BTreeMap<_, _>>();
        if fields.is_empty() {
            return Err("branch key must contain at least one field".to_string());
        }
        let mut object = serde_json::Map::new();
        for (field, value) in &fields {
            object.insert(field.as_str().to_string(), value.to_json_value());
        }
        let json = serde_json::Value::Object(object).to_string();
        Ok(Self { fields, json })
    }

    fn from_remote_record<'a>(
        record: &nervix_models::RemoteRuntimeRecord,
        field_names: impl IntoIterator<Item = &'a Identifier>,
    ) -> Result<Option<Self>, String> {
        let mut fields = BTreeMap::new();
        for field_name in field_names {
            let Some(field) = record
                .fields
                .iter()
                .find(|field| field.name == field_name.as_str())
            else {
                return Ok(None);
            };
            fields.insert(
                field_name.clone(),
                RuntimeValue::from_remote(field.value.clone()),
            );
        }
        Self::from_fields(fields).map(Some)
    }

    pub(crate) fn from_remote_key(
        fields: Option<Vec<RemoteRuntimeField>>,
    ) -> Result<Option<Self>, String> {
        let Some(fields) = fields else {
            return Ok(None);
        };
        let mut values = BTreeMap::new();
        for field in fields {
            let name = Identifier::try_from(field.name.clone()).map_err(|error| {
                format!(
                    "remote branch key field '{}' is invalid: {error}",
                    field.name
                )
            })?;
            values.insert(name, RuntimeValue::from_remote(field.value));
        }
        Self::from_fields(values).map(Some)
    }

    pub(crate) fn to_remote_key(key: &Option<Self>) -> Option<Vec<RemoteRuntimeField>> {
        key.as_ref().map(|key| {
            key.fields
                .iter()
                .map(|(name, value)| RemoteRuntimeField {
                    name: name.as_str().to_string(),
                    value: value.to_remote(),
                })
                .collect()
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        self.json.as_str()
    }

    fn fields(&self) -> impl Iterator<Item = (&Identifier, &RuntimeValue)> {
        self.fields.iter()
    }
}

fn branch_key_display(key: &Option<BranchKey>) -> &str {
    key.as_ref().map(BranchKey::as_str).unwrap_or("none")
}

fn kafka_domain_offset_describe_from_schedule(
    topic: &str,
    instances: u64,
    schedule: &KafkaPartitionSchedule,
) -> KafkaDomainOffsetDescribe {
    let mut instance_assignments = schedule.instance_assignments.clone();
    let expected_instances = usize::try_from(instances).unwrap_or_default();
    if instance_assignments.len() < expected_instances {
        instance_assignments.resize(expected_instances, Vec::new());
    }
    KafkaDomainOffsetDescribe {
        topic: topic.to_string(),
        instances,
        observed_partitions: schedule.observed_partitions.clone(),
        rebalance_epoch: schedule.rebalance_epoch,
        instance_assignments,
    }
}

impl std::fmt::Display for BranchKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

struct DomainExecution {
    schedule: DomainSchedule,
    passive_only: bool,
    start_version: u64,
    shutdown: watch::Sender<bool>,
    graph: SharedActiveGraph,
    relay_registries: HashMap<Identifier, RelayRegistry>,
    relay_schemas: HashMap<Identifier, Arc<CompiledSchema>>,
    relay_services: HashMap<Identifier, Arc<RelayBoundaryServices>>,
    lookups: HashMap<Identifier, Arc<LookupRuntime>>,
    udfs: UdfExecutor,
    relay_branchings: HashMap<Identifier, Vec<Identifier>>,
    relay_branching_schemas: HashMap<Identifier, Option<StdArc<arrow_schema::Schema>>>,
    materialized_stream_specs: HashMap<Identifier, RuntimeMaterializedRelaySpec>,
    materialized_stream_owner_nodes: HashMap<Identifier, Option<String>>,
    branched_ingestors: HashMap<Identifier, Vec<BranchedIngestorSpec>>,
    branched_entrypoints: HashMap<Identifier, Vec<Arc<IngestorRouteRuntime>>>,
    codecs: HashMap<Identifier, Arc<CompiledCodec>>,
    signaling_protocols: HashMap<Identifier, Arc<CompiledSignalingProtocol>>,
    endpoint_routes: HashMap<Identifier, EndpointRoute>,
    node_tasks: HashMap<RegistryEntity, ScheduledNodeTask>,
    emitter_tasks: HashMap<RegistryEntity, ScheduledEmitterTask>,
    generator_tasks: HashMap<RegistryEntity, JoinHandle<()>>,
    reingestor_tasks: HashMap<RegistryEntity, Vec<JoinHandle<()>>>,
    clients: HashMap<Identifier, Arc<Model>>,
    tasks: Vec<JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterPublishingDrainState {
    AwaitingConfirmation,
    RetryingInfrastructure,
    RetryingIcebergCommit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmitterPublishingDrainStatus {
    pub emitter: Identifier,
    pub state: EmitterPublishingDrainState,
    pub pending_messages: usize,
    pub retry_backoff: Option<Duration>,
    pub retry_wait: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainDrainStatus {
    pub active_ingestors: usize,
    pub active_generators: usize,
    pub outstanding_acks: usize,
    pub buffered_emitter_messages: usize,
    pub emitter_publishing: Vec<EmitterPublishingDrainStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDrainStatus {
    pub buffered_relay_batches: usize,
    pub node_work_items: usize,
    pub emitter_publishing: Vec<EmitterPublishingDrainStatus>,
}

impl EntityDrainStatus {
    pub fn is_drained(&self) -> bool {
        self.buffered_relay_batches == 0 && self.node_work_items == 0
    }

    pub fn outstanding_work(&self) -> usize {
        self.buffered_relay_batches
            .saturating_add(self.node_work_items)
    }
}

pub struct EntityGateHold {
    gates: Vec<RelayDispatchGateLease>,
}

struct EntityAlterHold {
    gates: EntityGateHold,
    quiesced_ingestors: Vec<Identifier>,
}

#[derive(Debug, Default)]
struct NodeQuiesceCounters {
    mailbox_and_in_flight: AtomicUsize,
    collected_inputs: AtomicUsize,
    output_buffers: AtomicUsize,
    force_flushes: AtomicUsize,
}

impl NodeQuiesceCounters {
    fn outstanding_work(&self) -> usize {
        self.mailbox_and_in_flight
            .load(Ordering::Acquire)
            .saturating_add(self.collected_inputs.load(Ordering::Acquire))
            .saturating_add(self.output_buffers.load(Ordering::Acquire))
            .saturating_add(self.force_flushes.load(Ordering::Acquire))
    }
}

struct NodeQuiesceWorkGuard {
    counters: Arc<NodeQuiesceCounters>,
}

impl NodeQuiesceWorkGuard {
    fn begin(counters: Arc<NodeQuiesceCounters>) -> Self {
        counters
            .mailbox_and_in_flight
            .fetch_add(1, Ordering::AcqRel);
        Self { counters }
    }
}

impl Drop for NodeQuiesceWorkGuard {
    fn drop(&mut self) {
        self.counters
            .mailbox_and_in_flight
            .fetch_sub(1, Ordering::AcqRel);
    }
}

struct BranchQuiesceGauges {
    counters: Arc<NodeQuiesceCounters>,
    collected_inputs: usize,
    output_buffers: usize,
}

impl BranchQuiesceGauges {
    fn new(counters: Arc<NodeQuiesceCounters>) -> Self {
        Self {
            counters,
            collected_inputs: 0,
            output_buffers: 0,
        }
    }

    fn observe(&mut self, branch: &BranchRuntime, processor: &Identifier) {
        let (collected_inputs, output_buffers) = branch
            .processors
            .get(processor)
            .map(|processor| {
                (
                    processor
                        .input_collectors
                        .values()
                        .map(|collector| collector.pending.len())
                        .fold(processor.pending_materialized.len(), usize::saturating_add),
                    processor
                        .operation
                        .output_routes()
                        .routes
                        .iter()
                        .map(|output| output.pending.len())
                        .sum(),
                )
            })
            .unwrap_or_default();
        Self::replace_gauge(
            &self.counters.collected_inputs,
            &mut self.collected_inputs,
            collected_inputs,
        );
        Self::replace_gauge(
            &self.counters.output_buffers,
            &mut self.output_buffers,
            output_buffers,
        );
    }

    fn replace_gauge(counter: &AtomicUsize, current: &mut usize, next: usize) {
        if next > *current {
            counter.fetch_add(next - *current, Ordering::AcqRel);
        } else if next < *current {
            counter.fetch_sub(*current - next, Ordering::AcqRel);
        }
        *current = next;
    }
}

impl Drop for BranchQuiesceGauges {
    fn drop(&mut self) {
        self.counters
            .collected_inputs
            .fetch_sub(self.collected_inputs, Ordering::AcqRel);
        self.counters
            .output_buffers
            .fetch_sub(self.output_buffers, Ordering::AcqRel);
    }
}

impl EntityGateHold {
    async fn wait_quiescent(&mut self) -> bool {
        for gate in &mut self.gates {
            tokio::task::consume_budget().await;
            if !gate.wait_quiescent().await {
                return false;
            }
        }
        true
    }

    pub fn release(mut self) {
        self.release_all();
    }

    fn release_all(&mut self) {
        self.gates.clear();
    }
}

impl Drop for EntityGateHold {
    fn drop(&mut self) {
        self.release_all();
    }
}

impl DomainDrainStatus {
    pub fn is_drained(&self) -> bool {
        self.active_ingestors == 0
            && self.active_generators == 0
            && self.outstanding_acks == 0
            && self.buffered_emitter_messages == 0
    }

    pub fn outstanding_work(&self) -> usize {
        self.active_ingestors
            .saturating_add(self.active_generators)
            .saturating_add(self.outstanding_acks)
            .saturating_add(self.buffered_emitter_messages)
    }
}

struct DomainActivityGuard {
    counter: Arc<AtomicUsize>,
    active: bool,
}

impl DomainActivityGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self {
            counter,
            active: false,
        }
    }

    fn set_active(&mut self, active: bool) {
        if self.active == active {
            return;
        }
        if active {
            self.counter.fetch_add(1, Ordering::AcqRel);
        } else {
            self.counter.fetch_sub(1, Ordering::AcqRel);
        }
        self.active = active;
    }
}

impl Drop for DomainActivityGuard {
    fn drop(&mut self) {
        self.set_active(false);
    }
}

#[derive(Debug)]
pub(crate) struct LookupRuntime {
    model: CreateLookup,
    resource_version: u64,
    schema: Arc<CompiledSchema>,
    batch: Arc<RuntimeRecordBatch>,
    entries: Arc<HashMap<String, usize>>,
}

#[derive(Debug)]
struct RelayPresence {
    last_seen_at: parking_lot::Mutex<Timestamp>,
}

#[derive(Debug, Clone)]
struct RelayRegistry {
    presences: Arc<DashMap<Option<BranchKey>, Arc<RelayPresence>, RandomState>>,
}

impl RelayRegistry {
    fn new() -> Self {
        Self {
            presences: Arc::new(DashMap::default()),
        }
    }

    fn touch(&self, key: &Option<BranchKey>, now: Timestamp) {
        if let Some(existing) = self.presences.get(key) {
            *existing.last_seen_at.lock() = now;
            return;
        }
        self.presences.insert(
            key.clone(),
            Arc::new(RelayPresence {
                last_seen_at: parking_lot::Mutex::new(now),
            }),
        );
    }

    fn contains_key(&self, key: &Option<BranchKey>) -> bool {
        self.presences.contains_key(key)
    }

    fn remove(&self, key: &Option<BranchKey>) {
        self.presences.remove(key);
    }

    fn keys(&self) -> Vec<String> {
        let mut keys = self
            .presences
            .iter()
            .filter_map(|entry| entry.key().as_ref().map(|key| key.as_str().to_string()))
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }
}

struct ConcreteRelayRuntime {
    key: Option<BranchKey>,
    runtime: Runtime,
    domain: Domain,
    relay: Identifier,
    registry: RelayRegistry,
    services: Arc<RelayBoundaryServices>,
}

struct ConcreteRelayRuntimeBuild {
    key: Option<BranchKey>,
    runtime: Runtime,
    domain: Domain,
    relay: Identifier,
    registry: RelayRegistry,
    services: Arc<RelayBoundaryServices>,
}

#[derive(Debug)]
struct RelayBoundaryServices {
    fanout: RelayBoundaryFanout,
    attached_runtime_consumer_count: AtomicUsize,
    detached_runtime_consumer_count: AtomicUsize,
    remote_runtime_consumers: ArcSwap<Vec<RemoteRuntimeConsumer>>,
    remote_dispatcher: Option<Arc<RemoteDispatcher>>,
}

impl std::fmt::Debug for ConcreteRelayRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcreteStreamRuntime")
            .field("domain", &self.domain)
            .field("relay", &self.relay)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct RelayBoundaryBuilder {
    fanout: RelayBoundaryFanout,
    attached_runtime_consumer_count: usize,
    detached_runtime_consumer_count: usize,
    registry: RelayRegistry,
    remote_runtime_consumers: Vec<RemoteRuntimeConsumer>,
}

#[derive(Debug)]
struct RelayConsumerFanout {
    dispatch_gate: Arc<RelayDispatchGate>,
    subscriptions: RelayBroadcast<RelayRecordBatch>,
    attached_runtime_consumers: RelayBroadcast<RelayRecordBatch>,
    detached_runtime_consumers: RelayBroadcast<RelayRecordBatch>,
}

#[derive(Debug)]
struct BranchCollapseNode {
    fanout: RelayConsumerFanout,
}

#[derive(Debug, Clone)]
enum RelayBoundaryFanout {
    Direct(Arc<RelayConsumerFanout>),
    BranchCollapse(Arc<BranchCollapseNode>),
}

#[derive(Debug, Clone)]
struct RemoteRuntimeConsumer {
    node_id: String,
    relay: Identifier,
    mode: AckMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HttpRouteKey {
    host: String,
    path: String,
}

#[derive(Debug, Clone)]
struct EndpointRoute {
    path: String,
    hostnames: Vec<String>,
    endpoint_type: EndpointType,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
}

#[derive(Clone)]
struct EndpointIngestBinding {
    runtime_key: RuntimeKey,
    quiesce: Arc<IngestorQuiesceControl>,
    domain: Domain,
    ingestor: Identifier,
    timestamp_source: Option<IngestTimestampSource>,
    output_routes: RelayProcessorOutputsNode,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
    codec: Arc<CompiledCodec>,
    branched_senders: HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EndpointDispatchOutcome {
    pub accepted: usize,
    pub rejected: usize,
    pub retry_after: Option<Duration>,
}

impl EndpointDispatchOutcome {
    pub fn is_accepted(self) -> bool {
        self.accepted > 0
    }
}

struct IngestorDependencies {
    output_routes: RelayProcessorOutputsNode,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
    codec: Arc<CompiledCodec>,
    branched_templates: HashMap<Identifier, (SharedActiveGraph, IngestorRouteTemplate)>,
}

struct IngestGroupContext {
    domain: Domain,
    ingestor: Identifier,
    timestamp_source: Option<IngestTimestampSource>,
    output_routes: RelayProcessorOutputsNode,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
}

/// Decoded messages to add to a source-owned ingest group.
///
/// A source may contribute one poll batch or several consecutive single-record polls.
/// The collector owns the actual group boundary: request-scoped sources flush at the
/// end of the request, while streaming sources flush at the row or idle-time bound.
struct IngestGroupDispatch<'a> {
    domain: &'a Domain,
    ingestor: &'a Identifier,
    timestamp_source: Option<&'a IngestTimestampSource>,
    output_routes: &'a RelayProcessorOutputsNode,
    filter_where: Option<&'a CompiledProgramWithMaterializedInterest>,
    records: Vec<RuntimeRecordBatch>,
    /// Row-aligned with `records`, or empty when the source carries no ingest metadata.
    metadata: Vec<IngestFilterMapMetadata>,
    /// Row-aligned with `records`. An empty set is replaced by a tracked ack root.
    acks: Vec<AckSet>,
    ingested_at: Timestamp,
    /// Sources differ only in when they flush this group: stream sources use the
    /// size/idle-time bounds, while request-scoped sources flush at request completion.
    collector: &'a mut IngestRouteCollector,
}

struct IngestGroupContribution<'a> {
    domain: &'a Domain,
    ingestor: &'a Identifier,
    timestamp_source: Option<&'a IngestTimestampSource>,
    output_routes: &'a RelayProcessorOutputsNode,
    filter_where: Option<&'a CompiledProgramWithMaterializedInterest>,
    records: Vec<RuntimeRecordBatch>,
    metadata: Vec<IngestFilterMapMetadata>,
    acks: Vec<AckSet>,
    ingested_at: Timestamp,
}

struct RawIngestDispatch<'a> {
    domain: &'a Domain,
    ingestor: &'a Identifier,
    timestamp_source: Option<&'a IngestTimestampSource>,
    output_routes: &'a RelayProcessorOutputsNode,
    filter_where: Option<&'a CompiledProgramWithMaterializedInterest>,
    branched_senders: &'a HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
    codec: Arc<CompiledCodec>,
    payload: &'a BufferedIngestPayload,
    collector: &'a mut IngestRouteCollector,
    flush: bool,
}

/// Row-aligned ingest group state.
///
/// Records, ingest metadata and acks are only ever selected or dropped together, which
/// is what keeps a message error attributable to the record that produced it and keeps
/// each record's ack identity its own once the group has been filtered.
struct IngestGroupRows {
    batch: RuntimeRecordBatch,
    record_metadata: Vec<RuntimeRecordMetadata>,
    /// Empty when the source carries no ingest metadata, otherwise row-aligned.
    ingest_metadata: Vec<IngestFilterMapMetadata>,
    acks: Vec<AckSet>,
}

#[derive(Default)]
struct PendingIngestGroup {
    records: Vec<RuntimeRecordBatch>,
    /// Empty when the source carries no ingest metadata, otherwise row-aligned.
    metadata: Vec<IngestFilterMapMetadata>,
    acks: Vec<AckSet>,
    ingested_at: Vec<Timestamp>,
}

impl PendingIngestGroup {
    fn append(
        &mut self,
        records: Vec<RuntimeRecordBatch>,
        metadata: Vec<IngestFilterMapMetadata>,
        acks: Vec<AckSet>,
        ingested_at: Timestamp,
    ) -> Result<(), String> {
        let row_count = records.len();
        if !metadata.is_empty() && metadata.len() != row_count {
            return Err(format!(
                "received {} ingest metadata rows for {row_count} records",
                metadata.len()
            ));
        }
        if acks.len() != row_count {
            return Err(format!(
                "received {} ack sets for {row_count} records",
                acks.len()
            ));
        }
        if !self.records.is_empty() && self.metadata.is_empty() != metadata.is_empty() {
            return Err("cannot mix ingest rows with and without source metadata".to_string());
        }
        if records.iter().any(|record| record.batch().num_rows() != 1) {
            return Err(
                "decoded a message into a batch that does not contain exactly one row".to_string(),
            );
        }

        self.records.extend(records);
        self.metadata.extend(metadata);
        self.acks.extend(acks);
        self.ingested_at
            .extend(std::iter::repeat_n(ingested_at, row_count));
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn len(&self) -> usize {
        self.records.len()
    }

    fn into_rows(self) -> Result<IngestGroupRows, String> {
        debug_assert_eq!(self.records.len(), self.ingested_at.len());
        debug_assert_eq!(self.records.len(), self.acks.len());
        debug_assert!(self.metadata.is_empty() || self.metadata.len() == self.records.len());
        let batch_refs = self.records.iter().collect::<Vec<_>>();
        Ok(IngestGroupRows {
            batch: RuntimeRecordBatch::concat(&batch_refs)?,
            record_metadata: self
                .ingested_at
                .into_iter()
                .map(|ingested_at| {
                    RuntimeRecordMetadata::from_ingested_at_watermarks(ingested_at, ingested_at)
                })
                .collect(),
            ingest_metadata: self.metadata,
            acks: self.acks,
        })
    }
}

impl IngestGroupRows {
    fn len(&self) -> usize {
        self.batch.batch().num_rows()
    }

    fn is_empty(&self) -> bool {
        self.batch.batch().num_rows() == 0
    }

    fn metadata_row(&self, row: usize) -> Option<&IngestFilterMapMetadata> {
        self.ingest_metadata.get(row)
    }

    fn metadata_rows(&self) -> Option<&[IngestFilterMapMetadata]> {
        (!self.ingest_metadata.is_empty()).then_some(self.ingest_metadata.as_slice())
    }

    fn row(&self, row: usize) -> Result<RuntimeRow, String> {
        let metadata = self.record_metadata.get(row).cloned().ok_or_else(|| {
            format!(
                "ingest row {row} is outside metadata with {} rows",
                self.record_metadata.len()
            )
        })?;
        RuntimeRow::new(Arc::new(self.batch.clone()), row, metadata)
    }

    /// Keeps only the rows selected by `keep`, moving records, metadata and acks
    /// together so the three stay row-aligned.
    fn select(self, keep: &[bool]) -> Result<Self, String> {
        let selected = |row: usize| keep.get(row).copied().unwrap_or(false);
        let predicate = BooleanArray::from_iter((0..self.len()).map(|row| Some(selected(row))));
        Ok(Self {
            batch: self.batch.filter(&predicate)?,
            record_metadata: self
                .record_metadata
                .into_iter()
                .enumerate()
                .filter_map(|(row, metadata)| selected(row).then_some(metadata))
                .collect(),
            ingest_metadata: self
                .ingest_metadata
                .into_iter()
                .enumerate()
                .filter_map(|(row, metadata)| selected(row).then_some(metadata))
                .collect(),
            acks: self
                .acks
                .into_iter()
                .enumerate()
                .filter_map(|(row, acks)| selected(row).then_some(acks))
                .collect(),
        })
    }
}

/// An ingestor `FILTER WHERE` message error, with the row it came from.
struct IngestorFilterWhereError<'a> {
    domain: &'a Domain,
    ingestor: &'a Identifier,
    output_routes: &'a RelayProcessorOutputsNode,
    record: &'a RuntimeRow,
    ingest_metadata: Option<&'a IngestFilterMapMetadata>,
    acks: AckSet,
    error: StructuredMessageError,
    materialized_state: HashMap<String, RuntimeValue>,
}

/// Accumulates one source ingest group before program execution, then holds its routed
/// messages long enough to build one Arrow batch per (relay, branch key).
#[derive(Default)]
struct IngestRouteCollector {
    context: Option<IngestGroupContext>,
    pending: PendingIngestGroup,
    routed: Vec<(Identifier, RelayMessage)>,
    flush_at: Option<Instant>,
}

impl IngestRouteCollector {
    fn collect(&mut self, contribution: IngestGroupContribution<'_>) -> Result<(), String> {
        let IngestGroupContribution {
            domain,
            ingestor,
            timestamp_source,
            output_routes,
            filter_where,
            records,
            metadata,
            acks,
            ingested_at,
        } = contribution;
        if records.is_empty() {
            return Ok(());
        }
        if let Some(existing) = self.context.as_ref()
            && (existing.domain != *domain || existing.ingestor != *ingestor)
        {
            return Err(format!(
                "ingest group for '{}.{}' cannot collect rows for '{}.{}'",
                existing.domain.as_str(),
                existing.ingestor.as_str(),
                domain.as_str(),
                ingestor.as_str()
            ));
        }
        self.pending.append(records, metadata, acks, ingested_at)?;
        if self.context.is_none() {
            self.context = Some(IngestGroupContext {
                domain: domain.clone(),
                ingestor: ingestor.clone(),
                timestamp_source: timestamp_source.cloned(),
                output_routes: output_routes.clone(),
                filter_where: filter_where.cloned(),
            });
        }
        self.flush_at = Some(Instant::now() + INGEST_GROUP_IDLE_FLUSH);
        Ok(())
    }

    fn take_pending(&mut self) -> Result<Option<(IngestGroupContext, IngestGroupRows)>, String> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        self.flush_at = None;
        let context = self
            .context
            .take()
            .expect("a non-empty ingest group must retain its execution context");
        let pending = std::mem::take(&mut self.pending);
        Ok(Some((context, pending.into_rows()?)))
    }

    fn push(&mut self, relay: Identifier, message: RelayMessage) {
        self.routed.push((relay, message));
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.routed.is_empty()
    }

    fn len(&self) -> usize {
        self.pending.len()
    }

    fn next_flush(&self) -> Option<Instant> {
        self.flush_at
    }

    /// Groups by (relay, branch key) preserving arrival order within each group.
    /// `RelayRecordBatch::from_messages` requires a uniform key per batch.
    fn drain_groups(&mut self) -> Vec<(Identifier, Vec<RelayMessage>)> {
        let mut groups: Vec<(Identifier, Option<BranchKey>, Vec<RelayMessage>)> = Vec::new();
        let mut group_indices: HashMap<(Identifier, Option<BranchKey>), usize> = HashMap::default();
        for (relay, message) in self.routed.drain(..) {
            let group_key = (relay.clone(), message.key.clone());
            if let Some(index) = group_indices.get(&group_key).copied() {
                groups[index].2.push(message);
            } else {
                group_indices.insert(group_key, groups.len());
                groups.push((relay, message.key.clone(), vec![message]));
            }
        }
        groups
            .into_iter()
            .map(|(relay, _, messages)| (relay, messages))
            .collect()
    }
}

#[derive(Clone, Default)]
struct IngestorRouteRuntimes {
    runtimes: Vec<Arc<IngestorRouteRuntime>>,
    senders: HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
}

type BranchedEntrypointInput = RelayRecordBatch;

struct BranchedEntrypointBatch {
    batch: RuntimeRecordBatch,
    metadata: Vec<RuntimeRecordMetadata>,
    keys: Vec<Option<BranchKey>>,
    acks: Vec<AckSet>,
}

#[derive(Clone)]
struct BranchedBranchSelection {
    key: Option<BranchKey>,
    rows: Vec<usize>,
}

struct BranchedBranchPlan {
    selections: Vec<BranchedBranchSelection>,
    valid_rows: Vec<(Option<BranchKey>, usize)>,
}

impl BranchedEntrypointBatch {
    fn from_inputs(inputs: Vec<BranchedEntrypointInput>) -> Result<Self, (String, Vec<AckSet>)> {
        if inputs.is_empty() {
            return Err((
                "cannot build branch batch from zero inputs".to_string(),
                Vec::new(),
            ));
        }
        let mut batches = Vec::<RuntimeRecordBatch>::new();
        let mut metadata = Vec::<RuntimeRecordMetadata>::new();
        let mut keys = Vec::<Option<BranchKey>>::new();
        let mut acks = Vec::<AckSet>::new();

        for input in inputs {
            let (runtime_batch, batch_metadata, batch_keys, batch_acks) =
                input.into_unkeyed_parts();
            batches.push(runtime_batch);
            metadata.extend(batch_metadata);
            keys.extend(batch_keys);
            acks.extend(batch_acks);
        }
        let batch_refs = batches.iter().collect::<Vec<_>>();
        let batch = RuntimeRecordBatch::concat(&batch_refs).map_err(|error| {
            (
                format!("failed to concatenate branch input batches: {error}"),
                acks.clone(),
            )
        })?;
        let row_count = batch.batch().num_rows();
        if metadata.len() != row_count || keys.len() != row_count || acks.len() != row_count {
            return Err((
                format!(
                    "branch input batch row count {row_count} does not match metadata {}, branch \
                     keys {}, acks {}",
                    metadata.len(),
                    keys.len(),
                    acks.len()
                ),
                acks,
            ));
        }

        Ok(Self {
            batch,
            metadata,
            keys,
            acks,
        })
    }

    fn branch_selections(&self) -> Result<BranchedBranchPlan, String> {
        let mut selections = Vec::<BranchedBranchSelection>::new();
        let mut positions = HashMap::<Option<BranchKey>, usize>::default();
        let mut valid_rows = Vec::new();
        for index in 0..self.metadata.len() {
            let key = self.keys.get(index).cloned().flatten();
            valid_rows.push((key.clone(), index));
            if let Some(position) = positions.get(&key).copied() {
                selections[position].rows.push(index);
                continue;
            }
            positions.insert(key.clone(), selections.len());
            selections.push(BranchedBranchSelection {
                key,
                rows: vec![index],
            });
        }

        Ok(BranchedBranchPlan {
            selections,
            valid_rows,
        })
    }

    fn filter_branch(
        &self,
        selection: BranchedBranchSelection,
        ack_boundary: BranchInstanceAckBoundary,
    ) -> Result<RelayRecordBatch, (String, Vec<AckSet>)> {
        let predicate = self
            .branch_predicate(&selection)
            .map_err(|error| (error, self.acks.clone()))?;
        let selected_rows = selected_rows(&predicate);
        let filtered_batch = self
            .batch
            .filter(&predicate)
            .map_err(|error| (error, self.acks.clone()))?;
        let mut metadata = Vec::with_capacity(selected_rows.len());
        let mut acks = Vec::with_capacity(selected_rows.len());
        for row in selected_rows {
            metadata.push(self.metadata[row].clone());
            acks.push(match ack_boundary {
                BranchInstanceAckBoundary::Preserve => self.acks[row].clone(),
                BranchInstanceAckBoundary::Reingestor(AckMode::Attached) => {
                    let forwarded = self.acks[row].attached();
                    self.acks[row].ack_success();
                    forwarded
                }
                BranchInstanceAckBoundary::Reingestor(AckMode::Detached) => {
                    self.acks[row].ack_success();
                    AckSet::empty()
                }
            });
        }
        RelayRecordBatch::from_filtered_parts(selection.key, filtered_batch, metadata, acks)
            .map_err(|error| (error, self.acks.clone()))
    }

    fn branch_predicate(
        &self,
        selection: &BranchedBranchSelection,
    ) -> Result<BooleanArray, String> {
        let row_count = self.batch.batch().num_rows();
        let mut selected = vec![false; row_count];
        for row in &selection.rows {
            let Some(value) = selected.get_mut(*row) else {
                return Err(format!(
                    "branch selection row {row} is outside batch with {row_count} rows"
                ));
            };
            *value = true;
        }
        Ok(BooleanArray::from(selected))
    }
}

fn selected_rows(predicate: &BooleanArray) -> Vec<usize> {
    (0..predicate.len())
        .filter(|row| predicate.is_valid(*row) && predicate.value(*row))
        .collect()
}

struct MessageErrorContext<'a> {
    domain: &'a Domain,
    node_kind: &'a str,
    node: &'a Identifier,
    source_route: Option<&'a Identifier>,
    message: &'a RelayMessage,
    error: &'a StructuredMessageError,
    partial_output: Option<&'a RuntimeRecordBatch>,
    materialized_state: &'a HashMap<String, RuntimeValue>,
    ingest_metadata: Option<&'a IngestFilterMapMetadata>,
}

struct MessageErrorHandling<'a> {
    domain: &'a Domain,
    node_kind: &'a str,
    node: &'a Identifier,
    source_route: Option<&'a Identifier>,
    policy: &'a MessageErrorPolicy,
    message: RelayMessage,
    error: StructuredMessageError,
    partial_output: Option<RuntimeRecordBatch>,
    materialized_state: HashMap<String, RuntimeValue>,
    ingest_metadata: Option<&'a IngestFilterMapMetadata>,
}

struct MessageErrorFailure {
    source_route: Option<Identifier>,
    reason: String,
    operation: MessageErrorOperation,
}

impl MessageErrorFailure {
    fn publish(source_route: Option<&Identifier>, reason: String) -> Self {
        Self::new(source_route, reason, MessageErrorOperation::Publish)
    }

    fn new(
        source_route: Option<&Identifier>,
        reason: String,
        operation: MessageErrorOperation,
    ) -> Self {
        Self {
            source_route: source_route.cloned(),
            reason,
            operation,
        }
    }
}

#[derive(Debug, Clone)]
struct MessageErrorCompileSchemas {
    input: Option<Arc<CompiledSchema>>,
    left: Option<Arc<CompiledSchema>>,
    right: Option<Arc<CompiledSchema>>,
    partial_output: Option<Arc<CompiledSchema>>,
    current_branching: Vec<Identifier>,
    allow_header_reads: bool,
}

#[derive(Debug)]
enum SingleRecordFilterMapOutcome {
    Filtered,
    Output(RuntimeRow),
    MessageError {
        error: StructuredMessageError,
        partial_output: Option<RuntimeRecordBatch>,
        materialized_state: HashMap<String, RuntimeValue>,
    },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct IngestFilterMapMetadata {
    values: HashMap<String, RuntimeValue>,
    headers: HashMap<String, Vec<String>>,
}

impl IngestFilterMapMetadata {
    pub(crate) fn from_headers(headers: IngestHeaders) -> Self {
        let mut metadata = Self::default();
        for (name, value) in headers {
            metadata.insert_header(name, value);
        }
        metadata
    }

    fn kafka(
        topic: String,
        partition: i32,
        offset: i64,
        _key: Option<String>,
        headers: IngestHeaders,
    ) -> Self {
        let mut metadata = Self::from_headers(headers);
        metadata
            .values
            .insert("topic".to_string(), RuntimeValue::String(topic));
        metadata
            .values
            .insert("partition".to_string(), RuntimeValue::I32(partition));
        metadata
            .values
            .insert("offset".to_string(), RuntimeValue::I64(offset));
        metadata
    }

    fn insert_header(&mut self, name: String, value: String) {
        self.headers.entry(name).or_default().push(value);
    }

    fn metadata_value(&self, name: &str) -> Option<&RuntimeValue> {
        self.values.get(name)
    }
}

#[derive(Debug)]
struct IngestHeaderFunctionInjector {
    rows: Vec<HashMap<String, Vec<String>>>,
}

impl IngestHeaderFunctionInjector {
    fn from_metadata(
        metadata: Option<&[IngestFilterMapMetadata]>,
        row_count: usize,
    ) -> Arc<Box<dyn VmFunctionInjector>> {
        let rows = metadata
            .map(|metadata| {
                metadata
                    .iter()
                    .map(|row| row.headers.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![HashMap::new(); row_count]);
        Arc::new(Box::new(Self { rows }))
    }
}

impl VmFunctionInjector for IngestHeaderFunctionInjector {
    fn inject(
        &self,
        function: &FunctionName,
        arguments: &[VmTypedArray],
        row_count: usize,
        _span: nervix_nspl::vm_program::Span,
    ) -> Result<VmTypedArray, nervix_vm::RuntimeError> {
        let [VmTypedArray::Utf8(names)] = arguments else {
            return Err(nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "function '{}' requires one STRING argument",
                    function.as_str()
                ),
            });
        };
        if self.rows.len() != row_count || names.len() != row_count {
            return Err(nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "function '{}' header context has {} rows for a {row_count}-row batch",
                    function.as_str(),
                    self.rows.len()
                ),
            });
        }
        if let FunctionName::ReadHeader = function {
            let values = names
                .iter()
                .zip(&self.rows)
                .map(|(name, headers)| {
                    name.and_then(|name| headers.get(name))
                        .and_then(|values| values.first())
                        .map(String::as_str)
                })
                .collect::<Vec<_>>();
            return Ok(VmTypedArray::Utf8(arrow_array::StringArray::from(values)));
        }
        if let FunctionName::ReadHeaders = function {
            let field = StdArc::new(arrow_schema::Field::new("item", ArrowDataType::Utf8, false));
            let mut builder = ListBuilder::new(StringBuilder::new()).with_field(field);
            for (name, headers) in names.iter().zip(&self.rows) {
                if let Some(values) = name.and_then(|name| headers.get(name)) {
                    for value in values {
                        builder.values().append_value(value);
                    }
                }
                builder.append(true);
            }
            return Ok(VmTypedArray::Generic(StdArc::new(builder.finish())));
        }
        Err(nervix_vm::RuntimeError::InvalidBatch {
            message: format!("function '{}' is not injectable", function.as_str()),
        })
    }
}

type RelayBoundaryFanoutMap = Arc<DashMap<(Domain, Identifier), RelayBoundaryFanout, RandomState>>;
type RelayRuntimeConsumerReceiver = RelaySubscriptionReceiver<RelayRecordBatch>;

struct RelayRuntimeFanIn {
    receiver: RelayRuntimeConsumerReceiver,
}

impl RelayConsumerFanout {
    fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            dispatch_gate: Arc::new(RelayDispatchGate::new()),
            subscriptions: RelayBroadcast::with_capacity(capacity),
            attached_runtime_consumers: RelayBroadcast::with_capacity(capacity),
            detached_runtime_consumers: RelayBroadcast::with_capacity(capacity),
        }
    }

    fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.subscriptions.new_receiver()
    }

    fn set_capacity(&self, capacity: NonZeroUsize) {
        self.subscriptions.set_capacity(capacity);
        self.attached_runtime_consumers.set_capacity(capacity);
        self.detached_runtime_consumers.set_capacity(capacity);
    }

    fn runtime_consumer_receiver_for_mode(&self, mode: AckMode) -> RelayRuntimeConsumerReceiver {
        self.runtime_consumer_broadcast_for_mode(mode)
            .new_receiver()
    }

    fn dispatch_gate(&self) -> Arc<RelayDispatchGate> {
        self.dispatch_gate.clone()
    }

    fn runtime_consumer_buffer_len(&self) -> usize {
        self.attached_runtime_consumers
            .len()
            .saturating_add(self.detached_runtime_consumers.len())
    }

    fn runtime_consumer_broadcast_for_mode(
        &self,
        mode: AckMode,
    ) -> &RelayBroadcast<RelayRecordBatch> {
        match mode {
            AckMode::Attached => &self.attached_runtime_consumers,
            AckMode::Detached => &self.detached_runtime_consumers,
        }
    }

    #[cfg(test)]
    fn runtime_consumer_buffer_len_for_mode(&self, mode: AckMode) -> usize {
        self.runtime_consumer_broadcast_for_mode(mode).len()
    }

    fn observe_buffer_lengths(
        &self,
        metrics: &RuntimeMetrics,
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        branch_key: Option<&BranchKey>,
    ) {
        let buffers = [
            (
                self.subscriptions.receiver_count(),
                self.subscriptions.len(),
                self.subscriptions.capacity(),
            ),
            (
                self.attached_runtime_consumers.receiver_count(),
                self.attached_runtime_consumers.len(),
                self.attached_runtime_consumers.capacity(),
            ),
            (
                self.detached_runtime_consumers.receiver_count(),
                self.detached_runtime_consumers.len(),
                self.detached_runtime_consumers.capacity(),
            ),
        ];
        let Some((len, capacity)) = buffers
            .into_iter()
            .filter(|(receivers, _, _)| *receivers > 0)
            .map(|(_, len, capacity)| (len, capacity))
            .max_by_key(|(len, _)| *len)
        else {
            return;
        };
        let observation = RelayBufferObservation {
            domain,
            relay,
            physical_node_id,
            direction: RELAY_BUFFER_DIRECTION_CONCRETE,
            len,
            capacity,
        };
        metrics.observe_global_relay_buffer_len(observation);
        if let Some(branch_key) = branch_key {
            metrics.observe_branch_relay_buffer_len(branch_key.as_str(), observation);
        }
    }

    async fn fanout_subscriptions(&self, batch: &RelayRecordBatch) {
        if self.subscriptions.receiver_count() == 0 {
            return;
        }
        let _ = self.subscriptions.broadcast(batch.detached()).await;
    }

    async fn dispatch_runtime_consumers(
        &self,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let _dispatch_permit = self.dispatch_gate.acquire_dispatch().await;
        let attached_receiver_count = self
            .runtime_consumer_broadcast_for_mode(AckMode::Attached)
            .receiver_count();
        if attached_runtime_consumer_count > 0
            && attached_receiver_count < attached_runtime_consumer_count
        {
            for ack in batch.acks.iter() {
                ack.no_ack("runtime consumer unavailable for attached delivery");
            }
            return Err(Box::new(batch.clone()));
        }
        if attached_receiver_count > 0 {
            let attached = batch.attached_for_receivers(attached_receiver_count);
            if let Err(error) = self
                .runtime_consumer_broadcast_for_mode(AckMode::Attached)
                .broadcast(attached)
                .await
            {
                let failed = error.0;
                for ack in failed.acks.iter() {
                    ack.no_ack("runtime consumer unavailable for attached delivery");
                }
                return Err(Box::new(batch.clone()));
            }
        }

        let detached_receiver_count = self
            .runtime_consumer_broadcast_for_mode(AckMode::Detached)
            .receiver_count();
        if detached_runtime_consumer_count > 0
            && detached_receiver_count < detached_runtime_consumer_count
        {
            warn!("detached runtime consumer receiver is unavailable");
        }
        if detached_receiver_count > 0 {
            let detached = batch.detached();
            if let Err(error) = self
                .runtime_consumer_broadcast_for_mode(AckMode::Detached)
                .broadcast(detached)
                .await
            {
                warn!(
                    error = %error,
                    "detached runtime consumer relay broadcast failed"
                );
            }
        }

        Ok(())
    }
}

impl BranchCollapseNode {
    fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            fanout: RelayConsumerFanout::with_capacity(capacity),
        }
    }

    fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.fanout.subscription_receiver()
    }

    fn set_capacity(&self, capacity: NonZeroUsize) {
        self.fanout.set_capacity(capacity);
    }

    fn runtime_consumer_receiver_for_mode(&self, mode: AckMode) -> RelayRuntimeConsumerReceiver {
        self.fanout.runtime_consumer_receiver_for_mode(mode)
    }

    fn observe_buffer_lengths(
        &self,
        metrics: &RuntimeMetrics,
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        branch_key: Option<&BranchKey>,
    ) {
        self.fanout
            .observe_buffer_lengths(metrics, domain, relay, physical_node_id, branch_key);
    }

    async fn fanout_subscriptions(&self, batch: &RelayRecordBatch) {
        self.fanout.fanout_subscriptions(batch).await;
    }

    async fn dispatch_runtime_consumers(
        &self,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout
            .dispatch_runtime_consumers(
                attached_runtime_consumer_count,
                detached_runtime_consumer_count,
                batch,
            )
            .await
    }
}

impl RelayBoundaryFanout {
    fn direct_with_capacity(capacity: NonZeroUsize) -> Self {
        Self::Direct(Arc::new(RelayConsumerFanout::with_capacity(capacity)))
    }

    fn branch_collapse_with_capacity(capacity: NonZeroUsize) -> Self {
        Self::BranchCollapse(Arc::new(BranchCollapseNode::with_capacity(capacity)))
    }

    fn uses_branch_collapse(&self) -> bool {
        match self {
            Self::Direct(_) => false,
            Self::BranchCollapse(_) => true,
        }
    }

    fn set_capacity(&self, capacity: NonZeroUsize) {
        match self {
            Self::Direct(fanout) => fanout.set_capacity(capacity),
            Self::BranchCollapse(branch_collapse) => branch_collapse.set_capacity(capacity),
        }
    }

    fn dispatch_gate(&self) -> Arc<RelayDispatchGate> {
        match self {
            Self::Direct(fanout) => fanout.dispatch_gate(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.dispatch_gate(),
        }
    }

    fn runtime_consumer_buffer_len(&self) -> usize {
        match self {
            Self::Direct(fanout) => fanout.runtime_consumer_buffer_len(),
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.fanout.runtime_consumer_buffer_len()
            }
        }
    }

    fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        match self {
            Self::Direct(fanout) => fanout.subscription_receiver(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.subscription_receiver(),
        }
    }

    fn runtime_consumer_receiver_for_mode(&self, mode: AckMode) -> RelayRuntimeConsumerReceiver {
        match self {
            Self::Direct(fanout) => fanout.runtime_consumer_receiver_for_mode(mode),
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.runtime_consumer_receiver_for_mode(mode)
            }
        }
    }

    #[cfg(test)]
    fn runtime_consumer_buffer_len_for_mode(&self, mode: AckMode) -> usize {
        match self {
            Self::Direct(fanout) => fanout.runtime_consumer_buffer_len_for_mode(mode),
            Self::BranchCollapse(branch_collapse) => branch_collapse
                .fanout
                .runtime_consumer_buffer_len_for_mode(mode),
        }
    }

    fn observe_buffer_lengths(
        &self,
        metrics: &RuntimeMetrics,
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        branch_key: Option<&BranchKey>,
    ) {
        match self {
            Self::Direct(fanout) => {
                fanout.observe_buffer_lengths(metrics, domain, relay, physical_node_id, branch_key);
            }
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.observe_buffer_lengths(
                    metrics,
                    domain,
                    relay,
                    physical_node_id,
                    branch_key,
                );
            }
        }
    }

    async fn fanout_subscriptions(&self, batch: &RelayRecordBatch) {
        match self {
            Self::Direct(fanout) => fanout.fanout_subscriptions(batch).await,
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.fanout_subscriptions(batch).await;
            }
        }
    }

    async fn dispatch_runtime_consumers(
        &self,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        match self {
            Self::Direct(fanout) => {
                fanout
                    .dispatch_runtime_consumers(
                        attached_runtime_consumer_count,
                        detached_runtime_consumer_count,
                        batch,
                    )
                    .await
            }
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse
                    .dispatch_runtime_consumers(
                        attached_runtime_consumer_count,
                        detached_runtime_consumer_count,
                        batch,
                    )
                    .await
            }
        }
    }
}

impl RelayRuntimeFanIn {
    fn new(receiver: RelayRuntimeConsumerReceiver) -> Self {
        Self { receiver }
    }

    #[cfg(test)]
    async fn recv(&mut self) -> Option<RelayRecordBatch> {
        tokio::task::consume_budget().await;
        match self.receiver.recv().await {
            Ok(batch) => Some(batch),
            Err(async_broadcast::RecvError::Overflowed(_)) => {
                unreachable!("relay broadcasts are backpressured and must not overflow")
            }
            Err(async_broadcast::RecvError::Closed) => None,
        }
    }

    fn try_recv(&mut self) -> Result<RelayRecordBatch, async_broadcast::TryRecvError> {
        self.receiver.try_recv()
    }

    fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<RelayRecordBatch>> {
        match self.receiver.poll_recv(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => std::task::Poll::Ready(Some(batch)),
            std::task::Poll::Ready(Some(Err(async_broadcast::RecvError::Overflowed(_)))) => {
                unreachable!("relay broadcasts are backpressured and must not overflow")
            }
            std::task::Poll::Ready(Some(Err(async_broadcast::RecvError::Closed)) | None) => {
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn pending_len(&self) -> usize {
        self.receiver.len()
    }

    /// Polls one relay batch while keeping quiesce accounting continuous across dequeue.
    fn poll_recv_with_quiesce(
        &mut self,
        cx: &mut std::task::Context<'_>,
        counters: Option<&Arc<NodeQuiesceCounters>>,
    ) -> std::task::Poll<Option<(RelayRecordBatch, Option<NodeQuiesceWorkGuard>)>> {
        let work = counters.map(|counters| NodeQuiesceWorkGuard::begin(counters.clone()));
        match self.poll_recv(cx) {
            std::task::Poll::Ready(Some(batch)) => std::task::Poll::Ready(Some((batch, work))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    /// Tries one relay batch while keeping quiesce accounting continuous across dequeue.
    fn try_recv_with_quiesce(
        &mut self,
        counters: Option<&Arc<NodeQuiesceCounters>>,
    ) -> Result<(RelayRecordBatch, Option<NodeQuiesceWorkGuard>), async_broadcast::TryRecvError>
    {
        let work = counters.map(|counters| NodeQuiesceWorkGuard::begin(counters.clone()));
        self.try_recv().map(|batch| (batch, work))
    }
}

const DOMAIN_TICK_HISTORY_LIMIT: usize = 256;

#[derive(Debug, Clone)]
struct ObservedDomainTick {
    tick_id: u64,
    logical_timestamp: Timestamp,
    wall_clock: Timestamp,
}

#[derive(Debug, Clone)]
struct RuntimeDomainClockState {
    logical_started_at: Timestamp,
    wall_started_at: Timestamp,
    time_rate: String,
}

#[derive(Debug)]
struct RuntimeDomainState {
    config: DomainConfig,
    status: nervix_models::DomainStatus,
    start_version: u64,
    last_start: nervix_models::DomainStartPoint,
    clock: Option<RuntimeDomainClockState>,
    ticks: parking_lot::Mutex<VecDeque<ObservedDomainTick>>,
}

#[derive(Debug)]
struct IngestorReadiness {
    expected_instances: u64,
    ready_instances: BTreeSet<u64>,
}

impl IngestorReadiness {
    fn new(expected_instances: u64) -> Self {
        Self {
            expected_instances,
            ready_instances: BTreeSet::new(),
        }
    }

    fn is_ready(&self) -> bool {
        self.expected_instances > 0 && self.ready_instances.len() as u64 >= self.expected_instances
    }
}

struct BranchRuntime {
    key: Option<BranchKey>,
    runtime: Runtime,
    domain: Domain,
    source_kind: ModelKind,
    source: Identifier,
    root_relay: Identifier,
    error_policies: ErrorPolicies,
    relays: HashMap<Identifier, ConcreteRelayRuntime>,
    materializers: HashMap<Identifier, Arc<ReplicatedMaterializedRelayState>>,
    materializer_epoch: Option<u64>,
    processors: HashMap<Identifier, RelayProcessorNode>,
}

fn output_error_policies(
    policy: &MessageErrorPolicy,
    general: GeneralErrorPolicy,
) -> ErrorPolicies {
    ErrorPolicies {
        message: policy.clone(),
        general,
    }
}

fn internal_processor_error_policies(general: GeneralErrorPolicy) -> ErrorPolicies {
    ErrorPolicies {
        message: MessageErrorPolicy::Log,
        general,
    }
}

struct BranchExecutionRuntime {
    domain: Domain,
    ingestor: Identifier,
    sender: mpsc::Sender<BranchedEntrypointInput>,
    shutdown: watch::Sender<bool>,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

struct IngestorRouteRuntime {
    sender: mpsc::Sender<BranchedEntrypointInput>,
    shutdown: watch::Sender<bool>,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
    branch_runtime: Arc<BranchExecutionRuntime>,
}

struct PendingIngestorRouteBatch {
    batches: Vec<RelayRecordBatch>,
    estimated_bytes: u64,
    flush_at: Instant,
}

struct IngestorRouteTask {
    runtime_handle: Runtime,
    domain: Domain,
    ingestor: Identifier,
    template: IngestorRouteTemplate,
    branch_sender: mpsc::Sender<BranchedEntrypointInput>,
    pending: HashMap<Option<BranchKey>, PendingIngestorRouteBatch>,
}

struct BranchExecutionDispatchContext<'a> {
    runtime_handle: &'a Runtime,
    domain: &'a Domain,
    ingestor: &'a Identifier,
    graph: &'a SharedActiveGraph,
    template: &'a BranchInstanceTemplate,
    now: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaDomainOffsetDescribe {
    pub topic: String,
    pub instances: u64,
    pub observed_partitions: Vec<i32>,
    pub rebalance_epoch: u64,
    pub instance_assignments: Vec<Vec<i32>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestorDescribe {
    pub running: bool,
    pub ready: bool,
    pub quiesce_state: Option<String>,
    pub quiesce_counters: IngestorQuiesceCounters,
    pub memory_backpressure_paused: bool,
    pub transient_error: Option<String>,
    pub reconnect_backoff: Option<String>,
    pub reconnect_wait_millis: Option<u64>,
    pub kafka_domain_offsets: Option<KafkaDomainOffsetDescribe>,
}

#[derive(Debug, Clone)]
struct RuntimeReconnectStatus {
    backoff: Duration,
    retry_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmitterRetryKind {
    Infrastructure,
    IcebergCommit,
}

#[derive(Debug, Clone)]
struct EmitterRetryStatus {
    kind: EmitterRetryKind,
    reconnect: RuntimeReconnectStatus,
}

struct EmitterConfirmationWaitGuard {
    active_waits: Arc<AtomicUsize>,
}

impl Drop for EmitterConfirmationWaitGuard {
    fn drop(&mut self) {
        self.active_waits.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct RuntimeReconnectBackoff {
    initial: Duration,
    next: Duration,
    max: Duration,
}

impl Default for RuntimeReconnectBackoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(250),
            next: Duration::from_millis(250),
            max: Duration::from_secs(30),
        }
    }
}

impl RuntimeReconnectBackoff {
    pub(in crate::runtime) fn from_policy(policy: ParsedRetryPolicy) -> Self {
        Self {
            initial: policy.backoff,
            next: policy.backoff,
            max: policy.max_backoff,
        }
    }

    pub(in crate::runtime) fn reset(&mut self) {
        self.next = self.initial;
    }

    pub(in crate::runtime) fn next_delay(&self) -> Duration {
        self.next
    }

    pub(in crate::runtime) fn take_next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.max);
        delay
    }

    pub(in crate::runtime) async fn wait(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> bool {
        let delay = self.take_next_delay();
        tokio::select! {
            changed = shutdown_rx.changed() => {
                !(changed.is_err() || *shutdown_rx.borrow())
            }
            _ = sleep(delay) => true,
        }
    }
    pub(in crate::runtime) async fn wait_with_ack_alive(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &AckSet,
    ) -> bool {
        let delay = self.take_next_delay();
        Self::wait_duration_with_ack_alive(delay, shutdown_rx, acks).await
    }

    pub(in crate::runtime) async fn wait_duration_with_ack_alive(
        delay: Duration,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &AckSet,
    ) -> bool {
        let deadline = Instant::now() + delay;
        loop {
            tokio::task::consume_budget().await;
            acks.ack_alive();
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                return true;
            }
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    return !(changed.is_err() || *shutdown_rx.borrow());
                }
                _ = sleep(remaining.min(Duration::from_millis(100))) => {}
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeFlushPolicy {
    Each {
        interval: Duration,
        max_batch_size: u64,
    },
    Immediate,
}

impl RuntimeFlushPolicy {
    const IMMEDIATE_MINIMUM_TIMEOUT: Duration = Duration::from_micros(100);

    fn interval(self) -> Duration {
        match self {
            Self::Each { interval, .. } => interval,
            Self::Immediate => Self::IMMEDIATE_MINIMUM_TIMEOUT,
        }
    }

    fn size_boundary_reached(self, pending_bytes: u64) -> bool {
        match self {
            Self::Each { max_batch_size, .. } => pending_bytes >= max_batch_size,
            Self::Immediate => false,
        }
    }
}

fn branched_entrypoint_inputs_acks(inputs: &[BranchedEntrypointInput]) -> Vec<AckSet> {
    inputs
        .iter()
        .flat_map(|input| input.acks.iter().cloned())
        .collect()
}

async fn branched_entrypoint_batch_from_inputs_blocking(
    inputs: Vec<BranchedEntrypointInput>,
) -> Result<Arc<BranchedEntrypointBatch>, (String, Vec<AckSet>)> {
    let acks = branched_entrypoint_inputs_acks(&inputs);
    match tokio::task::spawn_blocking(move || BranchedEntrypointBatch::from_inputs(inputs)).await {
        Ok(Ok(batch)) => Ok(Arc::new(batch)),
        Ok(Err(error)) => Err(error),
        Err(error) => Err((
            format!("branch input batch build task failed: {error}"),
            acks,
        )),
    }
}

async fn branched_branch_plan_blocking(
    input: Arc<BranchedEntrypointBatch>,
) -> Result<BranchedBranchPlan, String> {
    input.branch_selections()
}

async fn branched_branch_filter_blocking(
    input: Arc<BranchedEntrypointBatch>,
    selection: BranchedBranchSelection,
    ack_boundary: BranchInstanceAckBoundary,
) -> Result<(Option<BranchKey>, RelayRecordBatch), (String, Vec<AckSet>)> {
    let failure_input = input.clone();
    let key = selection.key.clone();
    match tokio::task::spawn_blocking(move || {
        input
            .filter_branch(selection, ack_boundary)
            .map(|batch| (key, batch))
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err((
            format!("branch filter task failed: {error}"),
            failure_input.acks.clone(),
        )),
    }
}

#[derive(Debug)]
struct ExpiringRelayState {
    registry: RelayRegistry,
}

#[derive(Debug, Clone, Copy)]
struct ExecutionBuildDeps<'a> {
    domain: &'a Domain,
    relay_schemas: &'a HashMap<Identifier, Arc<CompiledSchema>>,
    relay_branchings: &'a HashMap<Identifier, Vec<Identifier>>,
    materialized_relay_specs: &'a HashMap<Identifier, RuntimeMaterializedRelaySpec>,
    materialized_relay_owner_nodes: &'a HashMap<Identifier, Option<String>>,
    lookups: &'a HashMap<Identifier, Arc<LookupRuntime>>,
}

#[derive(Debug, Clone)]
struct EmitterTaskDeps {
    input_schema: Arc<CompiledSchema>,
    input_branching: Vec<Identifier>,
    materialized_relay_specs: HashMap<Identifier, RuntimeMaterializedRelaySpec>,
    materialized_relay_owner_nodes: HashMap<Identifier, Option<String>>,
    lookups: HashMap<Identifier, Arc<LookupRuntime>>,
}

#[derive(Debug, Clone)]
struct EmitterTaskBuildDeps<'a> {
    domain: &'a Domain,
    shutdown_tx: &'a watch::Sender<bool>,
    codecs: &'a HashMap<Identifier, Arc<CompiledCodec>>,
    clients: &'a HashMap<Identifier, Arc<Model>>,
    deps: EmitterTaskDeps,
}

/// One materialized relay's runtime task: the relay it serves, the replicated state it maintains,
/// the branch retention limits it enforces, and the fan-in it consumes.
struct MaterializerTaskSpec {
    relay: Identifier,
    state: Arc<ReplicatedMaterializedRelayState>,
    branch_ttl: Option<Duration>,
    branch_capacity: Option<usize>,
    receiver: RelayRuntimeFanIn,
}

struct GeneratorTaskSpec {
    generator: CreateGenerator,
    source_relay: Identifier,
    source_branching: Vec<Identifier>,
    routes: Vec<GeneratorTaskRouteSpec>,
}

struct GeneratorTaskRouteSpec {
    output: ProcessorOutput,
    program: CompiledProgramWithMaterializedInterest,
    output_schema: Arc<CompiledSchema>,
    output_registry: RelayRegistry,
    output_services: Arc<RelayBoundaryServices>,
}

#[derive(Default)]
struct GeneratorBranchTaskState {
    next_generation: Option<Timestamp>,
    routes: Vec<GeneratorRouteBranchTaskState>,
}

#[derive(Default)]
struct GeneratorRouteBranchTaskState {
    next_flush: Option<Timestamp>,
    pending: Vec<RelayMessage>,
}

#[derive(Debug)]
struct WindowEntry {
    sequence: u64,
    timestamp: Timestamp,
    message: RelayMessage,
    aggregate_inputs: Vec<WindowAggregateInput>,
}

#[derive(Debug, Clone)]
struct LinearHistogramDelayedRemoval {
    expires_at: Timestamp,
    bucket: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeValueSortKey(RuntimeValue);

impl PartialOrd for RuntimeValueSortKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RuntimeValueSortKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_runtime_values(&self.0, &other.0)
    }
}

#[derive(Debug, Clone)]
enum WindowAggregateAccumulator {
    Counter {
        count: usize,
    },
    Sequence {
        values: VecDeque<(Timestamp, u64, RuntimeValue)>,
    },
    SortedMap {
        counts: BTreeMap<RuntimeValueSortKey, usize>,
    },
    LinearHistogram {
        buckets: Vec<usize>,
        total: usize,
        min: f64,
        max: f64,
        width: f64,
        delay: Duration,
        delayed_removals: VecDeque<LinearHistogramDelayedRemoval>,
    },
    Sum {
        total: Option<RuntimeValue>,
    },
}

#[derive(Debug)]
struct WindowProcessorState {
    entries: VecDeque<WindowEntry>,
    next_sequence: u64,
    accumulators: Vec<WindowAggregateAccumulator>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaterializedLookupKeyMode {
    CurrentBranch,
    Root,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedRelayInterest {
    relay: Identifier,
    fields: Vec<String>,
    key_mode: MaterializedLookupKeyMode,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MaterializedProgramInterest {
    relays: Vec<MaterializedRelayInterest>,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeMaterializedRelaySpec {
    pub(crate) schema: StdArc<arrow_schema::Schema>,
    pub(crate) sensitivity: VmSchemaSensitivity,
    pub(crate) branching: Vec<Identifier>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledProgramWithMaterializedInterest {
    pub(crate) compiled: Arc<VmCompiledProgram>,
    pub(crate) output_sensitivity: VmSchemaSensitivity,
    pub(crate) materialized_interest: MaterializedProgramInterest,
    output_namespace_input: OutputNamespaceInput,
    lookup_hash_maps: Vec<LookupHashMapCall>,
    error_sites: Vec<CompiledMessageErrorSite>,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledBranchProgram {
    program: CompiledProgramWithMaterializedInterest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputNamespaceInput {
    Uninitialized,
    Finalized,
}

#[derive(Debug, Clone)]
struct CompiledMessageErrorSite {
    span: VmSpan,
    operation: MessageErrorOperation,
    operation_index: Option<u32>,
    fields: SortedSet<FieldPath>,
}

impl CompiledProgramWithMaterializedInterest {
    fn captures_partial_output(&self) -> bool {
        self.error_sites.iter().any(|site| {
            matches!(
                site.operation,
                MessageErrorOperation::Inherit | MessageErrorOperation::Set
            )
        })
    }

    fn structured_side_error(
        &self,
        reason: String,
        span: VmSpan,
        fallback_operation: MessageErrorOperation,
    ) -> StructuredMessageError {
        let site = self.error_sites.iter().find(|site| site.span == span);
        structured_message_error(
            MessageErrorCode::Evaluation,
            reason,
            site.map_or(fallback_operation, |site| site.operation),
            site.and_then(|site| site.operation_index),
            site.map(|site| site.fields.iter().cloned())
                .into_iter()
                .flatten(),
        )
    }
}

pub(crate) type EmitterHeaders = Vec<(String, String)>;

#[derive(Debug, Clone)]
pub(crate) struct CompiledEmitterFilterMapProgram {
    pub(crate) body: CompiledProgramWithMaterializedInterest,
    pub(crate) materialized_interest: MaterializedProgramInterest,
    pub(crate) codec_route: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RuntimeVmCompileContext<'a> {
    pub(crate) available_materialized_streams:
        &'a HashMap<Identifier, RuntimeMaterializedRelaySpec>,
    pub(crate) available_lookups: &'a HashMap<Identifier, Arc<LookupRuntime>>,
    pub(crate) current_branching: &'a [Identifier],
    pub(crate) current_branch_schema: Option<&'a StdArc<arrow_schema::Schema>>,
    pub(crate) current_branch_sensitivity: Option<&'a VmSchemaSensitivity>,
    pub(crate) udfs: Option<&'a UdfExecutor>,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeCompileTarget<'a> {
    domain: &'a Domain,
    identifier: &'a Identifier,
}

#[derive(Debug, Clone)]
struct RuntimeVmSchema {
    schema: StdArc<arrow_schema::Schema>,
    sensitivity: VmSchemaSensitivity,
}

impl RuntimeVmCompileContext<'_> {
    fn branch_binding(&self) -> Option<VmCompileBinding> {
        self.current_branch_schema.map(|schema| {
            let sensitivity = self.current_branch_sensitivity.cloned().unwrap_or_default();
            VmCompileBinding::readonly(BRANCH_NAMESPACE, schema.clone())
                .with_sensitivity(sensitivity)
        })
    }

    fn compile_options(&self, options: VmCompileOptions) -> VmCompileOptions {
        runtime_udf_compile_options(self.udfs, options)
    }
}

fn runtime_udf_compile_options(
    udfs: Option<&UdfExecutor>,
    mut options: VmCompileOptions,
) -> VmCompileOptions {
    if let Some(udfs) = udfs {
        options.udf_signatures = udfs.signatures().clone();
        options.injector = Some(Arc::new(Box::new(udfs.clone())));
    }
    options
}

#[derive(Debug, Clone)]
struct RuntimeVmSchemaPair {
    input: StdArc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    output: StdArc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledDomainUdfs {
    models: Vec<CreateUdf>,
    executor: UdfExecutor,
}

impl ExpiringRelayState {
    fn new() -> Self {
        Self {
            registry: RelayRegistry::new(),
        }
    }

    fn touch(&self, key: &Option<BranchKey>, now: Timestamp) {
        self.registry.touch(key, now);
    }

    fn contains_key(&self, key: &Option<BranchKey>) -> bool {
        self.registry.contains_key(key)
    }

    fn remove(&self, key: &Option<BranchKey>) {
        self.registry.remove(key);
    }
}

#[derive(Debug)]
pub(crate) struct StateSyncAck {
    pub(crate) placement: RuntimeStatePlacement,
    pub(crate) lsm: u64,
}

#[derive(Debug, Clone)]
struct ActiveDomainAlter;

pub(crate) struct DomainAlterGuard {
    domain: Domain,
    active_domain_alters: Arc<DashMap<Domain, ActiveDomainAlter, RandomState>>,
}

impl Drop for DomainAlterGuard {
    fn drop(&mut self) {
        self.active_domain_alters.remove(&self.domain);
    }
}

#[derive(Clone)]
pub struct Runtime {
    ingestors: Arc<DashMap<RuntimeKey, IngestorRuntime, RandomState>>,
    ingestor_quiescence: Arc<DashMap<RuntimeKey, Arc<IngestorQuiesceControl>, RandomState>>,
    ingestors_paused_for_memory_pressure: Arc<AtomicBool>,
    ingestor_transient_errors: Arc<DashMap<RuntimeKey, String, RandomState>>,
    ingestor_reconnect_backoffs: Arc<DashMap<RuntimeKey, RuntimeReconnectStatus, RandomState>>,
    ingestor_readiness: Arc<DashMap<RuntimeKey, IngestorReadiness, RandomState>>,
    emitter_transient_errors: Arc<DashMap<RuntimeKey, String, RandomState>>,
    emitter_retry_statuses: Arc<DashMap<RuntimeKey, EmitterRetryStatus, RandomState>>,
    emitter_confirmation_waits: Arc<DashMap<RuntimeKey, Arc<AtomicUsize>, RandomState>>,
    executions: Arc<DashMap<Domain, DomainExecution, RandomState>>,
    message_error_routes:
        Arc<DashMap<MessageErrorRouteKey, Arc<MessageErrorRouteRuntime>, RandomState>>,
    compiled_domain_udfs: Arc<DashMap<Domain, CompiledDomainUdfs, RandomState>>,
    schedule_apply_lock: Arc<Mutex<()>>,
    applied_cluster_revision: Arc<AtomicU64>,
    domain_instantiation_errors: Arc<DashMap<Domain, String, RandomState>>,
    domains: Arc<DashMap<Domain, RuntimeDomainState, RandomState>>,
    domain_status_changed: watch::Sender<u64>,
    in_flight_by_domain: Arc<DashMap<Domain, Arc<AckRootTracker>, RandomState>>,
    generator_activity_by_domain: Arc<DashMap<Domain, Arc<AtomicUsize>, RandomState>>,
    emitter_buffers: Arc<DashMap<RuntimeKey, Arc<AtomicUsize>, RandomState>>,
    force_flush_by_domain: Arc<DashMap<Domain, Arc<DomainForceFlush>, RandomState>>,
    node_quiesce_counters: Arc<DashMap<RuntimeKey, Arc<NodeQuiesceCounters>, RandomState>>,
    entity_gate_holds: Arc<DashMap<(Domain, u64), EntityAlterHold, RandomState>>,
    active_domain_alters: Arc<DashMap<Domain, ActiveDomainAlter, RandomState>>,
    state_schema_fingerprints: Arc<DashMap<RuntimeStateSchemaKey, [u8; 32], RandomState>>,
    domain_graphs: Arc<DashMap<Domain, SharedActiveGraph, RandomState>>,
    endpoint_bindings: Arc<DashMap<HttpRouteKey, Vec<EndpointIngestBinding>, RandomState>>,
    relay_boundary_fanouts: RelayBoundaryFanoutMap,
    events: broadcast::Sender<RuntimeEvent>,
    emitter_faults: Arc<EmitterFaultInjector>,
    ingestor_faults: Arc<IngestorFaultInjector>,
    otel_client_faults: Arc<OtelClientFaultInjector>,
    #[cfg(feature = "testing")]
    schedule_publication_faults: Arc<SchedulePublicationFaultInjector>,
    #[cfg(feature = "testing")]
    transaction_binding_drops: Arc<test_hooks::TransactionBindingDropInjector>,
    #[cfg(feature = "testing")]
    transaction_commit_pauses: Arc<test_hooks::TransactionCommitPauseInjector>,
    #[cfg(feature = "testing")]
    entity_gate_pauses: Arc<test_hooks::EntityGatePauseInjector>,
    resource_store: Arc<RwLock<Option<Arc<ResourceStore>>>>,
    resource_versions: Arc<RwLock<ResourceVersionStatus>>,
    remote_dispatcher: Arc<RwLock<Option<Arc<RemoteDispatcher>>>>,
    local_node_id: Arc<RwLock<Option<String>>>,
    next_remote_ack_id: Arc<AtomicU64>,
    pending_remote_acks: Arc<DashMap<u64, AckSet, RandomState>>,
    next_state_sync_correlation_id: Arc<AtomicU64>,
    pending_state_syncs: Arc<DashMap<u64, PendingStateSyncSender, RandomState>>,
    expiring_stream_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ExpiringRelayState>, RandomState>>,
    latest_resource_versions: Arc<DashMap<(Domain, Identifier), u64, RandomState>>,
    replicated_deduplicator_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedDeduplicatorState>, RandomState>>,
    replicated_kafka_offset_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedKafkaOffsetState>, RandomState>>,
    replicated_materialized_stream_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedMaterializedRelayState>, RandomState>>,
    materializer_epochs: Arc<DashMap<Domain, Arc<AtomicU64>, RandomState>>,
    materialized_state_changed: Arc<Notify>,
    replicated_window_processor_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedWindowProcessorState>, RandomState>>,
    replicated_wasm_processor_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedWasmProcessorState>, RandomState>>,
    replicated_branch_aggregated_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedBranchAggregatedState>, RandomState>>,
    wasm_runtime: Arc<WasmRuntime>,
    branch_instance_expiration_scan_interval: Duration,
    state_store: Option<Arc<RuntimeStateStore>>,
    state_snapshot_interval: Duration,
    state_replication_poll_interval: Duration,
    domain_drain_timeout: Duration,
    entity_gate_deadline: Duration,
    temp_dir: Arc<PathBuf>,
    metrics: RuntimeMetrics,
}

#[derive(Clone)]
struct RemoteDispatcher {
    cluster: Arc<cluster::ClusterHandle>,
    interconnect: Arc<Transport>,
    local_node_id: Arc<RwLock<Option<String>>>,
    next_remote_ack_id: Arc<AtomicU64>,
    pending_remote_acks: Arc<DashMap<u64, AckSet, RandomState>>,
}

impl std::fmt::Debug for RemoteDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteDispatcher").finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct RuntimeWasmDomainClock {
    runtime: Runtime,
    domain: Domain,
}

impl WasmDomainClock for RuntimeWasmDomainClock {
    fn now(&self) -> Timestamp {
        self.runtime
            .current_stream_expiration_time(&self.domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp)
    }
}

impl RelayBoundaryServices {
    fn new(
        fanout: RelayBoundaryFanout,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        remote_runtime_consumers: Vec<RemoteRuntimeConsumer>,
        remote_dispatcher: Option<Arc<RemoteDispatcher>>,
    ) -> Self {
        Self {
            fanout,
            attached_runtime_consumer_count: AtomicUsize::new(attached_runtime_consumer_count),
            detached_runtime_consumer_count: AtomicUsize::new(detached_runtime_consumer_count),
            remote_runtime_consumers: ArcSwap::from_pointee(remote_runtime_consumers),
            remote_dispatcher,
        }
    }

    fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.fanout.subscription_receiver()
    }

    fn observe_local_fanout_buffer_lengths(
        &self,
        metrics: &RuntimeMetrics,
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        branch_key: Option<&BranchKey>,
    ) {
        self.fanout
            .observe_buffer_lengths(metrics, domain, relay, physical_node_id, branch_key);
    }

    async fn fanout_local_subscriptions(&self, batch: &RelayRecordBatch) {
        self.fanout.fanout_subscriptions(batch).await;
    }

    async fn fanout_remote_subscriptions(
        &self,
        domain: &Domain,
        relay: &Identifier,
        batch: &RelayRecordBatch,
    ) {
        let Some(dispatcher) = &self.remote_dispatcher else {
            return;
        };
        let remote_runtime_consumers = self.remote_runtime_consumers.load_full();
        let excluded_nodes = remote_runtime_consumers
            .iter()
            .map(|consumer| consumer.node_id.clone())
            .collect::<BTreeSet<_>>();
        dispatcher
            .dispatch_subscription_fanout(domain, relay, &batch.detached(), &excluded_nodes)
            .await;
    }

    async fn dispatch_local_runtime_consumers(
        &self,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout
            .dispatch_runtime_consumers(
                self.attached_runtime_consumer_count.load(Ordering::Acquire),
                self.detached_runtime_consumer_count.load(Ordering::Acquire),
                batch,
            )
            .await
    }

    async fn dispatch_remote_runtime_consumers(
        &self,
        domain: &Domain,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let remote_runtime_consumers = self.remote_runtime_consumers.load_full();
        if remote_runtime_consumers.is_empty() {
            return Ok(());
        }
        for consumer in remote_runtime_consumers.iter() {
            let Some(dispatcher) = &self.remote_dispatcher else {
                if consumer.mode == AckMode::Attached {
                    for ack in batch.acks.iter() {
                        ack.no_ack("remote dispatcher unavailable for attached delivery");
                    }
                    return Err(Box::new(batch.clone()));
                }
                continue;
            };
            let remote_batch = match consumer.mode {
                AckMode::Attached => batch.attached(),
                AckMode::Detached => batch.detached(),
            };
            let batch_ipc = match remote_batch.batch.to_arrow_ipc_bytes() {
                Ok(bytes) => bytes,
                Err(error) => {
                    if consumer.mode == AckMode::Attached {
                        for ack in remote_batch.acks.iter() {
                            ack.no_ack(error.clone());
                        }
                        return Err(Box::new(batch.clone()));
                    }
                    warn!(
                        error = %error,
                        target_node = consumer.node_id,
                        "failed to serialize detached remote relay batch"
                    );
                    continue;
                }
            };
            let remote_acks = if consumer.mode == AckMode::Attached {
                let Some(local_node_id) = dispatcher.local_node_id() else {
                    for ack in remote_batch.acks.iter() {
                        ack.no_ack("local node id is unavailable for attached remote delivery");
                    }
                    return Err(Box::new(batch.clone()));
                };
                remote_batch
                    .acks
                    .iter()
                    .map(|ack| {
                        let ack_id = dispatcher.next_ack_id();
                        dispatcher.register_pending_ack(ack_id, ack.clone());
                        Some(RemoteAckRegistration {
                            ack_id,
                            reply_node_id: local_node_id.clone(),
                        })
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![None; remote_batch.acks.len()]
            };
            let result = dispatcher
                .dispatch(
                    &consumer.node_id,
                    Envelope::RelayPayload(RelayPayload {
                        kind: RelayPayloadKind::Routed,
                        domain: domain.clone(),
                        relay: consumer.relay.clone(),
                        key: BranchKey::to_remote_key(&remote_batch.key),
                        batch_ipc,
                        metadata: remote_batch
                            .metadata
                            .iter()
                            .map(RuntimeRecordMetadata::to_remote)
                            .collect(),
                        acks: remote_acks.clone(),
                    }),
                )
                .await;

            match (consumer.mode, result) {
                (AckMode::Attached, Ok(())) => {}
                (AckMode::Attached, Err(error)) => {
                    for (ack_set, remote_ack) in remote_batch.acks.iter().zip(remote_acks.iter()) {
                        if let Some(remote_ack) = remote_ack {
                            dispatcher.clear_pending_ack(remote_ack.ack_id);
                        }
                        ack_set.no_ack(error.clone());
                    }
                    return Err(Box::new(batch.clone()));
                }
                (AckMode::Detached, Err(error)) => {
                    warn!(
                        error = %error,
                        target_node = consumer.node_id,
                        "detached remote delivery failed"
                    );
                }
                (AckMode::Detached, Ok(())) => {}
            }
        }

        Ok(())
    }

    fn add_local_runtime_consumer(&self, mode: AckMode) -> RelayRuntimeFanIn {
        match mode {
            AckMode::Attached => {
                self.attached_runtime_consumer_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            AckMode::Detached => {
                self.detached_runtime_consumer_count
                    .fetch_add(1, Ordering::AcqRel);
            }
        }
        RelayRuntimeFanIn::new(self.fanout.runtime_consumer_receiver_for_mode(mode))
    }

    fn remove_local_runtime_consumer(&self, mode: AckMode) {
        let counter = match mode {
            AckMode::Attached => &self.attached_runtime_consumer_count,
            AckMode::Detached => &self.detached_runtime_consumer_count,
        };
        let previous = counter.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "relay runtime consumer count underflow");
    }

    fn replace_remote_runtime_consumers(&self, consumers: Vec<RemoteRuntimeConsumer>) {
        self.remote_runtime_consumers.store(StdArc::new(consumers));
    }

    async fn ingest_message(
        &self,
        metrics: &RuntimeMetrics,
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout_local_subscriptions(batch).await;
        self.fanout_remote_subscriptions(domain, relay, batch).await;
        self.observe_local_fanout_buffer_lengths(
            metrics,
            domain,
            relay,
            physical_node_id,
            batch.key.as_ref(),
        );
        self.dispatch_local_runtime_consumers(batch).await?;
        self.dispatch_remote_runtime_consumers(domain, batch).await
    }

    async fn ingest_concrete_message(
        &self,
        domain: &Domain,
        relay: &Identifier,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout_remote_subscriptions(domain, relay, batch).await;
        self.dispatch_remote_runtime_consumers(domain, batch).await
    }

    async fn inject_remote_message(
        &self,
        metrics: &RuntimeMetrics,
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout_local_subscriptions(batch).await;
        self.observe_local_fanout_buffer_lengths(
            metrics,
            domain,
            relay,
            physical_node_id,
            batch.key.as_ref(),
        );
        self.dispatch_local_runtime_consumers(batch).await
    }
}

impl ConcreteRelayRuntime {
    fn new(build: ConcreteRelayRuntimeBuild) -> Self {
        let ConcreteRelayRuntimeBuild {
            key,
            runtime,
            domain,
            relay,
            registry,
            services,
        } = build;
        Self {
            runtime,
            domain,
            relay,
            registry,
            services,
            key,
        }
    }

    async fn dispatch_boundary(&mut self, batch: &RelayRecordBatch) -> RelayDispatchResult {
        debug_assert_eq!(&self.key, &batch.key);
        let now = self
            .runtime
            .current_stream_expiration_time(&self.domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp);
        self.registry.touch(&batch.key, now);
        self.runtime
            .touch_stream_key(&self.domain, &self.relay, &batch.key, now);
        self.runtime.metrics.observe_global_stream_received(
            &self.domain,
            &self.relay,
            self.runtime.local_node_id.read().as_deref(),
            batch.message_count(),
            batch.estimated_bytes(),
            batch.domain_timestamp(),
        );
        self.runtime.mark_branch_aggregated_metrics_updated(
            &self.domain,
            ModelKind::Relay,
            &self.relay,
        );
        let physical_node_id = self.runtime.local_node_id.read().clone();
        self.services.fanout_local_subscriptions(batch).await;
        self.services.observe_local_fanout_buffer_lengths(
            &self.runtime.metrics,
            &self.domain,
            &self.relay,
            physical_node_id.as_deref(),
            self.key.as_ref(),
        );
        self.services
            .dispatch_local_runtime_consumers(batch)
            .await?;
        self.services
            .ingest_concrete_message(&self.domain, &self.relay, batch)
            .await?;

        Ok(())
    }
}

impl RemoteDispatcher {
    const DISPATCH_RETRY_INTERVAL: Duration = Duration::from_millis(25);
    const DISPATCH_TIMEOUT: Duration = Duration::from_secs(5);

    fn local_node_id(&self) -> Option<String> {
        self.local_node_id.read().clone()
    }

    fn next_ack_id(&self) -> u64 {
        self.next_remote_ack_id.fetch_add(1, Ordering::Relaxed)
    }

    fn register_pending_ack(&self, ack_id: u64, acks: AckSet) {
        self.pending_remote_acks.insert(ack_id, acks);
    }

    fn clear_pending_ack(&self, ack_id: u64) {
        self.pending_remote_acks.remove(&ack_id);
    }

    async fn dispatch_subscription_fanout(
        &self,
        domain: &Domain,
        relay: &Identifier,
        batch: &RelayRecordBatch,
        excluded_nodes: &BTreeSet<String>,
    ) {
        let Some(local_node_id) = self.local_node_id() else {
            return;
        };
        let batch_ipc = match batch.batch.to_arrow_ipc_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    error = %error,
                    "failed to serialize remote subscription batch"
                );
                return;
            }
        };
        let interested_nodes = self
            .cluster
            .nodes_with_subscription_interest(domain.as_str(), relay.as_str())
            .await;
        for node_id in interested_nodes {
            if node_id == local_node_id || excluded_nodes.contains(&node_id) {
                continue;
            }
            if let Err(error) = self
                .dispatch(
                    &node_id,
                    Envelope::RelayPayload(RelayPayload {
                        kind: RelayPayloadKind::SubscriptionFanout,
                        domain: domain.clone(),
                        relay: relay.clone(),
                        key: BranchKey::to_remote_key(&batch.key),
                        batch_ipc: batch_ipc.clone(),
                        metadata: batch
                            .metadata
                            .iter()
                            .map(RuntimeRecordMetadata::to_remote)
                            .collect(),
                        acks: vec![None; batch.acks.len()],
                    }),
                )
                .await
            {
                warn!(
                    target_node = node_id,
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    error = %error,
                    "failed to dispatch remote subscription payload"
                );
            }
        }
    }

    async fn dispatch(&self, node_id: &str, envelope: Envelope) -> Result<(), String> {
        let deadline = Instant::now() + Self::DISPATCH_TIMEOUT;
        loop {
            tokio::task::consume_budget().await;
            let result = async {
                let node = self
                    .cluster
                    .gossip_state()
                    .await
                    .live_nodes
                    .into_iter()
                    .find(|node| node.node_id == node_id)
                    .ok_or_else(|| {
                        format!("remote node '{node_id}' is not present in gossip membership")
                    })?;
                let target_addr = node.interconnect_advertise_addr.parse().map_err(|error| {
                    format!("invalid interconnect address for '{node_id}': {error}")
                })?;
                let mode = match node.interconnect_mode.as_str() {
                    "https" => InterconnectTransportMode::Tls,
                    _ => InterconnectTransportMode::Plain,
                };
                let connection = self
                    .interconnect
                    .connection_for(target_addr, "localhost", mode)
                    .await
                    .map_err(|error| {
                        format!("failed to connect interconnect for '{node_id}': {error}")
                    })?;
                connection
                    .send(envelope.clone())
                    .await
                    .map_err(|error| format!("failed to send remote relay payload: {error}"))
            }
            .await;
            let error = match result {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if Instant::now() >= deadline {
                return Err(error);
            }
            sleep(Self::DISPATCH_RETRY_INTERVAL).await;
        }
    }
}

fn push_remote_runtime_consumer(
    consumers: &mut Vec<RemoteRuntimeConsumer>,
    node_id: &str,
    relay: &Identifier,
    mode: AckMode,
) {
    if let Some(existing) = consumers
        .iter_mut()
        .find(|consumer| consumer.node_id == node_id && consumer.relay == *relay)
    {
        if mode == AckMode::Attached {
            existing.mode = AckMode::Attached;
        }
        return;
    }

    consumers.push(RemoteRuntimeConsumer {
        node_id: node_id.to_string(),
        relay: relay.clone(),
        mode,
    });
}

impl RelayBoundaryBuilder {
    fn runtime_consumer_receiver_for_mode(
        &mut self,
        mode: AckMode,
    ) -> RelayRuntimeConsumerReceiver {
        match mode {
            AckMode::Attached => {
                self.attached_runtime_consumer_count += 1;
            }
            AckMode::Detached => {
                self.detached_runtime_consumer_count += 1;
            }
        }
        self.fanout.runtime_consumer_receiver_for_mode(mode)
    }

    fn runtime_consumer_fan_in_for_mode(&mut self, mode: AckMode) -> RelayRuntimeFanIn {
        RelayRuntimeFanIn::new(self.runtime_consumer_receiver_for_mode(mode))
    }
}

#[derive(Debug, Clone, Copy)]
enum ProcessorInputFilterKind {
    FromWhere,
    FilterWhere,
}

enum MaterializedDependencyResolution {
    Ready(HashMap<String, RuntimeValue>),
    Skip,
    Wait,
}

impl ProcessorInputFilterKind {
    fn label(self) -> &'static str {
        match self {
            Self::FromWhere => "FROM WHERE",
            Self::FilterWhere => "FILTER WHERE",
        }
    }

    fn error_operation(self) -> MessageErrorOperation {
        match self {
            Self::FromWhere => MessageErrorOperation::SourceWhere,
            Self::FilterWhere => MessageErrorOperation::FilterWhere,
        }
    }
}

impl RelayProcessorOutputsNode {
    fn matches_template(&self, template: &RelayProcessorOutputsTemplate) -> bool {
        self.routes.len() == template.routes.len()
            && self
                .routes
                .iter()
                .zip(&template.routes)
                .all(|(runtime, desired)| {
                    runtime.relay == desired.output_relay
                        && runtime.construction == desired.construction
                        && runtime.flush_policy == desired.flush_policy
                        && runtime.message_error_policy == desired.message_error_policy
                })
    }

    fn apply_template(&mut self, template: &RelayProcessorOutputsTemplate) -> Result<bool, String> {
        if self.routes.len() != template.routes.len()
            || self
                .routes
                .iter()
                .zip(&template.routes)
                .any(|(runtime, desired)| runtime.relay != desired.output_relay)
        {
            return Err("dynamic processor update changed its route topology".to_string());
        }

        let mut changed = false;
        for (runtime, desired) in self.routes.iter_mut().zip(&template.routes) {
            let program_changed = runtime.construction != desired.construction
                || runtime.message_error_policy != desired.message_error_policy;
            changed |= program_changed || runtime.flush_policy != desired.flush_policy;
            if program_changed {
                runtime.compiled_program = None;
            }
            runtime.construction = desired.construction.clone();
            runtime.flush_policy = desired.flush_policy;
            runtime.message_error_policy = desired.message_error_policy.clone();
        }
        Ok(changed)
    }
}

impl RelayProcessorOperationNode {
    fn apply_template(&mut self, template: &RelayProcessorOperationTemplate) -> Result<(), String> {
        match (self, template) {
            (
                Self::Deduplicator {
                    output_routes,
                    deduplicate_on,
                    max_time,
                    ..
                },
                RelayProcessorOperationTemplate::Deduplicator {
                    output_routes: desired_outputs,
                    deduplicate_on: desired_deduplicate_on,
                    max_time: desired_max_time,
                },
            ) => {
                if deduplicate_on != desired_deduplicate_on {
                    return Err(
                        "dynamic deduplicator update changed its state keyspace".to_string()
                    );
                }
                output_routes.apply_template(desired_outputs)?;
                *max_time = *desired_max_time;
                Ok(())
            }
            (
                Self::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
                    ..
                },
                RelayProcessorOperationTemplate::WindowProcessor {
                    output_routes: desired_outputs,
                    width_messages: desired_width_messages,
                    step_messages: desired_step_messages,
                    width_duration: desired_width_duration,
                    step_duration: desired_step_duration,
                    aggregate: desired_aggregate,
                    ..
                },
            ) => {
                if width_messages != desired_width_messages
                    || step_messages != desired_step_messages
                    || width_duration != desired_width_duration
                    || step_duration != desired_step_duration
                    || aggregate != desired_aggregate
                {
                    return Err(
                        "dynamic window processor update changed its state shape".to_string()
                    );
                }
                output_routes.apply_template(desired_outputs)?;
                Ok(())
            }
            (
                Self::Reorderer {
                    output_routes,
                    order_by,
                    max_time,
                    ..
                },
                RelayProcessorOperationTemplate::Reorderer {
                    output_routes: desired_outputs,
                    order_by: desired_order_by,
                    max_time: desired_max_time,
                },
            ) => {
                if order_by != desired_order_by {
                    return Err("dynamic reorderer update changed its ordering key".to_string());
                }
                output_routes.apply_template(desired_outputs)?;
                *max_time = *desired_max_time;
                Ok(())
            }
            (
                Self::Correlator {
                    output_routes,
                    left_relays,
                    right_relays,
                    correlate_where,
                    match_policy,
                    max_time,
                    timeout_policy,
                    compiled_where_program,
                    compiled_output_programs,
                    ..
                },
                RelayProcessorOperationTemplate::Correlator {
                    output_routes: desired_outputs,
                    left_relays: desired_left_relays,
                    right_relays: desired_right_relays,
                    correlate_where: desired_correlate_where,
                    match_policy: desired_match_policy,
                    max_time: desired_max_time,
                    timeout_policy: desired_timeout_policy,
                },
            ) => {
                if left_relays != desired_left_relays || right_relays != desired_right_relays {
                    return Err("dynamic correlator update changed its input sides".to_string());
                }
                if timeout_policy != desired_timeout_policy {
                    return Err("dynamic correlator update changed its timeout wiring".to_string());
                }
                if correlate_where != desired_correlate_where {
                    *compiled_where_program = None;
                }
                if output_routes.apply_template(desired_outputs)? {
                    for program in compiled_output_programs {
                        *program = None;
                    }
                }
                *correlate_where = desired_correlate_where.clone();
                *match_policy = *desired_match_policy;
                *max_time = *desired_max_time;
                Ok(())
            }
            (
                Self::Junction { output_routes },
                RelayProcessorOperationTemplate::Junction {
                    output_routes: desired_outputs,
                },
            ) => output_routes.apply_template(desired_outputs).map(|_| ()),
            (
                Self::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                    ..
                },
                RelayProcessorOperationTemplate::Inferencer {
                    output_routes: desired_outputs,
                    resource: desired_resource,
                    resource_version: desired_resource_version,
                    file: desired_file,
                    inputs: desired_inputs,
                    output_schema: desired_output_schema,
                },
            ) => {
                if resource != desired_resource
                    || resource_version != desired_resource_version
                    || file != desired_file
                    || inputs != desired_inputs
                    || output_schema != desired_output_schema
                {
                    return Err(
                        "dynamic inferencer update changed its inference session".to_string()
                    );
                }
                output_routes.apply_template(desired_outputs)?;
                Ok(())
            }
            (
                Self::WasmProcessor {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    limits,
                    ..
                },
                RelayProcessorOperationTemplate::WasmProcessor {
                    output_routes: desired_outputs,
                    resource: desired_resource,
                    resource_version: desired_resource_version,
                    file: desired_file,
                    limits: desired_limits,
                    ..
                },
            ) if resource == desired_resource
                && resource_version == desired_resource_version
                && file == desired_file
                && limits == desired_limits
                && output_routes.matches_template(desired_outputs) =>
            {
                Ok(())
            }
            (Self::WasmProcessor { .. }, RelayProcessorOperationTemplate::WasmProcessor { .. }) => {
                Err("WASM processors do not support dynamic configuration refresh".to_string())
            }
            _ => Err("dynamic processor update changed its operation kind".to_string()),
        }
    }
}

impl RelayProcessorNode {
    fn source_filter_scope(&self, incoming_relay: &Identifier) -> RuntimeFilterScope {
        match &self.operation {
            RelayProcessorOperationNode::Correlator {
                left_relays,
                right_relays,
                ..
            } if left_relays.contains(incoming_relay) => RuntimeFilterScope::Source {
                namespace: "left",
                allow_header_reads: false,
                allow_metadata: false,
            },
            RelayProcessorOperationNode::Correlator { right_relays, .. }
                if right_relays.contains(incoming_relay) =>
            {
                RuntimeFilterScope::Source {
                    namespace: "right",
                    allow_header_reads: false,
                    allow_metadata: false,
                }
            }
            _ => RuntimeFilterScope::Source {
                namespace: "input",
                allow_header_reads: false,
                allow_metadata: false,
            },
        }
    }

    async fn resolve_materialized_dependencies(
        &self,
        branch: &BranchRuntime,
        branch_key: &Option<BranchKey>,
    ) -> Result<MaterializedDependencyResolution, String> {
        branch
            .runtime
            .resolve_materialized_dependencies(&branch.domain, branch_key, &self.materialized_state)
            .await
    }

    fn refresh(&mut self, runtime: &Runtime, domain: &Domain, graph: Option<StdArc<ActiveGraph>>) {
        let changed = match (&self.last_graph, &graph) {
            (Some(previous), Some(current)) => !StdArc::ptr_eq(previous, current),
            (None, None) => false,
            _ => true,
        };
        if !changed {
            return;
        }

        let requires_reinitialization = match (self.last_graph.as_ref(), graph.as_ref()) {
            (Some(previous), Some(current)) => {
                previous
                    .node(self.kind, &self.processor)
                    .map(|node| node.config.as_ref().clone())
                    != current
                        .node(self.kind, &self.processor)
                        .map(|node| node.config.as_ref().clone())
            }
            (None, Some(_)) | (Some(_), None) => true,
            (None, None) => false,
        };

        if requires_reinitialization {
            let result = graph
                .as_ref()
                .ok_or_else(|| {
                    format!(
                        "{} '{}' is absent from the refreshed graph",
                        self.kind.as_str(),
                        self.processor.as_str()
                    )
                })
                .and_then(|graph| {
                    let execution = runtime.executions.get(domain).ok_or_else(|| {
                        format!(
                            "domain '{}' has no execution for processor refresh",
                            domain.as_str()
                        )
                    })?;
                    processor_template_for_graph_node(
                        graph,
                        self.kind,
                        &self.processor,
                        &execution.relay_schemas,
                        Some(&execution.udfs),
                    )
                })
                .and_then(|template| self.apply_node_template(template));
            if let Err(error) = result {
                warn!(
                    kind = self.kind.as_str(),
                    processor = self.processor.as_str(),
                    error = %error,
                    "failed to refresh dynamic processor configuration"
                );
                return;
            }
            self.applied_generation = self.applied_generation.saturating_add(1);
        }
        self.last_graph = graph;
    }

    fn apply_node_template(&mut self, template: RelayProcessorTemplate) -> Result<(), String> {
        if self.kind != template.kind || self.processor != template.processor {
            return Err(format!(
                "processor template targets {} '{}', not {} '{}'",
                template.kind.as_str(),
                template.processor.as_str(),
                self.kind.as_str(),
                self.processor.as_str()
            ));
        }
        if self.input_relays != template.input_relays {
            return Err(format!(
                "dynamic {} update changed processor input topology",
                self.kind.as_str()
            ));
        }
        if self.materialized_state != template.materialized_state {
            return Err(format!(
                "dynamic {} update changed materialized-state dependencies",
                self.kind.as_str()
            ));
        }
        self.operation.apply_template(&template.operation)?;

        let mut previous_collectors = std::mem::take(&mut self.input_collectors);
        self.input_collectors = template
            .input_collect_policies
            .into_iter()
            .map(|(relay, policy)| {
                let mut collector = previous_collectors
                    .remove(&relay)
                    .unwrap_or_else(|| RuntimeInputCollector::new(policy));
                collector.policy = policy;
                (relay, collector)
            })
            .collect();

        if self.from_where != template.from_where {
            self.from_where = template.from_where;
            self.compiled_from_where.clear();
        }
        if self.filter_where != template.filter_where {
            self.filter_where = template.filter_where;
            self.compiled_filter_where.clear();
        }
        self.error_policies = template.error_policies;
        Ok(())
    }

    async fn filter_input_batch(
        &mut self,
        graph: &SharedActiveGraph,
        branch: &mut BranchRuntime,
        incoming_relay: &Identifier,
        batch: RelayRecordBatch,
        materialized_state: &HashMap<String, RuntimeValue>,
    ) -> Option<RelayRecordBatch> {
        let batch = self
            .filter_input_batch_with_kind(
                graph,
                branch,
                incoming_relay,
                batch,
                ProcessorInputFilterKind::FromWhere,
                materialized_state,
            )
            .await?;
        self.filter_input_batch_with_kind(
            graph,
            branch,
            incoming_relay,
            batch,
            ProcessorInputFilterKind::FilterWhere,
            materialized_state,
        )
        .await
    }

    fn concat_collected_input(
        &self,
        branch: &BranchRuntime,
        incoming_relay: &Identifier,
        batches: Vec<RelayRecordBatch>,
    ) -> Option<RelayRecordBatch> {
        let acks = batches
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        match RelayRecordBatch::concat(batches) {
            Ok(batch) => Some(batch),
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    self.kind.as_str(),
                    &self.processor,
                    &self.error_policies,
                    acks.iter(),
                    format!(
                        "{} '{}' failed to concatenate collected input from relay '{}': {error}",
                        self.kind.as_str(),
                        self.processor.as_str(),
                        incoming_relay.as_str(),
                    ),
                );
                None
            }
        }
    }

    fn accept_input<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
        incoming_relay: &'a Identifier,
        batch: RelayRecordBatch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let now = branch
                .runtime
                .current_stream_expiration_time(&branch.domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp);
            let Some(collector) = self.input_collectors.get_mut(incoming_relay) else {
                self.execute(graph, branch, incoming_relay, batch).await;
                return;
            };
            if !collector.push(batch, now) {
                return;
            }
            let batches = collector.take_pending();
            let Some(batch) = self.concat_collected_input(branch, incoming_relay, batches) else {
                return;
            };
            self.execute(graph, branch, incoming_relay, batch).await;
        })
    }

    fn flush_due_collected_inputs<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
        now: Timestamp,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let due_relays = self
                .input_collectors
                .iter()
                .filter_map(|(relay, collector)| collector.is_due(now).then_some(relay.clone()))
                .collect::<Vec<_>>();
            for relay in due_relays {
                let batches = self
                    .input_collectors
                    .get_mut(&relay)
                    .map(RuntimeInputCollector::take_pending)
                    .unwrap_or_default();
                let Some(batch) = self.concat_collected_input(branch, &relay, batches) else {
                    continue;
                };
                self.execute(graph, branch, &relay, batch).await;
            }
        })
    }

    fn flush_all_collected_inputs<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let pending_relays = self
                .input_collectors
                .iter()
                .filter_map(|(relay, collector)| {
                    (!collector.pending.is_empty()).then_some(relay.clone())
                })
                .collect::<Vec<_>>();
            for relay in pending_relays {
                let batches = self
                    .input_collectors
                    .get_mut(&relay)
                    .map(RuntimeInputCollector::take_pending)
                    .unwrap_or_default();
                let Some(batch) = self.concat_collected_input(branch, &relay, batches) else {
                    continue;
                };
                self.execute(graph, branch, &relay, batch).await;
            }
        })
    }

    fn drop_collected_inputs(&mut self, reason: &str) {
        for collector in self.input_collectors.values_mut() {
            for batch in collector.take_pending() {
                for ack in batch.acks {
                    ack.no_ack(reason.to_string());
                }
            }
        }
    }

    async fn filter_input_batch_with_kind(
        &mut self,
        graph: &SharedActiveGraph,
        branch: &mut BranchRuntime,
        incoming_relay: &Identifier,
        batch: RelayRecordBatch,
        kind: ProcessorInputFilterKind,
        materialized_state: &HashMap<String, RuntimeValue>,
    ) -> Option<RelayRecordBatch> {
        let Some(filter_where) = (match kind {
            ProcessorInputFilterKind::FromWhere => self.from_where.get(incoming_relay),
            ProcessorInputFilterKind::FilterWhere => self.filter_where.as_ref(),
        }) else {
            return Some(batch);
        };
        let filter_where = filter_where.clone();

        let needs_compile = match kind {
            ProcessorInputFilterKind::FromWhere => {
                !self.compiled_from_where.contains_key(incoming_relay)
            }
            ProcessorInputFilterKind::FilterWhere => {
                !self.compiled_filter_where.contains_key(incoming_relay)
            }
        };
        if needs_compile {
            let input_schema =
                match relay_schema_for_runtime(&branch.runtime, &branch.domain, incoming_relay) {
                    Ok(schema) => schema,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind.as_str(),
                            &self.processor,
                            &self.error_policies,
                            batch.acks.iter(),
                            error,
                        );
                        return None;
                    }
                };
            let materialized_stream_specs =
                materialized_stream_specs_for_graph(&branch.runtime, &branch.domain, graph);
            let current_branching = branch
                .runtime
                .executions
                .get(&branch.domain)
                .and_then(|execution| execution.relay_branchings.get(incoming_relay).cloned())
                .unwrap_or_default();
            let current_branch_schema =
                relay_branch_schema_for_runtime(&branch.runtime, &branch.domain, incoming_relay);
            let available_lookups = branch
                .runtime
                .executions
                .get(&branch.domain)
                .map(|execution| execution.lookups.clone())
                .unwrap_or_default();
            let udfs = branch
                .runtime
                .executions
                .get(&branch.domain)
                .map(|execution| execution.udfs.clone());
            let filter_scope = match kind {
                ProcessorInputFilterKind::FromWhere => self.source_filter_scope(incoming_relay),
                ProcessorInputFilterKind::FilterWhere => RuntimeFilterScope::Source {
                    namespace: "input",
                    allow_header_reads: false,
                    allow_metadata: false,
                },
            };
            match compile_scoped_filter_program(
                RuntimeCompileTarget {
                    domain: &branch.domain,
                    identifier: &self.processor,
                },
                Some(&filter_where),
                RuntimeVmSchema {
                    schema: batch.arrow_schema(),
                    sensitivity: input_schema.vm_sensitivity(),
                },
                kind.error_operation(),
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
                    udfs: udfs.as_ref(),
                },
                filter_scope,
            ) {
                Ok(Some(program)) => match kind {
                    ProcessorInputFilterKind::FromWhere => {
                        self.compiled_from_where
                            .insert(incoming_relay.clone(), program);
                    }
                    ProcessorInputFilterKind::FilterWhere => {
                        self.compiled_filter_where
                            .insert(incoming_relay.clone(), program);
                    }
                },
                Ok(None) => {}
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        self.kind.as_str(),
                        &self.processor,
                        &self.error_policies,
                        batch.acks.iter(),
                        format!("{} compile failed: {}", kind.label(), error),
                    );
                    return None;
                }
            }
        }

        let program = match kind {
            ProcessorInputFilterKind::FromWhere => self.compiled_from_where.get(incoming_relay),
            ProcessorInputFilterKind::FilterWhere => self.compiled_filter_where.get(incoming_relay),
        }
        .cloned();
        let Some(program) = program else {
            return Some(batch);
        };
        let plan = match plan_filter_map_messages(
            self.kind.as_str(),
            &self.processor,
            kind.label(),
            &program,
            batch,
            branch
                .runtime
                .current_stream_expiration_time(&branch.domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp),
            materialized_state,
        )
        .await
        {
            Ok(plan) => plan,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    self.kind.as_str(),
                    &self.processor,
                    &self.error_policies,
                    error.acks.iter(),
                    error.reason,
                );
                return None;
            }
        };
        branch
            .runtime
            .handle_planned_message_errors(
                &branch.domain,
                self.kind.as_str(),
                &self.processor,
                &self.error_policies,
                plan.message_errors,
            )
            .await;
        plan.batch
    }

    fn execute<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
        incoming_relay: &'a Identifier,
        batch: RelayRecordBatch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let current = graph.load_full();
            let current = current.as_ref().map(StdArc::clone);
            self.refresh(&branch.runtime, &branch.domain, current);
            let materialized_values = match self
                .resolve_materialized_dependencies(branch, &batch.key)
                .await
            {
                Ok(MaterializedDependencyResolution::Ready(values)) => values,
                Ok(MaterializedDependencyResolution::Skip) => {
                    for ack in batch.acks.iter() {
                        ack.ack_success();
                    }
                    return;
                }
                Ok(MaterializedDependencyResolution::Wait) => {
                    self.pending_materialized
                        .push_back((incoming_relay.clone(), batch));
                    return;
                }
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        self.kind.as_str(),
                        &self.processor,
                        &self.error_policies,
                        batch.acks.iter(),
                        format!(
                            "{} '{}' failed to resolve materialized dependencies: {error}",
                            self.kind.as_str(),
                            self.processor
                        ),
                    );
                    return;
                }
            };
            let Some(batch) = self
                .filter_input_batch(graph, branch, incoming_relay, batch, &materialized_values)
                .await
            else {
                return;
            };
            match &mut self.operation {
                RelayProcessorOperationNode::Deduplicator {
                    output_routes,
                    deduplicate_on,
                    max_time,
                    compiled_key_program,
                    state,
                } => {
                    let input_arrow_schema = batch.arrow_schema();
                    let key_input_batch = batch.batch.clone();
                    let key_input_keys = batch.keys.clone();
                    let execution_now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    let messages = match batch.try_into_messages() {
                        Ok(messages) => messages,
                        Err(error_and_batch) => {
                            let (error, batch) = *error_and_batch;
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "deduplicator '{}' failed to decode arrow batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };

                    if compiled_key_program.is_none() {
                        let udfs = branch.runtime.udf_executor(&branch.domain);
                        match compile_deduplicator_key_program(
                            &self.processor,
                            &self.input_relays,
                            deduplicate_on,
                            input_arrow_schema.clone(),
                            udfs.as_ref(),
                        ) {
                            Ok(program) => *compiled_key_program = Some(Box::new(program)),
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    messages.iter().map(|message| &message.acks),
                                    error,
                                );
                                return;
                            }
                        }
                    }
                    let Some(key_program) = compiled_key_program.as_ref() else {
                        return;
                    };
                    let lookup_columns = HashMap::default();
                    let vm_batch = match project_vm_input_batch(
                        &key_program.program.input_schema,
                        &VmInputProjectionSources {
                            carrier: &key_input_batch,
                            namespace_batches: &[],
                            strict_namespaces: &[],
                            keys: &key_input_keys,
                            side_inputs: &materialized_values,
                            ingest_metadata: None,
                            lookup_columns: &lookup_columns,
                            uninitialized: None,
                        },
                    ) {
                        Ok(batch) => batch,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                messages.iter().map(|message| &message.acks),
                                format!(
                                    "deduplicator '{}' failed to build DEDUPLICATE ON input \
                                     batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };
                    let key_result = execute_program_with_selection_in_context(
                        &key_program.program,
                        &vm_batch,
                        &VmExecutionContext {
                            now: execution_now,
                            injector: None,
                        },
                    )
                    .await;
                    let key_result = match key_result {
                        Ok(result) => result,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                messages.iter().map(|message| &message.acks),
                                format!(
                                    "deduplicator '{}' failed to evaluate DEDUPLICATE ON \
                                     expressions: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };

                    let mut forwarded_entries = Vec::new();
                    for (row, message) in messages.into_iter().enumerate() {
                        trace!(
                            processor = self.processor.as_str(),
                            operator = "deduplicator",
                            "branched relay operator received message"
                        );

                        let dedup_key = (0..key_program.key_count)
                            .map(|index| {
                                reorder_key_part(
                                    key_result
                                        .batch
                                        .column(key_program.key_column_offset + index),
                                    row,
                                )
                            })
                            .collect::<Vec<_>>();
                        let dedup_key = format!("{dedup_key:?}");
                        let RelayMessage { key, record, acks } = message;
                        if state.reserve_new_key(dedup_key.clone(), execution_now, *max_time) {
                            forwarded_entries.push((dedup_key, RelayMessage { key, record, acks }));
                        } else {
                            debug!(
                                deduplicator = self.processor.as_str(),
                                "branched deduplicator dropped duplicate message"
                            );
                            acks.ack_success();
                        }
                    }

                    if forwarded_entries.is_empty() {
                        return;
                    }

                    let (dedup_keys, forwarded_messages): (Vec<_>, Vec<_>) =
                        forwarded_entries.into_iter().unzip();
                    let source_schema = match relay_schema_for_runtime(
                        &branch.runtime,
                        &branch.domain,
                        incoming_relay,
                    ) {
                        Ok(schema) => schema,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                forwarded_messages.iter().map(|message| &message.acks),
                                error,
                            );
                            return;
                        }
                    };
                    let forwarded = match build_stream_record_batch_preserving_acks(
                        source_schema,
                        forwarded_messages,
                    ) {
                        Ok(batch) => batch,
                        Err((error, acks)) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                acks.iter(),
                                format!(
                                    "deduplicator '{}' failed to build output batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };

                    let Some(dispatched_acks) = dispatch_processor_outputs(
                        ProcessorOutputDispatchContext {
                            graph,
                            branch,
                            node_kind: self.kind.as_str(),
                            source_kind: self.kind,
                            processor: &self.processor,
                            error_policies: &self.error_policies,
                            input_relays: &self.input_relays,
                            filter_source: ProcessorOutputFilterSource::InputRelays,
                            resolved_materialized_state: Some(&materialized_values),
                        },
                        output_routes,
                        forwarded,
                    )
                    .await
                    else {
                        state.remove_reserved_keys(&dedup_keys);
                        return;
                    };

                    match state.latest_snapshot() {
                        Ok(snapshot) => {
                            if let Err(error) = branch
                                .runtime
                                .persist_deduplicator_snapshot(
                                    state,
                                    snapshot.lsm,
                                    &snapshot.payload,
                                )
                                .await
                            {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    dispatched_acks.iter(),
                                    error,
                                );
                                return;
                            }
                            for ack in dispatched_acks {
                                ack.ack_success();
                            }
                        }
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                dispatched_acks.iter(),
                                format!(
                                    "deduplicator '{}' failed to update state: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                        }
                    }
                }
                RelayProcessorOperationNode::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
                    compiled_aggregates,
                    state,
                    replicated_state,
                } => {
                    let messages = match batch.try_into_messages() {
                        Ok(messages) => messages,
                        Err(error_and_batch) => {
                            let (error, batch) = *error_and_batch;
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "window processor '{}' failed to decode arrow batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };
                    for message in messages {
                        tokio::task::consume_budget().await;
                        let timestamp = message_timestamp(&message);
                        let mut aggregate_inputs = Vec::new();
                        let mut aggregate_input_error = None;
                        for compiled in compiled_aggregates.iter() {
                            tokio::task::consume_budget().await;
                            match evaluate_window_aggregate_inputs(
                                compiled,
                                &message.record,
                                timestamp,
                            )
                            .await
                            {
                                Ok(inputs) => aggregate_inputs.extend(inputs),
                                Err(error) => {
                                    aggregate_input_error = Some(error);
                                    break;
                                }
                            }
                        }
                        if let Some(error) = aggregate_input_error {
                            branch
                                .runtime
                                .handle_message_error(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    message,
                                    MessageErrorFailure::publish(
                                        None,
                                        format!(
                                            "window processor '{}' aggregate input failed: {}",
                                            self.processor.as_str(),
                                            error
                                        ),
                                    ),
                                )
                                .await;
                            continue;
                        }
                        if let Err(error_and_message) =
                            state.push_message(aggregate, timestamp, message, aggregate_inputs)
                        {
                            let (error, message) = *error_and_message;
                            branch
                                .runtime
                                .handle_message_error(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    message,
                                    MessageErrorFailure::publish(
                                        None,
                                        format!(
                                            "window processor '{}' aggregate input failed: {}",
                                            self.processor.as_str(),
                                            error
                                        ),
                                    ),
                                )
                                .await;
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                state.entries.iter().map(|entry| &entry.message.acks),
                                format!(
                                    "window processor '{}' aggregate state failed: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            state.clear(aggregate);
                            continue;
                        }
                        flush_ready_window_processor(
                            WindowFlushContext {
                                graph,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                branch,
                                output_routes,
                            },
                            state,
                            aggregate,
                            compiled_aggregates,
                            WindowBounds {
                                width_messages: *width_messages,
                                step_messages: *step_messages,
                                width_duration: *width_duration,
                                step_duration: *step_duration,
                            },
                            timestamp,
                        )
                        .await;
                        if let Err(error) = persist_window_processor_live_state(
                            &branch.runtime,
                            &self.processor,
                            replicated_state,
                            state,
                        )
                        .await
                        {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                state.entries.iter().map(|entry| &entry.message.acks),
                                error,
                            );
                            state.clear(aggregate);
                        }
                    }
                }
                RelayProcessorOperationNode::Reorderer {
                    output_routes,
                    order_by,
                    max_time: _,
                    compiled_program,
                    output_buffers,
                    arrival_sequence,
                } => {
                    if compiled_program.is_none() {
                        let udfs = branch.runtime.udf_executor(&branch.domain);
                        match compile_reorderer_program(
                            &self.processor,
                            &self.input_relays,
                            order_by,
                            batch.arrow_schema(),
                            udfs.as_ref(),
                        ) {
                            Ok(program) => *compiled_program = Some(Box::new(program)),
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    error,
                                );
                                return;
                            }
                        }
                    }
                    let Some(program) = compiled_program.as_ref() else {
                        return;
                    };
                    let execution_now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    let lookup_columns = HashMap::default();
                    let vm_batch = match project_vm_input_batch(
                        &program.program.input_schema,
                        &VmInputProjectionSources {
                            carrier: &batch.batch,
                            namespace_batches: &[],
                            strict_namespaces: &[],
                            keys: &batch.keys,
                            side_inputs: &materialized_values,
                            ingest_metadata: None,
                            lookup_columns: &lookup_columns,
                            uninitialized: None,
                        },
                    ) {
                        Ok(batch) => batch,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "reorderer '{}' failed to build BY input batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };
                    let key_result = execute_program_with_selection_in_context(
                        &program.program,
                        &vm_batch,
                        &VmExecutionContext {
                            now: execution_now,
                            injector: None,
                        },
                    )
                    .await;
                    let key_result = match key_result {
                        Ok(result) => result,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "reorderer '{}' failed to evaluate BY expressions: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };
                    if output_buffers.len() != output_routes.routes.len() {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind.as_str(),
                            &self.processor,
                            &self.error_policies,
                            batch.acks.iter(),
                            format!(
                                "reorderer '{}' output buffer count does not match its routes",
                                self.processor.as_str()
                            ),
                        );
                        return;
                    }
                    let row_count = batch.batch.batch().num_rows();
                    let mut row_ordering = Vec::with_capacity(row_count);
                    for row in 0..row_count {
                        let key = (0..program.key_count)
                            .map(|index| {
                                reorder_key_part(
                                    key_result.batch.column(program.key_column_offset + index),
                                    row,
                                )
                            })
                            .collect::<Vec<_>>();
                        let sequence = *arrival_sequence;
                        *arrival_sequence = arrival_sequence.saturating_add(1);
                        row_ordering.push((key, sequence));
                    }
                    let estimated_bytes = batch.estimated_bytes();
                    let route_batches = batch.into_attached_fanout(output_routes.routes.len());
                    let mut due_outputs = Vec::new();
                    for (output_index, route_batch) in route_batches.into_iter().enumerate() {
                        let messages = match route_batch.try_into_messages() {
                            Ok(messages) => messages,
                            Err(error_and_batch) => {
                                let (error, batch) = *error_and_batch;
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    format!(
                                        "reorderer '{}' failed to decode arrow batch: {}",
                                        self.processor.as_str(),
                                        error
                                    ),
                                );
                                continue;
                            }
                        };
                        let output_buffer = &mut output_buffers[output_index];
                        output_buffer.estimated_bytes = output_buffer
                            .estimated_bytes
                            .saturating_add(estimated_bytes);
                        output_buffer
                            .pending
                            .extend(messages.into_iter().enumerate().map(|(row, message)| {
                                let (key, arrival_sequence) = &row_ordering[row];
                                ReordererPendingMessage {
                                    key: key.clone(),
                                    arrival_sequence: *arrival_sequence,
                                    received_at: execution_now,
                                    message,
                                }
                            }));
                        let output = &mut output_routes.routes[output_index];
                        match output
                            .schedule_input_flush(execution_now, output_buffer.estimated_bytes)
                        {
                            Some(true) => {
                                output.force_flush_at(execution_now);
                                due_outputs.push(output_index);
                            }
                            Some(false) => {}
                            None => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    output_buffer
                                        .pending
                                        .iter()
                                        .map(|entry| &entry.message.acks),
                                    format!(
                                        "reorderer '{}' output '{}' has no flush policy",
                                        self.processor.as_str(),
                                        output.relay.as_str()
                                    ),
                                );
                                output_buffer.clear();
                            }
                        }
                    }
                    for output_index in due_outputs {
                        flush_branch_reorderer_output(
                            ReordererFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                input_relays: &self.input_relays,
                            },
                            &mut output_buffers[output_index],
                            output_index,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Correlator {
                    output_routes,
                    left_relays,
                    right_relays,
                    correlate_where,
                    match_policy,
                    max_time: _,
                    timeout_policy: _,
                    compiled_where_program,
                    compiled_output_programs,
                    state,
                } => {
                    let side = if left_relays.contains(incoming_relay) {
                        CorrelatorSide::Left
                    } else if right_relays.contains(incoming_relay) {
                        CorrelatorSide::Right
                    } else {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind.as_str(),
                            &self.processor,
                            &self.error_policies,
                            batch.acks.iter(),
                            format!(
                                "correlator '{}' received unexpected relay '{}'",
                                self.processor.as_str(),
                                incoming_relay.as_str()
                            ),
                        );
                        return;
                    };
                    let execution_now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    if compiled_where_program.is_none() {
                        let Some(left_relay) = left_relays.first() else {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "correlator '{}' has no LEFT input relays",
                                    self.processor.as_str()
                                ),
                            );
                            return;
                        };
                        let Some(right_relay) = right_relays.first() else {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "correlator '{}' has no RIGHT input relays",
                                    self.processor.as_str()
                                ),
                            );
                            return;
                        };
                        let left_schema = match relay_schema_for_runtime(
                            &branch.runtime,
                            &branch.domain,
                            left_relay,
                        ) {
                            Ok(schema) => schema,
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    error.to_string(),
                                );
                                return;
                            }
                        };
                        let right_schema = match relay_schema_for_runtime(
                            &branch.runtime,
                            &branch.domain,
                            right_relay,
                        ) {
                            Ok(schema) => schema,
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    error.to_string(),
                                );
                                return;
                            }
                        };
                        match compile_correlator_where_program(
                            &self.processor,
                            correlate_where,
                            left_relays,
                            left_schema.arrow_schema(),
                            right_relays,
                            right_schema.arrow_schema(),
                            branch.runtime.udf_executor(&branch.domain).as_ref(),
                        ) {
                            Ok(program) => *compiled_where_program = Some(Box::new(program)),
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    batch.acks.iter(),
                                    error,
                                );
                                return;
                            }
                        }
                    }
                    let Some(where_program) = compiled_where_program.as_ref() else {
                        return;
                    };
                    let messages = match batch.clone().try_into_messages() {
                        Ok(messages) => messages,
                        Err(error_and_batch) => {
                            let (error, batch) = *error_and_batch;
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                batch.acks.iter(),
                                format!(
                                    "correlator '{}' failed to decode arrow batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };

                    let mut correlations =
                        Vec::<(CorrelatorPendingMessage, CorrelatorPendingMessage)>::new();
                    for message in messages {
                        let incoming = CorrelatorPendingMessage {
                            received_at: execution_now,
                            message,
                            materialized_state: materialized_values.clone(),
                        };
                        match correlate_incoming_message(
                            &self.processor,
                            where_program,
                            side,
                            *match_policy,
                            state,
                            incoming,
                            execution_now,
                        )
                        .await
                        {
                            Ok(Some(pair)) => correlations.push(pair),
                            Ok(None) => {}
                            Err((reason, acks)) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    acks.iter(),
                                    reason,
                                );
                            }
                        }
                    }
                    if correlations.is_empty() {
                        return;
                    }

                    if output_routes.routes.is_empty()
                        || compiled_output_programs.len() != output_routes.routes.len()
                    {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind.as_str(),
                            &self.processor,
                            &self.error_policies,
                            correlations.iter().flat_map(|(left, right)| {
                                [&left.message.acks, &right.message.acks]
                            }),
                            format!(
                                "correlator '{}' output programs do not match its destinations",
                                self.processor.as_str()
                            ),
                        );
                        return;
                    }
                    let Some(left_relay) = left_relays.first() else {
                        return;
                    };
                    let Some(right_relay) = right_relays.first() else {
                        return;
                    };
                    let left_schema =
                        match relay_schema_for_runtime(&branch.runtime, &branch.domain, left_relay)
                        {
                            Ok(schema) => schema,
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    correlations.iter().flat_map(|(left, right)| {
                                        [&left.message.acks, &right.message.acks]
                                    }),
                                    error,
                                );
                                return;
                            }
                        };
                    let right_schema = match relay_schema_for_runtime(
                        &branch.runtime,
                        &branch.domain,
                        right_relay,
                    ) {
                        Ok(schema) => schema,
                        Err(error) => {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                correlations.iter().flat_map(|(left, right)| {
                                    [&left.message.acks, &right.message.acks]
                                }),
                                error,
                            );
                            return;
                        }
                    };
                    let materialized_stream_specs =
                        materialized_stream_specs_for_graph(&branch.runtime, &branch.domain, graph);
                    let current_branching = branch
                        .runtime
                        .executions
                        .get(&branch.domain)
                        .and_then(|execution| execution.relay_branchings.get(left_relay).cloned())
                        .unwrap_or_default();
                    let current_branch_schema = relay_branch_schema_for_runtime(
                        &branch.runtime,
                        &branch.domain,
                        left_relay,
                    );
                    let available_lookups = branch
                        .runtime
                        .executions
                        .get(&branch.domain)
                        .map(|execution| execution.lookups.clone())
                        .unwrap_or_default();
                    let udfs = branch
                        .runtime
                        .executions
                        .get(&branch.domain)
                        .map(|execution| execution.udfs.clone());
                    for (output_index, compiled_output_program) in compiled_output_programs
                        .iter_mut()
                        .enumerate()
                        .take(output_routes.routes.len())
                    {
                        if compiled_output_program.is_some() {
                            continue;
                        }
                        let output = &output_routes.routes[output_index];
                        if output.construction.assignments.is_empty() {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                correlations.iter().flat_map(|(left, right)| {
                                    [&left.message.acks, &right.message.acks]
                                }),
                                format!(
                                    "correlator '{}' TO output '{}' has no SET assignments",
                                    self.processor.as_str(),
                                    output.relay.as_str()
                                ),
                            );
                            return;
                        }
                        let output_schema = match relay_schema_for_runtime(
                            &branch.runtime,
                            &branch.domain,
                            &output.relay,
                        ) {
                            Ok(schema) => schema,
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    correlations.iter().flat_map(|(left, right)| {
                                        [&left.message.acks, &right.message.acks]
                                    }),
                                    error,
                                );
                                return;
                            }
                        };
                        let compiled = CorrelatorOutputCompileContext {
                            processor: &self.processor,
                            left_schema: left_schema.arrow_schema(),
                            left_sensitivity: left_schema.vm_sensitivity(),
                            right_schema: right_schema.arrow_schema(),
                            right_sensitivity: right_schema.vm_sensitivity(),
                            output_relay: &output.relay,
                            output_schema: output_schema.arrow_schema(),
                            output_sensitivity: output_schema.vm_sensitivity(),
                            construction: &output.construction,
                            runtime: RuntimeVmCompileContext {
                                available_materialized_streams: &materialized_stream_specs,
                                available_lookups: &available_lookups,
                                current_branching: &current_branching,
                                current_branch_schema: current_branch_schema.as_ref(),
                                current_branch_sensitivity: None,
                                udfs: udfs.as_ref(),
                            },
                        }
                        .compile();
                        match compiled {
                            Ok(program) => {
                                *compiled_output_program = Some(Box::new(program));
                            }
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    correlations.iter().flat_map(|(left, right)| {
                                        [&left.message.acks, &right.message.acks]
                                    }),
                                    error,
                                );
                                return;
                            }
                        }
                    }

                    let output_count = output_routes.routes.len();
                    let mut messages_by_output = (0..output_count)
                        .map(|_| Vec::<RelayMessage>::new())
                        .collect::<Vec<_>>();
                    for (left, right) in correlations {
                        let key = left.message.key.clone();
                        let combined =
                            match correlator_input_row(&left.message.record, &right.message.record)
                            {
                                Ok(combined) => combined,
                                Err(error) => {
                                    branch.runtime.handle_internal_processor_error_for_acks(
                                        &branch.domain,
                                        self.kind.as_str(),
                                        &self.processor,
                                        &self.error_policies,
                                        [&left.message.acks, &right.message.acks],
                                        format!(
                                            "correlator '{}' failed to build paired Arrow input: \
                                             {error}",
                                            self.processor.as_str()
                                        ),
                                    );
                                    continue;
                                }
                            };
                        let mut materialized_state = left.materialized_state.clone();
                        materialized_state.extend(right.materialized_state.clone());
                        let mut pair_acks = Some(AckSet::merged([
                            left.message.acks.attached(),
                            right.message.acks.attached(),
                        ]));
                        for output_index in 0..output_count {
                            let route_acks = if output_index + 1 == output_count {
                                pair_acks
                                    .take()
                                    .expect("last correlator output must own the pair ACKs")
                            } else {
                                pair_acks
                                    .as_ref()
                                    .expect("correlator pair ACKs must remain available")
                                    .attached()
                            };
                            let Some(output_program) =
                                compiled_output_programs[output_index].as_deref()
                            else {
                                route_acks.no_ack(format!(
                                    "correlator '{}' output program is unavailable",
                                    self.processor.as_str()
                                ));
                                continue;
                            };
                            match evaluate_correlator_output_message(
                                &self.processor,
                                output_program,
                                key.clone(),
                                combined.clone(),
                                &materialized_state,
                                route_acks,
                                execution_now,
                            )
                            .await
                            {
                                Ok(Some(message)) => {
                                    messages_by_output[output_index].push(message);
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    let policy = output_routes.routes[output_index]
                                        .message_error_policy
                                        .clone();
                                    branch
                                        .runtime
                                        .handle_structured_message_error(MessageErrorHandling {
                                            domain: &branch.domain,
                                            node_kind: self.kind.as_str(),
                                            node: &self.processor,
                                            source_route: Some(
                                                &output_routes.routes[output_index].relay,
                                            ),
                                            policy: &policy,
                                            message: error.message,
                                            error: error.error,
                                            partial_output: error.partial_output,
                                            materialized_state: error.materialized_state,
                                            ingest_metadata: None,
                                        })
                                        .await;
                                }
                            }
                        }
                    }
                    for (output_index, messages) in messages_by_output.into_iter().enumerate() {
                        enqueue_correlator_output(
                            CorrelatorOutputContext {
                                graph,
                                branch,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                            },
                            output_index,
                            messages,
                            execution_now,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Junction { output_routes } => {
                    flush_branch_junction(
                        JunctionFlushContext {
                            graph,
                            branch,
                            node_kind: self.kind.as_str(),
                            processor: &self.processor,
                            error_policies: &self.error_policies,
                            input_relays: &self.input_relays,
                            output_routes,
                        },
                        batch,
                    )
                    .await;
                }
                RelayProcessorOperationNode::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                    output_buffers,
                    session,
                } => {
                    if output_buffers.len() != output_routes.routes.len() {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind.as_str(),
                            &self.processor,
                            &self.error_policies,
                            batch.acks.iter(),
                            format!(
                                "inferencer '{}' output buffer count does not match its routes",
                                self.processor.as_str()
                            ),
                        );
                        return;
                    }
                    let now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    let route_batches = batch.into_attached_fanout(output_routes.routes.len());
                    let mut due_outputs = Vec::new();
                    for (output_index, route_batch) in route_batches.into_iter().enumerate() {
                        let output_buffer = &mut output_buffers[output_index];
                        output_buffer.push(route_batch);
                        let output = &mut output_routes.routes[output_index];
                        match output.schedule_input_flush(now, output_buffer.estimated_bytes()) {
                            Some(true) => {
                                output.force_flush_at(now);
                                due_outputs.push(output_index);
                            }
                            Some(false) => {}
                            None => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    output_buffer
                                        .pending
                                        .iter()
                                        .flat_map(|batch| batch.acks.iter()),
                                    format!(
                                        "inferencer '{}' output '{}' has no flush policy",
                                        self.processor.as_str(),
                                        output.relay.as_str()
                                    ),
                                );
                                output_buffer.clear();
                            }
                        }
                    }
                    for output_index in due_outputs {
                        flush_branch_inferencer_output(
                            InferencerFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                resource,
                                resource_version: *resource_version,
                                file,
                                inputs,
                                output_schema,
                                input_relays: &self.input_relays,
                                session,
                            },
                            &mut output_buffers[output_index],
                            output_index,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::WasmProcessor {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    limits,
                    compiled,
                    instance,
                    replicated_state,
                    ack_map,
                    next_ack_token,
                    pending,
                } => {
                    pending.push(batch);
                    flush_branch_wasm_processor(
                        WasmFlushContext {
                            graph,
                            branch,
                            node_kind: self.kind.as_str(),
                            processor: &self.processor,
                            error_policies: &self.error_policies,
                            input_relays: &self.input_relays,
                            output_routes,
                            resource,
                            resource_version: *resource_version,
                            file,
                            limits: *limits,
                            replicated_state,
                        },
                        compiled,
                        instance,
                        ack_map,
                        next_ack_token,
                        pending,
                    )
                    .await;
                }
            }
        })
    }

    fn tick<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
        now: Timestamp,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.flush_due_collected_inputs(graph, branch, now).await;
            flush_due_processor_outputs(
                ProcessorOutputDispatchContext {
                    graph,
                    branch,
                    node_kind: self.kind.as_str(),
                    source_kind: self.kind,
                    processor: &self.processor,
                    error_policies: &self.error_policies,
                    input_relays: &self.input_relays,
                    filter_source: ProcessorOutputFilterSource::InputRelays,
                    resolved_materialized_state: None,
                },
                self.operation.output_routes_mut(),
                now,
            )
            .await;
            match &mut self.operation {
                RelayProcessorOperationNode::Deduplicator { .. } => {}
                RelayProcessorOperationNode::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
                    compiled_aggregates,
                    state,
                    replicated_state,
                } => {
                    let due = window_width_met(state, *width_messages, *width_duration, now);
                    let changed = flush_ready_window_processor(
                        WindowFlushContext {
                            graph,
                            node_kind: self.kind.as_str(),
                            processor: &self.processor,
                            error_policies: &self.error_policies,
                            branch,
                            output_routes,
                        },
                        state,
                        aggregate,
                        compiled_aggregates,
                        WindowBounds {
                            width_messages: *width_messages,
                            step_messages: *step_messages,
                            width_duration: *width_duration,
                            step_duration: *step_duration,
                        },
                        now,
                    )
                    .await;
                    if (due || changed)
                        && let Err(error) = persist_window_processor_live_state(
                            &branch.runtime,
                            &self.processor,
                            replicated_state,
                            state,
                        )
                        .await
                    {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind.as_str(),
                            &self.processor,
                            &self.error_policies,
                            state.entries.iter().map(|entry| &entry.message.acks),
                            error,
                        );
                        state.clear(aggregate);
                    }
                }
                RelayProcessorOperationNode::Junction { .. } => {}
                RelayProcessorOperationNode::Reorderer {
                    output_routes,
                    max_time,
                    output_buffers,
                    ..
                } => {
                    let mut due_outputs = Vec::new();
                    for (output_index, output_buffer) in output_buffers.iter().enumerate() {
                        if output_buffer.pending.is_empty() {
                            continue;
                        }
                        let max_time_due = output_buffer.pending.first().is_some_and(|entry| {
                            checked_add_duration_to_timestamp(entry.received_at, *max_time) <= now
                        });
                        let flush_due = output_routes.routes[output_index].flush_deadline_due(now);
                        if max_time_due || flush_due {
                            output_routes.routes[output_index].force_flush_at(now);
                            due_outputs.push(output_index);
                        }
                    }
                    for output_index in due_outputs {
                        flush_branch_reorderer_output(
                            ReordererFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                input_relays: &self.input_relays,
                            },
                            &mut output_buffers[output_index],
                            output_index,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Correlator {
                    max_time,
                    timeout_policy,
                    state,
                    ..
                } => {
                    let timed_out = {
                        let mut timed_out = Vec::new();

                        let mut left_remaining = Vec::new();
                        for entry in std::mem::take(&mut state.pending_left) {
                            if checked_add_duration_to_timestamp(entry.received_at, *max_time)
                                <= now
                            {
                                timed_out.push((timeout_policy.left.clone(), entry.message));
                            } else {
                                left_remaining.push(entry);
                            }
                        }
                        state.pending_left = left_remaining;

                        let mut right_remaining = Vec::new();
                        for entry in std::mem::take(&mut state.pending_right) {
                            if checked_add_duration_to_timestamp(entry.received_at, *max_time)
                                <= now
                            {
                                timed_out.push((timeout_policy.right.clone(), entry.message));
                            } else {
                                right_remaining.push(entry);
                            }
                        }
                        state.pending_right = right_remaining;

                        timed_out
                    };
                    for (action, message) in timed_out {
                        handle_correlator_timeout_action(
                            graph,
                            branch,
                            self.kind.as_str(),
                            &self.processor,
                            &self.error_policies,
                            &action,
                            message,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                    output_buffers,
                    session,
                } => {
                    let due_outputs = output_buffers
                        .iter()
                        .enumerate()
                        .filter_map(|(output_index, output_buffer)| {
                            (!output_buffer.pending.is_empty()
                                && output_routes.routes[output_index].flush_deadline_due(now))
                            .then_some(output_index)
                        })
                        .collect::<Vec<_>>();
                    for output_index in due_outputs {
                        output_routes.routes[output_index].force_flush_at(now);
                        flush_branch_inferencer_output(
                            InferencerFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                resource,
                                resource_version: *resource_version,
                                file,
                                inputs,
                                output_schema,
                                input_relays: &self.input_relays,
                                session,
                            },
                            &mut output_buffers[output_index],
                            output_index,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::WasmProcessor {
                    output_routes,
                    instance,
                    replicated_state,
                    ack_map,
                    ..
                } => {
                    let Some(branch_instance) = instance.as_mut() else {
                        return;
                    };
                    let due_timeouts = branch_instance.take_due_timeout_requests(now);
                    if due_timeouts.is_empty() {
                        return;
                    }
                    if output_routes.routes.is_empty() {
                        for (_, context) in std::mem::take(ack_map) {
                            context.acks.no_ack(format!(
                                "wasm processor '{}' has no output destinations",
                                self.processor.as_str()
                            ));
                        }
                        return;
                    }
                    let Some(schemas) = wasm_guest_call_schemas(
                        branch,
                        &self.processor,
                        &self.input_relays,
                        output_routes,
                        ack_map,
                    ) else {
                        return;
                    };
                    let output_key = branch.key.clone();
                    for timeout in due_timeouts {
                        let timeout_result = instance
                            .as_mut()
                            .expect("WASM timeout instance was checked")
                            .on_timeout(timeout.handle)
                            .await;
                        let outputs = match timeout_result {
                            Ok(outputs) => outputs,
                            Err(error) => {
                                let resource_limit_exceeded = error.is_resource_limit_exceeded();
                                let reason = format!(
                                    "wasm processor '{}' failed timeout callback: {}",
                                    self.processor.as_str(),
                                    error
                                );
                                branch.runtime.handle_general_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    ack_map.values().map(|context| &context.acks),
                                    reason,
                                );
                                ack_map.clear();
                                if resource_limit_exceeded {
                                    *instance = None;
                                }
                                return;
                            }
                        };
                        if dispatch_wasm_output_envelopes(
                            WasmOutputContext {
                                graph,
                                branch,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                input_relays: &self.input_relays,
                                input_schema: &schemas.input,
                                output_schemas: &schemas.outputs,
                                key: &output_key,
                                dispatch_error: "failed to forward timeout output",
                            },
                            outputs,
                            ack_map,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                    }
                    if let Err(error) = persist_wasm_guest_state(
                        &branch.runtime,
                        &self.processor,
                        replicated_state,
                        instance,
                    )
                    .await
                    {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            self.kind.as_str(),
                            &self.processor,
                            &self.error_policies,
                            std::iter::empty::<&AckSet>(),
                            error,
                        );
                    }
                }
            }
        })
    }

    /// Asks a WASM guest to release the output it is still buffering, because the host is
    /// quiescing this branch for a handoff or shutdown.
    ///
    /// Native processors buffer inside runtime-owned route state that the caller force-flushes
    /// directly; a guest owns its own buffering, so the host has to ask before it can conclude the
    /// branch has drained.
    fn flush_guest_buffers<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        branch: &'a mut BranchRuntime,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let RelayProcessorOperationNode::WasmProcessor {
                output_routes,
                instance,
                ack_map,
                ..
            } = &mut self.operation
            else {
                return;
            };
            if instance.is_none() {
                return;
            }
            if output_routes.routes.is_empty() {
                return;
            }
            let Some(schemas) = wasm_guest_call_schemas(
                branch,
                &self.processor,
                &self.input_relays,
                output_routes,
                ack_map,
            ) else {
                return;
            };
            let flush_result = instance
                .as_mut()
                .expect("WASM flush instance was checked")
                .flush()
                .await;
            let outputs = match flush_result {
                Ok(outputs) => outputs,
                Err(error) => {
                    let resource_limit_exceeded = error.is_resource_limit_exceeded();
                    let reason = format!(
                        "wasm processor '{}' failed quiesce flush: {}",
                        self.processor.as_str(),
                        error
                    );
                    branch.runtime.handle_general_error_for_acks(
                        &branch.domain,
                        self.kind.as_str(),
                        &self.processor,
                        &self.error_policies,
                        ack_map.values().map(|context| &context.acks),
                        reason,
                    );
                    ack_map.clear();
                    if resource_limit_exceeded {
                        *instance = None;
                    }
                    return;
                }
            };
            if outputs.is_empty() {
                return;
            }
            let output_key = branch.key.clone();
            let _ = dispatch_wasm_output_envelopes(
                WasmOutputContext {
                    graph,
                    branch,
                    node_kind: self.kind.as_str(),
                    processor: &self.processor,
                    error_policies: &self.error_policies,
                    output_routes,
                    input_relays: &self.input_relays,
                    input_schema: &schemas.input,
                    output_schemas: &schemas.outputs,
                    key: &output_key,
                    dispatch_error: "failed to forward quiesce flush output",
                },
                outputs,
                ack_map,
            )
            .await;
        })
    }

    fn next_deadline(&self) -> Option<Timestamp> {
        let operation_deadline = match &self.operation {
            RelayProcessorOperationNode::Deduplicator { .. } => None,
            RelayProcessorOperationNode::WindowProcessor {
                width_duration,
                state,
                ..
            } => window_next_deadline(state, *width_duration),
            RelayProcessorOperationNode::Junction { .. } => None,
            RelayProcessorOperationNode::Reorderer {
                max_time,
                output_buffers,
                ..
            } => output_buffers
                .iter()
                .filter_map(|buffer| buffer.pending.first())
                .map(|entry| checked_add_duration_to_timestamp(entry.received_at, *max_time))
                .min(),
            RelayProcessorOperationNode::Correlator {
                max_time, state, ..
            } => state
                .pending_left
                .iter()
                .chain(state.pending_right.iter())
                .map(|entry| checked_add_duration_to_timestamp(entry.received_at, *max_time))
                .min(),
            RelayProcessorOperationNode::Inferencer { .. } => None,
            RelayProcessorOperationNode::WasmProcessor { instance, .. } => {
                wasm_instance_next_deadline(instance.as_deref())
            }
        };
        operation_deadline
            .into_iter()
            .chain(
                self.input_collectors
                    .values()
                    .filter_map(|collector| collector.deadline),
            )
            .chain(self.operation.output_routes().next_flush())
            .min()
    }
}

/// The exact relay schemas one WASM guest call encodes against.
struct WasmGuestCallSchemas {
    input: Arc<CompiledSchema>,
    outputs: Vec<(Identifier, Arc<CompiledSchema>)>,
}

/// Resolves the input and output relay schemas a WASM guest call needs. Returns `None` after
/// NACKing everything the branch is holding when the graph can no longer describe them.
fn wasm_guest_call_schemas(
    branch: &BranchRuntime,
    processor: &Identifier,
    input_relays: &[Identifier],
    output_routes: &RelayProcessorOutputsNode,
    ack_map: &mut WasmAckMap,
) -> Option<WasmGuestCallSchemas> {
    let Some(input_relay) = input_relays.first() else {
        for (_, context) in std::mem::take(ack_map) {
            context.acks.no_ack(format!(
                "wasm processor '{}' has no input relays",
                processor.as_str()
            ));
        }
        return None;
    };
    let input_schema = match relay_schema_for_runtime(&branch.runtime, &branch.domain, input_relay)
    {
        Ok(schema) => schema,
        Err(error) => {
            for (_, context) in std::mem::take(ack_map) {
                context.acks.no_ack(error.clone());
            }
            return None;
        }
    };
    let mut output_schemas = Vec::with_capacity(output_routes.routes.len());
    for output in &output_routes.routes {
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, &output.relay) {
            Ok(schema) => output_schemas.push((output.relay.clone(), schema)),
            Err(error) => {
                for (_, context) in std::mem::take(ack_map) {
                    context.acks.no_ack(error.clone());
                }
                return None;
            }
        }
    }
    Some(WasmGuestCallSchemas {
        input: input_schema,
        outputs: output_schemas,
    })
}

fn wasm_instance_next_deadline(
    instance: Option<&nervix_wasm::WasmBranchInstance>,
) -> Option<Timestamp> {
    instance?
        .timeout_requests()
        .iter()
        .filter_map(|request| {
            let delay_nanos = i64::try_from(request.delay.as_nanos()).ok()?;
            Some(Timestamp::from_unix_nanos(
                request
                    .requested_at
                    .unix_nanos()
                    .saturating_add(delay_nanos),
            ))
        })
        .min()
}

impl RelayProcessorTemplate {
    fn instantiate_output(output: &RelayProcessorOutputTemplate) -> RelayProcessorOutputNode {
        RelayProcessorOutputNode {
            relay: output.output_relay.clone(),
            construction: output.construction.clone(),
            branch: None,
            flush_policy: output.flush_policy,
            message_error_policy: output.message_error_policy.clone(),
            pending: Vec::new(),
            next_flush: None,
            compiled_program: None,
            compiled_branch_program: None,
        }
    }

    fn instantiate_outputs(outputs: &RelayProcessorOutputsTemplate) -> RelayProcessorOutputsNode {
        RelayProcessorOutputsNode {
            routes: outputs
                .routes
                .iter()
                .map(Self::instantiate_output)
                .collect(),
        }
    }

    fn instantiate(
        &self,
        runtime: &Runtime,
        domain: &Domain,
        key: &Option<BranchKey>,
    ) -> Result<RelayProcessorNode, String> {
        Ok(RelayProcessorNode {
            kind: self.kind,
            processor: self.processor.clone(),
            input_relays: self.input_relays.clone(),
            input_collectors: self
                .input_collect_policies
                .iter()
                .map(|(relay, policy)| (relay.clone(), RuntimeInputCollector::new(*policy)))
                .collect(),
            error_policies: self.error_policies.clone(),
            from_where: self.from_where.clone(),
            compiled_from_where: HashMap::default(),
            filter_where: self.filter_where.clone(),
            materialized_state: self.materialized_state.clone(),
            pending_materialized: VecDeque::new(),
            compiled_filter_where: HashMap::default(),
            operation: match &self.operation {
                RelayProcessorOperationTemplate::Deduplicator {
                    output_routes,
                    deduplicate_on,
                    max_time,
                } => RelayProcessorOperationNode::Deduplicator {
                    output_routes: Self::instantiate_outputs(output_routes),
                    deduplicate_on: deduplicate_on.clone(),
                    max_time: *max_time,
                    compiled_key_program: None,
                    state: runtime
                        .replicated_deduplicator_state(
                            runtime.state_placement(
                                domain,
                                RuntimeStateKind::Deduplicator,
                                self.kind,
                                &self.processor,
                                key.clone(),
                            ),
                            Vec::new(),
                            0,
                        )
                        .map_err(|error| error.to_string())?,
                },
                RelayProcessorOperationTemplate::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
                    compiled_aggregates,
                } => {
                    let replicated_state = runtime
                        .replicated_window_processor_state(
                            runtime.state_placement(
                                domain,
                                RuntimeStateKind::WindowProcessor,
                                self.kind,
                                &self.processor,
                                key.clone(),
                            ),
                            None,
                            Vec::new(),
                            0,
                        )
                        .map_err(|error| error.to_string())?;
                    let input_relay = self.input_relays.first().ok_or_else(|| {
                        format!(
                            "window processor '{}' requires an input relay",
                            self.processor.as_str()
                        )
                    })?;
                    let input_schema = relay_schema_for_runtime(runtime, domain, input_relay)?;
                    let state = replicated_state.restore_state(aggregate, &input_schema)?;
                    RelayProcessorOperationNode::WindowProcessor {
                        output_routes: Self::instantiate_outputs(output_routes),
                        width_messages: *width_messages,
                        step_messages: *step_messages,
                        width_duration: *width_duration,
                        step_duration: *step_duration,
                        aggregate: aggregate.clone(),
                        compiled_aggregates: compiled_aggregates.clone(),
                        state,
                        replicated_state,
                    }
                }
                RelayProcessorOperationTemplate::Reorderer {
                    output_routes,
                    order_by,
                    max_time,
                } => {
                    let output_routes = Self::instantiate_outputs(output_routes);
                    let output_buffers = (0..output_routes.routes.len())
                        .map(|_| ReordererOutputBuffer::default())
                        .collect();
                    RelayProcessorOperationNode::Reorderer {
                        output_routes,
                        order_by: order_by.clone(),
                        max_time: *max_time,
                        compiled_program: None,
                        output_buffers,
                        arrival_sequence: 0,
                    }
                }
                RelayProcessorOperationTemplate::Correlator {
                    output_routes,
                    left_relays,
                    right_relays,
                    correlate_where,
                    match_policy,
                    max_time,
                    timeout_policy,
                } => {
                    let output_routes = Self::instantiate_outputs(output_routes);
                    let compiled_output_programs =
                        (0..output_routes.routes.len()).map(|_| None).collect();
                    RelayProcessorOperationNode::Correlator {
                        output_routes,
                        left_relays: left_relays.clone(),
                        right_relays: right_relays.clone(),
                        correlate_where: correlate_where.clone(),
                        match_policy: *match_policy,
                        max_time: *max_time,
                        timeout_policy: timeout_policy.clone(),
                        compiled_where_program: None,
                        compiled_output_programs,
                        state: CorrelatorBranchState::default(),
                    }
                }
                RelayProcessorOperationTemplate::Junction { output_routes } => {
                    RelayProcessorOperationNode::Junction {
                        output_routes: Self::instantiate_outputs(output_routes),
                    }
                }
                RelayProcessorOperationTemplate::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                } => {
                    let output_routes = Self::instantiate_outputs(output_routes);
                    let output_buffers = (0..output_routes.routes.len())
                        .map(|_| InferencerOutputBuffer::default())
                        .collect();
                    RelayProcessorOperationNode::Inferencer {
                        output_routes,
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
                        inputs: inputs.clone(),
                        output_schema: output_schema.clone(),
                        output_buffers,
                        session: None,
                    }
                }
                RelayProcessorOperationTemplate::WasmProcessor {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    limits,
                    compiled,
                } => {
                    let replicated_state = runtime
                        .replicated_wasm_processor_state(
                            runtime.state_placement(
                                domain,
                                RuntimeStateKind::WasmProcessor,
                                self.kind,
                                &self.processor,
                                key.clone(),
                            ),
                            Vec::new(),
                            0,
                        )
                        .map_err(|error| error.to_string())?;
                    RelayProcessorOperationNode::WasmProcessor {
                        output_routes: Self::instantiate_outputs(output_routes),
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
                        limits: *limits,
                        compiled: compiled.clone(),
                        instance: None,
                        replicated_state,
                        ack_map: HashMap::default(),
                        next_ack_token: 1,
                        pending: Vec::new(),
                    }
                }
            },
            last_graph: None,
            applied_generation: 0,
        })
    }
}

impl BranchInstanceTemplate {
    async fn prepare_wasm_processors(
        &mut self,
        runtime: &Runtime,
        domain: &Domain,
    ) -> Result<(), String> {
        for processor in self.processors.values_mut() {
            tokio::task::consume_budget().await;
            if let RelayProcessorOperationTemplate::WasmProcessor {
                resource,
                resource_version,
                file,
                compiled,
                ..
            } = &mut processor.operation
            {
                *compiled = Some(
                    runtime
                        .compile_wasm_processor_module(
                            domain,
                            &processor.processor,
                            resource,
                            *resource_version,
                            file,
                        )
                        .await?,
                );
            }
        }
        Ok(())
    }

    fn instantiate(
        &self,
        runtime: &Runtime,
        domain: &Domain,
        key: Option<BranchKey>,
    ) -> Result<Mutex<BranchRuntime>, String> {
        let relays = self
            .relays
            .iter()
            .map(|(relay, template)| {
                (
                    relay.clone(),
                    ConcreteRelayRuntime::new(ConcreteRelayRuntimeBuild {
                        runtime: runtime.clone(),
                        domain: domain.clone(),
                        relay: relay.clone(),
                        registry: template.registry.clone(),
                        services: template.services.clone(),
                        key: key.clone(),
                    }),
                )
            })
            .collect::<HashMap<_, _>>();
        let materializers = self
            .materialized_streams
            .iter()
            .filter(|relay| !runtime.materialized_relay_is_scheduled(domain, relay))
            .map(|relay| {
                let placement = runtime.state_placement(
                    domain,
                    RuntimeStateKind::MaterializedRelay,
                    ModelKind::Materializer,
                    relay,
                    key.clone(),
                );
                runtime
                    .replicated_materialized_stream_state(placement, None, Vec::new(), 0)
                    .map(|state| (relay.clone(), state))
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<HashMap<_, _>, String>>()?;
        let processors = self
            .processors
            .iter()
            .map(|(processor, template)| {
                Ok((
                    processor.clone(),
                    template.instantiate(runtime, domain, &key)?,
                ))
            })
            .collect::<Result<HashMap<_, _>, String>>()?;
        Ok(Mutex::new(BranchRuntime {
            key,
            runtime: runtime.clone(),
            domain: domain.clone(),
            source_kind: self.source_kind,
            source: self.source.clone(),
            root_relay: self.root_relay.clone(),
            relays,
            materializers,
            materializer_epoch: None,
            processors,
            error_policies: self.error_policies.clone(),
        }))
    }
}

impl BranchRuntime {
    fn restore_presence(&self, last_ingestion: Timestamp) {
        for relay in self.relays.values() {
            relay.registry.touch(&self.key, last_ingestion);
            self.runtime
                .touch_stream_key(&self.domain, &relay.relay, &self.key, last_ingestion);
        }
    }

    fn detach(&self) {
        for relay in self.relays.values() {
            relay.registry.remove(&self.key);
            self.runtime
                .remove_stream_key_presence(&self.domain, &relay.relay, &self.key);
        }
    }

    async fn evict(&mut self) {
        for processor in self.processors.values_mut() {
            processor.drop_collected_inputs("processor branch was evicted");
        }
        self.detach();
        for (relay, materialized_state) in &self.materializers {
            let local_node_id = self.runtime.local_node_id.read().clone();
            let is_primary = match (
                materialized_state.primary_node.as_deref(),
                local_node_id.as_deref(),
            ) {
                (Some(primary_node), Some(local_node_id)) => primary_node == local_node_id,
                (None, _) => true,
                _ => false,
            };
            if is_primary
                && let Err(error) = self
                    .runtime
                    .delete_materialized_stream_key(materialized_state, &self.key)
                    .await
            {
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    key = branch_key_display(&self.key),
                    error = %error,
                    "failed to delete evicted materialized relay key"
                );
            }
        }
    }

    fn reconcile_materializer_membership(&mut self, relay: &Identifier) {
        let current_epoch = self
            .runtime
            .materializer_epoch(&self.domain)
            .load(Ordering::Acquire);
        if self.materializer_epoch == Some(current_epoch) {
            return;
        }
        let desired_relays = self
            .runtime
            .executions
            .get(&self.domain)
            .map(|execution| {
                execution
                    .materialized_stream_specs
                    .keys()
                    .filter(|relay| {
                        !execution
                            .materialized_stream_owner_nodes
                            .get(*relay)
                            .is_some_and(Option::is_some)
                    })
                    .cloned()
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        self.materializers
            .retain(|identifier, _| desired_relays.contains(identifier));
        if desired_relays.contains(relay) && !self.materializers.contains_key(relay) {
            let placement = self.runtime.state_placement(
                &self.domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Materializer,
                relay,
                self.key.clone(),
            );
            match self
                .runtime
                .replicated_materialized_stream_state(placement, None, Vec::new(), 0)
            {
                Ok(state) => {
                    self.materializers.insert(relay.clone(), state);
                }
                Err(error) => {
                    warn!(
                        domain = self.domain.as_str(),
                        relay = relay.as_str(),
                        error = %error,
                        "failed to reconcile materialized relay membership"
                    );
                    return;
                }
            }
        }
        self.materializer_epoch = Some(current_epoch);
    }

    async fn materialize_stream_batch(&mut self, relay: &Identifier, batch: &RelayRecordBatch) {
        if self
            .runtime
            .materialized_relay_is_scheduled(&self.domain, relay)
        {
            return;
        }
        self.reconcile_materializer_membership(relay);
        let Some(state) = self.materializers.get(relay) else {
            return;
        };
        let messages = match batch.detached().try_into_messages() {
            Ok(messages) => messages,
            Err(error_and_batch) => {
                let (error, _) = *error_and_batch;
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    branch = branch_key_display(&self.key),
                    error = %error,
                    "failed to decode branch-local materialized state batch"
                );
                return;
            }
        };
        for message in messages {
            tokio::task::consume_budget().await;
            if let Err(error) = self
                .runtime
                .update_materialized_stream_last_by_timestamp(state, &batch.key, &message.record)
                .await
            {
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    branch = branch_key_display(&self.key),
                    error = %error,
                    "failed to update branch-local materialized relay state"
                );
            }
        }
    }

    fn processor_has_pending_materialized(&self, processor_id: &Identifier) -> bool {
        self.processors
            .get(processor_id)
            .is_some_and(|processor| !processor.pending_materialized.is_empty())
    }

    async fn retry_processor_pending_materialized(
        &mut self,
        graph: &SharedActiveGraph,
        processor_id: &Identifier,
    ) {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            return;
        };
        let pending_count = processor.pending_materialized.len();
        for _ in 0..pending_count {
            let Some((incoming_relay, batch)) = processor.pending_materialized.pop_front() else {
                break;
            };
            processor.execute(graph, self, &incoming_relay, batch).await;
        }
        self.processors.insert(processor_id.clone(), processor);
    }

    async fn retry_materialized_waiters(
        &mut self,
        graph: &SharedActiveGraph,
        updated_relay: &Identifier,
    ) {
        let processor_ids = self
            .processors
            .iter()
            .filter(|(_, processor)| {
                processor
                    .materialized_state
                    .iter()
                    .any(|dependency| &dependency.relay == updated_relay)
                    && !processor.pending_materialized.is_empty()
            })
            .map(|(identifier, _)| identifier.clone())
            .collect::<Vec<_>>();
        for processor_id in processor_ids {
            let Some(mut processor) = self.processors.remove(&processor_id) else {
                continue;
            };
            let pending_count = processor.pending_materialized.len();
            for _ in 0..pending_count {
                let Some((incoming_relay, batch)) = processor.pending_materialized.pop_front()
                else {
                    break;
                };
                processor.execute(graph, self, &incoming_relay, batch).await;
            }
            self.processors.insert(processor_id, processor);
        }
    }

    async fn dispatch(&mut self, graph: &SharedActiveGraph, batch: RelayRecordBatch) {
        let root_relay = self.root_relay.clone();
        self.runtime
            .metrics
            .observe_global_node_sent(NodeBatchObservation {
                domain: &self.domain,
                kind: self.source_kind,
                node: &self.source,
                relay: &root_relay,
                physical_node_id: self.runtime.local_node_id.read().as_deref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            });
        self.runtime.metrics.observe_branch_node_sent(
            branch_key_display(&self.key),
            NodeBatchObservation {
                domain: &self.domain,
                kind: self.source_kind,
                node: &self.source,
                relay: &root_relay,
                physical_node_id: self.runtime.local_node_id.read().as_deref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            },
        );
        self.runtime.mark_branch_aggregated_metrics_updated(
            &self.domain,
            self.source_kind,
            &self.source,
        );
        if self
            .dispatch_stream(graph, &root_relay, &batch)
            .await
            .is_err()
        {
            let reason = "branched root relay dispatch failed".to_string();
            if self.source_kind == ModelKind::Ingestor {
                self.runtime.handle_general_error_for_acks(
                    &self.domain,
                    self.source_kind.as_str(),
                    &self.source,
                    &self.error_policies,
                    batch.acks.iter(),
                    reason,
                );
            } else {
                self.runtime.handle_internal_processor_error_for_acks(
                    &self.domain,
                    self.source_kind.as_str(),
                    &self.source,
                    &self.error_policies,
                    batch.acks.iter(),
                    reason,
                );
            }
            return;
        }
        for ack in batch.acks.iter() {
            ack.ack_success();
        }
    }

    fn dispatch_stream<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        relay: &'a Identifier,
        batch: &'a RelayRecordBatch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RelayDispatchResult> + Send + 'a>> {
        Box::pin(async move {
            let Some(runtime_stream) = self.relays.get_mut(relay) else {
                return Err(Box::new(batch.clone()));
            };
            runtime_stream.dispatch_boundary(batch).await?;
            self.materialize_stream_batch(relay, batch).await;
            self.retry_materialized_waiters(graph, relay).await;
            self.runtime.metrics.observe_branch_stream_received(
                branch_key_display(&self.key),
                RelayBatchObservation {
                    domain: &self.domain,
                    relay,
                    physical_node_id: self.runtime.local_node_id.read().as_deref(),
                    messages: batch.message_count(),
                    bytes: batch.estimated_bytes(),
                    domain_timestamp: batch.domain_timestamp(),
                },
            );

            Ok(())
        })
    }

    async fn execute_processor_input(
        &mut self,
        graph: &SharedActiveGraph,
        processor_id: &Identifier,
        incoming_relay: &Identifier,
        batch: RelayRecordBatch,
    ) {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            for ack in batch.acks.iter() {
                ack.no_ack("processor is not instantiated for this branch");
            }
            return;
        };
        let delivery_observation = batch.delivery_observation(current_timestamp());
        let physical_node_id = self.runtime.local_node_id.read().clone();
        self.runtime
            .metrics
            .observe_global_node_received(NodeBatchObservation {
                domain: &self.domain,
                kind: processor.kind,
                node: &processor.processor,
                relay: incoming_relay,
                physical_node_id: physical_node_id.as_deref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: delivery_observation.domain_timestamp,
            });
        self.runtime.metrics.observe_branch_node_received(
            branch_key_display(&self.key),
            NodeBatchObservation {
                domain: &self.domain,
                kind: processor.kind,
                node: &processor.processor,
                relay: incoming_relay,
                physical_node_id: physical_node_id.as_deref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: delivery_observation.domain_timestamp,
            },
        );
        self.runtime.mark_branch_aggregated_metrics_updated(
            &self.domain,
            processor.kind,
            &processor.processor,
        );
        for seconds in delivery_observation.latency_seconds {
            self.runtime
                .metrics
                .observe_global_delivery_latency_at_domain_time(NodeLatencyObservation {
                    domain: &self.domain,
                    kind: processor.kind,
                    node: &processor.processor,
                    relay: incoming_relay,
                    physical_node_id: physical_node_id.as_deref(),
                    seconds,
                    domain_timestamp: delivery_observation.domain_timestamp,
                });
            self.runtime.metrics.observe_branch_delivery_latency(
                branch_key_display(&self.key),
                NodeLatencyObservation {
                    domain: &self.domain,
                    kind: processor.kind,
                    node: &processor.processor,
                    relay: incoming_relay,
                    physical_node_id: physical_node_id.as_deref(),
                    seconds,
                    domain_timestamp: delivery_observation.domain_timestamp,
                },
            );
        }
        processor
            .accept_input(graph, self, incoming_relay, batch)
            .await;
        self.processors.insert(processor_id.clone(), processor);
    }

    async fn flush_processor_collected_inputs(
        &mut self,
        graph: &SharedActiveGraph,
        processor_id: &Identifier,
    ) {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            return;
        };
        processor.flush_all_collected_inputs(graph, self).await;
        self.processors.insert(processor_id.clone(), processor);
    }

    async fn dispatch_output(
        &mut self,
        graph: &SharedActiveGraph,
        output: &RelayProcessorOutputNode,
        source_kind: ModelKind,
        source: &Identifier,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.runtime
            .metrics
            .observe_global_node_sent(NodeBatchObservation {
                domain: &self.domain,
                kind: source_kind,
                node: source,
                relay: &output.relay,
                physical_node_id: self.runtime.local_node_id.read().as_deref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            });
        self.runtime.metrics.observe_branch_node_sent(
            branch_key_display(&self.key),
            NodeBatchObservation {
                domain: &self.domain,
                kind: source_kind,
                node: source,
                relay: &output.relay,
                physical_node_id: self.runtime.local_node_id.read().as_deref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            },
        );
        self.runtime
            .mark_branch_aggregated_metrics_updated(&self.domain, source_kind, source);
        self.dispatch_stream(graph, &output.relay, batch).await
    }

    async fn tick(&mut self, graph: &SharedActiveGraph, now: Timestamp) {
        let processor_ids = self.processors.keys().cloned().collect::<Vec<_>>();
        for processor_id in processor_ids {
            let Some(mut processor) = self.processors.remove(&processor_id) else {
                continue;
            };
            processor.tick(graph, self, now).await;
            self.processors.insert(processor_id, processor);
        }
    }

    async fn force_flush(&mut self, graph: &SharedActiveGraph, now: Timestamp) {
        let processor_ids = self.processors.keys().cloned().collect::<Vec<_>>();
        for processor_id in processor_ids {
            tokio::task::consume_budget().await;
            let Some(mut processor) = self.processors.remove(&processor_id) else {
                continue;
            };
            processor.flush_all_collected_inputs(graph, self).await;
            processor.flush_guest_buffers(graph, self).await;
            let current = graph.load_full();
            processor.refresh(
                &self.runtime,
                &self.domain,
                current.as_ref().map(StdArc::clone),
            );
            for output in &mut processor.operation.output_routes_mut().routes {
                if !output.pending.is_empty() {
                    output.force_flush_at(now);
                }
            }
            processor.tick(graph, self, now).await;
            self.processors.insert(processor_id, processor);
        }
    }

    fn next_deadline(&self) -> Option<Timestamp> {
        self.processors
            .values()
            .filter_map(RelayProcessorNode::next_deadline)
            .min()
    }
}

impl IngestorRouteTask {
    fn handle_general_error(&self, acks: &[AckSet], reason: String) {
        if self.template.branch.source_kind == ModelKind::Ingestor {
            self.runtime_handle.handle_general_error_for_acks(
                &self.domain,
                self.template.branch.source_kind.as_str(),
                &self.ingestor,
                &self.template.branch.error_policies,
                acks.iter(),
                reason,
            );
        } else {
            self.runtime_handle
                .handle_internal_processor_error_for_acks(
                    &self.domain,
                    self.template.branch.source_kind.as_str(),
                    &self.ingestor,
                    &self.template.branch.error_policies,
                    acks.iter(),
                    reason,
                );
        }
    }

    async fn prepare_input(&self, input: BranchedEntrypointInput) -> Vec<RelayRecordBatch> {
        let input_batch = match branched_entrypoint_batch_from_inputs_blocking(vec![input]).await {
            Ok(batch) => batch,
            Err((error, acks)) => {
                self.handle_general_error(
                    &acks,
                    format!(
                        "{} '{}' failed to build route input batch: {}",
                        self.template.branch.source_kind.as_str(),
                        self.ingestor.as_str(),
                        error
                    ),
                );
                return Vec::new();
            }
        };
        let branch_plan = match branched_branch_plan_blocking(input_batch.clone()).await {
            Ok(plan) => plan,
            Err(error) => {
                self.handle_general_error(
                    &input_batch.acks,
                    format!(
                        "{} '{}' failed to evaluate output branch assignments: {}",
                        self.template.branch.source_kind.as_str(),
                        self.ingestor.as_str(),
                        error
                    ),
                );
                return Vec::new();
            }
        };
        if self.template.branch.source_kind == ModelKind::Ingestor {
            let row_count = input_batch.batch.batch().num_rows();
            let row_bytes = input_batch
                .batch
                .batch()
                .columns()
                .iter()
                .map(|column| {
                    column
                        .to_data()
                        .get_slice_memory_size()
                        .ok()
                        .and_then(|bytes| u64::try_from(bytes).ok())
                        .unwrap_or(u64::MAX)
                })
                .fold(0_u64, u64::saturating_add)
                .checked_div(u64::try_from(row_count).unwrap_or(u64::MAX))
                .unwrap_or_default();
            for (key, row) in &branch_plan.valid_rows {
                let Some(metadata) = input_batch.metadata.get(*row) else {
                    continue;
                };
                self.runtime_handle
                    .metrics
                    .observe_branch_node_without_stream_received(
                        branch_key_display(key),
                        NodeWithoutRelayObservation {
                            domain: &self.domain,
                            kind: self.template.branch.source_kind,
                            node: &self.template.branch.source,
                            physical_node_id: self.runtime_handle.local_node_id.read().as_deref(),
                            messages: 1,
                            bytes: row_bytes,
                            domain_timestamp: Some(metadata.ingested_at_high_watermark()),
                        },
                    );
                self.runtime_handle.mark_branch_aggregated_metrics_updated(
                    &self.domain,
                    self.template.branch.source_kind,
                    &self.template.branch.source,
                );
            }
        }

        let mut batch_builds = FuturesUnordered::new();
        for selection in branch_plan.selections {
            tokio::task::consume_budget().await;
            batch_builds.push(branched_branch_filter_blocking(
                input_batch.clone(),
                selection,
                self.template.ack_boundary,
            ));
        }
        let mut prepared = Vec::new();
        while let Some(batch_result) = futures_util::StreamExt::next(&mut batch_builds).await {
            tokio::task::consume_budget().await;
            match batch_result {
                Ok((_, batch)) => prepared.push(batch),
                Err((error, acks)) => self.handle_general_error(
                    &acks,
                    format!(
                        "{} '{}' failed to prepare output branch batch: {}",
                        self.template.branch.source_kind.as_str(),
                        self.ingestor.as_str(),
                        error
                    ),
                ),
            }
        }
        prepared
    }

    async fn flush_key(&mut self, key: &Option<BranchKey>) {
        let Some(pending) = self.pending.remove(key) else {
            return;
        };
        let acks = pending
            .batches
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        let batch = match RelayRecordBatch::concat(pending.batches) {
            Ok(batch) => batch,
            Err(error) => {
                self.handle_general_error(
                    &acks,
                    format!(
                        "{} '{}' failed to concatenate output route batch: {}",
                        self.template.branch.source_kind.as_str(),
                        self.ingestor.as_str(),
                        error
                    ),
                );
                return;
            }
        };
        if let Err(error) = self.branch_sender.send(batch).await {
            let batch = error.0;
            self.handle_general_error(
                &batch.acks,
                format!(
                    "{} '{}' failed to forward prepared batch for relay '{}'",
                    self.template.branch.source_kind.as_str(),
                    self.ingestor.as_str(),
                    self.template.branch.root_relay.as_str()
                ),
            );
        }
    }

    async fn accept(&mut self, input: BranchedEntrypointInput) {
        for batch in self.prepare_input(input).await {
            tokio::task::consume_budget().await;
            let key = batch.key.clone();
            let estimated_bytes = batch.estimated_bytes();
            let pending =
                self.pending
                    .entry(key.clone())
                    .or_insert_with(|| PendingIngestorRouteBatch {
                        batches: Vec::new(),
                        estimated_bytes: 0,
                        flush_at: Instant::now() + self.template.flush_policy.interval(),
                    });
            pending.estimated_bytes = pending.estimated_bytes.saturating_add(estimated_bytes);
            pending.batches.push(batch);
            if self
                .template
                .flush_policy
                .size_boundary_reached(pending.estimated_bytes)
            {
                self.flush_key(&key).await;
            }
        }
    }

    async fn flush_due(&mut self, now: Instant) {
        let keys = self
            .pending
            .iter()
            .filter_map(|(key, pending)| (pending.flush_at <= now).then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in keys {
            tokio::task::consume_budget().await;
            self.flush_key(&key).await;
        }
    }

    async fn flush_all(&mut self) {
        let keys = self.pending.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            tokio::task::consume_budget().await;
            self.flush_key(&key).await;
        }
    }

    fn next_flush(&self) -> Option<Instant> {
        self.pending.values().map(|pending| pending.flush_at).min()
    }

    async fn run(
        mut self,
        mut input: mpsc::Receiver<BranchedEntrypointInput>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        loop {
            tokio::task::consume_budget().await;
            let next_flush = self.next_flush();
            let flush_at =
                next_flush.unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
            tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    let _ = changed;
                    input.close();
                    while let Some(message) = input.recv().await {
                        tokio::task::consume_budget().await;
                        self.accept(message).await;
                    }
                    self.flush_all().await;
                    break;
                }
                _ = sleep_until(flush_at), if next_flush.is_some() => {
                    self.flush_due(Instant::now()).await;
                }
                message = input.recv() => {
                    let Some(message) = message else {
                        self.flush_all().await;
                        break;
                    };
                    self.accept(message).await;
                }
            }
        }
    }
}

impl IngestorRouteRuntime {
    fn new(
        runtime_handle: Runtime,
        domain: Domain,
        ingestor: Identifier,
        graph: SharedActiveGraph,
        template: IngestorRouteTemplate,
        expiration_scan_interval: Duration,
    ) -> Arc<Self> {
        let branch_runtime = BranchExecutionRuntime::new(
            runtime_handle.clone(),
            domain.clone(),
            ingestor.clone(),
            graph,
            template.branch.clone(),
            expiration_scan_interval,
        );
        let (sender, input) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let runtime = Arc::new(Self {
            sender,
            shutdown,
            task: parking_lot::Mutex::new(None),
            branch_runtime: branch_runtime.clone(),
        });
        let task = tokio::spawn(
            IngestorRouteTask {
                runtime_handle,
                domain,
                ingestor,
                template,
                branch_sender: branch_runtime.sender(),
                pending: HashMap::default(),
            }
            .run(input, shutdown_rx),
        );
        *runtime.task.lock() = Some(task);
        runtime
    }

    fn sender(&self) -> mpsc::Sender<BranchedEntrypointInput> {
        self.sender.clone()
    }

    async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        let task = self.task.lock().take();
        if let Some(task) = task {
            let _ = task.await;
        }
        self.branch_runtime.shutdown().await;
    }
}

impl Runtime {
    fn register_branch_lifecycle_metrics(&self, domain: &Domain, branch: Option<&Identifier>) {
        if let Some(branch) = branch {
            self.metrics
                .register_branch(domain, branch, self.local_node_id.read().as_deref());
        }
    }

    fn observe_branch_instance_created(
        &self,
        domain: &Domain,
        branch: Option<&Identifier>,
        key: &Option<BranchKey>,
    ) {
        if let Some(branch) = branch {
            self.metrics.observe_branch_instance_created(
                domain,
                branch,
                self.local_node_id.read().as_deref(),
                branch_key_display(key),
            );
        }
    }

    fn observe_branch_instance_removed(
        &self,
        domain: &Domain,
        branch: Option<&Identifier>,
        key: &Option<BranchKey>,
        reason: Option<BranchEvictionReason>,
    ) {
        let Some(branch) = branch else {
            return;
        };
        let physical_node_id = self.local_node_id.read();
        if let Some(reason) = reason {
            self.metrics.observe_branch_instance_removed(
                domain,
                branch,
                physical_node_id.as_deref(),
                branch_key_display(key),
                reason,
            );
        } else {
            self.metrics.observe_branch_instance_detached(
                domain,
                branch,
                physical_node_id.as_deref(),
                branch_key_display(key),
            );
        }
    }
}

impl BranchExecutionRuntime {
    async fn dispatch_prepared_inputs(
        context: BranchExecutionDispatchContext<'_>,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
        inputs: Vec<BranchedEntrypointInput>,
    ) -> Option<Timestamp> {
        let BranchExecutionDispatchContext {
            runtime_handle,
            domain,
            ingestor,
            graph,
            template,
            now,
        } = context;
        if inputs.is_empty() {
            return None;
        }

        let mut dispatches = FuturesUnordered::new();
        let mut next_deadline = None;
        for message in inputs {
            tokio::task::consume_budget().await;
            let key = message.key.clone();
            let instance = match instances.get_or_try_create_with(key.clone(), now, |key| {
                template.instantiate(runtime_handle, domain, key.clone())
            }) {
                Ok(instance) => instance,
                Err(error) => {
                    let reason = format!(
                        "failed to instantiate branch '{}': {}",
                        branch_key_display(&key),
                        error
                    );
                    if template.source_kind == ModelKind::Ingestor {
                        runtime_handle.handle_general_error_for_acks(
                            domain,
                            template.source_kind.as_str(),
                            ingestor,
                            &template.error_policies,
                            message.acks.iter(),
                            reason,
                        );
                    } else {
                        runtime_handle.handle_internal_processor_error_for_acks(
                            domain,
                            template.source_kind.as_str(),
                            ingestor,
                            &template.error_policies,
                            message.acks.iter(),
                            reason,
                        );
                    }
                    continue;
                }
            };
            if instance.created {
                runtime_handle.observe_branch_instance_created(
                    domain,
                    template.branch.as_ref(),
                    &key,
                );
                debug!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    key = branch_key_display(&key),
                    "created branch runtime"
                );
            }
            if let Some(max_instances) = template.branch_max_instances {
                evict_branch_instance_instances_to_capacity(
                    runtime_handle,
                    domain,
                    ingestor,
                    template.branch.as_ref(),
                    max_instances,
                    instances,
                )
                .await;
            }
            let state = instance.state.clone();
            let graph = graph.clone();
            let dispatch_key = key.clone();
            let dispatch_acks = message.acks.clone();
            dispatches.push(async move {
                let handle = AbortOnDropHandle::new(tokio::spawn(async move {
                    let mut branch = state.lock().await;
                    branch.dispatch(&graph, message).await;
                    branch.next_deadline()
                }));
                (dispatch_key, dispatch_acks, handle.await)
            });
        }
        while let Some((key, acks, result)) = futures_util::StreamExt::next(&mut dispatches).await {
            tokio::task::consume_budget().await;
            match result {
                Ok(deadline) => {
                    record_next_branch_instance_branch_deadline(&mut next_deadline, deadline);
                }
                Err(error) => {
                    runtime_handle.handle_internal_processor_error_for_acks(
                        domain,
                        template.source_kind.as_str(),
                        ingestor,
                        &template.error_policies,
                        acks.iter(),
                        format!(
                            "branch '{}' dispatch task failed: {}",
                            branch_key_display(&key),
                            error
                        ),
                    );
                }
            }
        }
        next_deadline
    }

    fn new(
        runtime_handle: Runtime,
        domain: Domain,
        ingestor: Identifier,
        graph: SharedActiveGraph,
        template: BranchInstanceTemplate,
        expiration_scan_interval: Duration,
    ) -> Arc<Self> {
        // input from ingestor/re-ingestor
        let (sender, mut input) = mpsc::channel(1);
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let runtime = Arc::new(Self {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
            sender,
            shutdown,
            task: parking_lot::Mutex::new(None),
        });
        runtime_handle.register_branch_lifecycle_metrics(&domain, template.branch.as_ref());

        let task = tokio::spawn(async move {
            let mut instances =
                BranchInstanceRegistry::<Option<BranchKey>, Mutex<BranchRuntime>>::new();
            let mut last_persisted_lru_lsm = match restore_branch_instance_lru_snapshot(
                &runtime_handle,
                &domain,
                &template,
                &mut instances,
            ) {
                Ok(lsm) => lsm,
                Err(error) => {
                    warn!(
                        domain = domain.as_str(),
                        ingestor = ingestor.as_str(),
                        error = %error,
                        "failed to restore branch lru snapshot"
                    );
                    0
                }
            };
            if let Some(max_instances) = template.branch_max_instances {
                evict_branch_instance_instances_to_capacity(
                    &runtime_handle,
                    &domain,
                    &ingestor,
                    template.branch.as_ref(),
                    max_instances,
                    &mut instances,
                )
                .await;
            }
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            let mut next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();
            let now = runtime_handle
                .current_stream_expiration_time(&domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp);
            let mut next_branch_deadline =
                tick_due_branch_instance_branches(&graph, now, &instances).await;

            loop {
                tokio::task::consume_budget().await;
                let now = runtime_handle
                    .current_stream_expiration_time(&domain)
                    .ok()
                    .flatten()
                    .unwrap_or_else(current_timestamp);
                let mut did_scheduled_work = false;
                if Instant::now() >= next_expiration_scan {
                    if let Some(branch_ttl) = template.branch_ttl {
                        expire_branch_instance_instances(
                            &runtime_handle,
                            &domain,
                            &ingestor,
                            template.branch.as_ref(),
                            now,
                            branch_ttl,
                            &mut instances,
                        )
                        .await;
                    }
                    next_expiration_scan = Instant::now() + expiration_scan_interval;
                    did_scheduled_work = true;
                }
                if Instant::now() >= next_lru_snapshot {
                    if let Err(error) = persist_branch_instance_lru_snapshot(
                        &runtime_handle,
                        &domain,
                        &template,
                        &instances,
                        &mut last_persisted_lru_lsm,
                    ) {
                        warn!(
                            domain = domain.as_str(),
                            ingestor = ingestor.as_str(),
                            error = %error,
                            "failed to persist branch lru snapshot"
                        );
                    }
                    next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();
                    did_scheduled_work = true;
                }
                if next_branch_deadline.is_some_and(|deadline| deadline <= now) {
                    next_branch_deadline =
                        tick_due_branch_instance_branches(&graph, now, &instances).await;
                    did_scheduled_work = true;
                }
                if did_scheduled_work {
                    continue;
                }

                let sleep_duration = {
                    let expiration_sleep = next_expiration_scan
                        .checked_duration_since(Instant::now())
                        .unwrap_or(Duration::ZERO);
                    let branch_sleep = next_branch_deadline.map(|deadline| {
                        wall_duration_until_domain_deadline(&runtime_handle, &domain, now, deadline)
                    });
                    branch_sleep
                        .map(|branch_sleep| expiration_sleep.min(branch_sleep))
                        .unwrap_or(expiration_sleep)
                        .min(
                            next_lru_snapshot
                                .checked_duration_since(Instant::now())
                                .unwrap_or(Duration::ZERO),
                        )
                };
                tokio::select! {
                    biased;
                    message = input.recv() => {
                        let Some(message) = message else {
                            break;
                        };
                        record_next_branch_instance_branch_deadline(
                            &mut next_branch_deadline,
                            Self::dispatch_prepared_inputs(
                                BranchExecutionDispatchContext {
                                    runtime_handle: &runtime_handle,
                                    domain: &domain,
                                    ingestor: &ingestor,
                                    graph: &graph,
                                    template: &template,
                                    now,
                                },
                                &mut instances,
                                vec![message],
                            )
                            .await,
                        );
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            input.close();
                            while let Some(message) = input.recv().await {
                                tokio::task::consume_budget().await;
                                let drain_now = runtime_handle
                                    .current_stream_expiration_time(&domain)
                                    .ok()
                                    .flatten()
                                    .unwrap_or_else(current_timestamp);
                                record_next_branch_instance_branch_deadline(
                                    &mut next_branch_deadline,
                                    Self::dispatch_prepared_inputs(
                                        BranchExecutionDispatchContext {
                                            runtime_handle: &runtime_handle,
                                            domain: &domain,
                                            ingestor: &ingestor,
                                            graph: &graph,
                                            template: &template,
                                            now: drain_now,
                                        },
                                        &mut instances,
                                        vec![message],
                                    )
                                    .await,
                                );
                            }
                            break;
                        }
                    }
                    _ = sleep(sleep_duration) => {}
                }
            }

            if let Err(error) = persist_branch_instance_lru_snapshot(
                &runtime_handle,
                &domain,
                &template,
                &instances,
                &mut last_persisted_lru_lsm,
            ) {
                warn!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    error = %error,
                    "failed to persist final branch lru snapshot"
                );
            }
            shutdown_all_branch_instance_instances(
                &runtime_handle,
                &domain,
                &ingestor,
                template.branch.as_ref(),
                &mut instances,
            )
            .await;
        });
        *runtime.task.lock() = Some(task);
        runtime
    }

    fn sender(&self) -> mpsc::Sender<BranchedEntrypointInput> {
        self.sender.clone()
    }

    async fn shutdown(&self) {
        const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

        let _ = self.shutdown.send(true);
        let Some(mut task) = self.task.lock().take() else {
            return;
        };

        match tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if error.is_cancelled() {
                    warn!(
                        domain = self.domain.as_str(),
                        ingestor = self.ingestor.as_str(),
                        "branched ingestor task was cancelled"
                    );
                } else {
                    warn!(
                        domain = self.domain.as_str(),
                        ingestor = self.ingestor.as_str(),
                        error = %error,
                        "branched ingestor task join failed"
                    );
                }
            }
            Err(_) => {
                warn!(
                    domain = self.domain.as_str(),
                    ingestor = self.ingestor.as_str(),
                    grace_period = %humantime::format_duration(SHUTDOWN_GRACE_PERIOD),
                    "branched ingestor task exceeded shutdown grace period; aborting"
                );
                task.abort();
                if let Err(error) = task.await
                    && !error.is_cancelled()
                {
                    warn!(
                        domain = self.domain.as_str(),
                        ingestor = self.ingestor.as_str(),
                        error = %error,
                        "aborted branched ingestor task join failed"
                    );
                }
            }
        }
    }
}

async fn expire_branch_instance_instances(
    runtime: &Runtime,
    domain: &Domain,
    ingestor: &Identifier,
    branch: Option<&Identifier>,
    now: Timestamp,
    expiration_after: Duration,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) {
    for (key, state) in instances.expire(now, expiration_after) {
        runtime.observe_branch_instance_removed(
            domain,
            branch,
            &key,
            Some(BranchEvictionReason::Ttl),
        );
        let mut branch = state.lock().await;
        branch.evict().await;
        debug!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            key = branch_key_display(&key),
            "expired branched processor root"
        );
    }
}

async fn evict_branch_instance_instances_to_capacity(
    runtime: &Runtime,
    domain: &Domain,
    ingestor: &Identifier,
    branch: Option<&Identifier>,
    max_instances: usize,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) {
    for (key, state) in instances.evict_lru_to_capacity(max_instances) {
        runtime.observe_branch_instance_removed(
            domain,
            branch,
            &key,
            Some(BranchEvictionReason::Lru),
        );
        let mut branch = state.lock().await;
        branch.evict().await;
        debug!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            key = branch_key_display(&key),
            max_instances,
            "evicted branch runtime by lru"
        );
    }
}

async fn shutdown_all_branch_instance_instances(
    runtime: &Runtime,
    domain: &Domain,
    ingestor: &Identifier,
    branch: Option<&Identifier>,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) {
    for (key, state) in instances.drain() {
        runtime.observe_branch_instance_removed(domain, branch, &key, None);
        let branch = state.lock().await;
        branch.detach();
        debug!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            key = branch_key_display(&key),
            "stopped branch runtime"
        );
    }
}

fn branch_lru_placement(
    runtime: &Runtime,
    domain: &Domain,
    template: &BranchInstanceTemplate,
) -> RuntimeStatePlacement {
    runtime.state_placement(
        domain,
        RuntimeStateKind::BranchLru,
        template.source_kind,
        &template.source,
        None,
    )
}

fn restore_branch_instance_lru_snapshot(
    runtime: &Runtime,
    domain: &Domain,
    template: &BranchInstanceTemplate,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> Result<u64, String> {
    let Some(store) = &runtime.state_store else {
        return Ok(0);
    };
    let placement = branch_lru_placement(runtime, domain, template);
    let Some(snapshot) = store
        .latest_snapshot(&placement)
        .map_err(|error| error.to_string())?
    else {
        return Ok(0);
    };
    for (key, last_ingestion) in decode_branch_lru_snapshot(&snapshot.payload)? {
        let mut state = template.instantiate(runtime, domain, key.clone())?;
        state.get_mut().restore_presence(last_ingestion);
        runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &key);
        instances.insert_restored(key, last_ingestion, state);
    }
    instances.set_version(snapshot.lsm);
    Ok(snapshot.lsm)
}

fn persist_branch_instance_lru_snapshot<V>(
    runtime: &Runtime,
    domain: &Domain,
    template: &BranchInstanceTemplate,
    instances: &BranchInstanceRegistry<Option<BranchKey>, V>,
    last_persisted_lsm: &mut u64,
) -> Result<(), String> {
    let Some(store) = &runtime.state_store else {
        return Ok(());
    };
    let lsm = instances.version();
    if lsm <= *last_persisted_lsm {
        return Ok(());
    }
    let placement = branch_lru_placement(runtime, domain, template);
    let payload = encode_branch_lru_snapshot(&instances.snapshot_entries())?;
    store
        .persist_latest_snapshot(&placement, lsm, &payload)
        .map_err(|error| error.to_string())?;
    *last_persisted_lsm = lsm;
    Ok(())
}

async fn tick_due_branch_instance_branches(
    graph: &SharedActiveGraph,
    now: Timestamp,
    instances: &BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> Option<Timestamp> {
    let mut next = None;
    for instance in instances.states() {
        let mut branch = instance.lock().await;
        if branch
            .next_deadline()
            .is_some_and(|deadline| deadline <= now)
        {
            branch.tick(graph, now).await;
        }
        record_next_branch_instance_branch_deadline(&mut next, branch.next_deadline());
    }
    next
}

fn record_next_branch_instance_branch_deadline(
    next: &mut Option<Timestamp>,
    candidate: Option<Timestamp>,
) {
    if let Some(candidate) = candidate {
        *next = Some(match *next {
            Some(current) => current.min(candidate),
            None => candidate,
        });
    }
}

fn wall_duration_until_domain_deadline(
    runtime: &Runtime,
    domain: &Domain,
    now: Timestamp,
    deadline: Timestamp,
) -> Duration {
    let Some(domain_state) = runtime.domains.get(domain) else {
        return wall_duration_until_timestamp(now, deadline);
    };
    if domain_state.config.pace != DomainPace::Paced {
        return wall_duration_until_timestamp(now, deadline);
    }
    domain_state
        .clock
        .as_ref()
        .and_then(|clock| wall_duration_until_logical_target(clock, now, deadline).ok())
        .unwrap_or(Duration::from_millis(100))
}

const PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);
const PROCESSOR_BRANCH_TASK_IDLE_SLEEP: Duration = Duration::from_secs(86_400);

#[derive(Debug)]
enum ProcessorBranchStopMode {
    Evict,
    Detach,
    Handoff(oneshot::Sender<ProcessorBranchHandoff>),
}

struct ProcessorBranchTask {
    input: mpsc::Sender<ProcessorBranchInput>,
    stop: mpsc::Sender<ProcessorBranchStopMode>,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

struct ProcessorBranchInput {
    relay: Identifier,
    batch: RelayRecordBatch,
    work: NodeQuiesceWorkGuard,
}

#[derive(Debug)]
struct ProcessorBranchHandoff {
    key: Option<BranchKey>,
    restored_at: Timestamp,
    pending_materialized: VecDeque<(Identifier, RelayRecordBatch)>,
}

enum ProcessorNodeCommand {
    Handoff {
        response: oneshot::Sender<Vec<ProcessorBranchHandoff>>,
    },
}

impl RelayInteractionCommand for ProcessorNodeCommand {
    fn drain_inputs_before_handling(&self) -> bool {
        true
    }

    fn cancels_external_waits_while_draining(&self) -> bool {
        true
    }
}

enum EmitterTaskCommand {
    Reconfigure {
        config: Box<CreateEmitter>,
        response: oneshot::Sender<()>,
    },
    Stop {
        deadline: Instant,
        response: oneshot::Sender<Result<(), String>>,
    },
}

impl RelayInteractionCommand for EmitterTaskCommand {
    fn drain_inputs_before_handling(&self) -> bool {
        matches!(self, Self::Stop { .. })
    }

    fn cancels_external_waits_while_draining(&self) -> bool {
        matches!(self, Self::Stop { .. })
    }
}

#[derive(Debug)]
struct ScheduledEmitterTask {
    commands: mpsc::Sender<EmitterTaskCommand>,
    stop_signal: watch::Sender<Option<Instant>>,
    task: JoinHandle<()>,
}

#[derive(Debug)]
struct ScheduledEmitterStopError {
    reason: String,
    task: Option<ScheduledEmitterTask>,
}

fn clear_emitter_stop_signal(stop_signal: &watch::Sender<Option<Instant>>, deadline: Instant) {
    stop_signal.send_if_modified(|pending| {
        if *pending == Some(deadline) {
            *pending = None;
            true
        } else {
            false
        }
    });
}

impl ScheduledEmitterStopError {
    fn recoverable(reason: impl Into<String>, task: ScheduledEmitterTask) -> Self {
        Self {
            reason: reason.into(),
            task: Some(task),
        }
    }

    fn reason(&self) -> &str {
        &self.reason
    }

    fn into_task(self) -> Option<ScheduledEmitterTask> {
        self.task
    }
}

impl ScheduledEmitterTask {
    async fn reconfigure_via(
        commands: &mpsc::Sender<EmitterTaskCommand>,
        config: Box<CreateEmitter>,
    ) -> Result<(), String> {
        let (response, receiver) = oneshot::channel();
        tokio::time::timeout(
            PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE,
            commands.send(EmitterTaskCommand::Reconfigure { config, response }),
        )
        .await
        .map_err(|_| "scheduled emitter task timed out accepting reconfiguration".to_string())?
        .map_err(|_| "scheduled emitter task is unavailable for reconfiguration".to_string())?;
        tokio::time::timeout(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, receiver)
            .await
            .map_err(|_| "scheduled emitter task timed out reconfiguring".to_string())?
            .map_err(|_| "scheduled emitter task dropped its reconfiguration response".to_string())
    }

    async fn stop(mut self, drain_timeout: Duration) -> Result<(), ScheduledEmitterStopError> {
        let (response, receiver) = oneshot::channel();
        let deadline = Instant::now() + drain_timeout;
        let command = EmitterTaskCommand::Stop { deadline, response };
        match tokio::time::timeout_at(deadline, self.commands.send(command)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(ScheduledEmitterStopError::recoverable(
                    "scheduled emitter task is unavailable for stopping",
                    self,
                ));
            }
            Err(_) => {
                return Err(ScheduledEmitterStopError::recoverable(
                    "scheduled emitter task timed out accepting its stop command",
                    self,
                ));
            }
        }
        self.stop_signal.send_replace(Some(deadline));
        let response_deadline = deadline + PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE;
        let response = match tokio::time::timeout_at(response_deadline, receiver).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                clear_emitter_stop_signal(&self.stop_signal, deadline);
                return Err(ScheduledEmitterStopError::recoverable(
                    "scheduled emitter task dropped its stop response",
                    self,
                ));
            }
            Err(_) => {
                clear_emitter_stop_signal(&self.stop_signal, deadline);
                return Err(ScheduledEmitterStopError::recoverable(
                    "scheduled emitter task timed out draining",
                    self,
                ));
            }
        };
        if let Err(reason) = response {
            clear_emitter_stop_signal(&self.stop_signal, deadline);
            return Err(ScheduledEmitterStopError::recoverable(reason, self));
        }
        match tokio::time::timeout(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, &mut self.task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                // A successful stop response means the buffered work and transport drain already
                // completed. The task has no work left to preserve even if its final join failed.
                warn!(error = %error, "scheduled emitter task join failed after a successful drain");
                Ok(())
            }
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                Ok(())
            }
        }
    }
}

struct ScheduledNodeTask {
    commands: mpsc::Sender<ProcessorNodeCommand>,
    task: JoinHandle<()>,
}

impl ScheduledNodeTask {
    async fn abort_and_join(&mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }

    async fn handoff(self) -> Result<Vec<ProcessorBranchHandoff>, String> {
        self.handoff_within(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE)
            .await
    }

    async fn handoff_within(
        mut self,
        grace_period: Duration,
    ) -> Result<Vec<ProcessorBranchHandoff>, String> {
        let (response, receiver) = oneshot::channel();
        match tokio::time::timeout(
            grace_period,
            self.commands
                .send(ProcessorNodeCommand::Handoff { response }),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                self.abort_and_join().await;
                return Err("scheduled node task is unavailable for handoff".to_string());
            }
            Err(_) => {
                self.abort_and_join().await;
                return Err("scheduled node task timed out accepting handoff".to_string());
            }
        }
        let handoffs = match tokio::time::timeout(grace_period, receiver).await {
            Ok(Ok(handoffs)) => handoffs,
            Ok(Err(_)) => {
                self.abort_and_join().await;
                return Err("scheduled node task dropped its handoff response".to_string());
            }
            Err(_) => {
                self.abort_and_join().await;
                return Err("scheduled node task timed out producing handoff residue".to_string());
            }
        };
        match tokio::time::timeout(grace_period, &mut self.task).await {
            Ok(Ok(())) => Ok(handoffs),
            Ok(Err(error)) => Err(format!("scheduled node task join failed: {error}")),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                Err("scheduled node task timed out stopping for handoff".to_string())
            }
        }
    }
}

/// The domain-scoped execution context a processor task runs inside: the runtime it calls back
/// into, the domain that owns it, and the active graph it evaluates against. Carrying the three as
/// one value keeps a task's spawn and run halves provably agreed on which domain's graph they serve.
#[derive(Clone)]
pub(in crate::runtime) struct ProcessorRuntimeContext {
    runtime_handle: Runtime,
    domain: Domain,
    graph: SharedActiveGraph,
}

impl ProcessorRuntimeContext {
    pub(in crate::runtime) fn new(
        runtime_handle: Runtime,
        domain: Domain,
        graph: SharedActiveGraph,
    ) -> Self {
        Self {
            runtime_handle,
            domain,
            graph,
        }
    }
}

pub(in crate::runtime) fn spawn_processor_node_runtime(
    context: ProcessorRuntimeContext,
    shutdown_tx: &watch::Sender<bool>,
    template: BranchInstanceTemplate,
    inputs: Vec<(Identifier, RelayRuntimeFanIn)>,
    expiration_scan_interval: Duration,
) -> ScheduledNodeTask {
    spawn_processor_node_runtime_with_handoffs(
        context,
        shutdown_tx,
        template,
        inputs,
        Vec::new(),
        expiration_scan_interval,
    )
}

pub(in crate::runtime) fn spawn_processor_node_runtime_with_handoffs(
    context: ProcessorRuntimeContext,
    shutdown_tx: &watch::Sender<bool>,
    template: BranchInstanceTemplate,
    inputs: Vec<(Identifier, RelayRuntimeFanIn)>,
    handoffs: Vec<ProcessorBranchHandoff>,
    expiration_scan_interval: Duration,
) -> ScheduledNodeTask {
    let shutdown_rx = shutdown_tx.subscribe();
    let (commands, command_rx) = mpsc::channel(1);
    let task = tokio::spawn(run_processor_node_runtime(
        context,
        template,
        inputs,
        shutdown_rx,
        command_rx,
        handoffs,
        expiration_scan_interval,
    ));
    ScheduledNodeTask { commands, task }
}

async fn run_processor_node_runtime(
    context: ProcessorRuntimeContext,
    template: BranchInstanceTemplate,
    inputs: Vec<(Identifier, RelayRuntimeFanIn)>,
    shutdown_rx: watch::Receiver<bool>,
    command_rx: mpsc::Receiver<ProcessorNodeCommand>,
    restored_handoffs: Vec<ProcessorBranchHandoff>,
    expiration_scan_interval: Duration,
) {
    let ProcessorRuntimeContext {
        runtime_handle,
        domain,
        graph,
    } = context;
    let processor = template.source.clone();
    runtime_handle.register_branch_lifecycle_metrics(&domain, template.branch.as_ref());
    let mut instances = BranchInstanceRegistry::<Option<BranchKey>, ProcessorBranchTask>::new();
    let mut last_persisted_lru_lsm = 0;
    if restored_handoffs.is_empty() {
        last_persisted_lru_lsm = match restore_processor_branch_lru_snapshot(
            &runtime_handle,
            &domain,
            &graph,
            &template,
            &mut instances,
        ) {
            Ok(lsm) => lsm,
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    processor = processor.as_str(),
                    error = %error,
                    "failed to restore processor branch lru snapshot"
                );
                0
            }
        };
    } else {
        for handoff in restored_handoffs {
            let key = handoff.key.clone();
            match spawn_processor_branch_task(
                ProcessorRuntimeContext::new(runtime_handle.clone(), domain.clone(), graph.clone()),
                &template,
                key.clone(),
                Some(handoff.restored_at),
                handoff.pending_materialized,
            ) {
                Ok(entry) => {
                    runtime_handle.observe_branch_instance_created(
                        &domain,
                        template.branch.as_ref(),
                        &key,
                    );
                    instances.insert_restored(key, handoff.restored_at, entry);
                }
                Err(error) => {
                    warn!(
                        domain = domain.as_str(),
                        processor = processor.as_str(),
                        error = %error,
                        "failed to restore handed-off processor branch"
                    );
                }
            }
        }
    }
    if let Some(max_instances) = template.branch_max_instances {
        evict_processor_branch_instances_to_capacity(
            &runtime_handle,
            &domain,
            &processor,
            template.branch.as_ref(),
            max_instances,
            &mut instances,
        )
        .await;
    }
    let quiesce_counters = runtime_handle.node_quiesce_counters(&domain, &processor);
    let interaction_inputs = inputs
        .into_iter()
        // Processor collection is branch-local and paced by the domain clock. The outer relay
        // interaction therefore delivers each dequeued batch unchanged.
        .map(|(relay, receiver)| RelayInteractionInput::new(relay, receiver, None))
        .collect();
    let mut interaction = RelayInteraction::with_commands(
        interaction_inputs,
        shutdown_rx,
        // Branch tasks own processor state and output buffers, so they remain the force-flush
        // participants. The supervisor only drains and dispatches relay input.
        None,
        Some(quiesce_counters),
        command_rx,
    )
    .expect("validated processor inputs must build a relay interaction");
    let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
    let mut next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();

    let mut handoff_response = None;
    loop {
        tokio::task::consume_budget().await;
        let now = runtime_handle
            .current_stream_expiration_time(&domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp);
        let mut did_scheduled_work = false;
        if Instant::now() >= next_expiration_scan {
            if let Some(branch_ttl) = template.branch_ttl {
                expire_processor_branch_instances(
                    &runtime_handle,
                    &domain,
                    &processor,
                    template.branch.as_ref(),
                    now,
                    branch_ttl,
                    &mut instances,
                )
                .await;
            }
            next_expiration_scan = Instant::now() + expiration_scan_interval;
            did_scheduled_work = true;
        }
        if Instant::now() >= next_lru_snapshot {
            if let Err(error) = persist_branch_instance_lru_snapshot(
                &runtime_handle,
                &domain,
                &template,
                &instances,
                &mut last_persisted_lru_lsm,
            ) {
                warn!(
                    domain = domain.as_str(),
                    processor = processor.as_str(),
                    error = %error,
                    "failed to persist processor branch lru snapshot"
                );
            }
            next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();
            did_scheduled_work = true;
        }
        if did_scheduled_work {
            continue;
        }

        let work = match interaction
            .next(Some(next_expiration_scan.min(next_lru_snapshot)))
            .await
        {
            Ok(work) => work,
            Err(error) => {
                runtime_handle.handle_internal_processor_error_for_acks(
                    &domain,
                    template.source_kind.as_str(),
                    &processor,
                    &template.error_policies,
                    error.acks(),
                    format!(
                        "processor '{}' relay interaction failed: {error}",
                        processor.as_str()
                    ),
                );
                continue;
            }
        };
        let (event, work) = work.into_parts();
        match event {
            RelayInteractionEvent::Batch { relay, batch } => {
                let work = work.expect("processor relay input must track quiesce work");
                dispatch_processor_node_input(
                    ProcessorNodeDispatchContext {
                        runtime_handle: &runtime_handle,
                        domain: &domain,
                        graph: &graph,
                        template: &template,
                        now,
                    },
                    &mut instances,
                    relay,
                    batch,
                    work,
                )
                .await;
            }
            RelayInteractionEvent::Wake => {}
            RelayInteractionEvent::Command(ProcessorNodeCommand::Handoff { response }) => {
                handoff_response = Some(response);
                break;
            }
            RelayInteractionEvent::ForceFlush(completion) => {
                // The supervisor never registers as a participant; keep the exhaustive arm from
                // stranding an obligation if that ownership changes in the future.
                completion.complete();
            }
            RelayInteractionEvent::Stopped(reason) => {
                debug!(
                    domain = domain.as_str(),
                    processor = processor.as_str(),
                    ?reason,
                    "processor relay interaction stopped"
                );
                break;
            }
        }
    }

    if let Err(error) = persist_branch_instance_lru_snapshot(
        &runtime_handle,
        &domain,
        &template,
        &instances,
        &mut last_persisted_lru_lsm,
    ) {
        warn!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            error = %error,
            "failed to persist final processor branch lru snapshot"
        );
    }
    if let Some(response) = handoff_response {
        let handoffs = handoff_all_processor_branch_instances(
            &runtime_handle,
            &domain,
            &processor,
            template.branch.as_ref(),
            &mut instances,
        )
        .await;
        let _ = response.send(handoffs);
    } else {
        shutdown_all_processor_branch_instances(
            &runtime_handle,
            &domain,
            &processor,
            template.branch.as_ref(),
            &mut instances,
        )
        .await;
    }
}

struct ProcessorNodeDispatchContext<'a> {
    runtime_handle: &'a Runtime,
    domain: &'a Domain,
    graph: &'a SharedActiveGraph,
    template: &'a BranchInstanceTemplate,
    now: Timestamp,
}

async fn dispatch_processor_node_input(
    context: ProcessorNodeDispatchContext<'_>,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
    relay: Identifier,
    batch: RelayRecordBatch,
    dequeued_work: NodeQuiesceWorkGuard,
) {
    let ProcessorNodeDispatchContext {
        runtime_handle,
        domain,
        graph,
        template,
        now,
    } = context;
    let key = batch.key.clone();
    let instance = match instances.get_or_try_create_with(key.clone(), now, |key| {
        spawn_processor_branch_task(
            ProcessorRuntimeContext::new(runtime_handle.clone(), domain.clone(), graph.clone()),
            template,
            key.clone(),
            None,
            VecDeque::new(),
        )
    }) {
        Ok(instance) => instance,
        Err(error) => {
            runtime_handle.handle_internal_processor_error_for_acks(
                domain,
                template.source_kind.as_str(),
                &template.source,
                &template.error_policies,
                batch.acks.iter(),
                format!(
                    "failed to instantiate processor branch '{}': {}",
                    branch_key_display(&key),
                    error
                ),
            );
            return;
        }
    };
    if instance.created {
        runtime_handle.observe_branch_instance_created(domain, template.branch.as_ref(), &key);
        debug!(
            domain = domain.as_str(),
            processor = template.source.as_str(),
            key = branch_key_display(&key),
            "created processor branch task"
        );
        if let Some(max_instances) = template.branch_max_instances {
            evict_processor_branch_instances_to_capacity(
                runtime_handle,
                domain,
                &template.source,
                template.branch.as_ref(),
                max_instances,
                instances,
            )
            .await;
        }
    }
    let input = ProcessorBranchInput {
        relay,
        batch,
        work: dequeued_work,
    };
    if let Err(mpsc::error::SendError(input)) = instance.state.input.send(input).await {
        runtime_handle.handle_internal_processor_error_for_acks(
            domain,
            template.source_kind.as_str(),
            &template.source,
            &template.error_policies,
            input.batch.acks.iter(),
            format!(
                "processor branch task '{}' is unavailable",
                branch_key_display(&key)
            ),
        );
        if let Some(entry) = instances.remove(&key) {
            runtime_handle.observe_branch_instance_removed(
                domain,
                template.branch.as_ref(),
                &key,
                None,
            );
            stop_processor_branch_task(
                domain,
                &template.source,
                &key,
                entry,
                ProcessorBranchStopMode::Detach,
            )
            .await;
        }
        drop(input.work);
    }
}

fn spawn_processor_branch_task(
    context: ProcessorRuntimeContext,
    template: &BranchInstanceTemplate,
    key: Option<BranchKey>,
    restored_at: Option<Timestamp>,
    pending_materialized: VecDeque<(Identifier, RelayRecordBatch)>,
) -> Result<ProcessorBranchTask, String> {
    let mut branch = template
        .instantiate(&context.runtime_handle, &context.domain, key)?
        .into_inner();
    if let Some(processor) = branch.processors.get_mut(&template.source) {
        processor.pending_materialized = pending_materialized;
    }
    if let Some(restored_at) = restored_at {
        branch.restore_presence(restored_at);
    }
    let (input_tx, input_rx) = mpsc::channel(1);
    let (stop_tx, stop_rx) = mpsc::channel(1);
    let processor = template.source.clone();
    let quiesce_counters = context
        .runtime_handle
        .node_quiesce_counters(&context.domain, &processor);
    let task = tokio::spawn(run_processor_branch_task(
        context,
        processor,
        branch,
        input_rx,
        stop_rx,
        quiesce_counters,
    ));
    Ok(ProcessorBranchTask {
        input: input_tx,
        stop: stop_tx,
        task: parking_lot::Mutex::new(Some(task)),
    })
}

async fn run_processor_branch_task(
    context: ProcessorRuntimeContext,
    processor: Identifier,
    mut branch: BranchRuntime,
    mut input: mpsc::Receiver<ProcessorBranchInput>,
    mut stop_rx: mpsc::Receiver<ProcessorBranchStopMode>,
    quiesce_counters: Arc<NodeQuiesceCounters>,
) {
    let ProcessorRuntimeContext {
        runtime_handle,
        domain,
        graph,
    } = context;
    let mut force_flush = runtime_handle.force_flush_participant(&domain, quiesce_counters.clone());
    let mut quiesce_gauges = BranchQuiesceGauges::new(quiesce_counters.clone());
    quiesce_gauges.observe(&branch, &processor);
    let stop_mode;
    loop {
        tokio::task::consume_budget().await;
        let now = runtime_handle
            .current_stream_expiration_time(&domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp);
        if branch
            .next_deadline()
            .is_some_and(|deadline| deadline <= now)
        {
            branch.tick(&graph, now).await;
            quiesce_gauges.observe(&branch, &processor);
            continue;
        }
        let sleep_duration = branch
            .next_deadline()
            .map(|deadline| {
                wall_duration_until_domain_deadline(&runtime_handle, &domain, now, deadline)
            })
            .unwrap_or(PROCESSOR_BRANCH_TASK_IDLE_SLEEP);
        let has_pending_materialized = branch.processor_has_pending_materialized(&processor);
        tokio::select! {
            biased;
            mode = stop_rx.recv() => {
                stop_mode = Some(mode.unwrap_or(ProcessorBranchStopMode::Detach));
                break;
            }
            received = input.recv() => {
                match received {
                    Some(ProcessorBranchInput { relay, batch, work }) => {
                        branch
                            .execute_processor_input(&graph, &processor, &relay, batch)
                            .await;
                        quiesce_gauges.observe(&branch, &processor);
                        drop(work);
                    }
                    None => {
                        stop_mode = Some(ProcessorBranchStopMode::Detach);
                        break;
                    }
                }
            }
            _ = runtime_handle.materialized_state_changed.notified(), if has_pending_materialized => {
                branch
                    .retry_processor_pending_materialized(&graph, &processor)
                    .await;
                quiesce_gauges.observe(&branch, &processor);
            }
            completion = force_flush.changed() => {
                let Ok(completion) = completion else {
                    stop_mode = Some(ProcessorBranchStopMode::Detach);
                    break;
                };
                branch.force_flush(&graph, now).await;
                quiesce_gauges.observe(&branch, &processor);
                completion.complete();
            }
            _ = sleep(sleep_duration) => {}
        }
    }
    while let Ok(ProcessorBranchInput { relay, batch, work }) = input.try_recv() {
        branch
            .execute_processor_input(&graph, &processor, &relay, batch)
            .await;
        quiesce_gauges.observe(&branch, &processor);
        drop(work);
    }
    match stop_mode {
        Some(ProcessorBranchStopMode::Evict) => branch.evict().await,
        Some(ProcessorBranchStopMode::Handoff(response)) => {
            branch
                .flush_processor_collected_inputs(&graph, &processor)
                .await;
            let now = runtime_handle
                .current_stream_expiration_time(&domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp);
            branch.force_flush(&graph, now).await;
            let pending_materialized = branch
                .processors
                .get_mut(&processor)
                .map(|processor| std::mem::take(&mut processor.pending_materialized))
                .unwrap_or_default();
            let handoff = ProcessorBranchHandoff {
                key: branch.key.clone(),
                restored_at: now,
                pending_materialized,
            };
            branch.detach();
            let _ = response.send(handoff);
        }
        Some(ProcessorBranchStopMode::Detach) | None => {
            branch
                .flush_processor_collected_inputs(&graph, &processor)
                .await;
            branch.detach();
        }
    }
}

async fn stop_processor_branch_task(
    domain: &Domain,
    processor: &Identifier,
    key: &Option<BranchKey>,
    entry: Arc<ProcessorBranchTask>,
    mode: ProcessorBranchStopMode,
) {
    let _ = entry.stop.send(mode).await;
    let Some(mut task) = entry.task.lock().take() else {
        return;
    };
    match tokio::time::timeout(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, &mut task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(
                domain = domain.as_str(),
                processor = processor.as_str(),
                key = branch_key_display(key),
                error = %error,
                "processor branch task join failed"
            );
        }
        Err(_) => {
            warn!(
                domain = domain.as_str(),
                processor = processor.as_str(),
                key = branch_key_display(key),
                grace_period = %humantime::format_duration(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE),
                "processor branch task exceeded shutdown grace period; aborting"
            );
            task.abort();
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                warn!(
                    domain = domain.as_str(),
                    processor = processor.as_str(),
                    key = branch_key_display(key),
                    error = %error,
                    "aborted processor branch task join failed"
                );
            }
        }
    }
}

async fn handoff_all_processor_branch_instances(
    runtime: &Runtime,
    domain: &Domain,
    processor: &Identifier,
    branch: Option<&Identifier>,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) -> Vec<ProcessorBranchHandoff> {
    let mut handoffs = Vec::new();
    for (key, entry) in instances.drain() {
        runtime.observe_branch_instance_removed(domain, branch, &key, None);
        let (response, receiver) = oneshot::channel();
        stop_processor_branch_task(
            domain,
            processor,
            &key,
            entry,
            ProcessorBranchStopMode::Handoff(response),
        )
        .await;
        if let Ok(handoff) = receiver.await {
            handoffs.push(handoff);
        }
    }
    handoffs
}

async fn expire_processor_branch_instances(
    runtime: &Runtime,
    domain: &Domain,
    processor: &Identifier,
    branch: Option<&Identifier>,
    now: Timestamp,
    expiration_after: Duration,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) {
    for (key, entry) in instances.expire(now, expiration_after) {
        runtime.observe_branch_instance_removed(
            domain,
            branch,
            &key,
            Some(BranchEvictionReason::Ttl),
        );
        stop_processor_branch_task(
            domain,
            processor,
            &key,
            entry,
            ProcessorBranchStopMode::Evict,
        )
        .await;
        debug!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            key = branch_key_display(&key),
            "expired processor branch task"
        );
    }
}

async fn evict_processor_branch_instances_to_capacity(
    runtime: &Runtime,
    domain: &Domain,
    processor: &Identifier,
    branch: Option<&Identifier>,
    max_instances: usize,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) {
    for (key, entry) in instances.evict_lru_to_capacity(max_instances) {
        runtime.observe_branch_instance_removed(
            domain,
            branch,
            &key,
            Some(BranchEvictionReason::Lru),
        );
        stop_processor_branch_task(
            domain,
            processor,
            &key,
            entry,
            ProcessorBranchStopMode::Evict,
        )
        .await;
        debug!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            key = branch_key_display(&key),
            max_instances,
            "evicted processor branch task by lru"
        );
    }
}

async fn shutdown_all_processor_branch_instances(
    runtime: &Runtime,
    domain: &Domain,
    processor: &Identifier,
    branch: Option<&Identifier>,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) {
    for (key, entry) in instances.drain() {
        runtime.observe_branch_instance_removed(domain, branch, &key, None);
        stop_processor_branch_task(
            domain,
            processor,
            &key,
            entry,
            ProcessorBranchStopMode::Detach,
        )
        .await;
        debug!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            key = branch_key_display(&key),
            "stopped processor branch task"
        );
    }
}

fn restore_processor_branch_lru_snapshot(
    runtime: &Runtime,
    domain: &Domain,
    graph: &SharedActiveGraph,
    template: &BranchInstanceTemplate,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) -> Result<u64, String> {
    let Some(store) = &runtime.state_store else {
        return Ok(0);
    };
    let placement = branch_lru_placement(runtime, domain, template);
    let Some(snapshot) = store
        .latest_snapshot(&placement)
        .map_err(|error| error.to_string())?
    else {
        return Ok(0);
    };
    for (key, last_ingestion) in decode_branch_lru_snapshot(&snapshot.payload)? {
        let entry = spawn_processor_branch_task(
            ProcessorRuntimeContext::new(runtime.clone(), domain.clone(), graph.clone()),
            template,
            key.clone(),
            Some(last_ingestion),
            VecDeque::new(),
        )?;
        runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &key);
        instances.insert_restored(key, last_ingestion, entry);
    }
    instances.set_version(snapshot.lsm);
    Ok(snapshot.lsm)
}

#[derive(Debug, Clone)]
struct LookupHashMapCall {
    lookup: Identifier,
    lookup_runtime: Arc<LookupRuntime>,
    lookup_field: String,
    generated_field: String,
    key_program: Arc<VmCompiledProgram>,
}

#[derive(Debug, Clone)]
struct PendingLookupHashMapCall {
    lookup: Identifier,
    lookup_runtime: Arc<LookupRuntime>,
    lookup_field: String,
    lookup_field_type: ArrowDataType,
    generated_field: String,
    key_expr: SpannedExpr,
}

fn collect_expr_field_refs(expr: &SpannedExpr, refs: &mut Vec<(String, String)>) {
    match &expr.inner {
        Expr::Literal(_) | Expr::InternalFieldRef(_) => {}
        Expr::FieldRef(field_ref) => {
            refs.push((field_ref.relay.clone(), field_ref.field.clone()));
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            collect_expr_field_refs(expr, refs);
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_field_refs(left, refs);
            collect_expr_field_refs(right, refs);
        }
        Expr::Call { args, .. } => {
            for arg in args {
                collect_expr_field_refs(arg, refs);
            }
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_expr_field_refs(operand, refs);
            }
            for branch in branches {
                collect_expr_field_refs(&branch.when, refs);
                collect_expr_field_refs(&branch.result, refs);
            }
            if let Some(else_result) = else_result {
                collect_expr_field_refs(else_result, refs);
            }
        }
    }
}

fn collect_program_field_refs(program: &nervix_nspl::vm_program::Program) -> Vec<(String, String)> {
    let mut refs = Vec::new();
    if let Some(filter) = &program.filter {
        collect_expr_field_refs(filter, &mut refs);
    }
    for branch_filter in &program.branch_filters {
        collect_expr_field_refs(branch_filter, &mut refs);
    }
    for (_field_ref, expr) in &program.set {
        collect_expr_field_refs(expr, &mut refs);
    }
    for invocation in &program.invoke {
        for arg in &invocation.inner.args {
            collect_expr_field_refs(arg, &mut refs);
        }
    }
    refs
}

fn lookup_hash_map_literal_arg(
    args: &[SpannedExpr],
    index: usize,
    function_span: nervix_nspl::vm_program::Span,
) -> Result<&str, String> {
    let Some(arg) = args.get(index) else {
        return Err(format!(
            "LOOKUP_HASH_MAP expects 3 arguments, found {}",
            args.len()
        ));
    };
    match &arg.inner {
        Expr::Literal(Literal::String(value)) => Ok(value.as_str()),
        _ => Err(format!(
            "LOOKUP_HASH_MAP argument {} must be a string literal at {}..{}",
            index + 1,
            function_span.start,
            function_span.end
        )),
    }
}

fn expr_contains_lookup_hash_map(expr: &SpannedExpr) -> bool {
    match &expr.inner {
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => false,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => expr_contains_lookup_hash_map(expr),
        Expr::Binary { left, right, .. } => {
            expr_contains_lookup_hash_map(left) || expr_contains_lookup_hash_map(right)
        }
        Expr::Call { function, args } => {
            if let FunctionName::LookupHashMap = function {
                return true;
            }
            args.iter().any(expr_contains_lookup_hash_map)
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            operand
                .as_deref()
                .is_some_and(expr_contains_lookup_hash_map)
                || branches.iter().any(|branch| {
                    expr_contains_lookup_hash_map(&branch.when)
                        || expr_contains_lookup_hash_map(&branch.result)
                })
                || else_result
                    .as_deref()
                    .is_some_and(expr_contains_lookup_hash_map)
        }
    }
}

fn expr_same_without_spans(left: &SpannedExpr, right: &SpannedExpr) -> bool {
    match (&left.inner, &right.inner) {
        (Expr::Literal(left), Expr::Literal(right)) => left == right,
        (Expr::FieldRef(left), Expr::FieldRef(right)) => left == right,
        (
            Expr::Unary {
                op: left_op,
                expr: left_expr,
            },
            Expr::Unary {
                op: right_op,
                expr: right_expr,
            },
        ) => left_op == right_op && expr_same_without_spans(left_expr, right_expr),
        (
            Expr::Binary {
                op: left_op,
                left: left_left,
                right: left_right,
            },
            Expr::Binary {
                op: right_op,
                left: right_left,
                right: right_right,
            },
        ) => {
            left_op == right_op
                && expr_same_without_spans(left_left, right_left)
                && expr_same_without_spans(left_right, right_right)
        }
        (
            Expr::Cast {
                expr: left_expr,
                data_type: left_type,
            },
            Expr::Cast {
                expr: right_expr,
                data_type: right_type,
            },
        ) => left_type == right_type && expr_same_without_spans(left_expr, right_expr),
        (
            Expr::Call {
                function: left_function,
                args: left_args,
            },
            Expr::Call {
                function: right_function,
                args: right_args,
            },
        ) => {
            left_function == right_function
                && left_args.len() == right_args.len()
                && left_args
                    .iter()
                    .zip(right_args)
                    .all(|(left, right)| expr_same_without_spans(left, right))
        }
        (
            Expr::Case {
                operand: left_operand,
                branches: left_branches,
                else_result: left_else,
            },
            Expr::Case {
                operand: right_operand,
                branches: right_branches,
                else_result: right_else,
            },
        ) => {
            let operands_match = match (left_operand, right_operand) {
                (Some(left), Some(right)) => expr_same_without_spans(left, right),
                (None, None) => true,
                _ => false,
            };
            let else_results_match = match (left_else, right_else) {
                (Some(left), Some(right)) => expr_same_without_spans(left, right),
                (None, None) => true,
                _ => false,
            };
            operands_match
                && else_results_match
                && left_branches.len() == right_branches.len()
                && left_branches
                    .iter()
                    .zip(right_branches)
                    .all(|(left, right)| {
                        expr_same_without_spans(&left.when, &right.when)
                            && expr_same_without_spans(&left.result, &right.result)
                    })
        }
        _ => false,
    }
}

fn rewrite_lookup_hash_map_expr(
    expr: &SpannedExpr,
    available_lookups: &HashMap<Identifier, Arc<LookupRuntime>>,
    pending_calls: &mut Vec<PendingLookupHashMapCall>,
) -> Result<SpannedExpr, String> {
    let rewritten = match &expr.inner {
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => expr.clone(),
        Expr::Unary { op, expr: inner } => nervix_nspl::vm_program::SpannedNode {
            inner: Expr::Unary {
                op: *op,
                expr: Box::new(rewrite_lookup_hash_map_expr(
                    inner,
                    available_lookups,
                    pending_calls,
                )?),
            },
            span: expr.span,
        },
        Expr::Binary { op, left, right } => nervix_nspl::vm_program::SpannedNode {
            inner: Expr::Binary {
                op: *op,
                left: Box::new(rewrite_lookup_hash_map_expr(
                    left,
                    available_lookups,
                    pending_calls,
                )?),
                right: Box::new(rewrite_lookup_hash_map_expr(
                    right,
                    available_lookups,
                    pending_calls,
                )?),
            },
            span: expr.span,
        },
        Expr::Cast {
            expr: inner,
            data_type,
        } => nervix_nspl::vm_program::SpannedNode {
            inner: Expr::Cast {
                expr: Box::new(rewrite_lookup_hash_map_expr(
                    inner,
                    available_lookups,
                    pending_calls,
                )?),
                data_type: data_type.clone(),
            },
            span: expr.span,
        },
        Expr::Case {
            operand,
            branches,
            else_result,
        } => nervix_nspl::vm_program::SpannedNode {
            inner: Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|operand| {
                        rewrite_lookup_hash_map_expr(operand, available_lookups, pending_calls)
                            .map(Box::new)
                    })
                    .transpose()?,
                branches: branches
                    .iter()
                    .map(|branch| {
                        Ok(CaseArm {
                            when: rewrite_lookup_hash_map_expr(
                                &branch.when,
                                available_lookups,
                                pending_calls,
                            )?,
                            result: rewrite_lookup_hash_map_expr(
                                &branch.result,
                                available_lookups,
                                pending_calls,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                else_result: else_result
                    .as_ref()
                    .map(|else_result| {
                        rewrite_lookup_hash_map_expr(else_result, available_lookups, pending_calls)
                            .map(Box::new)
                    })
                    .transpose()?,
            },
            span: expr.span,
        },
        Expr::Call { function, args } => {
            if let FunctionName::LookupHashMap = function {
                if args.len() != 3 {
                    return Err(format!(
                        "LOOKUP_HASH_MAP expects 3 arguments, found {}",
                        args.len()
                    ));
                }
                let lookup_name = lookup_hash_map_literal_arg(args, 0, expr.span)?;
                let lookup = Identifier::parse(lookup_name).map_err(|error| {
                    format!("LOOKUP_HASH_MAP hash map name '{lookup_name}' is invalid: {error}")
                })?;
                let lookup_field = lookup_hash_map_literal_arg(args, 2, expr.span)?.to_string();
                if expr_contains_lookup_hash_map(&args[1]) {
                    return Err("LOOKUP_HASH_MAP key expression cannot contain another \
                                LOOKUP_HASH_MAP"
                        .to_string());
                }
                let Some(lookup_runtime) = available_lookups.get(&lookup).cloned() else {
                    return Err(format!(
                        "LOOKUP_HASH_MAP hash map '{}' is not instantiated",
                        lookup.as_str()
                    ));
                };
                let lookup_field_type = lookup_runtime
                    .schema
                    .arrow_schema()
                    .field_with_name(&lookup_field)
                    .map(|field| field.data_type().clone())
                    .map_err(|_| {
                        format!(
                            "LOOKUP_HASH_MAP field '{}' is missing from hash map '{}' schema",
                            lookup_field,
                            lookup.as_str()
                        )
                    })?;
                let existing = pending_calls.iter().find(|call| {
                    call.lookup == lookup
                        && call.lookup_field == lookup_field
                        && expr_same_without_spans(&call.key_expr, &args[1])
                });
                let generated_field = if let Some(existing) = existing {
                    existing.generated_field.clone()
                } else {
                    let generated_field = format!("value_{}", pending_calls.len());
                    pending_calls.push(PendingLookupHashMapCall {
                        lookup,
                        lookup_runtime,
                        lookup_field,
                        lookup_field_type,
                        generated_field: generated_field.clone(),
                        key_expr: args[1].clone(),
                    });
                    generated_field
                };
                nervix_nspl::vm_program::SpannedNode {
                    inner: Expr::InternalFieldRef(InternalFieldRef {
                        namespace: InternalFieldNamespace::LookupHashMap,
                        field: generated_field,
                    }),
                    span: expr.span,
                }
            } else {
                nervix_nspl::vm_program::SpannedNode {
                    inner: Expr::Call {
                        function: function.clone(),
                        args: args
                            .iter()
                            .map(|arg| {
                                rewrite_lookup_hash_map_expr(arg, available_lookups, pending_calls)
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    },
                    span: expr.span,
                }
            }
        }
    };
    Ok(rewritten)
}

fn rewrite_lookup_hash_map_program(
    parsed: &nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
    available_lookups: &HashMap<Identifier, Arc<LookupRuntime>>,
) -> Result<
    (
        nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
        Vec<PendingLookupHashMapCall>,
    ),
    String,
> {
    let mut pending_calls = Vec::new();
    let program = nervix_nspl::vm_program::Program {
        filter: parsed
            .inner
            .filter
            .as_ref()
            .map(|expr| rewrite_lookup_hash_map_expr(expr, available_lookups, &mut pending_calls))
            .transpose()?,
        branch_filters: parsed
            .inner
            .branch_filters
            .iter()
            .map(|expr| rewrite_lookup_hash_map_expr(expr, available_lookups, &mut pending_calls))
            .collect::<Result<Vec<_>, _>>()?,
        set: parsed
            .inner
            .set
            .iter()
            .map(|(field, expr)| {
                rewrite_lookup_hash_map_expr(expr, available_lookups, &mut pending_calls)
                    .map(|expr| (field.clone(), expr))
            })
            .collect::<Result<Vec<_>, _>>()?,
        invoke: parsed
            .inner
            .invoke
            .iter()
            .map(|invocation| {
                Ok(nervix_nspl::vm_program::SpannedNode {
                    inner: nervix_nspl::vm_program::Invocation {
                        function: invocation.inner.function.clone(),
                        args: invocation
                            .inner
                            .args
                            .iter()
                            .map(|arg| {
                                rewrite_lookup_hash_map_expr(
                                    arg,
                                    available_lookups,
                                    &mut pending_calls,
                                )
                            })
                            .collect::<Result<Vec<_>, String>>()?,
                    },
                    span: invocation.span,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
    };
    Ok((
        nervix_nspl::vm_program::SpannedNode {
            inner: program,
            span: parsed.span,
        },
        pending_calls,
    ))
}

fn compile_lookup_hash_map_calls(
    pending_calls: Vec<PendingLookupHashMapCall>,
    writable_namespace: &str,
    bindings: &[VmCompileBinding],
    udfs: Option<&UdfExecutor>,
) -> Result<(Vec<LookupHashMapCall>, Option<VmCompileBinding>), String> {
    if pending_calls.is_empty() {
        return Ok((Vec::new(), None));
    }

    let lookup_fields = pending_calls
        .iter()
        .map(|call| {
            arrow_schema::Field::new(&call.generated_field, call.lookup_field_type.clone(), true)
        })
        .collect::<Vec<_>>();
    let lookup_binding = VmCompileBinding::internal_readonly(
        InternalFieldNamespace::LookupHashMap,
        StdArc::new(arrow_schema::Schema::new(lookup_fields)),
    );
    let mut compiled_calls = Vec::with_capacity(pending_calls.len());
    for call in pending_calls {
        let key_program = nervix_nspl::vm_program::SpannedNode {
            inner: nervix_nspl::vm_program::Program {
                filter: None,
                branch_filters: Vec::new(),
                set: vec![(
                    nervix_nspl::vm_program::FieldRef {
                        relay: writable_namespace.to_string(),
                        field: call.generated_field.clone(),
                    },
                    call.key_expr,
                )],
                invoke: Vec::new(),
            },
            span: (0..0).into(),
        };
        let signatures = udfs
            .map(|executor| executor.signatures().clone())
            .unwrap_or_default();
        let key_types = infer_vm_set_expr_types_for_bindings_with_udfs(
            &key_program,
            bindings.iter().cloned(),
            signatures,
        )
        .map_err(|error| {
            format!(
                "LOOKUP_HASH_MAP key compile failed for hash map '{}' field '{}': {}",
                call.lookup.as_str(),
                call.lookup_field,
                error.message
            )
        })?;
        let key_output_schema = StdArc::new(arrow_schema::Schema::new(
            key_types
                .into_iter()
                .map(|(name, data_type, nullable)| {
                    arrow_schema::Field::new(name, data_type, nullable)
                })
                .collect::<Vec<_>>(),
        ));
        let compiled_key = compile_vm_program_with_options_for_bindings_with_sensitivity(
            &key_program,
            key_output_schema,
            VmSchemaSensitivity::default(),
            bindings.iter().cloned(),
            runtime_udf_compile_options(
                udfs,
                VmCompileOptions {
                    output_mode: VmOutputMode::ExplicitOnly,
                    ..VmCompileOptions::default()
                },
            ),
        )
        .map_err(|error| {
            format!(
                "LOOKUP_HASH_MAP key compile failed for hash map '{}' field '{}': {}",
                call.lookup.as_str(),
                call.lookup_field,
                error.message
            )
        })?;
        compiled_calls.push(LookupHashMapCall {
            lookup: call.lookup,
            lookup_runtime: call.lookup_runtime,
            lookup_field: call.lookup_field,
            generated_field: call.generated_field,
            key_program: Arc::new(compiled_key),
        });
    }
    Ok((compiled_calls, Some(lookup_binding)))
}

fn referenced_materialized_stream_bindings(
    parsed: &nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
    writable_namespaces: &HashSet<String>,
    available_materialized_streams: &HashMap<Identifier, RuntimeMaterializedRelaySpec>,
    current_branching: &[Identifier],
) -> Result<(Vec<VmCompileBinding>, MaterializedProgramInterest), String> {
    let mut fields_by_relay = HashMap::<Identifier, HashSet<String>>::default();
    for (relay, field) in collect_program_field_refs(&parsed.inner) {
        if writable_namespaces.contains(&relay)
            || relay == INGEST_METADATA_NAMESPACE
            || relay == BRANCH_NAMESPACE
        {
            continue;
        }
        let Some(relay_name) = relay.strip_prefix("relay_state.") else {
            continue;
        };
        let Ok(relay) = Identifier::parse(relay_name) else {
            continue;
        };
        let Some(spec) = available_materialized_streams.get(&relay) else {
            continue;
        };
        if !spec.branching.is_empty() && spec.branching != current_branching {
            return Err(format!(
                "materialized relay 'relay_state.{}' uses branch fields ({}) but current input \
                 uses ({})",
                relay.as_str(),
                format_branched_by(&spec.branching),
                format_branched_by(current_branching),
            ));
        }
        fields_by_relay.entry(relay).or_default().insert(field);
    }

    let mut bindings = Vec::with_capacity(fields_by_relay.len());
    let mut interest = Vec::with_capacity(fields_by_relay.len());
    for (relay, fields) in fields_by_relay {
        let Some(spec) = available_materialized_streams.get(&relay) else {
            continue;
        };
        let mut ordered_fields = fields.into_iter().collect::<Vec<_>>();
        ordered_fields.sort();
        let projected_fields = spec
            .schema
            .fields()
            .iter()
            .filter(|field| ordered_fields.iter().any(|name| name == field.name()))
            .cloned()
            .collect::<Vec<_>>();
        let projected_sensitivity = VmSchemaSensitivity::from_sensitive_fields(
            ordered_fields
                .iter()
                .filter(|field| spec.sensitivity.is_sensitive(field))
                .cloned(),
        );
        bindings.push(
            VmCompileBinding::readonly(
                format!("relay_state.{}", relay.as_str()),
                StdArc::new(arrow_schema::Schema::new(projected_fields)),
            )
            .with_sensitivity(projected_sensitivity),
        );
        interest.push(MaterializedRelayInterest {
            relay,
            fields: ordered_fields,
            key_mode: if spec.branching.is_empty() {
                MaterializedLookupKeyMode::Root
            } else {
                MaterializedLookupKeyMode::CurrentBranch
            },
        });
    }
    interest.sort_by(|left, right| left.relay.as_str().cmp(right.relay.as_str()));

    Ok((bindings, MaterializedProgramInterest { relays: interest }))
}

fn ingest_source_supports_headers(source: &IngestSource) -> bool {
    matches!(
        source,
        IngestSource::Endpoint { .. }
            | IngestSource::Http { .. }
            | IngestSource::Kafka { .. }
            | IngestSource::Nats { .. }
            | IngestSource::Pulsar { .. }
            | IngestSource::RabbitMq { .. }
            | IngestSource::Sqs { .. }
    )
}

fn emit_sink_supports_headers(sink: &EmitSink) -> bool {
    matches!(
        sink,
        EmitSink::Kafka { .. }
            | EmitSink::Pulsar { .. }
            | EmitSink::RabbitMq { .. }
            | EmitSink::Nats { .. }
            | EmitSink::Sqs { .. }
    )
}

fn collect_expression_field_paths(expression: &SpannedExpr, fields: &mut Vec<FieldPath>) {
    match &expression.inner {
        Expr::FieldRef(field) => {
            fields.push(FieldPath::new(format!("{}.{}", field.relay, field.field)));
        }
        Expr::InternalFieldRef(_) => {}
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            collect_expression_field_paths(expr, fields);
        }
        Expr::Binary { left, right, .. } => {
            collect_expression_field_paths(left, fields);
            collect_expression_field_paths(right, fields);
        }
        Expr::Call { args, .. } => {
            for argument in args {
                collect_expression_field_paths(argument, fields);
            }
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_expression_field_paths(operand, fields);
            }
            for branch in branches {
                collect_expression_field_paths(&branch.when, fields);
                collect_expression_field_paths(&branch.result, fields);
            }
            if let Some(else_result) = else_result {
                collect_expression_field_paths(else_result, fields);
            }
        }
        Expr::Literal(_) => {}
    }
}

fn compiled_message_error_sites(
    program: &nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
    set_operations: &[MessageErrorOperation],
    filter_operation: Option<MessageErrorOperation>,
) -> Result<Vec<CompiledMessageErrorSite>, String> {
    if set_operations.len() != program.inner.set.len() {
        return Err(format!(
            "message-error metadata has {} SET operations for {} lowered assignments",
            set_operations.len(),
            program.inner.set.len()
        ));
    }
    let mut sites = Vec::with_capacity(
        program.inner.set.len()
            + usize::from(program.inner.filter.is_some())
            + program.inner.invoke.len(),
    );
    for (index, ((target, expression), operation)) in
        program.inner.set.iter().zip(set_operations).enumerate()
    {
        let mut fields = vec![FieldPath::new(format!("{}.{}", target.relay, target.field))];
        collect_expression_field_paths(expression, &mut fields);
        sites.push(CompiledMessageErrorSite {
            span: expression.span,
            operation: *operation,
            operation_index: Some(
                u32::try_from(index).map_err(|_| "too many ordered SET operations".to_string())?,
            ),
            fields: SortedSet::from_unsorted(fields),
        });
    }
    if let Some(expression) = &program.inner.filter {
        let mut fields = Vec::new();
        collect_expression_field_paths(expression, &mut fields);
        sites.push(CompiledMessageErrorSite {
            span: expression.span,
            operation: filter_operation.ok_or_else(|| {
                "message-error metadata is missing the filter operation".to_string()
            })?,
            operation_index: None,
            fields: SortedSet::from_unsorted(fields),
        });
    }
    for (index, invocation) in program.inner.invoke.iter().enumerate() {
        let mut fields = Vec::new();
        for argument in &invocation.inner.args {
            collect_expression_field_paths(argument, &mut fields);
        }
        sites.push(CompiledMessageErrorSite {
            span: invocation.span,
            operation: MessageErrorOperation::Invoke,
            operation_index: Some(
                u32::try_from(index)
                    .map_err(|_| "too many ordered INVOKE operations".to_string())?,
            ),
            fields: SortedSet::from_unsorted(fields),
        });
    }
    Ok(sites)
}

fn message_error_arrow_schema() -> StdArc<arrow_schema::Schema> {
    StdArc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("reference", ArrowDataType::Utf8, false),
        arrow_schema::Field::new("code", ArrowDataType::Utf8, false),
        arrow_schema::Field::new("message", ArrowDataType::Utf8, false),
        arrow_schema::Field::new("operation", ArrowDataType::Utf8, false),
        arrow_schema::Field::new("operation_index", ArrowDataType::UInt32, true),
        arrow_schema::Field::new(
            "fields",
            ArrowDataType::List(StdArc::new(arrow_schema::Field::new(
                "item",
                ArrowDataType::Utf8,
                false,
            ))),
            false,
        ),
        arrow_schema::Field::new(
            "occurred_at",
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        ),
    ]))
}

fn all_optional_arrow_schema(schema: &CompiledSchema) -> StdArc<arrow_schema::Schema> {
    StdArc::new(arrow_schema::Schema::new(
        schema
            .arrow_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ))
}

fn compile_message_error_set_program(
    domain: &Domain,
    node: &Identifier,
    assignments: &[Assignment],
    output_schema: Arc<CompiledSchema>,
    schemas: MessageErrorCompileSchemas,
    context: RuntimeVmCompileContext<'_>,
) -> Result<CompiledProgramWithMaterializedInterest, String> {
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments: assignments.to_vec(),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("error_output", "error_output"),
    )
    .map_err(|reason| {
        format!(
            "message-error SET for '{}' in domain '{}' is invalid: {reason}",
            node.as_str(),
            domain.as_str()
        )
    })?;
    let set_operations = vec![MessageErrorOperation::Set; parsed.inner.set.len()];
    let error_sites = compiled_message_error_sites(&parsed, &set_operations, None)?;
    let mut bindings = vec![
        VmCompileBinding::writable("error_output", output_schema.arrow_schema())
            .with_sensitivity(output_schema.vm_sensitivity()),
    ];
    if let Some(input) = schemas.input {
        bindings.push(
            VmCompileBinding::readonly("input", input.arrow_schema())
                .with_sensitivity(input.vm_sensitivity()),
        );
    }
    if let Some(left) = schemas.left {
        bindings.push(
            VmCompileBinding::readonly("left", left.arrow_schema())
                .with_sensitivity(left.vm_sensitivity()),
        );
    }
    if let Some(right) = schemas.right {
        bindings.push(
            VmCompileBinding::readonly("right", right.arrow_schema())
                .with_sensitivity(right.vm_sensitivity()),
        );
    }
    if let Some(partial_output) = schemas.partial_output {
        bindings.push(
            VmCompileBinding::readonly(
                "partial_output",
                all_optional_arrow_schema(partial_output.as_ref()),
            )
            .with_sensitivity(partial_output.vm_sensitivity()),
        );
    }
    bindings.push(VmCompileBinding::readonly(
        "error",
        message_error_arrow_schema(),
    ));

    let local_namespaces = HashSet::from_iter([
        "error_output".to_string(),
        "input".to_string(),
        "left".to_string(),
        "right".to_string(),
        "partial_output".to_string(),
        "error".to_string(),
    ]);
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &parsed,
        &local_namespaces,
        context.available_materialized_streams,
        &schemas.current_branching,
    )?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups)?;
    let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
        pending_lookup_calls,
        "error_output",
        &bindings,
        context.udfs,
    )?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }
    let output_sensitivity = output_schema.vm_sensitivity();
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema.arrow_schema(),
        output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            allow_header_reads: schemas.allow_header_reads,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| {
        format!(
            "message-error SET compile failed for '{}': {}",
            node.as_str(),
            error.message
        )
    })?;
    Ok(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    })
}

fn compile_expression_filter_program(
    target: RuntimeCompileTarget<'_>,
    filter: Option<&nervix_models::Expression>,
    input: RuntimeVmSchema,
    allow_header_reads: bool,
    filter_operation: MessageErrorOperation,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    compile_scoped_filter_program(
        target,
        filter,
        input,
        filter_operation,
        context,
        RuntimeFilterScope::Source {
            namespace: "input",
            allow_header_reads,
            allow_metadata: allow_header_reads,
        },
    )
}

fn compile_finalized_output_filter_program(
    domain: &Domain,
    identifier: &Identifier,
    filter: Option<&nervix_models::Expression>,
    output_schema: StdArc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    compile_scoped_filter_program(
        RuntimeCompileTarget { domain, identifier },
        filter,
        RuntimeVmSchema {
            schema: output_schema,
            sensitivity: output_sensitivity,
        },
        MessageErrorOperation::RouteWhere,
        context,
        RuntimeFilterScope::FinalizedOutput,
    )
}

#[derive(Debug, Clone, Copy)]
enum RuntimeFilterScope {
    Source {
        namespace: &'static str,
        allow_header_reads: bool,
        allow_metadata: bool,
    },
    FinalizedOutput,
}

impl RuntimeFilterScope {
    const fn namespace(self) -> &'static str {
        match self {
            Self::Source { namespace, .. } => namespace,
            Self::FinalizedOutput => "output",
        }
    }

    const fn allow_header_reads(self) -> bool {
        match self {
            Self::Source {
                allow_header_reads, ..
            } => allow_header_reads,
            Self::FinalizedOutput => false,
        }
    }

    const fn allow_metadata(self) -> bool {
        match self {
            Self::Source { allow_metadata, .. } => allow_metadata,
            Self::FinalizedOutput => false,
        }
    }
}

fn compile_scoped_filter_program(
    target: RuntimeCompileTarget<'_>,
    filter: Option<&nervix_models::Expression>,
    input: RuntimeVmSchema,
    filter_operation: MessageErrorOperation,
    context: RuntimeVmCompileContext<'_>,
    scope: RuntimeFilterScope,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let RuntimeCompileTarget { domain, identifier } = target;
    let RuntimeVmSchema {
        schema,
        sensitivity,
    } = input;
    let Some(filter) = filter else {
        return Ok(None);
    };
    let parsed = match scope {
        RuntimeFilterScope::Source { .. } => lower_route_construction(
            &RouteConstruction {
                where_clause: Some(filter.clone()),
                ..RouteConstruction::default()
            },
            SemanticNamespaces::new("input", "__invalid_filter_target"),
        ),
        RuntimeFilterScope::FinalizedOutput => lower_finalized_output_filter(filter, &schema),
    }
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!("filter for '{}' is invalid: {reason}", identifier.as_str()),
    })?;
    let error_sites =
        compiled_message_error_sites(&parsed, &[], Some(filter_operation)).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            }
        })?;
    let mut local_namespaces =
        HashSet::from_iter([scope.namespace().to_string(), BRANCH_NAMESPACE.to_string()]);
    if scope.allow_metadata() {
        local_namespaces.insert(INGEST_METADATA_NAMESPACE.to_string());
    }
    let mut bindings = vec![
        VmCompileBinding::writable(scope.namespace(), schema.clone())
            .with_sensitivity(sensitivity.clone()),
    ];
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "filter compile failed for '{}': {reason}",
                    identifier.as_str()
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
        pending_lookup_calls,
        scope.namespace(),
        &bindings,
        context.udfs,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "filter compile failed for '{}': {reason}",
            identifier.as_str()
        ),
    })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        schema,
        sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            allow_header_reads: scope.allow_header_reads(),
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "filter compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity: sensitivity,
        materialized_interest,
        output_namespace_input: match scope {
            RuntimeFilterScope::Source { .. } => OutputNamespaceInput::Uninitialized,
            RuntimeFilterScope::FinalizedOutput => OutputNamespaceInput::Finalized,
        },
        lookup_hash_maps,
        error_sites,
    }))
}

fn compile_processor_output_filter_map_program(
    target: RuntimeCompileTarget<'_>,
    input_relays: &[Identifier],
    output_relay: &Identifier,
    construction: &RouteConstruction,
    schemas: RuntimeVmSchemaPair,
    inferencer_tensors: Option<InferencerFilterMapTensors<'_>>,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let RuntimeCompileTarget { domain, identifier } = target;
    let RuntimeVmSchemaPair {
        input: input_schema,
        input_sensitivity,
        output: output_schema,
        output_sensitivity,
    } = schemas;
    let parsed = if let Some(tensors) = inferencer_tensors {
        lower_generated_route(
            construction,
            output_schema.as_ref(),
            tensors.output_arrow_schema().as_ref(),
        )
    } else {
        lower_transforming_route(construction, &input_schema, &output_schema)
    }
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output construction for '{}' is invalid: {reason}",
            identifier
        ),
    })?;
    if !parsed.inner.branch_filters.is_empty() {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "FILTER-MAP for '{}' may contain at most one WHERE clause",
                identifier.as_str()
            ),
        });
    }
    let inherited_count = if inferencer_tensors.is_some() {
        0
    } else {
        parsed
            .inner
            .set
            .len()
            .saturating_sub(construction.assignments.len())
    };
    let set_operations = (0..parsed.inner.set.len())
        .map(|index| {
            if index < inherited_count {
                MessageErrorOperation::Inherit
            } else {
                MessageErrorOperation::Set
            }
        })
        .collect::<Vec<_>>();
    let error_sites = compiled_message_error_sites(
        &parsed,
        &set_operations,
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    let original_parsed = parsed.clone();
    let mut bindings = vec![
        VmCompileBinding::writable("output", output_schema.clone())
            .with_sensitivity(output_sensitivity.clone()),
    ];
    if let Some(tensors) = inferencer_tensors {
        bindings.push(VmCompileBinding::readonly(
            "generated",
            tensors.output_arrow_schema(),
        ));
    } else {
        bindings.insert(
            0,
            VmCompileBinding::readonly("input", input_schema.clone())
                .with_sensitivity(input_sensitivity.clone()),
        );
        for relay in input_relays {
            bindings.push(
                VmCompileBinding::readonly(relay.as_str(), input_schema.clone())
                    .with_sensitivity(input_sensitivity.clone()),
            );
        }
    }
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let mut local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        "generated".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    if inferencer_tensors.is_none() {
        local_namespaces.extend(input_relays.iter().map(|relay| relay.as_str().to_string()));
        local_namespaces.insert(output_relay.as_str().to_string());
    }
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &original_parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) =
        compile_lookup_hash_map_calls(pending_lookup_calls, "output", &bindings, context.udfs)
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }

    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    }))
}

fn compile_output_branch_program(
    target: RuntimeCompileTarget<'_>,
    branch: Option<&OutputBranch>,
    input: RuntimeVmSchema,
    output: RuntimeVmSchema,
    branch_schema: Option<StdArc<arrow_schema::Schema>>,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledBranchProgram>, RuntimeError> {
    let RuntimeCompileTarget { domain, identifier } = target;
    let Some(OutputBranch::BranchedBy { assignments, .. }) = branch else {
        return Ok(None);
    };
    if assignments.is_empty() {
        return Ok(None);
    }
    let branch_schema = branch_schema.ok_or_else(|| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output branch construction for '{}' has no branch schema",
            identifier.as_str()
        ),
    })?;
    let parsed = lower_branch_construction(
        assignments,
        branch_schema.as_ref(),
        output.schema.as_ref(),
        input.schema.as_ref(),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output branch construction for '{}' is invalid: {}",
            identifier.as_str(),
            reason
        ),
    })?;
    let error_sites = compiled_message_error_sites(
        &parsed,
        &vec![MessageErrorOperation::Set; parsed.inner.set.len()],
        None,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    let original_parsed = parsed.clone();
    let mut bindings = vec![
        VmCompileBinding::readonly("input", input.schema.clone())
            .with_sensitivity(input.sensitivity),
        VmCompileBinding::readonly("output", output.schema.clone())
            .with_sensitivity(output.sensitivity.clone()),
        VmCompileBinding::readonly("message", output.schema).with_sensitivity(output.sensitivity),
        VmCompileBinding::writable(BRANCH_NAMESPACE, branch_schema.clone()),
    ];
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        "message".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &original_parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "output branch compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
        pending_lookup_calls,
        BRANCH_NAMESPACE,
        &bindings,
        context.udfs,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output branch compile failed for '{}': {}",
            identifier.as_str(),
            reason
        ),
    })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }
    let sensitivity = VmSchemaSensitivity::default();
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        branch_schema,
        sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output branch compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledBranchProgram {
        program: CompiledProgramWithMaterializedInterest {
            compiled: Arc::new(compiled),
            output_sensitivity: sensitivity,
            materialized_interest,
            output_namespace_input: OutputNamespaceInput::Uninitialized,
            lookup_hash_maps,
            error_sites,
        },
    }))
}

fn compile_wasm_output_filter_map_program(
    domain: &Domain,
    identifier: &Identifier,
    construction: &RouteConstruction,
    output_schema: StdArc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let parsed =
        lower_generated_route(construction, output_schema.as_ref(), output_schema.as_ref())
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "WASM output construction for '{}' is invalid: {reason}",
                    identifier
                ),
            })?;
    if !parsed.inner.branch_filters.is_empty() {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "FILTER-MAP for '{}' may contain at most one WHERE clause",
                identifier.as_str()
            ),
        });
    }
    if !parsed.inner.invoke.is_empty() {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "WASM processor '{}' TO clauses may use SET and WHERE, but not INVOKE",
                identifier.as_str()
            ),
        });
    }
    let set_operations = vec![MessageErrorOperation::Set; parsed.inner.set.len()];
    let error_sites = compiled_message_error_sites(
        &parsed,
        &set_operations,
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;

    let original_parsed = parsed.clone();
    let mut bindings = vec![
        VmCompileBinding::readonly("generated", output_schema.clone())
            .with_sensitivity(output_sensitivity.clone()),
        VmCompileBinding::writable("output", output_schema.clone())
            .with_sensitivity(output_sensitivity.clone()),
    ];
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let local_namespaces = HashSet::from_iter([
        "generated".to_string(),
        "output".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &original_parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) =
        compile_lookup_hash_map_calls(pending_lookup_calls, "output", &bindings, context.udfs)
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }

    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    }))
}

pub(crate) fn compile_emitter_filter_map_program(
    domain: &Domain,
    emitter: &CreateEmitter,
    input_schema: StdArc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    output_schema: StdArc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledEmitterFilterMapProgram>, RuntimeError> {
    if emitter.construction.is_empty() {
        return Ok(None);
    }
    let codec_route = emitter.encode_using_codec.is_some();
    if !codec_route
        && (emitter.construction.inherit.is_some()
            || !emitter.construction.assignments.is_empty()
            || !emitter.construction.invocations.is_empty())
    {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "direct emitter '{}' supports VALUES and WHERE only",
                emitter.name.as_str()
            ),
        });
    }
    let parsed = if codec_route {
        lower_transforming_route(
            &emitter.construction,
            input_schema.as_ref(),
            output_schema.as_ref(),
        )
    } else {
        lower_route_construction(
            &emitter.construction,
            SemanticNamespaces::new("input", "__invalid_direct_emitter_output"),
        )
    }
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "emitter route '{}' is invalid: {reason}",
            emitter.name.as_str()
        ),
    })?;
    if parsed
        .inner
        .invoke
        .iter()
        .any(|invocation| invocation.inner.function == FunctionName::WriteHeader)
        && !emit_sink_supports_headers(&emitter.sink)
    {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "{} emitters do not support FILTER-MAP headers",
                emitter.sink.transport_label()
            ),
        });
    }
    let inherited_count = if codec_route {
        parsed
            .inner
            .set
            .len()
            .saturating_sub(emitter.construction.assignments.len())
    } else {
        0
    };
    let set_operations = (0..parsed.inner.set.len())
        .map(|index| {
            if index < inherited_count {
                MessageErrorOperation::Inherit
            } else {
                MessageErrorOperation::Set
            }
        })
        .collect::<Vec<_>>();
    let error_sites = compiled_message_error_sites(
        &parsed,
        &set_operations,
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;

    let body = compile_emitter_filter_map_part(
        RuntimeCompileTarget {
            domain,
            identifier: &emitter.name,
        },
        parsed,
        RuntimeVmSchemaPair {
            input: input_schema,
            input_sensitivity,
            output: output_schema,
            output_sensitivity,
        },
        codec_route,
        error_sites,
        context,
    )?;
    let materialized_interest = body.materialized_interest.clone();
    Ok(Some(CompiledEmitterFilterMapProgram {
        body,
        materialized_interest,
        codec_route,
    }))
}

pub(crate) fn compile_sqs_fifo_group_program(
    domain: &Domain,
    emitter: &CreateEmitter,
    input_schema: StdArc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let EmitSink::Sqs {
        fifo_group: Some(SqsFifoGroup::Expression(expression)),
        ..
    } = emitter.sink.as_ref()
    else {
        return Ok(None);
    };
    let field = Identifier::parse("fifo_group").expect("internal FIFO field name is valid");
    let output_schema = StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        field.as_str(),
        ArrowDataType::Utf8,
        false,
    )]));
    let parsed = lower_transforming_route(
        &RouteConstruction {
            assignments: vec![Assignment {
                target: nervix_models::AssignmentTarget::bare(field),
                value: expression.clone(),
            }],
            ..RouteConstruction::default()
        },
        input_schema.as_ref(),
        output_schema.as_ref(),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "SQS FIFO GROUP expression for emitter '{}' is invalid: {reason}",
            emitter.name.as_str()
        ),
    })?;
    let error_sites = compiled_message_error_sites(&parsed, &[MessageErrorOperation::Set], None)
        .map_err(|reason| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason,
        })?;
    compile_emitter_filter_map_part(
        RuntimeCompileTarget {
            domain,
            identifier: &emitter.name,
        },
        parsed,
        RuntimeVmSchemaPair {
            input: input_schema,
            input_sensitivity,
            output: output_schema,
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        true,
        error_sites,
        context,
    )
    .map(Some)
}

fn compile_emitter_filter_map_part(
    target: RuntimeCompileTarget<'_>,
    parsed: nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
    schemas: RuntimeVmSchemaPair,
    codec_route: bool,
    error_sites: Vec<CompiledMessageErrorSite>,
    context: RuntimeVmCompileContext<'_>,
) -> Result<CompiledProgramWithMaterializedInterest, RuntimeError> {
    let RuntimeCompileTarget { domain, identifier } = target;
    let RuntimeVmSchemaPair {
        input: input_schema,
        input_sensitivity,
        output: output_schema,
        output_sensitivity,
    } = schemas;
    let mut bindings = if codec_route {
        vec![
            VmCompileBinding::readonly("input", input_schema.clone())
                .with_sensitivity(input_sensitivity.clone()),
            VmCompileBinding::writable("output", output_schema.clone())
                .with_sensitivity(output_sensitivity.clone()),
        ]
    } else {
        vec![
            VmCompileBinding::writable("input", input_schema.clone())
                .with_sensitivity(input_sensitivity.clone()),
            VmCompileBinding::readonly("message", input_schema).with_sensitivity(input_sensitivity),
        ]
    };
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "message".to_string(),
        "output".to_string(),
    ]);
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let lookup_output_namespace = if codec_route { "output" } else { "input" };
    let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
        pending_lookup_calls,
        lookup_output_namespace,
        &bindings,
        context.udfs,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            reason
        ),
    })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: if codec_route {
                VmOutputMode::ExplicitOnly
            } else {
                VmOutputMode::PassthroughByName
            },
            allow_sensitive_output: false,
            allow_header_writes: true,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    })
}

pub(crate) fn compile_session_filter_map_program(
    domain: &Domain,
    identifier: &Identifier,
    where_clause: Option<&nervix_models::Expression>,
    input_schema: StdArc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    compile_expression_filter_program(
        RuntimeCompileTarget { domain, identifier },
        where_clause,
        RuntimeVmSchema {
            schema: input_schema,
            sensitivity: input_sensitivity,
        },
        false,
        MessageErrorOperation::SourceWhere,
        context,
    )
}

pub(super) fn compile_key_projection_program(
    processor_kind: &str,
    processor: &Identifier,
    clause: &str,
    input_relays: &[Identifier],
    expressions: &[nervix_models::Expression],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<VmCompiledProgram, String> {
    if input_relays.is_empty() {
        return Err(format!(
            "{} '{}' {} requires at least one input relay",
            processor_kind,
            processor.as_str(),
            clause
        ));
    }
    let assignments = expressions
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            Ok(nervix_models::Assignment {
                target: nervix_models::AssignmentTarget::bare(
                    Identifier::parse(&format!("key_{index}")).map_err(|error| {
                        format!(
                            "{processor_kind} '{}' has invalid key target: {error}",
                            processor
                        )
                    })?,
                ),
                value: expression.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments,
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("input", "input"),
    )
    .map_err(|reason| {
        format!(
            "{} '{}' {} is invalid: {}",
            processor_kind,
            processor.as_str(),
            clause,
            reason
        )
    })?;
    let bindings = vec![VmCompileBinding::writable("input", input_schema.clone())];
    let signatures = udfs
        .map(|udfs| udfs.signatures().clone())
        .unwrap_or_default();
    let key_types =
        infer_vm_set_expr_types_for_bindings_with_udfs(&parsed, bindings.clone(), signatures)
            .map_err(|error| {
                format!(
                    "{} '{}' {} compile failed: {}",
                    processor_kind,
                    processor.as_str(),
                    clause,
                    error.message
                )
            })?;
    if key_types.len() != expressions.len() {
        return Err(format!(
            "{} '{}' {} inferred a different number of key fields",
            processor_kind,
            processor.as_str(),
            clause
        ));
    }
    let output_schema = StdArc::new(arrow_schema::Schema::new(
        key_types
            .into_iter()
            .map(|(name, data_type, nullable)| arrow_schema::Field::new(name, data_type, nullable))
            .collect::<Vec<_>>(),
    ));
    compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        VmSchemaSensitivity::default(),
        bindings,
        runtime_udf_compile_options(
            udfs,
            VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                ..VmCompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        format!(
            "{} '{}' {} compile failed: {}",
            processor_kind,
            processor.as_str(),
            clause,
            error.message
        )
    })
}

async fn evaluate_constant_expression_vm(
    expression: &nervix_models::Expression,
    udfs: Option<&UdfExecutor>,
) -> Result<RuntimeValue, String> {
    const OUTPUT_NAMESPACE: &str = "constant";
    const OUTPUT_FIELD: &str = "value";
    let assignment = nervix_models::Assignment {
        target: nervix_models::AssignmentTarget::bare(
            Identifier::parse(OUTPUT_FIELD).map_err(|error| error.to_string())?,
        ),
        value: expression.clone(),
    };
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments: vec![assignment],
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("input", OUTPUT_NAMESPACE),
    )
    .map_err(|reason| format!("constant expression is invalid: {reason}"))?;
    let empty_schema = StdArc::new(arrow_schema::Schema::empty());
    let infer_bindings = vec![
        VmCompileBinding::readonly("input", empty_schema.clone()),
        VmCompileBinding::writeonly(OUTPUT_NAMESPACE, empty_schema),
    ];
    let inferred = infer_vm_set_expr_types_for_bindings_with_udfs(
        &parsed,
        infer_bindings,
        udfs.map(|executor| executor.signatures().clone())
            .unwrap_or_default(),
    )
    .map_err(|error| {
        format!(
            "constant expression type inference failed: {}",
            error.message
        )
    })?;
    let output_schema = StdArc::new(arrow_schema::Schema::new(
        inferred
            .into_iter()
            .map(|(name, data_type, nullable)| arrow_schema::Field::new(name, data_type, nullable))
            .collect::<Vec<_>>(),
    ));
    let bindings = vec![
        VmCompileBinding::readonly("input", StdArc::new(arrow_schema::Schema::empty())),
        VmCompileBinding::writeonly(OUTPUT_NAMESPACE, output_schema.clone()),
    ];
    let compiled = Arc::new(
        compile_vm_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            output_schema,
            VmSchemaSensitivity::default(),
            bindings,
            runtime_udf_compile_options(
                udfs,
                VmCompileOptions {
                    output_mode: VmOutputMode::ExplicitOnly,
                    ..VmCompileOptions::default()
                },
            ),
        )
        .map_err(|error| format!("constant expression compile failed: {}", error.message))?,
    );
    let input = VmTypedBatch::try_new_with_row_count(
        compiled.input_schema.clone(),
        compiled
            .input_schema
            .fields()
            .iter()
            .map(|field| VmTypedArray::uninitialized(field.data_type().clone(), 1))
            .collect(),
        1,
    )
    .map_err(|error| error.to_string())?;
    let result = execute_program_with_selection_in_context(
        &compiled,
        &input,
        &VmExecutionContext {
            now: current_timestamp(),
            injector: None,
        },
    )
    .await
    .map_err(|error| format!("constant expression execution failed: {error}"))?;
    if result.selected_rows.as_slice() != [0] {
        return Err("constant expression did not produce exactly one row".to_string());
    }
    vm_output_value(&result.batch, 0, OUTPUT_FIELD)?
        .ok_or_else(|| "constant expression produced NULL".to_string())
}

fn compile_reorderer_program(
    processor: &Identifier,
    input_relays: &[Identifier],
    order_by: &[nervix_models::Expression],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledReordererProgram, String> {
    if order_by.is_empty() {
        return Err(format!(
            "reorderer '{}' requires at least one BY expression",
            processor.as_str()
        ));
    }
    let compiled = compile_key_projection_program(
        "reorderer",
        processor,
        "BY",
        input_relays,
        order_by,
        input_schema,
        udfs,
    )?;
    Ok(CompiledReordererProgram {
        key_column_offset: 0,
        key_count: order_by.len(),
        program: Arc::new(compiled),
    })
}

fn compile_correlator_where_program(
    processor: &Identifier,
    correlate_where: &nervix_models::Expression,
    left_relays: &[Identifier],
    left_schema: StdArc<arrow_schema::Schema>,
    right_relays: &[Identifier],
    right_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledCorrelatorWhereProgram, String> {
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(correlate_where.clone()),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new(
            "__invalid_correlator_bare_read",
            "__invalid_correlator_target",
        ),
    )
    .map_err(|reason| {
        format!(
            "correlator '{}' CORRELATE WHERE is invalid: {}",
            processor.as_str(),
            reason
        )
    })?;
    if left_relays.is_empty() || right_relays.is_empty() {
        return Err(format!(
            "correlator '{}' requires both LEFT and RIGHT inputs",
            processor.as_str()
        ));
    }
    let bindings = vec![
        VmCompileBinding::writable("left", left_schema.clone()),
        VmCompileBinding::readonly("right", right_schema.clone()),
    ];
    let program = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        left_schema.clone(),
        VmSchemaSensitivity::default(),
        bindings,
        runtime_udf_compile_options(udfs, VmCompileOptions::default()),
    )
    .map_err(|error| {
        format!(
            "correlator '{}' CORRELATE WHERE compile failed: {}",
            processor.as_str(),
            error.message
        )
    })?;
    Ok(CompiledCorrelatorWhereProgram {
        program: Arc::new(program),
    })
}

struct CorrelatorOutputCompileContext<'a> {
    processor: &'a Identifier,
    left_schema: StdArc<arrow_schema::Schema>,
    left_sensitivity: VmSchemaSensitivity,
    right_schema: StdArc<arrow_schema::Schema>,
    right_sensitivity: VmSchemaSensitivity,
    output_relay: &'a Identifier,
    output_schema: StdArc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    construction: &'a RouteConstruction,
    runtime: RuntimeVmCompileContext<'a>,
}

impl CorrelatorOutputCompileContext<'_> {
    fn compile(self) -> Result<CompiledCorrelatorOutputProgram, String> {
        let parsed = lower_route_construction(
            self.construction,
            SemanticNamespaces::new("__invalid_correlator_bare_read", "output"),
        )?;
        if !parsed.inner.branch_filters.is_empty()
            || !parsed.inner.invoke.is_empty()
            || parsed.inner.set.is_empty()
        {
            return Err(format!(
                "correlator '{}' TO output '{}' must contain SET assignments and may contain WHERE",
                self.processor.as_str(),
                self.output_relay.as_str()
            ));
        }
        let error_sites = compiled_message_error_sites(
            &parsed,
            &vec![MessageErrorOperation::Set; parsed.inner.set.len()],
            Some(MessageErrorOperation::RouteWhere),
        )?;
        let original_parsed = parsed.clone();
        let mut bindings = vec![
            VmCompileBinding::readonly("left", self.left_schema.clone())
                .with_sensitivity(self.left_sensitivity),
            VmCompileBinding::readonly("right", self.right_schema.clone())
                .with_sensitivity(self.right_sensitivity),
            VmCompileBinding::writeonly("output", self.output_schema.clone())
                .with_sensitivity(self.output_sensitivity.clone()),
        ];
        if let Some(binding) = self.runtime.branch_binding() {
            bindings.push(binding);
        }
        let local_namespaces = HashSet::from_iter([
            "left".to_string(),
            "right".to_string(),
            "output".to_string(),
            BRANCH_NAMESPACE.to_string(),
        ]);
        let (materialized_bindings, materialized_interest) =
            referenced_materialized_stream_bindings(
                &original_parsed,
                &local_namespaces,
                self.runtime.available_materialized_streams,
                self.runtime.current_branching,
            )?;
        bindings.extend(materialized_bindings);
        let (parsed, pending_lookup_calls) =
            rewrite_lookup_hash_map_program(&parsed, self.runtime.available_lookups)?;
        let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
            pending_lookup_calls,
            "output",
            &bindings,
            self.runtime.udfs,
        )?;
        if let Some(lookup_binding) = lookup_binding {
            bindings.push(lookup_binding);
        }
        let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            self.output_schema.clone(),
            self.output_sensitivity.clone(),
            bindings,
            self.runtime.compile_options(VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                ..VmCompileOptions::default()
            }),
        )
        .map_err(|error| {
            format!(
                "correlator '{}' TO output '{}' compile failed: {}",
                self.processor.as_str(),
                self.output_relay.as_str(),
                error.message
            )
        })?;
        Ok(CompiledCorrelatorOutputProgram {
            program: CompiledProgramWithMaterializedInterest {
                compiled: Arc::new(compiled),
                output_sensitivity: self.output_sensitivity,
                materialized_interest,
                output_namespace_input: OutputNamespaceInput::Uninitialized,
                lookup_hash_maps,
                error_sites,
            },
        })
    }
}

fn reorder_key_part(array: &VmTypedArray, row: usize) -> ReorderKeyPart {
    match array {
        VmTypedArray::UInt8(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::UInt64(array.value(row) as u64)
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::UInt16(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::UInt64(array.value(row) as u64)
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::UInt32(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::UInt64(array.value(row) as u64)
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::UInt64(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::UInt64(array.value(row))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Int8(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Int64(array.value(row) as i64)
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Int16(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Int64(array.value(row) as i64)
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Int32(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Int64(array.value(row) as i64)
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Int64(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Int64(array.value(row))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Float32(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Float64(OrderedFloat(array.value(row) as f64))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Float64(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Float64(OrderedFloat(array.value(row)))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Boolean(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Boolean(array.value(row))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Utf8(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Utf8(array.value(row).to_string())
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Datetime(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Datetime(array.value(row))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Generic(_) => ReorderKeyPart::Null,
        VmTypedArray::Uninitialized { .. } => ReorderKeyPart::Null,
    }
}

struct ReordererFlushContext<'a> {
    graph: &'a SharedActiveGraph,
    branch: &'a mut BranchRuntime,
    node_kind: &'a str,
    processor: &'a Identifier,
    error_policies: &'a ErrorPolicies,
    output_routes: &'a mut RelayProcessorOutputsNode,
    input_relays: &'a [Identifier],
}

async fn flush_branch_reorderer_output(
    context: ReordererFlushContext<'_>,
    output_buffer: &mut ReordererOutputBuffer,
    output_index: usize,
) {
    let graph = context.graph;
    let node_kind = context.node_kind;
    let processor = context.processor;
    let error_policies = context.error_policies;
    let output_routes = context.output_routes;
    let input_relays = context.input_relays;
    let branch = context.branch;
    output_routes.routes[output_index].clear_flush_deadline();

    if output_buffer.pending.is_empty() {
        return;
    }
    let Some(input_relay) = input_relays.first() else {
        output_routes.routes[output_index].clear_flush_deadline();
        return;
    };
    let mut pending = output_buffer.take_pending();
    pending.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then(left.arrival_sequence.cmp(&right.arrival_sequence))
    });
    let messages = pending
        .drain(..)
        .map(|entry| entry.message)
        .collect::<Vec<_>>();
    let input_schema = match relay_schema_for_runtime(&branch.runtime, &branch.domain, input_relay)
    {
        Ok(schema) => schema,
        Err(error) => {
            let message_error_policy = output_routes.routes[output_index]
                .message_error_policy
                .clone();
            for message in messages {
                branch
                    .runtime
                    .handle_message_error_with_policy(
                        &branch.domain,
                        node_kind,
                        processor,
                        &message_error_policy,
                        message,
                        MessageErrorFailure::new(
                            Some(&output_routes.routes[output_index].relay),
                            error.to_string(),
                            MessageErrorOperation::Finalize,
                        ),
                    )
                    .await;
            }
            output_routes.routes[output_index].clear_flush_deadline();
            return;
        }
    };
    let batch = match RelayRecordBatch::from_messages(input_schema, messages) {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                std::iter::empty::<&AckSet>(),
                format!(
                    "reorderer '{}' failed to build output batch: {}",
                    processor.as_str(),
                    error
                ),
            );
            output_routes.routes[output_index].clear_flush_deadline();
            return;
        }
    };
    if let Some(acks) = dispatch_processor_output(
        ProcessorOutputDispatchContext {
            graph,
            branch,
            node_kind,
            source_kind: ModelKind::Reorderer,
            processor,
            error_policies,
            input_relays,
            filter_source: ProcessorOutputFilterSource::InputRelays,
            resolved_materialized_state: None,
        },
        output_routes,
        batch,
        output_index,
    )
    .await
    {
        for ack in acks {
            ack.ack_success();
        }
    }
    output_routes.routes[output_index].clear_flush_deadline();
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorrelatorSide {
    Left,
    Right,
}

fn take_correlator_opposite_pending(
    state: &mut CorrelatorBranchState,
    incoming_side: CorrelatorSide,
) -> Vec<CorrelatorPendingMessage> {
    match incoming_side {
        CorrelatorSide::Left => std::mem::take(&mut state.pending_right),
        CorrelatorSide::Right => std::mem::take(&mut state.pending_left),
    }
}

fn restore_correlator_opposite_pending(
    state: &mut CorrelatorBranchState,
    incoming_side: CorrelatorSide,
    mut pending: Vec<CorrelatorPendingMessage>,
) {
    match incoming_side {
        CorrelatorSide::Left => {
            pending.extend(std::mem::take(&mut state.pending_right));
            state.pending_right = pending;
        }
        CorrelatorSide::Right => {
            pending.extend(std::mem::take(&mut state.pending_left));
            state.pending_left = pending;
        }
    }
}

fn store_correlator_unmatched_incoming(
    state: &mut CorrelatorBranchState,
    incoming_side: CorrelatorSide,
    incoming: CorrelatorPendingMessage,
    mut opposite_pending: Vec<CorrelatorPendingMessage>,
) {
    match incoming_side {
        CorrelatorSide::Left => {
            opposite_pending.extend(std::mem::take(&mut state.pending_right));
            state.pending_right = opposite_pending;
            state.pending_left.push(incoming);
        }
        CorrelatorSide::Right => {
            opposite_pending.extend(std::mem::take(&mut state.pending_left));
            state.pending_left = opposite_pending;
            state.pending_right.push(incoming);
        }
    }
}

async fn correlate_incoming_message(
    processor: &Identifier,
    program: &CompiledCorrelatorWhereProgram,
    incoming_side: CorrelatorSide,
    match_policy: CorrelatorMatchPolicy,
    state: &mut CorrelatorBranchState,
    incoming: CorrelatorPendingMessage,
    execution_now: Timestamp,
) -> Result<Option<(CorrelatorPendingMessage, CorrelatorPendingMessage)>, (String, Vec<AckSet>)> {
    let opposite_pending = take_correlator_opposite_pending(state, incoming_side);
    let mut evaluated = Vec::<(CorrelatorPendingMessage, bool)>::new();
    let mut pending_iter = opposite_pending.into_iter();

    while let Some(candidate) = pending_iter.next() {
        let (left, right) = match incoming_side {
            CorrelatorSide::Left => (&incoming, &candidate),
            CorrelatorSide::Right => (&candidate, &incoming),
        };
        let matched =
            match evaluate_correlator_where_match(processor, program, left, right, execution_now)
                .await
            {
                Ok(matched) => matched,
                Err(error) => {
                    let mut restore = evaluated
                        .into_iter()
                        .map(|(pending, _matched)| pending)
                        .collect::<Vec<_>>();
                    restore.extend(pending_iter);
                    restore_correlator_opposite_pending(state, incoming_side, restore);
                    return Err(error);
                }
            };
        evaluated.push((candidate, matched));
    }

    let mut matching = Vec::new();
    let mut remaining = Vec::new();
    for (pending, matched) in evaluated {
        if matched {
            matching.push(pending);
        } else {
            remaining.push(pending);
        }
    }

    if matching.is_empty() {
        store_correlator_unmatched_incoming(state, incoming_side, incoming, remaining);
        return Ok(None);
    }

    let selected_index = match match_policy {
        CorrelatorMatchPolicy::Earliest => 0,
        CorrelatorMatchPolicy::Latest => matching.len() - 1,
    };
    let selected = matching.remove(selected_index);
    for duplicate in matching {
        duplicate.message.acks.ack_success();
    }
    restore_correlator_opposite_pending(state, incoming_side, remaining);

    Ok(Some(match incoming_side {
        CorrelatorSide::Left => (incoming, selected),
        CorrelatorSide::Right => (selected, incoming),
    }))
}

async fn evaluate_correlator_where_match(
    processor: &Identifier,
    program: &CompiledCorrelatorWhereProgram,
    left: &CorrelatorPendingMessage,
    right: &CorrelatorPendingMessage,
    execution_now: Timestamp,
) -> Result<bool, (String, Vec<AckSet>)> {
    let acks = AckSet::merged([left.message.acks.attached(), right.message.acks.attached()]);
    let combined =
        correlator_input_row(&left.message.record, &right.message.record).map_err(|error| {
            (
                format!(
                    "correlator '{}' failed to build CORRELATE WHERE input batch: {}",
                    processor.as_str(),
                    error
                ),
                vec![acks.clone()],
            )
        })?;
    let keys = vec![left.message.key.clone()];
    let side_inputs = HashMap::default();
    let lookup_columns = HashMap::default();
    let input = project_vm_input_batch(
        &program.program.input_schema,
        &VmInputProjectionSources {
            carrier: combined.batch(),
            namespace_batches: &[],
            strict_namespaces: &["left", "right"],
            keys: &keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: None,
        },
    )
    .map_err(|error| {
        (
            format!(
                "correlator '{}' failed to project CORRELATE WHERE input batch: {}",
                processor.as_str(),
                error
            ),
            vec![acks.clone()],
        )
    })?;
    let result = execute_program_with_selection_in_context(
        &program.program,
        &input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| {
        (
            format!(
                "correlator '{}' failed to evaluate CORRELATE WHERE: {}",
                processor.as_str(),
                error
            ),
            vec![acks.clone()],
        )
    })?;
    Ok(!result.selected_rows.is_empty())
}

fn correlator_input_row(left: &RuntimeRow, right: &RuntimeRow) -> Result<RuntimeRow, String> {
    let mut fields = Vec::with_capacity(
        left.batch().schema().fields().len() + right.batch().schema().fields().len(),
    );
    let mut columns = Vec::with_capacity(fields.capacity());
    for (namespace, row) in [("left", left), ("right", right)] {
        for (index, field) in row.batch().schema().fields().iter().enumerate() {
            fields.push(StdArc::new(arrow_schema::Field::new(
                format!("{namespace}.{}", field.name()),
                field.data_type().clone(),
                field.is_nullable(),
            )));
            columns.push(row.batch().batch().column(index).slice(row.index(), 1));
        }
    }
    let schema = StdArc::new(arrow_schema::Schema::new(fields));
    let batch = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
        )
    } else {
        RecordBatch::try_new(schema.clone(), columns)
    }
    .map_err(|error| error.to_string())?;
    RuntimeRow::new(
        Arc::new(RuntimeRecordBatch::from_record_batch(schema, batch)?),
        0,
        correlator_output_metadata(left.metadata(), right.metadata()),
    )
}

fn correlator_output_metadata(
    left: &RuntimeRecordMetadata,
    right: &RuntimeRecordMetadata,
) -> RuntimeRecordMetadata {
    RuntimeRecordMetadata::from_ingested_at_watermarks(
        left.ingested_at_low_watermark()
            .min(right.ingested_at_low_watermark()),
        left.ingested_at_high_watermark()
            .max(right.ingested_at_high_watermark()),
    )
}

async fn evaluate_correlator_output_message(
    processor: &Identifier,
    program: &CompiledCorrelatorOutputProgram,
    key: Option<BranchKey>,
    combined: RuntimeRow,
    materialized_state: &HashMap<String, RuntimeValue>,
    acks: AckSet,
    execution_now: Timestamp,
) -> Result<Option<RelayMessage>, Box<PlannedMessageError>> {
    let source_message = RelayMessage {
        key: key.clone(),
        record: combined.clone(),
        acks,
    };
    let keys = vec![key];
    let lookup_columns = compute_lookup_hash_map_columns(
        &program.program,
        combined.batch(),
        &[],
        &keys,
        materialized_state,
        None,
        execution_now,
    )
    .await
    .map_err(|error| {
        Box::new(planned_structured_message_error(
            source_message.clone(),
            structured_message_error(
                MessageErrorCode::Evaluation,
                format!(
                    "correlator '{}' failed to prepare TO output lookup inputs: {}",
                    processor.as_str(),
                    error
                ),
                MessageErrorOperation::Set,
                None,
                std::iter::empty(),
            ),
            None,
            materialized_state.clone(),
        ))
    })?;
    let uninitialized = VmUninitializedInput {
        fields: program
            .program
            .compiled
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("output."))
            .map(|field| field.name().clone())
            .collect(),
    };
    let input = project_vm_input_batch(
        &program.program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: combined.batch(),
            namespace_batches: &[],
            strict_namespaces: &["left", "right"],
            keys: &keys,
            side_inputs: materialized_state,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized),
        },
    )
    .map_err(|error| {
        Box::new(planned_structured_message_error(
            source_message.clone(),
            structured_message_error(
                MessageErrorCode::Internal,
                format!(
                    "correlator '{}' failed to build TO output input batch: {}",
                    processor.as_str(),
                    error
                ),
                MessageErrorOperation::Set,
                None,
                std::iter::empty(),
            ),
            None,
            materialized_state.clone(),
        ))
    })?;
    let result = execute_program_with_selection_in_context(
        &program.program.compiled,
        &input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| {
        Box::new(planned_structured_message_error(
            source_message.clone(),
            structured_message_error(
                MessageErrorCode::Internal,
                format!(
                    "correlator '{}' failed to evaluate TO output: {}",
                    processor.as_str(),
                    error
                ),
                MessageErrorOperation::Set,
                None,
                std::iter::empty(),
            ),
            None,
            materialized_state.clone(),
        ))
    })?;
    if result.selected_rows.is_empty() {
        source_message.acks.ack_success();
        return Ok(None);
    }
    if result.selected_rows.len() != 1 || result.batch.row_count() != 1 {
        return Err(Box::new(planned_structured_message_error(
            source_message,
            structured_message_error(
                MessageErrorCode::Internal,
                format!(
                    "correlator '{}' TO output produced {} rows for one correlation",
                    processor.as_str(),
                    result.batch.row_count()
                ),
                MessageErrorOperation::Finalize,
                None,
                std::iter::empty(),
            ),
            None,
            HashMap::default(),
        )));
    }
    if let Some(side_error) = result.batch.errors().iter().flatten().next() {
        let partial_output = vm_partial_output_row_to_runtime_batch(&result.batch, 0).ok();
        let materialized_state = materialized_state.clone();
        return Err(Box::new(planned_structured_message_error(
            source_message,
            program.program.structured_side_error(
                format!(
                    "correlator '{}' TO output side error {}: {} at {}",
                    processor.as_str(),
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ),
                side_error.span,
                MessageErrorOperation::Set,
            ),
            partial_output,
            materialized_state,
        )));
    }
    let RelayMessage { key, acks, .. } = source_message;
    let output =
        vm_typed_batch_selected_rows_to_runtime_batch(&result.batch, &[0]).map_err(|error| {
            Box::new(planned_structured_message_error(
                RelayMessage {
                    key: key.clone(),
                    record: combined.clone(),
                    acks: acks.clone(),
                },
                structured_message_error(
                    MessageErrorCode::Validation,
                    format!(
                        "correlator '{}' failed to finalize TO output row: {}",
                        processor.as_str(),
                        error
                    ),
                    MessageErrorOperation::Finalize,
                    None,
                    invalid_output_fields(&result.batch, 0),
                ),
                vm_partial_output_row_to_runtime_batch(&result.batch, 0).ok(),
                materialized_state.clone(),
            ))
        })?;
    let record =
        RuntimeRow::new(Arc::new(output), 0, combined.metadata().clone()).map_err(|error| {
            Box::new(planned_structured_message_error(
                RelayMessage {
                    key: key.clone(),
                    record: combined,
                    acks: acks.clone(),
                },
                structured_message_error(
                    MessageErrorCode::Internal,
                    error,
                    MessageErrorOperation::Finalize,
                    None,
                    std::iter::empty(),
                ),
                None,
                materialized_state.clone(),
            ))
        })?;
    Ok(Some(RelayMessage { key, record, acks }))
}

struct CorrelatorOutputContext<'a> {
    graph: &'a SharedActiveGraph,
    branch: &'a mut BranchRuntime,
    node_kind: &'a str,
    processor: &'a Identifier,
    error_policies: &'a ErrorPolicies,
    output_routes: &'a mut RelayProcessorOutputsNode,
}

async fn enqueue_correlator_output(
    context: CorrelatorOutputContext<'_>,
    output_index: usize,
    messages: Vec<RelayMessage>,
    execution_now: Timestamp,
) {
    let CorrelatorOutputContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        output_routes,
    } = context;
    if messages.is_empty() {
        return;
    }
    let Some(output) = output_routes.routes.get_mut(output_index) else {
        for message in messages {
            message.acks.no_ack(format!(
                "correlator '{}' has no output destination at index {}",
                processor.as_str(),
                output_index
            ));
        }
        return;
    };
    let output_relay = output.relay.clone();
    let output_schema =
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, &output_relay) {
            Ok(schema) => schema,
            Err(error) => {
                let policy = output.message_error_policy.clone();
                for message in messages {
                    branch
                        .runtime
                        .handle_message_error_with_policy(
                            &branch.domain,
                            node_kind,
                            processor,
                            &policy,
                            message,
                            MessageErrorFailure::new(
                                Some(&output_relay),
                                error.to_string(),
                                MessageErrorOperation::Finalize,
                            ),
                        )
                        .await;
                }
                return;
            }
        };
    let batch = match build_stream_record_batch_preserving_acks(output_schema, messages) {
        Ok(batch) => batch,
        Err((error, acks)) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                acks.iter(),
                format!(
                    "correlator '{}' failed to build output batch: {}",
                    processor.as_str(),
                    error
                ),
            );
            return;
        }
    };
    if !output.enqueue(batch, execution_now) {
        return;
    }
    let pending = output.take_pending();
    let pending_acks = pending
        .iter()
        .flat_map(|batch| batch.acks.iter().cloned())
        .collect::<Vec<_>>();
    let forwarded = match RelayRecordBatch::concat(pending) {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                pending_acks.iter(),
                format!(
                    "correlator '{}' failed to concatenate output for relay '{}': {}",
                    processor.as_str(),
                    output_relay.as_str(),
                    error
                ),
            );
            return;
        }
    };
    if branch
        .dispatch_output(graph, output, ModelKind::Correlator, processor, &forwarded)
        .await
        .is_ok()
    {
        for ack in &forwarded.acks {
            ack.ack_success();
        }
    } else {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            format!(
                "correlator '{}' failed to forward output to relay '{}'",
                processor.as_str(),
                output_relay.as_str()
            ),
        );
    }
}

async fn handle_correlator_timeout_action(
    graph: &SharedActiveGraph,
    branch: &mut BranchRuntime,
    node_kind: &str,
    processor: &Identifier,
    error_policies: &ErrorPolicies,
    action: &CorrelationTimeoutAction,
    message: RelayMessage,
) {
    match action {
        CorrelationTimeoutAction::Drop => {
            message.acks.ack_success();
        }
        CorrelationTimeoutAction::SendTo { relay } => {
            let output = RelayProcessorOutputNode {
                relay: relay.clone(),
                construction: RouteConstruction {
                    inherit: Some(nervix_models::Inheritance::All),
                    ..RouteConstruction::default()
                },
                branch: None,
                flush_policy: None,
                message_error_policy: error_policies.message.clone(),
                pending: Vec::new(),
                next_flush: None,
                compiled_program: None,
                compiled_branch_program: None,
            };
            let output_schema =
                match relay_schema_for_runtime(&branch.runtime, &branch.domain, relay) {
                    Ok(schema) => schema,
                    Err(error) => {
                        branch
                            .runtime
                            .handle_message_error(
                                &branch.domain,
                                node_kind,
                                processor,
                                error_policies,
                                message,
                                MessageErrorFailure::publish(None, error.to_string()),
                            )
                            .await;
                        return;
                    }
                };
            let batch = match RelayRecordBatch::from_messages(output_schema, vec![message]) {
                Ok(batch) => batch,
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        std::iter::empty::<&AckSet>(),
                        format!(
                            "correlator '{}' failed to build timeout batch: {}",
                            processor.as_str(),
                            error
                        ),
                    );
                    return;
                }
            };
            if branch
                .dispatch_output(graph, &output, ModelKind::Correlator, processor, &batch)
                .await
                .is_ok()
            {
                for ack in batch.acks.iter() {
                    ack.ack_success();
                }
            } else {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    batch.acks.iter(),
                    format!(
                        "correlator '{}' failed to forward timeout message",
                        processor.as_str()
                    ),
                );
            }
        }
    }
}

fn compile_ingestor_filter_map_program(
    domain: &Domain,
    identifier: &Identifier,
    source: &IngestSource,
    construction: &RouteConstruction,
    schemas: RuntimeVmSchemaPair,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let parsed = lower_transforming_route(construction, &schemas.input, &schemas.output).map_err(
        |reason| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "ingestor output construction for '{}' is invalid: {reason}",
                identifier
            ),
        },
    )?;
    if !parsed.inner.branch_filters.is_empty() {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "FILTER-MAP for '{}' may contain at most one WHERE clause",
                identifier.as_str()
            ),
        });
    }
    let inherited_count = parsed
        .inner
        .set
        .len()
        .saturating_sub(construction.assignments.len());
    let set_operations = (0..parsed.inner.set.len())
        .map(|index| {
            if index < inherited_count {
                MessageErrorOperation::Inherit
            } else {
                MessageErrorOperation::Set
            }
        })
        .collect::<Vec<_>>();
    let error_sites = compiled_message_error_sites(
        &parsed,
        &set_operations,
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;

    let mut bindings = vec![
        VmCompileBinding::readonly("input", schemas.input.clone())
            .with_sensitivity(schemas.input_sensitivity),
        VmCompileBinding::writable("output", schemas.output.clone())
            .with_sensitivity(schemas.output_sensitivity.clone()),
    ];
    let writable_namespaces = HashSet::from_iter(["input".to_string(), "output".to_string()]);
    if let Some(metadata_schema) = ingestor_filter_map_metadata_arrow_schema(source) {
        bindings.push(VmCompileBinding::readonly(
            INGEST_METADATA_NAMESPACE,
            metadata_schema,
        ));
    }
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &parsed,
        &writable_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) =
        compile_lookup_hash_map_calls(pending_lookup_calls, "output", &bindings, context.udfs)
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }

    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        schemas.output,
        schemas.output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            allow_header_reads: ingest_source_supports_headers(source),
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity: schemas.output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    }))
}

/// The schema surface a generator's set-only route compiles against: the output it constructs, that
/// output's sensitivity, the materialized source it reads, and the branch it preserves.
struct GeneratorSetProgramSchemas {
    output: StdArc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    source: StdArc<arrow_schema::Schema>,
    branch: Option<StdArc<arrow_schema::Schema>>,
}

fn compile_generator_set_program(
    domain: &Domain,
    generator: &CreateGenerator,
    output: &ProcessorOutput,
    schemas: GeneratorSetProgramSchemas,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledProgramWithMaterializedInterest, RuntimeError> {
    let GeneratorSetProgramSchemas {
        output: output_schema,
        output_sensitivity,
        source: source_schema,
        branch: branch_schema,
    } = schemas;
    let parsed =
        lower_set_only_route(&output.construction, output_schema.as_ref()).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "generator '{}' output '{}' is invalid: {reason}",
                    generator.name, output.relay
                ),
            }
        })?;
    let error_sites = compiled_message_error_sites(
        &parsed,
        &vec![MessageErrorOperation::Set; parsed.inner.set.len()],
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    let mut bindings = vec![
        VmCompileBinding::writable("output", output_schema.clone())
            .with_sensitivity(output_sensitivity.clone()),
        VmCompileBinding::readonly(
            format!("relay_state.{}", generator.materialized_relay),
            source_schema,
        ),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(VmCompileBinding::readonly("branch", branch_schema));
    }
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
        runtime_udf_compile_options(
            udfs,
            VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                ..VmCompileOptions::default()
            },
        ),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "generator '{}' output '{}' compile failed: {}",
            generator.name, output.relay, error.message
        ),
    })?;
    Ok(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest: MaterializedProgramInterest::default(),
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps: Vec::new(),
        error_sites,
    })
}

fn ingestor_filter_map_metadata_arrow_schema(
    source: &IngestSource,
) -> Option<StdArc<arrow_schema::Schema>> {
    match source {
        IngestSource::Kafka { .. } => Some(StdArc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("topic", ArrowDataType::Utf8, true),
            arrow_schema::Field::new("partition", ArrowDataType::Int32, true),
            arrow_schema::Field::new("offset", ArrowDataType::Int64, true),
        ]))),
        _ => None,
    }
}

pub(crate) async fn execute_filter_map_on_record(
    owner: &Identifier,
    filter_map: &CompiledProgramWithMaterializedInterest,
    record: RuntimeRow,
    branch_key: Option<&BranchKey>,
    filter_map_metadata: Option<&IngestFilterMapMetadata>,
    side_inputs: &HashMap<String, RuntimeValue>,
    execution_now: Timestamp,
) -> Result<Option<RuntimeRow>, String> {
    let keys = vec![branch_key.cloned()];
    let metadata = vec![record.metadata().clone()];
    let carrier = record.one_row_batch();
    let outcome = evaluate_filter_map_on_batch(
        "subscription",
        owner,
        filter_map,
        FilterMapOutcomeInputs {
            carrier: &carrier,
            record_metadata: &metadata,
            keys: &keys,
            filter_map_metadata: filter_map_metadata.map(std::slice::from_ref),
            side_inputs,
        },
        execution_now,
    )
    .await?
    .into_iter()
    .next()
    .expect("filter-map returns one outcome per input record");
    match outcome {
        SingleRecordFilterMapOutcome::Filtered => Ok(None),
        SingleRecordFilterMapOutcome::Output(record) => Ok(Some(record)),
        SingleRecordFilterMapOutcome::MessageError { error, .. } => {
            Err(format!("FILTER-MAP message error: {}", error.message))
        }
    }
}

/// Runs one filter-map program over a whole group of records in a single VM execution
/// and returns one outcome per input record, in input row order.
///
/// The columnar VM already filters many rows at once: `ExecutionResult::selected_rows`
/// carries the input row index behind every surviving output row. Walking that mapping
/// is what keeps message-error attribution and ack identity tied to the record each
/// outcome came from, so a group never has to be evaluated a row at a time.
struct FilterMapOutcomeInputs<'a> {
    carrier: &'a RuntimeRecordBatch,
    record_metadata: &'a [RuntimeRecordMetadata],
    keys: &'a [Option<BranchKey>],
    filter_map_metadata: Option<&'a [IngestFilterMapMetadata]>,
    side_inputs: &'a HashMap<String, RuntimeValue>,
}

async fn evaluate_filter_map_on_batch(
    processor_kind: &str,
    processor: &Identifier,
    filter_map: &CompiledProgramWithMaterializedInterest,
    inputs: FilterMapOutcomeInputs<'_>,
    execution_now: Timestamp,
) -> Result<Vec<SingleRecordFilterMapOutcome>, String> {
    let FilterMapOutcomeInputs {
        carrier,
        record_metadata,
        keys,
        filter_map_metadata,
        side_inputs,
    } = inputs;
    let row_count = carrier.batch().num_rows();
    if row_count == 0 {
        return Ok(Vec::new());
    }
    if record_metadata.len() != row_count {
        return Err(format!(
            "FILTER-MAP received {} runtime metadata rows for {row_count} records",
            record_metadata.len()
        ));
    }
    if keys.len() != row_count {
        return Err(format!(
            "FILTER-MAP received {} branch keys for {row_count} records",
            keys.len()
        ));
    }
    if let Some(metadata) = filter_map_metadata
        && metadata.len() != row_count
    {
        return Err(format!(
            "FILTER-MAP received {} ingest metadata rows for {row_count} records",
            metadata.len()
        ));
    }
    let executed = execute_filter_map_program_on_batch(
        processor_kind,
        processor,
        filter_map,
        FilterMapBatchInputs {
            carrier,
            namespace_batches: &[],
            keys,
            side_inputs,
            ingest_metadata: filter_map_metadata,
        },
        execution_now,
        (0..row_count).map(|_| AckSet::empty()).collect(),
    )
    .await
    .map_err(|error| error.reason)?;
    if executed.selected_rows.len() != executed.batch.row_count() {
        return Err(format!(
            "FILTER-MAP produced {} rows for {} selected rows",
            executed.batch.row_count(),
            executed.selected_rows.len()
        ));
    }
    // Rows the program filtered out never appear in `selected_rows`, so starting every
    // record at `Filtered` and overwriting the survivors keeps the result row-aligned
    // with the input without a second pass over the predicate.
    let mut outcomes = (0..row_count)
        .map(|_| SingleRecordFilterMapOutcome::Filtered)
        .collect::<Vec<_>>();
    let state_snapshot = relay_state_snapshot_from_side_inputs(side_inputs);
    let mut successful_output_rows = Vec::new();
    let mut successful_input_rows = Vec::new();
    for (output_row, input_row) in executed.selected_rows.iter().copied().enumerate() {
        let (Some(slot), Some(metadata)) = (
            outcomes.get_mut(input_row),
            record_metadata.get(input_row).cloned(),
        ) else {
            return Err(format!(
                "FILTER-MAP selected row {input_row} outside its {row_count}-record input"
            ));
        };
        if let Some(side_error) = executed.batch.errors().row(output_row).first() {
            *slot = SingleRecordFilterMapOutcome::MessageError {
                error: filter_map.structured_side_error(
                    format!(
                        "FILTER-MAP side error {}: {} at {}",
                        side_error.code.as_str(),
                        side_error.message,
                        side_error.span
                    ),
                    side_error.span,
                    MessageErrorOperation::Set,
                ),
                partial_output: vm_partial_output_row_to_runtime_batch(&executed.batch, output_row)
                    .ok(),
                materialized_state: state_snapshot.clone(),
            };
            continue;
        }
        let _ = metadata;
        successful_output_rows.push(output_row);
        successful_input_rows.push(input_row);
    }
    if !successful_output_rows.is_empty() {
        let output_batch = Arc::new(vm_typed_batch_selected_rows_to_runtime_batch(
            &executed.batch,
            &successful_output_rows,
        )?);
        for (output_row, input_row) in successful_input_rows.into_iter().enumerate() {
            outcomes[input_row] = SingleRecordFilterMapOutcome::Output(RuntimeRow::new(
                output_batch.clone(),
                output_row,
                record_metadata[input_row].clone(),
            )?);
        }
    }
    Ok(outcomes)
}

#[derive(Debug, Clone, Copy)]
struct InferencerFilterMapTensors<'a> {
    output_schema: &'a [InferencerTensorDeclaration],
}

impl InferencerFilterMapTensors<'_> {
    fn output_arrow_schema(&self) -> StdArc<arrow_schema::Schema> {
        StdArc::new(arrow_schema::Schema::new(
            self.output_schema
                .iter()
                .map(|declaration| {
                    arrow_schema::Field::new(
                        &declaration.tensor,
                        crate::runtime_schema::arrow_data_type(&declaration.schema.message_type()),
                        false,
                    )
                })
                .collect::<Vec<_>>(),
        ))
    }
}

fn expression_reads_sensitive_source(
    expression: &nervix_models::Expression,
    sensitivity: &VmSchemaSensitivity,
) -> bool {
    match expression {
        nervix_models::Expression::Literal(_) => false,
        nervix_models::Expression::Field(reference) => {
            matches!(
                reference.scope,
                nervix_models::FieldScope::Bare | nervix_models::FieldScope::Input
            ) && sensitivity.is_sensitive(reference.field.as_str())
        }
        nervix_models::Expression::Unary { expression, .. }
        | nervix_models::Expression::Cast { expression, .. } => {
            expression_reads_sensitive_source(expression, sensitivity)
        }
        nervix_models::Expression::Binary { left, right, .. } => {
            expression_reads_sensitive_source(left, sensitivity)
                || expression_reads_sensitive_source(right, sensitivity)
        }
        nervix_models::Expression::Call {
            function,
            arguments,
        } => {
            !function.as_str().eq_ignore_ascii_case("leak_sensitive")
                && arguments
                    .iter()
                    .any(|argument| expression_reads_sensitive_source(argument, sensitivity))
        }
        nervix_models::Expression::UdfCall { arguments, .. } => arguments
            .iter()
            .any(|argument| expression_reads_sensitive_source(argument, sensitivity)),
        nervix_models::Expression::Array(items) => items
            .iter()
            .any(|item| expression_reads_sensitive_source(item, sensitivity)),
        nervix_models::Expression::If {
            condition,
            then_result,
            else_result,
        } => {
            expression_reads_sensitive_source(condition, sensitivity)
                || expression_reads_sensitive_source(then_result, sensitivity)
                || expression_reads_sensitive_source(else_result, sensitivity)
        }
        nervix_models::Expression::Case {
            operand,
            branches,
            else_result,
        } => {
            operand
                .as_deref()
                .is_some_and(|operand| expression_reads_sensitive_source(operand, sensitivity))
                || branches.iter().any(|branch| {
                    expression_reads_sensitive_source(&branch.when, sensitivity)
                        || expression_reads_sensitive_source(&branch.result, sensitivity)
                })
                || else_result.as_deref().is_some_and(|else_result| {
                    expression_reads_sensitive_source(else_result, sensitivity)
                })
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ProcessorOutputFilterSource<'a> {
    InputRelays,
    OutputRelay,
    Inferencer(InferencerFilterMapTensors<'a>),
}

impl ProcessorOutputFilterSource<'_> {
    fn relays(&self, input_relays: &[Identifier]) -> Vec<Identifier> {
        match self {
            Self::InputRelays | Self::OutputRelay | Self::Inferencer(_) => input_relays.to_vec(),
        }
    }

    fn inferencer_tensors(&self) -> Option<InferencerFilterMapTensors<'_>> {
        if let Self::Inferencer(tensors) = self {
            Some(*tensors)
        } else {
            None
        }
    }
}

struct ProcessorOutputDispatchContext<'a> {
    graph: &'a SharedActiveGraph,
    branch: &'a mut BranchRuntime,
    node_kind: &'a str,
    source_kind: ModelKind,
    processor: &'a Identifier,
    error_policies: &'a ErrorPolicies,
    input_relays: &'a [Identifier],
    filter_source: ProcessorOutputFilterSource<'a>,
    resolved_materialized_state: Option<&'a HashMap<String, RuntimeValue>>,
}

struct PendingProcessorOutputMessage {
    row: usize,
    output_index: usize,
    key: Option<BranchKey>,
    record: RuntimeRow,
}

struct PendingProcessorOutputBatch {
    output_index: usize,
    input_rows: Vec<usize>,
    key: Option<BranchKey>,
    batch: RuntimeRecordBatch,
    metadata: Vec<RuntimeRecordMetadata>,
}

impl PendingProcessorOutputBatch {
    fn into_relay_batch(self, acks: Vec<AckSet>) -> Result<RelayRecordBatch, String> {
        RelayRecordBatch::from_filtered_parts(self.key, self.batch, self.metadata, acks)
    }
}

fn pending_output_batches_by_key(
    output_index: usize,
    input_rows: &[usize],
    keys: Vec<Option<BranchKey>>,
    batch: RuntimeRecordBatch,
    metadata: &[RuntimeRecordMetadata],
) -> Result<Vec<PendingProcessorOutputBatch>, String> {
    if input_rows.len() != keys.len()
        || input_rows.len() != batch.batch().num_rows()
        || input_rows.len() != metadata.len()
    {
        return Err(format!(
            "pending output has {} input rows, {} keys, {} Arrow rows, and {} metadata rows",
            input_rows.len(),
            keys.len(),
            batch.batch().num_rows(),
            metadata.len()
        ));
    }
    let mut groups = Vec::<(Option<BranchKey>, Vec<usize>)>::new();
    let mut positions = HashMap::<Option<BranchKey>, usize>::default();
    for (row, key) in keys.into_iter().enumerate() {
        if let Some(position) = positions.get(&key).copied() {
            groups[position].1.push(row);
        } else {
            positions.insert(key.clone(), groups.len());
            groups.push((key, vec![row]));
        }
    }
    groups
        .into_iter()
        .map(|(key, rows)| {
            Ok(PendingProcessorOutputBatch {
                output_index,
                input_rows: rows.iter().map(|row| input_rows[*row]).collect(),
                key,
                batch: batch.take(&rows)?,
                metadata: rows.iter().map(|row| metadata[*row].clone()).collect(),
            })
        })
        .collect()
}

struct PendingProcessorOutputMessageError {
    row: usize,
    key: Option<BranchKey>,
    record: RuntimeRow,
    error: StructuredMessageError,
    partial_output: Option<RuntimeRecordBatch>,
    materialized_state: HashMap<String, RuntimeValue>,
}

fn processor_output_input_sensitivity(
    branch: &BranchRuntime,
    relays: &[Identifier],
) -> VmSchemaSensitivity {
    relays
        .first()
        .and_then(|relay| relay_schema_for_runtime(&branch.runtime, &branch.domain, relay).ok())
        .map(|schema| schema.vm_sensitivity())
        .unwrap_or_default()
}

async fn evaluate_processor_output_events(
    context: &mut ProcessorOutputDispatchContext<'_>,
    output: &mut RelayProcessorOutputNode,
    output_index: usize,
    batch: &RelayRecordBatch,
) -> Result<
    (
        Vec<PendingProcessorOutputMessage>,
        Vec<PendingProcessorOutputBatch>,
        Vec<PendingProcessorOutputMessageError>,
    ),
    PlannedGeneralError,
> {
    let input_relays = context.filter_source.relays(context.input_relays);
    if output.compiled_program.is_none() {
        let materialized_stream_specs = materialized_stream_specs_for_graph(
            &context.branch.runtime,
            &context.branch.domain,
            context.graph,
        );
        let current_branching = input_relays
            .first()
            .and_then(|relay| {
                context
                    .branch
                    .runtime
                    .executions
                    .get(&context.branch.domain)
                    .and_then(|execution| execution.relay_branchings.get(relay).cloned())
            })
            .unwrap_or_default();
        let current_branch_schema = input_relays.first().and_then(|relay| {
            relay_branch_schema_for_runtime(&context.branch.runtime, &context.branch.domain, relay)
        });
        let available_lookups = context
            .branch
            .runtime
            .executions
            .get(&context.branch.domain)
            .map(|execution| execution.lookups.clone())
            .unwrap_or_default();
        let udfs = context
            .branch
            .runtime
            .executions
            .get(&context.branch.domain)
            .map(|execution| execution.udfs.clone());
        let output_schema = match relay_schema_for_runtime(
            &context.branch.runtime,
            &context.branch.domain,
            &output.relay,
        ) {
            Ok(schema) => schema,
            Err(error) => {
                return Err(PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: error.to_string(),
                });
            }
        };
        let input_sensitivity = processor_output_input_sensitivity(context.branch, &input_relays);
        let compile_context = RuntimeVmCompileContext {
            available_materialized_streams: &materialized_stream_specs,
            available_lookups: &available_lookups,
            current_branching: &current_branching,
            current_branch_schema: current_branch_schema.as_ref(),
            current_branch_sensitivity: None,
            udfs: udfs.as_ref(),
        };
        let compiled = match context.filter_source {
            ProcessorOutputFilterSource::OutputRelay => compile_finalized_output_filter_program(
                &context.branch.domain,
                context.processor,
                output.construction.where_clause.as_ref(),
                output_schema.arrow_schema(),
                output_schema.vm_sensitivity(),
                compile_context,
            ),
            ProcessorOutputFilterSource::InputRelays
            | ProcessorOutputFilterSource::Inferencer(_) => {
                compile_processor_output_filter_map_program(
                    RuntimeCompileTarget {
                        domain: &context.branch.domain,
                        identifier: context.processor,
                    },
                    &input_relays,
                    &output.relay,
                    &output.construction,
                    RuntimeVmSchemaPair {
                        input: batch.arrow_schema(),
                        input_sensitivity,
                        output: output_schema.arrow_schema(),
                        output_sensitivity: output_schema.vm_sensitivity(),
                    },
                    context.filter_source.inferencer_tensors(),
                    compile_context,
                )
            }
        };
        match compiled {
            Ok(program) => output.compiled_program = program,
            Err(error) => {
                return Err(PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: error.to_string(),
                });
            }
        }
    }

    let Some(program) = output.compiled_program.as_ref() else {
        let output_schema = relay_schema_for_runtime(
            &context.branch.runtime,
            &context.branch.domain,
            &output.relay,
        )
        .map_err(|error| PlannedGeneralError {
            acks: batch.acks.clone(),
            reason: error.to_string(),
        })?;
        let projected = batch
            .batch
            .project(output_schema.arrow_schema())
            .map_err(|error| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "{} '{}' failed to project output relay '{}': {}",
                    context.node_kind,
                    context.processor.as_str(),
                    output.relay.as_str(),
                    error
                ),
            })?;
        return Ok((
            Vec::new(),
            vec![PendingProcessorOutputBatch {
                output_index,
                input_rows: (0..projected.batch().num_rows()).collect(),
                key: batch.key.clone(),
                batch: projected,
                metadata: batch.metadata.clone(),
            }],
            Vec::new(),
        ));
    };

    let execution_now = context
        .branch
        .runtime
        .current_stream_expiration_time(&context.branch.domain)
        .ok()
        .flatten()
        .unwrap_or_else(current_timestamp);
    let side_inputs = if let Some(resolved) = context.resolved_materialized_state {
        resolved.clone()
    } else {
        let owner_nodes = context
            .branch
            .runtime
            .executions
            .get(&context.branch.domain)
            .map(|execution| execution.materialized_stream_owner_nodes.clone())
            .unwrap_or_default();
        context
            .branch
            .runtime
            .load_materialized_side_inputs(
                &context.branch.domain,
                &batch.key,
                &program.materialized_interest,
                &owner_nodes,
            )
            .await
            .map_err(|error| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "{} '{}' failed to load materialized side inputs: {}",
                    context.node_kind,
                    context.processor.as_str(),
                    error
                ),
            })?
    };
    let executed = execute_filter_map_program_on_batch(
        context.node_kind,
        context.processor,
        program,
        FilterMapBatchInputs {
            carrier: &batch.batch,
            namespace_batches: &[],
            keys: &batch.keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
        },
        execution_now,
        batch.acks.clone(),
    )
    .await?;
    let state_snapshot = relay_state_snapshot_from_side_inputs(&side_inputs);
    let output_schema = match relay_schema_for_runtime(
        &context.branch.runtime,
        &context.branch.domain,
        &output.relay,
    ) {
        Ok(schema) => schema,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: error.to_string(),
            });
        }
    };
    let mut success_output_rows = Vec::new();
    let mut success_input_rows = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, &input_row) in executed.selected_rows.iter().enumerate() {
        if let Some(side_error) = executed.batch.errors().row(output_row).first() {
            let partial_output =
                vm_partial_output_row_to_runtime_batch(&executed.batch, output_row).ok();
            let record = batch
                .runtime_row(input_row)
                .map_err(|error| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "{} '{}' failed to materialize FILTER-MAP error input row: {}",
                        context.node_kind,
                        context.processor.as_str(),
                        error
                    ),
                })?;
            message_errors.push(PendingProcessorOutputMessageError {
                row: input_row,
                key: batch.keys[input_row].clone(),
                record,
                error: program.structured_side_error(
                    format!(
                        "{} '{}' FILTER-MAP side error {}: {} at {}",
                        context.node_kind,
                        context.processor.as_str(),
                        side_error.code.as_str(),
                        side_error.message,
                        side_error.span
                    ),
                    side_error.span,
                    MessageErrorOperation::Set,
                ),
                partial_output,
                materialized_state: state_snapshot.clone(),
            });
            continue;
        }
        success_output_rows.push(output_row);
        success_input_rows.push(input_row);
    }
    let output_batches = if success_output_rows.is_empty() {
        Vec::new()
    } else {
        let output_batch =
            vm_typed_batch_selected_rows_to_runtime_batch(&executed.batch, &success_output_rows)
                .map_err(|error| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "{} '{}' failed to materialize successful FILTER-MAP rows: {}",
                        context.node_kind,
                        context.processor.as_str(),
                        error
                    ),
                })?;
        if output_batch.schema().as_ref() != output_schema.arrow_schema().as_ref() {
            return Err(PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "{} '{}' FILTER-MAP output schema does not match relay '{}'",
                    context.node_kind,
                    context.processor.as_str(),
                    output.relay.as_str()
                ),
            });
        }
        let metadata = success_input_rows
            .iter()
            .map(|input_row| batch.metadata[*input_row].clone())
            .collect::<Vec<_>>();
        vec![PendingProcessorOutputBatch {
            output_index,
            input_rows: success_input_rows,
            key: batch.key.clone(),
            batch: output_batch,
            metadata,
        }]
    };
    Ok((Vec::new(), output_batches, message_errors))
}

async fn dispatch_processor_outputs(
    context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    batch: RelayRecordBatch,
) -> Option<Vec<AckSet>> {
    dispatch_selected_processor_outputs(context, outputs, batch, None, false).await
}

async fn dispatch_processor_output(
    context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    batch: RelayRecordBatch,
    output_index: usize,
) -> Option<Vec<AckSet>> {
    dispatch_selected_processor_outputs(context, outputs, batch, Some(output_index), true).await
}

async fn dispatch_selected_processor_outputs(
    mut context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    batch: RelayRecordBatch,
    selected_output: Option<usize>,
    flush_selected_immediately: bool,
) -> Option<Vec<AckSet>> {
    if batch.message_count() == 0 {
        return Some(Vec::new());
    }

    let output_relays = outputs
        .routes
        .iter()
        .map(|output| output.relay.clone())
        .collect::<Vec<_>>();

    let mut pending_messages = Vec::new();
    let mut pending_batches = Vec::new();
    let mut pending_errors = Vec::new();
    for (output_index, output) in outputs.routes.iter_mut().enumerate() {
        if selected_output.is_some_and(|selected| selected != output_index) {
            continue;
        }
        let (messages, batches, errors) = match evaluate_processor_output_events(
            &mut context,
            output,
            output_index,
            &batch,
        )
        .await
        {
            Ok(events) => events,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        error.acks.iter(),
                        error.reason,
                    );
                return None;
            }
        };
        pending_messages.extend(messages);
        pending_batches.extend(batches);
        pending_errors.extend(errors.into_iter().map(|error| (output_index, error)));
    }

    let mut delivery_counts = vec![0usize; batch.acks.len()];
    for message in &pending_messages {
        delivery_counts[message.row] += 1;
    }
    for pending_batch in &pending_batches {
        for row in &pending_batch.input_rows {
            delivery_counts[*row] += 1;
        }
    }
    for (_, error) in &pending_errors {
        delivery_counts[error.row] += 1;
    }

    let RelayRecordBatch { acks, .. } = batch;
    let mut ack_queues = Vec::with_capacity(delivery_counts.len());
    for (row, ack) in acks.into_iter().enumerate() {
        let delivery_count = delivery_counts[row];
        if delivery_count == 0 {
            ack.ack_success();
            ack_queues.push(VecDeque::new());
            continue;
        }
        let mut queue = VecDeque::with_capacity(delivery_count);
        for _ in 1..delivery_count {
            queue.push_back(ack.attached());
        }
        queue.push_front(ack);
        ack_queues.push(queue);
    }

    let mut messages_by_output = vec![Vec::new(); output_relays.len()];
    let mut batches_by_output = vec![Vec::new(); output_relays.len()];
    for message in pending_messages {
        let Some(acks) = ack_queues[message.row].pop_front() else {
            continue;
        };
        messages_by_output[message.output_index].push(RelayMessage {
            key: message.key,
            record: message.record,
            acks,
        });
    }
    for pending_batch in pending_batches {
        let mut batch_acks = Vec::with_capacity(pending_batch.input_rows.len());
        for row in &pending_batch.input_rows {
            let Some(acks) = ack_queues[*row].pop_front() else {
                continue;
            };
            batch_acks.push(acks);
        }
        if batch_acks.len() != pending_batch.input_rows.len() {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    batch_acks.iter(),
                    "processor output batch ack count does not match selected row count"
                        .to_string(),
                );
            return None;
        }
        let output_index = pending_batch.output_index;
        let error_acks = batch_acks.clone();
        match pending_batch.into_relay_batch(batch_acks) {
            Ok(batch) => batches_by_output[output_index].push(batch),
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        error_acks.iter(),
                        error,
                    );
                return None;
            }
        }
    }

    for (output_index, error) in pending_errors {
        let Some(acks) = ack_queues[error.row].pop_front() else {
            continue;
        };
        context
            .branch
            .runtime
            .handle_structured_message_error(MessageErrorHandling {
                domain: &context.branch.domain,
                node_kind: context.node_kind,
                node: context.processor,
                source_route: Some(&outputs.routes[output_index].relay),
                policy: &outputs.routes[output_index].message_error_policy,
                message: RelayMessage {
                    key: error.key,
                    record: error.record,
                    acks,
                },
                error: error.error,
                partial_output: error.partial_output,
                materialized_state: error.materialized_state,
                ingest_metadata: None,
            })
            .await;
    }

    let execution_now = context
        .branch
        .runtime
        .current_stream_expiration_time(&context.branch.domain)
        .ok()
        .flatten()
        .unwrap_or_else(current_timestamp);
    let mut dispatched_acks = Vec::new();
    for (output_index, (messages, mut batches)) in messages_by_output
        .into_iter()
        .zip(batches_by_output)
        .enumerate()
    {
        let output = &mut outputs.routes[output_index];
        let relay = &output_relays[output_index];
        if !messages.is_empty() {
            let output_schema = match relay_schema_for_runtime(
                &context.branch.runtime,
                &context.branch.domain,
                relay,
            ) {
                Ok(schema) => schema,
                Err(error) => {
                    let message_error_policy = output.message_error_policy.clone();
                    for message in messages {
                        context
                            .branch
                            .runtime
                            .handle_message_error_with_policy(
                                &context.branch.domain,
                                context.node_kind,
                                context.processor,
                                &message_error_policy,
                                message,
                                MessageErrorFailure::new(
                                    Some(relay),
                                    error.to_string(),
                                    MessageErrorOperation::Finalize,
                                ),
                            )
                            .await;
                    }
                    return None;
                }
            };
            match build_stream_record_batch_preserving_acks(output_schema, messages) {
                Ok(batch) => batches.push(batch),
                Err((error, acks)) => {
                    context
                        .branch
                        .runtime
                        .handle_internal_processor_error_for_acks(
                            &context.branch.domain,
                            context.node_kind,
                            context.processor,
                            context.error_policies,
                            acks.iter(),
                            format!(
                                "{} '{}' failed to build output batch for relay '{}': {}",
                                context.node_kind,
                                context.processor.as_str(),
                                relay.as_str(),
                                error
                            ),
                        );
                    return None;
                }
            }
        }
        if batches.is_empty() {
            continue;
        }
        let mut should_flush = false;
        for batch in batches.drain(..) {
            should_flush |= output.enqueue(batch, execution_now);
        }
        if flush_selected_immediately
            && selected_output.is_some_and(|selected| selected == output_index)
        {
            output.force_flush_at(execution_now);
            should_flush = true;
        }
        if !should_flush {
            continue;
        }
        let pending = output.take_pending();
        let pending_acks = pending
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        let forwarded = match RelayRecordBatch::concat(pending) {
            Ok(batch) => batch,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        pending_acks.iter(),
                        format!(
                            "{} '{}' failed to concat output batches for relay '{}': {}",
                            context.node_kind,
                            context.processor.as_str(),
                            relay.as_str(),
                            error
                        ),
                    );
                return None;
            }
        };
        if context
            .branch
            .dispatch_output(
                context.graph,
                output,
                context.source_kind,
                context.processor,
                &forwarded,
            )
            .await
            .is_ok()
        {
            dispatched_acks.extend(forwarded.acks.iter().cloned());
        } else {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    forwarded.acks.iter(),
                    format!(
                        "{} '{}' failed to forward message to relay '{}'",
                        context.node_kind,
                        context.processor.as_str(),
                        relay.as_str()
                    ),
                );
            return None;
        }
    }
    Some(dispatched_acks)
}

async fn flush_due_processor_outputs(
    context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    now: Timestamp,
) {
    for output in &mut outputs.routes {
        if !output.flush_due(now) {
            continue;
        }
        let pending = output.take_pending();
        let pending_acks = pending
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        let forwarded = match RelayRecordBatch::concat(pending) {
            Ok(batch) => batch,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        pending_acks.iter(),
                        format!(
                            "{} '{}' failed to concat buffered output batches for relay '{}': {}",
                            context.node_kind,
                            context.processor.as_str(),
                            output.relay.as_str(),
                            error
                        ),
                    );
                continue;
            }
        };
        if context
            .branch
            .dispatch_output(
                context.graph,
                output,
                context.source_kind,
                context.processor,
                &forwarded,
            )
            .await
            .is_ok()
        {
            for ack in &forwarded.acks {
                ack.ack_success();
            }
        } else {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    forwarded.acks.iter(),
                    format!(
                        "{} '{}' failed to forward buffered output to relay '{}'",
                        context.node_kind,
                        context.processor.as_str(),
                        output.relay.as_str()
                    ),
                );
        }
    }
}

async fn plan_filter_map_messages(
    processor_kind: &str,
    processor: &Identifier,
    program_label: &str,
    program: &CompiledProgramWithMaterializedInterest,
    mut batch: RelayRecordBatch,
    execution_now: Timestamp,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> Result<FilterMapPlan, PlannedGeneralError> {
    let lookup_columns = match compute_lookup_hash_map_columns(
        program,
        &batch.batch,
        &[],
        &batch.keys,
        side_inputs,
        None,
        execution_now,
    )
    .await
    {
        Ok(columns) => columns,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks: batch.acks,
                reason: format!(
                    "{} '{}' failed to prepare LOOKUP_HASH_MAP inputs: {}",
                    processor_kind,
                    processor.as_str(),
                    error
                ),
            });
        }
    };
    let uninitialized = VmUninitializedInput {
        fields: program
            .compiled
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("output."))
            .map(|field| field.name().clone())
            .collect(),
    };
    let vm_batch = match project_vm_input_batch(
        &program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: &batch.batch,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &batch.keys,
            side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized),
        },
    ) {
        Ok(vm_batch) => vm_batch,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks: batch.acks,
                reason: format!(
                    "{} '{}' failed to prepare {} input batch: {}",
                    processor_kind,
                    processor.as_str(),
                    program_label,
                    error
                ),
            });
        }
    };
    let key = batch.key.clone();
    let keys = batch.keys.clone();
    let metadata = batch.metadata.clone();
    let mut acks = std::mem::take(&mut batch.acks);
    let state_snapshot = relay_state_snapshot_from_side_inputs(side_inputs);
    let result = match execute_program_with_selection_in_context(
        &program.compiled,
        &vm_batch,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks,
                reason: format!(
                    "{} '{}' {} execution failed: {}",
                    processor_kind,
                    processor.as_str(),
                    program_label,
                    error
                ),
            });
        }
    };

    let mut selected_rows = vec![false; acks.len()];
    for &row in &result.selected_rows {
        if row < selected_rows.len() {
            selected_rows[row] = true;
        }
    }
    for (row, selected) in selected_rows.iter().enumerate() {
        if !selected {
            acks[row].ack_success();
        }
    }

    let mut success_output_rows = Vec::new();
    let mut success_input_rows = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, &input_row) in result.selected_rows.iter().enumerate() {
        if let Some(side_error) = result.batch.errors().row(output_row).first() {
            let partial_output = if program.captures_partial_output() {
                Some(vm_partial_output_row_to_runtime_batch(
                    &result.batch,
                    output_row,
                ))
            } else {
                None
            };
            let partial_output_failure = partial_output
                .as_ref()
                .and_then(|partial_output| partial_output.as_ref().err());
            let reason = format!(
                "{} '{}' {} side error {}: {} at {}",
                processor_kind,
                processor.as_str(),
                program_label,
                side_error.code.as_str(),
                side_error.message,
                side_error.span
            );
            let reason = if let Some(partial_output_failure) = partial_output_failure {
                format!("{reason}; failed to capture partial output: {partial_output_failure}")
            } else {
                reason
            };
            let record = batch
                .runtime_row(input_row)
                .map_err(|error| PlannedGeneralError {
                    acks: acks.clone(),
                    reason: format!(
                        "{} '{}' failed to materialize {} error input row: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        error
                    ),
                })?;
            message_errors.push(planned_structured_message_error(
                RelayMessage {
                    key: keys[input_row].clone(),
                    record,
                    acks: std::mem::take(&mut acks[input_row]),
                },
                program.structured_side_error(
                    reason,
                    side_error.span,
                    operation_for_filter_label(program_label),
                ),
                partial_output.and_then(Result::ok),
                state_snapshot.clone(),
            ));
            continue;
        }
        let invalid_fields = invalid_output_fields(&result.batch, output_row);
        if !invalid_fields.is_empty() {
            let record =
                batch
                    .runtime_row(input_row)
                    .map_err(|decode_error| PlannedGeneralError {
                        acks: acks.clone(),
                        reason: format!(
                            "{} '{}' failed to materialize {} error input row: {}",
                            processor_kind,
                            processor.as_str(),
                            program_label,
                            decode_error
                        ),
                    })?;
            message_errors.push(planned_structured_message_error(
                RelayMessage {
                    key: keys[input_row].clone(),
                    record,
                    acks: std::mem::take(&mut acks[input_row]),
                },
                structured_message_error(
                    MessageErrorCode::Evaluation,
                    format!(
                        "{} '{}' failed to materialize {} output row: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        "required output fields are uninitialized or null"
                    ),
                    operation_for_filter_label(program_label),
                    None,
                    invalid_fields,
                ),
                None,
                state_snapshot.clone(),
            ));
            continue;
        }
        success_output_rows.push(output_row);
        success_input_rows.push(input_row);
    }

    let batch = if success_output_rows.is_empty() {
        None
    } else {
        let output_batch =
            vm_typed_batch_selected_rows_to_runtime_batch(&result.batch, &success_output_rows)
                .map_err(|error| PlannedGeneralError {
                    acks: acks.clone(),
                    reason: format!(
                        "{} '{}' failed to materialize successful {} rows: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        error
                    ),
                })?;
        let output_metadata = success_input_rows
            .iter()
            .map(|input_row| metadata[*input_row].clone())
            .collect::<Vec<_>>();
        let output_acks = success_input_rows
            .iter()
            .map(|input_row| std::mem::take(&mut acks[*input_row]))
            .collect::<Vec<_>>();
        let error_acks = output_acks.clone();
        Some(
            RelayRecordBatch::from_filtered_parts(key, output_batch, output_metadata, output_acks)
                .map_err(|error| PlannedGeneralError {
                    acks: error_acks,
                    reason: format!(
                        "{} '{}' failed to build {} output batch: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        error
                    ),
                })?,
        )
    };

    Ok(FilterMapPlan {
        batch,
        message_errors,
    })
}

struct EmitterFilterMapPlan {
    batch: Option<RelayRecordBatch>,
    headers: Option<Vec<EmitterHeaders>>,
    source_rows: Vec<usize>,
    message_errors: Vec<PlannedMessageError>,
}

async fn plan_emitter_filter_map_batch(
    emitter: &Identifier,
    program: &CompiledEmitterFilterMapProgram,
    mut input: RelayRecordBatch,
    execution_now: Timestamp,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> Result<EmitterFilterMapPlan, PlannedGeneralError> {
    let acks = std::mem::take(&mut input.acks);
    let body_result = execute_filter_map_program_on_batch(
        "emitter",
        emitter,
        &program.body,
        FilterMapBatchInputs {
            carrier: &input.batch,
            namespace_batches: &[],
            keys: &input.keys,
            side_inputs,
            ingest_metadata: None,
        },
        execution_now,
        acks,
    )
    .await?;
    let mut acks = body_result.acks;
    let state_snapshot = relay_state_snapshot_from_side_inputs(side_inputs);

    let mut selected_rows = vec![false; acks.len()];
    for &row in &body_result.selected_rows {
        if row < selected_rows.len() {
            selected_rows[row] = true;
        }
    }
    for (row, selected) in selected_rows.iter().enumerate() {
        if !selected {
            acks[row].ack_success();
        }
    }

    let mut successful_output_rows = Vec::new();
    let mut successful_input_rows = Vec::new();
    let mut headers = (!body_result.invocations.is_empty()).then(Vec::new);
    let mut message_errors = Vec::new();
    for (output_row, &input_row) in body_result.selected_rows.iter().enumerate() {
        let source_record = |context: &str| {
            input
                .runtime_row(input_row)
                .map_err(|error| PlannedGeneralError {
                    acks: acks.clone(),
                    reason: format!(
                        "emitter '{}' failed to materialize {context} input row: {error}",
                        emitter.as_str()
                    ),
                })
        };
        if let Some(side_error) = body_result.batch.errors().row(output_row).first() {
            let source_record = source_record("FILTER-MAP error")?;
            let partial_output = program
                .codec_route
                .then(|| {
                    vm_partial_output_row_to_runtime_batch(&body_result.batch, output_row).ok()
                })
                .flatten();
            let reason = format!(
                "emitter '{}' FILTER-MAP side error {}: {} at {}",
                emitter.as_str(),
                side_error.code.as_str(),
                side_error.message,
                side_error.span
            );
            message_errors.push(planned_structured_message_error(
                RelayMessage {
                    key: input.keys[input_row].clone(),
                    record: source_record,
                    acks: std::mem::take(&mut acks[input_row]),
                },
                program.body.structured_side_error(
                    reason,
                    side_error.span,
                    if program.codec_route {
                        MessageErrorOperation::Set
                    } else {
                        MessageErrorOperation::Values
                    },
                ),
                partial_output,
                state_snapshot.clone(),
            ));
            continue;
        }
        let message_headers =
            match emitter_headers_from_invocations(&body_result.invocations, output_row) {
                Ok(headers) => headers,
                Err(error) => {
                    let source_record = source_record("FILTER-MAP header error")?;
                    let partial_output = program
                        .codec_route
                        .then(|| {
                            vm_partial_output_row_to_runtime_batch(&body_result.batch, output_row)
                                .ok()
                        })
                        .flatten();
                    message_errors.push(planned_structured_message_error(
                        RelayMessage {
                            key: input.keys[input_row].clone(),
                            record: source_record,
                            acks: std::mem::take(&mut acks[input_row]),
                        },
                        structured_message_error(
                            MessageErrorCode::Evaluation,
                            format!(
                                "emitter '{}' failed to materialize FILTER-MAP headers: {}",
                                emitter.as_str(),
                                error
                            ),
                            MessageErrorOperation::Invoke,
                            None,
                            std::iter::empty(),
                        ),
                        partial_output,
                        state_snapshot.clone(),
                    ));
                    continue;
                }
            };
        let invalid_fields = invalid_output_fields(&body_result.batch, output_row);
        if !invalid_fields.is_empty() {
            let source_record = source_record("FILTER-MAP validation error")?;
            let partial_output = program
                .codec_route
                .then(|| {
                    vm_partial_output_row_to_runtime_batch(&body_result.batch, output_row).ok()
                })
                .flatten();
            message_errors.push(planned_structured_message_error(
                RelayMessage {
                    key: input.keys[input_row].clone(),
                    record: source_record,
                    acks: std::mem::take(&mut acks[input_row]),
                },
                structured_message_error(
                    MessageErrorCode::Validation,
                    format!(
                        "emitter '{}' FILTER-MAP output row has uninitialized required fields",
                        emitter.as_str()
                    ),
                    if program.codec_route {
                        MessageErrorOperation::Finalize
                    } else {
                        MessageErrorOperation::Values
                    },
                    None,
                    invalid_fields,
                ),
                partial_output,
                state_snapshot.clone(),
            ));
            continue;
        }
        successful_output_rows.push(output_row);
        successful_input_rows.push(input_row);
        if let Some(headers) = &mut headers {
            headers.push(message_headers);
        }
    }

    let batch = if successful_output_rows.is_empty() {
        None
    } else {
        let output_batch = vm_typed_batch_selected_rows_to_runtime_batch(
            &body_result.batch,
            &successful_output_rows,
        )
        .map_err(|error| PlannedGeneralError {
            acks: acks.clone(),
            reason: format!(
                "emitter '{}' failed to finalize FILTER-MAP output batch: {error}",
                emitter.as_str()
            ),
        })?;
        let metadata = successful_input_rows
            .iter()
            .map(|input_row| input.metadata[*input_row].clone())
            .collect::<Vec<_>>();
        let output_acks = successful_input_rows
            .iter()
            .map(|input_row| std::mem::take(&mut acks[*input_row]))
            .collect::<Vec<_>>();
        let error_acks = output_acks.clone();
        Some(
            RelayRecordBatch::from_filtered_parts(
                input.key.clone(),
                output_batch,
                metadata,
                output_acks,
            )
            .map_err(|error| PlannedGeneralError {
                acks: error_acks,
                reason: format!(
                    "emitter '{}' failed to build FILTER-MAP output batch: {error}",
                    emitter.as_str()
                ),
            })?,
        )
    };

    Ok(EmitterFilterMapPlan {
        batch,
        headers,
        source_rows: successful_input_rows,
        message_errors,
    })
}

pub(in crate::runtime) async fn evaluate_sqs_fifo_group_program(
    emitter: &Identifier,
    program: &CompiledProgramWithMaterializedInterest,
    batch: &RelayRecordBatch,
    execution_now: Timestamp,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> Result<Vec<Result<Option<String>, String>>, PlannedGeneralError> {
    let row_count = batch.batch.batch().num_rows();
    let result = execute_filter_map_program_on_batch(
        "emitter",
        emitter,
        program,
        FilterMapBatchInputs {
            carrier: &batch.batch,
            namespace_batches: &[],
            keys: &batch.keys,
            side_inputs,
            ingest_metadata: None,
        },
        execution_now,
        batch.acks.clone(),
    )
    .await?;
    let mut groups = (0..row_count)
        .map(|_| Err("SQS FIFO GROUP expression omitted its input row".to_string()))
        .collect::<Vec<_>>();
    for (output_row, input_row) in result.selected_rows.into_iter().enumerate() {
        if input_row >= row_count {
            return Err(PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "emitter '{}' SQS FIFO GROUP expression referenced missing input row \
                     {input_row}",
                    emitter.as_str()
                ),
            });
        }
        if let Some(side_error) = result.batch.errors().row(output_row).first() {
            groups[input_row] = Err(format!(
                "SQS FIFO GROUP expression failed with {} at {}",
                side_error.code.as_str(),
                side_error.span
            ));
            continue;
        }
        groups[input_row] =
            match vm_output_value(&result.batch, output_row, "fifo_group").and_then(|value| {
                match value {
                    Some(RuntimeValue::String(value)) => Ok(value),
                    Some(value) => Err(format!(
                        "SQS FIFO GROUP expression produced {}, expected STRING",
                        runtime_value_type_name(&value)
                    )),
                    None => Err("SQS FIFO GROUP expression produced NULL".to_string()),
                }
            }) {
                Ok(value) => Ok(Some(value)),
                Err(reason) => Err(reason),
            };
    }
    Ok(groups)
}

struct ExecutedFilterMap {
    batch: VmTypedBatch,
    selected_rows: Vec<usize>,
    invocations: Vec<nervix_vm::FunctionInvocation>,
    acks: Vec<AckSet>,
}

struct VmUninitializedInput {
    fields: HashSet<String>,
}

impl VmUninitializedInput {
    fn contains(&self, field: &arrow_schema::Field) -> bool {
        self.fields.contains(field.name())
    }
}

async fn execute_prepared_filter_map(
    processor_kind: &str,
    processor: &Identifier,
    program: &CompiledProgramWithMaterializedInterest,
    vm_batch: VmTypedBatch,
    execution_now: Timestamp,
    acks: Vec<AckSet>,
    injector: Option<Arc<Box<dyn VmFunctionInjector>>>,
) -> Result<ExecutedFilterMap, PlannedGeneralError> {
    let result = match execute_program_with_selection_in_context(
        &program.compiled,
        &vm_batch,
        &VmExecutionContext {
            now: execution_now,
            injector,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks,
                reason: format!(
                    "{} '{}' FILTER-MAP execution failed: {}",
                    processor_kind,
                    processor.as_str(),
                    error
                ),
            });
        }
    };
    Ok(ExecutedFilterMap {
        batch: result.batch,
        selected_rows: result.selected_rows,
        invocations: result.invocations,
        acks,
    })
}

struct FilterMapBatchInputs<'a> {
    carrier: &'a RuntimeRecordBatch,
    namespace_batches: &'a [(&'a str, &'a RuntimeRecordBatch)],
    keys: &'a [Option<BranchKey>],
    side_inputs: &'a HashMap<String, RuntimeValue>,
    ingest_metadata: Option<&'a [IngestFilterMapMetadata]>,
}

async fn execute_filter_map_program_on_batch(
    processor_kind: &str,
    processor: &Identifier,
    program: &CompiledProgramWithMaterializedInterest,
    inputs: FilterMapBatchInputs<'_>,
    execution_now: Timestamp,
    acks: Vec<AckSet>,
) -> Result<ExecutedFilterMap, PlannedGeneralError> {
    let lookup_columns = match compute_lookup_hash_map_columns(
        program,
        inputs.carrier,
        inputs.namespace_batches,
        inputs.keys,
        inputs.side_inputs,
        inputs.ingest_metadata,
        execution_now,
    )
    .await
    {
        Ok(columns) => columns,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks,
                reason: format!(
                    "{} '{}' failed to prepare LOOKUP_HASH_MAP inputs: {}",
                    processor_kind,
                    processor.as_str(),
                    error
                ),
            });
        }
    };
    let uninitialized_fields = match program.output_namespace_input {
        OutputNamespaceInput::Uninitialized => program
            .compiled
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("output."))
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>(),
        OutputNamespaceInput::Finalized => HashSet::default(),
    };
    let uninitialized = (!uninitialized_fields.is_empty()).then_some(VmUninitializedInput {
        fields: uninitialized_fields,
    });
    let vm_batch = match project_vm_input_batch(
        &program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: inputs.carrier,
            namespace_batches: inputs.namespace_batches,
            strict_namespaces: &[],
            keys: inputs.keys,
            side_inputs: inputs.side_inputs,
            ingest_metadata: inputs.ingest_metadata,
            lookup_columns: &lookup_columns,
            uninitialized: uninitialized.as_ref(),
        },
    ) {
        Ok(vm_batch) => vm_batch,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks,
                reason: format!(
                    "{} '{}' failed to prepare FILTER-MAP input batch: {}",
                    processor_kind,
                    processor.as_str(),
                    error
                ),
            });
        }
    };
    execute_prepared_filter_map(
        processor_kind,
        processor,
        program,
        vm_batch,
        execution_now,
        acks,
        inputs.ingest_metadata.map(|metadata| {
            IngestHeaderFunctionInjector::from_metadata(
                Some(metadata),
                inputs.carrier.batch().num_rows(),
            )
        }),
    )
    .await
}

async fn evaluate_output_branch_program(
    owner: &Identifier,
    program: &CompiledBranchProgram,
    input: &RuntimeRecordBatch,
    output: &RuntimeRecordBatch,
    keys: &[Option<BranchKey>],
    side_inputs: &HashMap<String, RuntimeValue>,
    execution_now: Timestamp,
) -> Result<Vec<Result<Option<BranchKey>, String>>, String> {
    let row_count = output.batch().num_rows();
    if input.batch().num_rows() != row_count || keys.len() != row_count {
        return Err(format!(
            "branch construction for '{}' received {} input rows, {} output rows, and {} keys",
            owner.as_str(),
            input.batch().num_rows(),
            row_count,
            keys.len()
        ));
    }
    let namespace_batches = [("input", input), ("output", output), ("message", output)];
    let lookup_columns = compute_lookup_hash_map_columns(
        &program.program,
        output,
        &namespace_batches,
        keys,
        side_inputs,
        None,
        execution_now,
    )
    .await?;
    let uninitialized = VmUninitializedInput {
        fields: program
            .program
            .compiled
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("branch."))
            .map(|field| field.name().clone())
            .collect(),
    };
    let vm_input = project_vm_input_batch(
        &program.program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: output,
            namespace_batches: &namespace_batches,
            strict_namespaces: &["input", "output", "message"],
            keys,
            side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized),
        },
    )?;
    let result = execute_program_with_selection_in_context(
        &program.program.compiled,
        &vm_input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| {
        format!(
            "branch construction VM for '{}' failed: {}",
            owner.as_str(),
            error
        )
    })?;
    let mut outcomes = (0..row_count)
        .map(|_| Err("branch construction VM did not preserve the input row".to_string()))
        .collect::<Vec<_>>();
    for (output_row, input_row) in result.selected_rows.iter().copied().enumerate() {
        if input_row >= outcomes.len() {
            return Err(format!(
                "branch construction VM for '{}' selected unknown row {}",
                owner.as_str(),
                input_row
            ));
        }
        if let Some(error) = result.batch.errors().row(output_row).first() {
            outcomes[input_row] = Err(format!(
                "branch SET failed with {}: {} at {}",
                error.code.as_str(),
                error.message,
                error.span
            ));
            continue;
        }
        let mut fields = Vec::with_capacity(result.batch.schema().fields().len());
        for (column_index, field) in result.batch.schema().fields().iter().enumerate() {
            let array = result.batch.column(column_index).to_array_ref();
            let value = runtime_value_from_arrow_array(
                array.as_ref(),
                &parse_as_type_from_arrow(field.data_type())?,
                false,
                output_row,
                field.name(),
            )?
            .ok_or_else(|| format!("branch field '{}' is null", field.name()))?;
            let name = Identifier::parse(field.name()).map_err(|error| {
                format!(
                    "compiled branch field '{}' is invalid: {}",
                    field.name(),
                    error
                )
            })?;
            fields.push((name, value));
        }
        outcomes[input_row] = BranchKey::from_fields(fields).map(Some);
    }
    Ok(outcomes)
}

fn emitter_headers_from_invocations(
    invocations: &[nervix_vm::FunctionInvocation],
    row: usize,
) -> Result<EmitterHeaders, String> {
    let mut headers = Vec::new();
    for invocation in invocations {
        if invocation.function != FunctionName::WriteHeader {
            return Err(format!(
                "unsupported invocation '{}'",
                invocation.function.as_str()
            ));
        }
        let [VmTypedArray::Utf8(names), VmTypedArray::Utf8(values)] =
            invocation.arguments.as_slice()
        else {
            return Err("write_header arguments must both be STRING".to_string());
        };
        if row >= names.len() || row >= values.len() {
            return Err(format!(
                "write_header result does not contain output row {row}"
            ));
        }
        if names.is_null(row) || values.is_null(row) {
            return Err("write_header arguments cannot be NULL".to_string());
        }
        headers.push((names.value(row).to_string(), values.value(row).to_string()));
    }
    Ok(headers)
}

fn message_timestamp(message: &RelayMessage) -> Timestamp {
    message.record.metadata().ingested_at_low_watermark()
}

fn current_window_emit_high_watermark(
    runtime: &Runtime,
    domain: &Domain,
) -> Result<Timestamp, String> {
    runtime
        .current_stream_expiration_time(domain)?
        .ok_or_else(|| format!("domain '{}' has no current timestamp", domain.as_str()))
}

fn window_output_metadata(
    state: &WindowProcessorState,
    emit_high_watermark: Timestamp,
) -> Result<RuntimeRecordMetadata, String> {
    let low = state
        .entries
        .iter()
        .map(|entry| entry.timestamp)
        .min()
        .ok_or_else(|| "window aggregate requires a non-empty window".to_string())?;
    Ok(RuntimeRecordMetadata::from_ingested_at_watermarks(
        low,
        emit_high_watermark,
    ))
}

async fn flush_ready_window_processor(
    context: WindowFlushContext<'_>,
    state: &mut WindowProcessorState,
    aggregate: &WindowAggregateProgram,
    compiled_aggregates: &[CompiledWindowAggregateProgram],
    bounds: WindowBounds,
    now: Timestamp,
) -> bool {
    let WindowFlushContext {
        graph,
        node_kind,
        processor,
        error_policies,
        branch,
        output_routes,
    } = context;
    if output_routes.routes.is_empty() {
        state.clear(aggregate);
        return true;
    }
    if output_routes.routes.len() != compiled_aggregates.len() {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            state.entries.iter().map(|entry| &entry.message.acks),
            format!(
                "window processor '{}' has {} output routes but {} compiled aggregate programs",
                processor.as_str(),
                output_routes.routes.len(),
                compiled_aggregates.len()
            ),
        );
        state.clear(aggregate);
        return true;
    }
    let mut changed = false;
    match state.purge_timeouts(now) {
        Ok(purged) => {
            changed |= purged;
        }
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                state.entries.iter().map(|entry| &entry.message.acks),
                format!(
                    "window processor '{}' failed to purge timed aggregate state: {}",
                    processor.as_str(),
                    error
                ),
            );
            state.clear(aggregate);
            return true;
        }
    }
    while window_width_met(state, bounds.width_messages, bounds.width_duration, now) {
        let Some(first_entry) = state.entries.front() else {
            break;
        };
        let emit_high_watermark =
            match current_window_emit_high_watermark(&branch.runtime, &branch.domain) {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        state.entries.iter().map(|entry| &entry.message.acks),
                        format!(
                            "window processor '{}' cannot emit aggregate: {}",
                            processor.as_str(),
                            error
                        ),
                    );
                    state.clear(aggregate);
                    changed = true;
                    break;
                }
            };
        let output_metadata = match window_output_metadata(state, emit_high_watermark) {
            Ok(metadata) => metadata,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    state.entries.iter().map(|entry| &entry.message.acks),
                    format!(
                        "window processor '{}' cannot emit aggregate: {}",
                        processor.as_str(),
                        error
                    ),
                );
                state.clear(aggregate);
                changed = true;
                break;
            }
        };
        let mut route_failed = false;
        for (output_index, compiled_aggregate) in compiled_aggregates.iter().enumerate() {
            let output_relay = output_routes.routes[output_index].relay.clone();
            let output_schema =
                match relay_schema_for_runtime(&branch.runtime, &branch.domain, &output_relay) {
                    Ok(schema) => schema,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            node_kind,
                            processor,
                            error_policies,
                            state.entries.iter().map(|entry| &entry.message.acks),
                            error,
                        );
                        route_failed = true;
                        break;
                    }
                };
            let output_batch =
                match evaluate_window_aggregate(compiled_aggregate, state, &output_schema).await {
                    Ok(record) => record,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            node_kind,
                            processor,
                            error_policies,
                            state.entries.iter().map(|entry| &entry.message.acks),
                            format!(
                                "window processor '{}' output route '{}' aggregate failed: {}",
                                processor.as_str(),
                                output_relay.as_str(),
                                error
                            ),
                        );
                        route_failed = true;
                        break;
                    }
                };
            let output_message = RelayMessage {
                key: first_entry.message.key.clone(),
                record: match RuntimeRow::new(Arc::new(output_batch), 0, output_metadata.clone()) {
                    Ok(record) => record,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            node_kind,
                            processor,
                            error_policies,
                            state.entries.iter().map(|entry| &entry.message.acks),
                            format!(
                                "window processor '{}' failed to construct output route '{}' row: \
                                 {}",
                                processor.as_str(),
                                output_relay.as_str(),
                                error
                            ),
                        );
                        route_failed = true;
                        break;
                    }
                },
                acks: AckSet::merged(
                    state
                        .entries
                        .iter()
                        .map(|entry| entry.message.acks.attached()),
                ),
            };
            let forwarded =
                match RelayRecordBatch::from_messages(output_schema, vec![output_message]) {
                    Ok(batch) => batch,
                    Err(error) => {
                        branch.runtime.handle_internal_processor_error_for_acks(
                            &branch.domain,
                            node_kind,
                            processor,
                            error_policies,
                            state.entries.iter().map(|entry| &entry.message.acks),
                            format!(
                                "window processor '{}' failed to build output route '{}' batch: {}",
                                processor.as_str(),
                                output_relay.as_str(),
                                error
                            ),
                        );
                        route_failed = true;
                        break;
                    }
                };
            if let Some(acks) = dispatch_processor_output(
                ProcessorOutputDispatchContext {
                    graph,
                    branch,
                    node_kind,
                    source_kind: ModelKind::WindowProcessor,
                    processor,
                    error_policies,
                    input_relays: std::slice::from_ref(&output_relay),
                    filter_source: ProcessorOutputFilterSource::OutputRelay,
                    resolved_materialized_state: None,
                },
                output_routes,
                forwarded,
                output_index,
            )
            .await
            {
                for ack in acks {
                    ack.ack_success();
                }
            }
        }
        if route_failed {
            state.clear(aggregate);
            changed = true;
            break;
        }
        if let Err(error) = advance_window(
            state,
            aggregate,
            bounds.step_messages,
            bounds.step_duration,
            now,
        ) {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                state.entries.iter().map(|entry| &entry.message.acks),
                format!(
                    "window processor '{}' failed to advance window: {}",
                    processor.as_str(),
                    error
                ),
            );
            state.clear(aggregate);
            changed = true;
            break;
        }
        changed = true;
        if state.entries.is_empty() {
            break;
        }
    }
    changed
}

async fn persist_window_processor_live_state(
    runtime: &Runtime,
    processor: &Identifier,
    replicated_state: &ReplicatedWindowProcessorState,
    state: &WindowProcessorState,
) -> Result<(), String> {
    let (lsm, payload) = replicated_state.replace_state(state).map_err(|error| {
        format!(
            "window processor '{}' failed to encode branch state: {}",
            processor.as_str(),
            error
        )
    })?;
    runtime
        .persist_window_processor_snapshot(replicated_state, lsm, &payload)
        .await
}

impl WindowAggregateAccumulator {
    fn new(demand: &WindowAggregateDemand) -> Self {
        match demand.storage {
            WindowAggregateStorageKind::Counter => Self::Counter { count: 0 },
            WindowAggregateStorageKind::Sequence => Self::Sequence {
                values: VecDeque::new(),
            },
            WindowAggregateStorageKind::SortedMap => Self::SortedMap {
                counts: BTreeMap::new(),
            },
            WindowAggregateStorageKind::Histogram => {
                let config = demand
                    .linear_histogram
                    .as_ref()
                    .expect("linear histogram aggregate spec must carry histogram config");
                Self::LinearHistogram {
                    buckets: vec![0; config.buckets],
                    total: 0,
                    min: config.min,
                    max: config.max,
                    width: (config.max - config.min) / config.buckets as f64,
                    delay: config.delay,
                    delayed_removals: VecDeque::new(),
                }
            }
            WindowAggregateStorageKind::Sum => Self::Sum { total: None },
        }
    }

    fn to_snapshot(&self) -> WindowAggregateAccumulatorSnapshot {
        match self {
            Self::Counter { count } => {
                WindowAggregateAccumulatorSnapshot::Counter { count: *count }
            }
            Self::Sequence { values } => WindowAggregateAccumulatorSnapshot::Sequence {
                values: values
                    .iter()
                    .map(|(timestamp, sequence, value)| WindowSequenceValueSnapshot {
                        timestamp: *timestamp,
                        sequence: *sequence,
                        value: value.to_remote(),
                    })
                    .collect(),
            },
            Self::SortedMap { counts } => WindowAggregateAccumulatorSnapshot::SortedMap {
                counts: counts
                    .iter()
                    .map(|(value, count)| WindowSortedCountSnapshot {
                        value: value.0.to_remote(),
                        count: *count,
                    })
                    .collect(),
            },
            Self::LinearHistogram {
                buckets,
                total,
                min,
                max,
                width,
                delay,
                delayed_removals,
            } => WindowAggregateAccumulatorSnapshot::LinearHistogram {
                buckets: buckets.clone(),
                total: *total,
                min: *min,
                max: *max,
                width: *width,
                delay_nanos: u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX),
                delayed_removals: delayed_removals
                    .iter()
                    .map(|removal| LinearHistogramDelayedRemovalSnapshot {
                        expires_at: removal.expires_at,
                        bucket: removal.bucket,
                    })
                    .collect(),
            },
            Self::Sum { total } => WindowAggregateAccumulatorSnapshot::Sum {
                total: total.as_ref().map(RuntimeValue::to_remote),
            },
        }
    }

    fn from_snapshot(snapshot: WindowAggregateAccumulatorSnapshot) -> Self {
        match snapshot {
            WindowAggregateAccumulatorSnapshot::Counter { count } => Self::Counter { count },
            WindowAggregateAccumulatorSnapshot::Sequence { values } => Self::Sequence {
                values: values
                    .into_iter()
                    .map(|value| {
                        (
                            value.timestamp,
                            value.sequence,
                            RuntimeValue::from_remote(value.value),
                        )
                    })
                    .collect(),
            },
            WindowAggregateAccumulatorSnapshot::SortedMap { counts } => Self::SortedMap {
                counts: counts
                    .into_iter()
                    .map(|entry| {
                        (
                            RuntimeValueSortKey(RuntimeValue::from_remote(entry.value)),
                            entry.count,
                        )
                    })
                    .collect(),
            },
            WindowAggregateAccumulatorSnapshot::LinearHistogram {
                buckets,
                total,
                min,
                max,
                width,
                delay_nanos,
                delayed_removals,
            } => Self::LinearHistogram {
                buckets,
                total,
                min,
                max,
                width,
                delay: Duration::from_nanos(delay_nanos),
                delayed_removals: delayed_removals
                    .into_iter()
                    .map(|removal| LinearHistogramDelayedRemoval {
                        expires_at: removal.expires_at,
                        bucket: removal.bucket,
                    })
                    .collect(),
            },
            WindowAggregateAccumulatorSnapshot::Sum { total } => Self::Sum {
                total: total.map(RuntimeValue::from_remote),
            },
        }
    }

    fn purge_expired(&mut self, now: Timestamp) -> Result<(), String> {
        let Self::LinearHistogram {
            buckets,
            total,
            delayed_removals,
            ..
        } = self
        else {
            return Ok(());
        };
        while delayed_removals
            .front()
            .is_some_and(|removal| removal.expires_at <= now)
        {
            let removal = delayed_removals
                .pop_front()
                .expect("front removal exists after is_some_and");
            let Some(count) = buckets.get_mut(removal.bucket) else {
                return Err("linear histogram delayed removal bucket is out of range".to_string());
            };
            if *count == 0 {
                return Err(
                    "linear histogram accumulator is missing delayed removed value".to_string(),
                );
            }
            *count -= 1;
            *total = total.saturating_sub(1);
        }
        Ok(())
    }

    fn next_deadline(&self) -> Option<Timestamp> {
        let Self::LinearHistogram {
            delayed_removals, ..
        } = self
        else {
            return None;
        };
        delayed_removals.front().map(|removal| removal.expires_at)
    }

    fn add(
        &mut self,
        _demand: &WindowAggregateDemand,
        timestamp: Timestamp,
        sequence: u64,
        value: Option<RuntimeValue>,
    ) -> Result<(), String> {
        self.purge_expired(timestamp)?;
        match self {
            Self::Counter { count } => {
                *count = count.saturating_add(1);
                Ok(())
            }
            Self::Sequence { values } => {
                let value = value
                    .ok_or_else(|| "sequence aggregate structure requires a value".to_string())?;
                values.push_back((timestamp, sequence, value));
                Ok(())
            }
            Self::SortedMap { counts } => {
                let value = value
                    .ok_or_else(|| "ordered aggregate structure requires a value".to_string())?;
                *counts.entry(RuntimeValueSortKey(value)).or_insert(0) += 1;
                Ok(())
            }
            Self::LinearHistogram {
                buckets,
                total,
                min,
                max,
                width,
                delay: _,
                delayed_removals: _,
            } => {
                let value = value
                    .ok_or_else(|| "PERCENTILE_LINEAR_HISTOGRAM requires a value".to_string())?;
                let value = runtime_value_to_f64(&value)?;
                let bucket = linear_histogram_bucket(value, *min, *max, *width, buckets.len())?;
                buckets[bucket] = buckets[bucket].saturating_add(1);
                *total = total.saturating_add(1);
                Ok(())
            }
            Self::Sum { total } => {
                let value = value.ok_or_else(|| "SUM requires a value".to_string())?;
                *total = Some(match total.take() {
                    Some(current) => sum_runtime_values(current, value)?,
                    None => value,
                });
                Ok(())
            }
        }
    }

    fn remove(
        &mut self,
        _demand: &WindowAggregateDemand,
        removal_time: Timestamp,
        timestamp: Timestamp,
        sequence: u64,
        value: Option<RuntimeValue>,
    ) -> Result<(), String> {
        self.purge_expired(removal_time)?;
        match self {
            Self::Counter { count } => {
                *count = count.saturating_sub(1);
                Ok(())
            }
            Self::Sequence { values } => {
                let Some(index) = values
                    .iter()
                    .position(|(entry_timestamp, entry_sequence, _)| {
                        *entry_timestamp == timestamp && *entry_sequence == sequence
                    })
                else {
                    return Err("sequence accumulator is missing removed window entry".to_string());
                };
                values.remove(index);
                Ok(())
            }
            Self::SortedMap { counts } => {
                let value = value
                    .ok_or_else(|| "ordered aggregate structure requires a value".to_string())?;
                decrement_runtime_value_count(counts, value)
            }
            Self::LinearHistogram {
                buckets,
                total,
                min,
                max,
                width,
                delay,
                delayed_removals,
            } => {
                let value = value
                    .ok_or_else(|| "PERCENTILE_LINEAR_HISTOGRAM requires a value".to_string())?;
                let value = runtime_value_to_f64(&value)?;
                let bucket = linear_histogram_bucket(value, *min, *max, *width, buckets.len())?;
                if delay.is_zero() {
                    let Some(count) = buckets.get_mut(bucket) else {
                        return Err("linear histogram bucket is out of range".to_string());
                    };
                    if *count == 0 {
                        return Err(
                            "linear histogram accumulator is missing removed value".to_string()
                        );
                    }
                    *count -= 1;
                    *total = total.saturating_sub(1);
                    return Ok(());
                }
                delayed_removals.push_back(LinearHistogramDelayedRemoval {
                    expires_at: checked_add_duration_to_timestamp(removal_time, *delay),
                    bucket,
                });
                Ok(())
            }
            Self::Sum { total } => {
                let value = value.ok_or_else(|| "SUM requires a value".to_string())?;
                *total = match total.take() {
                    Some(current) => subtract_runtime_values(current, value)?,
                    None => None,
                };
                Ok(())
            }
        }
    }

    fn evaluate(
        &self,
        function: WindowAggregateFunction,
        percentile: Option<f64>,
    ) -> Result<RuntimeValue, String> {
        match (function, self) {
            (WindowAggregateFunction::Count, Self::Counter { count }) => {
                Ok(RuntimeValue::I64(*count as i64))
            }
            (WindowAggregateFunction::First, Self::Sequence { values }) => values
                .iter()
                .min_by_key(|(timestamp, sequence, _)| (*timestamp, *sequence))
                .map(|(_, _, value)| value.clone())
                .ok_or_else(|| "FIRST requires a non-empty window".to_string()),
            (WindowAggregateFunction::Last, Self::Sequence { values }) => values
                .iter()
                .max_by_key(|(timestamp, sequence, _)| (*timestamp, *sequence))
                .map(|(_, _, value)| value.clone())
                .ok_or_else(|| "LAST requires a non-empty window".to_string()),
            (WindowAggregateFunction::Max, Self::SortedMap { counts }) => counts
                .last_key_value()
                .map(|(value, _)| value.0.clone())
                .ok_or_else(|| "MAX requires a non-empty window".to_string()),
            (WindowAggregateFunction::Min, Self::SortedMap { counts }) => counts
                .first_key_value()
                .map(|(value, _)| value.0.clone())
                .ok_or_else(|| "MIN requires a non-empty window".to_string()),
            (
                WindowAggregateFunction::PercentileLinearHistogram,
                Self::LinearHistogram {
                    buckets,
                    total,
                    min,
                    max,
                    width,
                    ..
                },
            ) => {
                let percentile = percentile.ok_or_else(|| {
                    "PERCENTILE_LINEAR_HISTOGRAM requires a constant percentile".to_string()
                })?;
                percentile_from_linear_histogram(buckets, *total, *min, *max, *width, percentile)
            }
            (WindowAggregateFunction::Sum, Self::Sum { total }) => total
                .clone()
                .ok_or_else(|| "SUM requires a non-empty window".to_string()),
            _ => Err(format!(
                "{function:?} aggregate is backed by an incompatible accumulator"
            )),
        }
    }
}

impl WindowProcessorState {
    fn new(program: &WindowAggregateProgram) -> Self {
        let accumulators = program
            .demands()
            .iter()
            .map(WindowAggregateAccumulator::new)
            .collect();
        Self {
            entries: VecDeque::new(),
            next_sequence: 0,
            accumulators,
        }
    }

    fn to_snapshot(&self) -> Result<WindowProcessorStateSnapshot, String> {
        Ok(WindowProcessorStateSnapshot {
            entries: self
                .entries
                .iter()
                .map(|entry| {
                    Ok(WindowEntrySnapshot {
                        sequence: entry.sequence,
                        timestamp: entry.timestamp,
                        key: BranchKey::to_remote_key(&entry.message.key),
                        record: entry.message.record.to_remote()?,
                        aggregate_inputs: entry
                            .aggregate_inputs
                            .iter()
                            .map(|input| input.value.as_ref().map(RuntimeValue::to_remote))
                            .collect(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            next_sequence: self.next_sequence,
            accumulators: self
                .accumulators
                .iter()
                .map(WindowAggregateAccumulator::to_snapshot)
                .collect(),
        })
    }

    fn from_snapshot(
        program: &WindowAggregateProgram,
        input_schema: &CompiledSchema,
        snapshot: WindowProcessorStateSnapshot,
    ) -> Result<Self, String> {
        if snapshot.accumulators.len() != program.demands().len() {
            return Err(format!(
                "window snapshot accumulator count {} does not match aggregate demand count {}",
                snapshot.accumulators.len(),
                program.demands().len()
            ));
        }
        Ok(Self {
            entries: snapshot
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(WindowEntry {
                        sequence: entry.sequence,
                        timestamp: entry.timestamp,
                        message: RelayMessage {
                            key: BranchKey::from_remote_key(entry.key)?,
                            record: input_schema.runtime_row_from_remote(entry.record)?,
                            acks: AckSet::empty(),
                        },
                        aggregate_inputs: entry
                            .aggregate_inputs
                            .into_iter()
                            .map(|value| WindowAggregateInput {
                                value: value.map(RuntimeValue::from_remote),
                            })
                            .collect(),
                    })
                })
                .collect::<Result<VecDeque<_>, String>>()?,
            next_sequence: snapshot.next_sequence,
            accumulators: snapshot
                .accumulators
                .into_iter()
                .map(WindowAggregateAccumulator::from_snapshot)
                .collect(),
        })
    }

    fn push_message(
        &mut self,
        program: &WindowAggregateProgram,
        timestamp: Timestamp,
        message: RelayMessage,
        inputs: Vec<WindowAggregateInput>,
    ) -> Result<(), Box<(String, RelayMessage)>> {
        let sequence = self.next_sequence;
        self.apply_aggregate_inputs(
            program.demands(),
            timestamp,
            sequence,
            &inputs,
            WindowAccumulatorAction::Add,
        )
        .map_err(|error| Box::new((error, message.clone())))?;
        self.entries.push_back(WindowEntry {
            sequence,
            timestamp,
            message,
            aggregate_inputs: inputs,
        });
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(())
    }

    fn clear(&mut self, program: &WindowAggregateProgram) {
        self.entries.clear();
        self.accumulators = program
            .demands()
            .iter()
            .map(WindowAggregateAccumulator::new)
            .collect();
    }

    fn purge_timeouts(&mut self, now: Timestamp) -> Result<bool, String> {
        let mut changed = false;
        for accumulator in &mut self.accumulators {
            if accumulator
                .next_deadline()
                .is_some_and(|deadline| deadline <= now)
            {
                accumulator.purge_expired(now)?;
                changed = true;
            }
        }
        Ok(changed)
    }

    fn next_timeout_deadline(&self) -> Option<Timestamp> {
        self.accumulators
            .iter()
            .filter_map(WindowAggregateAccumulator::next_deadline)
            .min()
    }

    fn pop_front_entry(
        &mut self,
        program: &WindowAggregateProgram,
        removal_time: Timestamp,
    ) -> Result<Option<WindowEntry>, String> {
        let Some(entry) = self.entries.pop_front() else {
            return Ok(None);
        };
        self.apply_aggregate_inputs(
            program.demands(),
            entry.timestamp,
            entry.sequence,
            &entry.aggregate_inputs,
            WindowAccumulatorAction::Remove { at: removal_time },
        )?;
        Ok(Some(entry))
    }

    fn apply_aggregate_inputs(
        &mut self,
        demands: &[WindowAggregateDemand],
        timestamp: Timestamp,
        sequence: u64,
        inputs: &[WindowAggregateInput],
        action: WindowAccumulatorAction,
    ) -> Result<(), String> {
        if inputs.len() != self.accumulators.len() {
            return Err(format!(
                "window aggregate input count {} does not match accumulator count {}",
                inputs.len(),
                self.accumulators.len()
            ));
        }
        for ((input, accumulator), demand) in inputs.iter().zip(&mut self.accumulators).zip(demands)
        {
            match action {
                WindowAccumulatorAction::Add => {
                    accumulator.add(demand, timestamp, sequence, input.value.clone())?
                }
                WindowAccumulatorAction::Remove { at } => {
                    accumulator.remove(demand, at, timestamp, sequence, input.value.clone())?
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct WindowAggregateInput {
    value: Option<RuntimeValue>,
}

async fn evaluate_window_aggregate_inputs(
    program: &CompiledWindowAggregateProgram,
    row: &RuntimeRow,
    execution_now: Timestamp,
) -> Result<Vec<WindowAggregateInput>, String> {
    let carrier = row.one_row_batch();
    let keys = [None];
    let side_inputs = HashMap::new();
    let lookup_columns = HashMap::new();
    let uninitialized = VmUninitializedInput {
        fields: program
            .input_program
            .input_schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("window_input."))
            .map(|field| field.name().clone())
            .collect(),
    };
    let input = project_vm_input_batch(
        &program.input_program.input_schema,
        &VmInputProjectionSources {
            carrier: &carrier,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized),
        },
    )?;
    let result = execute_program_with_selection_in_context(
        &program.input_program,
        &input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    if result.selected_rows.as_slice() != [0] {
        return Err("window aggregate input VM did not preserve its input row".to_string());
    }
    if let Some(error) = result.batch.errors().row(0).first() {
        return Err(format!(
            "window aggregate input VM failed with {}: {}",
            error.code.as_str(),
            error.message
        ));
    }
    program
        .input_fields
        .iter()
        .map(|field_name| {
            let Some(field_name) = field_name else {
                return Ok(WindowAggregateInput { value: None });
            };
            let column_index = result.batch.schema().index_of(field_name).map_err(|_| {
                format!("window aggregate input VM produced no '{field_name}' field")
            })?;
            let field = result.batch.schema().field(column_index);
            let array = result.batch.column(column_index).to_array_ref();
            runtime_value_from_arrow_array(
                array.as_ref(),
                &parse_as_type_from_arrow(field.data_type())?,
                true,
                0,
                field_name,
            )
            .map(|value| WindowAggregateInput { value })
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum WindowAccumulatorAction {
    Add,
    Remove { at: Timestamp },
}

fn decrement_runtime_value_count(
    counts: &mut BTreeMap<RuntimeValueSortKey, usize>,
    value: RuntimeValue,
) -> Result<(), String> {
    let key = RuntimeValueSortKey(value);
    let Some(count) = counts.get_mut(&key) else {
        return Err("sorted accumulator is missing removed window value".to_string());
    };
    *count -= 1;
    if *count == 0 {
        counts.remove(&key);
    }
    Ok(())
}

fn linear_histogram_bucket(
    value: f64,
    min: f64,
    max: f64,
    width: f64,
    bucket_count: usize,
) -> Result<usize, String> {
    if !value.is_finite() {
        return Err("PERCENTILE_LINEAR_HISTOGRAM requires finite numeric values".to_string());
    }
    if bucket_count == 0 {
        return Err("PERCENTILE_LINEAR_HISTOGRAM requires at least one bucket".to_string());
    }
    if value <= min {
        return Ok(0);
    }
    if value >= max {
        return Ok(bucket_count - 1);
    }
    Ok(((value - min) / width).floor() as usize)
}

fn percentile_from_linear_histogram(
    buckets: &[usize],
    total: usize,
    min: f64,
    max: f64,
    width: f64,
    percentile: f64,
) -> Result<RuntimeValue, String> {
    if total == 0 {
        return Err("PERCENTILE_LINEAR_HISTOGRAM requires a non-empty window".to_string());
    }
    let rank = ((percentile / 100.0) * ((total - 1) as f64)).round() as usize;
    let mut seen = 0usize;
    for (index, count) in buckets.iter().enumerate() {
        seen += *count;
        if seen > rank {
            let midpoint = min + (index as f64 + 0.5) * width;
            return Ok(RuntimeValue::F64(OrderedFloat(midpoint.clamp(min, max))));
        }
    }
    Err("PERCENTILE_LINEAR_HISTOGRAM histogram is empty".to_string())
}

fn window_width_met(
    state: &WindowProcessorState,
    width_messages: Option<usize>,
    width_duration: Option<Duration>,
    now: Timestamp,
) -> bool {
    if state.entries.is_empty() {
        return false;
    }
    if let Some(width_messages) = width_messages
        && state.entries.len() >= width_messages
    {
        return true;
    }
    if let Some(width_duration) = width_duration
        && let Some(first) = state.entries.front()
        && timestamp_elapsed(first.timestamp, now) >= width_duration
    {
        return true;
    }
    false
}

fn window_next_deadline(
    state: &WindowProcessorState,
    width_duration: Option<Duration>,
) -> Option<Timestamp> {
    let width_deadline = width_duration.and_then(|width_duration| {
        state
            .entries
            .front()
            .map(|first| checked_add_duration_to_timestamp(first.timestamp, width_duration))
    });
    match (width_deadline, state.next_timeout_deadline()) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

fn timestamp_elapsed(start: Timestamp, end: Timestamp) -> Duration {
    end.into_datetime()
        .signed_duration_since(start.into_datetime())
        .to_std()
        .unwrap_or(Duration::ZERO)
}

fn advance_window(
    state: &mut WindowProcessorState,
    program: &WindowAggregateProgram,
    step_messages: Option<usize>,
    step_duration: Option<Duration>,
    removal_time: Timestamp,
) -> Result<(), String> {
    let remove_messages = step_messages.unwrap_or(0).min(state.entries.len());
    for _ in 0..remove_messages {
        if let Some(entry) = state.pop_front_entry(program, removal_time)? {
            entry.message.acks.ack_success();
        }
    }
    if let Some(step_duration) = step_duration
        && let Some(first) = state.entries.front()
    {
        let cutoff = checked_add_duration_to_timestamp(first.timestamp, step_duration);
        while state
            .entries
            .front()
            .is_some_and(|entry| entry.timestamp < cutoff)
        {
            if let Some(entry) = state.pop_front_entry(program, removal_time)? {
                entry.message.acks.ack_success();
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct WindowAggregateFunctionInjector {
    accumulators: Vec<WindowAggregateAccumulator>,
    demand_types: Vec<ArrowDataType>,
    demand_offset: usize,
}

impl VmFunctionInjector for WindowAggregateFunctionInjector {
    fn inject(
        &self,
        function: &FunctionName,
        _arguments: &[VmTypedArray],
        row_count: usize,
        _span: nervix_nspl::vm_program::Span,
    ) -> Result<VmTypedArray, nervix_vm::RuntimeError> {
        let FunctionName::WindowAggregate(invocation) = function else {
            return Err(nervix_vm::RuntimeError::InvalidBatch {
                message: format!("function '{}' is not a window aggregate", function.as_str()),
            });
        };
        let accumulator_id = self.demand_offset.saturating_add(invocation.demand_id);
        let accumulator = self.accumulators.get(accumulator_id).ok_or_else(|| {
            nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "window aggregate is missing accumulator for route demand {} (shared demand \
                     {})",
                    invocation.demand_id, accumulator_id
                ),
            }
        })?;
        let value = accumulator
            .evaluate(invocation.function, invocation.percentile)
            .map_err(|message| nervix_vm::RuntimeError::InvalidBatch { message })?;
        let data_type = self.demand_types.get(invocation.demand_id).ok_or_else(|| {
            nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "window aggregate is missing output type for demand {}",
                    invocation.demand_id
                ),
            }
        })?;
        runtime_value_arrow_array(data_type, Some(&value), row_count)
            .and_then(|array| {
                VmTypedArray::try_from_array_ref(array).map_err(|error| error.to_string())
            })
            .map_err(|message| nervix_vm::RuntimeError::InvalidBatch { message })
    }
}

async fn evaluate_window_aggregate(
    program: &CompiledWindowAggregateProgram,
    state: &WindowProcessorState,
    output_schema: &CompiledSchema,
) -> Result<RuntimeRecordBatch, String> {
    let injector: Arc<Box<dyn VmFunctionInjector>> =
        Arc::new(Box::new(WindowAggregateFunctionInjector {
            accumulators: state.accumulators.clone(),
            demand_types: program.demand_types.clone(),
            demand_offset: program.demand_offset,
        }));
    let mut columns = Vec::with_capacity(output_schema.arrow_schema().fields().len());
    for field in output_schema.arrow_schema().fields() {
        let value = if let Some(assignment) = program
            .assignments
            .iter()
            .find(|assignment| assignment.target.field == *field.name())
        {
            Some(
                evaluate_window_aggregate_expr(
                    &assignment.value,
                    &assignment.target.field,
                    injector.clone(),
                )
                .await?,
            )
        } else if field.is_nullable() {
            None
        } else {
            return Err(format!(
                "window aggregate did not initialize required output field '{}'",
                field.name()
            ));
        };
        columns.push(runtime_value_arrow_array(
            field.data_type(),
            value.as_ref(),
            1,
        )?);
    }
    let batch = RecordBatch::try_new(output_schema.arrow_schema(), columns)
        .map_err(|error| error.to_string())?;
    RuntimeRecordBatch::from_record_batch(output_schema.arrow_schema(), batch)
}

fn evaluate_window_aggregate_expr<'a>(
    expr: &'a CompiledWindowAggregateExpr,
    target_field: &'a str,
    injector: Arc<Box<dyn VmFunctionInjector>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RuntimeValue, String>> + Send + 'a>>
{
    Box::pin(async move {
        match expr {
            CompiledWindowAggregateExpr::Scalar(program) => {
                let input = VmTypedBatch::try_new(
                    program.input_schema.clone(),
                    program
                        .input_schema
                        .fields()
                        .iter()
                        .map(|field| VmTypedArray::uninitialized(field.data_type().clone(), 1))
                        .collect(),
                )
                .map_err(|error| error.to_string())?;
                let result = execute_program_with_selection_in_context(
                    program,
                    &input,
                    &VmExecutionContext {
                        now: Timestamp::now(),
                        injector: Some(injector),
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
                let column_index = result.batch.schema().index_of(target_field).map_err(|_| {
                    format!("window aggregate VM produced no '{target_field}' output field")
                })?;
                let field = result.batch.schema().field(column_index);
                let array = result.batch.column(column_index).to_array_ref();
                runtime_value_from_arrow_array(
                    array.as_ref(),
                    &parse_as_type_from_arrow(field.data_type())?,
                    false,
                    0,
                    target_field,
                )?
                .ok_or_else(|| format!("window aggregate VM produced null '{target_field}' output"))
            }
            CompiledWindowAggregateExpr::Array { items, fixed_size } => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(
                        evaluate_window_aggregate_expr(item, target_field, injector.clone())
                            .await?,
                    );
                }
                if *fixed_size {
                    Ok(RuntimeValue::Array(values))
                } else {
                    Ok(RuntimeValue::Vec(values))
                }
            }
        }
    })
}

fn runtime_value_to_f64(value: &RuntimeValue) -> Result<f64, String> {
    match value {
        RuntimeValue::U8(value) => Ok(*value as f64),
        RuntimeValue::I8(value) => Ok(*value as f64),
        RuntimeValue::U16(value) => Ok(*value as f64),
        RuntimeValue::I16(value) => Ok(*value as f64),
        RuntimeValue::U32(value) => Ok(*value as f64),
        RuntimeValue::I32(value) => Ok(*value as f64),
        RuntimeValue::U64(value) => Ok(*value as f64),
        RuntimeValue::I64(value) => Ok(*value as f64),
        RuntimeValue::F32(value) => Ok(value.0 as f64),
        RuntimeValue::F64(value) => Ok(value.0),
        other => Err(format!(
            "expected numeric value, found {}",
            runtime_value_type_name(other)
        )),
    }
}

fn sum_runtime_values(left: RuntimeValue, right: RuntimeValue) -> Result<RuntimeValue, String> {
    match (left, right) {
        (RuntimeValue::U8(left), RuntimeValue::U8(right)) => Ok(RuntimeValue::U8(left + right)),
        (RuntimeValue::I8(left), RuntimeValue::I8(right)) => Ok(RuntimeValue::I8(left + right)),
        (RuntimeValue::U16(left), RuntimeValue::U16(right)) => Ok(RuntimeValue::U16(left + right)),
        (RuntimeValue::I16(left), RuntimeValue::I16(right)) => Ok(RuntimeValue::I16(left + right)),
        (RuntimeValue::U32(left), RuntimeValue::U32(right)) => Ok(RuntimeValue::U32(left + right)),
        (RuntimeValue::I32(left), RuntimeValue::I32(right)) => Ok(RuntimeValue::I32(left + right)),
        (RuntimeValue::U64(left), RuntimeValue::U64(right)) => Ok(RuntimeValue::U64(left + right)),
        (RuntimeValue::I64(left), RuntimeValue::I64(right)) => Ok(RuntimeValue::I64(left + right)),
        (RuntimeValue::F32(left), RuntimeValue::F32(right)) => {
            Ok(RuntimeValue::F32(OrderedFloat(left.0 + right.0)))
        }
        (RuntimeValue::F64(left), RuntimeValue::F64(right)) => {
            Ok(RuntimeValue::F64(OrderedFloat(left.0 + right.0)))
        }
        (left, right) => Err(format!(
            "SUM cannot combine {} and {}",
            runtime_value_type_name(&left),
            runtime_value_type_name(&right)
        )),
    }
}

fn subtract_runtime_values(
    left: RuntimeValue,
    right: RuntimeValue,
) -> Result<Option<RuntimeValue>, String> {
    let value = match (left, right) {
        (RuntimeValue::U8(left), RuntimeValue::U8(right)) => RuntimeValue::U8(left - right),
        (RuntimeValue::I8(left), RuntimeValue::I8(right)) => RuntimeValue::I8(left - right),
        (RuntimeValue::U16(left), RuntimeValue::U16(right)) => RuntimeValue::U16(left - right),
        (RuntimeValue::I16(left), RuntimeValue::I16(right)) => RuntimeValue::I16(left - right),
        (RuntimeValue::U32(left), RuntimeValue::U32(right)) => RuntimeValue::U32(left - right),
        (RuntimeValue::I32(left), RuntimeValue::I32(right)) => RuntimeValue::I32(left - right),
        (RuntimeValue::U64(left), RuntimeValue::U64(right)) => RuntimeValue::U64(left - right),
        (RuntimeValue::I64(left), RuntimeValue::I64(right)) => RuntimeValue::I64(left - right),
        (RuntimeValue::F32(left), RuntimeValue::F32(right)) => {
            RuntimeValue::F32(OrderedFloat(left.0 - right.0))
        }
        (RuntimeValue::F64(left), RuntimeValue::F64(right)) => {
            RuntimeValue::F64(OrderedFloat(left.0 - right.0))
        }
        (left, right) => {
            return Err(format!(
                "SUM cannot remove {} from {}",
                runtime_value_type_name(&right),
                runtime_value_type_name(&left)
            ));
        }
    };
    if runtime_value_is_zero(&value) {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

fn runtime_value_is_zero(value: &RuntimeValue) -> bool {
    match value {
        RuntimeValue::U8(value) => *value == 0,
        RuntimeValue::I8(value) => *value == 0,
        RuntimeValue::U16(value) => *value == 0,
        RuntimeValue::I16(value) => *value == 0,
        RuntimeValue::U32(value) => *value == 0,
        RuntimeValue::I32(value) => *value == 0,
        RuntimeValue::U64(value) => *value == 0,
        RuntimeValue::I64(value) => *value == 0,
        RuntimeValue::F32(value) => value.0 == 0.0,
        RuntimeValue::F64(value) => value.0 == 0.0,
        _ => false,
    }
}

fn compare_runtime_values(left: &RuntimeValue, right: &RuntimeValue) -> std::cmp::Ordering {
    match (left, right) {
        (RuntimeValue::U8(left), RuntimeValue::U8(right)) => left.cmp(right),
        (RuntimeValue::I8(left), RuntimeValue::I8(right)) => left.cmp(right),
        (RuntimeValue::U16(left), RuntimeValue::U16(right)) => left.cmp(right),
        (RuntimeValue::I16(left), RuntimeValue::I16(right)) => left.cmp(right),
        (RuntimeValue::U32(left), RuntimeValue::U32(right)) => left.cmp(right),
        (RuntimeValue::I32(left), RuntimeValue::I32(right)) => left.cmp(right),
        (RuntimeValue::U64(left), RuntimeValue::U64(right)) => left.cmp(right),
        (RuntimeValue::I64(left), RuntimeValue::I64(right)) => left.cmp(right),
        (RuntimeValue::F32(left), RuntimeValue::F32(right)) => left.cmp(right),
        (RuntimeValue::F64(left), RuntimeValue::F64(right)) => left.cmp(right),
        (RuntimeValue::String(left), RuntimeValue::String(right)) => left.cmp(right),
        (RuntimeValue::Datetime(left), RuntimeValue::Datetime(right)) => left.cmp(right),
        (RuntimeValue::Bool(left), RuntimeValue::Bool(right)) => left.cmp(right),
        _ => left.to_key_fragment().cmp(&right.to_key_fragment()),
    }
}

fn materialized_record_from_entries(
    entries: Vec<(String, nervix_models::RemoteRuntimeRecord)>,
    key: Option<&str>,
) -> Option<nervix_models::RemoteRuntimeRecord> {
    let Some(key) = key else {
        return entries.into_iter().next().map(|(_, record)| record);
    };
    entries
        .into_iter()
        .find_map(|(entry_key, record)| (entry_key == key).then_some(record))
}

macro_rules! append_filter_map_numeric_list_value {
    ($builder:expr, $value:expr, $field:expr, $pattern:path) => {{
        match $value {
            $pattern(value) => {
                $builder.append_value(*value);
                Ok(())
            }
            value => Err(format!(
                "FILTER-MAP input field '{}' expected {:?}, got {}",
                $field.name(),
                $field.data_type(),
                runtime_value_type_name(value)
            )),
        }
    }};
}

macro_rules! define_filter_map_numeric_list_appender {
    ($fn_name:ident, $builder:ty, $pattern:path) => {
        fn $fn_name(
            builder: &mut $builder,
            value: &RuntimeValue,
            field: &arrow_schema::Field,
        ) -> Result<(), String> {
            append_filter_map_numeric_list_value!(builder, value, field, $pattern)
        }
    };
}

define_filter_map_numeric_list_appender!(append_filter_map_u8, UInt8Builder, RuntimeValue::U8);
define_filter_map_numeric_list_appender!(append_filter_map_i8, Int8Builder, RuntimeValue::I8);
define_filter_map_numeric_list_appender!(append_filter_map_u16, UInt16Builder, RuntimeValue::U16);
define_filter_map_numeric_list_appender!(append_filter_map_i16, Int16Builder, RuntimeValue::I16);
define_filter_map_numeric_list_appender!(append_filter_map_u32, UInt32Builder, RuntimeValue::U32);
define_filter_map_numeric_list_appender!(append_filter_map_i32, Int32Builder, RuntimeValue::I32);
define_filter_map_numeric_list_appender!(append_filter_map_u64, UInt64Builder, RuntimeValue::U64);
define_filter_map_numeric_list_appender!(append_filter_map_i64, Int64Builder, RuntimeValue::I64);

fn append_filter_map_f32(
    builder: &mut Float32Builder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::F32(value) => {
            builder.append_value(value.into_inner());
            Ok(())
        }
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

fn append_filter_map_f64(
    builder: &mut Float64Builder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::F64(value) => {
            builder.append_value(value.into_inner());
            Ok(())
        }
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

fn append_filter_map_bool(
    builder: &mut BooleanBuilder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::Bool(value) => {
            builder.append_value(*value);
            Ok(())
        }
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

fn append_filter_map_string(
    builder: &mut StringBuilder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::String(value) => {
            builder.append_value(value);
            Ok(())
        }
        RuntimeValue::Datetime(value) => {
            builder.append_value(value.to_rfc3339());
            Ok(())
        }
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

fn append_filter_map_datetime(
    builder: &mut TimestampNanosecondBuilder,
    value: &RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    match value {
        RuntimeValue::Datetime(value) => match value.timestamp_nanos_opt() {
            Some(value) => {
                builder.append_value(value);
                Ok(())
            }
            None => Err(format!(
                "FILTER-MAP input field '{}' datetime is out of nanosecond range",
                field.name()
            )),
        },
        value => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
}

fn append_filter_map_nested_value(
    builder: &mut dyn ArrayBuilder,
    data_type: &ArrowDataType,
    value: Option<&RuntimeValue>,
    field: &arrow_schema::Field,
) -> Result<(), String> {
    macro_rules! append_primitive {
        ($builder:ty, $append:ident) => {{
            let builder = builder
                .as_any_mut()
                .downcast_mut::<$builder>()
                .ok_or_else(|| {
                    format!(
                        "FILTER-MAP input field '{}' has an incompatible Arrow builder",
                        field.name()
                    )
                })?;
            if let Some(value) = value {
                $append(builder, value, field)?;
            } else {
                builder.append_null();
            }
            Ok(())
        }};
    }

    match data_type {
        ArrowDataType::UInt8 => append_primitive!(UInt8Builder, append_filter_map_u8),
        ArrowDataType::Int8 => append_primitive!(Int8Builder, append_filter_map_i8),
        ArrowDataType::UInt16 => append_primitive!(UInt16Builder, append_filter_map_u16),
        ArrowDataType::Int16 => append_primitive!(Int16Builder, append_filter_map_i16),
        ArrowDataType::UInt32 => append_primitive!(UInt32Builder, append_filter_map_u32),
        ArrowDataType::Int32 => append_primitive!(Int32Builder, append_filter_map_i32),
        ArrowDataType::UInt64 => append_primitive!(UInt64Builder, append_filter_map_u64),
        ArrowDataType::Int64 => append_primitive!(Int64Builder, append_filter_map_i64),
        ArrowDataType::Float32 => append_primitive!(Float32Builder, append_filter_map_f32),
        ArrowDataType::Float64 => append_primitive!(Float64Builder, append_filter_map_f64),
        ArrowDataType::Boolean => append_primitive!(BooleanBuilder, append_filter_map_bool),
        ArrowDataType::Utf8 => append_primitive!(StringBuilder, append_filter_map_string),
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
            if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" =>
        {
            append_primitive!(TimestampNanosecondBuilder, append_filter_map_datetime)
        }
        ArrowDataType::List(element) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| {
                    format!(
                        "FILTER-MAP input field '{}' has an incompatible list builder",
                        field.name()
                    )
                })?;
            let values = match value {
                Some(RuntimeValue::Vec(values)) => Some(values),
                None => None,
                Some(value) => {
                    return Err(format!(
                        "FILTER-MAP input field '{}' expected VEC, got {}",
                        field.name(),
                        runtime_value_type_name(value)
                    ));
                }
            };
            if let Some(values) = values {
                for value in values {
                    append_filter_map_nested_value(
                        builder.values().as_mut(),
                        element.data_type(),
                        Some(value),
                        field,
                    )?;
                }
            }
            builder.append(values.is_some());
            Ok(())
        }
        ArrowDataType::FixedSizeList(element, len) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| {
                    format!(
                        "FILTER-MAP input field '{}' has an incompatible fixed-list builder",
                        field.name()
                    )
                })?;
            let expected = usize::try_from(*len).map_err(|_| {
                format!(
                    "FILTER-MAP input field '{}' has invalid array length",
                    field.name()
                )
            })?;
            let values = match value {
                Some(RuntimeValue::Array(values)) if values.len() == expected => Some(values),
                Some(RuntimeValue::Array(values)) => {
                    return Err(format!(
                        "FILTER-MAP input field '{}' expected array length {expected}, got {}",
                        field.name(),
                        values.len()
                    ));
                }
                None => None,
                Some(value) => {
                    return Err(format!(
                        "FILTER-MAP input field '{}' expected ARRAY, got {}",
                        field.name(),
                        runtime_value_type_name(value)
                    ));
                }
            };
            for index in 0..expected {
                append_filter_map_nested_value(
                    builder.values().as_mut(),
                    element.data_type(),
                    values.map(|values| &values[index]),
                    field,
                )?;
            }
            builder.append(values.is_some());
            Ok(())
        }
        data_type => Err(format!(
            "FILTER-MAP input field '{}' has unsupported nested type {data_type:?}",
            field.name()
        )),
    }
}

struct VmInputProjectionSources<'a> {
    carrier: &'a RuntimeRecordBatch,
    namespace_batches: &'a [(&'a str, &'a RuntimeRecordBatch)],
    strict_namespaces: &'a [&'a str],
    keys: &'a [Option<BranchKey>],
    side_inputs: &'a HashMap<String, RuntimeValue>,
    ingest_metadata: Option<&'a [IngestFilterMapMetadata]>,
    lookup_columns: &'a HashMap<String, VmTypedArray>,
    uninitialized: Option<&'a VmUninitializedInput>,
}

fn project_vm_input_batch(
    schema: &StdArc<arrow_schema::Schema>,
    sources: &VmInputProjectionSources<'_>,
) -> Result<VmTypedBatch, String> {
    let row_count = sources.carrier.batch().num_rows();
    if sources.keys.len() != row_count {
        return Err(format!(
            "branch key count {} does not match batch row count {row_count}",
            sources.keys.len()
        ));
    }
    if let Some(metadata) = sources.ingest_metadata
        && metadata.len() != row_count
    {
        return Err(format!(
            "ingest metadata count {} does not match batch row count {row_count}",
            metadata.len()
        ));
    }
    for (namespace, batch) in sources.namespace_batches {
        if batch.batch().num_rows() != row_count {
            return Err(format!(
                "namespace '{namespace}' batch row count {} does not match carrier row count \
                 {row_count}",
                batch.batch().num_rows()
            ));
        }
    }
    let carrier_schema = sources.carrier.schema();
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            if let Some(uninitialized) = sources.uninitialized
                && uninitialized.contains(field)
            {
                return Ok(VmTypedArray::uninitialized(
                    field.data_type().clone(),
                    row_count,
                ));
            }
            if let Some(column) = sources.lookup_columns.get(field.name()) {
                return Ok(column.clone());
            }
            if let Ok(index) = carrier_schema.index_of(field.name()) {
                return carrier_input_column(sources.carrier, index, field);
            }
            if let Some(value) = sources.side_inputs.get(field.name()) {
                return runtime_values_input_column(
                    std::iter::repeat_n(Some(value), row_count),
                    row_count,
                    field,
                );
            }
            if let Some((namespace, field_name)) = field.name().split_once('.') {
                if namespace == INGEST_METADATA_NAMESPACE {
                    return runtime_values_input_column(
                        (0..row_count).map(|row| {
                            sources
                                .ingest_metadata
                                .and_then(|metadata| metadata.get(row))
                                .and_then(|metadata| metadata.metadata_value(field_name))
                        }),
                        row_count,
                        field,
                    );
                }
                if namespace == BRANCH_NAMESPACE {
                    return branch_key_input_column(sources.keys, field_name, field);
                }
                if let Some((_, batch)) = sources
                    .namespace_batches
                    .iter()
                    .find(|(candidate, _)| *candidate == namespace)
                    && let Ok(index) = batch.schema().index_of(field_name)
                {
                    return carrier_input_column(batch, index, field);
                }
                if sources.strict_namespaces.contains(&namespace) {
                    if field.is_nullable() {
                        return runtime_values_input_column(
                            std::iter::repeat_n(None, row_count),
                            row_count,
                            field,
                        );
                    }
                    return Err(format!(
                        "FILTER-MAP input record is missing field '{}'",
                        field.name()
                    ));
                }
                if namespace != INGEST_METADATA_NAMESPACE
                    && let Ok(index) = carrier_schema.index_of(field_name)
                {
                    return carrier_input_column(sources.carrier, index, field);
                }
            }
            if field.is_nullable() {
                return runtime_values_input_column(
                    std::iter::repeat_n(None, row_count),
                    row_count,
                    field,
                );
            }
            Err(format!(
                "FILTER-MAP input record is missing field '{}'",
                field.name()
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    VmTypedBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}

fn carrier_input_column(
    carrier: &RuntimeRecordBatch,
    index: usize,
    field: &arrow_schema::Field,
) -> Result<VmTypedArray, String> {
    let column = carrier.batch().column(index);
    if column.data_type() != field.data_type() {
        return Err(format!(
            "input field '{}' expected {:?}, found carrier column type {:?}",
            field.name(),
            field.data_type(),
            column.data_type()
        ));
    }
    VmTypedArray::try_from_array_ref(column.clone()).map_err(|error| error.to_string())
}

fn branch_key_input_column(
    keys: &[Option<BranchKey>],
    field_name: &str,
    field: &arrow_schema::Field,
) -> Result<VmTypedArray, String> {
    runtime_values_input_column(
        keys.iter()
            .map(|key| key.as_ref().and_then(|key| key.field_value(field_name))),
        keys.len(),
        field,
    )
}

fn runtime_values_input_column<'a>(
    values: impl Iterator<Item = Option<&'a RuntimeValue>>,
    len: usize,
    field: &arrow_schema::Field,
) -> Result<VmTypedArray, String> {
    if let ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz)) =
        field.data_type()
        && (tz.as_ref() == "+00:00" || tz.as_ref() == "UTC")
    {
        let nanos = values
            .map(|value| match value {
                Some(RuntimeValue::Datetime(value)) => {
                    value.timestamp_nanos_opt().map(Some).ok_or_else(|| {
                        format!(
                            "FILTER-MAP input field '{}' datetime is out of nanosecond range",
                            field.name()
                        )
                    })
                }
                Some(value) => Err(format!(
                    "FILTER-MAP input field '{}' expected {:?}, got {}",
                    field.name(),
                    field.data_type(),
                    runtime_value_type_name(value)
                )),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(VmTypedArray::Datetime(
            nanos
                .into_iter()
                .collect::<arrow_array::TimestampNanosecondArray>()
                .with_timezone_utc(),
        ));
    }
    let mut builder = make_builder(field.data_type(), len);
    for value in values {
        append_filter_map_nested_value(builder.as_mut(), field.data_type(), value, field)?;
    }
    VmTypedArray::try_from_array_ref(builder.finish()).map_err(|error| error.to_string())
}

fn relay_state_snapshot_from_side_inputs(
    side_inputs: &HashMap<String, RuntimeValue>,
) -> HashMap<String, RuntimeValue> {
    side_inputs
        .iter()
        .filter(|(name, _)| name.starts_with("relay_state."))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn lookup_generated_input_field<'a>(
    program: &'a CompiledProgramWithMaterializedInterest,
    call_index: usize,
    name: &str,
) -> Option<&'a arrow_schema::Field> {
    if let Ok(index) = program.compiled.input_schema.index_of(name) {
        return Some(program.compiled.input_schema.field(index));
    }
    program.lookup_hash_maps[call_index + 1..]
        .iter()
        .find_map(|call| {
            call.key_program
                .input_schema
                .index_of(name)
                .ok()
                .map(|index| call.key_program.input_schema.field(index))
        })
}

async fn compute_lookup_hash_map_columns(
    program: &CompiledProgramWithMaterializedInterest,
    carrier: &RuntimeRecordBatch,
    namespace_batches: &[(&str, &RuntimeRecordBatch)],
    keys: &[Option<BranchKey>],
    side_inputs: &HashMap<String, RuntimeValue>,
    ingest_metadata: Option<&[IngestFilterMapMetadata]>,
    execution_now: Timestamp,
) -> Result<HashMap<String, VmTypedArray>, String> {
    let mut lookup_columns = HashMap::new();
    if program.lookup_hash_maps.is_empty() {
        return Ok(lookup_columns);
    }
    let row_count = carrier.batch().num_rows();
    for (call_index, call) in program.lookup_hash_maps.iter().enumerate() {
        let uninitialized = VmUninitializedInput {
            fields: call
                .key_program
                .input_schema
                .fields()
                .iter()
                .filter(|field| field.name().starts_with("output."))
                .map(|field| field.name().clone())
                .collect(),
        };
        let vm_batch = project_vm_input_batch(
            &call.key_program.input_schema,
            &VmInputProjectionSources {
                carrier,
                namespace_batches,
                strict_namespaces: &[],
                keys,
                side_inputs,
                ingest_metadata,
                lookup_columns: &lookup_columns,
                uninitialized: Some(&uninitialized),
            },
        )?;
        let result = execute_program_with_selection_in_context(
            &call.key_program,
            &vm_batch,
            &VmExecutionContext {
                now: execution_now,
                injector: None,
            },
        )
        .await
        .map_err(|error| {
            format!(
                "LOOKUP_HASH_MAP key execution failed for hash map '{}' field '{}': {}",
                call.lookup.as_str(),
                call.lookup_field,
                error
            )
        })?;
        let key_column = result
            .batch
            .schema()
            .index_of(&call.generated_field)
            .ok()
            .map(|index| {
                let array = result.batch.column(index).to_array_ref();
                parse_as_type_from_arrow(array.data_type()).map(|ty| (array, ty))
            })
            .transpose()?;
        let mut row_keys: Vec<Option<String>> = vec![None; row_count];
        for (output_row, &input_row) in result.selected_rows.iter().enumerate() {
            if let Some(side_error) = result.batch.errors().row(output_row).first() {
                return Err(format!(
                    "LOOKUP_HASH_MAP key side error {}: {} at {}",
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ));
            }
            let Some((array, ty)) = key_column.as_ref() else {
                continue;
            };
            if let Some(value) = runtime_value_from_arrow_array(
                array.as_ref(),
                ty,
                true,
                output_row,
                &call.generated_field,
            )? {
                row_keys[input_row] = Some(value.to_key_fragment());
            }
        }
        let generated_name = VmCompileNamespace::Internal(InternalFieldNamespace::LookupHashMap)
            .qualified_field_name(&call.generated_field);
        let Some(field) = lookup_generated_input_field(program, call_index, &generated_name) else {
            continue;
        };
        let lookup_values = row_keys
            .iter()
            .map(|key| {
                let Some(row) = key
                    .as_deref()
                    .and_then(|key| call.lookup_runtime.entries.get(key))
                    .copied()
                else {
                    return Ok(None);
                };
                call.lookup_runtime.batch.value(row, &call.lookup_field)
            })
            .collect::<Result<Vec<_>, String>>()?;
        let column = runtime_values_input_column(
            lookup_values.iter().map(Option::as_ref),
            row_count,
            field,
        )?;
        lookup_columns.insert(generated_name, column);
    }
    Ok(lookup_columns)
}

fn vm_output_value(
    batch: &VmTypedBatch,
    row: usize,
    field_name: &str,
) -> Result<Option<RuntimeValue>, String> {
    let column_index = match batch.schema().index_of(field_name) {
        Ok(index) => index,
        Err(_) => return Ok(None),
    };
    let field = batch.schema().field(column_index);
    let array = batch.column(column_index).to_array_ref();
    runtime_value_from_arrow_array(
        array.as_ref(),
        &parse_as_type_from_arrow(field.data_type())?,
        field.is_nullable(),
        row,
        field_name,
    )
}

fn compile_inferencer_input_mappings(
    processor: &Identifier,
    mappings: &[InferencerTensorMapping],
    input_schema: StdArc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    udfs: Option<&UdfExecutor>,
) -> Result<VmCompiledProgram, String> {
    let assignments = mappings
        .iter()
        .map(|mapping| {
            Ok(nervix_models::Assignment {
                target: nervix_models::AssignmentTarget::bare(
                    Identifier::parse(&mapping.tensor).map_err(|error| {
                        format!(
                            "inferencer '{}' tensor name '{}' is not a valid field: {error}",
                            processor, mapping.tensor
                        )
                    })?,
                ),
                value: mapping.expression.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments,
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("input", "mapped_input"),
    )
    .map_err(|reason| {
        format!(
            "inferencer '{}' INPUTS mapping is invalid: {reason}",
            processor
        )
    })?;
    let output_schema = StdArc::new(arrow_schema::Schema::new(
        mappings
            .iter()
            .map(|mapping| {
                arrow_schema::Field::new(
                    &mapping.tensor,
                    crate::runtime_schema::arrow_data_type(&mapping.schema.message_type()),
                    false,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let output_sensitivity = VmSchemaSensitivity::from_sensitive_fields(
        mappings
            .iter()
            .filter(|mapping| {
                expression_reads_sensitive_source(&mapping.expression, &input_sensitivity)
            })
            .map(|mapping| mapping.tensor.clone()),
    );
    compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema.clone(),
        output_sensitivity.clone(),
        [
            VmCompileBinding::readonly("input", input_schema).with_sensitivity(input_sensitivity),
            VmCompileBinding::writeonly("mapped_input", output_schema)
                .with_sensitivity(output_sensitivity),
        ],
        runtime_udf_compile_options(
            udfs,
            VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                ..VmCompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        format!(
            "inferencer '{}' INPUTS compile failed: {}",
            processor, error.message
        )
    })
}

fn vm_typed_batch_to_runtime_batch(batch: &VmTypedBatch) -> Result<RuntimeRecordBatch, String> {
    let record_batch = batch.to_record_batch().map_err(|error| error.to_string())?;
    RuntimeRecordBatch::from_record_batch(batch.schema().clone(), record_batch)
}

fn vm_typed_batch_selected_rows_to_runtime_batch(
    batch: &VmTypedBatch,
    selected_rows: &[usize],
) -> Result<RuntimeRecordBatch, String> {
    if selected_rows.len() == batch.row_count() {
        return vm_typed_batch_to_runtime_batch(batch);
    }
    let selected = selected_rows.iter().copied().collect::<HashSet<_>>();
    let predicate =
        BooleanArray::from_iter((0..batch.row_count()).map(|row| Some(selected.contains(&row))));
    let columns = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(column, field)| {
            let column = filter_arrow_array(column.to_array_ref().as_ref(), &predicate)
                .map_err(|error| error.to_string())?;
            if !field.is_nullable() && column.null_count() > 0 {
                return Err(format!(
                    "required output column '{}' contains null values",
                    field.name()
                ));
            }
            Ok(column)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let record_batch = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            batch.schema().clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(selected_rows.len())),
        )
    } else {
        RecordBatch::try_new(batch.schema().clone(), columns)
    }
    .map_err(|error| error.to_string())?;
    RuntimeRecordBatch::from_record_batch(batch.schema().clone(), record_batch)
}

fn relay_schema_for_runtime(
    runtime: &Runtime,
    domain: &Domain,
    relay: &Identifier,
) -> Result<Arc<CompiledSchema>, String> {
    let Some(execution) = runtime.executions.get(domain) else {
        return Err(format!("domain '{}' is not instantiated", domain.as_str()));
    };
    execution.relay_schemas.get(relay).cloned().ok_or_else(|| {
        format!(
            "stream '{}' schema is not instantiated in domain '{}'",
            relay.as_str(),
            domain.as_str()
        )
    })
}

fn relay_branch_schema_for_runtime(
    runtime: &Runtime,
    domain: &Domain,
    relay: &Identifier,
) -> Option<StdArc<arrow_schema::Schema>> {
    runtime
        .executions
        .get(domain)
        .and_then(|execution| execution.relay_branching_schemas.get(relay).cloned())
        .flatten()
}

fn materialized_stream_specs_for_graph(
    runtime: &Runtime,
    domain: &Domain,
    _graph: &SharedActiveGraph,
) -> HashMap<Identifier, RuntimeMaterializedRelaySpec> {
    let Some(execution) = runtime.executions.get(domain) else {
        return HashMap::default();
    };
    execution.materialized_stream_specs.clone()
}

async fn flush_branch_junction(context: JunctionFlushContext<'_>, forwarded: RelayRecordBatch) {
    let JunctionFlushContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        input_relays,
        output_routes,
    } = context;
    if let Some(acks) = dispatch_processor_outputs(
        ProcessorOutputDispatchContext {
            graph,
            branch,
            node_kind,
            source_kind: ModelKind::Junction,
            processor,
            error_policies,
            input_relays,
            filter_source: ProcessorOutputFilterSource::InputRelays,
            resolved_materialized_state: None,
        },
        output_routes,
        forwarded,
    )
    .await
    {
        for ack in acks {
            ack.ack_success();
        }
    }
}

async fn flush_branch_inferencer_output(
    context: InferencerFlushContext<'_>,
    output_buffer: &mut InferencerOutputBuffer,
    output_index: usize,
) {
    let InferencerFlushContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        output_routes,
        resource,
        resource_version,
        file,
        inputs,
        output_schema,
        input_relays,
        session,
    } = context;
    output_routes.routes[output_index].clear_flush_deadline();
    let pending = output_buffer.take_pending();
    if pending.is_empty() {
        return;
    }
    let pending_acks = pending
        .iter()
        .flat_map(|batch| batch.acks.iter().cloned())
        .collect::<Vec<_>>();
    let forwarded = match RelayRecordBatch::concat(pending) {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                pending_acks.iter(),
                format!(
                    "inferencer '{}' failed to concatenate buffered input batches for output: {}",
                    processor.as_str(),
                    error
                ),
            );
            return;
        }
    };

    let version = match branch.runtime.resolve_resource_id(
        &branch.domain,
        resource,
        resource_version,
        resource.as_str(),
    ) {
        Ok(id) => id.version,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                forwarded.acks.iter(),
                error,
            );
            return;
        }
    };
    if session
        .as_ref()
        .is_none_or(|loaded| loaded.version() != version)
    {
        let Some(resource_store) = branch.runtime.resource_store.read().clone() else {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                forwarded.acks.iter(),
                "resource store is not attached".to_string(),
            );
            return;
        };
        let resource_id = ResourceId::new(branch.domain.clone(), resource.clone(), version);
        let path = match resource_store.resolve_content_path(&resource_id, file) {
            Ok(path) => path,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    forwarded.acks.iter(),
                    error.to_string(),
                );
                return;
            }
        };
        match inferencer::OnnxInferencerSession::load(version, &path).await {
            Ok(loaded) => *session = Some(loaded),
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    forwarded.acks.iter(),
                    format!(
                        "inferencer '{}' failed to load resource '{}@{}' file '{}': {}",
                        processor.as_str(),
                        resource.as_str(),
                        version,
                        file,
                        error
                    ),
                );
                return;
            }
        }
    }

    let input_key = forwarded.key.clone();
    let input_keys = forwarded.keys.clone();
    let input_batch = forwarded.batch.clone();
    let messages = match forwarded.try_into_messages() {
        Ok(messages) => messages,
        Err(error_and_batch) => {
            let (error, batch) = *error_and_batch;
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                batch.acks.iter(),
                format!(
                    "inferencer '{}' failed to decode arrow batch: {}",
                    processor.as_str(),
                    error
                ),
            );
            return;
        }
    };

    let execution_mode = inputs
        .first()
        .map(|mapping| &mapping.schema)
        .or_else(|| output_schema.first().map(|declaration| &declaration.schema))
        .map(|schema| {
            if schema.batch_axis().is_some() {
                InferencerExecutionMode::Batched
            } else {
                InferencerExecutionMode::PerMessage
            }
        })
        .unwrap_or(InferencerExecutionMode::PerMessage);
    let Some(session) = session.as_ref() else {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            messages.iter().map(|message| &message.acks),
            format!(
                "inferencer '{}' ONNX session was not loaded",
                processor.as_str()
            ),
        );
        return;
    };
    let input_sensitivity = input_relays
        .first()
        .and_then(|relay| {
            relay_schema_for_runtime(&branch.runtime, &branch.domain, relay)
                .ok()
                .map(|schema| schema.vm_sensitivity())
        })
        .unwrap_or_default();
    let mapped_program = match compile_inferencer_input_mappings(
        processor,
        inputs,
        input_batch.schema().clone(),
        input_sensitivity,
        branch.runtime.udf_executor(&branch.domain).as_ref(),
    ) {
        Ok(program) => program,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                error,
            );
            return;
        }
    };
    let side_inputs = HashMap::default();
    let lookup_columns = HashMap::default();
    let mapped_vm_input = match project_vm_input_batch(
        &mapped_program.input_schema,
        &VmInputProjectionSources {
            carrier: &input_batch,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &input_keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: None,
        },
    ) {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                format!("inferencer '{}' INPUTS batch failed: {error}", processor),
            );
            return;
        }
    };
    let mapped_program = Arc::new(mapped_program);
    let mapped_result = match execute_program_with_selection_in_context(
        &mapped_program,
        &mapped_vm_input,
        &VmExecutionContext {
            now: current_timestamp(),
            injector: None,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                format!(
                    "inferencer '{}' INPUTS execution failed: {error}",
                    processor
                ),
            );
            return;
        }
    };
    let mapped_batch = match vm_typed_batch_to_runtime_batch(&mapped_result.batch) {
        Ok(batch) if batch.batch().num_rows() == messages.len() => batch,
        Ok(batch) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                format!(
                    "inferencer '{}' INPUTS produced {} rows for {} messages",
                    processor,
                    batch.batch().num_rows(),
                    messages.len()
                ),
            );
            return;
        }
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                format!("inferencer '{}' INPUTS output failed: {error}", processor),
            );
            return;
        }
    };
    let output_columns = match session
        .execute(&mapped_batch, inputs, output_schema, execution_mode)
        .await
    {
        Ok(output_fields) => output_fields,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                format!(
                    "inferencer '{}' failed ONNX execution for resource '{}@{}' file '{}': {}",
                    processor.as_str(),
                    resource.as_str(),
                    version,
                    file,
                    error
                ),
            );
            return;
        }
    };
    if output_columns.len() != output_schema.len()
        || output_columns
            .iter()
            .any(|column| column.len() != messages.len())
    {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            messages.iter().map(|message| &message.acks),
            format!(
                "inferencer '{}' returned {} output columns for {} declarations or a column with \
                 an invalid row count for {} input messages",
                processor.as_str(),
                output_columns.len(),
                output_schema.len(),
                messages.len()
            ),
        );
        return;
    }
    let inferencer_tensors = InferencerFilterMapTensors { output_schema };
    let tensor_schema = inferencer_tensors.output_arrow_schema();
    let tensor_batch = match tensor_schema
        .fields()
        .iter()
        .map(|field| {
            let column_index = tensor_schema
                .index_of(field.name())
                .map_err(|error| error.to_string())?;
            runtime_values_input_column(
                output_columns[column_index].iter().map(Some),
                messages.len(),
                field,
            )
            .map(|column| column.to_array_ref())
        })
        .collect::<Result<Vec<_>, _>>()
        .and_then(|columns| {
            RecordBatch::try_new(tensor_schema.clone(), columns).map_err(|error| error.to_string())
        })
        .and_then(|batch| RuntimeRecordBatch::from_record_batch(tensor_schema, batch))
    {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                format!(
                    "inferencer '{}' failed to build output tensor columns: {}",
                    processor.as_str(),
                    error
                ),
            );
            return;
        }
    };
    let output_metadata = messages
        .iter()
        .map(|message| message.record.metadata().clone())
        .collect::<Vec<_>>();
    let output_acks = messages
        .iter()
        .map(|message| message.acks.clone())
        .collect::<Vec<_>>();
    let output_batch = match RelayRecordBatch::from_filtered_parts(
        input_key,
        tensor_batch,
        output_metadata,
        output_acks,
    ) {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                format!(
                    "inferencer '{}' failed to build output batch: {}",
                    processor.as_str(),
                    error
                ),
            );
            return;
        }
    };
    if let Some(acks) = dispatch_processor_output(
        ProcessorOutputDispatchContext {
            graph,
            branch,
            node_kind,
            source_kind: ModelKind::Inferencer,
            processor,
            error_policies,
            input_relays,
            filter_source: ProcessorOutputFilterSource::Inferencer(inferencer_tensors),
            resolved_materialized_state: None,
        },
        output_routes,
        output_batch,
        output_index,
    )
    .await
    {
        for ack in acks {
            ack.ack_success();
        }
    }
}

async fn flush_branch_wasm_processor(
    context: WasmFlushContext<'_>,
    compiled: &mut Option<WasmCompiledBranchProcessor>,
    instance: &mut Option<Box<nervix_wasm::WasmBranchInstance>>,
    ack_map: &mut WasmAckMap,
    next_ack_token: &mut u64,
    pending: &mut Vec<RelayRecordBatch>,
) {
    let WasmFlushContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        input_relays,
        output_routes,
        resource,
        resource_version,
        file,
        limits,
        replicated_state,
    } = context;
    if pending.is_empty() {
        return;
    }
    let grouped_batches = std::mem::take(pending);
    let forwarded = match RelayRecordBatch::concat(grouped_batches.clone()) {
        Ok(forwarded) => forwarded,
        Err(error) => {
            for batch in grouped_batches {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    batch.acks.iter(),
                    format!(
                        "wasm processor '{}' failed to concat arrow batches: {}",
                        processor.as_str(),
                        error
                    ),
                );
            }
            return;
        }
    };

    if output_routes.routes.is_empty() {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            format!(
                "wasm processor '{}' has no output destinations",
                processor.as_str()
            ),
        );
        return;
    }
    let Some(primary_input_relay) = input_relays.first() else {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            format!(
                "wasm processor '{}' has no input relays",
                processor.as_str()
            ),
        );
        return;
    };
    let input_schema =
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, primary_input_relay) {
            Ok(schema) => schema,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    forwarded.acks.iter(),
                    error,
                );
                return;
            }
        };
    let mut output_schemas = Vec::with_capacity(output_routes.routes.len());
    for output in &output_routes.routes {
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, &output.relay) {
            Ok(schema) => output_schemas.push((output.relay.clone(), schema)),
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    forwarded.acks.iter(),
                    error,
                );
                return;
            }
        }
    }

    if let Err(error) = ensure_wasm_processor_instance(
        WasmInstanceContext {
            branch,
            processor,
            resource,
            resource_version,
            file,
            limits,
            guest_input_relay: primary_input_relay,
            input_schema: &input_schema,
            output_schemas: &output_schemas,
            replicated_state,
        },
        compiled,
        instance,
    )
    .await
    {
        branch.runtime.handle_general_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            error,
        );
        return;
    }

    if instance.is_none() {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            format!(
                "wasm processor '{}' instance is unavailable",
                processor.as_str()
            ),
        );
        return;
    }

    let (envelope, input_ack_map) = match wasm_envelope_from_relay_batch(&forwarded, next_ack_token)
    {
        Ok(envelope) => envelope,
        Err(error) => {
            branch.runtime.handle_general_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                forwarded.acks.iter(),
                error,
            );
            return;
        }
    };
    ack_map.extend(input_ack_map);
    let process_result = instance
        .as_mut()
        .expect("WASM instance presence was checked")
        .process_envelope(&envelope)
        .await;
    let outputs = match process_result {
        Ok(outputs) => outputs,
        Err(error) => {
            let resource_limit_exceeded = error.is_resource_limit_exceeded();
            branch.runtime.handle_general_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                ack_map.values().map(|context| &context.acks),
                format!(
                    "wasm processor '{}' failed to process batch: {}",
                    processor.as_str(),
                    error
                ),
            );
            ack_map.clear();
            if resource_limit_exceeded {
                *instance = None;
            }
            return;
        }
    };

    let output_branch_key = branch.key.clone();
    if let Err(error) = dispatch_wasm_output_envelopes(
        WasmOutputContext {
            graph,
            branch,
            node_kind,
            processor,
            error_policies,
            output_routes,
            input_relays,
            input_schema: &input_schema,
            output_schemas: &output_schemas,
            key: &output_branch_key,
            dispatch_error: "failed to forward message",
        },
        outputs,
        ack_map,
    )
    .await
    {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            error,
        );
        return;
    }
    let persist_result =
        persist_wasm_guest_state(&branch.runtime, processor, replicated_state, instance).await;
    if let Err(error) = persist_result {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            std::iter::empty::<&AckSet>(),
            error,
        );
    }
}

struct WasmInstanceContext<'a> {
    branch: &'a BranchRuntime,
    processor: &'a Identifier,
    resource: &'a Identifier,
    resource_version: Option<u64>,
    file: &'a str,
    limits: nervix_models::WasmProcessorLimits,
    guest_input_relay: &'a Identifier,
    input_schema: &'a Arc<CompiledSchema>,
    output_schemas: &'a [(Identifier, Arc<CompiledSchema>)],
    replicated_state: &'a ReplicatedWasmProcessorState,
}

impl Runtime {
    async fn compile_wasm_processor_module(
        &self,
        domain: &Domain,
        processor: &Identifier,
        resource: &Identifier,
        resource_version: Option<u64>,
        file: &str,
    ) -> Result<WasmCompiledBranchProcessor, String> {
        let id = self.resolve_resource_id(domain, resource, resource_version, resource.as_str())?;
        let version = id.version;
        let Some(resource_store) = self.resource_store.read().clone() else {
            return Err("resource store is not attached".to_string());
        };
        let path = resource_store
            .resolve_content_path(&id, file)
            .map_err(|error| error.to_string())?;
        let wasm = tokio::fs::read(&path).await.map_err(|error| {
            format!(
                "failed to read wasm processor '{}' resource '{}@{}' file '{}': {}",
                processor.as_str(),
                resource.as_str(),
                version,
                path.display(),
                error
            )
        })?;
        let compiled = self
            .wasm_runtime
            .compile_processor(&wasm)
            .await
            .map_err(|error| {
                format!(
                    "failed to compile wasm processor '{}' resource '{}@{}' file '{}': {}",
                    processor.as_str(),
                    resource.as_str(),
                    version,
                    file,
                    error
                )
            })?;
        Ok(WasmCompiledBranchProcessor {
            version,
            compiled: Arc::new(compiled),
        })
    }
}

async fn ensure_wasm_processor_instance(
    context: WasmInstanceContext<'_>,
    compiled: &mut Option<WasmCompiledBranchProcessor>,
    instance: &mut Option<Box<nervix_wasm::WasmBranchInstance>>,
) -> Result<(), String> {
    let WasmInstanceContext {
        branch,
        processor,
        resource,
        resource_version,
        file,
        limits,
        guest_input_relay,
        input_schema,
        output_schemas,
        replicated_state,
    } = context;
    let version = branch
        .runtime
        .resolve_resource_id(
            &branch.domain,
            resource,
            resource_version,
            resource.as_str(),
        )?
        .version;
    let needs_compile = compiled
        .as_ref()
        .is_none_or(|compiled| compiled.version != version);
    if needs_compile {
        *compiled = Some(
            branch
                .runtime
                .compile_wasm_processor_module(
                    &branch.domain,
                    processor,
                    resource,
                    Some(version),
                    file,
                )
                .await?,
        );
        *instance = None;
    }

    if instance.is_none() {
        let Some(compiled) = compiled.as_ref() else {
            return Err(format!(
                "wasm processor '{}' was not compiled",
                processor.as_str()
            ));
        };
        let init = WasmBranchInit {
            domain_name: branch.domain.as_str().to_string(),
            domain_type: "runtime".to_string(),
            branch_key: branch
                .key
                .as_ref()
                .map(|key| key.as_str().as_bytes().to_vec()),
            input_schema: input_schema
                .wasm_processor_schema(guest_input_relay.as_str().to_string()),
            output_schemas: output_schemas
                .iter()
                .map(|(relay, schema)| schema.wasm_processor_schema(relay.as_str().to_string()))
                .collect(),
        };
        let clock = RuntimeWasmDomainClock {
            runtime: branch.runtime.clone(),
            domain: branch.domain.clone(),
        };
        let restored_guest_state = replicated_state.restore_guest_state();
        *instance = Some(Box::new(
            compiled
                .compiled
                .instantiate_branch(
                    limits,
                    init,
                    Box::new(clock),
                    restored_guest_state.as_deref(),
                )
                .await
                .map_err(|error| {
                    format!(
                        "failed to instantiate wasm processor '{}' branch '{}': {}",
                        processor.as_str(),
                        branch_key_display(&branch.key),
                        error
                    )
                })?,
        ));
    }
    Ok(())
}

fn wasm_envelope_from_relay_batch(
    batch: &RelayRecordBatch,
    next_ack_token: &mut u64,
) -> Result<(WasmEnvelope, WasmAckMap), String> {
    let arrow_ipc_batch = batch.batch.to_arrow_ipc_bytes()?;
    let row_count = batch.batch.batch().num_rows();
    if row_count != batch.acks.len() || row_count != batch.metadata.len() {
        return Err(format!(
            "wasm input row count {} does not match ack count {} and metadata count {}",
            row_count,
            batch.acks.len(),
            batch.metadata.len()
        ));
    }
    let mut rows = Vec::with_capacity(batch.acks.len());
    let mut ack_map = HashMap::with_capacity(batch.acks.len());
    let input_batch = Arc::new(batch.batch.clone());
    for (input_row, (metadata, acks)) in batch.metadata.iter().zip(batch.acks.iter()).enumerate() {
        let token = *next_ack_token;
        *next_ack_token = next_ack_token.saturating_add(1);
        rows.push(WasmOutputRow {
            tokens: vec![WasmAckToken(token)],
            source_token: Some(WasmAckToken(token)),
        });
        ack_map.insert(
            token,
            WasmAckContext {
                acks: acks.clone(),
                metadata: metadata.clone(),
                input_batch: Arc::clone(&input_batch),
                input_row,
            },
        );
    }
    Ok((
        WasmEnvelope::input(
            arrow_ipc_batch,
            WasmAckSidecar {
                rows,
                acked: Vec::new(),
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        ),
        ack_map,
    ))
}

struct WasmOutputContext<'a> {
    graph: &'a SharedActiveGraph,
    branch: &'a mut BranchRuntime,
    node_kind: &'a str,
    processor: &'a Identifier,
    error_policies: &'a ErrorPolicies,
    output_routes: &'a mut RelayProcessorOutputsNode,
    input_relays: &'a [Identifier],
    input_schema: &'a Arc<CompiledSchema>,
    output_schemas: &'a [(Identifier, Arc<CompiledSchema>)],
    key: &'a Option<BranchKey>,
    dispatch_error: &'static str,
}

struct WasmDecodedOutputBatch {
    batch: RelayRecordBatch,
    uninitialized_columns: HashSet<usize>,
}

impl WasmDecodedOutputBatch {
    fn materialize_uninitialized_for_relay(&mut self) -> Result<(), String> {
        let schema = self.batch.arrow_schema();
        for column_index in &self.uninitialized_columns {
            let field = schema.fields().get(*column_index).ok_or_else(|| {
                format!("uninitialized output column {column_index} is outside the relay schema")
            })?;
            if !field.is_nullable() {
                return Err(format!(
                    "required relay field '{}' remains uninitialized",
                    field.name()
                ));
            }
        }
        self.uninitialized_columns.clear();
        Ok(())
    }
}

#[derive(Debug)]
struct WasmMaterializedOutput {
    output_route_index: usize,
    schema: Arc<CompiledSchema>,
    batch: RuntimeRecordBatch,
    acks: WasmAckSidecar,
    uninitialized_columns: HashSet<usize>,
}

#[derive(Debug, Error)]
enum WasmOutputError {
    #[error("expected an output envelope at callback index {envelope_index}")]
    UnexpectedEnvelopeKind { envelope_index: usize },
    #[error("WASM output group at callback index {envelope_index} has no routed outputs")]
    EmptyOutputGroup { envelope_index: usize },
    #[error("unknown WASM output relay '{output_relay}'")]
    UnknownOutputRelay { output_relay: String },
    #[error(
        "WASM output relay '{output_relay}' has {actual} columns, but its destination schema has \
         {expected} fields"
    )]
    RoutedOutputColumnCountMismatch {
        output_relay: String,
        expected: usize,
        actual: usize,
    },
    #[error("WASM output group has invalid generated Arrow IPC: {reason}")]
    InvalidGeneratedArrowIpc { reason: String },
    #[error("WASM generated Arrow IPC has {actual} record batches instead of exactly one")]
    GeneratedRecordBatchCount { actual: usize },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} ('{field_name}') references \
         generated column {column_index}, but the generated pool has {generated_column_count} \
         columns"
    )]
    GeneratedColumnOutOfRange {
        output_relay: String,
        field_index: usize,
        field_name: String,
        column_index: u32,
        generated_column_count: usize,
    },
    #[error("WASM generated column {column_index} is not referenced by any routed output")]
    UnreferencedGeneratedColumn { column_index: usize },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} ('{field_name}') references \
         incompatible generated column {column_index}: expected {expected}, actual {actual}"
    )]
    GeneratedColumnTypeMismatch {
        output_relay: String,
        field_index: usize,
        field_name: String,
        column_index: u32,
        expected: String,
        actual: String,
    },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} ('{field_name}') references \
         generated column {column_index} with {actual} rows, but the routed output has {expected} \
         rows"
    )]
    GeneratedColumnRowCountMismatch {
        output_relay: String,
        field_index: usize,
        field_name: String,
        column_index: u32,
        expected: usize,
        actual: usize,
    },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} references input column \
         {column_index}, but the input schema has {input_column_count} fields"
    )]
    InputColumnOutOfRange {
        output_relay: String,
        field_index: usize,
        column_index: u32,
        input_column_count: usize,
    },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} references incompatible input \
         column {column_index}: expected {expected}, actual {actual}"
    )]
    InputColumnTypeMismatch {
        output_relay: String,
        field_index: usize,
        column_index: u32,
        expected: String,
        actual: String,
    },
    #[error("WASM output relay '{output_relay}' row {row_index} is missing a source token")]
    MissingSourceToken {
        output_relay: String,
        row_index: usize,
    },
    #[error(
        "WASM output relay '{output_relay}' row {row_index} references unknown source token \
         {token}"
    )]
    UnknownSourceToken {
        output_relay: String,
        row_index: usize,
        token: u64,
    },
    #[error(
        "WASM output relay '{output_relay}' row {row_index} source token {token} is absent from \
         row lineage"
    )]
    SourceTokenNotCarried {
        output_relay: String,
        row_index: usize,
        token: u64,
    },
    #[error("invalid WASM token decision for token {token}: {reason}")]
    InvalidTokenDecision { token: u64, reason: String },
    #[error("failed to build WASM output batch for relay '{output_relay}': {reason}")]
    OutputBatchBuild {
        output_relay: String,
        reason: String,
    },
}

struct WasmOutputValidator<'a> {
    ack_map: &'a WasmAckMap,
    input_schema: &'a Arc<CompiledSchema>,
    output_schemas: &'a [(Identifier, Arc<CompiledSchema>)],
    output_routes: &'a RelayProcessorOutputsNode,
}

impl WasmOutputValidator<'_> {
    fn validate(
        &self,
        outputs: Vec<WasmEnvelope>,
    ) -> Result<Vec<WasmMaterializedOutput>, WasmOutputError> {
        self.validate_token_decisions(&outputs)?;
        let mut materialized = Vec::new();
        for (envelope_index, output) in outputs.into_iter().enumerate() {
            materialized.extend(self.materialize_group(envelope_index, output)?);
        }
        Ok(materialized)
    }

    fn validate_token_decisions(&self, outputs: &[WasmEnvelope]) -> Result<(), WasmOutputError> {
        let mut carried_tokens = HashSet::<u64>::default();
        let mut terminal_tokens = HashSet::<u64>::default();
        for (envelope_index, output) in outputs.iter().enumerate() {
            let WasmEnvelope::Output { outputs, .. } = output else {
                return Err(WasmOutputError::UnexpectedEnvelopeKind { envelope_index });
            };
            if outputs.is_empty() {
                return Err(WasmOutputError::EmptyOutputGroup { envelope_index });
            }
            for output in outputs {
                for (row_index, row) in output.acks.rows.iter().enumerate() {
                    if let Some(source_token) = row.source_token
                        && !self.ack_map.contains_key(&source_token.0)
                    {
                        return Err(WasmOutputError::UnknownSourceToken {
                            output_relay: output.output_relay.clone(),
                            row_index,
                            token: source_token.0,
                        });
                    }
                    let mut row_tokens = HashSet::<u64>::default();
                    for token in &row.tokens {
                        if !self.ack_map.contains_key(&token.0) {
                            return Err(WasmOutputError::InvalidTokenDecision {
                                token: token.0,
                                reason: "carried token is unknown to this branch instance"
                                    .to_string(),
                            });
                        }
                        if !row_tokens.insert(token.0) {
                            return Err(WasmOutputError::InvalidTokenDecision {
                                token: token.0,
                                reason: "token occurs more than once in one output row".to_string(),
                            });
                        }
                        carried_tokens.insert(token.0);
                    }
                }
                for token_set in &output.acks.acked {
                    self.validate_terminal_set(token_set, &mut terminal_tokens, "ACK")?;
                }
                for token_set in &output.acks.nacked {
                    self.validate_terminal_tokens(&token_set.tokens, &mut terminal_tokens, "NACK")?;
                }
                for token_set in &output.acks.message_errors {
                    self.validate_terminal_tokens(
                        &token_set.tokens,
                        &mut terminal_tokens,
                        "message error",
                    )?;
                }
            }
        }
        if let Some(token) = carried_tokens.intersection(&terminal_tokens).next() {
            return Err(WasmOutputError::InvalidTokenDecision {
                token: *token,
                reason: "token is both carried and terminally completed in one callback"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn validate_terminal_set(
        &self,
        token_set: &WasmAckTokenSet,
        terminal_tokens: &mut HashSet<u64>,
        decision: &str,
    ) -> Result<(), WasmOutputError> {
        self.validate_terminal_tokens(&token_set.tokens, terminal_tokens, decision)
    }

    fn validate_terminal_tokens(
        &self,
        tokens: &[WasmAckToken],
        terminal_tokens: &mut HashSet<u64>,
        decision: &str,
    ) -> Result<(), WasmOutputError> {
        for token in tokens {
            if !self.ack_map.contains_key(&token.0) {
                return Err(WasmOutputError::InvalidTokenDecision {
                    token: token.0,
                    reason: format!("terminal {decision} token is unknown to this branch instance"),
                });
            }
            if !terminal_tokens.insert(token.0) {
                return Err(WasmOutputError::InvalidTokenDecision {
                    token: token.0,
                    reason: "token receives more than one terminal decision in one callback"
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    fn materialize_group(
        &self,
        envelope_index: usize,
        output: WasmEnvelope,
    ) -> Result<Vec<WasmMaterializedOutput>, WasmOutputError> {
        let WasmEnvelope::Output {
            generated_arrow_ipc_batch,
            outputs,
        } = output
        else {
            return Err(WasmOutputError::UnexpectedEnvelopeKind { envelope_index });
        };
        if outputs.is_empty() {
            return Err(WasmOutputError::EmptyOutputGroup { envelope_index });
        }
        let generated_batch = self.decode_generated_batch(&generated_arrow_ipc_batch)?;
        let generated_column_count = generated_batch.as_ref().map_or(0, RecordBatch::num_columns);
        let mut referenced_generated_columns = vec![false; generated_column_count];
        let mut materialized = Vec::with_capacity(outputs.len());
        for output in outputs {
            materialized.push(self.materialize_routed_output(
                output,
                generated_batch.as_ref(),
                &mut referenced_generated_columns,
            )?);
        }
        if let Some(column_index) = referenced_generated_columns
            .iter()
            .position(|referenced| !referenced)
        {
            return Err(WasmOutputError::UnreferencedGeneratedColumn { column_index });
        }
        Ok(materialized)
    }

    fn materialize_routed_output(
        &self,
        output: WasmRoutedOutput,
        generated_batch: Option<&RecordBatch>,
        referenced_generated_columns: &mut [bool],
    ) -> Result<WasmMaterializedOutput, WasmOutputError> {
        let WasmRoutedOutput {
            output_relay,
            columns,
            acks,
        } = output;
        let output_identifier =
            Identifier::parse(&output_relay).map_err(|_| WasmOutputError::UnknownOutputRelay {
                output_relay: output_relay.clone(),
            })?;
        let Some(schema) = wasm_output_schema(self.output_schemas, &output_identifier) else {
            return Err(WasmOutputError::UnknownOutputRelay { output_relay });
        };
        let Some(output_route_index) = self
            .output_routes
            .routes
            .iter()
            .position(|route| route.relay == output_identifier)
        else {
            return Err(WasmOutputError::UnknownOutputRelay { output_relay });
        };
        let destination_schema = schema.arrow_schema();
        let destination_fields = destination_schema.fields();
        if columns.len() != destination_fields.len() {
            return Err(WasmOutputError::RoutedOutputColumnCountMismatch {
                output_relay,
                expected: destination_fields.len(),
                actual: columns.len(),
            });
        }
        let has_input_columns = columns.iter().any(WasmOutputColumnRef::is_input);
        self.validate_source_tokens(&output_relay, &acks.rows, has_input_columns)?;
        let mut uninitialized_columns = HashSet::default();
        let arrays = columns
            .into_iter()
            .zip(destination_fields)
            .enumerate()
            .map(|(field_index, (column, destination_field))| match column {
                WasmOutputColumnRef::Generated { column_index } => {
                    let generated_column_count =
                        generated_batch.map_or(0, RecordBatch::num_columns);
                    let generated_index = usize::try_from(column_index).map_err(|_| {
                        WasmOutputError::GeneratedColumnOutOfRange {
                            output_relay: output_relay.clone(),
                            field_index,
                            field_name: destination_field.name().to_string(),
                            column_index,
                            generated_column_count,
                        }
                    })?;
                    let Some(generated_batch) = generated_batch else {
                        return Err(WasmOutputError::GeneratedColumnOutOfRange {
                            output_relay: output_relay.clone(),
                            field_index,
                            field_name: destination_field.name().to_string(),
                            column_index,
                            generated_column_count,
                        });
                    };
                    let generated_schema = generated_batch.schema();
                    let Some(generated_field) = generated_schema.fields().get(generated_index)
                    else {
                        return Err(WasmOutputError::GeneratedColumnOutOfRange {
                            output_relay: output_relay.clone(),
                            field_index,
                            field_name: destination_field.name().to_string(),
                            column_index,
                            generated_column_count,
                        });
                    };
                    let expected_generated_field = destination_field.as_ref().clone().with_name("");
                    if generated_field.as_ref() != &expected_generated_field {
                        return Err(WasmOutputError::GeneratedColumnTypeMismatch {
                            output_relay: output_relay.clone(),
                            field_index,
                            field_name: destination_field.name().to_string(),
                            column_index,
                            expected: format!("{expected_generated_field:?}"),
                            actual: format!("{generated_field:?}"),
                        });
                    }
                    if generated_batch.num_rows() != acks.rows.len() {
                        return Err(WasmOutputError::GeneratedColumnRowCountMismatch {
                            output_relay: output_relay.clone(),
                            field_index,
                            field_name: destination_field.name().to_string(),
                            column_index,
                            expected: acks.rows.len(),
                            actual: generated_batch.num_rows(),
                        });
                    }
                    referenced_generated_columns[generated_index] = true;
                    Ok(generated_batch.column(generated_index).clone())
                }
                WasmOutputColumnRef::Input { column_index } => self.materialize_input_column(
                    &output_relay,
                    field_index,
                    destination_field,
                    column_index,
                    &acks.rows,
                ),
                WasmOutputColumnRef::Uninitialized => {
                    uninitialized_columns.insert(field_index);
                    Ok(new_null_array(
                        destination_field.data_type(),
                        acks.rows.len(),
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let record_batch =
            RecordBatch::try_new(destination_schema.clone(), arrays).map_err(|error| {
                WasmOutputError::OutputBatchBuild {
                    output_relay: output_relay.clone(),
                    reason: error.to_string(),
                }
            })?;
        let batch = RuntimeRecordBatch::from_record_batch(destination_schema, record_batch)
            .map_err(|reason| WasmOutputError::OutputBatchBuild {
                output_relay: output_relay.clone(),
                reason,
            })?;
        Ok(WasmMaterializedOutput {
            output_route_index,
            schema: Arc::clone(schema),
            batch,
            acks,
            uninitialized_columns,
        })
    }

    fn decode_generated_batch(&self, ipc: &[u8]) -> Result<Option<RecordBatch>, WasmOutputError> {
        if ipc.is_empty() {
            return Ok(None);
        }
        let invalid = |reason: String| WasmOutputError::InvalidGeneratedArrowIpc { reason };
        let mut cursor = std::io::Cursor::new(ipc);
        let (actual_schema, mut batches) = {
            let reader = StreamReader::try_new(&mut cursor, None)
                .map_err(|error| invalid(error.to_string()))?;
            let actual_schema = reader.schema();
            let batches = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| invalid(error.to_string()))?;
            (actual_schema, batches)
        };
        let consumed = usize::try_from(cursor.position()).unwrap_or(usize::MAX);
        if consumed != ipc.len() {
            return Err(invalid(format!(
                "IPC stream has {} trailing bytes",
                ipc.len().saturating_sub(consumed)
            )));
        }
        if batches.len() != 1 {
            return Err(WasmOutputError::GeneratedRecordBatchCount {
                actual: batches.len(),
            });
        }
        if actual_schema.fields().is_empty() {
            return Err(invalid(
                "encoded zero-column Arrow streams are not valid empty generated pools".to_string(),
            ));
        }
        if let Some(field_index) = actual_schema
            .fields()
            .iter()
            .position(|field| !field.name().is_empty())
        {
            return Err(invalid(format!(
                "generated field {field_index} has non-empty name '{}'",
                actual_schema.field(field_index).name()
            )));
        }
        Ok(batches.pop())
    }

    fn validate_source_tokens(
        &self,
        output_relay: &str,
        rows: &[WasmOutputRow],
        required: bool,
    ) -> Result<(), WasmOutputError> {
        for (row_index, row) in rows.iter().enumerate() {
            let Some(source_token) = row.source_token else {
                if required {
                    return Err(WasmOutputError::MissingSourceToken {
                        output_relay: output_relay.to_string(),
                        row_index,
                    });
                }
                continue;
            };
            if !self.ack_map.contains_key(&source_token.0) {
                return Err(WasmOutputError::UnknownSourceToken {
                    output_relay: output_relay.to_string(),
                    row_index,
                    token: source_token.0,
                });
            }
            if !row.tokens.contains(&source_token) {
                return Err(WasmOutputError::SourceTokenNotCarried {
                    output_relay: output_relay.to_string(),
                    row_index,
                    token: source_token.0,
                });
            }
        }
        Ok(())
    }

    fn materialize_input_column(
        &self,
        output_relay: &str,
        field_index: usize,
        destination_field: &StdArc<arrow_schema::Field>,
        column_index: u32,
        rows: &[WasmOutputRow],
    ) -> Result<ArrayRef, WasmOutputError> {
        let input_index =
            usize::try_from(column_index).map_err(|_| WasmOutputError::InputColumnOutOfRange {
                output_relay: output_relay.to_string(),
                field_index,
                column_index,
                input_column_count: self.input_schema.arrow_schema().fields().len(),
            })?;
        let input_schema = self.input_schema.arrow_schema();
        let Some(source_field) = input_schema.fields().get(input_index) else {
            return Err(WasmOutputError::InputColumnOutOfRange {
                output_relay: output_relay.to_string(),
                field_index,
                column_index,
                input_column_count: input_schema.fields().len(),
            });
        };
        if source_field.data_type() != destination_field.data_type()
            || source_field.is_nullable() != destination_field.is_nullable()
        {
            return Err(WasmOutputError::InputColumnTypeMismatch {
                output_relay: output_relay.to_string(),
                field_index,
                column_index,
                expected: format!("{destination_field:?}"),
                actual: format!("{source_field:?}"),
            });
        }
        if rows.is_empty() {
            return Ok(new_empty_array(destination_field.data_type()));
        }
        let sources = rows
            .iter()
            .enumerate()
            .map(|(row_index, row)| {
                let source_token =
                    row.source_token
                        .ok_or_else(|| WasmOutputError::MissingSourceToken {
                            output_relay: output_relay.to_string(),
                            row_index,
                        })?;
                self.ack_map.get(&source_token.0).ok_or_else(|| {
                    WasmOutputError::UnknownSourceToken {
                        output_relay: output_relay.to_string(),
                        row_index,
                        token: source_token.0,
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let first = sources[0];
        let one_batch = sources
            .iter()
            .all(|source| Arc::ptr_eq(&first.input_batch, &source.input_batch));
        if one_batch {
            let array = first.input_batch.batch().column(input_index);
            let identity = sources.len() == first.input_batch.batch().num_rows()
                && sources
                    .iter()
                    .enumerate()
                    .all(|(row, source)| source.input_row == row);
            if identity {
                return Ok(array.clone());
            }
            let start = first.input_row;
            let contiguous = sources
                .iter()
                .enumerate()
                .all(|(offset, source)| source.input_row == start.saturating_add(offset));
            if contiguous {
                return Ok(array.slice(start, sources.len()));
            }
            let indices = UInt64Array::from_iter_values(
                sources
                    .iter()
                    .map(|source| u64::try_from(source.input_row).unwrap_or(u64::MAX)),
            );
            return take_arrow_array(array.as_ref(), &indices, None).map_err(|error| {
                WasmOutputError::OutputBatchBuild {
                    output_relay: output_relay.to_string(),
                    reason: error.to_string(),
                }
            });
        }
        let slices = sources
            .iter()
            .map(|source| {
                source
                    .input_batch
                    .batch()
                    .column(input_index)
                    .slice(source.input_row, 1)
            })
            .collect::<Vec<_>>();
        let arrays = slices
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        concat_arrow_arrays(&arrays).map_err(|error| WasmOutputError::OutputBatchBuild {
            output_relay: output_relay.to_string(),
            reason: error.to_string(),
        })
    }
}

async fn dispatch_wasm_output_envelopes(
    context: WasmOutputContext<'_>,
    outputs: Vec<WasmEnvelope>,
    ack_map: &mut WasmAckMap,
) -> Result<(), String> {
    let WasmOutputContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        output_routes,
        input_relays,
        input_schema,
        output_schemas,
        key,
        dispatch_error,
    } = context;
    let validated_outputs = match (WasmOutputValidator {
        ack_map,
        input_schema,
        output_schemas,
        output_routes,
    })
    .validate(outputs)
    {
        Ok(outputs) => outputs,
        Err(error) => {
            let reason = format!(
                "wasm processor '{}' produced invalid output: {}",
                processor.as_str(),
                error
            );
            branch.runtime.handle_general_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                ack_map.values().map(|context| &context.acks),
                reason,
            );
            ack_map.clear();
            return Ok(());
        }
    };
    let mut token_use_counts = wasm_output_token_use_counts(&validated_outputs);
    for output in validated_outputs {
        let output_route = &output_routes.routes[output.output_route_index];
        let message_error_relay = output_route.relay.clone();
        let message_error_policy = output_route.message_error_policy.clone();
        apply_wasm_sidecar_terminal_decisions(
            WasmSidecarTerminalContext {
                branch,
                node_kind,
                processor,
                error_policies,
                message_error_relay: &message_error_relay,
                message_error_policy: &message_error_policy,
            },
            ack_map,
            &output.acks,
        )
        .await;
        let output_route = &mut output_routes.routes[output.output_route_index];
        let output_batch = relay_batch_from_wasm_output(
            key,
            output.schema,
            output.batch,
            output.acks.rows,
            output.uninitialized_columns,
            ack_map,
            &mut token_use_counts,
        )?;
        if output_batch.batch.message_count() == 0 {
            continue;
        }
        if let Some(acks) = dispatch_wasm_output_route(
            WasmRouteDispatchContext {
                graph,
                branch,
                node_kind,
                processor,
                error_policies,
                input_relays,
                dispatch_error,
            },
            output_batch,
            output_route,
        )
        .await
        {
            for ack in acks {
                ack.ack_success();
            }
        } else {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                ack_map.values().map(|context| &context.acks),
                format!("wasm processor '{}' {}", processor.as_str(), dispatch_error),
            );
        }
    }
    Ok(())
}

struct WasmRouteDispatchContext<'a> {
    graph: &'a SharedActiveGraph,
    branch: &'a mut BranchRuntime,
    node_kind: &'a str,
    processor: &'a Identifier,
    error_policies: &'a ErrorPolicies,
    input_relays: &'a [Identifier],
    dispatch_error: &'static str,
}

async fn dispatch_wasm_output_route(
    context: WasmRouteDispatchContext<'_>,
    mut decoded: WasmDecodedOutputBatch,
    output: &mut RelayProcessorOutputNode,
) -> Option<Vec<AckSet>> {
    if output.compiled_program.is_none() {
        let Some(primary_input_relay) = context.input_relays.first() else {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "wasm processor '{}' has no input relays",
                        context.processor.as_str()
                    ),
                );
            return None;
        };
        let materialized_stream_specs = materialized_stream_specs_for_graph(
            &context.branch.runtime,
            &context.branch.domain,
            context.graph,
        );
        let current_branching = context
            .branch
            .runtime
            .executions
            .get(&context.branch.domain)
            .and_then(|execution| execution.relay_branchings.get(primary_input_relay).cloned())
            .unwrap_or_default();
        let current_branch_schema = relay_branch_schema_for_runtime(
            &context.branch.runtime,
            &context.branch.domain,
            primary_input_relay,
        );
        let available_lookups = context
            .branch
            .runtime
            .executions
            .get(&context.branch.domain)
            .map(|execution| execution.lookups.clone())
            .unwrap_or_default();
        let udfs = context
            .branch
            .runtime
            .executions
            .get(&context.branch.domain)
            .map(|execution| execution.udfs.clone());
        let output_schema = match relay_schema_for_runtime(
            &context.branch.runtime,
            &context.branch.domain,
            &output.relay,
        ) {
            Ok(schema) => schema,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        decoded.batch.acks.iter(),
                        error,
                    );
                return None;
            }
        };
        match compile_wasm_output_filter_map_program(
            &context.branch.domain,
            context.processor,
            &output.construction,
            output_schema.arrow_schema(),
            output_schema.vm_sensitivity(),
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized_stream_specs,
                available_lookups: &available_lookups,
                current_branching: &current_branching,
                current_branch_schema: current_branch_schema.as_ref(),
                current_branch_sensitivity: None,
                udfs: udfs.as_ref(),
            },
        ) {
            Ok(program) => output.compiled_program = program,
            Err(error) => {
                context
                    .branch
                    .runtime
                    .handle_internal_processor_error_for_acks(
                        &context.branch.domain,
                        context.node_kind,
                        context.processor,
                        context.error_policies,
                        decoded.batch.acks.iter(),
                        error.to_string(),
                    );
                return None;
            }
        }
    }

    let Some(program) = output.compiled_program.as_ref() else {
        if let Err(error) = decoded.materialize_uninitialized_for_relay() {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "wasm processor '{}' failed to materialize output for relay '{}': {}",
                        context.processor.as_str(),
                        output.relay.as_str(),
                        error
                    ),
                );
            return None;
        }
        let dispatched_acks = decoded.batch.acks.to_vec();
        if context
            .branch
            .dispatch_output(
                context.graph,
                output,
                ModelKind::WasmProcessor,
                context.processor,
                &decoded.batch,
            )
            .await
            .is_ok()
        {
            return Some(dispatched_acks);
        }
        context
            .branch
            .runtime
            .handle_internal_processor_error_for_acks(
                &context.branch.domain,
                context.node_kind,
                context.processor,
                context.error_policies,
                decoded.batch.acks.iter(),
                format!(
                    "wasm processor '{}' {} to relay '{}'",
                    context.processor.as_str(),
                    context.dispatch_error,
                    output.relay.as_str()
                ),
            );
        return None;
    };
    let execution_now = context
        .branch
        .runtime
        .current_stream_expiration_time(&context.branch.domain)
        .ok()
        .flatten()
        .unwrap_or_else(current_timestamp);
    let owner_nodes = context
        .branch
        .runtime
        .executions
        .get(&context.branch.domain)
        .map(|execution| execution.materialized_stream_owner_nodes.clone())
        .unwrap_or_default();
    let side_inputs = match context
        .branch
        .runtime
        .load_materialized_side_inputs(
            &context.branch.domain,
            &decoded.batch.key,
            &program.materialized_interest,
            &owner_nodes,
        )
        .await
    {
        Ok(side_inputs) => side_inputs,
        Err(error) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "{} '{}' failed to load materialized side inputs: {}",
                        context.node_kind,
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let output_arrow_schema = decoded.batch.arrow_schema();
    let uninitialized_input = VmUninitializedInput {
        fields: decoded
            .uninitialized_columns
            .iter()
            .filter_map(|column_index| output_arrow_schema.fields().get(*column_index))
            .map(|field| format!("generated.{}", field.name()))
            .collect(),
    };
    let lookup_columns = match compute_lookup_hash_map_columns(
        program,
        &decoded.batch.batch,
        &[],
        &decoded.batch.keys,
        &side_inputs,
        None,
        execution_now,
    )
    .await
    {
        Ok(columns) => columns,
        Err(error) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "{} '{}' failed to prepare LOOKUP_HASH_MAP columns: {}",
                        context.node_kind,
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let vm_input = match project_vm_input_batch(
        &program.compiled.input_schema,
        &VmInputProjectionSources {
            carrier: &decoded.batch.batch,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &decoded.batch.keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: Some(&uninitialized_input),
        },
    ) {
        Ok(input) => input,
        Err(error) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "{} '{}' failed to project WASM output into FILTER-MAP input: {}",
                        context.node_kind,
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let executed = match execute_program_with_selection_in_context(
        &program.compiled,
        &vm_input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    {
        Ok(executed) => executed,
        Err(error) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    decoded.batch.acks.iter(),
                    format!(
                        "{} '{}' FILTER-MAP execution failed: {}",
                        context.node_kind,
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let mut success_output_rows = Vec::new();
    let mut success_input_rows = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, &input_row) in executed.selected_rows.iter().enumerate() {
        if let Some(side_error) = executed.batch.errors().row(output_row).first() {
            let partial_output =
                vm_partial_output_row_to_runtime_batch(&executed.batch, output_row).ok();
            let record = match decoded.batch.runtime_row(input_row) {
                Ok(record) => record,
                Err(error) => {
                    context
                        .branch
                        .runtime
                        .handle_internal_processor_error_for_acks(
                            &context.branch.domain,
                            context.node_kind,
                            context.processor,
                            context.error_policies,
                            decoded.batch.acks.iter(),
                            format!(
                                "{} '{}' failed to address WASM input row {}: {}",
                                context.node_kind,
                                context.processor.as_str(),
                                input_row,
                                error
                            ),
                        );
                    return None;
                }
            };
            message_errors.push(PendingProcessorOutputMessageError {
                row: input_row,
                key: decoded.batch.keys[input_row].clone(),
                record,
                error: program.structured_side_error(
                    format!(
                        "{} '{}' FILTER-MAP side error {}: {} at {}",
                        context.node_kind,
                        context.processor.as_str(),
                        side_error.code.as_str(),
                        side_error.message,
                        side_error.span
                    ),
                    side_error.span,
                    MessageErrorOperation::Set,
                ),
                partial_output,
                materialized_state: relay_state_snapshot_from_side_inputs(&side_inputs),
            });
            continue;
        }
        success_output_rows.push(output_row);
        success_input_rows.push(input_row);
    }
    let mut delivery_counts = vec![0usize; decoded.batch.acks.len()];
    for row in &success_input_rows {
        delivery_counts[*row] += 1;
    }
    for error in &message_errors {
        delivery_counts[error.row] += 1;
    }
    let mut ack_queues = Vec::with_capacity(decoded.batch.acks.len());
    for (row, ack) in decoded.batch.acks.into_iter().enumerate() {
        let delivery_count = delivery_counts[row];
        if delivery_count == 0 {
            ack.ack_success();
            ack_queues.push(VecDeque::new());
            continue;
        }
        let mut queue = VecDeque::with_capacity(delivery_count);
        for _ in 1..delivery_count {
            queue.push_back(ack.attached());
        }
        queue.push_front(ack);
        ack_queues.push(queue);
    }
    let mut planned_errors = Vec::new();
    for error in message_errors {
        let Some(acks) = ack_queues[error.row].pop_front() else {
            continue;
        };
        planned_errors.push(PlannedMessageError {
            message: RelayMessage {
                key: error.key,
                record: error.record,
                acks,
            },
            error: error.error,
            partial_output: error.partial_output,
            materialized_state: error.materialized_state,
        });
    }
    context
        .branch
        .runtime
        .handle_planned_message_errors_with_policy(
            &context.branch.domain,
            context.node_kind,
            context.processor,
            Some(&output.relay),
            &output.message_error_policy,
            planned_errors,
        )
        .await;
    if success_output_rows.is_empty() {
        return Some(Vec::new());
    }
    let output_batch = match vm_typed_batch_selected_rows_to_runtime_batch(
        &executed.batch,
        &success_output_rows,
    ) {
        Ok(batch) => batch,
        Err(error) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    ack_queues.iter().flatten(),
                    format!(
                        "{} '{}' failed to materialize successful FILTER-MAP rows: {}",
                        context.node_kind,
                        context.processor.as_str(),
                        error
                    ),
                );
            return None;
        }
    };
    let metadata = success_input_rows
        .iter()
        .map(|input_row| decoded.batch.metadata[*input_row].clone())
        .collect::<Vec<_>>();
    let mut batch_acks = Vec::with_capacity(success_input_rows.len());
    for row in &success_input_rows {
        let Some(acks) = ack_queues[*row].pop_front() else {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    batch_acks.iter(),
                    "WASM processor output batch ack count does not match selected row count"
                        .to_string(),
                );
            return None;
        };
        batch_acks.push(acks);
    }
    let forwarded = match RelayRecordBatch::from_filtered_parts(
        decoded.batch.key.clone(),
        output_batch,
        metadata,
        batch_acks,
    ) {
        Ok(batch) => batch,
        Err(error) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    context.node_kind,
                    context.processor,
                    context.error_policies,
                    ack_queues.iter().flatten(),
                    error,
                );
            return None;
        }
    };
    let dispatched_acks = forwarded.acks.to_vec();
    if context
        .branch
        .dispatch_output(
            context.graph,
            output,
            ModelKind::WasmProcessor,
            context.processor,
            &forwarded,
        )
        .await
        .is_ok()
    {
        Some(dispatched_acks)
    } else {
        context
            .branch
            .runtime
            .handle_internal_processor_error_for_acks(
                &context.branch.domain,
                context.node_kind,
                context.processor,
                context.error_policies,
                forwarded.acks.iter(),
                format!(
                    "wasm processor '{}' {} to relay '{}'",
                    context.processor.as_str(),
                    context.dispatch_error,
                    output.relay.as_str()
                ),
            );
        None
    }
}

fn wasm_output_schema<'a>(
    output_schemas: &'a [(Identifier, Arc<CompiledSchema>)],
    output_relay: &Identifier,
) -> Option<&'a Arc<CompiledSchema>> {
    output_schemas
        .iter()
        .find_map(|(relay, schema)| (relay == output_relay).then_some(schema))
}

fn wasm_output_token_use_counts(outputs: &[WasmMaterializedOutput]) -> HashMap<u64, usize> {
    let mut token_use_counts = HashMap::<u64, usize>::default();
    for output in outputs {
        for row in &output.acks.rows {
            for token in &row.tokens {
                *token_use_counts.entry(token.0).or_default() += 1;
            }
        }
    }
    token_use_counts
}

struct WasmSidecarTerminalContext<'a> {
    branch: &'a BranchRuntime,
    node_kind: &'a str,
    processor: &'a Identifier,
    error_policies: &'a ErrorPolicies,
    message_error_relay: &'a Identifier,
    message_error_policy: &'a MessageErrorPolicy,
}

async fn apply_wasm_sidecar_terminal_decisions(
    context: WasmSidecarTerminalContext<'_>,
    ack_map: &mut WasmAckMap,
    sidecar: &WasmAckSidecar,
) {
    let WasmSidecarTerminalContext {
        branch,
        node_kind,
        processor,
        error_policies,
        message_error_relay,
        message_error_policy,
    } = context;
    for message_error in &sidecar.message_errors {
        for token in &message_error.tokens {
            let context = ack_map
                .remove(&token.0)
                .expect("message error token should have been validated");
            let record = match context
                .input_batch
                .runtime_row(context.input_row, context.metadata.clone())
            {
                Ok(record) => record,
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        std::iter::once(&context.acks),
                        format!(
                            "wasm processor '{}' failed to materialize message-error input row: {}",
                            processor.as_str(),
                            error
                        ),
                    );
                    continue;
                }
            };
            branch
                .runtime
                .handle_message_error_with_policy(
                    &branch.domain,
                    node_kind,
                    processor,
                    message_error_policy,
                    RelayMessage {
                        key: branch.key.clone(),
                        record,
                        acks: context.acks,
                    },
                    MessageErrorFailure::new(
                        Some(message_error_relay),
                        message_error.reason.clone(),
                        MessageErrorOperation::Wasm,
                    ),
                )
                .await;
        }
    }
    for acked in &sidecar.acked {
        for token in &acked.tokens {
            let context = ack_map
                .remove(&token.0)
                .expect("terminal ACK token should have been validated");
            context.acks.ack_success();
        }
    }
    for nacked in &sidecar.nacked {
        for token in &nacked.tokens {
            let context = ack_map
                .remove(&token.0)
                .expect("terminal NACK token should have been validated");
            context.acks.no_ack(nacked.reason.clone());
        }
    }
}

async fn persist_wasm_guest_state(
    runtime: &Runtime,
    processor: &Identifier,
    replicated_state: &ReplicatedWasmProcessorState,
    instance: &mut Option<Box<nervix_wasm::WasmBranchInstance>>,
) -> Result<(), String> {
    let save_result = match instance.as_mut() {
        Some(instance) => instance.save_state().await,
        None => {
            return Err(format!(
                "wasm processor '{}' instance is unavailable while saving guest state",
                processor.as_str()
            ));
        }
    };
    let guest_state = match save_result {
        Ok(guest_state) => guest_state,
        Err(error) => {
            let resource_limit_exceeded = error.is_resource_limit_exceeded();
            let reason = format!(
                "wasm processor '{}' failed to save guest state: {}",
                processor.as_str(),
                error
            );
            if resource_limit_exceeded {
                *instance = None;
            }
            return Err(reason);
        }
    };
    let (lsm, payload) = replicated_state
        .replace_guest_state(guest_state)
        .map_err(|error| error.to_string())?;
    runtime
        .persist_wasm_processor_snapshot(replicated_state, lsm, &payload)
        .await
}

fn relay_batch_from_wasm_output(
    key: &Option<BranchKey>,
    schema: Arc<CompiledSchema>,
    batch: RuntimeRecordBatch,
    rows: Vec<WasmOutputRow>,
    uninitialized_columns: HashSet<usize>,
    ack_map: &mut WasmAckMap,
    token_use_counts: &mut HashMap<u64, usize>,
) -> Result<WasmDecodedOutputBatch, String> {
    let mut metadata = Vec::with_capacity(rows.len());
    let mut acks = Vec::with_capacity(rows.len());
    for row in rows {
        let source_context = row
            .source_token
            .and_then(|source_token| ack_map.get(&source_token.0));
        metadata.push(source_context.map_or_else(
            || {
                let now = current_timestamp();
                RuntimeRecordMetadata::from_ingested_at_watermarks(now, now)
            },
            |context| context.metadata.clone(),
        ));
        let mut row_ack_sets = Vec::with_capacity(row.tokens.len());
        for token in row.tokens {
            let remaining_uses = token_use_counts
                .get_mut(&token.0)
                .expect("validated token use count should exist");
            if *remaining_uses > 1 {
                *remaining_uses -= 1;
                let context = ack_map
                    .get(&token.0)
                    .expect("validated carried token should remain live");
                row_ack_sets.push(context.acks.attached());
            } else {
                let context = ack_map
                    .remove(&token.0)
                    .expect("last validated token use should remain live");
                row_ack_sets.push(context.acks);
            }
        }
        acks.push(AckSet::merged(row_ack_sets));
    }
    if batch.schema().as_ref() != schema.arrow_schema().as_ref() {
        return Err("WASM output Arrow schema does not match its relay schema".to_string());
    }
    RelayRecordBatch::from_filtered_parts(key.clone(), batch, metadata, acks).map(|batch| {
        WasmDecodedOutputBatch {
            batch,
            uninitialized_columns,
        }
    })
}

fn generator_context_batch(
    schema: &StdArc<arrow_schema::Schema>,
    values: &HashMap<String, RuntimeValue>,
) -> Result<VmTypedBatch, String> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            ArrowDataType::UInt8 => Ok(VmTypedArray::UInt8(arrow_array::UInt8Array::from(vec![
                match values.get(field.name()) {
                    Some(RuntimeValue::U8(value)) => Some(*value),
                    Some(_) => {
                        return Err(format!(
                            "generator input field '{}' has incompatible type",
                            field.name()
                        ));
                    }
                    None => None,
                },
            ]))),
            ArrowDataType::Int8 => Ok(VmTypedArray::Int8(arrow_array::Int8Array::from(vec![
                match values.get(field.name()) {
                    Some(RuntimeValue::I8(value)) => Some(*value),
                    Some(_) => {
                        return Err(format!(
                            "generator input field '{}' has incompatible type",
                            field.name()
                        ));
                    }
                    None => None,
                },
            ]))),
            ArrowDataType::UInt16 => {
                Ok(VmTypedArray::UInt16(arrow_array::UInt16Array::from(vec![
                    match values.get(field.name()) {
                        Some(RuntimeValue::U16(value)) => Some(*value),
                        Some(_) => {
                            return Err(format!(
                                "generator input field '{}' has incompatible type",
                                field.name()
                            ));
                        }
                        None => None,
                    },
                ])))
            }
            ArrowDataType::Int16 => Ok(VmTypedArray::Int16(arrow_array::Int16Array::from(vec![
                match values.get(field.name()) {
                    Some(RuntimeValue::I16(value)) => Some(*value),
                    Some(_) => {
                        return Err(format!(
                            "generator input field '{}' has incompatible type",
                            field.name()
                        ));
                    }
                    None => None,
                },
            ]))),
            ArrowDataType::UInt32 => {
                Ok(VmTypedArray::UInt32(arrow_array::UInt32Array::from(vec![
                    match values.get(field.name()) {
                        Some(RuntimeValue::U32(value)) => Some(*value),
                        Some(_) => {
                            return Err(format!(
                                "generator input field '{}' has incompatible type",
                                field.name()
                            ));
                        }
                        None => None,
                    },
                ])))
            }
            ArrowDataType::Int32 => Ok(VmTypedArray::Int32(arrow_array::Int32Array::from(vec![
                match values.get(field.name()) {
                    Some(RuntimeValue::I32(value)) => Some(*value),
                    Some(_) => {
                        return Err(format!(
                            "generator input field '{}' has incompatible type",
                            field.name()
                        ));
                    }
                    None => None,
                },
            ]))),
            ArrowDataType::UInt64 => {
                Ok(VmTypedArray::UInt64(arrow_array::UInt64Array::from(vec![
                    match values.get(field.name()) {
                        Some(RuntimeValue::U64(value)) => Some(*value),
                        Some(_) => {
                            return Err(format!(
                                "generator input field '{}' has incompatible type",
                                field.name()
                            ));
                        }
                        None => None,
                    },
                ])))
            }
            ArrowDataType::Int64 => Ok(VmTypedArray::Int64(arrow_array::Int64Array::from(vec![
                match values.get(field.name()) {
                    Some(RuntimeValue::I64(value)) => Some(*value),
                    Some(_) => {
                        return Err(format!(
                            "generator input field '{}' has incompatible type",
                            field.name()
                        ));
                    }
                    None => None,
                },
            ]))),
            ArrowDataType::Float32 => Ok(VmTypedArray::Float32(arrow_array::Float32Array::from(
                vec![match values.get(field.name()) {
                    Some(RuntimeValue::F32(value)) => Some(value.into_inner()),
                    Some(_) => {
                        return Err(format!(
                            "generator input field '{}' has incompatible type",
                            field.name()
                        ));
                    }
                    None => None,
                }],
            ))),
            ArrowDataType::Float64 => Ok(VmTypedArray::Float64(arrow_array::Float64Array::from(
                vec![match values.get(field.name()) {
                    Some(RuntimeValue::F64(value)) => Some(value.into_inner()),
                    Some(_) => {
                        return Err(format!(
                            "generator input field '{}' has incompatible type",
                            field.name()
                        ));
                    }
                    None => None,
                }],
            ))),
            ArrowDataType::Boolean => Ok(VmTypedArray::Boolean(arrow_array::BooleanArray::from(
                vec![match values.get(field.name()) {
                    Some(RuntimeValue::Bool(value)) => Some(*value),
                    Some(_) => {
                        return Err(format!(
                            "generator input field '{}' has incompatible type",
                            field.name()
                        ));
                    }
                    None => None,
                }],
            ))),
            ArrowDataType::Utf8 => Ok(VmTypedArray::Utf8(arrow_array::StringArray::from(vec![
                match values.get(field.name()) {
                    Some(RuntimeValue::String(value)) => Some(value.as_str()),
                    Some(_) => {
                        return Err(format!(
                            "generator input field '{}' has incompatible type",
                            field.name()
                        ));
                    }
                    None => None,
                },
            ]))),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
                if tz.as_ref() == "+00:00" =>
            {
                Ok(VmTypedArray::Datetime(
                    arrow_array::TimestampNanosecondArray::from(vec![
                        match values.get(field.name()) {
                            Some(RuntimeValue::Datetime(value)) => value.timestamp_nanos_opt(),
                            Some(_) => {
                                return Err(format!(
                                    "generator input field '{}' has incompatible type",
                                    field.name()
                                ));
                            }
                            None => None,
                        },
                    ])
                    .with_timezone_utc(),
                ))
            }
            other => Err(format!(
                "generator input field '{}' uses unsupported type {:?}",
                field.name(),
                other
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;

    VmTypedBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}

async fn execute_generator_program_on_context(
    program: &CompiledProgramWithMaterializedInterest,
    input: &VmTypedBatch,
    execution_now: Timestamp,
    materialized_state: &HashMap<String, RuntimeValue>,
) -> Result<SingleRecordFilterMapOutcome, String> {
    let result = execute_program_with_selection_in_context(
        &program.compiled,
        input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| format!("GENERATOR execution failed: {error}"))?;
    if result.batch.row_count() == 0 {
        return Ok(SingleRecordFilterMapOutcome::Filtered);
    }
    if result.batch.row_count() != 1 {
        return Err(format!(
            "GENERATOR produced {} rows for a single input key",
            result.batch.row_count()
        ));
    }
    if let Some(side_error) = result.batch.errors().iter().flatten().next() {
        return Ok(SingleRecordFilterMapOutcome::MessageError {
            error: program.structured_side_error(
                format!(
                    "GENERATOR side error {}: {} at {}",
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ),
                side_error.span,
                MessageErrorOperation::Set,
            ),
            partial_output: vm_partial_output_row_to_runtime_batch(&result.batch, 0).ok(),
            materialized_state: materialized_state.clone(),
        });
    }
    let batch = vm_typed_batch_selected_rows_to_runtime_batch(&result.batch, &[0])?;
    RuntimeRow::new(
        Arc::new(batch),
        0,
        RuntimeRecordMetadata::from_ingested_at_watermarks(execution_now, execution_now),
    )
    .map(SingleRecordFilterMapOutcome::Output)
}

fn checked_add_duration_to_timestamp(base: Timestamp, duration: Duration) -> Timestamp {
    let nanos = duration.as_nanos().min(i64::MAX as u128) as i64;
    base.into_datetime()
        .checked_add_signed(TimeDelta::nanoseconds(nanos))
        .map(Timestamp::from)
        .unwrap_or(base)
}

fn advance_scheduled_timestamp(
    next: &mut Option<Timestamp>,
    interval: Duration,
    current: Timestamp,
) {
    let mut scheduled = next.unwrap_or(current);
    while scheduled <= current {
        let advanced = checked_add_duration_to_timestamp(scheduled, interval);
        if advanced <= scheduled {
            break;
        }
        scheduled = advanced;
    }
    *next = Some(scheduled);
}

fn wall_duration_until_timestamp(current: Timestamp, target: Timestamp) -> Duration {
    if target <= current {
        return Duration::ZERO;
    }
    target
        .into_datetime()
        .signed_duration_since(current.into_datetime())
        .to_std()
        .unwrap_or(Duration::ZERO)
}

struct GeneratorFlushContext<'a> {
    runtime: &'a Runtime,
    domain: &'a Domain,
    generator: &'a Identifier,
    output_relay: &'a Identifier,
    output_schema: &'a Arc<CompiledSchema>,
    output_registry: &'a RelayRegistry,
    output_services: &'a Arc<RelayBoundaryServices>,
    task_events: &'a broadcast::Sender<RuntimeEvent>,
}

async fn flush_generator_groups(
    context: GeneratorFlushContext<'_>,
    pending_groups: &mut Vec<(Option<BranchKey>, Vec<RelayMessage>)>,
) {
    let GeneratorFlushContext {
        runtime,
        domain,
        generator,
        output_relay,
        output_schema,
        output_registry,
        output_services,
        task_events,
    } = context;
    for (_key, messages) in std::mem::take(pending_groups) {
        let batch = match RelayRecordBatch::from_messages(output_schema.clone(), messages) {
            Ok(batch) => batch,
            Err(error) => {
                let _ = task_events.send(RuntimeEvent::Error(format!(
                    "failed to build generator batch for '{}' in domain '{}': {}",
                    generator.as_str(),
                    domain.as_str(),
                    error
                )));
                continue;
            }
        };
        if let Err(error) = runtime
            .ingest_stream_boundary_message(
                domain,
                output_relay,
                output_registry,
                output_services,
                &batch,
            )
            .await
        {
            let _ = task_events.send(RuntimeEvent::Error(format!(
                "failed to flush generator '{}' into relay '{}' in domain '{}'",
                generator.as_str(),
                output_relay.as_str(),
                domain.as_str(),
            )));
            drop(error);
        }
    }
}
fn runtime_value_type_name(value: &RuntimeValue) -> &'static str {
    match value {
        RuntimeValue::U8(_) => "U8",
        RuntimeValue::I8(_) => "I8",
        RuntimeValue::U16(_) => "U16",
        RuntimeValue::I16(_) => "I16",
        RuntimeValue::U32(_) => "U32",
        RuntimeValue::I32(_) => "I32",
        RuntimeValue::U64(_) => "U64",
        RuntimeValue::I64(_) => "I64",
        RuntimeValue::Bool(_) => "BOOL",
        RuntimeValue::String(_) => "STRING",
        RuntimeValue::Datetime(_) => "DATETIME",
        RuntimeValue::F32(_) => "F32",
        RuntimeValue::F64(_) => "F64",
        RuntimeValue::Array(_) => "ARRAY",
        RuntimeValue::Vec(_) => "VEC",
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

async fn decode_ingested_payload(
    codec: Arc<CompiledCodec>,
    payload: &[u8],
) -> Result<RuntimeRecordBatch, CodecError> {
    if !codec.requires_blocking_decode() {
        return decode_with_codec(&codec, payload);
    }

    let codec_name = codec.name.as_str().to_string();
    let payload = payload.to_vec();
    tokio::task::spawn_blocking(move || decode_with_codec(&codec, &payload))
        .await
        .map_err(|error| CodecError::InvalidCodec {
            codec: codec_name,
            reason: format!("blocking decode task failed: {error}"),
        })?
}

async fn decode_ingested_payload_owned(
    codec: Arc<CompiledCodec>,
    payload: Vec<u8>,
) -> Result<RuntimeRecordBatch, CodecError> {
    if !codec.requires_blocking_decode() {
        return decode_with_codec_owned(&codec, payload);
    }

    let codec_name = codec.name.as_str().to_string();
    tokio::task::spawn_blocking(move || decode_with_codec_owned(&codec, payload))
        .await
        .map_err(|error| CodecError::InvalidCodec {
            codec: codec_name,
            reason: format!("blocking decode task failed: {error}"),
        })?
}

pub(crate) fn scheduled_branched_stream_owner_nodes(
    schedule: &DomainSchedule,
    relay: &Identifier,
) -> Vec<String> {
    let specs = branched_node_specs_from_models(
        schedule
            .nodes
            .iter()
            .map(|node| (node.kind, node.identifier.clone(), (*node.config).clone())),
    );
    let mut producers = Vec::new();
    for spec in &specs.entrypoints {
        if &spec.root_relay == relay {
            producers.push((spec.kind, spec.identifier.clone()));
        }
    }
    for node_spec in &specs.processors {
        if node_spec.spec.output_relays().contains(relay) {
            producers.push((node_spec.spec.kind, node_spec.spec.processor.clone()));
        }
    }
    let mut owners = BTreeSet::new();
    for (kind, identifier) in producers {
        let Some(producer_node) = schedule
            .nodes
            .iter()
            .find(|node| node.kind == kind && node.identifier == identifier)
        else {
            continue;
        };
        match producer_node.config.as_ref() {
            Model::Ingestor(CreateIngestor {
                source: IngestSource::Endpoint { .. },
                ..
            }) => {
                for owner in &producer_node.assigned_nodes {
                    owners.insert(owner.clone());
                }
            }
            _ => {
                if let Some(owner) = producer_node.execution_node() {
                    owners.insert(owner.to_string());
                }
            }
        }
    }
    owners.into_iter().collect()
}

fn current_timestamp() -> Timestamp {
    Timestamp::now()
}

fn structured_message_error(
    code: MessageErrorCode,
    message: String,
    operation: MessageErrorOperation,
    operation_index: Option<u32>,
    fields: impl IntoIterator<Item = FieldPath>,
) -> StructuredMessageError {
    StructuredMessageError {
        reference: uuid::Uuid::now_v7(),
        code,
        message,
        operation,
        operation_index,
        fields: SortedSet::from_unsorted(fields.into_iter().collect()),
        occurred_at: current_timestamp(),
    }
}

fn planned_structured_message_error(
    message: RelayMessage,
    error: StructuredMessageError,
    partial_output: Option<RuntimeRecordBatch>,
    materialized_state: HashMap<String, RuntimeValue>,
) -> PlannedMessageError {
    PlannedMessageError {
        message,
        error,
        partial_output,
        materialized_state,
    }
}

fn operation_for_filter_label(label: &str) -> MessageErrorOperation {
    match label {
        "FROM WHERE" => MessageErrorOperation::SourceWhere,
        "FILTER WHERE" => MessageErrorOperation::FilterWhere,
        _ => MessageErrorOperation::Set,
    }
}

fn preserved_message_error_branch(
    target_branching: &[Identifier],
    incoming: &Option<BranchKey>,
    relay: &Identifier,
    reference: uuid::Uuid,
) -> Result<Option<BranchKey>, String> {
    match (target_branching.is_empty(), incoming.as_ref()) {
        (true, None) | (false, Some(_)) => Ok(incoming.clone()),
        (true, Some(_)) => Err(format!(
            "unbranched DLQ relay '{}' cannot receive branched message error {}",
            relay, reference
        )),
        (false, None) => Err(format!(
            "branched DLQ relay '{}' cannot receive unbranched message error {}",
            relay, reference
        )),
    }
}

fn vm_partial_output_row_to_runtime_batch(
    batch: &VmTypedBatch,
    row: usize,
) -> Result<RuntimeRecordBatch, String> {
    if row >= batch.row_count() {
        return Err(format!(
            "partial output row {row} is outside batch with {} rows",
            batch.row_count()
        ));
    }
    let fields_and_columns = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .filter_map(|(field, column)| {
            let array = match column {
                VmTypedArray::Uninitialized { .. } => return None,
                column => column.to_array_ref().slice(row, 1),
            };
            let field_name = field
                .name()
                .strip_prefix("output.")
                .unwrap_or(field.name())
                .to_string();
            Some((
                StdArc::new(arrow_schema::Field::new(
                    field_name,
                    field.data_type().clone(),
                    true,
                )),
                array,
            ))
        })
        .collect::<Vec<_>>();
    let (fields, columns): (Vec<_>, Vec<_>) = fields_and_columns.into_iter().unzip();
    let schema = StdArc::new(arrow_schema::Schema::new(fields));
    let record_batch = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
        )
    } else {
        RecordBatch::try_new(schema.clone(), columns)
    }
    .map_err(|error| error.to_string())?;
    RuntimeRecordBatch::from_record_batch(schema, record_batch)
}

fn invalid_output_fields(batch: &VmTypedBatch, row: usize) -> Vec<FieldPath> {
    batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .filter_map(|(field, column)| {
            let invalid = match column {
                VmTypedArray::Uninitialized { .. } => true,
                VmTypedArray::UInt8(array) => array.is_null(row),
                VmTypedArray::Int8(array) => array.is_null(row),
                VmTypedArray::UInt16(array) => array.is_null(row),
                VmTypedArray::Int16(array) => array.is_null(row),
                VmTypedArray::UInt32(array) => array.is_null(row),
                VmTypedArray::Int32(array) => array.is_null(row),
                VmTypedArray::UInt64(array) => array.is_null(row),
                VmTypedArray::Int64(array) => array.is_null(row),
                VmTypedArray::Float32(array) => array.is_null(row),
                VmTypedArray::Float64(array) => array.is_null(row),
                VmTypedArray::Boolean(array) => array.is_null(row),
                VmTypedArray::Utf8(array) => array.is_null(row),
                VmTypedArray::Datetime(array) => array.is_null(row),
                VmTypedArray::Generic(array) => array.is_null(row),
            };
            (invalid && !field.is_nullable()).then(|| {
                FieldPath::new(format!(
                    "output.{}",
                    field.name().strip_prefix("output.").unwrap_or(field.name())
                ))
            })
        })
        .collect()
}

fn domain_clock_window_matches(
    clock: &RuntimeDomainClockState,
    period: Duration,
    skew: Duration,
    event_timestamp: Timestamp,
) -> Result<bool, String> {
    let time_rate = clock.time_rate.parse::<f64>().map_err(|error| {
        format!(
            "invalid time rate '{}' for paced domain clock: {error}",
            clock.time_rate
        )
    })?;
    if !time_rate.is_finite() || time_rate <= 0.0 {
        return Err(format!(
            "invalid time rate '{}' for paced domain clock",
            clock.time_rate
        ));
    }

    let tick_spacing_nanos = ((period.as_nanos() as f64) / time_rate).max(1.0);
    let first_tick = clock.wall_started_at;
    let event_offset_nanos = event_timestamp
        .into_datetime()
        .signed_duration_since(first_tick.into_datetime())
        .num_nanoseconds()
        .unwrap_or(if event_timestamp >= first_tick {
            i64::MAX
        } else {
            i64::MIN
        }) as f64;
    let approx_index = event_offset_nanos / tick_spacing_nanos;
    let candidates = [
        approx_index.floor() as i64 - 1,
        approx_index.floor() as i64,
        approx_index.ceil() as i64,
        approx_index.ceil() as i64 + 1,
        0,
    ];

    for candidate in candidates {
        if candidate < 0 {
            continue;
        }
        let candidate_offset_nanos = (candidate as f64 * tick_spacing_nanos)
            .round()
            .clamp(i64::MIN as f64, i64::MAX as f64) as i64;
        let tick_wall = first_tick
            .into_datetime()
            .checked_add_signed(TimeDelta::nanoseconds(candidate_offset_nanos))
            .map(Timestamp::from);
        let Some(tick_wall) = tick_wall else {
            continue;
        };
        if event_timestamp
            .into_datetime()
            .signed_duration_since(tick_wall.into_datetime())
            .abs()
            .to_std()
            .is_ok_and(|distance| distance <= skew)
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn materialized_record_is_newer(
    existing: &nervix_models::RemoteRuntimeRecordMetadata,
    candidate: &nervix_models::RemoteRuntimeRecordMetadata,
) -> bool {
    let existing_high = existing.ingested_at_high_watermark;
    let candidate_high = candidate.ingested_at_high_watermark;
    if candidate_high != existing_high {
        return candidate_high > existing_high;
    }
    let existing_low = existing.ingested_at_low_watermark;
    let candidate_low = candidate.ingested_at_low_watermark;
    candidate_low > existing_low
}

fn current_domain_logical_time(
    clock: &RuntimeDomainClockState,
    latest_tick: Option<&ObservedDomainTick>,
    wall_now: Timestamp,
) -> Result<Timestamp, String> {
    let time_rate = clock.time_rate.parse::<f64>().map_err(|error| {
        format!(
            "invalid time rate '{}' for paced domain clock: {error}",
            clock.time_rate
        )
    })?;
    if !time_rate.is_finite() || time_rate <= 0.0 {
        return Err(format!(
            "invalid time rate '{}' for paced domain clock",
            clock.time_rate
        ));
    }

    let (anchor_logical, anchor_wall) = if let Some(tick) = latest_tick {
        (tick.logical_timestamp, tick.wall_clock)
    } else {
        (clock.logical_started_at, clock.wall_started_at)
    };
    let wall_elapsed = wall_now
        .into_datetime()
        .signed_duration_since(anchor_wall.into_datetime());
    let wall_elapsed_nanos =
        wall_elapsed
            .num_nanoseconds()
            .unwrap_or(if wall_elapsed < TimeDelta::zero() {
                i64::MIN
            } else {
                i64::MAX
            });
    let logical_elapsed_nanos = ((wall_elapsed_nanos.max(0) as f64) * time_rate)
        .round()
        .clamp(0.0, i64::MAX as f64) as i64;
    Ok(anchor_logical
        .into_datetime()
        .checked_add_signed(TimeDelta::nanoseconds(logical_elapsed_nanos))
        .map(Timestamp::from)
        .unwrap_or(anchor_logical))
}

fn wall_duration_until_logical_target(
    clock: &RuntimeDomainClockState,
    current_logical: Timestamp,
    target_logical: Timestamp,
) -> Result<Duration, String> {
    let time_rate = clock.time_rate.parse::<f64>().map_err(|error| {
        format!(
            "invalid time rate '{}' for paced domain clock: {error}",
            clock.time_rate
        )
    })?;
    if !time_rate.is_finite() || time_rate <= 0.0 {
        return Err(format!(
            "invalid time rate '{}' for paced domain clock",
            clock.time_rate
        ));
    }
    if target_logical <= current_logical {
        return Ok(Duration::ZERO);
    }
    let logical_delta = target_logical
        .into_datetime()
        .signed_duration_since(current_logical.into_datetime())
        .to_std()
        .unwrap_or(Duration::ZERO);
    let wall_delta_nanos = ((logical_delta.as_nanos() as f64) / time_rate)
        .round()
        .clamp(0.0, u64::MAX as f64) as u64;
    Ok(Duration::from_nanos(wall_delta_nanos.max(1)))
}

fn normalize_http_host(host: &str) -> String {
    host.split(':')
        .next()
        .unwrap_or(host)
        .trim()
        .to_ascii_lowercase()
}

fn client_config_value(
    config: &[nervix_models::ClientConfigEntry],
    key: &str,
    missing_message: impl FnOnce() -> String,
) -> Result<String, String> {
    config
        .iter()
        .find(|entry| entry.key.eq_ignore_ascii_case(key))
        .map(|entry| entry.value.clone())
        .ok_or_else(missing_message)
}

fn optional_client_config_value<'a>(
    config: &'a [nervix_models::ClientConfigEntry],
    key: &str,
) -> Option<&'a str> {
    config
        .iter()
        .find(|entry| entry.key.eq_ignore_ascii_case(key))
        .map(|entry| entry.value.as_str())
}

fn optional_bool_client_config_value(
    config: &[nervix_models::ClientConfigEntry],
    key: &str,
) -> Result<Option<bool>, String> {
    let Some(value) = optional_client_config_value(config, key) else {
        return Ok(None);
    };

    if value.eq_ignore_ascii_case("true") {
        Ok(Some(true))
    } else if value.eq_ignore_ascii_case("false") {
        Ok(Some(false))
    } else {
        Err(format!(
            "invalid boolean client config key '{key}' value '{value}'"
        ))
    }
}

fn next_retry_delay(current: Duration, policy: ParsedRetryPolicy) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(policy.max_backoff)
        .min(policy.max_backoff)
}

#[cfg(test)]
mod tests;
