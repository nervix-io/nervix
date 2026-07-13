use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use ahash::{HashMap, HashMapExt, HashSet, RandomState};
use arc_swap::ArcSwapOption;
use arrow_arith::boolean::and_kleene;
use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array,
    builder::{
        BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder, Int8Builder,
        Int16Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
        TimestampNanosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    },
    new_empty_array,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::DataType as ArrowDataType;
use arrow_select::{concat::concat as concat_arrow_arrays, take::take as take_arrow_array};
use chrono::{TimeDelta, TimeZone, Utc};
use dashmap::DashMap;
use fjall::Database;
use futures_util::stream::FuturesUnordered;
use nervix_interconnect::{
    Envelope, RelayPayload, RelayPayloadKind, Transport, TransportMode as InterconnectTransportMode,
};
use nervix_models::{
    AckMode, BranchValueMapping, ClickHouseValueMapping, ClientConfigEntry, ClusterSchedule,
    CodecProtobufConfig, CodecWireFormat, CorrelationTimeoutAction, CorrelatorMatchPolicy,
    CreateClientAzureBlob, CreateClientGcs, CreateClientHttp, CreateClientIcebergRest,
    CreateClientKafka, CreateClientKinesis, CreateClientMqtt, CreateClientNats,
    CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
    CreateClientS3, CreateClientSqs, CreateClientWebsockets, CreateClientZeroMq, CreateCodec,
    CreateEmitter, CreateEndpoint, CreateGenerator, CreateIngestor, CreateLookup, CreateReingestor,
    CreateRelay, CreateSignalingProtocol, CreateWireSchemaStmt, Domain, DomainConfig, DomainPace,
    DomainSchedule, DomainState, DomainTick, EmitSink, EndpointType, ErrorFieldMapping,
    ErrorPolicies, GeneralErrorPolicy, IcebergCatalog, IcebergStorageBackend, IcebergValueMapping,
    Identifier, IngestSource, IngestTimestampSource, KafkaIngestMode, KafkaOffsetMode,
    KafkaPartitionSchedule, KinesisIngestMode, MessageErrorPolicy, Model, ModelKind,
    MongoDbConflictAction, MongoDbValueMapping, MqttIngestMode, MqttQos, MqttSession,
    MySqlConflictAction, MySqlValueMapping, PostgresConflictAction, PostgresValueMapping,
    PulsarIngestMode, RabbitMqIngestMode, RemoteAckOutcome, RemoteAckRegistration,
    RemoteAckResolution, RemoteRuntimeField, ResourceVersionStatus, RetryPolicy, ScheduledNode,
    SqsIngestMode, Timestamp,
};
use nervix_nspl::{
    vm_program::{
        BinaryOp, Expr, FunctionName, InternalFieldNamespace, InternalFieldRef, Literal,
        SpannedExpr, UnaryOp, parse_program,
    },
    window_processor::aggregate::{
        WindowAggregateDemand, WindowAggregateExpr, WindowAggregateFunction,
        WindowAggregateProgram, parse_aggregate_program,
    },
};
#[cfg(test)]
use nervix_vm::SPAWN_BLOCKING_ROW_THRESHOLD as VM_SPAWN_BLOCKING_ROW_THRESHOLD;
use nervix_vm::{
    CompileBinding as VmCompileBinding, CompileNamespace as VmCompileNamespace,
    CompileOptions as VmCompileOptions, CompiledProgram as VmCompiledProgram,
    ExecutionContext as VmExecutionContext, OutputMode as VmOutputMode,
    SchemaSensitivity as VmSchemaSensitivity, TypedArray as VmTypedArray,
    TypedBatch as VmTypedBatch,
    compile_program_for_bindings_with_sensitivity as compile_vm_program_for_bindings_with_sensitivity,
    compile_program_with_options_for_bindings_with_sensitivity as compile_vm_program_with_options_for_bindings_with_sensitivity,
    execute_program_with_selection_in_context,
    infer_set_expr_types_for_bindings as infer_vm_set_expr_types_for_bindings,
};
use nervix_wasm::{
    DomainClock as WasmDomainClock, WasmAckSidecar, WasmAckToken, WasmAckTokenSet, WasmBranchInit,
    WasmEnvelope, WasmOutputColumn, WasmOutputRow, WasmRuntime, WasmRuntimeConfig,
};
use ordered_float::OrderedFloat;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use registry::{ActiveGraph, RuntimeChange, RuntimeChanges};
use tempfile::TempDir;
use thiserror::Error;
use tokio::{
    io::AsyncBufReadExt,
    sync::{Mutex, broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Duration, Instant, sleep, sleep_until},
};
use tokio_stream::StreamExt;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, info, trace, warn};
use upon::Engine as TemplateEngine;

use crate::{
    cluster,
    metrics::{
        NodeBatchObservation, NodeLatencyObservation, NodeWithoutRelayObservation,
        RelayBatchObservation, RelayBufferObservation, RuntimeMetrics,
    },
    resource::ResourceStore,
    runtime_ack::{AckCompletion, AckOutcome, AckProgress, AckSet},
    runtime_schema::{
        CodecError, CompiledCodec, CompiledSchema, DecodedRecord, ProtobufCodecDescriptor,
        RuntimeRecord, RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeValue,
        compile_codec_with_protobuf, compile_schema, decode_with_codec, decode_with_codec_owned,
        encode_with_codec,
    },
};

mod branch_aggregated_state;
mod branch_instance_registry;
mod branch_lru_state;
mod client_config;
mod deduplicator;
mod emitters;
mod http_client;
mod ingestors;
mod kafka_offset_state;
mod materialized_state;
mod planning;
mod processors;
mod relay_batch;
mod relay_channel;
mod runtime_impl;
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
use http_client::HttpClientConfig;
pub(crate) use ingestors::kafka::KafkaIngestor;
use kafka_offset_state::ReplicatedKafkaOffsetState;
use materialized_state::{
    ReplicatedMaterializedRelayState, decode_materialized_stream_snapshot,
    encode_materialized_stream_snapshot_entries,
};
use planning::{
    branched_ingestor_specs_from_active_graph, branched_ingestor_specs_from_models,
    branched_ingestor_specs_from_scheduled_nodes, branched_processor_ids, format_branched_by,
    materialize_branch_instance_template, resolve_concrete_branch,
    resolve_concrete_branch_from_mappings,
};
use processors::{
    BranchInstanceAckBoundary, BranchInstanceTemplate, BranchedIngestorSpec,
    BranchedProcessorOperationSpec, BranchedProcessorOutputSpec, BranchedProcessorOutputsSpec,
    BranchedProcessorSpec, CompiledCorrelatorOutputProgram, CompiledCorrelatorWhereProgram,
    CompiledReordererProgram, CorrelatorBranchState, CorrelatorPendingMessage, FilterMapPlan,
    InferencerFlushContext, JunctionFlushContext, PlannedGeneralError, PlannedMessageError,
    RelayProcessorNode, RelayProcessorOperationNode, RelayProcessorOperationTemplate,
    RelayProcessorOutputNode, RelayProcessorOutputTemplate, RelayProcessorOutputsNode,
    RelayProcessorOutputsTemplate, RelayProcessorRelayTemplate, RelayProcessorTemplate,
    ReorderKeyPart, ReordererPendingMessage, SharedCorrelatorBranchState, WasmAckContext,
    WasmAckMap, WasmCompiledBranchProcessor, WasmFlushContext, WindowBounds, WindowFlushContext,
};
pub use relay_batch::RelayMessage;
pub(crate) use relay_batch::RelayRecordBatch;
use relay_batch::build_stream_record_batch_preserving_acks;
pub(crate) use relay_channel::{RelayBroadcast, RelayReceiver as RelaySubscriptionReceiver};
pub(crate) type RelaySubscriptionRecvError = async_broadcast::RecvError;
use service_url::ServiceUrl;
pub(crate) use state_store::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStateKind, RuntimeStatePlacement,
    RuntimeStateStore,
};
use test_hooks::EmitterFaultMode;
pub use test_hooks::{EmitterFaultInjector, IngestorFaultInjector, RuntimeTestHooks};
use tls::RustlsClientConfigSource;
use wasm_state::ReplicatedWasmProcessorState;
pub(crate) use websocket_signaling::WebsocketSignalingSession;
use window_state::{
    LinearHistogramDelayedRemovalSnapshot, ReplicatedWindowProcessorState,
    WindowAggregateAccumulatorSnapshot, WindowEntrySnapshot, WindowProcessorStateSnapshot,
    WindowSequenceValueSnapshot, WindowSortedCountSnapshot,
};

#[cfg(test)]
const STUPID_CHANNEL_CAPACITY_REMOVE_ME: usize = 1;
const RELAY_BUFFER_DIRECTION_CONCRETE: &str = "concrete";
const BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_STATE_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_STATE_REPLICATION_POLL_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_TEMP_DIR: &str = "/tmp";
const DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL: Duration = Duration::from_secs(1);
const REMOTE_RELAY_INSTANTIATION_WAIT: Duration = Duration::from_secs(5);
const REMOTE_RELAY_INSTANTIATION_POLL: Duration = Duration::from_millis(25);
const REMOTE_ACK_ALIVE_INTERVAL: Duration = Duration::from_millis(100);
const INGEST_MESSAGE_NAMESPACE: &str = "message";
const INGEST_METADATA_NAMESPACE: &str = "metadata";
const INGEST_HEADERS_NAMESPACE: &str = "headers";
const BRANCH_NAMESPACE: &str = "branch";
const WASM_INPUT_NAMESPACE: &str = "input";

pub(crate) type IngestHeaders = Vec<(String, String)>;

type SharedActiveGraph = Arc<ArcSwapOption<ActiveGraph>>;
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

impl RuntimeKey {
    fn new(domain: Domain, identifier: Identifier) -> Self {
        Self { domain, identifier }
    }
}

enum IngestorRuntime {
    Background {
        shutdown: watch::Sender<bool>,
        branched: Vec<Arc<BranchedIngestorRuntime>>,
        tasks: Vec<JoinHandle<()>>,
    },
    Endpoint {
        route_keys: Vec<HttpRouteKey>,
        branched: Vec<Arc<BranchedIngestorRuntime>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BranchKey {
    fields: BTreeMap<Identifier, RuntimeValue>,
    json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConcreteBranch {
    Root,
    Key(BranchKey),
}

impl BranchKey {
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

    fn from_record<'a>(
        record: &RuntimeRecord,
        field_names: impl IntoIterator<Item = &'a Identifier>,
    ) -> Result<Option<Self>, String> {
        let mut fields = BTreeMap::new();
        for field_name in field_names {
            let Some(value) = record.value(field_name.as_str()) else {
                return Ok(None);
            };
            fields.insert(field_name.clone(), value.clone());
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

    fn value(&self, field: &str) -> Option<&RuntimeValue> {
        let field = Identifier::try_from(field.to_string()).ok()?;
        self.fields.get(&field)
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

impl ConcreteBranch {
    fn into_relay_key(self) -> Option<BranchKey> {
        match self {
            Self::Root => None,
            Self::Key(key) => Some(key),
        }
    }
}

struct DomainExecution {
    schedule: DomainSchedule,
    passive_only: bool,
    shutdown: watch::Sender<bool>,
    graph: SharedActiveGraph,
    relay_registries: HashMap<Identifier, RelayRegistry>,
    relay_schemas: HashMap<Identifier, Arc<CompiledSchema>>,
    relay_services: HashMap<Identifier, Arc<RelayBoundaryServices>>,
    lookups: HashMap<Identifier, Arc<LookupRuntime>>,
    relay_branchings: HashMap<Identifier, Vec<Identifier>>,
    relay_branching_schemas: HashMap<Identifier, Option<Arc<arrow_schema::Schema>>>,
    materialized_stream_specs: HashMap<Identifier, RuntimeMaterializedRelaySpec>,
    materialized_stream_owner_nodes: HashMap<Identifier, Option<String>>,
    branched_ingestors: HashMap<Identifier, Vec<BranchedIngestorSpec>>,
    branched_entrypoints: HashMap<Identifier, Vec<Arc<BranchedIngestorRuntime>>>,
    codecs: HashMap<Identifier, Arc<CompiledCodec>>,
    signaling_protocols: HashMap<Identifier, Arc<CreateSignalingProtocol>>,
    endpoint_routes: HashMap<Identifier, EndpointRoute>,
    tasks: Vec<JoinHandle<()>>,
}

#[derive(Debug)]
pub(crate) struct LookupRuntime {
    model: CreateLookup,
    resource_version: u64,
    schema: Arc<CompiledSchema>,
    entries: Arc<HashMap<String, DecodedRecord>>,
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
    attached_runtime_consumer_count: usize,
    detached_runtime_consumer_count: usize,
    remote_runtime_consumers: Arc<[RemoteRuntimeConsumer]>,
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
    signaling_protocol: Option<Arc<CreateSignalingProtocol>>,
}

#[derive(Clone)]
struct EndpointIngestBinding {
    runtime_key: RuntimeKey,
    domain: Domain,
    ingestor: Identifier,
    timestamp_source: Option<IngestTimestampSource>,
    output_routes: RelayProcessorOutputsNode,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
    codec: Arc<CompiledCodec>,
    branching: Vec<Identifier>,
    branch_value_mappings: Vec<BranchValueMapping>,
    branched_senders: HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
}

struct IngestorDependencies {
    output_routes: RelayProcessorOutputsNode,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
    codec: Arc<CompiledCodec>,
    branching: Vec<Identifier>,
    branch_value_mappings: Vec<BranchValueMapping>,
    branched_templates: HashMap<Identifier, (SharedActiveGraph, BranchInstanceTemplate)>,
}

struct IngestDispatch<'a> {
    domain: &'a Domain,
    ingestor: &'a Identifier,
    timestamp_source: Option<&'a IngestTimestampSource>,
    branching: &'a [Identifier],
    branch_value_mappings: Option<&'a [BranchValueMapping]>,
    output_routes: &'a mut RelayProcessorOutputsNode,
    filter_where: Option<&'a CompiledProgramWithMaterializedInterest>,
    branched_senders: &'a HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
    record: DecodedRecord,
    filter_map_metadata: Option<IngestFilterMapMetadata>,
    ingested_at: Timestamp,
    acks: AckSet,
}

#[derive(Clone, Default)]
struct BranchedIngestorRuntimes {
    runtimes: Vec<Arc<BranchedIngestorRuntime>>,
    senders: HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
}

struct IngestBatchSelection<'a> {
    domain: &'a Domain,
    ingestor: &'a Identifier,
    branching: &'a [Identifier],
    branch_value_mappings: Option<&'a [BranchValueMapping]>,
    filter_where: Option<&'a CompiledProgramWithMaterializedInterest>,
    records: &'a [RuntimeRecord],
    filter_map_metadata: Option<&'a [IngestFilterMapMetadata]>,
}

enum BranchedEntrypointInput {
    PendingBranchingBatch(RelayRecordBatch),
    UnresolvedRecord { record: RuntimeRecord, acks: AckSet },
}

struct BranchedEntrypointBatch {
    schema: Arc<CompiledSchema>,
    batch: RuntimeRecordBatch,
    records: Vec<RuntimeRecord>,
    metadata: Vec<RuntimeRecordMetadata>,
    keys: Vec<Option<BranchKey>>,
    acks: Vec<AckSet>,
}

#[derive(Clone)]
struct BranchedBranchSelection {
    key: Option<BranchKey>,
    filters: Vec<(Identifier, RuntimeValue)>,
}

struct BranchedEntrypointRowError {
    record: RuntimeRecord,
    acks: AckSet,
    reason: String,
}

struct BranchedBranchPlan {
    selections: Vec<BranchedBranchSelection>,
    row_errors: Vec<BranchedEntrypointRowError>,
    valid_rows: Vec<(Option<BranchKey>, usize)>,
}

impl BranchedEntrypointInput {
    fn estimated_bytes(&self) -> u64 {
        match self {
            Self::PendingBranchingBatch(batch) => batch.estimated_bytes(),
            Self::UnresolvedRecord { record, .. } => record.estimated_bytes(),
        }
    }
}

impl BranchedEntrypointBatch {
    fn from_inputs(
        schema: Arc<CompiledSchema>,
        inputs: Vec<BranchedEntrypointInput>,
    ) -> Result<Self, (String, Vec<AckSet>)> {
        let mut batches = Vec::<RuntimeRecordBatch>::new();
        let mut records = Vec::<RuntimeRecord>::new();
        let mut metadata = Vec::<RuntimeRecordMetadata>::new();
        let mut keys = Vec::<Option<BranchKey>>::new();
        let mut acks = Vec::<AckSet>::new();
        let mut record_only_inputs = Vec::<RuntimeRecord>::new();
        let mut had_input = false;

        for input in inputs {
            had_input = true;
            match input {
                BranchedEntrypointInput::PendingBranchingBatch(batch) => {
                    Self::push_pending_record_batch(
                        &schema,
                        &mut batches,
                        &mut record_only_inputs,
                        &acks,
                    )?;
                    let (runtime_batch, batch_records, batch_metadata, batch_keys, batch_acks) =
                        batch.into_unkeyed_parts();
                    batches.push(runtime_batch);
                    records.extend(batch_records);
                    metadata.extend(batch_metadata);
                    keys.extend(batch_keys);
                    acks.extend(batch_acks);
                }
                BranchedEntrypointInput::UnresolvedRecord {
                    record,
                    acks: record_acks,
                } => {
                    metadata.push(record.metadata().clone());
                    keys.push(None);
                    record_only_inputs.push(record.clone());
                    records.push(record);
                    acks.push(record_acks);
                }
            }
        }

        if !had_input {
            return Err((
                "cannot build branch batch from zero inputs".to_string(),
                acks,
            ));
        }
        Self::push_pending_record_batch(&schema, &mut batches, &mut record_only_inputs, &acks)?;
        let batch_refs = batches.iter().collect::<Vec<_>>();
        let batch = RuntimeRecordBatch::concat(&batch_refs).map_err(|error| {
            (
                format!("failed to concatenate branch input batches: {error}"),
                acks.clone(),
            )
        })?;
        let row_count = batch.batch().num_rows();
        if records.len() != row_count
            || metadata.len() != row_count
            || keys.len() != row_count
            || acks.len() != row_count
        {
            return Err((
                format!(
                    "branch input batch row count {row_count} does not match records {}, metadata \
                     {}, branch keys {}, acks {}",
                    records.len(),
                    metadata.len(),
                    keys.len(),
                    acks.len()
                ),
                acks,
            ));
        }

        Ok(Self {
            schema,
            batch,
            records,
            metadata,
            keys,
            acks,
        })
    }

    fn push_pending_record_batch(
        schema: &Arc<CompiledSchema>,
        batches: &mut Vec<RuntimeRecordBatch>,
        records: &mut Vec<RuntimeRecord>,
        acks: &[AckSet],
    ) -> Result<(), (String, Vec<AckSet>)> {
        if records.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(records);
        batches.push(schema.arrow_batch_from_records(&pending).map_err(|error| {
            (
                format!("failed to build branch input batch: {error}"),
                acks.to_vec(),
            )
        })?);
        Ok(())
    }

    fn branch_selections(
        &self,
        mappings: &[BranchValueMapping],
        source: &Identifier,
        root_relay: &Identifier,
    ) -> BranchedBranchPlan {
        let mut selections = Vec::<BranchedBranchSelection>::new();
        let mut positions = HashMap::default();
        let mut row_errors = Vec::new();
        let mut valid_rows = Vec::new();
        for (index, record) in self.records.iter().enumerate() {
            match resolve_concrete_branch_from_mappings(
                record,
                self.keys.get(index).and_then(Option::as_ref),
                mappings,
                source,
            ) {
                Ok(branch) => {
                    let key = branch.into_relay_key();
                    valid_rows.push((key.clone(), index));
                    if positions.contains_key(&key) {
                        continue;
                    }
                    let filters = mappings
                        .iter()
                        .filter(|mapping| mapping.relay.as_str() != BRANCH_NAMESPACE)
                        .filter_map(|mapping| {
                            record
                                .value(mapping.relay_field.as_str())
                                .cloned()
                                .map(|value| (mapping.relay_field.clone(), value))
                        })
                        .collect::<Vec<_>>();
                    positions.insert(key.clone(), selections.len());
                    selections.push(BranchedBranchSelection { key, filters });
                }
                Err(error) => {
                    row_errors.push(BranchedEntrypointRowError {
                        record: record.clone(),
                        acks: self.acks[index].clone(),
                        reason: format!(
                            "branch entrypoint '{}' failed to resolve branch for relay '{}': {}",
                            source.as_str(),
                            root_relay.as_str(),
                            error
                        ),
                    });
                }
            }
        }

        BranchedBranchPlan {
            selections,
            row_errors,
            valid_rows,
        }
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
        let mut records = Vec::with_capacity(selected_rows.len());
        let mut metadata = Vec::with_capacity(selected_rows.len());
        let mut acks = Vec::with_capacity(selected_rows.len());
        for row in selected_rows {
            records.push(self.records[row].clone());
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
        RelayRecordBatch::from_filtered_parts(
            selection.key,
            filtered_batch,
            records,
            metadata,
            acks,
        )
        .map_err(|error| (error, self.acks.clone()))
    }

    fn branch_predicate(
        &self,
        selection: &BranchedBranchSelection,
    ) -> Result<BooleanArray, String> {
        let row_count = self.batch.batch().num_rows();
        let mut predicate = None::<BooleanArray>;
        for (field, value) in &selection.filters {
            let field_predicate =
                self.schema
                    .arrow_eq_predicate(&self.batch, field.as_str(), value)?;
            predicate = Some(match predicate {
                Some(current) => and_kleene(&current, &field_predicate)
                    .map_err(|error| format!("branch predicate conjunction failed: {error}"))?,
                None => field_predicate,
            });
        }
        Ok(predicate
            .unwrap_or_else(|| BooleanArray::from_iter(std::iter::repeat_n(Some(true), row_count))))
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
    message: &'a RelayMessage,
    reason: &'a str,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct IngestFilterMapMetadata {
    values: HashMap<String, RuntimeValue>,
    headers: HashMap<String, RuntimeValue>,
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
        if let Some(RuntimeValue::String(existing)) = self.headers.get_mut(&name) {
            existing.push_str(", ");
            existing.push_str(&value);
        } else {
            self.headers.insert(name, RuntimeValue::String(value));
        }
    }

    fn metadata_value(&self, name: &str) -> Option<&RuntimeValue> {
        self.values.get(name)
    }

    fn header_value(&self, name: &str) -> Option<&RuntimeValue> {
        self.headers.get(name)
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
    ) -> Result<(), RelayRecordBatch> {
        let attached_receiver_count = self
            .runtime_consumer_broadcast_for_mode(AckMode::Attached)
            .receiver_count();
        if attached_runtime_consumer_count > 0
            && attached_receiver_count < attached_runtime_consumer_count
        {
            for ack in batch.acks.iter() {
                ack.no_ack("runtime consumer unavailable for attached delivery");
            }
            return Err(batch.clone());
        }
        if attached_receiver_count > 0 {
            let attached = batch.attached();
            if let Err(error) = self
                .runtime_consumer_broadcast_for_mode(AckMode::Attached)
                .broadcast(attached)
                .await
            {
                let failed = error.0;
                for ack in failed.acks.iter() {
                    ack.no_ack("runtime consumer unavailable for attached delivery");
                }
                return Err(batch.clone());
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
    ) -> Result<(), RelayRecordBatch> {
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
    ) -> Result<(), RelayRecordBatch> {
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

    fn try_recv(&mut self) -> Result<Option<RelayRecordBatch>, ()> {
        match self.receiver.try_recv() {
            Ok(batch) => Ok(Some(batch)),
            Err(async_broadcast::TryRecvError::Empty) => Ok(None),
            Err(async_broadcast::TryRecvError::Overflowed(_)) => {
                unreachable!("relay broadcasts are backpressured and must not overflow")
            }
            Err(async_broadcast::TryRecvError::Closed) => Err(()),
        }
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
    processors: HashMap<Identifier, RelayProcessorNode>,
    processors_by_input: HashMap<Identifier, Vec<Identifier>>,
}

fn message_only_error_policies(policy: &MessageErrorPolicy) -> ErrorPolicies {
    ErrorPolicies {
        message: policy.clone(),
        general: GeneralErrorPolicy::Log,
    }
}

fn wasm_error_policies(
    message_policy: &MessageErrorPolicy,
    global_policy: &GeneralErrorPolicy,
) -> ErrorPolicies {
    ErrorPolicies {
        message: message_policy.clone(),
        general: global_policy.clone(),
    }
}

struct BranchedIngestorRuntime {
    domain: Domain,
    ingestor: Identifier,
    sender: mpsc::Sender<BranchedEntrypointInput>,
    shutdown: watch::Sender<bool>,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

struct BranchedBranchDispatchContext<'a> {
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

#[derive(Debug, Clone)]
pub(in crate::runtime) struct RuntimeReconnectBackoff {
    next: Duration,
    max: Duration,
}

impl Default for RuntimeReconnectBackoff {
    fn default() -> Self {
        Self {
            next: Duration::from_millis(250),
            max: Duration::from_secs(30),
        }
    }
}

impl RuntimeReconnectBackoff {
    pub(in crate::runtime) fn reset(&mut self) {
        self.next = Duration::from_millis(250);
    }

    pub(in crate::runtime) fn next_delay(&self) -> Duration {
        self.next
    }

    pub(in crate::runtime) async fn wait(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> bool {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.max);
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
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.max);
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

enum BatchedInput {
    Batch(RelayRecordBatch),
    Closed,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeFlushPolicy {
    Each {
        interval: Duration,
        max_batch_size: u64,
    },
    Immediate,
}

fn relay_batches_estimated_bytes(batches: &[RelayRecordBatch]) -> u64 {
    batches
        .iter()
        .map(RelayRecordBatch::estimated_bytes)
        .sum::<u64>()
}

fn relay_batches_into_batched_input(batches: Vec<RelayRecordBatch>) -> BatchedInput {
    BatchedInput::Batch(
        RelayRecordBatch::concat(batches)
            .expect("relay receive boundary batches must concatenate into one arrow batch"),
    )
}

fn branched_entrypoint_inputs_estimated_bytes(inputs: &[BranchedEntrypointInput]) -> u64 {
    inputs
        .iter()
        .map(BranchedEntrypointInput::estimated_bytes)
        .fold(0_u64, u64::saturating_add)
}

fn branched_entrypoint_inputs_acks(inputs: &[BranchedEntrypointInput]) -> Vec<AckSet> {
    let mut acks = Vec::new();
    for input in inputs {
        match input {
            BranchedEntrypointInput::PendingBranchingBatch(batch) => {
                acks.extend(batch.acks.iter().cloned());
            }
            BranchedEntrypointInput::UnresolvedRecord {
                acks: record_acks, ..
            } => {
                acks.push(record_acks.clone());
            }
        }
    }
    acks
}

async fn branched_entrypoint_batch_from_inputs_blocking(
    schema: Arc<CompiledSchema>,
    inputs: Vec<BranchedEntrypointInput>,
) -> Result<Arc<BranchedEntrypointBatch>, (String, Vec<AckSet>)> {
    let acks = branched_entrypoint_inputs_acks(&inputs);
    match tokio::task::spawn_blocking(move || BranchedEntrypointBatch::from_inputs(schema, inputs))
        .await
    {
        Ok(Ok(batch)) => Ok(Arc::new(batch)),
        Ok(Err(error)) => Err(error),
        Err(error) => Err((
            format!("branch input batch build task failed: {error}"),
            acks,
        )),
    }
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
    relay_branching_schemas: &'a HashMap<Identifier, Option<Arc<arrow_schema::Schema>>>,
    materialized_relay_specs: &'a HashMap<Identifier, RuntimeMaterializedRelaySpec>,
    materialized_relay_owner_nodes: &'a HashMap<Identifier, Option<String>>,
    lookups: &'a HashMap<Identifier, Arc<LookupRuntime>>,
}

#[derive(Debug, Clone)]
struct EmitterTaskDeps {
    input_schema: Arc<CompiledSchema>,
    input_branching: Vec<Identifier>,
    input_branching_schema: Option<Arc<arrow_schema::Schema>>,
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

struct GeneratorTaskSpec {
    generator: CreateGenerator,
    program: Arc<CompiledFilterMapProgram>,
    source_relays: Vec<Identifier>,
    source_nodes: HashMap<Identifier, Option<String>>,
    output_branching: Vec<Identifier>,
    output_schema: Arc<CompiledSchema>,
    output_registry: RelayRegistry,
    output_services: Arc<RelayBoundaryServices>,
}

#[derive(Debug)]
struct WindowEntry {
    sequence: u64,
    timestamp: Timestamp,
    message: RelayMessage,
}

#[derive(Debug)]
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

#[derive(Debug)]
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
    pub(crate) schema: Arc<arrow_schema::Schema>,
    pub(crate) sensitivity: VmSchemaSensitivity,
    pub(crate) branching: Vec<Identifier>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledProgramWithMaterializedInterest {
    pub(crate) compiled: Arc<VmCompiledProgram>,
    pub(crate) output_sensitivity: VmSchemaSensitivity,
    pub(crate) materialized_interest: MaterializedProgramInterest,
    lookup_hash_maps: Vec<LookupHashMapCall>,
}

pub(crate) type EmitterHeaders = Vec<(String, String)>;

#[derive(Debug, Clone)]
pub(crate) struct CompiledEmitterFilterMapProgram {
    pub(crate) body: CompiledProgramWithMaterializedInterest,
    pub(crate) headers: Option<CompiledProgramWithMaterializedInterest>,
    pub(crate) materialized_interest: MaterializedProgramInterest,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RuntimeVmCompileContext<'a> {
    pub(crate) available_materialized_streams:
        &'a HashMap<Identifier, RuntimeMaterializedRelaySpec>,
    pub(crate) available_lookups: &'a HashMap<Identifier, Arc<LookupRuntime>>,
    pub(crate) current_branching: &'a [Identifier],
    pub(crate) current_branch_schema: Option<&'a Arc<arrow_schema::Schema>>,
    pub(crate) current_branch_sensitivity: Option<&'a VmSchemaSensitivity>,
}

impl RuntimeVmCompileContext<'_> {
    fn branch_binding(&self) -> Option<VmCompileBinding> {
        self.current_branch_schema.map(|schema| {
            let sensitivity = self.current_branch_sensitivity.cloned().unwrap_or_default();
            VmCompileBinding::readonly(BRANCH_NAMESPACE, schema.clone())
                .with_sensitivity(sensitivity)
        })
    }
}

#[derive(Debug, Clone)]
struct RuntimeVmSchemaPair {
    input: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    output: Arc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
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

#[derive(Clone)]
pub struct Runtime {
    ingestors: Arc<DashMap<RuntimeKey, IngestorRuntime, RandomState>>,
    ingestors_paused_for_memory_pressure: Arc<AtomicBool>,
    ingestor_transient_errors: Arc<DashMap<RuntimeKey, String, RandomState>>,
    ingestor_reconnect_backoffs: Arc<DashMap<RuntimeKey, RuntimeReconnectStatus, RandomState>>,
    ingestor_readiness: Arc<DashMap<RuntimeKey, IngestorReadiness, RandomState>>,
    emitter_transient_errors: Arc<DashMap<RuntimeKey, String, RandomState>>,
    emitter_reconnect_backoffs: Arc<DashMap<RuntimeKey, RuntimeReconnectStatus, RandomState>>,
    executions: Arc<DashMap<Domain, DomainExecution, RandomState>>,
    schedule_apply_lock: Arc<Mutex<()>>,
    domain_instantiation_errors: Arc<DashMap<Domain, String, RandomState>>,
    domains: Arc<DashMap<Domain, RuntimeDomainState, RandomState>>,
    domain_graphs: Arc<DashMap<Domain, SharedActiveGraph, RandomState>>,
    endpoint_bindings: Arc<DashMap<HttpRouteKey, Vec<EndpointIngestBinding>, RandomState>>,
    relay_boundary_fanouts: RelayBoundaryFanoutMap,
    events: broadcast::Sender<RuntimeEvent>,
    emitter_faults: Arc<EmitterFaultInjector>,
    ingestor_faults: Arc<IngestorFaultInjector>,
    resource_store: Arc<RwLock<Option<Arc<ResourceStore>>>>,
    resource_versions: Arc<RwLock<ResourceVersionStatus>>,
    remote_dispatcher: Arc<RwLock<Option<Arc<RemoteDispatcher>>>>,
    local_node_id: Arc<RwLock<Option<String>>>,
    next_remote_ack_id: Arc<AtomicU64>,
    pending_remote_acks: Arc<DashMap<u64, AckSet, RandomState>>,
    next_state_sync_correlation_id: Arc<AtomicU64>,
    pending_state_syncs: Arc<DashMap<u64, PendingStateSyncSender, RandomState>>,
    expiring_stream_states: Arc<DashMap<RuntimeKey, Arc<ExpiringRelayState>, RandomState>>,
    latest_resource_versions: Arc<DashMap<Identifier, u64, RandomState>>,
    replicated_deduplicator_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedDeduplicatorState>, RandomState>>,
    replicated_kafka_offset_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedKafkaOffsetState>, RandomState>>,
    replicated_materialized_stream_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedMaterializedRelayState>, RandomState>>,
    replicated_window_processor_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedWindowProcessorState>, RandomState>>,
    replicated_wasm_processor_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedWasmProcessorState>, RandomState>>,
    replicated_branch_aggregated_states:
        Arc<DashMap<RuntimeStatePlacement, Arc<ReplicatedBranchAggregatedState>, RandomState>>,
    correlator_states:
        Arc<DashMap<RuntimeStatePlacement, SharedCorrelatorBranchState, RandomState>>,
    wasm_runtime: Arc<WasmRuntime>,
    branch_instance_expiration_scan_interval: Duration,
    state_store: Option<Arc<RuntimeStateStore>>,
    state_snapshot_interval: Duration,
    state_replication_poll_interval: Duration,
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
        let excluded_nodes = self
            .remote_runtime_consumers
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
    ) -> Result<(), RelayRecordBatch> {
        self.fanout
            .dispatch_runtime_consumers(
                self.attached_runtime_consumer_count,
                self.detached_runtime_consumer_count,
                batch,
            )
            .await
    }

    async fn dispatch_remote_runtime_consumers(
        &self,
        domain: &Domain,
        batch: &RelayRecordBatch,
    ) -> Result<(), RelayRecordBatch> {
        if self.remote_runtime_consumers.is_empty() {
            return Ok(());
        }
        for consumer in self.remote_runtime_consumers.iter() {
            let Some(dispatcher) = &self.remote_dispatcher else {
                if consumer.mode == AckMode::Attached {
                    for ack in batch.acks.iter() {
                        ack.no_ack("remote dispatcher unavailable for attached delivery");
                    }
                    return Err(batch.clone());
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
                        return Err(batch.clone());
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
                    return Err(batch.clone());
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
                    return Err(batch.clone());
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

    async fn ingest_message(
        &self,
        metrics: &RuntimeMetrics,
        domain: &Domain,
        relay: &Identifier,
        physical_node_id: Option<&str>,
        batch: &RelayRecordBatch,
    ) -> Result<(), RelayRecordBatch> {
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
    ) -> Result<(), RelayRecordBatch> {
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
    ) -> Result<(), RelayRecordBatch> {
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

    async fn dispatch_boundary(
        &mut self,
        batch: &RelayRecordBatch,
    ) -> Result<(), RelayRecordBatch> {
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
        let mut live_nodes = self.cluster.live_node_ids().await;
        live_nodes.sort();
        live_nodes.dedup();
        for node_id in live_nodes {
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
        let gossip = self.cluster.gossip_state().await;
        let Some(node) = gossip
            .live_nodes
            .into_iter()
            .find(|node| node.node_id == node_id)
        else {
            return Err(format!(
                "remote node '{}' is not present in gossip membership",
                node_id
            ));
        };
        let target_addr = node
            .interconnect_advertise_addr
            .parse()
            .map_err(|error| format!("invalid interconnect address for '{}': {error}", node_id))?;
        let mode = match node.interconnect_mode.as_str() {
            "https" => InterconnectTransportMode::Tls,
            _ => InterconnectTransportMode::Plain,
        };
        let connection = self
            .interconnect
            .connection_for(target_addr, "localhost", mode)
            .await
            .map_err(|error| {
                format!("failed to connect interconnect for '{}': {error}", node_id)
            })?;
        connection
            .send(envelope)
            .await
            .map_err(|error| format!("failed to send remote relay payload: {error}"))
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

fn push_grouped_by_key<K, T>(
    groups: &mut Vec<(K, Vec<T>)>,
    positions: &mut HashMap<K, usize>,
    key: K,
    item: T,
) where
    K: Clone + Eq + std::hash::Hash,
{
    let index = if let Some(index) = positions.get(&key).copied() {
        index
    } else {
        let index = groups.len();
        positions.insert(key.clone(), index);
        groups.push((key, Vec::new()));
        index
    };
    groups[index].1.push(item);
}

#[derive(Debug, Clone, Copy)]
enum ProcessorInputFilterKind {
    FromWhere,
    FilterWhere,
}

impl ProcessorInputFilterKind {
    fn label(self) -> &'static str {
        match self {
            Self::FromWhere => "FROM WHERE",
            Self::FilterWhere => "FILTER WHERE",
        }
    }
}

impl RelayProcessorNode {
    fn refresh(&mut self, graph: Option<Arc<ActiveGraph>>) {
        let changed = match (&self.last_graph, &graph) {
            (Some(previous), Some(current)) => !Arc::ptr_eq(previous, current),
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
            self.generation = self.generation.saturating_add(1);
        }
        self.last_graph = graph;
    }

    async fn filter_input_batch(
        &mut self,
        graph: &SharedActiveGraph,
        branch: &mut BranchRuntime,
        incoming_relay: &Identifier,
        batch: RelayRecordBatch,
    ) -> Option<RelayRecordBatch> {
        let batch = self
            .filter_input_batch_with_kind(
                graph,
                branch,
                incoming_relay,
                batch,
                ProcessorInputFilterKind::FromWhere,
            )
            .await?;
        self.filter_input_batch_with_kind(
            graph,
            branch,
            incoming_relay,
            batch,
            ProcessorInputFilterKind::FilterWhere,
        )
        .await
    }

    async fn filter_input_batch_with_kind(
        &mut self,
        graph: &SharedActiveGraph,
        branch: &mut BranchRuntime,
        incoming_relay: &Identifier,
        batch: RelayRecordBatch,
        kind: ProcessorInputFilterKind,
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
            let filter_input_relays = match kind {
                ProcessorInputFilterKind::FromWhere => vec![incoming_relay.clone()],
                ProcessorInputFilterKind::FilterWhere => self.input_relays.clone(),
            };
            match compile_filter_map_program(
                &branch.domain,
                &self.processor,
                &filter_input_relays,
                Some(&filter_where),
                batch.arrow_schema(),
                input_schema.vm_sensitivity(),
                input_schema.arrow_schema(),
                input_schema.vm_sensitivity(),
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &available_lookups,
                    current_branching: &current_branching,
                    current_branch_schema: current_branch_schema.as_ref(),
                    current_branch_sensitivity: None,
                },
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
        let owner_nodes = branch
            .runtime
            .executions
            .get(&branch.domain)
            .map(|execution| execution.materialized_stream_owner_nodes.clone())
            .unwrap_or_default();
        let side_inputs = match branch
            .runtime
            .load_materialized_side_inputs(
                &branch.domain,
                &batch.key,
                &program.materialized_interest,
                &owner_nodes,
            )
            .await
        {
            Ok(values) => values,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    self.kind.as_str(),
                    &self.processor,
                    &self.error_policies,
                    batch.acks.iter(),
                    format!(
                        "{} '{}' failed to load {} side inputs: {}",
                        self.kind.as_str(),
                        self.processor.as_str(),
                        kind.label(),
                        error
                    ),
                );
                return None;
            }
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
            &side_inputs,
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
            let current = current.as_ref().map(Arc::clone);
            self.refresh(current);
            let Some(batch) = self
                .filter_input_batch(graph, branch, incoming_relay, batch)
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
                        match compile_deduplicator_key_program(
                            &self.processor,
                            &self.input_relays,
                            deduplicate_on,
                            input_arrow_schema.clone(),
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
                    let records = messages
                        .iter()
                        .map(|message| message.record.clone())
                        .collect::<Vec<_>>();
                    let vm_batch = match vm_typed_batch_from_runtime_records(
                        &records,
                        &key_program.program.input_schema,
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
                        &VmExecutionContext { now: execution_now },
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
                            payload = message.record.to_json_string(),
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
                        match state.apply_new_key(dedup_key.clone(), execution_now, *max_time) {
                            Ok(Some(_)) => {
                                forwarded_entries
                                    .push((dedup_key, RelayMessage { key, record, acks }));
                            }
                            Ok(None) => {
                                debug!(
                                    deduplicator = self.processor.as_str(),
                                    deduplicate_on = deduplicate_on.as_str(),
                                    key = dedup_key.as_str(),
                                    "branched deduplicator dropped duplicate message"
                                );
                                acks.ack_success();
                            }
                            Err(error) => {
                                branch.runtime.handle_internal_processor_error_for_acks(
                                    &branch.domain,
                                    self.kind.as_str(),
                                    &self.processor,
                                    &self.error_policies,
                                    std::iter::once(&acks),
                                    format!(
                                        "deduplicator '{}' failed to update state: {}",
                                        self.processor.as_str(),
                                        error
                                    ),
                                );
                            }
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
                        let timestamp = message_timestamp(&message);
                        if let Err(error_and_message) =
                            state.push_message(aggregate, timestamp, message)
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
                                    format!(
                                        "window processor '{}' aggregate input failed: {}",
                                        self.processor.as_str(),
                                        error
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
                    flush_each,
                    compiled_program,
                    pending,
                    arrival_sequence,
                    next_flush,
                } => {
                    if compiled_program.is_none() {
                        match compile_reorderer_program(
                            &self.processor,
                            &self.input_relays,
                            order_by,
                            batch.arrow_schema(),
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
                                    "reorderer '{}' failed to decode arrow batch: {}",
                                    self.processor.as_str(),
                                    error
                                ),
                            );
                            return;
                        }
                    };
                    let records = messages
                        .iter()
                        .map(|message| message.record.clone())
                        .collect::<Vec<_>>();
                    let vm_batch = match vm_typed_batch_from_runtime_records(
                        &records,
                        &program.program.input_schema,
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
                        &VmExecutionContext { now: execution_now },
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
                    for (row, message) in messages.into_iter().enumerate() {
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
                        pending.push(ReordererPendingMessage {
                            key,
                            arrival_sequence: sequence,
                            received_at: execution_now,
                            message,
                        });
                    }
                    match flush_each {
                        RuntimeFlushPolicy::Immediate => {
                            flush_branch_reorderer(
                                ReordererFlushContext {
                                    graph,
                                    branch,
                                    node_kind: self.kind.as_str(),
                                    processor: &self.processor,
                                    error_policies: &self.error_policies,
                                    output_routes,
                                    input_relays: &self.input_relays,
                                },
                                pending,
                                next_flush,
                            )
                            .await;
                        }
                        RuntimeFlushPolicy::Each {
                            interval: duration, ..
                        } => {
                            if next_flush.is_none() {
                                *next_flush = Some(checked_add_duration_to_timestamp(
                                    execution_now,
                                    *duration,
                                ));
                            }
                        }
                    }
                }
                RelayProcessorOperationNode::Correlator {
                    output_routes,
                    left_relays,
                    right_relays,
                    correlate_where,
                    match_policy,
                    output_assignments,
                    max_time: _,
                    flush_each,
                    timeout_policy: _,
                    compiled_where_program,
                    compiled_output_program,
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
                        };
                        match correlate_incoming_message(
                            &self.processor,
                            left_relays,
                            right_relays,
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

                    if compiled_output_program.is_none() {
                        let Some(base_output_relay) = output_routes
                            .routes
                            .first()
                            .map(|output| output.relay.clone())
                        else {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                correlations.iter().flat_map(|(left, right)| {
                                    [&left.message.acks, &right.message.acks]
                                }),
                                format!(
                                    "correlator '{}' has no output destinations",
                                    self.processor.as_str()
                                ),
                            );
                            return;
                        };
                        let Some(left_relay) = left_relays.first() else {
                            branch.runtime.handle_internal_processor_error_for_acks(
                                &branch.domain,
                                self.kind.as_str(),
                                &self.processor,
                                &self.error_policies,
                                correlations.iter().flat_map(|(left, right)| {
                                    [&left.message.acks, &right.message.acks]
                                }),
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
                                correlations.iter().flat_map(|(left, right)| {
                                    [&left.message.acks, &right.message.acks]
                                }),
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
                        let output_schema = match relay_schema_for_runtime(
                            &branch.runtime,
                            &branch.domain,
                            &base_output_relay,
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
                        match (CorrelatorOutputCompileContext {
                            processor: &self.processor,
                            left_relays,
                            left_schema: left_schema.arrow_schema(),
                            right_relays,
                            right_schema: right_schema.arrow_schema(),
                            output_relay: &base_output_relay,
                            output_schema: output_schema.arrow_schema(),
                            output_assignments,
                        })
                        .compile()
                        {
                            Ok(program) => *compiled_output_program = Some(Box::new(program)),
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
                    let Some(output_program) = compiled_output_program.as_ref() else {
                        return;
                    };
                    let mut output_messages = Vec::new();
                    for (left, right) in correlations {
                        match evaluate_correlator_output_message(
                            &self.processor,
                            left_relays,
                            right_relays,
                            output_program,
                            left,
                            right,
                            execution_now,
                        )
                        .await
                        {
                            Ok(message) => output_messages.push(message),
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
                    if output_messages.is_empty() {
                        return;
                    }
                    let should_flush = {
                        let mut state = state.lock();
                        state.output_pending.extend(output_messages);
                        match flush_each {
                            RuntimeFlushPolicy::Immediate => true,
                            RuntimeFlushPolicy::Each {
                                interval,
                                max_batch_size,
                            } => {
                                if state.next_flush.is_none() {
                                    state.next_flush = Some(checked_add_duration_to_timestamp(
                                        execution_now,
                                        *interval,
                                    ));
                                }
                                let due = state
                                    .next_flush
                                    .is_some_and(|deadline| deadline <= execution_now);
                                let estimated_bytes = relay_schema_for_runtime(
                                    &branch.runtime,
                                    &branch.domain,
                                    &output_routes
                                        .routes
                                        .first()
                                        .map(|output| output.relay.clone())
                                        .unwrap_or_else(|| incoming_relay.clone()),
                                )
                                .ok()
                                .and_then(|schema| {
                                    RelayRecordBatch::from_messages(
                                        schema,
                                        state.output_pending.clone(),
                                    )
                                    .ok()
                                })
                                .map(|batch| batch.estimated_bytes())
                                .unwrap_or(*max_batch_size);
                                due || estimated_bytes >= *max_batch_size
                            }
                        }
                    };
                    if should_flush {
                        flush_branch_correlator(CorrelatorFlushContext {
                            graph,
                            branch,
                            node_kind: self.kind.as_str(),
                            processor: &self.processor,
                            error_policies: &self.error_policies,
                            output_routes,
                            state,
                        })
                        .await;
                    }
                }
                RelayProcessorOperationNode::Junction {
                    output_routes,
                    flush_each,
                    pending,
                    next_flush,
                } => {
                    let now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    pending.push(batch);
                    match flush_each {
                        RuntimeFlushPolicy::Immediate => {
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
                                pending,
                                next_flush,
                            )
                            .await;
                        }
                        RuntimeFlushPolicy::Each {
                            interval: flush_each,
                            max_batch_size,
                        } => {
                            if next_flush.is_none() {
                                *next_flush =
                                    Some(checked_add_duration_to_timestamp(now, *flush_each));
                            }
                            if next_flush.is_some_and(|deadline| deadline <= now)
                                || relay_batches_estimated_bytes(pending) >= *max_batch_size
                            {
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
                                    pending,
                                    next_flush,
                                )
                                .await;
                            }
                        }
                    }
                }
                RelayProcessorOperationNode::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    outputs,
                    flush_each,
                    pending,
                    next_flush,
                } => {
                    let now = branch
                        .runtime
                        .current_stream_expiration_time(&branch.domain)
                        .ok()
                        .flatten()
                        .unwrap_or_else(current_timestamp);
                    pending.push(batch);
                    match flush_each {
                        RuntimeFlushPolicy::Immediate => {
                            flush_branch_inferencer(
                                InferencerFlushContext {
                                    branch,
                                    node_kind: self.kind.as_str(),
                                    processor: &self.processor,
                                    error_policies: &self.error_policies,
                                    output_routes,
                                    resource,
                                    resource_version: *resource_version,
                                    file,
                                    inputs,
                                    outputs,
                                },
                                pending,
                                next_flush,
                            )
                            .await;
                        }
                        RuntimeFlushPolicy::Each {
                            interval: flush_each,
                            max_batch_size,
                        } => {
                            if next_flush.is_none() {
                                *next_flush =
                                    Some(checked_add_duration_to_timestamp(now, *flush_each));
                            }
                            if next_flush.is_some_and(|deadline| deadline <= now)
                                || relay_batches_estimated_bytes(pending) >= *max_batch_size
                            {
                                flush_branch_inferencer(
                                    InferencerFlushContext {
                                        branch,
                                        node_kind: self.kind.as_str(),
                                        processor: &self.processor,
                                        error_policies: &self.error_policies,
                                        output_routes,
                                        resource,
                                        resource_version: *resource_version,
                                        file,
                                        inputs,
                                        outputs,
                                    },
                                    pending,
                                    next_flush,
                                )
                                .await;
                            }
                        }
                    }
                }
                RelayProcessorOperationNode::WasmProcessor {
                    output_routes,
                    resource,
                    resource_version,
                    file,
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
            match &mut self.operation {
                RelayProcessorOperationNode::Deduplicator { .. } => {}
                RelayProcessorOperationNode::WindowProcessor {
                    output_routes,
                    width_messages,
                    step_messages,
                    width_duration,
                    step_duration,
                    aggregate,
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
                RelayProcessorOperationNode::Junction {
                    output_routes,
                    flush_each,
                    pending,
                    next_flush,
                    ..
                } => {
                    if let RuntimeFlushPolicy::Each { .. } = flush_each
                        && next_flush.is_some_and(|deadline| deadline <= now)
                    {
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
                            pending,
                            next_flush,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Reorderer {
                    output_routes,
                    max_time,
                    flush_each,
                    pending,
                    next_flush,
                    ..
                } => {
                    let max_time_due = pending.first().is_some_and(|entry| {
                        checked_add_duration_to_timestamp(entry.received_at, *max_time) <= now
                    });
                    let flush_due = if let RuntimeFlushPolicy::Each { .. } = flush_each {
                        next_flush.is_some_and(|deadline| deadline <= now)
                    } else {
                        false
                    };
                    if max_time_due || flush_due {
                        flush_branch_reorderer(
                            ReordererFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                input_relays: &self.input_relays,
                            },
                            pending,
                            next_flush,
                        )
                        .await;
                    }
                }
                RelayProcessorOperationNode::Correlator {
                    output_routes,
                    max_time,
                    flush_each,
                    timeout_policy,
                    state,
                    ..
                } => {
                    let timed_out = {
                        let mut state = state.lock();
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
                    if let RuntimeFlushPolicy::Each { .. } = flush_each {
                        let flush_due = state
                            .lock()
                            .next_flush
                            .is_some_and(|deadline| deadline <= now);
                        if flush_due {
                            flush_branch_correlator(CorrelatorFlushContext {
                                graph,
                                branch,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                state,
                            })
                            .await;
                        }
                    }
                }
                RelayProcessorOperationNode::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    outputs,
                    flush_each,
                    pending,
                    next_flush,
                } => {
                    if let RuntimeFlushPolicy::Each { .. } = flush_each
                        && next_flush.is_some_and(|deadline| deadline <= now)
                    {
                        flush_branch_inferencer(
                            InferencerFlushContext {
                                branch,
                                node_kind: self.kind.as_str(),
                                processor: &self.processor,
                                error_policies: &self.error_policies,
                                output_routes,
                                resource,
                                resource_version: *resource_version,
                                file,
                                inputs,
                                outputs,
                            },
                            pending,
                            next_flush,
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
                    let Some(instance) = instance.as_mut() else {
                        return;
                    };
                    let due_timeouts = instance.take_due_timeout_requests(now);
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
                    let input_schema = match relay_schema_for_runtime(
                        &branch.runtime,
                        &branch.domain,
                        match self.input_relays.first() {
                            Some(input_relay) => input_relay,
                            None => {
                                for (_, context) in std::mem::take(ack_map) {
                                    context.acks.no_ack(format!(
                                        "wasm processor '{}' has no input relays",
                                        self.processor.as_str()
                                    ));
                                }
                                return;
                            }
                        },
                    ) {
                        Ok(schema) => schema,
                        Err(error) => {
                            for (_, context) in std::mem::take(ack_map) {
                                context.acks.no_ack(error.clone());
                            }
                            return;
                        }
                    };
                    let mut output_schemas = Vec::with_capacity(output_routes.routes.len());
                    for output in &output_routes.routes {
                        match relay_schema_for_runtime(
                            &branch.runtime,
                            &branch.domain,
                            &output.relay,
                        ) {
                            Ok(schema) => output_schemas.push((output.relay.clone(), schema)),
                            Err(error) => {
                                for (_, context) in std::mem::take(ack_map) {
                                    context.acks.no_ack(error.clone());
                                }
                                return;
                            }
                        }
                    }
                    let output_key = branch.key.clone();
                    for timeout in due_timeouts {
                        let outputs = match instance.on_timeout(timeout.handle).await {
                            Ok(outputs) => outputs,
                            Err(error) => {
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
                                input_schema: &input_schema,
                                output_schemas: &output_schemas,
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

    fn next_deadline(&self) -> Option<Timestamp> {
        match &self.operation {
            RelayProcessorOperationNode::Deduplicator { .. } => None,
            RelayProcessorOperationNode::WindowProcessor {
                width_duration,
                state,
                ..
            } => window_next_deadline(state, *width_duration),
            RelayProcessorOperationNode::Junction { next_flush, .. } => *next_flush,
            RelayProcessorOperationNode::Reorderer {
                next_flush,
                max_time,
                pending,
                ..
            } => pending
                .first()
                .map(|entry| checked_add_duration_to_timestamp(entry.received_at, *max_time))
                .into_iter()
                .chain(*next_flush)
                .min(),
            RelayProcessorOperationNode::Correlator {
                max_time, state, ..
            } => {
                let state = state.lock();
                state
                    .pending_left
                    .iter()
                    .chain(state.pending_right.iter())
                    .map(|entry| checked_add_duration_to_timestamp(entry.received_at, *max_time))
                    .chain(state.next_flush)
                    .min()
            }
            RelayProcessorOperationNode::Inferencer { next_flush, .. } => *next_flush,
            RelayProcessorOperationNode::WasmProcessor { instance, .. } => {
                wasm_instance_next_deadline(instance.as_deref())
            }
        }
    }
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
            filter_map: output.filter_map.clone(),
            compiled_program: None,
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
            mode: self.mode,
            error_policies: self.error_policies.clone(),
            from_where: self.from_where.clone(),
            compiled_from_where: HashMap::default(),
            filter_where: self.filter_where.clone(),
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
                            RuntimeStatePlacement {
                                domain: domain.clone(),
                                state: RuntimeStateKind::Deduplicator,
                                kind: self.kind,
                                identifier: self.processor.clone(),
                                branch_key: key.clone(),
                            },
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
                } => {
                    let replicated_state = runtime
                        .replicated_window_processor_state(
                            RuntimeStatePlacement {
                                domain: domain.clone(),
                                state: RuntimeStateKind::WindowProcessor,
                                kind: self.kind,
                                identifier: self.processor.clone(),
                                branch_key: key.clone(),
                            },
                            None,
                            Vec::new(),
                            0,
                        )
                        .map_err(|error| error.to_string())?;
                    let state = replicated_state.restore_state(aggregate)?;
                    RelayProcessorOperationNode::WindowProcessor {
                        output_routes: Self::instantiate_outputs(output_routes),
                        width_messages: *width_messages,
                        step_messages: *step_messages,
                        width_duration: *width_duration,
                        step_duration: *step_duration,
                        aggregate: aggregate.clone(),
                        state,
                        replicated_state,
                    }
                }
                RelayProcessorOperationTemplate::Reorderer {
                    output_routes,
                    order_by,
                    max_time,
                    flush_each,
                } => RelayProcessorOperationNode::Reorderer {
                    output_routes: Self::instantiate_outputs(output_routes),
                    order_by: order_by.clone(),
                    max_time: *max_time,
                    flush_each: *flush_each,
                    compiled_program: None,
                    pending: Vec::new(),
                    arrival_sequence: 0,
                    next_flush: None,
                },
                RelayProcessorOperationTemplate::Correlator {
                    output_routes,
                    left_relays,
                    right_relays,
                    correlate_where,
                    match_policy,
                    output_assignments,
                    max_time,
                    flush_each,
                    timeout_policy,
                } => RelayProcessorOperationNode::Correlator {
                    output_routes: Self::instantiate_outputs(output_routes),
                    left_relays: left_relays.clone(),
                    right_relays: right_relays.clone(),
                    correlate_where: correlate_where.clone(),
                    match_policy: *match_policy,
                    output_assignments: output_assignments.clone(),
                    max_time: *max_time,
                    flush_each: *flush_each,
                    timeout_policy: timeout_policy.clone(),
                    compiled_where_program: None,
                    compiled_output_program: None,
                    state: runtime.correlator_state(RuntimeStatePlacement {
                        domain: domain.clone(),
                        state: RuntimeStateKind::Correlator,
                        kind: self.kind,
                        identifier: self.processor.clone(),
                        branch_key: key.clone(),
                    }),
                },
                RelayProcessorOperationTemplate::Junction {
                    output_routes,
                    flush_each,
                } => RelayProcessorOperationNode::Junction {
                    output_routes: Self::instantiate_outputs(output_routes),
                    flush_each: *flush_each,
                    pending: Vec::new(),
                    next_flush: None,
                },
                RelayProcessorOperationTemplate::Inferencer {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    outputs,
                    flush_each,
                } => RelayProcessorOperationNode::Inferencer {
                    output_routes: Self::instantiate_outputs(output_routes),
                    resource: resource.clone(),
                    resource_version: *resource_version,
                    file: file.clone(),
                    inputs: inputs.clone(),
                    outputs: outputs.clone(),
                    flush_each: *flush_each,
                    pending: Vec::new(),
                    next_flush: None,
                },
                RelayProcessorOperationTemplate::WasmProcessor {
                    output_routes,
                    resource,
                    resource_version,
                    file,
                } => {
                    let replicated_state = runtime
                        .replicated_wasm_processor_state(
                            RuntimeStatePlacement {
                                domain: domain.clone(),
                                state: RuntimeStateKind::WasmProcessor,
                                kind: self.kind,
                                identifier: self.processor.clone(),
                                branch_key: key.clone(),
                            },
                            Vec::new(),
                            0,
                        )
                        .map_err(|error| error.to_string())?;
                    RelayProcessorOperationNode::WasmProcessor {
                        output_routes: Self::instantiate_outputs(output_routes),
                        resource: resource.clone(),
                        resource_version: *resource_version,
                        file: file.clone(),
                        compiled: None,
                        instance: None,
                        replicated_state,
                        ack_map: HashMap::default(),
                        next_ack_token: 1,
                        pending: Vec::new(),
                    }
                }
            },
            last_graph: None,
            generation: 0,
        })
    }
}

impl BranchInstanceTemplate {
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
            .map(|relay| {
                let placement = RuntimeStatePlacement {
                    domain: domain.clone(),
                    state: RuntimeStateKind::MaterializedRelay,
                    kind: ModelKind::Materializer,
                    identifier: relay.clone(),
                    branch_key: key.clone(),
                };
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
            processors,
            processors_by_input: self.processors_by_input.clone(),
            error_policies: self.error_policies.clone(),
        }))
    }
}

impl BranchRuntime {
    fn detach(&self) {
        for relay in self.relays.values() {
            relay.registry.remove(&self.key);
            self.runtime
                .remove_stream_key_presence(&self.domain, &relay.relay, &self.key);
        }
    }

    async fn evict(&mut self) {
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

    async fn materialize_stream_batch(&self, relay: &Identifier, batch: &RelayRecordBatch) {
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
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), RelayRecordBatch>> + Send + 'a>,
    > {
        Box::pin(async move {
            let Some(runtime_stream) = self.relays.get_mut(relay) else {
                return Err(batch.clone());
            };
            runtime_stream.dispatch_boundary(batch).await?;
            self.materialize_stream_batch(relay, batch).await;
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

            let processor_ids = self
                .processors_by_input
                .get(relay)
                .cloned()
                .unwrap_or_default();
            for processor_id in processor_ids {
                let Some(mut processor) = self.processors.remove(&processor_id) else {
                    continue;
                };
                self.runtime
                    .metrics
                    .observe_global_node_received(NodeBatchObservation {
                        domain: &self.domain,
                        kind: processor.kind,
                        node: &processor.processor,
                        relay,
                        physical_node_id: self.runtime.local_node_id.read().as_deref(),
                        messages: batch.message_count(),
                        bytes: batch.estimated_bytes(),
                        domain_timestamp: batch.domain_timestamp(),
                    });
                self.runtime.metrics.observe_branch_node_received(
                    branch_key_display(&self.key),
                    NodeBatchObservation {
                        domain: &self.domain,
                        kind: processor.kind,
                        node: &processor.processor,
                        relay,
                        physical_node_id: self.runtime.local_node_id.read().as_deref(),
                        messages: batch.message_count(),
                        bytes: batch.estimated_bytes(),
                        domain_timestamp: batch.domain_timestamp(),
                    },
                );
                self.runtime.mark_branch_aggregated_metrics_updated(
                    &self.domain,
                    processor.kind,
                    &processor.processor,
                );
                let delivery_latencies = batch.delivery_latency_seconds(current_timestamp());
                for seconds in delivery_latencies {
                    self.runtime
                        .metrics
                        .observe_global_delivery_latency_at_domain_time(NodeLatencyObservation {
                            domain: &self.domain,
                            kind: processor.kind,
                            node: &processor.processor,
                            relay,
                            physical_node_id: self.runtime.local_node_id.read().as_deref(),
                            seconds,
                            domain_timestamp: batch.domain_timestamp(),
                        });
                    self.runtime.metrics.observe_branch_delivery_latency(
                        branch_key_display(&self.key),
                        NodeLatencyObservation {
                            domain: &self.domain,
                            kind: processor.kind,
                            node: &processor.processor,
                            relay,
                            physical_node_id: self.runtime.local_node_id.read().as_deref(),
                            seconds,
                            domain_timestamp: batch.domain_timestamp(),
                        },
                    );
                    self.runtime.mark_branch_aggregated_metrics_updated(
                        &self.domain,
                        processor.kind,
                        &processor.processor,
                    );
                }
                let child_message = match processor.mode {
                    AckMode::Attached => batch.attached(),
                    AckMode::Detached => batch.detached(),
                };
                processor.execute(graph, self, relay, child_message).await;
                self.processors.insert(processor_id, processor);
            }

            Ok(())
        })
    }

    async fn dispatch_output(
        &mut self,
        graph: &SharedActiveGraph,
        output: &RelayProcessorOutputNode,
        source_kind: ModelKind,
        source: &Identifier,
        batch: &RelayRecordBatch,
    ) -> Result<(), RelayRecordBatch> {
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

    fn next_deadline(&self) -> Option<Timestamp> {
        self.processors
            .values()
            .filter_map(RelayProcessorNode::next_deadline)
            .min()
    }
}

impl BranchedIngestorRuntime {
    async fn dispatch_entrypoint_inputs(
        context: BranchedBranchDispatchContext<'_>,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
        inputs: Vec<BranchedEntrypointInput>,
    ) -> Option<Timestamp> {
        let BranchedBranchDispatchContext {
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

        let input_batch = match branched_entrypoint_batch_from_inputs_blocking(
            template.entrypoint_schema.clone(),
            inputs,
        )
        .await
        {
            Ok(batch) => batch,
            Err((error, acks)) => {
                let reason = format!(
                    "branched entrypoint '{}' failed to build input batch: {}",
                    ingestor.as_str(),
                    error
                );
                if template.source_kind == ModelKind::Ingestor {
                    runtime_handle.handle_general_error_for_acks(
                        domain,
                        template.source_kind.as_str(),
                        ingestor,
                        &template.error_policies,
                        acks.iter(),
                        reason,
                    );
                } else {
                    runtime_handle.handle_internal_processor_error_for_acks(
                        domain,
                        template.source_kind.as_str(),
                        ingestor,
                        &template.error_policies,
                        acks.iter(),
                        reason,
                    );
                }
                return None;
            }
        };
        let branch_plan = input_batch.branch_selections(
            &template.entrypoint_branch_mappings,
            &template.source,
            &template.root_relay,
        );
        for row_error in branch_plan.row_errors {
            tokio::task::consume_budget().await;
            runtime_handle
                .handle_message_error(
                    domain,
                    template.source_kind.as_str(),
                    &template.source,
                    &template.error_policies,
                    RelayMessage {
                        key: None,
                        record: row_error.record,
                        acks: row_error.acks,
                    },
                    row_error.reason,
                )
                .await;
        }
        if template.source_kind == ModelKind::Ingestor {
            for (key, row) in &branch_plan.valid_rows {
                let record = &input_batch.records[*row];
                runtime_handle
                    .metrics
                    .observe_branch_node_without_stream_received(
                        branch_key_display(key),
                        NodeWithoutRelayObservation {
                            domain,
                            kind: template.source_kind,
                            node: &template.source,
                            physical_node_id: runtime_handle.local_node_id.read().as_deref(),
                            messages: 1,
                            bytes: record.estimated_bytes(),
                            domain_timestamp: Some(record.metadata().ingested_at_high_watermark()),
                        },
                    );
                runtime_handle.mark_branch_aggregated_metrics_updated(
                    domain,
                    template.source_kind,
                    &template.source,
                );
            }
        }

        let mut batch_builds = FuturesUnordered::new();
        for selection in branch_plan.selections {
            tokio::task::consume_budget().await;
            batch_builds.push(branched_branch_filter_blocking(
                input_batch.clone(),
                selection,
                template.entrypoint_ack_boundary,
            ));
        }

        let mut dispatches = FuturesUnordered::new();
        let mut next_deadline = None;
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                built = futures_util::StreamExt::next(&mut batch_builds), if !batch_builds.is_empty() => {
                    let Some(batch_result) = built else {
                        continue;
                    };
                    let (key, message) = match batch_result {
                        Ok(output) => output,
                        Err((error, acks)) => {
                            let reason = format!(
                                "branched entrypoint '{}' failed to filter branch batch: {}",
                                ingestor.as_str(),
                                error
                            );
                            if template.source_kind == ModelKind::Ingestor {
                                runtime_handle.handle_general_error_for_acks(
                                    domain,
                                    template.source_kind.as_str(),
                                    ingestor,
                                    &template.error_policies,
                                    acks.iter(),
                                    reason,
                                );
                            } else {
                                runtime_handle.handle_internal_processor_error_for_acks(
                                    domain,
                                    template.source_kind.as_str(),
                                    ingestor,
                                    &template.error_policies,
                                    acks.iter(),
                                    reason,
                                );
                            }
                            continue;
                        }
                    };
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
                        debug!(
                            domain = domain.as_str(),
                            ingestor = ingestor.as_str(),
                            key = branch_key_display(&key),
                            "created branch runtime"
                        );
                    }
                    if let Some(max_instances) = template.branch_max_instances {
                        evict_branch_instance_instances_to_capacity(
                            domain,
                            ingestor,
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
                dispatched = futures_util::StreamExt::next(&mut dispatches), if !dispatches.is_empty() => {
                    let Some((key, acks, result)) = dispatched else {
                        continue;
                    };
                    match result {
                        Ok(deadline) => {
                            record_next_branch_instance_branch_deadline(
                                &mut next_deadline,
                                deadline,
                            );
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
                else => break,
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
                    &domain,
                    &ingestor,
                    max_instances,
                    &mut instances,
                )
                .await;
            }
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            let mut next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();
            let mut pending_inputs = Vec::<BranchedEntrypointInput>::new();
            let mut pending_flush_at = None::<Instant>;
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
                if let Some(deadline) = pending_flush_at
                    && Instant::now() >= deadline
                {
                    let ready = std::mem::take(&mut pending_inputs);
                    pending_flush_at = None;
                    record_next_branch_instance_branch_deadline(
                        &mut next_branch_deadline,
                        Self::dispatch_entrypoint_inputs(
                            BranchedBranchDispatchContext {
                                runtime_handle: &runtime_handle,
                                domain: &domain,
                                ingestor: &ingestor,
                                graph: &graph,
                                template: &template,
                                now,
                            },
                            &mut instances,
                            ready,
                        )
                        .await,
                    );
                    continue;
                }
                let mut did_scheduled_work = false;
                if Instant::now() >= next_expiration_scan {
                    if let Some(branch_ttl) = template.branch_ttl {
                        expire_branch_instance_instances(
                            &domain,
                            &ingestor,
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
                        .min(
                            pending_flush_at
                                .and_then(|deadline| {
                                    deadline.checked_duration_since(Instant::now())
                                })
                                .unwrap_or(Duration::MAX),
                        )
                };
                tokio::select! {
                    biased;
                    message = input.recv() => {
                        let Some(message) = message else {
                            let ready = std::mem::take(&mut pending_inputs);
                            record_next_branch_instance_branch_deadline(
                                &mut next_branch_deadline,
                                Self::dispatch_entrypoint_inputs(
                                    BranchedBranchDispatchContext {
                                        runtime_handle: &runtime_handle,
                                        domain: &domain,
                                        ingestor: &ingestor,
                                        graph: &graph,
                                        template: &template,
                                        now,
                                    },
                                    &mut instances,
                                    ready,
                                )
                                .await,
                            );
                            break;
                        };
                        match template.entrypoint_flush_each {
                            RuntimeFlushPolicy::Immediate => {
                                record_next_branch_instance_branch_deadline(
                                    &mut next_branch_deadline,
                                    Self::dispatch_entrypoint_inputs(
                                        BranchedBranchDispatchContext {
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
                            RuntimeFlushPolicy::Each {
                                interval: flush_each,
                                max_batch_size,
                            } => {
                                pending_inputs.push(message);
                                if pending_flush_at.is_none() {
                                    pending_flush_at = Some(Instant::now() + flush_each);
                                }
                                if branched_entrypoint_inputs_estimated_bytes(&pending_inputs)
                                    >= max_batch_size
                                {
                                    let ready = std::mem::take(&mut pending_inputs);
                                    pending_flush_at = None;
                                    record_next_branch_instance_branch_deadline(
                                        &mut next_branch_deadline,
                                        Self::dispatch_entrypoint_inputs(
                                            BranchedBranchDispatchContext {
                                                runtime_handle: &runtime_handle,
                                                domain: &domain,
                                                ingestor: &ingestor,
                                                graph: &graph,
                                                template: &template,
                                                now,
                                            },
                                            &mut instances,
                                            ready,
                                        )
                                        .await,
                                    );
                                }
                            }
                        }
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            let ready = std::mem::take(&mut pending_inputs);
                            record_next_branch_instance_branch_deadline(
                                &mut next_branch_deadline,
                                Self::dispatch_entrypoint_inputs(
                                    BranchedBranchDispatchContext {
                                        runtime_handle: &runtime_handle,
                                        domain: &domain,
                                        ingestor: &ingestor,
                                        graph: &graph,
                                        template: &template,
                                        now,
                                    },
                                    &mut instances,
                                    ready,
                                )
                                .await,
                            );
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
            shutdown_all_branch_instance_instances(&domain, &ingestor, &mut instances).await;
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
    domain: &Domain,
    ingestor: &Identifier,
    now: Timestamp,
    expiration_after: Duration,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) {
    for (key, state) in instances.expire(now, expiration_after) {
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
    domain: &Domain,
    ingestor: &Identifier,
    max_instances: usize,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) {
    for (key, state) in instances.evict_lru_to_capacity(max_instances) {
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
    domain: &Domain,
    ingestor: &Identifier,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) {
    for (key, state) in instances.drain() {
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
    domain: &Domain,
    template: &BranchInstanceTemplate,
) -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeStateKind::BranchLru,
        kind: template.source_kind,
        identifier: template.source.clone(),
        branch_key: None,
    }
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
    let placement = branch_lru_placement(domain, template);
    let Some(snapshot) = store
        .latest_snapshot(&placement)
        .map_err(|error| error.to_string())?
    else {
        return Ok(0);
    };
    for (key, last_ingestion) in decode_branch_lru_snapshot(&snapshot.payload)? {
        let state = template.instantiate(runtime, domain, key.clone())?;
        instances.insert_restored(key, last_ingestion, state);
    }
    instances.set_version(snapshot.lsm);
    Ok(snapshot.lsm)
}

fn persist_branch_instance_lru_snapshot(
    runtime: &Runtime,
    domain: &Domain,
    template: &BranchInstanceTemplate,
    instances: &BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
    last_persisted_lsm: &mut u64,
) -> Result<(), String> {
    let Some(store) = &runtime.state_store else {
        return Ok(());
    };
    let lsm = instances.version();
    if lsm <= *last_persisted_lsm {
        return Ok(());
    }
    let placement = branch_lru_placement(domain, template);
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

pub(crate) type CompiledFilterMapProgram = VmCompiledProgram;

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

struct PreparedFilterMapProgram {
    parsed: nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
    bindings: Vec<VmCompileBinding>,
    materialized_interest: MaterializedProgramInterest,
    lookup_hash_maps: Vec<LookupHashMapCall>,
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
        unset: parsed.inner.unset.clone(),
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
        Arc::new(arrow_schema::Schema::new(lookup_fields)),
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
                unset: Vec::new(),
            },
            span: (0..0).into(),
        };
        let key_types =
            infer_vm_set_expr_types_for_bindings(&key_program, bindings.iter().cloned()).map_err(
                |error| {
                    format!(
                        "LOOKUP_HASH_MAP key compile failed for hash map '{}' field '{}': {}",
                        call.lookup.as_str(),
                        call.lookup_field,
                        error.message
                    )
                },
            )?;
        let key_output_schema = Arc::new(arrow_schema::Schema::new(
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
            VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                ..VmCompileOptions::default()
            },
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
            || relay == INGEST_HEADERS_NAMESPACE
            || relay == BRANCH_NAMESPACE
        {
            continue;
        }
        let Ok(relay) = Identifier::parse(&relay) else {
            continue;
        };
        let Some(spec) = available_materialized_streams.get(&relay) else {
            continue;
        };
        if !spec.branching.is_empty() && spec.branching != current_branching {
            return Err(format!(
                "materialized relay '{}' uses branch fields ({}) but current input uses ({})",
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
                relay.as_str(),
                Arc::new(arrow_schema::Schema::new(projected_fields)),
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
    if let IngestSource::Endpoint { .. }
    | IngestSource::Http { .. }
    | IngestSource::Kafka { .. }
    | IngestSource::Nats { .. }
    | IngestSource::Pulsar { .. }
    | IngestSource::RabbitMq { .. }
    | IngestSource::Sqs { .. } = source
    {
        true
    } else {
        false
    }
}

fn ingestor_filter_map_headers_arrow_schema(
    source: &IngestSource,
    parsed: &nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
) -> Option<Arc<arrow_schema::Schema>> {
    if !ingest_source_supports_headers(source) {
        return None;
    }
    let fields = collect_program_field_refs(&parsed.inner)
        .into_iter()
        .filter_map(|(relay, field)| {
            if relay == INGEST_HEADERS_NAMESPACE {
                Some(field)
            } else {
                None
            }
        })
        .collect::<BTreeSet<_>>();
    if fields.is_empty() {
        return None;
    }
    Some(Arc::new(arrow_schema::Schema::new(
        fields
            .into_iter()
            .map(|field| arrow_schema::Field::new(field, ArrowDataType::Utf8, true))
            .collect::<Vec<_>>(),
    )))
}

fn emit_sink_supports_headers(sink: &EmitSink) -> bool {
    if let EmitSink::Kafka { .. }
    | EmitSink::Pulsar { .. }
    | EmitSink::RabbitMq { .. }
    | EmitSink::Nats { .. }
    | EmitSink::Sqs { .. } = sink
    {
        true
    } else {
        false
    }
}

fn emitter_filter_map_headers_arrow_schema(
    parsed: &nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
) -> Option<Arc<arrow_schema::Schema>> {
    let fields = parsed
        .inner
        .set
        .iter()
        .filter_map(|(field, _)| {
            if field.relay == INGEST_HEADERS_NAMESPACE {
                Some(field.field.clone())
            } else {
                None
            }
        })
        .collect::<BTreeSet<_>>();
    if fields.is_empty() {
        return None;
    }
    Some(Arc::new(arrow_schema::Schema::new(
        fields
            .into_iter()
            .map(|field| arrow_schema::Field::new(field, ArrowDataType::Utf8, true))
            .collect::<Vec<_>>(),
    )))
}

fn emitter_filter_map_local_namespaces(from_relay: &Identifier) -> HashSet<String> {
    HashSet::from_iter([
        INGEST_MESSAGE_NAMESPACE.to_string(),
        INGEST_HEADERS_NAMESPACE.to_string(),
        BRANCH_NAMESPACE.to_string(),
        from_relay.as_str().to_string(),
    ])
}

fn emitter_filter_map_message_bindings(
    from_relay: &Identifier,
    input_schema: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
) -> Vec<VmCompileBinding> {
    let mut bindings = vec![
        VmCompileBinding::writable(INGEST_MESSAGE_NAMESPACE, input_schema.clone())
            .with_sensitivity(input_sensitivity.clone()),
    ];
    if from_relay.as_str() != INGEST_MESSAGE_NAMESPACE {
        bindings.push(
            VmCompileBinding::writable(from_relay.as_str(), input_schema)
                .with_sensitivity(input_sensitivity),
        );
    }
    bindings
}

fn merge_materialized_program_interests(
    interests: impl IntoIterator<Item = MaterializedProgramInterest>,
) -> MaterializedProgramInterest {
    let mut relays = BTreeMap::<String, MaterializedRelayInterest>::new();
    for interest in interests {
        for relay in interest.relays {
            let key = relay.relay.as_str().to_string();
            relays
                .entry(key)
                .and_modify(|existing| {
                    for field in &relay.fields {
                        if !existing.fields.contains(field) {
                            existing.fields.push(field.clone());
                        }
                    }
                    existing.fields.sort();
                })
                .or_insert(relay);
        }
    }
    MaterializedProgramInterest {
        relays: relays.into_values().collect(),
    }
}

pub(crate) fn compile_filter_map_program(
    domain: &Domain,
    identifier: &Identifier,
    relay_names: &[Identifier],
    filter_map: Option<&str>,
    input_schema: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    output_schema: Arc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let Some(filter_map) = filter_map else {
        return Ok(None);
    };
    let prepared = prepare_filter_map_program(
        domain,
        identifier,
        relay_names,
        filter_map,
        input_schema,
        input_sensitivity,
        context,
    )?;
    let compiled = compile_vm_program_for_bindings_with_sensitivity(
        &prepared.parsed,
        output_schema,
        output_sensitivity.clone(),
        prepared.bindings,
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
        materialized_interest: prepared.materialized_interest,
        lookup_hash_maps: prepared.lookup_hash_maps,
    }))
}

pub(crate) fn compile_processor_output_filter_map_program(
    domain: &Domain,
    identifier: &Identifier,
    input_relays: &[Identifier],
    output_relay: &Identifier,
    filter_map: Option<&str>,
    input_schema: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    output_schema: Arc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let Some(filter_map) = filter_map else {
        return Ok(None);
    };
    let mut parsed =
        parse_program(filter_map).map_err(|error| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "FILTER-MAP parse failed for '{}': {}",
                identifier.as_str(),
                Runtime::vm_program_error(error)
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
    let input_relay_names = input_relays
        .iter()
        .map(|relay| relay.as_str().to_string())
        .collect::<Vec<_>>();
    parsed
        .inner
        .rewrite_unset_sources_to_destination(&input_relay_names, output_relay.as_str());
    let original_parsed = parsed.clone();
    let mut bindings = Vec::new();
    let mut output_bound = false;
    for relay in input_relays {
        if relay == output_relay {
            bindings.push(
                VmCompileBinding::writable(relay.as_str(), input_schema.clone())
                    .with_sensitivity(input_sensitivity.clone()),
            );
            output_bound = true;
        } else {
            bindings.push(
                VmCompileBinding::readonly(relay.as_str(), input_schema.clone())
                    .with_sensitivity(input_sensitivity.clone()),
            );
        }
    }
    if !output_bound {
        bindings.push(
            VmCompileBinding::writeonly(output_relay.as_str(), output_schema.clone())
                .with_sensitivity(output_sensitivity.clone()),
        );
    }
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let local_namespaces = input_relays
        .iter()
        .map(|relay| relay.as_str().to_string())
        .chain(std::iter::once(output_relay.as_str().to_string()))
        .chain(std::iter::once(BRANCH_NAMESPACE.to_string()))
        .collect::<HashSet<_>>();
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
        compile_lookup_hash_map_calls(pending_lookup_calls, output_relay.as_str(), &bindings)
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

    let compiled = compile_vm_program_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
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
        lookup_hash_maps,
    }))
}

fn compile_wasm_output_filter_map_program(
    domain: &Domain,
    identifier: &Identifier,
    input_relays: &[Identifier],
    output_relay: &Identifier,
    filter_map: Option<&str>,
    input_schema: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    output_schema: Arc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let Some(filter_map) = filter_map else {
        return Ok(None);
    };
    let parsed = parse_program(filter_map).map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP parse failed for '{}': {}",
            identifier.as_str(),
            Runtime::vm_program_error(error)
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
    if !parsed.inner.unset.is_empty() {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "WASM processor '{}' TO clauses may use SET and WHERE, but not UNSET",
                identifier.as_str()
            ),
        });
    }

    let original_parsed = parsed.clone();
    let mut bindings = vec![
        VmCompileBinding::writable(output_relay.as_str(), output_schema.clone())
            .with_sensitivity(output_sensitivity.clone()),
    ];
    for input_relay in input_relays {
        if input_relay != output_relay {
            bindings.push(
                VmCompileBinding::readonly(input_relay.as_str(), input_schema.clone())
                    .with_sensitivity(input_sensitivity.clone()),
            );
        }
    }
    if input_relays
        .iter()
        .all(|relay| relay.as_str() != WASM_INPUT_NAMESPACE)
        && output_relay.as_str() != WASM_INPUT_NAMESPACE
    {
        bindings.push(
            VmCompileBinding::readonly(WASM_INPUT_NAMESPACE, input_schema)
                .with_sensitivity(input_sensitivity),
        );
    }
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let mut local_namespaces = input_relays
        .iter()
        .map(|relay| relay.as_str().to_string())
        .collect::<HashSet<_>>();
    local_namespaces.insert(output_relay.as_str().to_string());
    local_namespaces.insert(WASM_INPUT_NAMESPACE.to_string());
    local_namespaces.insert(BRANCH_NAMESPACE.to_string());
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
        compile_lookup_hash_map_calls(pending_lookup_calls, output_relay.as_str(), &bindings)
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

    let compiled = compile_vm_program_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
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
        lookup_hash_maps,
    }))
}

pub(crate) fn compile_emitter_filter_map_program(
    domain: &Domain,
    emitter: &CreateEmitter,
    input_schema: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    output_schema: Arc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledEmitterFilterMapProgram>, RuntimeError> {
    let Some(filter_map) = emitter.filter_map.as_deref() else {
        return Ok(None);
    };
    let parsed = parse_program(filter_map).map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP parse failed for '{}': {}",
            emitter.name.as_str(),
            Runtime::vm_program_error(error)
        ),
    })?;
    if !parsed.inner.branch_filters.is_empty() {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "FILTER-MAP for '{}' may contain at most one WHERE clause",
                emitter.name.as_str()
            ),
        });
    }
    if parsed
        .inner
        .unset
        .iter()
        .any(|field| field.relay == INGEST_HEADERS_NAMESPACE)
    {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "FILTER-MAP for '{}' cannot UNSET emitter headers; omit a header by not setting it",
                emitter.name.as_str()
            ),
        });
    }
    if emitter_filter_map_headers_arrow_schema(&parsed).is_some()
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

    let original_parsed = parsed.clone();
    let body_program = nervix_nspl::vm_program::Program {
        filter: parsed.inner.filter.clone(),
        branch_filters: Vec::new(),
        set: parsed
            .inner
            .set
            .iter()
            .filter(|(field, _)| field.relay != INGEST_HEADERS_NAMESPACE)
            .cloned()
            .collect(),
        unset: parsed.inner.unset.clone(),
    };
    let body_parsed = nervix_nspl::vm_program::SpannedNode {
        inner: body_program,
        span: parsed.span,
    };
    let body = compile_emitter_filter_map_part(
        domain,
        &emitter.name,
        &emitter.from_relay,
        &original_parsed,
        body_parsed,
        input_schema.clone(),
        input_sensitivity.clone(),
        output_schema,
        output_sensitivity,
        None,
        VmOutputMode::PassthroughByName,
        context,
    )?;

    let headers = if let Some(header_schema) = emitter_filter_map_headers_arrow_schema(&parsed) {
        let header_program = nervix_nspl::vm_program::Program {
            filter: parsed.inner.filter,
            branch_filters: Vec::new(),
            set: parsed
                .inner
                .set
                .into_iter()
                .filter(|(field, _)| field.relay == INGEST_HEADERS_NAMESPACE)
                .collect(),
            unset: Vec::new(),
        };
        let header_parsed = nervix_nspl::vm_program::SpannedNode {
            inner: header_program,
            span: parsed.span,
        };
        Some(compile_emitter_filter_map_part(
            domain,
            &emitter.name,
            &emitter.from_relay,
            &original_parsed,
            header_parsed,
            input_schema,
            input_sensitivity,
            header_schema,
            VmSchemaSensitivity::default(),
            Some(INGEST_HEADERS_NAMESPACE),
            VmOutputMode::ExplicitOnly,
            context,
        )?)
    } else {
        None
    };

    let materialized_interest = merge_materialized_program_interests(
        std::iter::once(body.materialized_interest.clone()).chain(
            headers
                .as_ref()
                .map(|headers| headers.materialized_interest.clone()),
        ),
    );
    Ok(Some(CompiledEmitterFilterMapProgram {
        body,
        headers,
        materialized_interest,
    }))
}

fn compile_emitter_filter_map_part(
    domain: &Domain,
    identifier: &Identifier,
    from_relay: &Identifier,
    original_parsed: &nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
    parsed: nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
    input_schema: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    output_schema: Arc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    writeonly_namespace: Option<&str>,
    output_mode: VmOutputMode,
    context: RuntimeVmCompileContext<'_>,
) -> Result<CompiledProgramWithMaterializedInterest, RuntimeError> {
    let mut bindings = Vec::new();
    if let Some(namespace) = writeonly_namespace {
        bindings.push(
            VmCompileBinding::writeonly(namespace, output_schema.clone())
                .with_sensitivity(output_sensitivity.clone()),
        );
    }
    bindings.extend(emitter_filter_map_message_bindings(
        from_relay,
        input_schema,
        input_sensitivity,
    ));
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let local_namespaces = emitter_filter_map_local_namespaces(from_relay);
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        original_parsed,
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
        compile_lookup_hash_map_calls(pending_lookup_calls, INGEST_MESSAGE_NAMESPACE, &bindings)
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
        VmCompileOptions {
            output_mode,
            allow_sensitive_output: true,
            ..VmCompileOptions::default()
        },
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
        lookup_hash_maps,
    })
}

pub(crate) fn compile_session_filter_map_program(
    domain: &Domain,
    identifier: &Identifier,
    relay_names: &[Identifier],
    filter_map: Option<&str>,
    input_schema: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let Some(filter_map) = filter_map else {
        return Ok(None);
    };
    let prepared = prepare_filter_map_program(
        domain,
        identifier,
        relay_names,
        filter_map,
        input_schema.clone(),
        input_sensitivity.clone(),
        context,
    )?;
    let (output_schema, output_sensitivity) = infer_session_filter_map_output_schema(
        domain,
        identifier,
        &prepared,
        input_schema,
        input_sensitivity,
    )?;
    let compiled = compile_vm_program_for_bindings_with_sensitivity(
        &prepared.parsed,
        output_schema,
        output_sensitivity.clone(),
        prepared.bindings,
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
        materialized_interest: prepared.materialized_interest,
        lookup_hash_maps: prepared.lookup_hash_maps,
    }))
}

fn prepare_filter_map_program(
    domain: &Domain,
    identifier: &Identifier,
    relay_names: &[Identifier],
    filter_map: &str,
    input_schema: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<PreparedFilterMapProgram, RuntimeError> {
    let parsed = parse_program(filter_map).map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP parse failed for '{}': {}",
            identifier.as_str(),
            Runtime::vm_program_error(error)
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
    let writable_namespaces = relay_names
        .iter()
        .map(|name| name.as_str().to_string())
        .collect::<HashSet<_>>();
    let mut bindings = relay_names
        .iter()
        .map(|name| {
            VmCompileBinding::writable(name.as_str(), input_schema.clone())
                .with_sensitivity(input_sensitivity.clone())
        })
        .collect::<Vec<_>>();
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let local_namespaces = writable_namespaces
        .iter()
        .cloned()
        .chain(std::iter::once(BRANCH_NAMESPACE.to_string()))
        .collect::<HashSet<_>>();
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
    let writable_namespace =
        relay_names
            .first()
            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP for '{}' requires at least one writable relay",
                    identifier.as_str()
                ),
            })?;
    let (lookup_hash_maps, lookup_binding) =
        compile_lookup_hash_map_calls(pending_lookup_calls, writable_namespace.as_str(), &bindings)
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
    Ok(PreparedFilterMapProgram {
        parsed,
        bindings,
        materialized_interest,
        lookup_hash_maps,
    })
}

fn infer_session_filter_map_output_schema(
    domain: &Domain,
    identifier: &Identifier,
    prepared: &PreparedFilterMapProgram,
    input_schema: Arc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
) -> Result<(Arc<arrow_schema::Schema>, VmSchemaSensitivity), RuntimeError> {
    let unset = prepared
        .parsed
        .inner
        .unset
        .iter()
        .map(|field_ref| field_ref.field.clone())
        .collect::<HashSet<_>>();
    let inferred_set_fields =
        infer_vm_set_expr_types_for_bindings(&prepared.parsed, prepared.bindings.iter().cloned())
            .map_err(|error| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "FILTER-MAP output schema inference failed for '{}': {}",
                identifier.as_str(),
                error.message
            ),
        })?;
    let mut set_order = Vec::with_capacity(inferred_set_fields.len());
    let mut set_fields = HashMap::with_capacity(inferred_set_fields.len());
    for (name, data_type, nullable) in inferred_set_fields {
        if !set_fields.contains_key(&name) {
            set_order.push(name.clone());
        }
        set_fields.insert(name, (data_type, nullable));
    }

    let mut output_fields = Vec::new();
    let mut output_sensitive_fields = Vec::new();
    for field in input_schema.fields() {
        let name = field.name();
        if let Some((data_type, nullable)) = set_fields.remove(name) {
            output_fields.push(arrow_schema::Field::new(name, data_type, nullable));
        } else if !unset.contains(name) {
            output_fields.push(field.as_ref().clone());
            if input_sensitivity.is_sensitive(name) {
                output_sensitive_fields.push(name.clone());
            }
        }
    }
    for name in set_order {
        if let Some((data_type, nullable)) = set_fields.remove(&name) {
            output_fields.push(arrow_schema::Field::new(name, data_type, nullable));
        }
    }

    Ok((
        Arc::new(arrow_schema::Schema::new(output_fields)),
        VmSchemaSensitivity::from_sensitive_fields(output_sensitive_fields),
    ))
}

fn split_reorder_by_expressions(source: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in source.char_indices() {
        if let Some(active_quote) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == active_quote {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                let part = source[start..index].trim();
                if !part.is_empty() {
                    parts.push(part.to_string());
                }
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    let part = source[start..].trim();
    if !part.is_empty() {
        parts.push(part.to_string());
    }
    parts
}

pub(super) fn compile_key_projection_program(
    processor_kind: &str,
    processor: &Identifier,
    clause: &str,
    input_relays: &[Identifier],
    expressions: &[String],
    input_schema: Arc<arrow_schema::Schema>,
) -> Result<VmCompiledProgram, String> {
    let Some(primary_input_relay) = input_relays.first() else {
        return Err(format!(
            "{} '{}' {} requires at least one input relay",
            processor_kind,
            processor.as_str(),
            clause
        ));
    };
    let assignments = expressions
        .iter()
        .enumerate()
        .map(|(index, expr)| format!("{}.key_{} = {}", primary_input_relay.as_str(), index, expr))
        .collect::<Vec<_>>()
        .join(", ");
    let source = format!("SET {assignments}");
    let parsed = parse_program(&source).map_err(|error| {
        format!(
            "{} '{}' {} parse failed: {}",
            processor_kind,
            processor.as_str(),
            clause,
            Runtime::vm_program_error(error)
        )
    })?;
    let mut bindings = vec![VmCompileBinding::writable(
        primary_input_relay.as_str(),
        input_schema.clone(),
    )];
    bindings.extend(
        input_relays
            .iter()
            .skip(1)
            .map(|relay| VmCompileBinding::readonly(relay.as_str(), input_schema.clone())),
    );
    let key_types =
        infer_vm_set_expr_types_for_bindings(&parsed, bindings.clone()).map_err(|error| {
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
    let output_schema = Arc::new(arrow_schema::Schema::new(
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
        VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            ..VmCompileOptions::default()
        },
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

fn compile_reorderer_program(
    processor: &Identifier,
    input_relays: &[Identifier],
    order_by: &str,
    input_schema: Arc<arrow_schema::Schema>,
) -> Result<CompiledReordererProgram, String> {
    let expressions = split_reorder_by_expressions(order_by);
    if expressions.is_empty() {
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
        &expressions,
        input_schema,
    )?;
    Ok(CompiledReordererProgram {
        key_column_offset: 0,
        key_count: expressions.len(),
        program: compiled,
    })
}

fn compile_correlator_where_program(
    processor: &Identifier,
    correlate_where: &str,
    left_relays: &[Identifier],
    left_schema: Arc<arrow_schema::Schema>,
    right_relays: &[Identifier],
    right_schema: Arc<arrow_schema::Schema>,
) -> Result<CompiledCorrelatorWhereProgram, String> {
    let parsed = parse_program(correlate_where).map_err(|error| {
        format!(
            "correlator '{}' CORRELATE WHERE parse failed: {}",
            processor.as_str(),
            Runtime::vm_program_error(error)
        )
    })?;
    if parsed.inner.filter.is_none()
        || !parsed.inner.branch_filters.is_empty()
        || !parsed.inner.set.is_empty()
        || !parsed.inner.unset.is_empty()
    {
        return Err(format!(
            "correlator '{}' CORRELATE WHERE must contain exactly one WHERE clause",
            processor.as_str()
        ));
    }
    let mut bindings = Vec::with_capacity(left_relays.len() + right_relays.len());
    for (index, relay) in left_relays.iter().enumerate() {
        if index == 0 {
            bindings.push(VmCompileBinding::writable(
                relay.as_str(),
                left_schema.clone(),
            ));
        } else {
            bindings.push(VmCompileBinding::readonly(
                relay.as_str(),
                left_schema.clone(),
            ));
        }
    }
    for relay in right_relays {
        bindings.push(VmCompileBinding::readonly(
            relay.as_str(),
            right_schema.clone(),
        ));
    }
    let program = compile_vm_program_for_bindings_with_sensitivity(
        &parsed,
        left_schema.clone(),
        VmSchemaSensitivity::default(),
        bindings,
    )
    .map_err(|error| {
        format!(
            "correlator '{}' CORRELATE WHERE compile failed: {}",
            processor.as_str(),
            error.message
        )
    })?;
    Ok(CompiledCorrelatorWhereProgram { program })
}

struct CorrelatorOutputCompileContext<'a> {
    processor: &'a Identifier,
    left_relays: &'a [Identifier],
    left_schema: Arc<arrow_schema::Schema>,
    right_relays: &'a [Identifier],
    right_schema: Arc<arrow_schema::Schema>,
    output_relay: &'a Identifier,
    output_schema: Arc<arrow_schema::Schema>,
    output_assignments: &'a str,
}

impl CorrelatorOutputCompileContext<'_> {
    fn compile(self) -> Result<CompiledCorrelatorOutputProgram, String> {
        let source = format!("SET {}", self.output_assignments);
        let parsed = parse_program(&source).map_err(|error| {
            format!(
                "correlator '{}' OUTPUT parse failed: {}",
                self.processor.as_str(),
                Runtime::vm_program_error(error)
            )
        })?;
        if parsed.inner.filter.is_some()
            || !parsed.inner.branch_filters.is_empty()
            || !parsed.inner.unset.is_empty()
            || parsed.inner.set.is_empty()
        {
            return Err(format!(
                "correlator '{}' OUTPUT must contain explicit assignments only",
                self.processor.as_str()
            ));
        }
        let mut bindings = Vec::with_capacity(self.left_relays.len() + self.right_relays.len() + 1);
        for relay in self.left_relays {
            bindings.push(VmCompileBinding::readonly(
                relay.as_str(),
                self.left_schema.clone(),
            ));
        }
        for relay in self.right_relays {
            bindings.push(VmCompileBinding::readonly(
                relay.as_str(),
                self.right_schema.clone(),
            ));
        }
        bindings.push(VmCompileBinding::writeonly(
            self.output_relay.as_str(),
            self.output_schema.clone(),
        ));
        let program = compile_vm_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            self.output_schema.clone(),
            VmSchemaSensitivity::default(),
            bindings,
            VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                ..VmCompileOptions::default()
            },
        )
        .map_err(|error| {
            format!(
                "correlator '{}' OUTPUT compile failed: {}",
                self.processor.as_str(),
                error.message
            )
        })?;
        Ok(CompiledCorrelatorOutputProgram { program })
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

async fn flush_branch_reorderer(
    context: ReordererFlushContext<'_>,
    pending: &mut Vec<ReordererPendingMessage>,
    next_flush: &mut Option<Timestamp>,
) {
    let graph = context.graph;
    let node_kind = context.node_kind;
    let processor = context.processor;
    let error_policies = context.error_policies;
    let output_routes = context.output_routes;
    let input_relays = context.input_relays;
    let branch = context.branch;

    if pending.is_empty() {
        *next_flush = None;
        return;
    }
    let Some(input_relay) = input_relays.first() else {
        *next_flush = None;
        return;
    };
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
            for message in messages {
                branch
                    .runtime
                    .handle_message_error(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        message,
                        error.to_string(),
                    )
                    .await;
            }
            *next_flush = None;
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
            *next_flush = None;
            return;
        }
    };
    if let Some(acks) = dispatch_processor_outputs(
        ProcessorOutputDispatchContext {
            graph,
            branch,
            node_kind,
            source_kind: ModelKind::Reorderer,
            processor,
            error_policies,
            input_relays,
            filter_source: ProcessorOutputFilterSource::InputRelays,
        },
        output_routes,
        batch,
    )
    .await
    {
        for ack in acks {
            ack.ack_success();
        }
    }
    *next_flush = None;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorrelatorSide {
    Left,
    Right,
}

fn take_correlator_opposite_pending(
    state: &SharedCorrelatorBranchState,
    incoming_side: CorrelatorSide,
) -> Vec<CorrelatorPendingMessage> {
    let mut state = state.lock();
    match incoming_side {
        CorrelatorSide::Left => std::mem::take(&mut state.pending_right),
        CorrelatorSide::Right => std::mem::take(&mut state.pending_left),
    }
}

fn restore_correlator_opposite_pending(
    state: &SharedCorrelatorBranchState,
    incoming_side: CorrelatorSide,
    mut pending: Vec<CorrelatorPendingMessage>,
) {
    let mut state = state.lock();
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
    state: &SharedCorrelatorBranchState,
    incoming_side: CorrelatorSide,
    incoming: CorrelatorPendingMessage,
    mut opposite_pending: Vec<CorrelatorPendingMessage>,
) {
    let mut state = state.lock();
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
    left_relays: &[Identifier],
    right_relays: &[Identifier],
    program: &CompiledCorrelatorWhereProgram,
    incoming_side: CorrelatorSide,
    match_policy: CorrelatorMatchPolicy,
    state: &SharedCorrelatorBranchState,
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
        let matched = match evaluate_correlator_where_match(
            processor,
            left_relays,
            right_relays,
            program,
            left,
            right,
            execution_now,
        )
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
    left_relays: &[Identifier],
    right_relays: &[Identifier],
    program: &CompiledCorrelatorWhereProgram,
    left: &CorrelatorPendingMessage,
    right: &CorrelatorPendingMessage,
    execution_now: Timestamp,
) -> Result<bool, (String, Vec<AckSet>)> {
    let acks = AckSet::merged([left.message.acks.attached(), right.message.acks.attached()]);
    let combined = correlator_combined_record(
        left_relays,
        &left.message.record,
        right_relays,
        &right.message.record,
    );
    let input = vm_typed_batch_from_runtime_record(&combined, None, &program.program.input_schema)
        .map_err(|error| {
            (
                format!(
                    "correlator '{}' failed to build CORRELATE WHERE input batch: {}",
                    processor.as_str(),
                    error
                ),
                vec![acks.clone()],
            )
        })?;
    let result = execute_program_with_selection_in_context(
        &program.program,
        &input,
        &VmExecutionContext { now: execution_now },
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

fn correlator_combined_record(
    left_relays: &[Identifier],
    left: &RuntimeRecord,
    right_relays: &[Identifier],
    right: &RuntimeRecord,
) -> RuntimeRecord {
    let mut fields = Vec::new();
    for relay in left_relays {
        fields.extend(
            left.fields()
                .map(|(name, value)| (format!("{}.{}", relay.as_str(), name), value.clone())),
        );
    }
    for relay in right_relays {
        fields.extend(
            right
                .fields()
                .map(|(name, value)| (format!("{}.{}", relay.as_str(), name), value.clone())),
        );
    }
    RuntimeRecord::from_fields_with_metadata(
        fields,
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
    left_relays: &[Identifier],
    right_relays: &[Identifier],
    program: &CompiledCorrelatorOutputProgram,
    left: CorrelatorPendingMessage,
    right: CorrelatorPendingMessage,
    execution_now: Timestamp,
) -> Result<RelayMessage, (String, Vec<AckSet>)> {
    let key = left.message.key.clone();
    let acks = AckSet::merged([left.message.acks.attached(), right.message.acks.attached()]);
    let combined = correlator_combined_record(
        left_relays,
        &left.message.record,
        right_relays,
        &right.message.record,
    );
    let input = vm_typed_batch_from_runtime_record(&combined, None, &program.program.input_schema)
        .map_err(|error| {
            (
                format!(
                    "correlator '{}' failed to build OUTPUT input batch: {}",
                    processor.as_str(),
                    error
                ),
                vec![acks.clone()],
            )
        })?;
    let result = execute_program_with_selection_in_context(
        &program.program,
        &input,
        &VmExecutionContext { now: execution_now },
    )
    .await
    .map_err(|error| {
        (
            format!(
                "correlator '{}' failed to evaluate OUTPUT: {}",
                processor.as_str(),
                error
            ),
            vec![acks.clone()],
        )
    })?;
    if result.batch.row_count() != 1 {
        return Err((
            format!(
                "correlator '{}' OUTPUT produced {} rows for one correlation",
                processor.as_str(),
                result.batch.row_count()
            ),
            vec![acks],
        ));
    }
    if let Some(side_error) = result.batch.errors().iter().flatten().next() {
        return Err((
            format!(
                "correlator '{}' OUTPUT side error {}: {} at {}",
                processor.as_str(),
                side_error.code.as_str(),
                side_error.message,
                side_error.span
            ),
            vec![acks],
        ));
    }
    let record = vm_output_row_to_decoded_record(&result.batch, 0).map_err(|error| {
        (
            format!(
                "correlator '{}' failed to decode OUTPUT row: {}",
                processor.as_str(),
                error
            ),
            vec![acks.clone()],
        )
    })?;
    Ok(RelayMessage {
        key,
        record: record.into_runtime_record(combined.metadata().clone()),
        acks,
    })
}

struct CorrelatorFlushContext<'a> {
    graph: &'a SharedActiveGraph,
    branch: &'a mut BranchRuntime,
    node_kind: &'a str,
    processor: &'a Identifier,
    error_policies: &'a ErrorPolicies,
    output_routes: &'a mut RelayProcessorOutputsNode,
    state: &'a SharedCorrelatorBranchState,
}

async fn flush_branch_correlator(context: CorrelatorFlushContext<'_>) {
    let CorrelatorFlushContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        output_routes,
        state,
    } = context;
    let messages = {
        let mut state = state.lock();
        state.next_flush = None;
        std::mem::take(&mut state.output_pending)
    };
    if messages.is_empty() {
        return;
    }
    let Some(base_output_relay) = output_routes
        .routes
        .first()
        .map(|output| output.relay.clone())
    else {
        for message in messages {
            message.acks.no_ack(format!(
                "correlator '{}' has no output destinations",
                processor.as_str()
            ));
        }
        return;
    };
    let output_schema =
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, &base_output_relay) {
            Ok(schema) => schema,
            Err(error) => {
                for message in messages {
                    branch
                        .runtime
                        .handle_message_error(
                            &branch.domain,
                            node_kind,
                            processor,
                            error_policies,
                            message,
                            error.to_string(),
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
    if let Some(acks) = dispatch_processor_outputs(
        ProcessorOutputDispatchContext {
            graph,
            branch,
            node_kind,
            source_kind: ModelKind::Correlator,
            processor,
            error_policies,
            input_relays: std::slice::from_ref(&base_output_relay),
            filter_source: ProcessorOutputFilterSource::OutputRelay,
        },
        output_routes,
        batch,
    )
    .await
    {
        for ack in acks {
            ack.ack_success();
        }
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
                filter_map: None,
                compiled_program: None,
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
                                error.to_string(),
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
    output_relay: &Identifier,
    source: &IngestSource,
    filter_map: Option<&str>,
    schemas: RuntimeVmSchemaPair,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let Some(filter_map) = filter_map else {
        return Ok(None);
    };
    let parsed = parse_program(filter_map).map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP parse failed for '{}': {}",
            identifier.as_str(),
            Runtime::vm_program_error(error)
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

    let mut bindings = vec![
        VmCompileBinding::readonly(INGEST_MESSAGE_NAMESPACE, schemas.input.clone())
            .with_sensitivity(schemas.input_sensitivity),
        VmCompileBinding::writeonly(output_relay.as_str(), schemas.output.clone())
            .with_sensitivity(schemas.output_sensitivity.clone()),
    ];
    let writable_namespaces = HashSet::from_iter([output_relay.as_str().to_string()]);
    if let Some(metadata_schema) = ingestor_filter_map_metadata_arrow_schema(source) {
        bindings.push(VmCompileBinding::readonly(
            INGEST_METADATA_NAMESPACE,
            metadata_schema,
        ));
    }
    if let Some(headers_schema) = ingestor_filter_map_headers_arrow_schema(source, &parsed) {
        bindings.push(VmCompileBinding::readonly(
            INGEST_HEADERS_NAMESPACE,
            headers_schema,
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
        compile_lookup_hash_map_calls(pending_lookup_calls, output_relay.as_str(), &bindings)
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

    let compiled = compile_vm_program_for_bindings_with_sensitivity(
        &parsed,
        schemas.output,
        schemas.output_sensitivity.clone(),
        bindings,
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
        lookup_hash_maps,
    }))
}

fn parse_generator_program(
    identifier: &Identifier,
    program: &str,
) -> Result<nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>, String> {
    let parsed = parse_program(program).map_err(|error| {
        format!(
            "GENERATOR SET parse failed for '{}': {}",
            identifier.as_str(),
            Runtime::vm_program_error(error)
        )
    })?;
    if parsed.inner.filter.is_some()
        || !parsed.inner.branch_filters.is_empty()
        || !parsed.inner.unset.is_empty()
        || parsed.inner.set.is_empty()
    {
        return Err(format!(
            "GENERATOR '{}' must contain SET only",
            identifier.as_str()
        ));
    }
    Ok(parsed)
}

fn collect_generator_expr_streams(expr: &SpannedExpr, relays: &mut HashSet<String>) {
    match &expr.inner {
        Expr::Literal(_) | Expr::InternalFieldRef(_) => {}
        Expr::FieldRef(field_ref) => {
            relays.insert(field_ref.relay.clone());
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            collect_generator_expr_streams(expr, relays);
        }
        Expr::Binary { left, right, .. } => {
            collect_generator_expr_streams(left, relays);
            collect_generator_expr_streams(right, relays);
        }
        Expr::Call { args, .. } => {
            for arg in args {
                collect_generator_expr_streams(arg, relays);
            }
        }
    }
}

fn generator_source_streams(
    identifier: &Identifier,
    program: &str,
    into_relay: &Identifier,
) -> Result<Vec<Identifier>, String> {
    let parsed = parse_generator_program(identifier, program)?;
    let mut relay_names = HashSet::default();
    for (_field_ref, expr) in &parsed.inner.set {
        collect_generator_expr_streams(expr, &mut relay_names);
    }
    relay_names.remove(into_relay.as_str());
    let mut relay_names = relay_names.into_iter().collect::<Vec<_>>();
    relay_names.sort();
    relay_names
        .into_iter()
        .map(|stream| {
            Identifier::parse(&stream)
                .map_err(|error| format!("invalid generator namespace '{stream}': {error}"))
        })
        .collect()
}

fn compile_generator_set_program(
    domain: &Domain,
    generator: &CreateGenerator,
    output_schema: Arc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    source_schemas: &[(Identifier, Arc<arrow_schema::Schema>)],
) -> Result<Arc<CompiledFilterMapProgram>, RuntimeError> {
    let parsed = parse_generator_program(&generator.name, &generator.set).map_err(|reason| {
        RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason,
        }
    })?;
    let mut bindings = vec![VmCompileBinding::writable(
        generator.into_relay.as_str(),
        Arc::new(arrow_schema::Schema::new(Vec::<arrow_schema::Field>::new())),
    )];
    for (relay, schema) in source_schemas {
        bindings.push(VmCompileBinding::readonly(relay.as_str(), schema.clone()));
    }
    let compiled = compile_vm_program_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity,
        bindings,
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "GENERATOR SET compile failed for '{}': {}",
            generator.name.as_str(),
            error.message
        ),
    })?;
    Ok(Arc::new(compiled))
}

fn ingestor_filter_map_metadata_arrow_schema(
    source: &IngestSource,
) -> Option<Arc<arrow_schema::Schema>> {
    match source {
        IngestSource::Kafka { .. } => Some(Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("topic", ArrowDataType::Utf8, true),
            arrow_schema::Field::new("partition", ArrowDataType::Int32, true),
            arrow_schema::Field::new("offset", ArrowDataType::Int64, true),
        ]))),
        _ => None,
    }
}

pub(crate) async fn execute_filter_map_on_record(
    filter_map: &CompiledProgramWithMaterializedInterest,
    record: RuntimeRecord,
    branch_key: Option<&BranchKey>,
    filter_map_metadata: Option<&IngestFilterMapMetadata>,
    execution_now: Timestamp,
) -> Result<Option<RuntimeRecord>, String> {
    let metadata = record.metadata().clone();
    let record = augment_runtime_record_with_branch_key(record, branch_key);
    let record =
        augment_runtime_records_with_lookup_hash_maps(vec![record], filter_map, execution_now)
            .await?
            .into_iter()
            .next()
            .expect("single lookup-augmented record must remain");
    let batch = vm_typed_batch_from_runtime_record(
        &record,
        filter_map_metadata,
        &filter_map.compiled.input_schema,
    )?;
    let result = execute_program_with_selection_in_context(
        filter_map.compiled.as_ref(),
        &batch,
        &VmExecutionContext { now: execution_now },
    )
    .await
    .map_err(|error| format!("FILTER-MAP execution failed: {error}"))?;
    if result.batch.row_count() == 0 {
        return Ok(None);
    }
    if result.batch.row_count() != 1 {
        return Err(format!(
            "FILTER-MAP produced {} rows for a single input record",
            result.batch.row_count()
        ));
    }
    if let Some(side_error) = result.batch.errors().iter().flatten().next() {
        return Err(format!(
            "FILTER-MAP side error {}: {} at {}",
            side_error.code.as_str(),
            side_error.message,
            side_error.span
        ));
    }
    vm_output_row_to_decoded_record(&result.batch, 0)
        .map(|record| Some(record.into_runtime_record(metadata)))
}

#[derive(Debug, Clone, Copy)]
enum ProcessorOutputFilterSource {
    InputRelays,
    OutputRelay,
}

impl ProcessorOutputFilterSource {
    fn relays(self, input_relays: &[Identifier]) -> Vec<Identifier> {
        match self {
            Self::InputRelays | Self::OutputRelay => input_relays.to_vec(),
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
    filter_source: ProcessorOutputFilterSource,
}

struct PendingProcessorOutputMessage {
    row: usize,
    output_index: usize,
    key: Option<BranchKey>,
    record: RuntimeRecord,
}

struct PendingProcessorOutputBatch {
    output_index: usize,
    input_rows: Vec<usize>,
    key: Option<BranchKey>,
    batch: RuntimeRecordBatch,
    records: Vec<RuntimeRecord>,
    metadata: Vec<RuntimeRecordMetadata>,
}

impl PendingProcessorOutputBatch {
    fn into_relay_batch(self, acks: Vec<AckSet>) -> Result<RelayRecordBatch, String> {
        RelayRecordBatch::from_filtered_parts(
            self.key,
            self.batch,
            self.records,
            self.metadata,
            acks,
        )
    }
}

struct PendingProcessorOutputMessageError {
    row: usize,
    key: Option<BranchKey>,
    record: RuntimeRecord,
    reason: String,
}

fn pending_passthrough_output_batch(
    output_index: usize,
    batch: &RelayRecordBatch,
) -> PendingProcessorOutputBatch {
    PendingProcessorOutputBatch {
        output_index,
        input_rows: (0..batch.records.len()).collect(),
        key: batch.key.clone(),
        batch: batch.batch.clone(),
        records: batch.records.clone(),
        metadata: batch.metadata.clone(),
    }
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
    if output.compiled_program.is_none() && output.filter_map.is_some() {
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
        match compile_processor_output_filter_map_program(
            &context.branch.domain,
            context.processor,
            &input_relays,
            &output.relay,
            output.filter_map.as_deref(),
            batch.arrow_schema(),
            input_sensitivity,
            output_schema.arrow_schema(),
            output_schema.vm_sensitivity(),
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized_stream_specs,
                available_lookups: &available_lookups,
                current_branching: &current_branching,
                current_branch_schema: current_branch_schema.as_ref(),
                current_branch_sensitivity: None,
            },
        ) {
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
        let can_forward_batch = relay_schema_for_runtime(
            &context.branch.runtime,
            &context.branch.domain,
            &output.relay,
        )
        .ok()
        .is_some_and(|schema| schema.arrow_schema().as_ref() == batch.arrow_schema().as_ref());
        if can_forward_batch {
            return Ok((
                Vec::new(),
                vec![pending_passthrough_output_batch(output_index, batch)],
                Vec::new(),
            ));
        }
        let messages = batch
            .records
            .iter()
            .enumerate()
            .map(|(row, record)| PendingProcessorOutputMessage {
                row,
                output_index,
                key: batch.keys[row].clone(),
                record: record.clone(),
            })
            .collect();
        return Ok((messages, Vec::new(), Vec::new()));
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
    let side_inputs = context
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
        })?;
    let input_records = prepare_filter_map_input_records(
        context.node_kind,
        context.processor,
        program,
        batch.records.clone(),
        execution_now,
        &side_inputs,
        &batch.keys,
        &batch.acks,
    )
    .await?;
    let executed = execute_filter_map_program(
        context.node_kind,
        context.processor,
        program,
        &input_records,
        execution_now,
        batch.acks.clone(),
    )
    .await?;
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
    let output_batch =
        vm_typed_batch_to_runtime_batch(&executed.batch).map_err(|error| PlannedGeneralError {
            acks: batch.acks.clone(),
            reason: format!(
                "{} '{}' failed to materialize FILTER-MAP output batch: {}",
                context.node_kind,
                context.processor.as_str(),
                error
            ),
        })?;
    let mut success_output_rows = Vec::new();
    let mut success_input_rows = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, &input_row) in executed.selected_rows.iter().enumerate() {
        if let Some(side_error) = executed.batch.errors()[output_row].first() {
            message_errors.push(PendingProcessorOutputMessageError {
                row: input_row,
                key: batch.keys[input_row].clone(),
                record: batch.records[input_row].clone(),
                reason: format!(
                    "{} '{}' FILTER-MAP side error {}: {} at {}",
                    context.node_kind,
                    context.processor.as_str(),
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ),
            });
            continue;
        }
        success_output_rows.push(output_row);
        success_input_rows.push(input_row);
    }
    let output_batches = if success_output_rows.is_empty() {
        Vec::new()
    } else {
        let output_batch = if success_output_rows.len() == executed.batch.row_count() {
            output_batch
        } else {
            let success_output_rows = success_output_rows.iter().copied().collect::<HashSet<_>>();
            let keep = BooleanArray::from_iter(
                (0..executed.batch.row_count()).map(|row| Some(success_output_rows.contains(&row))),
            );
            output_batch
                .filter(&keep)
                .map_err(|error| PlannedGeneralError {
                    acks: batch.acks.clone(),
                    reason: format!(
                        "{} '{}' failed to filter FILTER-MAP output batch: {}",
                        context.node_kind,
                        context.processor.as_str(),
                        error
                    ),
                })?
        };
        let records = output_schema
            .decoded_records_from_arrow_batch(&output_batch)
            .map_err(|error| PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "{} '{}' failed to decode FILTER-MAP output sidecar records: {}",
                    context.node_kind,
                    context.processor.as_str(),
                    error
                ),
            })?
            .into_iter()
            .zip(success_input_rows.iter())
            .map(|(record, input_row)| {
                record.into_runtime_record(batch.metadata[*input_row].clone())
            })
            .collect::<Vec<_>>();
        let metadata = success_input_rows
            .iter()
            .map(|input_row| batch.metadata[*input_row].clone())
            .collect::<Vec<_>>();
        vec![PendingProcessorOutputBatch {
            output_index,
            input_rows: success_input_rows,
            key: batch.key.clone(),
            batch: output_batch,
            records,
            metadata,
        }]
    };
    Ok((Vec::new(), output_batches, message_errors))
}

async fn dispatch_processor_outputs(
    mut context: ProcessorOutputDispatchContext<'_>,
    outputs: &mut RelayProcessorOutputsNode,
    batch: RelayRecordBatch,
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
        pending_errors.extend(errors);
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
    for error in &pending_errors {
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

    let mut planned_errors = Vec::new();
    for error in pending_errors {
        let Some(acks) = ack_queues[error.row].pop_front() else {
            continue;
        };
        planned_errors.push(PlannedMessageError {
            message: RelayMessage {
                key: error.key,
                record: error.record,
                acks,
            },
            reason: error.reason,
        });
    }
    context
        .branch
        .runtime
        .handle_planned_message_errors(
            &context.branch.domain,
            context.node_kind,
            context.processor,
            context.error_policies,
            planned_errors,
        )
        .await;

    let mut dispatched_acks = Vec::new();
    for ((relay, messages), mut batches) in output_relays
        .into_iter()
        .zip(messages_by_output)
        .zip(batches_by_output)
    {
        if !messages.is_empty() {
            let output_schema = match relay_schema_for_runtime(
                &context.branch.runtime,
                &context.branch.domain,
                &relay,
            ) {
                Ok(schema) => schema,
                Err(error) => {
                    for message in messages {
                        context
                            .branch
                            .runtime
                            .handle_message_error(
                                &context.branch.domain,
                                context.node_kind,
                                context.processor,
                                context.error_policies,
                                message,
                                error.to_string(),
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
        let concat_acks = batches
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        let forwarded = match RelayRecordBatch::concat(batches) {
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
                        concat_acks.iter(),
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
        let output = RelayProcessorOutputNode {
            relay: relay.clone(),
            filter_map: None,
            compiled_program: None,
        };
        if context
            .branch
            .dispatch_output(
                context.graph,
                &output,
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

async fn plan_filter_map_messages(
    processor_kind: &str,
    processor: &Identifier,
    program_label: &str,
    program: &CompiledProgramWithMaterializedInterest,
    batch: RelayRecordBatch,
    execution_now: Timestamp,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> Result<FilterMapPlan, PlannedGeneralError> {
    let input_records = batch.records.clone();
    let source_records = input_records.clone();
    let input_records = augment_runtime_records_with_side_inputs(input_records, side_inputs);
    let input_records = match augment_runtime_records_with_branch_keys(input_records, &batch.keys) {
        Ok(records) => records,
        Err(error) => {
            return Err(PlannedGeneralError {
                acks: batch.acks,
                reason: format!(
                    "{} '{}' failed to prepare branch inputs: {}",
                    processor_kind,
                    processor.as_str(),
                    error
                ),
            });
        }
    };
    let input_records =
        match augment_runtime_records_with_lookup_hash_maps(input_records, program, execution_now)
            .await
        {
            Ok(records) => records,
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
    let RelayRecordBatch {
        key, keys, acks, ..
    } = batch;
    let mut acks = acks;
    let vm_batch =
        match vm_typed_batch_from_runtime_records(&input_records, &program.compiled.input_schema) {
            Ok(vm_batch) => vm_batch,
            Err(error) => {
                return Err(PlannedGeneralError {
                    acks,
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
    let result = match execute_program_with_selection_in_context(
        program.compiled.as_ref(),
        &vm_batch,
        &VmExecutionContext { now: execution_now },
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
    let mut success_records = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, &input_row) in result.selected_rows.iter().enumerate() {
        if let Some(side_error) = result.batch.errors()[output_row].first() {
            message_errors.push(PlannedMessageError {
                message: RelayMessage {
                    key: keys[input_row].clone(),
                    record: source_records[input_row].clone(),
                    acks: std::mem::take(&mut acks[input_row]),
                },
                reason: format!(
                    "{} '{}' {} side error {}: {} at {}",
                    processor_kind,
                    processor.as_str(),
                    program_label,
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ),
            });
            continue;
        }
        let record = match vm_output_row_to_decoded_record(&result.batch, output_row) {
            Ok(record) => record.into_runtime_record(source_records[input_row].metadata().clone()),
            Err(error) => {
                message_errors.push(PlannedMessageError {
                    message: RelayMessage {
                        key: keys[input_row].clone(),
                        record: source_records[input_row].clone(),
                        acks: std::mem::take(&mut acks[input_row]),
                    },
                    reason: format!(
                        "{} '{}' failed to materialize {} output row: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        error
                    ),
                });
                continue;
            }
        };
        success_output_rows.push(output_row);
        success_input_rows.push(input_row);
        success_records.push(record);
    }

    let batch = if success_output_rows.is_empty() {
        None
    } else {
        let output_batch = vm_typed_batch_to_runtime_batch(&result.batch).map_err(|error| {
            PlannedGeneralError {
                acks: acks.clone(),
                reason: format!(
                    "{} '{}' failed to materialize {} output batch: {}",
                    processor_kind,
                    processor.as_str(),
                    program_label,
                    error
                ),
            }
        })?;
        let output_batch = if success_output_rows.len() == result.batch.row_count() {
            output_batch
        } else {
            let success_output_rows = success_output_rows.iter().copied().collect::<HashSet<_>>();
            let keep = BooleanArray::from_iter(
                (0..result.batch.row_count()).map(|row| Some(success_output_rows.contains(&row))),
            );
            output_batch
                .filter(&keep)
                .map_err(|error| PlannedGeneralError {
                    acks: acks.clone(),
                    reason: format!(
                        "{} '{}' failed to filter {} output batch: {}",
                        processor_kind,
                        processor.as_str(),
                        program_label,
                        error
                    ),
                })?
        };
        let metadata = success_records
            .iter()
            .map(|record| record.metadata().clone())
            .collect::<Vec<_>>();
        let output_acks = success_input_rows
            .iter()
            .map(|input_row| std::mem::take(&mut acks[*input_row]))
            .collect::<Vec<_>>();
        let error_acks = output_acks.clone();
        Some(
            RelayRecordBatch::from_filtered_parts(
                key,
                output_batch,
                success_records,
                metadata,
                output_acks,
            )
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
    messages: Vec<RelayMessage>,
    headers: Vec<EmitterHeaders>,
    message_errors: Vec<PlannedMessageError>,
}

async fn plan_emitter_filter_map_messages(
    emitter: &Identifier,
    program: &CompiledEmitterFilterMapProgram,
    batch: RelayRecordBatch,
    execution_now: Timestamp,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> Result<EmitterFilterMapPlan, PlannedGeneralError> {
    let input_records = batch.records.clone();
    let source_records = input_records.clone();
    let body_input_records = prepare_filter_map_input_records(
        "emitter",
        emitter,
        &program.body,
        input_records.clone(),
        execution_now,
        side_inputs,
        &batch.keys,
        &batch.acks,
    )
    .await?;
    let header_input_records = if let Some(headers) = &program.headers {
        Some(
            prepare_filter_map_input_records(
                "emitter",
                emitter,
                headers,
                input_records,
                execution_now,
                side_inputs,
                &batch.keys,
                &batch.acks,
            )
            .await?,
        )
    } else {
        None
    };
    let RelayRecordBatch {
        keys,
        metadata: _,
        acks,
        ..
    } = batch;
    let body_result = execute_filter_map_program(
        "emitter",
        emitter,
        &program.body,
        &body_input_records,
        execution_now,
        acks,
    )
    .await?;
    let mut acks = body_result.acks;
    let header_result =
        if let (Some(headers), Some(records)) = (&program.headers, header_input_records.as_ref()) {
            Some(
                execute_filter_map_program(
                    "emitter",
                    emitter,
                    headers,
                    records,
                    execution_now,
                    acks.clone(),
                )
                .await?,
            )
        } else {
            None
        };
    let header_rows_by_input = header_result
        .as_ref()
        .map(|result| {
            result
                .selected_rows
                .iter()
                .enumerate()
                .map(|(output_row, input_row)| (*input_row, output_row))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();

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

    let mut messages = Vec::new();
    let mut headers = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, &input_row) in body_result.selected_rows.iter().enumerate() {
        if let Some(side_error) = body_result.batch.errors()[output_row].first() {
            message_errors.push(PlannedMessageError {
                message: RelayMessage {
                    key: keys[input_row].clone(),
                    record: source_records[input_row].clone(),
                    acks: std::mem::take(&mut acks[input_row]),
                },
                reason: format!(
                    "emitter '{}' FILTER-MAP side error {}: {} at {}",
                    emitter.as_str(),
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ),
            });
            continue;
        }
        let message_headers = if let Some(header_result) = header_result.as_ref() {
            let Some(&header_output_row) = header_rows_by_input.get(&input_row) else {
                message_errors.push(PlannedMessageError {
                    message: RelayMessage {
                        key: keys[input_row].clone(),
                        record: source_records[input_row].clone(),
                        acks: std::mem::take(&mut acks[input_row]),
                    },
                    reason: format!(
                        "emitter '{}' FILTER-MAP header selection did not include input row {}",
                        emitter.as_str(),
                        input_row
                    ),
                });
                continue;
            };
            if let Some(side_error) = header_result.batch.errors()[header_output_row].first() {
                message_errors.push(PlannedMessageError {
                    message: RelayMessage {
                        key: keys[input_row].clone(),
                        record: source_records[input_row].clone(),
                        acks: std::mem::take(&mut acks[input_row]),
                    },
                    reason: format!(
                        "emitter '{}' FILTER-MAP header side error {}: {} at {}",
                        emitter.as_str(),
                        side_error.code.as_str(),
                        side_error.message,
                        side_error.span
                    ),
                });
                continue;
            }
            match emitter_headers_from_output_row(&header_result.batch, header_output_row) {
                Ok(headers) => headers,
                Err(error) => {
                    message_errors.push(PlannedMessageError {
                        message: RelayMessage {
                            key: keys[input_row].clone(),
                            record: source_records[input_row].clone(),
                            acks: std::mem::take(&mut acks[input_row]),
                        },
                        reason: format!(
                            "emitter '{}' failed to materialize FILTER-MAP headers: {}",
                            emitter.as_str(),
                            error
                        ),
                    });
                    continue;
                }
            }
        } else {
            Vec::new()
        };
        let record = match vm_output_row_to_decoded_record(&body_result.batch, output_row) {
            Ok(record) => record.into_runtime_record(source_records[input_row].metadata().clone()),
            Err(error) => {
                message_errors.push(PlannedMessageError {
                    message: RelayMessage {
                        key: keys[input_row].clone(),
                        record: source_records[input_row].clone(),
                        acks: std::mem::take(&mut acks[input_row]),
                    },
                    reason: format!(
                        "emitter '{}' failed to materialize FILTER-MAP output row: {}",
                        emitter.as_str(),
                        error
                    ),
                });
                continue;
            }
        };
        messages.push(RelayMessage {
            key: keys[input_row].clone(),
            record,
            acks: std::mem::take(&mut acks[input_row]),
        });
        headers.push(message_headers);
    }

    Ok(EmitterFilterMapPlan {
        messages,
        headers,
        message_errors,
    })
}

struct ExecutedFilterMap {
    batch: VmTypedBatch,
    selected_rows: Vec<usize>,
    acks: Vec<AckSet>,
}

async fn prepare_filter_map_input_records(
    processor_kind: &str,
    processor: &Identifier,
    program: &CompiledProgramWithMaterializedInterest,
    input_records: Vec<RuntimeRecord>,
    execution_now: Timestamp,
    side_inputs: &HashMap<String, RuntimeValue>,
    branch_keys: &[Option<BranchKey>],
    acks: &[AckSet],
) -> Result<Vec<RuntimeRecord>, PlannedGeneralError> {
    let input_records = augment_runtime_records_with_side_inputs(input_records, side_inputs);
    let input_records = augment_runtime_records_with_branch_keys(input_records, branch_keys)
        .map_err(|error| PlannedGeneralError {
            acks: acks.to_vec(),
            reason: format!(
                "{} '{}' failed to prepare branch inputs: {}",
                processor_kind,
                processor.as_str(),
                error
            ),
        })?;
    augment_runtime_records_with_lookup_hash_maps(input_records, program, execution_now)
        .await
        .map_err(|error| PlannedGeneralError {
            acks: acks.to_vec(),
            reason: format!(
                "{} '{}' failed to prepare LOOKUP_HASH_MAP inputs: {}",
                processor_kind,
                processor.as_str(),
                error
            ),
        })
}

async fn execute_filter_map_program(
    processor_kind: &str,
    processor: &Identifier,
    program: &CompiledProgramWithMaterializedInterest,
    input_records: &[RuntimeRecord],
    execution_now: Timestamp,
    acks: Vec<AckSet>,
) -> Result<ExecutedFilterMap, PlannedGeneralError> {
    let vm_batch =
        match vm_typed_batch_from_runtime_records(input_records, &program.compiled.input_schema) {
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
    let result = match execute_program_with_selection_in_context(
        program.compiled.as_ref(),
        &vm_batch,
        &VmExecutionContext { now: execution_now },
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
        acks,
    })
}

fn emitter_headers_from_output_row(
    batch: &VmTypedBatch,
    row: usize,
) -> Result<EmitterHeaders, String> {
    let record = vm_output_row_to_decoded_record(batch, row)?;
    let mut headers = Vec::new();
    for field in batch.schema().fields() {
        let Some(value) = record.value(field.name()) else {
            continue;
        };
        match value {
            RuntimeValue::String(value) => headers.push((field.name().clone(), value.clone())),
            other => {
                return Err(format!(
                    "header '{}' evaluated to {}, expected STRING",
                    field.name(),
                    runtime_value_type_name(other)
                ));
            }
        }
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
    let Some(base_output_relay) = output_routes
        .routes
        .first()
        .map(|output| output.relay.clone())
    else {
        state.clear(aggregate);
        return true;
    };
    let output_schema =
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, &base_output_relay) {
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
                state.clear(aggregate);
                return true;
            }
        };
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
        let output_record = match evaluate_window_aggregate(
            aggregate,
            state,
            output_schema.arrow_schema().as_ref(),
        ) {
            Ok(record) => record,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    state.entries.iter().map(|entry| &entry.message.acks),
                    format!(
                        "window processor '{}' aggregate failed: {}",
                        processor.as_str(),
                        error
                    ),
                );
                state.clear(aggregate);
                changed = true;
                break;
            }
        };
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
        let output_message = RelayMessage {
            key: first_entry.message.key.clone(),
            record: output_record.into_runtime_record(output_metadata),
            acks: AckSet::merged(
                state
                    .entries
                    .iter()
                    .map(|entry| entry.message.acks.attached()),
            ),
        };
        let forwarded =
            match RelayRecordBatch::from_messages(output_schema.clone(), vec![output_message]) {
                Ok(batch) => batch,
                Err(error) => {
                    branch.runtime.handle_internal_processor_error_for_acks(
                        &branch.domain,
                        node_kind,
                        processor,
                        error_policies,
                        state.entries.iter().map(|entry| &entry.message.acks),
                        format!(
                            "window processor '{}' failed to build output batch: {}",
                            processor.as_str(),
                            error
                        ),
                    );
                    state.clear(aggregate);
                    changed = true;
                    break;
                }
            };
        if let Some(acks) = dispatch_processor_outputs(
            ProcessorOutputDispatchContext {
                graph,
                branch,
                node_kind,
                source_kind: ModelKind::WindowProcessor,
                processor,
                error_policies,
                input_relays: std::slice::from_ref(&base_output_relay),
                filter_source: ProcessorOutputFilterSource::OutputRelay,
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
        match demand.function {
            WindowAggregateFunction::Count => Self::Counter { count: 0 },
            WindowAggregateFunction::First | WindowAggregateFunction::Last => Self::Sequence {
                values: VecDeque::new(),
            },
            WindowAggregateFunction::Max | WindowAggregateFunction::Min => Self::SortedMap {
                counts: BTreeMap::new(),
            },
            WindowAggregateFunction::PercentileLinearHistogram => {
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
            WindowAggregateFunction::Sum => Self::Sum { total: None },
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
        demand: &WindowAggregateDemand,
        timestamp: Timestamp,
        sequence: u64,
        value: Option<RuntimeValue>,
    ) -> Result<(), String> {
        self.purge_expired(timestamp)?;
        let function = demand.function;
        match self {
            Self::Counter { count } => {
                *count = count.saturating_add(1);
                Ok(())
            }
            Self::Sequence { values } => {
                let value = value.ok_or_else(|| format!("{function:?} requires a value"))?;
                values.push_back((timestamp, sequence, value));
                Ok(())
            }
            Self::SortedMap { counts } => {
                let value = value.ok_or_else(|| format!("{function:?} requires a value"))?;
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
        demand: &WindowAggregateDemand,
        removal_time: Timestamp,
        timestamp: Timestamp,
        sequence: u64,
        value: Option<RuntimeValue>,
    ) -> Result<(), String> {
        self.purge_expired(removal_time)?;
        let function = demand.function;
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
                    return Err(format!(
                        "{function:?} sequence accumulator is missing removed window entry"
                    ));
                };
                values.remove(index);
                Ok(())
            }
            Self::SortedMap { counts } => {
                let value = value.ok_or_else(|| format!("{function:?} requires a value"))?;
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

    fn to_snapshot(&self) -> WindowProcessorStateSnapshot {
        WindowProcessorStateSnapshot {
            entries: self
                .entries
                .iter()
                .map(|entry| WindowEntrySnapshot {
                    sequence: entry.sequence,
                    timestamp: entry.timestamp,
                    key: BranchKey::to_remote_key(&entry.message.key),
                    record: entry.message.record.to_remote(),
                })
                .collect(),
            next_sequence: self.next_sequence,
            accumulators: self
                .accumulators
                .iter()
                .map(WindowAggregateAccumulator::to_snapshot)
                .collect(),
        }
    }

    fn from_snapshot(
        program: &WindowAggregateProgram,
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
                            record: RuntimeRecord::from_remote(entry.record),
                            acks: AckSet::empty(),
                        },
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
    ) -> Result<(), Box<(String, RelayMessage)>> {
        let sequence = self.next_sequence;
        let inputs = window_aggregate_inputs(program.demands(), Some(&message.record))
            .map_err(|error| Box::new((error, message.clone())))?;
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
        let inputs = window_aggregate_inputs(program.demands(), Some(&entry.message.record))?;
        self.apply_aggregate_inputs(
            program.demands(),
            entry.timestamp,
            entry.sequence,
            &inputs,
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

#[derive(Debug, Clone, Copy)]
enum WindowAccumulatorAction {
    Add,
    Remove { at: Timestamp },
}

fn window_aggregate_inputs(
    demands: &[WindowAggregateDemand],
    record: Option<&RuntimeRecord>,
) -> Result<Vec<WindowAggregateInput>, String> {
    demands
        .iter()
        .map(|demand| {
            let value = match &demand.input {
                Some(input) => Some(evaluate_runtime_expr(input, record)?),
                None => None,
            };
            Ok(WindowAggregateInput { value })
        })
        .collect()
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

fn evaluate_window_aggregate(
    program: &WindowAggregateProgram,
    state: &WindowProcessorState,
    output_schema: &arrow_schema::Schema,
) -> Result<DecodedRecord, String> {
    let mut fields = Vec::with_capacity(program.assignments.len());
    for assignment in &program.assignments {
        let target_type = output_schema
            .field_with_name(&assignment.target.field)
            .map(|field| field.data_type())
            .map_err(|_| {
                format!(
                    "output schema is missing aggregate target '{}'",
                    assignment.target.field
                )
            })?;
        let value = evaluate_window_aggregate_expr(&assignment.value.inner, state, target_type)?;
        fields.push((assignment.target.field.clone(), value));
    }
    Ok(DecodedRecord::from_fields(fields))
}

fn evaluate_window_aggregate_expr(
    expr: &WindowAggregateExpr,
    state: &WindowProcessorState,
    target_type: &ArrowDataType,
) -> Result<RuntimeValue, String> {
    match expr {
        WindowAggregateExpr::Scalar(expr) => evaluate_runtime_expr(&expr.inner, None),
        WindowAggregateExpr::Array(items) => {
            let values = items
                .iter()
                .map(|item| evaluate_window_aggregate_expr(&item.inner, state, target_type))
                .collect::<Result<Vec<_>, _>>()?;
            if let ArrowDataType::FixedSizeList(_, _) = target_type {
                Ok(RuntimeValue::Array(values))
            } else {
                Ok(RuntimeValue::Vec(values))
            }
        }
        WindowAggregateExpr::AggregateCall(call) => {
            let Some(accumulator) = state.accumulators.get(call.demand_id) else {
                return Err(format!(
                    "window aggregate is missing accumulator for {:?}",
                    call.function
                ));
            };
            accumulator.evaluate(call.function, call.percentile)
        }
    }
}

fn evaluate_runtime_expr(
    expr: &Expr,
    record: Option<&RuntimeRecord>,
) -> Result<RuntimeValue, String> {
    match expr {
        Expr::Literal(Literal::Int64(value)) => Ok(RuntimeValue::I64(*value)),
        Expr::Literal(Literal::Float64(value)) => Ok(RuntimeValue::F64(OrderedFloat(*value))),
        Expr::Literal(Literal::Bool(value)) => Ok(RuntimeValue::Bool(*value)),
        Expr::Literal(Literal::String(value)) => Ok(RuntimeValue::String(value.clone())),
        Expr::Literal(Literal::Null) => {
            Err("NULL requires a declared optional assignment target".to_string())
        }
        Expr::InternalFieldRef(field_ref) => Err(format!(
            "internal field reference {:?}.{} is not available in aggregate input",
            field_ref.namespace, field_ref.field
        )),
        Expr::FieldRef(field_ref) => record
            .and_then(|record| record.value(&field_ref.field))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "missing field '{}.{}' in aggregate input",
                    field_ref.relay, field_ref.field
                )
            }),
        Expr::Unary { op, expr } => {
            let value = evaluate_runtime_expr(&expr.inner, record)?;
            match op {
                UnaryOp::Neg => negate_runtime_value(value),
                UnaryOp::Not => match value {
                    RuntimeValue::Bool(value) => Ok(RuntimeValue::Bool(!value)),
                    other => Err(format!(
                        "NOT expects BOOL, found {}",
                        runtime_value_type_name(&other)
                    )),
                },
            }
        }
        Expr::Binary { op, left, right } => {
            let left = evaluate_runtime_expr(&left.inner, record)?;
            let right = evaluate_runtime_expr(&right.inner, record)?;
            evaluate_runtime_binary(*op, left, right)
        }
        Expr::Cast { expr, data_type } => {
            let value = evaluate_runtime_expr(&expr.inner, record)?;
            cast_runtime_value(value, data_type)
        }
        Expr::Call { function, args } => evaluate_runtime_function(function, args, record),
    }
}

fn evaluate_runtime_function(
    function: &FunctionName,
    args: &[SpannedExpr],
    record: Option<&RuntimeRecord>,
) -> Result<RuntimeValue, String> {
    let values = args
        .iter()
        .map(|arg| evaluate_runtime_expr(&arg.inner, record))
        .collect::<Result<Vec<_>, _>>()?;
    match function {
        FunctionName::Abs => {
            let value = values
                .into_iter()
                .next()
                .ok_or_else(|| "abs expects one argument".to_string())?;
            abs_runtime_value(value)
        }
        FunctionName::Lower => {
            unary_string_function(values, "lower", |value| value.to_ascii_lowercase())
        }
        FunctionName::Upper => {
            unary_string_function(values, "upper", |value| value.to_ascii_uppercase())
        }
        FunctionName::Trim => {
            unary_string_function(values, "trim", |value| value.trim().to_string())
        }
        FunctionName::Length => {
            let value = values
                .first()
                .ok_or_else(|| "length expects one argument".to_string())?;
            if let RuntimeValue::String(value) = value {
                Ok(RuntimeValue::I64(value.chars().count() as i64))
            } else {
                Err(format!(
                    "length expects STRING, found {}",
                    runtime_value_type_name(value)
                ))
            }
        }
        other => Err(format!(
            "function '{}' is not supported inside window aggregate expressions yet",
            other.as_str()
        )),
    }
}

fn unary_string_function(
    values: Vec<RuntimeValue>,
    name: &str,
    op: impl FnOnce(&str) -> String,
) -> Result<RuntimeValue, String> {
    let value = values
        .first()
        .ok_or_else(|| format!("{name} expects one argument"))?;
    if let RuntimeValue::String(value) = value {
        Ok(RuntimeValue::String(op(value)))
    } else {
        Err(format!(
            "{name} expects STRING, found {}",
            runtime_value_type_name(value)
        ))
    }
}

fn negate_runtime_value(value: RuntimeValue) -> Result<RuntimeValue, String> {
    match value {
        RuntimeValue::I8(value) => Ok(RuntimeValue::I8(-value)),
        RuntimeValue::I16(value) => Ok(RuntimeValue::I16(-value)),
        RuntimeValue::I32(value) => Ok(RuntimeValue::I32(-value)),
        RuntimeValue::I64(value) => Ok(RuntimeValue::I64(-value)),
        RuntimeValue::F32(value) => Ok(RuntimeValue::F32(OrderedFloat(-value.0))),
        RuntimeValue::F64(value) => Ok(RuntimeValue::F64(OrderedFloat(-value.0))),
        other => Err(format!(
            "numeric negation does not support {}",
            runtime_value_type_name(&other)
        )),
    }
}

fn abs_runtime_value(value: RuntimeValue) -> Result<RuntimeValue, String> {
    match value {
        RuntimeValue::I8(value) => Ok(RuntimeValue::I8(value.abs())),
        RuntimeValue::I16(value) => Ok(RuntimeValue::I16(value.abs())),
        RuntimeValue::I32(value) => Ok(RuntimeValue::I32(value.abs())),
        RuntimeValue::I64(value) => Ok(RuntimeValue::I64(value.abs())),
        RuntimeValue::F32(value) => Ok(RuntimeValue::F32(OrderedFloat(value.0.abs()))),
        RuntimeValue::F64(value) => Ok(RuntimeValue::F64(OrderedFloat(value.0.abs()))),
        other => Err(format!(
            "abs does not support {}",
            runtime_value_type_name(&other)
        )),
    }
}

fn evaluate_runtime_binary(
    op: BinaryOp,
    left: RuntimeValue,
    right: RuntimeValue,
) -> Result<RuntimeValue, String> {
    match op {
        BinaryOp::Add => numeric_binary(left, right, |left, right| left + right),
        BinaryOp::Sub => numeric_binary(left, right, |left, right| left - right),
        BinaryOp::Mul => numeric_binary(left, right, |left, right| left * right),
        BinaryOp::Div => numeric_binary(left, right, |left, right| left / right),
        BinaryOp::Rem => numeric_binary(left, right, |left, right| left % right),
        BinaryOp::Eq => Ok(RuntimeValue::Bool(left == right)),
        BinaryOp::NotEq => Ok(RuntimeValue::Bool(left != right)),
        BinaryOp::Gt => Ok(RuntimeValue::Bool(
            compare_runtime_values(&left, &right).is_gt(),
        )),
        BinaryOp::Lt => Ok(RuntimeValue::Bool(
            compare_runtime_values(&left, &right).is_lt(),
        )),
        BinaryOp::GtEq => Ok(RuntimeValue::Bool(
            !compare_runtime_values(&left, &right).is_lt(),
        )),
        BinaryOp::LtEq => Ok(RuntimeValue::Bool(
            !compare_runtime_values(&left, &right).is_gt(),
        )),
        BinaryOp::And => match (left, right) {
            (RuntimeValue::Bool(left), RuntimeValue::Bool(right)) => {
                Ok(RuntimeValue::Bool(left && right))
            }
            (left, right) => Err(format!(
                "AND expects BOOL operands, found {} and {}",
                runtime_value_type_name(&left),
                runtime_value_type_name(&right)
            )),
        },
        BinaryOp::Or => match (left, right) {
            (RuntimeValue::Bool(left), RuntimeValue::Bool(right)) => {
                Ok(RuntimeValue::Bool(left || right))
            }
            (left, right) => Err(format!(
                "OR expects BOOL operands, found {} and {}",
                runtime_value_type_name(&left),
                runtime_value_type_name(&right)
            )),
        },
    }
}

fn numeric_binary(
    left: RuntimeValue,
    right: RuntimeValue,
    op: impl FnOnce(f64, f64) -> f64,
) -> Result<RuntimeValue, String> {
    Ok(RuntimeValue::F64(OrderedFloat(op(
        runtime_value_to_f64(&left)?,
        runtime_value_to_f64(&right)?,
    ))))
}

fn cast_runtime_value(
    value: RuntimeValue,
    data_type: &ArrowDataType,
) -> Result<RuntimeValue, String> {
    match data_type {
        ArrowDataType::Float64 => Ok(RuntimeValue::F64(OrderedFloat(runtime_value_to_f64(
            &value,
        )?))),
        ArrowDataType::Float32 => Ok(RuntimeValue::F32(OrderedFloat(
            runtime_value_to_f64(&value)? as f32,
        ))),
        ArrowDataType::Int64 => Ok(RuntimeValue::I64(runtime_value_to_f64(&value)? as i64)),
        ArrowDataType::Utf8 => Ok(RuntimeValue::String(match value {
            RuntimeValue::String(value) => value,
            other => other.to_key_fragment(),
        })),
        _ => Ok(value),
    }
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

fn vm_typed_batch_from_runtime_record(
    record: &RuntimeRecord,
    filter_map_metadata: Option<&IngestFilterMapMetadata>,
    schema: &Arc<arrow_schema::Schema>,
) -> Result<VmTypedBatch, String> {
    vm_typed_batch_from_runtime_records_with_metadata(
        std::slice::from_ref(record),
        filter_map_metadata.map(std::slice::from_ref),
        schema,
    )
}

fn vm_typed_batch_from_runtime_records(
    records: &[RuntimeRecord],
    schema: &Arc<arrow_schema::Schema>,
) -> Result<VmTypedBatch, String> {
    vm_typed_batch_from_runtime_records_with_metadata(records, None, schema)
}

fn resolve_filter_map_input_value<'a>(
    record: &'a RuntimeRecord,
    filter_map_metadata: Option<&'a [IngestFilterMapMetadata]>,
    index: usize,
    field: &arrow_schema::Field,
) -> Result<Option<&'a RuntimeValue>, String> {
    if let Some(value) = record.value(field.name()) {
        return Ok(Some(value));
    }
    let Some((namespace, field_name)) = field.name().split_once('.') else {
        return Ok(record.value(field.name()));
    };

    if namespace == INGEST_METADATA_NAMESPACE {
        return Ok(filter_map_metadata
            .and_then(|rows| rows.get(index))
            .and_then(|row| row.metadata_value(field_name)));
    }

    if namespace == INGEST_HEADERS_NAMESPACE {
        return Ok(filter_map_metadata
            .and_then(|rows| rows.get(index))
            .and_then(|row| row.header_value(field_name)));
    }

    if namespace == BRANCH_NAMESPACE {
        return Ok(record.value(field.name()));
    }

    Ok(record.value(field_name))
}

fn filter_map_input_value<'a>(
    record: &'a RuntimeRecord,
    filter_map_metadata: Option<&'a [IngestFilterMapMetadata]>,
    index: usize,
    field: &arrow_schema::Field,
) -> Result<Option<&'a RuntimeValue>, String> {
    let value = resolve_filter_map_input_value(record, filter_map_metadata, index, field)?;
    if value.is_none() && !field.is_nullable() {
        return Err(format!(
            "FILTER-MAP input record is missing field '{}'",
            field.name()
        ));
    }
    Ok(value)
}

fn augment_runtime_record_with_side_inputs(
    record: RuntimeRecord,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> RuntimeRecord {
    if side_inputs.is_empty() {
        return record;
    }
    let metadata = record.metadata().clone();
    let mut fields = record
        .to_remote()
        .fields
        .into_iter()
        .map(|field| (field.name, RuntimeValue::from_remote(field.value)))
        .collect::<HashMap<_, _>>();
    for (name, value) in side_inputs {
        fields.insert(name.clone(), value.clone());
    }
    RuntimeRecord::from_fields_with_metadata(fields, metadata)
}

fn augment_runtime_records_with_side_inputs(
    records: Vec<RuntimeRecord>,
    side_inputs: &HashMap<String, RuntimeValue>,
) -> Vec<RuntimeRecord> {
    records
        .into_iter()
        .map(|record| augment_runtime_record_with_side_inputs(record, side_inputs))
        .collect()
}

fn augment_runtime_record_with_branch_key(
    record: RuntimeRecord,
    branch_key: Option<&BranchKey>,
) -> RuntimeRecord {
    let Some(branch_key) = branch_key else {
        return record;
    };
    let metadata = record.metadata().clone();
    let mut fields = record
        .to_remote()
        .fields
        .into_iter()
        .map(|field| (field.name, RuntimeValue::from_remote(field.value)))
        .collect::<HashMap<_, _>>();
    for (name, value) in branch_key.fields() {
        fields.insert(
            format!("{BRANCH_NAMESPACE}.{}", name.as_str()),
            value.clone(),
        );
    }
    RuntimeRecord::from_fields_with_metadata(fields, metadata)
}

fn augment_runtime_records_with_branch_keys(
    records: Vec<RuntimeRecord>,
    keys: &[Option<BranchKey>],
) -> Result<Vec<RuntimeRecord>, String> {
    if records.len() != keys.len() {
        return Err(format!(
            "branch key count {} does not match record count {}",
            keys.len(),
            records.len()
        ));
    }
    Ok(records
        .into_iter()
        .zip(keys)
        .map(|(record, key)| augment_runtime_record_with_branch_key(record, key.as_ref()))
        .collect())
}

fn runtime_record_with_field(
    record: RuntimeRecord,
    name: String,
    value: RuntimeValue,
) -> RuntimeRecord {
    let metadata = record.metadata().clone();
    let mut fields = record
        .to_remote()
        .fields
        .into_iter()
        .map(|field| (field.name, RuntimeValue::from_remote(field.value)))
        .collect::<HashMap<_, _>>();
    fields.insert(name, value);
    RuntimeRecord::from_fields_with_metadata(fields, metadata)
}

async fn runtime_record_lookup_key(
    call: &LookupHashMapCall,
    records: &[RuntimeRecord],
    execution_now: Timestamp,
) -> Result<Vec<Option<String>>, String> {
    let vm_batch = vm_typed_batch_from_runtime_records(records, &call.key_program.input_schema)?;
    let result = execute_program_with_selection_in_context(
        call.key_program.as_ref(),
        &vm_batch,
        &VmExecutionContext { now: execution_now },
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
    let mut keys = vec![None; records.len()];
    for (output_row, &input_row) in result.selected_rows.iter().enumerate() {
        if let Some(side_error) = result.batch.errors()[output_row].first() {
            return Err(format!(
                "LOOKUP_HASH_MAP key side error {}: {} at {}",
                side_error.code.as_str(),
                side_error.message,
                side_error.span
            ));
        }
        let record = vm_output_row_to_decoded_record(&result.batch, output_row)?;
        if let Some(value) = record.value(&call.generated_field) {
            keys[input_row] = Some(value.to_key_fragment());
        }
    }
    Ok(keys)
}

async fn augment_runtime_records_with_lookup_hash_maps(
    records: Vec<RuntimeRecord>,
    program: &CompiledProgramWithMaterializedInterest,
    execution_now: Timestamp,
) -> Result<Vec<RuntimeRecord>, String> {
    if program.lookup_hash_maps.is_empty() {
        return Ok(records);
    }
    let mut records = records;
    for call in &program.lookup_hash_maps {
        let keys = runtime_record_lookup_key(call, &records, execution_now).await?;
        records = records
            .into_iter()
            .zip(keys)
            .map(|(record, key)| {
                let Some(key) = key else {
                    return record;
                };
                let Some(lookup_record) = call.lookup_runtime.entries.get(&key) else {
                    return record;
                };
                let Some(value) = lookup_record.value(&call.lookup_field).cloned() else {
                    return record;
                };
                runtime_record_with_field(
                    record,
                    VmCompileNamespace::Internal(InternalFieldNamespace::LookupHashMap)
                        .qualified_field_name(&call.generated_field),
                    value,
                )
            })
            .collect();
    }
    Ok(records)
}

fn materialized_record_from_entries(
    entries: Vec<(String, RuntimeRecord)>,
    key: Option<&str>,
) -> Option<RuntimeRecord> {
    let Some(key) = key else {
        return entries.into_iter().next().map(|(_, record)| record);
    };
    entries
        .into_iter()
        .find_map(|(entry_key, record)| (entry_key == key).then_some(record))
}

fn filter_map_list_values<'a>(
    value: &'a RuntimeValue,
    field: &arrow_schema::Field,
) -> Result<&'a [RuntimeValue], String> {
    match value {
        RuntimeValue::Array(values) | RuntimeValue::Vec(values) => Ok(values),
        _ => Err(format!(
            "FILTER-MAP input field '{}' expected {:?}, got {}",
            field.name(),
            field.data_type(),
            runtime_value_type_name(value)
        )),
    }
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

macro_rules! build_filter_map_list_input {
    ($records:expr, $filter_map_metadata:expr, $field:expr, $builder:expr, $append:expr) => {{
        let mut builder = ListBuilder::new($builder);
        for (index, record) in $records.iter().enumerate() {
            let Some(value) = filter_map_input_value(record, $filter_map_metadata, index, $field)?
            else {
                builder.append(false);
                continue;
            };
            for value in filter_map_list_values(value, $field)? {
                $append(builder.values(), value, $field)?;
            }
            builder.append(true);
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

macro_rules! build_filter_map_fixed_list_input {
    ($records:expr, $filter_map_metadata:expr, $field:expr, $len:expr, $builder:expr, $append:expr) => {{
        let mut builder = FixedSizeListBuilder::new($builder, $len);
        for (index, record) in $records.iter().enumerate() {
            let Some(value) = filter_map_input_value(record, $filter_map_metadata, index, $field)?
            else {
                builder.append(false);
                continue;
            };
            let values = filter_map_list_values(value, $field)?;
            if values.len() != usize::try_from($len).unwrap_or(usize::MAX) {
                return Err(format!(
                    "FILTER-MAP input field '{}' expected array length {}, got {}",
                    $field.name(),
                    $len,
                    values.len()
                ));
            }
            for value in values {
                $append(builder.values(), value, $field)?;
            }
            builder.append(true);
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

fn build_filter_map_list_input_column(
    records: &[RuntimeRecord],
    filter_map_metadata: Option<&[IngestFilterMapMetadata]>,
    field: &arrow_schema::Field,
    element: &ArrowDataType,
) -> Result<ArrayRef, String> {
    match element {
        ArrowDataType::UInt8 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            UInt8Builder::new(),
            append_filter_map_u8
        ),
        ArrowDataType::Int8 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            Int8Builder::new(),
            append_filter_map_i8
        ),
        ArrowDataType::UInt16 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            UInt16Builder::new(),
            append_filter_map_u16
        ),
        ArrowDataType::Int16 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            Int16Builder::new(),
            append_filter_map_i16
        ),
        ArrowDataType::UInt32 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            UInt32Builder::new(),
            append_filter_map_u32
        ),
        ArrowDataType::Int32 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            Int32Builder::new(),
            append_filter_map_i32
        ),
        ArrowDataType::UInt64 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            UInt64Builder::new(),
            append_filter_map_u64
        ),
        ArrowDataType::Int64 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            Int64Builder::new(),
            append_filter_map_i64
        ),
        ArrowDataType::Float32 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            Float32Builder::new(),
            append_filter_map_f32
        ),
        ArrowDataType::Float64 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            Float64Builder::new(),
            append_filter_map_f64
        ),
        ArrowDataType::Boolean => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            BooleanBuilder::new(),
            append_filter_map_bool
        ),
        ArrowDataType::Utf8 => build_filter_map_list_input!(
            records,
            filter_map_metadata,
            field,
            StringBuilder::new(),
            append_filter_map_string
        ),
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
            if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" =>
        {
            let value_builder = TimestampNanosecondBuilder::new().with_data_type(
                ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+00:00".into())),
            );
            build_filter_map_list_input!(
                records,
                filter_map_metadata,
                field,
                value_builder,
                append_filter_map_datetime
            )
        }
        _ => Err(format!(
            "FILTER-MAP input field '{}' has unsupported list element type {:?}",
            field.name(),
            element
        )),
    }
}

fn build_filter_map_fixed_list_input_column(
    records: &[RuntimeRecord],
    filter_map_metadata: Option<&[IngestFilterMapMetadata]>,
    field: &arrow_schema::Field,
    element: &ArrowDataType,
    len: i32,
) -> Result<ArrayRef, String> {
    match element {
        ArrowDataType::UInt8 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            UInt8Builder::new(),
            append_filter_map_u8
        ),
        ArrowDataType::Int8 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            Int8Builder::new(),
            append_filter_map_i8
        ),
        ArrowDataType::UInt16 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            UInt16Builder::new(),
            append_filter_map_u16
        ),
        ArrowDataType::Int16 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            Int16Builder::new(),
            append_filter_map_i16
        ),
        ArrowDataType::UInt32 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            UInt32Builder::new(),
            append_filter_map_u32
        ),
        ArrowDataType::Int32 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            Int32Builder::new(),
            append_filter_map_i32
        ),
        ArrowDataType::UInt64 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            UInt64Builder::new(),
            append_filter_map_u64
        ),
        ArrowDataType::Int64 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            Int64Builder::new(),
            append_filter_map_i64
        ),
        ArrowDataType::Float32 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            Float32Builder::new(),
            append_filter_map_f32
        ),
        ArrowDataType::Float64 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            Float64Builder::new(),
            append_filter_map_f64
        ),
        ArrowDataType::Boolean => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            BooleanBuilder::new(),
            append_filter_map_bool
        ),
        ArrowDataType::Utf8 => build_filter_map_fixed_list_input!(
            records,
            filter_map_metadata,
            field,
            len,
            StringBuilder::new(),
            append_filter_map_string
        ),
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
            if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" =>
        {
            let value_builder = TimestampNanosecondBuilder::new().with_data_type(
                ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+00:00".into())),
            );
            build_filter_map_fixed_list_input!(
                records,
                filter_map_metadata,
                field,
                len,
                value_builder,
                append_filter_map_datetime
            )
        }
        _ => Err(format!(
            "FILTER-MAP input field '{}' has unsupported list element type {:?}",
            field.name(),
            element
        )),
    }
}

fn vm_typed_batch_from_runtime_records_with_metadata(
    records: &[RuntimeRecord],
    filter_map_metadata: Option<&[IngestFilterMapMetadata]>,
    schema: &Arc<arrow_schema::Schema>,
) -> Result<VmTypedBatch, String> {
    if let Some(metadata_rows) = filter_map_metadata
        && metadata_rows.len() != records.len()
    {
        return Err(format!(
            "FILTER-MAP metadata row count {} does not match record count {}",
            metadata_rows.len(),
            records.len()
        ));
    }

    let columns = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            ArrowDataType::UInt8 => Ok(VmTypedArray::UInt8(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::U8(value)) => Ok(Some(*value)),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::Int8 => Ok(VmTypedArray::Int8(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::I8(value)) => Ok(Some(*value)),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::UInt16 => Ok(VmTypedArray::UInt16(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::U16(value)) => Ok(Some(*value)),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::Int16 => Ok(VmTypedArray::Int16(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::I16(value)) => Ok(Some(*value)),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::UInt32 => Ok(VmTypedArray::UInt32(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::U32(value)) => Ok(Some(*value)),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::Int32 => Ok(VmTypedArray::Int32(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::I32(value)) => Ok(Some(*value)),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::UInt64 => Ok(VmTypedArray::UInt64(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::U64(value)) => Ok(Some(*value)),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::Int64 => Ok(VmTypedArray::Int64(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::I64(value)) => Ok(Some(*value)),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::Float32 => Ok(VmTypedArray::Float32(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::F32(value)) => Ok(Some(value.into_inner())),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::Float64 => Ok(VmTypedArray::Float64(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::F64(value)) => Ok(Some(value.into_inner())),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::Boolean => Ok(VmTypedArray::Boolean(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::Bool(value)) => Ok(Some(*value)),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::Utf8 => Ok(VmTypedArray::Utf8(
                records
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        match filter_map_input_value(record, filter_map_metadata, index, field)? {
                            Some(RuntimeValue::String(value)) => Ok(Some(value.clone())),
                            Some(RuntimeValue::Datetime(value)) => Ok(Some(value.to_rfc3339())),
                            Some(value) => Err(format!(
                                "FILTER-MAP input field '{}' expected {:?}, got {}",
                                field.name(),
                                field.data_type(),
                                runtime_value_type_name(value)
                            )),
                            None => Ok(None),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
                if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" =>
            {
                Ok(VmTypedArray::Datetime(
                    records
                        .iter()
                        .enumerate()
                        .map(|(index, record)| {
                            match filter_map_input_value(record, filter_map_metadata, index, field)?
                            {
                                Some(RuntimeValue::Datetime(value)) => {
                                    Ok(value.timestamp_nanos_opt())
                                }
                                Some(value) => Err(format!(
                                    "FILTER-MAP input field '{}' expected {:?}, got {}",
                                    field.name(),
                                    field.data_type(),
                                    runtime_value_type_name(value)
                                )),
                                None => Ok(None),
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .collect::<arrow_array::TimestampNanosecondArray>()
                        .with_timezone_utc(),
                ))
            }
            ArrowDataType::List(element) => {
                Ok(VmTypedArray::Generic(build_filter_map_list_input_column(
                    records,
                    filter_map_metadata,
                    field,
                    element.data_type(),
                )?))
            }
            ArrowDataType::FixedSizeList(element, len) => Ok(VmTypedArray::Generic(
                build_filter_map_fixed_list_input_column(
                    records,
                    filter_map_metadata,
                    field,
                    element.data_type(),
                    *len,
                )?,
            )),
            _ => Err(format!(
                "FILTER-MAP input field '{}' has unsupported type {:?}",
                field.name(),
                field.data_type()
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    VmTypedBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}

fn vm_output_row_to_decoded_record(
    batch: &VmTypedBatch,
    row: usize,
) -> Result<DecodedRecord, String> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            let value = match column {
                VmTypedArray::UInt8(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::U8(values.value(row))),
                VmTypedArray::Int8(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::I8(values.value(row))),
                VmTypedArray::UInt16(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::U16(values.value(row))),
                VmTypedArray::Int16(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::I16(values.value(row))),
                VmTypedArray::UInt32(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::U32(values.value(row))),
                VmTypedArray::Int32(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::I32(values.value(row))),
                VmTypedArray::UInt64(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::U64(values.value(row))),
                VmTypedArray::Int64(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::I64(values.value(row))),
                VmTypedArray::Float32(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::F32(values.value(row).into())),
                VmTypedArray::Float64(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::F64(values.value(row).into())),
                VmTypedArray::Boolean(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::Bool(values.value(row))),
                VmTypedArray::Utf8(values) => values
                    .is_valid(row)
                    .then(|| RuntimeValue::String(values.value(row).to_string())),
                VmTypedArray::Datetime(values) => values.is_valid(row).then(|| {
                    RuntimeValue::Datetime(
                        chrono::DateTime::from_timestamp_nanos(values.value(row)).fixed_offset(),
                    )
                }),
                VmTypedArray::Generic(_) => {
                    return Err(format!(
                        "FILTER-MAP output field '{}' has unsupported type {:?}",
                        field.name(),
                        field.data_type()
                    ));
                }
            };
            if let Some(value) = value {
                Ok(Some((field.name().to_string(), value)))
            } else if field.is_nullable() {
                Ok(None)
            } else {
                Err(format!(
                    "FILTER-MAP output field '{}' contains null at row {row}",
                    field.name()
                ))
            }
        })
        .collect::<Result<Vec<_>, String>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    Ok(DecodedRecord::from_fields(fields))
}

fn vm_typed_batch_to_runtime_batch(batch: &VmTypedBatch) -> Result<RuntimeRecordBatch, String> {
    let record_batch = batch.to_record_batch().map_err(|error| error.to_string())?;
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
) -> Option<Arc<arrow_schema::Schema>> {
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

async fn flush_branch_junction(
    context: JunctionFlushContext<'_>,
    pending: &mut Vec<RelayRecordBatch>,
    next_flush: &mut Option<Timestamp>,
) {
    let JunctionFlushContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        input_relays,
        output_routes,
    } = context;
    if pending.is_empty() {
        *next_flush = None;
        return;
    }
    let grouped_batches = std::mem::take(pending);
    *next_flush = None;
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
                        "junction '{}' failed to concat arrow batches: {}",
                        processor.as_str(),
                        error
                    ),
                );
            }
            return;
        }
    };

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

async fn flush_branch_inferencer(
    context: InferencerFlushContext<'_>,
    pending: &mut Vec<RelayRecordBatch>,
    next_flush: &mut Option<Timestamp>,
) {
    let InferencerFlushContext {
        branch,
        node_kind,
        processor,
        error_policies,
        output_routes,
        resource,
        resource_version,
        file,
        inputs,
        outputs,
        ..
    } = context;
    if pending.is_empty() {
        *next_flush = None;
        return;
    }
    let grouped_batches = std::mem::take(pending);
    *next_flush = None;
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
                        "inferencer '{}' failed to concat arrow batches: {}",
                        processor.as_str(),
                        error
                    ),
                );
            }
            return;
        }
    };

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

    let version = resource_version
        .map(|version| format!("@{version}"))
        .unwrap_or_else(|| "@latest".to_string());
    let output_names = output_routes
        .base_relay()
        .map(|relay| relay.as_str().to_string())
        .unwrap_or_else(|| "<none>".to_string());
    for message in messages {
        branch
            .runtime
            .handle_message_error(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                message,
                format!(
                    "inferencer '{}' reached branch-local runtime but ONNX execution is not \
                     implemented yet for resource '{}{}' file '{}' into output route '{}' ({} \
                     inputs, {} outputs)",
                    processor.as_str(),
                    resource.as_str(),
                    version,
                    file,
                    output_names,
                    inputs.len(),
                    outputs.len()
                ),
            )
            .await;
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

    let Some(instance) = instance.as_mut() else {
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
    };

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
    let outputs = match instance.process_envelope(&envelope).await {
        Ok(outputs) => outputs,
        Err(error) => {
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
    if let Err(error) =
        persist_wasm_guest_state(&branch.runtime, processor, replicated_state, instance).await
    {
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
    guest_input_relay: &'a Identifier,
    input_schema: &'a Arc<CompiledSchema>,
    output_schemas: &'a [(Identifier, Arc<CompiledSchema>)],
    replicated_state: &'a ReplicatedWasmProcessorState,
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
        guest_input_relay,
        input_schema,
        output_schemas,
        replicated_state,
    } = context;
    let version = match resource_version {
        Some(version) => version,
        None => branch
            .runtime
            .resolve_resource_version(resource, resource.as_str())?,
    };
    let needs_compile = compiled
        .as_ref()
        .is_none_or(|compiled| compiled.version != version);
    if needs_compile {
        let Some(resource_store) = branch.runtime.resource_store.read().clone() else {
            return Err("resource store is not attached".to_string());
        };
        let path = resource_store
            .resolve_content_path(resource, version, file)
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
        let module = branch
            .runtime
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
        *compiled = Some(WasmCompiledBranchProcessor {
            version,
            compiled: module,
        });
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
                .instantiate_branch(init, Box::new(clock), restored_guest_state.as_deref())
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
    if batch.records.len() != batch.acks.len() || batch.records.len() != batch.metadata.len() {
        return Err(format!(
            "wasm input row count {} does not match ack count {} and metadata count {}",
            batch.records.len(),
            batch.acks.len(),
            batch.metadata.len()
        ));
    }
    let mut rows = Vec::with_capacity(batch.acks.len());
    let mut ack_map = HashMap::with_capacity(batch.acks.len());
    let input_batch = Arc::new(batch.batch.clone());
    for (input_row, (record, acks)) in batch.records.iter().zip(batch.acks.iter()).enumerate() {
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
                metadata: record.metadata().clone(),
                record: record.clone(),
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
    input_records: Vec<Option<RuntimeRecord>>,
}

#[derive(Debug)]
struct WasmMaterializedOutput {
    output_route_index: usize,
    schema: Arc<CompiledSchema>,
    batch: RuntimeRecordBatch,
    acks: WasmAckSidecar,
}

#[derive(Debug, Error)]
enum WasmOutputError {
    #[error("expected an output envelope at callback index {envelope_index}")]
    UnexpectedEnvelopeKind { envelope_index: usize },
    #[error("unknown WASM output relay '{output_relay}'")]
    UnknownOutputRelay { output_relay: String },
    #[error(
        "WASM output relay '{output_relay}' has {actual} columns, but its destination schema has \
         {expected} fields"
    )]
    OutputColumnCountMismatch {
        output_relay: String,
        expected: usize,
        actual: usize,
    },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} ('{field_name}') has invalid \
         guest Arrow IPC: {reason}"
    )]
    InvalidGuestArrowIpc {
        output_relay: String,
        field_index: usize,
        field_name: String,
        reason: String,
    },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} guest Arrow field mismatch: \
         expected {expected}, actual {actual}"
    )]
    GuestArrowFieldMismatch {
        output_relay: String,
        field_index: usize,
        expected: String,
        actual: String,
    },
    #[error(
        "WASM output relay '{output_relay}' field {field_index} guest Arrow row count mismatch: \
         expected {expected}, actual {actual}"
    )]
    GuestArrowRowCountMismatch {
        output_relay: String,
        field_index: usize,
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
        outputs
            .into_iter()
            .enumerate()
            .map(|(envelope_index, output)| self.materialize(envelope_index, output))
            .collect()
    }

    fn validate_token_decisions(&self, outputs: &[WasmEnvelope]) -> Result<(), WasmOutputError> {
        let mut carried_tokens = HashSet::<u64>::default();
        let mut terminal_tokens = HashSet::<u64>::default();
        for (envelope_index, output) in outputs.iter().enumerate() {
            let WasmEnvelope::Output {
                output_relay, acks, ..
            } = output
            else {
                return Err(WasmOutputError::UnexpectedEnvelopeKind { envelope_index });
            };
            for (row_index, row) in acks.rows.iter().enumerate() {
                if let Some(source_token) = row.source_token
                    && !self.ack_map.contains_key(&source_token.0)
                {
                    return Err(WasmOutputError::UnknownSourceToken {
                        output_relay: output_relay.clone(),
                        row_index,
                        token: source_token.0,
                    });
                }
                let mut row_tokens = HashSet::<u64>::default();
                for token in &row.tokens {
                    if !self.ack_map.contains_key(&token.0) {
                        return Err(WasmOutputError::InvalidTokenDecision {
                            token: token.0,
                            reason: "carried token is unknown to this branch instance".to_string(),
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
            for token_set in &acks.acked {
                self.validate_terminal_set(token_set, &mut terminal_tokens, "ACK")?;
            }
            for token_set in &acks.nacked {
                self.validate_terminal_tokens(&token_set.tokens, &mut terminal_tokens, "NACK")?;
            }
            for token_set in &acks.message_errors {
                self.validate_terminal_tokens(
                    &token_set.tokens,
                    &mut terminal_tokens,
                    "message error",
                )?;
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

    fn materialize(
        &self,
        envelope_index: usize,
        output: WasmEnvelope,
    ) -> Result<WasmMaterializedOutput, WasmOutputError> {
        let WasmEnvelope::Output {
            output_relay,
            columns,
            acks,
        } = output
        else {
            return Err(WasmOutputError::UnexpectedEnvelopeKind { envelope_index });
        };
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
            return Err(WasmOutputError::OutputColumnCountMismatch {
                output_relay,
                expected: destination_fields.len(),
                actual: columns.len(),
            });
        }
        let has_input_columns = columns.iter().any(WasmOutputColumn::is_input);
        self.validate_source_tokens(&output_relay, &acks.rows, has_input_columns)?;
        let arrays = columns
            .into_iter()
            .zip(destination_fields)
            .enumerate()
            .map(|(field_index, (column, destination_field))| match column {
                WasmOutputColumn::GuestArrow { ipc } => self.decode_guest_column(
                    &output_relay,
                    field_index,
                    destination_field,
                    acks.rows.len(),
                    &ipc,
                ),
                WasmOutputColumn::Input { column_index } => self.materialize_input_column(
                    &output_relay,
                    field_index,
                    destination_field,
                    column_index,
                    &acks.rows,
                ),
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
        })
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

    fn decode_guest_column(
        &self,
        output_relay: &str,
        field_index: usize,
        destination_field: &Arc<arrow_schema::Field>,
        row_count: usize,
        ipc: &[u8],
    ) -> Result<ArrayRef, WasmOutputError> {
        let invalid = |reason: String| WasmOutputError::InvalidGuestArrowIpc {
            output_relay: output_relay.to_string(),
            field_index,
            field_name: destination_field.name().to_string(),
            reason,
        };
        if ipc.is_empty() {
            return Err(invalid("IPC payload is empty".to_string()));
        }
        let mut cursor = std::io::Cursor::new(ipc);
        let (actual_schema, batches) = {
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
        if actual_schema.fields().len() != 1 {
            return Err(invalid(format!(
                "IPC schema has {} fields instead of exactly one",
                actual_schema.fields().len()
            )));
        }
        if batches.len() != 1 {
            return Err(invalid(format!(
                "IPC stream has {} record batches instead of exactly one",
                batches.len()
            )));
        }
        let actual_field = actual_schema.field(0);
        if actual_field != destination_field.as_ref() {
            return Err(WasmOutputError::GuestArrowFieldMismatch {
                output_relay: output_relay.to_string(),
                field_index,
                expected: format!("{destination_field:?}"),
                actual: format!("{actual_field:?}"),
            });
        }
        let batch = &batches[0];
        if batch.num_columns() != 1 {
            return Err(invalid(format!(
                "record batch has {} columns instead of exactly one",
                batch.num_columns()
            )));
        }
        if batch.num_rows() != row_count {
            return Err(WasmOutputError::GuestArrowRowCountMismatch {
                output_relay: output_relay.to_string(),
                field_index,
                expected: row_count,
                actual: batch.num_rows(),
            });
        }
        Ok(batch.column(0).clone())
    }

    fn materialize_input_column(
        &self,
        output_relay: &str,
        field_index: usize,
        destination_field: &Arc<arrow_schema::Field>,
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
        apply_wasm_sidecar_terminal_decisions(
            branch,
            node_kind,
            processor,
            error_policies,
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
                input_schema,
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
    input_schema: &'a Arc<CompiledSchema>,
    dispatch_error: &'static str,
}

async fn dispatch_wasm_output_route(
    context: WasmRouteDispatchContext<'_>,
    decoded: WasmDecodedOutputBatch,
    output: &mut RelayProcessorOutputNode,
) -> Option<Vec<AckSet>> {
    if output.compiled_program.is_none() && output.filter_map.is_some() {
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
            context.input_relays,
            &output.relay,
            output.filter_map.as_deref(),
            context.input_schema.arrow_schema(),
            context.input_schema.vm_sensitivity(),
            output_schema.arrow_schema(),
            output_schema.vm_sensitivity(),
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized_stream_specs,
                available_lookups: &available_lookups,
                current_branching: &current_branching,
                current_branch_schema: current_branch_schema.as_ref(),
                current_branch_sensitivity: None,
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
        let dispatched_acks = decoded.batch.acks.iter().cloned().collect::<Vec<_>>();
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
    let input_records = match wasm_filter_map_records(
        &output.relay,
        context.input_relays,
        &decoded.batch.records,
        &decoded.input_records,
    ) {
        Ok(records) => records,
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
    let input_records = match prepare_filter_map_input_records(
        context.node_kind,
        context.processor,
        program,
        input_records,
        execution_now,
        &side_inputs,
        &decoded.batch.keys,
        &decoded.batch.acks,
    )
    .await
    {
        Ok(records) => records,
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
    let executed = match execute_filter_map_program(
        context.node_kind,
        context.processor,
        program,
        &input_records,
        execution_now,
        decoded.batch.acks.clone(),
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
                    error.acks.iter(),
                    error.reason,
                );
            return None;
        }
    };
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
    let output_batch = match vm_typed_batch_to_runtime_batch(&executed.batch).map_err(|error| {
        format!(
            "{} '{}' failed to materialize FILTER-MAP output batch: {}",
            context.node_kind,
            context.processor.as_str(),
            error
        )
    }) {
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
                    decoded.batch.acks.iter(),
                    error,
                );
            return None;
        }
    };
    let mut success_output_rows = Vec::new();
    let mut success_input_rows = Vec::new();
    let mut message_errors = Vec::new();
    for (output_row, &input_row) in executed.selected_rows.iter().enumerate() {
        if let Some(side_error) = executed.batch.errors()[output_row].first() {
            message_errors.push(PendingProcessorOutputMessageError {
                row: input_row,
                key: decoded.batch.keys[input_row].clone(),
                record: decoded.batch.records[input_row].clone(),
                reason: format!(
                    "{} '{}' FILTER-MAP side error {}: {} at {}",
                    context.node_kind,
                    context.processor.as_str(),
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ),
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
            reason: error.reason,
        });
    }
    context
        .branch
        .runtime
        .handle_planned_message_errors(
            &context.branch.domain,
            context.node_kind,
            context.processor,
            context.error_policies,
            planned_errors,
        )
        .await;
    if success_output_rows.is_empty() {
        return Some(Vec::new());
    }
    let output_batch = if success_output_rows.len() == executed.batch.row_count() {
        output_batch
    } else {
        let success_output_rows = success_output_rows.iter().copied().collect::<HashSet<_>>();
        let keep = BooleanArray::from_iter(
            (0..executed.batch.row_count()).map(|row| Some(success_output_rows.contains(&row))),
        );
        match output_batch.filter(&keep) {
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
                            "{} '{}' failed to filter FILTER-MAP output batch: {}",
                            context.node_kind,
                            context.processor.as_str(),
                            error
                        ),
                    );
                return None;
            }
        }
    };
    let records = match output_schema.decoded_records_from_arrow_batch(&output_batch) {
        Ok(records) => records
            .into_iter()
            .zip(success_input_rows.iter())
            .map(|(record, input_row)| {
                record.into_runtime_record(decoded.batch.metadata[*input_row].clone())
            })
            .collect::<Vec<_>>(),
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
                        "{} '{}' failed to decode FILTER-MAP output sidecar records: {}",
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
        records,
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
    let dispatched_acks = forwarded.acks.iter().cloned().collect::<Vec<_>>();
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

fn wasm_filter_map_records(
    output_relay: &Identifier,
    input_relays: &[Identifier],
    output_records: &[RuntimeRecord],
    input_records: &[Option<RuntimeRecord>],
) -> Result<Vec<RuntimeRecord>, String> {
    if output_records.len() != input_records.len() {
        return Err(format!(
            "WASM output record count {} does not match source input record count {}",
            output_records.len(),
            input_records.len()
        ));
    }
    Ok(output_records
        .iter()
        .zip(input_records)
        .map(|(output_record, input_record)| {
            let metadata = output_record.metadata().clone();
            let mut fields = HashMap::new();
            insert_wasm_filter_map_fields(&mut fields, output_relay.as_str(), output_record, true);
            if let Some(input_record) = input_record {
                for input_relay in input_relays {
                    insert_wasm_filter_map_fields(
                        &mut fields,
                        input_relay.as_str(),
                        input_record,
                        false,
                    );
                }
                if input_relays
                    .iter()
                    .all(|relay| relay.as_str() != WASM_INPUT_NAMESPACE)
                    && output_relay.as_str() != WASM_INPUT_NAMESPACE
                {
                    insert_wasm_filter_map_fields(
                        &mut fields,
                        WASM_INPUT_NAMESPACE,
                        input_record,
                        false,
                    );
                }
            }
            RuntimeRecord::from_fields_with_metadata(fields, metadata)
        })
        .collect())
}

fn insert_wasm_filter_map_fields(
    fields: &mut HashMap<String, RuntimeValue>,
    namespace: &str,
    record: &RuntimeRecord,
    include_unqualified: bool,
) {
    for (name, value) in record.fields() {
        if include_unqualified {
            fields.insert(name.to_string(), value.clone());
        }
        fields.insert(format!("{namespace}.{name}"), value.clone());
    }
}

async fn apply_wasm_sidecar_terminal_decisions(
    branch: &BranchRuntime,
    node_kind: &str,
    processor: &Identifier,
    error_policies: &ErrorPolicies,
    ack_map: &mut WasmAckMap,
    sidecar: &WasmAckSidecar,
) {
    for message_error in &sidecar.message_errors {
        for token in &message_error.tokens {
            let context = ack_map
                .remove(&token.0)
                .expect("message error token should have been validated");
            branch
                .runtime
                .handle_message_error(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    RelayMessage {
                        key: branch.key.clone(),
                        record: context.record,
                        acks: context.acks,
                    },
                    message_error.reason.clone(),
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
    instance: &mut nervix_wasm::WasmBranchInstance,
) -> Result<(), String> {
    let guest_state = instance.save_state().await.map_err(|error| {
        format!(
            "wasm processor '{}' failed to save guest state: {}",
            processor.as_str(),
            error
        )
    })?;
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
    ack_map: &mut WasmAckMap,
    token_use_counts: &mut HashMap<u64, usize>,
) -> Result<WasmDecodedOutputBatch, String> {
    let mut metadata = Vec::with_capacity(rows.len());
    let mut acks = Vec::with_capacity(rows.len());
    let mut input_records = Vec::with_capacity(rows.len());
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
        input_records.push(source_context.map(|context| context.record.clone()));

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
    RelayRecordBatch::from_runtime_batch(schema, key.clone(), batch, metadata, acks).map(|batch| {
        WasmDecodedOutputBatch {
            batch,
            input_records,
        }
    })
}

fn generator_context_batch(
    schema: &Arc<arrow_schema::Schema>,
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
    program: &CompiledFilterMapProgram,
    input: &VmTypedBatch,
    execution_now: Timestamp,
) -> Result<Option<RuntimeRecord>, String> {
    let result = execute_program_with_selection_in_context(
        program,
        input,
        &VmExecutionContext { now: execution_now },
    )
    .await
    .map_err(|error| format!("GENERATOR execution failed: {error}"))?;
    if result.batch.row_count() == 0 {
        return Ok(None);
    }
    if result.batch.row_count() != 1 {
        return Err(format!(
            "GENERATOR produced {} rows for a single input key",
            result.batch.row_count()
        ));
    }
    if let Some(side_error) = result.batch.errors().iter().flatten().next() {
        return Err(format!(
            "GENERATOR side error {}: {} at {}",
            side_error.code.as_str(),
            side_error.message,
            side_error.span
        ));
    }
    vm_output_row_to_decoded_record(&result.batch, 0).map(|record| {
        Some(
            record.into_runtime_record(RuntimeRecordMetadata::from_ingested_at_watermarks(
                execution_now,
                execution_now,
            )),
        )
    })
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
) -> Result<DecodedRecord, CodecError> {
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
) -> Result<DecodedRecord, CodecError> {
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

async fn encode_emitted_payload(
    codec: Arc<CompiledCodec>,
    record: RuntimeRecord,
) -> Result<Vec<u8>, CodecError> {
    if !codec.requires_blocking_encode() {
        return encode_with_codec(&codec, &record);
    }

    let codec_name = codec.name.as_str().to_string();
    tokio::task::spawn_blocking(move || encode_with_codec(&codec, &record))
        .await
        .map_err(|error| CodecError::InvalidCodec {
            codec: codec_name,
            reason: format!("blocking encode task failed: {error}"),
        })?
}

pub(crate) fn scheduled_branched_stream_owner_nodes(
    schedule: &DomainSchedule,
    relay: &Identifier,
) -> Vec<String> {
    let specs = branched_ingestor_specs_from_models(schedule.nodes.iter().map(|node| {
        (
            node.kind,
            node.identifier.clone(),
            (*node.config).clone(),
            node.effective_branching.clone(),
        )
    }));
    let mut owners = BTreeSet::new();
    for spec in specs {
        if !spec.contains_stream(relay) {
            continue;
        }
        let Some(ingestor_node) = schedule
            .nodes
            .iter()
            .find(|node| node.kind == spec.kind && node.identifier == spec.identifier)
        else {
            continue;
        };
        match ingestor_node.config.as_ref() {
            Model::Ingestor(CreateIngestor {
                source: IngestSource::Endpoint { .. },
                ..
            }) => {
                for owner in &ingestor_node.assigned_nodes {
                    owners.insert(owner.clone());
                }
            }
            _ => {
                if let Some(owner) = ingestor_node.execution_node() {
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
