use error_stack::Report;
use nervix_models::{
    AckMode, AvroType, BranchParameterization, ClickHouseValueMapping, CodecEncoding,
    CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig,
    CodecWireFormat, CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
    CreateClientAzureBlob, CreateClientClickHouse, CreateClientGcs, CreateClientHttp,
    CreateClientIcebergRest, CreateClientKafka, CreateClientKinesis, CreateClientMongoDb,
    CreateClientMqtt, CreateClientMySql, CreateClientNats, CreateClientPostgres,
    CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
    CreateClientS3, CreateClientSqs, CreateClientWebsockets, CreateClientZeroMq, CreateCodec,
    CreateCorrelator, CreateDeduplicator, CreateEmitter, CreateEndpoint, CreateGenerator,
    CreateInferencer, CreateIngestor, CreateLookup, CreateReingestor, CreateRelay, CreateReorderer,
    CreateSchema, CreateSignalingProtocol, CreateUnifier, CreateVhost, CreateWasmProcessor,
    CreateWindowProcessor, CreateWireSchema, CreateWireSchemaStmt, EmitSink, EndpointIngestMode,
    EndpointType, ErrorFieldMapping, ErrorPolicies, GeneralErrorPolicy, IcebergCatalog,
    IcebergStorageBackend, Identifier, InferencerTensorMapping, IngestSource,
    IngestTimestampSource, JsonType, KafkaConfigEntry, KafkaIngestMode, KafkaOffsetMode,
    KinesisIngestMode, MaterializedRelayState, MessageErrorPolicy, Model, MongoDbConflictAction,
    MqttIngestMode, MqttQos, MqttSession, MySqlConflictAction, NameError, NatsIngestMode,
    ParameterValueMapping, ParseAsType, PostgresConflictAction, ProcessorOutput, ProcessorOutputs,
    PulsarIngestMode, RabbitMqIngestMode, RedisPubSubIngestMode, RelayParameterization,
    RelayParameters, SchemaField, SignalingProtocolOnConnect, SqsIngestMode, VhostTlsResource,
    WebsocketsIngestMode, WindowBound, WireSchemaField, ZeroMqIngestMode,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredModelVersioned {
    Schema(StoredCreateSchema),
    WireSchema(StoredCreateWireSchemaStmt),
    Codec(StoredCreateCodec),
    TransportKafka(StoredCreateClientKafka),
    TransportPulsar(StoredCreateClientPulsar),
    TransportKinesis(StoredCreateClientKinesis),
    TransportHttp(StoredCreateClientHttp),
    TransportPrometheus(StoredCreateClientPrometheus),
    TransportRabbitMq(StoredCreateClientRabbitMq),
    TransportRedis(StoredCreateClientRedis),
    TransportMqtt(StoredCreateClientMqtt),
    TransportNats(StoredCreateClientNats),
    TransportZeroMq(StoredCreateClientZeroMq),
    TransportSqs(StoredCreateClientSqs),
    TransportWebsockets(StoredCreateClientWebsockets),
    TransportClickHouse(StoredCreateClientClickHouse),
    TransportPostgres(StoredCreateClientPostgres),
    TransportMySql(StoredCreateClientMySql),
    TransportMongoDb(StoredCreateClientMongoDb),
    TransportS3(StoredCreateClientS3),
    TransportGcs(StoredCreateClientGcs),
    TransportAzureBlob(StoredCreateClientAzureBlob),
    TransportIcebergRest(StoredCreateClientIcebergRest),
    Vhost(StoredCreateVhost),
    Endpoint(StoredCreateEndpoint),
    SignalingProtocol(StoredCreateSignalingProtocol),
    Generator(StoredCreateGenerator),
    Inferencer(StoredCreateInferencer),
    WasmProcessor(StoredCreateWasmProcessor),
    Ingestor(StoredCreateIngestor),
    Reingestor(StoredCreateReingestor),
    Relay(StoredCreateRelay),
    Lookup(StoredCreateLookup),
    Deduplicator(StoredCreateDeduplicator),
    Correlator(StoredCreateCorrelator),
    Reorderer(StoredCreateReorderer),
    Unifier(StoredCreateUnifier),
    WindowProcessor(StoredCreateWindowProcessor),
    Emitter(StoredCreateEmitter),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateSchema {
    pub name: String,
    pub fields: Vec<StoredSchemaField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredSchemaField {
    pub name: String,
    pub ty: StoredParseAsType,
    pub optional: bool,
    pub sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredCreateWireSchemaStmt {
    Json(StoredCreateWireSchema<StoredJsonType>),
    Avro(StoredCreateWireSchema<StoredAvroType>),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateWireSchema<T> {
    pub name: String,
    pub fields: Vec<StoredWireSchemaField<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredWireSchemaField<T> {
    pub name: String,
    pub ty: T,
    pub optional: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredJsonType {
    String,
    Number,
    Integer,
    Object,
    Array,
    Boolean,
    Null,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Datetime,
    F32,
    F64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredAvroType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record,
    Enum,
    Array,
    Map,
    Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredParseAsType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Bool,
    String,
    Datetime,
    F32,
    F64,
    Array {
        element: StoredScalarParseAsType,
        len: u32,
    },
    Vec {
        element: StoredScalarParseAsType,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredScalarParseAsType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Bool,
    String,
    Datetime,
    F32,
    F64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientKafka {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientPulsar {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientKinesis {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientHttp {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientPrometheus {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientRabbitMq {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientRedis {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientMqtt {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientNats {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientZeroMq {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientSqs {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientWebsockets {
    pub name: String,
    pub mount: Option<String>,
    pub signaling_protocol: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientClickHouse {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientPostgres {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientMySql {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientMongoDb {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientS3 {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientGcs {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientAzureBlob {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateClientIcebergRest {
    pub name: String,
    pub mount: Option<String>,
    pub config: Vec<StoredClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateCodec {
    pub name: String,
    pub wire_format: StoredCodecWireFormat,
    pub wire_schema: Option<String>,
    pub schema: String,
    pub encoding_rules: Vec<StoredCodecEncodingRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredCodecWireFormat {
    Json,
    Avro,
    JaqNative {
        format: StoredCodecJaqFormat,
        transformations: StoredCodecJaqTransformations,
    },
    Protobuf(StoredCodecProtobufConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCodecProtobufConfig {
    pub resource: String,
    pub resource_version: Option<u64>,
    pub config: Vec<StoredClientConfigEntry>,
    pub message: String,
    pub transformations: StoredCodecJaqTransformations,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredCodecJaqFormat {
    Json,
    Yaml,
    Toml,
    Xml,
    Cbor,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCodecJaqTransformations {
    pub on_ingestion: Option<String>,
    pub on_emitting: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCodecEncodingRule {
    pub field: String,
    pub encoding: StoredCodecEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredCodecEncoding {
    Rfc3339,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateVhost {
    pub name: String,
    pub hostnames: Vec<String>,
    pub tls: Option<StoredVhostTlsResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredVhostTlsResource {
    pub resource: String,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateEndpoint {
    pub name: String,
    pub on_vhost: String,
    pub path: String,
    pub endpoint_type: StoredEndpointType,
    pub signaling_protocol: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredEndpointType {
    Websockets,
    Http,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateSignalingProtocol {
    pub name: String,
    pub on_connect: StoredSignalingProtocolOnConnect,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredSignalingProtocolOnConnect {
    pub send_bodies: Vec<String>,
    pub wait_bodies: Vec<String>,
    pub timeout: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredClientConfigEntry {
    pub key: String,
    pub value: String,
}

pub type StoredKafkaConfigEntry = StoredClientConfigEntry;

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateIngestor {
    pub name: String,
    pub output_routes: StoredProcessorOutputs,
    pub decode_using_codec: String,
    pub parameterized_by: StoredBranchParameterization,
    pub flush_each: String,
    pub max_batch_size: Option<String>,
    pub timestamp_source: Option<StoredIngestTimestampSource>,
    pub source: StoredIngestSource,
    pub error_policies: StoredErrorPolicies,
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateGenerator {
    pub name: String,
    pub into_relay: String,
    pub parameterized_by: StoredBranchParameterization,
    pub each: String,
    pub flush_each: String,
    pub max_batch_size: Option<String>,
    pub set: String,
    pub message_error_policy: StoredMessageErrorPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredErrorPolicies {
    pub message: StoredMessageErrorPolicy,
    pub general: StoredGeneralErrorPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredMessageErrorPolicy {
    Ignore,
    Log,
    Dlq {
        relay: String,
        mappings: Vec<StoredErrorFieldMapping>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredGeneralErrorPolicy {
    Ignore,
    Log,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredErrorFieldMapping {
    pub field: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredIngestTimestampSource {
    Now,
    At(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateReingestor {
    pub name: String,
    pub from_relay: String,
    pub output_routes: StoredProcessorOutputs,
    pub parameterized_by: StoredBranchParameterization,
    pub flush_each: String,
    pub max_batch_size: Option<String>,
    pub mode: AckMode,
    pub message_error_policy: StoredMessageErrorPolicy,
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateInferencer {
    pub name: String,
    pub from_relay: String,
    pub output_routes: StoredProcessorOutputs,
    pub parameterized_by: StoredBranchParameterization,
    pub resource: String,
    pub resource_version: Option<u64>,
    pub file: String,
    pub inputs: Vec<StoredInferencerTensorMapping>,
    pub outputs: Vec<StoredInferencerTensorMapping>,
    pub flush_each: String,
    pub max_batch_size: Option<String>,
    pub mode: AckMode,
    pub message_error_policy: StoredMessageErrorPolicy,
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateWasmProcessor {
    pub name: String,
    pub from_relay: String,
    pub output_routes: StoredProcessorOutputs,
    pub parameterized_by: StoredBranchParameterization,
    pub resource: String,
    pub resource_version: Option<u64>,
    pub file: String,
    pub mode: AckMode,
    pub message_error_policy: StoredMessageErrorPolicy,
    pub global_error_policy: StoredGeneralErrorPolicy,
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredInferencerTensorMapping {
    pub tensor: String,
    pub relay: String,
    pub field: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredIngestSource {
    Http {
        client: String,
        every: String,
    },
    Kinesis {
        client: String,
        relay: String,
        instances: u64,
        mode: StoredKinesisIngestMode,
    },
    Kafka {
        client: String,
        topic: String,
        offset_mode: StoredKafkaOffsetMode,
        instances: u64,
        mode: StoredKafkaIngestMode,
    },
    Pulsar {
        client: String,
        topic: String,
        subscription: String,
        instances: u64,
        mode: StoredKafkaIngestMode,
    },
    RabbitMq {
        client: String,
        queue: String,
        instances: u64,
        mode: StoredRabbitMqIngestMode,
    },
    RedisPubSub {
        client: String,
        channel: String,
        mode: StoredRedisPubSubIngestMode,
    },
    Mqtt {
        client: String,
        topic: String,
        instances: u64,
        mode: StoredMqttIngestMode,
    },
    Nats {
        client: String,
        subject: String,
        queue_group: String,
        instances: u64,
        mode: StoredNatsIngestMode,
    },
    Prometheus {
        client: String,
        query: String,
        every: String,
    },
    ZeroMq {
        client: String,
        mode: StoredZeroMqIngestMode,
    },
    Sqs {
        client: String,
        queue: String,
        instances: u64,
        mode: StoredSqsIngestMode,
    },
    Endpoint {
        endpoint: String,
        mode: StoredEndpointIngestMode,
    },
    Websockets {
        client: String,
        mode: StoredWebsocketsIngestMode,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredKafkaOffsetMode {
    ConsumerGroup(String),
    Domain,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredKafkaIngestMode {
    AckParallel {
        max: u64,
        batch_timeout: String,
        timeout: String,
        retry_backoff: String,
        retry_max_backoff: String,
    },
    AckSequential {
        timeout: String,
        retry_backoff: String,
        retry_max_backoff: String,
    },
    NoAckParallel {
        max: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredKinesisIngestMode {
    AckSequential {
        timeout: String,
        retry_backoff: String,
        retry_max_backoff: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredRabbitMqIngestMode {
    AckSequential {
        timeout: String,
        retry_backoff: String,
        retry_max_backoff: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredRedisPubSubIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredMqttIngestMode {
    NoAckSequential {
        session: StoredMqttSession,
        qos: StoredMqttQos,
    },
    NoAckParallel {
        max: u64,
        session: StoredMqttSession,
        qos: StoredMqttQos,
    },
    AckSequential {
        timeout: String,
        retry_backoff: String,
        retry_max_backoff: String,
    },
    AckParallel {
        max: u64,
        batch_timeout: String,
        timeout: String,
        retry_backoff: String,
        retry_max_backoff: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredMqttSession {
    Clean,
    Persistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredMqttQos {
    AtMostOnce,
    AtLeastOnce,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredNatsIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredZeroMqIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredSqsIngestMode {
    AckSequential {
        timeout: String,
        retry_backoff: String,
        retry_max_backoff: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredEndpointIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredWebsocketsIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateRelay {
    pub name: String,
    pub schema: String,
    pub buffer: usize,
    pub parameterization: StoredRelayParameterization,
    pub materialized_state: Option<StoredMaterializedRelayState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredRelayParameterization {
    Parameterized { parameters: StoredRelayParameters },
    Unparameterized,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredRelayParameters {
    Inferred,
    Declared(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredParameterValueMapping {
    pub field: String,
    pub relay: String,
    pub relay_field: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredBranchParameterization {
    Parameterized {
        schema: String,
        values: Vec<StoredParameterValueMapping>,
        ttl: Option<String>,
    },
    Unparameterized,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredProcessorOutput {
    pub relay: String,
    pub filter_map: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredProcessorOutputs {
    pub routes: Vec<StoredProcessorOutput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredMaterializedRelayState {
    LastByTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateLookup {
    pub name: String,
    pub key_field: String,
    pub resource: String,
    pub path: String,
    pub decode_using_codec: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateUnifier {
    pub name: String,
    pub from_relays: Vec<String>,
    pub output_routes: StoredProcessorOutputs,
    pub parameterized_by: StoredBranchParameterization,
    pub flush_each: String,
    pub max_batch_size: Option<String>,
    pub mode: AckMode,
    pub message_error_policy: StoredMessageErrorPolicy,
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateDeduplicator {
    pub name: String,
    pub from_relay: String,
    pub output_routes: StoredProcessorOutputs,
    pub parameterized_by: StoredBranchParameterization,
    pub deduplicate_on: String,
    pub max_time: String,
    pub flush_each: String,
    pub max_batch_size: Option<String>,
    pub mode: AckMode,
    pub message_error_policy: StoredMessageErrorPolicy,
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateCorrelator {
    pub name: String,
    pub left_relay: String,
    pub right_relay: String,
    pub output_routes: StoredProcessorOutputs,
    pub parameterized_by: StoredBranchParameterization,
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
    pub match_policy: StoredCorrelatorMatchPolicy,
    pub output: String,
    pub max_time: String,
    pub flush_each: String,
    pub max_batch_size: Option<String>,
    pub timeout_policy: StoredCorrelationTimeoutPolicy,
    pub mode: AckMode,
    pub message_error_policy: StoredMessageErrorPolicy,
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredCorrelatorMatchPolicy {
    Earliest,
    Latest,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCorrelationTimeoutPolicy {
    pub left: StoredCorrelationTimeoutAction,
    pub right: StoredCorrelationTimeoutAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredCorrelationTimeoutAction {
    Drop,
    SendTo { relay: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateReorderer {
    pub name: String,
    pub from_relay: String,
    pub output_routes: StoredProcessorOutputs,
    pub parameterized_by: StoredBranchParameterization,
    pub order_by: String,
    pub max_time: String,
    pub flush_each: String,
    pub max_batch_size: Option<String>,
    pub mode: AckMode,
    pub message_error_policy: StoredMessageErrorPolicy,
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateWindowProcessor {
    pub name: String,
    pub from_relay: String,
    pub output_routes: StoredProcessorOutputs,
    pub parameterized_by: StoredBranchParameterization,
    pub width: StoredWindowBound,
    pub step: StoredWindowBound,
    pub aggregate: String,
    pub mode: AckMode,
    pub message_error_policy: StoredMessageErrorPolicy,
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredWindowBound {
    pub messages: Option<u64>,
    pub duration: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateEmitter {
    pub name: String,
    pub from_relay: String,
    pub encode_using_codec: Option<String>,
    pub sink: StoredEmitSink,
    pub flush_each: String,
    pub max_batch_size: Option<String>,
    pub mode: AckMode,
    pub error_policies: StoredErrorPolicies,
    pub filter_map: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredEmitSink {
    Kafka {
        client: String,
        topic: String,
    },
    Pulsar {
        client: String,
        topic: String,
    },
    Kinesis {
        client: String,
        relay: String,
    },
    RabbitMq {
        client: String,
        queue: String,
    },
    Redis {
        client: String,
        channel: String,
    },
    Mqtt {
        client: String,
        topic: String,
    },
    Nats {
        client: String,
        subject: String,
    },
    ZeroMq {
        client: String,
    },
    Sqs {
        client: String,
        queue: String,
    },
    ClickHouse {
        client: String,
        table: String,
        values: Vec<StoredClickHouseValueMapping>,
        flush_each: String,
    },
    Postgres {
        client: String,
        table: String,
        values: Vec<StoredPostgresValueMapping>,
        conflict_action: StoredPostgresConflictAction,
        max_batch: u64,
        flush_each: String,
    },
    MySql {
        client: String,
        table: String,
        values: Vec<StoredMySqlValueMapping>,
        conflict_action: StoredMySqlConflictAction,
        max_batch: u64,
        flush_each: String,
    },
    MongoDb {
        client: String,
        collection: String,
        values: Vec<StoredMongoDbValueMapping>,
        conflict_action: StoredMongoDbConflictAction,
        max_batch: u64,
        flush_each: String,
    },
    Iceberg {
        backend: StoredIcebergStorageBackend,
        client: String,
        table: String,
        values: Vec<StoredClickHouseValueMapping>,
        location: String,
        catalog: StoredIcebergCatalog,
        flush_each: String,
        max_batch_size: Option<String>,
        commit_each: String,
        max_commit_size: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredIcebergCatalog {
    Rest { client: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredIcebergStorageBackend {
    S3,
    Gcs,
    AzureBlob,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredClickHouseValueMapping {
    pub column: String,
    pub expression: String,
}

pub type StoredPostgresValueMapping = StoredClickHouseValueMapping;

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredPostgresConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

pub type StoredMySqlValueMapping = StoredClickHouseValueMapping;

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredMySqlConflictAction {
    None,
    DoNothing,
    DoUpdate,
}

pub type StoredMongoDbValueMapping = StoredClickHouseValueMapping;

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredMongoDbConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

impl From<Model> for StoredModelVersioned {
    fn from(value: Model) -> Self {
        match value {
            Model::Schema(v) => Self::Schema(v.into()),
            Model::WireSchema(v) => Self::WireSchema(v.into()),
            Model::Codec(v) => Self::Codec(v.into()),
            Model::ClientKafka(v) => Self::TransportKafka(v.into()),
            Model::ClientPulsar(v) => Self::TransportPulsar(v.into()),
            Model::ClientKinesis(v) => Self::TransportKinesis(v.into()),
            Model::ClientHttp(v) => Self::TransportHttp(v.into()),
            Model::ClientPrometheus(v) => Self::TransportPrometheus(v.into()),
            Model::ClientRabbitMq(v) => Self::TransportRabbitMq(v.into()),
            Model::ClientRedis(v) => Self::TransportRedis(v.into()),
            Model::ClientMqtt(v) => Self::TransportMqtt(v.into()),
            Model::ClientNats(v) => Self::TransportNats(v.into()),
            Model::ClientZeroMq(v) => Self::TransportZeroMq(v.into()),
            Model::ClientSqs(v) => Self::TransportSqs(v.into()),
            Model::ClientWebsockets(v) => Self::TransportWebsockets(v.into()),
            Model::ClientClickHouse(v) => Self::TransportClickHouse(v.into()),
            Model::ClientPostgres(v) => Self::TransportPostgres(v.into()),
            Model::ClientMySql(v) => Self::TransportMySql(v.into()),
            Model::ClientMongoDb(v) => Self::TransportMongoDb(v.into()),
            Model::ClientS3(v) => Self::TransportS3(v.into()),
            Model::ClientGcs(v) => Self::TransportGcs(v.into()),
            Model::ClientAzureBlob(v) => Self::TransportAzureBlob(v.into()),
            Model::ClientIcebergRest(v) => Self::TransportIcebergRest(v.into()),
            Model::Vhost(v) => Self::Vhost(v.into()),
            Model::Endpoint(v) => Self::Endpoint(v.into()),
            Model::SignalingProtocol(v) => Self::SignalingProtocol(v.into()),
            Model::Generator(v) => Self::Generator(v.into()),
            Model::Inferencer(v) => Self::Inferencer(v.into()),
            Model::WasmProcessor(v) => Self::WasmProcessor(v.into()),
            Model::Ingestor(v) => Self::Ingestor(v.into()),
            Model::Reingestor(v) => Self::Reingestor(v.into()),
            Model::Relay(v) => Self::Relay(v.into()),
            Model::Lookup(v) => Self::Lookup(v.into()),
            Model::Deduplicator(v) => Self::Deduplicator(v.into()),
            Model::Correlator(v) => Self::Correlator(v.into()),
            Model::Reorderer(v) => Self::Reorderer(v.into()),
            Model::Unifier(v) => Self::Unifier(v.into()),
            Model::WindowProcessor(v) => Self::WindowProcessor(v.into()),
            Model::Emitter(v) => Self::Emitter(v.into()),
            Model::Materializer(_) => {
                unreachable!("synthetic materializers must not be stored")
            }
        }
    }
}

impl TryFrom<StoredModelVersioned> for Model {
    type Error = Report<NameError>;

    fn try_from(value: StoredModelVersioned) -> Result<Self, Self::Error> {
        match value {
            StoredModelVersioned::Schema(v) => Ok(Model::Schema(v.try_into()?)),
            StoredModelVersioned::WireSchema(v) => Ok(Model::WireSchema(v.try_into()?)),
            StoredModelVersioned::Codec(v) => Ok(Model::Codec(v.try_into()?)),
            StoredModelVersioned::TransportKafka(v) => Ok(Model::ClientKafka(v.try_into()?)),
            StoredModelVersioned::TransportPulsar(v) => Ok(Model::ClientPulsar(v.try_into()?)),
            StoredModelVersioned::TransportKinesis(v) => Ok(Model::ClientKinesis(v.try_into()?)),
            StoredModelVersioned::TransportHttp(v) => Ok(Model::ClientHttp(v.try_into()?)),
            StoredModelVersioned::TransportPrometheus(v) => {
                Ok(Model::ClientPrometheus(v.try_into()?))
            }
            StoredModelVersioned::TransportRabbitMq(v) => Ok(Model::ClientRabbitMq(v.try_into()?)),
            StoredModelVersioned::TransportRedis(v) => Ok(Model::ClientRedis(v.try_into()?)),
            StoredModelVersioned::TransportMqtt(v) => Ok(Model::ClientMqtt(v.try_into()?)),
            StoredModelVersioned::TransportNats(v) => Ok(Model::ClientNats(v.try_into()?)),
            StoredModelVersioned::TransportZeroMq(v) => Ok(Model::ClientZeroMq(v.try_into()?)),
            StoredModelVersioned::TransportSqs(v) => Ok(Model::ClientSqs(v.try_into()?)),
            StoredModelVersioned::TransportWebsockets(v) => {
                Ok(Model::ClientWebsockets(v.try_into()?))
            }
            StoredModelVersioned::TransportClickHouse(v) => {
                Ok(Model::ClientClickHouse(v.try_into()?))
            }
            StoredModelVersioned::TransportPostgres(v) => Ok(Model::ClientPostgres(v.try_into()?)),
            StoredModelVersioned::TransportMySql(v) => Ok(Model::ClientMySql(v.try_into()?)),
            StoredModelVersioned::TransportMongoDb(v) => Ok(Model::ClientMongoDb(v.try_into()?)),
            StoredModelVersioned::TransportS3(v) => Ok(Model::ClientS3(v.try_into()?)),
            StoredModelVersioned::TransportGcs(v) => Ok(Model::ClientGcs(v.try_into()?)),
            StoredModelVersioned::TransportAzureBlob(v) => {
                Ok(Model::ClientAzureBlob(v.try_into()?))
            }
            StoredModelVersioned::TransportIcebergRest(v) => {
                Ok(Model::ClientIcebergRest(v.try_into()?))
            }
            StoredModelVersioned::Vhost(v) => Ok(Model::Vhost(v.try_into()?)),
            StoredModelVersioned::Endpoint(v) => Ok(Model::Endpoint(v.try_into()?)),
            StoredModelVersioned::SignalingProtocol(v) => {
                Ok(Model::SignalingProtocol(v.try_into()?))
            }
            StoredModelVersioned::Generator(v) => Ok(Model::Generator(v.try_into()?)),
            StoredModelVersioned::Inferencer(v) => Ok(Model::Inferencer(v.try_into()?)),
            StoredModelVersioned::WasmProcessor(v) => Ok(Model::WasmProcessor(v.try_into()?)),
            StoredModelVersioned::Ingestor(v) => Ok(Model::Ingestor(v.try_into()?)),
            StoredModelVersioned::Reingestor(v) => Ok(Model::Reingestor(v.try_into()?)),
            StoredModelVersioned::Relay(v) => Ok(Model::Relay(v.try_into()?)),
            StoredModelVersioned::Lookup(v) => Ok(Model::Lookup(v.try_into()?)),
            StoredModelVersioned::Deduplicator(v) => Ok(Model::Deduplicator(v.try_into()?)),
            StoredModelVersioned::Correlator(v) => Ok(Model::Correlator(v.try_into()?)),
            StoredModelVersioned::Reorderer(v) => Ok(Model::Reorderer(v.try_into()?)),
            StoredModelVersioned::Unifier(v) => Ok(Model::Unifier(v.try_into()?)),
            StoredModelVersioned::WindowProcessor(v) => Ok(Model::WindowProcessor(v.try_into()?)),
            StoredModelVersioned::Emitter(v) => Ok(Model::Emitter(v.try_into()?)),
        }
    }
}

impl From<CreateSchema> for StoredCreateSchema {
    fn from(value: CreateSchema) -> Self {
        Self {
            name: value.name.to_string(),
            fields: value.fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateSchema> for CreateSchema {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateSchema) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            fields: value
                .fields
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<SchemaField> for StoredSchemaField {
    fn from(value: SchemaField) -> Self {
        Self {
            name: value.name.to_string(),
            ty: value.ty.into(),
            optional: value.optional,
            sensitive: value.sensitive,
        }
    }
}

impl TryFrom<StoredSchemaField> for SchemaField {
    type Error = Report<NameError>;

    fn try_from(value: StoredSchemaField) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            ty: value.ty.into(),
            optional: value.optional,
            sensitive: value.sensitive,
        })
    }
}

impl From<CreateWireSchemaStmt> for StoredCreateWireSchemaStmt {
    fn from(value: CreateWireSchemaStmt) -> Self {
        match value {
            CreateWireSchemaStmt::Json(v) => Self::Json(v.into()),
            CreateWireSchemaStmt::Avro(v) => Self::Avro(v.into()),
        }
    }
}

impl TryFrom<StoredCreateWireSchemaStmt> for CreateWireSchemaStmt {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateWireSchemaStmt) -> Result<Self, Self::Error> {
        match value {
            StoredCreateWireSchemaStmt::Json(v) => Ok(Self::Json(v.try_into()?)),
            StoredCreateWireSchemaStmt::Avro(v) => Ok(Self::Avro(v.try_into()?)),
        }
    }
}

impl From<CreateWireSchema<JsonType>> for StoredCreateWireSchema<StoredJsonType> {
    fn from(value: CreateWireSchema<JsonType>) -> Self {
        Self {
            name: value.name.to_string(),
            fields: value.fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<CreateWireSchema<AvroType>> for StoredCreateWireSchema<StoredAvroType> {
    fn from(value: CreateWireSchema<AvroType>) -> Self {
        Self {
            name: value.name.to_string(),
            fields: value.fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateWireSchema<StoredJsonType>> for CreateWireSchema<JsonType> {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateWireSchema<StoredJsonType>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            fields: value
                .fields
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<StoredCreateWireSchema<StoredAvroType>> for CreateWireSchema<AvroType> {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateWireSchema<StoredAvroType>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            fields: value
                .fields
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<WireSchemaField<JsonType>> for StoredWireSchemaField<StoredJsonType> {
    fn from(value: WireSchemaField<JsonType>) -> Self {
        Self {
            name: value.name.to_string(),
            ty: value.ty.into(),
            optional: value.optional,
        }
    }
}

impl From<WireSchemaField<AvroType>> for StoredWireSchemaField<StoredAvroType> {
    fn from(value: WireSchemaField<AvroType>) -> Self {
        Self {
            name: value.name.to_string(),
            ty: value.ty.into(),
            optional: value.optional,
        }
    }
}

impl TryFrom<StoredWireSchemaField<StoredJsonType>> for WireSchemaField<JsonType> {
    type Error = Report<NameError>;

    fn try_from(value: StoredWireSchemaField<StoredJsonType>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            ty: value.ty.into(),
            optional: value.optional,
        })
    }
}

impl TryFrom<StoredWireSchemaField<StoredAvroType>> for WireSchemaField<AvroType> {
    type Error = Report<NameError>;

    fn try_from(value: StoredWireSchemaField<StoredAvroType>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            ty: value.ty.into(),
            optional: value.optional,
        })
    }
}

impl From<JsonType> for StoredJsonType {
    fn from(value: JsonType) -> Self {
        match value {
            JsonType::String => Self::String,
            JsonType::Number => Self::Number,
            JsonType::Integer => Self::Integer,
            JsonType::Object => Self::Object,
            JsonType::Array => Self::Array,
            JsonType::Boolean => Self::Boolean,
            JsonType::Null => Self::Null,
            JsonType::U8 => Self::U8,
            JsonType::I8 => Self::I8,
            JsonType::U16 => Self::U16,
            JsonType::I16 => Self::I16,
            JsonType::U32 => Self::U32,
            JsonType::I32 => Self::I32,
            JsonType::U64 => Self::U64,
            JsonType::I64 => Self::I64,
            JsonType::Datetime => Self::Datetime,
            JsonType::F32 => Self::F32,
            JsonType::F64 => Self::F64,
        }
    }
}

impl From<StoredJsonType> for JsonType {
    fn from(value: StoredJsonType) -> Self {
        match value {
            StoredJsonType::String => Self::String,
            StoredJsonType::Number => Self::Number,
            StoredJsonType::Integer => Self::Integer,
            StoredJsonType::Object => Self::Object,
            StoredJsonType::Array => Self::Array,
            StoredJsonType::Boolean => Self::Boolean,
            StoredJsonType::Null => Self::Null,
            StoredJsonType::U8 => Self::U8,
            StoredJsonType::I8 => Self::I8,
            StoredJsonType::U16 => Self::U16,
            StoredJsonType::I16 => Self::I16,
            StoredJsonType::U32 => Self::U32,
            StoredJsonType::I32 => Self::I32,
            StoredJsonType::U64 => Self::U64,
            StoredJsonType::I64 => Self::I64,
            StoredJsonType::Datetime => Self::Datetime,
            StoredJsonType::F32 => Self::F32,
            StoredJsonType::F64 => Self::F64,
        }
    }
}

impl From<AvroType> for StoredAvroType {
    fn from(value: AvroType) -> Self {
        match value {
            AvroType::Null => Self::Null,
            AvroType::Boolean => Self::Boolean,
            AvroType::Int => Self::Int,
            AvroType::Long => Self::Long,
            AvroType::Float => Self::Float,
            AvroType::Double => Self::Double,
            AvroType::Bytes => Self::Bytes,
            AvroType::String => Self::String,
            AvroType::Record => Self::Record,
            AvroType::Enum => Self::Enum,
            AvroType::Array => Self::Array,
            AvroType::Map => Self::Map,
            AvroType::Fixed => Self::Fixed,
        }
    }
}

impl From<StoredAvroType> for AvroType {
    fn from(value: StoredAvroType) -> Self {
        match value {
            StoredAvroType::Null => Self::Null,
            StoredAvroType::Boolean => Self::Boolean,
            StoredAvroType::Int => Self::Int,
            StoredAvroType::Long => Self::Long,
            StoredAvroType::Float => Self::Float,
            StoredAvroType::Double => Self::Double,
            StoredAvroType::Bytes => Self::Bytes,
            StoredAvroType::String => Self::String,
            StoredAvroType::Record => Self::Record,
            StoredAvroType::Enum => Self::Enum,
            StoredAvroType::Array => Self::Array,
            StoredAvroType::Map => Self::Map,
            StoredAvroType::Fixed => Self::Fixed,
        }
    }
}

impl From<ParseAsType> for StoredParseAsType {
    fn from(value: ParseAsType) -> Self {
        match value {
            ParseAsType::U8 => Self::U8,
            ParseAsType::I8 => Self::I8,
            ParseAsType::U16 => Self::U16,
            ParseAsType::I16 => Self::I16,
            ParseAsType::U32 => Self::U32,
            ParseAsType::I32 => Self::I32,
            ParseAsType::U64 => Self::U64,
            ParseAsType::I64 => Self::I64,
            ParseAsType::Bool => Self::Bool,
            ParseAsType::String => Self::String,
            ParseAsType::Datetime => Self::Datetime,
            ParseAsType::F32 => Self::F32,
            ParseAsType::F64 => Self::F64,
            ParseAsType::Array { element, len } => Self::Array {
                element: StoredScalarParseAsType::from_parse_as(*element),
                len,
            },
            ParseAsType::Vec { element } => Self::Vec {
                element: StoredScalarParseAsType::from_parse_as(*element),
            },
        }
    }
}

impl From<StoredParseAsType> for ParseAsType {
    fn from(value: StoredParseAsType) -> Self {
        match value {
            StoredParseAsType::U8 => Self::U8,
            StoredParseAsType::I8 => Self::I8,
            StoredParseAsType::U16 => Self::U16,
            StoredParseAsType::I16 => Self::I16,
            StoredParseAsType::U32 => Self::U32,
            StoredParseAsType::I32 => Self::I32,
            StoredParseAsType::U64 => Self::U64,
            StoredParseAsType::I64 => Self::I64,
            StoredParseAsType::Bool => Self::Bool,
            StoredParseAsType::String => Self::String,
            StoredParseAsType::Datetime => Self::Datetime,
            StoredParseAsType::F32 => Self::F32,
            StoredParseAsType::F64 => Self::F64,
            StoredParseAsType::Array { element, len } => Self::Array {
                element: Box::new(element.into()),
                len,
            },
            StoredParseAsType::Vec { element } => Self::Vec {
                element: Box::new(element.into()),
            },
        }
    }
}

impl StoredScalarParseAsType {
    fn from_parse_as(value: ParseAsType) -> Self {
        match value {
            ParseAsType::U8 => Self::U8,
            ParseAsType::I8 => Self::I8,
            ParseAsType::U16 => Self::U16,
            ParseAsType::I16 => Self::I16,
            ParseAsType::U32 => Self::U32,
            ParseAsType::I32 => Self::I32,
            ParseAsType::U64 => Self::U64,
            ParseAsType::I64 => Self::I64,
            ParseAsType::Bool => Self::Bool,
            ParseAsType::String => Self::String,
            ParseAsType::Datetime => Self::Datetime,
            ParseAsType::F32 => Self::F32,
            ParseAsType::F64 => Self::F64,
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => {
                panic!("nested array and vector schema types are not stored")
            }
        }
    }
}

impl From<StoredScalarParseAsType> for ParseAsType {
    fn from(value: StoredScalarParseAsType) -> Self {
        match value {
            StoredScalarParseAsType::U8 => Self::U8,
            StoredScalarParseAsType::I8 => Self::I8,
            StoredScalarParseAsType::U16 => Self::U16,
            StoredScalarParseAsType::I16 => Self::I16,
            StoredScalarParseAsType::U32 => Self::U32,
            StoredScalarParseAsType::I32 => Self::I32,
            StoredScalarParseAsType::U64 => Self::U64,
            StoredScalarParseAsType::I64 => Self::I64,
            StoredScalarParseAsType::Bool => Self::Bool,
            StoredScalarParseAsType::String => Self::String,
            StoredScalarParseAsType::Datetime => Self::Datetime,
            StoredScalarParseAsType::F32 => Self::F32,
            StoredScalarParseAsType::F64 => Self::F64,
        }
    }
}

impl From<CreateClientKafka> for StoredCreateClientKafka {
    fn from(value: CreateClientKafka) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientKafka> for CreateClientKafka {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientKafka) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientPulsar> for StoredCreateClientPulsar {
    fn from(value: CreateClientPulsar) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<CreateClientKinesis> for StoredCreateClientKinesis {
    fn from(value: CreateClientKinesis) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientPulsar> for CreateClientPulsar {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientPulsar) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl TryFrom<StoredCreateClientKinesis> for CreateClientKinesis {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientKinesis) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientHttp> for StoredCreateClientHttp {
    fn from(value: CreateClientHttp) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientHttp> for CreateClientHttp {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientHttp) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientPrometheus> for StoredCreateClientPrometheus {
    fn from(value: CreateClientPrometheus) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientPrometheus> for CreateClientPrometheus {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientPrometheus) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientRabbitMq> for StoredCreateClientRabbitMq {
    fn from(value: CreateClientRabbitMq) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientRabbitMq> for CreateClientRabbitMq {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientRabbitMq) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientRedis> for StoredCreateClientRedis {
    fn from(value: CreateClientRedis) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientRedis> for CreateClientRedis {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientRedis) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientMqtt> for StoredCreateClientMqtt {
    fn from(value: CreateClientMqtt) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientMqtt> for CreateClientMqtt {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientMqtt) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientNats> for StoredCreateClientNats {
    fn from(value: CreateClientNats) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientNats> for CreateClientNats {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientNats) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientZeroMq> for StoredCreateClientZeroMq {
    fn from(value: CreateClientZeroMq) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientZeroMq> for CreateClientZeroMq {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientZeroMq) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientSqs> for StoredCreateClientSqs {
    fn from(value: CreateClientSqs) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientSqs> for CreateClientSqs {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientSqs) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientWebsockets> for StoredCreateClientWebsockets {
    fn from(value: CreateClientWebsockets) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            signaling_protocol: value
                .signaling_protocol
                .map(|signaling_protocol| signaling_protocol.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientWebsockets> for CreateClientWebsockets {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientWebsockets) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            signaling_protocol: value
                .signaling_protocol
                .map(|signaling_protocol| Identifier::parse(&signaling_protocol))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientClickHouse> for StoredCreateClientClickHouse {
    fn from(value: CreateClientClickHouse) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientClickHouse> for CreateClientClickHouse {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientClickHouse) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientPostgres> for StoredCreateClientPostgres {
    fn from(value: CreateClientPostgres) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientPostgres> for CreateClientPostgres {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientPostgres) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientMySql> for StoredCreateClientMySql {
    fn from(value: CreateClientMySql) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientMySql> for CreateClientMySql {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientMySql) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientMongoDb> for StoredCreateClientMongoDb {
    fn from(value: CreateClientMongoDb) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientMongoDb> for CreateClientMongoDb {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientMongoDb) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientS3> for StoredCreateClientS3 {
    fn from(value: CreateClientS3) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientS3> for CreateClientS3 {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientS3) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientGcs> for StoredCreateClientGcs {
    fn from(value: CreateClientGcs) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientGcs> for CreateClientGcs {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientGcs) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientAzureBlob> for StoredCreateClientAzureBlob {
    fn from(value: CreateClientAzureBlob) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientAzureBlob> for CreateClientAzureBlob {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientAzureBlob) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateClientIcebergRest> for StoredCreateClientIcebergRest {
    fn from(value: CreateClientIcebergRest) -> Self {
        Self {
            name: value.name.to_string(),
            mount: value.mount.map(|mount| mount.to_string()),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateClientIcebergRest> for CreateClientIcebergRest {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateClientIcebergRest) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            mount: value
                .mount
                .map(|mount| Identifier::parse(&mount))
                .transpose()?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<CreateVhost> for StoredCreateVhost {
    fn from(value: CreateVhost) -> Self {
        Self {
            name: value.name.to_string(),
            hostnames: value.hostnames,
            tls: value.tls.map(Into::into),
        }
    }
}

impl TryFrom<StoredCreateVhost> for CreateVhost {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateVhost) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            hostnames: value.hostnames,
            tls: value.tls.map(TryInto::try_into).transpose()?,
        })
    }
}

impl From<VhostTlsResource> for StoredVhostTlsResource {
    fn from(value: VhostTlsResource) -> Self {
        Self {
            resource: value.resource.to_string(),
            version: value.version,
        }
    }
}

impl TryFrom<StoredVhostTlsResource> for VhostTlsResource {
    type Error = Report<NameError>;

    fn try_from(value: StoredVhostTlsResource) -> Result<Self, Self::Error> {
        Ok(Self {
            resource: Identifier::parse(&value.resource)?,
            version: value.version,
        })
    }
}

impl From<CreateEndpoint> for StoredCreateEndpoint {
    fn from(value: CreateEndpoint) -> Self {
        Self {
            name: value.name.to_string(),
            on_vhost: value.on_vhost.to_string(),
            path: value.path,
            endpoint_type: value.endpoint_type.into(),
            signaling_protocol: value
                .signaling_protocol
                .map(|signaling_protocol| signaling_protocol.to_string()),
        }
    }
}

impl TryFrom<StoredCreateEndpoint> for CreateEndpoint {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateEndpoint) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            on_vhost: Identifier::parse(&value.on_vhost)?,
            path: value.path,
            endpoint_type: value.endpoint_type.into(),
            signaling_protocol: value
                .signaling_protocol
                .map(|signaling_protocol| Identifier::parse(&signaling_protocol))
                .transpose()?,
        })
    }
}

impl From<CreateSignalingProtocol> for StoredCreateSignalingProtocol {
    fn from(value: CreateSignalingProtocol) -> Self {
        Self {
            name: value.name.to_string(),
            on_connect: value.on_connect.into(),
        }
    }
}

impl TryFrom<StoredCreateSignalingProtocol> for CreateSignalingProtocol {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateSignalingProtocol) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            on_connect: value.on_connect.into(),
        })
    }
}

impl From<SignalingProtocolOnConnect> for StoredSignalingProtocolOnConnect {
    fn from(value: SignalingProtocolOnConnect) -> Self {
        Self {
            send_bodies: value.send_bodies,
            wait_bodies: value.wait_bodies,
            timeout: value.timeout,
        }
    }
}

impl From<StoredSignalingProtocolOnConnect> for SignalingProtocolOnConnect {
    fn from(value: StoredSignalingProtocolOnConnect) -> Self {
        Self {
            send_bodies: value.send_bodies,
            wait_bodies: value.wait_bodies,
            timeout: value.timeout,
        }
    }
}

impl From<CreateGenerator> for StoredCreateGenerator {
    fn from(value: CreateGenerator) -> Self {
        Self {
            name: value.name.to_string(),
            into_relay: value.into_relay.to_string(),
            parameterized_by: value.parameterized_by.into(),
            each: value.each,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            set: value.set,
            message_error_policy: value.message_error_policy.into(),
        }
    }
}

impl TryFrom<StoredCreateGenerator> for CreateGenerator {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateGenerator) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            into_relay: Identifier::parse(&value.into_relay)?,
            parameterized_by: value.parameterized_by.try_into()?,
            each: value.each,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            set: value.set,
            message_error_policy: value.message_error_policy.try_into()?,
        })
    }
}

impl From<ErrorPolicies> for StoredErrorPolicies {
    fn from(value: ErrorPolicies) -> Self {
        Self {
            message: value.message.into(),
            general: value.general.into(),
        }
    }
}

impl TryFrom<StoredErrorPolicies> for ErrorPolicies {
    type Error = Report<NameError>;

    fn try_from(value: StoredErrorPolicies) -> Result<Self, Self::Error> {
        Ok(Self {
            message: value.message.try_into()?,
            general: value.general.into(),
        })
    }
}

impl From<MessageErrorPolicy> for StoredMessageErrorPolicy {
    fn from(value: MessageErrorPolicy) -> Self {
        match value {
            MessageErrorPolicy::Ignore => Self::Ignore,
            MessageErrorPolicy::Log => Self::Log,
            MessageErrorPolicy::Dlq { relay, mappings } => Self::Dlq {
                relay: relay.to_string(),
                mappings: mappings.into_iter().map(Into::into).collect(),
            },
        }
    }
}

impl TryFrom<StoredMessageErrorPolicy> for MessageErrorPolicy {
    type Error = Report<NameError>;

    fn try_from(value: StoredMessageErrorPolicy) -> Result<Self, Self::Error> {
        match value {
            StoredMessageErrorPolicy::Ignore => Ok(Self::Ignore),
            StoredMessageErrorPolicy::Log => Ok(Self::Log),
            StoredMessageErrorPolicy::Dlq { relay, mappings } => Ok(Self::Dlq {
                relay: Identifier::parse(&relay)?,
                mappings: mappings
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
        }
    }
}

impl From<GeneralErrorPolicy> for StoredGeneralErrorPolicy {
    fn from(value: GeneralErrorPolicy) -> Self {
        match value {
            GeneralErrorPolicy::Ignore => Self::Ignore,
            GeneralErrorPolicy::Log => Self::Log,
        }
    }
}

impl From<StoredGeneralErrorPolicy> for GeneralErrorPolicy {
    fn from(value: StoredGeneralErrorPolicy) -> Self {
        match value {
            StoredGeneralErrorPolicy::Ignore => Self::Ignore,
            StoredGeneralErrorPolicy::Log => Self::Log,
        }
    }
}

impl From<ErrorFieldMapping> for StoredErrorFieldMapping {
    fn from(value: ErrorFieldMapping) -> Self {
        Self {
            field: value.field.to_string(),
            value: value.value,
        }
    }
}

impl TryFrom<StoredErrorFieldMapping> for ErrorFieldMapping {
    type Error = Report<NameError>;

    fn try_from(value: StoredErrorFieldMapping) -> Result<Self, Self::Error> {
        Ok(Self {
            field: Identifier::parse(&value.field)?,
            value: value.value,
        })
    }
}

impl From<KafkaConfigEntry> for StoredKafkaConfigEntry {
    fn from(value: KafkaConfigEntry) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

impl From<StoredKafkaConfigEntry> for KafkaConfigEntry {
    fn from(value: StoredKafkaConfigEntry) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

impl From<CreateCodec> for StoredCreateCodec {
    fn from(value: CreateCodec) -> Self {
        Self {
            name: value.name.to_string(),
            wire_format: value.wire_format.into(),
            wire_schema: value.wire_schema.map(|wire_schema| wire_schema.to_string()),
            schema: value.schema.to_string(),
            encoding_rules: value.encoding_rules.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateCodec> for CreateCodec {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateCodec) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            wire_format: value.wire_format.try_into()?,
            wire_schema: value
                .wire_schema
                .as_deref()
                .map(Identifier::parse)
                .transpose()?,
            schema: Identifier::parse(&value.schema)?,
            encoding_rules: value
                .encoding_rules
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<CodecWireFormat> for StoredCodecWireFormat {
    fn from(value: CodecWireFormat) -> Self {
        match value {
            CodecWireFormat::Json => Self::Json,
            CodecWireFormat::Avro => Self::Avro,
            CodecWireFormat::JaqNative {
                format,
                transformations,
            } => Self::JaqNative {
                format: format.into(),
                transformations: transformations.into(),
            },
            CodecWireFormat::Protobuf(config) => Self::Protobuf(config.into()),
        }
    }
}

impl TryFrom<StoredCodecWireFormat> for CodecWireFormat {
    type Error = Report<NameError>;

    fn try_from(value: StoredCodecWireFormat) -> Result<Self, Self::Error> {
        Ok(match value {
            StoredCodecWireFormat::Json => Self::Json,
            StoredCodecWireFormat::Avro => Self::Avro,
            StoredCodecWireFormat::JaqNative {
                format,
                transformations,
            } => Self::JaqNative {
                format: format.into(),
                transformations: transformations.into(),
            },
            StoredCodecWireFormat::Protobuf(config) => Self::Protobuf(config.try_into()?),
        })
    }
}

impl From<CodecProtobufConfig> for StoredCodecProtobufConfig {
    fn from(value: CodecProtobufConfig) -> Self {
        Self {
            resource: value.resource.as_str().to_string(),
            resource_version: value.resource_version,
            config: value.config.into_iter().map(Into::into).collect(),
            message: value.message,
            transformations: value.transformations.into(),
        }
    }
}

impl TryFrom<StoredCodecProtobufConfig> for CodecProtobufConfig {
    type Error = Report<NameError>;

    fn try_from(value: StoredCodecProtobufConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            resource: Identifier::parse(&value.resource)?,
            resource_version: value.resource_version,
            config: value.config.into_iter().map(Into::into).collect(),
            message: value.message,
            transformations: value.transformations.into(),
        })
    }
}

impl From<CodecJaqFormat> for StoredCodecJaqFormat {
    fn from(value: CodecJaqFormat) -> Self {
        match value {
            CodecJaqFormat::Json => Self::Json,
            CodecJaqFormat::Yaml => Self::Yaml,
            CodecJaqFormat::Toml => Self::Toml,
            CodecJaqFormat::Xml => Self::Xml,
            CodecJaqFormat::Cbor => Self::Cbor,
        }
    }
}

impl From<StoredCodecJaqFormat> for CodecJaqFormat {
    fn from(value: StoredCodecJaqFormat) -> Self {
        match value {
            StoredCodecJaqFormat::Json => Self::Json,
            StoredCodecJaqFormat::Yaml => Self::Yaml,
            StoredCodecJaqFormat::Toml => Self::Toml,
            StoredCodecJaqFormat::Xml => Self::Xml,
            StoredCodecJaqFormat::Cbor => Self::Cbor,
        }
    }
}

impl From<CodecJaqTransformations> for StoredCodecJaqTransformations {
    fn from(value: CodecJaqTransformations) -> Self {
        Self {
            on_ingestion: value.on_ingestion,
            on_emitting: value.on_emitting,
        }
    }
}

impl From<StoredCodecJaqTransformations> for CodecJaqTransformations {
    fn from(value: StoredCodecJaqTransformations) -> Self {
        Self {
            on_ingestion: value.on_ingestion,
            on_emitting: value.on_emitting,
        }
    }
}

impl From<CodecEncodingRule> for StoredCodecEncodingRule {
    fn from(value: CodecEncodingRule) -> Self {
        Self {
            field: value.field.as_str().to_string(),
            encoding: value.encoding.into(),
        }
    }
}

impl TryFrom<StoredCodecEncodingRule> for CodecEncodingRule {
    type Error = Report<NameError>;

    fn try_from(value: StoredCodecEncodingRule) -> Result<Self, Self::Error> {
        Ok(Self {
            field: Identifier::parse(&value.field)?,
            encoding: value.encoding.into(),
        })
    }
}

impl From<CodecEncoding> for StoredCodecEncoding {
    fn from(value: CodecEncoding) -> Self {
        match value {
            CodecEncoding::Rfc3339 => Self::Rfc3339,
        }
    }
}

impl From<StoredCodecEncoding> for CodecEncoding {
    fn from(value: StoredCodecEncoding) -> Self {
        match value {
            StoredCodecEncoding::Rfc3339 => Self::Rfc3339,
        }
    }
}

impl From<CreateIngestor> for StoredCreateIngestor {
    fn from(value: CreateIngestor) -> Self {
        Self {
            name: value.name.to_string(),
            output_routes: value.output_routes.into(),
            decode_using_codec: value.decode_using_codec.to_string(),
            parameterized_by: value.parameterized_by.into(),
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            timestamp_source: value.timestamp_source.map(Into::into),
            source: value.source.into(),
            error_policies: value.error_policies.into(),
            filter_where: value.filter_where,
        }
    }
}

impl From<ParameterValueMapping> for StoredParameterValueMapping {
    fn from(value: ParameterValueMapping) -> Self {
        Self {
            field: value.field.to_string(),
            relay: value.relay.to_string(),
            relay_field: value.relay_field.to_string(),
        }
    }
}

impl TryFrom<StoredParameterValueMapping> for ParameterValueMapping {
    type Error = Report<NameError>;

    fn try_from(value: StoredParameterValueMapping) -> Result<Self, Self::Error> {
        Ok(Self {
            field: Identifier::parse(&value.field)?,
            relay: Identifier::parse(&value.relay)?,
            relay_field: Identifier::parse(&value.relay_field)?,
        })
    }
}

impl From<BranchParameterization> for StoredBranchParameterization {
    fn from(value: BranchParameterization) -> Self {
        match value {
            BranchParameterization::Parameterized {
                schema,
                values,
                ttl,
            } => Self::Parameterized {
                schema: schema.to_string(),
                values: values.into_iter().map(Into::into).collect(),
                ttl,
            },
            BranchParameterization::Unparameterized => Self::Unparameterized,
        }
    }
}

impl TryFrom<StoredBranchParameterization> for BranchParameterization {
    type Error = Report<NameError>;

    fn try_from(value: StoredBranchParameterization) -> Result<Self, Self::Error> {
        Ok(match value {
            StoredBranchParameterization::Parameterized {
                schema,
                values,
                ttl,
            } => BranchParameterization::Parameterized {
                schema: Identifier::parse(&schema)?,
                values: values
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
                ttl,
            },
            StoredBranchParameterization::Unparameterized => {
                BranchParameterization::unparameterized()
            }
        })
    }
}

impl From<ProcessorOutput> for StoredProcessorOutput {
    fn from(value: ProcessorOutput) -> Self {
        Self {
            relay: value.relay.to_string(),
            filter_map: value.filter_map,
        }
    }
}

impl TryFrom<StoredProcessorOutput> for ProcessorOutput {
    type Error = Report<NameError>;

    fn try_from(value: StoredProcessorOutput) -> Result<Self, Self::Error> {
        Ok(Self {
            relay: Identifier::parse(&value.relay)?,
            filter_map: value.filter_map,
        })
    }
}

impl From<ProcessorOutputs> for StoredProcessorOutputs {
    fn from(value: ProcessorOutputs) -> Self {
        Self {
            routes: value.routes.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredProcessorOutputs> for ProcessorOutputs {
    type Error = Report<NameError>;

    fn try_from(value: StoredProcessorOutputs) -> Result<Self, Self::Error> {
        Ok(Self {
            routes: value
                .routes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<StoredCreateIngestor> for CreateIngestor {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateIngestor) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            output_routes: value.output_routes.try_into()?,
            decode_using_codec: Identifier::parse(&value.decode_using_codec)?,
            parameterized_by: value.parameterized_by.try_into()?,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            timestamp_source: value.timestamp_source.map(TryInto::try_into).transpose()?,
            source: value.source.try_into()?,
            error_policies: value.error_policies.try_into()?,
            filter_where: value.filter_where,
        })
    }
}

impl From<IngestTimestampSource> for StoredIngestTimestampSource {
    fn from(value: IngestTimestampSource) -> Self {
        match value {
            IngestTimestampSource::Now => Self::Now,
            IngestTimestampSource::At(field) => Self::At(field.to_string()),
        }
    }
}

impl TryFrom<StoredIngestTimestampSource> for IngestTimestampSource {
    type Error = Report<NameError>;

    fn try_from(value: StoredIngestTimestampSource) -> Result<Self, Self::Error> {
        match value {
            StoredIngestTimestampSource::Now => Ok(Self::Now),
            StoredIngestTimestampSource::At(field) => Ok(Self::At(Identifier::parse(&field)?)),
        }
    }
}

impl From<CreateReingestor> for StoredCreateReingestor {
    fn from(value: CreateReingestor) -> Self {
        Self {
            name: value.name.to_string(),
            from_relay: value.from_relay.to_string(),
            output_routes: value.output_routes.into(),
            parameterized_by: value.parameterized_by.into(),
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.into(),
            filter_where: value.filter_where,
        }
    }
}

impl TryFrom<StoredCreateReingestor> for CreateReingestor {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateReingestor) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            from_relay: Identifier::parse(&value.from_relay)?,
            output_routes: value.output_routes.try_into()?,
            parameterized_by: value.parameterized_by.try_into()?,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.try_into()?,
            filter_where: value.filter_where,
        })
    }
}

impl From<CreateInferencer> for StoredCreateInferencer {
    fn from(value: CreateInferencer) -> Self {
        Self {
            name: value.name.to_string(),
            from_relay: value.from_relay.to_string(),
            output_routes: value.output_routes.into(),
            parameterized_by: value.parameterized_by.into(),
            resource: value.resource.to_string(),
            resource_version: value.resource_version,
            file: value.file,
            inputs: value.inputs.into_iter().map(Into::into).collect(),
            outputs: value.outputs.into_iter().map(Into::into).collect(),
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.into(),
            filter_where: value.filter_where,
        }
    }
}

impl TryFrom<StoredCreateInferencer> for CreateInferencer {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateInferencer) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            from_relay: Identifier::parse(&value.from_relay)?,
            output_routes: value.output_routes.try_into()?,
            parameterized_by: value.parameterized_by.try_into()?,
            resource: Identifier::parse(&value.resource)?,
            resource_version: value.resource_version,
            file: value.file,
            inputs: value
                .inputs
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            outputs: value
                .outputs
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.try_into()?,
            filter_where: value.filter_where,
        })
    }
}

impl From<CreateWasmProcessor> for StoredCreateWasmProcessor {
    fn from(value: CreateWasmProcessor) -> Self {
        Self {
            name: value.name.to_string(),
            from_relay: value.from_relay.to_string(),
            output_routes: value.output_routes.into(),
            parameterized_by: value.parameterized_by.into(),
            resource: value.resource.to_string(),
            resource_version: value.resource_version,
            file: value.file,
            mode: value.mode,
            message_error_policy: value.message_error_policy.into(),
            global_error_policy: value.global_error_policy.into(),
            filter_where: value.filter_where,
        }
    }
}

impl TryFrom<StoredCreateWasmProcessor> for CreateWasmProcessor {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateWasmProcessor) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            from_relay: Identifier::parse(&value.from_relay)?,
            output_routes: value.output_routes.try_into()?,
            parameterized_by: value.parameterized_by.try_into()?,
            resource: Identifier::parse(&value.resource)?,
            resource_version: value.resource_version,
            file: value.file,
            mode: value.mode,
            message_error_policy: value.message_error_policy.try_into()?,
            global_error_policy: value.global_error_policy.into(),
            filter_where: value.filter_where,
        })
    }
}

impl From<InferencerTensorMapping> for StoredInferencerTensorMapping {
    fn from(value: InferencerTensorMapping) -> Self {
        Self {
            tensor: value.tensor,
            relay: value.relay.to_string(),
            field: value.field.to_string(),
        }
    }
}

impl TryFrom<StoredInferencerTensorMapping> for InferencerTensorMapping {
    type Error = Report<NameError>;

    fn try_from(value: StoredInferencerTensorMapping) -> Result<Self, Self::Error> {
        Ok(Self {
            tensor: value.tensor,
            relay: Identifier::parse(&value.relay)?,
            field: Identifier::parse(&value.field)?,
        })
    }
}

impl From<IngestSource> for StoredIngestSource {
    fn from(value: IngestSource) -> Self {
        match value {
            IngestSource::Http { client, every } => Self::Http {
                client: client.to_string(),
                every,
            },
            IngestSource::Kinesis {
                client,
                relay,
                instances,
                mode,
            } => Self::Kinesis {
                client: client.to_string(),
                relay: relay.to_string(),
                instances,
                mode: mode.into(),
            },
            IngestSource::Kafka {
                client,
                topic,
                offset_mode,
                instances,
                mode,
            } => Self::Kafka {
                client: client.to_string(),
                topic: topic.to_string(),
                offset_mode: offset_mode.into(),
                instances,
                mode: mode.into(),
            },
            IngestSource::Pulsar {
                client,
                topic,
                subscription,
                instances,
                mode,
            } => Self::Pulsar {
                client: client.to_string(),
                topic: topic.to_string(),
                subscription: subscription.to_string(),
                instances,
                mode: mode.into(),
            },
            IngestSource::RabbitMq {
                client,
                queue,
                instances,
                mode,
            } => Self::RabbitMq {
                client: client.to_string(),
                queue: queue.to_string(),
                instances,
                mode: mode.into(),
            },
            IngestSource::RedisPubSub {
                client,
                channel,
                mode,
            } => Self::RedisPubSub {
                client: client.to_string(),
                channel: channel.to_string(),
                mode: mode.into(),
            },
            IngestSource::Mqtt {
                client,
                topic,
                instances,
                mode,
            } => Self::Mqtt {
                client: client.to_string(),
                topic,
                instances,
                mode: mode.into(),
            },
            IngestSource::Nats {
                client,
                subject,
                queue_group,
                instances,
                mode,
            } => Self::Nats {
                client: client.to_string(),
                subject: subject.to_string(),
                queue_group: queue_group.to_string(),
                instances,
                mode: mode.into(),
            },
            IngestSource::Prometheus {
                client,
                query,
                every,
            } => Self::Prometheus {
                client: client.to_string(),
                query,
                every,
            },
            IngestSource::ZeroMq { client, mode } => Self::ZeroMq {
                client: client.to_string(),
                mode: mode.into(),
            },
            IngestSource::Sqs {
                client,
                queue,
                instances,
                mode,
            } => Self::Sqs {
                client: client.to_string(),
                queue: queue.to_string(),
                instances,
                mode: mode.into(),
            },
            IngestSource::Endpoint { endpoint, mode } => Self::Endpoint {
                endpoint: endpoint.to_string(),
                mode: mode.into(),
            },
            IngestSource::Websockets { client, mode } => Self::Websockets {
                client: client.to_string(),
                mode: mode.into(),
            },
        }
    }
}

impl TryFrom<StoredIngestSource> for IngestSource {
    type Error = Report<NameError>;

    fn try_from(value: StoredIngestSource) -> Result<Self, Self::Error> {
        match value {
            StoredIngestSource::Http { client, every } => Ok(Self::Http {
                client: Identifier::parse(&client)?,
                every,
            }),
            StoredIngestSource::Kinesis {
                client,
                relay,
                instances,
                mode,
            } => Ok(Self::Kinesis {
                client: Identifier::parse(&client)?,
                relay: Identifier::parse(&relay)?,
                instances,
                mode: mode.into(),
            }),
            StoredIngestSource::Kafka {
                client,
                topic,
                offset_mode,
                instances,
                mode,
            } => Ok(Self::Kafka {
                client: Identifier::parse(&client)?,
                topic: Identifier::parse(&topic)?,
                offset_mode: offset_mode.try_into()?,
                instances,
                mode: mode.into(),
            }),
            StoredIngestSource::Pulsar {
                client,
                topic,
                subscription,
                instances,
                mode,
            } => Ok(Self::Pulsar {
                client: Identifier::parse(&client)?,
                topic: Identifier::parse(&topic)?,
                subscription: Identifier::parse(&subscription)?,
                instances,
                mode: PulsarIngestMode::from(mode),
            }),
            StoredIngestSource::RabbitMq {
                client,
                queue,
                instances,
                mode,
            } => Ok(Self::RabbitMq {
                client: Identifier::parse(&client)?,
                queue: Identifier::parse(&queue)?,
                instances,
                mode: mode.into(),
            }),
            StoredIngestSource::RedisPubSub {
                client,
                channel,
                mode,
            } => Ok(Self::RedisPubSub {
                client: Identifier::parse(&client)?,
                channel: Identifier::parse(&channel)?,
                mode: mode.into(),
            }),
            StoredIngestSource::Mqtt {
                client,
                topic,
                instances,
                mode,
            } => Ok(Self::Mqtt {
                client: Identifier::parse(&client)?,
                topic,
                instances,
                mode: mode.into(),
            }),
            StoredIngestSource::Nats {
                client,
                subject,
                queue_group,
                instances,
                mode,
            } => Ok(Self::Nats {
                client: Identifier::parse(&client)?,
                subject: Identifier::parse(&subject)?,
                queue_group: Identifier::parse(&queue_group)?,
                instances,
                mode: mode.into(),
            }),
            StoredIngestSource::Prometheus {
                client,
                query,
                every,
            } => Ok(Self::Prometheus {
                client: Identifier::parse(&client)?,
                query,
                every,
            }),
            StoredIngestSource::ZeroMq { client, mode } => Ok(Self::ZeroMq {
                client: Identifier::parse(&client)?,
                mode: mode.into(),
            }),
            StoredIngestSource::Sqs {
                client,
                queue,
                instances,
                mode,
            } => Ok(Self::Sqs {
                client: Identifier::parse(&client)?,
                queue: Identifier::parse(&queue)?,
                instances,
                mode: mode.into(),
            }),
            StoredIngestSource::Endpoint { endpoint, mode } => Ok(Self::Endpoint {
                endpoint: Identifier::parse(&endpoint)?,
                mode: mode.into(),
            }),
            StoredIngestSource::Websockets { client, mode } => Ok(Self::Websockets {
                client: Identifier::parse(&client)?,
                mode: mode.into(),
            }),
        }
    }
}

impl From<KafkaOffsetMode> for StoredKafkaOffsetMode {
    fn from(value: KafkaOffsetMode) -> Self {
        match value {
            KafkaOffsetMode::ConsumerGroup(group) => Self::ConsumerGroup(group.to_string()),
            KafkaOffsetMode::Domain => Self::Domain,
        }
    }
}

impl TryFrom<StoredKafkaOffsetMode> for KafkaOffsetMode {
    type Error = Report<NameError>;

    fn try_from(value: StoredKafkaOffsetMode) -> Result<Self, Self::Error> {
        match value {
            StoredKafkaOffsetMode::ConsumerGroup(group) => {
                Ok(Self::ConsumerGroup(Identifier::parse(&group)?))
            }
            StoredKafkaOffsetMode::Domain => Ok(Self::Domain),
        }
    }
}

impl From<EndpointType> for StoredEndpointType {
    fn from(value: EndpointType) -> Self {
        match value {
            EndpointType::Websockets => Self::Websockets,
            EndpointType::Http => Self::Http,
        }
    }
}

impl From<StoredEndpointType> for EndpointType {
    fn from(value: StoredEndpointType) -> Self {
        match value {
            StoredEndpointType::Websockets => Self::Websockets,
            StoredEndpointType::Http => Self::Http,
        }
    }
}

impl From<KafkaIngestMode> for StoredKafkaIngestMode {
    fn from(value: KafkaIngestMode) -> Self {
        match value {
            KafkaIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => Self::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_backoff: retry_policy.backoff,
                retry_max_backoff: retry_policy.max_backoff,
            },
            KafkaIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::AckSequential {
                timeout,
                retry_backoff: retry_policy.backoff,
                retry_max_backoff: retry_policy.max_backoff,
            },
            KafkaIngestMode::NoAckParallel { max } => Self::NoAckParallel { max },
        }
    }
}

impl From<KinesisIngestMode> for StoredKinesisIngestMode {
    fn from(value: KinesisIngestMode) -> Self {
        match value {
            KinesisIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::AckSequential {
                timeout,
                retry_backoff: retry_policy.backoff,
                retry_max_backoff: retry_policy.max_backoff,
            },
        }
    }
}

impl From<StoredKinesisIngestMode> for KinesisIngestMode {
    fn from(value: StoredKinesisIngestMode) -> Self {
        match value {
            StoredKinesisIngestMode::AckSequential {
                timeout,
                retry_backoff,
                retry_max_backoff,
            } => Self::AckSequential {
                timeout,
                retry_policy: nervix_models::RetryPolicy {
                    backoff: retry_backoff,
                    max_backoff: retry_max_backoff,
                },
            },
        }
    }
}

impl From<StoredKafkaIngestMode> for KafkaIngestMode {
    fn from(value: StoredKafkaIngestMode) -> Self {
        match value {
            StoredKafkaIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_backoff,
                retry_max_backoff,
            } => Self::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy: nervix_models::RetryPolicy {
                    backoff: retry_backoff,
                    max_backoff: retry_max_backoff,
                },
            },
            StoredKafkaIngestMode::AckSequential {
                timeout,
                retry_backoff,
                retry_max_backoff,
            } => Self::AckSequential {
                timeout,
                retry_policy: nervix_models::RetryPolicy {
                    backoff: retry_backoff,
                    max_backoff: retry_max_backoff,
                },
            },
            StoredKafkaIngestMode::NoAckParallel { max } => Self::NoAckParallel { max },
        }
    }
}

impl From<RabbitMqIngestMode> for StoredRabbitMqIngestMode {
    fn from(value: RabbitMqIngestMode) -> Self {
        match value {
            RabbitMqIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::AckSequential {
                timeout,
                retry_backoff: retry_policy.backoff,
                retry_max_backoff: retry_policy.max_backoff,
            },
        }
    }
}

impl From<StoredRabbitMqIngestMode> for RabbitMqIngestMode {
    fn from(value: StoredRabbitMqIngestMode) -> Self {
        match value {
            StoredRabbitMqIngestMode::AckSequential {
                timeout,
                retry_backoff,
                retry_max_backoff,
            } => Self::AckSequential {
                timeout,
                retry_policy: nervix_models::RetryPolicy {
                    backoff: retry_backoff,
                    max_backoff: retry_max_backoff,
                },
            },
        }
    }
}

impl From<RedisPubSubIngestMode> for StoredRedisPubSubIngestMode {
    fn from(value: RedisPubSubIngestMode) -> Self {
        match value {
            RedisPubSubIngestMode::NoAckSequential => Self::NoAckSequential,
        }
    }
}

impl From<StoredRedisPubSubIngestMode> for RedisPubSubIngestMode {
    fn from(value: StoredRedisPubSubIngestMode) -> Self {
        match value {
            StoredRedisPubSubIngestMode::NoAckSequential => Self::NoAckSequential,
        }
    }
}

impl From<MqttIngestMode> for StoredMqttIngestMode {
    fn from(value: MqttIngestMode) -> Self {
        match value {
            MqttIngestMode::NoAckSequential { session, qos } => Self::NoAckSequential {
                session: session.into(),
                qos: qos.into(),
            },
            MqttIngestMode::NoAckParallel { max, session, qos } => Self::NoAckParallel {
                max,
                session: session.into(),
                qos: qos.into(),
            },
            MqttIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::AckSequential {
                timeout,
                retry_backoff: retry_policy.backoff,
                retry_max_backoff: retry_policy.max_backoff,
            },
            MqttIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => Self::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_backoff: retry_policy.backoff,
                retry_max_backoff: retry_policy.max_backoff,
            },
        }
    }
}

impl From<StoredMqttIngestMode> for MqttIngestMode {
    fn from(value: StoredMqttIngestMode) -> Self {
        match value {
            StoredMqttIngestMode::NoAckSequential { session, qos } => Self::NoAckSequential {
                session: session.into(),
                qos: qos.into(),
            },
            StoredMqttIngestMode::NoAckParallel { max, session, qos } => Self::NoAckParallel {
                max,
                session: session.into(),
                qos: qos.into(),
            },
            StoredMqttIngestMode::AckSequential {
                timeout,
                retry_backoff,
                retry_max_backoff,
            } => Self::AckSequential {
                timeout,
                retry_policy: nervix_models::RetryPolicy {
                    backoff: retry_backoff,
                    max_backoff: retry_max_backoff,
                },
            },
            StoredMqttIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_backoff,
                retry_max_backoff,
            } => Self::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy: nervix_models::RetryPolicy {
                    backoff: retry_backoff,
                    max_backoff: retry_max_backoff,
                },
            },
        }
    }
}

impl From<MqttSession> for StoredMqttSession {
    fn from(value: MqttSession) -> Self {
        match value {
            MqttSession::Clean => Self::Clean,
            MqttSession::Persistent => Self::Persistent,
        }
    }
}

impl From<StoredMqttSession> for MqttSession {
    fn from(value: StoredMqttSession) -> Self {
        match value {
            StoredMqttSession::Clean => Self::Clean,
            StoredMqttSession::Persistent => Self::Persistent,
        }
    }
}

impl From<MqttQos> for StoredMqttQos {
    fn from(value: MqttQos) -> Self {
        match value {
            MqttQos::AtMostOnce => Self::AtMostOnce,
            MqttQos::AtLeastOnce => Self::AtLeastOnce,
        }
    }
}

impl From<StoredMqttQos> for MqttQos {
    fn from(value: StoredMqttQos) -> Self {
        match value {
            StoredMqttQos::AtMostOnce => Self::AtMostOnce,
            StoredMqttQos::AtLeastOnce => Self::AtLeastOnce,
        }
    }
}

impl From<NatsIngestMode> for StoredNatsIngestMode {
    fn from(value: NatsIngestMode) -> Self {
        match value {
            NatsIngestMode::NoAckSequential => Self::NoAckSequential,
        }
    }
}

impl From<StoredNatsIngestMode> for NatsIngestMode {
    fn from(value: StoredNatsIngestMode) -> Self {
        match value {
            StoredNatsIngestMode::NoAckSequential => Self::NoAckSequential,
        }
    }
}

impl From<ZeroMqIngestMode> for StoredZeroMqIngestMode {
    fn from(value: ZeroMqIngestMode) -> Self {
        match value {
            ZeroMqIngestMode::NoAckSequential => Self::NoAckSequential,
        }
    }
}

impl From<StoredZeroMqIngestMode> for ZeroMqIngestMode {
    fn from(value: StoredZeroMqIngestMode) -> Self {
        match value {
            StoredZeroMqIngestMode::NoAckSequential => Self::NoAckSequential,
        }
    }
}

impl From<SqsIngestMode> for StoredSqsIngestMode {
    fn from(value: SqsIngestMode) -> Self {
        match value {
            SqsIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::AckSequential {
                timeout,
                retry_backoff: retry_policy.backoff,
                retry_max_backoff: retry_policy.max_backoff,
            },
        }
    }
}

impl From<StoredSqsIngestMode> for SqsIngestMode {
    fn from(value: StoredSqsIngestMode) -> Self {
        match value {
            StoredSqsIngestMode::AckSequential {
                timeout,
                retry_backoff,
                retry_max_backoff,
            } => Self::AckSequential {
                timeout,
                retry_policy: nervix_models::RetryPolicy {
                    backoff: retry_backoff,
                    max_backoff: retry_max_backoff,
                },
            },
        }
    }
}

impl From<EndpointIngestMode> for StoredEndpointIngestMode {
    fn from(value: EndpointIngestMode) -> Self {
        match value {
            EndpointIngestMode::NoAckSequential => Self::NoAckSequential,
        }
    }
}

impl From<StoredEndpointIngestMode> for EndpointIngestMode {
    fn from(value: StoredEndpointIngestMode) -> Self {
        match value {
            StoredEndpointIngestMode::NoAckSequential => EndpointIngestMode::NoAckSequential,
        }
    }
}

impl From<WebsocketsIngestMode> for StoredWebsocketsIngestMode {
    fn from(value: WebsocketsIngestMode) -> Self {
        match value {
            WebsocketsIngestMode::NoAckSequential => Self::NoAckSequential,
        }
    }
}

impl From<StoredWebsocketsIngestMode> for WebsocketsIngestMode {
    fn from(value: StoredWebsocketsIngestMode) -> Self {
        match value {
            StoredWebsocketsIngestMode::NoAckSequential => WebsocketsIngestMode::NoAckSequential,
        }
    }
}

impl From<CreateRelay> for StoredCreateRelay {
    fn from(value: CreateRelay) -> Self {
        let parameterization = match value.parameterization {
            RelayParameterization::Parameterized { parameters } => {
                let parameters = match parameters {
                    RelayParameters::Declared(schema) => {
                        StoredRelayParameters::Declared(schema.to_string())
                    }
                    RelayParameters::Inferred => StoredRelayParameters::Inferred,
                };
                StoredRelayParameterization::Parameterized { parameters }
            }
            RelayParameterization::Unparameterized => StoredRelayParameterization::Unparameterized,
        };
        Self {
            name: value.name.to_string(),
            schema: value.schema.to_string(),
            buffer: value.buffer,
            parameterization,
            materialized_state: value.materialized_state.map(Into::into),
        }
    }
}

impl TryFrom<StoredCreateRelay> for CreateRelay {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateRelay) -> Result<Self, Self::Error> {
        let parameterization = match value.parameterization {
            StoredRelayParameterization::Parameterized { parameters } => {
                let parameters = match parameters {
                    StoredRelayParameters::Declared(schema) => {
                        RelayParameters::declared(Identifier::parse(&schema)?)
                    }
                    StoredRelayParameters::Inferred => RelayParameters::inferred(),
                };
                RelayParameterization::parameterized(parameters)
            }
            StoredRelayParameterization::Unparameterized => {
                RelayParameterization::unparameterized()
            }
        };
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            schema: Identifier::parse(&value.schema)?,
            buffer: value.buffer,
            parameterization,
            materialized_state: value.materialized_state.map(Into::into),
        })
    }
}

impl From<MaterializedRelayState> for StoredMaterializedRelayState {
    fn from(value: MaterializedRelayState) -> Self {
        match value {
            MaterializedRelayState::LastByTimestamp => Self::LastByTimestamp,
        }
    }
}

impl From<StoredMaterializedRelayState> for MaterializedRelayState {
    fn from(value: StoredMaterializedRelayState) -> Self {
        match value {
            StoredMaterializedRelayState::LastByTimestamp => Self::LastByTimestamp,
        }
    }
}

impl From<CreateLookup> for StoredCreateLookup {
    fn from(value: CreateLookup) -> Self {
        Self {
            name: value.name.to_string(),
            key_field: value.key_field.to_string(),
            resource: value.resource.to_string(),
            path: value.path,
            decode_using_codec: value.decode_using_codec.to_string(),
        }
    }
}

impl TryFrom<StoredCreateLookup> for CreateLookup {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateLookup) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            key_field: Identifier::parse(&value.key_field)?,
            resource: Identifier::parse(&value.resource)?,
            path: value.path,
            decode_using_codec: Identifier::parse(&value.decode_using_codec)?,
        })
    }
}

impl From<CreateDeduplicator> for StoredCreateDeduplicator {
    fn from(value: CreateDeduplicator) -> Self {
        Self {
            name: value.name.to_string(),
            from_relay: value.from_relay.to_string(),
            output_routes: value.output_routes.into(),
            parameterized_by: value.parameterized_by.into(),
            deduplicate_on: value.deduplicate_on,
            max_time: value.max_time,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.into(),
            filter_where: value.filter_where,
        }
    }
}

impl TryFrom<StoredCreateDeduplicator> for CreateDeduplicator {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateDeduplicator) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            from_relay: Identifier::parse(&value.from_relay)?,
            output_routes: value.output_routes.try_into()?,
            parameterized_by: value.parameterized_by.try_into()?,
            deduplicate_on: value.deduplicate_on,
            max_time: value.max_time,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.try_into()?,
            filter_where: value.filter_where,
        })
    }
}

impl From<CreateCorrelator> for StoredCreateCorrelator {
    fn from(value: CreateCorrelator) -> Self {
        Self {
            name: value.name.to_string(),
            left_relay: value.left_relay.to_string(),
            right_relay: value.right_relay.to_string(),
            output_routes: value.output_routes.into(),
            parameterized_by: value.parameterized_by.into(),
            left_on: value.left_on,
            right_on: value.right_on,
            match_policy: value.match_policy.into(),
            output: value.output,
            max_time: value.max_time,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            timeout_policy: value.timeout_policy.into(),
            mode: value.mode,
            message_error_policy: value.message_error_policy.into(),
            filter_where: value.filter_where,
        }
    }
}

impl TryFrom<StoredCreateCorrelator> for CreateCorrelator {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateCorrelator) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            left_relay: Identifier::parse(&value.left_relay)?,
            right_relay: Identifier::parse(&value.right_relay)?,
            output_routes: value.output_routes.try_into()?,
            parameterized_by: value.parameterized_by.try_into()?,
            left_on: value.left_on,
            right_on: value.right_on,
            match_policy: value.match_policy.into(),
            output: value.output,
            max_time: value.max_time,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            timeout_policy: value.timeout_policy.try_into()?,
            mode: value.mode,
            message_error_policy: value.message_error_policy.try_into()?,
            filter_where: value.filter_where,
        })
    }
}

impl From<CorrelatorMatchPolicy> for StoredCorrelatorMatchPolicy {
    fn from(value: CorrelatorMatchPolicy) -> Self {
        match value {
            CorrelatorMatchPolicy::Earliest => Self::Earliest,
            CorrelatorMatchPolicy::Latest => Self::Latest,
        }
    }
}

impl From<StoredCorrelatorMatchPolicy> for CorrelatorMatchPolicy {
    fn from(value: StoredCorrelatorMatchPolicy) -> Self {
        match value {
            StoredCorrelatorMatchPolicy::Earliest => Self::Earliest,
            StoredCorrelatorMatchPolicy::Latest => Self::Latest,
        }
    }
}

impl From<CorrelationTimeoutPolicy> for StoredCorrelationTimeoutPolicy {
    fn from(value: CorrelationTimeoutPolicy) -> Self {
        Self {
            left: value.left.into(),
            right: value.right.into(),
        }
    }
}

impl TryFrom<StoredCorrelationTimeoutPolicy> for CorrelationTimeoutPolicy {
    type Error = Report<NameError>;

    fn try_from(value: StoredCorrelationTimeoutPolicy) -> Result<Self, Self::Error> {
        Ok(Self {
            left: value.left.try_into()?,
            right: value.right.try_into()?,
        })
    }
}

impl From<CorrelationTimeoutAction> for StoredCorrelationTimeoutAction {
    fn from(value: CorrelationTimeoutAction) -> Self {
        match value {
            CorrelationTimeoutAction::Drop => Self::Drop,
            CorrelationTimeoutAction::SendTo { relay } => Self::SendTo {
                relay: relay.to_string(),
            },
        }
    }
}

impl TryFrom<StoredCorrelationTimeoutAction> for CorrelationTimeoutAction {
    type Error = Report<NameError>;

    fn try_from(value: StoredCorrelationTimeoutAction) -> Result<Self, Self::Error> {
        match value {
            StoredCorrelationTimeoutAction::Drop => Ok(Self::Drop),
            StoredCorrelationTimeoutAction::SendTo { relay } => Ok(Self::SendTo {
                relay: Identifier::parse(&relay)?,
            }),
        }
    }
}

impl From<CreateReorderer> for StoredCreateReorderer {
    fn from(value: CreateReorderer) -> Self {
        Self {
            name: value.name.to_string(),
            from_relay: value.from_relay.to_string(),
            output_routes: value.output_routes.into(),
            parameterized_by: value.parameterized_by.into(),
            order_by: value.order_by,
            max_time: value.max_time,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.into(),
            filter_where: value.filter_where,
        }
    }
}

impl TryFrom<StoredCreateReorderer> for CreateReorderer {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateReorderer) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            from_relay: Identifier::parse(&value.from_relay)?,
            output_routes: value.output_routes.try_into()?,
            parameterized_by: value.parameterized_by.try_into()?,
            order_by: value.order_by,
            max_time: value.max_time,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.try_into()?,
            filter_where: value.filter_where,
        })
    }
}

impl From<CreateWindowProcessor> for StoredCreateWindowProcessor {
    fn from(value: CreateWindowProcessor) -> Self {
        Self {
            name: value.name.to_string(),
            from_relay: value.from_relay.to_string(),
            output_routes: value.output_routes.into(),
            parameterized_by: value.parameterized_by.into(),
            width: value.width.into(),
            step: value.step.into(),
            aggregate: value.aggregate,
            mode: value.mode,
            message_error_policy: value.message_error_policy.into(),
            filter_where: value.filter_where,
        }
    }
}

impl TryFrom<StoredCreateWindowProcessor> for CreateWindowProcessor {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateWindowProcessor) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            from_relay: Identifier::parse(&value.from_relay)?,
            output_routes: value.output_routes.try_into()?,
            parameterized_by: value.parameterized_by.try_into()?,
            width: value.width.into(),
            step: value.step.into(),
            aggregate: value.aggregate,
            mode: value.mode,
            message_error_policy: value.message_error_policy.try_into()?,
            filter_where: value.filter_where,
        })
    }
}

impl From<WindowBound> for StoredWindowBound {
    fn from(value: WindowBound) -> Self {
        Self {
            messages: value.messages,
            duration: value.duration,
        }
    }
}

impl From<StoredWindowBound> for WindowBound {
    fn from(value: StoredWindowBound) -> Self {
        Self {
            messages: value.messages,
            duration: value.duration,
        }
    }
}

impl From<CreateUnifier> for StoredCreateUnifier {
    fn from(value: CreateUnifier) -> Self {
        Self {
            name: value.name.to_string(),
            from_relays: value
                .from_relays
                .into_iter()
                .map(|relay| relay.to_string())
                .collect(),
            output_routes: value.output_routes.into(),
            parameterized_by: value.parameterized_by.into(),
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.into(),
            filter_where: value.filter_where,
        }
    }
}

impl TryFrom<StoredCreateUnifier> for CreateUnifier {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateUnifier) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            from_relays: value
                .from_relays
                .into_iter()
                .map(|stream| Identifier::parse(&stream))
                .collect::<Result<Vec<_>, _>>()?,
            output_routes: value.output_routes.try_into()?,
            parameterized_by: value.parameterized_by.try_into()?,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            message_error_policy: value.message_error_policy.try_into()?,
            filter_where: value.filter_where,
        })
    }
}

impl From<CreateEmitter> for StoredCreateEmitter {
    fn from(value: CreateEmitter) -> Self {
        Self {
            name: value.name.to_string(),
            from_relay: value.from_relay.to_string(),
            encode_using_codec: value.encode_using_codec.map(|codec| codec.to_string()),
            sink: value.sink.into(),
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            error_policies: value.error_policies.into(),
            filter_map: value.filter_map,
        }
    }
}

impl TryFrom<StoredCreateEmitter> for CreateEmitter {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateEmitter) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            from_relay: Identifier::parse(&value.from_relay)?,
            encode_using_codec: value
                .encode_using_codec
                .map(|codec| Identifier::parse(&codec))
                .transpose()?,
            sink: value.sink.try_into()?,
            flush_each: value.flush_each,
            max_batch_size: value.max_batch_size,
            mode: value.mode,
            error_policies: value.error_policies.try_into()?,
            filter_map: value.filter_map,
        })
    }
}

impl From<PostgresConflictAction> for StoredPostgresConflictAction {
    fn from(value: PostgresConflictAction) -> Self {
        match value {
            PostgresConflictAction::None => Self::None,
            PostgresConflictAction::DoNothing { target } => Self::DoNothing { target },
            PostgresConflictAction::DoUpdate { target } => Self::DoUpdate { target },
        }
    }
}

impl From<StoredPostgresConflictAction> for PostgresConflictAction {
    fn from(value: StoredPostgresConflictAction) -> Self {
        match value {
            StoredPostgresConflictAction::None => Self::None,
            StoredPostgresConflictAction::DoNothing { target } => Self::DoNothing { target },
            StoredPostgresConflictAction::DoUpdate { target } => Self::DoUpdate { target },
        }
    }
}

impl From<MySqlConflictAction> for StoredMySqlConflictAction {
    fn from(value: MySqlConflictAction) -> Self {
        match value {
            MySqlConflictAction::None => Self::None,
            MySqlConflictAction::DoNothing => Self::DoNothing,
            MySqlConflictAction::DoUpdate => Self::DoUpdate,
        }
    }
}

impl From<StoredMySqlConflictAction> for MySqlConflictAction {
    fn from(value: StoredMySqlConflictAction) -> Self {
        match value {
            StoredMySqlConflictAction::None => Self::None,
            StoredMySqlConflictAction::DoNothing => Self::DoNothing,
            StoredMySqlConflictAction::DoUpdate => Self::DoUpdate,
        }
    }
}

impl From<MongoDbConflictAction> for StoredMongoDbConflictAction {
    fn from(value: MongoDbConflictAction) -> Self {
        match value {
            MongoDbConflictAction::None => Self::None,
            MongoDbConflictAction::DoNothing { target } => Self::DoNothing { target },
            MongoDbConflictAction::DoUpdate { target } => Self::DoUpdate { target },
        }
    }
}

impl From<StoredMongoDbConflictAction> for MongoDbConflictAction {
    fn from(value: StoredMongoDbConflictAction) -> Self {
        match value {
            StoredMongoDbConflictAction::None => Self::None,
            StoredMongoDbConflictAction::DoNothing { target } => Self::DoNothing { target },
            StoredMongoDbConflictAction::DoUpdate { target } => Self::DoUpdate { target },
        }
    }
}

impl From<EmitSink> for StoredEmitSink {
    fn from(value: EmitSink) -> Self {
        match value {
            EmitSink::Kafka { client, topic } => Self::Kafka {
                client: client.to_string(),
                topic: topic.to_string(),
            },
            EmitSink::Pulsar { client, topic } => Self::Pulsar {
                client: client.to_string(),
                topic: topic.to_string(),
            },
            EmitSink::Kinesis { client, relay } => Self::Kinesis {
                client: client.to_string(),
                relay: relay.to_string(),
            },
            EmitSink::RabbitMq { client, queue } => Self::RabbitMq {
                client: client.to_string(),
                queue: queue.to_string(),
            },
            EmitSink::Redis { client, channel } => Self::Redis {
                client: client.to_string(),
                channel: channel.to_string(),
            },
            EmitSink::Mqtt { client, topic } => Self::Mqtt {
                client: client.to_string(),
                topic: topic.to_string(),
            },
            EmitSink::Nats { client, subject } => Self::Nats {
                client: client.to_string(),
                subject: subject.to_string(),
            },
            EmitSink::ZeroMq { client } => Self::ZeroMq {
                client: client.to_string(),
            },
            EmitSink::Sqs { client, queue } => Self::Sqs {
                client: client.to_string(),
                queue: queue.to_string(),
            },
            EmitSink::ClickHouse {
                client,
                table,
                values,
                flush_each,
            } => Self::ClickHouse {
                client: client.to_string(),
                table: table.to_string(),
                values: values.into_iter().map(Into::into).collect(),
                flush_each,
            },
            EmitSink::Postgres {
                client,
                table,
                values,
                conflict_action,
                max_batch,
                flush_each,
            } => Self::Postgres {
                client: client.to_string(),
                table: table.to_string(),
                values: values.into_iter().map(Into::into).collect(),
                conflict_action: conflict_action.into(),
                max_batch,
                flush_each,
            },
            EmitSink::MySql {
                client,
                table,
                values,
                conflict_action,
                max_batch,
                flush_each,
            } => Self::MySql {
                client: client.to_string(),
                table: table.to_string(),
                values: values.into_iter().map(Into::into).collect(),
                conflict_action: conflict_action.into(),
                max_batch,
                flush_each,
            },
            EmitSink::MongoDb {
                client,
                collection,
                values,
                conflict_action,
                max_batch,
                flush_each,
            } => Self::MongoDb {
                client: client.to_string(),
                collection: collection.to_string(),
                values: values.into_iter().map(Into::into).collect(),
                conflict_action: conflict_action.into(),
                max_batch,
                flush_each,
            },
            EmitSink::Iceberg {
                backend,
                client,
                table,
                values,
                location,
                catalog,
                flush_each,
                max_batch_size,
                commit_each,
                max_commit_size,
            } => Self::Iceberg {
                backend: backend.into(),
                client: client.to_string(),
                table: table.to_string(),
                values: values.into_iter().map(Into::into).collect(),
                location,
                catalog: catalog.into(),
                flush_each,
                max_batch_size,
                commit_each,
                max_commit_size,
            },
        }
    }
}

impl TryFrom<StoredEmitSink> for EmitSink {
    type Error = Report<NameError>;

    fn try_from(value: StoredEmitSink) -> Result<Self, Self::Error> {
        match value {
            StoredEmitSink::Kafka { client, topic } => Ok(Self::Kafka {
                client: Identifier::parse(&client)?,
                topic: Identifier::parse(&topic)?,
            }),
            StoredEmitSink::Pulsar { client, topic } => Ok(Self::Pulsar {
                client: Identifier::parse(&client)?,
                topic: Identifier::parse(&topic)?,
            }),
            StoredEmitSink::Kinesis { client, relay } => Ok(Self::Kinesis {
                client: Identifier::parse(&client)?,
                relay: Identifier::parse(&relay)?,
            }),
            StoredEmitSink::RabbitMq { client, queue } => Ok(Self::RabbitMq {
                client: Identifier::parse(&client)?,
                queue: Identifier::parse(&queue)?,
            }),
            StoredEmitSink::Redis { client, channel } => Ok(Self::Redis {
                client: Identifier::parse(&client)?,
                channel: Identifier::parse(&channel)?,
            }),
            StoredEmitSink::Mqtt { client, topic } => Ok(Self::Mqtt {
                client: Identifier::parse(&client)?,
                topic: Identifier::parse(&topic)?,
            }),
            StoredEmitSink::Nats { client, subject } => Ok(Self::Nats {
                client: Identifier::parse(&client)?,
                subject: Identifier::parse(&subject)?,
            }),
            StoredEmitSink::ZeroMq { client } => Ok(Self::ZeroMq {
                client: Identifier::parse(&client)?,
            }),
            StoredEmitSink::Sqs { client, queue } => Ok(Self::Sqs {
                client: Identifier::parse(&client)?,
                queue: Identifier::parse(&queue)?,
            }),
            StoredEmitSink::ClickHouse {
                client,
                table,
                values,
                flush_each,
            } => Ok(Self::ClickHouse {
                client: Identifier::parse(&client)?,
                table: Identifier::parse(&table)?,
                values: values.into_iter().map(Into::into).collect(),
                flush_each,
            }),
            StoredEmitSink::Postgres {
                client,
                table,
                values,
                conflict_action,
                max_batch,
                flush_each,
            } => Ok(Self::Postgres {
                client: Identifier::parse(&client)?,
                table: Identifier::parse(&table)?,
                values: values.into_iter().map(Into::into).collect(),
                conflict_action: conflict_action.into(),
                max_batch,
                flush_each,
            }),
            StoredEmitSink::MySql {
                client,
                table,
                values,
                conflict_action,
                max_batch,
                flush_each,
            } => Ok(Self::MySql {
                client: Identifier::parse(&client)?,
                table: Identifier::parse(&table)?,
                values: values.into_iter().map(Into::into).collect(),
                conflict_action: conflict_action.into(),
                max_batch,
                flush_each,
            }),
            StoredEmitSink::MongoDb {
                client,
                collection,
                values,
                conflict_action,
                max_batch,
                flush_each,
            } => Ok(Self::MongoDb {
                client: Identifier::parse(&client)?,
                collection: Identifier::parse(&collection)?,
                values: values.into_iter().map(Into::into).collect(),
                conflict_action: conflict_action.into(),
                max_batch,
                flush_each,
            }),
            StoredEmitSink::Iceberg {
                backend,
                client,
                table,
                values,
                location,
                catalog,
                flush_each,
                max_batch_size,
                commit_each,
                max_commit_size,
            } => Ok(Self::Iceberg {
                backend: backend.into(),
                client: Identifier::parse(&client)?,
                table: Identifier::parse(&table)?,
                values: values.into_iter().map(Into::into).collect(),
                location,
                catalog: catalog.into(),
                flush_each,
                max_batch_size,
                commit_each,
                max_commit_size,
            }),
        }
    }
}

impl From<IcebergCatalog> for StoredIcebergCatalog {
    fn from(value: IcebergCatalog) -> Self {
        match value {
            IcebergCatalog::Rest { client } => Self::Rest {
                client: client.to_string(),
            },
        }
    }
}

impl From<StoredIcebergCatalog> for IcebergCatalog {
    fn from(value: StoredIcebergCatalog) -> Self {
        match value {
            StoredIcebergCatalog::Rest { client } => Self::Rest {
                client: Identifier::parse(&client)
                    .expect("stored Iceberg REST catalog client must be a valid identifier"),
            },
        }
    }
}

impl From<IcebergStorageBackend> for StoredIcebergStorageBackend {
    fn from(value: IcebergStorageBackend) -> Self {
        match value {
            IcebergStorageBackend::S3 => Self::S3,
            IcebergStorageBackend::Gcs => Self::Gcs,
            IcebergStorageBackend::AzureBlob => Self::AzureBlob,
        }
    }
}

impl From<StoredIcebergStorageBackend> for IcebergStorageBackend {
    fn from(value: StoredIcebergStorageBackend) -> Self {
        match value {
            StoredIcebergStorageBackend::S3 => Self::S3,
            StoredIcebergStorageBackend::Gcs => Self::Gcs,
            StoredIcebergStorageBackend::AzureBlob => Self::AzureBlob,
        }
    }
}

impl From<ClickHouseValueMapping> for StoredClickHouseValueMapping {
    fn from(value: ClickHouseValueMapping) -> Self {
        Self {
            column: value.column,
            expression: value.expression,
        }
    }
}

impl From<StoredClickHouseValueMapping> for ClickHouseValueMapping {
    fn from(value: StoredClickHouseValueMapping) -> Self {
        Self {
            column: value.column,
            expression: value.expression,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identifier(raw: &str) -> Identifier {
        Identifier::parse(raw).expect("valid identifier")
    }

    fn parameterized_by(schema: &str, relay: &str, fields: &[&str]) -> BranchParameterization {
        BranchParameterization::parameterized(
            identifier(schema),
            fields
                .iter()
                .map(|field| ParameterValueMapping {
                    field: identifier(field),
                    relay: identifier(relay),
                    relay_field: identifier(field),
                })
                .collect(),
        )
    }

    #[test]
    fn stored_model_envelope_roundtrips_multiple_model_variants() {
        let models = vec![
            Model::Schema(CreateSchema {
                name: identifier("events"),
                fields: vec![SchemaField {
                    name: identifier("user_id"),
                    ty: ParseAsType::U32,
                    optional: false,
                    sensitive: false,
                }],
            }),
            Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                name: identifier("events_json"),
                fields: vec![WireSchemaField {
                    name: identifier("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                }],
            })),
            Model::ClientHttp(CreateClientHttp {
                name: identifier("http_client"),
                mount: None,
                config: vec![
                    StoredClientConfigEntry {
                        key: "endpoint".to_string(),
                        value: "https://example.com/api".to_string(),
                    }
                    .into(),
                ],
            }),
            Model::Endpoint(CreateEndpoint {
                name: identifier("events_http"),
                on_vhost: identifier("public"),
                path: "/ingest".to_string(),
                endpoint_type: EndpointType::Http,
                signaling_protocol: None,
            }),
            Model::SignalingProtocol(CreateSignalingProtocol {
                name: identifier("binance_ws"),
                on_connect: SignalingProtocolOnConnect {
                    send_bodies: vec![r#"{"method":"SUBSCRIBE","id":1}"#.to_string()],
                    wait_bodies: vec![r#"{"id":1,"result":null}"#.to_string()],
                    timeout: "5s".to_string(),
                },
            }),
            Model::Ingestor(CreateIngestor {
                name: identifier("events_ingestor"),
                output_routes: ProcessorOutputs::single(identifier("events_stream")),
                decode_using_codec: identifier("events_codec"),
                parameterized_by: parameterized_by("events", "events_stream", &["tenant"]),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                timestamp_source: None,
                source: IngestSource::Http {
                    client: identifier("http_client"),
                    every: "5s".to_string(),
                },
                error_policies: ErrorPolicies::handled_by_log(),

                filter_where: None,
            }),
            Model::Unifier(CreateUnifier {
                name: identifier("events_unifier"),
                from_relays: vec![identifier("events_a"), identifier("events_b")],
                output_routes: ProcessorOutputs::single(identifier("events_stream")),
                parameterized_by: parameterized_by("events", "events_stream", &["tenant"]),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Attached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            }),
            Model::Reingestor(CreateReingestor {
                name: identifier("events_splitter"),
                from_relay: identifier("events_stream"),
                output_routes: ProcessorOutputs::new(vec![
                    ProcessorOutput {
                        relay: identifier("events_errors"),
                        filter_map: Some(
                            r#"SET severity = lower(level) WHERE level = "error""#.to_string(),
                        ),
                    },
                    ProcessorOutput::new(identifier("events_other")),
                ]),
                parameterized_by: parameterized_by("events", "events_stream", &["tenant"]),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Attached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            }),
            Model::Reingestor(CreateReingestor {
                name: identifier("events_forwarder"),
                from_relay: identifier("events_stream"),
                output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                    relay: identifier("events_projected"),
                    filter_map: Some(
                        "SET normalized = lower(raw) UNSET raw WHERE active".to_string(),
                    ),
                }]),
                parameterized_by: parameterized_by("events", "events_stream", &["tenant"]),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Detached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: Some("WHERE tenant = \"acme\"".to_string()),
            }),
            Model::WindowProcessor(CreateWindowProcessor {
                name: identifier("events_window"),
                from_relay: identifier("events_stream"),
                output_routes: ProcessorOutputs::single(identifier("events_summary")),
                parameterized_by: parameterized_by("events", "events_stream", &["tenant"]),
                width: WindowBound {
                    messages: Some(100),
                    duration: Some("10s".to_string()),
                },
                step: WindowBound {
                    messages: Some(10),
                    duration: Some("1s".to_string()),
                },
                aggregate: "events_summary.count = COUNT(events_stream.id)".to_string(),
                mode: AckMode::Attached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            }),
            Model::Emitter(CreateEmitter {
                name: identifier("events_emitter"),
                from_relay: identifier("events_stream"),
                encode_using_codec: Some(identifier("events_codec")),
                sink: EmitSink::Nats {
                    client: identifier("nats_client"),
                    subject: identifier("events_subject"),
                },
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Detached,
                error_policies: ErrorPolicies::handled_by_log(),

                filter_map: None,
            }),
        ];

        for model in models {
            let stored = StoredModelVersioned::from(model.clone());
            let roundtrip = Model::try_from(stored).expect("stored model should roundtrip");
            assert_eq!(roundtrip, model);
        }
    }

    #[test]
    fn stored_model_roundtrip_preserves_optional_schema_fields() {
        let models = vec![
            Model::Schema(CreateSchema {
                name: identifier("events"),
                fields: vec![
                    SchemaField {
                        name: identifier("user_id"),
                        ty: ParseAsType::U32,
                        optional: false,
                        sensitive: false,
                    },
                    SchemaField {
                        name: identifier("nickname"),
                        ty: ParseAsType::String,
                        optional: true,
                        sensitive: false,
                    },
                ],
            }),
            Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                name: identifier("events_json"),
                fields: vec![
                    WireSchemaField {
                        name: identifier("user_id"),
                        ty: JsonType::Integer,
                        optional: false,
                    },
                    WireSchemaField {
                        name: identifier("nickname"),
                        ty: JsonType::String,
                        optional: true,
                    },
                ],
            })),
        ];

        for model in models {
            let stored = StoredModelVersioned::from(model.clone());
            let roundtrip = Model::try_from(stored).expect("stored model should roundtrip");
            assert_eq!(roundtrip, model);
        }
    }

    #[test]
    fn stored_model_try_from_rejects_invalid_identifiers() {
        let err = Model::try_from(StoredModelVersioned::Emitter(StoredCreateEmitter {
            name: "events_emitter".to_string(),
            from_relay: "events_stream".to_string(),
            encode_using_codec: Some("events_codec".to_string()),
            sink: StoredEmitSink::Kafka {
                client: "bad client".to_string(),
                topic: "events_topic".to_string(),
            },
            flush_each: "100ms".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            mode: AckMode::Attached,
            error_policies: StoredErrorPolicies {
                message: StoredMessageErrorPolicy::Log,
                general: StoredGeneralErrorPolicy::Log,
            },
            filter_map: None,
        }))
        .expect_err("invalid identifiers must fail");

        assert!(matches!(
            err.current_context(),
            NameError::InvalidChar { ch: ' ' }
        ));
    }

    #[test]
    fn stored_type_enums_roundtrip_to_runtime_enums() {
        assert_eq!(JsonType::from(StoredJsonType::Boolean), JsonType::Boolean);
        assert_eq!(StoredJsonType::from(JsonType::Array), StoredJsonType::Array);
        assert_eq!(AvroType::from(StoredAvroType::Double), AvroType::Double);
        assert_eq!(StoredAvroType::from(AvroType::Fixed), StoredAvroType::Fixed);
        assert_eq!(
            ParseAsType::from(StoredParseAsType::Datetime),
            ParseAsType::Datetime
        );
        assert_eq!(
            StoredParseAsType::from(ParseAsType::F32),
            StoredParseAsType::F32
        );
        assert_eq!(
            EndpointType::from(StoredEndpointType::Websockets),
            EndpointType::Websockets
        );
        assert_eq!(
            StoredEndpointType::from(EndpointType::Http),
            StoredEndpointType::Http
        );
    }
}
