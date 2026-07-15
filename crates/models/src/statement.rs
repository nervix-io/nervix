use std::ops::{Deref, DerefMut};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter, EnumProperty, EnumString, IntoEnumIterator, IntoStaticStr};
use thiserror::Error;

use crate::{CreateSchema, CreateWireSchemaStmt, Domain, Identifier, ParseAsType, Timestamp};

pub type DomainId = Domain;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Statement {
    CreateDomain(CreateStatement<CreateDomain>),
    CreateUser(CreateStatement<CreateUser>),
    CreateResource(CreateStatement<CreateResource>),
    UploadResource(UploadResource),
    StartDomain(StartDomain),
    StopDomain(StopDomain),
    Create(CreateStatement<Box<Model>>),
    AlterRelay(AlterRelay),
    Drop(DropModel),
    DropNode(DropNode),
    CordonNode(CordonNode),
    UncordonNode(UncordonNode),
    DrainNode(DrainNode),
    DescribeRelay(DescribeRelay),
    DescribeDomain(DescribeDomain),
    DescribeIngestor(DescribeIngestor),
    DescribeResource(DescribeResource),
    DescribeLookup(DescribeLookup),
    DescribeEndpoint(DescribeEndpoint),
    DescribeDeduplicator(DescribeDeduplicator),
    DescribeReingestor(DescribeReingestor),
    DescribeCorrelator(DescribeCorrelator),
    DescribeReorderer(DescribeReorderer),
    DescribeEmitter(DescribeEmitter),
    DescribeWindowProcessor(DescribeWindowProcessor),
    DescribeWasmProcessor(DescribeWasmProcessor),
    LookupQuery(LookupQuery),
    ShowCreate(ShowCreate),
    ShowRelayMaterializedState(ShowRelayMaterializedState),
    ShowClusterStatus(ShowClusterStatus),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStatement<T> {
    #[serde(default)]
    pub if_not_exists: bool,
    pub body: T,
}

impl<T> CreateStatement<T> {
    pub fn new(body: T, if_not_exists: bool) -> Self {
        Self {
            if_not_exists,
            body,
        }
    }

    pub fn map_body<U>(self, map: impl FnOnce(T) -> U) -> CreateStatement<U> {
        CreateStatement {
            if_not_exists: self.if_not_exists,
            body: map(self.body),
        }
    }
}

impl<T> AsRef<T> for CreateStatement<T> {
    fn as_ref(&self) -> &T {
        &self.body
    }
}

impl<T> AsMut<T> for CreateStatement<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.body
    }
}

impl<T> Deref for CreateStatement<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

impl<T> DerefMut for CreateStatement<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.body
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    EnumString,
    EnumIter,
    EnumProperty,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum ModelKind {
    #[strum(props(completion_label = "ref:schema"))]
    Schema,
    #[strum(props(completion_label = "ref:wire_schema"))]
    WireSchema,
    #[strum(props(completion_label = "ref:codec"))]
    Codec,
    #[strum(props(completion_label = "ref:client"))]
    Client,
    #[strum(props(completion_label = "ref:vhost"))]
    Vhost,
    #[strum(props(completion_label = "ref:branch"))]
    Branch,
    #[strum(props(completion_label = "ref:endpoint"))]
    Endpoint,
    #[strum(props(completion_label = "ref:signaling_protocol"))]
    SignalingProtocol,
    #[strum(props(completion_label = "ref:generator"))]
    Generator,
    #[strum(props(completion_label = "ref:inferencer"))]
    Inferencer,
    #[strum(props(completion_label = "ref:wasm_processor"))]
    WasmProcessor,
    #[strum(props(completion_label = "ref:ingestor"))]
    Ingestor,
    #[strum(props(completion_label = "ref:reingestor"))]
    Reingestor,
    #[strum(props(completion_label = "ref:relay"))]
    Relay,
    #[strum(props(completion_label = "ref:materializer"))]
    Materializer,
    #[strum(props(completion_label = "ref:lookup"))]
    Lookup,
    #[strum(props(completion_label = "ref:junction"))]
    Junction,
    #[strum(props(completion_label = "ref:deduplicator"))]
    Deduplicator,
    #[strum(props(completion_label = "ref:correlator"))]
    Correlator,
    #[strum(props(completion_label = "ref:reorderer"))]
    Reorderer,
    #[strum(props(completion_label = "ref:window_processor"))]
    WindowProcessor,
    #[strum(props(completion_label = "ref:emitter"))]
    Emitter,
}

impl ModelKind {
    pub fn completion_label(self) -> &'static str {
        self.get_str("completion_label")
            .expect("every model kind must define a completion_label")
    }

    pub fn from_completion_label(label: &str) -> Option<Self> {
        Self::iter().find(|kind| kind.completion_label() == label)
    }

    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShowCreate {
    pub kind: ModelKind,
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShowClusterStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShowRelayMaterializedState {
    pub relay: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateDomain {
    pub id: DomainId,
    pub config: DomainConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateUser {
    pub name: Identifier,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateResource {
    pub identifier: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadResource {
    pub identifier: Identifier,
    pub source_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartDomain {
    pub start: DomainStartPoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StopDomain;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainConfig {
    pub pace: DomainPace,
    pub period: String,
    pub skew: String,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE", ascii_case_insensitive)]
pub enum DomainPace {
    Paced,
    Unpaced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DomainStartPoint {
    #[default]
    Resume,
    Now {
        time_rate: String,
    },
    At {
        timestamp: String,
        time_rate: String,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, AsRefStr, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE", ascii_case_insensitive)]
pub enum DomainStatus {
    Stopped,
    Running,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainTick {
    pub tick_id: u64,
    pub logical_timestamp: Timestamp,
    pub wall_clock: Timestamp,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainState {
    pub id: DomainId,
    pub config: DomainConfig,
    pub status: DomainStatus,
    #[serde(default)]
    pub start_version: u64,
    #[serde(default)]
    pub last_start: DomainStartPoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropModel {
    pub kind: ModelKind,
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropNode {
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CordonNode {
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UncordonNode {
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainNode {
    pub node_id: String,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE", ascii_case_insensitive)]
pub enum SubscriptionDeliveryBehavior {
    Blocking,
    Dropping,
}

fn default_subscription_delivery_behavior() -> SubscriptionDeliveryBehavior {
    SubscriptionDeliveryBehavior::Blocking
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateSubscription {
    pub name: Identifier,
    pub relay: Identifier,
    #[serde(default = "default_subscription_delivery_behavior")]
    pub delivery_behavior: SubscriptionDeliveryBehavior,
    #[serde(default)]
    pub batch_sample_rate: Option<String>,
    pub filter_map: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubscription {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeRelay {
    pub relay: Identifier,
    pub bindings: Vec<SubscriptionBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DescribeDomain;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeIngestor {
    pub ingestor: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeResource {
    pub identifier: Identifier,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeLookup {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeDeduplicator {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeReingestor {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeCorrelator {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeEndpoint {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeReorderer {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeEmitter {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeWindowProcessor {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeWasmProcessor {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupQuery {
    pub name: Identifier,
    pub key: SubscriptionLiteral,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SubscriptionBinding {
    pub field: Identifier,
    pub value: SubscriptionLiteral,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum SubscriptionLiteral {
    String(String),
    Number(String),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Model {
    Schema(CreateSchema),
    WireSchema(CreateWireSchemaStmt),
    Codec(CreateCodec),
    ClientKafka(CreateClientKafka),
    ClientPulsar(CreateClientPulsar),
    ClientKinesis(CreateClientKinesis),
    ClientHttp(CreateClientHttp),
    ClientPrometheus(CreateClientPrometheus),
    ClientMqtt(CreateClientMqtt),
    ClientNats(CreateClientNats),
    ClientRabbitMq(CreateClientRabbitMq),
    ClientRedis(CreateClientRedis),
    ClientZeroMq(CreateClientZeroMq),
    ClientSqs(CreateClientSqs),
    ClientWebsockets(CreateClientWebsockets),
    ClientClickHouse(CreateClientClickHouse),
    ClientPostgres(CreateClientPostgres),
    ClientMySql(CreateClientMySql),
    ClientMongoDb(CreateClientMongoDb),
    ClientS3(CreateClientS3),
    ClientGcs(CreateClientGcs),
    ClientAzureBlob(CreateClientAzureBlob),
    ClientIcebergRest(CreateClientIcebergRest),
    Vhost(CreateVhost),
    Branch(CreateBranch),
    Endpoint(CreateEndpoint),
    SignalingProtocol(CreateSignalingProtocol),
    Generator(CreateGenerator),
    Inferencer(CreateInferencer),
    WasmProcessor(CreateWasmProcessor),
    Ingestor(CreateIngestor),
    Reingestor(CreateReingestor),
    Relay(CreateRelay),
    Materializer(CreateMaterializer),
    Lookup(CreateLookup),
    Junction(CreateJunction),
    Deduplicator(CreateDeduplicator),
    Correlator(CreateCorrelator),
    Reorderer(CreateReorderer),
    WindowProcessor(CreateWindowProcessor),
    Emitter(CreateEmitter),
}

impl Model {
    pub fn kind(&self) -> ModelKind {
        match self {
            Self::Schema(_) => ModelKind::Schema,
            Self::WireSchema(_) => ModelKind::WireSchema,
            Self::Codec(_) => ModelKind::Codec,
            Self::ClientKafka(_)
            | Self::ClientPulsar(_)
            | Self::ClientKinesis(_)
            | Self::ClientHttp(_)
            | Self::ClientPrometheus(_)
            | Self::ClientMqtt(_)
            | Self::ClientNats(_)
            | Self::ClientRabbitMq(_)
            | Self::ClientRedis(_)
            | Self::ClientZeroMq(_)
            | Self::ClientSqs(_)
            | Self::ClientWebsockets(_)
            | Self::ClientClickHouse(_)
            | Self::ClientPostgres(_)
            | Self::ClientMySql(_)
            | Self::ClientMongoDb(_)
            | Self::ClientS3(_)
            | Self::ClientGcs(_)
            | Self::ClientAzureBlob(_)
            | Self::ClientIcebergRest(_) => ModelKind::Client,
            Self::Vhost(_) => ModelKind::Vhost,
            Self::Branch(_) => ModelKind::Branch,
            Self::Endpoint(_) => ModelKind::Endpoint,
            Self::SignalingProtocol(_) => ModelKind::SignalingProtocol,
            Self::Generator(_) => ModelKind::Generator,
            Self::Inferencer(_) => ModelKind::Inferencer,
            Self::WasmProcessor(_) => ModelKind::WasmProcessor,
            Self::Ingestor(_) => ModelKind::Ingestor,
            Self::Reingestor(_) => ModelKind::Reingestor,
            Self::Relay(_) => ModelKind::Relay,
            Self::Materializer(_) => ModelKind::Materializer,
            Self::Lookup(_) => ModelKind::Lookup,
            Self::Junction(_) => ModelKind::Junction,
            Self::Deduplicator(_) => ModelKind::Deduplicator,
            Self::Correlator(_) => ModelKind::Correlator,
            Self::Reorderer(_) => ModelKind::Reorderer,
            Self::WindowProcessor(_) => ModelKind::WindowProcessor,
            Self::Emitter(_) => ModelKind::Emitter,
        }
    }

    pub fn identifier(&self) -> &Identifier {
        match self {
            Self::Schema(v) => &v.name,
            Self::WireSchema(v) => match v {
                CreateWireSchemaStmt::Json(v) => &v.name,
                CreateWireSchemaStmt::Cbor(v) => &v.name,
                CreateWireSchemaStmt::Avro(v) => &v.name,
            },
            Self::Codec(v) => &v.name,
            Self::ClientKafka(v) => &v.name,
            Self::ClientPulsar(v) => &v.name,
            Self::ClientKinesis(v) => &v.name,
            Self::ClientHttp(v) => &v.name,
            Self::ClientPrometheus(v) => &v.name,
            Self::ClientMqtt(v) => &v.name,
            Self::ClientNats(v) => &v.name,
            Self::ClientRabbitMq(v) => &v.name,
            Self::ClientRedis(v) => &v.name,
            Self::ClientZeroMq(v) => &v.name,
            Self::ClientSqs(v) => &v.name,
            Self::ClientWebsockets(v) => &v.name,
            Self::ClientClickHouse(v) => &v.name,
            Self::ClientPostgres(v) => &v.name,
            Self::ClientMySql(v) => &v.name,
            Self::ClientMongoDb(v) => &v.name,
            Self::ClientS3(v) => &v.name,
            Self::ClientGcs(v) => &v.name,
            Self::ClientAzureBlob(v) => &v.name,
            Self::ClientIcebergRest(v) => &v.name,
            Self::Vhost(v) => &v.name,
            Self::Branch(v) => &v.name,
            Self::Endpoint(v) => &v.name,
            Self::SignalingProtocol(v) => &v.name,
            Self::Generator(v) => &v.name,
            Self::Inferencer(v) => &v.name,
            Self::WasmProcessor(v) => &v.name,
            Self::Ingestor(v) => &v.name,
            Self::Reingestor(v) => &v.name,
            Self::Relay(v) => &v.name,
            Self::Materializer(v) => &v.relay,
            Self::Lookup(v) => &v.name,
            Self::Junction(v) => &v.name,
            Self::Deduplicator(v) => &v.name,
            Self::Correlator(v) => &v.name,
            Self::Reorderer(v) => &v.name,
            Self::WindowProcessor(v) => &v.name,
            Self::Emitter(v) => &v.name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCodec {
    pub name: Identifier,
    pub wire_format: CodecWireFormat,
    pub wire_schema: Option<Identifier>,
    pub schema: Identifier,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub encoding_rules: Vec<CodecEncodingRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CodecJaqTransformations {
    pub on_ingestion: Option<String>,
    pub on_emitting: Option<String>,
}

impl CodecJaqTransformations {
    pub fn has_any(&self) -> bool {
        self.on_ingestion.is_some() || self.on_emitting.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum CodecJaqFormat {
    Json,
    Yaml,
    Toml,
    Xml,
    Cbor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CodecWireFormat {
    Json,
    Cbor,
    Avro,
    JaqNative {
        format: CodecJaqFormat,
        transformations: CodecJaqTransformations,
    },
    Protobuf(CodecProtobufConfig),
}

impl CodecWireFormat {
    pub fn supports_decoding(&self) -> bool {
        match self {
            Self::Json | Self::Cbor | Self::Avro => true,
            Self::JaqNative {
                transformations, ..
            }
            | Self::Protobuf(CodecProtobufConfig {
                transformations, ..
            }) => transformations.on_ingestion.is_some(),
        }
    }

    pub fn supports_encoding(&self) -> bool {
        match self {
            Self::Json | Self::Cbor | Self::Avro => true,
            Self::JaqNative {
                transformations, ..
            }
            | Self::Protobuf(CodecProtobufConfig {
                transformations, ..
            }) => transformations.on_emitting.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodecProtobufConfig {
    pub resource: Identifier,
    pub resource_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<ClientConfigEntry>,
    pub message: String,
    pub transformations: CodecJaqTransformations,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodecEncodingRule {
    pub field: Identifier,
    pub encoding: CodecEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CodecEncoding {
    Rfc3339,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateEmitter {
    pub name: Identifier,
    pub from_relay: Identifier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encode_using_codec: Option<Identifier>,
    pub sink: EmitSink,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub error_policies: ErrorPolicies,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_map: Option<String>,
}

impl CreateEmitter {
    pub fn flush_policy(&self) -> (&str, Option<&str>) {
        (self.flush_each.as_str(), self.max_batch_size.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateGenerator {
    pub name: Identifier,
    pub into_relay: Identifier,
    pub branched_by: BranchSelection,
    pub each: String,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub set: String,
    pub message_error_policy: MessageErrorPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorPolicies {
    pub message: MessageErrorPolicy,
    pub general: GeneralErrorPolicy,
}

impl ErrorPolicies {
    pub const fn handled_by_log() -> Self {
        Self {
            message: MessageErrorPolicy::Log,
            general: GeneralErrorPolicy::Log,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageErrorPolicy {
    Ignore,
    Log,
    Dlq {
        relay: Identifier,
        mappings: Vec<ErrorFieldMapping>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeneralErrorPolicy {
    Ignore,
    Log,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorFieldMapping {
    pub field: Identifier,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum EmitSink {
    Kafka {
        client: Identifier,
        topic: Identifier,
    },
    Pulsar {
        client: Identifier,
        topic: Identifier,
    },
    Kinesis {
        client: Identifier,
        relay: Identifier,
    },
    #[strum(serialize = "RABBITMQ")]
    RabbitMq {
        client: Identifier,
        queue: Identifier,
    },
    Redis {
        client: Identifier,
        channel: Identifier,
    },
    Mqtt {
        client: Identifier,
        topic: Identifier,
    },
    Nats {
        client: Identifier,
        subject: Identifier,
    },
    #[strum(serialize = "ZEROMQ")]
    ZeroMq { client: Identifier },
    Sqs {
        client: Identifier,
        queue: Identifier,
    },
    ClickHouse {
        client: Identifier,
        table: Identifier,
        values: Vec<ClickHouseValueMapping>,
        flush_each: String,
    },
    Postgres {
        client: Identifier,
        table: Identifier,
        values: Vec<PostgresValueMapping>,
        conflict_action: PostgresConflictAction,
        max_batch: u64,
        flush_each: String,
    },
    MySql {
        client: Identifier,
        table: Identifier,
        values: Vec<MySqlValueMapping>,
        conflict_action: MySqlConflictAction,
        max_batch: u64,
        flush_each: String,
    },
    MongoDb {
        client: Identifier,
        collection: Identifier,
        values: Vec<MongoDbValueMapping>,
        conflict_action: MongoDbConflictAction,
        max_batch: u64,
        flush_each: String,
    },
    Iceberg {
        backend: IcebergStorageBackend,
        client: Identifier,
        table: Identifier,
        values: Vec<IcebergValueMapping>,
        location: String,
        catalog: IcebergCatalog,
        flush_each: String,
        max_batch_size: Option<String>,
        commit_each: String,
        max_commit_size: String,
    },
}

impl EmitSink {
    pub fn transport_label(&self) -> &str {
        self.as_ref()
    }

    pub fn client(&self) -> &Identifier {
        match self {
            Self::Kafka { client, .. }
            | Self::Pulsar { client, .. }
            | Self::Kinesis { client, .. }
            | Self::RabbitMq { client, .. }
            | Self::Redis { client, .. }
            | Self::Mqtt { client, .. }
            | Self::Nats { client, .. }
            | Self::ZeroMq { client }
            | Self::Sqs { client, .. }
            | Self::ClickHouse { client, .. }
            | Self::Postgres { client, .. }
            | Self::MySql { client, .. }
            | Self::MongoDb { client, .. }
            | Self::Iceberg { client, .. } => client,
        }
    }

    pub fn iceberg_catalog_client(&self) -> Option<&Identifier> {
        if let Self::Iceberg {
            catalog: IcebergCatalog::Rest { client },
            ..
        } = self
        {
            Some(client)
        } else {
            None
        }
    }

    pub fn requires_codec(&self) -> bool {
        match self {
            Self::Kafka { .. }
            | Self::Pulsar { .. }
            | Self::Kinesis { .. }
            | Self::RabbitMq { .. }
            | Self::Redis { .. }
            | Self::Mqtt { .. }
            | Self::Nats { .. }
            | Self::ZeroMq { .. }
            | Self::Sqs { .. } => true,
            Self::ClickHouse { .. }
            | Self::Postgres { .. }
            | Self::MySql { .. }
            | Self::MongoDb { .. }
            | Self::Iceberg { .. } => false,
        }
    }

    pub fn flush_policy(&self) -> Option<(&str, Option<&str>)> {
        match self {
            Self::ClickHouse { flush_each, .. }
            | Self::Postgres { flush_each, .. }
            | Self::MySql { flush_each, .. }
            | Self::MongoDb { flush_each, .. } => Some((flush_each.as_str(), None)),
            Self::Iceberg {
                flush_each,
                max_batch_size,
                ..
            } => Some((flush_each.as_str(), max_batch_size.as_deref())),
            Self::Kafka { .. }
            | Self::Pulsar { .. }
            | Self::Kinesis { .. }
            | Self::RabbitMq { .. }
            | Self::Redis { .. }
            | Self::Mqtt { .. }
            | Self::Nats { .. }
            | Self::ZeroMq { .. }
            | Self::Sqs { .. } => None,
        }
    }

    pub fn commit_policy(&self) -> Option<(&str, &str)> {
        match self {
            Self::Iceberg {
                commit_each,
                max_commit_size,
                ..
            } => Some((commit_each.as_str(), max_commit_size.as_str())),
            Self::Kafka { .. }
            | Self::Pulsar { .. }
            | Self::Kinesis { .. }
            | Self::RabbitMq { .. }
            | Self::Redis { .. }
            | Self::Mqtt { .. }
            | Self::Nats { .. }
            | Self::ZeroMq { .. }
            | Self::Sqs { .. }
            | Self::ClickHouse { .. }
            | Self::Postgres { .. }
            | Self::MySql { .. }
            | Self::MongoDb { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseValueMapping {
    pub column: String,
    pub expression: String,
}

pub type PostgresValueMapping = ClickHouseValueMapping;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostgresConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

pub type MySqlValueMapping = ClickHouseValueMapping;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MySqlConflictAction {
    None,
    DoNothing,
    DoUpdate,
}

pub type MongoDbValueMapping = ClickHouseValueMapping;
pub type IcebergValueMapping = ClickHouseValueMapping;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcebergCatalog {
    Rest { client: Identifier },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergStorageBackend {
    S3,
    #[strum(serialize = "GCS")]
    Gcs,
    #[strum(serialize = "AZURE_BLOB")]
    AzureBlob,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MongoDbConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientKafka {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientPulsar {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientKinesis {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientHttp {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientPrometheus {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientMqtt {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientNats {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientRabbitMq {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientRedis {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientZeroMq {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientSqs {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientWebsockets {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub signaling_protocol: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientClickHouse {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientPostgres {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientMySql {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientMongoDb {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientS3 {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientGcs {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientAzureBlob {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientIcebergRest {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientConfigEntry {
    pub key: String,
    pub value: String,
}

pub type KafkaConfigEntry = ClientConfigEntry;
pub type PulsarConfigEntry = ClientConfigEntry;
pub type KinesisConfigEntry = ClientConfigEntry;
pub type HttpConfigEntry = ClientConfigEntry;
pub type RabbitMqConfigEntry = ClientConfigEntry;
pub type RedisConfigEntry = ClientConfigEntry;
pub type MqttConfigEntry = ClientConfigEntry;
pub type NatsConfigEntry = ClientConfigEntry;
pub type PrometheusConfigEntry = ClientConfigEntry;
pub type ZeroMqConfigEntry = ClientConfigEntry;
pub type SqsConfigEntry = ClientConfigEntry;
pub type WebsocketsConfigEntry = ClientConfigEntry;
pub type ClickHouseConfigEntry = ClientConfigEntry;
pub type PostgresConfigEntry = ClientConfigEntry;
pub type MySqlConfigEntry = ClientConfigEntry;
pub type MongoDbConfigEntry = ClientConfigEntry;
pub type S3ConfigEntry = ClientConfigEntry;
pub type GcsConfigEntry = ClientConfigEntry;
pub type AzureBlobConfigEntry = ClientConfigEntry;
pub type IcebergRestConfigEntry = ClientConfigEntry;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchValueMapping {
    pub field: Identifier,
    pub relay: Identifier,
    pub relay_field: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateBranch {
    pub name: Identifier,
    pub schema: Identifier,
    pub ttl: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eviction: Option<BranchEviction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchEviction {
    Lru { max_instances: u64 },
}

impl BranchEviction {
    pub const fn max_instances(&self) -> u64 {
        match self {
            Self::Lru { max_instances } => *max_instances,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchSelection {
    BranchedBy { branch: Identifier },
    Unbranched,
}

impl BranchSelection {
    pub fn branched_by(branch: Identifier) -> Self {
        Self::BranchedBy { branch }
    }

    pub fn unbranched() -> Self {
        Self::Unbranched
    }

    pub fn branch(&self) -> Option<&Identifier> {
        match self {
            Self::BranchedBy { branch } => Some(branch),
            Self::Unbranched => None,
        }
    }

    pub fn is_unbranched(&self) -> bool {
        match self {
            Self::BranchedBy { .. } => false,
            Self::Unbranched => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchInitiatorSelection {
    BranchedBy {
        branch: Identifier,
        values: Vec<BranchValueMapping>,
    },
    Unbranched,
}

impl BranchInitiatorSelection {
    pub fn branched_by(branch: Identifier, values: Vec<BranchValueMapping>) -> Self {
        Self::BranchedBy { branch, values }
    }

    pub fn unbranched() -> Self {
        Self::Unbranched
    }

    pub fn branch(&self) -> Option<&Identifier> {
        match self {
            Self::BranchedBy { branch, .. } => Some(branch),
            Self::Unbranched => None,
        }
    }

    pub fn values(&self) -> &[BranchValueMapping] {
        match self {
            Self::BranchedBy { values, .. } => values,
            Self::Unbranched => &[],
        }
    }

    pub fn is_unbranched(&self) -> bool {
        match self {
            Self::BranchedBy { .. } => false,
            Self::Unbranched => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateIngestor {
    pub name: Identifier,
    pub output_routes: ProcessorOutputs,
    pub decode_using_codec: Identifier,
    pub branched_by: BranchInitiatorSelection,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub timestamp_source: Option<IngestTimestampSource>,
    pub source: IngestSource,
    pub error_policies: ErrorPolicies,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorOutput {
    pub relay: Identifier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_map: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorInputWhere {
    pub relay: Identifier,
    pub where_clause: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorInputs {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<Identifier>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub r#where: Vec<ProcessorInputWhere>,
}

impl ProcessorInputs {
    pub fn new(from: Vec<Identifier>, r#where: Vec<ProcessorInputWhere>) -> Self {
        Self { from, r#where }
    }

    pub fn single(relay: Identifier) -> Self {
        Self {
            from: vec![relay],
            r#where: Vec::new(),
        }
    }

    pub fn first(&self) -> Option<&Identifier> {
        self.from.first()
    }

    pub fn relays(&self) -> &[Identifier] {
        &self.from
    }

    pub fn input_where(&self) -> &[ProcessorInputWhere] {
        &self.r#where
    }

    pub fn where_clauses(&self) -> &[ProcessorInputWhere] {
        &self.r#where
    }
}

impl ProcessorOutput {
    pub fn new(relay: Identifier) -> Self {
        Self {
            relay,
            filter_map: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorOutputs {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<ProcessorOutput>,
}

impl ProcessorOutputs {
    pub fn new(routes: Vec<ProcessorOutput>) -> Self {
        Self { routes }
    }

    pub fn single(relay: Identifier) -> Self {
        Self {
            routes: vec![ProcessorOutput::new(relay)],
        }
    }

    pub fn relays(&self) -> impl Iterator<Item = &Identifier> {
        self.outputs().map(|output| &output.relay)
    }

    pub fn outputs(&self) -> impl Iterator<Item = &ProcessorOutput> {
        self.routes.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestTimestampSource {
    Now,
    At(Identifier),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateReingestor {
    pub name: Identifier,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchInitiatorSelection,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub mode: AckMode,
    pub message_error_policy: MessageErrorPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateInferencer {
    pub name: Identifier,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub resource: Identifier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<u64>,
    pub file: String,
    pub inputs: Vec<InferencerTensorMapping>,
    pub outputs: Vec<InferencerTensorMapping>,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub message_error_policy: MessageErrorPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<String>,
}

impl CreateInferencer {
    pub fn execution_mode(&self) -> Result<InferencerExecutionMode, InferencerTensorSchemaError> {
        let mut execution_mode = None;
        for mapping in self.inputs.iter().chain(&self.outputs) {
            let batch_axis_count = mapping.schema.batch_axis_count();
            if batch_axis_count > 1 {
                return Err(InferencerTensorSchemaError::MultipleBatchAxes {
                    tensor: mapping.tensor.clone(),
                });
            }
            if mapping.schema.fixed_element_count().is_none() {
                return Err(InferencerTensorSchemaError::ElementCountOverflow {
                    tensor: mapping.tensor.clone(),
                });
            }
            let mapping_mode = if batch_axis_count == 1 {
                InferencerExecutionMode::Batched
            } else {
                InferencerExecutionMode::PerMessage
            };
            if let Some(execution_mode) = execution_mode
                && execution_mode != mapping_mode
            {
                return Err(InferencerTensorSchemaError::MixedExecutionModes);
            }
            execution_mode = Some(mapping_mode);
        }
        Ok(execution_mode.unwrap_or(InferencerExecutionMode::PerMessage))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InferencerExecutionMode {
    PerMessage,
    Batched,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InferencerTensorSchemaError {
    #[error("tensor '{tensor}' contains more than one BATCH axis")]
    MultipleBatchAxes { tensor: String },
    #[error("inferencer mixes batched and per-message tensor bindings")]
    MixedExecutionModes,
    #[error("tensor '{tensor}' fixed element count exceeds the supported size")]
    ElementCountOverflow { tensor: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWasmProcessor {
    pub name: Identifier,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub resource: Identifier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<u64>,
    pub file: String,
    pub message_error_policy: MessageErrorPolicy,
    pub global_error_policy: GeneralErrorPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferencerTensorMapping {
    pub tensor: String,
    pub schema: InferencerTensorSchema,
    pub relay: Identifier,
    pub field: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferencerTensorSchema {
    pub representation: InferencerTensorRepresentation,
    pub element_type: InferencerTensorElementType,
    pub dimensions: Vec<InferencerTensorDimension>,
}

impl InferencerTensorSchema {
    pub fn batch_axis(&self) -> Option<usize> {
        self.dimensions
            .iter()
            .position(InferencerTensorDimension::is_batch)
    }

    pub fn batch_axis_count(&self) -> usize {
        self.dimensions
            .iter()
            .filter(|dimension| dimension.is_batch())
            .count()
    }

    pub fn fixed_element_count(&self) -> Option<usize> {
        self.dimensions
            .iter()
            .filter_map(|dimension| match dimension {
                InferencerTensorDimension::Fixed(size) => Some(*size as usize),
                InferencerTensorDimension::Dynamic | InferencerTensorDimension::Batch => None,
            })
            .try_fold(1_usize, usize::checked_mul)
    }

    pub fn is_compatible_with_field_type(&self, field_type: &ParseAsType) -> bool {
        let mut field_type = field_type;
        for dimension in &self.dimensions {
            match dimension {
                InferencerTensorDimension::Fixed(expected) => {
                    let ParseAsType::Array { element, len } = field_type else {
                        return false;
                    };
                    if len != expected {
                        return false;
                    }
                    field_type = element;
                }
                InferencerTensorDimension::Dynamic => {
                    let ParseAsType::Vec { element } = field_type else {
                        return false;
                    };
                    field_type = element;
                }
                InferencerTensorDimension::Batch => {}
            }
        }
        field_type == &ParseAsType::F32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum InferencerTensorRepresentation {
    Dense,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum InferencerTensorElementType {
    F32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InferencerTensorDimension {
    Fixed(u32),
    Dynamic,
    Batch,
}

impl InferencerTensorDimension {
    pub fn is_batch(&self) -> bool {
        if let Self::Batch = self { true } else { false }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateVhost {
    pub name: Identifier,
    pub hostnames: Vec<String>,
    pub tls: Option<VhostTlsResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VhostTlsResource {
    pub resource: Identifier,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateEndpoint {
    pub name: Identifier,
    pub on_vhost: Identifier,
    pub path: String,
    pub endpoint_type: EndpointType,
    pub signaling_protocol: Option<Identifier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum EndpointType {
    Websockets,
    Http,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateSignalingProtocol {
    pub name: Identifier,
    pub on_connect: SignalingProtocolOnConnect,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalingProtocolOnConnect {
    pub send_bodies: Vec<String>,
    pub wait_bodies: Vec<String>,
    pub timeout: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum IngestSource {
    Http {
        client: Identifier,
        every: String,
    },
    Kinesis {
        client: Identifier,
        relay: Identifier,
        instances: u64,
        mode: KinesisIngestMode,
    },
    Kafka {
        client: Identifier,
        topic: Identifier,
        offset_mode: KafkaOffsetMode,
        instances: u64,
        mode: KafkaIngestMode,
    },
    Pulsar {
        client: Identifier,
        topic: Identifier,
        subscription: Identifier,
        instances: u64,
        mode: PulsarIngestMode,
    },
    Mqtt {
        client: Identifier,
        topic: String,
        instances: u64,
        mode: MqttIngestMode,
    },
    Nats {
        client: Identifier,
        subject: Identifier,
        queue_group: Identifier,
        instances: u64,
        mode: NatsIngestMode,
    },
    #[strum(serialize = "RABBITMQ")]
    RabbitMq {
        client: Identifier,
        queue: Identifier,
        instances: u64,
        mode: RabbitMqIngestMode,
    },
    #[strum(serialize = "REDIS")]
    RedisPubSub {
        client: Identifier,
        channel: Identifier,
        mode: RedisPubSubIngestMode,
    },
    Prometheus {
        client: Identifier,
        query: String,
        every: String,
    },
    #[strum(serialize = "ZEROMQ")]
    ZeroMq {
        client: Identifier,
        mode: ZeroMqIngestMode,
    },
    Sqs {
        client: Identifier,
        queue: Identifier,
        instances: u64,
        mode: SqsIngestMode,
    },
    Endpoint {
        endpoint: Identifier,
        mode: EndpointIngestMode,
    },
    Websockets {
        client: Identifier,
        mode: WebsocketsIngestMode,
    },
}

impl IngestSource {
    pub fn transport_label(&self) -> &str {
        self.as_ref()
    }

    pub fn source_ref(&self) -> &Identifier {
        match self {
            Self::Http { client, .. }
            | Self::Kinesis { client, .. }
            | Self::Kafka { client, .. }
            | Self::Pulsar { client, .. }
            | Self::Mqtt { client, .. }
            | Self::Nats { client, .. }
            | Self::RabbitMq { client, .. }
            | Self::RedisPubSub { client, .. }
            | Self::Prometheus { client, .. }
            | Self::ZeroMq { client, .. }
            | Self::Sqs { client, .. }
            | Self::Websockets { client, .. } => client,
            Self::Endpoint { endpoint, .. } => endpoint,
        }
    }

    pub fn source_kind(&self) -> ModelKind {
        match self {
            Self::Endpoint { .. } => ModelKind::Endpoint,
            _ => ModelKind::Client,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KafkaOffsetMode {
    ConsumerGroup(Identifier),
    Domain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub backoff: String,
    pub max_backoff: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KafkaIngestMode {
    AckParallel {
        max: u64,
        batch_timeout: String,
        timeout: String,
        retry_policy: RetryPolicy,
    },
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
    NoAckParallel {
        max: u64,
    },
}

pub type PulsarIngestMode = KafkaIngestMode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KinesisIngestMode {
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum MqttSession {
    Clean,
    Persistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqttQos {
    AtMostOnce,
    AtLeastOnce,
}

impl MqttQos {
    pub const fn level(self) -> u8 {
        match self {
            Self::AtMostOnce => 0,
            Self::AtLeastOnce => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqttIngestMode {
    NoAckSequential {
        session: MqttSession,
        qos: MqttQos,
    },
    NoAckParallel {
        max: u64,
        session: MqttSession,
        qos: MqttQos,
    },
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
    AckParallel {
        max: u64,
        batch_timeout: String,
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

impl MqttIngestMode {
    pub const fn session(&self) -> MqttSession {
        match self {
            Self::NoAckSequential { session, .. } | Self::NoAckParallel { session, .. } => *session,
            Self::AckSequential { .. } | Self::AckParallel { .. } => MqttSession::Persistent,
        }
    }

    pub const fn qos(&self) -> MqttQos {
        match self {
            Self::NoAckSequential { qos, .. } | Self::NoAckParallel { qos, .. } => *qos,
            Self::AckSequential { .. } | Self::AckParallel { .. } => MqttQos::AtLeastOnce,
        }
    }

    pub const fn is_ack(&self) -> bool {
        match self {
            Self::AckSequential { .. } | Self::AckParallel { .. } => true,
            Self::NoAckSequential { .. } | Self::NoAckParallel { .. } => false,
        }
    }

    pub const fn max_inflight(&self) -> usize {
        match self {
            Self::AckParallel { max, .. } | Self::NoAckParallel { max, .. } => *max as usize,
            Self::AckSequential { .. } | Self::NoAckSequential { .. } => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NatsIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RabbitMqIngestMode {
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RedisPubSubIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZeroMqIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqsIngestMode {
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebsocketsIngestMode {
    NoAckSequential,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateRelay {
    pub name: Identifier,
    pub schema: Identifier,
    #[serde(default = "default_relay_buffer")]
    pub buffer: usize,
    pub branching: RelayBranching,
    #[serde(default)]
    pub materialized_state: Option<MaterializedRelayState>,
}

impl CreateRelay {
    pub fn apply_alter(&mut self, operation: &AlterRelayOperation) {
        match operation {
            AlterRelayOperation::SetCapacity { capacity } => {
                self.buffer = *capacity;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterRelay {
    pub relay: Identifier,
    pub operation: AlterRelayOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterRelayOperation {
    SetCapacity { capacity: usize },
}

pub const fn default_relay_buffer() -> usize {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelayBranching {
    BranchedBy { branch: Identifier },
    Unbranched,
}

impl RelayBranching {
    pub fn branched_by(branch: Identifier) -> Self {
        Self::BranchedBy { branch }
    }

    pub fn unbranched() -> Self {
        Self::Unbranched
    }

    pub fn branch(&self) -> Option<&Identifier> {
        match self {
            Self::BranchedBy { branch } => Some(branch),
            Self::Unbranched => None,
        }
    }

    pub fn is_unbranched(&self) -> bool {
        match self {
            Self::Unbranched => true,
            Self::BranchedBy { .. } => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaterializedRelayState {
    LastByTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterSchedule {
    pub domains: Vec<DomainSchedule>,
}

impl ClusterSchedule {
    pub fn domain(&self, domain: &Domain) -> Option<&DomainSchedule> {
        self.domains.iter().find(|item| item.domain == *domain)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainSchedule {
    pub domain: Domain,
    pub nodes: Vec<ScheduledNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaPartitionSchedule {
    pub observed_partitions: Vec<i32>,
    pub rebalance_epoch: u64,
    pub instance_assignments: Vec<Vec<i32>>,
}

impl KafkaPartitionSchedule {
    pub fn new(instances: u64, observed_partitions: Vec<i32>, rebalance_epoch: u64) -> Self {
        let shard_count = usize::try_from(instances.max(1)).unwrap_or(usize::MAX);
        let mut observed_partitions = observed_partitions;
        observed_partitions.sort_unstable();
        let mut instance_assignments = vec![Vec::new(); shard_count];
        for (ordinal, partition) in observed_partitions.iter().copied().enumerate() {
            let instance_idx = ordinal % shard_count;
            if let Some(assigned) = instance_assignments.get_mut(instance_idx) {
                assigned.push(partition);
            }
        }
        Self {
            observed_partitions,
            rebalance_epoch,
            instance_assignments,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledNode {
    pub identifier: Identifier,
    pub kind: ModelKind,
    pub config: Box<Model>,
    pub effective_branching: Option<Vec<Identifier>>,
    pub effective_branching_schema: Option<Identifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_partition_schedule: Option<KafkaPartitionSchedule>,
    #[serde(default)]
    pub primary_node: Option<String>,
    #[serde(default)]
    pub assigned_nodes: Vec<String>,
}

impl ScheduledNode {
    pub fn is_assigned_to(&self, node_id: &str) -> bool {
        self.assigned_nodes
            .iter()
            .any(|assigned| assigned == node_id)
    }

    pub fn assigned_single_node(&self) -> Option<&str> {
        match self.assigned_nodes.as_slice() {
            [node_id] => Some(node_id.as_str()),
            _ => None,
        }
    }

    pub fn primary_node(&self) -> Option<&str> {
        self.primary_node.as_deref()
    }

    pub fn replica_nodes(&self) -> Vec<&str> {
        let primary = self.primary_node();
        self.assigned_nodes
            .iter()
            .filter_map(|node_id| {
                if Some(node_id.as_str()) == primary {
                    None
                } else {
                    Some(node_id.as_str())
                }
            })
            .collect()
    }

    pub fn is_primary_on(&self, node_id: &str) -> bool {
        if let Some(primary_node) = self.primary_node() {
            primary_node == node_id
        } else {
            self.is_assigned_to(node_id)
        }
    }

    pub fn execution_node(&self) -> Option<&str> {
        match self.config.as_ref() {
            Model::Ingestor(CreateIngestor {
                source: IngestSource::Endpoint { .. },
                ..
            }) => None,
            _ => self.primary_node().or_else(|| self.assigned_single_node()),
        }
    }

    pub fn executes_on(&self, node_id: &str) -> bool {
        match self.config.as_ref() {
            Model::Ingestor(CreateIngestor {
                source: IngestSource::Endpoint { .. },
                ..
            }) => self.is_assigned_to(node_id),
            _ => self.is_primary_on(node_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateMaterializer {
    pub relay: Identifier,
    pub state: MaterializedRelayState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateLookup {
    pub name: Identifier,
    pub key_field: Identifier,
    pub resource: Identifier,
    pub path: String,
    pub decode_using_codec: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateJunction {
    pub name: Identifier,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub message_error_policy: MessageErrorPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateDeduplicator {
    pub name: Identifier,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub deduplicate_on: String,
    pub max_time: String,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub message_error_policy: MessageErrorPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCorrelator {
    pub name: Identifier,
    pub left: ProcessorInputs,
    pub right: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub correlate_where: String,
    pub match_policy: CorrelatorMatchPolicy,
    pub output: String,
    pub max_time: String,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub timeout_policy: CorrelationTimeoutPolicy,
    pub message_error_policy: MessageErrorPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<String>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE", ascii_case_insensitive)]
pub enum CorrelatorMatchPolicy {
    Earliest,
    Latest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrelationTimeoutPolicy {
    pub left: CorrelationTimeoutAction,
    pub right: CorrelationTimeoutAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CorrelationTimeoutAction {
    Drop,
    SendTo { relay: Identifier },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateReorderer {
    pub name: Identifier,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub order_by: String,
    pub max_time: String,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub message_error_policy: MessageErrorPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWindowProcessor {
    pub name: Identifier,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub width: WindowBound,
    pub step: WindowBound,
    pub aggregate: String,
    pub message_error_policy: MessageErrorPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowBound {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,
}

impl WindowBound {
    pub fn is_empty(&self) -> bool {
        self.messages.is_none() && self.duration.is_none()
    }

    pub fn to_describe_string(&self) -> String {
        let mut parts = Vec::new();
        if let Some(messages) = self.messages {
            parts.push(format!("{messages} MESSAGES"));
        }
        if let Some(duration) = &self.duration {
            parts.push(format!("{duration} DURATION"));
        }
        parts.join(" ")
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Default,
    AsRefStr,
    EnumString,
    IntoStaticStr,
)]
pub enum AckMode {
    #[default]
    #[strum(serialize = "ATTACHED")]
    Attached,
    #[strum(serialize = "DETACHED")]
    Detached,
}

#[cfg(test)]
mod tests {
    use super::{
        AckMode, BranchInitiatorSelection, BranchSelection, ClusterSchedule, CreateSchema,
        DomainSchedule, ErrorPolicies, InferencerTensorDimension, InferencerTensorElementType,
        InferencerTensorRepresentation, InferencerTensorSchema, KafkaPartitionSchedule,
        MessageErrorPolicy, Model, ModelKind, ScheduledNode,
    };
    use crate::{
        CreateIngestor, CreateJunction, Domain, EndpointIngestMode, Identifier, IngestSource,
        ParseAsType, ProcessorInputs, ProcessorOutputs, SchemaField,
    };

    fn identifier(raw: &str) -> Identifier {
        Identifier::try_from(raw).expect("valid identifier")
    }

    fn domain(raw: &str) -> Domain {
        Domain::try_from(raw).expect("valid domain")
    }

    #[test]
    fn model_kind_completion_labels_roundtrip() {
        for (kind, label, keyword) in [
            (ModelKind::Schema, "ref:schema", "schema"),
            (ModelKind::WireSchema, "ref:wire_schema", "wire_schema"),
            (ModelKind::Codec, "ref:codec", "codec"),
            (ModelKind::Client, "ref:client", "client"),
            (ModelKind::Vhost, "ref:vhost", "vhost"),
            (ModelKind::Endpoint, "ref:endpoint", "endpoint"),
            (
                ModelKind::SignalingProtocol,
                "ref:signaling_protocol",
                "signaling_protocol",
            ),
            (ModelKind::Inferencer, "ref:inferencer", "inferencer"),
            (ModelKind::Ingestor, "ref:ingestor", "ingestor"),
            (ModelKind::Reingestor, "ref:reingestor", "reingestor"),
            (ModelKind::Relay, "ref:relay", "relay"),
            (ModelKind::Junction, "ref:junction", "junction"),
            (ModelKind::Deduplicator, "ref:deduplicator", "deduplicator"),
            (ModelKind::Emitter, "ref:emitter", "emitter"),
        ] {
            assert_eq!(kind.completion_label(), label);
            assert_eq!(ModelKind::from_completion_label(label), Some(kind));
            assert_eq!(kind.as_str(), keyword);
        }

        assert_eq!(ModelKind::from_completion_label("ref:unknown"), None);
    }

    #[test]
    fn inferencer_tensor_schema_requires_exact_array_axis_structure() {
        let schema = InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![
                InferencerTensorDimension::Fixed(2),
                InferencerTensorDimension::Dynamic,
                InferencerTensorDimension::Fixed(3),
            ],
        };
        let exact = ParseAsType::Array {
            len: 2,
            element: Box::new(ParseAsType::Vec {
                element: Box::new(ParseAsType::Array {
                    len: 3,
                    element: Box::new(ParseAsType::F32),
                }),
            }),
        };
        let flattened = ParseAsType::Array {
            len: 6,
            element: Box::new(ParseAsType::F32),
        };

        assert!(schema.is_compatible_with_field_type(&exact));
        assert!(!schema.is_compatible_with_field_type(&flattened));
    }

    #[test]
    fn cluster_schedule_returns_matching_domain() {
        let alpha = DomainSchedule {
            domain: domain("alpha"),
            nodes: Vec::new(),
        };
        let beta = DomainSchedule {
            domain: domain("beta"),
            nodes: Vec::new(),
        };
        let schedule = ClusterSchedule {
            domains: vec![alpha.clone(), beta],
        };

        assert_eq!(schedule.domain(&domain("alpha")), Some(&alpha));
        assert_eq!(schedule.domain(&domain("gamma")), None);
    }

    #[test]
    fn scheduled_node_assignment_checks_exact_node_id() {
        let node = ScheduledNode {
            identifier: identifier("orders_ingestor"),
            kind: ModelKind::Schema,
            config: Box::new(Model::Schema(CreateSchema {
                name: identifier("orders"),
                fields: vec![SchemaField {
                    name: identifier("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                }],
            })),
            effective_branching: Some(vec![identifier("tenant")]),
            effective_branching_schema: None,
            kafka_partition_schedule: None,
            primary_node: Some("node-a".to_string()),
            assigned_nodes: vec!["node-a".to_string()],
        };

        assert!(node.is_assigned_to("node-a"));
        assert!(!node.is_assigned_to("node-b"));
        assert!(
            !ScheduledNode {
                assigned_nodes: Vec::new(),
                ..node
            }
            .is_assigned_to("node-a")
        );
    }

    #[test]
    fn scheduled_node_single_assignment_only_when_exactly_one_node_is_present() {
        let node = ScheduledNode {
            identifier: identifier("orders_ingestor"),
            kind: ModelKind::Schema,
            config: Box::new(Model::Schema(CreateSchema {
                name: identifier("orders"),
                fields: vec![SchemaField {
                    name: identifier("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                }],
            })),
            effective_branching: None,
            effective_branching_schema: None,
            kafka_partition_schedule: None,
            primary_node: Some("node-a".to_string()),
            assigned_nodes: vec!["node-a".to_string()],
        };

        assert_eq!(node.assigned_single_node(), Some("node-a"));
        assert_eq!(
            ScheduledNode {
                assigned_nodes: vec!["node-a".to_string(), "node-b".to_string()],
                ..node.clone()
            }
            .assigned_single_node(),
            None
        );
        assert_eq!(
            ScheduledNode {
                assigned_nodes: Vec::new(),
                ..node
            }
            .assigned_single_node(),
            None
        );
    }

    #[test]
    fn ack_mode_default_is_attached() {
        assert_eq!(AckMode::default(), AckMode::Attached);
    }

    #[test]
    fn scheduled_node_exposes_primary_and_replicas() {
        let node = ScheduledNode {
            identifier: identifier("orders_ingestor"),
            kind: ModelKind::Schema,
            config: Box::new(Model::Schema(CreateSchema {
                name: identifier("orders"),
                fields: vec![SchemaField {
                    name: identifier("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                }],
            })),
            effective_branching: None,
            effective_branching_schema: None,
            kafka_partition_schedule: None,
            primary_node: Some("node-a".to_string()),
            assigned_nodes: vec![
                "node-a".to_string(),
                "node-b".to_string(),
                "node-c".to_string(),
            ],
        };

        assert_eq!(node.primary_node(), Some("node-a"));
        assert_eq!(node.replica_nodes(), vec!["node-b", "node-c"]);
        assert!(node.is_primary_on("node-a"));
        assert!(!node.is_primary_on("node-b"));
    }

    #[test]
    fn scheduled_node_execution_uses_primary_except_for_endpoint_ingestors() {
        let replicated_junction = ScheduledNode {
            identifier: identifier("orders_merge"),
            kind: ModelKind::Junction,
            config: Box::new(Model::Junction(CreateJunction {
                name: identifier("orders_merge"),
                from: ProcessorInputs::new(
                    vec![identifier("orders_in_a"), identifier("orders_in_b")],
                    Vec::new(),
                ),
                output_routes: ProcessorOutputs::single(identifier("orders_out")),
                branched_by: BranchSelection::unbranched(),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Attached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            })),
            effective_branching: None,
            effective_branching_schema: None,
            kafka_partition_schedule: None,
            primary_node: Some("node-a".to_string()),
            assigned_nodes: vec!["node-a".to_string(), "node-b".to_string()],
        };
        let endpoint_ingestor = ScheduledNode {
            identifier: identifier("orders_http"),
            kind: ModelKind::Ingestor,
            config: Box::new(Model::Ingestor(CreateIngestor {
                name: identifier("orders_http"),
                output_routes: ProcessorOutputs::single(identifier("orders_out")),
                decode_using_codec: identifier("codec"),
                branched_by: BranchInitiatorSelection::unbranched(),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                timestamp_source: None,
                source: IngestSource::Endpoint {
                    endpoint: identifier("public_http"),
                    mode: EndpointIngestMode::NoAckSequential,
                },
                error_policies: ErrorPolicies::handled_by_log(),

                filter_where: None,
            })),
            effective_branching: None,
            effective_branching_schema: None,
            kafka_partition_schedule: None,
            primary_node: Some("node-a".to_string()),
            assigned_nodes: vec!["node-a".to_string(), "node-b".to_string()],
        };

        assert_eq!(replicated_junction.execution_node(), Some("node-a"));
        assert!(replicated_junction.executes_on("node-a"));
        assert!(!replicated_junction.executes_on("node-b"));

        assert_eq!(endpoint_ingestor.execution_node(), None);
        assert!(endpoint_ingestor.executes_on("node-a"));
        assert!(endpoint_ingestor.executes_on("node-b"));
    }

    #[test]
    fn kafka_partition_schedule_assigns_partitions_round_robin_by_instance() {
        let schedule = KafkaPartitionSchedule::new(2, vec![3, 1, 2, 0], 7);

        assert_eq!(schedule.observed_partitions, vec![0, 1, 2, 3]);
        assert_eq!(schedule.rebalance_epoch, 7);
        assert_eq!(schedule.instance_assignments, vec![vec![0, 2], vec![1, 3]]);
    }
}
