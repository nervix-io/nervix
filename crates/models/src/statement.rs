use std::ops::{Deref, DerefMut};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter, EnumProperty, EnumString, IntoEnumIterator, IntoStaticStr};
use thiserror::Error;

use crate::{
    AlterSchema, AlterWireSchema, AvroType, CborType, CreateAvroWireSchema, CreateCborWireSchema,
    CreateJsonWireSchema, CreateSchema, CreateUdf, Domain, Identifier, JsonType, ParseAsType,
    Timestamp,
};

pub type DomainId = Domain;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Statement {
    CreateDomain(CreateStatement<CreateDomain>),
    AlterDomain(AlterDomain),
    CreateUser(CreateStatement<CreateUser>),
    CreateResource(CreateStatement<CreateResource>),
    UploadResource(UploadResource),
    StartDomain(StartDomain),
    StopDomain(StopDomain),
    Create(CreateStatement<Box<Model>>),
    AlterSchema(AlterSchema),
    AlterWireJsonSchema(AlterWireSchema<JsonType>),
    AlterWireCborSchema(AlterWireSchema<CborType>),
    AlterWireAvroSchema(AlterWireSchema<AvroType>),
    AlterRelay(AlterRelay),
    AlterJunction(AlterJunction),
    AlterDeduplicator(AlterDeduplicator),
    AlterReorderer(AlterReorderer),
    AlterEmitter(AlterEmitter),
    AlterIngestor(AlterIngestor),
    AlterReingestor(AlterReingestor),
    AlterGenerator(AlterGenerator),
    AlterPlacement(AlterPlacement),
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
    DescribeJunction(DescribeJunction),
    DescribeDeduplicator(DescribeDeduplicator),
    DescribeReingestor(DescribeReingestor),
    DescribeCorrelator(DescribeCorrelator),
    DescribeReorderer(DescribeReorderer),
    DescribeEmitter(DescribeEmitter),
    DescribeWindowProcessor(DescribeWindowProcessor),
    DescribeWasmProcessor(DescribeWasmProcessor),
    DescribeUdf(DescribeUdf),
    DescribePlacement(DescribePlacement),
    LookupQuery(LookupQuery),
    ShowCreate(ShowCreate),
    ShowUdfs(ShowUdfs),
    ShowPlacements(ShowPlacements),
    ShowRelayMaterializedState(ShowRelayMaterializedState),
    ShowClusterStatus(ShowClusterStatus),
    ShowTransactions(ShowTransactions),
}

impl Statement {
    pub fn is_model_mutation(&self) -> bool {
        match self {
            Self::Create(_)
            | Self::AlterSchema(_)
            | Self::AlterWireJsonSchema(_)
            | Self::AlterWireCborSchema(_)
            | Self::AlterWireAvroSchema(_)
            | Self::AlterRelay(_)
            | Self::AlterJunction(_)
            | Self::AlterDeduplicator(_)
            | Self::AlterReorderer(_)
            | Self::AlterEmitter(_)
            | Self::AlterIngestor(_)
            | Self::AlterReingestor(_)
            | Self::AlterGenerator(_)
            | Self::AlterPlacement(_)
            | Self::Drop(_) => true,
            Self::CreateDomain(_)
            | Self::AlterDomain(_)
            | Self::CreateUser(_)
            | Self::CreateResource(_)
            | Self::UploadResource(_)
            | Self::StartDomain(_)
            | Self::StopDomain(_)
            | Self::DropNode(_)
            | Self::CordonNode(_)
            | Self::UncordonNode(_)
            | Self::DrainNode(_)
            | Self::DescribeRelay(_)
            | Self::DescribeDomain(_)
            | Self::DescribeIngestor(_)
            | Self::DescribeResource(_)
            | Self::DescribeLookup(_)
            | Self::DescribeEndpoint(_)
            | Self::DescribeJunction(_)
            | Self::DescribeDeduplicator(_)
            | Self::DescribeReingestor(_)
            | Self::DescribeCorrelator(_)
            | Self::DescribeReorderer(_)
            | Self::DescribeEmitter(_)
            | Self::DescribeWindowProcessor(_)
            | Self::DescribeWasmProcessor(_)
            | Self::DescribeUdf(_)
            | Self::DescribePlacement(_)
            | Self::LookupQuery(_)
            | Self::ShowCreate(_)
            | Self::ShowUdfs(_)
            | Self::ShowPlacements(_)
            | Self::ShowRelayMaterializedState(_)
            | Self::ShowClusterStatus(_)
            | Self::ShowTransactions(_) => false,
        }
    }
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
    #[strum(props(completion_label = "ref:schema", keyword = "SCHEMA"))]
    Schema,
    #[strum(props(
        completion_label = "ref:wire_json_schema",
        keyword = "WIRE JSON SCHEMA"
    ))]
    WireJsonSchema,
    #[strum(props(
        completion_label = "ref:wire_cbor_schema",
        keyword = "WIRE CBOR SCHEMA"
    ))]
    WireCborSchema,
    #[strum(props(
        completion_label = "ref:wire_avro_schema",
        keyword = "WIRE AVRO SCHEMA"
    ))]
    WireAvroSchema,
    #[strum(props(completion_label = "ref:codec", keyword = "CODEC"))]
    Codec,
    #[strum(props(completion_label = "ref:client", keyword = "CLIENT"))]
    Client,
    #[strum(props(completion_label = "ref:vhost", keyword = "VHOST"))]
    Vhost,
    #[strum(props(completion_label = "ref:branch", keyword = "BRANCH"))]
    Branch,
    #[strum(props(completion_label = "ref:endpoint", keyword = "ENDPOINT"))]
    Endpoint,
    #[strum(props(
        completion_label = "ref:signaling_protocol",
        keyword = "SIGNALING PROTOCOL"
    ))]
    SignalingProtocol,
    #[strum(props(completion_label = "ref:generator", keyword = "GENERATOR"))]
    Generator,
    #[strum(props(completion_label = "ref:inferencer", keyword = "INFERENCER"))]
    Inferencer,
    #[strum(props(completion_label = "ref:wasm_processor", keyword = "WASM PROCESSOR"))]
    WasmProcessor,
    #[strum(props(completion_label = "ref:ingestor", keyword = "INGESTOR"))]
    Ingestor,
    #[strum(props(completion_label = "ref:reingestor", keyword = "REINGESTOR"))]
    Reingestor,
    #[strum(props(completion_label = "ref:relay", keyword = "RELAY"))]
    Relay,
    #[strum(props(completion_label = "ref:materializer", keyword = "MATERIALIZER"))]
    Materializer,
    #[strum(props(completion_label = "ref:lookup", keyword = "HASH MAP"))]
    Lookup,
    #[strum(props(completion_label = "ref:junction", keyword = "JUNCTION"))]
    Junction,
    #[strum(props(completion_label = "ref:deduplicator", keyword = "DEDUPLICATOR"))]
    Deduplicator,
    #[strum(props(completion_label = "ref:correlator", keyword = "CORRELATOR"))]
    Correlator,
    #[strum(props(completion_label = "ref:reorderer", keyword = "REORDERER"))]
    Reorderer,
    #[strum(props(
        completion_label = "ref:window_processor",
        keyword = "WINDOW PROCESSOR"
    ))]
    WindowProcessor,
    #[strum(props(completion_label = "ref:emitter", keyword = "EMITTER"))]
    Emitter,
    #[strum(props(completion_label = "ref:placement", keyword = "PLACEMENT"))]
    Placement,
    #[strum(props(completion_label = "ref:udf", keyword = "UDF"))]
    Udf,
}

impl ModelKind {
    pub fn completion_label(self) -> &'static str {
        self.get_str("completion_label")
            .expect("every model kind must define a completion_label")
    }

    /// The NSPL keyword phrase that names this kind in `DROP` and `SHOW CREATE`.
    pub fn keyword_phrase(self) -> &'static str {
        self.get_str("keyword")
            .expect("every model kind must define a keyword phrase")
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
pub struct ShowTransactions;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShowUdfs;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShowPlacements;

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
pub struct AlterDomain {
    pub policy: PlacementPolicy,
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
    pub placement: PlacementPolicy,
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
    Default,
    AsRefStr,
    strum::Display,
)]
pub enum PlacementPolicy {
    #[strum(serialize = "REQUIRE COLOCATION")]
    RequireColocation,
    #[strum(serialize = "PREFER COLOCATION")]
    PreferColocation,
    #[default]
    #[strum(serialize = "NEUTRAL")]
    Neutral,
    #[strum(serialize = "SUGGEST SEPARATION")]
    SuggestSeparation,
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
    Paused,
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
pub struct DomainClockState {
    pub wall_started_at: Timestamp,
    pub logical_start: Timestamp,
    pub time_rate: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainState {
    pub id: DomainId,
    pub config: DomainConfig,
    pub status: DomainStatus,
    pub start_version: u64,
    pub last_start: DomainStartPoint,
    pub clock: Option<DomainClockState>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<crate::Expression>,
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
pub struct DescribeJunction {
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
pub struct DescribeUdf {
    pub name: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribePlacement {
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
pub struct CreatePlacement {
    pub name: Identifier,
    pub from: Vec<Identifier>,
    pub to: Vec<Identifier>,
    pub policy: PlacementPolicy,
    pub rank: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterPlacement {
    pub placement: Identifier,
    pub operations: Vec<AlterPlacementOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterPlacementOperation {
    SetPolicy {
        policy: PlacementPolicy,
    },
    SetRank {
        rank: u64,
    },
    DropRank,
    SetMembers {
        from: Vec<Identifier>,
        to: Vec<Identifier>,
    },
    RenameTo {
        name: Identifier,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterPlacementError {
    #[error("ALTER targets placement `{requested}`, but the stored placement is `{stored}`")]
    PlacementNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error("a placement must declare at least one FROM member")]
    EmptyFrom,
    #[error("a placement must declare at least one TO member")]
    EmptyTo,
    #[error("placement RANK 0 is invalid; RANK must be greater than zero")]
    RankZero,
}

impl CreatePlacement {
    pub fn new(
        name: Identifier,
        from: Vec<Identifier>,
        to: Vec<Identifier>,
        policy: PlacementPolicy,
        rank: Option<u64>,
    ) -> Result<Self, AlterPlacementError> {
        let mut placement = Self {
            name,
            from,
            to,
            policy,
            rank,
        };
        placement.normalize_members();
        placement.validate()?;
        Ok(placement)
    }

    pub fn apply_alter(&mut self, alter: &AlterPlacement) -> Result<(), AlterPlacementError> {
        if self.name != alter.placement {
            return Err(AlterPlacementError::PlacementNameMismatch {
                stored: self.name.clone(),
                requested: alter.placement.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            match operation {
                AlterPlacementOperation::SetPolicy { policy } => candidate.policy = *policy,
                AlterPlacementOperation::SetRank { rank } => candidate.rank = Some(*rank),
                AlterPlacementOperation::DropRank => candidate.rank = None,
                AlterPlacementOperation::SetMembers { from, to } => {
                    candidate.from = from.clone();
                    candidate.to = to.clone();
                    candidate.normalize_members();
                }
                AlterPlacementOperation::RenameTo { name } => candidate.name = name.clone(),
            }
            candidate.validate()?;
        }
        *self = candidate;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), AlterPlacementError> {
        if self.from.is_empty() {
            return Err(AlterPlacementError::EmptyFrom);
        }
        if self.to.is_empty() {
            return Err(AlterPlacementError::EmptyTo);
        }
        if self.rank == Some(0) {
            return Err(AlterPlacementError::RankZero);
        }
        Ok(())
    }

    fn normalize_members(&mut self) {
        deduplicate_identifiers(&mut self.from);
        deduplicate_identifiers(&mut self.to);
    }
}

fn deduplicate_identifiers(identifiers: &mut Vec<Identifier>) {
    let mut seen = Vec::new();
    identifiers.retain(|identifier| {
        if seen.contains(identifier) {
            false
        } else {
            seen.push(identifier.clone());
            true
        }
    });
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Model {
    Schema(CreateSchema),
    WireJsonSchema(CreateJsonWireSchema),
    WireCborSchema(CreateCborWireSchema),
    WireAvroSchema(CreateAvroWireSchema),
    Codec(CreateCodec),
    ClientKafka(CreateClientKafka),
    ClientPulsar(CreateClientPulsar),
    ClientHttp(CreateClientHttp),
    ClientSentry(CreateClientSentry),
    ClientOtel(CreateClientOtel),
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
    Placement(CreatePlacement),
    Udf(CreateUdf),
}

impl Model {
    pub fn kind(&self) -> ModelKind {
        match self {
            Self::Schema(_) => ModelKind::Schema,
            Self::WireJsonSchema(_) => ModelKind::WireJsonSchema,
            Self::WireCborSchema(_) => ModelKind::WireCborSchema,
            Self::WireAvroSchema(_) => ModelKind::WireAvroSchema,
            Self::Codec(_) => ModelKind::Codec,
            Self::ClientKafka(_)
            | Self::ClientPulsar(_)
            | Self::ClientHttp(_)
            | Self::ClientSentry(_)
            | Self::ClientOtel(_)
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
            Self::Placement(_) => ModelKind::Placement,
            Self::Udf(_) => ModelKind::Udf,
        }
    }

    pub fn identifier(&self) -> &Identifier {
        match self {
            Self::Schema(v) => &v.name,
            Self::WireJsonSchema(v) => &v.name,
            Self::WireCborSchema(v) => &v.name,
            Self::WireAvroSchema(v) => &v.name,
            Self::Codec(v) => &v.name,
            Self::ClientKafka(v) => &v.name,
            Self::ClientPulsar(v) => &v.name,
            Self::ClientHttp(v) => &v.name,
            Self::ClientSentry(v) => &v.name,
            Self::ClientOtel(v) => &v.name,
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
            Self::Placement(v) => &v.name,
            Self::Udf(v) => &v.name,
        }
    }

    pub fn client_type_label(&self) -> Option<&'static str> {
        match self {
            Self::ClientKafka(_) => Some("KAFKA"),
            Self::ClientPulsar(_) => Some("PULSAR"),
            Self::ClientHttp(_) => Some("HTTP"),
            Self::ClientSentry(_) => Some("SENTRY"),
            Self::ClientOtel(_) => Some("OTEL"),
            Self::ClientPrometheus(_) => Some("PROMETHEUS"),
            Self::ClientMqtt(_) => Some("MQTT"),
            Self::ClientNats(_) => Some("NATS"),
            Self::ClientRabbitMq(_) => Some("RABBITMQ"),
            Self::ClientRedis(_) => Some("REDIS"),
            Self::ClientZeroMq(_) => Some("ZEROMQ"),
            Self::ClientSqs(_) => Some("SQS"),
            Self::ClientWebsockets(_) => Some("WEBSOCKETS"),
            Self::ClientClickHouse(_) => Some("CLICKHOUSE"),
            Self::ClientPostgres(_) => Some("POSTGRES"),
            Self::ClientMySql(_) => Some("MYSQL"),
            Self::ClientMongoDb(_) => Some("MONGODB"),
            Self::ClientS3(_) => Some("S3"),
            Self::ClientGcs(_) => Some("GCS"),
            Self::ClientAzureBlob(_) => Some("AZURE_BLOB"),
            Self::ClientIcebergRest(_) => Some("ICEBERG_REST"),
            Self::Schema(_)
            | Self::WireJsonSchema(_)
            | Self::WireCborSchema(_)
            | Self::WireAvroSchema(_)
            | Self::Codec(_)
            | Self::Vhost(_)
            | Self::Branch(_)
            | Self::Endpoint(_)
            | Self::SignalingProtocol(_)
            | Self::Generator(_)
            | Self::Inferencer(_)
            | Self::WasmProcessor(_)
            | Self::Ingestor(_)
            | Self::Reingestor(_)
            | Self::Relay(_)
            | Self::Materializer(_)
            | Self::Lookup(_)
            | Self::Junction(_)
            | Self::Deduplicator(_)
            | Self::Correlator(_)
            | Self::Reorderer(_)
            | Self::WindowProcessor(_)
            | Self::Emitter(_)
            | Self::Placement(_)
            | Self::Udf(_) => None,
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
    pub const fn wire_schema_kind(&self) -> Option<ModelKind> {
        match self {
            Self::Json => Some(ModelKind::WireJsonSchema),
            Self::Cbor => Some(ModelKind::WireCborSchema),
            Self::Avro => Some(ModelKind::WireAvroSchema),
            Self::JaqNative { .. } | Self::Protobuf(_) => None,
        }
    }

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
    pub from: ProcessorInputs,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encode_using_codec: Option<Identifier>,
    pub sink: Box<EmitSink>,
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
    pub error_policies: ErrorPolicies,
    pub publishing_mode: EmitterPublishingMode,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default)]
    pub construction: crate::RouteConstruction,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

impl CreateEmitter {
    pub fn flush_policy(&self) -> (&str, Option<&str>) {
        (self.flush_each.as_str(), self.max_batch_size.as_deref())
    }

    pub fn apply_alter(&mut self, alter: &AlterEmitter) -> Result<(), AlterEmitterError> {
        if self.name != alter.emitter {
            return Err(AlterEmitterError::EmitterNameMismatch {
                stored: self.name.clone(),
                requested: alter.emitter.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.apply_alter_operation(operation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn apply_alter_operation(
        &mut self,
        operation: &AlterEmitterOperation,
    ) -> Result<(), AlterEmitterError> {
        match operation {
            AlterEmitterOperation::AddFrom {
                relay,
                where_clause,
            } => {
                self.ensure_input_absent(relay)?;
                self.from.from.push(relay.clone());
                if let Some(where_clause) = where_clause {
                    self.from.r#where.push(ProcessorInputWhere {
                        relay: relay.clone(),
                        where_clause: where_clause.clone(),
                    });
                }
            }
            AlterEmitterOperation::DropFrom { relay } => {
                let index = self.input_index(relay)?;
                if self.from.from.len() == 1 {
                    return Err(AlterEmitterError::CannotDropLastInput);
                }
                self.from.from.remove(index);
                self.from
                    .r#where
                    .retain(|input_where| input_where.relay != *relay);
            }
            AlterEmitterOperation::AlterFromSetWhere {
                relay,
                where_clause,
            } => {
                self.input_index(relay)?;
                if let Some(input_where) = self
                    .from
                    .r#where
                    .iter_mut()
                    .find(|input_where| input_where.relay == *relay)
                {
                    input_where.where_clause = where_clause.clone();
                } else {
                    self.from.r#where.push(ProcessorInputWhere {
                        relay: relay.clone(),
                        where_clause: where_clause.clone(),
                    });
                }
            }
            AlterEmitterOperation::AlterFromDropWhere { relay } => {
                self.input_index(relay)?;
                let Some(index) = self
                    .from
                    .r#where
                    .iter()
                    .position(|input_where| input_where.relay == *relay)
                else {
                    return Err(AlterEmitterError::InputWhereNotConfigured {
                        relay: relay.clone(),
                    });
                };
                self.from.r#where.remove(index);
            }
            AlterEmitterOperation::SetSink {
                sink,
                publishing_mode,
            } => {
                if !sink.accepts_publishing_mode(publishing_mode) {
                    return Err(AlterEmitterError::PublishingModeUnsupported {
                        sink: sink.transport_label().to_string(),
                        mode: publishing_mode.kind_label().to_string(),
                    });
                }
                let mut sink = sink.clone();
                sink.copy_flush_policy_from(self);
                self.sink = sink;
                self.publishing_mode = publishing_mode.clone();
            }
            AlterEmitterOperation::SetClient { client } => {
                *self.sink.client_mut() = client.clone();
            }
            AlterEmitterOperation::SetEncodeUsing { codec } => {
                self.encode_using_codec = Some(codec.clone());
            }
            AlterEmitterOperation::DropEncode => {
                if self.encode_using_codec.take().is_none() {
                    return Err(AlterEmitterError::EncodeNotConfigured);
                }
            }
            AlterEmitterOperation::SetCollect { policy } => {
                self.from.collect_policy = Some(policy.clone());
            }
            AlterEmitterOperation::DropCollect => {
                self.from.collect_policy = None;
            }
            AlterEmitterOperation::SetAttachment { mode } => {
                self.mode = *mode;
            }
            AlterEmitterOperation::SetPublishingMode { mode } => {
                if !self.sink.accepts_publishing_mode(mode) {
                    return Err(AlterEmitterError::PublishingModeUnsupported {
                        sink: self.sink.transport_label().to_string(),
                        mode: mode.kind_label().to_string(),
                    });
                }
                self.publishing_mode = mode.clone();
            }
            AlterEmitterOperation::SetFlush {
                flush_each,
                max_batch_size,
            } => {
                self.flush_each = flush_each.clone();
                self.max_batch_size = max_batch_size.clone();
                let mut sink = self.sink.clone();
                sink.copy_flush_policy_from(self);
                self.sink = sink;
            }
            AlterEmitterOperation::SetCommit {
                commit_each,
                max_commit_size,
            } => {
                let EmitSink::Iceberg {
                    commit_each: current_commit_each,
                    max_commit_size: current_max_commit_size,
                    ..
                } = self.sink.as_mut()
                else {
                    return Err(AlterEmitterError::CommitPolicyUnsupported);
                };
                *current_commit_each = commit_each.clone();
                *current_max_commit_size = max_commit_size.clone();
            }
        }
        Ok(())
    }

    fn input_index(&self, relay: &Identifier) -> Result<usize, AlterEmitterError> {
        self.from
            .from
            .iter()
            .position(|candidate| candidate == relay)
            .ok_or_else(|| AlterEmitterError::InputNotFound {
                relay: relay.clone(),
            })
    }

    fn ensure_input_absent(&self, relay: &Identifier) -> Result<(), AlterEmitterError> {
        if self.from.from.iter().any(|candidate| candidate == relay) {
            Err(AlterEmitterError::InputAlreadyExists {
                relay: relay.clone(),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterEmitter {
    pub emitter: Identifier,
    pub operations: Vec<AlterEmitterOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterEmitterOperation {
    AddFrom {
        relay: Identifier,
        where_clause: Option<crate::Expression>,
    },
    DropFrom {
        relay: Identifier,
    },
    AlterFromSetWhere {
        relay: Identifier,
        where_clause: crate::Expression,
    },
    AlterFromDropWhere {
        relay: Identifier,
    },
    SetSink {
        sink: Box<EmitSink>,
        publishing_mode: EmitterPublishingMode,
    },
    SetClient {
        client: Identifier,
    },
    SetEncodeUsing {
        codec: Identifier,
    },
    DropEncode,
    SetCollect {
        policy: InputCollectPolicy,
    },
    DropCollect,
    SetAttachment {
        mode: AckMode,
    },
    SetPublishingMode {
        mode: EmitterPublishingMode,
    },
    SetFlush {
        flush_each: String,
        max_batch_size: Option<String>,
    },
    SetCommit {
        commit_each: String,
        max_commit_size: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterEmitterError {
    #[error("ALTER targets emitter `{requested}`, but the stored emitter is `{stored}`")]
    EmitterNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error("input relay `{relay}` is already configured")]
    InputAlreadyExists { relay: Identifier },
    #[error("input relay `{relay}` is not configured")]
    InputNotFound { relay: Identifier },
    #[error("input relay `{relay}` has no WHERE clause")]
    InputWhereNotConfigured { relay: Identifier },
    #[error("an emitter must retain at least one input")]
    CannotDropLastInput,
    #[error("emitter encoding is not configured")]
    EncodeNotConfigured,
    #[error("COMMIT policy is only supported by Iceberg emitters")]
    CommitPolicyUnsupported,
    #[error("{sink} emitters do not support publishing mode {mode}")]
    PublishingModeUnsupported { sink: String, mode: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateGenerator {
    pub name: Identifier,
    pub materialized_relay: Identifier,
    pub branched_by: BranchSelection,
    pub each: String,
    pub output_routes: ProcessorOutputs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterGenerator {
    pub generator: Identifier,
    pub operations: Vec<AlterGeneratorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterGeneratorOperation {
    SetMaterializedState { relay: Identifier },
    SetEach { each: String },
    SetBranching { branching: BranchSelection },
    AddRoute { route: ProcessorOutput },
    DropRoute { relay: Identifier },
    ReplaceRoute { route: ProcessorOutput },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterGeneratorError {
    #[error("ALTER targets generator `{requested}`, but the stored generator is `{stored}`")]
    GeneratorNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error("route target `{relay}` is not configured")]
    RouteTargetNotFound { relay: Identifier },
    #[error("route target `{relay}` is ambiguous because it is configured more than once")]
    RouteTargetAmbiguous { relay: Identifier },
    #[error("a generator must retain at least one route")]
    CannotDropLastRoute,
}

impl CreateGenerator {
    pub fn apply_alter(&mut self, alter: &AlterGenerator) -> Result<(), AlterGeneratorError> {
        if self.name != alter.generator {
            return Err(AlterGeneratorError::GeneratorNameMismatch {
                stored: self.name.clone(),
                requested: alter.generator.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            match operation {
                AlterGeneratorOperation::SetMaterializedState { relay } => {
                    candidate.materialized_relay = relay.clone();
                }
                AlterGeneratorOperation::SetEach { each } => {
                    candidate.each = each.clone();
                }
                AlterGeneratorOperation::SetBranching { branching } => {
                    candidate.branched_by = branching.clone();
                }
                AlterGeneratorOperation::AddRoute { route } => {
                    candidate.output_routes.routes.push(route.clone());
                }
                AlterGeneratorOperation::DropRoute { relay } => {
                    let index = candidate.unique_route_index(relay)?;
                    if candidate.output_routes.routes.len() == 1 {
                        return Err(AlterGeneratorError::CannotDropLastRoute);
                    }
                    candidate.output_routes.routes.remove(index);
                }
                AlterGeneratorOperation::ReplaceRoute { route } => {
                    let index = candidate.unique_route_index(&route.relay)?;
                    candidate.output_routes.routes[index] = route.clone();
                }
            }
        }
        *self = candidate;
        Ok(())
    }

    fn unique_route_index(&self, relay: &Identifier) -> Result<usize, AlterGeneratorError> {
        let mut indexes = self
            .output_routes
            .routes
            .iter()
            .enumerate()
            .filter_map(|(index, route)| (route.relay == *relay).then_some(index));
        let Some(index) = indexes.next() else {
            return Err(AlterGeneratorError::RouteTargetNotFound {
                relay: relay.clone(),
            });
        };
        if indexes.next().is_some() {
            return Err(AlterGeneratorError::RouteTargetAmbiguous {
                relay: relay.clone(),
            });
        }
        Ok(index)
    }
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
        assignments: Vec<crate::Assignment>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeneralErrorPolicy {
    Ignore,
    Log,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqsFifoGroup {
    FromBranch,
    Expression(crate::Expression),
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
    ZeroMq {
        client: Identifier,
    },
    Sqs {
        client: Identifier,
        queue: String,
        fifo_group: Option<SqsFifoGroup>,
    },
    Sentry {
        client: Identifier,
    },
    Otel {
        client: Identifier,
        signal: OtelSignal,
        values: Vec<OtelValueMapping>,
        attributes: Vec<OtelValueMapping>,
        resource: Vec<OtelValueMapping>,
        scope: Option<OtelScope>,
    },
    #[strum(serialize = "CLICKHOUSE")]
    ClickHouse {
        client: Identifier,
        table: Identifier,
        values: Vec<ClickHouseValueMapping>,
        max_batch: u64,
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
    #[strum(serialize = "MYSQL")]
    MySql {
        client: Identifier,
        table: Identifier,
        values: Vec<MySqlValueMapping>,
        conflict_action: MySqlConflictAction,
        max_batch: u64,
        flush_each: String,
    },
    #[strum(serialize = "MONGODB")]
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
            | Self::RabbitMq { client, .. }
            | Self::Redis { client, .. }
            | Self::Mqtt { client, .. }
            | Self::Nats { client, .. }
            | Self::ZeroMq { client }
            | Self::Sqs { client, .. }
            | Self::Sentry { client }
            | Self::Otel { client, .. }
            | Self::ClickHouse { client, .. }
            | Self::Postgres { client, .. }
            | Self::MySql { client, .. }
            | Self::MongoDb { client, .. }
            | Self::Iceberg { client, .. } => client,
        }
    }

    pub fn accepts_publishing_mode(&self, mode: &EmitterPublishingMode) -> bool {
        match self {
            Self::Kafka { .. } | Self::Pulsar { .. } | Self::RabbitMq { .. } => {
                matches!(
                    mode,
                    EmitterPublishingMode::NoAck { .. } | EmitterPublishingMode::BrokerAck { .. }
                )
            }
            Self::Mqtt { .. } => matches!(
                mode,
                EmitterPublishingMode::MqttQos0 { .. }
                    | EmitterPublishingMode::MqttQos1 { .. }
                    | EmitterPublishingMode::MqttQos2 { .. }
            ),
            Self::Nats { .. } => matches!(
                mode,
                EmitterPublishingMode::NoAck { .. } | EmitterPublishingMode::NatsJetStream { .. }
            ),
            Self::Redis { .. } | Self::ZeroMq { .. } => {
                matches!(mode, EmitterPublishingMode::NoAck { .. })
            }
            Self::Sqs { .. } => matches!(
                mode,
                EmitterPublishingMode::SqsSingle { .. } | EmitterPublishingMode::SqsBatch { .. }
            ),
            Self::Sentry { .. }
            | Self::Otel { .. }
            | Self::ClickHouse { .. }
            | Self::Postgres { .. }
            | Self::MySql { .. }
            | Self::MongoDb { .. }
            | Self::Iceberg { .. } => {
                matches!(mode, EmitterPublishingMode::RequestAck { .. })
            }
        }
    }

    fn client_mut(&mut self) -> &mut Identifier {
        match self {
            Self::Kafka { client, .. }
            | Self::Pulsar { client, .. }
            | Self::RabbitMq { client, .. }
            | Self::Redis { client, .. }
            | Self::Mqtt { client, .. }
            | Self::Nats { client, .. }
            | Self::ZeroMq { client }
            | Self::Sqs { client, .. }
            | Self::Sentry { client }
            | Self::Otel { client, .. }
            | Self::ClickHouse { client, .. }
            | Self::Postgres { client, .. }
            | Self::MySql { client, .. }
            | Self::MongoDb { client, .. }
            | Self::Iceberg { client, .. } => client,
        }
    }

    fn copy_flush_policy_from(&mut self, emitter: &CreateEmitter) {
        match self {
            Self::ClickHouse { flush_each, .. }
            | Self::Postgres { flush_each, .. }
            | Self::MySql { flush_each, .. }
            | Self::MongoDb { flush_each, .. } => {
                *flush_each = emitter.flush_each.clone();
            }
            Self::Iceberg {
                flush_each,
                max_batch_size,
                ..
            } => {
                *flush_each = emitter.flush_each.clone();
                *max_batch_size = emitter.max_batch_size.clone();
            }
            Self::Kafka { .. }
            | Self::Pulsar { .. }
            | Self::RabbitMq { .. }
            | Self::Redis { .. }
            | Self::Mqtt { .. }
            | Self::Nats { .. }
            | Self::ZeroMq { .. }
            | Self::Sqs { .. }
            | Self::Sentry { .. }
            | Self::Otel { .. } => {}
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

    pub fn expected_client_type(&self) -> &'static str {
        match self {
            Self::Kafka { .. } => "KAFKA",
            Self::Pulsar { .. } => "PULSAR",
            Self::RabbitMq { .. } => "RABBITMQ",
            Self::Redis { .. } => "REDIS",
            Self::Mqtt { .. } => "MQTT",
            Self::Nats { .. } => "NATS",
            Self::ZeroMq { .. } => "ZEROMQ",
            Self::Sqs { .. } => "SQS",
            Self::Sentry { .. } => "SENTRY",
            Self::Otel { .. } => "OTEL",
            Self::ClickHouse { .. } => "CLICKHOUSE",
            Self::Postgres { .. } => "POSTGRES",
            Self::MySql { .. } => "MYSQL",
            Self::MongoDb { .. } => "MONGODB",
            Self::Iceberg {
                backend: IcebergStorageBackend::S3,
                ..
            } => "S3",
            Self::Iceberg {
                backend: IcebergStorageBackend::Gcs,
                ..
            } => "GCS",
            Self::Iceberg {
                backend: IcebergStorageBackend::AzureBlob,
                ..
            } => "AZURE_BLOB",
        }
    }

    pub fn accepts_client(&self, client: &Model) -> bool {
        matches!(
            (self, client),
            (Self::Kafka { .. }, Model::ClientKafka(_))
                | (Self::Pulsar { .. }, Model::ClientPulsar(_))
                | (Self::RabbitMq { .. }, Model::ClientRabbitMq(_))
                | (Self::Redis { .. }, Model::ClientRedis(_))
                | (Self::Mqtt { .. }, Model::ClientMqtt(_))
                | (Self::Nats { .. }, Model::ClientNats(_))
                | (Self::ZeroMq { .. }, Model::ClientZeroMq(_))
                | (Self::Sqs { .. }, Model::ClientSqs(_))
                | (Self::Sentry { .. }, Model::ClientSentry(_))
                | (Self::Otel { .. }, Model::ClientOtel(_))
                | (Self::ClickHouse { .. }, Model::ClientClickHouse(_))
                | (Self::Postgres { .. }, Model::ClientPostgres(_))
                | (Self::MySql { .. }, Model::ClientMySql(_))
                | (Self::MongoDb { .. }, Model::ClientMongoDb(_))
                | (
                    Self::Iceberg {
                        backend: IcebergStorageBackend::S3,
                        ..
                    },
                    Model::ClientS3(_),
                )
                | (
                    Self::Iceberg {
                        backend: IcebergStorageBackend::Gcs,
                        ..
                    },
                    Model::ClientGcs(_),
                )
                | (
                    Self::Iceberg {
                        backend: IcebergStorageBackend::AzureBlob,
                        ..
                    },
                    Model::ClientAzureBlob(_),
                )
        )
    }

    pub fn requires_codec(&self) -> bool {
        match self {
            Self::Kafka { .. }
            | Self::Pulsar { .. }
            | Self::RabbitMq { .. }
            | Self::Redis { .. }
            | Self::Mqtt { .. }
            | Self::Nats { .. }
            | Self::ZeroMq { .. }
            | Self::Sqs { .. }
            | Self::Sentry { .. } => true,
            Self::Otel { .. }
            | Self::ClickHouse { .. }
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
            | Self::RabbitMq { .. }
            | Self::Redis { .. }
            | Self::Mqtt { .. }
            | Self::Nats { .. }
            | Self::ZeroMq { .. }
            | Self::Sqs { .. }
            | Self::Sentry { .. }
            | Self::Otel { .. } => None,
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
            | Self::RabbitMq { .. }
            | Self::Redis { .. }
            | Self::Mqtt { .. }
            | Self::Nats { .. }
            | Self::ZeroMq { .. }
            | Self::Sqs { .. }
            | Self::Sentry { .. }
            | Self::Otel { .. }
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
    pub expression: crate::Expression,
}

pub type OtelValueMapping = ClickHouseValueMapping;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OtelSignal {
    Logs,
    Traces,
    Metric(OtelMetric),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtelMetric {
    pub name: String,
    pub unit: String,
    pub description: Option<String>,
    pub kind: OtelMetricKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OtelMetricKind {
    Gauge,
    Sum {
        monotonic: bool,
        temporality: OtelAggregationTemporality,
    },
    Histogram {
        temporality: OtelAggregationTemporality,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum OtelAggregationTemporality {
    Delta,
    Cumulative,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtelScope {
    pub name: String,
    pub version: Option<String>,
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
pub struct CreateClientHttp {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientSentry {
    pub name: Identifier,
    pub mount: Option<Identifier>,
    pub config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateClientOtel {
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
pub type HttpConfigEntry = ClientConfigEntry;
pub type SentryConfigEntry = ClientConfigEntry;
pub type OtelConfigEntry = ClientConfigEntry;
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
pub struct CreateIngestor {
    pub name: Identifier,
    pub output_routes: ProcessorOutputs,
    pub decode_using_codec: Identifier,
    pub timestamp_source: Option<IngestTimestampSource>,
    pub source: IngestSource,
    pub general_error_policy: GeneralErrorPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
}

impl CreateIngestor {
    pub fn apply_alter(&mut self, alter: &AlterIngestor) -> Result<(), AlterIngestorError> {
        if self.name != alter.ingestor {
            return Err(AlterIngestorError::IngestorNameMismatch {
                stored: self.name.clone(),
                requested: alter.ingestor.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.apply_alter_operation(operation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn apply_alter_operation(
        &mut self,
        operation: &AlterIngestorOperation,
    ) -> Result<(), AlterIngestorError> {
        match operation {
            AlterIngestorOperation::SetSource { source } => {
                self.source = source.clone();
            }
            AlterIngestorOperation::SetQuiesce { quiesce } => {
                self.source.set_quiesce(quiesce.clone())?;
            }
            AlterIngestorOperation::SetDecodeUsing { codec } => {
                self.decode_using_codec = codec.clone();
            }
            AlterIngestorOperation::SetTimestamp { source } => {
                self.timestamp_source = Some(source.clone());
            }
            AlterIngestorOperation::DropTimestamp => {
                self.timestamp_source = None;
            }
            AlterIngestorOperation::SetFilterWhere { where_clause } => {
                self.filter_where = Some(where_clause.clone());
            }
            AlterIngestorOperation::DropFilterWhere => {
                self.filter_where = None;
            }
            AlterIngestorOperation::AddRoute { route } => {
                self.output_routes.routes.push(route.clone());
            }
            AlterIngestorOperation::DropRoute { relay } => {
                let index = self.unique_route_index(relay)?;
                if self.output_routes.routes.len() == 1 {
                    return Err(AlterIngestorError::CannotDropLastRoute);
                }
                self.output_routes.routes.remove(index);
            }
            AlterIngestorOperation::ReplaceRoute { route } => {
                let index = self.unique_route_index(&route.relay)?;
                self.output_routes.routes[index] = route.clone();
            }
            AlterIngestorOperation::SetGeneralError { policy } => {
                self.general_error_policy = policy.clone();
            }
        }
        Ok(())
    }

    fn unique_route_index(&self, relay: &Identifier) -> Result<usize, AlterIngestorError> {
        let mut indexes = self
            .output_routes
            .routes
            .iter()
            .enumerate()
            .filter_map(|(index, route)| (route.relay == *relay).then_some(index));
        let Some(index) = indexes.next() else {
            return Err(AlterIngestorError::RouteTargetNotFound {
                relay: relay.clone(),
            });
        };
        if indexes.next().is_some() {
            return Err(AlterIngestorError::RouteTargetAmbiguous {
                relay: relay.clone(),
            });
        }
        Ok(index)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterIngestor {
    pub ingestor: Identifier,
    pub operations: Vec<AlterIngestorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterIngestorOperation {
    SetSource { source: IngestSource },
    SetQuiesce { quiesce: IngestQuiesceMode },
    SetDecodeUsing { codec: Identifier },
    SetTimestamp { source: IngestTimestampSource },
    DropTimestamp,
    SetFilterWhere { where_clause: crate::Expression },
    DropFilterWhere,
    AddRoute { route: ProcessorOutput },
    DropRoute { relay: Identifier },
    ReplaceRoute { route: ProcessorOutput },
    SetGeneralError { policy: GeneralErrorPolicy },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterIngestorError {
    #[error("ALTER targets ingestor `{requested}`, but the stored ingestor is `{stored}`")]
    IngestorNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error("route target `{relay}` is not configured")]
    RouteTargetNotFound { relay: Identifier },
    #[error("route target `{relay}` is ambiguous because it is configured more than once")]
    RouteTargetAmbiguous { relay: Identifier },
    #[error("an ingestor must retain at least one route")]
    CannotDropLastRoute,
    #[error("{transport} ingestors do not support ON QUIESCE {mode}")]
    UnsupportedQuiesceMode { transport: String, mode: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorOutput {
    pub relay: Identifier,
    #[serde(default)]
    pub construction: crate::RouteConstruction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_policy: Option<OutputFlushPolicy>,
    pub message_error_policy: MessageErrorPolicy,
    pub branch: Option<crate::OutputBranch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputFlushPolicy {
    pub flush_each: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputCollectPolicy {
    pub collect_for: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorInputWhere {
    pub relay: Identifier,
    pub where_clause: crate::Expression,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorInputs {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<Identifier>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub r#where: Vec<ProcessorInputWhere>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collect_policy: Option<InputCollectPolicy>,
}

impl ProcessorInputs {
    pub fn new(from: Vec<Identifier>, r#where: Vec<ProcessorInputWhere>) -> Self {
        Self {
            from,
            r#where,
            collect_policy: None,
        }
    }

    pub fn single(relay: Identifier) -> Self {
        Self {
            from: vec![relay],
            r#where: Vec::new(),
            collect_policy: None,
        }
    }

    pub fn with_collect_policy(
        mut self,
        collect_for: String,
        max_batch_size: Option<String>,
    ) -> Self {
        self.collect_policy = Some(InputCollectPolicy {
            collect_for,
            max_batch_size,
        });
        self
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
            construction: crate::RouteConstruction::default(),
            flush_policy: None,
            message_error_policy: MessageErrorPolicy::Log,
            branch: None,
        }
    }

    pub fn with_flush_policy(
        relay: Identifier,
        flush_each: String,
        max_batch_size: Option<String>,
    ) -> Self {
        Self {
            relay,
            construction: crate::RouteConstruction::default(),
            flush_policy: Some(OutputFlushPolicy {
                flush_each,
                max_batch_size,
            }),
            message_error_policy: MessageErrorPolicy::Log,
            branch: None,
        }
    }

    pub fn with_branch(mut self, branch: crate::OutputBranch) -> Self {
        self.branch = Some(branch);
        self
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

    pub fn with_flush_policy(mut self, flush_each: String, max_batch_size: Option<String>) -> Self {
        for output in &mut self.routes {
            output.flush_policy = Some(OutputFlushPolicy {
                flush_each: flush_each.clone(),
                max_batch_size: max_batch_size.clone(),
            });
        }
        self
    }

    pub fn with_branch(mut self, branch: crate::OutputBranch) -> Self {
        for output in &mut self.routes {
            output.branch = Some(branch.clone());
        }
        self
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
    pub mode: AckMode,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterReingestor {
    pub reingestor: Identifier,
    pub operations: Vec<AlterProcessorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterReingestorError {
    #[error("ALTER targets reingestor `{requested}`, but the stored reingestor is `{stored}`")]
    ReingestorNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error(transparent)]
    Processor(#[from] AlterProcessorError),
}

impl CreateReingestor {
    pub fn apply_alter(&mut self, alter: &AlterReingestor) -> Result<(), AlterReingestorError> {
        if self.name != alter.reingestor {
            return Err(AlterReingestorError::ReingestorNameMismatch {
                stored: self.name.clone(),
                requested: alter.reingestor.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.processor_alter_target().apply(operation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn processor_alter_target(&mut self) -> ProcessorAlterTarget<'_> {
        ProcessorAlterTarget {
            from: &mut self.from,
            output_routes: &mut self.output_routes,
            branched_by: None,
            mode: &mut self.mode,
            filter_where: &mut self.filter_where,
            materialized_state: &mut self.materialized_state,
        }
    }
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
    pub output_schema: Vec<InferencerTensorDeclaration>,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

impl CreateInferencer {
    pub fn execution_mode(&self) -> Result<InferencerExecutionMode, InferencerTensorSchemaError> {
        let mut execution_mode = None;
        for (tensor, schema) in self
            .inputs
            .iter()
            .map(|mapping| (mapping.tensor.as_str(), &mapping.schema))
            .chain(
                self.output_schema
                    .iter()
                    .map(|declaration| (declaration.tensor.as_str(), &declaration.schema)),
            )
        {
            let batch_axis_count = schema.batch_axis_count();
            if batch_axis_count > 1 {
                return Err(InferencerTensorSchemaError::MultipleBatchAxes {
                    tensor: tensor.to_string(),
                });
            }
            if schema.fixed_element_count().is_none() {
                return Err(InferencerTensorSchemaError::ElementCountOverflow {
                    tensor: tensor.to_string(),
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
    pub limits: WasmProcessorLimits,
    pub global_error_policy: GeneralErrorPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmProcessorLimits {
    pub max_fuel: u64,
    pub max_memory_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferencerTensorMapping {
    pub tensor: String,
    pub schema: InferencerTensorSchema,
    pub expression: crate::Expression,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferencerTensorDeclaration {
    pub tensor: String,
    pub schema: InferencerTensorSchema,
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

    pub fn message_type(&self) -> ParseAsType {
        self.dimensions
            .iter()
            .rev()
            .fold(ParseAsType::F32, |element, dimension| match dimension {
                InferencerTensorDimension::Fixed(len) => ParseAsType::Array {
                    element: Box::new(element),
                    len: *len,
                },
                InferencerTensorDimension::Dynamic => ParseAsType::Vec {
                    element: Box::new(element),
                },
                InferencerTensorDimension::Batch => element,
            })
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
        matches!(self, Self::Batch)
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
    pub format: SignalingWireFormat,
    pub on_connect: SignalingProtocolOnConnect,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum SignalingWireFormat {
    Json,
    Yaml,
    Toml,
    Xml,
    Cbor,
    Raw,
    Protobuf(SignalingProtobufConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalingProtobufConfig {
    pub resource: Identifier,
    pub resource_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<ClientConfigEntry>,
    pub send_message: String,
    pub wait_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalingProtocolOnConnect {
    /// Whether payload streams to the relay from the moment the connection opens.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub accept_data: bool,
    /// Handshake steps, executed strictly in written order.
    pub steps: Vec<SignalingStep>,
    /// Matchers that abort the handshake during any step.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fail_matchers: Vec<String>,
    pub timeout: String,
}

/// One step of a handshake: frames written, or an outcome waited for.
///
/// A step completes before the next begins, which is what makes a request able to depend on an
/// earlier reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalingStep {
    Send(Vec<String>),
    Wait(SignalingWaitStep),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalingWaitStep {
    /// Matchers that must all be satisfied, in any arrival order, for the step to complete.
    pub matchers: Vec<String>,
    /// Program merged into the handshake state, valid only for a single-matcher step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<String>,
    /// Matchers that abort the handshake while this step is waiting.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fail_matchers: Vec<String>,
    /// Whether completing this step starts streaming payload to the relay.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub accept_data: bool,
}

impl SignalingWaitStep {
    pub fn new(matchers: Vec<String>) -> Self {
        Self {
            matchers,
            capture: None,
            fail_matchers: Vec::new(),
            accept_data: false,
        }
    }
}

impl SignalingProtocolOnConnect {
    pub fn wait_steps(&self) -> impl Iterator<Item = &SignalingWaitStep> {
        self.steps.iter().filter_map(|step| match step {
            SignalingStep::Wait(wait) => Some(wait),
            SignalingStep::Send(_) => None,
        })
    }

    pub fn sends(&self) -> impl Iterator<Item = &String> {
        self.steps
            .iter()
            .filter_map(|step| match step {
                SignalingStep::Send(programs) => Some(programs),
                SignalingStep::Wait(_) => None,
            })
            .flatten()
    }

    /// Whether payload ever starts streaming before the handshake finishes.
    pub fn accepts_data_during_handshake(&self) -> bool {
        self.accept_data || self.wait_steps().any(|wait| wait.accept_data)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum IngestSource {
    Http {
        client: Identifier,
        every: String,
        quiesce: IngestQuiesceMode,
    },
    Kafka {
        client: Identifier,
        topic: Identifier,
        offset_mode: KafkaOffsetMode,
        instances: u64,
        mode: KafkaIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Pulsar {
        client: Identifier,
        topic: Identifier,
        subscription: Identifier,
        instances: u64,
        mode: PulsarIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Mqtt {
        client: Identifier,
        topic: String,
        instances: u64,
        mode: MqttIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Nats {
        client: Identifier,
        subject: Identifier,
        queue_group: Identifier,
        instances: u64,
        mode: NatsIngestMode,
        quiesce: IngestQuiesceMode,
    },
    #[strum(serialize = "RABBITMQ")]
    RabbitMq {
        client: Identifier,
        queue: Identifier,
        instances: u64,
        mode: RabbitMqIngestMode,
        quiesce: IngestQuiesceMode,
    },
    #[strum(serialize = "REDIS")]
    RedisPubSub {
        client: Identifier,
        channel: Identifier,
        mode: RedisPubSubIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Prometheus {
        client: Identifier,
        query: String,
        every: String,
        quiesce: IngestQuiesceMode,
    },
    #[strum(serialize = "ZEROMQ")]
    ZeroMq {
        client: Identifier,
        mode: ZeroMqIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Sqs {
        client: Identifier,
        queue: Identifier,
        instances: u64,
        mode: SqsIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Endpoint {
        endpoint: Identifier,
        mode: EndpointIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Websockets {
        client: Identifier,
        mode: WebsocketsIngestMode,
        quiesce: IngestQuiesceMode,
    },
}

impl IngestSource {
    pub fn transport_label(&self) -> &str {
        self.as_ref()
    }

    pub fn source_ref(&self) -> &Identifier {
        match self {
            Self::Http { client, .. }
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

    pub fn quiesce(&self) -> &IngestQuiesceMode {
        match self {
            Self::Http { quiesce, .. }
            | Self::Kafka { quiesce, .. }
            | Self::Pulsar { quiesce, .. }
            | Self::Mqtt { quiesce, .. }
            | Self::Nats { quiesce, .. }
            | Self::RabbitMq { quiesce, .. }
            | Self::RedisPubSub { quiesce, .. }
            | Self::Prometheus { quiesce, .. }
            | Self::ZeroMq { quiesce, .. }
            | Self::Sqs { quiesce, .. }
            | Self::Endpoint { quiesce, .. }
            | Self::Websockets { quiesce, .. } => quiesce,
        }
    }

    pub fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        match self {
            Self::Kafka { .. } | Self::Pulsar { .. } | Self::RabbitMq { .. } | Self::Sqs { .. } => {
                matches!(quiesce, IngestQuiesceMode::Suspend)
            }
            Self::Mqtt { mode, .. } => match quiesce {
                IngestQuiesceMode::Suspend => {
                    mode.session() == MqttSession::Persistent && mode.qos() == MqttQos::AtLeastOnce
                }
                IngestQuiesceMode::Buffer { .. } | IngestQuiesceMode::Drop => true,
                IngestQuiesceMode::EndpointBuffer { .. } | IngestQuiesceMode::Reject { .. } => {
                    false
                }
            },
            Self::Nats { .. } | Self::RedisPubSub { .. } | Self::Websockets { .. } => {
                matches!(
                    quiesce,
                    IngestQuiesceMode::Buffer { .. } | IngestQuiesceMode::Drop
                )
            }
            Self::ZeroMq { .. } => matches!(
                quiesce,
                IngestQuiesceMode::Suspend
                    | IngestQuiesceMode::Buffer { .. }
                    | IngestQuiesceMode::Drop
            ),
            Self::Http { .. } | Self::Prometheus { .. } => matches!(
                quiesce,
                IngestQuiesceMode::Suspend | IngestQuiesceMode::Buffer { .. }
            ),
            Self::Endpoint { .. } => matches!(
                quiesce,
                IngestQuiesceMode::EndpointBuffer { .. } | IngestQuiesceMode::Reject { .. }
            ),
        }
    }

    pub fn set_quiesce(&mut self, quiesce: IngestQuiesceMode) -> Result<(), AlterIngestorError> {
        if !self.supports_quiesce(&quiesce) {
            return Err(AlterIngestorError::UnsupportedQuiesceMode {
                transport: self.transport_label().to_string(),
                mode: quiesce.kind_label().to_string(),
            });
        }
        match self {
            Self::Http {
                quiesce: current, ..
            }
            | Self::Kafka {
                quiesce: current, ..
            }
            | Self::Pulsar {
                quiesce: current, ..
            }
            | Self::Mqtt {
                quiesce: current, ..
            }
            | Self::Nats {
                quiesce: current, ..
            }
            | Self::RabbitMq {
                quiesce: current, ..
            }
            | Self::RedisPubSub {
                quiesce: current, ..
            }
            | Self::Prometheus {
                quiesce: current, ..
            }
            | Self::ZeroMq {
                quiesce: current, ..
            }
            | Self::Sqs {
                quiesce: current, ..
            }
            | Self::Endpoint {
                quiesce: current, ..
            }
            | Self::Websockets {
                quiesce: current, ..
            } => *current = quiesce,
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestQuiesceOverflow {
    DropOldest,
    DropNewest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestQuiesceMode {
    Suspend,
    Buffer {
        max_size: String,
        overflow: IngestQuiesceOverflow,
    },
    Drop,
    Reject {
        retry_after: String,
    },
    EndpointBuffer {
        max_size: String,
    },
}

impl IngestQuiesceMode {
    pub const fn kind_label(&self) -> &'static str {
        match self {
            Self::Suspend => "SUSPEND",
            Self::Buffer { .. } | Self::EndpointBuffer { .. } => "BUFFER",
            Self::Drop => "DROP",
            Self::Reject { .. } => "REJECT",
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
pub enum EmitterAckWindow {
    Sequential,
    Parallel { max: u64 },
}

impl EmitterAckWindow {
    pub fn max_in_flight(&self) -> u64 {
        match self {
            Self::Sequential => 1,
            Self::Parallel { max } => *max,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmitterPublishingMode {
    NoAck {
        retry_policy: RetryPolicy,
    },
    BrokerAck {
        window: EmitterAckWindow,
        ack_timeout: String,
        retry_policy: RetryPolicy,
    },
    MqttQos0 {
        retry_policy: RetryPolicy,
    },
    MqttQos1 {
        window: EmitterAckWindow,
        ack_timeout: String,
        retry_policy: RetryPolicy,
    },
    MqttQos2 {
        window: EmitterAckWindow,
        ack_timeout: String,
        retry_policy: RetryPolicy,
    },
    NatsJetStream {
        window: EmitterAckWindow,
        ack_timeout: String,
        retry_policy: RetryPolicy,
    },
    SqsSingle {
        retry_policy: RetryPolicy,
    },
    SqsBatch {
        retry_policy: RetryPolicy,
    },
    RequestAck {
        retry_policy: RetryPolicy,
    },
}

impl EmitterPublishingMode {
    pub fn retry_policy(&self) -> &RetryPolicy {
        match self {
            Self::NoAck { retry_policy }
            | Self::BrokerAck { retry_policy, .. }
            | Self::MqttQos0 { retry_policy }
            | Self::MqttQos1 { retry_policy, .. }
            | Self::MqttQos2 { retry_policy, .. }
            | Self::NatsJetStream { retry_policy, .. }
            | Self::SqsSingle { retry_policy }
            | Self::SqsBatch { retry_policy }
            | Self::RequestAck { retry_policy } => retry_policy,
        }
    }

    pub fn ack_window(&self) -> Option<&EmitterAckWindow> {
        match self {
            Self::BrokerAck { window, .. }
            | Self::MqttQos1 { window, .. }
            | Self::MqttQos2 { window, .. }
            | Self::NatsJetStream { window, .. } => Some(window),
            Self::NoAck { .. }
            | Self::MqttQos0 { .. }
            | Self::SqsSingle { .. }
            | Self::SqsBatch { .. }
            | Self::RequestAck { .. } => None,
        }
    }

    pub fn ack_timeout(&self) -> Option<&str> {
        match self {
            Self::BrokerAck { ack_timeout, .. }
            | Self::MqttQos1 { ack_timeout, .. }
            | Self::MqttQos2 { ack_timeout, .. }
            | Self::NatsJetStream { ack_timeout, .. } => Some(ack_timeout),
            Self::NoAck { .. }
            | Self::MqttQos0 { .. }
            | Self::SqsSingle { .. }
            | Self::SqsBatch { .. }
            | Self::RequestAck { .. } => None,
        }
    }

    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::NoAck { .. } => "NO_ACK",
            Self::BrokerAck { .. } => "ACK",
            Self::MqttQos0 { .. } => "QOS 0",
            Self::MqttQos1 { .. } => "QOS 1 ACK",
            Self::MqttQos2 { .. } => "QOS 2 ACK",
            Self::NatsJetStream { .. } => "JETSTREAM ACK",
            Self::SqsSingle { .. } => "SINGLE",
            Self::SqsBatch { .. } => "BATCH",
            Self::RequestAck { .. } => "ACK",
        }
    }
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
    NoAckParallel,
}

pub type PulsarIngestMode = KafkaIngestMode;

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
    pub fn apply_alter(&mut self, alter: &AlterRelay) -> Result<(), AlterRelayError> {
        if self.name != alter.relay {
            return Err(AlterRelayError::RelayNameMismatch {
                stored: self.name.clone(),
                requested: alter.relay.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.apply_alter_operation(operation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn apply_alter_operation(
        &mut self,
        operation: &AlterRelayOperation,
    ) -> Result<(), AlterRelayError> {
        match operation {
            AlterRelayOperation::SetCapacity { capacity } => {
                if *capacity == 0 {
                    return Err(AlterRelayError::InvalidCapacity);
                }
                self.buffer = *capacity;
            }
            AlterRelayOperation::SetSchema { schema } => {
                self.schema = schema.clone();
            }
            AlterRelayOperation::SetBranching { branching } => {
                self.branching = branching.clone();
            }
            AlterRelayOperation::SetMaterializedState => {
                self.materialized_state = Some(MaterializedRelayState::LastByTimestamp);
            }
            AlterRelayOperation::DropMaterializedState => {
                if self.materialized_state.take().is_none() {
                    return Err(AlterRelayError::MaterializedStateNotConfigured);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterRelay {
    pub relay: Identifier,
    pub operations: Vec<AlterRelayOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterRelayOperation {
    SetCapacity { capacity: usize },
    SetSchema { schema: Identifier },
    SetBranching { branching: RelayBranching },
    SetMaterializedState,
    DropMaterializedState,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterRelayError {
    #[error("ALTER targets relay `{requested}`, but the stored relay is `{stored}`")]
    RelayNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error("relay capacity must be greater than 0")]
    InvalidCapacity,
    #[error("relay materialized state is not configured")]
    MaterializedStateNotConfigured,
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
    pub placement_groups: Vec<PlacementGroupSchedule>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlacementRuntimeNode {
    pub kind: ModelKind,
    pub identifier: Identifier,
}

impl PlacementRuntimeNode {
    pub fn new(kind: ModelKind, identifier: Identifier) -> Self {
        Self { kind, identifier }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementGroupSchedule {
    pub members: Vec<PlacementRuntimeNode>,
    pub primary_node: Option<String>,
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
    #[serde(default)]
    pub schema_fingerprint: [u8; 32],
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
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterJunction {
    pub junction: Identifier,
    pub operations: Vec<AlterProcessorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterJunctionError {
    #[error("ALTER targets junction `{requested}`, but the stored junction is `{stored}`")]
    JunctionNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error(transparent)]
    Processor(#[from] AlterProcessorError),
}

impl CreateJunction {
    pub fn apply_alter(&mut self, alter: &AlterJunction) -> Result<(), AlterJunctionError> {
        if self.name != alter.junction {
            return Err(AlterJunctionError::JunctionNameMismatch {
                stored: self.name.clone(),
                requested: alter.junction.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.processor_alter_target().apply(operation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn processor_alter_target(&mut self) -> ProcessorAlterTarget<'_> {
        ProcessorAlterTarget {
            from: &mut self.from,
            output_routes: &mut self.output_routes,
            branched_by: Some(&mut self.branched_by),
            mode: &mut self.mode,
            filter_where: &mut self.filter_where,
            materialized_state: &mut self.materialized_state,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateDeduplicator {
    pub name: Identifier,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub deduplicate_on: Vec<crate::Expression>,
    pub max_time: String,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterDeduplicator {
    pub deduplicator: Identifier,
    pub operations: Vec<AlterDeduplicatorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterDeduplicatorOperation {
    Processor(Box<AlterProcessorOperation>),
    SetDeduplicateOn { expressions: Vec<crate::Expression> },
    SetMaxTime { max_time: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterDeduplicatorError {
    #[error("ALTER targets deduplicator `{requested}`, but the stored deduplicator is `{stored}`")]
    DeduplicatorNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error(transparent)]
    Processor(#[from] AlterProcessorError),
}

impl CreateDeduplicator {
    pub fn apply_alter(&mut self, alter: &AlterDeduplicator) -> Result<(), AlterDeduplicatorError> {
        if self.name != alter.deduplicator {
            return Err(AlterDeduplicatorError::DeduplicatorNameMismatch {
                stored: self.name.clone(),
                requested: alter.deduplicator.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            match operation {
                AlterDeduplicatorOperation::Processor(operation) => {
                    candidate.processor_alter_target().apply(operation)?;
                }
                AlterDeduplicatorOperation::SetDeduplicateOn { expressions } => {
                    candidate.deduplicate_on = expressions.clone();
                }
                AlterDeduplicatorOperation::SetMaxTime { max_time } => {
                    candidate.max_time = max_time.clone();
                }
            }
        }
        *self = candidate;
        Ok(())
    }

    fn processor_alter_target(&mut self) -> ProcessorAlterTarget<'_> {
        ProcessorAlterTarget {
            from: &mut self.from,
            output_routes: &mut self.output_routes,
            branched_by: Some(&mut self.branched_by),
            mode: &mut self.mode,
            filter_where: &mut self.filter_where,
            materialized_state: &mut self.materialized_state,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCorrelator {
    pub name: Identifier,
    pub left: ProcessorInputs,
    pub right: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub correlate_where: crate::Expression,
    pub match_policy: CorrelatorMatchPolicy,
    pub max_time: String,
    pub timeout_policy: CorrelationTimeoutPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
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
    pub order_by: Vec<crate::Expression>,
    pub max_time: String,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterReorderer {
    pub reorderer: Identifier,
    pub operations: Vec<AlterReordererOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterReordererOperation {
    Processor(Box<AlterProcessorOperation>),
    SetOrderBy { expressions: Vec<crate::Expression> },
    SetMaxTime { max_time: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterReordererError {
    #[error("ALTER targets reorderer `{requested}`, but the stored reorderer is `{stored}`")]
    ReordererNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error(transparent)]
    Processor(#[from] AlterProcessorError),
}

impl CreateReorderer {
    pub fn apply_alter(&mut self, alter: &AlterReorderer) -> Result<(), AlterReordererError> {
        if self.name != alter.reorderer {
            return Err(AlterReordererError::ReordererNameMismatch {
                stored: self.name.clone(),
                requested: alter.reorderer.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            match operation {
                AlterReordererOperation::Processor(operation) => {
                    candidate.processor_alter_target().apply(operation)?;
                }
                AlterReordererOperation::SetOrderBy { expressions } => {
                    candidate.order_by = expressions.clone();
                }
                AlterReordererOperation::SetMaxTime { max_time } => {
                    candidate.max_time = max_time.clone();
                }
            }
        }
        *self = candidate;
        Ok(())
    }

    fn processor_alter_target(&mut self) -> ProcessorAlterTarget<'_> {
        ProcessorAlterTarget {
            from: &mut self.from,
            output_routes: &mut self.output_routes,
            branched_by: Some(&mut self.branched_by),
            mode: &mut self.mode,
            filter_where: &mut self.filter_where,
            materialized_state: &mut self.materialized_state,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterProcessorOperation {
    AddFrom {
        relay: Identifier,
        where_clause: Option<crate::Expression>,
    },
    DropFrom {
        relay: Identifier,
    },
    AlterFromSetWhere {
        relay: Identifier,
        where_clause: crate::Expression,
    },
    AlterFromDropWhere {
        relay: Identifier,
    },
    SetCollect {
        policy: InputCollectPolicy,
    },
    DropCollect,
    SetFilterWhere {
        where_clause: crate::Expression,
    },
    DropFilterWhere,
    SetMode {
        mode: AckMode,
    },
    SetBranching {
        branching: BranchSelection,
    },
    AddMaterializedState {
        dependency: crate::MaterializedStateDependency,
    },
    DropMaterializedState {
        relay: Identifier,
    },
    AlterMaterializedState {
        relay: Identifier,
        policy: crate::MaterializedStatePolicy,
    },
    AddRoute {
        route: ProcessorOutput,
    },
    DropRoute {
        relay: Identifier,
    },
    ReplaceRoute {
        route: ProcessorOutput,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterProcessorError {
    #[error("input relay `{relay}` is already configured")]
    InputAlreadyExists { relay: Identifier },
    #[error("input relay `{relay}` is not configured")]
    InputNotFound { relay: Identifier },
    #[error("input relay `{relay}` has no WHERE clause")]
    InputWhereNotConfigured { relay: Identifier },
    #[error("a processor must retain at least one input")]
    CannotDropLastInput,
    #[error("materialized-state dependency `{relay}` is already configured")]
    MaterializedStateAlreadyConfigured { relay: Identifier },
    #[error("materialized-state dependency `{relay}` is not configured")]
    MaterializedStateNotConfigured { relay: Identifier },
    #[error("route target `{relay}` is not configured")]
    RouteTargetNotFound { relay: Identifier },
    #[error("route target `{relay}` is ambiguous because it is configured more than once")]
    RouteTargetAmbiguous { relay: Identifier },
    #[error("a processor must retain at least one route")]
    CannotDropLastRoute,
    #[error("this processor configures branching per route")]
    BranchingUnsupported,
}

struct ProcessorAlterTarget<'a> {
    from: &'a mut ProcessorInputs,
    output_routes: &'a mut ProcessorOutputs,
    branched_by: Option<&'a mut BranchSelection>,
    mode: &'a mut AckMode,
    filter_where: &'a mut Option<crate::Expression>,
    materialized_state: &'a mut Vec<crate::MaterializedStateDependency>,
}

impl ProcessorAlterTarget<'_> {
    fn apply(&mut self, operation: &AlterProcessorOperation) -> Result<(), AlterProcessorError> {
        match operation {
            AlterProcessorOperation::AddFrom {
                relay,
                where_clause,
            } => {
                self.ensure_input_absent(relay)?;
                self.from.from.push(relay.clone());
                if let Some(where_clause) = where_clause {
                    self.from.r#where.push(ProcessorInputWhere {
                        relay: relay.clone(),
                        where_clause: where_clause.clone(),
                    });
                }
            }
            AlterProcessorOperation::DropFrom { relay } => {
                let index = self.input_index(relay)?;
                if self.from.from.len() == 1 {
                    return Err(AlterProcessorError::CannotDropLastInput);
                }
                self.from.from.remove(index);
                self.from
                    .r#where
                    .retain(|input_where| input_where.relay != *relay);
            }
            AlterProcessorOperation::AlterFromSetWhere {
                relay,
                where_clause,
            } => {
                self.input_index(relay)?;
                if let Some(input_where) = self
                    .from
                    .r#where
                    .iter_mut()
                    .find(|input_where| input_where.relay == *relay)
                {
                    input_where.where_clause = where_clause.clone();
                } else {
                    self.from.r#where.push(ProcessorInputWhere {
                        relay: relay.clone(),
                        where_clause: where_clause.clone(),
                    });
                }
            }
            AlterProcessorOperation::AlterFromDropWhere { relay } => {
                self.input_index(relay)?;
                let Some(index) = self
                    .from
                    .r#where
                    .iter()
                    .position(|input_where| input_where.relay == *relay)
                else {
                    return Err(AlterProcessorError::InputWhereNotConfigured {
                        relay: relay.clone(),
                    });
                };
                self.from.r#where.remove(index);
            }
            AlterProcessorOperation::SetCollect { policy } => {
                self.from.collect_policy = Some(policy.clone());
            }
            AlterProcessorOperation::DropCollect => {
                self.from.collect_policy = None;
            }
            AlterProcessorOperation::SetFilterWhere { where_clause } => {
                *self.filter_where = Some(where_clause.clone());
            }
            AlterProcessorOperation::DropFilterWhere => {
                *self.filter_where = None;
            }
            AlterProcessorOperation::SetMode { mode } => {
                *self.mode = *mode;
            }
            AlterProcessorOperation::SetBranching { branching } => {
                let Some(branched_by) = self.branched_by.as_deref_mut() else {
                    return Err(AlterProcessorError::BranchingUnsupported);
                };
                *branched_by = branching.clone();
            }
            AlterProcessorOperation::AddMaterializedState { dependency } => {
                if self
                    .materialized_state
                    .iter()
                    .any(|existing| existing.relay == dependency.relay)
                {
                    return Err(AlterProcessorError::MaterializedStateAlreadyConfigured {
                        relay: dependency.relay.clone(),
                    });
                }
                self.materialized_state.push(dependency.clone());
            }
            AlterProcessorOperation::DropMaterializedState { relay } => {
                let index = self.materialized_state_index(relay)?;
                self.materialized_state.remove(index);
            }
            AlterProcessorOperation::AlterMaterializedState { relay, policy } => {
                let index = self.materialized_state_index(relay)?;
                self.materialized_state[index].policy = policy.clone();
            }
            AlterProcessorOperation::AddRoute { route } => {
                self.output_routes.routes.push(route.clone());
            }
            AlterProcessorOperation::DropRoute { relay } => {
                let index = self.unique_route_index(relay)?;
                if self.output_routes.routes.len() == 1 {
                    return Err(AlterProcessorError::CannotDropLastRoute);
                }
                self.output_routes.routes.remove(index);
            }
            AlterProcessorOperation::ReplaceRoute { route } => {
                let index = self.unique_route_index(&route.relay)?;
                self.output_routes.routes[index] = route.clone();
            }
        }
        Ok(())
    }

    fn input_index(&self, relay: &Identifier) -> Result<usize, AlterProcessorError> {
        self.from
            .from
            .iter()
            .position(|candidate| candidate == relay)
            .ok_or_else(|| AlterProcessorError::InputNotFound {
                relay: relay.clone(),
            })
    }

    fn ensure_input_absent(&self, relay: &Identifier) -> Result<(), AlterProcessorError> {
        if self.from.from.iter().any(|candidate| candidate == relay) {
            Err(AlterProcessorError::InputAlreadyExists {
                relay: relay.clone(),
            })
        } else {
            Ok(())
        }
    }

    fn materialized_state_index(&self, relay: &Identifier) -> Result<usize, AlterProcessorError> {
        self.materialized_state
            .iter()
            .position(|dependency| dependency.relay == *relay)
            .ok_or_else(|| AlterProcessorError::MaterializedStateNotConfigured {
                relay: relay.clone(),
            })
    }

    fn unique_route_index(&self, relay: &Identifier) -> Result<usize, AlterProcessorError> {
        let mut indexes = self
            .output_routes
            .routes
            .iter()
            .enumerate()
            .filter_map(|(index, route)| (route.relay == *relay).then_some(index));
        let Some(index) = indexes.next() else {
            return Err(AlterProcessorError::RouteTargetNotFound {
                relay: relay.clone(),
            });
        };
        if indexes.next().is_some() {
            return Err(AlterProcessorError::RouteTargetAmbiguous {
                relay: relay.clone(),
            });
        }
        Ok(index)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWindowProcessor {
    pub name: Identifier,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub width: WindowBound,
    pub step: WindowBound,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowBound {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,
}

impl WindowBound {
    /// A bound counted in messages alone.
    pub fn of_messages(messages: u64) -> Self {
        Self {
            messages: Some(messages),
            duration: None,
        }
    }

    /// A bound measured in time alone.
    pub fn of_duration(duration: String) -> Self {
        Self {
            messages: None,
            duration: Some(duration),
        }
    }

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
        AckMode, AlterDeduplicator, AlterDeduplicatorError, AlterDeduplicatorOperation,
        AlterEmitter, AlterEmitterError, AlterEmitterOperation, AlterGenerator,
        AlterGeneratorError, AlterGeneratorOperation, AlterIngestor, AlterIngestorError,
        AlterIngestorOperation, AlterJunction, AlterJunctionError, AlterPlacement,
        AlterPlacementError, AlterPlacementOperation, AlterProcessorError, AlterProcessorOperation,
        AlterReingestor, AlterReingestorError, AlterRelay, AlterRelayError, AlterRelayOperation,
        AlterReorderer, AlterReordererError, AlterReordererOperation, BranchSelection,
        ClusterSchedule, CreateDeduplicator, CreateEmitter, CreateGenerator, CreatePlacement,
        CreateReingestor, CreateRelay, CreateReorderer, CreateSchema, DomainSchedule, EmitSink,
        EmitterPublishingMode, ErrorPolicies, GeneralErrorPolicy, InferencerTensorDimension,
        InferencerTensorElementType, InferencerTensorRepresentation, InferencerTensorSchema,
        KafkaPartitionSchedule, MaterializedRelayState, Model, ModelKind, OutputFlushPolicy,
        PlacementPolicy, RelayBranching, RetryPolicy, ScheduledNode,
    };
    use crate::{
        CreateIngestor, CreateJunction, Domain, EndpointIngestMode, Expression, Identifier,
        IngestQuiesceMode, IngestSource, Literal, MaterializedStateDependency,
        MaterializedStatePolicy, ParseAsType, ProcessorInputs, ProcessorOutput, ProcessorOutputs,
        SchemaField,
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
            (
                ModelKind::WireJsonSchema,
                "ref:wire_json_schema",
                "wire_json_schema",
            ),
            (
                ModelKind::WireCborSchema,
                "ref:wire_cbor_schema",
                "wire_cbor_schema",
            ),
            (
                ModelKind::WireAvroSchema,
                "ref:wire_avro_schema",
                "wire_avro_schema",
            ),
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
            (ModelKind::Placement, "ref:placement", "placement"),
            (ModelKind::Udf, "ref:udf", "udf"),
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
        assert_eq!(schema.message_type(), exact);
    }

    #[test]
    fn cluster_schedule_returns_matching_domain() {
        let alpha = DomainSchedule {
            domain: domain("alpha"),
            nodes: Vec::new(),
            placement_groups: Vec::new(),
        };
        let beta = DomainSchedule {
            domain: domain("beta"),
            nodes: Vec::new(),
            placement_groups: Vec::new(),
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
            schema_fingerprint: [0; 32],
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
            schema_fingerprint: [0; 32],
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
            schema_fingerprint: [0; 32],
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
                output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                    identifier("orders_out"),
                    "100ms".to_string(),
                    Some("1MiB".to_string()),
                )]),
                branched_by: BranchSelection::unbranched(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            })),
            effective_branching: None,
            effective_branching_schema: None,
            schema_fingerprint: [0; 32],
            kafka_partition_schedule: None,
            primary_node: Some("node-a".to_string()),
            assigned_nodes: vec!["node-a".to_string(), "node-b".to_string()],
        };
        let endpoint_ingestor = ScheduledNode {
            identifier: identifier("orders_http"),
            kind: ModelKind::Ingestor,
            config: Box::new(Model::Ingestor(CreateIngestor {
                name: identifier("orders_http"),
                output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                    identifier("orders_out"),
                    "100ms".to_string(),
                    Some("1MiB".to_string()),
                )]),
                decode_using_codec: identifier("codec"),
                timestamp_source: None,
                source: IngestSource::Endpoint {
                    endpoint: identifier("public_http"),
                    mode: EndpointIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::EndpointBuffer {
                        max_size: "1MiB".to_string(),
                    },
                },
                general_error_policy: GeneralErrorPolicy::Log,

                filter_where: None,
            })),
            effective_branching: None,
            effective_branching_schema: None,
            schema_fingerprint: [0; 32],
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

    #[test]
    fn relay_alter_applies_operations_in_order_and_is_atomic() {
        let mut relay = CreateRelay {
            name: identifier("events"),
            schema: identifier("event_v1"),
            buffer: 1,
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        };
        relay
            .apply_alter(&AlterRelay {
                relay: identifier("events"),
                operations: vec![
                    AlterRelayOperation::SetCapacity { capacity: 8 },
                    AlterRelayOperation::SetSchema {
                        schema: identifier("event_v2"),
                    },
                    AlterRelayOperation::SetCapacity { capacity: 16 },
                    AlterRelayOperation::SetMaterializedState,
                ],
            })
            .expect("relay alter should apply");
        assert_eq!(relay.buffer, 16);
        assert_eq!(relay.schema, identifier("event_v2"));
        assert_eq!(
            relay.materialized_state,
            Some(MaterializedRelayState::LastByTimestamp)
        );

        let before = relay.clone();
        let error = relay
            .apply_alter(&AlterRelay {
                relay: identifier("events"),
                operations: vec![
                    AlterRelayOperation::SetCapacity { capacity: 32 },
                    AlterRelayOperation::DropMaterializedState,
                    AlterRelayOperation::DropMaterializedState,
                ],
            })
            .expect_err("the second drop must fail");
        assert_eq!(error, AlterRelayError::MaterializedStateNotConfigured);
        assert_eq!(relay, before, "failed ALTER must not partially apply");
    }

    #[test]
    fn junction_alter_preserves_order_and_rejects_ambiguous_routes_atomically() {
        let mut junction = CreateJunction {
            name: identifier("route_events"),
            from: ProcessorInputs::new(vec![identifier("incoming")], Vec::new()),
            output_routes: ProcessorOutputs::new(vec![
                ProcessorOutput::with_flush_policy(
                    identifier("accepted"),
                    "100ms".to_string(),
                    Some("1MiB".to_string()),
                ),
                ProcessorOutput::with_flush_policy(
                    identifier("accepted"),
                    "200ms".to_string(),
                    Some("2MiB".to_string()),
                ),
            ]),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        let before = junction.clone();
        let error = junction
            .apply_alter(&AlterJunction {
                junction: identifier("route_events"),
                operations: vec![
                    AlterProcessorOperation::SetMode {
                        mode: AckMode::Detached,
                    },
                    AlterProcessorOperation::DropRoute {
                        relay: identifier("accepted"),
                    },
                ],
            })
            .expect_err("duplicate route targets must be ambiguous");
        assert_eq!(
            error,
            AlterJunctionError::Processor(AlterProcessorError::RouteTargetAmbiguous {
                relay: identifier("accepted"),
            })
        );
        assert_eq!(junction, before, "failed ALTER must not partially apply");
    }

    #[test]
    fn junction_alter_applies_ordered_collection_filter_dependency_and_route_updates() {
        let mut junction = CreateJunction {
            name: identifier("route_events"),
            from: ProcessorInputs::single(identifier("incoming_a")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                identifier("accepted"),
                "100ms".to_string(),
                Some("1MiB".to_string()),
            )]),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        let true_expression = Expression::Literal(Literal::Bool(true));
        let false_expression = Expression::Literal(Literal::Bool(false));
        let replacement = ProcessorOutput::with_flush_policy(
            identifier("accepted"),
            "250ms".to_string(),
            Some("2MiB".to_string()),
        );
        junction
            .apply_alter(&AlterJunction {
                junction: identifier("route_events"),
                operations: vec![
                    AlterProcessorOperation::AddFrom {
                        relay: identifier("incoming_b"),
                        where_clause: Some(true_expression.clone()),
                    },
                    AlterProcessorOperation::AlterFromSetWhere {
                        relay: identifier("incoming_b"),
                        where_clause: false_expression.clone(),
                    },
                    AlterProcessorOperation::SetFilterWhere {
                        where_clause: true_expression.clone(),
                    },
                    AlterProcessorOperation::SetFilterWhere {
                        where_clause: false_expression.clone(),
                    },
                    AlterProcessorOperation::AddMaterializedState {
                        dependency: MaterializedStateDependency {
                            relay: identifier("profiles"),
                            policy: MaterializedStatePolicy::RequiredWait,
                        },
                    },
                    AlterProcessorOperation::AddMaterializedState {
                        dependency: MaterializedStateDependency {
                            relay: identifier("accounts"),
                            policy: MaterializedStatePolicy::RequiredSkip,
                        },
                    },
                    AlterProcessorOperation::AlterMaterializedState {
                        relay: identifier("profiles"),
                        policy: MaterializedStatePolicy::RequiredSkip,
                    },
                    AlterProcessorOperation::AddRoute {
                        route: ProcessorOutput::with_flush_policy(
                            identifier("audit"),
                            "100ms".to_string(),
                            Some("1MiB".to_string()),
                        ),
                    },
                    AlterProcessorOperation::ReplaceRoute {
                        route: replacement.clone(),
                    },
                    AlterProcessorOperation::SetMode {
                        mode: AckMode::Detached,
                    },
                ],
            })
            .expect("ordered junction alter should apply");

        assert_eq!(
            junction.from.from,
            vec![identifier("incoming_a"), identifier("incoming_b")]
        );
        assert_eq!(junction.from.r#where[0].where_clause, false_expression);
        assert_eq!(
            junction.filter_where,
            Some(Expression::Literal(Literal::Bool(false)))
        );
        assert_eq!(
            junction
                .materialized_state
                .iter()
                .map(|dependency| dependency.relay.clone())
                .collect::<Vec<_>>(),
            vec![identifier("profiles"), identifier("accounts")]
        );
        assert_eq!(
            junction.materialized_state[0].policy,
            MaterializedStatePolicy::RequiredSkip
        );
        assert_eq!(
            junction
                .output_routes
                .routes
                .iter()
                .map(|route| route.relay.clone())
                .collect::<Vec<_>>(),
            vec![identifier("accepted"), identifier("audit")]
        );
        assert_eq!(junction.output_routes.routes[0], replacement);
        assert_eq!(junction.mode, AckMode::Detached);
    }

    #[test]
    fn relay_alter_reports_each_typed_error() {
        let relay = CreateRelay {
            name: identifier("events"),
            schema: identifier("event"),
            buffer: 1,
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        };
        let cases = [
            (
                AlterRelay {
                    relay: identifier("other"),
                    operations: vec![AlterRelayOperation::SetCapacity { capacity: 2 }],
                },
                AlterRelayError::RelayNameMismatch {
                    stored: identifier("events"),
                    requested: identifier("other"),
                },
            ),
            (
                AlterRelay {
                    relay: identifier("events"),
                    operations: vec![AlterRelayOperation::SetCapacity { capacity: 0 }],
                },
                AlterRelayError::InvalidCapacity,
            ),
            (
                AlterRelay {
                    relay: identifier("events"),
                    operations: vec![AlterRelayOperation::DropMaterializedState],
                },
                AlterRelayError::MaterializedStateNotConfigured,
            ),
        ];
        for (alter, expected) in cases {
            let mut candidate = relay.clone();
            assert_eq!(candidate.apply_alter(&alter), Err(expected));
            assert_eq!(candidate, relay);
        }
    }

    #[test]
    fn junction_alter_reports_each_typed_lookup_and_last_element_error() {
        let base = CreateJunction {
            name: identifier("route_events"),
            from: ProcessorInputs::single(identifier("incoming")),
            output_routes: ProcessorOutputs::single(identifier("accepted")),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: vec![MaterializedStateDependency {
                relay: identifier("profiles"),
                policy: MaterializedStatePolicy::RequiredWait,
            }],
        };
        let true_expression = Expression::Literal(Literal::Bool(true));
        let cases = vec![
            (
                AlterJunction {
                    junction: identifier("other"),
                    operations: Vec::new(),
                },
                AlterJunctionError::JunctionNameMismatch {
                    stored: identifier("route_events"),
                    requested: identifier("other"),
                },
            ),
            (
                AlterJunction {
                    junction: identifier("route_events"),
                    operations: vec![AlterProcessorOperation::AddFrom {
                        relay: identifier("incoming"),
                        where_clause: None,
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::InputAlreadyExists {
                    relay: identifier("incoming"),
                }),
            ),
            (
                AlterJunction {
                    junction: identifier("route_events"),
                    operations: vec![AlterProcessorOperation::DropFrom {
                        relay: identifier("missing"),
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::InputNotFound {
                    relay: identifier("missing"),
                }),
            ),
            (
                AlterJunction {
                    junction: identifier("route_events"),
                    operations: vec![AlterProcessorOperation::AlterFromDropWhere {
                        relay: identifier("incoming"),
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::InputWhereNotConfigured {
                    relay: identifier("incoming"),
                }),
            ),
            (
                AlterJunction {
                    junction: identifier("route_events"),
                    operations: vec![AlterProcessorOperation::DropFrom {
                        relay: identifier("incoming"),
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::CannotDropLastInput),
            ),
            (
                AlterJunction {
                    junction: identifier("route_events"),
                    operations: vec![AlterProcessorOperation::AddMaterializedState {
                        dependency: MaterializedStateDependency {
                            relay: identifier("profiles"),
                            policy: MaterializedStatePolicy::RequiredSkip,
                        },
                    }],
                },
                AlterJunctionError::Processor(
                    AlterProcessorError::MaterializedStateAlreadyConfigured {
                        relay: identifier("profiles"),
                    },
                ),
            ),
            (
                AlterJunction {
                    junction: identifier("route_events"),
                    operations: vec![AlterProcessorOperation::AlterMaterializedState {
                        relay: identifier("missing"),
                        policy: MaterializedStatePolicy::RequiredSkip,
                    }],
                },
                AlterJunctionError::Processor(
                    AlterProcessorError::MaterializedStateNotConfigured {
                        relay: identifier("missing"),
                    },
                ),
            ),
            (
                AlterJunction {
                    junction: identifier("route_events"),
                    operations: vec![AlterProcessorOperation::ReplaceRoute {
                        route: ProcessorOutput::new(identifier("missing")),
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::RouteTargetNotFound {
                    relay: identifier("missing"),
                }),
            ),
            (
                AlterJunction {
                    junction: identifier("route_events"),
                    operations: vec![AlterProcessorOperation::DropRoute {
                        relay: identifier("accepted"),
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::CannotDropLastRoute),
            ),
        ];

        for (alter, expected) in cases {
            let mut candidate = base.clone();
            assert_eq!(candidate.apply_alter(&alter), Err(expected));
            assert_eq!(candidate, base);
        }

        let mut with_where = base.clone();
        with_where
            .apply_alter(&AlterJunction {
                junction: identifier("route_events"),
                operations: vec![AlterProcessorOperation::AlterFromSetWhere {
                    relay: identifier("incoming"),
                    where_clause: true_expression,
                }],
            })
            .expect("set WHERE should succeed");
    }

    #[test]
    fn emitter_alter_applies_operations_in_order_and_is_atomic() {
        let mut emitter = CreateEmitter {
            name: identifier("event_sink"),
            from: ProcessorInputs::single(identifier("events")),
            encode_using_codec: Some(identifier("event_codec")),
            sink: Box::new(EmitSink::ZeroMq {
                client: identifier("sink_a"),
            }),
            flush_each: "1s".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode: EmitterPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            mode: AckMode::Attached,
            construction: crate::RouteConstruction::default(),
            materialized_state: Vec::new(),
        };
        emitter
            .apply_alter(&AlterEmitter {
                emitter: identifier("event_sink"),
                operations: vec![
                    AlterEmitterOperation::AddFrom {
                        relay: identifier("backup_events"),
                        where_clause: Some(Expression::Literal(Literal::Bool(true))),
                    },
                    AlterEmitterOperation::AlterFromDropWhere {
                        relay: identifier("backup_events"),
                    },
                    AlterEmitterOperation::SetClient {
                        client: identifier("sink_b"),
                    },
                    AlterEmitterOperation::SetFlush {
                        flush_each: "2s".to_string(),
                        max_batch_size: Some("2MiB".to_string()),
                    },
                    AlterEmitterOperation::SetFlush {
                        flush_each: "IMMEDIATE".to_string(),
                        max_batch_size: None,
                    },
                    AlterEmitterOperation::SetAttachment {
                        mode: AckMode::Detached,
                    },
                ],
            })
            .expect("emitter alter should apply");
        assert_eq!(emitter.sink.client(), &identifier("sink_b"));
        assert_eq!(emitter.flush_policy(), ("IMMEDIATE", None));
        assert_eq!(emitter.mode, AckMode::Detached);
        assert_eq!(
            emitter.from.relays(),
            &[identifier("events"), identifier("backup_events")]
        );
        assert!(emitter.from.where_clauses().is_empty());

        let before = emitter.clone();
        let error = emitter
            .apply_alter(&AlterEmitter {
                emitter: identifier("event_sink"),
                operations: vec![
                    AlterEmitterOperation::SetClient {
                        client: identifier("sink_c"),
                    },
                    AlterEmitterOperation::DropEncode,
                    AlterEmitterOperation::DropEncode,
                ],
            })
            .expect_err("the second codec drop must fail");
        assert_eq!(error, AlterEmitterError::EncodeNotConfigured);
        assert_eq!(emitter, before, "failed ALTER must not partially apply");
    }

    #[test]
    fn emitter_alter_reports_name_and_commit_policy_errors() {
        let emitter = CreateEmitter {
            name: identifier("event_sink"),
            from: ProcessorInputs::single(identifier("events")),
            encode_using_codec: Some(identifier("event_codec")),
            sink: Box::new(EmitSink::ZeroMq {
                client: identifier("sink"),
            }),
            flush_each: "IMMEDIATE".to_string(),
            max_batch_size: None,
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode: EmitterPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            mode: AckMode::Attached,
            construction: crate::RouteConstruction::default(),
            materialized_state: Vec::new(),
        };
        let cases = [
            (
                AlterEmitter {
                    emitter: identifier("other"),
                    operations: Vec::new(),
                },
                AlterEmitterError::EmitterNameMismatch {
                    stored: identifier("event_sink"),
                    requested: identifier("other"),
                },
            ),
            (
                AlterEmitter {
                    emitter: identifier("event_sink"),
                    operations: vec![AlterEmitterOperation::SetCommit {
                        commit_each: "1m".to_string(),
                        max_commit_size: "1GiB".to_string(),
                    }],
                },
                AlterEmitterError::CommitPolicyUnsupported,
            ),
            (
                AlterEmitter {
                    emitter: identifier("event_sink"),
                    operations: vec![AlterEmitterOperation::AddFrom {
                        relay: identifier("events"),
                        where_clause: None,
                    }],
                },
                AlterEmitterError::InputAlreadyExists {
                    relay: identifier("events"),
                },
            ),
            (
                AlterEmitter {
                    emitter: identifier("event_sink"),
                    operations: vec![AlterEmitterOperation::DropFrom {
                        relay: identifier("missing"),
                    }],
                },
                AlterEmitterError::InputNotFound {
                    relay: identifier("missing"),
                },
            ),
            (
                AlterEmitter {
                    emitter: identifier("event_sink"),
                    operations: vec![AlterEmitterOperation::DropFrom {
                        relay: identifier("events"),
                    }],
                },
                AlterEmitterError::CannotDropLastInput,
            ),
        ];
        for (alter, expected) in cases {
            let mut candidate = emitter.clone();
            assert_eq!(candidate.apply_alter(&alter), Err(expected));
            assert_eq!(candidate, emitter);
        }
    }

    #[test]
    fn ingestor_alter_applies_operations_in_order_and_is_atomic() {
        let route = ProcessorOutput {
            relay: identifier("events"),
            construction: crate::RouteConstruction::default(),
            flush_policy: Some(OutputFlushPolicy {
                flush_each: "1s".to_string(),
                max_batch_size: Some("1MiB".to_string()),
            }),
            message_error_policy: super::MessageErrorPolicy::Log,
            branch: Some(crate::OutputBranch::Unbranched),
        };
        let mut ingestor = CreateIngestor {
            name: identifier("event_source"),
            output_routes: ProcessorOutputs::new(vec![route.clone()]),
            decode_using_codec: identifier("event_codec"),
            timestamp_source: None,
            source: IngestSource::Endpoint {
                endpoint: identifier("ingress_a"),
                mode: EndpointIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::EndpointBuffer {
                    max_size: "1MiB".to_string(),
                },
            },
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        };
        ingestor
            .apply_alter(&AlterIngestor {
                ingestor: identifier("event_source"),
                operations: vec![
                    AlterIngestorOperation::SetSource {
                        source: IngestSource::Endpoint {
                            endpoint: identifier("ingress_b"),
                            mode: EndpointIngestMode::NoAckSequential,
                            quiesce: IngestQuiesceMode::EndpointBuffer {
                                max_size: "1MiB".to_string(),
                            },
                        },
                    },
                    AlterIngestorOperation::SetDecodeUsing {
                        codec: identifier("event_codec_v2"),
                    },
                    AlterIngestorOperation::SetTimestamp {
                        source: super::IngestTimestampSource::Now,
                    },
                    AlterIngestorOperation::SetFilterWhere {
                        where_clause: Expression::Literal(Literal::Bool(true)),
                    },
                    AlterIngestorOperation::ReplaceRoute {
                        route: ProcessorOutput {
                            relay: identifier("events"),
                            flush_policy: Some(OutputFlushPolicy {
                                flush_each: "IMMEDIATE".to_string(),
                                max_batch_size: None,
                            }),
                            ..route.clone()
                        },
                    },
                    AlterIngestorOperation::AddRoute {
                        route: ProcessorOutput {
                            relay: identifier("audit"),
                            ..route.clone()
                        },
                    },
                    AlterIngestorOperation::SetGeneralError {
                        policy: GeneralErrorPolicy::Ignore,
                    },
                ],
            })
            .expect("ingestor alter should apply");

        assert_eq!(
            ingestor.source,
            IngestSource::Endpoint {
                endpoint: identifier("ingress_b"),
                mode: EndpointIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::EndpointBuffer {
                    max_size: "1MiB".to_string(),
                },
            }
        );
        assert_eq!(ingestor.decode_using_codec, identifier("event_codec_v2"));
        assert_eq!(
            ingestor.timestamp_source,
            Some(super::IngestTimestampSource::Now)
        );
        assert_eq!(ingestor.output_routes.routes.len(), 2);
        assert_eq!(ingestor.general_error_policy, GeneralErrorPolicy::Ignore);

        let before = ingestor.clone();
        let error = ingestor
            .apply_alter(&AlterIngestor {
                ingestor: identifier("event_source"),
                operations: vec![
                    AlterIngestorOperation::SetDecodeUsing {
                        codec: identifier("event_codec_v3"),
                    },
                    AlterIngestorOperation::DropRoute {
                        relay: identifier("missing"),
                    },
                ],
            })
            .expect_err("missing route target should fail");
        assert_eq!(
            error,
            AlterIngestorError::RouteTargetNotFound {
                relay: identifier("missing")
            }
        );
        assert_eq!(ingestor, before, "failed ALTER must not partially apply");
    }

    #[test]
    fn ingestor_alter_reports_name_ambiguity_and_last_route_errors() {
        let route = ProcessorOutput::new(identifier("events"));
        let base = CreateIngestor {
            name: identifier("event_source"),
            output_routes: ProcessorOutputs::new(vec![route.clone()]),
            decode_using_codec: identifier("event_codec"),
            timestamp_source: None,
            source: IngestSource::Endpoint {
                endpoint: identifier("ingress"),
                mode: EndpointIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::EndpointBuffer {
                    max_size: "1MiB".to_string(),
                },
            },
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        };

        let mut candidate = base.clone();
        assert_eq!(
            candidate.apply_alter(&AlterIngestor {
                ingestor: identifier("other"),
                operations: Vec::new(),
            }),
            Err(AlterIngestorError::IngestorNameMismatch {
                stored: identifier("event_source"),
                requested: identifier("other"),
            })
        );
        assert_eq!(candidate, base);

        let mut candidate = base.clone();
        assert_eq!(
            candidate.apply_alter(&AlterIngestor {
                ingestor: identifier("event_source"),
                operations: vec![AlterIngestorOperation::DropRoute {
                    relay: identifier("events"),
                }],
            }),
            Err(AlterIngestorError::CannotDropLastRoute)
        );
        assert_eq!(candidate, base);

        let mut ambiguous = base.clone();
        ambiguous.output_routes.routes.push(route);
        let before = ambiguous.clone();
        assert_eq!(
            ambiguous.apply_alter(&AlterIngestor {
                ingestor: identifier("event_source"),
                operations: vec![AlterIngestorOperation::DropRoute {
                    relay: identifier("events"),
                }],
            }),
            Err(AlterIngestorError::RouteTargetAmbiguous {
                relay: identifier("events"),
            })
        );
        assert_eq!(ambiguous, before);
    }

    fn deduplicator() -> CreateDeduplicator {
        CreateDeduplicator {
            name: identifier("dedup_events"),
            from: ProcessorInputs::single(identifier("incoming")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(identifier(
                "outgoing",
            ))]),
            branched_by: BranchSelection::unbranched(),
            deduplicate_on: vec![Expression::Literal(Literal::I64(1))],
            max_time: "10m".to_string(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }
    }

    fn reorderer() -> CreateReorderer {
        CreateReorderer {
            name: identifier("order_events"),
            from: ProcessorInputs::single(identifier("incoming")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(identifier(
                "outgoing",
            ))]),
            branched_by: BranchSelection::unbranched(),
            order_by: vec![Expression::Literal(Literal::I64(1))],
            max_time: "10m".to_string(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }
    }

    #[test]
    fn deduplicator_alter_applies_common_and_specific_operations_in_written_order() {
        let mut candidate = deduplicator();
        candidate
            .apply_alter(&AlterDeduplicator {
                deduplicator: identifier("dedup_events"),
                operations: vec![
                    AlterDeduplicatorOperation::Processor(Box::new(
                        AlterProcessorOperation::AddFrom {
                            relay: identifier("secondary"),
                            where_clause: Some(Expression::Literal(Literal::Bool(true))),
                        },
                    )),
                    AlterDeduplicatorOperation::SetDeduplicateOn {
                        expressions: vec![Expression::Literal(Literal::I64(2))],
                    },
                    AlterDeduplicatorOperation::SetDeduplicateOn {
                        expressions: vec![Expression::Literal(Literal::I64(3))],
                    },
                    AlterDeduplicatorOperation::SetMaxTime {
                        max_time: "1m".to_string(),
                    },
                    AlterDeduplicatorOperation::SetMaxTime {
                        max_time: "2m".to_string(),
                    },
                    AlterDeduplicatorOperation::Processor(Box::new(
                        AlterProcessorOperation::SetMode {
                            mode: AckMode::Detached,
                        },
                    )),
                ],
            })
            .expect("deduplicator ALTER should apply");

        assert_eq!(
            candidate.from.from,
            vec![identifier("incoming"), identifier("secondary")]
        );
        assert_eq!(
            candidate.deduplicate_on,
            vec![Expression::Literal(Literal::I64(3))]
        );
        assert_eq!(candidate.max_time, "2m");
        assert_eq!(candidate.mode, AckMode::Detached);
    }

    #[test]
    fn reorderer_alter_applies_common_and_specific_operations_in_written_order() {
        let mut candidate = reorderer();
        candidate
            .apply_alter(&AlterReorderer {
                reorderer: identifier("order_events"),
                operations: vec![
                    AlterReordererOperation::SetOrderBy {
                        expressions: vec![Expression::Literal(Literal::I64(2))],
                    },
                    AlterReordererOperation::SetOrderBy {
                        expressions: vec![Expression::Literal(Literal::I64(3))],
                    },
                    AlterReordererOperation::SetMaxTime {
                        max_time: "1m".to_string(),
                    },
                    AlterReordererOperation::SetMaxTime {
                        max_time: "2m".to_string(),
                    },
                    AlterReordererOperation::Processor(Box::new(
                        AlterProcessorOperation::SetFilterWhere {
                            where_clause: Expression::Literal(Literal::Bool(true)),
                        },
                    )),
                ],
            })
            .expect("reorderer ALTER should apply");

        assert_eq!(
            candidate.order_by,
            vec![Expression::Literal(Literal::I64(3))]
        );
        assert_eq!(candidate.max_time, "2m");
        assert_eq!(
            candidate.filter_where,
            Some(Expression::Literal(Literal::Bool(true)))
        );
    }

    #[test]
    fn processor_alters_are_atomic_and_report_typed_target_errors() {
        let mut deduplicator = deduplicator();
        let original = deduplicator.clone();
        assert_eq!(
            deduplicator.apply_alter(&AlterDeduplicator {
                deduplicator: identifier("dedup_events"),
                operations: vec![
                    AlterDeduplicatorOperation::SetMaxTime {
                        max_time: "1s".to_string(),
                    },
                    AlterDeduplicatorOperation::Processor(Box::new(
                        AlterProcessorOperation::DropRoute {
                            relay: identifier("missing"),
                        },
                    )),
                ],
            }),
            Err(AlterDeduplicatorError::Processor(
                AlterProcessorError::RouteTargetNotFound {
                    relay: identifier("missing"),
                }
            ))
        );
        assert_eq!(deduplicator, original);

        let mut reorderer = reorderer();
        let original = reorderer.clone();
        assert_eq!(
            reorderer.apply_alter(&AlterReorderer {
                reorderer: identifier("other"),
                operations: Vec::new(),
            }),
            Err(AlterReordererError::ReordererNameMismatch {
                stored: identifier("order_events"),
                requested: identifier("other"),
            })
        );
        assert_eq!(reorderer, original);
    }

    #[test]
    fn reingestor_alter_is_ordered_atomic_and_rejects_node_branching() {
        let mut reingestor = CreateReingestor {
            name: identifier("repartition"),
            from: ProcessorInputs::single(identifier("incoming")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(identifier(
                "outgoing",
            ))]),
            mode: AckMode::Attached,
            materialized_state: Vec::new(),
            filter_where: None,
        };
        reingestor
            .apply_alter(&AlterReingestor {
                reingestor: identifier("repartition"),
                operations: vec![
                    AlterProcessorOperation::SetMode {
                        mode: AckMode::Detached,
                    },
                    AlterProcessorOperation::AddFrom {
                        relay: identifier("secondary"),
                        where_clause: Some(Expression::Literal(Literal::Bool(true))),
                    },
                    AlterProcessorOperation::SetFilterWhere {
                        where_clause: Expression::Literal(Literal::Bool(true)),
                    },
                ],
            })
            .expect("reingestor alter should apply");
        assert_eq!(reingestor.mode, AckMode::Detached);
        assert_eq!(reingestor.from.from.len(), 2);
        assert!(reingestor.filter_where.is_some());

        let before = reingestor.clone();
        assert_eq!(
            reingestor.apply_alter(&AlterReingestor {
                reingestor: identifier("repartition"),
                operations: vec![
                    AlterProcessorOperation::SetMode {
                        mode: AckMode::Attached,
                    },
                    AlterProcessorOperation::SetBranching {
                        branching: BranchSelection::unbranched(),
                    },
                ],
            }),
            Err(AlterReingestorError::Processor(
                AlterProcessorError::BranchingUnsupported
            ))
        );
        assert_eq!(reingestor, before, "failed ALTER must not partially apply");
    }

    #[test]
    fn generator_alter_is_ordered_atomic_and_reports_route_errors() {
        let route = ProcessorOutput::new(identifier("outgoing"));
        let mut generator = CreateGenerator {
            name: identifier("synth"),
            materialized_relay: identifier("state"),
            branched_by: BranchSelection::unbranched(),
            each: "1s".to_string(),
            output_routes: ProcessorOutputs::new(vec![route.clone()]),
        };
        generator
            .apply_alter(&AlterGenerator {
                generator: identifier("synth"),
                operations: vec![
                    AlterGeneratorOperation::SetEach {
                        each: "500ms".to_string(),
                    },
                    AlterGeneratorOperation::SetEach {
                        each: "250ms".to_string(),
                    },
                    AlterGeneratorOperation::SetMaterializedState {
                        relay: identifier("state_v2"),
                    },
                    AlterGeneratorOperation::AddRoute {
                        route: ProcessorOutput::new(identifier("audit")),
                    },
                ],
            })
            .expect("generator alter should apply");
        assert_eq!(generator.each, "250ms");
        assert_eq!(generator.materialized_relay, identifier("state_v2"));
        assert_eq!(generator.output_routes.routes.len(), 2);

        let before = generator.clone();
        assert_eq!(
            generator.apply_alter(&AlterGenerator {
                generator: identifier("synth"),
                operations: vec![
                    AlterGeneratorOperation::SetEach {
                        each: "10ms".to_string(),
                    },
                    AlterGeneratorOperation::DropRoute {
                        relay: identifier("missing"),
                    },
                ],
            }),
            Err(AlterGeneratorError::RouteTargetNotFound {
                relay: identifier("missing")
            })
        );
        assert_eq!(generator, before, "failed ALTER must not partially apply");

        let mut single = CreateGenerator {
            output_routes: ProcessorOutputs::new(vec![route.clone()]),
            ..generator
        };
        assert_eq!(
            single.apply_alter(&AlterGenerator {
                generator: identifier("synth"),
                operations: vec![AlterGeneratorOperation::DropRoute {
                    relay: identifier("outgoing"),
                }],
            }),
            Err(AlterGeneratorError::CannotDropLastRoute)
        );

        single.output_routes.routes.push(route);
        assert_eq!(
            single.apply_alter(&AlterGenerator {
                generator: identifier("synth"),
                operations: vec![AlterGeneratorOperation::DropRoute {
                    relay: identifier("outgoing"),
                }],
            }),
            Err(AlterGeneratorError::RouteTargetAmbiguous {
                relay: identifier("outgoing")
            })
        );
    }

    #[test]
    fn placement_creation_collapses_duplicate_members() {
        let placement = CreatePlacement::new(
            identifier("corridor"),
            vec![identifier("ingest"), identifier("ingest")],
            vec![identifier("emit"), identifier("emit")],
            PlacementPolicy::PreferColocation,
            None,
        )
        .expect("placement should be valid");

        assert_eq!(placement.from, vec![identifier("ingest")]);
        assert_eq!(placement.to, vec![identifier("emit")]);
    }

    #[test]
    fn placement_alter_applies_operations_in_order_and_is_atomic() {
        let mut placement = CreatePlacement::new(
            identifier("corridor"),
            vec![identifier("ingest")],
            vec![identifier("emit")],
            PlacementPolicy::PreferColocation,
            None,
        )
        .expect("placement should be valid");
        placement
            .apply_alter(&AlterPlacement {
                placement: identifier("corridor"),
                operations: vec![
                    AlterPlacementOperation::SetRank { rank: 3 },
                    AlterPlacementOperation::SetRank { rank: 1 },
                    AlterPlacementOperation::SetPolicy {
                        policy: PlacementPolicy::RequireColocation,
                    },
                    AlterPlacementOperation::SetMembers {
                        from: vec![identifier("source"), identifier("source")],
                        to: vec![identifier("sink")],
                    },
                    AlterPlacementOperation::RenameTo {
                        name: identifier("critical"),
                    },
                ],
            })
            .expect("placement alter should apply");

        assert_eq!(placement.name, identifier("critical"));
        assert_eq!(placement.rank, Some(1));
        assert_eq!(placement.policy, PlacementPolicy::RequireColocation);
        assert_eq!(placement.from, vec![identifier("source")]);
        assert_eq!(placement.to, vec![identifier("sink")]);

        let before = placement.clone();
        assert_eq!(
            placement.apply_alter(&AlterPlacement {
                placement: identifier("critical"),
                operations: vec![
                    AlterPlacementOperation::SetPolicy {
                        policy: PlacementPolicy::Neutral,
                    },
                    AlterPlacementOperation::SetRank { rank: 0 },
                ],
            }),
            Err(AlterPlacementError::RankZero)
        );
        assert_eq!(placement, before, "failed ALTER must not partially apply");
    }
}
