//! Public semantic models for Plexus statements.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** Typed statement bodies and configuration values after language parsing.
//! - **Depends on.** Vocabulary names, timestamps, clock values and primitive collections.
//! - **Must not know.** Parser recovery state, registry validation, scheduling or runtime tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::{Deref, DerefMut},
};

use error_stack::Report;
use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter, EnumProperty, EnumString, IntoEnumIterator, IntoStaticStr};
use thiserror::Error;

use crate::{
    AlterDeduplicator, AlterEmitter, AlterJunction, AlterReingestor, AlterReorderer, AlterSchema,
    AlterWireSchema, AvroType, Backup, BranchName, CborType, ClientConfigEntry, ClientName,
    ClusterNodeName, CodecName, ConnectorKind, CorrelatorName, CreateAvroWireSchema,
    CreateCborWireSchema, CreateClientAzureBlob, CreateClientClickHouse, CreateClientGcs,
    CreateClientHttp, CreateClientIcebergRest, CreateClientKafka, CreateClientMongoDb,
    CreateClientMqtt, CreateClientMySql, CreateClientNats, CreateClientOtel, CreateClientPostgres,
    CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
    CreateClientS3, CreateClientSentry, CreateClientSqs, CreateClientSyslog,
    CreateClientWebsockets, CreateClientZeroMq, CreateCorrelator, CreateDeduplicator,
    CreateEmitter, CreateInferencer, CreateJsonWireSchema, CreateJunction, CreateReingestor,
    CreateReorderer, CreateSchema, CreateUdf, CreateWasmProcessor, CreateWindowProcessor,
    DeduplicatorName, DomainClockPeriod, DomainClockSkew, DomainClockState, DomainName,
    DomainTimeRate, EmitterName, EndpointName, FieldName, GeneratorName, IngestQuiesceMode,
    IngestorName, JsonType, JunctionName, LookupName, ModelName, NodeRef, PlacementName,
    ProcessorOutput, ProcessorOutputs, RebindResource, ReingestorName, RelayName, ReordererName,
    RequestedResourceVersion, ResetWasmState, ResourceName, Restore, SchemaName,
    SignalingProtocolName, SubscriptionName, Timestamp, TransactionInspectionRequest, UdfName,
    UserName, VhostName, WasmProcessorName, WindowProcessorName, WireSchemaName,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum Statement {
    CreateDomain(CreateStatement<CreateDomain>),
    AlterDomain(AlterDomain),
    CreateUser(CreateStatement<CreateUser>),
    CreateResource(CreateStatement<CreateResource>),
    RebindResource(RebindResource),
    ResetWasmState(ResetWasmState),
    UploadResource(UploadResource),
    Backup(Backup),
    Restore(Restore),
    StartDomain(StartDomain),
    StopDomain(StopDomain),
    Create(CreateStatement<Box<Model<RequestedResourceVersion>>>),
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
    Relocate(Relocation),
    DescribeRelocation(Relocation),
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
    ShowIngestors(ShowIngestors),
    ShowEmitters(ShowEmitters),
    ShowPlacements(ShowPlacements),
    ShowRelayMaterializedState(ShowRelayMaterializedState),
    ShowClusterStatus(ShowClusterStatus),
    ShowTransactions(ShowTransactions),
    DescribeTransaction(DescribeTransaction),
    DescribeCluster(DescribeCluster),
    DescribeClient(DescribeClient),
}

impl Statement {
    pub fn requires_domain_mutation_ownership(&self) -> bool {
        self.is_model_mutation()
            || matches!(
                self,
                Self::CreateDomain(_)
                    | Self::AlterDomain(_)
                    | Self::StartDomain(_)
                    | Self::StopDomain(_)
                    | Self::DrainNode(_)
                    | Self::Relocate(_)
                    | Self::ResetWasmState(_)
            )
    }

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
            | Self::RebindResource(_)
            | Self::Drop(_) => true,
            Self::CreateDomain(_)
            | Self::AlterDomain(_)
            | Self::CreateUser(_)
            | Self::CreateResource(_)
            | Self::UploadResource(_)
            | Self::Backup(_)
            | Self::Restore(_)
            | Self::StartDomain(_)
            | Self::StopDomain(_)
            | Self::DropNode(_)
            | Self::CordonNode(_)
            | Self::UncordonNode(_)
            | Self::DrainNode(_)
            | Self::Relocate(_)
            | Self::DescribeRelocation(_)
            | Self::DescribeRelay(_)
            | Self::DescribeDomain(_)
            | Self::DescribeIngestor(_)
            | Self::DescribeResource(_)
            | Self::DescribeLookup(_)
            | Self::DescribeEndpoint(_)
            | Self::DescribeClient(_)
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
            | Self::ShowIngestors(_)
            | Self::ShowEmitters(_)
            | Self::ShowPlacements(_)
            | Self::ShowRelayMaterializedState(_)
            | Self::ShowClusterStatus(_)
            | Self::ShowTransactions(_)
            | Self::DescribeTransaction(_)
            | Self::DescribeCluster(_)
            | Self::ResetWasmState(_) => false,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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
            .assured("the strum property is declared on every variant of this enum")
    }

    /// The Plexus keyword phrase that names this kind in `DROP` and `SHOW CREATE`.
    pub fn keyword_phrase(self) -> &'static str {
        self.get_str("keyword")
            .assured("the strum property is declared on every variant of this enum")
    }

    pub fn from_completion_label(label: &str) -> Option<Self> {
        Self::iter().find(|kind| kind.completion_label() == label)
    }

    pub fn as_str(self) -> &'static str {
        self.into()
    }

    pub fn is_processor(self) -> bool {
        matches!(
            self,
            Self::Inferencer
                | Self::WasmProcessor
                | Self::Junction
                | Self::Deduplicator
                | Self::Correlator
                | Self::Reorderer
                | Self::WindowProcessor
        )
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ShowCreate {
    pub kind: ModelKind,
    pub name: ModelName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ShowClusterStatus;

/// Describes every live node of the cluster: the prerequisites it brings, and how its timings
/// differ from the leader's.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DescribeCluster;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ShowTransactions;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ShowUdfs;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ShowPlacements;

/// Lists the ingestors of the selected domain with their source, schema or codec, execution owner
/// and state, and the producers each client ingestor has attached.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ShowIngestors;

/// Lists configured emitters of the selected domain with their declared connector and entity.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ShowEmitters;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ShowRelayMaterializedState {
    pub relay: RelayName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateDomain {
    pub id: DomainName,
    pub config: DomainConfig,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterDomain {
    pub policy: PlacementPolicy,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateUser {
    pub name: UserName,
    pub password: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateResource {
    pub identifier: ResourceName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct UploadResource {
    pub identifier: ResourceName,
    pub source_path: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StartDomain {
    pub start: DomainStartPoint,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct StopDomain;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainConfig {
    pub pace: DomainPace,
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
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
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
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE", ascii_case_insensitive)]
pub enum DomainPace {
    Paced {
        period: DomainClockPeriod,
        skew: DomainClockSkew,
    },
    Unpaced,
}

impl DomainPace {
    pub const fn is_paced(self) -> bool {
        matches!(self, Self::Paced { .. })
    }

    pub const fn period(self) -> Option<DomainClockPeriod> {
        match self {
            Self::Paced { period, .. } => Some(period),
            Self::Unpaced => None,
        }
    }

    pub const fn skew(self) -> Option<DomainClockSkew> {
        match self {
            Self::Paced { skew, .. } => Some(skew),
            Self::Unpaced => None,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum DomainStartPoint {
    #[default]
    Resume,
    Now {
        time_rate: DomainTimeRate,
    },
    At {
        timestamp: Timestamp,
        time_rate: DomainTimeRate,
    },
}

impl DomainStartPoint {
    pub const fn resolve_at(&self, wall_started_at: Timestamp) -> (Timestamp, DomainTimeRate) {
        match self {
            Self::Resume => (wall_started_at, DomainTimeRate::ONE),
            Self::Now { time_rate } => (wall_started_at, *time_rate),
            Self::At {
                timestamp,
                time_rate,
            } => (*timestamp, *time_rate),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    AsRefStr,
    EnumString,
    IntoStaticStr,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
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
    pub period: crate::DomainClockPeriod,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainState {
    pub id: DomainName,
    pub config: DomainConfig,
    pub status: DomainStatus,
    pub start_version: u64,
    pub last_start: DomainStartPoint,
    pub clock: Option<DomainClockState>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DropModel {
    pub kind: ModelKind,
    pub name: ModelName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DropNode {
    pub node_id: ClusterNodeName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CordonNode {
    pub node_id: ClusterNodeName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct UncordonNode {
    pub node_id: ClusterNodeName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DrainNode {
    pub node_id: ClusterNodeName,
}

/// One kind-qualified runtime node named by a relocation selection or `FOR` override.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct RelocationMember {
    pub kind: ModelKind,
    pub name: ModelName,
}

impl RelocationMember {
    pub fn new(kind: ModelKind, name: ModelName) -> Self {
        Self { kind, name }
    }

    /// The `<kind> <name>` spelling used in canonical Plexus and in diagnostics.
    pub fn to_plexus(&self) -> String {
        format!("{} {}", self.kind.keyword_phrase(), self.name.as_str())
    }
}

/// How a relocation selects the runtime nodes it moves.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RelocationSelection {
    /// The listed runtime nodes, which need not be connected.
    List(Vec<RelocationMember>),
    /// Every runtime node covered by a directed corridor between the endpoints.
    Corridor {
        from: Vec<RelocationMember>,
        to: Vec<RelocationMember>,
    },
}

impl RelocationSelection {
    /// Every member written in the statement, in written order.
    pub fn members(&self) -> Vec<&RelocationMember> {
        match self {
            Self::List(members) => members.iter().collect(),
            Self::Corridor { from, to } => from.iter().chain(to).collect(),
        }
    }

    fn to_plexus(&self) -> String {
        match self {
            Self::List(members) => format_relocation_members(members),
            Self::Corridor { from, to } => format!(
                "FROM {} TO {}",
                format_relocation_members(from),
                format_relocation_members(to)
            ),
        }
    }
}

/// Whether a hard group's soft preferences shape the relocation plan.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    AsRefStr,
    EnumProperty,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[strum(serialize_all = "lowercase")]
pub enum RelocationPreferenceStrategy {
    #[strum(props(keyword = "FOLLOW PREFERENCES"))]
    Follow,
    #[strum(props(keyword = "IGNORE PREFERENCES"))]
    Ignore,
}

impl RelocationPreferenceStrategy {
    /// The composed Plexus keyword phrase that spells this strategy.
    pub fn keyword_phrase(self) -> &'static str {
        self.get_str("keyword")
            .assured("the strum property is declared on every variant of this enum")
    }

    pub fn follows_preferences(self) -> bool {
        matches!(self, Self::Follow)
    }
}

/// A `FOR <kind> <name> FOLLOW|IGNORE PREFERENCES` clause, which sets the strategy of the named
/// runtime node's whole hard group.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RelocationPreferenceOverride {
    pub member: RelocationMember,
    pub strategy: RelocationPreferenceStrategy,
}

/// The relocation both `RELOCATE` and `DESCRIBE RELOCATION` describe.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct Relocation {
    pub selection: RelocationSelection,
    pub destination: ClusterNodeName,
    pub strategy: RelocationPreferenceStrategy,
    pub overrides: Vec<RelocationPreferenceOverride>,
}

impl Relocation {
    /// Renders the clauses shared by both statements, with the default strategy before any
    /// `FOR` override and members in written order.
    pub fn to_plexus_clauses(&self) -> String {
        let mut rendered = format!(
            "{} ONTO NODE {} {}",
            self.selection.to_plexus(),
            self.destination,
            self.strategy.keyword_phrase()
        );
        for override_clause in &self.overrides {
            rendered.push_str(&format!(
                " FOR {} {}",
                override_clause.member.to_plexus(),
                override_clause.strategy.keyword_phrase()
            ));
        }
        rendered
    }
}

fn format_relocation_members(members: &[RelocationMember]) -> String {
    members
        .iter()
        .map(RelocationMember::to_plexus)
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    AsRefStr,
    EnumIter,
    EnumString,
    IntoStaticStr,
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
    pub name: SubscriptionName,
    pub relay: RelayName,
    #[serde(default = "default_subscription_delivery_behavior")]
    pub delivery_behavior: SubscriptionDeliveryBehavior,
    #[serde(default)]
    pub batch_sample_rate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<crate::Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubscription {
    pub name: SubscriptionName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeRelay {
    pub relay: RelayName,
    pub bindings: Vec<SubscriptionBinding>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DescribeDomain;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeIngestor {
    pub ingestor: IngestorName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeResource {
    pub identifier: ResourceName,
    pub version: Option<u64>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeLookup {
    pub name: LookupName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeJunction {
    pub name: JunctionName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeDeduplicator {
    pub name: DeduplicatorName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeReingestor {
    pub name: ReingestorName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeCorrelator {
    pub name: CorrelatorName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeEndpoint {
    pub name: EndpointName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeClient {
    pub name: ClientName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeReorderer {
    pub name: ReordererName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeEmitter {
    pub name: EmitterName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeWindowProcessor {
    pub name: WindowProcessorName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeWasmProcessor {
    pub name: WasmProcessorName,
    pub format: InspectionFormat,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeUdf {
    pub name: UdfName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribePlacement {
    pub name: PlacementName,
}

/// `DESCRIBE TRANSACTION`: one side-effect-free read of a transaction's impact report.
///
/// The request is exactly what the inspection service consumes, so the statement is answered
/// without a second representation of which transaction and operation it names.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DescribeTransaction {
    pub request: TransactionInspectionRequest,
    pub format: InspectionFormat,
}

/// How an inspection statement renders the typed facts it read. `TEXT` is the default; the typed
/// inspection travels beside either rendering.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum InspectionFormat {
    #[default]
    Text,
    Json,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct LookupQuery {
    pub name: LookupName,
    pub key: SubscriptionLiteral,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SubscriptionBinding {
    pub field: FieldName,
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreatePlacement {
    pub name: PlacementName,
    pub from: Vec<ModelName>,
    pub to: Vec<ModelName>,
    pub policy: PlacementPolicy,
    pub rank: Option<NonZeroU64>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterPlacement {
    pub placement: PlacementName,
    pub operations: Vec<AlterPlacementOperation>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterPlacementOperation {
    SetPolicy {
        policy: PlacementPolicy,
    },
    SetRank {
        rank: NonZeroU64,
    },
    DropRank,
    SetMembers {
        from: Vec<ModelName>,
        to: Vec<ModelName>,
    },
    RenameTo {
        name: PlacementName,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterPlacementError {
    #[error("ALTER targets placement `{requested}`, but the stored placement is `{stored}`")]
    PlacementNameMismatch {
        stored: PlacementName,
        requested: PlacementName,
    },
    #[error("a placement must declare at least one FROM member")]
    EmptyFrom,
    #[error("a placement must declare at least one TO member")]
    EmptyTo,
}

impl CreatePlacement {
    pub fn new(
        name: PlacementName,
        from: Vec<ModelName>,
        to: Vec<ModelName>,
        policy: PlacementPolicy,
        rank: Option<NonZeroU64>,
    ) -> error_stack::Result<Self, AlterPlacementError> {
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

    pub fn apply_alter(
        &mut self,
        alter: &AlterPlacement,
    ) -> error_stack::Result<(), AlterPlacementError> {
        if self.name != alter.placement {
            return Err(Report::new(AlterPlacementError::PlacementNameMismatch {
                stored: self.name.clone(),
                requested: alter.placement.clone(),
            }));
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

    pub fn validate(&self) -> error_stack::Result<(), AlterPlacementError> {
        if self.from.is_empty() {
            return Err(Report::new(AlterPlacementError::EmptyFrom));
        }
        if self.to.is_empty() {
            return Err(Report::new(AlterPlacementError::EmptyTo));
        }
        Ok(())
    }

    fn normalize_members(&mut self) {
        deduplicate_identifiers(&mut self.from);
        deduplicate_identifiers(&mut self.to);
    }
}

fn deduplicate_identifiers(identifiers: &mut Vec<ModelName>) {
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

/// A model shape that its [`ModelKind`] names, and that names its kind back.
///
/// A store keyed by kind reads a model whose kind the caller has already named, so the caller
/// receives that shape and never re-checks what it asked for. The projection below is where the
/// union is narrowed, and only a store that owns the kind-to-shape invariant calls it.
///
/// Two families of model stay outside this pairing and are read as the [`Model`] union they
/// genuinely are. [`ModelKind::Client`] configures one model per external system, so naming that
/// kind does not name a shape. The JSON and CBOR wire schemas are the same shape under two kinds,
/// so that shape does not name a kind.
pub trait UniquelyKindedModel: Sized {
    /// The kind whose stored models all have this shape.
    const KIND: ModelKind;

    /// This shape, when `model` holds it.
    fn from_model(model: Model) -> Option<Self>;

    /// The borrowing form of [`Self::from_model`].
    fn from_model_ref(model: &Model) -> Option<&Self>;

    /// The mutable form of [`Self::from_model_ref`], for altering a stored model in place.
    fn from_model_mut(model: &mut Model) -> Option<&mut Self>;
}

/// Declares the [`Model`] union.
///
/// Each line ends in whether its kind and its shape name each other one-to-one. `paired` earns the
/// [`UniquelyKindedModel`] projection. `shared` says the pairing is broken in one direction or the
/// other, so naming the kind leaves the union as the only honest answer.
macro_rules! declare_models {
    ($($Variant:ident($Model:ident $(<$Version:ident>)?) => $Kind:ident, $connector:expr, $family:ident;)+) => {
        /// One domain entity's configuration.
        ///
        /// `Version` is the type a resource binding's version takes. A stored Model binds concrete
        /// numbers, which is the default; a parsed `CREATE` statement carries
        /// [`RequestedResourceVersion`] until planning resolves it.
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            Serialize,
            Deserialize,
            Archive,
            RkyvSerialize,
            RkyvDeserialize,
        )]
        #[cfg_attr(nervix_lint, nervix::value(model, reason = "public configuration union converted into validated execution plans"))]
        pub enum Model<Version = u64> {
            $($Variant($Model $(<$Version>)?),)+
        }

        impl<Version> Model<Version> {
            pub fn kind(&self) -> ModelKind {
                match self {
                    $(Self::$Variant(_) => ModelKind::$Kind,)+
                }
            }

            pub fn name(&self) -> ModelName {
                match self {
                    $(Self::$Variant(model) => (&model.name).into(),)+
                }
            }

            /// The connector this model configures a client of. Only a client Model is a
            /// client of one.
            pub fn connector_kind(&self) -> Option<ConnectorKind> {
                match self {
                    $(Self::$Variant(_) => $connector,)+
                }
            }

            /// The fields every configured connector client declares, borrowed from its owner.
            pub fn configured_client(&self) -> Option<crate::ConfiguredClientRef<'_, Version>> {
                match self {
                    $(Self::$Variant(model) => declare_models!(@client $Kind model, $Model $(<$Version>)?),)+
                }
            }

            pub(crate) fn client_config_mut(&mut self) -> Option<&mut Vec<ClientConfigEntry>> {
                match self {
                    $(Self::$Variant(model) => declare_models!(@client_config $Kind model),)+
                }
            }

            /// Rebuilds this model with the version of every resource it binds passed through
            /// `map`, in the order the model declares its bindings. A model that binds no resource
            /// is rebuilt unchanged.
            pub fn try_map_resource_versions<Next, Error>(
                self,
                mut map: impl FnMut(&ResourceName, Version) -> Result<Next, Error>,
            ) -> Result<Model<Next>, Error> {
                let mapped = match self {
                    $(Self::$Variant(model) => {
                        Model::$Variant(declare_models!(@map_versions model, map $(, $Version)?))
                    })+
                };
                Ok(mapped)
            }
        }

        $(declare_models!(@family $family $Variant($Model) => $Kind);)+
    };
    (@map_versions $model:ident, $map:ident) => {
        $model
    };
    (@client Client $model:ident, $Client:ident<$Version:ident>) => {
        Some(crate::ConfiguredClientRef {
            connector: $Client::<$Version>::CONNECTOR,
            name: &$model.name,
            mount: $model.mount.as_ref(),
            config: &$model.config,
        })
    };
    (@client $kind:ident $model:ident, $Client:ident $(<$Version:ident>)?) => { { let _ = $model; None } };
    (@client_config Client $model:ident) => { Some(&mut $model.config) };
    (@client_config $kind:ident $model:ident) => { { let _ = $model; None } };
    (@map_versions $model:ident, $map:ident, $Version:ident) => {
        $model.try_map_resource_versions(&mut $map)?
    };
    (@family paired $Variant:ident($Model:ty) => $Kind:ident) => {
        impl UniquelyKindedModel for $Model {
            const KIND: ModelKind = ModelKind::$Kind;

            fn from_model(model: Model) -> Option<Self> {
                if let Model::$Variant(model) = model {
                    Some(model)
                } else {
                    None
                }
            }

            fn from_model_ref(model: &Model) -> Option<&Self> {
                if let Model::$Variant(model) = model {
                    Some(model)
                } else {
                    None
                }
            }

            fn from_model_mut(model: &mut Model) -> Option<&mut Self> {
                if let Model::$Variant(model) = model {
                    Some(model)
                } else {
                    None
                }
            }
        }
    };
    (@family shared $Variant:ident($Model:ty) => $Kind:ident) => {};
}

declare_models! {
    Schema(CreateSchema) => Schema, None, paired;
    WireJsonSchema(CreateJsonWireSchema) => WireJsonSchema, None, shared;
    WireCborSchema(CreateCborWireSchema) => WireCborSchema, None, shared;
    WireAvroSchema(CreateAvroWireSchema) => WireAvroSchema, None, paired;
    Codec(CreateCodec<Version>) => Codec, None, paired;
    ClientKafka(CreateClientKafka<Version>) => Client, Some(CreateClientKafka::<Version>::CONNECTOR), shared;
    ClientPulsar(CreateClientPulsar<Version>) => Client, Some(CreateClientPulsar::<Version>::CONNECTOR), shared;
    ClientHttp(CreateClientHttp<Version>) => Client, Some(CreateClientHttp::<Version>::CONNECTOR), shared;
    ClientSentry(CreateClientSentry<Version>) => Client, Some(CreateClientSentry::<Version>::CONNECTOR), shared;
    ClientOtel(CreateClientOtel<Version>) => Client, Some(CreateClientOtel::<Version>::CONNECTOR), shared;
    ClientPrometheus(CreateClientPrometheus<Version>) => Client, Some(CreateClientPrometheus::<Version>::CONNECTOR), shared;
    ClientMqtt(CreateClientMqtt<Version>) => Client, Some(CreateClientMqtt::<Version>::CONNECTOR), shared;
    ClientNats(CreateClientNats<Version>) => Client, Some(CreateClientNats::<Version>::CONNECTOR), shared;
    ClientRabbitMq(CreateClientRabbitMq<Version>) => Client, Some(CreateClientRabbitMq::<Version>::CONNECTOR), shared;
    ClientRedis(CreateClientRedis<Version>) => Client, Some(CreateClientRedis::<Version>::CONNECTOR), shared;
    ClientZeroMq(CreateClientZeroMq<Version>) => Client, Some(CreateClientZeroMq::<Version>::CONNECTOR), shared;
    ClientSqs(CreateClientSqs<Version>) => Client, Some(CreateClientSqs::<Version>::CONNECTOR), shared;
    ClientWebsockets(CreateClientWebsockets<Version>) => Client, Some(CreateClientWebsockets::<Version>::CONNECTOR), shared;
    ClientSyslog(CreateClientSyslog<Version>) => Client, Some(CreateClientSyslog::<Version>::CONNECTOR), shared;
    ClientClickHouse(CreateClientClickHouse<Version>) => Client, Some(CreateClientClickHouse::<Version>::CONNECTOR), shared;
    ClientPostgres(CreateClientPostgres<Version>) => Client, Some(CreateClientPostgres::<Version>::CONNECTOR), shared;
    ClientMySql(CreateClientMySql<Version>) => Client, Some(CreateClientMySql::<Version>::CONNECTOR), shared;
    ClientMongoDb(CreateClientMongoDb<Version>) => Client, Some(CreateClientMongoDb::<Version>::CONNECTOR), shared;
    ClientS3(CreateClientS3<Version>) => Client, Some(CreateClientS3::<Version>::CONNECTOR), shared;
    ClientGcs(CreateClientGcs<Version>) => Client, Some(CreateClientGcs::<Version>::CONNECTOR), shared;
    ClientAzureBlob(CreateClientAzureBlob<Version>) => Client, Some(CreateClientAzureBlob::<Version>::CONNECTOR), shared;
    ClientIcebergRest(CreateClientIcebergRest<Version>) => Client, Some(CreateClientIcebergRest::<Version>::CONNECTOR), shared;
    Vhost(CreateVhost<Version>) => Vhost, None, paired;
    Branch(CreateBranch) => Branch, None, paired;
    Endpoint(CreateEndpoint) => Endpoint, None, paired;
    SignalingProtocol(CreateSignalingProtocol<Version>) => SignalingProtocol, None, paired;
    Generator(CreateGenerator) => Generator, None, paired;
    Inferencer(CreateInferencer<Version>) => Inferencer, None, paired;
    WasmProcessor(CreateWasmProcessor<Version>) => WasmProcessor, None, paired;
    Ingestor(CreateIngestor) => Ingestor, None, paired;
    Reingestor(CreateReingestor) => Reingestor, None, paired;
    Relay(CreateRelay) => Relay, None, paired;
    Lookup(CreateLookup<Version>) => Lookup, None, paired;
    Junction(CreateJunction) => Junction, None, paired;
    Deduplicator(CreateDeduplicator) => Deduplicator, None, paired;
    Correlator(CreateCorrelator) => Correlator, None, paired;
    Reorderer(CreateReorderer) => Reorderer, None, paired;
    WindowProcessor(CreateWindowProcessor) => WindowProcessor, None, paired;
    Emitter(CreateEmitter) => Emitter, None, paired;
    Placement(CreatePlacement) => Placement, None, paired;
    Udf(CreateUdf) => Udf, None, paired;
}

impl Model {
    pub fn executes_on_every_cluster_node(&self) -> bool {
        if let Self::Ingestor(ingestor) = self {
            ingestor.input.executes_on_every_cluster_node()
        } else {
            false
        }
    }

    /// The relays this model reads as materialized state. State is resolved by key rather than
    /// delivered as records, so these are dependencies and not record inputs.
    pub fn materialized_state_relays(&self) -> Vec<&RelayName> {
        let dependencies = match self {
            Self::Generator(generator) => return vec![&generator.materialized_relay],
            Self::Emitter(model) => &model.materialized_state,
            Self::Reingestor(model) => &model.materialized_state,
            Self::Inferencer(model) => &model.materialized_state,
            Self::WasmProcessor(model) => &model.materialized_state,
            Self::Junction(model) => &model.materialized_state,
            Self::Deduplicator(model) => &model.materialized_state,
            Self::Correlator(model) => &model.materialized_state,
            Self::Reorderer(model) => &model.materialized_state,
            Self::WindowProcessor(model) => &model.materialized_state,
            _ => return Vec::new(),
        };
        dependencies
            .iter()
            .map(|dependency| &dependency.relay)
            .collect()
    }

    /// The declared output routes of a producing model, in written order.
    pub const fn output_routes(&self) -> Option<&ProcessorOutputs> {
        match self {
            Self::Generator(model) => Some(&model.output_routes),
            Self::Ingestor(model) => Some(&model.output_routes),
            Self::Reingestor(model) => Some(&model.output_routes),
            Self::Inferencer(model) => Some(&model.output_routes),
            Self::WasmProcessor(model) => Some(&model.output_routes),
            Self::Junction(model) => Some(&model.output_routes),
            Self::Deduplicator(model) => Some(&model.output_routes),
            Self::Correlator(model) => Some(&model.output_routes),
            Self::Reorderer(model) => Some(&model.output_routes),
            Self::WindowProcessor(model) => Some(&model.output_routes),
            _ => None,
        }
    }
}

impl<Version> Model<Version> {
    /// How this model is addressed: the kind it is and the name it carries, together.
    pub fn node_ref(&self) -> NodeRef {
        NodeRef::new(self.kind(), self.name())
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateCodec<Version = u64> {
    pub name: CodecName,
    pub wire_format: CodecWireFormat<Version>,
    pub schema: SchemaName,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub encoding_rules: Vec<CodecEncodingRule>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Default,
)]
pub struct CodecJaqTransformations {
    pub on_ingestion: Option<String>,
    pub on_emitting: Option<String>,
    /// The program a batch of `on_emitting` outputs passes through, which yields the one value a
    /// batching emitter publishes. It is only ever present beside `on_emitting`.
    pub on_emitting_batch: Option<String>,
}

impl CodecJaqTransformations {
    pub fn has_any(&self) -> bool {
        self.on_ingestion.is_some() || self.on_emitting.is_some()
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
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum CodecJaqFormat {
    Json,
    Yaml,
    Toml,
    Xml,
    Cbor,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum CodecWireFormat<Version = u64> {
    Json {
        wire_schema: WireSchemaName,
    },
    Cbor {
        wire_schema: WireSchemaName,
    },
    Avro {
        wire_schema: WireSchemaName,
    },
    Syslog,
    JaqNative {
        format: CodecJaqFormat,
        transformations: CodecJaqTransformations,
    },
    Protobuf(CodecProtobufConfig<Version>),
}

impl CodecWireFormat {
    /// The wire schema this format reads, as one reference whose kind and identifier are chosen
    /// together. Formats that carry their own decoding contract name no wire schema at all.
    pub fn wire_schema_reference(&self) -> Option<NodeRef> {
        match self {
            Self::Json { wire_schema } => Some(NodeRef::new(
                ModelKind::WireJsonSchema,
                ModelName::from(wire_schema),
            )),
            Self::Cbor { wire_schema } => Some(NodeRef::new(
                ModelKind::WireCborSchema,
                ModelName::from(wire_schema),
            )),
            Self::Avro { wire_schema } => Some(NodeRef::new(
                ModelKind::WireAvroSchema,
                ModelName::from(wire_schema),
            )),
            Self::Syslog | Self::JaqNative { .. } | Self::Protobuf(_) => None,
        }
    }

    /// Pairs this format with the wire schema `lookup` holds for it. Formats that carry their own
    /// decoding contract resolve without consulting the lookup at all.
    pub fn resolve<'a, L>(&'a self, lookup: &'a L) -> Result<ResolvedCodecWireFormat<'a>, L::Error>
    where
        L: WireSchemaLookup,
    {
        match self {
            Self::Json { wire_schema } => lookup
                .json_wire_schema(wire_schema)
                .map(ResolvedCodecWireFormat::Json),
            Self::Cbor { wire_schema } => lookup
                .cbor_wire_schema(wire_schema)
                .map(ResolvedCodecWireFormat::Cbor),
            Self::Avro { wire_schema } => lookup
                .avro_wire_schema(wire_schema)
                .map(ResolvedCodecWireFormat::Avro),
            Self::Syslog => Ok(ResolvedCodecWireFormat::Syslog),
            Self::JaqNative {
                format,
                transformations,
            } => Ok(ResolvedCodecWireFormat::JaqNative {
                format: *format,
                transformations,
            }),
            Self::Protobuf(config) => Ok(ResolvedCodecWireFormat::Protobuf(config)),
        }
    }

    /// How this format frames the member values of one batch into a single payload.
    pub fn batch_container(&self) -> CodecBatchContainer<'_> {
        match self {
            Self::Json { .. } | Self::Cbor { .. } | Self::Avro { .. } | Self::Syslog => {
                CodecBatchContainer::Format {
                    transformation: None,
                }
            }
            Self::JaqNative {
                transformations, ..
            } => CodecBatchContainer::Format {
                transformation: transformations.on_emitting_batch.as_deref(),
            },
            Self::Protobuf(CodecProtobufConfig {
                batch_message,
                transformations,
                ..
            }) => {
                let Some(message) = batch_message.as_deref() else {
                    return CodecBatchContainer::Undeclared;
                };
                CodecBatchContainer::ProtobufMessage {
                    message,
                    transformation: transformations.on_emitting_batch.as_deref(),
                }
            }
        }
    }

    pub fn supports_encoding(&self) -> bool {
        match self {
            Self::Json { .. } | Self::Cbor { .. } | Self::Avro { .. } | Self::Syslog => true,
            Self::JaqNative {
                transformations, ..
            }
            | Self::Protobuf(CodecProtobufConfig {
                transformations, ..
            }) => transformations.on_emitting.is_some(),
        }
    }
}

impl<Version> CodecWireFormat<Version> {
    /// Whether this codec can decode incoming payloads, independent of resource version state.
    pub fn supports_decoding(&self) -> bool {
        match self {
            Self::Json { .. } | Self::Cbor { .. } | Self::Avro { .. } | Self::Syslog => true,
            Self::JaqNative {
                transformations, ..
            }
            | Self::Protobuf(CodecProtobufConfig {
                transformations, ..
            }) => transformations.on_ingestion.is_some(),
        }
    }
}

/// The container a codec publishes one batch in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecBatchContainer<'a> {
    /// The format's own container — an array, a sequence, a single-key table, a single root
    /// element or a syslog `MSG` array — unless an `ON EMITTING BATCH` program replaces it.
    Format { transformation: Option<&'a str> },
    /// The protobuf `BATCH MESSAGE`: its single repeated field holds the members, or an
    /// `ON EMITTING BATCH` program builds an instance of it.
    ProtobufMessage {
        message: &'a str,
        transformation: Option<&'a str>,
    },
    /// A protobuf codec that names no `BATCH MESSAGE`. Protobuf has no self-delimiting sequence,
    /// so such a codec has no container a batch could be published in.
    Undeclared,
}

impl CodecBatchContainer<'_> {
    /// The `ON EMITTING BATCH` program that builds the batch value, when the codec declares one.
    pub const fn transformation(&self) -> Option<&str> {
        match self {
            Self::Format { transformation } | Self::ProtobufMessage { transformation, .. } => {
                *transformation
            }
            Self::Undeclared => None,
        }
    }
}

/// A codec's wire format with the wire schema it names already looked up. Resolving into one
/// variant is what keeps a format and a wire schema of another kind from travelling together, so
/// validation and compilation read the definition their format actually describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedCodecWireFormat<'a> {
    Json(&'a CreateJsonWireSchema),
    Cbor(&'a CreateCborWireSchema),
    Avro(&'a CreateAvroWireSchema),
    Syslog,
    JaqNative {
        format: CodecJaqFormat,
        transformations: &'a CodecJaqTransformations,
    },
    Protobuf(&'a CodecProtobufConfig),
}

/// Where the wire schema a codec's format names is found.
///
/// One method per schemaful wire format keeps the pairing in the type system: a JSON codec can
/// only ask for a JSON wire schema, so no store can answer a format with a definition of another
/// kind and no caller has to check that it did not.
pub trait WireSchemaLookup {
    /// Why a named wire schema could not be produced.
    type Error;

    fn json_wire_schema(&self, name: &WireSchemaName)
    -> Result<&CreateJsonWireSchema, Self::Error>;

    fn cbor_wire_schema(&self, name: &WireSchemaName)
    -> Result<&CreateCborWireSchema, Self::Error>;

    fn avro_wire_schema(&self, name: &WireSchemaName)
    -> Result<&CreateAvroWireSchema, Self::Error>;
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CodecProtobufConfig<Version = u64> {
    pub resource: ResourceName,
    pub resource_version: Version,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<ClientConfigEntry>,
    pub message: String,
    /// The message a batch is published as. Without a batch transformation it declares exactly one
    /// field, `repeated <message>`, which holds the members.
    pub batch_message: Option<String>,
    pub transformations: CodecJaqTransformations,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CodecEncodingRule {
    pub field: FieldName,
    pub encoding: CodecEncoding,
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
)]
pub enum CodecEncoding {
    Rfc3339,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateGenerator {
    pub name: GeneratorName,
    pub materialized_relay: RelayName,
    pub branched_by: BranchSelection,
    pub each: DomainClockPeriod,
    pub output_routes: ProcessorOutputs,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterGenerator {
    pub generator: GeneratorName,
    pub operations: Vec<AlterGeneratorOperation>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterGeneratorOperation {
    SetMaterializedState { relay: RelayName },
    SetEach { each: DomainClockPeriod },
    SetBranching { branching: BranchSelection },
    AddRoute { route: ProcessorOutput },
    DropRoute { relay: RelayName },
    ReplaceRoute { route: ProcessorOutput },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterGeneratorError {
    #[error("ALTER targets generator `{requested}`, but the stored generator is `{stored}`")]
    GeneratorNameMismatch {
        stored: GeneratorName,
        requested: GeneratorName,
    },
    #[error("route target `{relay}` is not configured")]
    RouteTargetNotFound { relay: RelayName },
    #[error("route target `{relay}` is ambiguous because it is configured more than once")]
    RouteTargetAmbiguous { relay: RelayName },
    #[error("a generator must retain at least one route")]
    CannotDropLastRoute,
}

impl CreateGenerator {
    pub fn apply_alter(
        &mut self,
        alter: &AlterGenerator,
    ) -> error_stack::Result<(), AlterGeneratorError> {
        if self.name != alter.generator {
            return Err(Report::new(AlterGeneratorError::GeneratorNameMismatch {
                stored: self.name.clone(),
                requested: alter.generator.clone(),
            }));
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            match operation {
                AlterGeneratorOperation::SetMaterializedState { relay } => {
                    candidate.materialized_relay = relay.clone();
                }
                AlterGeneratorOperation::SetEach { each } => {
                    candidate.each = *each;
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
                        return Err(Report::new(AlterGeneratorError::CannotDropLastRoute));
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

    fn unique_route_index(
        &self,
        relay: &RelayName,
    ) -> error_stack::Result<usize, AlterGeneratorError> {
        let mut indexes = self
            .output_routes
            .routes
            .iter()
            .enumerate()
            .filter_map(|(index, route)| (route.relay == *relay).then_some(index));
        let Some(index) = indexes.next() else {
            return Err(Report::new(AlterGeneratorError::RouteTargetNotFound {
                relay: relay.clone(),
            }));
        };
        if indexes.next().is_some() {
            return Err(Report::new(AlterGeneratorError::RouteTargetAmbiguous {
                relay: relay.clone(),
            }));
        }
        Ok(index)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MessageErrorPolicy {
    Ignore,
    Log,
    Dlq {
        relay: RelayName,
        assignments: Vec<crate::Assignment>,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum GeneralErrorPolicy {
    Ignore,
    Log,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateBranch {
    pub name: BranchName,
    pub schema: SchemaName,
    pub ttl: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eviction: Option<BranchEviction>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum BranchEviction {
    Lru { max_instances: NonZeroU64 },
}

impl BranchEviction {
    pub const fn max_instances(&self) -> NonZeroU64 {
        match self {
            Self::Lru { max_instances } => *max_instances,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum BranchSelection {
    BranchedBy { branch: BranchName },
    Unbranched,
}

impl BranchSelection {
    pub fn branched_by(branch: BranchName) -> Self {
        Self::BranchedBy { branch }
    }

    pub fn unbranched() -> Self {
        Self::Unbranched
    }

    pub fn branch(&self) -> Option<&BranchName> {
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

impl std::fmt::Display for BranchSelection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BranchedBy { branch } => write!(formatter, "{branch}"),
            Self::Unbranched => formatter.write_str("UNBRANCHED"),
        }
    }
}

/// The branch contract after the registry has resolved every referenced model.
///
/// A branched declaration retains both identities and the complete key schema so execution does
/// not have to reconstruct branch semantics from parallel fields or model lookups.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ResolvedBranching {
    Unbranched,
    Branched {
        branch: BranchName,
        schema: CreateSchema,
    },
}

impl ResolvedBranching {
    pub fn unbranched() -> Self {
        Self::Unbranched
    }

    pub fn branched(branch: BranchName, schema: CreateSchema) -> Self {
        Self::Branched { branch, schema }
    }

    pub fn branch(&self) -> Option<&BranchName> {
        match self {
            Self::Unbranched => None,
            Self::Branched { branch, .. } => Some(branch),
        }
    }

    pub fn schema(&self) -> Option<&CreateSchema> {
        match self {
            Self::Unbranched => None,
            Self::Branched { schema, .. } => Some(schema),
        }
    }

    pub fn fields(&self) -> &[crate::SchemaField] {
        match self {
            Self::Unbranched => &[],
            Self::Branched { schema, .. } => &schema.fields,
        }
    }

    pub fn field_names(&self) -> impl Iterator<Item = &FieldName> {
        self.fields().iter().map(|field| &field.name)
    }

    pub fn is_unbranched(&self) -> bool {
        matches!(self, Self::Unbranched)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateIngestor {
    pub name: IngestorName,
    pub output_routes: ProcessorOutputs,
    /// What the ingestor reads, and how what it reads becomes records of its input schema.
    pub input: crate::IngestorInput,
    pub timestamp_source: Option<IngestTimestampSource>,
    pub general_error_policy: GeneralErrorPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
}

impl CreateIngestor {
    /// Applies an alteration whose head names the pair this ingestor reads, operation by
    /// operation, and keeps the result only when every operation applies.
    pub fn apply_alter(
        &mut self,
        alter: &AlterIngestor,
    ) -> error_stack::Result<(), AlterIngestorError> {
        if self.name != alter.ingestor {
            return Err(Report::new(AlterIngestorError::IngestorNameMismatch {
                stored: self.name.clone(),
                requested: alter.ingestor.clone(),
            }));
        }
        let stored = self.input.entity_kind();
        let requested = alter.alteration.kind();
        if stored != requested {
            return Err(Report::new(AlterIngestorError::KindMismatch {
                stored,
                requested,
            }));
        }

        let mut candidate = self.clone();
        alter.alteration.apply_to(&mut candidate)?;
        *self = candidate;
        Ok(())
    }

    /// Applies the operations of an alteration of a transport's pair in order.
    pub(crate) fn apply_transport_operations<Source>(
        &mut self,
        operations: &[AlterTransportIngestorOperation<Source>],
    ) -> error_stack::Result<(), AlterIngestorError>
    where
        Source: crate::connector::AlteredIngestorEntity,
    {
        for operation in operations {
            match operation {
                AlterTransportIngestorOperation::Ingestor(operation) => {
                    self.apply_operation(operation)?;
                }
                AlterTransportIngestorOperation::SetDecodeUsing { codec } => {
                    let held = self
                        .input
                        .codec_mut()
                        .verified(crate::connector::ALTERED_PAIR_CHECKED);
                    *held = codec.clone();
                }
            }
        }
        Ok(())
    }

    /// Applies the operations of an alteration in order.
    pub(crate) fn apply_operations<Source>(
        &mut self,
        operations: &[AlterIngestorOperation<Source>],
    ) -> error_stack::Result<(), AlterIngestorError>
    where
        Source: crate::connector::AlteredIngestorEntity,
    {
        for operation in operations {
            self.apply_operation(operation)?;
        }
        Ok(())
    }

    fn apply_operation<Source>(
        &mut self,
        operation: &AlterIngestorOperation<Source>,
    ) -> error_stack::Result<(), AlterIngestorError>
    where
        Source: crate::connector::AlteredIngestorEntity,
    {
        match operation {
            AlterIngestorOperation::SetSource { source } => {
                source.set_into(&mut self.input);
            }
            AlterIngestorOperation::SetQuiesce { quiesce } => {
                Source::set_quiesce_of(&mut self.input, quiesce.clone())?;
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
                    return Err(Report::new(AlterIngestorError::CannotDropLastRoute));
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

    fn unique_route_index(
        &self,
        relay: &RelayName,
    ) -> error_stack::Result<usize, AlterIngestorError> {
        let mut indexes = self
            .output_routes
            .routes
            .iter()
            .enumerate()
            .filter_map(|(index, route)| (route.relay == *relay).then_some(index));
        let Some(index) = indexes.next() else {
            return Err(Report::new(AlterIngestorError::RouteTargetNotFound {
                relay: relay.clone(),
            }));
        };
        if indexes.next().is_some() {
            return Err(Report::new(AlterIngestorError::RouteTargetAmbiguous {
                relay: relay.clone(),
            }));
        }
        Ok(index)
    }
}

/// `ALTER <CONNECTOR> <ENTITY> INGESTOR <name> <operation>, …`: operations on the ingestor the
/// head names, nested by the pair it names.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterIngestor {
    pub ingestor: IngestorName,
    pub alteration: crate::IngestorAlteration,
}

impl AlterIngestor {
    /// The connector and entity the statement's head names.
    pub const fn kind(&self) -> crate::IngestorEntityKind {
        self.alteration.kind()
    }
}

/// One operation every ingestor accepts, whose `SET FROM` states a source of the entity
/// `Source`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterIngestorOperation<Source> {
    SetSource { source: Source },
    SetQuiesce { quiesce: IngestQuiesceMode },
    SetTimestamp { source: IngestTimestampSource },
    DropTimestamp,
    SetFilterWhere { where_clause: crate::Expression },
    DropFilterWhere,
    AddRoute { route: ProcessorOutput },
    DropRoute { relay: RelayName },
    ReplaceRoute { route: ProcessorOutput },
    SetGeneralError { policy: GeneralErrorPolicy },
}

/// One operation of an ingestor that reads a transport: an operation every ingestor accepts, or
/// the codec that decodes the transport's payloads.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterTransportIngestorOperation<Source> {
    /// Boxed because a shared operation can carry a whole route, which would otherwise size every
    /// codec operation like one.
    Ingestor(Box<AlterIngestorOperation<Source>>),
    SetDecodeUsing {
        codec: CodecName,
    },
}

impl<Source> From<AlterIngestorOperation<Source>> for AlterTransportIngestorOperation<Source> {
    fn from(operation: AlterIngestorOperation<Source>) -> Self {
        Self::Ingestor(Box::new(operation))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterIngestorError {
    #[error("ALTER targets ingestor `{requested}`, but the stored ingestor is `{stored}`")]
    IngestorNameMismatch {
        stored: IngestorName,
        requested: IngestorName,
    },
    #[error("the stored ingestor reads {stored}, so ALTER {requested} INGESTOR cannot alter it")]
    KindMismatch {
        stored: crate::IngestorEntityKind,
        requested: crate::IngestorEntityKind,
    },
    #[error("route target `{relay}` is not configured")]
    RouteTargetNotFound { relay: RelayName },
    #[error("route target `{relay}` is ambiguous because it is configured more than once")]
    RouteTargetAmbiguous { relay: RelayName },
    #[error("an ingestor must retain at least one route")]
    CannotDropLastRoute,
    #[error("{kind} ingestors do not support ON QUIESCE {mode}")]
    UnsupportedQuiesceMode {
        kind: crate::IngestorEntityKind,
        mode: &'static str,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum IngestTimestampSource {
    /// The domain clock's instant when the record is delivered.
    Now,
    /// A `DATETIME` field of the decoded payload.
    At(FieldName),
    /// A metadata field the ingestor's entity declares as `DATETIME`, written
    /// `TIMESTAMP AT metadata.<field>`.
    AtMetadata(FieldName),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateVhost<Version = u64> {
    pub name: VhostName,
    pub hostnames: Vec<String>,
    pub tls: Option<VhostTlsResource<Version>>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct VhostTlsResource<Version = u64> {
    pub resource: ResourceName,
    pub version: Version,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateEndpoint {
    pub name: EndpointName,
    pub on_vhost: VhostName,
    pub path: String,
    pub endpoint_type: EndpointType,
    pub signaling_protocol: Option<SignalingProtocolName>,
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
    strum::Display,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum EndpointType {
    Websockets,
    Http,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateSignalingProtocol<Version = u64> {
    pub name: SignalingProtocolName,
    pub format: SignalingWireFormat<Version>,
    pub on_connect: SignalingProtocolOnConnect,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum SignalingWireFormat<Version = u64> {
    Json,
    Yaml,
    Toml,
    Xml,
    Cbor,
    Raw,
    Protobuf(SignalingProtobufConfig<Version>),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SignalingProtobufConfig<Version = u64> {
    pub resource: ResourceName,
    pub resource_version: Version,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<ClientConfigEntry>,
    pub send_message: String,
    pub wait_message: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum SignalingStep {
    Send(Vec<String>),
    Wait(SignalingWaitStep),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RetryPolicy {
    pub backoff: String,
    pub max_backoff: String,
}

/// How many acknowledgements an acknowledging mode keeps outstanding at once: one for
/// `ACK SEQUENTIAL`, and up to `max` for `ACK PARALLEL MAX <max>`.
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
)]
pub enum AckWindow {
    Sequential,
    Parallel { max: NonZeroU64 },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateRelay {
    pub name: RelayName,
    pub schema: SchemaName,
    #[serde(default = "default_relay_buffer")]
    #[rkyv(with = crate::CountAsU64)]
    pub buffer: NonZeroUsize,
    pub branching: RelayBranching,
    #[serde(default)]
    pub materialized_state: Option<MaterializedRelayState>,
}

impl CreateRelay {
    pub fn apply_alter(&mut self, alter: &AlterRelay) -> error_stack::Result<(), AlterRelayError> {
        if self.name != alter.relay {
            return Err(Report::new(AlterRelayError::RelayNameMismatch {
                stored: self.name.clone(),
                requested: alter.relay.clone(),
            }));
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
    ) -> error_stack::Result<(), AlterRelayError> {
        match operation {
            AlterRelayOperation::SetCapacity { capacity } => {
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
                    return Err(Report::new(AlterRelayError::MaterializedStateNotConfigured));
                }
            }
        }
        Ok(())
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterRelay {
    pub relay: RelayName,
    pub operations: Vec<AlterRelayOperation>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterRelayOperation {
    SetCapacity {
        #[rkyv(with = crate::CountAsU64)]
        capacity: NonZeroUsize,
    },
    SetSchema {
        schema: SchemaName,
    },
    SetBranching {
        branching: RelayBranching,
    },
    SetMaterializedState,
    DropMaterializedState,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterRelayError {
    #[error("ALTER targets relay `{requested}`, but the stored relay is `{stored}`")]
    RelayNameMismatch {
        stored: RelayName,
        requested: RelayName,
    },
    #[error("relay materialized state is not configured")]
    MaterializedStateNotConfigured,
}

pub const fn default_relay_buffer() -> NonZeroUsize {
    NonZeroUsize::MIN
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RelayBranching {
    BranchedBy { branch: BranchName },
    Unbranched,
}

impl RelayBranching {
    pub fn branched_by(branch: BranchName) -> Self {
        Self::BranchedBy { branch }
    }

    pub fn unbranched() -> Self {
        Self::Unbranched
    }

    pub fn branch(&self) -> Option<&BranchName> {
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

/// The materialized state a relay keeps, spelled in `AsRefStr` as the keyword phrase that follows
/// `WITH MATERIALIZED STATE`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    EnumIter,
)]
pub enum MaterializedRelayState {
    #[strum(serialize = "LAST BY TIMESTAMP")]
    LastByTimestamp,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateLookup<Version = u64> {
    pub name: LookupName,
    pub key_field: FieldName,
    pub resource: ResourceName,
    pub resource_version: Version,
    pub path: String,
    pub decode_using_codec: CodecName,
}

#[cfg(test)]
#[path = "statement_tests.rs"]
mod tests;
