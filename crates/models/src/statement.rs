use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    ops::{Deref, DerefMut},
};

use indexmap::IndexMap;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter, EnumProperty, EnumString, IntoEnumIterator, IntoStaticStr};
use thiserror::Error;

use crate::{
    AlterSchema, AlterWireSchema, AvroType, BranchName, CborType, ChannelName, ClientName,
    ClusterNodeName, CodecName, CollectionName, ConsumerGroupName, CorrelatorName,
    CreateAvroWireSchema, CreateCborWireSchema, CreateJsonWireSchema, CreateSchema, CreateUdf,
    DeduplicatorName, DomainName, EmitterName, EndpointName, FieldName, GeneratorName,
    InferencerName, IngestorName, JsonType, JunctionName, LookupName, ModelName, NodeRef,
    ParseAsType, PlacementName, PulsarSubscriptionName, QueueGroupName, QueueName, ReingestorName,
    RelayName, ReordererName, ResourceName, SchemaName, SignalingProtocolName, SubjectName,
    SubscriptionName, TableName, Timestamp, TopicName, UdfName, UserName, VhostName,
    WasmProcessorName, WindowProcessorName, WireSchemaName,
};

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
            | Self::Relocate(_)
            | Self::DescribeRelocation(_)
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

    /// The NSPL keyword phrase that names this kind in `DROP` and `SHOW CREATE`.
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShowCreate {
    pub kind: ModelKind,
    pub name: ModelName,
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
    pub relay: RelayName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateDomain {
    pub id: DomainName,
    pub config: DomainConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterDomain {
    pub policy: PlacementPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateUser {
    pub name: UserName,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateResource {
    pub identifier: ResourceName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadResource {
    pub identifier: ResourceName,
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
    pub id: DomainName,
    pub config: DomainConfig,
    pub status: DomainStatus,
    pub start_version: u64,
    pub last_start: DomainStartPoint,
    pub clock: Option<DomainClockState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropModel {
    pub kind: ModelKind,
    pub name: ModelName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropNode {
    pub node_id: ClusterNodeName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CordonNode {
    pub node_id: ClusterNodeName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UncordonNode {
    pub node_id: ClusterNodeName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainNode {
    pub node_id: ClusterNodeName,
}

/// One kind-qualified runtime node named by a relocation selection or `FOR` override.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelocationMember {
    pub kind: ModelKind,
    pub name: ModelName,
}

impl RelocationMember {
    pub fn new(kind: ModelKind, name: ModelName) -> Self {
        Self { kind, name }
    }

    /// The `<kind> <name>` spelling used in canonical NSPL and in diagnostics.
    pub fn to_nspl(&self) -> String {
        format!("{} {}", self.kind.keyword_phrase(), self.name.as_str())
    }
}

/// How a relocation selects the runtime nodes it moves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    fn to_nspl(&self) -> String {
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
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AsRefStr, EnumProperty,
)]
#[strum(serialize_all = "lowercase")]
pub enum RelocationPreferenceStrategy {
    #[strum(props(keyword = "FOLLOW PREFERENCES"))]
    Follow,
    #[strum(props(keyword = "IGNORE PREFERENCES"))]
    Ignore,
}

impl RelocationPreferenceStrategy {
    /// The composed NSPL keyword phrase that spells this strategy.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelocationPreferenceOverride {
    pub member: RelocationMember,
    pub strategy: RelocationPreferenceStrategy,
}

/// The relocation both `RELOCATE` and `DESCRIBE RELOCATION` describe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relocation {
    pub selection: RelocationSelection,
    pub destination: ClusterNodeName,
    pub strategy: RelocationPreferenceStrategy,
    pub overrides: Vec<RelocationPreferenceOverride>,
}

impl Relocation {
    /// Renders the clauses shared by both statements, with the default strategy before any
    /// `FOR` override and members in written order.
    pub fn to_nspl_clauses(&self) -> String {
        let mut rendered = format!(
            "{} ONTO NODE {} {}",
            self.selection.to_nspl(),
            self.destination,
            self.strategy.keyword_phrase()
        );
        for override_clause in &self.overrides {
            rendered.push_str(&format!(
                " FOR {} {}",
                override_clause.member.to_nspl(),
                override_clause.strategy.keyword_phrase()
            ));
        }
        rendered
    }
}

fn format_relocation_members(members: &[RelocationMember]) -> String {
    members
        .iter()
        .map(RelocationMember::to_nspl)
        .collect::<Vec<_>>()
        .join(", ")
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeRelay {
    pub relay: RelayName,
    pub bindings: Vec<SubscriptionBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DescribeDomain;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeIngestor {
    pub ingestor: IngestorName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeResource {
    pub identifier: ResourceName,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeLookup {
    pub name: LookupName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeJunction {
    pub name: JunctionName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeDeduplicator {
    pub name: DeduplicatorName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeReingestor {
    pub name: ReingestorName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeCorrelator {
    pub name: CorrelatorName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeEndpoint {
    pub name: EndpointName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeReorderer {
    pub name: ReordererName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeEmitter {
    pub name: EmitterName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeWindowProcessor {
    pub name: WindowProcessorName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeWasmProcessor {
    pub name: WasmProcessorName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeUdf {
    pub name: UdfName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribePlacement {
    pub name: PlacementName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterPlacement {
    pub placement: PlacementName,
    pub operations: Vec<AlterPlacementOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

macro_rules! declare_models {
    ($($Variant:ident($Model:ty) => $Kind:ident, $client_label:expr;)+) => {
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
        pub enum Model {
            $($Variant($Model),)+
        }

        impl Model {
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

            pub fn client_type_label(&self) -> Option<&'static str> {
                match self {
                    $(Self::$Variant(_) => $client_label,)+
                }
            }
        }
    };
}

declare_models! {
    Schema(CreateSchema) => Schema, None;
    WireJsonSchema(CreateJsonWireSchema) => WireJsonSchema, None;
    WireCborSchema(CreateCborWireSchema) => WireCborSchema, None;
    WireAvroSchema(CreateAvroWireSchema) => WireAvroSchema, None;
    Codec(CreateCodec) => Codec, None;
    ClientKafka(CreateClientKafka) => Client, Some("KAFKA");
    ClientPulsar(CreateClientPulsar) => Client, Some("PULSAR");
    ClientHttp(CreateClientHttp) => Client, Some("HTTP");
    ClientSentry(CreateClientSentry) => Client, Some("SENTRY");
    ClientOtel(CreateClientOtel) => Client, Some("OTEL");
    ClientPrometheus(CreateClientPrometheus) => Client, Some("PROMETHEUS");
    ClientMqtt(CreateClientMqtt) => Client, Some("MQTT");
    ClientNats(CreateClientNats) => Client, Some("NATS");
    ClientRabbitMq(CreateClientRabbitMq) => Client, Some("RABBITMQ");
    ClientRedis(CreateClientRedis) => Client, Some("REDIS");
    ClientZeroMq(CreateClientZeroMq) => Client, Some("ZEROMQ");
    ClientSqs(CreateClientSqs) => Client, Some("SQS");
    ClientWebsockets(CreateClientWebsockets) => Client, Some("WEBSOCKETS");
    ClientSyslog(CreateClientSyslog) => Client, Some("SYSLOG");
    ClientClickHouse(CreateClientClickHouse) => Client, Some("CLICKHOUSE");
    ClientPostgres(CreateClientPostgres) => Client, Some("POSTGRES");
    ClientMySql(CreateClientMySql) => Client, Some("MYSQL");
    ClientMongoDb(CreateClientMongoDb) => Client, Some("MONGODB");
    ClientS3(CreateClientS3) => Client, Some("S3");
    ClientGcs(CreateClientGcs) => Client, Some("GCS");
    ClientAzureBlob(CreateClientAzureBlob) => Client, Some("AZURE_BLOB");
    ClientIcebergRest(CreateClientIcebergRest) => Client, Some("ICEBERG_REST");
    Vhost(CreateVhost) => Vhost, None;
    Branch(CreateBranch) => Branch, None;
    Endpoint(CreateEndpoint) => Endpoint, None;
    SignalingProtocol(CreateSignalingProtocol) => SignalingProtocol, None;
    Generator(CreateGenerator) => Generator, None;
    Inferencer(CreateInferencer) => Inferencer, None;
    WasmProcessor(CreateWasmProcessor) => WasmProcessor, None;
    Ingestor(CreateIngestor) => Ingestor, None;
    Reingestor(CreateReingestor) => Reingestor, None;
    Relay(CreateRelay) => Relay, None;
    Lookup(CreateLookup) => Lookup, None;
    Junction(CreateJunction) => Junction, None;
    Deduplicator(CreateDeduplicator) => Deduplicator, None;
    Correlator(CreateCorrelator) => Correlator, None;
    Reorderer(CreateReorderer) => Reorderer, None;
    WindowProcessor(CreateWindowProcessor) => WindowProcessor, None;
    Emitter(CreateEmitter) => Emitter, None;
    Placement(CreatePlacement) => Placement, None;
    Udf(CreateUdf) => Udf, None;
}

impl Model {
    pub fn executes_on_every_cluster_node(&self) -> bool {
        if let Self::Ingestor(ingestor) = self {
            ingestor.source.executes_on_every_cluster_node()
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

    /// How this model is addressed: the kind it is and the name it carries, together.
    pub fn node_ref(&self) -> NodeRef {
        NodeRef::new(self.kind(), self.name())
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateCodec {
    pub name: CodecName,
    pub wire_format: CodecWireFormat,
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
pub enum CodecWireFormat {
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
    Protobuf(CodecProtobufConfig),
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
pub struct CodecProtobufConfig {
    pub resource: ResourceName,
    pub resource_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<ClientConfigEntry>,
    pub message: String,
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
pub struct CreateEmitter {
    pub name: EmitterName,
    pub from: ProcessorInputs,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encode_using_codec: Option<CodecName>,
    pub sink: Box<EmitSink>,
    pub flush_policy: FlushPolicy,
    pub error_policies: ErrorPolicies,
    pub publishing_mode: EmitterPublishingMode,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default)]
    pub construction: crate::RouteConstruction,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

impl CreateEmitter {
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
                self.sink = sink.clone();
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
            AlterEmitterOperation::SetFlush { flush_policy } => {
                self.flush_policy = flush_policy.clone();
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

    fn input_index(&self, relay: &RelayName) -> Result<usize, AlterEmitterError> {
        self.from
            .from
            .iter()
            .position(|candidate| candidate == relay)
            .ok_or_else(|| AlterEmitterError::InputNotFound {
                relay: relay.clone(),
            })
    }

    fn ensure_input_absent(&self, relay: &RelayName) -> Result<(), AlterEmitterError> {
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
    pub emitter: EmitterName,
    pub operations: Vec<AlterEmitterOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterEmitterOperation {
    AddFrom {
        relay: RelayName,
        where_clause: Option<crate::Expression>,
    },
    DropFrom {
        relay: RelayName,
    },
    AlterFromSetWhere {
        relay: RelayName,
        where_clause: crate::Expression,
    },
    AlterFromDropWhere {
        relay: RelayName,
    },
    SetSink {
        sink: Box<EmitSink>,
        publishing_mode: EmitterPublishingMode,
    },
    SetClient {
        client: ClientName,
    },
    SetEncodeUsing {
        codec: CodecName,
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
        flush_policy: FlushPolicy,
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
        stored: EmitterName,
        requested: EmitterName,
    },
    #[error("input relay `{relay}` is already configured")]
    InputAlreadyExists { relay: RelayName },
    #[error("input relay `{relay}` is not configured")]
    InputNotFound { relay: RelayName },
    #[error("input relay `{relay}` has no WHERE clause")]
    InputWhereNotConfigured { relay: RelayName },
    #[error("an emitter must retain at least one input")]
    CannotDropLastInput,
    #[error("emitter encoding is not configured")]
    EncodeNotConfigured,
    #[error("COMMIT policy is only supported by Iceberg emitters")]
    CommitPolicyUnsupported,
    #[error("{sink} emitters do not support publishing mode {mode}")]
    PublishingModeUnsupported { sink: String, mode: String },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateGenerator {
    pub name: GeneratorName,
    pub materialized_relay: RelayName,
    pub branched_by: BranchSelection,
    pub each: String,
    pub output_routes: ProcessorOutputs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterGenerator {
    pub generator: GeneratorName,
    pub operations: Vec<AlterGeneratorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterGeneratorOperation {
    SetMaterializedState { relay: RelayName },
    SetEach { each: String },
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

    fn unique_route_index(&self, relay: &RelayName) -> Result<usize, AlterGeneratorError> {
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
pub enum SqsFifoGroup {
    FromBranch,
    Expression(crate::Expression),
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
pub enum EmitSink {
    Kafka {
        client: ClientName,
        topic: TopicName,
    },
    Pulsar {
        client: ClientName,
        topic: TopicName,
    },
    #[strum(serialize = "RABBITMQ")]
    RabbitMq {
        client: ClientName,
        queue: QueueName,
    },
    Redis {
        client: ClientName,
        channel: ChannelName,
    },
    Mqtt {
        client: ClientName,
        topic: TopicName,
    },
    Nats {
        client: ClientName,
        subject: SubjectName,
    },
    #[strum(serialize = "ZEROMQ")]
    ZeroMq {
        client: ClientName,
    },
    Sqs {
        client: ClientName,
        queue: String,
        fifo_group: Option<SqsFifoGroup>,
    },
    Sentry {
        client: ClientName,
    },
    Syslog {
        client: ClientName,
    },
    Otel {
        client: ClientName,
        signal: OtelSignal,
        values: Vec<OtelValueMapping>,
        attributes: Vec<OtelValueMapping>,
        resource: Vec<OtelValueMapping>,
        scope: Option<OtelScope>,
    },
    #[strum(serialize = "CLICKHOUSE")]
    ClickHouse {
        client: ClientName,
        table: TableName,
        values: Vec<ClickHouseValueMapping>,
        max_batch: NonZeroU64,
    },
    Postgres {
        client: ClientName,
        table: TableName,
        values: Vec<PostgresValueMapping>,
        conflict_action: PostgresConflictAction,
        max_batch: NonZeroU64,
    },
    #[strum(serialize = "MYSQL")]
    MySql {
        client: ClientName,
        table: TableName,
        values: Vec<MySqlValueMapping>,
        conflict_action: MySqlConflictAction,
        max_batch: NonZeroU64,
    },
    #[strum(serialize = "MONGODB")]
    MongoDb {
        client: ClientName,
        collection: CollectionName,
        values: Vec<MongoDbValueMapping>,
        conflict_action: MongoDbConflictAction,
        max_batch: NonZeroU64,
    },
    Iceberg {
        backend: IcebergStorageBackend,
        client: ClientName,
        table: TableName,
        values: Vec<IcebergValueMapping>,
        location: String,
        catalog: IcebergCatalog,
        commit_each: String,
        max_commit_size: String,
    },
}

impl EmitSink {
    pub fn transport_label(&self) -> &str {
        self.as_ref()
    }

    pub fn client(&self) -> &ClientName {
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
            | Self::Syslog { client }
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
            Self::Redis { .. } | Self::ZeroMq { .. } | Self::Syslog { .. } => {
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

    fn client_mut(&mut self) -> &mut ClientName {
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
            | Self::Syslog { client }
            | Self::Otel { client, .. }
            | Self::ClickHouse { client, .. }
            | Self::Postgres { client, .. }
            | Self::MySql { client, .. }
            | Self::MongoDb { client, .. }
            | Self::Iceberg { client, .. } => client,
        }
    }

    pub fn iceberg_catalog_client(&self) -> Option<&ClientName> {
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
            Self::Syslog { .. } => "SYSLOG",
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
                | (Self::Syslog { .. }, Model::ClientSyslog(_))
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
            Self::Syslog { .. } => true,
            Self::Otel { .. }
            | Self::ClickHouse { .. }
            | Self::Postgres { .. }
            | Self::MySql { .. }
            | Self::MongoDb { .. }
            | Self::Iceberg { .. } => false,
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
            | Self::Syslog { .. }
            | Self::Otel { .. }
            | Self::ClickHouse { .. }
            | Self::Postgres { .. }
            | Self::MySql { .. }
            | Self::MongoDb { .. } => None,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClickHouseValueMapping {
    pub column: String,
    pub expression: crate::Expression,
}

pub type OtelValueMapping = ClickHouseValueMapping;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum OtelSignal {
    Logs,
    Traces,
    Metric(OtelMetric),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelMetric {
    pub name: String,
    pub unit: String,
    pub description: Option<String>,
    pub kind: OtelMetricKind,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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
pub enum OtelAggregationTemporality {
    Delta,
    Cumulative,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OtelScope {
    pub name: String,
    pub version: Option<String>,
}

pub type PostgresValueMapping = ClickHouseValueMapping;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum PostgresConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

pub type MySqlValueMapping = ClickHouseValueMapping;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MySqlConflictAction {
    None,
    DoNothing,
    DoUpdate,
}

pub type MongoDbValueMapping = ClickHouseValueMapping;
pub type IcebergValueMapping = ClickHouseValueMapping;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum IcebergCatalog {
    Rest { client: ClientName },
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
pub enum IcebergStorageBackend {
    S3,
    #[strum(serialize = "GCS")]
    Gcs,
    #[strum(serialize = "AZURE_BLOB")]
    AzureBlob,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MongoDbConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientConfigEntry {
    pub key: String,
    pub value: String,
}

/// Declare clients that share the connector-owned name, mount and configuration shape.
macro_rules! declare_clients {
    ($($Client:ident => $Config:ident,)+) => {
        $(
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
            pub struct $Client {
                pub name: ClientName,
                pub mount: Option<ResourceName>,
                pub config: Vec<ClientConfigEntry>,
            }

            pub type $Config = ClientConfigEntry;
        )+
    };
}

declare_clients! {
    CreateClientKafka => KafkaConfigEntry,
    CreateClientPulsar => PulsarConfigEntry,
    CreateClientHttp => HttpConfigEntry,
    CreateClientSentry => SentryConfigEntry,
    CreateClientOtel => OtelConfigEntry,
    CreateClientPrometheus => PrometheusConfigEntry,
    CreateClientMqtt => MqttConfigEntry,
    CreateClientNats => NatsConfigEntry,
    CreateClientRabbitMq => RabbitMqConfigEntry,
    CreateClientRedis => RedisConfigEntry,
    CreateClientZeroMq => ZeroMqConfigEntry,
    CreateClientSqs => SqsConfigEntry,
    CreateClientSyslog => SyslogConfigEntry,
    CreateClientClickHouse => ClickHouseConfigEntry,
    CreateClientPostgres => PostgresConfigEntry,
    CreateClientMySql => MySqlConfigEntry,
    CreateClientMongoDb => MongoDbConfigEntry,
    CreateClientS3 => S3ConfigEntry,
    CreateClientGcs => GcsConfigEntry,
    CreateClientAzureBlob => AzureBlobConfigEntry,
    CreateClientIcebergRest => IcebergRestConfigEntry,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateClientWebsockets {
    pub name: ClientName,
    pub mount: Option<ResourceName>,
    pub signaling_protocol: Option<SignalingProtocolName>,
    pub config: Vec<ClientConfigEntry>,
}

pub type WebsocketsConfigEntry = ClientConfigEntry;

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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateIngestor {
    pub name: IngestorName,
    pub output_routes: ProcessorOutputs,
    pub decode_using_codec: CodecName,
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

    fn unique_route_index(&self, relay: &RelayName) -> Result<usize, AlterIngestorError> {
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
    pub ingestor: IngestorName,
    pub operations: Vec<AlterIngestorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterIngestorOperation {
    SetSource { source: IngestSource },
    SetQuiesce { quiesce: IngestQuiesceMode },
    SetDecodeUsing { codec: CodecName },
    SetTimestamp { source: IngestTimestampSource },
    DropTimestamp,
    SetFilterWhere { where_clause: crate::Expression },
    DropFilterWhere,
    AddRoute { route: ProcessorOutput },
    DropRoute { relay: RelayName },
    ReplaceRoute { route: ProcessorOutput },
    SetGeneralError { policy: GeneralErrorPolicy },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterIngestorError {
    #[error("ALTER targets ingestor `{requested}`, but the stored ingestor is `{stored}`")]
    IngestorNameMismatch {
        stored: IngestorName,
        requested: IngestorName,
    },
    #[error("route target `{relay}` is not configured")]
    RouteTargetNotFound { relay: RelayName },
    #[error("route target `{relay}` is ambiguous because it is configured more than once")]
    RouteTargetAmbiguous { relay: RelayName },
    #[error("an ingestor must retain at least one route")]
    CannotDropLastRoute,
    #[error("{transport} ingestors do not support ON QUIESCE {mode}")]
    UnsupportedQuiesceMode { transport: String, mode: String },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProcessorOutput {
    pub relay: RelayName,
    #[serde(default)]
    pub construction: crate::RouteConstruction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_policy: Option<FlushPolicy>,
    pub message_error_policy: MessageErrorPolicy,
    pub branch: Option<crate::OutputBranch>,
}

/// How a route or an emitter releases what it has buffered.
///
/// `FLUSH IMMEDIATE` releases each message as it arrives, so there is no batch left to bound.
/// `FLUSH EACH` releases on a cadence and always bounds the batch it releases. One variant per
/// form is what keeps a cadence without a bound, and a bound without a cadence, out of the model.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum FlushPolicy {
    Immediate,
    Each {
        interval: String,
        max_batch_size: String,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InputCollectPolicy {
    pub collect_for: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProcessorInputWhere {
    pub relay: RelayName,
    pub where_clause: crate::Expression,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProcessorInputs {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<RelayName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub r#where: Vec<ProcessorInputWhere>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collect_policy: Option<InputCollectPolicy>,
}

impl ProcessorInputs {
    pub fn new(from: Vec<RelayName>, r#where: Vec<ProcessorInputWhere>) -> Self {
        Self {
            from,
            r#where,
            collect_policy: None,
        }
    }

    pub fn single(relay: RelayName) -> Self {
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

    pub fn first(&self) -> Option<&RelayName> {
        self.from.first()
    }

    pub fn relays(&self) -> &[RelayName] {
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
    pub fn new(relay: RelayName) -> Self {
        Self {
            relay,
            construction: crate::RouteConstruction::default(),
            flush_policy: None,
            message_error_policy: MessageErrorPolicy::Log,
            branch: None,
        }
    }

    pub fn with_flush_policy(relay: RelayName, flush_policy: FlushPolicy) -> Self {
        Self {
            relay,
            construction: crate::RouteConstruction::default(),
            flush_policy: Some(flush_policy),
            message_error_policy: MessageErrorPolicy::Log,
            branch: None,
        }
    }

    pub fn with_branch(mut self, branch: crate::OutputBranch) -> Self {
        self.branch = Some(branch);
        self
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProcessorOutputs {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<ProcessorOutput>,
}

impl ProcessorOutputs {
    pub fn new(routes: Vec<ProcessorOutput>) -> Self {
        Self { routes }
    }

    pub fn single(relay: RelayName) -> Self {
        Self {
            routes: vec![ProcessorOutput::new(relay)],
        }
    }

    pub fn with_flush_policy(mut self, flush_policy: FlushPolicy) -> Self {
        for output in &mut self.routes {
            output.flush_policy = Some(flush_policy.clone());
        }
        self
    }

    pub fn with_branch(mut self, branch: crate::OutputBranch) -> Self {
        for output in &mut self.routes {
            output.branch = Some(branch.clone());
        }
        self
    }

    pub fn relays(&self) -> impl Iterator<Item = &RelayName> {
        self.outputs().map(|output| &output.relay)
    }

    pub fn outputs(&self) -> impl Iterator<Item = &ProcessorOutput> {
        self.routes.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum IngestTimestampSource {
    Now,
    At(FieldName),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateReingestor {
    pub name: ReingestorName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub mode: AckMode,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterReingestor {
    pub reingestor: ReingestorName,
    pub operations: Vec<AlterProcessorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterReingestorError {
    #[error("ALTER targets reingestor `{requested}`, but the stored reingestor is `{stored}`")]
    ReingestorNameMismatch {
        stored: ReingestorName,
        requested: ReingestorName,
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateInferencer {
    pub name: InferencerName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub resource: ResourceName,
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateWasmProcessor {
    pub name: WasmProcessorName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub resource: ResourceName,
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
pub struct WasmProcessorLimits {
    pub max_fuel: NonZeroU64,
    pub max_memory_bytes: NonZeroU64,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InferencerTensorMapping {
    pub tensor: String,
    pub schema: InferencerTensorSchema,
    pub expression: crate::Expression,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InferencerTensorDeclaration {
    pub tensor: String,
    pub schema: InferencerTensorSchema,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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
                InferencerTensorDimension::Fixed(size) => Some(
                    usize::try_from(size.get())
                        .assured("u32 fits usize on every architecture supported by the models"),
                ),
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
pub enum InferencerTensorRepresentation {
    Dense,
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
pub enum InferencerTensorElementType {
    F32,
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
pub enum InferencerTensorDimension {
    Fixed(NonZeroU32),
    Dynamic,
    Batch,
}

impl InferencerTensorDimension {
    pub fn is_batch(&self) -> bool {
        matches!(self, Self::Batch)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateVhost {
    pub name: VhostName,
    pub hostnames: Vec<String>,
    pub tls: Option<VhostTlsResource>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct VhostTlsResource {
    pub resource: ResourceName,
    pub version: Option<u64>,
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
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum EndpointType {
    Websockets,
    Http,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateSignalingProtocol {
    pub name: SignalingProtocolName,
    pub format: SignalingWireFormat,
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
pub enum SignalingWireFormat {
    Json,
    Yaml,
    Toml,
    Xml,
    Cbor,
    Raw,
    Protobuf(SignalingProtobufConfig),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SignalingProtobufConfig {
    pub resource: ResourceName,
    pub resource_version: Option<u64>,
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
pub enum IngestSource {
    Http {
        client: ClientName,
        every: String,
        quiesce: IngestQuiesceMode,
    },
    Kafka {
        client: ClientName,
        topic: TopicName,
        offset_mode: KafkaOffsetMode,
        instances: NonZeroU64,
        mode: KafkaIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Pulsar {
        client: ClientName,
        topic: TopicName,
        subscription: PulsarSubscriptionName,
        instances: NonZeroU64,
        mode: PulsarIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Mqtt {
        client: ClientName,
        topic: String,
        instances: NonZeroU64,
        mode: MqttIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Nats {
        client: ClientName,
        subject: SubjectName,
        queue_group: QueueGroupName,
        instances: NonZeroU64,
        mode: NatsIngestMode,
        quiesce: IngestQuiesceMode,
    },
    #[strum(serialize = "RABBITMQ")]
    RabbitMq {
        client: ClientName,
        queue: QueueName,
        instances: NonZeroU64,
        mode: RabbitMqIngestMode,
        quiesce: IngestQuiesceMode,
    },
    #[strum(serialize = "REDIS")]
    RedisPubSub {
        client: ClientName,
        channel: ChannelName,
        mode: RedisPubSubIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Prometheus {
        client: ClientName,
        query: String,
        every: String,
        quiesce: IngestQuiesceMode,
    },
    #[strum(serialize = "ZEROMQ")]
    ZeroMq {
        client: ClientName,
        mode: ZeroMqIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Sqs {
        client: ClientName,
        queue: QueueName,
        instances: NonZeroU64,
        mode: SqsIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Endpoint {
        endpoint: EndpointName,
        mode: EndpointIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Websockets {
        client: ClientName,
        mode: WebsocketsIngestMode,
        quiesce: IngestQuiesceMode,
    },
    Syslog {
        client: ClientName,
        quiesce: IngestQuiesceMode,
    },
}

impl IngestSource {
    pub fn executes_on_every_cluster_node(&self) -> bool {
        match self {
            Self::Endpoint { .. } | Self::Syslog { .. } => true,
            Self::Http { .. }
            | Self::Kafka { .. }
            | Self::Pulsar { .. }
            | Self::Mqtt { .. }
            | Self::Nats { .. }
            | Self::RabbitMq { .. }
            | Self::RedisPubSub { .. }
            | Self::Prometheus { .. }
            | Self::ZeroMq { .. }
            | Self::Sqs { .. }
            | Self::Websockets { .. } => false,
        }
    }

    pub fn transport_label(&self) -> &str {
        self.as_ref()
    }

    pub fn source_ref(&self) -> ModelName {
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
            | Self::Websockets { client, .. }
            | Self::Syslog { client, .. } => client.into(),
            Self::Endpoint { endpoint, .. } => endpoint.into(),
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
            | Self::Websockets { quiesce, .. }
            | Self::Syslog { quiesce, .. } => quiesce,
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
            Self::ZeroMq { .. } | Self::Syslog { .. } => matches!(
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
            }
            | Self::Syslog {
                quiesce: current, ..
            } => *current = quiesce,
        }
        Ok(())
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
)]
pub enum IngestQuiesceOverflow {
    DropOldest,
    DropNewest,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum KafkaOffsetMode {
    ConsumerGroup(ConsumerGroupName),
    Domain,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RetryPolicy {
    pub backoff: String,
    pub max_backoff: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EmitterAckWindow {
    Sequential,
    Parallel { max: NonZeroU64 },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum KafkaIngestMode {
    AckParallel {
        max: NonZeroU64,
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
pub enum MqttSession {
    Clean,
    Persistent,
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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
        max: NonZeroU64,
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NatsIngestMode {
    NoAckSequential,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RabbitMqIngestMode {
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RedisPubSubIngestMode {
    NoAckSequential,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ZeroMqIngestMode {
    NoAckSequential,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum SqsIngestMode {
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EndpointIngestMode {
    NoAckSequential,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum WebsocketsIngestMode {
    NoAckSequential,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateRelay {
    pub name: RelayName,
    pub schema: SchemaName,
    #[serde(default = "default_relay_buffer")]
    pub buffer: NonZeroUsize,
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
    pub relay: RelayName,
    pub operations: Vec<AlterRelayOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterRelayOperation {
    SetCapacity { capacity: NonZeroUsize },
    SetSchema { schema: SchemaName },
    SetBranching { branching: RelayBranching },
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MaterializedRelayState {
    LastByTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterSchedule {
    pub domains: BTreeMap<DomainName, DomainSchedule>,
}

impl ClusterSchedule {
    pub fn domain(&self, domain: &DomainName) -> Option<&DomainSchedule> {
        self.domains.get(domain)
    }
}

impl FromIterator<DomainSchedule> for ClusterSchedule {
    fn from_iter<I: IntoIterator<Item = DomainSchedule>>(schedules: I) -> Self {
        Self {
            domains: schedules
                .into_iter()
                .map(|schedule| (schedule.domain.clone(), schedule))
                .collect(),
        }
    }
}

/// A domain's scheduled nodes, in the order the registry emitted them and keyed by runtime node
/// identity so callers resolve a node by kind and identifier without scanning the sequence.
pub type ScheduledNodes = IndexMap<NodeRef, ScheduledNode>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainSchedule {
    pub domain: DomainName,
    pub nodes: ScheduledNodes,
    pub placement_groups: Vec<PlacementGroupSchedule>,
}

impl DomainSchedule {
    /// Builds a schedule from the registry's emitted node sequence, keeping that order and keying
    /// each node by its runtime identity.
    pub fn new(
        domain: DomainName,
        nodes: impl IntoIterator<Item = ScheduledNode>,
        placement_groups: Vec<PlacementGroupSchedule>,
    ) -> Self {
        Self {
            domain,
            nodes: nodes
                .into_iter()
                .map(|node| (node.identity(), node))
                .collect(),
            placement_groups,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementGroupSchedule {
    pub members: Vec<NodeRef>,
    pub primary_node: Option<ClusterNodeName>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaPartitionSchedule {
    pub observed_partitions: Vec<i32>,
    pub rebalance_epoch: u64,
    pub instance_assignments: Vec<Vec<i32>>,
}

impl KafkaPartitionSchedule {
    pub fn new(instances: NonZeroU64, observed_partitions: Vec<i32>, rebalance_epoch: u64) -> Self {
        let shard_count = usize::try_from(instances.get()).unwrap_or(usize::MAX);
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
    pub identifier: ModelName,
    pub kind: ModelKind,
    pub config: Box<Model>,
    pub effective_branching: Option<Vec<FieldName>>,
    pub effective_branching_schema: Option<SchemaName>,
    #[serde(default)]
    pub schema_fingerprint: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_partition_schedule: Option<KafkaPartitionSchedule>,
    #[serde(default)]
    pub primary_node: Option<ClusterNodeName>,
    #[serde(default)]
    pub assigned_nodes: Vec<ClusterNodeName>,
}

impl ScheduledNode {
    /// The runtime node this scheduled entry configures. Kind and identifier together name a node
    /// in a domain, and this is the key its schedule is stored under.
    pub fn identity(&self) -> NodeRef {
        NodeRef::new(self.kind, self.identifier.clone())
    }

    fn executes_on_every_cluster_node(&self) -> bool {
        self.config.executes_on_every_cluster_node()
    }

    pub fn is_assigned_to(&self, node_id: &ClusterNodeName) -> bool {
        self.assigned_nodes
            .iter()
            .any(|assigned| assigned == node_id)
    }

    pub fn assigned_single_node(&self) -> Option<&ClusterNodeName> {
        match self.assigned_nodes.as_slice() {
            [node_id] => Some(node_id),
            _ => None,
        }
    }

    pub fn primary_node(&self) -> Option<&ClusterNodeName> {
        self.primary_node.as_ref()
    }

    pub fn replica_nodes(&self) -> Vec<&ClusterNodeName> {
        let primary = self.primary_node();
        self.assigned_nodes
            .iter()
            .filter(|node_id| Some(*node_id) != primary)
            .collect()
    }

    pub fn is_primary_on(&self, node_id: &ClusterNodeName) -> bool {
        if let Some(primary_node) = self.primary_node() {
            primary_node == node_id
        } else {
            self.is_assigned_to(node_id)
        }
    }

    pub fn execution_node(&self) -> Option<&ClusterNodeName> {
        if self.executes_on_every_cluster_node() {
            None
        } else {
            self.primary_node().or_else(|| self.assigned_single_node())
        }
    }

    pub fn executes_on(&self, node_id: &ClusterNodeName) -> bool {
        if self.executes_on_every_cluster_node() {
            self.is_assigned_to(node_id)
        } else {
            self.is_primary_on(node_id)
        }
    }

    /// True when `other` gives this runtime node the same primary owner and the same replica set.
    /// A schedule difference that fails this check is an assignment change, which activates
    /// narrowly instead of rebuilding the domain.
    pub fn has_same_assignment_as(&self, other: &Self) -> bool {
        self.primary_node == other.primary_node && self.assigned_nodes == other.assigned_nodes
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateLookup {
    pub name: LookupName,
    pub key_field: FieldName,
    pub resource: ResourceName,
    pub path: String,
    pub decode_using_codec: CodecName,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateJunction {
    pub name: JunctionName,
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
    pub junction: JunctionName,
    pub operations: Vec<AlterProcessorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterJunctionError {
    #[error("ALTER targets junction `{requested}`, but the stored junction is `{stored}`")]
    JunctionNameMismatch {
        stored: JunctionName,
        requested: JunctionName,
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateDeduplicator {
    pub name: DeduplicatorName,
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
    pub deduplicator: DeduplicatorName,
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
        stored: DeduplicatorName,
        requested: DeduplicatorName,
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateCorrelator {
    pub name: CorrelatorName,
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
    Debug,
    Clone,
    Copy,
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
pub enum CorrelatorMatchPolicy {
    Earliest,
    Latest,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CorrelationTimeoutPolicy {
    pub left: CorrelationTimeoutAction,
    pub right: CorrelationTimeoutAction,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum CorrelationTimeoutAction {
    Drop,
    SendTo { relay: RelayName },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateReorderer {
    pub name: ReordererName,
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
    pub reorderer: ReordererName,
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
        stored: ReordererName,
        requested: ReordererName,
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
        relay: RelayName,
        where_clause: Option<crate::Expression>,
    },
    DropFrom {
        relay: RelayName,
    },
    AlterFromSetWhere {
        relay: RelayName,
        where_clause: crate::Expression,
    },
    AlterFromDropWhere {
        relay: RelayName,
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
        relay: RelayName,
    },
    AlterMaterializedState {
        relay: RelayName,
        policy: crate::MaterializedStatePolicy,
    },
    AddRoute {
        route: ProcessorOutput,
    },
    DropRoute {
        relay: RelayName,
    },
    ReplaceRoute {
        route: ProcessorOutput,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterProcessorError {
    #[error("input relay `{relay}` is already configured")]
    InputAlreadyExists { relay: RelayName },
    #[error("input relay `{relay}` is not configured")]
    InputNotFound { relay: RelayName },
    #[error("input relay `{relay}` has no WHERE clause")]
    InputWhereNotConfigured { relay: RelayName },
    #[error("a processor must retain at least one input")]
    CannotDropLastInput,
    #[error("materialized-state dependency `{relay}` is already configured")]
    MaterializedStateAlreadyConfigured { relay: RelayName },
    #[error("materialized-state dependency `{relay}` is not configured")]
    MaterializedStateNotConfigured { relay: RelayName },
    #[error("route target `{relay}` is not configured")]
    RouteTargetNotFound { relay: RelayName },
    #[error("route target `{relay}` is ambiguous because it is configured more than once")]
    RouteTargetAmbiguous { relay: RelayName },
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

    fn input_index(&self, relay: &RelayName) -> Result<usize, AlterProcessorError> {
        self.from
            .from
            .iter()
            .position(|candidate| candidate == relay)
            .ok_or_else(|| AlterProcessorError::InputNotFound {
                relay: relay.clone(),
            })
    }

    fn ensure_input_absent(&self, relay: &RelayName) -> Result<(), AlterProcessorError> {
        if self.from.from.iter().any(|candidate| candidate == relay) {
            Err(AlterProcessorError::InputAlreadyExists {
                relay: relay.clone(),
            })
        } else {
            Ok(())
        }
    }

    fn materialized_state_index(&self, relay: &RelayName) -> Result<usize, AlterProcessorError> {
        self.materialized_state
            .iter()
            .position(|dependency| dependency.relay == *relay)
            .ok_or_else(|| AlterProcessorError::MaterializedStateNotConfigured {
                relay: relay.clone(),
            })
    }

    fn unique_route_index(&self, relay: &RelayName) -> Result<usize, AlterProcessorError> {
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateWindowProcessor {
    pub name: WindowProcessorName,
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
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
    use nonzero_ext::nonzero;

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
        EmitterPublishingMode, ErrorPolicies, FlushPolicy, GeneralErrorPolicy,
        InferencerTensorDimension, InferencerTensorElementType, InferencerTensorRepresentation,
        InferencerTensorSchema, KafkaPartitionSchedule, MaterializedRelayState, Model, ModelKind,
        PlacementPolicy, RelayBranching, RetryPolicy, ScheduledNode,
    };
    use crate::{
        ClusterNodeName, CreateIngestor, CreateJunction, DomainName, EndpointIngestMode,
        Expression, IngestQuiesceMode, IngestSource, Literal, MaterializedStateDependency,
        MaterializedStatePolicy, ParseAsType, ProcessorInputs, ProcessorOutput, ProcessorOutputs,
        SchemaField,
    };

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn domain(raw: &str) -> DomainName {
        DomainName::try_from(raw).expect("valid domain")
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
                InferencerTensorDimension::Fixed(nonzero!(2u32)),
                InferencerTensorDimension::Dynamic,
                InferencerTensorDimension::Fixed(nonzero!(3u32)),
            ],
        };
        let exact = ParseAsType::Array {
            len: nonzero!(2u32),
            element: Box::new(ParseAsType::Vec {
                element: Box::new(ParseAsType::Array {
                    len: nonzero!(3u32),
                    element: Box::new(ParseAsType::F32),
                }),
            }),
        };
        let flattened = ParseAsType::Array {
            len: nonzero!(6u32),
            element: Box::new(ParseAsType::F32),
        };

        assert!(schema.is_compatible_with_field_type(&exact));
        assert!(!schema.is_compatible_with_field_type(&flattened));
        assert_eq!(schema.message_type(), exact);
    }

    #[test]
    fn cluster_schedule_returns_matching_domain() {
        let alpha = DomainSchedule::new(domain("alpha"), Vec::new(), Vec::new());
        let beta = DomainSchedule::new(domain("beta"), Vec::new(), Vec::new());
        let schedule = ClusterSchedule::from_iter([alpha.clone(), beta]);

        assert_eq!(schedule.domain(&domain("alpha")), Some(&alpha));
        assert_eq!(schedule.domain(&domain("gamma")), None);
    }

    #[test]
    fn scheduled_node_assignment_checks_exact_node_id() {
        let node = ScheduledNode {
            identifier: named("orders_ingestor"),
            kind: ModelKind::Schema,
            config: Box::new(Model::Schema(CreateSchema {
                name: named("orders"),
                fields: vec![SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                }],
            })),
            effective_branching: Some(vec![named("tenant")]),
            effective_branching_schema: None,
            schema_fingerprint: [0; 32],
            kafka_partition_schedule: None,
            primary_node: Some(named::<ClusterNodeName>("node-a")),
            assigned_nodes: vec![named::<ClusterNodeName>("node-a")],
        };

        assert!(node.is_assigned_to(&named::<ClusterNodeName>("node-a")));
        assert!(!node.is_assigned_to(&named::<ClusterNodeName>("node-b")));
        assert!(
            !ScheduledNode {
                assigned_nodes: Vec::new(),
                ..node
            }
            .is_assigned_to(&named::<ClusterNodeName>("node-a"))
        );
    }

    #[test]
    fn scheduled_node_single_assignment_only_when_exactly_one_node_is_present() {
        let node = ScheduledNode {
            identifier: named("orders_ingestor"),
            kind: ModelKind::Schema,
            config: Box::new(Model::Schema(CreateSchema {
                name: named("orders"),
                fields: vec![SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                }],
            })),
            effective_branching: None,
            effective_branching_schema: None,
            schema_fingerprint: [0; 32],
            kafka_partition_schedule: None,
            primary_node: Some(named::<ClusterNodeName>("node-a")),
            assigned_nodes: vec![named::<ClusterNodeName>("node-a")],
        };

        assert_eq!(node.assigned_single_node(), Some(&named("node-a")));
        assert_eq!(
            ScheduledNode {
                assigned_nodes: vec![
                    named::<ClusterNodeName>("node-a"),
                    named::<ClusterNodeName>("node-b")
                ],
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
            identifier: named("orders_ingestor"),
            kind: ModelKind::Schema,
            config: Box::new(Model::Schema(CreateSchema {
                name: named("orders"),
                fields: vec![SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                }],
            })),
            effective_branching: None,
            effective_branching_schema: None,
            schema_fingerprint: [0; 32],
            kafka_partition_schedule: None,
            primary_node: Some(named::<ClusterNodeName>("node-a")),
            assigned_nodes: vec![
                named::<ClusterNodeName>("node-a"),
                named::<ClusterNodeName>("node-b"),
                named::<ClusterNodeName>("node-c"),
            ],
        };

        assert_eq!(node.primary_node(), Some(&named("node-a")));
        assert_eq!(
            node.replica_nodes(),
            vec![&named("node-b"), &named("node-c")]
        );
        assert!(node.is_primary_on(&named::<ClusterNodeName>("node-a")));
        assert!(!node.is_primary_on(&named::<ClusterNodeName>("node-b")));
    }

    #[test]
    fn scheduled_node_execution_uses_primary_except_for_server_listener_ingestors() {
        let replicated_junction = ScheduledNode {
            identifier: named("orders_merge"),
            kind: ModelKind::Junction,
            config: Box::new(Model::Junction(CreateJunction {
                name: named("orders_merge"),
                from: ProcessorInputs::new(
                    vec![named("orders_in_a"), named("orders_in_b")],
                    Vec::new(),
                ),
                output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                    named("orders_out"),
                    FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    },
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
            primary_node: Some(named::<ClusterNodeName>("node-a")),
            assigned_nodes: vec![
                named::<ClusterNodeName>("node-a"),
                named::<ClusterNodeName>("node-b"),
            ],
        };
        let endpoint_ingestor = ScheduledNode {
            identifier: named("orders_http"),
            kind: ModelKind::Ingestor,
            config: Box::new(Model::Ingestor(CreateIngestor {
                name: named("orders_http"),
                output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                    named("orders_out"),
                    FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    },
                )]),
                decode_using_codec: named("codec"),
                timestamp_source: None,
                source: IngestSource::Endpoint {
                    endpoint: named("public_http"),
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
            primary_node: Some(named::<ClusterNodeName>("node-a")),
            assigned_nodes: vec![
                named::<ClusterNodeName>("node-a"),
                named::<ClusterNodeName>("node-b"),
            ],
        };
        let syslog_ingestor = ScheduledNode {
            identifier: named("orders_syslog"),
            kind: ModelKind::Ingestor,
            config: Box::new(Model::Ingestor(CreateIngestor {
                name: named("orders_syslog"),
                output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                    named("orders_out"),
                    FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    },
                )]),
                decode_using_codec: named("codec"),
                timestamp_source: None,
                source: IngestSource::Syslog {
                    client: named("syslog_listener"),
                    quiesce: IngestQuiesceMode::Suspend,
                },
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            })),
            effective_branching: None,
            effective_branching_schema: None,
            schema_fingerprint: [0; 32],
            kafka_partition_schedule: None,
            primary_node: Some(named::<ClusterNodeName>("node-a")),
            assigned_nodes: vec![
                named::<ClusterNodeName>("node-a"),
                named::<ClusterNodeName>("node-b"),
            ],
        };

        assert_eq!(replicated_junction.execution_node(), Some(&named("node-a")));
        assert!(replicated_junction.executes_on(&named::<ClusterNodeName>("node-a")));
        assert!(!replicated_junction.executes_on(&named::<ClusterNodeName>("node-b")));

        assert_eq!(endpoint_ingestor.execution_node(), None);
        assert!(endpoint_ingestor.executes_on(&named::<ClusterNodeName>("node-a")));
        assert!(endpoint_ingestor.executes_on(&named::<ClusterNodeName>("node-b")));

        assert_eq!(syslog_ingestor.execution_node(), None);
        assert!(syslog_ingestor.executes_on(&named::<ClusterNodeName>("node-a")));
        assert!(syslog_ingestor.executes_on(&named::<ClusterNodeName>("node-b")));
    }

    #[test]
    fn kafka_partition_schedule_assigns_partitions_round_robin_by_instance() {
        let schedule = KafkaPartitionSchedule::new(nonzero!(2u64), vec![3, 1, 2, 0], 7);

        assert_eq!(schedule.observed_partitions, vec![0, 1, 2, 3]);
        assert_eq!(schedule.rebalance_epoch, 7);
        assert_eq!(schedule.instance_assignments, vec![vec![0, 2], vec![1, 3]]);
    }

    #[test]
    fn relay_alter_applies_operations_in_order_and_is_atomic() {
        let mut relay = CreateRelay {
            name: named("events"),
            schema: named("event_v1"),
            buffer: nonzero!(1usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        };
        relay
            .apply_alter(&AlterRelay {
                relay: named("events"),
                operations: vec![
                    AlterRelayOperation::SetCapacity {
                        capacity: nonzero!(8usize),
                    },
                    AlterRelayOperation::SetSchema {
                        schema: named("event_v2"),
                    },
                    AlterRelayOperation::SetCapacity {
                        capacity: nonzero!(16usize),
                    },
                    AlterRelayOperation::SetMaterializedState,
                ],
            })
            .expect("relay alter should apply");
        assert_eq!(relay.buffer, nonzero!(16usize));
        assert_eq!(relay.schema, named("event_v2"));
        assert_eq!(
            relay.materialized_state,
            Some(MaterializedRelayState::LastByTimestamp)
        );

        let before = relay.clone();
        let error = relay
            .apply_alter(&AlterRelay {
                relay: named("events"),
                operations: vec![
                    AlterRelayOperation::SetCapacity {
                        capacity: nonzero!(32usize),
                    },
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
            name: named("route_events"),
            from: ProcessorInputs::new(vec![named("incoming")], Vec::new()),
            output_routes: ProcessorOutputs::new(vec![
                ProcessorOutput::with_flush_policy(
                    named("accepted"),
                    FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    },
                ),
                ProcessorOutput::with_flush_policy(
                    named("accepted"),
                    FlushPolicy::Each {
                        interval: "200ms".to_string(),
                        max_batch_size: "2MiB".to_string(),
                    },
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
                junction: named("route_events"),
                operations: vec![
                    AlterProcessorOperation::SetMode {
                        mode: AckMode::Detached,
                    },
                    AlterProcessorOperation::DropRoute {
                        relay: named("accepted"),
                    },
                ],
            })
            .expect_err("duplicate route targets must be ambiguous");
        assert_eq!(
            error,
            AlterJunctionError::Processor(AlterProcessorError::RouteTargetAmbiguous {
                relay: named("accepted"),
            })
        );
        assert_eq!(junction, before, "failed ALTER must not partially apply");
    }

    #[test]
    fn junction_alter_applies_ordered_collection_filter_dependency_and_route_updates() {
        let mut junction = CreateJunction {
            name: named("route_events"),
            from: ProcessorInputs::single(named("incoming_a")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                named("accepted"),
                FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                },
            )]),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        let true_expression = Expression::Literal(Literal::Bool(true));
        let false_expression = Expression::Literal(Literal::Bool(false));
        let replacement = ProcessorOutput::with_flush_policy(
            named("accepted"),
            FlushPolicy::Each {
                interval: "250ms".to_string(),
                max_batch_size: "2MiB".to_string(),
            },
        );
        junction
            .apply_alter(&AlterJunction {
                junction: named("route_events"),
                operations: vec![
                    AlterProcessorOperation::AddFrom {
                        relay: named("incoming_b"),
                        where_clause: Some(true_expression.clone()),
                    },
                    AlterProcessorOperation::AlterFromSetWhere {
                        relay: named("incoming_b"),
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
                            relay: named("profiles"),
                            policy: MaterializedStatePolicy::RequiredWait,
                        },
                    },
                    AlterProcessorOperation::AddMaterializedState {
                        dependency: MaterializedStateDependency {
                            relay: named("accounts"),
                            policy: MaterializedStatePolicy::RequiredSkip,
                        },
                    },
                    AlterProcessorOperation::AlterMaterializedState {
                        relay: named("profiles"),
                        policy: MaterializedStatePolicy::RequiredSkip,
                    },
                    AlterProcessorOperation::AddRoute {
                        route: ProcessorOutput::with_flush_policy(
                            named("audit"),
                            FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            },
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
            vec![named("incoming_a"), named("incoming_b")]
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
            vec![named("profiles"), named("accounts")]
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
            vec![named("accepted"), named("audit")]
        );
        assert_eq!(junction.output_routes.routes[0], replacement);
        assert_eq!(junction.mode, AckMode::Detached);
    }

    #[test]
    fn relay_alter_reports_each_typed_error() {
        let relay = CreateRelay {
            name: named("events"),
            schema: named("event"),
            buffer: nonzero!(1usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        };
        let cases = [
            (
                AlterRelay {
                    relay: named("other"),
                    operations: vec![AlterRelayOperation::SetCapacity {
                        capacity: nonzero!(2usize),
                    }],
                },
                AlterRelayError::RelayNameMismatch {
                    stored: named("events"),
                    requested: named("other"),
                },
            ),
            (
                AlterRelay {
                    relay: named("events"),
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
            name: named("route_events"),
            from: ProcessorInputs::single(named("incoming")),
            output_routes: ProcessorOutputs::single(named("accepted")),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: vec![MaterializedStateDependency {
                relay: named("profiles"),
                policy: MaterializedStatePolicy::RequiredWait,
            }],
        };
        let true_expression = Expression::Literal(Literal::Bool(true));
        let cases = vec![
            (
                AlterJunction {
                    junction: named("other"),
                    operations: Vec::new(),
                },
                AlterJunctionError::JunctionNameMismatch {
                    stored: named("route_events"),
                    requested: named("other"),
                },
            ),
            (
                AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::AddFrom {
                        relay: named("incoming"),
                        where_clause: None,
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::InputAlreadyExists {
                    relay: named("incoming"),
                }),
            ),
            (
                AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::DropFrom {
                        relay: named("missing"),
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::InputNotFound {
                    relay: named("missing"),
                }),
            ),
            (
                AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::AlterFromDropWhere {
                        relay: named("incoming"),
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::InputWhereNotConfigured {
                    relay: named("incoming"),
                }),
            ),
            (
                AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::DropFrom {
                        relay: named("incoming"),
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::CannotDropLastInput),
            ),
            (
                AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::AddMaterializedState {
                        dependency: MaterializedStateDependency {
                            relay: named("profiles"),
                            policy: MaterializedStatePolicy::RequiredSkip,
                        },
                    }],
                },
                AlterJunctionError::Processor(
                    AlterProcessorError::MaterializedStateAlreadyConfigured {
                        relay: named("profiles"),
                    },
                ),
            ),
            (
                AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::AlterMaterializedState {
                        relay: named("missing"),
                        policy: MaterializedStatePolicy::RequiredSkip,
                    }],
                },
                AlterJunctionError::Processor(
                    AlterProcessorError::MaterializedStateNotConfigured {
                        relay: named("missing"),
                    },
                ),
            ),
            (
                AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::ReplaceRoute {
                        route: ProcessorOutput::new(named("missing")),
                    }],
                },
                AlterJunctionError::Processor(AlterProcessorError::RouteTargetNotFound {
                    relay: named("missing"),
                }),
            ),
            (
                AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::DropRoute {
                        relay: named("accepted"),
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
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::AlterFromSetWhere {
                    relay: named("incoming"),
                    where_clause: true_expression,
                }],
            })
            .expect("set WHERE should succeed");
    }

    #[test]
    fn emitter_alter_applies_operations_in_order_and_is_atomic() {
        let mut emitter = CreateEmitter {
            name: named("event_sink"),
            from: ProcessorInputs::single(named("events")),
            encode_using_codec: Some(named("event_codec")),
            sink: Box::new(EmitSink::ZeroMq {
                client: named("sink_a"),
            }),
            flush_policy: FlushPolicy::Each {
                interval: "1s".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
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
                emitter: named("event_sink"),
                operations: vec![
                    AlterEmitterOperation::AddFrom {
                        relay: named("backup_events"),
                        where_clause: Some(Expression::Literal(Literal::Bool(true))),
                    },
                    AlterEmitterOperation::AlterFromDropWhere {
                        relay: named("backup_events"),
                    },
                    AlterEmitterOperation::SetClient {
                        client: named("sink_b"),
                    },
                    AlterEmitterOperation::SetFlush {
                        flush_policy: FlushPolicy::Each {
                            interval: "2s".to_string(),
                            max_batch_size: "2MiB".to_string(),
                        },
                    },
                    AlterEmitterOperation::SetFlush {
                        flush_policy: FlushPolicy::Immediate,
                    },
                    AlterEmitterOperation::SetAttachment {
                        mode: AckMode::Detached,
                    },
                ],
            })
            .expect("emitter alter should apply");
        assert_eq!(emitter.sink.client(), &named("sink_b"));
        assert_eq!(emitter.flush_policy, FlushPolicy::Immediate);
        assert_eq!(emitter.mode, AckMode::Detached);
        assert_eq!(
            emitter.from.relays(),
            &[named("events"), named("backup_events")]
        );
        assert!(emitter.from.where_clauses().is_empty());

        let before = emitter.clone();
        let error = emitter
            .apply_alter(&AlterEmitter {
                emitter: named("event_sink"),
                operations: vec![
                    AlterEmitterOperation::SetClient {
                        client: named("sink_c"),
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
            name: named("event_sink"),
            from: ProcessorInputs::single(named("events")),
            encode_using_codec: Some(named("event_codec")),
            sink: Box::new(EmitSink::ZeroMq {
                client: named("sink"),
            }),
            flush_policy: FlushPolicy::Immediate,
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
                    emitter: named("other"),
                    operations: Vec::new(),
                },
                AlterEmitterError::EmitterNameMismatch {
                    stored: named("event_sink"),
                    requested: named("other"),
                },
            ),
            (
                AlterEmitter {
                    emitter: named("event_sink"),
                    operations: vec![AlterEmitterOperation::SetCommit {
                        commit_each: "1m".to_string(),
                        max_commit_size: "1GiB".to_string(),
                    }],
                },
                AlterEmitterError::CommitPolicyUnsupported,
            ),
            (
                AlterEmitter {
                    emitter: named("event_sink"),
                    operations: vec![AlterEmitterOperation::AddFrom {
                        relay: named("events"),
                        where_clause: None,
                    }],
                },
                AlterEmitterError::InputAlreadyExists {
                    relay: named("events"),
                },
            ),
            (
                AlterEmitter {
                    emitter: named("event_sink"),
                    operations: vec![AlterEmitterOperation::DropFrom {
                        relay: named("missing"),
                    }],
                },
                AlterEmitterError::InputNotFound {
                    relay: named("missing"),
                },
            ),
            (
                AlterEmitter {
                    emitter: named("event_sink"),
                    operations: vec![AlterEmitterOperation::DropFrom {
                        relay: named("events"),
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
            relay: named("events"),
            construction: crate::RouteConstruction::default(),
            flush_policy: Some(FlushPolicy::Each {
                interval: "1s".to_string(),
                max_batch_size: "1MiB".to_string(),
            }),
            message_error_policy: super::MessageErrorPolicy::Log,
            branch: Some(crate::OutputBranch::Unbranched),
        };
        let mut ingestor = CreateIngestor {
            name: named("event_source"),
            output_routes: ProcessorOutputs::new(vec![route.clone()]),
            decode_using_codec: named("event_codec"),
            timestamp_source: None,
            source: IngestSource::Endpoint {
                endpoint: named("ingress_a"),
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
                ingestor: named("event_source"),
                operations: vec![
                    AlterIngestorOperation::SetSource {
                        source: IngestSource::Endpoint {
                            endpoint: named("ingress_b"),
                            mode: EndpointIngestMode::NoAckSequential,
                            quiesce: IngestQuiesceMode::EndpointBuffer {
                                max_size: "1MiB".to_string(),
                            },
                        },
                    },
                    AlterIngestorOperation::SetDecodeUsing {
                        codec: named("event_codec_v2"),
                    },
                    AlterIngestorOperation::SetTimestamp {
                        source: super::IngestTimestampSource::Now,
                    },
                    AlterIngestorOperation::SetFilterWhere {
                        where_clause: Expression::Literal(Literal::Bool(true)),
                    },
                    AlterIngestorOperation::ReplaceRoute {
                        route: ProcessorOutput {
                            relay: named("events"),
                            flush_policy: Some(FlushPolicy::Immediate),
                            ..route.clone()
                        },
                    },
                    AlterIngestorOperation::AddRoute {
                        route: ProcessorOutput {
                            relay: named("audit"),
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
                endpoint: named("ingress_b"),
                mode: EndpointIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::EndpointBuffer {
                    max_size: "1MiB".to_string(),
                },
            }
        );
        assert_eq!(ingestor.decode_using_codec, named("event_codec_v2"));
        assert_eq!(
            ingestor.timestamp_source,
            Some(super::IngestTimestampSource::Now)
        );
        assert_eq!(ingestor.output_routes.routes.len(), 2);
        assert_eq!(ingestor.general_error_policy, GeneralErrorPolicy::Ignore);

        let before = ingestor.clone();
        let error = ingestor
            .apply_alter(&AlterIngestor {
                ingestor: named("event_source"),
                operations: vec![
                    AlterIngestorOperation::SetDecodeUsing {
                        codec: named("event_codec_v3"),
                    },
                    AlterIngestorOperation::DropRoute {
                        relay: named("missing"),
                    },
                ],
            })
            .expect_err("missing route target should fail");
        assert_eq!(
            error,
            AlterIngestorError::RouteTargetNotFound {
                relay: named("missing")
            }
        );
        assert_eq!(ingestor, before, "failed ALTER must not partially apply");
    }

    #[test]
    fn ingestor_alter_reports_name_ambiguity_and_last_route_errors() {
        let route = ProcessorOutput::new(named("events"));
        let base = CreateIngestor {
            name: named("event_source"),
            output_routes: ProcessorOutputs::new(vec![route.clone()]),
            decode_using_codec: named("event_codec"),
            timestamp_source: None,
            source: IngestSource::Endpoint {
                endpoint: named("ingress"),
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
                ingestor: named("other"),
                operations: Vec::new(),
            }),
            Err(AlterIngestorError::IngestorNameMismatch {
                stored: named("event_source"),
                requested: named("other"),
            })
        );
        assert_eq!(candidate, base);

        let mut candidate = base.clone();
        assert_eq!(
            candidate.apply_alter(&AlterIngestor {
                ingestor: named("event_source"),
                operations: vec![AlterIngestorOperation::DropRoute {
                    relay: named("events"),
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
                ingestor: named("event_source"),
                operations: vec![AlterIngestorOperation::DropRoute {
                    relay: named("events"),
                }],
            }),
            Err(AlterIngestorError::RouteTargetAmbiguous {
                relay: named("events"),
            })
        );
        assert_eq!(ambiguous, before);
    }

    fn deduplicator() -> CreateDeduplicator {
        CreateDeduplicator {
            name: named("dedup_events"),
            from: ProcessorInputs::single(named("incoming")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(named("outgoing"))]),
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
            name: named("order_events"),
            from: ProcessorInputs::single(named("incoming")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(named("outgoing"))]),
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
                deduplicator: named("dedup_events"),
                operations: vec![
                    AlterDeduplicatorOperation::Processor(Box::new(
                        AlterProcessorOperation::AddFrom {
                            relay: named("secondary"),
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
            vec![named("incoming"), named("secondary")]
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
                reorderer: named("order_events"),
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
                deduplicator: named("dedup_events"),
                operations: vec![
                    AlterDeduplicatorOperation::SetMaxTime {
                        max_time: "1s".to_string(),
                    },
                    AlterDeduplicatorOperation::Processor(Box::new(
                        AlterProcessorOperation::DropRoute {
                            relay: named("missing"),
                        },
                    )),
                ],
            }),
            Err(AlterDeduplicatorError::Processor(
                AlterProcessorError::RouteTargetNotFound {
                    relay: named("missing"),
                }
            ))
        );
        assert_eq!(deduplicator, original);

        let mut reorderer = reorderer();
        let original = reorderer.clone();
        assert_eq!(
            reorderer.apply_alter(&AlterReorderer {
                reorderer: named("other"),
                operations: Vec::new(),
            }),
            Err(AlterReordererError::ReordererNameMismatch {
                stored: named("order_events"),
                requested: named("other"),
            })
        );
        assert_eq!(reorderer, original);
    }

    #[test]
    fn reingestor_alter_is_ordered_atomic_and_rejects_node_branching() {
        let mut reingestor = CreateReingestor {
            name: named("repartition"),
            from: ProcessorInputs::single(named("incoming")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(named("outgoing"))]),
            mode: AckMode::Attached,
            materialized_state: Vec::new(),
            filter_where: None,
        };
        reingestor
            .apply_alter(&AlterReingestor {
                reingestor: named("repartition"),
                operations: vec![
                    AlterProcessorOperation::SetMode {
                        mode: AckMode::Detached,
                    },
                    AlterProcessorOperation::AddFrom {
                        relay: named("secondary"),
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
                reingestor: named("repartition"),
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
        let route = ProcessorOutput::new(named("outgoing"));
        let mut generator = CreateGenerator {
            name: named("synth"),
            materialized_relay: named("state"),
            branched_by: BranchSelection::unbranched(),
            each: "1s".to_string(),
            output_routes: ProcessorOutputs::new(vec![route.clone()]),
        };
        generator
            .apply_alter(&AlterGenerator {
                generator: named("synth"),
                operations: vec![
                    AlterGeneratorOperation::SetEach {
                        each: "500ms".to_string(),
                    },
                    AlterGeneratorOperation::SetEach {
                        each: "250ms".to_string(),
                    },
                    AlterGeneratorOperation::SetMaterializedState {
                        relay: named("state_v2"),
                    },
                    AlterGeneratorOperation::AddRoute {
                        route: ProcessorOutput::new(named("audit")),
                    },
                ],
            })
            .expect("generator alter should apply");
        assert_eq!(generator.each, "250ms");
        assert_eq!(generator.materialized_relay, named("state_v2"));
        assert_eq!(generator.output_routes.routes.len(), 2);

        let before = generator.clone();
        assert_eq!(
            generator.apply_alter(&AlterGenerator {
                generator: named("synth"),
                operations: vec![
                    AlterGeneratorOperation::SetEach {
                        each: "10ms".to_string(),
                    },
                    AlterGeneratorOperation::DropRoute {
                        relay: named("missing"),
                    },
                ],
            }),
            Err(AlterGeneratorError::RouteTargetNotFound {
                relay: named("missing")
            })
        );
        assert_eq!(generator, before, "failed ALTER must not partially apply");

        let mut single = CreateGenerator {
            output_routes: ProcessorOutputs::new(vec![route.clone()]),
            ..generator
        };
        assert_eq!(
            single.apply_alter(&AlterGenerator {
                generator: named("synth"),
                operations: vec![AlterGeneratorOperation::DropRoute {
                    relay: named("outgoing"),
                }],
            }),
            Err(AlterGeneratorError::CannotDropLastRoute)
        );

        single.output_routes.routes.push(route);
        assert_eq!(
            single.apply_alter(&AlterGenerator {
                generator: named("synth"),
                operations: vec![AlterGeneratorOperation::DropRoute {
                    relay: named("outgoing"),
                }],
            }),
            Err(AlterGeneratorError::RouteTargetAmbiguous {
                relay: named("outgoing")
            })
        );
    }

    #[test]
    fn placement_creation_collapses_duplicate_members() {
        let placement = CreatePlacement::new(
            named("corridor"),
            vec![named("ingest"), named("ingest")],
            vec![named("emit"), named("emit")],
            PlacementPolicy::PreferColocation,
            None,
        )
        .expect("placement should be valid");

        assert_eq!(placement.from, vec![named("ingest")]);
        assert_eq!(placement.to, vec![named("emit")]);
    }

    #[test]
    fn placement_alter_applies_operations_in_order_and_is_atomic() {
        let mut placement = CreatePlacement::new(
            named("corridor"),
            vec![named("ingest")],
            vec![named("emit")],
            PlacementPolicy::PreferColocation,
            None,
        )
        .expect("placement should be valid");
        placement
            .apply_alter(&AlterPlacement {
                placement: named("corridor"),
                operations: vec![
                    AlterPlacementOperation::SetRank {
                        rank: nonzero!(3u64),
                    },
                    AlterPlacementOperation::SetRank {
                        rank: nonzero!(1u64),
                    },
                    AlterPlacementOperation::SetPolicy {
                        policy: PlacementPolicy::RequireColocation,
                    },
                    AlterPlacementOperation::SetMembers {
                        from: vec![named("source"), named("source")],
                        to: vec![named("sink")],
                    },
                    AlterPlacementOperation::RenameTo {
                        name: named("critical"),
                    },
                ],
            })
            .expect("placement alter should apply");

        assert_eq!(placement.name, named("critical"));
        assert_eq!(placement.rank, Some(nonzero!(1u64)));
        assert_eq!(placement.policy, PlacementPolicy::RequireColocation);
        assert_eq!(placement.from, vec![named("source")]);
        assert_eq!(placement.to, vec![named("sink")]);

        let before = placement.clone();
        assert_eq!(
            placement.apply_alter(&AlterPlacement {
                placement: named("other"),
                operations: vec![AlterPlacementOperation::SetPolicy {
                    policy: PlacementPolicy::Neutral,
                }],
            }),
            Err(AlterPlacementError::PlacementNameMismatch {
                stored: named("critical"),
                requested: named("other"),
            })
        );
        assert_eq!(placement, before, "failed ALTER must not partially apply");
    }
}
