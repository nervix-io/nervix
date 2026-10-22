//! The destinations an emitter publishes to, and what every one of them declares.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The union of sink Models and the kind that names every sink pair, the declaration
//!   each sink entity makes whatever its statement says, the contract the body of a sink statement
//!   fulfils, and the value mapping the direct `VALUES` sinks share.
//! - **Depends on.** The connector modules' entity Models, the emitter's batching and publishing
//!   vocabulary, the shapes a declaration is written in, expressions, and the names a body
//!   references.
//! - **Must not know.** How a sink is opened, published to, retried or committed at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::fmt::{Display, Formatter};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    ClickHouseSink, ClickHouseSinkEntity, ClientSink, ClientSinkEntity, DeclaredBounds,
    EnvelopeClauseKind, HeaderModel, HttpSink, HttpSinkEntity, IcebergSink, IcebergSinkEntity,
    KafkaSink, KafkaSinkEntity, LocatorDeclaration, MongoDbSink, MongoDbSinkEntity, MqttSink,
    MqttSinkEntity, MySqlSink, MySqlSinkEntity, NatsSink, NatsSinkEntity, OptionDeclaration,
    OtelSink, OtelSinkEntity, PostgresSink, PostgresSinkEntity, PropertyDeclaration, PulsarSink,
    PulsarSinkEntity, RabbitMqSink, RabbitMqSinkEntity, RedisSink, RedisSinkEntity,
    RuntimeCapability, SentrySink, SentrySinkEntity, SqsSink, SqsSinkEntity, SyslogSink,
    SyslogSinkEntity, ZeroMqSink, ZeroMqSinkEntity, entity_union::entity_union,
};
use crate::{
    ClientName, ConnectorKind, EmitterBatchLegality, EmitterPublishingMode,
    EmitterPublishingModeKind, Expression, SchemaName,
};

/// One column, attribute or field a direct `VALUES` sink writes, and the expression that computes
/// it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SinkValueMapping {
    pub column: String,
    pub expression: Expression,
}

/// The expressions a direct `VALUES` sink sends across its external boundary, in the groups its
/// statement writes them in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkValueMappings<'a> {
    pub values: &'a [SinkValueMapping],
    pub attributes: &'a [SinkValueMapping],
    pub resource: &'a [SinkValueMapping],
}

impl<'a> SinkValueMappings<'a> {
    /// The mappings of a sink whose payload a codec encodes, which writes no direct value.
    pub const fn none() -> Self {
        Self {
            values: &[],
            attributes: &[],
            resource: &[],
        }
    }

    /// The mappings of a sink that writes one `VALUES` map and nothing beside it.
    pub const fn values(values: &'a [SinkValueMapping]) -> Self {
        Self {
            values,
            attributes: &[],
            resource: &[],
        }
    }

    /// Every mapping in written order: values, then attributes, then resource attributes.
    pub fn iter(self) -> impl Iterator<Item = &'a SinkValueMapping> {
        self.values
            .iter()
            .chain(self.attributes)
            .chain(self.resource)
    }
}

/// A configured client the body of a sink names, and the connector that client must configure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkClientReference<'a> {
    pub client: &'a ClientName,
    pub connector: ConnectorKind,
}

/// The model the body of one sink names to publish to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkReference<'a> {
    /// The configured client the sink publishes through.
    Client(SinkClientReference<'a>),
    /// The schema of the native batches session consumers receive.
    Schema(&'a SchemaName),
}

/// What kind of model the body of a sink statement names to publish to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkReferenceKind {
    /// A configured client of one connector.
    Client(ConnectorKind),
    /// A configured client of any one of these connectors, which the statement names before it
    /// with `ON <connector>`: the object store an Iceberg sink writes its data files to.
    StorageClient(&'static [ConnectorKind]),
    /// The schema of the native batches session consumers receive.
    Schema,
}

impl SinkReferenceKind {
    /// Whether a body of this reference kind can publish through a client of `connector`.
    ///
    /// A storage client is one of the object stores an entity declares, three at most.
    pub fn admits_client(self, connector: ConnectorKind) -> bool {
        match self {
            Self::Client(declared) => declared == connector,
            Self::StorageClient(declared) => declared.contains(&connector),
            Self::Schema => false,
        }
    }
}

/// How an emitter's body is formed: what follows its publishing mode, or what its destination
/// writes in place of a codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterBodyForm {
    /// `ENCODE USING <codec>`: a codec encodes every payload.
    Codec,
    /// `ENCODE USING <codec>` or `WITHOUT BODY`: a request carries an encoded payload or none.
    CodecOrWithoutBody,
    /// `VALUES { … }` in the destination: the sink writes the mapped values directly.
    Values,
    /// The output the emitter constructs against the destination's schema, delivered as native
    /// batches.
    Native,
}

impl EmitterBodyForm {
    /// Whether every emitter of this form names a codec.
    pub const fn requires_codec(self) -> bool {
        match self {
            Self::Codec => true,
            Self::CodecOrWithoutBody | Self::Values | Self::Native => false,
        }
    }
}

/// What every sink of one entity declares, whatever its statement says: everything a statement
/// can say about it, and what it does with what a statement says.
///
/// This lives in the vocabulary so the registry can validate an emitter, and a host can prepare
/// one, without naming the connector crate that implements its sink. The grammar, completion,
/// documentation and the generated conformance tests read the same declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitSinkCapabilities {
    /// What the `write_header` calls of an emitter's record become on the message it publishes,
    /// or `None` where its messages carry no transport headers.
    pub headers: Option<HeaderModel>,
    /// What its body names to publish to.
    pub reference: SinkReferenceKind,
    /// How an emitter's body is formed.
    pub body: EmitterBodyForm,
    /// Whether an emitter may write the batching clause, and how large a batch may grow.
    pub batch: EmitterBatchLegality,
    /// Whether a batch can only be published through the codec's `ON EMITTING BATCH` value.
    pub requires_batch_transformation: bool,
    /// The publishing modes its emitters may name, each kind at most once.
    pub publishing_modes: &'static [EmitterPublishingModeKind],
    /// The name that locates what it writes, for an entity whose destination names one.
    pub locator: Option<LocatorDeclaration>,
    /// The options its destination writes after the locator, in the order a statement writes
    /// them.
    pub options: &'static [OptionDeclaration],
    /// The envelope clauses its emitters may write, each kind at most once.
    pub envelope: &'static [EnvelopeClauseKind],
    /// The closed set of keys its emitters' `PROPERTIES { … }` may set.
    pub properties: &'static [PropertyDeclaration],
    /// What its connector holds and how long it waits.
    pub bounds: DeclaredBounds,
}

impl EmitSinkCapabilities {
    /// Whether an emitter's programs can write transport headers with `write_header`.
    pub const fn writes_headers(&self) -> bool {
        self.headers.is_some()
    }

    /// Whether an emitter may name a publishing mode of `kind`.
    ///
    /// An entity declares each kind at most once, of the ten kinds there are.
    pub fn accepts_publishing_mode_kind(&self, kind: EmitterPublishingModeKind) -> bool {
        self.publishing_modes.contains(&kind)
    }

    /// The option its destination opens with `keywords`, where it declares one.
    ///
    /// An entity declares a handful of options in one constant, so scanning them is bounded by
    /// the declaration.
    pub fn option(&self, keywords: &str) -> Option<&'static OptionDeclaration> {
        self.options
            .iter()
            .find(|option| option.keywords == keywords)
    }

    /// What a statement can make every sink of the entity do at run time: publish in each
    /// declared mode, write each option or choice, batch where batching is available, and set
    /// each envelope clause and property.
    pub fn runtime_capabilities(&self) -> Vec<RuntimeCapability> {
        let mut capabilities = Vec::new();
        for kind in self.publishing_modes {
            capabilities.push(RuntimeCapability::Publishing(*kind));
        }
        capabilities.extend(RuntimeCapability::of_options(self.options));
        if self.batch.is_available() {
            capabilities.push(RuntimeCapability::Batch);
        }
        for clause in self.envelope {
            capabilities.push(RuntimeCapability::Envelope(*clause));
        }
        for property in self.properties {
            capabilities.push(RuntimeCapability::Property {
                name: property.name,
            });
        }
        capabilities.extend(RuntimeCapability::of_bounds(self.bounds));
        capabilities
    }
}

/// The declaration one sink entity's Model makes for every statement of that entity.
pub trait SinkEntity {
    const CAPABILITIES: EmitSinkCapabilities;
}

/// What the body of one sink statement states: what it publishes to and through, and the values it
/// writes directly.
pub trait SinkBody {
    /// The client the body publishes through, or the schema its session consumers receive.
    fn reference(&self) -> SinkReference<'_>;

    /// The client the body publishes through, to rebind it. A body that names a schema has none.
    fn client_mut(&mut self) -> Option<&mut ClientName>;

    /// The catalog client the body commits through, for a body that names one beside the client
    /// it publishes through.
    fn catalog_client(&self) -> Option<SinkClientReference<'_>> {
        None
    }

    /// The expressions the body sends across its external boundary directly, without a codec.
    fn value_mappings(&self) -> SinkValueMappings<'_> {
        SinkValueMappings::none()
    }

    /// The commit cadence and maximum commit size as written, for a body that stages what it
    /// publishes and commits it separately.
    fn commit_policy(&self) -> Option<(&str, &str)> {
        None
    }
}

/// What the kind of one connector's sink entities answers for each of them.
pub trait SinkEntityKind {
    fn connector(&self) -> ConnectorKind;

    /// The keyword that names the entity within its connector.
    fn keyword(&self) -> &'static str;

    /// The name of the pair: the connector's keyword, then the entity's.
    fn phrase(&self) -> &'static str;

    fn capabilities(&self) -> EmitSinkCapabilities;
}

entity_union! {
    /// The destination an emitter publishes to, nested by the connector that serves it and the
    /// entity it writes.
    pub enum EmitSink as SinkBody;
    /// The connector and entity of a sink, as capability decisions read them before an emitter is
    /// fully parsed.
    pub enum EmitSinkKind as SinkEntityKind;
    kinds {}
    models {
        Client(ClientSink, ClientSinkEntity),
        Http(HttpSink, HttpSinkEntity),
        Kafka(KafkaSink, KafkaSinkEntity),
        Pulsar(PulsarSink, PulsarSinkEntity),
        RabbitMq(RabbitMqSink, RabbitMqSinkEntity),
        Redis(RedisSink, RedisSinkEntity),
        Mqtt(MqttSink, MqttSinkEntity),
        Nats(NatsSink, NatsSinkEntity),
        ZeroMq(ZeroMqSink, ZeroMqSinkEntity),
        Sqs(SqsSink, SqsSinkEntity),
        Sentry(SentrySink, SentrySinkEntity),
        Syslog(SyslogSink, SyslogSinkEntity),
        Otel(OtelSink, OtelSinkEntity),
        ClickHouse(ClickHouseSink, ClickHouseSinkEntity),
        Postgres(PostgresSink, PostgresSinkEntity),
        MySql(MySqlSink, MySqlSinkEntity),
        MongoDb(MongoDbSink, MongoDbSinkEntity),
        Iceberg(IcebergSink, IcebergSinkEntity),
    }
}

/// A pair is written as its phrase: the connector's keyword, then the entity's.
impl Display for EmitSinkKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.phrase())
    }
}

impl EmitSinkKind {
    /// The connector that serves this sink.
    pub fn connector(self) -> ConnectorKind {
        self.entity().connector()
    }

    /// The keyword that names the entity this sink writes.
    pub fn entity_keyword(self) -> &'static str {
        self.entity().keyword()
    }

    /// The name of this pair: its connector's keyword, then its entity's.
    pub fn phrase(self) -> &'static str {
        self.entity().phrase()
    }

    /// What every sink of this pair declares.
    pub fn capabilities(self) -> EmitSinkCapabilities {
        self.entity().capabilities()
    }

    /// Whether a sink of this pair can publish under `mode`: its entity declares the mode's kind.
    pub fn accepts_publishing_mode(self, mode: &EmitterPublishingMode) -> bool {
        self.capabilities()
            .accepts_publishing_mode_kind(mode.kind())
    }
}

impl EmitSink {
    /// The connector that serves this sink.
    pub fn connector(&self) -> ConnectorKind {
        self.kind().connector()
    }

    pub fn capabilities(&self) -> EmitSinkCapabilities {
        self.kind().capabilities()
    }

    /// The NSPL keyword of the sink's connector, as `DESCRIBE` and diagnostics name it.
    pub fn transport_label(&self) -> &'static str {
        self.connector().keyword()
    }

    /// What this sink's body names to publish to.
    pub fn reference(&self) -> SinkReference<'_> {
        self.body().reference()
    }

    /// The client this sink publishes through. A sink that serves session consumers has none.
    pub fn client(&self) -> Option<&ClientName> {
        match self.reference() {
            SinkReference::Client(reference) => Some(reference.client),
            SinkReference::Schema(_) => None,
        }
    }

    pub(crate) fn client_mut(&mut self) -> Option<&mut ClientName> {
        self.body_mut().client_mut()
    }

    /// The catalog client this sink commits through, for a sink that names one beside the client
    /// it publishes through.
    pub fn catalog_client(&self) -> Option<SinkClientReference<'_>> {
        self.body().catalog_client()
    }

    /// The expressions a direct `VALUES` sink sends across its external boundary, in written order.
    pub fn direct_value_mappings(&self) -> impl Iterator<Item = &SinkValueMapping> {
        self.body().value_mappings().iter()
    }

    pub fn accepts_publishing_mode(&self, mode: &EmitterPublishingMode) -> bool {
        self.kind().accepts_publishing_mode(mode)
    }

    /// The connector a client this sink publishes through must configure. A sink that serves
    /// session consumers publishes through no client.
    pub fn client_connector(&self) -> Option<ConnectorKind> {
        match self.reference() {
            SinkReference::Client(reference) => Some(reference.connector),
            SinkReference::Schema(_) => None,
        }
    }

    pub fn requires_codec(&self) -> bool {
        self.capabilities().body.requires_codec()
    }

    /// Whether an emitter publishing to this sink may write the batching clause, must write it,
    /// and how large a batch may grow.
    pub fn batch_legality(&self) -> EmitterBatchLegality {
        self.capabilities().batch
    }

    /// True when a batch can only be published through the codec's `ON EMITTING BATCH` value.
    pub fn requires_batch_transformation(&self) -> bool {
        self.capabilities().requires_batch_transformation
    }

    pub fn commit_policy(&self) -> Option<(&str, &str)> {
        self.body().commit_policy()
    }

    /// Whether `other` is this sink published through a possibly different client: the same
    /// connector and entity, with every field but the client equal.
    pub fn same_definition(&self, other: &Self) -> bool {
        let mut rebound = other.clone();
        match (self.client(), rebound.client_mut()) {
            (Some(client), Some(rebound_client)) => *rebound_client = client.clone(),
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => return false,
        }
        *self == rebound
    }
}
