//! The destinations an emitter publishes to, and what every one of them declares.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The union of emitter Models and the kind that names every emitter pair, the declaration
//!   each emitter entity makes whatever its statement says, the contract the body of an emitter statement
//!   fulfils, and the value mapping the direct `VALUES` emitters share.
//! - **Depends on.** The connector modules' entity Models, the emitter's batching and publishing
//!   vocabulary, the shapes a declaration is written in, expressions, and the names a body
//!   references.
//! - **Must not know.** How an emitter is opened, published to, retried or committed at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::fmt::{Display, Formatter};

use meticulous::ResultExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    ClickHouseEmitter, ClickHouseEmitterEntity, ClientEmitter, ClientEmitterEntity, DeclaredBounds,
    EnvelopeClauseKind, HeaderModel, HttpEmitter, HttpEmitterEntity, IcebergEmitter,
    IcebergEmitterEntity, KafkaEmitter, KafkaEmitterEntity, LocatorDeclaration, MongoDbEmitter,
    MongoDbEmitterEntity, MqttEmitter, MqttEmitterEntity, MySqlEmitter, MySqlEmitterEntity,
    NatsEmitter, NatsEmitterEntity, OptionDeclaration, OtelEmitter, OtelEmitterEntity,
    PostgresEmitter, PostgresEmitterEntity, PropertyDeclaration, PulsarEmitter,
    PulsarEmitterEntity, RabbitMqEmitter, RabbitMqEmitterEntity, RedisEmitter, RedisEmitterEntity,
    RuntimeCapability, SentryEmitter, SentryEmitterEntity, SqsEmitter, SqsEmitterEntity,
    SyslogEmitter, SyslogEmitterEntity, ZeroMqEmitter, ZeroMqEmitterEntity,
    entity_union::emitter_destinations,
};
use crate::{
    AlterEmitterError, ClickHouseTableEmitterMode, ClientEmitterMode, ClientName, ConnectorKind,
    EmitterBatchLegality, EmitterPublishing, EmitterPublishingModeKind, Expression,
    HttpRequestEmitterMode, IcebergTableEmitterMode, KafkaTopicEmitterMode,
    MongoDbCollectionEmitterMode, MqttTopicEmitterMode, MySqlTableEmitterMode,
    NatsSubjectEmitterMode, OtelEmitterMode, PostgresTableEmitterMode, PulsarTopicEmitterMode,
    RabbitMqEmitterMode, RedisPubSubEmitterMode, SchemaName, SentryEventEmitterMode,
    SqsQueueEmitterMode, SyslogCollectorEmitterMode, ZeroMqPushEmitterMode,
};

/// One column, attribute or field a direct `VALUES` emitter writes, and the expression that computes
/// it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct EmitterValueMapping {
    pub column: String,
    pub expression: Expression,
}

/// The expressions a direct `VALUES` emitter sends across its external boundary, in the groups its
/// statement writes them in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitterValueMappings<'a> {
    pub values: &'a [EmitterValueMapping],
    pub attributes: &'a [EmitterValueMapping],
    pub resource: &'a [EmitterValueMapping],
}

impl<'a> EmitterValueMappings<'a> {
    /// Every mapping and its group, in the same order as the projected columns.
    pub fn declared(self) -> impl Iterator<Item = crate::DeclaredValueMapping<'a>> {
        self.values
            .iter()
            .map(|mapping| crate::DeclaredValueMapping {
                section: crate::MappedValueSection::Values,
                mapping,
            })
            .chain(
                self.attributes
                    .iter()
                    .map(|mapping| crate::DeclaredValueMapping {
                        section: crate::MappedValueSection::Attributes,
                        mapping,
                    }),
            )
            .chain(
                self.resource
                    .iter()
                    .map(|mapping| crate::DeclaredValueMapping {
                        section: crate::MappedValueSection::Resource,
                        mapping,
                    }),
            )
    }

    /// The mappings of an emitter whose payload a codec encodes, which writes no direct value.
    pub const fn none() -> Self {
        Self {
            values: &[],
            attributes: &[],
            resource: &[],
        }
    }

    /// The mappings of an emitter that writes one `VALUES` map and nothing beside it.
    pub const fn values(values: &'a [EmitterValueMapping]) -> Self {
        Self {
            values,
            attributes: &[],
            resource: &[],
        }
    }

    /// Every mapping in written order: values, then attributes, then resource attributes.
    pub fn iter(self) -> impl Iterator<Item = &'a EmitterValueMapping> {
        self.values
            .iter()
            .chain(self.attributes)
            .chain(self.resource)
    }
}

/// A configured client the body of an emitter names, and the connector that client must configure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitterClientReference<'a> {
    pub client: &'a ClientName,
    pub connector: ConnectorKind,
}

/// The model the body of one emitter names to publish to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterReference<'a> {
    /// The configured client the emitter publishes through.
    Client(EmitterClientReference<'a>),
    /// An object-store client whose concrete connector is resolved during planning.
    StorageClient(&'a ClientName),
    /// The schema of the native batches session consumers receive.
    Schema(&'a SchemaName),
}

/// What kind of model the body of an emitter statement names to publish to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterReferenceKind {
    /// A configured client of one connector.
    Client(ConnectorKind),
    /// A configured client of any one of these connectors, resolved from its configured client: the object store an Iceberg destination writes its data files to.
    StorageClient(&'static [ConnectorKind]),
    /// The schema of the native batches session consumers receive.
    Schema,
}

impl EmitterReferenceKind {
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

impl Display for EmitterReferenceKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client(connector) => f.write_str(connector.keyword()),
            Self::StorageClient(connectors) => {
                for (index, connector) in connectors.iter().enumerate() {
                    if index > 0 {
                        f.write_str(" or ")?;
                    }
                    f.write_str(connector.keyword())?;
                }
                Ok(())
            }
            Self::Schema => f.write_str("SCHEMA"),
        }
    }
}

/// How an emitter's body is formed: what follows its publishing mode, or what its destination
/// writes in place of a codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum EmitterBodyForm {
    /// `ENCODE USING <codec>`: a codec encodes every payload.
    #[strum(serialize = "ENCODE USING")]
    Codec,
    /// `ENCODE USING <codec>` or `WITHOUT BODY`: a request carries an encoded payload or none.
    #[strum(serialize = "ENCODE USING or WITHOUT BODY")]
    CodecOrWithoutBody,
    /// `VALUES { … }` in the destination: the emitter writes the mapped values directly.
    #[strum(serialize = "VALUES")]
    Values,
    /// The output the emitter constructs against the destination's schema, delivered as native
    /// batches.
    #[strum(serialize = "native Arrow output")]
    Native,
}

impl EmitterBodyForm {
    /// Whether a configured body is one of the forms this entity declares.
    pub const fn admits(self, body: &crate::EmitterBody) -> bool {
        matches!(
            (self, body),
            (
                Self::Codec | Self::CodecOrWithoutBody,
                crate::EmitterBody::Codec { .. }
            ) | (Self::CodecOrWithoutBody, crate::EmitterBody::WithoutBody)
                | (Self::Values, crate::EmitterBody::Values)
                | (Self::Native, crate::EmitterBody::Client)
        )
    }

    /// Whether every emitter of this form names a codec.
    pub const fn requires_codec(self) -> bool {
        match self {
            Self::Codec => true,
            Self::CodecOrWithoutBody | Self::Values | Self::Native => false,
        }
    }
}

/// What every emitter of one entity declares, whatever its statement says: everything a statement
/// can say about it, and what it does with what a statement says.
///
/// This lives in the vocabulary so the registry can validate an emitter, and a host can prepare
/// one, without naming the connector crate that implements its emitter. The grammar, completion,
/// documentation and the generated conformance tests read the same declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitterEntityCapabilities {
    /// What the `write_header` calls of an emitter's record become on the message it publishes,
    /// or `None` where its messages carry no transport headers.
    pub headers: Option<HeaderModel>,
    /// What its body names to publish to.
    pub reference: EmitterReferenceKind,
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

impl EmitterEntityCapabilities {
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

    /// Whether its emitters may write the envelope clause `kind`.
    ///
    /// An entity declares each of the nine clause kinds at most once.
    pub fn declares_envelope_clause(&self, kind: EnvelopeClauseKind) -> bool {
        self.envelope.contains(&kind)
    }

    /// The property its emitters' `PROPERTIES { … }` may set as `name`, where it declares one.
    ///
    /// An entity declares its closed property set in one constant, so scanning it is bounded by the
    /// declaration rather than by any statement.
    pub fn property(&self, name: &str) -> Option<&'static PropertyDeclaration> {
        self.properties
            .iter()
            .find(|property| property.name == name)
    }

    /// What a statement can make every emitter of the entity do at run time: publish in each
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

/// The declaration one emitter entity's Model makes for every statement of that entity.
pub trait EmitterDeclaration {
    const CAPABILITIES: EmitterEntityCapabilities;
}

/// What the body of one emitter statement states: what it publishes to and through, and the values it
/// writes directly.
pub trait EmitterEntityBody: EmitterPublication {
    /// The client the body publishes through, or the schema its session consumers receive.
    fn reference(&self) -> EmitterReference<'_>;

    /// The client the body publishes through, to rebind it. A body that names a schema has none.
    fn client_mut(&mut self) -> Option<&mut ClientName>;

    /// The catalog client the body commits through, for a body that names one beside the client
    /// it publishes through.
    fn catalog_client(&self) -> Option<EmitterClientReference<'_>> {
        None
    }

    /// The expressions the body sends across its external boundary directly, without a codec.
    fn value_mappings(&self) -> EmitterValueMappings<'_> {
        EmitterValueMappings::none()
    }

    /// The exact types direct mappings may compute. Codec and native bodies have none.
    fn mapped_value_types(&self) -> Option<crate::MappedValueTypes> {
        None
    }

    /// The commit cadence and maximum commit size as written, for a body that stages what it
    /// publishes and commits it separately.
    fn commit_policy(&self) -> Option<(&str, crate::PayloadSizeLimit)> {
        None
    }

    /// The key the body's destination routes each record by, for a destination that computes one
    /// for every record: a `RABBITMQ EXCHANGE` emitter's `ROUTING KEY`.
    fn routing_key(&self) -> Option<&Expression> {
        None
    }
}

/// What the kind of one connector's emitter entities answers for each of them.
pub trait EmitterEntityMetadata {
    fn connector(&self) -> ConnectorKind;

    /// The keyword that names the entity within its connector.
    fn keyword(&self) -> &'static str;

    /// The name of the pair: the connector's keyword, then the entity's.
    fn phrase(&self) -> &'static str;

    fn capabilities(&self) -> EmitterEntityCapabilities;
}

emitter_destinations! {
    /// The destination an emitter publishes to, nested by the connector that serves it and the
    /// entity it writes.
    pub enum EmitterEntity as EmitterEntityBody;
    /// The connector and entity of an emitter, as capability decisions read them before an emitter is
    /// fully parsed.
    pub enum EmitterEntityKind as EmitterEntityMetadata;
    pub enum EmitterModeChange;
    models {
        Client(ClientEmitter, ClientEmitterEntity, ClientEmitterMode),
        Http(HttpEmitter, HttpEmitterEntity, HttpRequestEmitterMode),
        Kafka(KafkaEmitter, KafkaEmitterEntity, KafkaTopicEmitterMode),
        Pulsar(PulsarEmitter, PulsarEmitterEntity, PulsarTopicEmitterMode),
        RabbitMq(RabbitMqEmitter, RabbitMqEmitterEntity, RabbitMqEmitterMode),
        Redis(RedisEmitter, RedisEmitterEntity, RedisPubSubEmitterMode),
        Mqtt(MqttEmitter, MqttEmitterEntity, MqttTopicEmitterMode),
        Nats(NatsEmitter, NatsEmitterEntity, NatsSubjectEmitterMode),
        ZeroMq(ZeroMqEmitter, ZeroMqEmitterEntity, ZeroMqPushEmitterMode),
        Sqs(SqsEmitter, SqsEmitterEntity, SqsQueueEmitterMode),
        Sentry(SentryEmitter, SentryEmitterEntity, SentryEventEmitterMode),
        Syslog(SyslogEmitter, SyslogEmitterEntity, SyslogCollectorEmitterMode),
        Otel(OtelEmitter, OtelEmitterEntity, OtelEmitterMode),
        ClickHouse(ClickHouseEmitter, ClickHouseEmitterEntity, ClickHouseTableEmitterMode),
        Postgres(PostgresEmitter, PostgresEmitterEntity, PostgresTableEmitterMode),
        MySql(MySqlEmitter, MySqlEmitterEntity, MySqlTableEmitterMode),
        MongoDb(MongoDbEmitter, MongoDbEmitterEntity, MongoDbCollectionEmitterMode),
        Iceberg(IcebergEmitter, IcebergEmitterEntity, IcebergTableEmitterMode),
    }
}

/// A pair is written as its phrase: the connector's keyword, then the entity's.
impl Display for EmitterEntityKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.phrase())
    }
}

impl EmitterEntityKind {
    /// The connector that serves this emitter.
    pub fn connector(self) -> ConnectorKind {
        self.entity().connector()
    }

    /// The keyword that names the entity this emitter writes.
    pub fn entity_keyword(self) -> &'static str {
        self.entity().keyword()
    }

    /// The name of this pair: its connector's keyword, then its entity's.
    pub fn phrase(self) -> &'static str {
        self.entity().phrase()
    }

    /// What every emitter of this pair declares.
    pub fn capabilities(self) -> EmitterEntityCapabilities {
        self.entity().capabilities()
    }
}

impl EmitterEntity {
    /// The connector that serves this emitter.
    pub fn connector(&self) -> ConnectorKind {
        self.kind().connector()
    }

    /// The entity-owned publication policy, borrowed for inspection.
    pub fn publishing(&self) -> EmitterPublishing<'_> {
        self.body().publishing()
    }

    pub fn mode_change(&self) -> EmitterModeChange {
        self.body().mode_change()
    }

    pub(crate) fn replace_mode(
        &mut self,
        mode: &EmitterModeChange,
    ) -> error_stack::Result<(), AlterEmitterError> {
        self.body_mut().replace_mode(mode)
    }

    pub fn capabilities(&self) -> EmitterEntityCapabilities {
        self.kind().capabilities()
    }

    /// The Plexus connector and entity phrase, as inspection and diagnostics name it.
    pub fn transport_label(&self) -> &'static str {
        self.kind().phrase()
    }

    /// What this emitter's body names to publish to.
    pub fn reference(&self) -> EmitterReference<'_> {
        self.body().reference()
    }

    /// The client this emitter publishes through. A emitter that serves session consumers has none.
    pub fn client(&self) -> Option<&ClientName> {
        match self.reference() {
            EmitterReference::Client(reference) => Some(reference.client),
            EmitterReference::StorageClient(client) => Some(client),
            EmitterReference::Schema(_) => None,
        }
    }

    pub(crate) fn client_mut(&mut self) -> Option<&mut ClientName> {
        self.body_mut().client_mut()
    }

    /// The catalog client this emitter commits through, for an emitter that names one beside the client
    /// it publishes through.
    pub fn catalog_client(&self) -> Option<EmitterClientReference<'_>> {
        self.body().catalog_client()
    }

    /// The expressions a direct `VALUES` emitter sends across its external boundary, in written order.
    pub fn value_mappings(&self) -> EmitterValueMappings<'_> {
        self.body().value_mappings()
    }

    /// The exact mapped types declared by this configured entity.
    pub fn mapped_value_types(&self) -> Option<crate::MappedValueTypes> {
        self.body().mapped_value_types()
    }

    /// The expression that computes the key this emitter's destination routes each record by, for
    /// a destination that computes one.
    pub fn routing_key(&self) -> Option<&Expression> {
        self.body().routing_key()
    }

    pub fn direct_value_mappings(&self) -> impl Iterator<Item = &EmitterValueMapping> {
        self.body().value_mappings().iter()
    }

    pub fn requires_codec(&self) -> bool {
        self.capabilities().body.requires_codec()
    }

    /// Whether an emitter publishing to this emitter may write the batching clause, must write it,
    /// and how large a batch may grow.
    pub fn batch_legality(&self) -> EmitterBatchLegality {
        self.capabilities().batch
    }

    /// True when a batch can only be published through the codec's `ON EMITTING BATCH` value.
    pub fn requires_batch_transformation(&self) -> bool {
        self.capabilities().requires_batch_transformation
    }

    pub fn commit_policy(&self) -> Option<(&str, crate::PayloadSizeLimit)> {
        self.body().commit_policy()
    }

    /// Whether `other` is this emitter published through a possibly different client: the same
    /// connector and entity, with every field but the client equal.
    pub fn same_definition(&self, other: &Self) -> bool {
        if self.kind() != other.kind() {
            return false;
        }
        let mut rebound = other.clone();
        match (self.client(), rebound.client_mut()) {
            (Some(client), Some(rebound_client)) => *rebound_client = client.clone(),
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => return false,
        }
        rebound
            .replace_mode(&self.mode_change())
            .verified("the exact entity kinds were compared above");
        *self == rebound
    }
}

/// The exact entity a concrete emitter destination represents.
pub trait EmitterEntityModel {
    const KIND: EmitterEntityKind;
    type PublishingMode;
}

/// A borrowed publication policy and a typed update of that entity's policy.
pub trait EmitterPublication {
    fn publishing(&self) -> EmitterPublishing<'_>;
    fn mode_change(&self) -> EmitterModeChange;
    fn replace_mode(
        &mut self,
        mode: &EmitterModeChange,
    ) -> error_stack::Result<(), AlterEmitterError>;
}
