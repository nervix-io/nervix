//! The destinations an emitter publishes to, and what every one of them declares.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The union of sink Models and the kind that names every sink pair, the declaration
//!   each sink entity makes whatever its statement says, the contract the body of a sink statement
//!   fulfils, and the value mapping the direct `VALUES` sinks share.
//! - **Depends on.** The connector modules' entity Models, the emitter's batching and publishing
//!   vocabulary, expressions, and the names a body references.
//! - **Must not know.** How a sink is opened, published to, retried or committed at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    ClickHouseSink, ClickHouseSinkEntity, ClientSink, ClientSinkEntity, HttpSink, HttpSinkEntity,
    IcebergSink, IcebergSinkEntity, KafkaSink, KafkaSinkEntity, MongoDbSink, MongoDbSinkEntity,
    MqttSink, MqttSinkEntity, MySqlSink, MySqlSinkEntity, NatsSink, NatsSinkEntity, OtelSink,
    OtelSinkEntity, PostgresSink, PostgresSinkEntity, PulsarSink, PulsarSinkEntity, RabbitMqSink,
    RabbitMqSinkEntity, RedisSink, RedisSinkEntity, SentrySink, SentrySinkEntity, SqsSink,
    SqsSinkEntity, SyslogSink, SyslogSinkEntity, ZeroMqSink, ZeroMqSinkEntity,
    entity_union::entity_union,
};
use crate::{
    ClientName, ConnectorKind, EmitterBatchRequirement, EmitterPublishingMode, Expression,
    PayloadSizeLimit, SchemaName,
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

/// The model the body of one sink names to publish to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkReference<'a> {
    /// A configured client, which must configure `connector`.
    Client {
        client: &'a ClientName,
        connector: ConnectorKind,
    },
    /// The schema of the native batches session consumers receive.
    Schema(&'a SchemaName),
}

/// What every sink of one entity declares, whatever its statement says.
///
/// This lives in the vocabulary so the registry can validate an emitter, and a host can prepare
/// one, without naming the connector crate that implements its sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitSinkCapabilities {
    /// Whether an emitter's programs can write transport headers with `write_header`.
    pub writes_headers: bool,
    /// Whether a codec encodes the payload this sink publishes.
    pub requires_codec: bool,
    /// Whether an emitter publishing to this sink may omit the batching clause.
    pub batch_requirement: EmitterBatchRequirement,
    /// Whether a batch can only be published through the codec's `ON EMITTING BATCH` value.
    pub requires_batch_transformation: bool,
    /// The largest batch payload the destination protocol itself admits, where it fixes one.
    pub batch_size_maximum: Option<PayloadSizeLimit>,
}

/// The declaration one sink entity's Model makes for every statement of that entity.
pub trait SinkEntity {
    const CAPABILITIES: EmitSinkCapabilities;

    /// Whether a sink of this entity can publish under `mode`.
    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool;
}

/// What the body of one sink statement states: what it publishes to and through, and the values it
/// writes directly.
pub trait SinkBody {
    /// The client the body publishes through, or the schema its session consumers receive.
    fn reference(&self) -> SinkReference<'_>;

    /// The client the body publishes through, to rebind it. A body that names a schema has none.
    fn client_mut(&mut self) -> Option<&mut ClientName>;

    /// The catalog client the body commits through, for a body that names one beside its own.
    fn catalog_client(&self) -> Option<&ClientName> {
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

    /// How NSPL spells the entity.
    fn keyword(&self) -> &'static str;

    /// The connector and the entity together, as a statement head names them.
    fn phrase(&self) -> &'static str;

    fn capabilities(&self) -> EmitSinkCapabilities;

    fn accepts_publishing_mode(&self, mode: &EmitterPublishingMode) -> bool;
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

impl EmitSinkKind {
    /// The connector that serves this sink.
    pub fn connector(self) -> ConnectorKind {
        self.entity().connector()
    }

    /// How NSPL spells the entity this sink writes.
    pub fn entity_keyword(self) -> &'static str {
        self.entity().keyword()
    }

    /// The connector and the entity together, as a statement head names them.
    pub fn phrase(self) -> &'static str {
        self.entity().phrase()
    }

    /// What every sink of this pair declares.
    pub fn capabilities(self) -> EmitSinkCapabilities {
        self.entity().capabilities()
    }

    /// Whether a sink of this pair can publish under `mode`.
    pub fn accepts_publishing_mode(self, mode: &EmitterPublishingMode) -> bool {
        self.entity().accepts_publishing_mode(mode)
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
            SinkReference::Client { client, .. } => Some(client),
            SinkReference::Schema(_) => None,
        }
    }

    pub(crate) fn client_mut(&mut self) -> Option<&mut ClientName> {
        self.body_mut().client_mut()
    }

    /// The catalog client this sink commits through, for a sink that names one beside its own.
    pub fn catalog_client(&self) -> Option<&ClientName> {
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
            SinkReference::Client { connector, .. } => Some(connector),
            SinkReference::Schema(_) => None,
        }
    }

    pub fn requires_codec(&self) -> bool {
        self.capabilities().requires_codec
    }

    /// Whether an emitter publishing to this sink may omit the batching clause.
    pub fn batch_requirement(&self) -> EmitterBatchRequirement {
        self.capabilities().batch_requirement
    }

    /// True when a batch can only be published through the codec's `ON EMITTING BATCH` value.
    pub fn requires_batch_transformation(&self) -> bool {
        self.capabilities().requires_batch_transformation
    }

    /// The largest batch payload the destination protocol itself admits, where it fixes one.
    pub fn batch_size_maximum(&self) -> Option<PayloadSizeLimit> {
        self.capabilities().batch_size_maximum
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
