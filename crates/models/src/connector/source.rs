//! The external transports an ingestor reads, and what every one of them declares.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The union of transport source Models and the kind that names every source pair,
//!   the declaration each source entity makes whatever its statement says, the contract the body
//!   of a source statement fulfils, the union of the alterations an `ALTER … INGESTOR` statement
//!   holds, and the quiesce, delivery and acknowledgement vocabulary those bodies share.
//! - **Depends on.** The connector modules' entity Models and alterations, the metadata
//!   namespaces, and the names a body references.
//! - **Must not know.** How a source is opened, polled, quiesced or acknowledged at run time.
//!
//! The head of an ingestor statement names the pair it reads, so an alteration holds the
//! operations of one pair's entity Model and can set no source of another pair.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{
    fmt::{Display, Formatter},
    num::NonZeroU64,
};

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use super::{
    ClientIngestorAlteration, ClientSourceEntity, HttpIngestorAlteration, HttpSource,
    HttpSourceEntity, KafkaIngestorAlteration, KafkaSource, KafkaSourceEntity,
    MqttIngestorAlteration, MqttSource, MqttSourceEntity, NatsIngestorAlteration, NatsSource,
    NatsSourceEntity, PrometheusIngestorAlteration, PrometheusSource, PrometheusSourceEntity,
    PulsarIngestorAlteration, PulsarSource, PulsarSourceEntity, RabbitMqIngestorAlteration,
    RabbitMqSource, RabbitMqSourceEntity, RedisIngestorAlteration, RedisSource, RedisSourceEntity,
    SqsIngestorAlteration, SqsSource, SqsSourceEntity, SyslogIngestorAlteration, SyslogSource,
    SyslogSourceEntity, WebsocketsIngestorAlteration, WebsocketsSource, WebsocketsSourceEntity,
    ZeroMqIngestorAlteration, ZeroMqSource, ZeroMqSourceEntity,
    entity_union::{entity_union, ingestor_alteration_union},
};
use crate::{
    AlterIngestorError, ClientName, ConnectorKind, EndpointName, EndpointType, IngestorInput,
    Model, ModelKind, ModelName, RetryPolicy, SourceMetadataScope,
};

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

/// The acknowledgement a source's delivery mode declares, with its durations as written.
///
/// Every delivery mode states its acknowledgement through this one shape, so validating a mode and
/// parsing it into the policy a source runs read the same declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestAcknowledgement<'a> {
    /// No message waits on an acknowledgement.
    Unacknowledged,
    /// One message at a time waits on its acknowledgement.
    Sequential {
        timeout: &'a str,
        retry: &'a RetryPolicy,
    },
    /// Up to `max` messages wait on their acknowledgements together.
    Parallel {
        max: NonZeroU64,
        batch_timeout: &'a str,
        timeout: &'a str,
        retry: &'a RetryPolicy,
    },
}

/// What kind of model the body of a source statement names to read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceReferenceKind {
    /// A configured client of one connector.
    Client(ConnectorKind),
    /// An endpoint of one type this cluster serves.
    Endpoint(EndpointType),
    /// The schema every batch a producer submits matches.
    Schema,
}

impl SourceReferenceKind {
    /// The kind of the model a body of this reference kind names.
    pub const fn model_kind(self) -> ModelKind {
        match self {
            Self::Client(_) => ModelKind::Client,
            Self::Endpoint(_) => ModelKind::Endpoint,
            Self::Schema => ModelKind::Schema,
        }
    }

    /// The connector a referenced client must configure. A body that names an endpoint or a
    /// schema references no client.
    pub const fn client_connector(self) -> Option<ConnectorKind> {
        match self {
            Self::Client(connector) => Some(connector),
            Self::Endpoint(_) | Self::Schema => None,
        }
    }

    /// The type a referenced endpoint must be. A body that names a client or a schema references
    /// no endpoint.
    pub const fn endpoint_type(self) -> Option<EndpointType> {
        match self {
            Self::Endpoint(endpoint_type) => Some(endpoint_type),
            Self::Client(_) | Self::Schema => None,
        }
    }

    /// Whether a body of this reference kind can name `model`: a client of its connector, an
    /// endpoint of its type, or a schema.
    pub fn admits<Version>(self, model: &Model<Version>) -> bool {
        match self {
            Self::Client(connector) => model.connector_kind() == Some(connector),
            Self::Endpoint(endpoint_type) => {
                let Model::Endpoint(endpoint) = model else {
                    return false;
                };
                endpoint.endpoint_type == endpoint_type
            }
            Self::Schema => model.kind() == ModelKind::Schema,
        }
    }
}

/// How an endpoint's requests or messages are delivered. The endpoint answers each one when its
/// ingestor admits it, so none waits on an acknowledgement.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EndpointIngestMode {
    NoAck,
}

impl EndpointIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }

    /// Whether an endpoint delivering in this mode can honor `quiesce`: it holds what arrives in a
    /// bounded buffer, or refuses it with a time to retry after.
    pub const fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        match self {
            Self::NoAck => matches!(
                quiesce,
                IngestQuiesceMode::EndpointBuffer { .. } | IngestQuiesceMode::Reject { .. }
            ),
        }
    }
}

/// The model the body of one transport source names to read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceReference<'a> {
    Client(&'a ClientName),
    Endpoint(&'a EndpointName),
}

impl SourceReference<'_> {
    pub fn name(self) -> ModelName {
        match self {
            Self::Client(client) => client.into(),
            Self::Endpoint(endpoint) => endpoint.into(),
        }
    }
}

/// What a source's message positions still mean once the connection that delivered them is gone,
/// and so what a host does with an acknowledgement or a rejection it could not complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum PositionClass {
    /// A position names a place in a log the source can seek back to on any connection, such as
    /// a Kafka offset. A rejection that failed is retried with the same positions after the
    /// source reconnects, before it reads again.
    Replayable,
    /// A position names one delivery on one connection, such as a RabbitMQ delivery tag, an SQS
    /// receipt handle or an MQTT packet identifier. Once that connection is lost the broker
    /// redelivers everything it delivered and was not acknowledged, so after a reconnect an
    /// acknowledgement or a rejection still pending for it is discarded and counted as a
    /// transport redelivery.
    BrokerRedelivered,
    /// The source keeps no position: nothing is acknowledged or rejected on the transport, so
    /// nothing is ever pending.
    None,
}

/// What every source of one entity declares, whatever its statement says.
///
/// This lives in the vocabulary so the registry can validate an ingestor, and a host can prepare
/// one, without naming the connector crate that implements its source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestSourceCapabilities {
    /// Whether its messages carry transport headers that `read_header` and `read_headers` can
    /// read.
    pub reads_headers: bool,
    /// The typed `metadata` namespace its messages expose to the ingestor's programs.
    pub metadata: SourceMetadataScope,
    /// Whether it executes on every live cluster node rather than on one scheduled owner.
    pub executes_on_every_cluster_node: bool,
    /// What its body names to read from.
    pub reference: SourceReferenceKind,
    /// What its message positions mean once the connection that delivered them is gone.
    pub position_class: PositionClass,
}

/// The declaration one source entity's Model makes for every statement of that entity.
pub trait SourceEntity {
    const CAPABILITIES: IngestSourceCapabilities;
}

/// What the body of one transport source statement states: what it reads through, how it behaves
/// while its ingestor is quiesced, and what its delivery mode acknowledges.
pub trait SourceBody {
    /// The client or endpoint the body names.
    fn reference(&self) -> SourceReference<'_>;

    fn quiesce(&self) -> &IngestQuiesceMode;

    fn quiesce_mut(&mut self) -> &mut IngestQuiesceMode;

    /// Whether a source with this body can honor `quiesce`. A body's own delivery mode can
    /// decide, as an MQTT session does.
    fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool;

    /// The acknowledgement this body's delivery mode declares. A body that declares no delivery
    /// mode acknowledges nothing.
    fn acknowledgement(&self) -> IngestAcknowledgement<'_>;
}

/// The pair one entity Model is a source of, which the head of its ingestor's statements names.
pub trait IngestorEntityModel {
    const KIND: IngestSourceKind;
}

/// A source Model an alteration's `SET FROM` states, and how the alteration reaches the stored
/// input of the ingestor it changes, which reads this Model's pair.
pub(crate) trait AlteredIngestorSource: IngestorEntityModel + Clone {
    /// Makes this Model the source `input` reads.
    fn set_into(&self, input: &mut IngestorInput);

    /// Sets the quiesce mode of the source `input` reads, which its pair must honor.
    fn set_quiesce_of(
        input: &mut IngestorInput,
        quiesce: IngestQuiesceMode,
    ) -> error_stack::Result<(), AlterIngestorError>;
}

/// What the kind of one connector's source entities answers for each of them.
pub trait SourceEntityKind {
    fn connector(&self) -> ConnectorKind;

    /// The keyword that names the entity within its connector.
    fn keyword(&self) -> &'static str;

    /// The name of the pair: the connector's keyword, then the entity's.
    fn phrase(&self) -> &'static str;

    /// The label a form shows for a source of this entity.
    fn form_label(&self) -> &'static str;

    fn capabilities(&self) -> IngestSourceCapabilities;
}

entity_union! {
    /// An external transport an ingestor reads, nested by the connector that serves it and the
    /// entity it consumes.
    ///
    /// A client source is not one of these: its batches need no codec, so
    /// [`IngestorInput`](crate::IngestorInput) holds it beside the transport contract instead.
    pub enum IngestSource as SourceBody;
    /// The connector and entity of a source, as capability decisions read them before an
    /// ingestor is fully parsed.
    pub enum IngestSourceKind as SourceEntityKind;
    kinds {
        Client(ClientSourceEntity),
    }
    models {
        Http(HttpSource, HttpSourceEntity),
        Kafka(KafkaSource, KafkaSourceEntity),
        Pulsar(PulsarSource, PulsarSourceEntity),
        Mqtt(MqttSource, MqttSourceEntity),
        Nats(NatsSource, NatsSourceEntity),
        RabbitMq(RabbitMqSource, RabbitMqSourceEntity),
        Redis(RedisSource, RedisSourceEntity),
        Prometheus(PrometheusSource, PrometheusSourceEntity),
        ZeroMq(ZeroMqSource, ZeroMqSourceEntity),
        Sqs(SqsSource, SqsSourceEntity),
        Websockets(WebsocketsSource, WebsocketsSourceEntity),
        Syslog(SyslogSource, SyslogSourceEntity),
    }
}

ingestor_alteration_union! {
    /// The operations of one `ALTER <CONNECTOR> <ENTITY> INGESTOR` statement, nested by the pair
    /// its head names, so its `SET FROM` can only state a source of that pair.
    pub enum IngestorAlteration;
    Client(ClientIngestorAlteration),
    Http(HttpIngestorAlteration),
    Kafka(KafkaIngestorAlteration),
    Pulsar(PulsarIngestorAlteration),
    Mqtt(MqttIngestorAlteration),
    Nats(NatsIngestorAlteration),
    RabbitMq(RabbitMqIngestorAlteration),
    Redis(RedisIngestorAlteration),
    Prometheus(PrometheusIngestorAlteration),
    ZeroMq(ZeroMqIngestorAlteration),
    Sqs(SqsIngestorAlteration),
    Websockets(WebsocketsIngestorAlteration),
    Syslog(SyslogIngestorAlteration),
}

/// A pair is written as its phrase: the connector's keyword, then the entity's.
impl Display for IngestSourceKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.phrase())
    }
}

impl IngestSourceKind {
    /// The connector that serves this source.
    pub fn connector(self) -> ConnectorKind {
        self.entity().connector()
    }

    /// The keyword that names the entity this source consumes.
    pub fn entity_keyword(self) -> &'static str {
        self.entity().keyword()
    }

    /// The name of this pair: its connector's keyword, then its entity's.
    pub fn phrase(self) -> &'static str {
        self.entity().phrase()
    }

    /// What every source of this pair declares.
    pub fn capabilities(self) -> IngestSourceCapabilities {
        self.entity().capabilities()
    }

    /// The key a form identifies this pair by: its phrase in lower case, joined by a hyphen.
    pub fn form_key(self) -> String {
        self.phrase().to_ascii_lowercase().replace(' ', "-")
    }

    /// The label a form shows for this pair.
    pub fn form_label(self) -> &'static str {
        self.entity().form_label()
    }

    pub fn reads_headers(self) -> bool {
        self.capabilities().reads_headers
    }

    /// The connector a client this source reads through must configure. A source that names an
    /// endpoint or a schema reads through no client.
    pub fn client_connector(self) -> Option<ConnectorKind> {
        self.capabilities().reference.client_connector()
    }
}

impl IngestSource {
    /// The connector that serves this source.
    pub fn connector(&self) -> ConnectorKind {
        self.kind().connector()
    }

    pub fn executes_on_every_cluster_node(&self) -> bool {
        self.kind().capabilities().executes_on_every_cluster_node
    }

    /// The client or endpoint this source's body names.
    pub fn reference(&self) -> SourceReference<'_> {
        self.body().reference()
    }

    /// The name of the client or endpoint this source reads through.
    pub fn source_ref(&self) -> ModelName {
        self.reference().name()
    }

    /// The kind of the model [`Self::source_ref`] names.
    pub fn source_kind(&self) -> ModelKind {
        self.kind().capabilities().reference.model_kind()
    }

    /// Whether the messages this source reads carry transport headers that `read_header` and
    /// `read_headers` can read.
    pub fn reads_headers(&self) -> bool {
        self.kind().reads_headers()
    }

    /// The acknowledgement this source's delivery mode declares. A source whose statement declares
    /// no delivery mode acknowledges nothing.
    pub fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.body().acknowledgement()
    }

    pub fn quiesce(&self) -> &IngestQuiesceMode {
        self.body().quiesce()
    }

    pub fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        self.body().supports_quiesce(quiesce)
    }

    pub fn set_quiesce(
        &mut self,
        quiesce: IngestQuiesceMode,
    ) -> error_stack::Result<(), AlterIngestorError> {
        if !self.supports_quiesce(&quiesce) {
            return Err(Report::new(AlterIngestorError::UnsupportedQuiesceMode {
                kind: self.kind(),
                mode: quiesce.kind_label(),
            }));
        }
        *self.body_mut().quiesce_mut() = quiesce;
        Ok(())
    }
}
