//! The external transports an ingestor reads, and what every one of them declares.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The union of transport source Models and the kind that names every source pair,
//!   the declaration each source entity makes whatever its statement says, the contract the body
//!   of a source statement fulfils, the union of the alterations an `ALTER … INGESTOR` statement
//!   holds, and the quiesce, delivery and acknowledgement vocabulary those bodies share.
//! - **Depends on.** The connector modules' entity Models and alterations, the metadata
//!   namespaces, the shapes a declaration is written in, and the names a body references.
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
    num::{NonZeroU16, NonZeroU64},
};

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter};

use super::{
    ChoiceRequirement, ClientIngestorAlteration, ClientIngestorEntity, DeclaredBounds, HeaderModel,
    HttpIngestor, HttpIngestorEntity, HttpRequestIngestorAlteration, KafkaIngestor,
    KafkaIngestorEntity, KafkaTopicIngestorAlteration, LocatorDeclaration, MqttIngestor,
    MqttIngestorEntity, MqttTopicIngestorAlteration, NatsIngestor, NatsIngestorEntity,
    NatsSubjectIngestorAlteration, OptionDeclaration, PrometheusIngestor, PrometheusIngestorEntity,
    PrometheusQueryIngestorAlteration, PulsarIngestor, PulsarIngestorEntity,
    PulsarTopicIngestorAlteration, RabbitMqIngestor, RabbitMqIngestorEntity,
    RabbitMqQueueIngestorAlteration, RedisIngestor, RedisIngestorAlteration, RedisIngestorEntity,
    RuntimeCapability, SqsIngestor, SqsIngestorEntity, SqsQueueIngestorAlteration, SyslogIngestor,
    SyslogIngestorEntity, SyslogListenerIngestorAlteration, WebsocketsIngestor,
    WebsocketsIngestorAlteration, WebsocketsIngestorEntity, ZeroMqIngestor, ZeroMqIngestorEntity,
    ZeroMqPullIngestorAlteration,
    entity_union::{entity_union, ingestor_alteration_union},
};
use crate::{
    AlterIngestorError, ClientName, ConnectorKind, EndpointName, EndpointType, IngestorInput,
    IngestorMetadataDeclaration, Model, ModelKind, ModelName, RetryPolicy,
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
        self.kind().keyword()
    }

    /// Which of the quiesce modes this one is, whatever sizes and durations it writes.
    pub const fn kind(&self) -> IngestQuiesceKind {
        match self {
            Self::Suspend => IngestQuiesceKind::Suspend,
            Self::Buffer { .. } => IngestQuiesceKind::Buffer,
            Self::Drop => IngestQuiesceKind::Drop,
            Self::Reject { .. } => IngestQuiesceKind::Reject,
            Self::EndpointBuffer { .. } => IngestQuiesceKind::EndpointBuffer,
        }
    }
}

/// The quiesce modes an `ON QUIESCE` clause names, whatever sizes and durations it writes.
///
/// An endpoint buffers what it admits without an overflow choice, so its buffer is a kind of its
/// own, which only an endpoint can honor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter)]
pub enum IngestQuiesceKind {
    Suspend,
    Buffer,
    Drop,
    Reject,
    EndpointBuffer,
}

impl IngestQuiesceKind {
    /// The control value of the declared quiesce keyword in a visual editor.
    pub fn form_key(self) -> String {
        self.keyword().to_ascii_lowercase()
    }

    /// The keyword that opens the mode after `ON QUIESCE`.
    pub const fn keyword(self) -> &'static str {
        match self {
            Self::Suspend => "SUSPEND",
            Self::Buffer | Self::EndpointBuffer => "BUFFER",
            Self::Drop => "DROP",
            Self::Reject => "REJECT",
        }
    }

    /// The mode written in full after `ON QUIESCE`, as documentation and generated statements
    /// write it.
    pub const fn example(self) -> &'static str {
        match self {
            Self::Suspend => "SUSPEND",
            Self::Buffer => "BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST",
            Self::Drop => "DROP",
            Self::Reject => "REJECT RETRY AFTER 5s",
            Self::EndpointBuffer => "BUFFER MAX SIZE 64MiB",
        }
    }
}

/// One quiesce mode an entity's transport can honor, and the option choices it needs written
/// beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredQuiesce {
    pub kind: IngestQuiesceKind,
    /// The choices of the entity's options a statement must make for its source to honor this
    /// mode, such as the persistent session at QoS 1 an MQTT broker keeps messages for.
    pub requires: &'static [ChoiceRequirement],
}

impl DeclaredQuiesce {
    pub const SUSPEND: Self = Self::honoring(IngestQuiesceKind::Suspend);
    pub const BUFFER: Self = Self::honoring(IngestQuiesceKind::Buffer);
    pub const DROP: Self = Self::honoring(IngestQuiesceKind::Drop);
    pub const REJECT: Self = Self::honoring(IngestQuiesceKind::Reject);
    pub const ENDPOINT_BUFFER: Self = Self::honoring(IngestQuiesceKind::EndpointBuffer);

    /// A quiesce mode every statement of the entity can name.
    const fn honoring(kind: IngestQuiesceKind) -> Self {
        Self {
            kind,
            requires: &[],
        }
    }

    /// This mode, honored only where a statement makes the `requires` choices.
    pub const fn requiring(self, requires: &'static [ChoiceRequirement]) -> Self {
        Self { requires, ..self }
    }
}

/// The acknowledgement an ingestor's `MODE` clause names, whatever window and durations it
/// writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter)]
pub enum IngestDeliveryKind {
    NoAck,
    /// Each endpoint request waits independently, within its declared request window.
    AckRequest,
    AckSequential,
    AckParallel,
}

impl IngestDeliveryKind {
    /// The control value of the declared delivery phrase in a visual editor.
    pub fn form_key(self) -> String {
        self.phrase().to_ascii_lowercase().replace(' ', "-")
    }

    /// Whether delivery waits for a downstream acknowledgement.
    pub const fn acknowledged(self) -> bool {
        match self {
            Self::NoAck => false,
            Self::AckRequest | Self::AckSequential | Self::AckParallel => true,
        }
    }

    /// How a `MODE` clause names it.
    pub const fn phrase(self) -> &'static str {
        match self {
            Self::NoAck => "NO ACK",
            Self::AckRequest => "ACK",
            Self::AckSequential => "ACK SEQUENTIAL",
            Self::AckParallel => "ACK PARALLEL",
        }
    }

    /// A sender must retry an acknowledged request itself; quiesce cannot retain it after the
    /// endpoint has answered it.
    pub const fn allows_quiesce(self, quiesce: IngestQuiesceKind) -> bool {
        match self {
            Self::AckRequest => matches!(quiesce, IngestQuiesceKind::Reject),
            Self::NoAck | Self::AckSequential | Self::AckParallel => true,
        }
    }
}

/// One delivery mode an entity's ingestors may name, and the option choices it needs written
/// beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredDelivery {
    pub kind: IngestDeliveryKind,
    /// The mode written in full after `MODE`, as documentation and generated statements write it.
    pub example: &'static str,
    /// The choices of the entity's options a statement must make to name this mode.
    pub requires: &'static [ChoiceRequirement],
}

impl DeclaredDelivery {
    pub const ACK_REQUEST: Self = Self {
        kind: IngestDeliveryKind::AckRequest,
        example: "ACK MAX 8 ACK TIMEOUT 5s",
        requires: &[],
    };
    pub const NO_ACK: Self = Self {
        kind: IngestDeliveryKind::NoAck,
        example: "NO ACK",
        requires: &[],
    };
    pub const ACK_SEQUENTIAL: Self = Self {
        kind: IngestDeliveryKind::AckSequential,
        example: "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        requires: &[],
    };
    /// A parallel window that collects acknowledgements for up to its batch timeout before it
    /// confirms them together.
    pub const ACK_PARALLEL: Self = Self {
        kind: IngestDeliveryKind::AckParallel,
        example: "ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s \
                  MAX 30s",
        requires: &[],
    };

    /// This mode, named only where a statement makes the `requires` choices.
    pub const fn requiring(self, requires: &'static [ChoiceRequirement]) -> Self {
        Self { requires, ..self }
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
    /// An endpoint's independent requests, whose senders own retry.
    Request { max: NonZeroU16, timeout: &'a str },
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
pub enum IngestorReferenceKind {
    /// A configured client of one connector.
    Client(ConnectorKind),
    /// An endpoint of one type this cluster serves.
    Endpoint(EndpointType),
    /// The schema every batch a producer submits matches.
    Schema,
}

impl IngestorReferenceKind {
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

impl IngestAcknowledgement<'_> {
    /// Which delivery mode declares this acknowledgement.
    pub const fn kind(&self) -> IngestDeliveryKind {
        match self {
            Self::Unacknowledged => IngestDeliveryKind::NoAck,
            Self::Request { .. } => IngestDeliveryKind::AckRequest,
            Self::Sequential { .. } => IngestDeliveryKind::AckSequential,
            Self::Parallel { .. } => IngestDeliveryKind::AckParallel,
        }
    }
}

/// Whether an endpoint answers on admission or after its request's downstream acknowledgement.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EndpointIngestMode {
    NoAck,
    Ack { max: NonZeroU16, timeout: String },
}

impl EndpointIngestMode {
    pub fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
            Self::Ack { max, timeout } => IngestAcknowledgement::Request { max: *max, timeout },
        }
    }
}

/// The model the body of one transport source names to read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestorReference<'a> {
    Client(&'a ClientName),
    Endpoint(&'a EndpointName),
}

impl IngestorReference<'_> {
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

/// How far a source's transport has confirmed its positions once acknowledging them returns.
///
/// The host reports no progress for an acknowledged batch until acknowledging it returns, so this
/// is what that progress means on the transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum AcknowledgementConfirmation {
    /// The transport answers every acknowledgement or commit, and acknowledging returns once the
    /// answer arrived, such as an SQS `DeleteMessage` response, a Kafka consumer-group commit or a
    /// Pulsar `CommandAckResponse`.
    Answered,
    /// The protocol does not answer an acknowledgement, and acknowledging returns once it is
    /// written to the connection, such as a RabbitMQ `basic.ack` or an MQTT `PUBACK`.
    Written,
    /// The source acknowledges nothing on its transport.
    None,
}

/// What every source of one entity declares, whatever its statement says: everything a statement
/// can say about it, and what it does with what a statement says.
///
/// This lives in the vocabulary so the registry can validate an ingestor, and a host can prepare
/// one, without naming the connector crate that implements its source. The grammar, completion,
/// documentation and the generated conformance tests read the same declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestorEntityCapabilities {
    /// How its messages carry the transport headers that `read_header` and `read_headers` read,
    /// or `None` where they carry none.
    pub headers: Option<HeaderModel>,
    /// The typed `metadata` namespace its messages expose to the ingestor's programs.
    pub metadata: IngestorMetadataDeclaration,
    /// Whether it executes on every live cluster node rather than on one scheduled owner.
    pub executes_on_every_cluster_node: bool,
    /// What its body names to read from.
    pub reference: IngestorReferenceKind,
    /// What its message positions mean once the connection that delivered them is gone.
    pub position_class: PositionClass,
    /// How far its transport has confirmed its positions once acknowledging them returns.
    pub acknowledgement_confirmation: AcknowledgementConfirmation,
    /// The delivery modes its ingestors may name, each kind at most once.
    pub delivery: &'static [DeclaredDelivery],
    /// The quiesce modes its transport can honor, each kind at most once.
    pub quiesce: &'static [DeclaredQuiesce],
    /// The name that locates what it reads, for an entity whose body names one.
    pub locator: Option<LocatorDeclaration>,
    /// The options its body writes after the locator, in the order a statement writes them.
    pub options: &'static [OptionDeclaration],
    /// What its connector holds and how long it waits.
    pub bounds: DeclaredBounds,
}

impl IngestorEntityCapabilities {
    /// Whether its messages carry transport headers that `read_header` and `read_headers` can
    /// read.
    pub const fn reads_headers(&self) -> bool {
        self.headers.is_some()
    }

    /// The declaration of the delivery mode `kind`, where the entity declares it.
    ///
    /// An entity declares at most one entry per kind, of the three kinds there are.
    pub fn declared_delivery(&self, kind: IngestDeliveryKind) -> Option<&'static DeclaredDelivery> {
        self.delivery.iter().find(|delivery| delivery.kind == kind)
    }

    /// The declaration of the quiesce mode `kind`, where the entity's transport can honor it.
    ///
    /// An entity declares at most one entry per kind, of the five kinds there are.
    pub fn declared_quiesce(&self, kind: IngestQuiesceKind) -> Option<&'static DeclaredQuiesce> {
        self.quiesce.iter().find(|quiesce| quiesce.kind == kind)
    }

    /// The option its body opens with `keywords`, where it declares one.
    ///
    /// An entity declares a handful of options in one constant, so scanning them is bounded by
    /// the declaration.
    pub fn option(&self, keywords: &str) -> Option<&'static OptionDeclaration> {
        self.options
            .iter()
            .find(|option| option.keywords == keywords)
    }

    /// What a statement can make every source of the entity do at run time: deliver in each
    /// declared mode, write each option or choice, read each typed metadata field, and report a
    /// stalled peer within each deadline key its waits name.
    pub fn runtime_capabilities(&self) -> Vec<RuntimeCapability> {
        let mut capabilities = Vec::new();
        for delivery in self.delivery {
            capabilities.push(RuntimeCapability::Delivery(delivery.kind));
        }
        capabilities.extend(RuntimeCapability::of_options(self.options));
        for field in self.metadata.fields() {
            capabilities.push(RuntimeCapability::Metadata {
                field: field.name(),
            });
        }
        capabilities.extend(RuntimeCapability::of_bounds(self.bounds));
        capabilities
    }

    /// Whether a source of this entity whose body is `body` can honor `quiesce`: the entity
    /// declares the mode, and the body makes every option choice the declaration requires for
    /// it.
    pub fn honors_quiesce(
        &self,
        quiesce: &IngestQuiesceMode,
        body: &dyn IngestorEntityBody,
    ) -> bool {
        if !body.acknowledgement().kind().allows_quiesce(quiesce.kind()) {
            return false;
        }
        let Some(declared) = self.declared_quiesce(quiesce.kind()) else {
            return false;
        };
        ChoiceRequirement::all_met_by(declared.requires, body)
    }
}

/// The declaration one source entity's Model makes for every statement of that entity.
pub trait IngestorDeclaration {
    const CAPABILITIES: IngestorEntityCapabilities;

    /// Whether this source can honor `quiesce`, as its entity declares.
    fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool
    where
        Self: IngestorEntityBody + Sized,
    {
        Self::CAPABILITIES.honors_quiesce(quiesce, self)
    }
}

/// What the body of one transport source statement states: what it reads through, how it behaves
/// while its ingestor is quiesced, and what its delivery mode acknowledges.
pub trait IngestorEntityBody {
    /// The client or endpoint the body names.
    fn reference(&self) -> IngestorReference<'_>;

    fn quiesce(&self) -> &IngestQuiesceMode;

    fn quiesce_mut(&mut self) -> &mut IngestQuiesceMode;

    /// The acknowledgement this body's delivery mode declares. A body that declares no delivery
    /// mode acknowledges nothing.
    fn acknowledgement(&self) -> IngestAcknowledgement<'_>;

    /// The name of the choice this body makes, or holds by default, for the declared option its
    /// entity opens with `option`. A body answers for the options a declared requirement names;
    /// its entity's declaration states which those are.
    fn chosen(&self, _option: &str) -> Option<&'static str> {
        None
    }

    /// Refuses a client configuration this entity cannot read, beyond the keys its side reads. A
    /// value that holds a template is judged when the entity starts.
    fn validate_client_config(
        &self,
        _config: &[crate::ClientConfigEntry],
    ) -> Result<(), Report<crate::ClientConfigSchemaError>> {
        Ok(())
    }
}

impl ChoiceRequirement {
    /// Whether `body` makes the choice this requirement needs.
    fn met_by(&self, body: &dyn IngestorEntityBody) -> bool {
        body.chosen(self.option) == Some(self.choice)
    }

    /// Whether `body` makes every choice `requirements` need.
    pub(crate) fn all_met_by(requirements: &[Self], body: &dyn IngestorEntityBody) -> bool {
        requirements
            .iter()
            .all(|requirement| requirement.met_by(body))
    }
}

/// The pair one entity Model is a source of, which the head of its ingestor's statements names.
pub trait IngestorEntityModel {
    const KIND: IngestorEntityKind;
}

/// A source Model an alteration's `SET FROM` states, and how the alteration reaches the stored
/// input of the ingestor it changes, which reads this Model's pair.
pub(crate) trait AlteredIngestorEntity: IngestorEntityModel + Clone {
    /// Makes this Model the source `input` reads.
    fn set_into(&self, input: &mut IngestorInput);

    /// Sets the quiesce mode of the source `input` reads, which its pair must honor.
    fn set_quiesce_of(
        input: &mut IngestorInput,
        quiesce: IngestQuiesceMode,
    ) -> error_stack::Result<(), AlterIngestorError>;
}

/// What the kind of one connector's source entities answers for each of them.
pub trait IngestorEntityMetadata {
    fn connector(&self) -> ConnectorKind;

    /// The keyword that names the entity within its connector.
    fn keyword(&self) -> &'static str;

    /// The name of the pair: the connector's keyword, then the entity's.
    fn phrase(&self) -> &'static str;

    /// The label a form shows for a source of this entity.
    fn form_label(&self) -> &'static str;

    fn capabilities(&self) -> IngestorEntityCapabilities;
}

entity_union! {
    /// An external transport an ingestor reads, nested by the connector that serves it and the
    /// entity it consumes.
    ///
    /// A client source is not one of these: its batches need no codec, so
    /// [`IngestorInput`](crate::IngestorInput) holds it beside the transport contract instead.
    pub enum IngestorEntity as IngestorEntityBody;
    /// The connector and entity of a source, as capability decisions read them before an
    /// ingestor is fully parsed.
    pub enum IngestorEntityKind as IngestorEntityMetadata;
    kinds {
        Client(ClientIngestorEntity),
    }
    models {
        Http(HttpIngestor, HttpIngestorEntity),
        Kafka(KafkaIngestor, KafkaIngestorEntity),
        Pulsar(PulsarIngestor, PulsarIngestorEntity),
        Mqtt(MqttIngestor, MqttIngestorEntity),
        Nats(NatsIngestor, NatsIngestorEntity),
        RabbitMq(RabbitMqIngestor, RabbitMqIngestorEntity),
        Redis(RedisIngestor, RedisIngestorEntity),
        Prometheus(PrometheusIngestor, PrometheusIngestorEntity),
        ZeroMq(ZeroMqIngestor, ZeroMqIngestorEntity),
        Sqs(SqsIngestor, SqsIngestorEntity),
        Websockets(WebsocketsIngestor, WebsocketsIngestorEntity),
        Syslog(SyslogIngestor, SyslogIngestorEntity),
    }
}

ingestor_alteration_union! {
    /// The operations of one `ALTER <CONNECTOR> <ENTITY> INGESTOR` statement, nested by the pair
    /// its head names, so its `SET FROM` can only state a source of that pair.
    pub enum IngestorAlteration;
    Client(ClientIngestorAlteration),
    Http(HttpRequestIngestorAlteration),
    Kafka(KafkaTopicIngestorAlteration),
    Pulsar(PulsarTopicIngestorAlteration),
    Mqtt(MqttTopicIngestorAlteration),
    Nats(NatsSubjectIngestorAlteration),
    RabbitMq(RabbitMqQueueIngestorAlteration),
    Redis(RedisIngestorAlteration),
    Prometheus(PrometheusQueryIngestorAlteration),
    ZeroMq(ZeroMqPullIngestorAlteration),
    Sqs(SqsQueueIngestorAlteration),
    Websockets(WebsocketsIngestorAlteration),
    Syslog(SyslogListenerIngestorAlteration),
}

/// A pair is written as its phrase: the connector's keyword, then the entity's.
impl Display for IngestorEntityKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.phrase())
    }
}

impl IngestorEntityKind {
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
    pub fn capabilities(self) -> IngestorEntityCapabilities {
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
        self.capabilities().reads_headers()
    }

    /// The connector a client this source reads through must configure. A source that names an
    /// endpoint or a schema reads through no client.
    pub fn client_connector(self) -> Option<ConnectorKind> {
        self.capabilities().reference.client_connector()
    }
}

impl IngestorEntity {
    /// The connector that serves this source.
    pub fn connector(&self) -> ConnectorKind {
        self.kind().connector()
    }

    /// Refuses a client this source cannot read through, beyond the keys its side reads.
    pub fn validate_client_config(
        &self,
        config: &[crate::ClientConfigEntry],
    ) -> Result<(), Report<crate::ClientConfigSchemaError>> {
        self.body().validate_client_config(config)
    }

    pub fn executes_on_every_cluster_node(&self) -> bool {
        self.kind().capabilities().executes_on_every_cluster_node
    }

    /// The client or endpoint this source's body names.
    pub fn reference(&self) -> IngestorReference<'_> {
        self.body().reference()
    }

    /// The name of the client or endpoint this source reads through.
    pub fn source_ref(&self) -> ModelName {
        self.reference().name()
    }

    /// The kind of the model [`Self::source_ref`] names.
    pub fn entity_kind(&self) -> ModelKind {
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

    /// Whether this source can honor `quiesce`: its entity declares the mode, and this source's
    /// statement makes every option choice the declaration requires for it.
    pub fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        self.kind()
            .capabilities()
            .honors_quiesce(quiesce, self.body())
    }

    /// The choice this source's statement makes, or holds by default, for the declared option
    /// opened by `option`.
    pub fn chosen(&self, option: &str) -> Option<&'static str> {
        self.body().chosen(option)
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
