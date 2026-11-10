//! Typed semantic choices for structured client controls.
//!
//! Layer: edges.
//!
//! - **Owns.** The session request and reply shapes for typed choice lookup, their bounded paging,
//!   typed values, and presentation metadata.
//! - **Depends on.** The shared wire codec and vocabulary names, node references, and placement
//!   policy.
//! - **Must not know.** Browser draft state, parser expectations, registry snapshots, or how a
//!   server resolves a choice.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::Report;
use flatbuffers::WIPOffset;
use nervix_models::{
    ClientIngestorEntity, DomainName, FieldName, HttpIngestorEntity, IngestorEntityKind,
    KafkaIngestorEntity, MqttIngestorEntity, NatsIngestorEntity, NodeRef, PlacementPolicy,
    PrometheusIngestorEntity, PulsarIngestorEntity, RabbitMqIngestorEntity, RedisIngestorEntity,
    RequestedResourceVersion, ResourceName, SqsIngestorEntity, SyslogIngestorEntity,
    WebsocketsIngestorEntity, ZeroMqIngestorEntity,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    common::{WireValueError, decode_node_ref, encode_node_ref},
    wire,
};

/// The semantic question a structured control asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChoiceTarget {
    DomainPace,
    PlacementPolicy,
    Schema,
    Branch,
    Relay,
    /// The fields of one relay's records, asked with the domain and then the relay as
    /// dependencies.
    RelayField,
    WireJsonSchema,
    WireCborSchema,
    WireAvroSchema,
    Resource,
    CompletedResourceVersion,
    Vhost,
    SignalingProtocol,
    Codec,
    /// Fields of the output schema selected by one configured codec.
    CodecField,
    IngestHttpRequestSource,
    IngestHttpEndpointSource,
    IngestKafkaSource,
    IngestPulsarSource,
    IngestMqttSource,
    IngestNatsSource,
    IngestNatsStreamSource,
    IngestRabbitMqSource,
    IngestRedisPubSubSource,
    IngestPrometheusSource,
    IngestZeroMqSource,
    IngestSqsSource,
    IngestWebsocketsConnectionSource,
    IngestWebsocketsEndpointSource,
    IngestSyslogSource,
    IngestCodec,
    IngestUnbranchedRelay,
    IngestBranchedRelay,
    BranchField,
    ProcessorCompatibleInputRelay,
    ProcessorInputBranchRelay,
    ProcessorMaterializedRelay,
    EmitterHttpRequestReference,
    EmitterKafkaTopicReference,
    EmitterPulsarTopicReference,
    EmitterRabbitMqQueueReference,
    EmitterRabbitMqExchangeReference,
    EmitterRedisPubSubReference,
    EmitterMqttTopicReference,
    EmitterNatsSubjectReference,
    EmitterNatsStreamReference,
    EmitterZeroMqPushReference,
    EmitterSqsQueueReference,
    EmitterSentryEventReference,
    EmitterSyslogCollectorReference,
    EmitterOtelLogsReference,
    EmitterOtelTracesReference,
    EmitterOtelMetricsReference,
    EmitterClickHouseTableReference,
    EmitterPostgresTableReference,
    EmitterMySqlTableReference,
    EmitterMongoDbCollectionReference,
    EmitterIcebergTableReference,
    EmitterIcebergCatalog,
    EmitterCodec,
    EmitterCompatibleInputRelay,
    EmitterMaterializedRelay,
    EmitterErrorRelay,
    SchemaField,
    /// Relays with materialized state on exactly a generator's branch, asked with the domain
    /// alone for an unbranched generator, or with the domain and then its named branch.
    GeneratorMaterializedRelay,
}

wire_enum!(ALL_CHOICE_TARGETS: ChoiceTarget => wire::ChoiceTarget {
    DomainPace,
    PlacementPolicy,
    Schema,
    Branch,
    Relay,
    RelayField,
    WireJsonSchema,
    WireCborSchema,
    WireAvroSchema,
    Resource,
    CompletedResourceVersion,
    Vhost,
    SignalingProtocol,
    Codec,
    CodecField,
    IngestHttpRequestSource,
    IngestHttpEndpointSource,
    IngestKafkaSource,
    IngestPulsarSource,
    IngestMqttSource,
    IngestNatsSource,
    IngestNatsStreamSource,
    IngestRabbitMqSource,
    IngestRedisPubSubSource,
    IngestPrometheusSource,
    IngestZeroMqSource,
    IngestSqsSource,
    IngestWebsocketsConnectionSource,
    IngestWebsocketsEndpointSource,
    IngestSyslogSource,
    IngestCodec,
    IngestUnbranchedRelay,
    IngestBranchedRelay,
    BranchField,
    ProcessorCompatibleInputRelay,
    ProcessorInputBranchRelay,
    ProcessorMaterializedRelay,
    EmitterHttpRequestReference,
    EmitterKafkaTopicReference,
    EmitterPulsarTopicReference,
    EmitterRabbitMqQueueReference,
    EmitterRabbitMqExchangeReference,
    EmitterRedisPubSubReference,
    EmitterMqttTopicReference,
    EmitterNatsSubjectReference,
    EmitterNatsStreamReference,
    EmitterZeroMqPushReference,
    EmitterSqsQueueReference,
    EmitterSentryEventReference,
    EmitterSyslogCollectorReference,
    EmitterOtelLogsReference,
    EmitterOtelTracesReference,
    EmitterOtelMetricsReference,
    EmitterClickHouseTableReference,
    EmitterPostgresTableReference,
    EmitterMySqlTableReference,
    EmitterMongoDbCollectionReference,
    EmitterIcebergTableReference,
    EmitterIcebergCatalog,
    EmitterCodec,
    EmitterCompatibleInputRelay,
    EmitterMaterializedRelay,
    EmitterErrorRelay,
    SchemaField,
    GeneratorMaterializedRelay,
});

impl ChoiceTarget {
    /// The configured reference named by one emitter entity's body.
    pub fn for_emitter_reference(kind: nervix_models::EmitterEntityKind) -> Self {
        match kind {
            nervix_models::EmitterEntityKind::Client(
                nervix_models::ClientEmitterEntity::Consumer,
            ) => Self::Schema,
            nervix_models::EmitterEntityKind::Http(nervix_models::HttpEmitterEntity::Request) => {
                Self::EmitterHttpRequestReference
            }
            nervix_models::EmitterEntityKind::Kafka(nervix_models::KafkaEmitterEntity::Topic) => {
                Self::EmitterKafkaTopicReference
            }
            nervix_models::EmitterEntityKind::Pulsar(nervix_models::PulsarEmitterEntity::Topic) => {
                Self::EmitterPulsarTopicReference
            }
            nervix_models::EmitterEntityKind::RabbitMq(
                nervix_models::RabbitMqEmitterEntity::Queue,
            ) => Self::EmitterRabbitMqQueueReference,
            nervix_models::EmitterEntityKind::RabbitMq(
                nervix_models::RabbitMqEmitterEntity::Exchange,
            ) => Self::EmitterRabbitMqExchangeReference,
            nervix_models::EmitterEntityKind::Redis(nervix_models::RedisEmitterEntity::PubSub) => {
                Self::EmitterRedisPubSubReference
            }
            nervix_models::EmitterEntityKind::Mqtt(nervix_models::MqttEmitterEntity::Topic) => {
                Self::EmitterMqttTopicReference
            }
            nervix_models::EmitterEntityKind::Nats(nervix_models::NatsEmitterEntity::Subject) => {
                Self::EmitterNatsSubjectReference
            }
            nervix_models::EmitterEntityKind::Nats(nervix_models::NatsEmitterEntity::Stream) => {
                Self::EmitterNatsStreamReference
            }
            nervix_models::EmitterEntityKind::ZeroMq(nervix_models::ZeroMqEmitterEntity::Push) => {
                Self::EmitterZeroMqPushReference
            }
            nervix_models::EmitterEntityKind::Sqs(nervix_models::SqsEmitterEntity::Queue) => {
                Self::EmitterSqsQueueReference
            }
            nervix_models::EmitterEntityKind::Sentry(nervix_models::SentryEmitterEntity::Event) => {
                Self::EmitterSentryEventReference
            }
            nervix_models::EmitterEntityKind::Syslog(
                nervix_models::SyslogEmitterEntity::Collector,
            ) => Self::EmitterSyslogCollectorReference,
            nervix_models::EmitterEntityKind::Otel(nervix_models::OtelEmitterEntity::Logs) => {
                Self::EmitterOtelLogsReference
            }
            nervix_models::EmitterEntityKind::Otel(nervix_models::OtelEmitterEntity::Traces) => {
                Self::EmitterOtelTracesReference
            }
            nervix_models::EmitterEntityKind::Otel(nervix_models::OtelEmitterEntity::Metrics) => {
                Self::EmitterOtelMetricsReference
            }
            nervix_models::EmitterEntityKind::ClickHouse(
                nervix_models::ClickHouseEmitterEntity::Table,
            ) => Self::EmitterClickHouseTableReference,
            nervix_models::EmitterEntityKind::Postgres(
                nervix_models::PostgresEmitterEntity::Table,
            ) => Self::EmitterPostgresTableReference,
            nervix_models::EmitterEntityKind::MySql(nervix_models::MySqlEmitterEntity::Table) => {
                Self::EmitterMySqlTableReference
            }
            nervix_models::EmitterEntityKind::MongoDb(
                nervix_models::MongoDbEmitterEntity::Collection,
            ) => Self::EmitterMongoDbCollectionReference,
            nervix_models::EmitterEntityKind::Iceberg(
                nervix_models::IcebergEmitterEntity::Table,
            ) => Self::EmitterIcebergTableReference,
        }
    }

    /// The emitter declaration a reference question consumes.
    pub fn emitter_reference_entity(self) -> Option<nervix_models::EmitterEntityKind> {
        match self {
            Self::EmitterHttpRequestReference => Some(nervix_models::EmitterEntityKind::Http(
                nervix_models::HttpEmitterEntity::Request,
            )),
            Self::EmitterKafkaTopicReference => Some(nervix_models::EmitterEntityKind::Kafka(
                nervix_models::KafkaEmitterEntity::Topic,
            )),
            Self::EmitterPulsarTopicReference => Some(nervix_models::EmitterEntityKind::Pulsar(
                nervix_models::PulsarEmitterEntity::Topic,
            )),
            Self::EmitterRabbitMqQueueReference => {
                Some(nervix_models::EmitterEntityKind::RabbitMq(
                    nervix_models::RabbitMqEmitterEntity::Queue,
                ))
            }
            Self::EmitterRabbitMqExchangeReference => {
                Some(nervix_models::EmitterEntityKind::RabbitMq(
                    nervix_models::RabbitMqEmitterEntity::Exchange,
                ))
            }
            Self::EmitterRedisPubSubReference => Some(nervix_models::EmitterEntityKind::Redis(
                nervix_models::RedisEmitterEntity::PubSub,
            )),
            Self::EmitterMqttTopicReference => Some(nervix_models::EmitterEntityKind::Mqtt(
                nervix_models::MqttEmitterEntity::Topic,
            )),
            Self::EmitterNatsSubjectReference => Some(nervix_models::EmitterEntityKind::Nats(
                nervix_models::NatsEmitterEntity::Subject,
            )),
            Self::EmitterNatsStreamReference => Some(nervix_models::EmitterEntityKind::Nats(
                nervix_models::NatsEmitterEntity::Stream,
            )),
            Self::EmitterZeroMqPushReference => Some(nervix_models::EmitterEntityKind::ZeroMq(
                nervix_models::ZeroMqEmitterEntity::Push,
            )),
            Self::EmitterSqsQueueReference => Some(nervix_models::EmitterEntityKind::Sqs(
                nervix_models::SqsEmitterEntity::Queue,
            )),
            Self::EmitterSentryEventReference => Some(nervix_models::EmitterEntityKind::Sentry(
                nervix_models::SentryEmitterEntity::Event,
            )),
            Self::EmitterSyslogCollectorReference => {
                Some(nervix_models::EmitterEntityKind::Syslog(
                    nervix_models::SyslogEmitterEntity::Collector,
                ))
            }
            Self::EmitterOtelLogsReference => Some(nervix_models::EmitterEntityKind::Otel(
                nervix_models::OtelEmitterEntity::Logs,
            )),
            Self::EmitterOtelTracesReference => Some(nervix_models::EmitterEntityKind::Otel(
                nervix_models::OtelEmitterEntity::Traces,
            )),
            Self::EmitterOtelMetricsReference => Some(nervix_models::EmitterEntityKind::Otel(
                nervix_models::OtelEmitterEntity::Metrics,
            )),
            Self::EmitterClickHouseTableReference => {
                Some(nervix_models::EmitterEntityKind::ClickHouse(
                    nervix_models::ClickHouseEmitterEntity::Table,
                ))
            }
            Self::EmitterPostgresTableReference => {
                Some(nervix_models::EmitterEntityKind::Postgres(
                    nervix_models::PostgresEmitterEntity::Table,
                ))
            }
            Self::EmitterMySqlTableReference => Some(nervix_models::EmitterEntityKind::MySql(
                nervix_models::MySqlEmitterEntity::Table,
            )),
            Self::EmitterMongoDbCollectionReference => {
                Some(nervix_models::EmitterEntityKind::MongoDb(
                    nervix_models::MongoDbEmitterEntity::Collection,
                ))
            }
            Self::EmitterIcebergTableReference => Some(nervix_models::EmitterEntityKind::Iceberg(
                nervix_models::IcebergEmitterEntity::Table,
            )),
            _ => None,
        }
    }

    /// The wire-owned target identity used to bind a structured-choice cursor.
    pub fn cursor_tag(self) -> u8 {
        let target: wire::ChoiceTarget = self.into();
        target.0
    }

    /// The choices of what a source names: its client or endpoint, or, for a client source, the
    /// input schema its producers' batches carry.
    pub const fn for_ingest_source(kind: IngestorEntityKind) -> Self {
        match kind {
            IngestorEntityKind::Client(ClientIngestorEntity::Producer) => Self::Schema,
            IngestorEntityKind::Http(HttpIngestorEntity::Request) => Self::IngestHttpRequestSource,
            IngestorEntityKind::Http(HttpIngestorEntity::Endpoint) => {
                Self::IngestHttpEndpointSource
            }
            IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic) => Self::IngestKafkaSource,
            IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic) => Self::IngestPulsarSource,
            IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => Self::IngestMqttSource,
            IngestorEntityKind::Nats(NatsIngestorEntity::Subject) => Self::IngestNatsSource,
            IngestorEntityKind::Nats(NatsIngestorEntity::Stream) => Self::IngestNatsStreamSource,
            IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue) => {
                Self::IngestRabbitMqSource
            }
            IngestorEntityKind::Redis(RedisIngestorEntity::PubSub) => Self::IngestRedisPubSubSource,
            IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query) => {
                Self::IngestPrometheusSource
            }
            IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull) => Self::IngestZeroMqSource,
            IngestorEntityKind::Sqs(SqsIngestorEntity::Queue) => Self::IngestSqsSource,
            IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection) => {
                Self::IngestWebsocketsConnectionSource
            }
            IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint) => {
                Self::IngestWebsocketsEndpointSource
            }
            IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener) => Self::IngestSyslogSource,
        }
    }

    pub const fn ingest_source_kind(self) -> Option<IngestorEntityKind> {
        match self {
            Self::IngestHttpRequestSource => {
                Some(IngestorEntityKind::Http(HttpIngestorEntity::Request))
            }
            Self::IngestHttpEndpointSource => {
                Some(IngestorEntityKind::Http(HttpIngestorEntity::Endpoint))
            }
            Self::IngestKafkaSource => Some(IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic)),
            Self::IngestPulsarSource => {
                Some(IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic))
            }
            Self::IngestMqttSource => Some(IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic)),
            Self::IngestNatsSource => Some(IngestorEntityKind::Nats(NatsIngestorEntity::Subject)),
            Self::IngestNatsStreamSource => {
                Some(IngestorEntityKind::Nats(NatsIngestorEntity::Stream))
            }
            Self::IngestRabbitMqSource => {
                Some(IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue))
            }
            Self::IngestRedisPubSubSource => {
                Some(IngestorEntityKind::Redis(RedisIngestorEntity::PubSub))
            }
            Self::IngestPrometheusSource => Some(IngestorEntityKind::Prometheus(
                PrometheusIngestorEntity::Query,
            )),
            Self::IngestZeroMqSource => {
                Some(IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull))
            }
            Self::IngestSqsSource => Some(IngestorEntityKind::Sqs(SqsIngestorEntity::Queue)),
            Self::IngestWebsocketsConnectionSource => Some(IngestorEntityKind::Websockets(
                WebsocketsIngestorEntity::Connection,
            )),
            Self::IngestWebsocketsEndpointSource => Some(IngestorEntityKind::Websockets(
                WebsocketsIngestorEntity::Endpoint,
            )),
            Self::IngestSyslogSource => {
                Some(IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener))
            }
            Self::DomainPace
            | Self::PlacementPolicy
            | Self::Schema
            | Self::Branch
            | Self::Relay
            | Self::RelayField
            | Self::WireJsonSchema
            | Self::WireCborSchema
            | Self::WireAvroSchema
            | Self::Resource
            | Self::CompletedResourceVersion
            | Self::Vhost
            | Self::SignalingProtocol
            | Self::Codec
            | Self::CodecField
            | Self::IngestCodec
            | Self::IngestUnbranchedRelay
            | Self::IngestBranchedRelay
            | Self::BranchField
            | Self::ProcessorCompatibleInputRelay
            | Self::ProcessorInputBranchRelay
            | Self::ProcessorMaterializedRelay
            | Self::EmitterHttpRequestReference
            | Self::EmitterKafkaTopicReference
            | Self::EmitterPulsarTopicReference
            | Self::EmitterRabbitMqQueueReference
            | Self::EmitterRabbitMqExchangeReference
            | Self::EmitterRedisPubSubReference
            | Self::EmitterMqttTopicReference
            | Self::EmitterNatsSubjectReference
            | Self::EmitterNatsStreamReference
            | Self::EmitterZeroMqPushReference
            | Self::EmitterSqsQueueReference
            | Self::EmitterSentryEventReference
            | Self::EmitterSyslogCollectorReference
            | Self::EmitterOtelLogsReference
            | Self::EmitterOtelTracesReference
            | Self::EmitterOtelMetricsReference
            | Self::EmitterClickHouseTableReference
            | Self::EmitterPostgresTableReference
            | Self::EmitterMySqlTableReference
            | Self::EmitterMongoDbCollectionReference
            | Self::EmitterIcebergTableReference
            | Self::EmitterIcebergCatalog
            | Self::EmitterCodec
            | Self::EmitterCompatibleInputRelay
            | Self::EmitterMaterializedRelay
            | Self::EmitterErrorRelay
            | Self::SchemaField
            | Self::GeneratorMaterializedRelay => None,
        }
    }
}

/// Whether a domain clock advances with wall time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DomainPaceChoice {
    Unpaced,
    Paced,
}

wire_enum!(ALL_DOMAIN_PACE_CHOICES: DomainPaceChoice => wire::DomainPaceChoice {
    Unpaced,
    Paced,
});

wire_enum!(ALL_CHOICE_PLACEMENT_POLICIES: PlacementPolicy => wire::PlacementPolicyChoice {
    RequireColocation,
    PreferColocation,
    Neutral,
    SuggestSeparation,
});

/// A semantic selection. Its variant, rather than its display label, decides what it means.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChoiceValue {
    DomainPace(DomainPaceChoice),
    PlacementPolicy(PlacementPolicy),
    Domain(DomainName),
    Resource(ResourceName),
    ResourceVersion(RequestedResourceVersion),
    Model(NodeRef),
    /// A field of the record the lookup's dependencies select.
    Field(FieldName),
}

impl ChoiceValue {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<EncodedUnion<wire::ChoiceValue>, Report<WireEncodeError>> {
        let encoded = match self {
            Self::DomainPace(value) => EncodedUnion::new(
                wire::ChoiceValue::DomainPaceVariant,
                wire::DomainPaceVariant::create(
                    encoder.fbb(),
                    &wire::DomainPaceVariantArgs {
                        value: Some((*value).into()),
                    },
                ),
            ),
            Self::PlacementPolicy(value) => EncodedUnion::new(
                wire::ChoiceValue::PlacementPolicyVariant,
                wire::PlacementPolicyVariant::create(
                    encoder.fbb(),
                    &wire::PlacementPolicyVariantArgs {
                        value: Some((*value).into()),
                    },
                ),
            ),
            Self::Domain(domain) => {
                let domain = encoder.text("DomainChoiceReference.domain", domain.as_str())?;
                EncodedUnion::new(
                    wire::ChoiceValue::DomainChoiceReference,
                    wire::DomainChoiceReference::create(
                        encoder.fbb(),
                        &wire::DomainChoiceReferenceArgs {
                            domain: Some(domain),
                        },
                    ),
                )
            }
            Self::Resource(resource) => {
                let resource =
                    encoder.text("ResourceChoiceReference.resource", resource.as_str())?;
                EncodedUnion::new(
                    wire::ChoiceValue::ResourceChoiceReference,
                    wire::ResourceChoiceReference::create(
                        encoder.fbb(),
                        &wire::ResourceChoiceReferenceArgs {
                            resource: Some(resource),
                        },
                    ),
                )
            }
            Self::ResourceVersion(RequestedResourceVersion::Number(version)) => EncodedUnion::new(
                wire::ChoiceValue::ResourceVersionNumber,
                wire::ResourceVersionNumber::create(
                    encoder.fbb(),
                    &wire::ResourceVersionNumberArgs { version: *version },
                ),
            ),
            Self::ResourceVersion(RequestedResourceVersion::Latest) => EncodedUnion::new(
                wire::ChoiceValue::LatestResourceVersion,
                wire::LatestResourceVersion::create(
                    encoder.fbb(),
                    &wire::LatestResourceVersionArgs {},
                ),
            ),
            Self::Model(node) => {
                let node = encode_node_ref(encoder, node)?;
                EncodedUnion::new(
                    wire::ChoiceValue::ModelChoiceReference,
                    wire::ModelChoiceReference::create(
                        encoder.fbb(),
                        &wire::ModelChoiceReferenceArgs { node: Some(node) },
                    ),
                )
            }
            Self::Field(field) => {
                let field = encoder.text("FieldChoiceReference.field", field.as_str())?;
                EncodedUnion::new(
                    wire::ChoiceValue::FieldChoiceReference,
                    wire::FieldChoiceReference::create(
                        encoder.fbb(),
                        &wire::FieldChoiceReferenceArgs { field: Some(field) },
                    ),
                )
            }
        };
        Ok(encoded)
    }

    fn decode_selection(
        decoder: Decoder<'_>,
        selection: wire::ChoiceSelection<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        if let Some(value) = selection.value_as_domain_pace_variant() {
            let value = decoder.required_enumeration("DomainPaceVariant.value", value.value())?;
            return Ok(Self::DomainPace(value));
        }
        if let Some(value) = selection.value_as_placement_policy_variant() {
            let value =
                decoder.required_enumeration("PlacementPolicyVariant.value", value.value())?;
            return Ok(Self::PlacementPolicy(value));
        }
        if let Some(value) = selection.value_as_domain_choice_reference() {
            let domain = decoder.name("DomainChoiceReference.domain", value.domain())?;
            return Ok(Self::Domain(domain));
        }
        if let Some(value) = selection.value_as_resource_choice_reference() {
            let resource = decoder.name("ResourceChoiceReference.resource", value.resource())?;
            return Ok(Self::Resource(resource));
        }
        if let Some(value) = selection.value_as_resource_version_number() {
            return Ok(Self::ResourceVersion(RequestedResourceVersion::Number(
                value.version(),
            )));
        }
        if selection.value_as_latest_resource_version().is_some() {
            return Ok(Self::ResourceVersion(RequestedResourceVersion::Latest));
        }
        if let Some(value) = selection.value_as_model_choice_reference() {
            return Ok(Self::Model(decode_node_ref(decoder, value.node())?));
        }
        if let Some(value) = selection.value_as_field_choice_reference() {
            let field = decoder.name("FieldChoiceReference.field", value.field())?;
            return Ok(Self::Field(field));
        }
        Err(decoder.unknown_union("ChoiceSelection.value", selection.value_type().0))
    }

    fn decode_choice(
        decoder: Decoder<'_>,
        choice: wire::Choice<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        if let Some(value) = choice.value_as_domain_pace_variant() {
            let value = decoder.required_enumeration("DomainPaceVariant.value", value.value())?;
            return Ok(Self::DomainPace(value));
        }
        if let Some(value) = choice.value_as_placement_policy_variant() {
            let value =
                decoder.required_enumeration("PlacementPolicyVariant.value", value.value())?;
            return Ok(Self::PlacementPolicy(value));
        }
        if let Some(value) = choice.value_as_domain_choice_reference() {
            let domain = decoder.name("DomainChoiceReference.domain", value.domain())?;
            return Ok(Self::Domain(domain));
        }
        if let Some(value) = choice.value_as_resource_choice_reference() {
            let resource = decoder.name("ResourceChoiceReference.resource", value.resource())?;
            return Ok(Self::Resource(resource));
        }
        if let Some(value) = choice.value_as_resource_version_number() {
            return Ok(Self::ResourceVersion(RequestedResourceVersion::Number(
                value.version(),
            )));
        }
        if choice.value_as_latest_resource_version().is_some() {
            return Ok(Self::ResourceVersion(RequestedResourceVersion::Latest));
        }
        if let Some(value) = choice.value_as_model_choice_reference() {
            return Ok(Self::Model(decode_node_ref(decoder, value.node())?));
        }
        if let Some(value) = choice.value_as_field_choice_reference() {
            let field = decoder.name("FieldChoiceReference.field", value.field())?;
            return Ok(Self::Field(field));
        }
        Err(decoder.unknown_union("Choice.value", choice.value_type().0))
    }
}

/// A typed selection on which another control's choices depend.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChoiceSelection {
    pub value: ChoiceValue,
}

impl ChoiceSelection {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::ChoiceSelection<'fbb>>, Report<WireEncodeError>> {
        let value = self.value.encode(encoder)?;
        Ok(wire::ChoiceSelection::create(
            encoder.fbb(),
            &wire::ChoiceSelectionArgs {
                value_type: value.discriminant,
                value: Some(value.value),
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        selection: wire::ChoiceSelection<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            value: ChoiceValue::decode_selection(decoder, selection)?,
        })
    }
}

/// A typed choice query with a bounded result page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChoiceLookupRequest {
    target: ChoiceTarget,
    dependencies: Vec<ChoiceSelection>,
    search: String,
    page_size: u16,
    page_cursor: Option<String>,
}

impl ChoiceLookupRequest {
    pub fn new(target: ChoiceTarget, dependencies: Vec<ChoiceSelection>, search: String) -> Self {
        Self {
            target,
            dependencies,
            search,
            page_size: 64,
            page_cursor: None,
        }
    }

    pub fn with_page(
        mut self,
        page_size: u16,
        page_cursor: Option<String>,
    ) -> Result<Self, Report<WireValueError>> {
        if !(1..=100).contains(&page_size) {
            return Err(Report::new(WireValueError::InvalidChoicePageSize {
                size: page_size,
            }));
        }
        self.page_size = page_size;
        self.page_cursor = page_cursor;
        Ok(self)
    }

    pub const fn target(&self) -> ChoiceTarget {
        self.target
    }

    pub fn dependencies(&self) -> &[ChoiceSelection] {
        &self.dependencies
    }

    pub fn search(&self) -> &str {
        &self.search
    }

    pub fn page_size(&self) -> usize {
        usize::from(self.page_size)
    }

    pub fn page_cursor(&self) -> Option<&str> {
        self.page_cursor.as_deref()
    }

    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::ChoiceLookupRequest<'fbb>>, Report<WireEncodeError>> {
        let dependencies = encoder.table_vector(
            "ChoiceLookupRequest.dependencies",
            &self.dependencies,
            ChoiceSelection::encode,
        )?;
        let search = encoder.text("ChoiceLookupRequest.search", &self.search)?;
        let page_cursor = encoder.optional_text(
            "ChoiceLookupRequest.page_cursor",
            self.page_cursor.as_deref(),
        )?;
        Ok(wire::ChoiceLookupRequest::create(
            encoder.fbb(),
            &wire::ChoiceLookupRequestArgs {
                target: Some(self.target.into()),
                dependencies: Some(dependencies),
                search: Some(search),
                page_size: self.page_size,
                page_cursor,
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        request: wire::ChoiceLookupRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let target =
            decoder.required_enumeration("ChoiceLookupRequest.target", request.target())?;
        let dependencies = decoder.table_vector(
            "ChoiceLookupRequest.dependencies",
            request.dependencies(),
            |selection| ChoiceSelection::decode(decoder, selection),
        )?;
        let search = decoder.text("ChoiceLookupRequest.search", request.search())?;
        if !(1..=100).contains(&request.page_size()) {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "ChoiceLookupRequest.page_size",
                kind: "choice page size",
            }));
        }
        let page_cursor =
            decoder.optional_text("ChoiceLookupRequest.page_cursor", request.page_cursor())?;
        Ok(Self {
            target,
            dependencies,
            search,
            page_size: request.page_size(),
            page_cursor,
        })
    }
}

/// Text that presents one choice without carrying its semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChoicePresentation {
    pub label: String,
    pub detail: Option<String>,
    pub group: Option<String>,
}

impl ChoicePresentation {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::ChoicePresentation<'fbb>>, Report<WireEncodeError>> {
        let label = encoder.text("ChoicePresentation.label", &self.label)?;
        let detail = encoder.optional_text("ChoicePresentation.detail", self.detail.as_deref())?;
        let group = encoder.optional_text("ChoicePresentation.group", self.group.as_deref())?;
        Ok(wire::ChoicePresentation::create(
            encoder.fbb(),
            &wire::ChoicePresentationArgs {
                label: Some(label),
                detail,
                group,
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        presentation: wire::ChoicePresentation<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            label: decoder.text("ChoicePresentation.label", presentation.label())?,
            detail: decoder.optional_text("ChoicePresentation.detail", presentation.detail())?,
            group: decoder.optional_text("ChoicePresentation.group", presentation.group())?,
        })
    }
}

/// One typed value and the text used to present it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Choice {
    pub value: ChoiceValue,
    pub presentation: ChoicePresentation,
}

impl Choice {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::Choice<'fbb>>, Report<WireEncodeError>> {
        let value = self.value.encode(encoder)?;
        let presentation = self.presentation.encode(encoder)?;
        Ok(wire::Choice::create(
            encoder.fbb(),
            &wire::ChoiceArgs {
                value_type: value.discriminant,
                value: Some(value.value),
                presentation: Some(presentation),
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        choice: wire::Choice<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            value: ChoiceValue::decode_choice(decoder, choice)?,
            presentation: ChoicePresentation::decode(decoder, choice.presentation())?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "configured-choice replies project lookup and context state into this fixed \
                  session-protocol disposition"
    )
)]
pub enum ChoiceStatus {
    Ready,
    MissingContext,
    StaleContext,
    LookupFailed,
}

wire_enum!(ALL_CHOICE_STATUSES: ChoiceStatus => wire::ChoiceStatus {
    Ready,
    MissingContext,
    StaleContext,
    LookupFailed,
});

/// One bounded page of choices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChoiceOutcome {
    pub status: ChoiceStatus,
    pub choices: Vec<Choice>,
    pub page_cursor: Option<String>,
}

impl ChoiceOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let choices =
            encoder.table_vector("ChoiceOutcome.choices", &self.choices, Choice::encode)?;
        let page_cursor =
            encoder.optional_text("ChoiceOutcome.page_cursor", self.page_cursor.as_deref())?;
        let outcome = wire::ChoiceOutcome::create(
            encoder.fbb(),
            &wire::ChoiceOutcomeArgs {
                status: Some(self.status.into()),
                choices: Some(choices),
                page_cursor,
            },
        );
        Ok(EncodedUnion::new(wire::ReplyBody::ChoiceOutcome, outcome))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::ChoiceOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            status: decoder.required_enumeration("ChoiceOutcome.status", outcome.status())?,
            choices: decoder.table_vector(
                "ChoiceOutcome.choices",
                outcome.choices(),
                |choice| Choice::decode(decoder, choice),
            )?,
            page_cursor: decoder
                .optional_text("ChoiceOutcome.page_cursor", outcome.page_cursor())?,
        })
    }
}
