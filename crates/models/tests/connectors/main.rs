//! Every connector and entity pair declares what the vocabulary says it does.
//!
//! Layer: test harness.
//!
//! - **Owns.** The checks that the source and emitter unions name every pair once, that each pair's
//!   keywords, capabilities, quiesce modes, publishing modes and references are the declared
//!   ones, and that every generated Model states what its pair declares.
//! - **Depends on.** The vocabulary and the generators of `nervix-arbitrary`.
//! - **Must not know.** The language, the registry, or how a connector is hosted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain, ModelVariant};
use nervix_models::{
    AckWindow, AlterEmitter, AlterEmitterError, AlterEmitterOperation, AlterIngestorError,
    BatchMessageLimit, BatchUnavailability, ClickHouseEmitter, ClickHouseEmitterEntity,
    ClickHouseTableEmitter, ClickHouseValueMapping, ClientConsumerEmitter, ClientEmitterEntity,
    ClientIngestMode, ClientIngestorEntity, ClientName, ConnectorKind, EmitterBatchLegality,
    EmitterBatchPolicy, EmitterBodyForm, EmitterClientReference, EmitterEntity, EmitterEntityKind,
    EmitterReference, EmitterValueMapping, EndpointIngestMode, EndpointType, Expression,
    HeaderLayout, HeaderModel, HttpEmitterEntity, HttpEndpointIngestor, HttpIngestor,
    HttpIngestorEntity, IcebergCatalog, IcebergEmitter, IcebergEmitterEntity, IcebergTableEmitter,
    IngestAcknowledgement, IngestQuiesceMode, IngestQuiesceOverflow, IngestorEntity,
    IngestorEntityKind, IngestorMetadataType, IngestorReferenceKind, KafkaEmitterEntity,
    KafkaIngestMode, KafkaIngestorEntity, KafkaOffsetMode, KafkaTopicEmitter, KafkaTopicIngestor,
    Literal, Model, ModelKind, MongoDbCollectionEmitter, MongoDbConflictAction, MongoDbEmitter,
    MongoDbEmitterEntity, MqttEmitterEntity, MqttIngestMode, MqttIngestorEntity, MqttQos,
    MqttSession, MqttTopicIngestor, MySqlConflictAction, MySqlEmitter, MySqlEmitterEntity,
    MySqlTableEmitter, NatsEmitterEntity, NatsIngestorEntity, OtelEmitter, OtelEmitterEntity,
    OtelLogsEmitter, OtelMappings, OtelMetric, OtelMetricKind, OtelMetricsEmitter,
    OtelTracesEmitter, PostgresConflictAction, PostgresEmitter, PostgresEmitterEntity,
    PostgresTableEmitter, PrometheusIngestorEntity, PulsarEmitterEntity, PulsarIngestorEntity,
    PulsarTopicEmitter, RabbitMqEmitterEntity, RabbitMqIngestorEntity, RedisEmitter,
    RedisEmitterEntity, RedisIngestor, RedisIngestorEntity, RedisPubSubEmitter,
    RedisPubSubIngestMode, RedisPubSubIngestor, RetryPolicy, SchemaName, SentryEmitter,
    SentryEmitterEntity, SentryEventEmitter, SqsEmitterEntity, SqsIngestorEntity,
    SyslogEmitterEntity, SyslogIngestorEntity, WebsocketsEndpointIngestor,
    WebsocketsIngestorEntity, ZeroMqEmitterEntity, ZeroMqIngestorEntity,
};
use rstest::rstest;
use strum::IntoEnumIterator as _;

mod surface;

fn named<N>(raw: &str) -> N
where
    N: TryFrom<String>,
    N::Error: std::fmt::Debug,
{
    N::try_from(raw.to_owned()).expect("the fixture name satisfies the identifier grammar")
}

/// Bytes that differ from seed to seed, so a check sees more than one shape of a value.
fn seeded_bytes(seed: u8) -> Vec<u8> {
    (0..512_u16)
        .map(|index| {
            let low = index.to_le_bytes()[0];
            low.wrapping_mul(31).wrapping_add(seed.wrapping_mul(17))
        })
        .collect()
}

fn mapping(column: &str) -> EmitterValueMapping {
    EmitterValueMapping {
        column: column.to_owned(),
        expression: Expression::Literal(Literal::String(column.to_owned())),
    }
}

fn retry_policy() -> RetryPolicy {
    RetryPolicy {
        backoff: "1s".to_owned(),
        max_backoff: "30s".to_owned(),
    }
}

/// Every policy kind observed by the shared capability catalog.
fn publishing_modes() -> Vec<(String, nervix_models::EmitterPublishingModeKind)> {
    nervix_models::EmitterPublishingModeKind::iter()
        .map(|kind| (format!("{kind:?}"), kind))
        .collect()
}

/// Fails when two pairs share a phrase, which would make one of them unaddressable.
fn assert_names_each_once(phrases: &[&'static str]) {
    let mut distinct = phrases.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), phrases.len(), "{phrases:?}");
}

#[test]
fn every_connector_kind_is_spelled_as_plexus_spells_it() {
    let keywords = ConnectorKind::iter()
        .map(ConnectorKind::keyword)
        .collect::<Vec<_>>();
    assert_eq!(
        keywords,
        [
            "CLIENT",
            "HTTP",
            "WEBSOCKETS",
            "KAFKA",
            "PULSAR",
            "RABBITMQ",
            "REDIS",
            "MQTT",
            "NATS",
            "ZEROMQ",
            "SQS",
            "SYSLOG",
            "PROMETHEUS",
            "SENTRY",
            "OTEL",
            "CLICKHOUSE",
            "POSTGRES",
            "MYSQL",
            "MONGODB",
            "ICEBERG",
            "S3",
            "GCS",
            "AZURE_BLOB",
            "ICEBERG_REST",
        ]
    );
    for kind in ConnectorKind::iter() {
        assert_eq!(kind.as_ref(), kind.keyword());
    }
}

#[test]
fn the_source_kinds_name_every_pair_once_with_its_phrase() {
    let phrases = IngestorEntityKind::all()
        .map(IngestorEntityKind::phrase)
        .collect::<Vec<_>>();
    assert_eq!(
        phrases,
        [
            "CLIENT PRODUCER",
            "HTTP REQUEST",
            "HTTP ENDPOINT",
            "KAFKA TOPIC",
            "PULSAR TOPIC",
            "MQTT TOPIC",
            "NATS SUBJECT",
            "NATS STREAM",
            "RABBITMQ QUEUE",
            "REDIS PUBSUB",
            "PROMETHEUS QUERY",
            "ZEROMQ PULL",
            "SQS QUEUE",
            "WEBSOCKETS CONNECTION",
            "WEBSOCKETS ENDPOINT",
            "SYSLOG LISTENER",
        ]
    );
    assert_names_each_once(&phrases);
}

#[test]
fn the_emitter_kinds_name_every_pair_once_with_its_phrase() {
    let phrases = EmitterEntityKind::all()
        .map(EmitterEntityKind::phrase)
        .collect::<Vec<_>>();
    assert_eq!(
        phrases,
        [
            "CLIENT CONSUMER",
            "HTTP REQUEST",
            "KAFKA TOPIC",
            "PULSAR TOPIC",
            "RABBITMQ QUEUE",
            "RABBITMQ EXCHANGE",
            "REDIS PUBSUB",
            "MQTT TOPIC",
            "NATS SUBJECT",
            "ZEROMQ PUSH",
            "SQS QUEUE",
            "SENTRY EVENT",
            "SYSLOG COLLECTOR",
            "OTEL LOGS",
            "OTEL TRACES",
            "OTEL METRICS",
            "CLICKHOUSE TABLE",
            "POSTGRES TABLE",
            "MYSQL TABLE",
            "MONGODB COLLECTION",
            "ICEBERG TABLE",
        ]
    );
    assert_names_each_once(&phrases);
}

#[test]
fn a_phrase_is_the_keyword_of_its_connector_then_of_its_entity() {
    for kind in IngestorEntityKind::all() {
        assert_eq!(
            kind.phrase(),
            format!("{} {}", kind.connector().keyword(), kind.entity_keyword())
        );
    }
    for kind in EmitterEntityKind::all() {
        assert_eq!(
            kind.phrase(),
            format!("{} {}", kind.connector().keyword(), kind.entity_keyword())
        );
    }
}

#[test]
fn the_transport_sources_are_every_source_pair_but_the_client_producer() {
    let transports = IngestorEntityKind::modeled().collect::<Vec<_>>();
    let mut expected = IngestorEntityKind::all().collect::<Vec<_>>();
    expected.retain(|kind| *kind != IngestorEntityKind::Client(ClientIngestorEntity::Producer));
    assert_eq!(transports, expected);
}

#[test]
fn a_source_form_names_its_pair_by_key_and_label() {
    let forms = IngestorEntityKind::all()
        .map(|kind| (kind.form_key(), kind.form_label()))
        .collect::<Vec<_>>();
    let expected = [
        ("client-producer", "Client batches"),
        ("http-request", "HTTP polling"),
        ("http-endpoint", "HTTP endpoint"),
        ("kafka-topic", "Kafka"),
        ("pulsar-topic", "Pulsar"),
        ("mqtt-topic", "MQTT"),
        ("nats-subject", "NATS"),
        ("nats-stream", "NATS JetStream"),
        ("rabbitmq-queue", "RabbitMQ"),
        ("redis-pubsub", "Redis Pub/Sub"),
        ("prometheus-query", "Prometheus"),
        ("zeromq-pull", "ZeroMQ"),
        ("sqs-queue", "SQS"),
        ("websockets-connection", "WebSocket client"),
        ("websockets-endpoint", "WebSocket endpoint"),
        ("syslog-listener", "Syslog"),
    ];
    let expected = expected
        .into_iter()
        .map(|(key, label)| (key.to_owned(), label))
        .collect::<Vec<_>>();
    assert_eq!(forms, expected);
}

/// What one source pair declares, as the table below states it.
struct IngestorDeclaration {
    headers: Option<HeaderModel>,
    /// The metadata fields it declares, by name and type, in declared order.
    metadata: &'static [(&'static str, IngestorMetadataType)],
    every_node: bool,
    reference: IngestorReferenceKind,
}

fn source_declaration(kind: IngestorEntityKind) -> IngestorDeclaration {
    let headers_only = |headers, connector| IngestorDeclaration {
        headers,
        metadata: &[],
        every_node: false,
        reference: IngestorReferenceKind::Client(connector),
    };
    match kind {
        IngestorEntityKind::Client(ClientIngestorEntity::Producer) => IngestorDeclaration {
            headers: None,
            metadata: &[],
            every_node: false,
            reference: IngestorReferenceKind::Schema,
        },
        IngestorEntityKind::Http(HttpIngestorEntity::Request) => IngestorDeclaration {
            headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
            metadata: &[
                ("status", IngestorMetadataType::U16),
                ("polled_at", IngestorMetadataType::Datetime),
            ],
            every_node: false,
            reference: IngestorReferenceKind::Client(ConnectorKind::Http),
        },
        IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic) => IngestorDeclaration {
            headers: Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
            metadata: &[
                ("topic", IngestorMetadataType::String),
                ("partition", IngestorMetadataType::I32),
                ("offset", IngestorMetadataType::I64),
                ("key", IngestorMetadataType::Bytes),
                ("timestamp", IngestorMetadataType::Datetime),
                ("timestamp_type", IngestorMetadataType::String),
                ("tombstone", IngestorMetadataType::Bool),
            ],
            every_node: false,
            reference: IngestorReferenceKind::Client(ConnectorKind::Kafka),
        },
        IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic) => headers_only(
            Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
            ConnectorKind::Pulsar,
        ),
        IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => {
            headers_only(None, ConnectorKind::Mqtt)
        }
        IngestorEntityKind::Nats(NatsIngestorEntity::Subject) => headers_only(
            Some(HeaderModel::exact(HeaderLayout::RepeatsByName)),
            ConnectorKind::Nats,
        ),
        IngestorEntityKind::Nats(NatsIngestorEntity::Stream) => headers_only(
            Some(HeaderModel::exact(HeaderLayout::RepeatsByName)),
            ConnectorKind::Nats,
        ),
        IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue) => headers_only(
            Some(HeaderModel::exact(HeaderLayout::SortedByName)),
            ConnectorKind::RabbitMq,
        ),
        IngestorEntityKind::Redis(RedisIngestorEntity::PubSub) => {
            headers_only(None, ConnectorKind::Redis)
        }
        IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query) => {
            headers_only(None, ConnectorKind::Prometheus)
        }
        IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull) => {
            headers_only(None, ConnectorKind::ZeroMq)
        }
        IngestorEntityKind::Sqs(SqsIngestorEntity::Queue) => headers_only(
            Some(HeaderModel::exact(HeaderLayout::LastWins)),
            ConnectorKind::Sqs,
        ),
        IngestorEntityKind::Http(HttpIngestorEntity::Endpoint) => IngestorDeclaration {
            headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
            metadata: &[
                ("method", IngestorMetadataType::String),
                ("path", IngestorMetadataType::String),
                ("query", IngestorMetadataType::String),
                ("remote_addr", IngestorMetadataType::String),
                ("host", IngestorMetadataType::String),
            ],
            every_node: true,
            reference: IngestorReferenceKind::Endpoint(EndpointType::Http),
        },
        IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection) => {
            IngestorDeclaration {
                headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
                metadata: &[("frame_type", IngestorMetadataType::String)],
                every_node: false,
                reference: IngestorReferenceKind::Client(ConnectorKind::Websockets),
            }
        }
        IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint) => IngestorDeclaration {
            headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
            metadata: &[
                ("frame_type", IngestorMetadataType::String),
                ("remote_addr", IngestorMetadataType::String),
                ("path", IngestorMetadataType::String),
            ],
            every_node: true,
            reference: IngestorReferenceKind::Endpoint(EndpointType::Websockets),
        },
        IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener) => IngestorDeclaration {
            headers: None,
            metadata: &[("peer_addr", IngestorMetadataType::String)],
            every_node: true,
            reference: IngestorReferenceKind::Client(ConnectorKind::Syslog),
        },
    }
}

#[test]
fn every_source_pair_declares_its_headers_metadata_execution_and_reference() {
    for kind in IngestorEntityKind::all() {
        let expected = source_declaration(kind);
        let declared = kind.capabilities();
        assert_eq!(declared.headers, expected.headers, "{kind:?}");
        assert_eq!(kind.reads_headers(), expected.headers.is_some(), "{kind:?}");
        let metadata = declared
            .metadata
            .fields()
            .iter()
            .map(|field| (field.name(), field.ty()))
            .collect::<Vec<_>>();
        assert_eq!(metadata, expected.metadata, "{kind:?}");
        assert_eq!(
            declared.executes_on_every_cluster_node, expected.every_node,
            "{kind:?}"
        );
        assert_eq!(declared.reference, expected.reference, "{kind:?}");
        assert_eq!(
            kind.client_connector(),
            expected.reference.client_connector(),
            "{kind:?}"
        );
    }
}

#[test]
fn a_reference_kind_names_the_kind_of_model_a_body_references() {
    let client = IngestorReferenceKind::Client(ConnectorKind::Kafka);
    assert_eq!(client.model_kind(), ModelKind::Client);
    assert_eq!(client.client_connector(), Some(ConnectorKind::Kafka));
    assert_eq!(client.endpoint_type(), None);
    let endpoint = IngestorReferenceKind::Endpoint(EndpointType::Websockets);
    assert_eq!(endpoint.model_kind(), ModelKind::Endpoint);
    assert_eq!(endpoint.client_connector(), None);
    assert_eq!(endpoint.endpoint_type(), Some(EndpointType::Websockets));
    assert_eq!(
        IngestorReferenceKind::Schema.model_kind(),
        ModelKind::Schema
    );
    assert_eq!(IngestorReferenceKind::Schema.client_connector(), None);
    assert_eq!(IngestorReferenceKind::Schema.endpoint_type(), None);
}

#[test]
fn a_transport_source_states_what_its_pair_declares() {
    for kind in IngestorEntityKind::modeled() {
        for seed in 0..4 {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let source = arbitrary
                .ingest_source_of(kind)
                .assured("every modeled pair has a transport Model");
            let declared = kind.capabilities();
            assert_eq!(source.kind(), kind);
            assert_eq!(source.connector(), kind.connector());
            assert_eq!(
                kind.phrase(),
                format!("{} {}", kind.connector().keyword(), kind.entity_keyword())
            );
            assert_eq!(kind.to_string(), kind.phrase());
            assert_eq!(source.reads_headers(), declared.reads_headers());
            assert_eq!(
                source.executes_on_every_cluster_node(),
                declared.executes_on_every_cluster_node
            );
            assert_eq!(source.entity_kind(), declared.reference.model_kind());
            assert!(source.supports_quiesce(source.quiesce()), "{source:?}");
        }
    }
}

/// The quiesce modes one source pair supports, of the five the vocabulary defines, in the order
/// suspend, buffer, drop, reject, endpoint buffer.
#[rstest]
#[case::http(IngestorEntityKind::Http(HttpIngestorEntity::Request), [true, true, false, false, false])]
#[case::kafka(IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic), [true, false, false, false, false])]
#[case::pulsar(IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic), [true, false, false, false, false])]
#[case::nats(IngestorEntityKind::Nats(NatsIngestorEntity::Subject), [false, true, true, false, false])]
#[case::nats_stream(IngestorEntityKind::Nats(NatsIngestorEntity::Stream), [true, true, true, false, false])]
#[case::rabbitmq(IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue), [true, false, false, false, false])]
#[case::redis(IngestorEntityKind::Redis(RedisIngestorEntity::PubSub), [false, true, true, false, false])]
#[case::prometheus(IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query), [true, true, false, false, false])]
#[case::zeromq(IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull), [true, true, true, false, false])]
#[case::sqs(IngestorEntityKind::Sqs(SqsIngestorEntity::Queue), [true, false, false, false, false])]
#[case::http_endpoint(IngestorEntityKind::Http(HttpIngestorEntity::Endpoint), [false, false, false, true, true])]
#[case::websockets(IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection), [false, true, true, false, false])]
#[case::websockets_endpoint(IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint), [false, false, false, true, true])]
#[case::syslog(IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener), [true, true, true, false, false])]
fn a_source_supports_the_quiesce_modes_its_entity_declares(
    #[case] kind: IngestorEntityKind,
    #[case] expected: [bool; 5],
) {
    let modes = [
        IngestQuiesceMode::Suspend,
        IngestQuiesceMode::Buffer {
            max_size: "1MiB".to_owned(),
            overflow: IngestQuiesceOverflow::DropOldest,
        },
        IngestQuiesceMode::Drop,
        IngestQuiesceMode::Reject {
            retry_after: "1s".to_owned(),
        },
        IngestQuiesceMode::EndpointBuffer {
            max_size: "1MiB".to_owned(),
        },
    ];
    let bytes = seeded_bytes(1);
    let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
    let mut source = arbitrary
        .ingest_source_of(kind)
        .assured("every modeled pair has a transport Model");
    for (mode, supported) in modes.into_iter().zip(expected) {
        assert_eq!(source.supports_quiesce(&mode), supported, "{mode:?}");
        let outcome = source.set_quiesce(mode.clone());
        assert_eq!(outcome.is_ok(), supported, "{mode:?}");
        if supported {
            assert_eq!(source.quiesce(), &mode);
        }
    }
}

#[test]
fn an_mqtt_source_suspends_only_a_persistent_session_delivered_at_least_once() {
    let source = |session, qos, mode| {
        IngestorEntity::from(MqttTopicIngestor {
            client: named("broker"),
            topic: named("orders/#"),
            instances: std::num::NonZeroU64::MIN,
            session,
            qos,
            mode,
            quiesce: IngestQuiesceMode::Drop,
        })
    };
    let persistent = source(
        MqttSession::Persistent,
        MqttQos::Qos1,
        MqttIngestMode::NoAck,
    );
    let exactly_once = source(
        MqttSession::Persistent,
        MqttQos::Qos2,
        MqttIngestMode::NoAck,
    );
    let clean = source(MqttSession::Clean, MqttQos::Qos1, MqttIngestMode::NoAck);
    let at_most_once = source(
        MqttSession::Persistent,
        MqttQos::Qos0,
        MqttIngestMode::NoAck,
    );
    let acknowledged = source(
        MqttSession::Persistent,
        MqttQos::Qos1,
        MqttIngestMode::AckSequential {
            timeout: "5s".to_owned(),
            retry_policy: retry_policy(),
        },
    );
    assert!(persistent.supports_quiesce(&IngestQuiesceMode::Suspend));
    assert!(exactly_once.supports_quiesce(&IngestQuiesceMode::Suspend));
    assert!(!clean.supports_quiesce(&IngestQuiesceMode::Suspend));
    assert!(!at_most_once.supports_quiesce(&IngestQuiesceMode::Suspend));
    assert!(acknowledged.supports_quiesce(&IngestQuiesceMode::Suspend));
    assert_eq!(acknowledged.unmet_delivery(), None);
    assert_eq!(
        source(
            MqttSession::Clean,
            MqttQos::Qos1,
            MqttIngestMode::AckSequential {
                timeout: "5s".to_owned(),
                retry_policy: retry_policy(),
            },
        )
        .unmet_delivery()
        .map(|unmet| unmet.to_string()),
        Some(
            "MQTT TOPIC ingestors name MODE ACK SEQUENTIAL only with SESSION PERSISTENT and QOS 1 \
             or 2; the statement writes SESSION CLEAN and QOS 1"
                .to_owned()
        )
    );
    assert!(clean.supports_quiesce(&IngestQuiesceMode::Drop));
    assert!(!clean.supports_quiesce(&IngestQuiesceMode::Reject {
        retry_after: "1s".to_owned(),
    }));
}

#[test]
fn a_source_references_the_client_or_endpoint_its_body_names() {
    let kafka = IngestorEntity::from(KafkaTopicIngestor {
        client: named("events"),
        topic: named("orders"),
        offset_mode: KafkaOffsetMode::Domain,
        instances: std::num::NonZeroU64::MIN,
        mode: KafkaIngestMode::NoAck,
        quiesce: IngestQuiesceMode::Suspend,
    });
    assert_eq!(kafka.source_ref().as_str(), "events");
    assert_eq!(kafka.entity_kind(), ModelKind::Client);
    assert_eq!(
        kafka.acknowledgement(),
        IngestAcknowledgement::Unacknowledged
    );

    let endpoint = IngestorEntity::from(HttpEndpointIngestor {
        endpoint: named("intake"),
        mode: EndpointIngestMode::NoAck,
        quiesce: IngestQuiesceMode::Reject {
            retry_after: "1s".to_owned(),
        },
    });
    assert_eq!(endpoint.source_ref().as_str(), "intake");
    assert_eq!(endpoint.entity_kind(), ModelKind::Endpoint);

    let stream = IngestorEntity::from(WebsocketsEndpointIngestor {
        endpoint: named("stream"),
        mode: EndpointIngestMode::NoAck,
        quiesce: IngestQuiesceMode::EndpointBuffer {
            max_size: "1MiB".to_owned(),
        },
    });
    assert_eq!(stream.source_ref().as_str(), "stream");
    assert_eq!(stream.entity_kind(), ModelKind::Endpoint);
    assert_eq!(
        stream.kind(),
        IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint)
    );
}

#[test]
fn declared_metadata_fields_read_as_optional_schema_fields_in_declared_order() {
    for kind in IngestorEntityKind::all() {
        let expected = source_declaration(kind)
            .metadata
            .iter()
            .map(|(name, ty)| ((*name).to_owned(), ty.parse_as_type()))
            .collect::<Vec<_>>();
        let schema_fields = kind
            .capabilities()
            .metadata
            .schema_fields()
            .into_iter()
            .map(|field| {
                assert!(field.optional, "{kind:?} {field:?}");
                assert!(!field.sensitive, "{kind:?} {field:?}");
                (field.name.as_str().to_owned(), field.ty)
            })
            .collect::<Vec<_>>();
        assert_eq!(schema_fields, expected, "{kind:?}");
    }
}

/// What one emitter pair declares, as the table below states it.
struct EmitterDeclaration {
    headers: Option<HeaderModel>,
    body: EmitterBodyForm,
    batch: EmitterBatchLegality,
    requires_batch_transformation: bool,
    modes: &'static [&'static str],
}

fn emitter_declaration(kind: EmitterEntityKind) -> EmitterDeclaration {
    let codec = |headers, modes| EmitterDeclaration {
        headers,
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        modes,
    };
    let request = |batch| EmitterDeclaration {
        headers: None,
        body: EmitterBodyForm::Values,
        batch,
        requires_batch_transformation: false,
        modes: &["RequestAck"],
    };
    let required = EmitterBatchLegality::Required { maximum: None };
    let optional = EmitterBatchLegality::Optional { maximum: None };
    match kind {
        EmitterEntityKind::Client(ClientEmitterEntity::Consumer) => EmitterDeclaration {
            headers: None,
            body: EmitterBodyForm::Native,
            batch: required,
            requires_batch_transformation: false,
            modes: &["ClientAck"],
        },
        EmitterEntityKind::Http(HttpEmitterEntity::Request) => EmitterDeclaration {
            headers: Some(HeaderModel::lowercase(HeaderLayout::SortedByName)),
            body: EmitterBodyForm::CodecOrWithoutBody,
            ..request(EmitterBatchLegality::Unavailable {
                reason: BatchUnavailability::OneRequestPerRecord,
            })
        },
        EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic) => codec(
            Some(HeaderModel::exact(HeaderLayout::OrderedRepeats)),
            &["NoAck", "BrokerAck"],
        ),
        EmitterEntityKind::Pulsar(PulsarEmitterEntity::Topic) => codec(
            Some(HeaderModel::exact(HeaderLayout::LastWins)),
            &["NoAck", "BrokerAck"],
        ),
        EmitterEntityKind::RabbitMq(
            RabbitMqEmitterEntity::Queue | RabbitMqEmitterEntity::Exchange,
        ) => codec(
            Some(HeaderModel::exact(HeaderLayout::SortedByName)),
            &["NoAck", "BrokerAck"],
        ),
        EmitterEntityKind::Redis(RedisEmitterEntity::PubSub)
        | EmitterEntityKind::ZeroMq(ZeroMqEmitterEntity::Push)
        | EmitterEntityKind::Syslog(SyslogEmitterEntity::Collector) => codec(None, &["NoAck"]),
        EmitterEntityKind::Mqtt(MqttEmitterEntity::Topic) => {
            codec(None, &["MqttQos0", "MqttQos1", "MqttQos2"])
        }
        EmitterEntityKind::Nats(NatsEmitterEntity::Subject) => codec(
            Some(HeaderModel::exact(HeaderLayout::RepeatsByName)),
            &["NoAck", "NatsJetStream"],
        ),
        EmitterEntityKind::Sqs(SqsEmitterEntity::Queue) => EmitterDeclaration {
            batch: EmitterBatchLegality::Optional {
                maximum: Some("1MiB".parse().assured("1 MiB is a whole number of bytes")),
            },
            ..codec(
                Some(
                    HeaderModel::exact(HeaderLayout::LastWins)
                        .with_typed_values(nervix_models::SQS_ATTRIBUTE_VALUES),
                ),
                &["RequestAck"],
            )
        },
        EmitterEntityKind::Sentry(SentryEmitterEntity::Event) => EmitterDeclaration {
            requires_batch_transformation: true,
            ..codec(None, &["RequestAck"])
        },
        EmitterEntityKind::Otel(
            OtelEmitterEntity::Logs | OtelEmitterEntity::Traces | OtelEmitterEntity::Metrics,
        )
        | EmitterEntityKind::Iceberg(IcebergEmitterEntity::Table) => request(optional),
        EmitterEntityKind::Postgres(PostgresEmitterEntity::Table) => EmitterDeclaration {
            batch: EmitterBatchLegality::Required {
                maximum: Some(
                    "1073741822B"
                        .parse()
                        .assured("PQ_LARGE_MESSAGE_LIMIT is a whole number of bytes"),
                ),
            },
            ..request(required)
        },
        EmitterEntityKind::ClickHouse(ClickHouseEmitterEntity::Table)
        | EmitterEntityKind::MySql(MySqlEmitterEntity::Table)
        | EmitterEntityKind::MongoDb(MongoDbEmitterEntity::Collection) => request(required),
    }
}

#[test]
fn every_emitter_pair_declares_its_headers_codec_batching_and_publishing_modes() {
    for kind in EmitterEntityKind::all() {
        let expected = emitter_declaration(kind);
        let declared = kind.capabilities();
        assert_eq!(declared.headers, expected.headers, "{kind:?}");
        assert_eq!(
            declared.writes_headers(),
            expected.headers.is_some(),
            "{kind:?}"
        );
        assert_eq!(declared.body, expected.body, "{kind:?}");
        assert_eq!(declared.batch, expected.batch, "{kind:?}");
        assert_eq!(
            declared.requires_batch_transformation, expected.requires_batch_transformation,
            "{kind:?}"
        );
        for (name, mode) in publishing_modes() {
            assert_eq!(
                declared.publishing_modes.contains(&mode),
                expected.modes.contains(&name.as_str()),
                "{kind:?} {name}"
            );
        }
    }
}

#[test]
fn the_sqs_emitter_admits_at_most_the_message_size_sqs_does() {
    let EmitterBatchLegality::Optional {
        maximum: Some(maximum),
    } = EmitterEntityKind::Sqs(SqsEmitterEntity::Queue)
        .capabilities()
        .batch
    else {
        panic!("SQS batches optionally, up to the message size it fixes");
    };
    assert_eq!(maximum.bytes().get(), 1024 * 1024);
}

#[test]
fn an_emitter_states_what_its_pair_declares() {
    for kind in EmitterEntityKind::all() {
        for seed in 0..4 {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let entity = arbitrary.emitter_entity_of(kind);
            let declared = kind.capabilities();
            assert_eq!(entity.kind(), kind);
            assert_eq!(entity.connector(), kind.connector());
            assert_eq!(entity.transport_label(), kind.phrase());
            assert_eq!(entity.capabilities(), declared);
            assert_eq!(entity.requires_codec(), declared.body.requires_codec());
            assert_eq!(entity.batch_legality(), declared.batch);
            assert_eq!(
                entity.mapped_value_types().is_some(),
                declared.body == nervix_models::EmitterBodyForm::Values,
                "{kind} declares direct mapping types exactly when it has a VALUES body",
            );
            assert_eq!(
                entity.requires_batch_transformation(),
                declared.requires_batch_transformation
            );
            assert!(
                declared
                    .publishing_modes
                    .contains(&entity.publishing().kind())
            );
            match entity.reference() {
                EmitterReference::Client(EmitterClientReference { client, connector }) => {
                    assert_eq!(entity.client(), Some(client));
                    assert!(declared.reference.admits_client(connector));
                }
                EmitterReference::StorageClient(client) => {
                    assert_eq!(entity.client(), Some(client));
                    assert!(matches!(
                        declared.reference,
                        nervix_models::EmitterReferenceKind::StorageClient(_)
                    ));
                }
                EmitterReference::Schema(_) => {
                    assert_eq!(entity.client(), None);
                    assert_eq!(
                        kind,
                        EmitterEntityKind::Client(ClientEmitterEntity::Consumer)
                    );
                }
            }
        }
    }
}

#[test]
fn every_client_model_names_the_connector_it_configures_and_no_other_model_does() {
    let mut clients = Vec::new();
    for variant in ModelVariant::ALL {
        let bytes = seeded_bytes(3);
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        let requested = arbitrary.model_of(variant);
        let model: Model = requested
            .try_map_resource_versions(|_, _| Ok::<_, std::convert::Infallible>(1))
            .expect("mapping a version to a constant cannot fail");
        if model.kind() == ModelKind::Client {
            clients.push(model);
        } else {
            assert_eq!(model.connector_kind(), None, "{variant:?}");
        }
    }
    let connectors = clients
        .iter()
        .map(|client| {
            client
                .connector_kind()
                .assured("every client Model configures a connector")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        connectors,
        [
            ConnectorKind::Kafka,
            ConnectorKind::Pulsar,
            ConnectorKind::Http,
            ConnectorKind::Sentry,
            ConnectorKind::Otel,
            ConnectorKind::Prometheus,
            ConnectorKind::Mqtt,
            ConnectorKind::Nats,
            ConnectorKind::RabbitMq,
            ConnectorKind::Redis,
            ConnectorKind::ZeroMq,
            ConnectorKind::Sqs,
            ConnectorKind::Websockets,
            ConnectorKind::Syslog,
            ConnectorKind::ClickHouse,
            ConnectorKind::Postgres,
            ConnectorKind::MySql,
            ConnectorKind::MongoDb,
            ConnectorKind::S3,
            ConnectorKind::Gcs,
            ConnectorKind::AzureBlob,
            ConnectorKind::IcebergRest,
        ]
    );
}

fn iceberg_emitter() -> IcebergTableEmitter {
    IcebergTableEmitter {
        mode: nervix_models::IcebergTablePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("lake"),
        table: named("events"),
        namespace: named("Analytics.Events"),
        values: vec![mapping("value")],
        location: "s3://lake/events".to_owned(),
        catalog: IcebergCatalog::Rest {
            client: named("catalog"),
        },
        commit_each: "30s".to_owned(),
        max_commit_size: "64MiB".parse().expect("valid commit size"),
    }
}

#[test]
fn an_iceberg_destination_retains_its_storage_client_for_resolution() {
    let connector = EmitterEntity::from(iceberg_emitter());
    let EmitterReference::StorageClient(client) = connector.reference() else {
        panic!("storage reference");
    };
    assert_eq!(client, &named::<ClientName>("lake"));
    assert_eq!(connector.client(), Some(client));
    let catalog: ClientName = named("catalog");
    assert_eq!(
        connector.catalog_client(),
        Some(EmitterClientReference {
            client: &catalog,
            connector: ConnectorKind::IcebergRest,
        })
    );
    assert_eq!(
        connector.commit_policy(),
        Some(("30s", "64MiB".parse().expect("valid commit size")))
    );
    let EmitterEntity::Iceberg(IcebergEmitter::Table(table)) = &connector else {
        panic!("an Iceberg table emitter was built, found {connector:?}");
    };
    assert_eq!(table.catalog.connector(), ConnectorKind::IcebergRest);
}

#[test]
fn only_an_iceberg_emitter_names_a_catalog_client_and_a_commit_policy() {
    for kind in EmitterEntityKind::all() {
        let bytes = seeded_bytes(7);
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        let connector = arbitrary.emitter_entity_of(kind);
        let iceberg = kind == EmitterEntityKind::Iceberg(IcebergEmitterEntity::Table);
        assert_eq!(connector.catalog_client().is_some(), iceberg, "{kind:?}");
        assert_eq!(connector.commit_policy().is_some(), iceberg, "{kind:?}");
    }
}

fn otel_mappings() -> OtelMappings {
    OtelMappings {
        values: vec![mapping("value")],
        attributes: vec![mapping("attribute")],
        resource: vec![mapping("resource")],
        scope: None,
    }
}

#[test]
fn an_otel_emitter_names_its_signal_by_its_entity() {
    let metric = OtelMetric {
        name: "queue.depth".to_owned(),
        unit: "1".to_owned(),
        description: None,
        kind: OtelMetricKind::Gauge,
    };
    let logs = OtelEmitter::from(OtelLogsEmitter {
        mode: nervix_models::OtelLogsPublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("collector"),
        mappings: otel_mappings(),
    });
    let traces = OtelEmitter::from(OtelTracesEmitter {
        mode: nervix_models::OtelTracesPublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("collector"),
        mappings: otel_mappings(),
    });
    let metrics = OtelEmitter::from(OtelMetricsEmitter {
        mode: nervix_models::OtelMetricsPublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("collector"),
        metric: metric.clone(),
        mappings: otel_mappings(),
    });
    assert_eq!(logs.entity(), OtelEmitterEntity::Logs);
    assert_eq!(traces.entity(), OtelEmitterEntity::Traces);
    assert_eq!(metrics.entity(), OtelEmitterEntity::Metrics);
    let OtelEmitter::Metrics(OtelMetricsEmitter {
        metric: declared, ..
    }) = &metrics
    else {
        panic!("a metrics emitter was built, found {metrics:?}");
    };
    assert_eq!(declared, &metric);
    for connector in [&logs, &traces, &metrics] {
        assert_eq!(connector.client(), &named::<ClientName>("collector"));
        assert_eq!(connector.mappings(), &otel_mappings());
    }
}

#[test]
fn direct_value_mappings_cover_all_external_value_sections() {
    let client: ClientName = named("emitter");
    let mapping = |column: &str| ClickHouseValueMapping {
        column: column.to_owned(),
        expression: Expression::Literal(Literal::String(column.to_owned())),
    };
    let single_value = || vec![mapping("value")];
    let emitters = [
        EmitterEntity::from(OtelLogsEmitter {
            mode: nervix_models::OtelLogsPublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: client.clone(),
            mappings: OtelMappings {
                values: single_value(),
                attributes: vec![mapping("attribute")],
                resource: vec![mapping("resource")],
                scope: None,
            },
        }),
        EmitterEntity::ClickHouse(ClickHouseEmitter::Table(ClickHouseTableEmitter {
            mode: nervix_models::ClickHouseTablePublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: client.clone(),
            table: named("events"),
            values: single_value(),
        })),
        EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
            mode: nervix_models::PostgresTablePublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: client.clone(),
            table: named("events"),
            values: single_value(),
            conflict_action: PostgresConflictAction::None,
        })),
        EmitterEntity::MySql(MySqlEmitter::Table(MySqlTableEmitter {
            mode: nervix_models::MySqlTablePublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: client.clone(),
            table: named("events"),
            values: single_value(),
            conflict_action: MySqlConflictAction::None,
        })),
        EmitterEntity::MongoDb(MongoDbEmitter::Collection(MongoDbCollectionEmitter {
            mode: nervix_models::MongoDbCollectionPublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: client.clone(),
            collection: named::<nervix_models::MongoDbCollectionName>("events"),
            values: single_value(),
            conflict_action: MongoDbConflictAction::None,
        })),
        EmitterEntity::Iceberg(IcebergEmitter::Table(IcebergTableEmitter {
            mode: nervix_models::IcebergTablePublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: client.clone(),
            table: named("events"),
            namespace: named("Analytics.Events"),
            values: single_value(),
            location: "s3://bucket/events".to_owned(),
            catalog: IcebergCatalog::Rest {
                client: client.clone(),
            },
            commit_each: "1s".to_owned(),
            max_commit_size: "1MB".parse().expect("valid commit size"),
        })),
    ];

    for (index, connector) in emitters.iter().enumerate() {
        let columns = connector
            .direct_value_mappings()
            .map(|mapping| mapping.column.as_str())
            .collect::<Vec<_>>();
        let expected = if index == 0 {
            vec!["value", "attribute", "resource"]
        } else {
            vec!["value"]
        };
        assert_eq!(columns, expected, "{connector:?}");
    }

    let emitter_without_values = EmitterEntity::Sentry(SentryEmitter::Event(SentryEventEmitter {
        client,
        platform: None,
        mode: nervix_models::SentryEventPublishingMode::Ack {
            retry_policy: retry_policy(),
        },
    }));
    assert_eq!(emitter_without_values.direct_value_mappings().count(), 0);
}

#[test]
fn two_emitters_are_one_definition_when_only_their_client_differs() {
    let kafka = |client: &str, topic: &str| {
        EmitterEntity::from(KafkaTopicEmitter {
            mode: nervix_models::KafkaTopicPublishingMode::NoAck {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: named(client),
            topic: named(topic),
        })
    };
    assert!(kafka("first", "orders").same_definition(&kafka("first", "orders")));
    assert!(kafka("first", "orders").same_definition(&kafka("second", "orders")));
    assert!(!kafka("first", "orders").same_definition(&kafka("first", "payments")));

    let pulsar = EmitterEntity::from(PulsarTopicEmitter {
        mode: nervix_models::PulsarTopicPublishingMode::NoAck {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("first"),
        topic: named("orders"),
    });
    assert!(!kafka("first", "orders").same_definition(&pulsar));

    let consumer = |schema: &str| {
        EmitterEntity::from(ClientConsumerEmitter {
            mode: nervix_models::ClientConsumerPublishingMode::Ack {
                window: nervix_models::AckWindow::Sequential,
                ack_timeout: "30s".to_string(),
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            schema: named::<SchemaName>(schema),
        })
    };
    assert!(consumer("orders").same_definition(&consumer("orders")));
    assert!(!consumer("orders").same_definition(&consumer("payments")));
    assert!(!consumer("orders").same_definition(&kafka("first", "orders")));
    assert!(!kafka("first", "orders").same_definition(&consumer("orders")));

    let iceberg = |client: &str, catalog: &str| {
        let mut connector = iceberg_emitter();
        connector.client = named(client);
        connector.catalog = IcebergCatalog::Rest {
            client: named(catalog),
        };
        EmitterEntity::from(connector)
    };
    assert!(iceberg("lake", "catalog").same_definition(&iceberg("other_lake", "catalog")));
    assert!(!iceberg("lake", "catalog").same_definition(&iceberg("lake", "other_catalog")));
}

/// A batching clause every emitter's contract admits: small enough for the tightest destination.
fn admissible_batch() -> EmitterBatchPolicy {
    EmitterBatchPolicy {
        max_messages: BatchMessageLimit::try_from(8_u32)
            .assured("eight messages are within the limit's range"),
        max_size: "1KiB"
            .parse()
            .assured("one kibibyte is a whole number of bytes"),
    }
}

#[test]
fn naming_another_client_keeps_a_emitter_of_every_pair_the_same_definition() {
    let other: ClientName = named("rebound");
    for kind in EmitterEntityKind::all() {
        for seed in 0..4 {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let mut original = arbitrary.create_emitter_to(kind);
            // An alteration checks the batching contract again, which is not what this check is
            // about, so the emitter batches exactly as its emitter requires.
            original.batch = match original.connector.batch_legality() {
                EmitterBatchLegality::Required { .. } => Some(admissible_batch()),
                EmitterBatchLegality::Optional { .. }
                | EmitterBatchLegality::Unavailable { .. } => None,
            };
            let alter = AlterEmitter {
                kind,
                emitter: original.name.clone(),
                operations: vec![AlterEmitterOperation::SetClient {
                    client: other.clone(),
                }],
            };
            let mut rebound = original.clone();
            let outcome = rebound.apply_alter(&alter);

            if original.connector.client().is_none() {
                let error = outcome.expect_err(
                    "an emitter that serves session consumers publishes through no client",
                );
                assert_eq!(
                    error.current_context(),
                    &AlterEmitterError::ClientUnsupported,
                    "{kind:?}"
                );
                assert_eq!(rebound, original, "{kind:?}");
                continue;
            }
            outcome.expect("an emitter that publishes through a client can name another");
            assert_eq!(rebound.connector.kind(), kind);
            assert_eq!(rebound.connector.client(), Some(&other), "{kind:?}");
            assert!(
                original.connector.same_definition(&rebound.connector),
                "{kind:?}"
            );
            assert!(
                rebound.connector.same_definition(&original.connector),
                "{kind:?}"
            );
        }
    }
}

#[test]
fn a_source_acknowledges_as_its_delivery_mode_states() {
    let retry = retry_policy();
    let max = std::num::NonZeroU64::new(8).assured("eight is not zero");
    let sequential = IngestAcknowledgement::Sequential {
        timeout: "5s",
        retry: &retry,
    };
    let parallel = IngestAcknowledgement::Parallel {
        max,
        batch_timeout: "250ms",
        timeout: "5s",
        retry: &retry,
    };

    let kafka = |mode| {
        IngestorEntity::from(KafkaTopicIngestor {
            client: named("events"),
            topic: named("orders"),
            offset_mode: KafkaOffsetMode::Domain,
            instances: std::num::NonZeroU64::MIN,
            mode,
            quiesce: IngestQuiesceMode::Suspend,
        })
    };
    assert_eq!(
        kafka(KafkaIngestMode::AckSequential {
            timeout: "5s".to_owned(),
            retry_policy: retry.clone(),
        })
        .acknowledgement(),
        sequential
    );
    assert_eq!(
        kafka(KafkaIngestMode::AckParallel {
            max,
            batch_timeout: "250ms".to_owned(),
            timeout: "5s".to_owned(),
            retry_policy: retry.clone(),
        })
        .acknowledgement(),
        parallel
    );
    assert_eq!(
        kafka(KafkaIngestMode::NoAck).acknowledgement(),
        IngestAcknowledgement::Unacknowledged
    );

    let mqtt_modes = [
        MqttIngestMode::NoAck,
        MqttIngestMode::AckSequential {
            timeout: "5s".to_owned(),
            retry_policy: retry.clone(),
        },
        MqttIngestMode::AckParallel {
            max,
            batch_timeout: "250ms".to_owned(),
            timeout: "5s".to_owned(),
            retry_policy: retry.clone(),
        },
    ];
    let expected = [IngestAcknowledgement::Unacknowledged, sequential, parallel];
    for (mode, expected) in mqtt_modes.into_iter().zip(expected) {
        let acknowledges = mode.is_ack();
        let source = IngestorEntity::from(MqttTopicIngestor {
            client: named("broker"),
            topic: named("orders/#"),
            instances: std::num::NonZeroU64::MIN,
            session: MqttSession::Persistent,
            qos: MqttQos::Qos1,
            mode,
            quiesce: IngestQuiesceMode::Drop,
        });
        assert_eq!(source.acknowledgement(), expected);
        assert_eq!(
            acknowledges,
            expected != IngestAcknowledgement::Unacknowledged,
            "{source:?}"
        );
    }
}

#[test]
fn an_entity_model_converts_into_every_enum_that_holds_it() {
    let source = RedisPubSubIngestor {
        client: named("cache"),
        channel: named("orders"),
        mode: RedisPubSubIngestMode::NoAck,
        quiesce: IngestQuiesceMode::Drop,
    };
    let nested = RedisIngestor::from(source.clone());
    assert_eq!(nested.entity(), RedisIngestorEntity::PubSub);
    assert_eq!(
        IngestorEntity::from(source.clone()),
        IngestorEntity::from(nested)
    );
    assert_eq!(
        IngestorEntity::from(source).kind(),
        IngestorEntityKind::from(RedisIngestorEntity::PubSub)
    );
    assert_eq!(
        IngestorEntityKind::from(ClientIngestorEntity::Producer),
        IngestorEntityKind::Client(ClientIngestorEntity::Producer)
    );

    let connector = RedisPubSubEmitter {
        mode: nervix_models::RedisPubSubPublishingMode::NoAck {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("cache"),
        channel: named("orders"),
    };
    let nested = RedisEmitter::from(connector.clone());
    assert_eq!(nested.entity(), RedisEmitterEntity::PubSub);
    assert_eq!(
        EmitterEntity::from(connector.clone()),
        EmitterEntity::from(nested)
    );
    assert_eq!(
        EmitterEntity::from(connector).kind(),
        EmitterEntityKind::from(RedisEmitterEntity::PubSub)
    );
}

#[test]
fn a_client_producer_window_counts_the_batches_it_admits() {
    let mode = |window| ClientIngestMode {
        window,
        ack_timeout: "5s".to_owned(),
        retry_policy: retry_policy(),
    };
    assert_eq!(mode(AckWindow::Sequential).window_size().get(), 1);
    let max = std::num::NonZeroU64::new(8).assured("eight is not zero");
    assert_eq!(mode(AckWindow::Parallel { max }).window_size(), max);
}

#[test]
fn an_ingest_source_refuses_a_quiesce_mode_its_transport_lacks() {
    let buffer = IngestQuiesceMode::EndpointBuffer {
        max_size: "1MiB".to_string(),
    };
    let mut source = IngestorEntity::Http(HttpIngestor::Endpoint(HttpEndpointIngestor {
        endpoint: named("ingress"),
        mode: EndpointIngestMode::NoAck,
        quiesce: buffer.clone(),
    }));

    let error = source
        .set_quiesce(IngestQuiesceMode::Suspend)
        .expect_err("an endpoint cannot suspend its callers");

    assert_eq!(
        error.current_context(),
        &AlterIngestorError::UnsupportedQuiesceMode {
            kind: IngestorEntityKind::Http(HttpIngestorEntity::Endpoint),
            mode: "SUSPEND",
        }
    );
    assert_eq!(
        error.to_string(),
        "HTTP ENDPOINT ingestors do not support ON QUIESCE SUSPEND"
    );
    assert_eq!(source.quiesce(), &buffer);
}
