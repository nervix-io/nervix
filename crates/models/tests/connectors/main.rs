//! Every connector and entity pair declares what the vocabulary says it does.
//!
//! Layer: test harness.
//!
//! - **Owns.** The checks that the source and sink unions name every pair once, that each pair's
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
    BatchMessageLimit, BatchUnavailability, ClickHouseSink, ClickHouseSinkEntity,
    ClickHouseTableSink, ClickHouseValueMapping, ClientConsumerSink, ClientIngestMode, ClientName,
    ClientSinkEntity, ClientSourceEntity, CollectionName, ConnectorKind, EmitSink, EmitSinkKind,
    EmitterBatchLegality, EmitterBatchPolicy, EmitterBodyForm, EmitterPublishingMode,
    EndpointIngestMode, EndpointType, Expression, HttpEndpointSource, HttpSinkEntity, HttpSource,
    HttpSourceEntity, IcebergCatalog, IcebergSink, IcebergSinkEntity, IcebergStorageBackend,
    IcebergTableSink, IngestAcknowledgement, IngestQuiesceMode, IngestQuiesceOverflow,
    IngestSource, IngestSourceKind, KafkaIngestMode, KafkaOffsetMode, KafkaSinkEntity,
    KafkaSourceEntity, KafkaTopicSink, KafkaTopicSource, Literal, Model, ModelKind,
    MongoDbCollectionSink, MongoDbConflictAction, MongoDbSink, MongoDbSinkEntity, MqttIngestMode,
    MqttQos, MqttSession, MqttSinkEntity, MqttSourceEntity, MqttTopicSource, MySqlConflictAction,
    MySqlSink, MySqlSinkEntity, MySqlTableSink, NatsSinkEntity, NatsSourceEntity, OtelLogsSink,
    OtelMappings, OtelMetric, OtelMetricKind, OtelMetricsSink, OtelSink, OtelSinkEntity,
    OtelTracesSink, ParseAsType, PostgresConflictAction, PostgresSink, PostgresSinkEntity,
    PostgresTableSink, PrometheusSourceEntity, PulsarSinkEntity, PulsarSourceEntity,
    PulsarTopicSink, RabbitMqSinkEntity, RabbitMqSourceEntity, RedisPubSubIngestMode,
    RedisPubSubSink, RedisPubSubSource, RedisSink, RedisSinkEntity, RedisSource, RedisSourceEntity,
    RetryPolicy, SchemaName, SentryEventSink, SentrySink, SentrySinkEntity, SinkClientReference,
    SinkReference, SinkValueMapping, SourceMetadataScope, SourceReferenceKind, SqsSinkEntity,
    SqsSourceEntity, SyslogSinkEntity, SyslogSourceEntity, TableName, WebsocketsEndpointSource,
    WebsocketsSourceEntity, ZeroMqSinkEntity, ZeroMqSourceEntity,
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

fn mapping(column: &str) -> SinkValueMapping {
    SinkValueMapping {
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

/// One publishing mode of every kind the vocabulary defines, by its variant name.
fn publishing_modes() -> Vec<(&'static str, EmitterPublishingMode)> {
    let confirmed = || (AckWindow::Sequential, "5s".to_owned(), retry_policy());
    let (window, ack_timeout, retry) = confirmed();
    let client_ack = EmitterPublishingMode::ClientAck {
        window,
        ack_timeout,
        retry_policy: retry,
    };
    let (window, ack_timeout, retry) = confirmed();
    let broker_ack = EmitterPublishingMode::BrokerAck {
        window,
        ack_timeout,
        retry_policy: retry,
    };
    let (window, ack_timeout, retry) = confirmed();
    let mqtt_qos_1 = EmitterPublishingMode::MqttQos1 {
        window,
        ack_timeout,
        retry_policy: retry,
    };
    let (window, ack_timeout, retry) = confirmed();
    let mqtt_qos_2 = EmitterPublishingMode::MqttQos2 {
        window,
        ack_timeout,
        retry_policy: retry,
    };
    let (window, ack_timeout, retry) = confirmed();
    let jet_stream = EmitterPublishingMode::NatsJetStream {
        window,
        ack_timeout,
        retry_policy: retry,
    };
    vec![
        ("ClientAck", client_ack),
        (
            "NoAck",
            EmitterPublishingMode::NoAck {
                retry_policy: retry_policy(),
            },
        ),
        ("BrokerAck", broker_ack),
        (
            "MqttQos0",
            EmitterPublishingMode::MqttQos0 {
                retry_policy: retry_policy(),
            },
        ),
        ("MqttQos1", mqtt_qos_1),
        ("MqttQos2", mqtt_qos_2),
        ("NatsJetStream", jet_stream),
        (
            "SqsSingle",
            EmitterPublishingMode::SqsSingle {
                retry_policy: retry_policy(),
            },
        ),
        (
            "SqsBatch",
            EmitterPublishingMode::SqsBatch {
                retry_policy: retry_policy(),
            },
        ),
        (
            "RequestAck",
            EmitterPublishingMode::RequestAck {
                retry_policy: retry_policy(),
            },
        ),
    ]
}

/// Fails when two pairs share a phrase, which would make one of them unaddressable.
fn assert_names_each_once(phrases: &[&'static str]) {
    let mut distinct = phrases.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), phrases.len(), "{phrases:?}");
}

#[test]
fn every_connector_kind_is_spelled_as_nspl_spells_it() {
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
    let phrases = IngestSourceKind::all()
        .map(IngestSourceKind::phrase)
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
fn the_sink_kinds_name_every_pair_once_with_its_phrase() {
    let phrases = EmitSinkKind::all()
        .map(EmitSinkKind::phrase)
        .collect::<Vec<_>>();
    assert_eq!(
        phrases,
        [
            "CLIENT CONSUMER",
            "HTTP REQUEST",
            "KAFKA TOPIC",
            "PULSAR TOPIC",
            "RABBITMQ QUEUE",
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
    for kind in IngestSourceKind::all() {
        assert_eq!(
            kind.phrase(),
            format!("{} {}", kind.connector().keyword(), kind.entity_keyword())
        );
    }
    for kind in EmitSinkKind::all() {
        assert_eq!(
            kind.phrase(),
            format!("{} {}", kind.connector().keyword(), kind.entity_keyword())
        );
    }
}

#[test]
fn the_transport_sources_are_every_source_pair_but_the_client_producer() {
    let transports = IngestSourceKind::modeled().collect::<Vec<_>>();
    let mut expected = IngestSourceKind::all().collect::<Vec<_>>();
    expected.retain(|kind| *kind != IngestSourceKind::Client(ClientSourceEntity::Producer));
    assert_eq!(transports, expected);
}

#[test]
fn a_source_form_names_its_pair_by_key_and_label() {
    let forms = IngestSourceKind::all()
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
struct SourceDeclaration {
    reads_headers: bool,
    metadata: SourceMetadataScope,
    every_node: bool,
    reference: SourceReferenceKind,
}

fn source_declaration(kind: IngestSourceKind) -> SourceDeclaration {
    let headers_only = |reads_headers, connector| SourceDeclaration {
        reads_headers,
        metadata: SourceMetadataScope::Headers,
        every_node: false,
        reference: SourceReferenceKind::Client(connector),
    };
    match kind {
        IngestSourceKind::Client(ClientSourceEntity::Producer) => SourceDeclaration {
            reads_headers: false,
            metadata: SourceMetadataScope::Headers,
            every_node: false,
            reference: SourceReferenceKind::Schema,
        },
        IngestSourceKind::Http(HttpSourceEntity::Request) => {
            headers_only(true, ConnectorKind::Http)
        }
        IngestSourceKind::Kafka(KafkaSourceEntity::Topic) => SourceDeclaration {
            reads_headers: true,
            metadata: SourceMetadataScope::Kafka,
            every_node: false,
            reference: SourceReferenceKind::Client(ConnectorKind::Kafka),
        },
        IngestSourceKind::Pulsar(PulsarSourceEntity::Topic) => {
            headers_only(true, ConnectorKind::Pulsar)
        }
        IngestSourceKind::Mqtt(MqttSourceEntity::Topic) => headers_only(false, ConnectorKind::Mqtt),
        IngestSourceKind::Nats(NatsSourceEntity::Subject) => {
            headers_only(true, ConnectorKind::Nats)
        }
        IngestSourceKind::RabbitMq(RabbitMqSourceEntity::Queue) => {
            headers_only(true, ConnectorKind::RabbitMq)
        }
        IngestSourceKind::Redis(RedisSourceEntity::PubSub) => {
            headers_only(false, ConnectorKind::Redis)
        }
        IngestSourceKind::Prometheus(PrometheusSourceEntity::Query) => {
            headers_only(false, ConnectorKind::Prometheus)
        }
        IngestSourceKind::ZeroMq(ZeroMqSourceEntity::Pull) => {
            headers_only(false, ConnectorKind::ZeroMq)
        }
        IngestSourceKind::Sqs(SqsSourceEntity::Queue) => headers_only(true, ConnectorKind::Sqs),
        IngestSourceKind::Http(HttpSourceEntity::Endpoint) => SourceDeclaration {
            reads_headers: true,
            metadata: SourceMetadataScope::Headers,
            every_node: true,
            reference: SourceReferenceKind::Endpoint(EndpointType::Http),
        },
        IngestSourceKind::Websockets(WebsocketsSourceEntity::Connection) => SourceDeclaration {
            reads_headers: true,
            metadata: SourceMetadataScope::Websockets,
            every_node: false,
            reference: SourceReferenceKind::Client(ConnectorKind::Websockets),
        },
        IngestSourceKind::Websockets(WebsocketsSourceEntity::Endpoint) => SourceDeclaration {
            reads_headers: true,
            metadata: SourceMetadataScope::Headers,
            every_node: true,
            reference: SourceReferenceKind::Endpoint(EndpointType::Websockets),
        },
        IngestSourceKind::Syslog(SyslogSourceEntity::Listener) => SourceDeclaration {
            reads_headers: false,
            metadata: SourceMetadataScope::Syslog,
            every_node: true,
            reference: SourceReferenceKind::Client(ConnectorKind::Syslog),
        },
    }
}

#[test]
fn every_source_pair_declares_its_headers_metadata_execution_and_reference() {
    for kind in IngestSourceKind::all() {
        let expected = source_declaration(kind);
        let declared = kind.capabilities();
        assert_eq!(declared.reads_headers, expected.reads_headers, "{kind:?}");
        assert_eq!(kind.reads_headers(), expected.reads_headers, "{kind:?}");
        assert_eq!(declared.metadata, expected.metadata, "{kind:?}");
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
    let client = SourceReferenceKind::Client(ConnectorKind::Kafka);
    assert_eq!(client.model_kind(), ModelKind::Client);
    assert_eq!(client.client_connector(), Some(ConnectorKind::Kafka));
    assert_eq!(client.endpoint_type(), None);
    let endpoint = SourceReferenceKind::Endpoint(EndpointType::Websockets);
    assert_eq!(endpoint.model_kind(), ModelKind::Endpoint);
    assert_eq!(endpoint.client_connector(), None);
    assert_eq!(endpoint.endpoint_type(), Some(EndpointType::Websockets));
    assert_eq!(SourceReferenceKind::Schema.model_kind(), ModelKind::Schema);
    assert_eq!(SourceReferenceKind::Schema.client_connector(), None);
    assert_eq!(SourceReferenceKind::Schema.endpoint_type(), None);
}

#[test]
fn a_transport_source_states_what_its_pair_declares() {
    for kind in IngestSourceKind::modeled() {
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
            assert_eq!(source.reads_headers(), declared.reads_headers);
            assert_eq!(
                source.executes_on_every_cluster_node(),
                declared.executes_on_every_cluster_node
            );
            assert_eq!(source.source_kind(), declared.reference.model_kind());
            assert!(source.supports_quiesce(source.quiesce()), "{source:?}");
        }
    }
}

/// The quiesce modes one source pair supports, of the five the vocabulary defines, in the order
/// suspend, buffer, drop, reject, endpoint buffer.
#[rstest]
#[case::http(IngestSourceKind::Http(HttpSourceEntity::Request), [true, true, false, false, false])]
#[case::kafka(IngestSourceKind::Kafka(KafkaSourceEntity::Topic), [true, false, false, false, false])]
#[case::pulsar(IngestSourceKind::Pulsar(PulsarSourceEntity::Topic), [true, false, false, false, false])]
#[case::nats(IngestSourceKind::Nats(NatsSourceEntity::Subject), [false, true, true, false, false])]
#[case::rabbitmq(IngestSourceKind::RabbitMq(RabbitMqSourceEntity::Queue), [true, false, false, false, false])]
#[case::redis(IngestSourceKind::Redis(RedisSourceEntity::PubSub), [false, true, true, false, false])]
#[case::prometheus(IngestSourceKind::Prometheus(PrometheusSourceEntity::Query), [true, true, false, false, false])]
#[case::zeromq(IngestSourceKind::ZeroMq(ZeroMqSourceEntity::Pull), [true, true, true, false, false])]
#[case::sqs(IngestSourceKind::Sqs(SqsSourceEntity::Queue), [true, false, false, false, false])]
#[case::http_endpoint(IngestSourceKind::Http(HttpSourceEntity::Endpoint), [false, false, false, true, true])]
#[case::websockets(IngestSourceKind::Websockets(WebsocketsSourceEntity::Connection), [false, true, true, false, false])]
#[case::websockets_endpoint(IngestSourceKind::Websockets(WebsocketsSourceEntity::Endpoint), [false, false, false, true, true])]
#[case::syslog(IngestSourceKind::Syslog(SyslogSourceEntity::Listener), [true, true, true, false, false])]
fn a_source_supports_the_quiesce_modes_its_entity_declares(
    #[case] kind: IngestSourceKind,
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
    let source = |mode| {
        IngestSource::from(MqttTopicSource {
            client: named("broker"),
            topic: named("orders/#"),
            instances: std::num::NonZeroU64::MIN,
            mode,
            quiesce: IngestQuiesceMode::Drop,
        })
    };
    let persistent = source(MqttIngestMode::NoAck {
        session: MqttSession::Persistent,
        qos: MqttQos::AtLeastOnce,
    });
    let clean = source(MqttIngestMode::NoAck {
        session: MqttSession::Clean,
        qos: MqttQos::AtLeastOnce,
    });
    let at_most_once = source(MqttIngestMode::NoAck {
        session: MqttSession::Persistent,
        qos: MqttQos::AtMostOnce,
    });
    let acknowledged = source(MqttIngestMode::AckSequential {
        timeout: "5s".to_owned(),
        retry_policy: retry_policy(),
    });
    assert!(persistent.supports_quiesce(&IngestQuiesceMode::Suspend));
    assert!(!clean.supports_quiesce(&IngestQuiesceMode::Suspend));
    assert!(!at_most_once.supports_quiesce(&IngestQuiesceMode::Suspend));
    assert!(acknowledged.supports_quiesce(&IngestQuiesceMode::Suspend));
    assert!(clean.supports_quiesce(&IngestQuiesceMode::Drop));
    assert!(!clean.supports_quiesce(&IngestQuiesceMode::Reject {
        retry_after: "1s".to_owned(),
    }));
}

#[test]
fn a_source_references_the_client_or_endpoint_its_body_names() {
    let kafka = IngestSource::from(KafkaTopicSource {
        client: named("events"),
        topic: named("orders"),
        offset_mode: KafkaOffsetMode::Domain,
        instances: std::num::NonZeroU64::MIN,
        mode: KafkaIngestMode::NoAck,
        quiesce: IngestQuiesceMode::Suspend,
    });
    assert_eq!(kafka.source_ref().as_str(), "events");
    assert_eq!(kafka.source_kind(), ModelKind::Client);
    assert_eq!(
        kafka.acknowledgement(),
        IngestAcknowledgement::Unacknowledged
    );

    let endpoint = IngestSource::from(HttpEndpointSource {
        endpoint: named("intake"),
        mode: EndpointIngestMode::NoAck,
        quiesce: IngestQuiesceMode::Reject {
            retry_after: "1s".to_owned(),
        },
    });
    assert_eq!(endpoint.source_ref().as_str(), "intake");
    assert_eq!(endpoint.source_kind(), ModelKind::Endpoint);

    let stream = IngestSource::from(WebsocketsEndpointSource {
        endpoint: named("stream"),
        mode: EndpointIngestMode::NoAck,
        quiesce: IngestQuiesceMode::EndpointBuffer {
            max_size: "1MiB".to_owned(),
        },
    });
    assert_eq!(stream.source_ref().as_str(), "stream");
    assert_eq!(stream.source_kind(), ModelKind::Endpoint);
    assert_eq!(
        stream.kind(),
        IngestSourceKind::Websockets(WebsocketsSourceEntity::Endpoint)
    );
}

#[test]
fn the_metadata_scopes_declare_their_fields_in_order() {
    let described = |scope: SourceMetadataScope| {
        scope
            .fields()
            .into_iter()
            .map(|field| {
                assert!(field.optional, "{field:?}");
                assert!(!field.sensitive, "{field:?}");
                (field.name.as_str().to_owned(), field.ty)
            })
            .collect::<Vec<_>>()
    };
    assert!(described(SourceMetadataScope::Headers).is_empty());
    assert_eq!(
        described(SourceMetadataScope::Kafka),
        [
            ("topic".to_owned(), ParseAsType::String),
            ("partition".to_owned(), ParseAsType::I32),
            ("offset".to_owned(), ParseAsType::I64),
        ]
    );
    assert_eq!(
        described(SourceMetadataScope::Syslog),
        [("peer_addr".to_owned(), ParseAsType::String)]
    );
}

/// What one sink pair declares, as the table below states it.
struct SinkDeclaration {
    writes_headers: bool,
    body: EmitterBodyForm,
    batch: EmitterBatchLegality,
    requires_batch_transformation: bool,
    modes: &'static [&'static str],
}

fn sink_declaration(kind: EmitSinkKind) -> SinkDeclaration {
    let codec = |writes_headers, modes| SinkDeclaration {
        writes_headers,
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        modes,
    };
    let request = |batch| SinkDeclaration {
        writes_headers: false,
        body: EmitterBodyForm::Values,
        batch,
        requires_batch_transformation: false,
        modes: &["RequestAck"],
    };
    let required = EmitterBatchLegality::Required { maximum: None };
    let optional = EmitterBatchLegality::Optional { maximum: None };
    match kind {
        EmitSinkKind::Client(ClientSinkEntity::Consumer) => SinkDeclaration {
            writes_headers: false,
            body: EmitterBodyForm::Native,
            batch: required,
            requires_batch_transformation: false,
            modes: &["ClientAck"],
        },
        EmitSinkKind::Http(HttpSinkEntity::Request) => SinkDeclaration {
            writes_headers: true,
            body: EmitterBodyForm::CodecOrWithoutBody,
            ..request(EmitterBatchLegality::Unavailable {
                reason: BatchUnavailability::OneRequestPerRecord,
            })
        },
        EmitSinkKind::Kafka(KafkaSinkEntity::Topic)
        | EmitSinkKind::Pulsar(PulsarSinkEntity::Topic)
        | EmitSinkKind::RabbitMq(RabbitMqSinkEntity::Queue) => codec(true, &["NoAck", "BrokerAck"]),
        EmitSinkKind::Redis(RedisSinkEntity::PubSub)
        | EmitSinkKind::ZeroMq(ZeroMqSinkEntity::Push)
        | EmitSinkKind::Syslog(SyslogSinkEntity::Collector) => codec(false, &["NoAck"]),
        EmitSinkKind::Mqtt(MqttSinkEntity::Topic) => {
            codec(false, &["MqttQos0", "MqttQos1", "MqttQos2"])
        }
        EmitSinkKind::Nats(NatsSinkEntity::Subject) => codec(true, &["NoAck", "NatsJetStream"]),
        EmitSinkKind::Sqs(SqsSinkEntity::Queue) => SinkDeclaration {
            batch: EmitterBatchLegality::Optional {
                maximum: Some(
                    "256KiB"
                        .parse()
                        .assured("256 KiB is a whole number of bytes"),
                ),
            },
            ..codec(true, &["SqsSingle", "SqsBatch"])
        },
        EmitSinkKind::Sentry(SentrySinkEntity::Event) => SinkDeclaration {
            requires_batch_transformation: true,
            ..codec(false, &["RequestAck"])
        },
        EmitSinkKind::Otel(
            OtelSinkEntity::Logs | OtelSinkEntity::Traces | OtelSinkEntity::Metrics,
        )
        | EmitSinkKind::Iceberg(IcebergSinkEntity::Table) => request(optional),
        EmitSinkKind::ClickHouse(ClickHouseSinkEntity::Table)
        | EmitSinkKind::Postgres(PostgresSinkEntity::Table)
        | EmitSinkKind::MySql(MySqlSinkEntity::Table)
        | EmitSinkKind::MongoDb(MongoDbSinkEntity::Collection) => request(required),
    }
}

#[test]
fn every_sink_pair_declares_its_headers_codec_batching_and_publishing_modes() {
    for kind in EmitSinkKind::all() {
        let expected = sink_declaration(kind);
        let declared = kind.capabilities();
        assert_eq!(declared.writes_headers, expected.writes_headers, "{kind:?}");
        assert_eq!(declared.body, expected.body, "{kind:?}");
        assert_eq!(declared.batch, expected.batch, "{kind:?}");
        assert_eq!(
            declared.requires_batch_transformation, expected.requires_batch_transformation,
            "{kind:?}"
        );
        for (name, mode) in publishing_modes() {
            assert_eq!(
                kind.accepts_publishing_mode(&mode),
                expected.modes.contains(&name),
                "{kind:?} {name}"
            );
            assert_eq!(
                declared.publishing_modes.contains(&mode.kind()),
                expected.modes.contains(&name),
                "{kind:?} {name}"
            );
        }
    }
}

#[test]
fn the_sqs_sink_admits_at_most_the_message_size_sqs_does() {
    let EmitterBatchLegality::Optional {
        maximum: Some(maximum),
    } = EmitSinkKind::Sqs(SqsSinkEntity::Queue).capabilities().batch
    else {
        panic!("SQS batches optionally, up to the message size it fixes");
    };
    assert_eq!(maximum.bytes().get(), 256 * 1024);
}

#[test]
fn a_sink_states_what_its_pair_declares() {
    for kind in EmitSinkKind::all() {
        for seed in 0..4 {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let sink = arbitrary.emit_sink_of(kind);
            let declared = kind.capabilities();
            assert_eq!(sink.kind(), kind);
            assert_eq!(sink.connector(), kind.connector());
            assert_eq!(sink.transport_label(), kind.connector().keyword());
            assert_eq!(sink.capabilities(), declared);
            assert_eq!(sink.requires_codec(), declared.body.requires_codec());
            assert_eq!(sink.batch_legality(), declared.batch);
            assert_eq!(
                sink.requires_batch_transformation(),
                declared.requires_batch_transformation
            );
            for (name, mode) in publishing_modes() {
                assert_eq!(
                    sink.accepts_publishing_mode(&mode),
                    kind.accepts_publishing_mode(&mode),
                    "{kind:?} {name}"
                );
            }
            match sink.reference() {
                SinkReference::Client(SinkClientReference { client, connector }) => {
                    assert_eq!(sink.client(), Some(client));
                    assert_eq!(sink.client_connector(), Some(connector));
                }
                SinkReference::Schema(_) => {
                    assert_eq!(sink.client(), None);
                    assert_eq!(sink.client_connector(), None);
                    assert_eq!(kind, EmitSinkKind::Client(ClientSinkEntity::Consumer));
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

fn iceberg_sink(backend: IcebergStorageBackend) -> IcebergTableSink {
    IcebergTableSink {
        backend,
        client: named("lake"),
        table: named("events"),
        values: vec![mapping("value")],
        location: "s3://lake/events".to_owned(),
        catalog: IcebergCatalog::Rest {
            client: named("catalog"),
        },
        commit_each: "30s".to_owned(),
        max_commit_size: "64MiB".to_owned(),
    }
}

#[rstest]
#[case::s3(IcebergStorageBackend::S3, ConnectorKind::S3)]
#[case::gcs(IcebergStorageBackend::Gcs, ConnectorKind::Gcs)]
#[case::azure_blob(IcebergStorageBackend::AzureBlob, ConnectorKind::AzureBlob)]
fn an_iceberg_sink_writes_through_the_object_store_its_backend_names(
    #[case] backend: IcebergStorageBackend,
    #[case] connector: ConnectorKind,
) {
    let sink = EmitSink::from(iceberg_sink(backend));
    assert_eq!(backend.connector(), connector);
    assert_eq!(sink.client_connector(), Some(connector));
    assert_eq!(sink.client(), Some(&named::<ClientName>("lake")));
    let catalog: ClientName = named("catalog");
    assert_eq!(
        sink.catalog_client(),
        Some(SinkClientReference {
            client: &catalog,
            connector: ConnectorKind::IcebergRest,
        })
    );
    assert_eq!(sink.commit_policy(), Some(("30s", "64MiB")));
    let EmitSink::Iceberg(IcebergSink::Table(table)) = &sink else {
        panic!("an Iceberg table sink was built, found {sink:?}");
    };
    assert_eq!(table.catalog.connector(), ConnectorKind::IcebergRest);
}

#[test]
fn only_an_iceberg_sink_names_a_catalog_client_and_a_commit_policy() {
    for kind in EmitSinkKind::all() {
        let bytes = seeded_bytes(7);
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        let sink = arbitrary.emit_sink_of(kind);
        let iceberg = kind == EmitSinkKind::Iceberg(IcebergSinkEntity::Table);
        assert_eq!(sink.catalog_client().is_some(), iceberg, "{kind:?}");
        assert_eq!(sink.commit_policy().is_some(), iceberg, "{kind:?}");
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
fn an_otel_sink_names_its_signal_by_its_entity() {
    let metric = OtelMetric {
        name: "queue.depth".to_owned(),
        unit: "1".to_owned(),
        description: None,
        kind: OtelMetricKind::Gauge,
    };
    let logs = OtelSink::from(OtelLogsSink {
        client: named("collector"),
        mappings: otel_mappings(),
    });
    let traces = OtelSink::from(OtelTracesSink {
        client: named("collector"),
        mappings: otel_mappings(),
    });
    let metrics = OtelSink::from(OtelMetricsSink {
        client: named("collector"),
        metric: metric.clone(),
        mappings: otel_mappings(),
    });
    assert_eq!(logs.entity(), OtelSinkEntity::Logs);
    assert_eq!(traces.entity(), OtelSinkEntity::Traces);
    assert_eq!(metrics.entity(), OtelSinkEntity::Metrics);
    let OtelSink::Metrics(OtelMetricsSink {
        metric: declared, ..
    }) = &metrics
    else {
        panic!("a metrics sink was built, found {metrics:?}");
    };
    assert_eq!(declared, &metric);
    for sink in [&logs, &traces, &metrics] {
        assert_eq!(sink.client(), &named::<ClientName>("collector"));
        assert_eq!(sink.mappings(), &otel_mappings());
    }
}

#[test]
fn direct_value_mappings_cover_all_external_value_sections() {
    let client: ClientName = named("sink");
    let table: TableName = named("events");
    let mapping = |column: &str| ClickHouseValueMapping {
        column: column.to_owned(),
        expression: Expression::Literal(Literal::String(column.to_owned())),
    };
    let single_value = || vec![mapping("value")];
    let sinks = [
        EmitSink::from(OtelLogsSink {
            client: client.clone(),
            mappings: OtelMappings {
                values: single_value(),
                attributes: vec![mapping("attribute")],
                resource: vec![mapping("resource")],
                scope: None,
            },
        }),
        EmitSink::ClickHouse(ClickHouseSink::Table(ClickHouseTableSink {
            client: client.clone(),
            table: table.clone(),
            values: single_value(),
        })),
        EmitSink::Postgres(PostgresSink::Table(PostgresTableSink {
            client: client.clone(),
            table: table.clone(),
            values: single_value(),
            conflict_action: PostgresConflictAction::None,
        })),
        EmitSink::MySql(MySqlSink::Table(MySqlTableSink {
            client: client.clone(),
            table: table.clone(),
            values: single_value(),
            conflict_action: MySqlConflictAction::None,
        })),
        EmitSink::MongoDb(MongoDbSink::Collection(MongoDbCollectionSink {
            client: client.clone(),
            collection: named::<CollectionName>("events"),
            values: single_value(),
            conflict_action: MongoDbConflictAction::None,
        })),
        EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
            backend: IcebergStorageBackend::S3,
            client: client.clone(),
            table,
            values: single_value(),
            location: "s3://bucket/events".to_owned(),
            catalog: IcebergCatalog::Rest {
                client: client.clone(),
            },
            commit_each: "1s".to_owned(),
            max_commit_size: "1MB".to_owned(),
        })),
    ];

    for (index, sink) in sinks.iter().enumerate() {
        let columns = sink
            .direct_value_mappings()
            .map(|mapping| mapping.column.as_str())
            .collect::<Vec<_>>();
        let expected = if index == 0 {
            vec!["value", "attribute", "resource"]
        } else {
            vec!["value"]
        };
        assert_eq!(columns, expected, "{sink:?}");
    }

    let sink_without_values = EmitSink::Sentry(SentrySink::Event(SentryEventSink { client }));
    assert_eq!(sink_without_values.direct_value_mappings().count(), 0);
}

#[test]
fn two_sinks_are_one_definition_when_only_their_client_differs() {
    let kafka = |client: &str, topic: &str| {
        EmitSink::from(KafkaTopicSink {
            client: named(client),
            topic: named(topic),
        })
    };
    assert!(kafka("first", "orders").same_definition(&kafka("first", "orders")));
    assert!(kafka("first", "orders").same_definition(&kafka("second", "orders")));
    assert!(!kafka("first", "orders").same_definition(&kafka("first", "payments")));

    let pulsar = EmitSink::from(PulsarTopicSink {
        client: named("first"),
        topic: named("orders"),
    });
    assert!(!kafka("first", "orders").same_definition(&pulsar));

    let consumer = |schema: &str| {
        EmitSink::from(ClientConsumerSink {
            schema: named::<SchemaName>(schema),
        })
    };
    assert!(consumer("orders").same_definition(&consumer("orders")));
    assert!(!consumer("orders").same_definition(&consumer("payments")));
    assert!(!consumer("orders").same_definition(&kafka("first", "orders")));
    assert!(!kafka("first", "orders").same_definition(&consumer("orders")));

    let iceberg = |client: &str, catalog: &str| {
        let mut sink = iceberg_sink(IcebergStorageBackend::S3);
        sink.client = named(client);
        sink.catalog = IcebergCatalog::Rest {
            client: named(catalog),
        };
        EmitSink::from(sink)
    };
    assert!(iceberg("lake", "catalog").same_definition(&iceberg("other_lake", "catalog")));
    assert!(!iceberg("lake", "catalog").same_definition(&iceberg("lake", "other_catalog")));
}

/// A batching clause every sink's contract admits: small enough for the tightest destination.
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
fn naming_another_client_keeps_a_sink_of_every_pair_the_same_definition() {
    let other: ClientName = named("rebound");
    for kind in EmitSinkKind::all() {
        for seed in 0..4 {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let mut original = arbitrary.create_emitter_to(kind);
            // An alteration checks the batching contract again, which is not what this check is
            // about, so the emitter batches exactly as its sink requires.
            original.batch = match original.sink.batch_legality() {
                EmitterBatchLegality::Required { .. } => Some(admissible_batch()),
                EmitterBatchLegality::Optional { .. }
                | EmitterBatchLegality::Unavailable { .. } => None,
            };
            let alter = AlterEmitter {
                emitter: original.name.clone(),
                operations: vec![AlterEmitterOperation::SetClient {
                    client: other.clone(),
                }],
            };
            let mut rebound = original.clone();
            let outcome = rebound.apply_alter(&alter);

            if original.sink.client().is_none() {
                let error = outcome
                    .expect_err("a sink that serves session consumers publishes through no client");
                assert_eq!(
                    error.current_context(),
                    &AlterEmitterError::ClientUnsupported,
                    "{kind:?}"
                );
                assert_eq!(rebound, original, "{kind:?}");
                continue;
            }
            outcome.expect("a sink that publishes through a client can name another");
            assert_eq!(rebound.sink.kind(), kind);
            assert_eq!(rebound.sink.client(), Some(&other), "{kind:?}");
            assert!(original.sink.same_definition(&rebound.sink), "{kind:?}");
            assert!(rebound.sink.same_definition(&original.sink), "{kind:?}");
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
        IngestSource::from(KafkaTopicSource {
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
        MqttIngestMode::NoAck {
            session: MqttSession::Clean,
            qos: MqttQos::AtMostOnce,
        },
        MqttIngestMode::NoAck {
            session: MqttSession::Persistent,
            qos: MqttQos::AtLeastOnce,
        },
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
    let expected = [
        IngestAcknowledgement::Unacknowledged,
        IngestAcknowledgement::Unacknowledged,
        sequential,
        parallel,
    ];
    for (mode, expected) in mqtt_modes.into_iter().zip(expected) {
        let acknowledges = mode.is_ack();
        let source = IngestSource::from(MqttTopicSource {
            client: named("broker"),
            topic: named("orders/#"),
            instances: std::num::NonZeroU64::MIN,
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
    let source = RedisPubSubSource {
        client: named("cache"),
        channel: named("orders"),
        mode: RedisPubSubIngestMode::NoAck,
        quiesce: IngestQuiesceMode::Drop,
    };
    let nested = RedisSource::from(source.clone());
    assert_eq!(nested.entity(), RedisSourceEntity::PubSub);
    assert_eq!(
        IngestSource::from(source.clone()),
        IngestSource::from(nested)
    );
    assert_eq!(
        IngestSource::from(source).kind(),
        IngestSourceKind::from(RedisSourceEntity::PubSub)
    );
    assert_eq!(
        IngestSourceKind::from(ClientSourceEntity::Producer),
        IngestSourceKind::Client(ClientSourceEntity::Producer)
    );

    let sink = RedisPubSubSink {
        client: named("cache"),
        channel: named("orders"),
    };
    let nested = RedisSink::from(sink.clone());
    assert_eq!(nested.entity(), RedisSinkEntity::PubSub);
    assert_eq!(EmitSink::from(sink.clone()), EmitSink::from(nested));
    assert_eq!(
        EmitSink::from(sink).kind(),
        EmitSinkKind::from(RedisSinkEntity::PubSub)
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
    let mut source = IngestSource::Http(HttpSource::Endpoint(HttpEndpointSource {
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
            kind: IngestSourceKind::Http(HttpSourceEntity::Endpoint),
            mode: "SUSPEND",
        }
    );
    assert_eq!(
        error.to_string(),
        "HTTP ENDPOINT ingestors do not support ON QUIESCE SUSPEND"
    );
    assert_eq!(source.quiesce(), &buffer);
}
