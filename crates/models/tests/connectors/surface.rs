//! Every connector entity declares the public surface the vocabulary says it does.
//!
//! The tables below state, independently of the declarations, what a statement can say about each
//! entity: its delivery or publishing modes and their requirements, its quiesce modes, its locator,
//! its options, its reference, and for each client its pooling and the owner of its
//! configuration keys. The checks after them hold the declarations to their own rules: every
//! example writes its keywords, every requirement names a declared choice, and every generated
//! Model states only what its entity declares.

use std::num::NonZeroUsize;

use meticulous::OptionExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    ChoiceRequirement, ClickHouseSinkEntity, ClientConfigOwner, ClientPooling, ClientSinkEntity,
    ClientSourceEntity, ConnectorKind, EmitSinkKind, EmitterBatchLegality, EmitterBody,
    EmitterBodyForm, EmitterPublishingModeKind, EnvelopeClauseKind, ExternalName, HttpSinkEntity,
    HttpSourceEntity, IcebergSinkEntity, IcebergStorageBackend, IngestDeliveryKind,
    IngestQuiesceKind, IngestSourceKind, KafkaConsumerGroupName, KafkaSinkEntity,
    KafkaSourceEntity, KafkaTopicName, LocatorDeclaration, LocatorName, MongoDbSinkEntity,
    MqttSinkEntity, MqttSourceEntity, MqttTopicFilter, MySqlSinkEntity, NatsQueueGroupName,
    NatsSinkEntity, NatsSourceEntity, NatsSubjectName, OptionDeclaration, OptionForm,
    OptionPresence, OtelSinkEntity, PostgresSinkEntity, PrometheusSourceEntity, PulsarSinkEntity,
    PulsarSourceEntity, PulsarSubscriptionName, PulsarTopicName, RabbitMqQueueName,
    RabbitMqSinkEntity, RabbitMqSourceEntity, RedisChannelName, RedisSinkEntity, RedisSourceEntity,
    RuntimeCapability, SentrySinkEntity, SinkReferenceKind, SqsQueueName, SqsSinkEntity,
    SqsSourceEntity, SyslogSinkEntity, SyslogSourceEntity, WebsocketsSourceEntity,
    ZeroMqSinkEntity, ZeroMqSourceEntity,
};
use strum::IntoEnumIterator as _;

use super::seeded_bytes;

/// The delivery modes and the quiesce modes one source pair declares, as the table below states
/// them, each with whether it requires a choice written beside it.
struct SourceSurface {
    delivery: &'static [(IngestDeliveryKind, bool)],
    quiesce: &'static [(IngestQuiesceKind, bool)],
    locator: Option<(&'static str, &'static str)>,
    options: &'static [(&'static str, bool)],
}

const NO_ACK: &[(IngestDeliveryKind, bool)] = &[(IngestDeliveryKind::NoAck, false)];
const EVERY_DELIVERY: &[(IngestDeliveryKind, bool)] = &[
    (IngestDeliveryKind::NoAck, false),
    (IngestDeliveryKind::AckSequential, false),
    (IngestDeliveryKind::AckParallel, false),
];
const SUSPEND: &[(IngestQuiesceKind, bool)] = &[(IngestQuiesceKind::Suspend, false)];
const PUSH: &[(IngestQuiesceKind, bool)] = &[
    (IngestQuiesceKind::Buffer, false),
    (IngestQuiesceKind::Drop, false),
];
const POLLING: &[(IngestQuiesceKind, bool)] = &[
    (IngestQuiesceKind::Suspend, false),
    (IngestQuiesceKind::Buffer, false),
];
const FLEXIBLE: &[(IngestQuiesceKind, bool)] = &[
    (IngestQuiesceKind::Suspend, false),
    (IngestQuiesceKind::Buffer, false),
    (IngestQuiesceKind::Drop, false),
];
const ENDPOINT: &[(IngestQuiesceKind, bool)] = &[
    (IngestQuiesceKind::Reject, false),
    (IngestQuiesceKind::EndpointBuffer, false),
];

fn source_surface(kind: IngestSourceKind) -> SourceSurface {
    let unlocated = |delivery, quiesce, options| SourceSurface {
        delivery,
        quiesce,
        locator: None,
        options,
    };
    match kind {
        IngestSourceKind::Client(ClientSourceEntity::Producer) => unlocated(
            &[
                (IngestDeliveryKind::AckSequential, false),
                (IngestDeliveryKind::AckParallel, false),
            ],
            SUSPEND,
            &[],
        ),
        IngestSourceKind::Http(HttpSourceEntity::Request) => {
            unlocated(NO_ACK, POLLING, &[("EVERY", true)])
        }
        IngestSourceKind::Http(HttpSourceEntity::Endpoint)
        | IngestSourceKind::Websockets(WebsocketsSourceEntity::Endpoint) => unlocated(
            &[
                (IngestDeliveryKind::NoAck, false),
                (IngestDeliveryKind::AckRequest, false),
            ],
            ENDPOINT,
            &[],
        ),
        IngestSourceKind::Kafka(KafkaSourceEntity::Topic) => SourceSurface {
            delivery: EVERY_DELIVERY,
            quiesce: SUSPEND,
            locator: Some(("TOPIC", "kafka_topic")),
            options: &[("OFFSET BY", true), ("INSTANCES", false)],
        },
        IngestSourceKind::Pulsar(PulsarSourceEntity::Topic) => SourceSurface {
            delivery: EVERY_DELIVERY,
            quiesce: SUSPEND,
            locator: Some(("TOPIC", "pulsar_topic")),
            options: &[("SUBSCRIPTION", true), ("INSTANCES", false)],
        },
        IngestSourceKind::Mqtt(MqttSourceEntity::Topic) => SourceSurface {
            delivery: &[
                (IngestDeliveryKind::NoAck, false),
                (IngestDeliveryKind::AckSequential, true),
                (IngestDeliveryKind::AckParallel, true),
            ],
            quiesce: &[
                (IngestQuiesceKind::Suspend, true),
                (IngestQuiesceKind::Buffer, false),
                (IngestQuiesceKind::Drop, false),
            ],
            locator: Some(("TOPIC", "mqtt_topic_filter")),
            options: &[("INSTANCES", false), ("SESSION", false), ("QOS", false)],
        },
        IngestSourceKind::Nats(NatsSourceEntity::Subject) => SourceSurface {
            delivery: NO_ACK,
            quiesce: PUSH,
            locator: Some(("SUBJECT", "nats_subject")),
            options: &[("QUEUE GROUP", true), ("INSTANCES", true)],
        },
        IngestSourceKind::RabbitMq(RabbitMqSourceEntity::Queue) => SourceSurface {
            delivery: &[(IngestDeliveryKind::AckSequential, false)],
            quiesce: SUSPEND,
            locator: Some(("QUEUE", "rabbitmq_queue")),
            options: &[("INSTANCES", false)],
        },
        IngestSourceKind::Sqs(SqsSourceEntity::Queue) => SourceSurface {
            delivery: &[(IngestDeliveryKind::AckSequential, false)],
            quiesce: SUSPEND,
            locator: Some(("QUEUE", "sqs_queue")),
            options: &[("INSTANCES", false)],
        },
        IngestSourceKind::Redis(RedisSourceEntity::PubSub) => SourceSurface {
            delivery: NO_ACK,
            quiesce: PUSH,
            locator: Some(("CHANNEL", "redis_channel")),
            options: &[],
        },
        IngestSourceKind::Prometheus(PrometheusSourceEntity::Query) => unlocated(
            NO_ACK,
            POLLING,
            &[("QUERY", true), ("MAX BODY SIZE", false), ("EVERY", true)],
        ),
        IngestSourceKind::ZeroMq(ZeroMqSourceEntity::Pull)
        | IngestSourceKind::Syslog(SyslogSourceEntity::Listener) => {
            unlocated(NO_ACK, FLEXIBLE, &[])
        }
        IngestSourceKind::Websockets(WebsocketsSourceEntity::Connection) => {
            unlocated(NO_ACK, PUSH, &[])
        }
    }
}

/// Each option's keywords, and whether every statement writes it.
fn option_shapes(options: &[OptionDeclaration]) -> Vec<(&'static str, bool)> {
    let mut shapes = Vec::new();
    for option in options {
        shapes.push((option.keywords, option.is_required()));
    }
    shapes
}

/// A locator's keywords and the label completion offers for its name.
fn locator_shape(locator: Option<LocatorDeclaration>) -> Option<(&'static str, &'static str)> {
    let locator = locator?;
    let label = match locator.name {
        LocatorName::External(name) => name.label,
        LocatorName::Unquoted { label } => label,
    };
    Some((locator.keywords, label))
}

#[test]
fn every_source_pair_declares_its_modes_quiesce_modes_locator_and_options() {
    for kind in IngestSourceKind::all() {
        let expected = source_surface(kind);
        let declared = kind.capabilities();
        let mut delivery = Vec::new();
        for declared_delivery in declared.delivery {
            delivery.push((
                declared_delivery.kind,
                !declared_delivery.requires.is_empty(),
            ));
        }
        assert_eq!(delivery, expected.delivery, "{kind}");
        let mut quiesce = Vec::new();
        for declared_quiesce in declared.quiesce {
            quiesce.push((declared_quiesce.kind, !declared_quiesce.requires.is_empty()));
        }
        assert_eq!(quiesce, expected.quiesce, "{kind}");
        assert_eq!(locator_shape(declared.locator), expected.locator, "{kind}");
        assert_eq!(option_shapes(declared.options), expected.options, "{kind}");
    }
}

/// The publishing modes, reference, locator and options one sink pair declares.
struct SinkSurface {
    reference: SinkReferenceKind,
    locator: Option<(&'static str, &'static str)>,
    options: &'static [(&'static str, bool)],
}

fn sink_surface(kind: EmitSinkKind) -> SinkSurface {
    let client = |connector| SinkReferenceKind::Client(connector);
    let located = |connector, keywords, label, options| SinkSurface {
        reference: client(connector),
        locator: Some((keywords, label)),
        options,
    };
    let unlocated = |connector, options| SinkSurface {
        reference: client(connector),
        locator: None,
        options,
    };
    let otel = &[("ATTRIBUTES", false), ("RESOURCE", false), ("SCOPE", false)];
    let conflict = &[("ON CONFLICT", false)];
    match kind {
        EmitSinkKind::Client(ClientSinkEntity::Consumer) => SinkSurface {
            reference: SinkReferenceKind::Schema,
            locator: None,
            options: &[],
        },
        EmitSinkKind::Http(HttpSinkEntity::Request) => {
            unlocated(ConnectorKind::Http, &[("METHOD", true), ("PATH", true)])
        }
        EmitSinkKind::Kafka(KafkaSinkEntity::Topic) => {
            located(ConnectorKind::Kafka, "TOPIC", "topic_name", &[])
        }
        EmitSinkKind::Pulsar(PulsarSinkEntity::Topic) => {
            located(ConnectorKind::Pulsar, "TOPIC", "topic_name", &[])
        }
        EmitSinkKind::Mqtt(MqttSinkEntity::Topic) => {
            located(ConnectorKind::Mqtt, "TOPIC", "topic_name", &[])
        }
        EmitSinkKind::RabbitMq(RabbitMqSinkEntity::Queue) => {
            located(ConnectorKind::RabbitMq, "QUEUE", "queue_name", &[])
        }
        EmitSinkKind::Redis(RedisSinkEntity::PubSub) => {
            located(ConnectorKind::Redis, "CHANNEL", "channel_name", &[])
        }
        EmitSinkKind::Nats(NatsSinkEntity::Subject) => {
            located(ConnectorKind::Nats, "SUBJECT", "subject_name", &[])
        }
        EmitSinkKind::Sqs(SqsSinkEntity::Queue) => located(
            ConnectorKind::Sqs,
            "QUEUE",
            "queue_name",
            &[("FIFO GROUP", false)],
        ),
        EmitSinkKind::ZeroMq(ZeroMqSinkEntity::Push) => unlocated(ConnectorKind::ZeroMq, &[]),
        EmitSinkKind::Sentry(SentrySinkEntity::Event) => unlocated(ConnectorKind::Sentry, &[]),
        EmitSinkKind::Syslog(SyslogSinkEntity::Collector) => unlocated(ConnectorKind::Syslog, &[]),
        EmitSinkKind::Otel(OtelSinkEntity::Logs | OtelSinkEntity::Traces) => {
            unlocated(ConnectorKind::Otel, otel)
        }
        EmitSinkKind::Otel(OtelSinkEntity::Metrics) => unlocated(
            ConnectorKind::Otel,
            &[
                ("METRIC", true),
                ("ATTRIBUTES", false),
                ("RESOURCE", false),
                ("SCOPE", false),
            ],
        ),
        EmitSinkKind::ClickHouse(ClickHouseSinkEntity::Table) => located(
            ConnectorKind::ClickHouse,
            "INSERT TO TABLE",
            "table_name",
            &[],
        ),
        EmitSinkKind::Postgres(PostgresSinkEntity::Table) => located(
            ConnectorKind::Postgres,
            "INSERT TO TABLE",
            "table_name",
            conflict,
        ),
        EmitSinkKind::MySql(MySqlSinkEntity::Table) => located(
            ConnectorKind::MySql,
            "INSERT TO TABLE",
            "table_name",
            conflict,
        ),
        EmitSinkKind::MongoDb(MongoDbSinkEntity::Collection) => located(
            ConnectorKind::MongoDb,
            "INSERT TO COLLECTION",
            "collection_name",
            conflict,
        ),
        EmitSinkKind::Iceberg(IcebergSinkEntity::Table) => SinkSurface {
            reference: SinkReferenceKind::StorageClient(&[
                ConnectorKind::S3,
                ConnectorKind::Gcs,
                ConnectorKind::AzureBlob,
            ]),
            locator: Some(("TABLE", "table_name")),
            options: &[("LOCATION", true), ("CATALOG", true), ("COMMIT EACH", true)],
        },
    }
}

#[test]
fn every_sink_pair_declares_its_reference_locator_and_options_and_no_envelope_yet() {
    for kind in EmitSinkKind::all() {
        let expected = sink_surface(kind);
        let declared = kind.capabilities();
        assert_eq!(declared.reference, expected.reference, "{kind}");
        assert_eq!(locator_shape(declared.locator), expected.locator, "{kind}");
        assert_eq!(option_shapes(declared.options), expected.options, "{kind}");
        assert!(declared.envelope.is_empty(), "{kind}");
        assert!(declared.properties.is_empty(), "{kind}");
    }
}

#[test]
fn an_iceberg_sink_declares_every_object_store_its_backend_names() {
    let declared = EmitSinkKind::Iceberg(IcebergSinkEntity::Table).capabilities();
    for backend in [
        IcebergStorageBackend::S3,
        IcebergStorageBackend::Gcs,
        IcebergStorageBackend::AzureBlob,
    ] {
        assert!(
            declared.reference.admits_client(backend.connector()),
            "{backend:?}"
        );
    }
    assert!(!declared.reference.admits_client(ConnectorKind::IcebergRest));
}

/// Checks that one external name type declares a label and an example its own rule admits.
fn assert_declares_an_admitted_example<N: ExternalName>(label: &str) {
    let declaration = N::DECLARATION;
    assert_eq!(declaration.label, label);
    assert!(
        N::parse(declaration.example).is_ok(),
        "{label} refuses its own example {:?}",
        declaration.example
    );
}

#[test]
fn every_external_name_declares_its_label_and_an_example_its_rule_admits() {
    assert_declares_an_admitted_example::<KafkaTopicName>("kafka_topic");
    assert_declares_an_admitted_example::<KafkaConsumerGroupName>("kafka_consumer_group");
    assert_declares_an_admitted_example::<PulsarTopicName>("pulsar_topic");
    assert_declares_an_admitted_example::<PulsarSubscriptionName>("pulsar_subscription");
    assert_declares_an_admitted_example::<RabbitMqQueueName>("rabbitmq_queue");
    assert_declares_an_admitted_example::<SqsQueueName>("sqs_queue");
    assert_declares_an_admitted_example::<RedisChannelName>("redis_channel");
    assert_declares_an_admitted_example::<NatsSubjectName>("nats_subject");
    assert_declares_an_admitted_example::<NatsQueueGroupName>("nats_queue_group");
    assert_declares_an_admitted_example::<MqttTopicFilter>("mqtt_topic_filter");
}

/// Every option an entity of either side declares.
fn every_option() -> Vec<(String, OptionDeclaration)> {
    let mut options = Vec::new();
    for kind in IngestSourceKind::all() {
        for option in kind.capabilities().options {
            options.push((format!("{kind} INGESTOR"), *option));
        }
    }
    for kind in EmitSinkKind::all() {
        for option in kind.capabilities().options {
            options.push((format!("{kind} EMITTER"), *option));
        }
    }
    for connector in ConnectorKind::iter() {
        let Some(client) = connector.client() else {
            continue;
        };
        for option in client.options {
            options.push((format!("{connector} CLIENT"), *option));
        }
    }
    options
}

#[test]
fn every_option_example_writes_its_keywords_and_every_default_is_a_declared_choice() {
    for (entity, option) in every_option() {
        let examples = option.examples();
        assert!(!examples.is_empty(), "{entity} {}", option.keywords);
        for example in examples {
            assert!(
                example.starts_with(&format!("{} ", option.keywords)),
                "{entity}: {example} does not open with {}",
                option.keywords
            );
        }
        let OptionPresence::Optional {
            default: Some(default),
        } = option.presence
        else {
            continue;
        };
        match option.form {
            OptionForm::Choice(_) => assert!(
                option.choice(default).is_some(),
                "{entity}: {} defaults to {default}, which it does not offer",
                option.keywords
            ),
            OptionForm::Value { .. } => assert_eq!(
                option.choice(default),
                None,
                "{entity}: {} takes a value and offers no choices",
                option.keywords
            ),
        }
    }
}

#[test]
fn every_requirement_names_a_choice_its_entity_declares() {
    for kind in IngestSourceKind::all() {
        let declared = kind.capabilities();
        let mut requirements: Vec<ChoiceRequirement> = Vec::new();
        for delivery in declared.delivery {
            requirements.extend_from_slice(delivery.requires);
        }
        for quiesce in declared.quiesce {
            requirements.extend_from_slice(quiesce.requires);
        }
        for requirement in requirements {
            let option = declared
                .option(requirement.option)
                .unwrap_or_else(|| panic!("{kind} declares no {}", requirement.option));
            assert!(
                option.choice(requirement.choice).is_some(),
                "{kind}: {} offers no {}",
                requirement.option,
                requirement.choice
            );
        }
    }
}

#[test]
fn every_delivery_and_publishing_mode_kind_is_declared_at_most_once() {
    for kind in IngestSourceKind::all() {
        let declared = kind.capabilities();
        for delivery in IngestDeliveryKind::iter() {
            let mut count = 0;
            for declared_delivery in declared.delivery {
                if declared_delivery.kind == delivery {
                    count += 1;
                }
            }
            assert!(count <= 1, "{kind} declares {delivery:?} {count} times");
        }
        for quiesce in IngestQuiesceKind::iter() {
            let mut count = 0;
            for declared_quiesce in declared.quiesce {
                if declared_quiesce.kind == quiesce {
                    count += 1;
                }
            }
            assert!(count <= 1, "{kind} declares {quiesce:?} {count} times");
        }
    }
    for kind in EmitSinkKind::all() {
        let declared = kind.capabilities();
        for mode in EmitterPublishingModeKind::iter() {
            let mut count = 0;
            for declared_mode in declared.publishing_modes {
                if *declared_mode == mode {
                    count += 1;
                }
            }
            assert!(count <= 1, "{kind} declares {mode:?} {count} times");
        }
        for clause in EnvelopeClauseKind::iter() {
            let mut count = 0;
            for declared_clause in declared.envelope {
                if *declared_clause == clause {
                    count += 1;
                }
            }
            assert!(count <= 1, "{kind} declares {clause:?} {count} times");
        }
    }
}

#[test]
fn every_client_declares_its_pooling_and_the_owner_of_its_configuration_keys() {
    for connector in ConnectorKind::iter() {
        let expected = match connector {
            ConnectorKind::Client | ConnectorKind::Iceberg => None,
            ConnectorKind::Kafka => Some((ClientPooling::Unpooled, ClientConfigOwner::Driver)),
            ConnectorKind::S3
            | ConnectorKind::Gcs
            | ConnectorKind::AzureBlob
            | ConnectorKind::IcebergRest => Some((
                ClientPooling::Unpooled,
                ClientConfigOwner::NervixWithPropertyNamespaces,
            )),
            ConnectorKind::Postgres
            | ConnectorKind::MySql
            | ConnectorKind::MongoDb
            | ConnectorKind::Redis => Some((
                ClientPooling::Pooled,
                ClientConfigOwner::NervixWithAddressQuery,
            )),
            ConnectorKind::RabbitMq | ConnectorKind::Nats => Some((
                ClientPooling::Unpooled,
                ClientConfigOwner::NervixWithAddressQuery,
            )),
            ConnectorKind::Http
            | ConnectorKind::Websockets
            | ConnectorKind::Pulsar
            | ConnectorKind::Mqtt
            | ConnectorKind::ZeroMq
            | ConnectorKind::Sqs
            | ConnectorKind::Syslog
            | ConnectorKind::Prometheus
            | ConnectorKind::Sentry
            | ConnectorKind::Otel
            | ConnectorKind::ClickHouse => {
                Some((ClientPooling::Unpooled, ClientConfigOwner::Nervix))
            }
        };
        let declared = connector
            .client()
            .map(|client| (client.pooling, client.config));
        assert_eq!(declared, expected, "{connector}");
    }
    let websockets = ConnectorKind::Websockets
        .client()
        .assured("a WebSockets client is a configured client");
    assert_eq!(
        option_shapes(websockets.options),
        [("WITH SIGNALING PROTOCOL", false)]
    );
}

#[test]
fn the_runtime_capabilities_of_a_pair_are_its_modes_options_metadata_batching_and_stalled_peers() {
    let named = |capabilities: Vec<RuntimeCapability>| {
        capabilities
            .into_iter()
            .map(|capability| capability.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        named(
            IngestSourceKind::Kafka(KafkaSourceEntity::Topic)
                .capabilities()
                .runtime_capabilities()
        ),
        [
            "MODE NO ACK",
            "MODE ACK SEQUENTIAL",
            "MODE ACK PARALLEL",
            "OFFSET BY DOMAIN",
            "OFFSET BY CONSUMER GROUP",
            "INSTANCES",
            "metadata.topic",
            "metadata.partition",
            "metadata.offset",
        ]
    );
    assert_eq!(
        named(
            EmitSinkKind::Sqs(SqsSinkEntity::Queue)
                .capabilities()
                .runtime_capabilities()
        ),
        [
            "MODE SINGLE",
            "MODE BATCH",
            "FIFO GROUP <expression>",
            "FIFO GROUP FROM BRANCH",
            "BATCH",
            "STALLED PEER timeout_ms",
        ]
    );
    assert_eq!(
        named(
            EmitSinkKind::Http(HttpSinkEntity::Request)
                .capabilities()
                .runtime_capabilities()
        ),
        ["MODE ACK", "METHOD", "PATH", "STALLED PEER timeout_ms"]
    );
}

#[test]
fn an_envelope_clause_and_a_property_are_named_as_a_statement_writes_them() {
    for clause in EnvelopeClauseKind::iter() {
        assert_eq!(
            RuntimeCapability::Envelope(clause).to_string(),
            clause.keywords()
        );
        let example = clause.example();
        let written =
            example == clause.keywords() || example.starts_with(&format!("{} ", clause.keywords()));
        assert!(written, "{clause:?} is written as {example}");
    }
    let property = RuntimeCapability::Property { name: "priority" };
    assert_eq!(property.to_string(), "PROPERTIES priority");
}

#[test]
fn an_mqtt_source_answers_the_session_and_quality_of_service_it_subscribes_with() {
    let kind = IngestSourceKind::Mqtt(MqttSourceEntity::Topic);
    for seed in 0..8 {
        let bytes = seeded_bytes(seed);
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        let source = arbitrary
            .ingest_source_of(kind)
            .assured("every modeled pair has a transport Model");
        let session = source.chosen("SESSION").assured("MQTT answers its session");
        let qos = source
            .chosen("QOS")
            .assured("MQTT answers its quality of service");
        assert!(matches!(session, "CLEAN" | "PERSISTENT"), "{session}");
        assert!(matches!(qos, "0" | "1"), "{qos}");
        assert_eq!(source.chosen("INSTANCES"), None);
    }
    let kafka = IngestSourceKind::Kafka(KafkaSourceEntity::Topic);
    let bytes = seeded_bytes(1);
    let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
    let source = arbitrary
        .ingest_source_of(kafka)
        .assured("every modeled pair has a transport Model");
    assert_eq!(source.chosen("SESSION"), None);
}

/// Checks that a generated ingestor and a generated emitter state only what their entities
/// declare.
///
/// Each reads its own half of the input. A generated ingestor can read every byte it is given,
/// and once the bytes run out every choice takes its first option, so an emitter drawn after it
/// from the same bytes would always publish to the first sink.
fn assert_generated_entities_state_what_they_declare(bytes: &[u8]) {
    let (ingestor_bytes, emitter_bytes) = bytes.split_at(bytes.len() / 2);
    assert_generated_ingestor_states_what_its_entity_declares(ingestor_bytes);
    assert_generated_emitter_states_what_its_entity_declares(emitter_bytes);
}

/// Checks that a generated ingestor names a declared mode whose requirements it meets, and a
/// quiesce mode its entity honors.
fn assert_generated_ingestor_states_what_its_entity_declares(bytes: &[u8]) {
    let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
    let sources = IngestSourceKind::all().collect::<Vec<_>>();
    let count = NonZeroUsize::new(sources.len()).assured("the vocabulary declares sources");
    let source_kind = sources[arbitrary.entropy().index(count)];
    let ingestor = arbitrary.create_ingestor_from(source_kind);
    let declared = source_kind.capabilities();
    if let Some(source) = ingestor.input.transport_source() {
        let delivery = declared
            .declared_delivery(source.acknowledgement().kind())
            .unwrap_or_else(|| panic!("{source_kind} names an undeclared mode: {source:?}"));
        for requirement in delivery.requires {
            assert_eq!(
                source.chosen(requirement.option),
                Some(requirement.choice),
                "{source_kind} names a mode without its requirements: {source:?}"
            );
        }
    }
    assert!(
        ingestor.input.supports_quiesce(ingestor.input.quiesce()),
        "{source_kind} quiesces in a mode it does not honor: {:?}",
        ingestor.input.quiesce()
    );
}

/// Checks that a generated emitter names a declared publishing mode, writes a body of the
/// declared form and batching its declared legality admits, and publishes through a client of a
/// declared connector.
fn assert_generated_emitter_states_what_its_entity_declares(bytes: &[u8]) {
    let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
    let sinks = EmitSinkKind::all().collect::<Vec<_>>();
    let count = NonZeroUsize::new(sinks.len()).assured("the vocabulary declares sinks");
    let sink_kind = sinks[arbitrary.entropy().index(count)];
    let emitter = arbitrary.create_emitter_to(sink_kind);
    let declared = sink_kind.capabilities();
    assert!(
        declared.accepts_publishing_mode_kind(emitter.publishing_mode.kind()),
        "{sink_kind} names an undeclared mode: {:?}",
        emitter.publishing_mode
    );
    let formed = match (declared.body, &emitter.body) {
        (EmitterBodyForm::Codec, EmitterBody::Codec { .. })
        | (EmitterBodyForm::CodecOrWithoutBody, EmitterBody::Codec { .. })
        | (EmitterBodyForm::CodecOrWithoutBody, EmitterBody::WithoutBody)
        | (EmitterBodyForm::Values, EmitterBody::Values)
        | (EmitterBodyForm::Native, EmitterBody::Client) => true,
        (
            EmitterBodyForm::Codec
            | EmitterBodyForm::CodecOrWithoutBody
            | EmitterBodyForm::Values
            | EmitterBodyForm::Native,
            _,
        ) => false,
    };
    assert!(formed, "{sink_kind} writes {:?}", emitter.body);
    let batching = match (declared.batch, &emitter.batch) {
        (EmitterBatchLegality::Required { .. }, Some(_))
        | (EmitterBatchLegality::Optional { .. }, _)
        | (EmitterBatchLegality::Unavailable { .. }, None) => true,
        (EmitterBatchLegality::Required { .. }, None)
        | (EmitterBatchLegality::Unavailable { .. }, Some(_)) => false,
    };
    assert!(batching, "{sink_kind} batches as {:?}", emitter.batch);
    match emitter.sink.client_connector() {
        Some(connector) => assert!(
            declared.reference.admits_client(connector),
            "{sink_kind} publishes through a {connector} client"
        ),
        None => assert_eq!(declared.reference, SinkReferenceKind::Schema, "{sink_kind}"),
    }
}

#[test]
fn bolero_generated_entities_state_only_what_their_entity_declares() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(assert_generated_entities_state_what_they_declare);
}
