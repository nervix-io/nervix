//! Every connector entity declares the public surface the vocabulary says it does.
//!
//! The tables below state, independently of the declarations, what a statement can say about each
//! entity: its delivery or publishing modes and their requirements, its quiesce modes, its locator,
//! its options, its reference, and for each client its pooling and the owner of its
//! configuration keys. The checks after them hold the declarations to their own rules: every
//! example writes its keywords, every requirement names a declared choice, and every generated
//! Model states only what its entity declares.

use std::{collections::BTreeSet, num::NonZeroUsize};

use meticulous::OptionExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    ChoiceRequirement, ClickHouseEmitterEntity, ClientConfigReaders, ClientEmitterEntity,
    ClientIngestorEntity, ClientPooling, ConnectorKind, EmitterBatchLegality, EmitterBody,
    EmitterBodyForm, EmitterEntityKind, EmitterPublishingModeKind, EmitterReferenceKind,
    EnvelopeClauseKind, ExternalName, HttpEmitterEntity, HttpIngestorEntity, IcebergEmitterEntity,
    IcebergStorageBackend, IngestDeliveryKind, IngestQuiesceKind, IngestorEntityKind,
    IngestorReferenceKind, KafkaConsumerGroupName, KafkaEmitterEntity, KafkaIngestorEntity,
    KafkaTopicName, LocatorDeclaration, LocatorName, MongoDbEmitterEntity, MqttEmitterEntity,
    MqttIngestorEntity, MqttTopicFilter, MySqlEmitterEntity, NatsEmitterEntity, NatsIngestorEntity,
    NatsQueueGroupName, NatsSubjectName, OptionDeclaration, OptionForm, OptionPresence,
    OtelEmitterEntity, PostgresEmitterEntity, PrometheusIngestorEntity, PulsarEmitterEntity,
    PulsarIngestorEntity, PulsarSubscriptionName, PulsarTopicName, RabbitMqEmitterEntity,
    RabbitMqExchangeName, RabbitMqIngestorEntity, RabbitMqQueueName, RedisChannelName,
    RedisEmitterEntity, RedisIngestorEntity, RuntimeCapability, SentryEmitterEntity,
    SqsEmitterEntity, SqsIngestorEntity, SqsQueueName, SyslogEmitterEntity, SyslogIngestorEntity,
    WebsocketsIngestorEntity, ZeroMqEmitterEntity, ZeroMqIngestorEntity,
};
use strum::IntoEnumIterator as _;

use super::seeded_bytes;

/// The delivery modes and the quiesce modes one source pair declares, as the table below states
/// them, each with whether it requires a choice written beside it.
struct IngestorSurface {
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

fn source_surface(kind: IngestorEntityKind) -> IngestorSurface {
    let unlocated = |delivery, quiesce, options| IngestorSurface {
        delivery,
        quiesce,
        locator: None,
        options,
    };
    match kind {
        IngestorEntityKind::Client(ClientIngestorEntity::Producer) => unlocated(
            &[
                (IngestDeliveryKind::AckSequential, false),
                (IngestDeliveryKind::AckParallel, false),
            ],
            SUSPEND,
            &[],
        ),
        IngestorEntityKind::Http(HttpIngestorEntity::Request) => unlocated(
            NO_ACK,
            POLLING,
            &[
                ("METHOD", true),
                ("HEADERS", false),
                ("MAX BODY SIZE", false),
                ("EVERY", true),
            ],
        ),
        IngestorEntityKind::Http(HttpIngestorEntity::Endpoint)
        | IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint) => unlocated(
            &[
                (IngestDeliveryKind::NoAck, false),
                (IngestDeliveryKind::AckRequest, false),
            ],
            ENDPOINT,
            &[],
        ),
        IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic) => IngestorSurface {
            delivery: EVERY_DELIVERY,
            quiesce: SUSPEND,
            locator: Some(("TOPIC", "kafka_topic")),
            options: &[("OFFSET BY", true), ("INSTANCES", false)],
        },
        IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic) => IngestorSurface {
            delivery: EVERY_DELIVERY,
            quiesce: SUSPEND,
            locator: Some(("TOPIC", "pulsar_topic")),
            options: &[("SUBSCRIPTION", true), ("INSTANCES", false)],
        },
        IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => IngestorSurface {
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
            options: &[("INSTANCES", false), ("SESSION", true), ("QOS", true)],
        },
        IngestorEntityKind::Nats(NatsIngestorEntity::Subject) => IngestorSurface {
            delivery: NO_ACK,
            quiesce: PUSH,
            locator: Some(("SUBJECT", "nats_subject")),
            options: &[("QUEUE GROUP", true), ("INSTANCES", true)],
        },
        IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue) => IngestorSurface {
            delivery: &[
                (IngestDeliveryKind::NoAck, false),
                (IngestDeliveryKind::AckSequential, false),
                (IngestDeliveryKind::AckParallel, false),
            ],
            quiesce: SUSPEND,
            locator: Some(("QUEUE", "rabbitmq_queue")),
            options: &[("INSTANCES", false)],
        },
        IngestorEntityKind::Sqs(SqsIngestorEntity::Queue) => IngestorSurface {
            delivery: &[(IngestDeliveryKind::AckSequential, false)],
            quiesce: SUSPEND,
            locator: Some(("QUEUE", "sqs_queue")),
            options: &[("INSTANCES", false)],
        },
        IngestorEntityKind::Redis(RedisIngestorEntity::PubSub) => IngestorSurface {
            delivery: NO_ACK,
            quiesce: PUSH,
            locator: Some(("CHANNEL", "redis_channel")),
            options: &[],
        },
        IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query) => unlocated(
            NO_ACK,
            POLLING,
            &[("QUERY", true), ("MAX BODY SIZE", false), ("EVERY", true)],
        ),
        IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull)
        | IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener) => {
            unlocated(NO_ACK, FLEXIBLE, &[])
        }
        IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection) => {
            unlocated(NO_ACK, PUSH, &[("HEADERS", false)])
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
    for kind in IngestorEntityKind::all() {
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

/// The reference, locator, options and envelope clauses one emitter pair declares.
struct EmitterSurface {
    reference: EmitterReferenceKind,
    locator: Option<(&'static str, &'static str)>,
    options: &'static [(&'static str, bool)],
    envelope: &'static [EnvelopeClauseKind],
}

fn emitter_surface(kind: EmitterEntityKind) -> EmitterSurface {
    let client = |connector| EmitterReferenceKind::Client(connector);
    let located = |connector, keywords, label, options| EmitterSurface {
        reference: client(connector),
        locator: Some((keywords, label)),
        options,
        envelope: &[],
    };
    let unlocated = |connector, options| EmitterSurface {
        reference: client(connector),
        locator: None,
        options,
        envelope: &[],
    };
    let otel = &[("ATTRIBUTES", false), ("RESOURCE", false), ("SCOPE", false)];
    let conflict = &[("ON CONFLICT", false)];
    // Both RabbitMQ entities publish through the same channel, so either can ask the broker to
    // return a message no queue takes.
    let mandatory = &[EnvelopeClauseKind::Mandatory];
    match kind {
        EmitterEntityKind::Client(ClientEmitterEntity::Consumer) => EmitterSurface {
            reference: EmitterReferenceKind::Schema,
            locator: None,
            options: &[],
            envelope: &[],
        },
        EmitterEntityKind::Http(HttpEmitterEntity::Request) => {
            unlocated(ConnectorKind::Http, &[("METHOD", true), ("PATH", true)])
        }
        EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic) => EmitterSurface {
            envelope: &[EnvelopeClauseKind::Timestamp],
            ..located(ConnectorKind::Kafka, "TOPIC", "kafka_topic", &[])
        },
        EmitterEntityKind::Pulsar(PulsarEmitterEntity::Topic) => {
            located(ConnectorKind::Pulsar, "TOPIC", "pulsar_topic", &[])
        }
        EmitterEntityKind::Mqtt(MqttEmitterEntity::Topic) => {
            located(ConnectorKind::Mqtt, "TOPIC", "mqtt_topic", &[])
        }
        EmitterEntityKind::RabbitMq(RabbitMqEmitterEntity::Queue) => EmitterSurface {
            envelope: mandatory,
            ..located(ConnectorKind::RabbitMq, "QUEUE", "rabbitmq_queue", &[])
        },
        EmitterEntityKind::RabbitMq(RabbitMqEmitterEntity::Exchange) => EmitterSurface {
            envelope: mandatory,
            ..located(
                ConnectorKind::RabbitMq,
                "EXCHANGE",
                "rabbitmq_exchange",
                &[("ROUTING KEY", true)],
            )
        },
        EmitterEntityKind::Redis(RedisEmitterEntity::PubSub) => {
            located(ConnectorKind::Redis, "CHANNEL", "redis_channel", &[])
        }
        EmitterEntityKind::Nats(NatsEmitterEntity::Subject) => {
            located(ConnectorKind::Nats, "SUBJECT", "nats_publish_subject", &[])
        }
        EmitterEntityKind::Sqs(SqsEmitterEntity::Queue) => EmitterSurface {
            envelope: &[
                EnvelopeClauseKind::Delay,
                EnvelopeClauseKind::DeduplicationId,
            ],
            ..located(
                ConnectorKind::Sqs,
                "QUEUE",
                "sqs_queue",
                &[("FIFO GROUP", false), ("SEND", true)],
            )
        },
        EmitterEntityKind::ZeroMq(ZeroMqEmitterEntity::Push) => {
            unlocated(ConnectorKind::ZeroMq, &[])
        }
        EmitterEntityKind::Sentry(SentryEmitterEntity::Event) => {
            unlocated(ConnectorKind::Sentry, &[("PLATFORM", false)])
        }
        EmitterEntityKind::Syslog(SyslogEmitterEntity::Collector) => {
            unlocated(ConnectorKind::Syslog, &[])
        }
        EmitterEntityKind::Otel(OtelEmitterEntity::Logs | OtelEmitterEntity::Traces) => {
            unlocated(ConnectorKind::Otel, otel)
        }
        EmitterEntityKind::Otel(OtelEmitterEntity::Metrics) => unlocated(
            ConnectorKind::Otel,
            &[
                ("METRIC", true),
                ("ATTRIBUTES", false),
                ("RESOURCE", false),
                ("SCOPE", false),
            ],
        ),
        EmitterEntityKind::ClickHouse(ClickHouseEmitterEntity::Table) => {
            located(ConnectorKind::ClickHouse, "TABLE", "clickhouse_table", &[])
        }
        EmitterEntityKind::Postgres(PostgresEmitterEntity::Table) => {
            located(ConnectorKind::Postgres, "TABLE", "postgres_table", conflict)
        }
        EmitterEntityKind::MySql(MySqlEmitterEntity::Table) => {
            located(ConnectorKind::MySql, "TABLE", "mysql_table", conflict)
        }
        EmitterEntityKind::MongoDb(MongoDbEmitterEntity::Collection) => located(
            ConnectorKind::MongoDb,
            "COLLECTION",
            "mongodb_collection",
            conflict,
        ),
        EmitterEntityKind::Iceberg(IcebergEmitterEntity::Table) => EmitterSurface {
            reference: EmitterReferenceKind::StorageClient(&[
                ConnectorKind::S3,
                ConnectorKind::Gcs,
                ConnectorKind::AzureBlob,
            ]),
            locator: Some(("TABLE", "iceberg_table")),
            options: &[
                ("NAMESPACE", true),
                ("LOCATION", true),
                ("CATALOG", true),
                ("COMMIT EACH", true),
            ],
            envelope: &[],
        },
    }
}

#[test]
fn every_emitter_pair_declares_its_reference_locator_options_and_envelope_clauses() {
    for kind in EmitterEntityKind::all() {
        let expected = emitter_surface(kind);
        let declared = kind.capabilities();
        assert_eq!(declared.reference, expected.reference, "{kind}");
        assert_eq!(locator_shape(declared.locator), expected.locator, "{kind}");
        assert_eq!(option_shapes(declared.options), expected.options, "{kind}");
        assert_eq!(declared.envelope, expected.envelope, "{kind}");
        // No entity declares an envelope property yet.
        assert!(declared.properties.is_empty(), "{kind}");
    }
}

/// A record emitter carries each record's envelope, and no other emitter contract has one, so only
/// an entity whose records a codec encodes may declare envelope clauses or properties.
#[test]
fn only_an_entity_whose_records_a_codec_encodes_declares_an_envelope() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        if declared.envelope.is_empty() && declared.properties.is_empty() {
            continue;
        }
        assert_eq!(
            declared.body,
            EmitterBodyForm::Codec,
            "{kind} declares an envelope its records do not carry"
        );
    }
}

#[test]
fn an_iceberg_emitter_declares_every_object_store_its_backend_names() {
    let declared = EmitterEntityKind::Iceberg(IcebergEmitterEntity::Table).capabilities();
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
    assert_declares_an_admitted_example::<RabbitMqExchangeName>("rabbitmq_exchange");
    assert_declares_an_admitted_example::<SqsQueueName>("sqs_queue");
    assert_declares_an_admitted_example::<RedisChannelName>("redis_channel");
    assert_declares_an_admitted_example::<nervix_models::RedisChannelPattern>(
        "redis_channel_pattern",
    );
    assert_declares_an_admitted_example::<NatsSubjectName>("nats_subject");
    assert_declares_an_admitted_example::<NatsQueueGroupName>("nats_queue_group");
    assert_declares_an_admitted_example::<MqttTopicFilter>("mqtt_topic_filter");
}

/// Every option an entity of either side declares.
fn every_option() -> Vec<(String, OptionDeclaration)> {
    let mut options = Vec::new();
    for kind in IngestorEntityKind::all() {
        for option in kind.capabilities().options {
            options.push((format!("{kind} INGESTOR"), *option));
        }
    }
    for kind in EmitterEntityKind::all() {
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
    for kind in IngestorEntityKind::all() {
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
                !requirement.choices.is_empty(),
                "{kind}: a requirement of {} accepts no choice",
                requirement.option
            );
            for choice in requirement.choices {
                assert!(
                    option.choice(choice).is_some(),
                    "{kind}: {} offers no {choice}",
                    requirement.option
                );
            }
        }
    }
}

#[test]
fn every_delivery_and_publishing_mode_kind_is_declared_at_most_once() {
    for kind in IngestorEntityKind::all() {
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
    for kind in EmitterEntityKind::all() {
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
fn every_client_declares_its_pooling_and_its_current_key_schema() {
    for connector in ConnectorKind::iter() {
        let expected_pool = match connector {
            ConnectorKind::Client | ConnectorKind::Iceberg => None,
            ConnectorKind::Postgres
            | ConnectorKind::MySql
            | ConnectorKind::MongoDb
            | ConnectorKind::Redis => Some(ClientPooling::Pooled),
            _ => Some(ClientPooling::Unpooled),
        };
        assert_eq!(
            connector.client().map(|client| client.pooling),
            expected_pool,
            "{connector}"
        );
        if let Some(client) = connector.client() {
            assert!(
                !client.config.keys().is_empty(),
                "{connector} declares its current keys"
            );
            for key in client.config.keys() {
                assert_eq!(client.config.key(key.name), Some(key));
                assert_eq!(key.name.to_ascii_lowercase(), key.name);
            }
        }
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
fn only_a_client_both_sides_reference_declares_a_key_for_one_side() {
    let mut read_through = BTreeSet::new();
    for kind in IngestorEntityKind::all() {
        if let IngestorReferenceKind::Client(connector) = kind.capabilities().reference {
            read_through.insert(connector);
        }
    }
    let mut written_through = BTreeSet::new();
    for kind in EmitterEntityKind::all() {
        for connector in ConnectorKind::iter() {
            if kind.capabilities().reference.admits_client(connector) {
                written_through.insert(connector);
            }
        }
    }
    for connector in ConnectorKind::iter() {
        let Some(client) = connector.client() else {
            continue;
        };
        let referenced_from_both_sides =
            read_through.contains(&connector) && written_through.contains(&connector);
        for key in client.config.keys() {
            if key.readers == ClientConfigReaders::Every {
                continue;
            }
            assert!(
                referenced_from_both_sides,
                "{connector} declares '{}' for one side, but only one side references its client",
                key.name
            );
        }
    }
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
            IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic)
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
            "metadata.key",
            "metadata.timestamp",
            "metadata.timestamp_type",
            "metadata.tombstone",
        ]
    );
    assert_eq!(
        named(
            EmitterEntityKind::Sqs(SqsEmitterEntity::Queue)
                .capabilities()
                .runtime_capabilities()
        ),
        [
            "MODE ACK",
            "FIFO GROUP <expression>",
            "FIFO GROUP FROM BRANCH",
            "SEND SINGLE",
            "SEND BATCH",
            "BATCH",
            "DELAY",
            "DEDUPLICATION ID",
            "STALLED PEER timeout_ms",
        ]
    );
    assert_eq!(
        named(
            EmitterEntityKind::Http(HttpEmitterEntity::Request)
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
    let kind = IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic);
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
        assert!(matches!(qos, "0" | "1" | "2"), "{qos}");
        assert_eq!(source.chosen("INSTANCES"), None);
    }
    let kafka = IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic);
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
/// from the same bytes would always publish to the first emitter.
fn assert_generated_entities_state_what_they_declare(bytes: &[u8]) {
    let (ingestor_bytes, emitter_bytes) = bytes.split_at(bytes.len() / 2);
    assert_generated_ingestor_states_what_its_entity_declares(ingestor_bytes);
    assert_generated_emitter_states_what_its_entity_declares(emitter_bytes);
}

/// Checks that a generated ingestor names a declared mode whose requirements it meets, and a
/// quiesce mode its entity honors.
fn assert_generated_ingestor_states_what_its_entity_declares(bytes: &[u8]) {
    let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
    let sources = IngestorEntityKind::all().collect::<Vec<_>>();
    let count = NonZeroUsize::new(sources.len()).assured("the vocabulary declares sources");
    let entity_kind = sources[arbitrary.entropy().index(count)];
    let ingestor = arbitrary.create_ingestor_from(entity_kind);
    let declared = entity_kind.capabilities();
    if let Some(source) = ingestor.input.transport_entity() {
        let delivery = declared
            .declared_delivery(source.acknowledgement().kind())
            .unwrap_or_else(|| panic!("{entity_kind} names an undeclared mode: {source:?}"));
        assert_eq!(
            source.unmet_delivery(),
            None,
            "{entity_kind} names a mode without its requirements: {source:?}"
        );
        for requirement in delivery.requires {
            let Some(chosen) = source.chosen(requirement.option) else {
                panic!(
                    "{entity_kind} answers for no {}: {source:?}",
                    requirement.option
                );
            };
            assert!(
                requirement.accepts(chosen),
                "{entity_kind} names a mode without its requirements: {source:?}"
            );
        }
    }
    assert!(
        ingestor.input.supports_quiesce(ingestor.input.quiesce()),
        "{entity_kind} quiesces in a mode it does not honor: {:?}",
        ingestor.input.quiesce()
    );
}

/// Checks that a generated emitter names a declared publishing mode, writes a body of the
/// declared form, batching its declared legality admits and only the envelope clauses and
/// properties its entity declares, and publishes through a client of a declared connector.
fn assert_generated_emitter_states_what_its_entity_declares(bytes: &[u8]) {
    let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
    let emitters = EmitterEntityKind::all().collect::<Vec<_>>();
    let count = NonZeroUsize::new(emitters.len()).assured("the vocabulary declares emitters");
    let emitter_kind = emitters[arbitrary.entropy().index(count)];
    let emitter = arbitrary.create_emitter_to(emitter_kind);
    let declared = emitter_kind.capabilities();
    assert!(
        declared.accepts_publishing_mode_kind(emitter.connector.publishing().kind()),
        "{emitter_kind} names an undeclared mode: {:?}",
        emitter.connector.publishing()
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
    assert!(formed, "{emitter_kind} writes {:?}", emitter.body);
    let mapped_types = emitter.connector.mapped_value_types();
    assert_eq!(
        mapped_types.is_some(),
        declared.body == EmitterBodyForm::Values,
        "{emitter_kind} must declare the types of its direct mappings",
    );
    let batching = match (declared.batch, &emitter.batch) {
        (EmitterBatchLegality::Required { .. }, Some(_))
        | (EmitterBatchLegality::Optional { .. }, _)
        | (EmitterBatchLegality::Unavailable { .. }, None) => true,
        (EmitterBatchLegality::Required { .. }, None)
        | (EmitterBatchLegality::Unavailable { .. }, Some(_)) => false,
    };
    assert!(batching, "{emitter_kind} batches as {:?}", emitter.batch);
    if let Err(refused) = emitter.envelope.check_declared(emitter_kind) {
        panic!("{emitter_kind} writes an undeclared envelope: {refused:?}");
    }
    match emitter.connector.reference() {
        nervix_models::EmitterReference::Client(reference) => {
            assert!(declared.reference.admits_client(reference.connector))
        }
        nervix_models::EmitterReference::StorageClient(_) => assert!(matches!(
            declared.reference,
            EmitterReferenceKind::StorageClient(_)
        )),
        nervix_models::EmitterReference::Schema(_) => {
            assert_eq!(declared.reference, EmitterReferenceKind::Schema)
        }
    }
}

#[test]
fn bolero_generated_entities_state_only_what_their_entity_declares() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(assert_generated_entities_state_what_they_declare);
}
