//! Emitter start-plan owner tests.
//!
//! Layer: test harness.
//! - **Owns.** Focused configuration, client and delivery-policy planning assertions.
//! - **Depends on.** The emitter decision owner and typed vocabulary fixtures.
//! - **Must not know.** Connector I/O or runtime task orchestration.

use nervix_models::{
    AckMode, BatchMessageLimit, CreateClientAzureBlob, CreateClientClickHouse, CreateClientGcs,
    CreateClientHttp, CreateClientIcebergRest, CreateClientKafka, CreateClientMongoDb,
    CreateClientMqtt, CreateClientMySql, CreateClientNats, CreateClientOtel, CreateClientPostgres,
    CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis, CreateClientS3,
    CreateClientSentry, CreateClientSqs, CreateClientSyslog, CreateClientZeroMq,
    EmitterBatchLegality, ErrorPolicies, FlushPolicy, OtelLogsEmitter, ProcessorInputs,
    RouteConstruction, SentryEventEmitter,
};
use nonzero_ext::nonzero;
use rstest::rstest;

use super::*;

fn expression(raw: &str) -> Expression {
    nervix_plexus::parse_expression(raw).expect("valid semantic expression")
}

fn value_mapping(column: &str, raw: &str) -> ClickHouseValueMapping {
    ClickHouseValueMapping {
        column: column.to_string(),
        expression: expression(raw),
    }
}

fn named<T>(value: &str) -> T
where
    T: TryFrom<String>,
    <T as TryFrom<String>>::Error: std::fmt::Debug,
{
    T::try_from(value.to_string()).expect("fixture name must be valid")
}

fn retry_policy(backoff: &str, max_backoff: &str) -> RetryPolicy {
    RetryPolicy {
        backoff: backoff.to_string(),
        max_backoff: max_backoff.to_string(),
    }
}

/// Pool bounds for the pooled client fixtures, whose subject is the plan rather than the
/// declared capacity.
fn pool_bounds() -> ClientPoolBounds {
    ClientPoolBounds::new(1, nonzero!(4u32)).assured("one does not exceed four")
}

fn client_mount() -> Option<ClientResourceMount> {
    Some(ClientResourceMount {
        resource: named("certs"),
        version: 3,
    })
}

fn client_config() -> Vec<ClientConfigEntry> {
    vec![ClientConfigEntry {
        key: "addr".to_string(),
        value: "{{ certs }}/endpoint".to_string(),
    }]
}

fn http_client_config(endpoint: &str) -> Vec<ClientConfigEntry> {
    vec![
        ClientConfigEntry {
            key: "Endpoint".to_string(),
            value: endpoint.to_string(),
        },
        ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "5000".to_string(),
        },
    ]
}

fn batch_policy() -> EmitterBatchPolicy {
    EmitterBatchPolicy {
        max_messages: BatchMessageLimit::try_from(100u32).expect("100 is a valid limit"),
        max_size: "1MiB".parse().expect("1MiB is a valid size"),
    }
}

fn declared_client(name: &str) -> EmitterClientSpec<DeclaredClientConfig> {
    EmitterClientSpec {
        name: named(name),
        config: DeclaredClientConfig {
            mount: client_mount(),
            entries: client_config(),
        },
    }
}

fn emitter(connector: EmitterEntity) -> CreateEmitter {
    CreateEmitter {
        labels: Default::default(),
        instances: nervix_models::EmitterInstances::ONE,
        name: named("orders_out"),
        from: ProcessorInputs::single(named("orders")),
        body: nervix_models::EmitterBody::Values,
        envelope: Box::new(nervix_models::EmitterEnvelope::default()),
        connector: Box::new(connector),
        batch: None,
        flush_policy: FlushPolicy::Immediate,
        error_policies: ErrorPolicies::handled_by_log(),
        mode: AckMode::Attached,
        construction: RouteConstruction::default(),
        materialized_state: Vec::new(),
    }
}

fn iceberg_emitter() -> EmitterEntity {
    EmitterEntity::Iceberg(IcebergEmitter::Table(IcebergTableEmitter {
        mode: nervix_models::IcebergTablePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "100ms".to_string(),
                max_backoff: "1s".to_string(),
            },
        },
        client: named("upstream"),
        table: named("orders"),
        namespace: named("Analytics.Events"),
        values: vec![value_mapping("id", "input.id")],
        location: "s3://warehouse/orders".to_string(),
        catalog: IcebergCatalog::Rest {
            client: named("catalog"),
        },
        commit_each: "1s".to_string(),
        max_commit_size: "1MiB".parse().expect("valid commit size"),
    }))
}

fn iceberg_catalog() -> Model {
    Model::ClientIcebergRest(CreateClientIcebergRest {
        labels: Default::default(),
        name: named("catalog"),
        mount: client_mount(),
        config: client_config(),
    })
}

/// One emitter an emitter can declare, with the client Models that emitter names.
struct EmitterCase {
    connector: EmitterEntity,
    client: Model,
    catalog_client: Option<Model>,
}

impl EmitterCase {
    /// The emitter publishing to this case's emitter, declaring the batching clause a database
    /// emitter requires and leaving it out everywhere else.
    fn emitter(&self) -> CreateEmitter {
        let mut emitter = emitter(self.connector.clone());
        if let EmitterBatchLegality::Required { .. } = self.connector.batch_legality() {
            emitter.batch = Some(batch_policy());
        }
        emitter
    }

    fn decide(
        &self,
    ) -> Result<EmitterStartPlan<DeclaredClientConfig>, Report<EmitterStartPlanError>> {
        EmitterStartPlan::decide(
            &self.emitter(),
            EmitterClientModels {
                client: Some(&self.client),
                catalog_client: self.catalog_client.as_ref(),
            },
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum EmitterKind {
    Http,
    Kafka,
    Pulsar,
    RabbitMq,
    Redis,
    Mqtt,
    Nats,
    ZeroMq,
    Syslog,
    Sqs,
    Sentry,
    Otel,
    ClickHouse,
    Postgres,
    MySql,
    MongoDb,
    IcebergS3,
    IcebergGcs,
    IcebergAzureBlob,
}

impl EmitterKind {
    fn label(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Kafka => "kafka",
            Self::Pulsar => "pulsar",
            Self::RabbitMq => "rabbitmq",
            Self::Redis => "redis",
            Self::Mqtt => "mqtt",
            Self::Nats => "nats",
            Self::ZeroMq => "zeromq",
            Self::Syslog => "syslog",
            Self::Sqs => "sqs",
            Self::Sentry => "sentry",
            Self::Otel => "otel",
            Self::ClickHouse => "clickhouse",
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::MongoDb => "mongodb",
            Self::IcebergS3 | Self::IcebergGcs | Self::IcebergAzureBlob => "iceberg",
        }
    }

    fn case(self) -> EmitterCase {
        let client = || named::<ClientName>("upstream");
        match self {
            Self::Http => EmitterCase {
                connector: EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter {
                    mode: nervix_models::HttpRequestPublishingMode::Ack {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    method: expression("input.method"),
                    path: expression("concat('/orders/', input.id)"),
                })),
                client: Model::ClientHttp(CreateClientHttp {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: http_client_config("https://orders.example.com:8443"),
                }),
                catalog_client: None,
            },
            Self::Kafka => EmitterCase {
                connector: EmitterEntity::Kafka(KafkaEmitter::Topic(KafkaTopicEmitter {
                    mode: nervix_models::KafkaTopicPublishingMode::NoAck {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    topic: named("orders"),
                })),
                client: Model::ClientKafka(CreateClientKafka {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::Pulsar => EmitterCase {
                connector: EmitterEntity::Pulsar(PulsarEmitter::Topic(PulsarTopicEmitter {
                    mode: nervix_models::PulsarTopicPublishingMode::NoAck {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    topic: named("orders"),
                })),
                client: Model::ClientPulsar(CreateClientPulsar {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::RabbitMq => EmitterCase {
                connector: EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(RabbitMqQueueEmitter {
                    mode: nervix_models::RabbitMqQueuePublishingMode::NoAck {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    queue: named("orders"),
                })),
                client: Model::ClientRabbitMq(CreateClientRabbitMq {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::Redis => EmitterCase {
                connector: EmitterEntity::Redis(RedisEmitter::PubSub(RedisPubSubEmitter {
                    mode: nervix_models::RedisPubSubPublishingMode::NoAck {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    channel: named("orders"),
                })),
                client: Model::ClientRedis(CreateClientRedis {
                    labels: Default::default(),
                    name: client(),
                    pool: pool_bounds(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::Mqtt => EmitterCase {
                connector: EmitterEntity::Mqtt(MqttEmitter::Topic(MqttTopicEmitter {
                    mode: MqttTopicPublishingMode::NoAck {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    topic: named("orders"),
                    qos: MqttQos::Qos0,
                })),
                client: Model::ClientMqtt(CreateClientMqtt {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::Nats => EmitterCase {
                connector: EmitterEntity::Nats(NatsEmitter::Subject(NatsSubjectEmitter {
                    mode: nervix_models::NatsSubjectPublishingMode::NoAck {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    subject: named("orders"),
                })),
                client: Model::ClientNats(CreateClientNats {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::ZeroMq => EmitterCase {
                connector: EmitterEntity::ZeroMq(ZeroMqEmitter::Push(ZeroMqPushEmitter {
                    mode: nervix_models::ZeroMqPushPublishingMode::NoAck {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                })),
                client: Model::ClientZeroMq(CreateClientZeroMq {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::Syslog => EmitterCase {
                connector: EmitterEntity::Syslog(SyslogEmitter::Collector(
                    SyslogCollectorEmitter {
                        mode: nervix_models::SyslogCollectorPublishingMode::NoAck {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                    },
                )),
                client: Model::ClientSyslog(CreateClientSyslog {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::Sqs => EmitterCase {
                connector: EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
                    mode: nervix_models::SqsQueuePublishingMode::Ack {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    queue: nervix_models::SqsQueueName::parse("orders").expect("valid queue"),
                    fifo_group: None,
                    send: nervix_models::SqsSend::Batch,
                })),
                client: Model::ClientSqs(CreateClientSqs {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::Sentry => EmitterCase {
                connector: EmitterEntity::Sentry(SentryEmitter::Event(SentryEventEmitter {
                    mode: nervix_models::SentryEventPublishingMode::Ack {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    platform: Some("python".to_owned()),
                })),
                client: Model::ClientSentry(CreateClientSentry {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::Otel => EmitterCase {
                connector: EmitterEntity::from(OtelLogsEmitter {
                    mode: nervix_models::OtelLogsPublishingMode::Ack {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    mappings: OtelMappings {
                        values: vec![value_mapping("id", "input.id")],
                        attributes: Vec::new(),
                        resource: Vec::new(),
                        scope: None,
                    },
                }),
                client: Model::ClientOtel(CreateClientOtel {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::ClickHouse => EmitterCase {
                connector: EmitterEntity::ClickHouse(ClickHouseEmitter::Table(
                    ClickHouseTableEmitter {
                        mode: nervix_models::ClickHouseTablePublishingMode::Ack {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        table: named("orders"),
                        values: vec![value_mapping("id", "input.id")],
                    },
                )),
                client: Model::ClientClickHouse(CreateClientClickHouse {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::Postgres => EmitterCase {
                connector: EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
                    mode: nervix_models::PostgresTablePublishingMode::Ack {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    table: named("orders"),
                    values: vec![value_mapping("id", "input.id")],
                    conflict_action: nervix_models::PostgresConflictAction::None,
                })),
                client: Model::ClientPostgres(CreateClientPostgres {
                    labels: Default::default(),
                    name: client(),
                    pool: pool_bounds(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::MySql => EmitterCase {
                connector: EmitterEntity::MySql(MySqlEmitter::Table(MySqlTableEmitter {
                    mode: nervix_models::MySqlTablePublishingMode::Ack {
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: client(),
                    table: named("orders"),
                    values: vec![value_mapping("id", "input.id")],
                    conflict_action: nervix_models::MySqlConflictAction::None,
                })),
                client: Model::ClientMySql(CreateClientMySql {
                    labels: Default::default(),
                    name: client(),
                    pool: pool_bounds(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::MongoDb => EmitterCase {
                connector: EmitterEntity::MongoDb(MongoDbEmitter::Collection(
                    MongoDbCollectionEmitter {
                        mode: nervix_models::MongoDbCollectionPublishingMode::Ack {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        collection: named("orders"),
                        values: vec![value_mapping("id", "input.id")],
                        conflict_action: nervix_models::MongoDbConflictAction::None,
                    },
                )),
                client: Model::ClientMongoDb(CreateClientMongoDb {
                    labels: Default::default(),
                    name: client(),
                    pool: pool_bounds(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: None,
            },
            Self::IcebergS3 => EmitterCase {
                connector: iceberg_emitter(),
                client: Model::ClientS3(CreateClientS3 {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: Some(iceberg_catalog()),
            },
            Self::IcebergGcs => EmitterCase {
                connector: iceberg_emitter(),
                client: Model::ClientGcs(CreateClientGcs {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: Some(iceberg_catalog()),
            },
            Self::IcebergAzureBlob => EmitterCase {
                connector: iceberg_emitter(),
                client: Model::ClientAzureBlob(CreateClientAzureBlob {
                    labels: Default::default(),
                    name: client(),
                    mount: client_mount(),
                    config: client_config(),
                }),
                catalog_client: Some(iceberg_catalog()),
            },
        }
    }
}

#[rstest]
#[case::http(EmitterKind::Http)]
#[case::kafka(EmitterKind::Kafka)]
#[case::pulsar(EmitterKind::Pulsar)]
#[case::rabbitmq(EmitterKind::RabbitMq)]
#[case::redis(EmitterKind::Redis)]
#[case::mqtt(EmitterKind::Mqtt)]
#[case::nats(EmitterKind::Nats)]
#[case::zeromq(EmitterKind::ZeroMq)]
#[case::syslog(EmitterKind::Syslog)]
#[case::sqs(EmitterKind::Sqs)]
#[case::sentry(EmitterKind::Sentry)]
#[case::otel(EmitterKind::Otel)]
#[case::clickhouse(EmitterKind::ClickHouse)]
#[case::postgres(EmitterKind::Postgres)]
#[case::mysql(EmitterKind::MySql)]
#[case::mongodb(EmitterKind::MongoDb)]
#[case::iceberg_s3(EmitterKind::IcebergS3)]
#[case::iceberg_gcs(EmitterKind::IcebergGcs)]
#[case::iceberg_azure_blob(EmitterKind::IcebergAzureBlob)]
fn decides_each_emitter_from_its_client_model(#[case] kind: EmitterKind) {
    let plan = kind
        .case()
        .decide()
        .expect("an emitter with a matching client must be planned");

    assert_eq!(plan.label, kind.label());
    assert_eq!(
        plan.retry_policy,
        ParsedRetryPolicy {
            backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    );
}

#[test]
fn every_row_emitter_carries_its_lowered_mapping_into_the_start_plan() {
    for kind in [
        EmitterKind::ClickHouse,
        EmitterKind::Postgres,
        EmitterKind::MySql,
        EmitterKind::MongoDb,
        EmitterKind::IcebergS3,
    ] {
        let plan = kind
            .case()
            .decide()
            .expect("the row emitter should be planned");
        let mapping = match &plan.connector {
            EmitterConnectorPlan::ClickHouse(ClickHouseEmitterPlan::Table(connector)) => {
                &connector.mapping
            }
            EmitterConnectorPlan::Postgres(PostgresEmitterPlan::Table(connector)) => {
                &connector.mapping
            }
            EmitterConnectorPlan::MySql(MySqlEmitterPlan::Table(connector)) => &connector.mapping,
            EmitterConnectorPlan::MongoDb(MongoDbEmitterPlan::Collection(connector)) => {
                &connector.mapping
            }
            EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(connector)) => {
                &connector.mapping
            }
            _ => panic!("the case must be a row emitter"),
        };
        assert_eq!(mapping.columns, ["id"], "{}", kind.label());
        assert_eq!(mapping.program.inner.set.len(), 1, "{}", kind.label());
        assert_eq!(
            mapping.program.inner.set[0].0.field,
            "c0",
            "{}",
            kind.label()
        );
    }
}

#[test]
fn otel_plan_combines_values_and_attributes_in_declared_order() {
    let mut case = EmitterKind::Otel.case();
    let EmitterEntity::Otel(OtelEmitter::Logs(OtelLogsEmitter { mappings, .. })) =
        &mut case.connector
    else {
        panic!("the case must be OTEL logs");
    };
    mappings
        .attributes
        .push(value_mapping("customer", "input.customer"));
    mappings
        .resource
        .push(value_mapping("service.name", "'orders'"));
    let plan = case.decide().expect("OTEL mappings should be planned");
    let EmitterConnectorPlan::Otel(OtelEmitterPlan::Logs(OtelLogsEmitterPlan {
        export: connector,
    })) = plan.connector
    else {
        panic!("the plan must be OTEL");
    };
    assert_eq!(connector.mapping.columns, ["id", "customer"]);
    assert_eq!(connector.values, ["id"]);
    assert_eq!(connector.attributes, ["customer"]);
    assert_eq!(
        connector.resource,
        [OtelResourceAttribute {
            key: "service.name".to_string(),
            value: OtelLiteral::String("orders".to_string()),
        }]
    );
}

#[test]
fn otel_resource_requires_a_literal_during_planning() {
    let mut case = EmitterKind::Otel.case();
    let EmitterEntity::Otel(OtelEmitter::Logs(OtelLogsEmitter { mappings, .. })) =
        &mut case.connector
    else {
        panic!("the case must be OTEL logs");
    };
    mappings
        .resource
        .push(value_mapping("service.name", "input.customer"));
    let error = case
        .decide()
        .expect_err("resource attributes must be fixed values");
    assert_eq!(
        *error.current_context(),
        EmitterStartPlanError::InvalidOtelResource {
            kind: case.connector.kind(),
            emitter: named("orders_out"),
            attribute: "service.name".to_string(),
        }
    );
}

/// Duration text `humantime` panicked on instead of refusing: spans that add up to the last
/// second a duration holds, and fractions of exactly one more second.
const TOO_LONG_DURATION_TEXT: &str = "18446744073709551615s 1000000000ns";

#[test]
fn iceberg_commit_cadence_keeps_why_its_text_names_no_duration() {
    for (commit_each, why) in [
        ("oops", "expected number at 0"),
        (
            TOO_LONG_DURATION_TEXT,
            "it is longer than a duration can be",
        ),
    ] {
        let mut case = EmitterKind::IcebergS3.case();
        let EmitterEntity::Iceberg(IcebergEmitter::Table(IcebergTableEmitter {
            commit_each: cadence,
            ..
        })) = &mut case.connector
        else {
            panic!("the case must be Iceberg");
        };
        *cadence = commit_each.to_string();
        let error = case.decide().expect_err("the commit cadence must be valid");
        assert_eq!(
            *error.current_context(),
            EmitterStartPlanError::InvalidIcebergCommit {
                emitter: named("orders_out"),
                setting: IcebergCommitSetting::CommitEach,
                value: commit_each.to_string(),
            }
        );
        assert_eq!(
            error
                .downcast_ref::<nervix_models::DurationTextError>()
                .map(ToString::to_string),
            Some(why.to_string())
        );
    }
}

#[test]
fn iceberg_plan_preserves_namespace_and_exact_commit_bytes() {
    let case = EmitterKind::IcebergS3.case();
    let plan = case.decide().expect("valid Iceberg plan");
    let EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(table)) = plan.connector else {
        panic!("the plan must be Iceberg");
    };
    assert_eq!(table.namespace.as_str(), "Analytics.Events");
    assert_eq!(table.commit.max_size, 1_048_576);
}

#[test]
fn carries_the_batch_clause_into_the_plan_of_every_emitter() {
    for kind in [
        EmitterKind::Kafka,
        EmitterKind::Sentry,
        EmitterKind::Otel,
        EmitterKind::IcebergS3,
    ] {
        let case = kind.case();
        let unbatched = case.decide().expect("the emitter must be planned");
        assert_eq!(unbatched.connector.batch(), None, "{}", kind.label());

        let mut emitter = case.emitter();
        emitter.batch = Some(batch_policy());
        let batched = EmitterStartPlan::decide(
            &emitter,
            EmitterClientModels {
                client: Some(&case.client),
                catalog_client: case.catalog_client.as_ref(),
            },
        )
        .expect("the batching emitter must be planned");
        assert_eq!(
            batched.connector.batch(),
            Some(batch_policy()),
            "{}",
            kind.label()
        );
    }

    for kind in [
        EmitterKind::ClickHouse,
        EmitterKind::Postgres,
        EmitterKind::MySql,
        EmitterKind::MongoDb,
    ] {
        let plan = kind
            .case()
            .decide()
            .expect("the database emitter must be planned");
        assert_eq!(
            plan.connector.batch(),
            Some(batch_policy()),
            "{}",
            kind.label()
        );
    }
}

#[test]
fn sentry_plan_resolves_the_platform_before_runtime_composition() {
    for (authored, expected) in [(None, "other"), (Some("python"), "python"), (Some(""), "")] {
        let mut case = EmitterKind::Sentry.case();
        let EmitterEntity::Sentry(SentryEmitter::Event(event)) = &mut case.connector else {
            panic!("the Sentry fixture constructs an event entity");
        };
        event.platform = authored.map(str::to_owned);
        let Ok(plan) = case.decide() else {
            panic!("the Sentry fixture has its matching client");
        };
        let EmitterConnectorPlan::Sentry(SentryEmitterPlan::Event(event)) = plan.connector else {
            panic!("a Sentry entity produces its typed event plan");
        };
        assert_eq!(event.platform, expected);
        let resolved = event.resolve_clients(&mut |client| {
            Ok::<_, std::convert::Infallible>(ResolvedClientConfig {
                entries: client.config.entries.clone(),
                mounts: None,
            })
        });
        let Ok(resolved) = resolved;
        assert_eq!(resolved.platform, expected);
    }
}

#[test]
fn plans_an_http_emitter_with_its_client_origin() {
    let case = EmitterKind::Http.case();
    let emitter = case.emitter();

    let plan = EmitterStartPlan::decide(
        &emitter,
        EmitterClientModels {
            client: Some(&case.client),
            catalog_client: None,
        },
    )
    .expect("an HTTP emitter with an HTTP client must be planned");

    assert_eq!(plan.connector.batch(), None);
    let EmitterConnectorPlan::Http(HttpEmitterPlan::Request(connector)) = plan.connector else {
        panic!("an HTTP emitter must be planned as HTTP");
    };
    assert_eq!(
        connector.origin,
        HttpOrigin::parse("https://orders.example.com:8443")
            .expect("the fixture endpoint is an origin")
    );
    assert_eq!(connector.client.name, named::<ClientName>("upstream"));
}

#[test]
fn an_http_client_without_an_origin_endpoint_is_not_planned() {
    for config in [
        Vec::new(),
        http_client_config("https://orders.example.com/v1"),
    ] {
        let mut case = EmitterKind::Http.case();
        case.client = Model::ClientHttp(CreateClientHttp {
            labels: Default::default(),
            name: named("upstream"),
            mount: None,
            config,
        });

        let error = case
            .decide()
            .expect_err("an HTTP emitter needs an origin endpoint");

        assert_eq!(
            *error.current_context(),
            EmitterStartPlanError::InvalidHttpEndpoint {
                client: named("upstream"),
            }
        );
    }
}

#[test]
fn a_database_emitter_requires_the_batch_its_entity_declares() {
    let case = EmitterKind::MongoDb.case();
    let mut emitter = case.emitter();
    emitter.batch = None;
    let error = emitter
        .validate_batch()
        .expect_err("this entity requires a batch");
    assert_eq!(
        error.current_context(),
        &nervix_models::EmitterBatchContractError::Required {
            connector: emitter.connector.kind().phrase()
        },
    );
}

#[test]
fn carries_the_declared_client_and_the_emitter_parameters() {
    let plan = EmitterKind::Postgres
        .case()
        .decide()
        .expect("a Postgres emitter with a Postgres client must be planned");

    let EmitterConnectorPlan::Postgres(PostgresEmitterPlan::Table(connector)) = plan.connector
    else {
        panic!("a Postgres emitter must be planned as Postgres");
    };
    assert_eq!(connector.client, declared_client("upstream"));
    assert_eq!(
        connector.table,
        named::<nervix_models::PostgresTableName>("orders")
    );
    assert_eq!(connector.conflict_action, PostgresConflictAction::None);
    assert_eq!(connector.batch, batch_policy());
    assert_eq!(
        connector.pooled_client(),
        PooledClientPlan {
            transport: PooledTransport::Postgres,
            bounds: pool_bounds(),
        }
    );
}

#[test]
fn plans_an_iceberg_emitter_with_its_storage_and_catalog_clients() {
    let plan = EmitterKind::IcebergGcs
        .case()
        .decide()
        .expect("an Iceberg emitter with GCS storage and a REST catalog must be planned");

    let EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(connector)) = plan.connector else {
        panic!("an Iceberg emitter must be planned as Iceberg");
    };
    assert_eq!(connector.backend, IcebergStorageBackend::Gcs);
    assert_eq!(connector.storage, declared_client("upstream"));
    assert_eq!(connector.catalog, declared_client("catalog"));
    assert_eq!(connector.location, "s3://warehouse/orders");
    assert_eq!(connector.commit.interval, Duration::from_secs(1));
    assert_eq!(connector.commit.max_size, 1_048_576);
}

#[test]
fn decides_a_broker_confirmation_window_timeout_and_retry_policy() {
    let mut case = EmitterKind::Kafka.case();
    let EmitterEntity::Kafka(KafkaEmitter::Topic(destination)) = &mut case.connector else {
        panic!("expected Kafka destination");
    };
    destination.mode = nervix_models::KafkaTopicPublishingMode::Ack {
        window: AckWindow::Parallel {
            max: nonzero!(17u64),
        },
        ack_timeout: "3s".to_string(),
        retry_policy: retry_policy("25ms", "2s"),
    };

    let plan = case
        .decide()
        .expect("a Kafka emitter accepts an acknowledged broker mode");

    assert_eq!(
        plan.retry_policy,
        ParsedRetryPolicy {
            backoff: Duration::from_millis(25),
            max_backoff: Duration::from_secs(2),
        }
    );
    let EmitterConnectorPlan::Kafka(KafkaEmitterPlan::Topic(connector)) = plan.connector else {
        panic!("a Kafka emitter must be planned as Kafka");
    };
    assert_eq!(
        connector.mode,
        BrokerPublishingMode::Ack(AckConfirmation {
            max_in_flight: nonzero!(17usize),
            timeout: Duration::from_secs(3),
        })
    );
}

#[test]
fn decides_transport_specific_mqtt_nats_and_sqs_modes() {
    let mut mqtt = EmitterKind::Mqtt.case();
    let EmitterEntity::Mqtt(MqttEmitter::Topic(destination)) = &mut mqtt.connector else {
        panic!("expected Mqtt destination");
    };
    destination.qos = MqttQos::Qos2;
    destination.mode = MqttTopicPublishingMode::Ack {
        window: AckWindow::Sequential,
        ack_timeout: "7s".to_string(),
        retry_policy: retry_policy("10ms", "1s"),
    };
    let EmitterConnectorPlan::Mqtt(MqttEmitterPlan::Topic(mqtt)) =
        mqtt.decide().expect("valid MQTT mode").connector
    else {
        panic!("an MQTT emitter must be planned as MQTT");
    };
    assert_eq!(
        mqtt.mode,
        MqttPublishingMode::Qos2(AckConfirmation {
            max_in_flight: nonzero!(1usize),
            timeout: Duration::from_secs(7),
        })
    );

    let mut stream = EmitterKind::Nats.case();
    let EmitterEntity::Nats(NatsEmitter::Subject(destination)) = &stream.connector else {
        panic!("expected Nats destination");
    };
    stream.connector = EmitterEntity::Nats(NatsEmitter::Stream(NatsStreamEmitter {
        client: destination.client.clone(),
        stream: nervix_models::NatsStreamName::try_from("orders").expect("valid stream"),
        subject: destination.subject.clone(),
        mode: nervix_models::NatsStreamPublishingMode::Ack {
            window: AckWindow::Parallel {
                max: nonzero!(23u64),
            },
            ack_timeout: "11s".to_string(),
            retry_policy: retry_policy("10ms", "1s"),
        },
    }));
    let EmitterConnectorPlan::Nats(NatsEmitterPlan::Stream(stream)) =
        stream.decide().expect("valid NATS stream mode").connector
    else {
        panic!("a NATS emitter must be planned as NATS");
    };
    assert_eq!(
        stream.confirmation,
        AckConfirmation {
            max_in_flight: nonzero!(23usize),
            timeout: Duration::from_secs(11),
        }
    );

    let EmitterConnectorPlan::Nats(NatsEmitterPlan::Subject(_core)) = EmitterKind::Nats
        .case()
        .decide()
        .expect("a NATS emitter accepts NO_ACK")
        .connector
    else {
        panic!("a NATS emitter must be planned as NATS");
    };

    let EmitterConnectorPlan::Sqs(SqsEmitterPlan::Queue(sqs)) = EmitterKind::Sqs
        .case()
        .decide()
        .expect("an SQS emitter accepts SEND BATCH")
        .connector
    else {
        panic!("an SQS emitter must be planned as SQS");
    };
    assert_eq!(sqs.send, nervix_models::SqsSend::Batch);
}

#[rstest]
#[case::inverted(
    retry_policy("2s", "25ms"),
    EmitterStartPlanError::RetryMaxBackoffBelowBackoff
)]
#[case::zero_backoff(retry_policy("0s", "1s"), EmitterStartPlanError::ZeroRetryBackoff)]
#[case::unparseable(
        retry_policy("oops", "1s"),
        EmitterStartPlanError::InvalidDuration {
            setting: EmitterDurationSetting::RetryBackoff,
            value: "oops".to_string(),
        }
    )]
#[case::too_long(
        retry_policy("1s", TOO_LONG_DURATION_TEXT),
        EmitterStartPlanError::InvalidDuration {
            setting: EmitterDurationSetting::RetryMaxBackoff,
            value: TOO_LONG_DURATION_TEXT.to_string(),
        }
    )]
fn rejects_an_unusable_retry_policy(
    #[case] policy: RetryPolicy,
    #[case] expected: EmitterStartPlanError,
) {
    let mut case = EmitterKind::Kafka.case();
    let EmitterEntity::Kafka(KafkaEmitter::Topic(destination)) = &mut case.connector else {
        panic!("expected Kafka destination");
    };
    destination.mode = nervix_models::KafkaTopicPublishingMode::NoAck {
        retry_policy: policy,
    };

    let error = case
        .decide()
        .expect_err("an unusable retry policy must not be planned");

    assert_eq!(error.current_context(), &expected);
}

/// The MQTT emitter of `EmitterKind::Mqtt` publishing at `qos` in `mode`.
fn mqtt_case(qos: MqttQos, mode: MqttTopicPublishingMode) -> EmitterCase {
    let mut case = EmitterKind::Mqtt.case();
    let EmitterEntity::Mqtt(MqttEmitter::Topic(destination)) = &mut case.connector else {
        panic!("expected Mqtt destination");
    };
    destination.qos = qos;
    destination.mode = mode;
    case
}

fn mqtt_acknowledged() -> MqttTopicPublishingMode {
    MqttTopicPublishingMode::Ack {
        window: AckWindow::Parallel {
            max: nonzero!(4u64),
        },
        ack_timeout: "2s".to_string(),
        retry_policy: retry_policy("10ms", "1s"),
    }
}

fn mqtt_unacknowledged() -> MqttTopicPublishingMode {
    MqttTopicPublishingMode::NoAck {
        retry_policy: retry_policy("10ms", "1s"),
    }
}

#[test]
fn each_quality_of_service_plans_with_the_confirmation_its_mode_declares() {
    let confirmation = AckConfirmation {
        max_in_flight: nonzero!(4usize),
        timeout: Duration::from_secs(2),
    };
    for (case, expected) in [
        (
            mqtt_case(MqttQos::Qos0, mqtt_unacknowledged()),
            MqttPublishingMode::Qos0,
        ),
        (
            mqtt_case(MqttQos::Qos1, mqtt_acknowledged()),
            MqttPublishingMode::Qos1(confirmation),
        ),
        (
            mqtt_case(MqttQos::Qos2, mqtt_acknowledged()),
            MqttPublishingMode::Qos2(confirmation),
        ),
    ] {
        let EmitterConnectorPlan::Mqtt(MqttEmitterPlan::Topic(plan)) =
            case.decide().expect("a legal pairing is planned").connector
        else {
            panic!("an MQTT emitter must be planned as MQTT");
        };
        assert_eq!(plan.mode, expected);
    }
}

#[rstest]
#[case::unanswered(
    MqttQos::Qos0,
    mqtt_acknowledged(),
    "MQTT TOPIC emitters name MODE ACK only with QOS 1 or 2; the statement writes QOS 0"
)]
#[case::at_least_once(
    MqttQos::Qos1,
    mqtt_unacknowledged(),
    "MQTT TOPIC emitters name MODE NO ACK only with QOS 0; the statement writes QOS 1"
)]
#[case::exactly_once(
    MqttQos::Qos2,
    mqtt_unacknowledged(),
    "MQTT TOPIC emitters name MODE NO ACK only with QOS 0; the statement writes QOS 2"
)]
fn a_pairing_the_declaration_refuses_has_no_plan(
    #[case] qos: MqttQos,
    #[case] mode: MqttTopicPublishingMode,
    #[case] refusal: &str,
) {
    let error = mqtt_case(qos, mode)
        .decide()
        .expect_err("a pairing the declaration refuses must not be planned");

    let EmitterStartPlanError::UnmetPublishing(unmet) = error.current_context() else {
        panic!("expected the declared requirement, got {error:?}");
    };
    assert_eq!(unmet.to_string(), refusal);
}

#[rstest]
#[case::zero("0s", EmitterStartPlanError::ZeroAckTimeout)]
#[case::unparseable(
        "oops",
        EmitterStartPlanError::InvalidDuration {
            setting: EmitterDurationSetting::AckTimeout,
            value: "oops".to_string(),
        }
    )]
#[case::too_long(
        TOO_LONG_DURATION_TEXT,
        EmitterStartPlanError::InvalidDuration {
            setting: EmitterDurationSetting::AckTimeout,
            value: TOO_LONG_DURATION_TEXT.to_string(),
        }
    )]
fn rejects_an_unusable_ack_timeout(
    #[case] ack_timeout: &str,
    #[case] expected: EmitterStartPlanError,
) {
    let mut case = EmitterKind::RabbitMq.case();
    let EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(destination)) = &mut case.connector else {
        panic!("expected Kafka destination");
    };
    destination.mode = nervix_models::RabbitMqQueuePublishingMode::Ack {
        window: AckWindow::Sequential,
        ack_timeout: ack_timeout.to_string(),
        retry_policy: retry_policy("25ms", "2s"),
    };

    let error = case
        .decide()
        .expect_err("an unusable ack timeout must not be planned");

    assert_eq!(error.current_context(), &expected);
}

#[test]
fn reports_a_missing_client() {
    let case = EmitterKind::Kafka.case();

    let error = EmitterStartPlan::decide(
        &case.emitter(),
        EmitterClientModels {
            client: None,
            catalog_client: None,
        },
    )
    .expect_err("an emitter without its client must not be planned");

    assert_eq!(
        error.current_context(),
        &EmitterStartPlanError::MissingClient {
            connector: "KAFKA TOPIC",
            client: named("upstream"),
        }
    );
}

#[test]
fn reports_a_client_of_another_kind() {
    let mut case = EmitterKind::Kafka.case();
    case.client = EmitterKind::Pulsar.case().client;

    let error = case
        .decide()
        .expect_err("a Kafka emitter must not be planned over a Pulsar client");

    assert_eq!(
        error.current_context(),
        &EmitterStartPlanError::ClientKindMismatch {
            connector: "KAFKA TOPIC",
            expected: nervix_models::EmitterReferenceKind::Client(
                nervix_models::ConnectorKind::Kafka
            ),
            found: "PULSAR",
            client: named("upstream"),
        }
    );
}

#[test]
fn reports_an_iceberg_storage_client_of_another_kind() {
    let mut case = EmitterKind::IcebergS3.case();
    case.client = EmitterKind::Kafka.case().client;

    let error = case
        .decide()
        .expect_err("an Iceberg destination needs an object-store client");

    assert_eq!(
        error.current_context(),
        &EmitterStartPlanError::ClientKindMismatch {
            connector: "ICEBERG TABLE",
            expected: nervix_models::EmitterReferenceKind::StorageClient(&[
                nervix_models::ConnectorKind::S3,
                nervix_models::ConnectorKind::Gcs,
                nervix_models::ConnectorKind::AzureBlob
            ]),
            found: "KAFKA",
            client: named("upstream"),
        }
    );
}

#[test]
fn reports_a_missing_iceberg_catalog_client() {
    let mut case = EmitterKind::IcebergS3.case();
    case.catalog_client = None;

    let error = case
        .decide()
        .expect_err("an Iceberg emitter without its catalog must not be planned");

    assert_eq!(
        error.current_context(),
        &EmitterStartPlanError::MissingCatalogClient {
            client: named("catalog"),
        }
    );
}

#[test]
fn reports_an_iceberg_catalog_client_of_another_kind() {
    let mut case = EmitterKind::IcebergS3.case();
    case.catalog_client = Some(EmitterKind::IcebergS3.case().client);

    let error = case
        .decide()
        .expect_err("an Iceberg catalog must be an ICEBERG_REST client");

    assert_eq!(
        error.current_context(),
        &EmitterStartPlanError::CatalogClientKindMismatch {
            client: named("catalog"),
            found: "S3",
        }
    );
}

#[test]
fn reports_a_client_model_registered_under_another_name() {
    let mut case = EmitterKind::Sentry.case();
    case.client = Model::ClientSentry(CreateClientSentry {
        labels: Default::default(),
        name: named("different"),
        mount: None,
        config: Vec::new(),
    });

    let error = case
        .decide()
        .expect_err("a differently named client must not be planned");

    assert_eq!(
        error.current_context(),
        &EmitterStartPlanError::ClientIdentityMismatch {
            expected: named("upstream"),
            found: named("different"),
        }
    );
}

#[test]
fn binds_every_client_to_its_resolved_configuration_in_emitter_order() {
    let decided = EmitterKind::IcebergAzureBlob
        .case()
        .decide()
        .expect("an Iceberg emitter with Azure Blob storage and a REST catalog must be planned");
    let mut resolved_names = Vec::new();

    let plan = decided
        .resolve_clients(|client| {
            resolved_names.push(client.name.clone());
            let mut entries = client.config.entries.clone();
            entries.push(ClientConfigEntry {
                key: "resolved".to_string(),
                value: client.name.as_str().to_string(),
            });
            Ok::<_, std::convert::Infallible>(ResolvedClientConfig {
                entries,
                mounts: None,
            })
        })
        .expect("an infallible resolution must bind every client");

    assert_eq!(
        resolved_names,
        vec![named::<ClientName>("upstream"), named("catalog")]
    );
    let EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(connector)) = plan.connector else {
        panic!("resolution must keep the Iceberg emitter");
    };
    assert_eq!(connector.storage.name, named::<ClientName>("upstream"));
    assert_eq!(connector.storage.config.entries[1].value, "upstream");
    assert_eq!(connector.catalog.name, named::<ClientName>("catalog"));
    assert_eq!(connector.catalog.config.entries[1].value, "catalog");
}

#[test]
fn a_failed_resolution_fails_the_plan() {
    let decided = EmitterKind::Kafka
        .case()
        .decide()
        .expect("a Kafka emitter with a Kafka client must be planned");

    let failure = decided
        .resolve_clients(|client| Err(client.name.clone()))
        .expect_err("a failed resolution must not yield a plan");

    assert_eq!(failure, named::<ClientName>("upstream"));
}
