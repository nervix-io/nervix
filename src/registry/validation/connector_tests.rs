//! Validation of the source and emitter contracts.
//!
//! Layer: test harness.
//!
//! - **Owns.** Assertions that each connector's client, publishing, mapping, header, timestamp and
//!   listener rules accept the current forms and refuse the rest with the owning model named.
//! - **Depends on.** The connector validation and the registry test fixtures.
//! - **Must not know.** How a connector is instantiated or driven.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::fs;

use nervix_models::{
    AckMode, AckWindow, AlterEmitter, AlterEmitterOperation, ClientConfigEntry,
    ClientConfigSchemaError, ClientName, CodecName, CodecWireFormat, CreateClientHttp,
    CreateClientPrometheus, CreateClientS3, CreateClientSqs, CreateWireSchema, EmitterEntityKind,
    ErrorPolicies, FlushPolicy, GeneralErrorPolicy, HttpEmitterEntity, HttpEndpointIngestor,
    HttpIngestor, IcebergCatalog, IcebergTableEmitter, IngestorEntity, IngestorName, JsonType,
    KafkaConsumerGroupName, KafkaEmitter, KafkaIngestMode, KafkaIngestor, KafkaOffsetMode,
    KafkaTopicEmitter, KafkaTopicIngestor, MaterializedRelayState, MessageErrorPolicy,
    MqttIngestMode, MqttIngestor, MqttQos, MqttSession, MqttTopicIngestor, NatsEmitter,
    NatsSubjectEmitter, OtelAggregationTemporality, OtelLogsEmitter, OtelMetric, OtelValueMapping,
    OutputBranch, PostgresEmitter, PostgresTableEmitter, ProcessorInputs, ProcessorOutputs,
    PrometheusApiRootError, PulsarEmitter, PulsarEmitterEntity, PulsarTopicEmitter,
    RabbitMqEmitter, RabbitMqEmitterEntity, RabbitMqQueueEmitter, RetryPolicy, SentryEmitter,
    SentryEventEmitter, SignalingProtobufConfig, SqsEmitterEntity, SyslogCollectorEmitter,
    SyslogEmitter, SyslogListenerIngestor, WireSchemaField, WireSchemaName, ZeroMqEmitter,
    ZeroMqPushEmitter,
};
use nonzero_ext::nonzero;

use super::*;
use crate::registry::{
    mutation::RegistryMutation,
    storage::Registry,
    test_fixtures::{
        TOO_LONG_DURATION_TEXT, branch, branch_for_relay, branch_schema, branch_schema_with_types,
        branched_by, client_model, codec, emitter, endpoint, explicitly_unbranched_relay,
        ingestor_statement, jaq_native_codec, named, protobuf_codec, relay, relay_branched_by,
        relay_branched_by_relay_branch, schema, signaling_protocol, temp_db_path,
        unbranched_transforming_outputs, vhost, wire_schema,
    },
};

#[test]
fn transport_policy_errors_keep_the_node_and_duration_reader_cause() {
    let domain: DomainName = named("validation");
    let identifier: ModelName = named("incoming");
    let retry = RetryPolicy {
        backoff: "10ms".to_string(),
        max_backoff: "1s".to_string(),
    };
    let Err(error) = validate_transport_delivery(
        &domain,
        &identifier,
        nervix_models::IngestAcknowledgement::Sequential {
            timeout: "nonsense",
            retry: &retry,
        },
    ) else {
        panic!("an unreadable ACK TIMEOUT must be refused before execution");
    };
    let RegistryError::InvalidConnectorDeclaration {
        domain: actual_domain,
        node,
        issue,
    } = error.current_context()
    else {
        panic!("the policy reader must identify its owning node: {error:#}");
    };
    assert_eq!(actual_domain, &domain);
    assert_eq!(
        node,
        &nervix_models::NodeRef::new(nervix_models::ModelKind::Ingestor, identifier)
    );
    assert_eq!(
        issue,
        &ConnectorDeclarationIssue::InvalidDuration {
            setting: ConnectorDurationSetting::AckTimeout,
            value: "nonsense".to_string(),
        }
    );
    assert!(error.contains::<nervix_models::DurationTextError>());
    assert!(format!("{error:#}").contains("expected number at 0"));
}

#[test]
fn transport_policy_deadlines_obey_the_shared_schedulable_bound() {
    let domain: DomainName = named("validation");
    let node = nervix_models::NodeRef::new(
        nervix_models::ModelKind::Ingestor,
        named::<ModelName>("incoming"),
    );
    let maximum = format!("{}ns", ClientDeadline::LONGEST.as_nanos());
    let beyond = ClientDeadline::LONGEST
        .as_nanos()
        .checked_add(1)
        .assured("half a u64 nanosecond count plus one is below u128::MAX");
    for setting in [
        ConnectorDurationSetting::AckTimeout,
        ConnectorDurationSetting::BatchTimeout,
        ConnectorDurationSetting::RetryBackoff,
        ConnectorDurationSetting::RetryMaximum,
    ] {
        let result = setting.parse(&maximum, &domain, &node);
        assert!(
            matches!(result, Ok(duration) if duration == ClientDeadline::LONGEST),
            "the declared maximum fits: {result:?}",
        );
        let Err(error) = setting.parse(&format!("{beyond}ns"), &domain, &node) else {
            panic!("a policy above the shared deadline bound cannot be scheduled");
        };
        assert!(
            matches!(error.current_context(), RegistryError::InvalidConnectorDeclaration {
            issue: ConnectorDeclarationIssue::UnschedulableDuration { setting: refused }, ..
        } if *refused == setting)
        );
    }
    let result = ConnectorDurationSetting::BatchTimeout.parse("0s", &domain, &node);
    assert!(
        matches!(result, Ok(duration) if duration.is_zero()),
        "zero batch timeout gathers without waiting: {result:?}",
    );
}

fn validate_signaling_protocol(
    protocol: &CreateSignalingProtocol,
) -> Result<(), Report<RegistryError>> {
    let domain = DomainName::parse("default").expect("valid domain");
    ensure_signaling_protocol_is_valid(&domain, &ModelName::from(&protocol.name), protocol)
}

#[test]
fn signaling_protocols_accept_valid_jaq_programs() {
    validate_signaling_protocol(&signaling_protocol(
        SignalingWireFormat::Json,
        &["{id: 1}"],
        &[".id == 1 and .result == null"],
        &[".error"],
    ))
    .expect("valid signaling protocol must be accepted");

    validate_signaling_protocol(&signaling_protocol(
        SignalingWireFormat::Protobuf(SignalingProtobufConfig {
            resource: named("proto_bundle"),
            resource_version: 1,
            config: Vec::new(),
            send_message: "nervix.test.Subscribe".to_string(),
            wait_message: "nervix.test.Ack".to_string(),
        }),
        &["{id: 1}"],
        &[".id == 1"],
        &[],
    ))
    .expect("valid protobuf signaling protocol must be accepted");
}

#[test]
fn signaling_protocols_reject_invalid_jaq_programs() {
    let error = validate_signaling_protocol(&signaling_protocol(
        SignalingWireFormat::Json,
        &["{id: 1}", ".["],
        &[".id == 1"],
        &[],
    ))
    .expect_err("invalid send program must be rejected");
    assert!(
        error.to_string().contains("SEND JAQ program #2 is invalid"),
        "unexpected error: {error:?}"
    );

    let error = validate_signaling_protocol(&signaling_protocol(
        SignalingWireFormat::Json,
        &["{id: 1}"],
        &[".id == 1"],
        &[".error", "if ."],
    ))
    .expect_err("invalid fail matcher must be rejected");
    assert!(
        error.to_string().contains("FAIL JAQ program #2 is invalid"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn signaling_protocols_require_send_and_wait_programs() {
    let error = validate_signaling_protocol(&signaling_protocol(
        SignalingWireFormat::Json,
        &[],
        &[".id == 1"],
        &[],
    ))
    .expect_err("missing send program must be rejected");
    assert!(
        error.to_string().contains("at least one SEND JAQ program"),
        "unexpected error: {error:?}"
    );

    let error = validate_signaling_protocol(&signaling_protocol(
        SignalingWireFormat::Json,
        &["{id: 1}"],
        &[],
        &[],
    ))
    .expect_err("missing wait matcher must be rejected");
    assert!(
        error.to_string().contains("at least one WAIT JAQ matcher"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn protobuf_signaling_protocols_require_both_message_types() {
    let error = validate_signaling_protocol(&signaling_protocol(
        SignalingWireFormat::Protobuf(SignalingProtobufConfig {
            resource: named("proto_bundle"),
            resource_version: 1,
            config: Vec::new(),
            send_message: "nervix.test.Subscribe".to_string(),
            wait_message: "  ".to_string(),
        }),
        &["{id: 1}"],
        &[".id == 1"],
        &[],
    ))
    .expect_err("missing wait message type must be rejected");

    assert!(
        error.to_string().contains("WAIT MESSAGE type"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn emitter_publishing_contract_rejects_model_level_bypasses() {
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        unreachable!("emitter helper must build an emitter model")
    };
    let models = ModelIndex::new();

    *kafka_publication(&mut emitter) = nervix_models::KafkaTopicPublishingMode::NoAck {
        retry_policy: RetryPolicy {
            backoff: "0s".to_string(),
            max_backoff: "1s".to_string(),
        },
    };
    let error = validate_emitter_publishing_contract(
        &domain,
        &ModelName::from(&emitter.name),
        &models,
        &emitter,
    )
    .expect_err("zero retry backoff must be rejected");
    assert!(format!("{error:#}").contains("BACKOFF must be greater than zero"));

    *kafka_publication(&mut emitter) = nervix_models::KafkaTopicPublishingMode::Ack {
        window: AckWindow::Sequential,
        ack_timeout: "0s".to_string(),
        retry_policy: RetryPolicy {
            backoff: "10ms".to_string(),
            max_backoff: "1s".to_string(),
        },
    };
    let error = validate_emitter_publishing_contract(
        &domain,
        &ModelName::from(&emitter.name),
        &models,
        &emitter,
    )
    .expect_err("zero confirmation timeout must be rejected");
    assert!(format!("{error:#}").contains("ACK TIMEOUT must be greater than zero"));

    *emitter.connector = EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
        mode: nervix_models::SqsQueuePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("sqs_main"),
        send: nervix_models::SqsSend::Single,
        queue: nervix_models::SqsQueueName::parse("events").expect("valid queue"),
        fifo_group: Some(SqsFifoGroup::Expression(Expression::Literal(
            nervix_models::Literal::String("group".to_string()),
        ))),
    }));
    *sqs_publication(&mut emitter) = nervix_models::SqsQueuePublishingMode::Ack {
        retry_policy: RetryPolicy {
            backoff: "1s".to_string(),
            max_backoff: "100ms".to_string(),
        },
    };
    let error = validate_emitter_publishing_contract(
        &domain,
        &ModelName::from(&emitter.name),
        &models,
        &emitter,
    )
    .expect_err("retry maxima below their initial backoff must be rejected");
    assert!(format!("{error:#}").contains("must be at least BACKOFF"));

    let nervix_models::SqsQueuePublishingMode::Ack { retry_policy } = sqs_publication(&mut emitter);
    retry_policy.max_backoff = "1s".to_string();
    let error = validate_emitter_publishing_contract(
        &domain,
        &ModelName::from(&emitter.name),
        &models,
        &emitter,
    )
    .expect_err("FIFO GROUP on a standard queue must be rejected");
    assert!(format!("{error:#}").contains("requires a queue name ending in .fifo"));
}

fn batch_policy(max_messages: u32, max_size: &str) -> nervix_models::EmitterBatchPolicy {
    nervix_models::EmitterBatchPolicy {
        max_messages: nervix_models::BatchMessageLimit::try_from(max_messages)
            .expect("the fixture message limit is within range"),
        max_size: max_size
            .parse()
            .expect("the fixture size is a whole number of bytes"),
    }
}

#[test]
fn publishing_contract_checks_the_batch_clause_against_its_emitter() {
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        unreachable!("emitter helper must build an emitter model")
    };
    let identifier = ModelName::from(&emitter.name);
    let models = ModelIndex::new();
    emitter.body = nervix_models::EmitterBody::Values;

    *emitter.connector = EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
        mode: nervix_models::PostgresTablePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("postgres_main"),
        table: named("events"),
        values: vec![otel_mapping("seq")],
        conflict_action: nervix_models::PostgresConflictAction::None,
    }));

    let error = validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect_err("a Postgres emitter without BATCH must be rejected");
    assert!(
        format!("{error:#}").contains("POSTGRES TABLE emitters require BATCH MAX MESSAGES"),
        "unexpected error: {error:#}"
    );

    emitter.batch = Some(batch_policy(500, "8MiB"));
    validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect("a Postgres emitter with BATCH is valid");

    *emitter.connector = EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
        mode: nervix_models::SqsQueuePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("sqs_main"),
        send: nervix_models::SqsSend::Single,
        queue: nervix_models::SqsQueueName::parse("events").expect("valid queue"),
        fifo_group: None,
    }));
    emitter.body = nervix_models::EmitterBody::Codec {
        codec: named("event_codec"),
    };
    *sqs_publication(&mut emitter) = nervix_models::SqsQueuePublishingMode::Ack {
        retry_policy: RetryPolicy {
            backoff: "10ms".to_string(),
            max_backoff: "1s".to_string(),
        },
    };
    let error = validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect_err("an SQS batch larger than one SQS message must be rejected");
    assert!(
        format!("{error:#}").contains("accept BATCH MAX SIZE up to 1MiB"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn batching_emitters_require_a_codec_container_their_emitter_can_publish() {
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        unreachable!("emitter helper must build an emitter model")
    };
    let identifier = ModelName::from(&emitter.name);
    let Model::Codec(mut protobuf) = protobuf_codec("event_codec", "event_schema", None, Some("."))
    else {
        unreachable!("protobuf_codec builds a codec model")
    };

    validate_emitter_batch_container(&domain, &identifier, &emitter, &protobuf)
        .expect("an emitter without BATCH needs no batch container");

    emitter.batch = Some(batch_policy(100, "1MiB"));
    let error = validate_emitter_batch_container(&domain, &identifier, &emitter, &protobuf)
        .expect_err("a protobuf codec without BATCH MESSAGE has no batch container");
    assert!(
        format!("{error:#}").contains("to declare a BATCH MESSAGE"),
        "unexpected error: {error:#}"
    );
    if let CodecWireFormat::Protobuf(config) = &mut protobuf.wire_format {
        config.batch_message = Some("nervix.test.NotificationBatch".to_string());
    }
    validate_emitter_batch_container(&domain, &identifier, &emitter, &protobuf)
        .expect("a protobuf codec with BATCH MESSAGE has a batch container");

    *emitter.connector = EmitterEntity::Sentry(SentryEmitter::Event(SentryEventEmitter {
        platform: None,
        mode: nervix_models::SentryEventPublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("sentry_main"),
    }));
    let Model::Codec(mut json) = jaq_native_codec("event_codec", "event_schema", None, Some("."))
    else {
        unreachable!("jaq_native_codec builds a codec model")
    };
    let error = validate_emitter_batch_container(&domain, &identifier, &emitter, &json)
        .expect_err("a batching Sentry emitter needs ON EMITTING BATCH");
    assert!(
        format!("{error:#}").contains(
            "SENTRY EVENT emitter requires codec 'event_codec' to declare an ON EMITTING BATCH"
        ),
        "unexpected error: {error:#}"
    );
    if let CodecWireFormat::JaqNative {
        transformations, ..
    } = &mut json.wire_format
    {
        transformations.on_emitting_batch = Some("{extra: {records: .}}".to_string());
    }
    validate_emitter_batch_container(&domain, &identifier, &emitter, &json)
        .expect("a Sentry emitter whose codec builds the batch event is valid");
}

fn otel_mapping(key: &str) -> OtelValueMapping {
    OtelValueMapping {
        column: key.to_string(),
        expression: Expression::Literal(nervix_models::Literal::String("value".to_string())),
    }
}

#[test]
fn direct_values_sensitivity_reports_the_typed_external_target() {
    let domain = DomainName::parse("default").assured("default is a valid domain name");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        panic!("the emitter fixture constructs an emitter model");
    };
    let identifier = ModelName::from(&emitter.name);
    let input_schema = CreateSchema {
        name: named("event"),
        fields: vec![SchemaField {
            name: named("secret"),
            ty: ParseAsType::String,
            optional: false,
            sensitive: true,
        }],
    };
    let mapping = nervix_models::ClickHouseValueMapping {
        column: "external_secret".to_string(),
        expression: nervix_plexus::parse_expression("input.secret")
            .assured("input.secret is a valid expression"),
    };
    *emitter.connector = EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
        mode: nervix_models::PostgresTablePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("database"),
        table: named("events"),
        values: vec![mapping],
        conflict_action: nervix_models::PostgresConflictAction::None,
    }));
    let models = ModelIndex::new();
    let error =
        validate_direct_value_mappings(&domain, &identifier, &models, &emitter, &input_schema)
            .expect_err("a sensitive VALUES expression must be rejected");
    assert!(matches!(
        error.current_context(),
        RegistryError::SensitiveEmitterValue {
            domain: error_domain,
            emitter: error_emitter,
            connector: "POSTGRES TABLE",
            target,
        } if error_domain == &domain
            && error_emitter == &identifier
            && target == "external_secret"
    ));

    let EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter { values, .. })) =
        emitter.connector.as_mut()
    else {
        panic!("the test emitter uses Postgres VALUES");
    };
    values[0].expression = nervix_plexus::parse_expression("leak_sensitive(input.secret)")
        .assured("leak_sensitive(input.secret) is a valid expression");
    validate_direct_value_mappings(&domain, &identifier, &models, &emitter, &input_schema)
        .assured("explicit leakage permits the direct VALUES mapping");
}

#[test]
fn direct_values_type_failure_keeps_the_vm_compile_cause() {
    let domain: DomainName = named("default");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        panic!("the emitter fixture constructs an emitter model");
    };
    let identifier = ModelName::from(&emitter.name);
    let input_schema = CreateSchema {
        name: named("event"),
        fields: vec![SchemaField {
            name: named("value"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    };
    let mapping = |column: &str, expression: &str| nervix_models::ClickHouseValueMapping {
        column: column.to_string(),
        expression: nervix_plexus::parse_expression(expression)
            .expect("the mapping expression is valid syntax"),
    };
    *emitter.connector = EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
        mode: nervix_models::PostgresTablePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("database"),
        table: named("events"),
        values: vec![
            mapping("first_value", "input.value"),
            mapping("external_value", "input.value"),
            mapping("last_value", "input.value"),
        ],
        conflict_action: nervix_models::PostgresConflictAction::None,
    }));
    for (expression, expected_code) in [
        (
            "input.missing",
            nervix_vm::CompileErrorCode::UnknownIdentifier,
        ),
        ("NULL", nervix_vm::CompileErrorCode::UntypedNull),
    ] {
        let EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
            values, ..
        })) = emitter.connector.as_mut()
        else {
            panic!("the test emitter uses Postgres VALUES");
        };
        values[1] = mapping("external_value", expression);
        let error = validate_direct_value_mappings(
            &domain,
            &identifier,
            &ModelIndex::new(),
            &emitter,
            &input_schema,
        )
        .expect_err("an uninferable expression must fail VALUES admission");
        assert!(matches!(
            error.current_context(),
            RegistryError::InvalidConnectorDeclaration {
                domain: actual_domain,
                node,
                issue: ConnectorDeclarationIssue::MappedExpressionInference {
                    section: nervix_models::MappedValueSection::Values,
                    key,
                },
            } if actual_domain == &domain
                && node == &nervix_models::NodeRef::new(nervix_models::ModelKind::Emitter, identifier.clone())
                && key == "external_value"
        ));
        let cause = error
            .frames()
            .find_map(|frame| frame.downcast_ref::<nervix_vm::CompileError>())
            .expect("the declaration refusal retains the typed VM cause");
        assert_eq!(cause.code, expected_code);
    }
}

#[test]
fn http_request_type_failure_keeps_the_vm_compile_cause() {
    let domain: DomainName = named("default");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        panic!("the emitter fixture constructs an emitter model");
    };
    let identifier = ModelName::from(&emitter.name);
    emitter.body = nervix_models::EmitterBody::WithoutBody;
    *emitter.connector = EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter {
        mode: nervix_models::HttpRequestPublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("api"),
        method: nervix_plexus::parse_expression("input.value")
            .expect("a field reference is a valid expression"),
        path: nervix_plexus::parse_expression("'/events'")
            .expect("a string literal is a valid expression"),
    }));
    let schema = CreateSchema {
        name: named("event"),
        fields: vec![SchemaField {
            name: named("value"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    };
    let error = validate_http_request_expressions(
        &domain,
        &identifier,
        &ModelIndex::new(),
        &emitter,
        &schema,
        &schema,
    )
    .expect_err("HTTP METHOD must be an exact STRING");
    assert!(matches!(
        error.current_context(),
        RegistryError::InvalidModel { domain, identifier, reason }
            if domain == "default"
                && identifier == "emit"
                && reason.contains("HTTP METHOD and PATH require exact non-sensitive STRING")
    ));
    assert!(error.contains::<nervix_vm::CompileError>());
}

#[test]
fn otel_literal_domains_are_admitted_by_the_registry_with_typed_refusals() {
    let domain = DomainName::parse("default").expect("valid domain");
    let identifier: ModelName = named("outgoing");
    for value in [-1, 0, 24, 25] {
        let connector = OtelEmitter::Logs(OtelLogsEmitter {
            mode: nervix_models::OtelLogsPublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".into(),
                    max_backoff: "1s".into(),
                },
            },
            client: named("destination"),
            mappings: OtelMappings {
                values: vec![
                    otel_mapping("time"),
                    otel_mapping("body"),
                    OtelValueMapping {
                        column: "severity_number".into(),
                        expression: Expression::Cast {
                            expression: Box::new(Expression::Literal(nervix_models::Literal::I64(
                                value,
                            ))),
                            target: ParseAsType::I32,
                        },
                    },
                ],
                attributes: Vec::new(),
                resource: Vec::new(),
                scope: None,
            },
        });
        let result = validate_otel_mapping_contract(&domain, &identifier, &connector);
        if (0..=24).contains(&value) {
            result.expect("severity boundary is in the declared domain");
        } else {
            assert_eq!(
                result
                    .expect_err("literal severity is outside its declared domain")
                    .current_context(),
                &RegistryError::InvalidOtelMapping {
                    domain: domain.clone(),
                    identifier: identifier.clone(),
                    issue: OtelMappingIssue::LiteralValue {
                        signal: OtelMappingSignal::Logs,
                        key: "severity_number",
                        rule: nervix_models::OtelValueRule::SeverityNumber
                    }
                }
            );
        }
    }
}

#[test]
fn otel_mapping_contract_validates_signal_keys_before_runtime() {
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        unreachable!("emitter helper must build an emitter model")
    };
    let identifier = ModelName::from(&emitter.name);
    emitter.body = nervix_models::EmitterBody::Values;

    let models = ModelIndex::new();

    *emitter.connector = EmitterEntity::from(OtelLogsEmitter {
        mode: nervix_models::OtelLogsPublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("otel_main"),
        mappings: OtelMappings {
            values: vec![otel_mapping("time"), otel_mapping("body")],
            attributes: Vec::new(),
            resource: Vec::new(),
            scope: None,
        },
    });
    validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect("complete OTEL LOGS mappings must be accepted");

    let EmitterEntity::Otel(OtelEmitter::Logs(OtelLogsEmitter { mappings, .. })) =
        emitter.connector.as_mut()
    else {
        unreachable!("test emitter must remain OTEL logs")
    };
    mappings.values.push(otel_mapping("body"));
    let error = validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect_err("duplicate OTEL VALUES keys must be rejected");
    assert_eq!(
        error.current_context(),
        &RegistryError::InvalidOtelMapping {
            domain: domain.clone(),
            identifier: identifier.clone(),
            issue: OtelMappingIssue::DuplicateValue {
                signal: OtelMappingSignal::Logs,
                key: "body".to_string(),
            },
        }
    );
    assert_eq!(
        format!("{error:#}"),
        "model 'emit' in domain 'default' is invalid: OTEL LOGS VALUES contains duplicate key \
         'body'"
    );

    *emitter.connector = EmitterEntity::from(OtelMetricsEmitter {
        mode: nervix_models::OtelMetricsPublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("otel_main"),
        metric: OtelMetric {
            name: "requests".to_string(),
            unit: "1".to_string(),
            description: None,
            kind: OtelMetricKind::Sum {
                monotonic: true,
                temporality: OtelAggregationTemporality::Delta,
            },
        },
        mappings: OtelMappings {
            values: vec![otel_mapping("time"), otel_mapping("value")],
            attributes: Vec::new(),
            resource: Vec::new(),
            scope: None,
        },
    });
    let error = validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect_err("DELTA metric streams without start_time must be rejected");
    assert_eq!(
        error.current_context(),
        &RegistryError::InvalidOtelMapping {
            domain: domain.clone(),
            identifier,
            issue: OtelMappingIssue::MissingDeltaValue {
                signal: OtelMappingSignal::MetricSum,
                key: "start_time",
            },
        }
    );
    assert_eq!(
        format!("{error:#}"),
        "model 'emit' in domain 'default' is invalid: OTEL METRIC SUM DELTA VALUES requires key \
         'start_time'"
    );
}

#[test]
fn sqs_fifo_group_is_validated_at_emitter_creation() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                explicitly_unbranched_relay("events", "event_schema"),
                branch_schema_with_types(
                    "tenant_branch_schema",
                    &[("tenant", ParseAsType::String)],
                ),
                branch("tenant_branch", "tenant_branch_schema"),
                relay_branched_by("tenant_events", "event_schema", "tenant_branch"),
                Model::ClientSqs(CreateClientSqs {
                    name: named("sqs_main"),
                    mount: None,
                    config: vec![
                        ClientConfigEntry {
                            key: "endpoint".to_string(),
                            value: "http://127.0.0.1:4566".to_string(),
                        },
                        ClientConfigEntry {
                            key: "region".to_string(),
                            value: "us-east-1".to_string(),
                        },
                        ClientConfigEntry {
                            key: "access_key_id".to_string(),
                            value: "fixture-key".to_string(),
                        },
                        ClientConfigEntry {
                            key: "secret_access_key".to_string(),
                            value: "fixture-secret".to_string(),
                        },
                    ],
                }),
            ],
        )
        .expect("SQS FIFO validation fixtures should install");

    let Model::Emitter(mut valid) = emitter("valid_fifo", "events", "event_codec", "sqs_main")
    else {
        unreachable!("emitter helper must build an emitter model")
    };
    valid.connector = Box::new(EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
        mode: nervix_models::SqsQueuePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("sqs_main"),
        send: nervix_models::SqsSend::Single,
        queue: nervix_models::SqsQueueName::parse("events.fifo").expect("valid queue"),
        fifo_group: Some(SqsFifoGroup::Expression(
            nervix_plexus::parse_expression("input.value").expect("valid FIFO group expression"),
        )),
    })));

    registry
        .apply_batch(&domain, vec![Model::Emitter(valid.clone())])
        .expect("non-sensitive STRING FIFO expressions should be accepted");

    let mut wrong_type = valid.clone();
    wrong_type.name = named("wrong_fifo_type");
    if let EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter { fifo_group, .. })) =
        wrong_type.connector.as_mut()
    {
        *fifo_group = Some(SqsFifoGroup::Expression(Expression::Literal(
            nervix_models::Literal::I64(42),
        )));
    }
    let error = registry
        .apply_batch(&domain, vec![Model::Emitter(wrong_type)])
        .expect_err("non-STRING FIFO expressions must be rejected");
    assert!(format!("{error:#}").contains("requires an exact non-sensitive STRING value"));

    let mut branch_fifo = valid.clone();
    branch_fifo.name = named("branch_fifo");
    branch_fifo.from = ProcessorInputs::single(named("tenant_events"));
    if let EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter { fifo_group, .. })) =
        branch_fifo.connector.as_mut()
    {
        *fifo_group = Some(SqsFifoGroup::FromBranch);
    }
    registry
        .apply_batch(&domain, vec![Model::Emitter(branch_fifo)])
        .expect("FIFO GROUP FROM BRANCH should accept a wholly branched input set");

    let mut mixed_inputs = valid.clone();
    mixed_inputs.name = named("mixed_fifo_inputs");
    mixed_inputs.from.from = vec![named("tenant_events"), named("events")];
    if let EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter { fifo_group, .. })) =
        mixed_inputs.connector.as_mut()
    {
        *fifo_group = Some(SqsFifoGroup::FromBranch);
    }
    let error = registry
        .apply_batch(&domain, vec![Model::Emitter(mixed_inputs)])
        .expect_err("every FIFO FROM BRANCH input must be branched");
    assert!(format!("{error:#}").contains("FROM BRANCH requires branched input"));

    let error = registry
        .apply_mutation_batch(
            &domain,
            vec![RegistryMutation::AlterEmitter(AlterEmitter {
                kind: EmitterEntityKind::Sqs(nervix_models::SqsEmitterEntity::Queue),
                emitter: named("branch_fifo"),
                operations: vec![AlterEmitterOperation::AddFrom {
                    relay: named("events"),
                    where_clause: None,
                }],
            })],
        )
        .expect_err("ALTER ADD FROM must not add an unbranched FIFO input");
    assert!(format!("{error:#}").contains("FROM BRANCH requires branched input"));

    let mut from_branch = valid;
    from_branch.name = named("unbranched_fifo");
    if let EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter { fifo_group, .. })) =
        from_branch.connector.as_mut()
    {
        *fifo_group = Some(SqsFifoGroup::FromBranch);
    }
    let error = registry
        .apply_batch(&domain, vec![Model::Emitter(from_branch)])
        .expect_err("FROM BRANCH on unbranched input must be rejected");
    assert!(format!("{error:#}").contains("FROM BRANCH requires branched input"));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn emitter_header_invocations_are_rejected_for_unsupported_emitters() {
    let domain = DomainName::parse("default").expect("valid domain");
    let schema = CreateSchema {
        name: named("event_schema"),
        fields: vec![SchemaField {
            name: named("tenant"),
            ty: ParseAsType::String,
            optional: false,
            sensitive: false,
        }],
    };
    let mut emitter = CreateEmitter {
        name: named("emit"),
        from: ProcessorInputs::single(named("events")),
        body: nervix_models::EmitterBody::Codec {
            codec: named("events_codec"),
        },
        envelope: Box::new(nervix_models::EmitterEnvelope::default()),
        connector: Box::new(EmitterEntity::ZeroMq(ZeroMqEmitter::Push(
            ZeroMqPushEmitter {
                mode: nervix_models::ZeroMqPushPublishingMode::NoAck {
                    retry_policy: RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: named("zeromq_main"),
            },
        ))),
        batch: None,
        flush_policy: FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string(),
        },
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        construction: nervix_plexus::parse_route_construction(
            "INHERIT ALL INVOKE write_header(\"tenant\", input.tenant)",
        )
        .expect("valid construction"),
        materialized_state: Vec::new(),
    };

    let error = super::effective_emitter_filter_map_schema(
        &domain,
        &ModelName::from(&emitter.name),
        &ModelIndex::new(),
        &emitter,
        &schema,
        &schema,
    )
    .expect_err("ZeroMQ emitters must reject write_header");
    assert!(format!("{error:#}").contains("ZEROMQ PUSH emitters do not support write_header"));

    *emitter.connector = EmitterEntity::Syslog(SyslogEmitter::Collector(SyslogCollectorEmitter {
        mode: nervix_models::SyslogCollectorPublishingMode::NoAck {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("syslog_main"),
    }));
    let error = super::effective_emitter_filter_map_schema(
        &domain,
        &ModelName::from(&emitter.name),
        &ModelIndex::new(),
        &emitter,
        &schema,
        &schema,
    )
    .expect_err("Syslog emitters must reject write_header");
    assert!(format!("{error:#}").contains("SYSLOG COLLECTOR emitters do not support write_header"));

    *emitter.connector = EmitterEntity::Kafka(KafkaEmitter::Topic(KafkaTopicEmitter {
        mode: nervix_models::KafkaTopicPublishingMode::NoAck {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("kafka_main"),
        topic: named("events_out"),
    }));
    super::effective_emitter_filter_map_schema(
        &domain,
        &ModelName::from(&emitter.name),
        &ModelIndex::new(),
        &emitter,
        &schema,
        &schema,
    )
    .expect("Kafka emitters must accept write_header");
}

/// A emitter that keeps one value per header name refuses an `INVOKE` clause that writes one literal
/// name twice, naming the header as the emitter records it and both calls, because the later write
/// would replace the earlier one on every record. Names compare as the emitter's header model records
/// them, an emitter that keeps every value accepts the repeat, and a name an expression computes is
/// left to the emitter at run time.
#[test]
fn a_repeated_literal_header_write_is_refused_where_the_emitter_keeps_one_value_per_name() {
    let domain = DomainName::parse("default").expect("valid domain");
    let schema = CreateSchema {
        name: named("event_schema"),
        fields: vec![SchemaField {
            name: named("tenant"),
            ty: ParseAsType::String,
            optional: false,
            sensitive: false,
        }],
    };
    let mut emitter = CreateEmitter {
        name: named("emit"),
        from: ProcessorInputs::single(named("events")),
        body: nervix_models::EmitterBody::Codec {
            codec: named("events_codec"),
        },
        envelope: Box::new(nervix_models::EmitterEnvelope::default()),
        connector: Box::new(EmitterEntity::Pulsar(PulsarEmitter::Topic(
            PulsarTopicEmitter {
                mode: nervix_models::PulsarTopicPublishingMode::NoAck {
                    retry_policy: RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: named("pulsar_main"),
                topic: named("events_out"),
            },
        ))),
        batch: None,
        flush_policy: FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string(),
        },
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        construction: nervix_plexus::parse_route_construction(
            "INHERIT ALL INVOKE write_header('route', 'a'), write_header('tenant', input.tenant), \
             write_header('route', 'b')",
        )
        .expect("valid construction"),
        materialized_state: Vec::new(),
    };
    let validate = |emitter: &CreateEmitter| {
        super::effective_emitter_filter_map_schema(
            &domain,
            &ModelName::from(&emitter.name),
            &ModelIndex::new(),
            emitter,
            &schema,
            &schema,
        )
    };
    let refused = |emitter: &CreateEmitter, connector: EmitterEntityKind, header: &str| {
        let error = validate(emitter).expect_err("a repeated literal header name is refused");
        assert_eq!(
            error.current_context(),
            &RegistryError::ReplacedHeaderWrite {
                domain: domain.clone(),
                emitter: named("emit"),
                connector,
                header: header.to_string(),
                earlier: 1,
                later: 3,
            }
        );
        format!("{error:#}")
    };

    let rendered = refused(
        &emitter,
        EmitterEntityKind::Pulsar(PulsarEmitterEntity::Topic),
        "route",
    );
    assert!(
        rendered.contains(
            "emitter 'emit' in domain 'default' writes header 'route' at INVOKE #1 and again at \
             #3, but PULSAR TOPIC emitters keep one value per header name, so the later write \
             would replace the earlier one on every record"
        ),
        "{rendered}"
    );

    *emitter.connector = EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
        mode: nervix_models::SqsQueuePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("sqs_main"),
        send: nervix_models::SqsSend::Single,
        queue: nervix_models::SqsQueueName::parse("events").expect("valid queue"),
        fifo_group: None,
    }));
    refused(
        &emitter,
        EmitterEntityKind::Sqs(SqsEmitterEntity::Queue),
        "route",
    );

    *emitter.connector = EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(RabbitMqQueueEmitter {
        mode: nervix_models::RabbitMqQueuePublishingMode::NoAck {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("rabbitmq_main"),
        queue: named("events_out"),
    }));
    refused(
        &emitter,
        EmitterEntityKind::RabbitMq(RabbitMqEmitterEntity::Queue),
        "route",
    );

    // HTTP records field names in lowercase, so two spellings of one name are one name.
    *emitter.connector = EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter {
        mode: nervix_models::HttpRequestPublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("api"),
        method: Expression::Literal(nervix_models::Literal::String("POST".to_string())),
        path: Expression::Literal(nervix_models::Literal::String("/events".to_string())),
    }));
    emitter.construction = nervix_plexus::parse_route_construction(
        "INHERIT ALL INVOKE write_header('X-Route', 'a'), write_header('Tenant', input.tenant), \
         write_header('x-route', 'b')",
    )
    .expect("valid construction");
    refused(
        &emitter,
        EmitterEntityKind::Http(HttpEmitterEntity::Request),
        "x-route",
    );

    // Names that differ in letter case are two names where an emitter keeps them exactly.
    *emitter.connector = EmitterEntity::Pulsar(PulsarEmitter::Topic(PulsarTopicEmitter {
        mode: nervix_models::PulsarTopicPublishingMode::NoAck {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("pulsar_main"),
        topic: named("events_out"),
    }));
    validate(&emitter).expect("X-Route and x-route are two Pulsar property names");

    // A emitter that keeps every value of a name publishes both writes.
    emitter.construction = nervix_plexus::parse_route_construction(
        "INHERIT ALL INVOKE write_header('route', 'a'), write_header('route', 'b')",
    )
    .expect("valid construction");
    *emitter.connector = EmitterEntity::Kafka(KafkaEmitter::Topic(KafkaTopicEmitter {
        mode: nervix_models::KafkaTopicPublishingMode::NoAck {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("kafka_main"),
        topic: named("events_out"),
    }));
    validate(&emitter).expect("a Kafka record keeps both headers");
    *emitter.connector = EmitterEntity::Nats(NatsEmitter::Subject(NatsSubjectEmitter {
        mode: nervix_models::NatsSubjectPublishingMode::NoAck {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("nats_main"),
        subject: named("events_out"),
    }));
    validate(&emitter).expect("a NATS message keeps both values of the name");

    // A name an expression computes is the emitter's to settle at run time.
    emitter.construction = nervix_plexus::parse_route_construction(
        "INHERIT ALL INVOKE write_header(input.tenant, 'a'), write_header(input.tenant, 'b')",
    )
    .expect("valid construction");
    *emitter.connector = EmitterEntity::Pulsar(PulsarEmitter::Topic(PulsarTopicEmitter {
        mode: nervix_models::PulsarTopicPublishingMode::NoAck {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("pulsar_main"),
        topic: named("events_out"),
    }));
    validate(&emitter).expect("computed names are not compared when the statement is validated");
}

#[test]
fn mqtt_instances_greater_than_one_are_valid() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let result = registry.apply_batch(
        &domain,
        vec![
            schema("event_schema"),
            wire_schema("event_wire"),
            codec("event_codec", "event_schema"),
            Model::ClientMqtt(nervix_models::CreateClientMqtt {
                name: named("mqtt_main"),
                mount: None,
                config: vec![nervix_models::MqttConfigEntry {
                    key: "addr".to_string(),
                    value: "mqtt://localhost:1883".to_string(),
                }],
            }),
            relay("notifications", "event_schema"),
            Model::Ingestor(CreateIngestor {
                name: IngestorName::parse("mqtt_ing").expect("valid identifier"),
                output_routes: unbranched_transforming_outputs("notifications"),
                input: nervix_models::IngestorInput::Transport(
                    nervix_models::TransportIngestorInput {
                        source: IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
                            client: ClientName::parse("mqtt_main").expect("valid identifier"),
                            topic: nervix_models::MqttTopicFilter::parse("notifications")
                                .expect("valid topic filter"),
                            instances: nonzero!(2u64),
                            session: MqttSession::Clean,
                            qos: MqttQos::Qos0,
                            mode: MqttIngestMode::NoAck,
                            quiesce: nervix_models::IngestQuiesceMode::Drop,
                        })),
                        codec: CodecName::parse("event_codec").expect("valid identifier"),
                    },
                ),
                timestamp_source: None,
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            }),
        ],
    );

    result.expect("MQTT multi-instance ingestors should not expose subscription mode");

    let _ = fs::remove_dir_all(path);
}

#[test]
fn ingestor_timestamp_field_must_use_rfc3339_schema_type() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let result = registry.apply_batch(
        &domain,
        vec![
            Model::Schema(CreateSchema {
                name: SchemaName::parse("event_schema").expect("valid identifier"),
                fields: vec![
                    SchemaField {
                        name: FieldName::parse("value").expect("valid identifier"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                    SchemaField {
                        name: FieldName::parse("occurred_at").expect("valid identifier"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                ],
            }),
            Model::WireJsonSchema(CreateWireSchema {
                name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                strictness: Default::default(),
                fields: vec![
                    WireSchemaField {
                        name: FieldName::parse("value").expect("valid identifier"),
                        ty: JsonType::String,
                        optional: false,
                    },
                    WireSchemaField {
                        name: FieldName::parse("occurred_at").expect("valid identifier"),
                        ty: JsonType::String,
                        optional: false,
                    },
                ],
            }),
            codec("event_codec", "event_schema"),
            client_model("broker"),
            relay("notifications", "event_schema"),
            Model::Ingestor(CreateIngestor {
                name: IngestorName::parse("ing").expect("valid identifier"),
                output_routes: unbranched_transforming_outputs("notifications"),
                input: nervix_models::IngestorInput::Transport(
                    nervix_models::TransportIngestorInput {
                        source: IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
                            client: ClientName::parse("broker").expect("valid identifier"),
                            topic: nervix_models::KafkaTopicName::parse("notifications")
                                .expect("valid topic"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(
                                KafkaConsumerGroupName::parse("cg").expect("valid consumer group"),
                            ),
                            instances: nonzero!(1u64),
                            mode: KafkaIngestMode::NoAck,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        })),
                        codec: CodecName::parse("event_codec").expect("valid identifier"),
                    },
                ),
                timestamp_source: Some(IngestTimestampSource::At(
                    FieldName::parse("occurred_at").expect("valid field name"),
                )),
                general_error_policy: GeneralErrorPolicy::Log,

                filter_where: None,
            }),
        ],
    );

    let error = result.expect_err("timestamp field with non-DATETIME type must fail");
    assert!(
        format!("{error:#}").contains("TIMESTAMP field 'occurred_at' must use DATETIME"),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(path);
}

/// A client ingestor reading `event_schema` into `notifications` under `mode`.
fn client_ingestor(mode: nervix_models::ClientIngestMode) -> Model {
    Model::Ingestor(CreateIngestor {
        name: IngestorName::parse("app_in").expect("valid identifier"),
        output_routes: unbranched_transforming_outputs("notifications"),
        input: nervix_models::IngestorInput::Client(nervix_models::ClientProducerIngestor {
            schema: SchemaName::parse("event_schema").expect("valid identifier"),
            mode,
        }),
        timestamp_source: None,
        general_error_policy: GeneralErrorPolicy::Log,
        filter_where: None,
    })
}

fn client_mode(
    ack_timeout: &str,
    backoff: &str,
    max_backoff: &str,
) -> nervix_models::ClientIngestMode {
    nervix_models::ClientIngestMode {
        window: AckWindow::Parallel {
            max: nonzero!(4u64),
        },
        ack_timeout: ack_timeout.to_string(),
        retry_policy: RetryPolicy {
            backoff: backoff.to_string(),
            max_backoff: max_backoff.to_string(),
        },
    }
}

fn apply_client_ingestor(
    registry: &Registry,
    with_schema: bool,
    mode: nervix_models::ClientIngestMode,
) -> Result<(), Report<RegistryError>> {
    let domain = DomainName::parse("default").expect("valid domain");
    let mut models = Vec::new();
    if with_schema {
        models.push(schema("event_schema"));
    } else {
        models.push(schema("other_schema"));
    }
    models.push(relay(
        "notifications",
        if with_schema {
            "event_schema"
        } else {
            "other_schema"
        },
    ));
    models.push(client_ingestor(mode));
    registry.apply_batch(&domain, models).map(|_| ())
}

#[test]
fn a_client_ingestor_validates_against_its_schema_without_a_codec() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    apply_client_ingestor(&registry, true, client_mode("30s", "100ms", "5s"))
        .expect("a client ingestor of an existing schema is valid");
    let _ = fs::remove_dir_all(path);
}

#[test]
fn a_client_ingestor_needs_its_schema() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let error = apply_client_ingestor(&registry, false, client_mode("30s", "100ms", "5s"))
        .expect_err("a client ingestor of a missing schema is invalid");
    assert!(
        format!("{error:#}").contains("event_schema"),
        "unexpected error: {error:#}"
    );
    let _ = fs::remove_dir_all(path);
}

/// Duration text that names no duration, with the reason each owner reports for it.
const UNREADABLE_DURATIONS: [(&str, &str); 2] = [
    ("oops", "expected number at 0"),
    (
        TOO_LONG_DURATION_TEXT,
        "it is longer than a duration can be",
    ),
];

/// Asserts that `error` rejects an invalid Model for exactly `expected`.
fn assert_invalid_model_reason(error: &Report<RegistryError>, expected: &str) {
    assert!(
        matches!(
            error.current_context(),
            RegistryError::InvalidModel { reason, .. } if reason == expected
        ),
        "{error:?}"
    );
}

#[test]
fn a_signaling_protocol_says_why_its_timeout_names_no_duration() {
    for (timeout, why) in UNREADABLE_DURATIONS {
        let mut protocol =
            signaling_protocol(SignalingWireFormat::Json, &["{id: 1}"], &[".id == 1"], &[]);
        protocol.on_connect.timeout = timeout.to_string();
        let error = validate_signaling_protocol(&protocol)
            .expect_err("the signaling timeout names no duration");
        assert_invalid_model_reason(
            &error,
            &format!("invalid signaling protocol timeout '{timeout}': {why}"),
        );
    }
}

#[test]
fn an_emitter_publishing_contract_says_why_a_duration_names_no_duration() {
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        unreachable!("emitter helper must build an emitter model")
    };
    let models = ModelIndex::new();
    for (value, why) in UNREADABLE_DURATIONS {
        let retry_policies = [
            RetryPolicy {
                backoff: value.to_string(),
                max_backoff: "1s".to_string(),
            },
            RetryPolicy {
                backoff: "10ms".to_string(),
                max_backoff: value.to_string(),
            },
        ];
        for (retry_policy, clause) in retry_policies.into_iter().zip(["BACKOFF", "MAX"]) {
            *kafka_publication(&mut emitter) =
                nervix_models::KafkaTopicPublishingMode::NoAck { retry_policy };
            let error = validate_emitter_publishing_contract(
                &domain,
                &ModelName::from(&emitter.name),
                &models,
                &emitter,
            )
            .expect_err("the retry policy names no duration");
            assert_invalid_model_reason(
                &error,
                &format!("invalid MODE RETRY POLICY {clause} '{value}': {why}"),
            );
        }

        *kafka_publication(&mut emitter) = nervix_models::KafkaTopicPublishingMode::Ack {
            window: AckWindow::Sequential,
            ack_timeout: value.to_string(),
            retry_policy: RetryPolicy {
                backoff: "10ms".to_string(),
                max_backoff: "1s".to_string(),
            },
        };
        let error = validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &models,
            &emitter,
        )
        .expect_err("the confirmation timeout names no duration");
        assert_invalid_model_reason(
            &error,
            &format!("invalid MODE ACK TIMEOUT '{value}': {why}"),
        );
    }
}

#[test]
fn an_ingestor_source_says_why_its_reject_retry_delay_names_no_duration() {
    let domain = DomainName::parse("default").expect("valid domain");
    let mut ingestor = ingestor_statement("edge_in", "notifications", "event_codec", "kafka", &[]);
    let identifier = ModelName::from(&ingestor.name);
    for (retry_after, why) in UNREADABLE_DURATIONS {
        let nervix_models::IngestorInput::Transport(input) = &mut ingestor.input else {
            unreachable!("the fixture builds a transport ingestor")
        };
        input.source = IngestorEntity::Http(HttpIngestor::Endpoint(HttpEndpointIngestor {
            endpoint: named("ingress"),
            mode: nervix_models::EndpointIngestMode::NoAck,
            quiesce: nervix_models::IngestQuiesceMode::Reject {
                retry_after: retry_after.to_string(),
            },
        }));
        let error = validate_ingestor_source(&domain, &identifier, &ingestor)
            .expect_err("the retry delay names no duration");
        assert_invalid_model_reason(
            &error,
            &format!("invalid quiesce REJECT RETRY AFTER duration '{retry_after}': {why}"),
        );
    }
}

/// An MQTT TOPIC source subscribing with `session` at `qos` under `mode`, as an alteration or a
/// restored Model can state it without the grammar's own check.
fn mqtt_source(session: MqttSession, qos: MqttQos, mode: MqttIngestMode) -> IngestorEntity {
    IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
        client: ClientName::parse("mqtt_main").expect("valid identifier"),
        topic: nervix_models::MqttTopicFilter::parse("notifications").expect("valid topic filter"),
        instances: nonzero!(1u64),
        session,
        qos,
        mode,
        quiesce: nervix_models::IngestQuiesceMode::Drop,
    }))
}

fn acknowledged_mqtt_mode() -> MqttIngestMode {
    MqttIngestMode::AckSequential {
        timeout: "5s".to_string(),
        retry_policy: RetryPolicy {
            backoff: "100ms".to_string(),
            max_backoff: "1s".to_string(),
        },
    }
}

#[test]
fn an_mqtt_ack_mode_outside_a_kept_session_is_refused_naming_both_written_values() {
    let domain: DomainName = named("validation");
    let mut ingestor = ingestor_statement("edge_in", "notifications", "event_codec", "kafka", &[]);
    let identifier = ModelName::from(&ingestor.name);
    let nervix_models::IngestorInput::Transport(input) = &mut ingestor.input else {
        unreachable!("the fixture builds a transport ingestor")
    };
    input.source = mqtt_source(MqttSession::Clean, MqttQos::Qos2, acknowledged_mqtt_mode());

    let refused = validate_ingestor_source(&domain, &identifier, &ingestor)
        .expect_err("an ACK mode needs the persistent session at QoS 1 or 2");

    let RegistryError::InvalidConnectorDeclaration { node, issue, .. } = refused.current_context()
    else {
        panic!("an unmet delivery requirement is a declaration refusal: {refused:?}");
    };
    assert_eq!(node.identifier.as_str(), "edge_in");
    let ConnectorDeclarationIssue::Delivery(unmet) = issue else {
        panic!("the refusal names the unmet delivery requirement: {issue:?}");
    };
    assert_eq!(
        unmet.written,
        vec![
            nervix_models::WrittenChoice {
                option: "SESSION",
                choice: Some("CLEAN"),
            },
            nervix_models::WrittenChoice {
                option: "QOS",
                choice: Some("2"),
            },
        ]
    );
    assert_eq!(
        refused.current_context().to_string(),
        "ingestor 'edge_in' in domain 'validation' is invalid: MQTT TOPIC ingestors name MODE ACK \
         SEQUENTIAL only with SESSION PERSISTENT and QOS 1 or 2; the statement writes SESSION \
         CLEAN and QOS 2"
    );
}

#[test]
fn an_mqtt_ack_mode_in_a_persistent_session_at_qos_one_or_two_is_valid() {
    let domain: DomainName = named("validation");
    let mut ingestor = ingestor_statement("edge_in", "notifications", "event_codec", "kafka", &[]);
    let identifier = ModelName::from(&ingestor.name);
    for qos in [MqttQos::Qos1, MqttQos::Qos2] {
        let nervix_models::IngestorInput::Transport(input) = &mut ingestor.input else {
            unreachable!("the fixture builds a transport ingestor")
        };
        input.source = mqtt_source(MqttSession::Persistent, qos, acknowledged_mqtt_mode());
        validate_ingestor_source(&domain, &identifier, &ingestor)
            .unwrap_or_else(|error| panic!("QoS {qos:?} keeps the session: {error:?}"));
    }
}

#[test]
fn a_client_ingestor_says_why_its_policy_duration_names_no_duration() {
    for (value, why) in UNREADABLE_DURATIONS {
        for (mode, clause) in [
            (client_mode(value, "100ms", "5s"), "ACK TIMEOUT"),
            (client_mode("30s", value, "5s"), "RETRY POLICY BACKOFF"),
        ] {
            let path = temp_db_path();
            let registry = Registry::open(&path).expect("registry should open");
            let error = apply_client_ingestor(&registry, true, mode)
                .expect_err("the policy duration names no duration");
            assert_invalid_model_reason(
                &error,
                &format!("invalid {clause} duration '{value}': {why}"),
            );
            let _ = fs::remove_dir_all(path);
        }
    }
}

#[test]
fn a_client_ingestor_needs_positive_bounded_durations_and_an_ordered_backoff() {
    for (mode, expected) in [
        (
            client_mode("0s", "100ms", "5s"),
            "ACK TIMEOUT must be greater than zero",
        ),
        (
            client_mode("30s", "10s", "5s"),
            "RETRY POLICY MAX 5s is shorter than its BACKOFF 10s",
        ),
        (
            client_mode("1000years", "100ms", "5s"),
            "longer than the longest duration a producer is told",
        ),
    ] {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let error = apply_client_ingestor(&registry, true, mode)
            .expect_err("an invalid client mode is refused");
        assert!(
            format!("{error:#}").contains(expected),
            "expected {expected:?}, got: {error:#}"
        );
        let _ = fs::remove_dir_all(path);
    }
}

#[test]
fn ingestor_route_validation_accepts_explicit_projection() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    registry
        .apply_batch(
            &domain,
            vec![
                Model::Schema(CreateSchema {
                    name: SchemaName::parse("event_schema").expect("valid identifier"),
                    fields: vec![
                        SchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: ParseAsType::I64,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: FieldName::parse("tenant").expect("valid identifier"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: FieldName::parse("raw").expect("valid identifier"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                    ],
                }),
                Model::Schema(CreateSchema {
                    name: SchemaName::parse("transformed_schema").expect("valid identifier"),
                    fields: vec![
                        SchemaField {
                            name: FieldName::parse("tenant").expect("valid identifier"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: FieldName::parse("total").expect("valid identifier"),
                            ty: ParseAsType::I64,
                            optional: false,
                            sensitive: false,
                        },
                    ],
                }),
                Model::WireJsonSchema(CreateWireSchema {
                    name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                    strictness: Default::default(),
                    fields: vec![
                        WireSchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: JsonType::Integer,
                            optional: false,
                        },
                        WireSchemaField {
                            name: FieldName::parse("tenant").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                        WireSchemaField {
                            name: FieldName::parse("raw").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                    ],
                }),
                codec("event_codec", "event_schema"),
                client_model("broker"),
                relay_branched_by_relay_branch("notifications", "transformed_schema"),
                branch_schema("tenant_branch", &["tenant"]),
                branch_for_relay("notifications", "tenant_branch"),
                Model::Ingestor(CreateIngestor {
                    name: IngestorName::parse("ing").expect("valid identifier"),
                    output_routes: (ProcessorOutputs::new(vec![ProcessorOutput {
                        relay: RelayName::parse("notifications").expect("valid identifier"),
                        construction: nervix_plexus::parse_route_construction(
                            "SET total = input.value, tenant = input.tenant",
                        )
                        .expect("route construction must parse"),
                        flush_policy: None,
                        message_error_policy: MessageErrorPolicy::Log,
                        branch: Some(branched_by("notifications", &["tenant"])),
                    }]))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    }),
                    input: nervix_models::IngestorInput::Transport(
                        nervix_models::TransportIngestorInput {
                            source: IngestorEntity::Kafka(KafkaIngestor::Topic(
                                KafkaTopicIngestor {
                                    client: ClientName::parse("broker").expect("valid identifier"),
                                    topic: nervix_models::KafkaTopicName::parse("notifications")
                                        .expect("valid topic"),
                                    offset_mode: KafkaOffsetMode::ConsumerGroup(
                                        KafkaConsumerGroupName::parse("cg")
                                            .expect("valid consumer group"),
                                    ),
                                    instances: nonzero!(1u64),
                                    mode: KafkaIngestMode::NoAck,
                                    quiesce: nervix_models::IngestQuiesceMode::Suspend,
                                },
                            )),
                            codec: CodecName::parse("event_codec").expect("valid identifier"),
                        },
                    ),
                    timestamp_source: None,
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            ],
        )
        .expect("batch with valid FILTER-MAP should succeed");

    let _ = fs::remove_dir_all(path);
}

#[test]
fn ingestor_filter_map_compile_errors_are_reported_on_leader() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let result = registry.apply_batch(
        &domain,
        vec![
            Model::Schema(CreateSchema {
                name: SchemaName::parse("event_schema").expect("valid identifier"),
                fields: vec![SchemaField {
                    name: FieldName::parse("value").expect("valid identifier"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            }),
            Model::Schema(CreateSchema {
                name: SchemaName::parse("transformed_schema").expect("valid identifier"),
                fields: vec![SchemaField {
                    name: FieldName::parse("total").expect("valid identifier"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            }),
            Model::WireJsonSchema(CreateWireSchema {
                name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                strictness: Default::default(),
                fields: vec![WireSchemaField {
                    name: FieldName::parse("value").expect("valid identifier"),
                    ty: JsonType::Integer,
                    optional: false,
                }],
            }),
            codec("event_codec", "event_schema"),
            client_model("broker"),
            relay("notifications", "transformed_schema"),
            Model::Ingestor(CreateIngestor {
                name: IngestorName::parse("ing").expect("valid identifier"),
                output_routes: (ProcessorOutputs::new(vec![ProcessorOutput {
                    relay: RelayName::parse("notifications").expect("valid identifier"),
                    construction: nervix_plexus::parse_route_construction(
                        "SET total = input.missing + 1",
                    )
                    .expect("route construction must parse"),
                    flush_policy: None,
                    message_error_policy: MessageErrorPolicy::Log,
                    branch: Some(OutputBranch::Unbranched),
                }]))
                .with_flush_policy(FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                }),
                input: nervix_models::IngestorInput::Transport(
                    nervix_models::TransportIngestorInput {
                        source: IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
                            client: ClientName::parse("broker").expect("valid identifier"),
                            topic: nervix_models::KafkaTopicName::parse("notifications")
                                .expect("valid topic"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(
                                KafkaConsumerGroupName::parse("cg").expect("valid consumer group"),
                            ),
                            instances: nonzero!(1u64),
                            mode: KafkaIngestMode::NoAck,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        })),
                        codec: CodecName::parse("event_codec").expect("valid identifier"),
                    },
                ),
                timestamp_source: None,
                general_error_policy: GeneralErrorPolicy::Log,

                filter_where: None,
            }),
        ],
    );

    let error = result.expect_err("invalid FILTER-MAP must fail");
    assert!(
        format!("{error:#}").contains("unknown input field 'missing'"),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn ingestor_inherit_all_except_rejects_required_uninitialized_field() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let result = registry.apply_batch(
        &domain,
        vec![
            Model::Schema(CreateSchema {
                name: SchemaName::parse("event_schema").expect("valid identifier"),
                fields: vec![
                    SchemaField {
                        name: FieldName::parse("value").expect("valid identifier"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    },
                    SchemaField {
                        name: FieldName::parse("tenant").expect("valid identifier"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                ],
            }),
            Model::WireJsonSchema(CreateWireSchema {
                name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                strictness: Default::default(),
                fields: vec![
                    WireSchemaField {
                        name: FieldName::parse("value").expect("valid identifier"),
                        ty: JsonType::Integer,
                        optional: false,
                    },
                    WireSchemaField {
                        name: FieldName::parse("tenant").expect("valid identifier"),
                        ty: JsonType::String,
                        optional: false,
                    },
                ],
            }),
            codec("event_codec", "event_schema"),
            client_model("broker"),
            relay("notifications", "event_schema"),
            Model::Ingestor(CreateIngestor {
                name: IngestorName::parse("ing").expect("valid identifier"),
                output_routes: (ProcessorOutputs::new(vec![ProcessorOutput {
                    relay: RelayName::parse("notifications").expect("valid identifier"),
                    construction: nervix_plexus::parse_route_construction(
                        "INHERIT ALL EXCEPT value",
                    )
                    .expect("route construction must parse"),
                    flush_policy: None,
                    message_error_policy: MessageErrorPolicy::Log,
                    branch: Some(OutputBranch::Unbranched),
                }]))
                .with_flush_policy(FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                }),
                input: nervix_models::IngestorInput::Transport(
                    nervix_models::TransportIngestorInput {
                        source: IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
                            client: ClientName::parse("broker").expect("valid identifier"),
                            topic: nervix_models::KafkaTopicName::parse("notifications")
                                .expect("valid topic"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(
                                KafkaConsumerGroupName::parse("cg").expect("valid consumer group"),
                            ),
                            instances: nonzero!(1u64),
                            mode: KafkaIngestMode::NoAck,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        })),
                        codec: CodecName::parse("event_codec").expect("valid identifier"),
                    },
                ),
                timestamp_source: None,
                general_error_policy: GeneralErrorPolicy::Log,

                filter_where: None,
            }),
        ],
    );

    let error = result.expect_err("excluded required output must remain uninitialized");
    assert!(
        format!("{error:#}").contains("required output field 'value' remains uninitialized"),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn emitter_accepts_same_schema_inputs_from_different_named_branches() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "source_a", "event_codec", "broker_out")
    else {
        unreachable!("emitter helper must build an emitter model")
    };
    emitter.from = ProcessorInputs::new(
        vec![named("source_a"), named("source_b")],
        vec![
            nervix_models::ProcessorInputWhere {
                relay: named("source_a"),
                where_clause: nervix_plexus::parse_expression("input.value = 'one'")
                    .expect("valid source filter"),
            },
            nervix_models::ProcessorInputWhere {
                relay: named("source_b"),
                where_clause: nervix_plexus::parse_expression("input.value = 'two'")
                    .expect("valid source filter"),
            },
        ],
    );

    registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                client_model("broker_out"),
                relay_branched_by("source_a", "event_schema", "branch_a"),
                relay_branched_by("source_b", "event_schema", "branch_b"),
                branch_schema("value_branch", &["value"]),
                branch("branch_a", "value_branch"),
                branch("branch_b", "value_branch"),
                Model::Emitter(emitter),
            ],
        )
        .expect("emitters may consume different named branches of one declared schema");

    let dataflow = registry
        .active_graph(&domain)
        .expect("graph should be installed")
        .to_dataflow_graph(domain.as_str());
    let edges = dataflow
        .edges
        .iter()
        .map(|edge| (edge.source.as_str(), edge.target.as_str()))
        .collect::<std::collections::BTreeSet<_>>();
    assert!(edges.contains(&("relay:source_a", "emitter:emit")));
    assert!(edges.contains(&("relay:source_b", "emitter:emit")));
    let emitter_edge = dataflow
        .edges
        .iter()
        .find(|edge| edge.target == "client_emitter:broker_out")
        .expect("emitter emitter edge must exist");
    assert_eq!(
        emitter_edge
            .metric
            .as_ref()
            .expect("emitter emitter edge must carry a metric")
            .relay,
        None,
        "multi-input sent metrics must aggregate without a misleading relay label"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn emitter_rejects_inputs_with_different_declared_schema_names() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "source_a", "event_codec", "broker_out")
    else {
        unreachable!("emitter helper must build an emitter model")
    };
    emitter.from = ProcessorInputs::new(vec![named("source_a"), named("source_b")], Vec::new());

    let error = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                schema("same_shape_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                client_model("broker_out"),
                relay("source_a", "event_schema"),
                relay("source_b", "same_shape_schema"),
                Model::Emitter(emitter),
            ],
        )
        .expect_err("emitter inputs must use the same declared schema");

    assert!(
        format!("{error:#}").contains(
            "input relay 'source_b' declares schema 'same_shape_schema', but all emitter inputs \
             must declare schema 'event_schema'"
        ),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn emitter_materialized_state_must_match_every_input_branch() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "source_a", "event_codec", "broker_out")
    else {
        unreachable!("emitter helper must build an emitter model")
    };
    emitter.from = ProcessorInputs::new(vec![named("source_a"), named("source_b")], Vec::new());
    emitter.materialized_state = vec![nervix_models::MaterializedStateDependency {
        relay: named("profiles"),
        policy: nervix_models::MaterializedStatePolicy::RequiredSkip,
    }];
    let Model::Relay(mut profiles) = relay_branched_by("profiles", "event_schema", "branch_a")
    else {
        unreachable!("relay helper must build a relay model")
    };
    profiles.materialized_state = Some(MaterializedRelayState::LastByTimestamp);

    let error = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                client_model("broker_out"),
                relay_branched_by("source_a", "event_schema", "branch_a"),
                relay_branched_by("source_b", "event_schema", "branch_b"),
                Model::Relay(profiles),
                branch_schema("value_branch", &["value"]),
                branch("branch_a", "value_branch"),
                branch("branch_b", "value_branch"),
                Model::Emitter(emitter),
            ],
        )
        .expect_err("materialized state must match every emitter input branch");

    assert!(
        format!("{error:#}").contains(
            "emitter materialized state requires relay 'source_b' and materialized relay \
             'profiles' to use the same exact branch"
        ),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn sentry_emitter_rejects_http_client() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut sentry_emitter) =
        emitter("emit", "notifications", "event_codec", "sentry_main")
    else {
        unreachable!("emitter helper must build an emitter model")
    };
    sentry_emitter.connector = Box::new(EmitterEntity::Sentry(SentryEmitter::Event(
        SentryEventEmitter {
            platform: None,
            mode: nervix_models::SentryEventPublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: named("sentry_main"),
        },
    )));

    let error = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                Model::ClientHttp(CreateClientHttp {
                    name: named("sentry_main"),
                    mount: None,
                    config: vec![
                        ClientConfigEntry {
                            key: "endpoint".to_string(),
                            value: "https://sentry.example/42".to_string(),
                        },
                        ClientConfigEntry {
                            key: "timeout_ms".to_string(),
                            value: "5000".to_string(),
                        },
                    ],
                }),
                relay("notifications", "event_schema"),
                Model::Emitter(sentry_emitter),
            ],
        )
        .expect_err("Sentry emitter must reject an HTTP client");

    assert!(
        format!("{error:#}").contains(
            "SENTRY EVENT emitter requires a SENTRY client, found HTTP client 'sentry_main'"
        ),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn a_syslog_ingestor_rejects_a_client_of_another_connector() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let error = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                client_model("collector"),
                relay("notifications", "event_schema"),
                Model::Ingestor(CreateIngestor {
                    name: named("syslog_ing"),
                    output_routes: unbranched_transforming_outputs("notifications"),
                    input: nervix_models::IngestorInput::Transport(
                        nervix_models::TransportIngestorInput {
                            source: IngestorEntity::from(SyslogListenerIngestor {
                                client: named("collector"),
                                mode: nervix_models::SyslogIngestMode::NoAck,
                                quiesce: nervix_models::IngestQuiesceMode::Drop,
                            }),
                            codec: named("event_codec"),
                        },
                    ),
                    timestamp_source: None,
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            ],
        )
        .expect_err("a SYSLOG LISTENER ingestor must reject a KAFKA client");

    assert_eq!(
        error.current_context(),
        &RegistryError::IngestorClientConnectorMismatch {
            domain: domain.clone(),
            ingestor: named("syslog_ing"),
            kind: nervix_models::IngestorEntityKind::Syslog(
                nervix_models::SyslogIngestorEntity::Listener
            ),
            client: named("collector"),
            required: nervix_models::ConnectorKind::Syslog,
            found: nervix_models::ConnectorKind::Kafka,
        }
    );
    assert_eq!(
        error.current_context().to_string(),
        "SYSLOG LISTENER ingestor 'syslog_ing' in domain 'default' requires a SYSLOG client, but \
         client 'collector' is a KAFKA client"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn an_endpoint_ingestor_rejects_an_endpoint_of_the_other_type() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let error = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                vhost("edge", &["edge.example.com"]),
                endpoint(
                    "stream",
                    "edge",
                    "/stream",
                    nervix_models::EndpointType::Websockets,
                ),
                relay("notifications", "event_schema"),
                Model::Ingestor(CreateIngestor {
                    name: named("web_in"),
                    output_routes: unbranched_transforming_outputs("notifications"),
                    input: nervix_models::IngestorInput::Transport(
                        nervix_models::TransportIngestorInput {
                            source: IngestorEntity::Http(HttpIngestor::Endpoint(
                                HttpEndpointIngestor {
                                    endpoint: named("stream"),
                                    mode: nervix_models::EndpointIngestMode::NoAck,
                                    quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                                        max_size: "1MiB".to_string(),
                                    },
                                },
                            )),
                            codec: named("event_codec"),
                        },
                    ),
                    timestamp_source: None,
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            ],
        )
        .expect_err("an HTTP ENDPOINT ingestor must reject a WEBSOCKETS endpoint");

    assert_eq!(
        error.current_context(),
        &RegistryError::IngestorEndpointTypeMismatch {
            domain: domain.clone(),
            ingestor: named("web_in"),
            kind: nervix_models::IngestorEntityKind::Http(
                nervix_models::HttpIngestorEntity::Endpoint
            ),
            endpoint: named("stream"),
            required: nervix_models::EndpointType::Http,
            found: nervix_models::EndpointType::Websockets,
        }
    );
    assert_eq!(
        error.current_context().to_string(),
        "HTTP ENDPOINT ingestor 'web_in' in domain 'default' requires a TYPE HTTP endpoint, but \
         endpoint 'stream' is TYPE WEBSOCKETS"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn an_iceberg_emitter_rejects_a_catalog_client_of_another_connector() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut iceberg_emitter) =
        emitter("emit", "notifications", "event_codec", "lake")
    else {
        unreachable!("emitter helper must build an emitter model")
    };
    iceberg_emitter.body = nervix_models::EmitterBody::Values;
    iceberg_emitter.construction = nervix_models::RouteConstruction::default();
    iceberg_emitter.connector = Box::new(EmitterEntity::from(IcebergTableEmitter {
        mode: nervix_models::IcebergTablePublishingMode::Ack {
            retry_policy: nervix_models::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("lake"),
        table: named("events"),
        namespace: named("Analytics.Events"),
        values: vec![nervix_models::IcebergValueMapping {
            column: "value".to_string(),
            expression: nervix_plexus::parse_expression("input.value")
                .assured("input.value is a valid expression"),
        }],
        location: "s3://lake/events".to_string(),
        catalog: IcebergCatalog::Rest {
            client: named("catalog"),
        },
        commit_each: "100ms".to_string(),
        max_commit_size: "1MiB".parse().expect("valid commit size"),
    }));

    let error = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                Model::ClientS3(CreateClientS3 {
                    name: named("lake"),
                    mount: None,
                    config: Vec::new(),
                }),
                client_model("catalog"),
                relay("notifications", "event_schema"),
                Model::Emitter(iceberg_emitter),
            ],
        )
        .expect_err("an ICEBERG TABLE emitter must reject a KAFKA catalog client");

    assert_invalid_model_reason(
        &error,
        "ICEBERG TABLE emitter requires an ICEBERG_REST catalog client, found KAFKA client \
         'catalog'",
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_duplicate_vhost_hostnames() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                vhost("edge", &["api.example.com"]),
                vhost("edge_internal", &["api.example.com"]),
            ],
        )
        .expect_err("duplicate hostname should fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));
    assert!(
        format!("{err}").contains("hostname 'api.example.com' is already assigned"),
        "unexpected error: {err}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn a_prometheus_client_is_created_only_with_a_config_its_source_can_use() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let prometheus_client = |config: &[(&str, &str)]| {
        Model::ClientPrometheus(CreateClientPrometheus {
            name: named("prom_main"),
            mount: None,
            config: config
                .iter()
                .map(|(key, value)| ClientConfigEntry {
                    key: (*key).to_string(),
                    value: (*value).to_string(),
                })
                .collect(),
        })
    };

    let error = registry
        .apply_batch(
            &domain,
            vec![prometheus_client(&[("addr", "http://prometheus:9090")])],
        )
        .expect_err("a Prometheus client without timeout_ms must be refused");
    assert_eq!(
        error.current_context(),
        &RegistryError::ClientConfig {
            domain: domain.clone(),
            client: named("prom_main")
        }
    );
    assert_eq!(
        error.downcast_ref::<ClientConfigSchemaError>(),
        Some(&ClientConfigSchemaError::MissingRequired { key: "timeout_ms" })
    );

    let error = registry
        .apply_batch(
            &domain,
            vec![prometheus_client(&[
                ("addr", "http://prometheus:9090/?stale=true"),
                ("timeout_ms", "5000"),
            ])],
        )
        .expect_err("a Prometheus address with a query string must be refused");
    assert_eq!(
        error.downcast_ref::<PrometheusApiRootError>(),
        Some(&PrometheusApiRootError::Query)
    );

    registry
        .apply_batch(
            &domain,
            vec![prometheus_client(&[
                ("addr", "https://prometheus.example.com/monitoring"),
                ("timeout_ms", "5000"),
                ("tls_ca_file", "/etc/nervix/tls/ca.pem"),
            ])],
        )
        .expect("a Prometheus client with an API root and a timeout must be created");

    let _ = fs::remove_dir_all(path);
}

fn kafka_publication(
    emitter: &mut nervix_models::CreateEmitter,
) -> &mut nervix_models::KafkaTopicPublishingMode {
    let EmitterEntity::Kafka(nervix_models::KafkaEmitter::Topic(destination)) =
        emitter.connector.as_mut()
    else {
        panic!("Kafka fixture");
    };
    &mut destination.mode
}
fn sqs_publication(
    emitter: &mut nervix_models::CreateEmitter,
) -> &mut nervix_models::SqsQueuePublishingMode {
    let EmitterEntity::Sqs(SqsEmitter::Queue(destination)) = emitter.connector.as_mut() else {
        panic!("SQS fixture");
    };
    &mut destination.mode
}

/// The Kafka fixture emitter, which declares no envelope clause and constructs its output.
fn kafka_emitter() -> CreateEmitter {
    let Model::Emitter(emitter) = emitter("enveloped", "events", "events_codec", "kafka_main")
    else {
        panic!("the fixture builds an emitter");
    };
    emitter
}

fn envelope_schema() -> CreateSchema {
    let field = |name: &str, ty: ParseAsType, optional: bool, sensitive: bool| SchemaField {
        name: named(name),
        ty,
        optional,
        sensitive,
    };
    CreateSchema {
        name: named("event_schema"),
        fields: vec![
            field("tenant", ParseAsType::String, false, false),
            field("at", ParseAsType::Datetime, false, false),
            field("secret", ParseAsType::String, false, true),
            field("maybe", ParseAsType::String, true, false),
        ],
    }
}

#[test]
fn an_envelope_clause_its_entity_does_not_declare_is_refused_naming_both() {
    let domain: DomainName = named("validation");
    let mut emitter = kafka_emitter();
    emitter.envelope.timestamp = Some(nervix_plexus::parse_expression("input.at").expect("parses"));

    let refused = validate_emitter_publishing_contract(
        &domain,
        &ModelName::from(&emitter.name),
        &ModelIndex::new(),
        &emitter,
    )
    .expect_err("KAFKA TOPIC declares no TIMESTAMP clause yet");

    let RegistryError::InvalidConnectorDeclaration { node, issue, .. } = refused.current_context()
    else {
        panic!("an undeclared clause is a declaration refusal: {refused:?}");
    };
    assert_eq!(node.identifier.as_str(), "enveloped");
    let undeclared = nervix_models::EnvelopeDeclarationError::UndeclaredClause {
        entity: EmitterEntityKind::Kafka(nervix_models::KafkaEmitterEntity::Topic),
        clause: nervix_models::EnvelopeClauseKind::Timestamp,
    };
    assert_eq!(issue, &ConnectorDeclarationIssue::Envelope(undeclared));
    assert_eq!(
        refused.current_context().to_string(),
        "emitter 'enveloped' in domain 'validation' is invalid: KAFKA TOPIC emitters declare no \
         TIMESTAMP clause"
    );
}

#[test]
fn an_envelope_duration_is_a_positive_schedulable_duration() {
    let domain: DomainName = named("validation");
    let node = NodeRef::new(ModelKind::Emitter, named::<ModelName>("enveloped"));
    for setting in [
        ConnectorDurationSetting::DeliverAfter,
        ConnectorDurationSetting::Delay,
    ] {
        assert_eq!(
            setting
                .parse("5s", &domain, &node)
                .expect("five seconds is a duration a transport can be given"),
            std::time::Duration::from_secs(5)
        );
        let zero = setting
            .parse("0s", &domain, &node)
            .expect_err("a zero duration delays nothing");
        assert!(
            format!("{zero:#}").contains(&format!("{setting} must be greater than zero")),
            "{zero:#}"
        );
        let unreadable = setting
            .parse(TOO_LONG_DURATION_TEXT, &domain, &node)
            .expect_err("a duration too long to read is refused");
        assert!(
            format!("{unreadable:#}").contains(&format!("invalid {setting} duration")),
            "{unreadable:#}"
        );
    }
}

/// An SQS QUEUE emitter sending to `queue`, which groups a FIFO queue's messages under one literal
/// group.
fn sqs_emitter(queue: &str) -> CreateEmitter {
    let mut emitter = kafka_emitter();
    let queue = nervix_models::SqsQueueName::parse(queue).expect("the fixture queue is valid");
    let fifo_group = if queue.is_fifo() {
        Some(SqsFifoGroup::Expression(Expression::Literal(
            nervix_models::Literal::String("group".to_string()),
        )))
    } else {
        None
    };
    *emitter.connector = EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
        client: named("sqs_main"),
        queue,
        fifo_group,
        send: nervix_models::SqsSend::Single,
        mode: nervix_models::SqsQueuePublishingMode::Ack {
            retry_policy: RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
    }));
    emitter
}

#[test]
fn an_sqs_queue_takes_a_delay_on_a_standard_queue_and_a_deduplication_id_on_a_fifo_queue() {
    let domain: DomainName = named("validation");
    let check = |emitter: &CreateEmitter| {
        validate_emitter_publishing_contract(
            &domain,
            &ModelName::from(&emitter.name),
            &ModelIndex::new(),
            emitter,
        )
    };
    let refusal = |emitter: &CreateEmitter| {
        let refused = check(emitter).expect_err("the queue refuses the clause");
        let RegistryError::InvalidConnectorDeclaration { issue, .. } = refused.current_context()
        else {
            panic!("an SQS envelope refusal is a declaration refusal: {refused:?}");
        };
        issue.clone()
    };
    let deduplication_id =
        || Some(nervix_plexus::parse_expression("input.tenant").expect("parses"));

    let mut standard = sqs_emitter("orders");
    for delay in ["1s", "900s"] {
        standard.envelope.delay = Some(delay.to_string());
        check(&standard).expect("SQS delays a standard queue's message by whole seconds");
    }
    for delay in ["1500ms", "901s"] {
        standard.envelope.delay = Some(delay.to_string());
        assert_eq!(
            refusal(&standard),
            ConnectorDeclarationIssue::SqsDelay {
                value: delay.to_string()
            }
        );
    }
    standard.envelope.delay = None;
    standard.envelope.deduplication_id = deduplication_id();
    assert_eq!(
        refusal(&standard),
        ConnectorDeclarationIssue::SqsStandardQueueDeduplication {
            queue: nervix_models::SqsQueueName::parse("orders").expect("valid queue"),
        }
    );

    let mut fifo = sqs_emitter("orders.fifo");
    fifo.envelope.deduplication_id = deduplication_id();
    check(&fifo).expect("SQS deduplicates a FIFO queue's messages by the id each carries");
    fifo.envelope.delay = Some("5s".to_string());
    assert_eq!(
        refusal(&fifo),
        ConnectorDeclarationIssue::SqsFifoQueueDelay {
            queue: nervix_models::SqsQueueName::parse("orders.fifo").expect("valid queue"),
        }
    );

    assert_eq!(
        ConnectorDeclarationIssue::SqsDelay {
            value: "1500ms".to_string()
        }
        .to_string(),
        "SQS DELAY must be whole seconds from 1s to 900s, found '1500ms'"
    );
}

#[test]
fn an_envelope_value_is_an_exact_non_null_non_sensitive_value_of_its_clause_type() {
    let domain: DomainName = named("validation");
    let schema = envelope_schema();
    let check = |emitter: &CreateEmitter| {
        validate_emitter_envelope_expressions(
            &domain,
            &ModelName::from(&emitter.name),
            &ModelIndex::new(),
            emitter,
            &schema,
            &schema,
        )
    };
    let expression =
        |text: &str| nervix_plexus::parse_expression(text).expect("the case expression parses");

    let mut emitter = kafka_emitter();
    emitter.envelope.timestamp = Some(expression("input.at"));
    emitter.envelope.message_id = Some(expression("leak_sensitive(input.secret)"));
    emitter.envelope.reply_to = Some(expression("output.tenant"));
    check(&emitter).expect("exact, non-null, non-sensitive values are accepted");

    let refused_as = |emitter: &CreateEmitter| {
        let refused = check(emitter).expect_err("the value is refused");
        let RegistryError::InvalidConnectorDeclaration { issue, .. } = refused.current_context()
        else {
            panic!("a refused value is a declaration refusal: {refused:?}");
        };
        issue.clone()
    };

    let mut wrong_type = kafka_emitter();
    wrong_type.envelope.timestamp = Some(expression("input.tenant"));
    assert_eq!(
        refused_as(&wrong_type),
        ConnectorDeclarationIssue::EnvelopeValue {
            site: EnvelopeValueSite::Clause(nervix_models::EnvelopeClauseKind::Timestamp),
            expected: ParseAsType::Datetime,
        }
    );

    let mut sensitive = kafka_emitter();
    sensitive.envelope.message_id = Some(expression("input.secret"));
    assert_eq!(
        refused_as(&sensitive),
        ConnectorDeclarationIssue::EnvelopeValue {
            site: EnvelopeValueSite::Clause(nervix_models::EnvelopeClauseKind::MessageId),
            expected: ParseAsType::String,
        }
    );

    let mut nullable = kafka_emitter();
    nullable.envelope.deduplication_id = Some(expression("input.maybe"));
    let issue = refused_as(&nullable);
    assert_eq!(
        issue.to_string(),
        "envelope DEDUPLICATION ID requires an exact, non-null, non-sensitive STRING value"
    );

    // Without a construction the emitter publishes its source records, so nothing is `output`.
    let mut unconstructed = kafka_emitter();
    unconstructed.construction = RouteConstruction::default();
    unconstructed.envelope.reply_to = Some(expression("output.tenant"));
    assert!(check(&unconstructed).is_err());
    unconstructed.envelope.reply_to = Some(expression("message.tenant"));
    check(&unconstructed).expect("message is the source record an emitter publishes as it is");
}

/// A RabbitMQ exchange emitter's routing key reads what an envelope value reads and computes an
/// exact, non-null, non-sensitive STRING, which the registry checks with the envelope's values and
/// names as the statement writes it.
#[test]
fn a_routing_key_is_an_exact_non_null_non_sensitive_string() {
    let domain: DomainName = named("validation");
    let schema = envelope_schema();
    let expression =
        |text: &str| nervix_plexus::parse_expression(text).expect("the case expression parses");
    let exchange_emitter = |routing_key: &str| {
        let mut emitter = kafka_emitter();
        emitter.connector = Box::new(EmitterEntity::RabbitMq(
            nervix_models::RabbitMqEmitter::Exchange(nervix_models::RabbitMqExchangeEmitter {
                mode: nervix_models::RabbitMqExchangePublishingMode::NoAck {
                    retry_policy: RetryPolicy {
                        backoff: "1s".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: named("rabbit_main"),
                exchange: nervix_models::RabbitMqExchangeName::parse("events")
                    .expect("events is an exchange name"),
                routing_key: expression(routing_key),
            }),
        ));
        emitter
    };
    let check = |emitter: &CreateEmitter| {
        validate_emitter_envelope_expressions(
            &domain,
            &ModelName::from(&emitter.name),
            &ModelIndex::new(),
            emitter,
            &schema,
            &schema,
        )
    };

    for accepted in [
        "input.tenant",
        "concat('orders.', output.tenant)",
        "leak_sensitive(input.secret)",
    ] {
        check(&exchange_emitter(accepted)).expect("an exact STRING routing key is accepted");
    }
    for refused in ["input.at", "input.secret", "input.maybe"] {
        let refused = check(&exchange_emitter(refused)).expect_err("the routing key is refused");
        let RegistryError::InvalidConnectorDeclaration { issue, .. } = refused.current_context()
        else {
            panic!("a refused routing key is a declaration refusal: {refused:?}");
        };
        assert_eq!(
            issue,
            &ConnectorDeclarationIssue::EnvelopeValue {
                site: EnvelopeValueSite::RoutingKey,
                expected: ParseAsType::String,
            }
        );
        assert_eq!(
            issue.to_string(),
            "ROUTING KEY requires an exact, non-null, non-sensitive STRING value"
        );
    }
}
