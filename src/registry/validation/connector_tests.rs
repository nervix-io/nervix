//! Validation of the source and sink contracts.
//!
//! Layer: test harness.
//!
//! - **Owns.** Assertions that each connector's client, publishing, mapping, header, timestamp and
//!   listener rules accept the current forms and refuse the rest with the owning model named.
//! - **Depends on.** The connector validation and the registry test fixtures.
//! - **Must not know.** How a connector is instantiated or driven.

use std::fs;

use nervix_models::{
    AckMode, AckWindow, AlterEmitter, AlterEmitterOperation, ClientConfigEntry, ClientName,
    CodecName, CodecWireFormat, ConsumerGroupName, CreateClientHttp, CreateClientSqs,
    CreateWireSchema, EmitterPublishingMode, ErrorPolicies, FlushPolicy, GeneralErrorPolicy,
    IngestorName, JsonType, KafkaIngestMode, KafkaOffsetMode, MaterializedRelayState,
    MessageErrorPolicy, MqttIngestMode, MqttQos, MqttSession, OtelMetric, OutputBranch,
    ProcessorInputs, ProcessorOutputs, RetryPolicy, SignalingProtobufConfig, TopicName,
    WireSchemaField, WireSchemaName,
};
use nonzero_ext::nonzero;

use super::*;
use crate::registry::{
    mutation::RegistryMutation,
    storage::Registry,
    test_fixtures::{
        TOO_LONG_DURATION_TEXT, branch, branch_for_relay, branch_schema, branch_schema_with_types,
        branched_by, client_model, codec, emitter, explicitly_unbranched_relay, ingestor_statement,
        jaq_native_codec, named, protobuf_codec, relay, relay_branched_by,
        relay_branched_by_relay_branch, schema, signaling_protocol, temp_db_path,
        unbranched_transforming_outputs, vhost, wire_schema,
    },
};

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

    emitter.publishing_mode = EmitterPublishingMode::NoAck {
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

    emitter.publishing_mode = EmitterPublishingMode::BrokerAck {
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

    emitter.publishing_mode = EmitterPublishingMode::MqttQos0 {
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
    .expect_err("foreign publishing modes must be rejected");
    assert!(format!("{error:#}").contains("KAFKA emitter does not support MODE QOS 0"));

    *emitter.sink = EmitSink::Sqs {
        client: named("sqs_main"),
        queue: "events".to_string(),
        fifo_group: Some(SqsFifoGroup::Expression(Expression::Literal(
            nervix_models::Literal::String("group".to_string()),
        ))),
    };
    emitter.publishing_mode = EmitterPublishingMode::SqsSingle {
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

    if let EmitterPublishingMode::SqsSingle { retry_policy } = &mut emitter.publishing_mode {
        retry_policy.max_backoff = "1s".to_string();
    }
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
fn publishing_contract_checks_the_batch_clause_against_its_sink() {
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        unreachable!("emitter helper must build an emitter model")
    };
    let identifier = ModelName::from(&emitter.name);
    let models = ModelIndex::new();
    emitter.body = nervix_models::EmitterBody::Values;
    emitter.publishing_mode = EmitterPublishingMode::RequestAck {
        retry_policy: RetryPolicy {
            backoff: "10ms".to_string(),
            max_backoff: "1s".to_string(),
        },
    };
    *emitter.sink = EmitSink::Postgres {
        client: named("postgres_main"),
        table: named("events"),
        values: Vec::new(),
        conflict_action: nervix_models::PostgresConflictAction::None,
    };

    let error = validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect_err("a Postgres emitter without BATCH must be rejected");
    assert!(
        format!("{error:#}").contains("POSTGRES emitters require BATCH MAX MESSAGES"),
        "unexpected error: {error:#}"
    );

    emitter.batch = Some(batch_policy(500, "8MiB"));
    validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect("a Postgres emitter with BATCH is valid");

    *emitter.sink = EmitSink::Sqs {
        client: named("sqs_main"),
        queue: "events".to_string(),
        fifo_group: None,
    };
    emitter.body = nervix_models::EmitterBody::Codec {
        codec: named("event_codec"),
    };
    emitter.publishing_mode = EmitterPublishingMode::SqsBatch {
        retry_policy: RetryPolicy {
            backoff: "10ms".to_string(),
            max_backoff: "1s".to_string(),
        },
    };
    let error = validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect_err("an SQS batch larger than one SQS message must be rejected");
    assert!(
        format!("{error:#}").contains("accept BATCH MAX SIZE up to 256KiB"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn batching_emitters_require_a_codec_container_their_sink_can_publish() {
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

    *emitter.sink = EmitSink::Sentry {
        client: named("sentry_main"),
    };
    let Model::Codec(mut json) = jaq_native_codec("event_codec", "event_schema", None, Some("."))
    else {
        unreachable!("jaq_native_codec builds a codec model")
    };
    let error = validate_emitter_batch_container(&domain, &identifier, &emitter, &json)
        .expect_err("a batching Sentry emitter needs ON EMITTING BATCH");
    assert!(
        format!("{error:#}").contains(
            "SENTRY emitter requires codec 'event_codec' to declare an ON EMITTING BATCH"
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
        expression: nervix_nspl::parse_expression("input.secret")
            .assured("input.secret is a valid expression"),
    };
    *emitter.sink = EmitSink::Postgres {
        client: named("database"),
        table: named("events"),
        values: vec![mapping],
        conflict_action: nervix_models::PostgresConflictAction::None,
    };
    let models = ModelIndex::new();
    let error =
        validate_direct_values_sensitivity(&domain, &identifier, &models, &emitter, &input_schema)
            .expect_err("a sensitive VALUES expression must be rejected");
    assert!(matches!(
        error.current_context(),
        RegistryError::SensitiveEmitterValue {
            domain: error_domain,
            emitter: error_emitter,
            sink: "POSTGRES",
            target,
        } if error_domain == &domain
            && error_emitter == &identifier
            && target == "external_secret"
    ));

    let EmitSink::Postgres { values, .. } = emitter.sink.as_mut() else {
        panic!("the test emitter uses Postgres VALUES");
    };
    values[0].expression = nervix_nspl::parse_expression("leak_sensitive(input.secret)")
        .assured("leak_sensitive(input.secret) is a valid expression");
    validate_direct_values_sensitivity(&domain, &identifier, &models, &emitter, &input_schema)
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
    let mapping = nervix_models::ClickHouseValueMapping {
        column: "external_value".to_string(),
        expression: nervix_nspl::parse_expression("input.missing")
            .expect("a missing field is still a valid expression"),
    };
    *emitter.sink = EmitSink::Postgres {
        client: named("database"),
        table: named("events"),
        values: vec![mapping],
        conflict_action: nervix_models::PostgresConflictAction::None,
    };
    let error = validate_direct_values_sensitivity(
        &domain,
        &identifier,
        &ModelIndex::new(),
        &emitter,
        &input_schema,
    )
    .expect_err("an unknown source field must fail VALUES type inference");
    assert!(matches!(
        error.current_context(),
        RegistryError::InvalidModel { domain, identifier, reason }
            if domain == "default"
                && identifier == "emit"
                && reason.contains("emitter VALUES type inference failed")
    ));
    assert!(error.contains::<nervix_vm::CompileError>());
}

#[test]
fn http_request_type_failure_keeps_the_vm_compile_cause() {
    let domain: DomainName = named("default");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        panic!("the emitter fixture constructs an emitter model");
    };
    let identifier = ModelName::from(&emitter.name);
    emitter.body = nervix_models::EmitterBody::WithoutBody;
    *emitter.sink = EmitSink::Http {
        client: named("api"),
        method: nervix_nspl::parse_expression("input.value")
            .expect("a field reference is a valid expression"),
        path: nervix_nspl::parse_expression("'/events'")
            .expect("a string literal is a valid expression"),
    };
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
fn otel_mapping_contract_validates_signal_keys_before_runtime() {
    let domain = DomainName::parse("default").expect("valid domain");
    let Model::Emitter(mut emitter) = emitter("emit", "events", "event_codec", "broker_out") else {
        unreachable!("emitter helper must build an emitter model")
    };
    let identifier = ModelName::from(&emitter.name);
    emitter.body = nervix_models::EmitterBody::Values;
    emitter.publishing_mode = EmitterPublishingMode::RequestAck {
        retry_policy: RetryPolicy {
            backoff: "10ms".to_string(),
            max_backoff: "1s".to_string(),
        },
    };
    let models = ModelIndex::new();

    *emitter.sink = EmitSink::Otel {
        client: named("otel_main"),
        signal: OtelSignal::Logs,
        values: vec![otel_mapping("time"), otel_mapping("body")],
        attributes: Vec::new(),
        resource: Vec::new(),
        scope: None,
    };
    validate_emitter_publishing_contract(&domain, &identifier, &models, &emitter)
        .expect("complete OTEL LOGS mappings must be accepted");

    let EmitSink::Otel { values, .. } = emitter.sink.as_mut() else {
        unreachable!("test emitter must remain OTEL")
    };
    values.push(otel_mapping("body"));
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

    *emitter.sink = EmitSink::Otel {
        client: named("otel_main"),
        signal: OtelSignal::Metric(OtelMetric {
            name: "requests".to_string(),
            unit: "1".to_string(),
            description: None,
            kind: OtelMetricKind::Sum {
                monotonic: true,
                temporality: OtelAggregationTemporality::Delta,
            },
        }),
        values: vec![otel_mapping("time"), otel_mapping("value")],
        attributes: Vec::new(),
        resource: Vec::new(),
        scope: None,
    };
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
                    config: vec![ClientConfigEntry {
                        key: "region".to_string(),
                        value: "us-east-1".to_string(),
                    }],
                }),
            ],
        )
        .expect("SQS FIFO validation fixtures should install");

    let Model::Emitter(mut valid) = emitter("valid_fifo", "events", "event_codec", "sqs_main")
    else {
        unreachable!("emitter helper must build an emitter model")
    };
    valid.sink = Box::new(EmitSink::Sqs {
        client: named("sqs_main"),
        queue: "events.fifo".to_string(),
        fifo_group: Some(SqsFifoGroup::Expression(
            nervix_nspl::parse_expression("input.value").expect("valid FIFO group expression"),
        )),
    });
    valid.publishing_mode = EmitterPublishingMode::SqsSingle {
        retry_policy: RetryPolicy {
            backoff: "10ms".to_string(),
            max_backoff: "1s".to_string(),
        },
    };
    registry
        .apply_batch(&domain, vec![Model::Emitter(valid.clone())])
        .expect("non-sensitive STRING FIFO expressions should be accepted");

    let mut wrong_type = valid.clone();
    wrong_type.name = named("wrong_fifo_type");
    if let EmitSink::Sqs { fifo_group, .. } = wrong_type.sink.as_mut() {
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
    if let EmitSink::Sqs { fifo_group, .. } = branch_fifo.sink.as_mut() {
        *fifo_group = Some(SqsFifoGroup::FromBranch);
    }
    registry
        .apply_batch(&domain, vec![Model::Emitter(branch_fifo)])
        .expect("FIFO GROUP FROM BRANCH should accept a wholly branched input set");

    let mut mixed_inputs = valid.clone();
    mixed_inputs.name = named("mixed_fifo_inputs");
    mixed_inputs.from.from = vec![named("tenant_events"), named("events")];
    if let EmitSink::Sqs { fifo_group, .. } = mixed_inputs.sink.as_mut() {
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
    if let EmitSink::Sqs { fifo_group, .. } = from_branch.sink.as_mut() {
        *fifo_group = Some(SqsFifoGroup::FromBranch);
    }
    let error = registry
        .apply_batch(&domain, vec![Model::Emitter(from_branch)])
        .expect_err("FROM BRANCH on unbranched input must be rejected");
    assert!(format!("{error:#}").contains("FROM BRANCH requires branched input"));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn emitter_header_invocations_are_rejected_for_unsupported_sinks() {
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
        sink: Box::new(EmitSink::ZeroMq {
            client: named("zeromq_main"),
        }),
        publishing_mode: EmitterPublishingMode::NoAck {
            retry_policy: RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        batch: None,
        flush_policy: FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string(),
        },
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        construction: nervix_nspl::parse_route_construction(
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
    assert!(format!("{error:#}").contains("ZEROMQ emitters do not support write_header"));

    *emitter.sink = EmitSink::Syslog {
        client: named("syslog_main"),
    };
    let error = super::effective_emitter_filter_map_schema(
        &domain,
        &ModelName::from(&emitter.name),
        &ModelIndex::new(),
        &emitter,
        &schema,
        &schema,
    )
    .expect_err("Syslog emitters must reject write_header");
    assert!(format!("{error:#}").contains("SYSLOG emitters do not support write_header"));

    *emitter.sink = EmitSink::Kafka {
        client: named("kafka_main"),
        topic: named("events_out"),
    };
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
            client_model("mqtt_main"),
            relay("notifications", "event_schema"),
            Model::Ingestor(CreateIngestor {
                name: IngestorName::parse("mqtt_ing").expect("valid identifier"),
                output_routes: unbranched_transforming_outputs("notifications"),
                input: nervix_models::IngestorInput::Transport(
                    nervix_models::TransportIngestorInput {
                        source: IngestSource::Mqtt {
                            client: ClientName::parse("mqtt_main").expect("valid identifier"),
                            topic: "notifications".to_string(),
                            instances: nonzero!(2u64),
                            mode: MqttIngestMode::NoAckSequential {
                                session: MqttSession::Clean,
                                qos: MqttQos::AtMostOnce,
                            },
                            quiesce: nervix_models::IngestQuiesceMode::Drop,
                        },
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
                        source: IngestSource::Kafka {
                            client: ClientName::parse("broker").expect("valid identifier"),
                            topic: TopicName::parse("notifications").expect("valid identifier"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(
                                ConsumerGroupName::parse("cg").expect("valid consumer group"),
                            ),
                            instances: nonzero!(1u64),
                            mode: KafkaIngestMode::NoAckParallel,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
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
        input: nervix_models::IngestorInput::Client(nervix_models::ClientIngestSource {
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
            emitter.publishing_mode = EmitterPublishingMode::NoAck { retry_policy };
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

        emitter.publishing_mode = EmitterPublishingMode::BrokerAck {
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
        input.source = IngestSource::Endpoint {
            endpoint: named("ingress"),
            mode: nervix_models::EndpointIngestMode::NoAckSequential,
            quiesce: nervix_models::IngestQuiesceMode::Reject {
                retry_after: retry_after.to_string(),
            },
        };
        let error = validate_ingestor_source(&domain, &identifier, &ingestor)
            .expect_err("the retry delay names no duration");
        assert_invalid_model_reason(
            &error,
            &format!("invalid quiesce REJECT RETRY AFTER duration '{retry_after}': {why}"),
        );
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
                        construction: nervix_nspl::parse_route_construction(
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
                            source: IngestSource::Kafka {
                                client: ClientName::parse("broker").expect("valid identifier"),
                                topic: TopicName::parse("notifications").expect("valid identifier"),
                                offset_mode: KafkaOffsetMode::ConsumerGroup(
                                    ConsumerGroupName::parse("cg").expect("valid consumer group"),
                                ),
                                instances: nonzero!(1u64),
                                mode: KafkaIngestMode::NoAckParallel,
                                quiesce: nervix_models::IngestQuiesceMode::Suspend,
                            },
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
                    construction: nervix_nspl::parse_route_construction(
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
                        source: IngestSource::Kafka {
                            client: ClientName::parse("broker").expect("valid identifier"),
                            topic: TopicName::parse("notifications").expect("valid identifier"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(
                                ConsumerGroupName::parse("cg").expect("valid consumer group"),
                            ),
                            instances: nonzero!(1u64),
                            mode: KafkaIngestMode::NoAckParallel,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
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
                    construction: nervix_nspl::parse_route_construction("INHERIT ALL EXCEPT value")
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
                        source: IngestSource::Kafka {
                            client: ClientName::parse("broker").expect("valid identifier"),
                            topic: TopicName::parse("notifications").expect("valid identifier"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(
                                ConsumerGroupName::parse("cg").expect("valid consumer group"),
                            ),
                            instances: nonzero!(1u64),
                            mode: KafkaIngestMode::NoAckParallel,
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
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
                where_clause: nervix_nspl::parse_expression("input.value = 'one'")
                    .expect("valid source filter"),
            },
            nervix_models::ProcessorInputWhere {
                relay: named("source_b"),
                where_clause: nervix_nspl::parse_expression("input.value = 'two'")
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
    let sink_edge = dataflow
        .edges
        .iter()
        .find(|edge| edge.target == "client_sink:broker_out")
        .expect("emitter sink edge must exist");
    assert_eq!(
        sink_edge
            .metric
            .as_ref()
            .expect("emitter sink edge must carry a metric")
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
    sentry_emitter.sink = Box::new(EmitSink::Sentry {
        client: named("sentry_main"),
    });
    sentry_emitter.publishing_mode = EmitterPublishingMode::RequestAck {
        retry_policy: RetryPolicy {
            backoff: "250ms".to_string(),
            max_backoff: "30s".to_string(),
        },
    };

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
                    config: vec![ClientConfigEntry {
                        key: "dsn".to_string(),
                        value: "https://key@sentry.example/42".to_string(),
                    }],
                }),
                relay("notifications", "event_schema"),
                Model::Emitter(sentry_emitter),
            ],
        )
        .expect_err("Sentry emitter must reject an HTTP client");

    assert!(
        format!("{error:#}")
            .contains("SENTRY emitter requires a SENTRY client, found HTTP client 'sentry_main'"),
        "unexpected error: {error:#}"
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
