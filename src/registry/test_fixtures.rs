//! Fixtures the registry's unit tests share.
//!
//! A fixture lives here only when tests in more than one registry module build the
//! same value: the Models a batch is assembled from, the schemas they reference, and
//! the temporary store a test opens. A fixture used by one module belongs in that
//! module's own test module instead.

use std::{
    fs,
    num::NonZeroU64,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use nervix_models::{
    AckMode, Assignment, AssignmentTarget, AssignmentTargetScope, BranchName, BranchSelection,
    ClientConfigEntry, ClientName, CodecEncoding, CodecEncodingRule, CodecJaqFormat,
    CodecJaqTransformations, CodecName, CodecProtobufConfig, CodecWireFormat, ConsumerGroupName,
    CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy, CreateBranch,
    CreateClientKafka, CreateClientSyslog, CreateCodec, CreateCorrelator, CreateDeduplicator,
    CreateEmitter, CreateIngestor, CreateJunction, CreatePlacement, CreateReingestor, CreateRelay,
    CreateSchema, CreateSignalingProtocol, CreateVhost, CreateWasmProcessor, CreateWindowProcessor,
    CreateWireSchema, DeduplicatorName, DomainName, DomainSchedule, EmitSink, EmitterName,
    EmitterPublishingMode, EndpointName, ErrorPolicies, Expression, FieldName, FieldReference,
    FieldScope, FlushPolicy, GeneralErrorPolicy, IngestSource, Inheritance, JsonType, JunctionName,
    KafkaConfigEntry, KafkaIngestMode, KafkaOffsetMode, MaterializedRelayState, Model, ModelKind,
    ModelName, NodeRef, OutputBranch, ParseAsType, PlacementPolicy, ProcessorInputs,
    ProcessorOutputs, ReingestorName, RelayBranching, RelayName, RetryPolicy, ScheduledNode,
    SchemaField, SchemaName, SignalingProtocolOnConnect, SignalingStep, SignalingWaitStep,
    SignalingWireFormat, TopicName, VhostName, WindowBound, WindowProcessorName, WireSchemaField,
    WireSchemaName,
};
use nonzero_ext::nonzero;

use crate::registry::storage::Registry;

pub(in crate::registry) fn temp_db_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("nervix-server-registry-test-{nanos}"))
}

pub(in crate::registry) fn sample_transport_model(name: &str) -> Model {
    Model::ClientKafka(CreateClientKafka {
        name: ClientName::parse(name).expect("valid identifier"),
        mount: None,
        config: vec![KafkaConfigEntry {
            key: "bootstrap.servers".to_string(),
            value: "localhost:9092".to_string(),
        }],
    })
}

pub(in crate::registry) fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).expect("valid name")
}

pub(in crate::registry) fn branch_name_for_relay(relay: &str) -> BranchName {
    named(&format!("by_{relay}"))
}

pub(in crate::registry) fn branched_by(relay: &str, fields: &[&str]) -> OutputBranch {
    OutputBranch::BranchedBy {
        branch: branch_name_for_relay(relay),
        assignments: fields
            .iter()
            .map(|field| Assignment {
                target: AssignmentTarget {
                    scope: AssignmentTargetScope::Bare,
                    field: named(field),
                },
                value: Expression::Field(FieldReference::scoped(FieldScope::Message, named(field))),
            })
            .collect(),
    }
}

pub(in crate::registry) fn with_output_branch(
    mut outputs: ProcessorOutputs,
    branch: OutputBranch,
) -> ProcessorOutputs {
    for output in &mut outputs.routes {
        output.branch = Some(branch.clone());
    }
    outputs
}

pub(in crate::registry) fn with_processor_branching(mut model: Model) -> Model {
    match &mut model {
        Model::Deduplicator(processor) => {
            let branch = processor
                .from
                .relays()
                .first()
                .expect("processor helper requires at least one input");
            processor.branched_by =
                BranchSelection::branched_by(branch_name_for_relay(branch.as_str()));
        }
        Model::Correlator(processor) => {
            let branch = processor
                .left
                .relays()
                .first()
                .expect("processor helper requires at least one input");
            processor.branched_by =
                BranchSelection::branched_by(branch_name_for_relay(branch.as_str()));
        }
        Model::Junction(processor) => {
            let branch = processor
                .from
                .relays()
                .first()
                .expect("processor helper requires at least one input");
            processor.branched_by =
                BranchSelection::branched_by(branch_name_for_relay(branch.as_str()));
        }
        Model::WindowProcessor(processor) => {
            let branch = processor
                .from
                .relays()
                .first()
                .expect("processor helper requires at least one input");
            processor.branched_by =
                BranchSelection::branched_by(branch_name_for_relay(branch.as_str()));
        }
        _ => panic!("model is not a branch-preserving processor"),
    }
    model
}

pub(in crate::registry) fn with_inherit_all(mut outputs: ProcessorOutputs) -> ProcessorOutputs {
    for output in &mut outputs.routes {
        output.construction.inherit = Some(Inheritance::All);
    }
    outputs
}

pub(in crate::registry) fn unbranched_transforming_outputs(relay: &str) -> ProcessorOutputs {
    with_output_branch(
        with_inherit_all(ProcessorOutputs::single(named(relay))).with_flush_policy(
            FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
        ),
        OutputBranch::Unbranched,
    )
}

pub(in crate::registry) fn branch_schema(name: &str, fields: &[&str]) -> Model {
    Model::Schema(CreateSchema {
        name: named(name),
        fields: fields
            .iter()
            .map(|field| SchemaField {
                name: named(field),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            })
            .collect(),
    })
}

pub(in crate::registry) fn branch(name: &str, schema: &str) -> Model {
    Model::Branch(CreateBranch {
        name: named(name),
        schema: named(schema),
        ttl: "5m".to_string(),
        eviction: None,
    })
}

pub(in crate::registry) fn branch_for_relay(relay: &str, schema: &str) -> Model {
    Model::Branch(CreateBranch {
        name: branch_name_for_relay(relay),
        schema: named(schema),
        ttl: "5m".to_string(),
        eviction: None,
    })
}

pub(in crate::registry) fn branch_schema_with_types(
    name: &str,
    fields: &[(&str, ParseAsType)],
) -> Model {
    Model::Schema(CreateSchema {
        name: named(name),
        fields: fields
            .iter()
            .map(|(field, ty)| SchemaField {
                name: named(field),
                ty: ty.clone(),
                optional: false,
                sensitive: false,
            })
            .collect(),
    })
}

pub(in crate::registry) fn schema(name: &str) -> Model {
    Model::Schema(CreateSchema {
        name: SchemaName::parse(name).expect("valid identifier"),
        fields: vec![SchemaField {
            name: FieldName::parse("value").expect("valid identifier"),
            ty: nervix_models::ParseAsType::String,
            optional: false,
            sensitive: false,
        }],
    })
}

pub(in crate::registry) fn wire_schema(name: &str) -> Model {
    Model::WireJsonSchema(CreateWireSchema {
        name: WireSchemaName::parse(name).expect("valid identifier"),
        strictness: Default::default(),
        fields: vec![WireSchemaField {
            name: FieldName::parse("value").expect("valid identifier"),
            ty: JsonType::String,
            optional: false,
        }],
    })
}

pub(in crate::registry) fn json_wire_schema_with_type(name: &str, field_type: JsonType) -> Model {
    Model::WireJsonSchema(CreateWireSchema {
        name: named(name),
        strictness: Default::default(),
        fields: vec![WireSchemaField {
            name: named("value"),
            ty: field_type,
            optional: false,
        }],
    })
}

pub(in crate::registry) fn avro_wire_schema_with_type(
    name: &str,
    field_type: nervix_models::AvroType,
) -> Model {
    Model::WireAvroSchema(CreateWireSchema {
        name: named(name),
        strictness: Default::default(),
        fields: vec![WireSchemaField {
            name: named("value"),
            ty: field_type,
            optional: false,
        }],
    })
}

pub(in crate::registry) fn client_model(name: &str) -> Model {
    sample_transport_model(name)
}

pub(in crate::registry) fn vhost(name: &str, hostnames: &[&str]) -> Model {
    Model::Vhost(CreateVhost {
        name: VhostName::parse(name).expect("valid identifier"),
        hostnames: hostnames
            .iter()
            .map(|hostname| (*hostname).to_string())
            .collect(),
        tls: None,
    })
}

pub(in crate::registry) fn endpoint(
    name: &str,
    vhost_name: &str,
    path: &str,
    endpoint_type: nervix_models::EndpointType,
) -> Model {
    Model::Endpoint(nervix_models::CreateEndpoint {
        name: EndpointName::parse(name).expect("valid identifier"),
        on_vhost: VhostName::parse(vhost_name).expect("valid identifier"),
        path: path.to_string(),
        endpoint_type,
        signaling_protocol: None,
    })
}

pub(in crate::registry) fn codec(name: &str, schema: &str) -> Model {
    Model::Codec(CreateCodec {
        name: CodecName::parse(name).expect("valid identifier"),
        wire_format: CodecWireFormat::Json {
            wire_schema: WireSchemaName::parse("event_wire").expect("valid identifier"),
        },
        schema: SchemaName::parse(schema).expect("valid identifier"),
        encoding_rules: Vec::new(),
    })
}

pub(in crate::registry) fn syslog_codec(name: &str, schema: &str) -> Model {
    Model::Codec(CreateCodec {
        name: named(name),
        wire_format: CodecWireFormat::Syslog,
        schema: named(schema),
        encoding_rules: Vec::new(),
    })
}

pub(in crate::registry) fn syslog_client(name: &str) -> Model {
    Model::ClientSyslog(CreateClientSyslog {
        name: named(name),
        mount: None,
        config: vec![ClientConfigEntry {
            key: "protocol".to_string(),
            value: "udp".to_string(),
        }],
    })
}

pub(in crate::registry) fn avro_codec(name: &str, wire_schema: &str, schema: &str) -> Model {
    Model::Codec(CreateCodec {
        name: named(name),
        wire_format: CodecWireFormat::Avro {
            wire_schema: named(wire_schema),
        },
        schema: named(schema),
        encoding_rules: Vec::new(),
    })
}

pub(in crate::registry) fn jaq_native_codec(
    name: &str,
    schema: &str,
    on_ingestion: Option<&str>,
    on_emitting: Option<&str>,
) -> Model {
    Model::Codec(CreateCodec {
        name: named(name),
        wire_format: CodecWireFormat::JaqNative {
            format: CodecJaqFormat::Json,
            transformations: CodecJaqTransformations {
                on_ingestion: on_ingestion.map(str::to_string),
                on_emitting: on_emitting.map(str::to_string),
            },
        },
        schema: named(schema),
        encoding_rules: Vec::new(),
    })
}

pub(in crate::registry) fn protobuf_codec(
    name: &str,
    schema: &str,
    on_ingestion: Option<&str>,
    on_emitting: Option<&str>,
) -> Model {
    Model::Codec(CreateCodec {
        name: named(name),
        wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
            resource: named("proto_bundle"),
            resource_version: Some(1),
            config: vec![ClientConfigEntry {
                key: "file".to_string(),
                value: "notification.proto".to_string(),
            }],
            message: "nervix.test.Notification".to_string(),
            transformations: CodecJaqTransformations {
                on_ingestion: on_ingestion.map(str::to_string),
                on_emitting: on_emitting.map(str::to_string),
            },
        }),
        schema: named(schema),
        encoding_rules: Vec::new(),
    })
}

pub(in crate::registry) fn rfc3339_json_codec_for_field(
    name: &str,
    wire_schema: &str,
    schema: &str,
    field: &str,
) -> Model {
    Model::Codec(CreateCodec {
        name: named(name),
        wire_format: CodecWireFormat::Json {
            wire_schema: named(wire_schema),
        },
        schema: named(schema),
        encoding_rules: vec![CodecEncodingRule {
            field: named(field),
            encoding: CodecEncoding::Rfc3339,
        }],
    })
}

pub(in crate::registry) fn ingestor(name: &str, into: &str, codec: &str, client: &str) -> Model {
    let Model::Ingestor(mut ingestor) = ingestor_with_params(name, into, codec, client, &[]) else {
        unreachable!("ingestor helper must build an ingestor model")
    };
    for output in &mut ingestor.output_routes.routes {
        output.branch = Some(OutputBranch::Unbranched);
    }
    Model::Ingestor(ingestor)
}

pub(in crate::registry) fn unbranched_ingestor(
    name: &str,
    into: &str,
    codec: &str,
    client: &str,
) -> Model {
    ingestor(name, into, codec, client)
}

pub(in crate::registry) fn ingestor_with_params(
    name: &str,
    into: &str,
    codec: &str,
    client: &str,
    branch_fields: &[&str],
) -> Model {
    let branch = if branch_fields.is_empty() {
        OutputBranch::Unbranched
    } else {
        branched_by(into, branch_fields)
    };
    Model::Ingestor(CreateIngestor {
        name: named(name),
        output_routes: with_output_branch(
            with_inherit_all(ProcessorOutputs::single(named(into))).with_flush_policy(
                FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                },
            ),
            branch,
        ),
        decode_using_codec: named(codec),
        timestamp_source: None,
        source: IngestSource::Kafka {
            client: ClientName::parse(client).expect("valid identifier"),
            topic: TopicName::parse("notifications").expect("valid identifier"),
            offset_mode: KafkaOffsetMode::ConsumerGroup(
                ConsumerGroupName::parse("cg").expect("valid consumer group"),
            ),
            instances: nonzero!(1u64),
            mode: KafkaIngestMode::AckSequential {
                timeout: "30s".to_string(),
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "200ms".to_string(),
                    max_backoff: "5s".to_string(),
                },
            },
            quiesce: nervix_models::IngestQuiesceMode::Suspend,
        },
        general_error_policy: GeneralErrorPolicy::Log,

        filter_where: None,
    })
}

pub(in crate::registry) fn relay(name: &str, schema: &str) -> Model {
    Model::Relay(CreateRelay {
        name: RelayName::parse(name).expect("valid identifier"),
        schema: SchemaName::parse(schema).expect("valid identifier"),
        buffer: nonzero!(1usize),
        branching: RelayBranching::unbranched(),
        materialized_state: None,
    })
}

pub(in crate::registry) fn relay_branched_by(name: &str, schema: &str, branch: &str) -> Model {
    let Model::Relay(mut relay) = relay(name, schema) else {
        unreachable!("relay helper must build a relay model")
    };
    relay.branching = RelayBranching::branched_by(named(branch));
    Model::Relay(relay)
}

pub(in crate::registry) fn relay_branched_by_relay_branch(name: &str, schema: &str) -> Model {
    let Model::Relay(mut relay) = relay(name, schema) else {
        unreachable!("relay helper must build a relay model")
    };
    relay.branching = RelayBranching::branched_by(branch_name_for_relay(name));
    Model::Relay(relay)
}

pub(in crate::registry) fn relay_branched_like(
    name: &str,
    schema: &str,
    source_relay: &str,
) -> Model {
    let Model::Relay(mut relay) = relay(name, schema) else {
        unreachable!("relay helper must build a relay model")
    };
    relay.branching = RelayBranching::branched_by(branch_name_for_relay(source_relay));
    Model::Relay(relay)
}

pub(in crate::registry) fn materialized_relay(name: &str, schema: &str) -> Model {
    Model::Relay(CreateRelay {
        name: RelayName::parse(name).expect("valid identifier"),
        schema: SchemaName::parse(schema).expect("valid identifier"),
        buffer: nonzero!(1usize),
        branching: RelayBranching::branched_by(branch_name_for_relay(name)),
        materialized_state: Some(MaterializedRelayState::LastByTimestamp),
    })
}

pub(in crate::registry) fn explicitly_unbranched_relay(name: &str, schema: &str) -> Model {
    let Model::Relay(mut relay) = relay(name, schema) else {
        unreachable!("relay helper must build a relay model")
    };
    relay.branching = RelayBranching::unbranched();
    Model::Relay(relay)
}

pub(in crate::registry) fn processor(name: &str, from_relay: &str, into_relay: &str) -> Model {
    deduplicator(
        name,
        from_relay,
        into_relay,
        &format!("{from_relay}.value"),
        "10m",
    )
}

pub(in crate::registry) fn wasm_processor(name: &str, from_relay: &str, into_relay: &str) -> Model {
    Model::WasmProcessor(CreateWasmProcessor {
        name: named(name),
        from: ProcessorInputs::single(named(from_relay)),
        output_routes: {
            let mut outputs = ProcessorOutputs::single(named(into_relay));
            outputs.routes[0].construction =
                nervix_nspl::parse_route_construction("SET value = value")
                    .expect("generated route construction must parse");
            outputs
        },
        branched_by: BranchSelection::unbranched(),
        resource: named("wasm_filter"),
        resource_version: Some(1),
        file: "processors/filter_even.wasm".to_string(),
        limits: nervix_models::WasmProcessorLimits {
            max_fuel: nonzero!(1_000_000_000u64),
            max_memory_bytes: nonzero!(67_108_864u64),
        },
        global_error_policy: GeneralErrorPolicy::Log,
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    })
}

pub(in crate::registry) fn unbranched_correlator(
    name: &str,
    left_relay: &str,
    right_relay: &str,
    into_relay: &str,
) -> Model {
    let mut output_routes =
        (ProcessorOutputs::single(named(into_relay))).with_flush_policy(FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string(),
        });
    output_routes.routes[0].construction =
        nervix_nspl::parse_route_construction("SET value = left.value")
            .expect("route construction must parse");
    Model::Correlator(CreateCorrelator {
        name: named(name),
        left: ProcessorInputs::single(named(left_relay)),
        right: ProcessorInputs::single(named(right_relay)),
        output_routes,
        branched_by: BranchSelection::unbranched(),
        correlate_where: nervix_nspl::parse_expression("left.value = right.value")
            .expect("correlator expression must parse"),
        match_policy: CorrelatorMatchPolicy::Earliest,
        max_time: "5s".to_string(),
        timeout_policy: CorrelationTimeoutPolicy {
            left: CorrelationTimeoutAction::Drop,
            right: CorrelationTimeoutAction::Drop,
        },
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    })
}

pub(in crate::registry) fn window_processor(
    name: &str,
    from_relay: &str,
    into_relay: &str,
    construction: &str,
) -> Model {
    let mut output_routes =
        ProcessorOutputs::single(RelayName::parse(into_relay).expect("valid identifier"));
    output_routes.routes[0].construction = nervix_nspl::parse_route_construction(construction)
        .expect("window route construction must parse");
    Model::WindowProcessor(CreateWindowProcessor {
        name: WindowProcessorName::parse(name).expect("valid identifier"),
        from: ProcessorInputs::single(RelayName::parse(from_relay).expect("valid identifier")),
        output_routes,
        branched_by: BranchSelection::branched_by(branch_name_for_relay(from_relay)),
        width: WindowBound {
            messages: Some(10),
            duration: None,
        },
        step: WindowBound {
            messages: Some(5),
            duration: None,
        },
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    })
}

pub(in crate::registry) fn junction(name: &str, from_relays: &[&str], into_relay: &str) -> Model {
    Model::Junction(CreateJunction {
        name: JunctionName::parse(name).expect("valid identifier"),
        from: ProcessorInputs::new(
            from_relays
                .iter()
                .map(|stream| RelayName::parse(stream).expect("valid identifier"))
                .collect(),
            Vec::new(),
        ),
        output_routes: with_inherit_all(ProcessorOutputs::single(
            RelayName::parse(into_relay).expect("valid identifier"),
        ))
        .with_flush_policy(FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string(),
        }),
        branched_by: BranchSelection::branched_by(branch_name_for_relay(
            from_relays
                .first()
                .expect("junction helper requires at least one input"),
        )),
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    })
}

pub(in crate::registry) fn deduplicator(
    name: &str,
    from_relay: &str,
    into_relay: &str,
    field: &str,
    max_time: &str,
) -> Model {
    Model::Deduplicator(CreateDeduplicator {
        name: DeduplicatorName::parse(name).expect("valid identifier"),
        from: ProcessorInputs::single(RelayName::parse(from_relay).expect("valid identifier")),
        output_routes: with_inherit_all(ProcessorOutputs::single(
            RelayName::parse(into_relay).expect("valid identifier"),
        ))
        .with_flush_policy(FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string(),
        }),
        branched_by: BranchSelection::branched_by(branch_name_for_relay(from_relay)),
        deduplicate_on: vec![
            nervix_nspl::parse_expression(&field.replace(&format!("{from_relay}."), "input."))
                .expect("deduplicate expression must parse"),
        ],
        max_time: max_time.to_string(),
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    })
}

pub(in crate::registry) fn reingestor(
    name: &str,
    from_relay: &str,
    into_relay: &str,
    params: &[&str],
) -> Model {
    let branch = if params.is_empty() {
        OutputBranch::Unbranched
    } else {
        branched_by(into_relay, params)
    };
    Model::Reingestor(CreateReingestor {
        name: ReingestorName::parse(name).expect("valid identifier"),
        from: ProcessorInputs::single(RelayName::parse(from_relay).expect("valid identifier")),
        output_routes: with_output_branch(
            with_inherit_all(ProcessorOutputs::single(
                RelayName::parse(into_relay).expect("valid identifier"),
            ))
            .with_flush_policy(FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string(),
            }),
            branch,
        ),
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    })
}

pub(in crate::registry) fn emitter(
    name: &str,
    from_relay: &str,
    codec: &str,
    client: &str,
) -> Model {
    Model::Emitter(CreateEmitter {
        name: EmitterName::parse(name).expect("valid identifier"),
        from: ProcessorInputs::single(RelayName::parse(from_relay).expect("valid identifier")),
        encode_using_codec: Some(CodecName::parse(codec).expect("valid identifier")),
        sink: Box::new(EmitSink::Kafka {
            client: ClientName::parse(client).expect("valid identifier"),
            topic: TopicName::parse("topic").expect("valid topic identifier"),
        }),
        publishing_mode: EmitterPublishingMode::NoAck {
            retry_policy: RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        flush_policy: FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string(),
        },
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),

        construction: nervix_models::RouteConstruction {
            inherit: Some(Inheritance::All),
            ..nervix_models::RouteConstruction::default()
        },
        materialized_state: Vec::new(),
    })
}

pub(in crate::registry) fn signaling_protocol(
    format: SignalingWireFormat,
    send_programs: &[&str],
    wait_matchers: &[&str],
    fail_matchers: &[&str],
) -> CreateSignalingProtocol {
    CreateSignalingProtocol {
        name: named("handshake"),
        format,
        on_connect: SignalingProtocolOnConnect {
            accept_data: false,
            steps: vec![
                SignalingStep::Send(send_programs.iter().map(|p| p.to_string()).collect()),
                SignalingStep::Wait(SignalingWaitStep::new(
                    wait_matchers.iter().map(|p| p.to_string()).collect(),
                )),
            ],
            fail_matchers: fail_matchers.iter().map(|p| p.to_string()).collect(),
            timeout: "5s".to_string(),
        },
    }
}

pub(in crate::registry) fn scheduled_node<'a>(
    schedule: &'a DomainSchedule,
    kind: ModelKind,
    identifier: &str,
) -> &'a ScheduledNode {
    let identity = NodeRef::new(kind, named::<ModelName>(identifier));
    schedule
        .nodes
        .get(&identity)
        .unwrap_or_else(|| panic!("missing scheduled node {kind:?}:{identifier}"))
}

pub(in crate::registry) fn full_graph_batch() -> Vec<Model> {
    vec![
        schema("event_schema"),
        branch_schema("value_branch", &["value"]),
        branch_for_relay("notifications", "value_branch"),
        wire_schema("event_wire"),
        codec("event_codec", "event_schema"),
        client_model("broker_in"),
        client_model("broker_out"),
        relay_branched_by_relay_branch("notifications", "event_schema"),
        relay_branched_like("p99", "event_schema", "notifications"),
        ingestor_with_params(
            "ing",
            "notifications",
            "event_codec",
            "broker_in",
            &["value"],
        ),
        processor("p99_proc", "notifications", "p99"),
        emitter("emit", "p99", "event_codec", "broker_out"),
    ]
}

pub(in crate::registry) fn placement(
    name: &str,
    from: &[&str],
    to: &[&str],
    policy: PlacementPolicy,
    rank: Option<NonZeroU64>,
) -> Model {
    Model::Placement(
        CreatePlacement::new(
            named(name),
            from.iter().map(|member| named(member)).collect(),
            to.iter().map(|member| named(member)).collect(),
            policy,
            rank,
        )
        .expect("placement helper must build a valid placement"),
    )
}

pub(in crate::registry) fn example_graph_models(
    name: &str,
    source: &str,
) -> (DomainName, Vec<nervix_models::Model>) {
    let statements = nervix_nspl::client_statement::parse_client_statement_sources(source)
        .unwrap_or_else(|error| panic!("{name} example should parse: {error:?}"));
    let mut domain = DomainName::parse("default").expect("valid domain");
    let mut models = Vec::new();

    for parsed in statements {
        match parsed.statement {
            nervix_nspl::client_statement::ClientStatement::UseDomain(next) => {
                domain = next;
            }
            nervix_nspl::client_statement::ClientStatement::UploadResource(_)
            | nervix_nspl::client_statement::ClientStatement::BeginTransaction
            | nervix_nspl::client_statement::ClientStatement::CommitTransaction
            | nervix_nspl::client_statement::ClientStatement::RevertTransaction
            | nervix_nspl::client_statement::ClientStatement::CreateSubscription(_)
            | nervix_nspl::client_statement::ClientStatement::DeleteSubscription(_) => {}
            nervix_nspl::client_statement::ClientStatement::Server(statement) => match statement {
                nervix_models::Statement::CreateDomain(create) => {
                    domain = create.body.id;
                }
                nervix_models::Statement::Create(create) => {
                    models.push(*create.body);
                }
                nervix_models::Statement::CreateResource(_)
                | nervix_models::Statement::UploadResource(_)
                | nervix_models::Statement::StartDomain(_) => {}
                other => panic!("unexpected {name} example statement: {other:?}"),
            },
            other => panic!("unexpected {name} example client statement: {other:?}"),
        }
    }

    (domain, models)
}

pub(in crate::registry) fn assert_example_graph_validates(name: &str, source: &str) {
    let (domain, models) = example_graph_models(name, source);
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    registry
        .apply_batch(&domain, models)
        .unwrap_or_else(|error| panic!("{name} example graph should validate: {error:?}"));

    let _ = fs::remove_dir_all(path);
}
