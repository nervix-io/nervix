use meticulous::ResultExt as _;
use nonzero_ext::nonzero;

use crate::{
    AckWindow, AlterGenerator, AlterGeneratorError, AlterGeneratorOperation, AlterIngestor,
    AlterIngestorError, AlterIngestorOperation, AlterPlacement, AlterPlacementError,
    AlterPlacementOperation, AlterRelay, AlterRelayError, AlterRelayOperation,
    AlterTransportIngestorOperation, BranchSelection, ClientIngestMode, ClientIngestorEntity,
    ClientProducerIngestor, CodecBatchContainer, CodecJaqFormat, CodecJaqTransformations,
    CodecProtobufConfig, CodecWireFormat, CreateGenerator, CreateIngestor, CreatePlacement,
    CreateRelay, EndpointIngestMode, Expression, FlushPolicy, GeneralErrorPolicy,
    HttpEndpointIngestor, HttpIngestorEntity, IngestQuiesceMode, IngestorAlteration,
    IngestorEntityKind, KafkaIngestorEntity, KafkaTopicIngestor, Literal, MaterializedRelayState,
    ModelKind, PlacementPolicy, ProcessorOutput, ProcessorOutputs, RelayBranching, RetryPolicy,
};

fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).expect("valid name")
}

/// An HTTP endpoint source that buffers what arrives while its ingestor is quiesced.
fn http_endpoint(endpoint: &str) -> HttpEndpointIngestor {
    HttpEndpointIngestor {
        endpoint: named(endpoint),
        mode: EndpointIngestMode::NoAck,
        quiesce: IngestQuiesceMode::EndpointBuffer {
            max_size: "1MiB".to_string(),
        },
    }
}

/// The operations of an `ALTER HTTP ENDPOINT INGESTOR` statement.
fn http_endpoint_alteration(
    operations: Vec<AlterTransportIngestorOperation<HttpEndpointIngestor>>,
) -> IngestorAlteration {
    operations.into()
}

#[test]
fn branch_selection_displays_its_named_or_unbranched_declaration() {
    let name =
        crate::BranchName::parse("by_tenant").assured("by_tenant is a valid branch identifier");
    assert_eq!(BranchSelection::branched_by(name).to_string(), "by_tenant");
    assert_eq!(BranchSelection::unbranched().to_string(), "UNBRANCHED");
}

#[test]
fn model_kind_completion_labels_roundtrip() {
    for (kind, label, keyword) in [
        (ModelKind::Schema, "ref:schema", "schema"),
        (
            ModelKind::WireJsonSchema,
            "ref:wire_json_schema",
            "wire_json_schema",
        ),
        (
            ModelKind::WireCborSchema,
            "ref:wire_cbor_schema",
            "wire_cbor_schema",
        ),
        (
            ModelKind::WireAvroSchema,
            "ref:wire_avro_schema",
            "wire_avro_schema",
        ),
        (ModelKind::Codec, "ref:codec", "codec"),
        (ModelKind::Client, "ref:client", "client"),
        (ModelKind::Vhost, "ref:vhost", "vhost"),
        (ModelKind::Endpoint, "ref:endpoint", "endpoint"),
        (
            ModelKind::SignalingProtocol,
            "ref:signaling_protocol",
            "signaling_protocol",
        ),
        (ModelKind::Inferencer, "ref:inferencer", "inferencer"),
        (ModelKind::Ingestor, "ref:ingestor", "ingestor"),
        (ModelKind::Reingestor, "ref:reingestor", "reingestor"),
        (ModelKind::Relay, "ref:relay", "relay"),
        (ModelKind::Junction, "ref:junction", "junction"),
        (ModelKind::Deduplicator, "ref:deduplicator", "deduplicator"),
        (ModelKind::Emitter, "ref:emitter", "emitter"),
        (ModelKind::Placement, "ref:placement", "placement"),
        (ModelKind::Udf, "ref:udf", "udf"),
    ] {
        assert_eq!(kind.completion_label(), label);
        assert_eq!(ModelKind::from_completion_label(label), Some(kind));
        assert_eq!(kind.as_str(), keyword);
    }

    assert_eq!(ModelKind::from_completion_label("ref:unknown"), None);
}

#[test]
fn relay_alter_applies_operations_in_order_and_is_atomic() {
    let mut relay = CreateRelay {
        name: named("events"),
        schema: named("event_v1"),
        buffer: nonzero!(1usize),
        branching: RelayBranching::unbranched(),
        materialized_state: None,
    };
    relay
        .apply_alter(&AlterRelay {
            relay: named("events"),
            operations: vec![
                AlterRelayOperation::SetCapacity {
                    capacity: nonzero!(8usize),
                },
                AlterRelayOperation::SetSchema {
                    schema: named("event_v2"),
                },
                AlterRelayOperation::SetCapacity {
                    capacity: nonzero!(16usize),
                },
                AlterRelayOperation::SetMaterializedState,
            ],
        })
        .expect("relay alter should apply");
    assert_eq!(relay.buffer, nonzero!(16usize));
    assert_eq!(relay.schema, named("event_v2"));
    assert_eq!(
        relay.materialized_state,
        Some(MaterializedRelayState::LastByTimestamp)
    );

    let before = relay.clone();
    let error = relay
        .apply_alter(&AlterRelay {
            relay: named("events"),
            operations: vec![
                AlterRelayOperation::SetCapacity {
                    capacity: nonzero!(32usize),
                },
                AlterRelayOperation::DropMaterializedState,
                AlterRelayOperation::DropMaterializedState,
            ],
        })
        .expect_err("the second drop must fail");
    assert_eq!(
        error.current_context(),
        &AlterRelayError::MaterializedStateNotConfigured
    );
    assert_eq!(relay, before, "failed ALTER must not partially apply");
}

#[test]
fn relay_alter_reports_each_typed_error() {
    let relay = CreateRelay {
        name: named("events"),
        schema: named("event"),
        buffer: nonzero!(1usize),
        branching: RelayBranching::unbranched(),
        materialized_state: None,
    };
    let cases = [
        (
            AlterRelay {
                relay: named("other"),
                operations: vec![AlterRelayOperation::SetCapacity {
                    capacity: nonzero!(2usize),
                }],
            },
            AlterRelayError::RelayNameMismatch {
                stored: named("events"),
                requested: named("other"),
            },
        ),
        (
            AlterRelay {
                relay: named("events"),
                operations: vec![AlterRelayOperation::DropMaterializedState],
            },
            AlterRelayError::MaterializedStateNotConfigured,
        ),
    ];
    for (alter, expected) in cases {
        let mut candidate = relay.clone();
        assert_eq!(
            candidate
                .apply_alter(&alter)
                .expect_err("the alteration is rejected")
                .current_context(),
            &expected
        );
        assert_eq!(candidate, relay);
    }
}

#[test]
fn codec_batch_containers_name_the_program_and_the_protobuf_message() {
    let transformations = |batch: Option<&str>| CodecJaqTransformations {
        on_ingestion: None,
        on_emitting: Some(".".to_string()),
        on_emitting_batch: batch.map(str::to_string),
    };
    let protobuf = |batch_message: Option<&str>, batch: Option<&str>| {
        CodecWireFormat::Protobuf(CodecProtobufConfig {
            resource: named("bundle"),
            resource_version: 1,
            config: Vec::new(),
            message: "nervix.test.Notification".to_string(),
            batch_message: batch_message.map(str::to_string),
            transformations: transformations(batch),
        })
    };

    assert_eq!(
        CodecWireFormat::Syslog.batch_container(),
        CodecBatchContainer::Format {
            transformation: None
        }
    );
    let jaq = CodecWireFormat::JaqNative {
        format: CodecJaqFormat::Json,
        transformations: transformations(Some("{records: .}")),
    };
    assert_eq!(jaq.batch_container().transformation(), Some("{records: .}"));
    assert_eq!(
        protobuf(None, None).batch_container(),
        CodecBatchContainer::Undeclared
    );
    assert_eq!(
        protobuf(Some("nervix.test.Batch"), Some("{items: .}")).batch_container(),
        CodecBatchContainer::ProtobufMessage {
            message: "nervix.test.Batch",
            transformation: Some("{items: .}"),
        }
    );
    assert_eq!(CodecBatchContainer::Undeclared.transformation(), None);
}

#[test]
fn ingestor_alter_applies_operations_in_order_and_is_atomic() {
    let route = ProcessorOutput {
        relay: named("events"),
        construction: crate::RouteConstruction::default(),
        flush_policy: Some(FlushPolicy::Each {
            interval: "1s".to_string(),
            max_batch_size: "1MiB".to_string(),
        }),
        message_error_policy: super::MessageErrorPolicy::Log,
        branch: Some(crate::OutputBranch::Unbranched),
    };
    let mut ingestor = CreateIngestor {
        name: named("event_source"),
        output_routes: ProcessorOutputs::new(vec![route.clone()]),
        input: crate::IngestorInput::Transport(crate::TransportIngestorInput {
            source: http_endpoint("ingress_a").into(),
            codec: named("event_codec"),
        }),
        timestamp_source: None,
        general_error_policy: GeneralErrorPolicy::Log,
        filter_where: None,
    };
    ingestor
        .apply_alter(&AlterIngestor {
            ingestor: named("event_source"),
            alteration: http_endpoint_alteration(vec![
                AlterIngestorOperation::SetSource {
                    source: http_endpoint("ingress_b"),
                }
                .into(),
                AlterTransportIngestorOperation::SetDecodeUsing {
                    codec: named("next_codec"),
                },
                AlterIngestorOperation::SetTimestamp {
                    source: super::IngestTimestampSource::Now,
                }
                .into(),
                AlterIngestorOperation::SetFilterWhere {
                    where_clause: Expression::Literal(Literal::Bool(true)),
                }
                .into(),
                AlterIngestorOperation::ReplaceRoute {
                    route: ProcessorOutput {
                        relay: named("events"),
                        flush_policy: Some(FlushPolicy::Immediate),
                        ..route.clone()
                    },
                }
                .into(),
                AlterIngestorOperation::AddRoute {
                    route: ProcessorOutput {
                        relay: named("audit"),
                        ..route.clone()
                    },
                }
                .into(),
                AlterIngestorOperation::SetGeneralError {
                    policy: GeneralErrorPolicy::Ignore,
                }
                .into(),
            ]),
        })
        .expect("ingestor alter should apply");

    assert_eq!(
        ingestor.input,
        crate::IngestorInput::Transport(crate::TransportIngestorInput {
            source: http_endpoint("ingress_b").into(),
            codec: named("next_codec"),
        })
    );
    assert_eq!(
        ingestor.timestamp_source,
        Some(super::IngestTimestampSource::Now)
    );
    assert_eq!(ingestor.output_routes.routes.len(), 2);
    assert_eq!(ingestor.general_error_policy, GeneralErrorPolicy::Ignore);

    let before = ingestor.clone();
    let error = ingestor
        .apply_alter(&AlterIngestor {
            ingestor: named("event_source"),
            alteration: http_endpoint_alteration(vec![
                AlterTransportIngestorOperation::SetDecodeUsing {
                    codec: named("last_codec"),
                },
                AlterIngestorOperation::DropRoute {
                    relay: named("missing"),
                }
                .into(),
            ]),
        })
        .expect_err("missing route target should fail");
    assert_eq!(
        error.current_context(),
        &AlterIngestorError::RouteTargetNotFound {
            relay: named("missing")
        }
    );
    assert_eq!(ingestor, before, "failed ALTER must not partially apply");
}

#[test]
fn ingestor_alter_reports_name_ambiguity_and_last_route_errors() {
    let route = ProcessorOutput::new(named("events"));
    let base = CreateIngestor {
        name: named("event_source"),
        output_routes: ProcessorOutputs::new(vec![route.clone()]),
        input: crate::IngestorInput::Transport(crate::TransportIngestorInput {
            source: http_endpoint("ingress").into(),
            codec: named("event_codec"),
        }),
        timestamp_source: None,
        general_error_policy: GeneralErrorPolicy::Log,
        filter_where: None,
    };

    let mut candidate = base.clone();
    assert_eq!(
        candidate
            .apply_alter(&AlterIngestor {
                ingestor: named("other"),
                alteration: http_endpoint_alteration(Vec::new()),
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterIngestorError::IngestorNameMismatch {
            stored: named("event_source"),
            requested: named("other"),
        }
    );
    assert_eq!(candidate, base);

    let mut candidate = base.clone();
    assert_eq!(
        candidate
            .apply_alter(&AlterIngestor {
                ingestor: named("event_source"),
                alteration: http_endpoint_alteration(vec![
                    AlterIngestorOperation::DropRoute {
                        relay: named("events"),
                    }
                    .into(),
                ]),
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterIngestorError::CannotDropLastRoute
    );
    assert_eq!(candidate, base);

    let mut ambiguous = base.clone();
    ambiguous.output_routes.routes.push(route);
    let before = ambiguous.clone();
    assert_eq!(
        ambiguous
            .apply_alter(&AlterIngestor {
                ingestor: named("event_source"),
                alteration: http_endpoint_alteration(vec![
                    AlterIngestorOperation::DropRoute {
                        relay: named("events"),
                    }
                    .into(),
                ]),
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterIngestorError::RouteTargetAmbiguous {
            relay: named("events"),
        }
    );
    assert_eq!(ambiguous, before);
}

#[test]
fn ingestor_alter_applies_only_under_the_head_of_the_stored_pair() {
    let base = CreateIngestor {
        name: named("event_source"),
        output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(named("events"))]),
        input: crate::IngestorInput::Transport(crate::TransportIngestorInput {
            source: http_endpoint("ingress").into(),
            codec: named("event_codec"),
        }),
        timestamp_source: None,
        general_error_policy: GeneralErrorPolicy::Log,
        filter_where: None,
    };
    let kafka_alteration: IngestorAlteration =
        vec![AlterTransportIngestorOperation::<KafkaTopicIngestor>::from(
            AlterIngestorOperation::SetGeneralError {
                policy: GeneralErrorPolicy::Ignore,
            },
        )]
        .into();

    let mut candidate = base.clone();
    let error = candidate
        .apply_alter(&AlterIngestor {
            ingestor: named("event_source"),
            alteration: kafka_alteration,
        })
        .expect_err("a Kafka topic alteration cannot alter an HTTP endpoint ingestor");
    assert_eq!(
        error.current_context(),
        &AlterIngestorError::KindMismatch {
            stored: IngestorEntityKind::Http(HttpIngestorEntity::Endpoint),
            requested: IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic),
        }
    );
    assert_eq!(
        error.current_context().to_string(),
        "the stored ingestor reads HTTP ENDPOINT, so ALTER KAFKA TOPIC INGESTOR cannot alter it"
    );
    assert_eq!(candidate, base);

    let mut candidate = base.clone();
    let error = candidate
        .apply_alter(&AlterIngestor {
            ingestor: named("event_source"),
            alteration: http_endpoint_alteration(vec![
                AlterIngestorOperation::SetQuiesce {
                    quiesce: IngestQuiesceMode::Suspend,
                }
                .into(),
            ]),
        })
        .expect_err("an endpoint cannot suspend its callers");
    assert_eq!(
        error.current_context().to_string(),
        "HTTP ENDPOINT ingestors do not support ON QUIESCE SUSPEND"
    );
    assert_eq!(candidate, base);

    let mut candidate = base.clone();
    candidate
        .apply_alter(&AlterIngestor {
            ingestor: named("event_source"),
            alteration: http_endpoint_alteration(vec![
                AlterIngestorOperation::SetQuiesce {
                    quiesce: IngestQuiesceMode::Reject {
                        retry_after: "3s".to_string(),
                    },
                }
                .into(),
            ]),
        })
        .expect("an endpoint can refuse its callers while quiesced");
    assert_eq!(
        candidate.input.quiesce(),
        &IngestQuiesceMode::Reject {
            retry_after: "3s".to_string(),
        }
    );
}

#[test]
fn client_producer_alter_sets_its_source_and_honors_only_suspension() {
    let mode = ClientIngestMode {
        window: AckWindow::Sequential,
        ack_timeout: "5s".to_string(),
        retry_policy: RetryPolicy {
            backoff: "100ms".to_string(),
            max_backoff: "1s".to_string(),
        },
    };
    let base = CreateIngestor {
        name: named("orders_in"),
        output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(named("orders"))]),
        input: crate::IngestorInput::Client(ClientProducerIngestor {
            schema: named("order"),
            mode: mode.clone(),
        }),
        timestamp_source: None,
        general_error_policy: GeneralErrorPolicy::Log,
        filter_where: None,
    };
    let replacement = ClientProducerIngestor {
        schema: named("order_next"),
        mode,
    };

    let mut candidate = base.clone();
    candidate
        .apply_alter(&AlterIngestor {
            ingestor: named("orders_in"),
            alteration: vec![
                AlterIngestorOperation::SetSource {
                    source: replacement.clone(),
                },
                AlterIngestorOperation::SetQuiesce {
                    quiesce: IngestQuiesceMode::Suspend,
                },
            ]
            .into(),
        })
        .expect("a client producer alteration applies to a client producer ingestor");
    assert_eq!(candidate.input, crate::IngestorInput::Client(replacement));
    assert_eq!(
        candidate.input.entity_kind(),
        IngestorEntityKind::Client(ClientIngestorEntity::Producer)
    );

    let mut candidate = base.clone();
    let error = candidate
        .apply_alter(&AlterIngestor {
            ingestor: named("orders_in"),
            alteration: vec![
                AlterIngestorOperation::<ClientProducerIngestor>::SetQuiesce {
                    quiesce: IngestQuiesceMode::Drop,
                },
            ]
            .into(),
        })
        .expect_err("a client producer keeps unsent batches with its producer");
    assert_eq!(
        error.current_context().to_string(),
        "CLIENT PRODUCER ingestors do not support ON QUIESCE DROP"
    );
    assert_eq!(candidate, base);
}

#[test]
fn generator_alter_is_ordered_atomic_and_reports_route_errors() {
    let route = ProcessorOutput::new(named("outgoing"));
    let mut generator = CreateGenerator {
        name: named("synth"),
        materialized_relay: named("state"),
        branched_by: BranchSelection::unbranched(),
        each: "1s"
            .parse()
            .assured("the fixture cadence is a positive duration"),
        output_routes: ProcessorOutputs::new(vec![route.clone()]),
    };
    generator
        .apply_alter(&AlterGenerator {
            generator: named("synth"),
            operations: vec![
                AlterGeneratorOperation::SetEach {
                    each: "500ms"
                        .parse()
                        .assured("the fixture cadence is a positive duration"),
                },
                AlterGeneratorOperation::SetEach {
                    each: "250ms"
                        .parse()
                        .assured("the fixture cadence is a positive duration"),
                },
                AlterGeneratorOperation::SetMaterializedState {
                    relay: named("state_v2"),
                },
                AlterGeneratorOperation::AddRoute {
                    route: ProcessorOutput::new(named("audit")),
                },
            ],
        })
        .expect("generator alter should apply");
    assert_eq!(generator.each.to_string(), "250ms");
    assert_eq!(generator.materialized_relay, named("state_v2"));
    assert_eq!(generator.output_routes.routes.len(), 2);

    let before = generator.clone();
    assert_eq!(
        generator
            .apply_alter(&AlterGenerator {
                generator: named("synth"),
                operations: vec![
                    AlterGeneratorOperation::SetEach {
                        each: "10ms"
                            .parse()
                            .assured("the fixture cadence is a positive duration"),
                    },
                    AlterGeneratorOperation::DropRoute {
                        relay: named("missing"),
                    },
                ],
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterGeneratorError::RouteTargetNotFound {
            relay: named("missing")
        }
    );
    assert_eq!(generator, before, "failed ALTER must not partially apply");
    assert_eq!(
        generator
            .apply_alter(&AlterGenerator {
                generator: named("other"),
                operations: Vec::new(),
            })
            .expect_err("the alteration names another generator")
            .current_context(),
        &AlterGeneratorError::GeneratorNameMismatch {
            stored: named("synth"),
            requested: named("other"),
        }
    );

    let mut single = CreateGenerator {
        output_routes: ProcessorOutputs::new(vec![route.clone()]),
        ..generator
    };
    assert_eq!(
        single
            .apply_alter(&AlterGenerator {
                generator: named("synth"),
                operations: vec![AlterGeneratorOperation::DropRoute {
                    relay: named("outgoing"),
                }],
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterGeneratorError::CannotDropLastRoute
    );

    single.output_routes.routes.push(route);
    assert_eq!(
        single
            .apply_alter(&AlterGenerator {
                generator: named("synth"),
                operations: vec![AlterGeneratorOperation::DropRoute {
                    relay: named("outgoing"),
                }],
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterGeneratorError::RouteTargetAmbiguous {
            relay: named("outgoing")
        }
    );
}

#[test]
fn placement_creation_collapses_duplicate_members() {
    let placement = CreatePlacement::new(
        named("corridor"),
        vec![named("ingest"), named("ingest")],
        vec![named("emit"), named("emit")],
        PlacementPolicy::PreferColocation,
        None,
    )
    .expect("placement should be valid");

    assert_eq!(placement.from, vec![named("ingest")]);
    assert_eq!(placement.to, vec![named("emit")]);
}

#[test]
fn placement_alter_applies_operations_in_order_and_is_atomic() {
    let mut placement = CreatePlacement::new(
        named("corridor"),
        vec![named("ingest")],
        vec![named("emit")],
        PlacementPolicy::PreferColocation,
        None,
    )
    .expect("placement should be valid");
    placement
        .apply_alter(&AlterPlacement {
            placement: named("corridor"),
            operations: vec![
                AlterPlacementOperation::SetRank {
                    rank: nonzero!(3u64),
                },
                AlterPlacementOperation::SetRank {
                    rank: nonzero!(1u64),
                },
                AlterPlacementOperation::SetPolicy {
                    policy: PlacementPolicy::RequireColocation,
                },
                AlterPlacementOperation::SetMembers {
                    from: vec![named("source"), named("source")],
                    to: vec![named("emitter")],
                },
                AlterPlacementOperation::RenameTo {
                    name: named("critical"),
                },
            ],
        })
        .expect("placement alter should apply");

    assert_eq!(placement.name, named("critical"));
    assert_eq!(placement.rank, Some(nonzero!(1u64)));
    assert_eq!(placement.policy, PlacementPolicy::RequireColocation);
    assert_eq!(placement.from, vec![named("source")]);
    assert_eq!(placement.to, vec![named("emitter")]);

    let before = placement.clone();
    assert_eq!(
        placement
            .apply_alter(&AlterPlacement {
                placement: named("other"),
                operations: vec![AlterPlacementOperation::SetPolicy {
                    policy: PlacementPolicy::Neutral,
                }],
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterPlacementError::PlacementNameMismatch {
            stored: named("critical"),
            requested: named("other"),
        }
    );
    assert_eq!(placement, before, "failed ALTER must not partially apply");
}

#[test]
fn a_placement_without_members_on_either_side_is_refused() {
    let error = CreatePlacement::new(
        named("corridor"),
        Vec::new(),
        vec![named("emit")],
        PlacementPolicy::Neutral,
        None,
    )
    .expect_err("a placement needs a FROM member");
    assert_eq!(error.current_context(), &AlterPlacementError::EmptyFrom);

    let error = CreatePlacement::new(
        named("corridor"),
        vec![named("ingest")],
        Vec::new(),
        PlacementPolicy::Neutral,
        None,
    )
    .expect_err("a placement needs a TO member");
    assert_eq!(error.current_context(), &AlterPlacementError::EmptyTo);

    let mut placement = CreatePlacement::new(
        named("corridor"),
        vec![named("ingest")],
        vec![named("emit")],
        PlacementPolicy::Neutral,
        None,
    )
    .expect("placement should be valid");
    let before = placement.clone();
    let error = placement
        .apply_alter(&AlterPlacement {
            placement: named("corridor"),
            operations: vec![
                AlterPlacementOperation::SetPolicy {
                    policy: PlacementPolicy::RequireColocation,
                },
                AlterPlacementOperation::SetMembers {
                    from: vec![named("source")],
                    to: Vec::new(),
                },
            ],
        })
        .expect_err("an alteration may not empty the TO members");
    assert_eq!(error.current_context(), &AlterPlacementError::EmptyTo);
    assert_eq!(placement, before, "failed ALTER must not partially apply");
}
