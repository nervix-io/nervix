//! Registry storage and mutation owner tests.
//!
//! Layer: test harness.
//! - **Owns.** Persisted-model, atomic mutation and current archive validation assertions.
//! - **Depends on.** The registry storage owner and graph fixtures.
//! - **Must not know.** Production task scheduling or external connector behavior.

use std::{collections::BTreeSet, fs};

use meticulous::ResultExt as _;
use nervix_models::{
    AckMode, AlterEmitter, AlterJunction, AlterProcessorOperation, AlterRelay, AlterRelayOperation,
    AlterSchema, AlterSchemaError, AlterSchemaOperation, AlterWireSchema, AlterWireSchemaOperation,
    BranchSelection, ClientName, ClusterNodeName, CodecWireFormat, CreateWireSchema, DropModel,
    EmitterName, FlushPolicy, IngestorName, JsonType, MaterializedRelayState, ParseAsType,
    ProcessorInputs, ProcessorOutputs, QuiesceLevel, RelayName, SchemaField, SchemaName,
    WireSchemaField,
};
use nonzero_ext::nonzero;

use super::*;
use crate::registry::test_fixtures::{
    assert_example_graph_validates, branch_for_relay, branch_schema, client_model, codec, emitter,
    full_graph_batch, ingestor, ingestor_with_params, named, processor, relay,
    relay_branched_by_relay_branch, relay_branched_like, sample_transport_model, schema,
    temp_db_path, wire_schema, with_inherit_all,
};

#[test]
fn runnable_example_graphs_validate() {
    assert_example_graph_validates("iot", include_str!("../../../examples/iot/iot.plexus"));
    assert_example_graph_validates(
        "nats_factory_windows",
        include_str!("../../../examples/nats-factory-windows/nats_factory_windows.plexus"),
    );
    assert_example_graph_validates(
        "datalake",
        include_str!("../../../examples/datalake/datalake.plexus"),
    );
    assert_example_graph_validates(
        "wasm_dual",
        include_str!("../../../examples/wasm-processors/wasm-dual.plexus"),
    );
    assert_example_graph_validates(
        "binance_websocket",
        include_str!("../../../examples/binance-websocket/binance_websocket.plexus"),
    );
    assert_example_graph_validates(
        "onnx_batched",
        include_str!("../../../examples/onnx-inference/batched.plexus"),
    );
    assert_example_graph_validates(
        "onnx_per_message",
        include_str!("../../../examples/onnx-inference/per-message.plexus"),
    );
}

#[test]
fn create_fails_when_model_already_exists() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let ns = DomainName::parse("default").expect("valid domain");

    registry
        .apply_batch(&ns, vec![sample_transport_model("kafka_main")])
        .expect("partial graph should succeed");
    let err = registry
        .apply_batch(&ns, vec![sample_transport_model("kafka_main")])
        .expect_err("duplicate create must fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::AlreadyExists { .. }
    ));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn emitter_configuration_keeps_persisted_codec_read_and_kind_errors() {
    let path = temp_db_path();
    let registry = Registry::open(&path).assured("the fixture opens an empty registry");
    let domain = named::<DomainName>("inspection");
    let identifier = named::<ModelName>("emit");
    registry
        .apply_batch(&domain, full_graph_batch())
        .assured("the complete emitter graph validates");
    let (emitter, payload) = registry
        .emitter_configuration(&domain, &identifier)
        .assured("the stored emitter and codec decode")
        .assured("the fixture graph contains emit");
    assert!(emitter.batch.is_none());
    assert_eq!(payload.to_string(), "JSON record");
    assert!(
        registry
            .emitter_configuration(&domain, named::<ModelName>("missing"))
            .assured("an absent name is an ordinary lookup")
            .is_none()
    );

    let key = encode_key(&domain, ModelKind::Codec, named::<ModelName>("event_codec"))
        .assured("the fixture codec key encodes");
    registry
        .storage
        .index
        .insert(key.clone(), b"invalid archive")
        .assured("the fixture corrupts only its own stored codec");
    let error = registry
        .emitter_configuration(&domain, &identifier)
        .expect_err("inspection must report the persisted codec decoding failure");
    assert!(error.frames().any(|frame| matches!(
        frame.downcast_ref::<RegistryError>(),
        Some(RegistryError::InvalidModelArchive)
    )));

    let misplaced = serialize_value(
        &schema("event_schema"),
        &nervix_models::EntityLabelLimits::from(&nervix_models::ClusterParameterValues::default()),
    )
    .assured("the current schema Model archives");
    registry
        .storage
        .index
        .insert(key.clone(), misplaced)
        .assured("the fixture writes a current Model beneath the wrong key kind");
    let error = registry
        .emitter_configuration(&domain, &identifier)
        .expect_err("inspection must report the codec key's stored kind mismatch");
    assert!(error.frames().any(|frame| matches!(
        frame.downcast_ref::<RegistryError>(),
        Some(RegistryError::StoredModelKindMismatch {
            expected_kind,
            ..
        }) if *expected_kind == ModelKind::Codec.as_str()
    )));

    registry
        .storage
        .index
        .remove(key)
        .assured("the fixture removes only the emitter's stored codec");
    let error = registry
        .emitter_configuration(&domain, &identifier)
        .expect_err("inspection must preserve a missing codec reference as a typed failure");
    assert!(matches!(
        error.current_context(),
        RegistryError::MissingReference { expected_kind, reference, .. }
            if *expected_kind == ModelKind::Codec.as_str() && reference == "event_codec"
    ));
    drop(registry);
    fs::remove_dir_all(path).assured("the fixture registry path is disposable");
}

#[test]
fn create_allows_same_identifier_for_different_kinds() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let ns = DomainName::parse("default").expect("valid domain");

    registry
        .apply_batch(
            &ns,
            vec![schema("shared_name"), client_model("shared_name")],
        )
        .expect("different kinds should be allowed to share an identifier");

    assert!(
        registry
            .get::<CreateSchema>(
                &ns,
                ModelName::parse("shared_name").expect("valid model name"),
            )
            .expect("schema read should succeed")
            .is_some()
    );
    assert!(
        registry
            .get_of_kind(
                &ns,
                ModelKind::Client,
                ModelName::parse("shared_name").expect("valid model name"),
            )
            .expect("client read should succeed")
            .is_some()
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn open_fails_when_persisted_state_is_invalid() {
    let path = temp_db_path();
    let db = Database::builder(&path)
        .open()
        .expect("database should open");
    let storage = ModelStorage::from_database(db).expect("storage should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let schema = schema("event_schema");
    let wire_schema = wire_schema("event_wire");
    let relay = relay("raw_events", "event_schema");
    let model = ingestor("kafka_ingestor", "raw_events", "event_codec", "kafka_main");

    storage
        .put(&domain, schema.kind(), &schema.name(), &schema)
        .expect("write should succeed");
    storage
        .put(
            &domain,
            wire_schema.kind(),
            &wire_schema.name(),
            &wire_schema,
        )
        .expect("write should succeed");
    storage
        .put(&domain, relay.kind(), &relay.name(), &relay)
        .expect("write should succeed");
    storage
        .put(&domain, model.kind(), &model.name(), &model)
        .expect("write should succeed");
    drop(storage);

    let err = Registry::open(&path)
        .err()
        .expect("invalid persisted state must fail startup");
    assert!(
        format!("{err}").contains("requires missing codec 'event_codec'"),
        "unexpected startup error: {err}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn list_identifiers_filters_by_kind_and_prefix() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let ns = DomainName::parse("default").expect("valid domain");

    registry
        .storage
        .put(
            &ns,
            ModelKind::Client,
            &ModelName::from(&ClientName::parse("kafka_main").expect("valid client name")),
            &sample_transport_model("kafka_main"),
        )
        .expect("write should succeed");

    let transports = registry
        .list_identifiers(&ns, ModelKind::Client, "kafka_")
        .expect("list should succeed");
    assert_eq!(
        transports
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>(),
        vec!["kafka_main"]
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn current_model_archive_validates_its_header() {
    let model = sample_transport_model("transport");
    let mut encoded = super::serialize_value(
        &model,
        &nervix_models::EntityLabelLimits::from(&nervix_models::ClusterParameterValues::default()),
    )
    .assured("a bounded current Model archives into its current stored shape");
    assert!(encoded.starts_with(super::MODEL_ARCHIVE_HEADER));
    assert_eq!(
        super::deserialize_value(&encoded).assured("a current Model reads its own encoding"),
        model
    );
    encoded[0] ^= 1;
    let Err(error) = super::deserialize_value(&encoded) else {
        panic!("a damaged current header must fail before archive decoding");
    };
    assert!(matches!(
        error.current_context(),
        RegistryError::InvalidModelArchive
    ));
}

#[test]
fn registry_models_decode_under_their_captured_complete_label_limits() {
    let mut model = schema("labelled");
    let value = "Δ".repeat(768);
    *model.labels_mut() = serde_json::from_value(serde_json::json!({"owner": value}))
        .assured("the fixture key is valid and its value is exact UTF-8");
    let limits = nervix_models::EntityLabelLimits {
        count: 3,
        key_bytes: 5,
        value_bytes: 2048,
        bytes: 4096,
    };
    let encoded = super::serialize_value(&model, &limits)
        .assured("the current map fits its committed raised policy");
    assert_eq!(
        super::deserialize_value(&encoded)
            .assured("the record carries the policy it was written under"),
        model
    );
    for (index, maximum, parameter) in [
        (
            0_usize,
            0_u64,
            nervix_models::ClusterParameterKey::EntityLabelsMaxCount,
        ),
        (
            1,
            4,
            nervix_models::ClusterParameterKey::EntityLabelsMaxKeyBytes,
        ),
        (
            2,
            1535,
            nervix_models::ClusterParameterKey::EntityLabelsMaxValueBytes,
        ),
        (
            3,
            1540,
            nervix_models::ClusterParameterKey::EntityLabelsMaxBytes,
        ),
    ] {
        let mut damaged = encoded.clone();
        let start = super::MODEL_ARCHIVE_HEADER.len() + index * size_of::<u64>();
        damaged[start..start + size_of::<u64>()].copy_from_slice(&maximum.to_le_bytes());
        let Err(error) = super::deserialize_value(&damaged) else {
            panic!("a complete map outside the record's policy is invalid");
        };
        assert!(
            matches!(error.downcast_ref::<nervix_models::EntityLabelError>(), Some(nervix_models::EntityLabelError::Limit { parameter: actual, .. }) if *actual == parameter)
        );
    }
}

#[test]
fn bolero_registry_archived_models_round_trip() {
    use nervix_arbitrary::{Arbitrary, Domain};
    use nervix_models::RequestedResourceVersion;

    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let requested = Arbitrary::new(bytes, Domain::Vocabulary).model();
            let pinned: Result<Model, std::convert::Infallible> = requested
                .try_map_resource_versions(|_, version| match version {
                    RequestedResourceVersion::Latest => Ok(0),
                    RequestedResourceVersion::Number(number) => Ok(number),
                });
            let model = match pinned {
                Ok(model) => model,
                Err(never) => match never {},
            };
            let encoded = super::serialize_value(
                &model,
                &nervix_models::EntityLabelLimits::from(
                    &nervix_models::ClusterParameterValues::default(),
                ),
            )
            .assured("bounded current Models encode through the registry owner");
            let decoded = super::deserialize_value(&encoded)
                .assured("a current registry archive restores its complete Model");
            assert_eq!(decoded, model);
        });
}

#[test]
fn get_roundtrip_returns_stored_model() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let ns = DomainName::parse("default").expect("valid domain");
    let id = ClientName::parse("kafka_main").expect("valid client name");
    let model = sample_transport_model("kafka_main");

    registry
        .storage
        .put(&ns, ModelKind::Client, &ModelName::from(&id), &model)
        .expect("create should succeed");
    let loaded = registry
        .get_of_kind(&ns, ModelKind::Client, &id)
        .expect("read should succeed")
        .expect("model should exist");

    assert_eq!(loaded, model);

    let _ = fs::remove_dir_all(path);
}

#[test]
fn bytes_schema_reloads_from_current_model_archive() {
    let path = temp_db_path();
    let domain = named::<DomainName>("binary_domain");
    let schema = CreateSchema {
        labels: Default::default(),
        name: named::<SchemaName>("binary_payload"),
        fields: vec![SchemaField {
            name: named("payload"),
            ty: ParseAsType::Bytes,
            optional: true,
            sensitive: true,
        }],
    };
    {
        let registry =
            Registry::open(&path).assured("the test database path can hold a new registry");
        registry
            .storage
            .put(
                &domain,
                ModelKind::Schema,
                &ModelName::from(&schema.name),
                &Model::Schema(schema.clone()),
            )
            .assured("a current BYTES schema serializes into the model store");
    }

    let reopened =
        Registry::open(&path).verified("the registry just closed with a valid BYTES schema");
    let loaded = reopened
        .get::<CreateSchema>(&domain, ModelName::from(&schema.name))
        .verified("the model store contains a current schema archive")
        .verified("the BYTES schema was stored under its own name");
    assert_eq!(loaded, schema);
    fs::remove_dir_all(path).assured("the test registry path is disposable");
}

/// A flush moves stored Models into an on-disk table, which hands each value back as a slice of
/// its block at whatever byte offset the block placed it. A restarted node reads every Model it
/// stored from there.
#[test]
fn models_reload_after_their_keyspace_is_flushed_to_disk() {
    let path = temp_db_path();
    let domain = named::<DomainName>("flushed_domain");
    let stored = {
        let registry = Registry::open(&path).assured("a new test path holds an empty registry");
        registry
            .apply_batch(&domain, full_graph_batch())
            .assured("the complete fixture graph validates");
        let stored = registry
            .storage
            .list_all_models()
            .verified("the memtable holds the Models this registry just stored");
        registry
            .storage
            .index
            .rotate_memtable_and_wait()
            .assured("the test keyspace flushes its memtable into an on-disk table");
        stored
    };

    let reopened = Registry::open(&path)
        .verified("the registry just closed after storing a valid graph in its table");
    let reloaded = reopened
        .storage
        .list_all_models()
        .verified("the reopened registry already listed these Models while opening");
    assert_eq!(reloaded, stored);
    fs::remove_dir_all(path).assured("the test registry path is disposable");
}

#[test]
fn synchronized_domain_schedule_persists_models_for_restart() {
    let source_path = temp_db_path();
    let source = Registry::open(&source_path).expect("source registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let mut models = full_graph_batch();
    let Model::Relay(notifications) = models
        .iter_mut()
        .find(|model| model.kind() == ModelKind::Relay && model.name().as_str() == "notifications")
        .expect("notifications relay should exist")
    else {
        panic!("notifications model should be a relay");
    };
    notifications.materialized_state = Some(MaterializedRelayState::LastByTimestamp);
    source
        .apply_batch(&domain, models)
        .expect("source graph should be valid");
    let schedule = source
        .active_graph(&domain)
        .expect("source graph should exist")
        .schedule_for_domain(
            &domain,
            &[ClusterNodeName::parse("node-1").expect("valid name")],
            0,
            None,
            &crate::registry::uniform_capabilities(&[
                ClusterNodeName::parse("node-1").expect("valid name")
            ]),
        );
    let scheduled_relay = schedule
        .nodes
        .get(&NodeRef::new(
            ModelKind::Relay,
            named::<ModelName>("notifications"),
        ))
        .expect("fixture schedule must include its materialized relay");
    assert_eq!(
        scheduled_relay.single_unit().assigned_nodes,
        [named::<ClusterNodeName>("node-1")]
    );

    let replica_path = temp_db_path();
    {
        let replica = Registry::open(&replica_path).expect("replica registry should open");
        replica
            .synchronize_cluster_schedule(&ClusterSchedule::from_iter([schedule]))
            .expect("schedule models should synchronize");
    }

    let reopened = Registry::open(&replica_path).expect("replica registry should reopen");
    assert_eq!(
        reopened
            .get::<CreateIngestor>(&domain, named::<ModelName>("ing"))
            .expect("replica model read should succeed"),
        source
            .get::<CreateIngestor>(&domain, named::<ModelName>("ing"))
            .expect("source model read should succeed")
    );

    let _ = fs::remove_dir_all(source_path);
    let _ = fs::remove_dir_all(replica_path);
}

#[test]
fn apply_batch_accepts_partial_graphs() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    registry
        .apply_batch(
            &domain,
            vec![schema("event_schema"), client_model("kafka_main")],
        )
        .expect("partial graph should succeed");

    let graph = registry
        .active_graph(&domain)
        .expect("graph should be installed");
    assert_eq!(graph.node_count(), 2);
    assert_eq!(graph.edge_count(), 0);

    let _ = fs::remove_dir_all(path);
}

#[test]
fn alter_relay_set_capacity_updates_stored_model_and_active_graph() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                relay("notifications", "event_schema"),
            ],
        )
        .expect("create should succeed");

    let changes = registry
        .alter_relay(
            &domain,
            AlterRelay {
                relay: named("notifications"),
                operations: vec![AlterRelayOperation::SetCapacity {
                    capacity: nonzero!(5usize),
                }],
            },
        )
        .expect("alter should succeed");
    assert!(changes.graph.is_some());

    let stored_relay = registry
        .get::<CreateRelay>(&domain, named::<ModelName>("notifications"))
        .expect("read should succeed")
        .expect("relay should exist");
    assert_eq!(stored_relay.buffer, nonzero!(5usize));

    let graph = registry
        .active_graph(&domain)
        .expect("graph should be installed");
    let node = graph
        .node(ModelKind::Relay, &named("notifications"))
        .expect("relay node should exist");
    let Model::Relay(graph_relay) = node.config.as_ref() else {
        panic!("graph node should contain relay config");
    };
    assert_eq!(graph_relay.buffer, nonzero!(5usize));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn mutation_plan_classifies_no_op_and_relay_capacity_from_the_model_diff() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                relay("notifications", "event_schema"),
            ],
        )
        .expect("initial graph should succeed");

    let noop = registry
        .plan_mutations(
            &domain,
            &[RegistryMutation::AlterRelay(AlterRelay {
                relay: named("notifications"),
                operations: vec![AlterRelayOperation::SetCapacity {
                    capacity: nonzero!(1usize),
                }],
            })],
        )
        .expect("no-op alter should plan");
    assert!(noop.is_noop());
    assert_eq!(noop.quiesce().level(), QuiesceLevel::Dynamic);
    assert!(noop.quiesce().affected_entities().is_empty());

    let capacity = registry
        .plan_mutations(
            &domain,
            &[RegistryMutation::AlterRelay(AlterRelay {
                relay: named("notifications"),
                operations: vec![AlterRelayOperation::SetCapacity {
                    capacity: nonzero!(5usize),
                }],
            })],
        )
        .expect("capacity alter should plan");
    assert!(!capacity.is_noop());
    assert_eq!(capacity.quiesce().level(), QuiesceLevel::Dynamic);
    assert!(capacity.quiesce().affected_entities().is_empty());

    let _ = fs::remove_dir_all(path);
}

#[test]
fn junction_alter_is_applied_before_diff_based_quiesce_classification() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                relay("incoming", "event_schema"),
                relay("outgoing", "event_schema"),
                Model::Junction(CreateJunction {
                    labels: Default::default(),
                    name: named("route_events"),
                    from: ProcessorInputs::single(named("incoming")),
                    output_routes: with_inherit_all(ProcessorOutputs::single(named("outgoing")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: BranchSelection::unbranched(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ],
        )
        .expect("initial graph should succeed");

    let dynamic = registry
        .plan_mutations(
            &domain,
            &[RegistryMutation::AlterJunction(AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::SetFilterWhere {
                    where_clause: nervix_plexus::parse_expression("input.value != ''")
                        .expect("valid expression"),
                }],
            })],
        )
        .expect("filter alter should plan");
    assert_eq!(dynamic.quiesce().level(), QuiesceLevel::Dynamic);

    let entity_pause = registry
        .plan_mutations(
            &domain,
            &[RegistryMutation::AlterJunction(AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::SetMode {
                    mode: AckMode::Detached,
                }],
            })],
        )
        .expect("mode alter should plan");
    assert_eq!(entity_pause.quiesce().level(), QuiesceLevel::EntityPause);

    let _ = fs::remove_dir_all(path);
}

#[test]
fn emitter_alter_is_applied_before_diff_based_quiesce_classification() {
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
                client_model("emitter_a"),
                client_model("emitter_b"),
                relay("outgoing", "event_schema"),
                emitter("event_emitter", "outgoing", "event_codec", "emitter_a"),
            ],
        )
        .expect("initial graph should succeed");

    let dynamic = registry
        .plan_mutations(
            &domain,
            &[RegistryMutation::AlterEmitter(AlterEmitter {
                kind: nervix_models::EmitterEntityKind::Kafka(
                    nervix_models::KafkaEmitterEntity::Topic,
                ),
                emitter: named("event_emitter"),
                operations: vec![nervix_models::AlterEmitterOperation::SetFlush {
                    flush_policy: FlushPolicy::Immediate,
                }],
            })],
        )
        .expect("flush alter should plan");
    assert_eq!(dynamic.quiesce().level(), QuiesceLevel::Dynamic);

    let entity_pause = registry
        .plan_mutations(
            &domain,
            &[RegistryMutation::AlterEmitter(AlterEmitter {
                kind: nervix_models::EmitterEntityKind::Kafka(
                    nervix_models::KafkaEmitterEntity::Topic,
                ),
                emitter: named("event_emitter"),
                operations: vec![nervix_models::AlterEmitterOperation::SetClient {
                    client: named("emitter_b"),
                }],
            })],
        )
        .expect("client alter should plan");
    assert_eq!(entity_pause.quiesce().level(), QuiesceLevel::EntityPause);
    assert_eq!(
        entity_pause.quiesce().affected_entities(),
        &[super::NodeRef {
            kind: ModelKind::Emitter,
            identifier: named("event_emitter"),
        }]
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn relay_drop_create_same_key_is_classified_as_a_model_change() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                schema("event_schema_v2"),
                relay("notifications", "event_schema"),
            ],
        )
        .expect("initial graph should succeed");

    let planned = registry
        .plan_mutations(
            &domain,
            &[
                RegistryMutation::Drop(DropModel {
                    kind: ModelKind::Relay,
                    name: named("notifications"),
                }),
                RegistryMutation::Create(Box::new(relay("notifications", "event_schema_v2"))),
            ],
        )
        .expect("relay recreation should plan");

    assert_eq!(planned.quiesce().level(), QuiesceLevel::DomainPause);
    assert_eq!(
        planned.quiesce().affected_entities(),
        &[super::NodeRef {
            kind: ModelKind::Relay,
            identifier: named("notifications"),
        }]
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn referenced_codec_drop_create_same_key_is_classified_as_domain_pause() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let mut models = full_graph_batch();
    models.push(Model::WireJsonSchema(CreateWireSchema {
        labels: Default::default(),
        name: named("event_wire_v2"),
        strictness: Default::default(),
        fields: vec![WireSchemaField {
            name: named("value"),
            ty: JsonType::String,
            optional: false,
        }],
    }));
    registry
        .apply_batch(&domain, models)
        .expect("initial graph should succeed");

    let Model::Codec(mut replacement) = codec("event_codec", "event_schema") else {
        unreachable!("codec helper must build a codec model");
    };
    replacement.wire_format = CodecWireFormat::Json {
        wire_schema: named("event_wire_v2"),
    };
    let planned = registry
        .plan_mutations(
            &domain,
            &[
                RegistryMutation::Drop(DropModel {
                    kind: ModelKind::Codec,
                    name: named("event_codec"),
                }),
                RegistryMutation::Create(Box::new(Model::Codec(replacement))),
            ],
        )
        .expect("referenced codec recreation should plan");

    assert_eq!(planned.quiesce().level(), QuiesceLevel::DomainPause);
    let expected = planned
        .base_graph()
        .whole_impact()
        .nodes
        .into_iter()
        .filter(|coverage| coverage.branches.is_some())
        .map(|coverage| coverage.node)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        planned
            .quiesce()
            .affected_entities()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn alter_relay_rejects_missing_relay_without_persisting() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let result = registry.alter_relay(
        &domain,
        AlterRelay {
            relay: named("notifications"),
            operations: vec![AlterRelayOperation::SetCapacity {
                capacity: nonzero!(5usize),
            }],
        },
    );
    assert!(matches!(
        result
            .expect_err("missing relay should be rejected")
            .current_context(),
        RegistryError::NotFound { .. }
    ));
    assert!(
        registry
            .get::<CreateRelay>(&domain, named::<ModelName>("notifications"),)
            .expect("read should succeed")
            .is_none()
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_missing_references_without_persisting() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![ingestor(
                "kafka_ingestor",
                "raw_events",
                "event_codec",
                "kafka_main",
            )],
        )
        .expect_err("missing dependencies must fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::MissingReference { .. }
    ));
    assert!(
        registry
            .get::<CreateIngestor>(
                &domain,
                IngestorName::parse("kafka_ingestor").expect("valid ingestor name"),
            )
            .expect("read should succeed")
            .is_none()
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_is_order_independent() {
    let domain = DomainName::parse("default").expect("valid domain");

    let path_a = temp_db_path();
    let registry_a = Registry::open(&path_a).expect("registry should open");
    registry_a
        .apply_batch(&domain, full_graph_batch())
        .expect("ordered batch should succeed");
    let graph_a = registry_a
        .active_graph(&domain)
        .expect("graph should be installed");

    let path_b = temp_db_path();
    let registry_b = Registry::open(&path_b).expect("registry should open");
    let batch_b = vec![
        schema("event_schema"),
        wire_schema("event_wire"),
        codec("event_codec", "event_schema"),
        client_model("broker_out"),
        relay_branched_like("p99", "event_schema", "notifications"),
        relay_branched_by_relay_branch("notifications", "event_schema"),
        emitter("emit", "p99", "event_codec", "broker_out"),
        branch_schema("value_branch", &["value"]),
        branch_for_relay("notifications", "value_branch"),
        ingestor_with_params(
            "ing",
            "notifications",
            "event_codec",
            "broker_in",
            &["value"],
        ),
        processor("p99_proc", "notifications", "p99"),
        client_model("broker_in"),
    ];

    registry_b
        .apply_batch(&domain, batch_b)
        .expect("reordered batch should also succeed");
    let graph_b = registry_b
        .active_graph(&domain)
        .expect("graph should be installed");

    assert_eq!(graph_a.node_count(), 12);
    assert_eq!(graph_a.edge_count(), 21);
    assert_eq!(graph_a.node_count(), graph_b.node_count());
    assert_eq!(graph_a.edge_count(), graph_b.edge_count());

    let _ = fs::remove_dir_all(path_a);
    let _ = fs::remove_dir_all(path_b);
}

#[test]
fn failed_batch_does_not_mutate_registry_state() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                client_model("broker_out"),
                emitter("emit", "missing_stream", "event_codec", "broker_out"),
            ],
        )
        .expect_err("invalid batch must fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::MissingReference { .. }
    ));
    assert!(
        registry.active_graph(&domain).is_none(),
        "failed batch must not install a graph"
    );
    assert!(
        registry
            .get::<CreateSchema>(
                &domain,
                SchemaName::parse("event_schema").expect("valid schema name"),
            )
            .expect("read should succeed")
            .is_none()
    );
    assert!(
        registry
            .get_of_kind(
                &domain,
                ModelKind::Client,
                RelayName::parse("broker_out").expect("valid relay name")
            )
            .expect("read should succeed")
            .is_none()
    );
    assert!(
        registry
            .get::<CreateEmitter>(
                &domain,
                EmitterName::parse("emit").expect("valid emitter name"),
            )
            .expect("read should succeed")
            .is_none()
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn planned_schema_alters_do_not_mutate_until_committed() {
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
            ],
        )
        .expect("create should succeed");
    let planned = registry
        .plan_mutations(
            &domain,
            &[
                RegistryMutation::AlterSchema(AlterSchema {
                    schema: named("event_schema"),
                    operations: vec![AlterSchemaOperation::AddField {
                        field: SchemaField {
                            name: named("note"),
                            ty: ParseAsType::String,
                            optional: true,
                            sensitive: false,
                        },
                    }],
                }),
                RegistryMutation::AlterWireJsonSchema(AlterWireSchema {
                    schema: named("event_wire"),
                    operations: vec![AlterWireSchemaOperation::AddField {
                        field: WireSchemaField {
                            name: named("note"),
                            ty: JsonType::String,
                            optional: true,
                        },
                    }],
                }),
            ],
        )
        .expect("planning should succeed");

    let before = registry
        .get::<CreateSchema>(&domain, named::<ModelName>("event_schema"))
        .expect("read should succeed")
        .expect("schema should exist");
    assert_eq!(before.fields.len(), 1, "planning must not persist");

    registry
        .commit_planned(planned)
        .expect("commit should succeed");

    let after = registry
        .get::<CreateSchema>(&domain, named::<ModelName>("event_schema"))
        .expect("read should succeed")
        .expect("schema should exist");
    assert_eq!(after.fields.len(), 2);

    let _ = fs::remove_dir_all(path);
}

#[test]
fn failed_mixed_schema_alter_batch_applies_nothing() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    registry
        .apply_batch(&domain, vec![schema("event_schema")])
        .expect("create should succeed");

    let error = registry
        .apply_mutation_batch(
            &domain,
            vec![
                RegistryMutation::Create(Box::new(schema("new_schema"))),
                RegistryMutation::AlterSchema(AlterSchema {
                    schema: named("event_schema"),
                    operations: vec![
                        AlterSchemaOperation::AddField {
                            field: SchemaField {
                                name: named("note"),
                                ty: ParseAsType::String,
                                optional: true,
                                sensitive: false,
                            },
                        },
                        AlterSchemaOperation::DropField {
                            field: named("missing"),
                        },
                    ],
                }),
            ],
        )
        .expect_err("invalid ALTER should reject the whole batch");

    assert_eq!(
        error.current_context(),
        &RegistryError::InvalidModel {
            domain: "default".to_string(),
            identifier: "event_schema".to_string(),
            reason: "field `missing` does not exist".to_string(),
        }
    );
    assert_eq!(
        error.downcast_ref::<AlterSchemaError>(),
        Some(&AlterSchemaError::FieldNotFound {
            field: named("missing"),
        }),
        "the refusal keeps the vocabulary's rejection beneath it"
    );
    assert!(
        registry
            .get::<CreateSchema>(&domain, named::<ModelName>("new_schema"))
            .expect("read should succeed")
            .is_none()
    );
    let schema = registry
        .get::<CreateSchema>(&domain, named::<ModelName>("event_schema"))
        .expect("read should succeed")
        .expect("schema should exist");
    assert_eq!(schema.fields.len(), 1);

    let _ = fs::remove_dir_all(path);
}

#[test]
fn deduplicator_dependencies_participate_in_candidate_graph_validation() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                schema("my_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "my_schema"),
                client_model("broker_in"),
                relay_branched_by_relay_branch("input", "my_schema"),
                branch_schema("value_branch", &["value"]),
                branch_for_relay("input", "value_branch"),
                ingestor_with_params("ing", "input", "event_codec", "broker_in", &["value"]),
                processor("p99_proc", "input", "missing_output"),
            ],
        )
        .expect_err("missing deduplicator output relay must fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::MissingReference { .. }
    ));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn drop_batch_removes_unused_model() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    registry
        .apply_batch(
            &domain,
            vec![schema("event_schema"), client_model("broker_in")],
        )
        .expect("partial graph should succeed");
    registry
        .drop_batch(
            &domain,
            vec![DropModel {
                kind: ModelKind::Client,
                name: ModelName::parse("broker_in").expect("valid identifier"),
            }],
        )
        .expect("drop should succeed");

    assert!(
        registry
            .get_of_kind(
                &domain,
                ModelKind::Client,
                RelayName::parse("broker_in").expect("valid relay name")
            )
            .expect("read should succeed")
            .is_none()
    );
    let graph = registry
        .active_graph(&domain)
        .expect("graph should still exist");
    assert_eq!(graph.node_count(), 1);
    assert_eq!(graph.edge_count(), 0);

    let _ = fs::remove_dir_all(path);
}

#[test]
fn drop_batch_rejects_delete_when_model_is_in_use() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    registry
        .apply_batch(&domain, full_graph_batch())
        .expect("full graph batch should succeed");

    let err = registry
        .drop_batch(
            &domain,
            vec![DropModel {
                kind: ModelKind::Schema,
                name: ModelName::parse("event_schema").expect("valid identifier"),
            }],
        )
        .expect_err("drop should be rejected while schema is in use");

    assert!(matches!(
        err.current_context(),
        RegistryError::DeleteInUse { .. }
    ));
    assert!(
        registry
            .get::<CreateSchema>(
                &domain,
                SchemaName::parse("event_schema").expect("valid schema name"),
            )
            .expect("read should succeed")
            .is_some()
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn drop_batch_allows_delete_of_emitter() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    registry
        .apply_batch(&domain, full_graph_batch())
        .expect("full graph batch should succeed");

    registry
        .drop_batch(
            &domain,
            vec![DropModel {
                kind: ModelKind::Emitter,
                name: ModelName::parse("emit").expect("valid identifier"),
            }],
        )
        .expect("emitter should be droppable");

    assert!(
        registry
            .get::<CreateEmitter>(
                &domain,
                EmitterName::parse("emit").expect("valid emitter name"),
            )
            .expect("read should succeed")
            .is_none()
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn drop_batch_rejects_delete_of_deduplicator_output_stream() {
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
                client_model("broker_in"),
                relay_branched_by_relay_branch("input", "event_schema"),
                relay_branched_like("output", "event_schema", "input"),
                branch_schema("value_branch", &["value"]),
                branch_for_relay("input", "value_branch"),
                ingestor_with_params("ing", "input", "event_codec", "broker_in", &["value"]),
                processor("p99_proc", "input", "output"),
            ],
        )
        .expect("deduplicator graph should succeed");

    let err = registry
        .drop_batch(
            &domain,
            vec![DropModel {
                kind: ModelKind::Relay,
                name: ModelName::parse("output").expect("valid identifier"),
            }],
        )
        .expect_err("deduplicator output relay should be blocked");

    assert!(matches!(
        err.current_context(),
        RegistryError::DeleteInUse { .. }
    ));

    let _ = fs::remove_dir_all(path);
}
