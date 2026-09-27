//! Direct tests for the data-plane processor plan materializer and binder.
//!
//! Owns: planner fixtures and assertions for typed runtime plans.
//! May depend on: the parent planning module and test-only model constructors.
//! Must not know: live control-plane state or external connector implementations.

use std::num::NonZeroU32;

use nervix_models::{
    BranchSelection, CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
    CreateDeduplicator, CreateInferencer, CreateJunction, CreateSchema, CreateWasmProcessor,
    CreateWindowProcessor, InferencerTensorDeclaration, InferencerTensorDimension,
    InferencerTensorElementType, InferencerTensorMapping, InferencerTensorRepresentation,
    InferencerTensorSchema, ParseAsType, ProcessorOutputs, RelayBranching, SchemaField,
    WasmProcessorLimits, WasmRejectedStatePolicy, WindowBound, ZeroMqIngestMode,
};
use nonzero_ext::nonzero;
use triomphe::Arc;

use super::*;

fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).assured("the test uses valid model names")
}

fn inferencer_tensor_schema(size: NonZeroU32) -> InferencerTensorSchema {
    InferencerTensorSchema {
        representation: InferencerTensorRepresentation::Dense,
        element_type: InferencerTensorElementType::F32,
        dimensions: vec![InferencerTensorDimension::Fixed(size)],
    }
}

fn inferencer_node(input_relays: Vec<RelayName>) -> BranchedProcessorSpec {
    BranchedProcessorSpec {
        kind: ModelKind::Inferencer,
        processor: named("score_model"),
        input_relays,
        input_collect_policies: HashMap::default(),
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        from_where: HashMap::default(),
        filter_where: None,
        materialized_state: Vec::new(),
        operation: BranchedProcessorOperationSpec::Inferencer {
            output_routes: BranchedProcessorOutputsSpec {
                routes: vec![BranchedProcessorOutputSpec {
                    relay: named("scores"),
                    construction: RouteConstruction::default(),
                    flush_policy: Some(FlushPolicy::Immediate),
                    message_error_policy: MessageErrorPolicy::Log,
                }],
            },
            resource: named("fraud_model"),
            resource_version: 1,
            file: "models/fraud.onnx".to_string(),
            inputs: Vec::new(),
            output_schema: Vec::new(),
        },
    }
}

fn window_node(output_routes: BranchedProcessorOutputsSpec) -> BranchedProcessorSpec {
    BranchedProcessorSpec {
        kind: ModelKind::WindowProcessor,
        processor: named("metric_window"),
        input_relays: vec![named("metrics")],
        input_collect_policies: HashMap::default(),
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        from_where: HashMap::default(),
        filter_where: None,
        materialized_state: Vec::new(),
        operation: BranchedProcessorOperationSpec::WindowProcessor {
            output_routes,
            width: WindowBound::of_messages(10),
            step: WindowBound::of_messages(5),
            state_limit: nervix_models::WindowStateLimit::Unbounded,
        },
    }
}

fn output_spec(
    relay: &str,
    construction_text: &str,
    flush_policy: Option<FlushPolicy>,
) -> BranchedProcessorOutputSpec {
    BranchedProcessorOutputSpec {
        relay: named(relay),
        construction: construction(construction_text),
        flush_policy,
        message_error_policy: MessageErrorPolicy::Log,
    }
}

fn processor_spec(
    kind: ModelKind,
    processor: &str,
    input_relays: Vec<RelayName>,
    operation: BranchedProcessorOperationSpec,
) -> BranchedProcessorSpec {
    BranchedProcessorSpec {
        kind,
        processor: named(processor),
        input_relays,
        input_collect_policies: HashMap::default(),
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        from_where: HashMap::default(),
        filter_where: None,
        materialized_state: Vec::new(),
        operation,
    }
}

#[test]
fn eager_binding_prepares_every_processor_family_before_publication() {
    let input = named::<RelayName>("incoming");
    let left = named::<RelayName>("left_events");
    let right = named::<RelayName>("right_events");
    let features = named::<RelayName>("features");
    let generic_schema = Arc::new(compile_schema(&CreateSchema {
        name: named("event"),
        fields: vec![
            SchemaField {
                name: named("id"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: named("value"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: named("key"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: named("vector"),
                ty: ParseAsType::Array {
                    element: Box::new(ParseAsType::F32),
                    len: nonzero!(2u32),
                },
                optional: false,
                sensitive: false,
            },
        ],
    }));
    let window_schema = Arc::new(compile_schema(&CreateSchema {
        name: named("window_result"),
        fields: vec![SchemaField {
            name: named("value"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    }));
    let score_schema = Arc::new(compile_schema(&CreateSchema {
        name: named("score_result"),
        fields: vec![SchemaField {
            name: named("score"),
            ty: ParseAsType::Array {
                element: Box::new(ParseAsType::F32),
                len: nonzero!(1u32),
            },
            optional: false,
            sensitive: false,
        }],
    }));
    let generic_output = |relay| BranchedProcessorOutputsSpec {
        routes: vec![output_spec(
            relay,
            "INHERIT ALL WHERE value > 0",
            Some(FlushPolicy::Immediate),
        )],
    };

    let mut junction = processor_spec(
        ModelKind::Junction,
        "route_events",
        vec![input.clone()],
        BranchedProcessorOperationSpec::Junction {
            output_routes: generic_output("junction_events"),
        },
    );
    junction
        .from_where
        .insert(input.clone(), expression("input.value > 0"));
    junction.filter_where = Some(expression("input.value >= 0"));
    let deduplicator = processor_spec(
        ModelKind::Deduplicator,
        "deduplicate_events",
        vec![input.clone()],
        BranchedProcessorOperationSpec::Deduplicator {
            output_routes: generic_output("unique_events"),
            deduplicate_on: vec![expression("input.key")],
            max_time: "1m".to_string(),
        },
    );
    let reorderer = processor_spec(
        ModelKind::Reorderer,
        "reorder_events",
        vec![input.clone()],
        BranchedProcessorOperationSpec::Reorderer {
            output_routes: generic_output("ordered_events"),
            order_by: vec![expression("input.value")],
            max_time: "1m".to_string(),
        },
    );
    let window = processor_spec(
        ModelKind::WindowProcessor,
        "window_events",
        vec![input.clone()],
        BranchedProcessorOperationSpec::WindowProcessor {
            output_routes: BranchedProcessorOutputsSpec {
                routes: vec![output_spec(
                    "window_events_out",
                    "SET value = SUM(input.value) WHERE value > 0",
                    None,
                )],
            },
            width: WindowBound::of_messages(2),
            step: WindowBound::of_messages(2),
            state_limit: nervix_models::WindowStateLimit::Unbounded,
        },
    );
    let inferencer = processor_spec(
        ModelKind::Inferencer,
        "score_events",
        vec![features.clone()],
        BranchedProcessorOperationSpec::Inferencer {
            output_routes: BranchedProcessorOutputsSpec {
                routes: vec![output_spec(
                    "scored_events",
                    "SET score = score",
                    Some(FlushPolicy::Immediate),
                )],
            },
            resource: named("score_model"),
            resource_version: 1,
            file: "models/score.onnx".to_string(),
            inputs: vec![InferencerTensorMapping {
                tensor: "features".to_string(),
                schema: inferencer_tensor_schema(nonzero!(2u32)),
                expression: expression("input.vector"),
            }],
            output_schema: vec![InferencerTensorDeclaration {
                tensor: "score".to_string(),
                schema: inferencer_tensor_schema(nonzero!(1u32)),
            }],
        },
    );
    let wasm = processor_spec(
        ModelKind::WasmProcessor,
        "transform_events",
        vec![input.clone()],
        BranchedProcessorOperationSpec::WasmProcessor {
            output_routes: BranchedProcessorOutputsSpec {
                routes: vec![output_spec(
                    "transformed_events",
                    "SET id = id, value = value, key = key, vector = vector WHERE value > 0",
                    None,
                )],
            },
            resource: named("event_transform"),
            resource_version: 1,
            file: "processors/transform.wasm".to_string(),
            limits: WasmProcessorLimits {
                max_fuel: nonzero!(1_000_000u64),
                max_memory_bytes: nonzero!(1_048_576u64),
            },
            rejected_state_policy: WasmRejectedStatePolicy::Preserve,
        },
    );
    let mut correlator = processor_spec(
        ModelKind::Correlator,
        "correlate_events",
        vec![left.clone(), right.clone()],
        BranchedProcessorOperationSpec::Correlator {
            output_routes: BranchedProcessorOutputsSpec {
                routes: vec![output_spec(
                    "correlated_events",
                    "SET id = left.id, value = right.value, key = left.key, vector = left.vector \
                     WHERE output.value > 0",
                    Some(FlushPolicy::Immediate),
                )],
            },
            left_relays: vec![left.clone()],
            right_relays: vec![right.clone()],
            correlate_where: expression("left.id = right.id"),
            match_policy: CorrelatorMatchPolicy::Earliest,
            max_time: "1m".to_string(),
            timeout_policy: CorrelationTimeoutPolicy {
                left: CorrelationTimeoutAction::Drop,
                right: CorrelationTimeoutAction::Drop,
            },
        },
    );
    correlator
        .from_where
        .insert(left.clone(), expression("left.value > 0"));
    correlator
        .from_where
        .insert(right.clone(), expression("right.value > 0"));

    let output_relays = [
        "junction_events",
        "unique_events",
        "ordered_events",
        "transformed_events",
        "correlated_events",
    ];
    let mut relay_schemas = [
        (input.clone(), generic_schema.clone()),
        (left.clone(), generic_schema.clone()),
        (right.clone(), generic_schema.clone()),
        (features.clone(), generic_schema.clone()),
        (named("window_events_out"), window_schema),
        (named("scored_events"), score_schema),
    ]
    .into_iter()
    .collect::<HashMap<_, _>>();
    for relay in output_relays {
        relay_schemas.insert(named(relay), generic_schema.clone());
    }
    let relay_branchings = relay_schemas
        .keys()
        .cloned()
        .map(|relay| (relay, ResolvedBranching::unbranched()))
        .collect::<HashMap<_, _>>();
    let nodes = [
        junction,
        deduplicator,
        reorderer,
        window,
        inferencer,
        wasm,
        correlator,
    ];
    let processors = materialize_nodes(&nodes, &relay_schemas, None)
        .assured("all processor fixtures use exact compatible schemas")
        .into_iter()
        .map(|processor| (processor.processor.clone(), processor))
        .collect();
    let mut template = junction_branch_template("published_revision", "incoming");
    template.processors = processors;

    bind_processor_template_programs(
        &named("test_domain"),
        &mut template,
        &relay_schemas,
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .assured("every processor program binds before the revision is published");

    let route_programs_are_bound = |routes: &RelayProcessorOutputsTemplate| {
        routes
            .routes
            .iter()
            .all(|route| route.compiled_program.is_some())
    };
    let junction = template
        .processors
        .get("route_events")
        .assured("the fixture contains its junction");
    assert_eq!(junction.compiled_from_where.len(), 1);
    assert_eq!(junction.compiled_filter_where.len(), 1);
    assert!(matches!(
        &junction.operation,
        RelayProcessorOperationTemplate::Junction { output_routes }
            if route_programs_are_bound(output_routes)
    ));
    assert!(matches!(
        &template
            .processors
            .get("deduplicate_events")
            .assured("the fixture contains its deduplicator")
            .operation,
        RelayProcessorOperationTemplate::Deduplicator {
            output_routes,
            compiled_key_program: Some(_),
            ..
        } if route_programs_are_bound(output_routes)
    ));
    assert!(matches!(
        &template
            .processors
            .get("reorder_events")
            .assured("the fixture contains its reorderer")
            .operation,
        RelayProcessorOperationTemplate::Reorderer {
            output_routes,
            compiled_program: Some(_),
            ..
        } if route_programs_are_bound(output_routes)
    ));
    assert!(matches!(
        &template
            .processors
            .get("window_events")
            .assured("the fixture contains its window processor")
            .operation,
        RelayProcessorOperationTemplate::WindowProcessor { output_routes, .. }
            if route_programs_are_bound(output_routes)
    ));
    assert!(matches!(
        &template
            .processors
            .get("score_events")
            .assured("the fixture contains its inferencer")
            .operation,
        RelayProcessorOperationTemplate::Inferencer { output_routes, .. }
            if route_programs_are_bound(output_routes)
    ));
    assert!(matches!(
        &template
            .processors
            .get("transform_events")
            .assured("the fixture contains its WASM processor")
            .operation,
        RelayProcessorOperationTemplate::WasmProcessor { output_routes, .. }
            if route_programs_are_bound(output_routes)
    ));
    let correlator = template
        .processors
        .get("correlate_events")
        .assured("the fixture contains its correlator");
    assert_eq!(correlator.compiled_from_where.len(), 2);
    assert!(matches!(
        &correlator.operation,
        RelayProcessorOperationTemplate::Correlator {
            compiled_where_program: Some(_),
            compiled_output_programs,
            ..
        } if compiled_output_programs.iter().all(Option::is_some)
    ));
}

#[test]
fn eager_binding_classifies_incomplete_published_inputs() {
    let domain = named::<DomainName>("test_domain");
    let input = named::<RelayName>("incoming");
    let output = named::<RelayName>("outgoing");
    let mut template = junction_branch_template("route_events", "incoming");
    let processor = template
        .processors
        .get_mut("route_events")
        .assured("the junction fixture contains its processor");
    assert!(matches!(
        &processor.operation,
        RelayProcessorOperationTemplate::Junction { .. }
    ));
    if let RelayProcessorOperationTemplate::Junction { output_routes } = &mut processor.operation {
        output_routes.routes.push(RelayProcessorOutputTemplate {
            output_relay: output.clone(),
            construction: construction("INHERIT ALL"),
            flush_policy: Some(RuntimeFlushPolicy::Immediate),
            message_error_policy: MessageErrorPolicy::Log,
            compiled_program: None,
        });
    }
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let relay_schemas = [(input.clone(), schema)].into_iter().collect();
    let relay_branchings = [(input.clone(), ResolvedBranching::unbranched())]
        .into_iter()
        .collect();

    let missing_input_schema = bind_processor_template_programs(
        &domain,
        &mut template.clone(),
        &HashMap::default(),
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("an input relay without its compiled schema must fail binding");
    assert!(matches!(
        missing_input_schema.current_context(),
        PlanningError::MissingInputSchema { relay, .. } if relay == &input
    ));

    let missing_branching = bind_processor_template_programs(
        &domain,
        &mut template.clone(),
        &relay_schemas,
        &HashMap::default(),
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("an input relay without resolved branching must fail binding");
    assert!(matches!(
        missing_branching.current_context(),
        PlanningError::MissingInputBranching { relay, .. } if relay == &input
    ));

    let secondary = named::<RelayName>("secondary");
    let mut secondary_template = template.clone();
    secondary_template
        .processors
        .get_mut("route_events")
        .assured("the junction fixture contains its processor")
        .input_relays
        .push(secondary.clone());
    let missing_secondary_schema = bind_processor_template_programs(
        &domain,
        &mut secondary_template.clone(),
        &relay_schemas,
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("every additional input relay must have a compiled schema");
    assert!(matches!(
        missing_secondary_schema.current_context(),
        PlanningError::MissingInputSchema { relay, .. } if relay == &secondary
    ));

    let mut secondary_schemas = relay_schemas.clone();
    secondary_schemas.insert(
        secondary.clone(),
        relay_schemas
            .get(&input)
            .assured("the primary input schema was installed")
            .clone(),
    );
    let missing_secondary_branching = bind_processor_template_programs(
        &domain,
        &mut secondary_template,
        &secondary_schemas,
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("every additional input relay must have resolved branching");
    assert!(matches!(
        missing_secondary_branching.current_context(),
        PlanningError::MissingInputBranching { relay, .. } if relay == &secondary
    ));

    let correlator_template = |left_relays: Vec<RelayName>, right_relays: Vec<RelayName>| {
        let mut template = junction_branch_template("correlate_events", "incoming");
        let processor = template
            .processors
            .get_mut("correlate_events")
            .assured("the correlator fixture contains its processor");
        processor.kind = ModelKind::Correlator;
        processor.operation = RelayProcessorOperationTemplate::Correlator {
            output_routes: RelayProcessorOutputsTemplate { routes: Vec::new() },
            left_relays,
            right_relays,
            correlate_where: expression("left.value = right.value"),
            match_policy: CorrelatorMatchPolicy::Earliest,
            max_time: Duration::from_secs(60),
            timeout_policy: CorrelationTimeoutPolicy {
                left: CorrelationTimeoutAction::Drop,
                right: CorrelationTimeoutAction::Drop,
            },
            compiled_where_program: None,
            compiled_output_programs: Vec::new(),
        };
        template
    };
    let missing = named::<RelayName>("missing");

    let missing_left_relay = bind_processor_template_programs(
        &domain,
        &mut correlator_template(Vec::new(), vec![input.clone()]),
        &relay_schemas,
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("a correlator must publish a left input relay");
    assert!(matches!(
        missing_left_relay.current_context(),
        PlanningError::MissingInputRelay { .. }
    ));

    let missing_right_relay = bind_processor_template_programs(
        &domain,
        &mut correlator_template(vec![input.clone()], Vec::new()),
        &relay_schemas,
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("a correlator must publish a right input relay");
    assert!(matches!(
        missing_right_relay.current_context(),
        PlanningError::MissingInputRelay { .. }
    ));

    let missing_left_schema = bind_processor_template_programs(
        &domain,
        &mut correlator_template(vec![missing.clone()], vec![input.clone()]),
        &relay_schemas,
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("a correlator left relay must have a compiled schema");
    assert!(matches!(
        missing_left_schema.current_context(),
        PlanningError::MissingInputSchema { relay, .. } if relay == &missing
    ));

    let missing_right_schema = bind_processor_template_programs(
        &domain,
        &mut correlator_template(vec![input.clone()], vec![missing.clone()]),
        &relay_schemas,
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("a correlator right relay must have a compiled schema");
    assert!(matches!(
        missing_right_schema.current_context(),
        PlanningError::MissingInputSchema { relay, .. } if relay == &missing
    ));

    let missing_output_schema = bind_processor_template_programs(
        &domain,
        &mut template.clone(),
        &relay_schemas,
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("an output relay without its compiled schema must fail binding");
    assert!(matches!(
        missing_output_schema.current_context(),
        PlanningError::MissingOutputSchema { relay, .. } if relay == &output
    ));

    template
        .processors
        .get_mut("route_events")
        .assured("the junction fixture contains its processor")
        .input_relays
        .clear();
    let missing_input = bind_processor_template_programs(
        &domain,
        &mut template,
        &relay_schemas,
        &relay_branchings,
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("a processor without an input relay must fail binding");
    assert!(matches!(
        missing_input.current_context(),
        PlanningError::MissingInputRelay { node, .. } if node.as_str() == "route_events"
    ));
}

#[test]
fn planning_parsers_preserve_typed_contract_failures() {
    let processor = named::<ModelName>("orders_processor");
    let relay = named::<RelayName>("orders");

    let window_duration = parse_optional_window_duration(
        &processor,
        WindowDurationSetting::Width,
        Some("not-a-duration"),
    )
    .expect_err("an invalid window width must fail");
    assert!(matches!(
        window_duration.current_context(),
        PlanningError::InvalidWindowDuration {
            node,
            setting: WindowDurationSetting::Width,
        } if node == &processor
    ));

    let output = BranchedProcessorOutputSpec {
        relay: relay.clone(),
        construction: RouteConstruction::default(),
        flush_policy: Some(FlushPolicy::Each {
            interval: "1s".to_string(),
            max_batch_size: "not-a-size".to_string(),
        }),
        message_error_policy: MessageErrorPolicy::Log,
    };
    let flush_size = materialize_output(
        ModelKind::Deduplicator,
        &processor,
        &output,
        FlushPolicyRequirement::Required,
    )
    .expect_err("an invalid flush batch size must fail");
    assert!(matches!(
        flush_size.current_context(),
        PlanningError::InvalidFlushMaxBatchSize {
            kind: ModelKind::Deduplicator,
            node,
            route,
        } if node == &processor && route == &relay
    ));

    let collect_interval = parse_input_collect_policy(
        ModelKind::Junction,
        &processor,
        &relay,
        &nervix_models::InputCollectPolicy {
            collect_for: "not-a-duration".to_string(),
            max_batch_size: None,
        },
    )
    .expect_err("an invalid collection interval must fail");
    assert!(matches!(
        collect_interval.current_context(),
        PlanningError::InvalidCollectInterval {
            kind: ModelKind::Junction,
            node,
            relay: error_relay,
        } if node == &processor && error_relay == &relay
    ));

    let collect_size = parse_input_collect_policy(
        ModelKind::Junction,
        &processor,
        &relay,
        &nervix_models::InputCollectPolicy {
            collect_for: "1s".to_string(),
            max_batch_size: Some("not-a-size".to_string()),
        },
    )
    .expect_err("an invalid collection batch size must fail");
    assert!(matches!(
        collect_size.current_context(),
        PlanningError::InvalidCollectMaxBatchSize {
            kind: ModelKind::Junction,
            node,
            relay: error_relay,
        } if node == &processor && error_relay == &relay
    ));

    let unbounded_collection = parse_input_collect_policy(
        ModelKind::Junction,
        &processor,
        &relay,
        &nervix_models::InputCollectPolicy {
            collect_for: "1s".to_string(),
            max_batch_size: None,
        },
    )
    .expect("a collection policy may omit its byte bound");
    assert_eq!(unbounded_collection.max_batch_size, None);

    let max_time = parse_max_time(ModelKind::Deduplicator, &processor, "not-a-duration")
        .expect_err("an invalid maximum retention time must fail");
    assert!(matches!(
        max_time.current_context(),
        PlanningError::InvalidMaxTime {
            kind: ModelKind::Deduplicator,
            node,
        } if node == &processor
    ));

    let branch_ttl =
        parse_branch_ttl_setting(Some("not-a-duration"), ModelKind::Deduplicator, &processor)
            .expect_err("an invalid branch TTL must fail");
    assert!(matches!(
        branch_ttl.current_context(),
        PlanningError::InvalidBranchTtl {
            kind: ModelKind::Deduplicator,
            node,
        } if node == &processor
    ));
}

#[test]
fn window_materialization_classifies_output_contract_failures() {
    let missing_output = materialize_nodes(
        &[window_node(BranchedProcessorOutputsSpec {
            routes: Vec::new(),
        })],
        &HashMap::default(),
        None,
    )
    .expect_err("a window processor without an output must fail");
    assert!(matches!(
        missing_output.current_context(),
        PlanningError::MissingWindowOutput { node } if node.as_str() == "metric_window"
    ));

    let inherited = RouteConstruction {
        inherit: Some(nervix_models::Inheritance::All),
        ..RouteConstruction::default()
    };
    let invalid_construction = materialize_nodes(
        &[window_node(BranchedProcessorOutputsSpec {
            routes: vec![BranchedProcessorOutputSpec {
                relay: named("metric_summary"),
                construction: inherited,
                flush_policy: None,
                message_error_policy: MessageErrorPolicy::Log,
            }],
        })],
        &HashMap::default(),
        None,
    )
    .expect_err("window output inheritance must fail lowering");
    assert!(matches!(
        invalid_construction.current_context(),
        PlanningError::InvalidWindowConstruction { node, route }
            if node.as_str() == "metric_window" && route.as_str() == "metric_summary"
    ));

    let compilation = materialize_nodes(
        &[window_node(BranchedProcessorOutputsSpec {
            routes: vec![BranchedProcessorOutputSpec {
                relay: named("metric_summary"),
                construction: construction("SET count = COUNT(input.value)"),
                flush_policy: None,
                message_error_policy: MessageErrorPolicy::Log,
            }],
        })],
        &HashMap::default(),
        None,
    )
    .expect_err("window output without runtime schemas must fail compilation");
    assert!(matches!(
        compilation.current_context(),
        PlanningError::WindowOutputCompilation { node, route }
            if node.as_str() == "metric_window" && route.as_str() == "metric_summary"
    ));
}

#[test]
fn inferencer_materialization_requires_an_input_and_schema() {
    let missing_input =
        materialize_nodes(&[inferencer_node(Vec::new())], &HashMap::default(), None)
            .expect_err("an inferencer without input must fail");
    assert!(matches!(
        missing_input.current_context(),
        PlanningError::MissingInputRelay {
            kind: ModelKind::Inferencer,
            node,
        } if node.as_str() == "score_model"
    ));

    let missing_schema = materialize_nodes(
        &[inferencer_node(vec![named("features")])],
        &HashMap::default(),
        None,
    )
    .expect_err("an inferencer input without a runtime schema must fail");
    assert!(matches!(
        missing_schema.current_context(),
        PlanningError::MissingInputSchema {
            kind: ModelKind::Inferencer,
            node,
            relay,
        } if node.as_str() == "score_model" && relay.as_str() == "features"
    ));
}

#[test]
fn relay_template_resolution_classifies_each_missing_owner() {
    let node = named::<ModelName>("orders_junction");
    let relay = named::<RelayName>("orders");
    let relay_ids = || std::iter::once(relay.clone()).collect();

    let missing_model = resolve_branch_relay_templates(
        ModelKind::Junction,
        &node,
        relay_ids(),
        &ModelIndex::default(),
        &HashMap::default(),
        &HashMap::default(),
    )
    .expect_err("an unconfigured relay must fail planning");
    assert!(matches!(
        missing_model.current_context(),
        PlanningError::MissingRelayModel {
            kind: ModelKind::Junction,
            node: error_node,
            route,
        } if error_node == &node && route == &relay
    ));

    let model_index = [Model::Relay(CreateRelay {
        name: relay.clone(),
        schema: named("orders_schema"),
        buffer: nonzero!(1usize),
        branching: RelayBranching::unbranched(),
        materialized_state: None,
    })]
    .into_iter()
    .collect::<ModelIndex>();
    let missing_registry = resolve_branch_relay_templates(
        ModelKind::Junction,
        &node,
        relay_ids(),
        &model_index,
        &HashMap::default(),
        &HashMap::default(),
    )
    .expect_err("a relay without a registry must fail planning");
    assert!(matches!(
        missing_registry.current_context(),
        PlanningError::MissingRelayRegistry {
            kind: ModelKind::Junction,
            node: error_node,
            route,
        } if error_node == &node && route == &relay
    ));

    let relay_registries = [(relay.clone(), RelayRegistry::new())]
        .into_iter()
        .collect();
    let missing_services = resolve_branch_relay_templates(
        ModelKind::Junction,
        &node,
        relay_ids(),
        &model_index,
        &relay_registries,
        &HashMap::default(),
    )
    .expect_err("a relay without boundary services must fail planning");
    assert!(matches!(
        missing_services.current_context(),
        PlanningError::MissingRelayServices {
            kind: ModelKind::Junction,
            node: error_node,
            route,
        } if error_node == &node && route == &relay
    ));
}

#[test]
fn processor_instance_materialization_requires_an_input_relay() {
    let node = BranchedProcessorNodeSpec {
        spec: BranchedProcessorSpec {
            kind: ModelKind::Junction,
            processor: named("orders_junction"),
            input_relays: Vec::new(),
            input_collect_policies: HashMap::default(),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),
            from_where: HashMap::default(),
            filter_where: None,
            materialized_state: Vec::new(),
            operation: BranchedProcessorOperationSpec::Junction {
                output_routes: BranchedProcessorOutputsSpec { routes: Vec::new() },
            },
        },
        branch: None,
        branch_ttl: None,
        branch_max_instances: None,
        wasm_state_reset: None,
        binding: Default::default(),
    };
    let error = materialize_processor_instance_template(
        &node,
        &ModelIndex::default(),
        &HashMap::default(),
        &HashMap::default(),
        &HashMap::default(),
        None,
    )
    .expect_err("a processor instance without input must fail planning");

    assert!(matches!(
        error.current_context(),
        PlanningError::MissingInputRelay {
            kind: ModelKind::Junction,
            node,
        } if node.as_str() == "orders_junction"
    ));
}

#[test]
fn inferencer_input_mappings_compile_when_template_is_materialized() {
    let input_relay = named::<RelayName>("features");
    let processor = named::<ModelName>("score_model");
    let input_schema = Arc::new(compile_schema(&CreateSchema {
        name: named("feature_schema"),
        fields: vec![SchemaField {
            name: named("vector"),
            ty: ParseAsType::Array {
                element: Box::new(ParseAsType::F32),
                len: nonzero!(2u32),
            },
            optional: false,
            sensitive: false,
        }],
    }));
    let node = BranchedProcessorSpec {
        kind: ModelKind::Inferencer,
        processor: processor.clone(),
        input_relays: vec![input_relay.clone()],
        input_collect_policies: HashMap::default(),
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        from_where: HashMap::default(),
        filter_where: None,
        materialized_state: Vec::new(),
        operation: BranchedProcessorOperationSpec::Inferencer {
            output_routes: BranchedProcessorOutputsSpec {
                routes: vec![BranchedProcessorOutputSpec {
                    relay: named("scores"),
                    construction: RouteConstruction::default(),
                    flush_policy: Some(FlushPolicy::Immediate),
                    message_error_policy: MessageErrorPolicy::Log,
                }],
            },
            resource: named("fraud_model"),
            resource_version: 1,
            file: "models/fraud.onnx".to_string(),
            inputs: vec![InferencerTensorMapping {
                tensor: "features".to_string(),
                schema: inferencer_tensor_schema(nonzero!(2u32)),
                expression: nervix_nspl::parse_expression("input.missing")
                    .expect("test expression must parse"),
            }],
            output_schema: vec![InferencerTensorDeclaration {
                tensor: "score".to_string(),
                schema: inferencer_tensor_schema(nonzero!(1u32)),
            }],
        },
    };
    let mut relay_schemas = HashMap::default();
    relay_schemas.insert(input_relay, input_schema);

    let error = materialize_nodes(&[node], &relay_schemas, None)
        .expect_err("invalid INPUTS mapping must fail template materialization");

    assert!(matches!(
        error.current_context(),
        PlanningError::InferencerInputCompilation { node, relay }
            if node == &processor && relay.as_str() == "features"
    ));
}

#[test]
fn missing_flush_policy_identifies_the_node_and_route() {
    let processor = named::<ModelName>("orders_deduplicator");
    let route = named::<RelayName>("deduplicated_orders");
    let output = BranchedProcessorOutputSpec {
        relay: route.clone(),
        construction: RouteConstruction::default(),
        flush_policy: None,
        message_error_policy: MessageErrorPolicy::Log,
    };

    let error = materialize_output(
        ModelKind::Deduplicator,
        &processor,
        &output,
        FlushPolicyRequirement::Required,
    )
    .expect_err("a flush-based route must declare its flush policy");

    assert!(matches!(
        error.current_context(),
        PlanningError::MissingFlushPolicy {
            kind,
            node,
            route: error_route,
        } if *kind == ModelKind::Deduplicator
            && node == &processor
            && error_route == &route
    ));
}

#[test]
fn invalid_flush_interval_identifies_the_node_and_route() {
    let processor = named::<ModelName>("orders_reorderer");
    let route = named::<RelayName>("ordered_orders");
    let output = BranchedProcessorOutputSpec {
        relay: route.clone(),
        construction: RouteConstruction::default(),
        flush_policy: Some(FlushPolicy::Each {
            interval: "not-a-duration".to_string(),
            max_batch_size: "1MiB".to_string(),
        }),
        message_error_policy: MessageErrorPolicy::Log,
    };

    let error = materialize_output(
        ModelKind::Reorderer,
        &processor,
        &output,
        FlushPolicyRequirement::Required,
    )
    .expect_err("an invalid flush interval must fail planning");

    assert!(matches!(
        error.current_context(),
        PlanningError::InvalidFlushInterval {
            kind,
            node,
            route: error_route,
        } if *kind == ModelKind::Reorderer
            && node == &processor
            && error_route == &route
    ));
}

#[test]
fn branched_node_specs_capture_downstream_processing_tree() {
    let specs = branched_node_specs_from_models(
        [
            branch_model("tenant", "orders", &["tenant"]),
            branch_model("tenant", "projected_orders", &["tenant"]),
            PlannedModel {
                kind: ModelKind::Ingestor,
                identifier: named("orders_ingestor"),
                model: nervix_models::Model::Ingestor(CreateIngestor {
                    name: named("orders_ingestor"),
                    output_routes: (ProcessorOutputs::single(named("orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(branched_by("orders", &["tenant"])),
                    decode_using_codec: named("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: named("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_orders"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_orders"),
                    from: ProcessorInputs::single(named("orders"))
                        .with_collect_policy("25ms".to_string(), Some("2MiB".to_string())),
                    output_routes: (ProcessorOutputs::single(named("projected_orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_projected_orders"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_projected_orders"),
                    from: ProcessorInputs::single(named("projected_orders")),
                    output_routes: (ProcessorOutputs::single(named("aggregated_orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("projected_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
            PlannedModel {
                kind: ModelKind::Emitter,
                identifier: named("orders_emitter"),
                model: nervix_models::Model::Emitter(CreateEmitter {
                    name: named("orders_emitter"),
                    from: ProcessorInputs::single(named("aggregated_orders")),
                    body: nervix_models::EmitterBody::Codec {
                        codec: named("orders_codec"),
                    },
                    sink: Box::new(EmitSink::ZeroMq {
                        client: named("zmq_client"),
                    }),
                    batch: None,
                    flush_policy: FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    },
                    mode: AckMode::Attached,
                    error_policies: ErrorPolicies::handled_by_log(),
                    publishing_mode: EmitterPublishingMode::NoAck {
                        retry_policy: RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    construction: nervix_models::RouteConstruction::default(),
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let spec = &specs.entrypoints[0];
    assert_eq!(spec.identifier, named("orders_ingestor"));
    assert_eq!(spec.root_relay, named("orders"));
    assert_eq!(spec.branch.as_ref(), Some(&named("by_orders")));
    assert_eq!(specs.processors.len(), 2);
    let dedup_orders = &specs.processors[0];
    assert_eq!(dedup_orders.spec.processor, named("dedup_orders"));
    assert_eq!(dedup_orders.spec.input_relays, vec![named("orders")]);
    let collect_policy = dedup_orders
        .spec
        .input_collect_policies
        .get(&RelayName::from(&named::<ModelName>("orders")))
        .expect("input collection policy must be planned for its source relay");
    assert_eq!(collect_policy.collect_for, "25ms");
    assert_eq!(collect_policy.max_batch_size.as_deref(), Some("2MiB"));
    assert_eq!(dedup_orders.branch.as_ref(), Some(&named("by_orders")));
    assert_eq!(dedup_orders.branch_ttl.as_deref(), Some("5m"));
    assert_eq!(dedup_orders.branch_max_instances, None);
    let BranchedProcessorOperationSpec::Deduplicator { output_routes, .. } =
        &dedup_orders.spec.operation
    else {
        panic!("expected deduplicator output");
    };
    let output = output_routes
        .routes
        .first()
        .expect("deduplicator should have output route");
    assert_eq!(output.relay, named("projected_orders"));
    let dedup_projected = &specs.processors[1];
    assert_eq!(
        dedup_projected.spec.processor,
        named("dedup_projected_orders")
    );
    assert_eq!(
        dedup_projected.spec.input_relays,
        vec![named("projected_orders")]
    );
    assert_eq!(dedup_projected.branch_ttl.as_deref(), Some("5m"));
}

#[test]
fn branched_node_specs_capture_window_processor_as_branch_node() {
    let specs = branched_node_specs_from_models(
        [
            branch_model("host", "metrics", &["host"]),
            branch_model("host", "metric_summary", &["host"]),
            PlannedModel {
                kind: ModelKind::Ingestor,
                identifier: named("metrics_ingestor"),
                model: nervix_models::Model::Ingestor(CreateIngestor {
                    name: named("metrics_ingestor"),
                    output_routes: (ProcessorOutputs::single(named("metrics")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(branched_by("metrics", &["host"])),
                    decode_using_codec: named("metrics_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: named("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::WindowProcessor,
                identifier: named("metric_window"),
                model: nervix_models::Model::WindowProcessor(CreateWindowProcessor {
                    name: named("metric_window"),
                    from: ProcessorInputs::single(named("metrics")),
                    output_routes: window_outputs(
                        "metric_summary",
                        "SET count = COUNT(input.latency)",
                    ),
                    branched_by: processor_branched_by("metrics", &["host"]),
                    width: WindowBound {
                        messages: Some(100),
                        duration: None,
                    },
                    step: WindowBound {
                        messages: Some(10),
                        duration: None,
                    },
                    state_limit: nervix_models::WindowStateLimit::Unbounded,
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_summary"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_summary"),
                    from: ProcessorInputs::single(named("metric_summary")),
                    output_routes: (ProcessorOutputs::single(named("projected_summary")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("metric_summary", &["host"]),
                    deduplicate_on: vec![expression("input.count")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let spec = &specs.entrypoints[0];
    assert_eq!(spec.root_relay, named("metrics"));
    assert_eq!(specs.processors.len(), 2);
    let window = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == named("metric_window"))
        .expect("window processor spec must exist");
    let BranchedProcessorOperationSpec::WindowProcessor {
        output_routes,
        width,
        step,
        ..
    } = &window.spec.operation
    else {
        panic!("expected window processor branch node");
    };
    let output = output_routes
        .routes
        .first()
        .expect("window processor should have output route");
    assert_eq!(output.relay, named("metric_summary"));
    assert_eq!(width.messages, Some(100));
    assert_eq!(step.messages, Some(10));
    assert_eq!(output.construction.assignments.len(), 1);
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == named("dedup_summary")
                && node.spec.input_relays == vec![named("metric_summary")])
    );
}

#[test]
fn branched_node_specs_capture_inferencer_as_branch_node() {
    let specs = branched_node_specs_from_models(
        [
            branch_model("tenant", "features", &["tenant"]),
            branch_model("tenant", "scores", &["tenant"]),
            PlannedModel {
                kind: ModelKind::Ingestor,
                identifier: named("features_ingestor"),
                model: nervix_models::Model::Ingestor(CreateIngestor {
                    name: named("features_ingestor"),
                    output_routes: (ProcessorOutputs::single(named("features")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(branched_by("features", &["tenant"])),
                    decode_using_codec: named("features_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: named("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::Inferencer,
                identifier: named("score_model"),
                model: nervix_models::Model::Inferencer(CreateInferencer {
                    name: named("score_model"),
                    from: ProcessorInputs::single(named("features")),
                    output_routes: (ProcessorOutputs::single(named("scores")))
                        .with_flush_policy(FlushPolicy::Immediate),
                    branched_by: processor_branched_by("features", &["tenant"]),
                    resource: named("fraud_model"),
                    resource_version: 3,
                    file: "models/fraud.onnx".to_string(),
                    inputs: vec![InferencerTensorMapping {
                        tensor: "features".to_string(),
                        schema: inferencer_tensor_schema(nonzero!(2u32)),
                        expression: expression("input.vector"),
                    }],
                    output_schema: vec![InferencerTensorDeclaration {
                        tensor: "score".to_string(),
                        schema: inferencer_tensor_schema(nonzero!(1u32)),
                    }],
                    mode: AckMode::Attached,
                    filter_where: Some(expression("input.active")),
                    materialized_state: Vec::new(),
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_scores"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_scores"),
                    from: ProcessorInputs::single(named("scores")),
                    output_routes: (ProcessorOutputs::single(named("projected_scores")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("scores", &["tenant"]),
                    deduplicate_on: vec![expression("input.score")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let spec = &specs.entrypoints[0];
    assert_eq!(spec.root_relay, named("features"));
    assert_eq!(specs.processors.len(), 2);
    let inferencer = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == named("score_model"))
        .expect("inferencer spec must exist");
    let BranchedProcessorOperationSpec::Inferencer {
        output_routes,
        resource,
        resource_version,
        file,
        inputs,
        output_schema,
        ..
    } = &inferencer.spec.operation
    else {
        panic!("expected inferencer branch node");
    };
    let output = output_routes
        .routes
        .first()
        .expect("inferencer should have output route");
    assert_eq!(output.relay, named("scores"));
    assert_eq!(resource, &named("fraud_model"));
    assert_eq!(*resource_version, 3);
    assert_eq!(file, "models/fraud.onnx");
    assert_eq!(inputs.len(), 1);
    assert_eq!(output_schema.len(), 1);
    assert_eq!(output.flush_policy, Some(FlushPolicy::Immediate));
    assert_eq!(
        inferencer.spec.filter_where,
        Some(expression("input.active"))
    );
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == named("dedup_scores")
                && node.spec.input_relays == vec![named("scores")])
    );
}

#[test]
fn branched_node_specs_capture_reingestor_entrypoint_tree() {
    let specs = branched_node_specs_from_models(
        [
            branch_model("tenant", "tenant_orders", &["tenant"]),
            PlannedModel {
                kind: ModelKind::Reingestor,
                identifier: named("tenant_partition"),
                model: nervix_models::Model::Reingestor(CreateReingestor {
                    name: named("tenant_partition"),
                    from: ProcessorInputs::single(named("orders")),
                    output_routes: with_inherit_all(ProcessorOutputs::single(named(
                        "tenant_orders",
                    )))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    })
                    .with_branch(branched_by("tenant_orders", &["tenant"])),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_orders"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_orders"),
                    from: ProcessorInputs::single(named("tenant_orders")),
                    output_routes: (ProcessorOutputs::single(named("projected_orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("tenant_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let spec = &specs.entrypoints[0];
    assert_eq!(spec.kind, ModelKind::Reingestor);
    assert_eq!(spec.identifier, named("tenant_partition"));
    assert_eq!(spec.root_relay, named("tenant_orders"));
    assert_eq!(spec.branch.as_ref(), Some(&named("by_tenant_orders")));
    assert_eq!(specs.processors.len(), 1);
    assert_eq!(specs.processors[0].spec.processor, named("dedup_orders"));
    assert_eq!(
        specs.processors[0].spec.input_relays,
        vec![named("tenant_orders")]
    );
    assert_eq!(
        specs.processors[0].branch.as_ref(),
        Some(&named("by_tenant_orders"))
    );
    assert_eq!(specs.processors[0].branch_ttl.as_deref(), Some("5m"));
}

#[test]
fn branched_node_specs_capture_processor_output_route_tree() {
    let specs = branched_node_specs_from_models(
        [
            branch_model("tenant", "orders", &["tenant"]),
            branch_model("tenant", "urgent_orders", &["tenant"]),
            branch_model("tenant", "default_orders", &["tenant"]),
            PlannedModel {
                kind: ModelKind::Ingestor,
                identifier: named("orders_ingestor"),
                model: nervix_models::Model::Ingestor(CreateIngestor {
                    name: named("orders_ingestor"),
                    output_routes: (ProcessorOutputs::single(named("orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(branched_by("orders", &["tenant"])),
                    decode_using_codec: named("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: named("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("orders_splitter"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("orders_splitter"),
                    from: ProcessorInputs::single(named("orders")),
                    output_routes: (ProcessorOutputs::new(vec![
                        ProcessorOutput {
                            relay: named("urgent_orders"),
                            construction: nervix_nspl::parse_route_construction(
                                "WHERE output.urgent",
                            )
                            .expect("route construction must parse"),
                            flush_policy: None,
                            message_error_policy: MessageErrorPolicy::Log,
                            branch: None,
                        },
                        ProcessorOutput {
                            relay: named("default_orders"),
                            construction: nervix_models::RouteConstruction::default(),
                            flush_policy: None,
                            message_error_policy: MessageErrorPolicy::Log,
                            branch: None,
                        },
                    ]))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    }),
                    branched_by: processor_branched_by("orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: Some(expression("input.active")),
                    materialized_state: Vec::new(),
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_urgent"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_urgent"),
                    from: ProcessorInputs::single(named("urgent_orders")),
                    output_routes: (ProcessorOutputs::single(named("urgent_projected")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("urgent_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_default"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_default"),
                    from: ProcessorInputs::single(named("default_orders")),
                    output_routes: (ProcessorOutputs::single(named("default_projected")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("default_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    assert_eq!(specs.processors.len(), 3);
    let splitter = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == named("orders_splitter"))
        .expect("splitter spec must exist");
    let BranchedProcessorOperationSpec::Deduplicator { output_routes, .. } =
        &splitter.spec.operation
    else {
        panic!("expected deduplicator output routes");
    };
    assert_eq!(splitter.spec.filter_where, Some(expression("input.active")));
    assert_eq!(output_routes.routes.len(), 2);
    assert_eq!(
        output_routes.routes[0].construction.where_clause,
        Some(expression("output.urgent"))
    );
    assert_eq!(output_routes.routes[0].relay, named("urgent_orders"));
    assert_eq!(output_routes.routes[1].relay, named("default_orders"));
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == named("dedup_urgent")
                && node.spec.input_relays == vec![named("urgent_orders")])
    );
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == named("dedup_default")
                && node.spec.input_relays == vec![named("default_orders")])
    );
}

#[test]
fn branched_node_specs_capture_junction_as_single_branch_processor() {
    let specs = branched_node_specs_from_models(
        [
            branch_model("tenant", "left_stream", &["tenant"]),
            branch_model("tenant", "right_stream", &["tenant"]),
            branch_model("tenant", "joined_stream", &["tenant"]),
            PlannedModel {
                kind: ModelKind::Ingestor,
                identifier: named("left_ingestor"),
                model: nervix_models::Model::Ingestor(CreateIngestor {
                    name: named("left_ingestor"),
                    output_routes: (ProcessorOutputs::single(named("left_stream")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(branched_by("left_stream", &["tenant"])),
                    decode_using_codec: named("notification_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: named("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::Ingestor,
                identifier: named("right_ingestor"),
                model: nervix_models::Model::Ingestor(CreateIngestor {
                    name: named("right_ingestor"),
                    output_routes: (ProcessorOutputs::single(named("right_stream")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(branched_by("right_stream", &["tenant"])),
                    decode_using_codec: named("notification_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: named("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::Junction,
                identifier: named("join_streams"),
                model: nervix_models::Model::Junction(CreateJunction {
                    name: named("join_streams"),
                    from: ProcessorInputs::new(
                        vec![named("left_stream"), named("right_stream")],
                        Vec::new(),
                    ),
                    output_routes: (ProcessorOutputs::single(named("joined_stream")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("left_stream", &["tenant"]),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_joined"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_joined"),
                    from: ProcessorInputs::single(named("joined_stream")),
                    output_routes: (ProcessorOutputs::single(named("projected_joined")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("joined_stream", &["tenant"]),
                    deduplicate_on: vec![expression("input.tenant")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 2);
    assert_eq!(
        specs
            .processors
            .iter()
            .filter(|node| node.spec.processor == named("join_streams"))
            .count(),
        1
    );
    let junction = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == named("join_streams"))
        .expect("junction spec must exist");
    assert_eq!(
        junction.spec.input_relays,
        vec![named("left_stream"), named("right_stream")]
    );
    let BranchedProcessorOperationSpec::Junction { output_routes, .. } = &junction.spec.operation
    else {
        panic!("expected junction processor");
    };
    let output = output_routes
        .routes
        .first()
        .expect("junction should have output route");
    assert_eq!(output.relay, named("joined_stream"));
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == named("dedup_joined"))
    );
}

#[test]
fn branched_node_specs_capture_single_processor_output_route_tree() {
    let specs = branched_node_specs_from_models(
        [
            branch_model("tenant", "orders", &["tenant"]),
            branch_model("tenant", "projected_orders", &["tenant"]),
            PlannedModel {
                kind: ModelKind::Ingestor,
                identifier: named("orders_ingestor"),
                model: nervix_models::Model::Ingestor(CreateIngestor {
                    name: named("orders_ingestor"),
                    output_routes: (ProcessorOutputs::single(named("orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(branched_by("orders", &["tenant"])),
                    decode_using_codec: named("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: named("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("orders_filter"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("orders_filter"),
                    from: ProcessorInputs::new(
                        vec![named("orders")],
                        vec![ProcessorInputWhere {
                            relay: named("orders"),
                            where_clause: expression("input.active"),
                        }],
                    ),
                    output_routes: (ProcessorOutputs::single(named("projected_orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: Some(expression("input.active")),
                    materialized_state: Vec::new(),
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_projected"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_projected"),
                    from: ProcessorInputs::single(named("projected_orders")),
                    output_routes: (ProcessorOutputs::single(named("aggregated_orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("projected_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let orders_filter = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == named("orders_filter"))
        .expect("orders filter spec must exist");
    assert_eq!(
        orders_filter
            .spec
            .from_where
            .get(&RelayName::from(&named::<ModelName>("orders"))),
        Some(&expression("input.active"))
    );
    let BranchedProcessorOperationSpec::Deduplicator { output_routes, .. } =
        &orders_filter.spec.operation
    else {
        panic!("expected processor output routes");
    };
    assert_eq!(
        orders_filter.spec.filter_where,
        Some(expression("input.active"))
    );
    assert_eq!(output_routes.routes.len(), 1);
    assert_eq!(output_routes.routes[0].relay, named("projected_orders"));
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == named("dedup_projected")
                && node.spec.input_relays == vec![named("projected_orders")])
    );
}

#[test]
fn branched_node_specs_include_singleton_branch_for_empty_branching() {
    let specs = branched_node_specs_from_models(
        [
            PlannedModel {
                kind: ModelKind::Ingestor,
                identifier: named("orders_ingestor"),
                model: nervix_models::Model::Ingestor(CreateIngestor {
                    name: named("orders_ingestor"),
                    output_routes: (ProcessorOutputs::single(named("orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(OutputBranch::Unbranched),
                    decode_using_codec: named("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: named("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_orders"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_orders"),
                    from: ProcessorInputs::single(named("orders")),
                    output_routes: (ProcessorOutputs::single(named("projected_orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: processor_branched_by("orders", &[]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    assert_eq!(specs.entrypoints[0].identifier, named("orders_ingestor"));
    assert_eq!(specs.entrypoints[0].root_relay, named("orders"));
    assert_eq!(specs.entrypoints[0].branch, None);
    assert_eq!(specs.entrypoints[0].branch_ttl, None);
    assert_eq!(specs.processors.len(), 1);
    assert_eq!(specs.processors[0].spec.processor, named("dedup_orders"));
    assert_eq!(specs.processors[0].branch_ttl, None);
    assert_eq!(specs.processors[0].branch, None);
    assert_eq!(specs.processors[0].branch_max_instances, None);
}

#[test]
fn branched_processor_specs_do_not_require_an_entrypoint() {
    let specs = branched_node_specs_from_models(
        [
            PlannedModel {
                kind: ModelKind::Relay,
                identifier: named("orders"),
                model: nervix_models::Model::Relay(CreateRelay {
                    name: named("orders"),
                    schema: named("order_event"),
                    buffer: nonzero!(1usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::Deduplicator,
                identifier: named("dedup_orders"),
                model: nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_orders"),
                    from: ProcessorInputs::single(named("orders")),
                    output_routes: (ProcessorOutputs::single(named("projected_orders")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert!(specs.entrypoints.is_empty());
    assert_eq!(specs.processors.len(), 1);
    assert_eq!(specs.processors[0].spec.processor, named("dedup_orders"));
    assert_eq!(specs.processors[0].spec.input_relays, vec![named("orders")]);
    assert_eq!(specs.processors[0].branch_ttl, None);
}

#[test]
fn branched_wasm_processor_specs_preserve_global_error_policy() {
    let specs = branched_node_specs_from_models(
        [
            PlannedModel {
                kind: ModelKind::Relay,
                identifier: named("orders"),
                model: nervix_models::Model::Relay(CreateRelay {
                    name: named("orders"),
                    schema: named("order_event"),
                    buffer: nonzero!(1usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }),
            },
            PlannedModel {
                kind: ModelKind::WasmProcessor,
                identifier: named("filter_orders"),
                model: nervix_models::Model::WasmProcessor(CreateWasmProcessor {
                    name: named("filter_orders"),
                    from: ProcessorInputs::single(named("orders")),
                    output_routes: ProcessorOutputs::single(named("filtered_orders")),
                    branched_by: BranchSelection::unbranched(),
                    resource: named("filter_resource"),
                    resource_version: 1,
                    file: "filter.wasm".to_string(),
                    limits: nervix_models::WasmProcessorLimits {
                        max_fuel: nonzero!(1_000_000_000u64),
                        max_memory_bytes: nonzero!(67_108_864u64),
                    },
                    global_error_policy: GeneralErrorPolicy::Ignore,
                    rejected_state_policy: Default::default(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.processors.len(), 1);
    assert_eq!(
        specs.processors[0].spec.error_policies.general,
        GeneralErrorPolicy::Ignore
    );
    assert_eq!(
        specs.processors[0].spec.error_policies.message,
        MessageErrorPolicy::Log
    );
}

#[test]
fn branched_node_specs_include_reingestor_with_declared_branching() {
    let specs = branched_node_specs_from_models(
        [
            branch_model("tenant", "tenant_notifications", &["tenant"]),
            PlannedModel {
                kind: ModelKind::Reingestor,
                identifier: named("tenant_partition"),
                model: nervix_models::Model::Reingestor(CreateReingestor {
                    name: named("tenant_partition"),
                    from: ProcessorInputs::single(named("notifications")),
                    output_routes: (ProcessorOutputs::single(named("tenant_notifications")))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        })
                        .with_branch(branched_by("tenant_notifications", &["tenant"])),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            },
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    assert_eq!(specs.entrypoints[0].identifier, named("tenant_partition"));
    assert_eq!(
        specs.entrypoints[0].root_relay,
        named("tenant_notifications")
    );
}

#[test]
fn window_route_demands_are_offset_by_the_routes_written_before_them() {
    let input_relay = named::<RelayName>("metrics");
    let totals_relay = named::<RelayName>("metric_totals");
    let extremes_relay = named::<RelayName>("metric_extremes");
    let metric_schema = Arc::new(compile_schema(&CreateSchema {
        name: named("metric"),
        fields: vec![
            SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: named("latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
        ],
    }));
    let totals_schema = Arc::new(compile_schema(&CreateSchema {
        name: named("metric_total"),
        fields: vec![
            SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: named("sample_count"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: named("first_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: named("total_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
        ],
    }));
    let extremes_schema = Arc::new(compile_schema(&CreateSchema {
        name: named("metric_extreme"),
        fields: vec![
            SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: named("max_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: named("min_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
        ],
    }));
    let totals_set = "SET tenant = FIRST(input.tenant), sample_count = COUNT(input.latency), \
                      first_latency = FIRST(input.latency), total_latency = SUM(input.latency)";
    let extremes_set = "SET tenant = LAST(input.tenant), max_latency = MAX(input.latency), \
                        min_latency = MIN(input.latency)";
    let node = BranchedProcessorSpec {
        kind: ModelKind::WindowProcessor,
        processor: named("route_scoped_latency"),
        input_relays: vec![input_relay.clone()],
        input_collect_policies: HashMap::default(),
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        from_where: HashMap::default(),
        filter_where: None,
        materialized_state: Vec::new(),
        operation: BranchedProcessorOperationSpec::WindowProcessor {
            output_routes: BranchedProcessorOutputsSpec {
                routes: vec![
                    BranchedProcessorOutputSpec {
                        relay: totals_relay.clone(),
                        construction: construction(totals_set),
                        flush_policy: Some(FlushPolicy::Immediate),
                        message_error_policy: MessageErrorPolicy::Log,
                    },
                    BranchedProcessorOutputSpec {
                        relay: extremes_relay.clone(),
                        construction: construction(extremes_set),
                        flush_policy: Some(FlushPolicy::Immediate),
                        message_error_policy: MessageErrorPolicy::Log,
                    },
                ],
            },
            width: WindowBound::of_messages(3),
            step: WindowBound::of_messages(3),
            state_limit: nervix_models::WindowStateLimit::Unbounded,
        },
    };
    let mut relay_schemas = HashMap::default();
    relay_schemas.insert(input_relay, metric_schema);
    relay_schemas.insert(totals_relay.clone(), totals_schema);
    relay_schemas.insert(extremes_relay.clone(), extremes_schema);

    let mut templates = materialize_nodes(&[node], &relay_schemas, None)
        .expect("two-route window processor must materialize");

    let template = templates.pop().expect("one template per spec");
    let RelayProcessorOperationTemplate::WindowProcessor {
        output_routes,
        aggregate,
        compiled_aggregates,
        ..
    } = &template.operation
    else {
        panic!("expected a window processor template");
    };

    // The routes keep their written order, and each compiled program stays aligned with the
    // route it was compiled for.
    let route_relays: Vec<_> = output_routes
        .routes
        .iter()
        .map(|route| route.output_relay.clone())
        .collect();
    assert_eq!(route_relays, vec![totals_relay, extremes_relay]);
    assert_eq!(compiled_aggregates.len(), 2);

    // `FIRST(input.tenant)`, `COUNT(input.latency)`, `FIRST(input.latency)` and
    // `SUM(input.latency)` need four separate structures; `MAX` and `MIN` over the same input
    // deduplicate into one, so the second route claims two.
    assert_eq!(compiled_aggregates[0].demand_offset, 0);
    assert_eq!(compiled_aggregates[1].demand_offset, 4);
    assert_eq!(aggregate.demands().len(), 6);

    // The compiled programs own the assignments once compilation has run.
    for route in &output_routes.routes {
        assert!(route.construction.assignments.is_empty());
    }
}
