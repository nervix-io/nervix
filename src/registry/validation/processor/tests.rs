//! Layer: harness.
//! Owns: current processor validation regressions.
//! Depends on: registry decisions, Models and expression validation.
//! Must not know: runtime execution internals.

use std::fs;

use nervix_models::{
    AckMode, BranchSelection, CreateJunction, CreateReingestor, CreateWireSchema,
    InputCollectPolicy, JsonType, MessageErrorPolicy, Model, OutputBranch, ParseAsType,
    ProcessorInputs, WireSchemaField, WireSchemaName,
};

use super::*;
use crate::registry::{
    storage::Registry,
    test_fixtures::{
        branch_for_relay, branch_name_for_relay, branch_schema, client_model, codec, deduplicator,
        example_graph_models, explicitly_unbranched_relay, ingestor_with_params, junction, named,
        processor, relay, relay_branched_by_relay_branch, relay_branched_like, schema,
        temp_db_path, unbranched_transforming_outputs, window_processor, with_inherit_all,
        with_output_branch,
    },
};

#[test]
fn apply_batch_accepts_inferencer_generated_output() {
    let (domain, models) = example_graph_models(
        "inferencer generated output schema",
        r#"
        CREATE SCHEMA features (
          tenant STRING,
          vector ARRAY<F32, 2>
        );

        CREATE SCHEMA scored (
          score ARRAY<F32, 1>
        );

        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE RELAY features SCHEMA features BRANCHED BY by_tenant_branch;
        CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant_branch;
        CREATE BRANCH by_tenant_branch
          SCHEMA tenant_branch TTL 5m;

        CREATE INFERENCER score_model
          FROM features
          USING RESOURCE fraud_model VERSION 1
          FILE 'models/simple_score.onnx'
          INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector }
          OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] }
          BRANCHED BY by_tenant_branch
          TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    registry
        .apply_batch(&domain, models)
        .expect("inferencer should construct output from immutable generated state");

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_mixed_inferencer_execution_modes() {
    let (domain, models) = example_graph_models(
        "mixed inferencer execution modes",
        r#"
        CREATE SCHEMA features ( vector ARRAY<F32, 2> );
        CREATE SCHEMA scored ( score ARRAY<F32, 1> );
        CREATE RELAY features SCHEMA features UNBRANCHED;
        CREATE RELAY scored SCHEMA scored UNBRANCHED;
        CREATE INFERENCER score_model
          FROM features
          USING RESOURCE fraud_model VERSION 1 FILE 'models/simple_score.onnx'
          INPUTS { "features" DENSE TENSOR<F32>[BATCH, 2] = input.vector }
          OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] }
          UNBRANCHED
          TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let error = registry
        .apply_batch(&domain, models)
        .expect_err("mixed inferencer execution modes must fail");
    assert!(error.to_string().contains("mixes batched and per-message"));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_multiple_batch_axes_in_one_tensor() {
    let (domain, models) = example_graph_models(
        "multiple inferencer batch axes",
        r#"
        CREATE SCHEMA features ( vector ARRAY<F32, 2> );
        CREATE SCHEMA scored ( score ARRAY<F32, 1> );
        CREATE RELAY features SCHEMA features UNBRANCHED;
        CREATE RELAY scored SCHEMA scored UNBRANCHED;
        CREATE INFERENCER score_model
          FROM features
          USING RESOURCE fraud_model VERSION 1 FILE 'models/simple_score.onnx'
          INPUTS { "features" DENSE TENSOR<F32>[BATCH, BATCH, 2] = input.vector }
          OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[BATCH, 1] }
          UNBRANCHED
          TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let error = registry
        .apply_batch(&domain, models)
        .expect_err("multiple BATCH axes must fail");
    assert!(
        error
            .to_string()
            .contains("contains more than one BATCH axis")
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_accepts_window_processor_generated_output_schema() {
    let (domain, models) = example_graph_models(
        "window processor generated output schema",
        r#"
        CREATE SCHEMA metric (
          tenant STRING,
          latency I64
        );

        CREATE SCHEMA metric_summary (
          tenant STRING,
          sample_count I64
        );

        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant_branch;
        CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
        CREATE BRANCH by_tenant_branch
          SCHEMA tenant_branch TTL 5m;

        CREATE WINDOW PROCESSOR latency_window
          FROM metrics
          WIDTH 2 MESSAGES
          STEP 2 MESSAGES
          BRANCHED BY by_tenant_branch
          TO metric_summaries
            SET tenant = FIRST(input.tenant),
                sample_count = COUNT(input.latency)
            ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    registry
        .apply_batch(&domain, models)
        .expect("window aggregate outputs should define non-input output fields");

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_window_processor_unassigned_output_field() {
    let (domain, models) = example_graph_models(
        "window processor unassigned output field",
        r#"
        CREATE SCHEMA metric (
          tenant STRING,
          latency U64
        );

        CREATE SCHEMA metric_summary (
          tenant STRING,
          total_latency U64
        );

        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant_branch;
        CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
        CREATE BRANCH by_tenant_branch
          SCHEMA tenant_branch TTL 5m;

        CREATE WINDOW PROCESSOR latency_window
          FROM metrics
          WIDTH 10s DURATION
          STEP 5s DURATION
          BRANCHED BY by_tenant_branch
          TO metric_summaries
            SET total_latency = SUM(input.latency)
            ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let err = registry
        .apply_batch(&domain, models)
        .expect_err("window aggregate should reject unassigned output fields");
    assert!(
        format!("{err}").contains(
            "window aggregate must assign required output field 'metric_summaries.tenant'"
        ),
        "unexpected error: {err}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_accepts_window_output_route_filter_on_generated_output() {
    let (domain, models) = example_graph_models(
        "window processor output route filter",
        r#"
        CREATE SCHEMA metric (
          tenant STRING,
          latency I64
        );

        CREATE SCHEMA metric_summary (
          tenant STRING,
          sample_count I64,
          total_latency I64
        );

        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant_branch;
        CREATE RELAY high_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
        CREATE RELAY low_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
        CREATE BRANCH by_tenant_branch
          SCHEMA tenant_branch TTL 5m;

        CREATE WINDOW PROCESSOR first_window
          FROM metrics
          WIDTH 2 MESSAGES
          STEP 2 MESSAGES
          BRANCHED BY by_tenant_branch
          TO high_summaries
            SET tenant = FIRST(input.tenant),
                sample_count = COUNT(input.latency),
                total_latency = SUM(input.latency)
            WHERE total_latency >= 100
            ON MESSAGE ERROR LOG
          TO low_summaries
            SET tenant = FIRST(input.tenant),
                sample_count = COUNT(input.latency),
                total_latency = SUM(input.latency)
            ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    registry
        .apply_batch(&domain, models)
        .expect("window output route predicates should read generated output fields");

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_window_output_route_filter_on_live_input() {
    let (domain, models) = example_graph_models(
        "window processor output input filter",
        r#"
        CREATE SCHEMA metric (
          tenant STRING,
          latency I64
        );

        CREATE SCHEMA metric_summary (
          tenant STRING,
          total_latency I64
        );

        CREATE RELAY metrics SCHEMA metric UNBRANCHED;
        CREATE RELAY metric_summaries SCHEMA metric_summary UNBRANCHED;

        CREATE WINDOW PROCESSOR latency_window
          FROM metrics
          WIDTH 2 MESSAGES
          STEP 2 MESSAGES
          UNBRANCHED
          TO metric_summaries
            SET tenant = FIRST(input.tenant),
                total_latency = SUM(input.latency)
            WHERE input.latency > 0
            ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let error = registry
        .apply_batch(&domain, models)
        .expect_err("window route WHERE must not expose live input");
    assert!(
        format!("{error}").contains("input is unavailable after set-only output finalization"),
        "unexpected error: {error}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_accepts_wasm_output_routes_on_generated_output() {
    let (domain, models) = example_graph_models(
        "wasm processor output routes",
        r#"
        CREATE SCHEMA metric (
          value I64,
          source STRING
        );

        CREATE SCHEMA projected_metric (
          value I64,
          source STRING OPTIONAL,
          bucket STRING
        );

        CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
        CREATE RELAY even_metrics SCHEMA metric UNBRANCHED;
        CREATE RELAY projected_metrics SCHEMA projected_metric UNBRANCHED;

        CREATE WASM PROCESSOR route_guest_output
          FROM raw_metrics
          FILTER WHERE input.value >= 0
          USING RESOURCE wasm_filter VERSION 1
          FILE 'processors/filter_even.wasm'
          MAX FUEL 1000000000 MAX MEMORY 64MiB
          UNBRANCHED
          TO even_metrics
            SET value = value, source = source
            WHERE value >= 10
            ON MESSAGE ERROR LOG
          TO projected_metrics
            SET value = value,
                source = source,
                bucket = lower(bucket)
            ON MESSAGE ERROR LOG
          ON GLOBAL ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    registry
        .apply_batch(&domain, models)
        .expect("wasm output routes should read guest output fields");

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_accepts_processor_from_where() {
    let (domain, models) = example_graph_models(
        "processor source where",
        r#"
        CREATE SCHEMA metric (
          value I64,
          source STRING
        );

        CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
        CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;

        CREATE DEDUPLICATOR dedup_metrics
          FROM raw_metrics WHERE input.value >= 0
          DEDUPLICATE ON input.source
          MAX TIME 10m
          UNBRANCHED
          TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    registry
        .apply_batch(&domain, models)
        .expect("source WHERE should validate against the input relay");

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_non_boolean_processor_from_where() {
    let (domain, models) = example_graph_models(
        "processor non-boolean source where",
        r#"
        CREATE SCHEMA metric (
          value I64,
          source STRING
        );

        CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
        CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;

        CREATE DEDUPLICATOR dedup_metrics
          FROM raw_metrics WHERE input.value
          DEDUPLICATE ON input.source
          MAX TIME 10m
          UNBRANCHED
          TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let err = registry
        .apply_batch(&domain, models)
        .expect_err("non-boolean source WHERE must fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));
    assert!(
        format!("{err:#}").contains("FROM WHERE compile failed"),
        "unexpected error: {err:#}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_processor_from_where_unavailable_scope() {
    let (domain, models) = example_graph_models(
        "processor source where other relay",
        r#"
        CREATE SCHEMA metric (
          value I64,
          source STRING
        );

        CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
        CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;

        CREATE DEDUPLICATOR dedup_metrics
          FROM raw_metrics WHERE branch.value >= 0
          DEDUPLICATE ON input.source
          MAX TIME 10m
          UNBRANCHED
          TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let err = registry
        .apply_batch(&domain, models)
        .expect_err("source WHERE cannot reference a branch during unbranched execution");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("FROM WHERE") && rendered.contains("branch"),
        "unexpected error: {rendered}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_duplicate_wasm_output_route() {
    let (domain, models) = example_graph_models(
        "wasm processor duplicate output route",
        r#"
        CREATE SCHEMA metric (
          value I64
        );

        CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
        CREATE RELAY projected_metrics SCHEMA metric UNBRANCHED;

        CREATE WASM PROCESSOR route_guest_output
          FROM raw_metrics
          USING RESOURCE wasm_filter VERSION 1
          FILE 'processors/filter_even.wasm'
          MAX FUEL 1000000000 MAX MEMORY 64MiB
          UNBRANCHED
          TO projected_metrics SET value = value ON MESSAGE ERROR LOG
          TO projected_metrics SET value = value WHERE value >= 0 ON MESSAGE ERROR LOG
          ON GLOBAL ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let err = registry
        .apply_batch(&domain, models)
        .expect_err("duplicate WASM output routes must be rejected");
    assert!(
        format!("{err}")
            .contains("WASM processor output relay 'projected_metrics' is declared more than once"),
        "unexpected error: {err}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_accepts_unconditional_processor_output_route() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain =
        DomainName::parse("unconditional_processor_output_route").expect("domain should parse");

    registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                explicitly_unbranched_relay("raw_events", "event_schema"),
                explicitly_unbranched_relay("projected_events", "event_schema"),
                Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_events"),
                    from: ProcessorInputs::single(named("raw_events")),
                    output_routes: with_inherit_all(ProcessorOutputs::single(named(
                        "projected_events",
                    )))
                    .with_flush_policy(FlushPolicy::Immediate),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![
                        nervix_nspl::parse_expression("input.value")
                            .expect("deduplicate expression must parse"),
                    ],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ],
        )
        .expect("unconditional output route should be accepted");

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_zero_input_collection_boundaries() {
    let cases = [
        (
            InputCollectPolicy {
                collect_for: "0s".to_string(),
                max_batch_size: None,
            },
            "COLLECT FOR duration must be greater than zero",
        ),
        (
            InputCollectPolicy {
                collect_for: "1s".to_string(),
                max_batch_size: Some("0B".to_string()),
            },
            "COLLECT MAX BATCH SIZE must be greater than zero",
        ),
    ];

    for (index, (collect_policy, expected)) in cases.into_iter().enumerate() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse(&format!("invalid_input_collection_{index}"))
            .expect("domain should parse");
        let mut inputs = ProcessorInputs::single(named("raw_events"));
        inputs.collect_policy = Some(collect_policy);
        let junction = Model::Junction(CreateJunction {
            name: named("collect_events"),
            from: inputs,
            output_routes: unbranched_transforming_outputs("collected_events"),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        });

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    explicitly_unbranched_relay("raw_events", "event_schema"),
                    explicitly_unbranched_relay("collected_events", "event_schema"),
                    junction,
                ],
            )
            .expect_err("zero input collection boundaries must be rejected");
        assert!(
            format!("{error:#}").contains(expected),
            "unexpected validation error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }
}

#[test]
fn apply_batch_rejects_incompatible_deduplicator_stream_schemas() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                Model::Schema(CreateSchema {
                    name: SchemaName::parse("wide_schema").expect("valid identifier"),
                    fields: vec![
                        SchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: FieldName::parse("extra").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                    ],
                }),
                relay_branched_by_relay_branch("notifications", "event_schema"),
                relay_branched_like("wide", "wide_schema", "notifications"),
                branch_schema("value_branch", &["value"]),
                branch_for_relay("notifications", "value_branch"),
                processor("project", "notifications", "wide"),
            ],
        )
        .expect_err("deduplicator schema mismatch should fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_multiple_deduplicator_inputs_with_different_schemas() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                Model::Schema(CreateSchema {
                    name: named("wide_schema"),
                    fields: vec![
                        SchemaField {
                            name: named("value"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: named("extra"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                    ],
                }),
                explicitly_unbranched_relay("notifications_a", "event_schema"),
                explicitly_unbranched_relay("notifications_b", "wide_schema"),
                explicitly_unbranched_relay("deduped", "event_schema"),
                Model::Deduplicator(CreateDeduplicator {
                    name: named("dedup_notifications"),
                    from: ProcessorInputs::new(
                        vec![named("notifications_a"), named("notifications_b")],
                        Vec::new(),
                    ),
                    output_routes: (ProcessorOutputs::single(named("deduped")))
                        .with_flush_policy(FlushPolicy::Immediate),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![
                        nervix_nspl::parse_expression("input.value")
                            .expect("deduplicate expression must parse"),
                    ],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ],
        )
        .expect_err("deduplicator input schema mismatch should fail");

    let message = format!("{err:#}");
    assert!(
        message.contains("deduplicator input"),
        "unexpected error: {message}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_incompatible_junction_stream_schemas() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                Model::Schema(CreateSchema {
                    name: SchemaName::parse("wide_schema").expect("valid identifier"),
                    fields: vec![
                        SchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: FieldName::parse("extra").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                    ],
                }),
                relay("notifications_a", "event_schema"),
                relay("notifications_b", "wide_schema"),
                relay("merged", "event_schema"),
                branch_schema("value_branch", &["value"]),
                branch_for_relay("notifications_a", "value_branch"),
                junction(
                    "join_streams",
                    &["notifications_a", "notifications_b"],
                    "merged",
                ),
            ],
        )
        .expect_err("junction schema mismatch should fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::IncompatibleSchema { .. }
    ));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_deduplicator_field_missing_from_schema() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                relay_branched_by_relay_branch("notifications", "event_schema"),
                relay("deduped", "event_schema"),
                branch_schema("value_branch", &["value"]),
                branch_for_relay("notifications", "value_branch"),
                deduplicator(
                    "dedup",
                    "notifications",
                    "deduped",
                    "notifications.transaction_id",
                    "10m",
                ),
            ],
        )
        .expect_err("missing dedup field should fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));
    assert!(format!("{err}").contains("DEDUPLICATE ON compile failed"));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_correlate_where_non_boolean_predicate() {
    let (domain, models) = example_graph_models(
        "correlator non-boolean predicate",
        r#"
        CREATE SCHEMA event (
          value STRING
        );

        CREATE SCHEMA correlated_event (
          value STRING
        );

        CREATE RELAY left_events SCHEMA event UNBRANCHED;
        CREATE RELAY right_events SCHEMA event UNBRANCHED;
        CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;

        CREATE CORRELATOR correlate_events
          LEFT FROM left_events
          RIGHT FROM right_events
          CORRELATE WHERE lower(left.value)
          MATCH EARLIEST
          MAX TIME 5s
          ON CORRELATION TIMEOUT DROP, DROP
          UNBRANCHED
          TO correlated_events
            SET value = left.value
            FLUSH IMMEDIATE
            ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let err = registry
        .apply_batch(&domain, models)
        .expect_err("non-boolean CORRELATE WHERE must fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));
    assert!(
        format!("{err:#}").contains("CORRELATE WHERE compile failed"),
        "unexpected error: {err:#}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_accepts_correlator_side_scoped_from_where() {
    let (domain, models) = example_graph_models(
        "correlator side source predicates",
        r#"
        CREATE SCHEMA left_event (
          value STRING,
          marker I64
        );

        CREATE SCHEMA right_event (
          value STRING,
          active BOOL
        );

        CREATE SCHEMA correlated_event (
          value STRING
        );

        CREATE RELAY left_events SCHEMA left_event UNBRANCHED;
        CREATE RELAY right_events SCHEMA right_event UNBRANCHED;
        CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;

        CREATE CORRELATOR correlate_events
          LEFT FROM left_events WHERE left.marker > 0
          RIGHT FROM right_events WHERE right.active
          CORRELATE WHERE left.value = right.value
          MATCH EARLIEST
          MAX TIME 5s
          ON CORRELATION TIMEOUT DROP, DROP
          UNBRANCHED
          TO correlated_events
            SET value = left.value
            FLUSH IMMEDIATE
            ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    registry
        .apply_batch(&domain, models)
        .expect("side source predicates should use their correlator side scope");

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_correlate_where_non_input_namespace() {
    let (domain, models) = example_graph_models(
        "correlator non-input namespace",
        r#"
        CREATE SCHEMA tenant_branch (
          tenant STRING
        );

        CREATE SCHEMA event (
          tenant STRING,
          value STRING
        );

        CREATE SCHEMA correlated_event (
          value STRING
        );

        CREATE RELAY left_events SCHEMA event BRANCHED BY by_tenant_branch;
        CREATE RELAY right_events SCHEMA event BRANCHED BY by_tenant_branch;
        CREATE RELAY correlated_events SCHEMA correlated_event BRANCHED BY by_tenant_branch;
        CREATE BRANCH by_tenant_branch
          SCHEMA tenant_branch TTL 5m;

        CREATE CORRELATOR correlate_events
          LEFT FROM left_events
          RIGHT FROM right_events
          CORRELATE WHERE input.tenant = left.tenant
          MATCH EARLIEST
          MAX TIME 5s
          ON CORRELATION TIMEOUT DROP, DROP
          BRANCHED BY by_tenant_branch
          TO correlated_events
            SET value = left.value
            FLUSH IMMEDIATE
            ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let err = registry
        .apply_batch(&domain, models)
        .expect_err("non-input CORRELATE WHERE namespace must fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("CORRELATE WHERE compile failed") && rendered.contains("input"),
        "unexpected error: {rendered}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_validates_each_correlator_output_against_its_destination_schema() {
    let (domain, models) = example_graph_models(
        "correlator destination schemas",
        r#"
        CREATE SCHEMA event (
          value STRING
        );

        CREATE SCHEMA correlated_event (
          value STRING
        );

        CREATE SCHEMA correlation_count (
          count I64
        );

        CREATE RELAY left_events SCHEMA event UNBRANCHED;
        CREATE RELAY right_events SCHEMA event UNBRANCHED;
        CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;
        CREATE RELAY correlation_counts SCHEMA correlation_count UNBRANCHED;

        CREATE CORRELATOR correlate_events
          LEFT FROM left_events
          RIGHT FROM right_events
          CORRELATE WHERE left.value = right.value
          MATCH EARLIEST
          MAX TIME 5s
          ON CORRELATION TIMEOUT DROP, DROP
          UNBRANCHED
          TO correlated_events
            SET value = left.value
            FLUSH IMMEDIATE
            ON MESSAGE ERROR LOG
          TO correlation_counts
            SET count = left.value
            FLUSH IMMEDIATE
            ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let err = registry
        .apply_batch(&domain, models)
        .expect_err("each correlator route must use its own destination schema");

    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("correlator TO output 'correlation_counts' compile failed"),
        "unexpected error: {rendered}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_correlator_left_side_schema_mismatch() {
    let (domain, models) = example_graph_models(
        "correlator left schema mismatch",
        r#"
        CREATE SCHEMA left_event (
          value STRING
        );

        CREATE SCHEMA other_left_event (
          value I64
        );

        CREATE SCHEMA right_event (
          value STRING
        );

        CREATE SCHEMA correlated_event (
          value STRING
        );

        CREATE RELAY left_events SCHEMA left_event UNBRANCHED;
        CREATE RELAY other_left_events SCHEMA other_left_event UNBRANCHED;
        CREATE RELAY right_events SCHEMA right_event UNBRANCHED;
        CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;

        CREATE CORRELATOR correlate_events
          LEFT FROM left_events, other_left_events
          RIGHT FROM right_events
          CORRELATE WHERE left.value = right.value
          MATCH EARLIEST
          MAX TIME 5s
          ON CORRELATION TIMEOUT DROP, DROP
          UNBRANCHED
          TO correlated_events
            SET value = left.value
            FLUSH IMMEDIATE
            ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");

    let err = registry
        .apply_batch(&domain, models)
        .expect_err("same-side correlator schema mismatch must fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::IncompatibleSchema { .. }
    ));
    assert!(
        format!("{err:#}").contains("correlator left input requires equal internal schemas"),
        "unexpected error: {err:#}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_window_message_target() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                relay_branched_by_relay_branch("notifications", "event_schema"),
                relay_branched_like("summaries", "event_schema", "notifications"),
                branch_schema("value_branch", &["value"]),
                branch_for_relay("notifications", "value_branch"),
                window_processor(
                    "window",
                    "notifications",
                    "summaries",
                    "SET message.value = COUNT(input.value)",
                ),
            ],
        )
        .expect_err("message is not a window output target");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));
    assert!(
        format!("{err}").contains("window SET targets must be bare or output.<field>"),
        "unexpected error: {err}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_window_aggregate_argument_outside_input() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                relay_branched_by_relay_branch("notifications", "event_schema"),
                relay_branched_like("summaries", "event_schema", "notifications"),
                branch_schema("value_branch", &["value"]),
                branch_for_relay("notifications", "value_branch"),
                window_processor(
                    "window",
                    "notifications",
                    "summaries",
                    "SET value = COUNT(output.value)",
                ),
            ],
        )
        .expect_err("aggregate arguments must read the original input");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));
    assert!(
        format!("{err}").contains("window aggregate arguments may read only input fields"),
        "unexpected error: {err}"
    );

    let _ = fs::remove_dir_all(path);
}

#[test]
fn apply_batch_rejects_output_predicate_missing_from_schema() {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");

    let err = registry
        .apply_batch(
            &domain,
            vec![
                Model::Schema(CreateSchema {
                    name: SchemaName::parse("event_schema").expect("valid identifier"),
                    fields: vec![
                        SchemaField {
                            name: FieldName::parse("tenant").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::String,
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
                            name: FieldName::parse("tenant").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                        WireSchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                    ],
                }),
                codec("event_codec", "event_schema"),
                client_model("broker_in"),
                relay_branched_by_relay_branch("notifications", "event_schema"),
                relay_branched_by_relay_branch("errors", "event_schema"),
                relay_branched_like("info", "event_schema", "errors"),
                branch_schema("tenant_branch", &["tenant"]),
                branch_for_relay("notifications", "tenant_branch"),
                branch_for_relay("errors", "tenant_branch"),
                ingestor_with_params(
                    "ing",
                    "notifications",
                    "event_codec",
                    "broker_in",
                    &["tenant"],
                ),
                Model::Reingestor(CreateReingestor {
                    name: named("route_logs"),
                    from: ProcessorInputs::single(named("notifications")),
                    output_routes: with_output_branch(
                        with_inherit_all(ProcessorOutputs::new(vec![
                            ProcessorOutput {
                                relay: named("errors"),
                                construction: nervix_nspl::parse_route_construction(
                                    r#"WHERE input.missing = "error""#,
                                )
                                .expect("route construction must parse"),
                                flush_policy: None,
                                message_error_policy: MessageErrorPolicy::Log,
                                branch: None,
                            },
                            ProcessorOutput::new(named("info")),
                        ]))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                        OutputBranch::BranchedBy {
                            branch: branch_name_for_relay("notifications"),
                            assignments: Vec::new(),
                        },
                    ),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ],
        )
        .expect_err("reingestor output predicate on missing field should fail");

    assert!(matches!(
        err.current_context(),
        RegistryError::InvalidModel { .. }
    ));
    assert!(
        format!("{err}").contains("unknown input field 'missing'"),
        "unexpected error: {err}"
    );

    let _ = fs::remove_dir_all(path);
}
