use std::{collections::BTreeMap, path::Path};

use arch_into::ArchInto as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_benchmark::{
    BenchmarkCatalog, Implementation, KafkaRenderInputs, LoadShape, LoadedBenchmark, RunSettings,
};
use nervix_client_core::split_query_statements;

const LANES: u32 = 16;

fn load(slug: &str) -> LoadedBenchmark {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("benchmark crate should be under the repository crates directory");
    BenchmarkCatalog::from_repository_root(repository_root)
        .load(slug)
        .expect("repository benchmark should load")
}

fn statements_starting_with(source: &str, prefix: &str) -> usize {
    split_query_statements(source)
        .unwrap_or_else(|error| panic!("rendered Nervix graph should parse: {error}"))
        .iter()
        .filter(|statement| statement.starts_with(prefix))
        .count()
}

#[test]
fn kafka_filter_map_implementations_render_from_one_workload() {
    let benchmark = load("kafka-filter-map");
    let settings = RunSettings::resolve(benchmark.definition(), &[], Some(1))
        .expect("benchmark settings should resolve");
    let dependency_endpoints = BTreeMap::from([(
        "kafka_docker_addr".to_string(),
        "kafka-benchmark:9093".to_string(),
    )]);
    let inputs = KafkaRenderInputs {
        kafka_bootstrap_servers: "kafka-benchmark:9093",
        input_topic: "benchmark_input",
        output_topic: "benchmark_output",
        consumer_group: "benchmark_consumer",
        lane_count: LANES,
        dependency_endpoints: &dependency_endpoints,
    };
    assert_eq!(
        benchmark.definition().load.shape,
        LoadShape::UniformPassthrough
    );

    let nervix = benchmark
        .render_implementation_with_parameters("nervix", inputs, &settings.parameters)
        .expect("Nervix implementation should render");
    assert_eq!(
        statements_starting_with(&nervix, "CREATE JUNCTION"),
        LANES.arch_into()
    );

    let vector = benchmark
        .render_implementation_with_parameters("vector", inputs, &settings.parameters)
        .expect("Vector implementation should render");
    assert!(vector.contains("bootstrap_servers: \"kafka-benchmark:9093\""));
    assert!(vector.contains("max_bytes: 8388608"));
    assert!(vector.contains("timeout_secs: 0.01"));
    assert!(!vector.contains("{%"));
    assert!(!vector.contains("{{"));
}

#[test]
fn kafka_dedup_window_renders_a_stateful_graph_and_a_matching_competitor() {
    let benchmark = load("kafka-dedup-window");
    let settings = RunSettings::resolve(benchmark.definition(), &[], Some(1))
        .expect("benchmark settings should resolve");
    let dependency_endpoints = BTreeMap::from([(
        "kafka_docker_addr".to_string(),
        "kafka-benchmark:9093".to_string(),
    )]);
    let inputs = KafkaRenderInputs {
        kafka_bootstrap_servers: "kafka-benchmark:9093",
        input_topic: "benchmark_input",
        output_topic: "benchmark_output",
        consumer_group: "benchmark_consumer",
        lane_count: LANES,
        dependency_endpoints: &dependency_endpoints,
    };

    // Three of every eight input messages survive the filter and the deduplicator, and the driver
    // holds the harness to exactly that count.
    let shape = &benchmark.definition().load.shape;
    assert_eq!(shape.messages_per_cycle(), 1_536);
    assert_eq!(shape.output_records_per_cycle(), 576);
    assert_eq!(shape.expected_output_records(1_000), Some(576_000));

    let nervix = benchmark
        .render_implementation_with_parameters("nervix", inputs, &settings.parameters)
        .expect("Nervix implementation should render");
    assert_eq!(
        statements_starting_with(&nervix, "CREATE DEDUPLICATOR"),
        LANES.arch_into()
    );
    assert_eq!(
        statements_starting_with(&nervix, "CREATE WINDOW PROCESSOR"),
        LANES.arch_into()
    );
    assert!(nervix.contains("FILTER WHERE contains(input.value, \"x\")"));
    assert!(nervix.contains("MAX TIME 10s"));
    assert!(nervix.contains("WIDTH 500 MESSAGES 1s DURATION"));
    // The high-volume routes carry the byte cap that actually binds; the post-window emitter
    // cannot, because it sees one summary per closed window.
    assert_eq!(
        nervix
            .matches("FLUSH EACH 50ms MAX BATCH SIZE 64KiB")
            .count(),
        2 * LANES.arch_into()
    );

    let sketch_settings = RunSettings::resolve(
        benchmark.definition(),
        &[
            "sketch_enabled=true".to_owned(),
            "sketch_precision=10".to_owned(),
        ],
        Some(1),
    )
    .assured("the optional sketch uses a declared boolean and bounded precision");
    let sketch_graph = benchmark
        .render_implementation_with_parameters("nervix", inputs, &sketch_settings.parameters)
        .assured("the sketch-enabled window graph renders");
    assert_eq!(
        sketch_graph
            .matches("distinct_estimate = APPROX_COUNT_DISTINCT(input.key, 10)")
            .count(),
        LANES.arch_into()
    );
    assert_eq!(
        statements_starting_with(&sketch_graph, "CREATE WINDOW PROCESSOR"),
        LANES.arch_into()
    );

    let vector = benchmark
        .render_implementation_with_parameters("vector", inputs, &settings.parameters)
        .expect("Vector implementation should render");
    assert!(vector.contains("bootstrap_servers: \"kafka-benchmark:9093\""));
    assert!(vector.contains("type: dedupe"));
    assert!(vector.contains("end_every_period_ms: 1000"));
    assert!(vector.contains("record_count: sum"));
    assert!(!vector.contains("{%"));
    assert!(!vector.contains("{{"));
}

#[test]
fn hot_path_workloads_render_the_publisher_matrix_and_remote_placement() {
    let dependency_endpoints = BTreeMap::new();
    let inputs = KafkaRenderInputs {
        kafka_bootstrap_servers: "kafka-benchmark:9093",
        input_topic: "benchmark_input",
        output_topic: "benchmark_output",
        consumer_group: "benchmark_consumer",
        lane_count: LANES,
        dependency_endpoints: &dependency_endpoints,
    };

    for slug in [
        "hot-path-ingest",
        "hot-path-relay-fanout",
        "hot-path-remote-delivery",
        "hot-path-processor",
    ] {
        let benchmark = load(slug);
        let settings = RunSettings::resolve(benchmark.definition(), &[], Some(1))
            .assured("the repository hot-path benchmark has validated settings");
        let graph = benchmark
            .render_implementation_with_parameters("nervix", inputs, &settings.parameters)
            .assured("the repository hot-path Nervix template is valid");
        assert_eq!(
            statements_starting_with(&graph, "CREATE INGESTOR"),
            LANES.arch_into(),
            "{slug} must create one publisher per requested lane"
        );
    }

    let fanout = load("hot-path-relay-fanout");
    assert_eq!(
        fanout.definition().load.shape,
        LoadShape::UniformFanout {
            outputs_per_input: 4,
        }
    );
    let settings = RunSettings::resolve(fanout.definition(), &[], Some(1))
        .assured("the repository fan-out benchmark has validated settings");
    let fanout_graph = fanout
        .render_implementation_with_parameters("nervix", inputs, &settings.parameters)
        .assured("the repository fan-out template is valid");
    assert_eq!(
        statements_starting_with(&fanout_graph, "CREATE JUNCTION"),
        4
    );

    let remote = load("hot-path-remote-delivery");
    let remote_implementation = match &remote.definition().implementations["nervix"] {
        Implementation::Nervix(implementation) => Some(implementation),
        Implementation::Container(_) => None,
    }
    .verified("the repository remote-delivery workload declares a Nervix implementation");
    assert_eq!(remote_implementation.nodes, 2);
    let settings = RunSettings::resolve(remote.definition(), &[], Some(1))
        .assured("the repository remote-delivery benchmark has validated settings");
    let placement = remote
        .render_after_start_with_parameters("nervix", inputs, &settings.parameters)
        .assured("the repository remote-delivery placement template is valid")
        .verified("the repository remote-delivery workload declares post-start placement");
    assert_eq!(
        statements_starting_with(&placement, "RELOCATE INGESTOR"),
        LANES.arch_into()
    );
    assert_eq!(statements_starting_with(&placement, "RELOCATE RELAY"), 1);
    assert_eq!(statements_starting_with(&placement, "RELOCATE EMITTER"), 1);

    let processor = load("hot-path-processor");
    let expression = "CASE WHEN contains_any(input.value, vec(input.value)) THEN \
                      upper(input.value) ELSE input.value END";
    let settings = RunSettings::resolve(
        processor.definition(),
        &[format!("transform_expression={expression}")],
        Some(1),
    )
    .assured("the processor declares a STRING expression parameter");
    let graph = processor
        .render_implementation_with_parameters("nervix", inputs, &settings.parameters)
        .assured("the configured processor expression renders");
    assert_eq!(
        graph.matches(&format!("SET value = {expression}")).count(),
        LANES.arch_into()
    );
}
