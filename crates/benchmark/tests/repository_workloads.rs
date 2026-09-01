use std::{collections::BTreeMap, path::Path};

use nervix_benchmark::{
    BenchmarkCatalog, KafkaRenderInputs, LoadShape, LoadedBenchmark, RunSettings,
};
use nervix_nspl::client_statement::parse_client_statement_sources;

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
    parse_client_statement_sources(source)
        .unwrap_or_else(|error| panic!("rendered Nervix graph should parse: {error:?}"))
        .iter()
        .filter(|statement| statement.source(source).starts_with(prefix))
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
        LANES as usize
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
    assert_eq!(shape.expected_output_records(1_000), 576_000);

    let nervix = benchmark
        .render_implementation_with_parameters("nervix", inputs, &settings.parameters)
        .expect("Nervix implementation should render");
    assert_eq!(
        statements_starting_with(&nervix, "CREATE DEDUPLICATOR"),
        LANES as usize
    );
    assert_eq!(
        statements_starting_with(&nervix, "CREATE WINDOW PROCESSOR"),
        LANES as usize
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
        2 * LANES as usize
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
