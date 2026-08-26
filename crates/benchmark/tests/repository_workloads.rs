use std::{collections::BTreeMap, path::Path};

use nervix_benchmark::{BenchmarkCatalog, KafkaRenderInputs, RunSettings};
use nervix_nspl::client_statement::parse_client_statement_sources;

#[test]
fn kafka_filter_map_implementations_render_from_one_workload() {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("benchmark crate should be under the repository crates directory");
    let benchmark = BenchmarkCatalog::from_repository_root(repository_root)
        .load("kafka-filter-map")
        .expect("repository benchmark should load");
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
        lane_count: 16,
        dependency_endpoints: &dependency_endpoints,
    };

    let nervix = benchmark
        .render_implementation_with_parameters("nervix", inputs, &settings.parameters)
        .expect("Nervix implementation should render");
    let statements = parse_client_statement_sources(&nervix)
        .unwrap_or_else(|error| panic!("rendered Nervix graph should parse: {error:?}"));
    assert_eq!(
        statements
            .iter()
            .filter(|statement| statement.source(&nervix).starts_with("CREATE JUNCTION"))
            .count(),
        16
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
