use std::{collections::BTreeMap, fs, path::Path};

use nervix_benchmark::{
    BenchmarkCatalog, BenchmarkError, ContainerImplementation, Implementation, KafkaRenderInputs,
    LoadDuration, LoadShape,
};

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("fixture parent should be created");
    }
    fs::write(path, contents).expect("fixture should be written");
}

fn write_benchmark(repository: &Path, slug: &str, duration: &str) {
    let directory = repository.join("benches/benchmarks").join(slug);
    write(
        &directory.join("benchmark.toml"),
        &format!(
            r#"
name = "Kafka forwarding"
description = "Forwards JSON records through one implementation."
dependencies = ["kafka"]

[load]
duration = {duration}
partitions = 3
value_bytes = 128
max_backlog_messages = 4096
wait_timeout_seconds = 30
warmup_seconds = 1

[load.shape]
kind = "uniform-passthrough"

[parameters]
batch_bytes = 1048576
enabled = true

[parameters.codec]
name = "json"

[implementations.nervix]
kind = "nervix"
template = "graph.nspl.upon"

[implementations.flink]
kind = "container"
image = "flink:2"
template = "flink.yaml.upon"
config_path = "/opt/flink/conf/flink-conf.yaml"
command = ["bin/flink", "run"]
"#,
        ),
    );
    write(
        &directory.join("graph.nspl.upon"),
        concat!(
            "bootstrap={{ kafka_bootstrap_servers }}\n",
            "input={{ input_topic }}\n",
            "output={{ output_topic }}\n",
            "group={{ consumer_group }}\n",
            "docker={{ dependencies.kafka_docker_addr }}\n",
            "lanes={% for lane in lanes %}[{{ lane }}]{% endfor %}\n",
            "batch={{ parameters.batch_bytes }}\n",
            "codec={{ parameters.codec.name }}\n",
        ),
    );
    write(
        &directory.join("flink.yaml.upon"),
        "brokers: {{ kafka_bootstrap_servers }}\n",
    );
}

#[test]
fn discovers_sorted_declarative_benchmarks_and_loads_implementations() {
    let repository = tempfile::tempdir().expect("temporary repository should be created");
    write_benchmark(repository.path(), "zeta-load", "\"auto\"");
    write_benchmark(repository.path(), "alpha-load", "45");

    let catalog = BenchmarkCatalog::from_repository_root(repository.path());
    let benchmarks = catalog.discover().expect("benchmarks should load");

    assert_eq!(
        benchmarks
            .iter()
            .map(|benchmark| benchmark.slug())
            .collect::<Vec<_>>(),
        ["alpha-load", "zeta-load"]
    );
    assert_eq!(
        benchmarks[0].definition().load.duration,
        LoadDuration::Seconds(45)
    );
    assert_eq!(benchmarks[1].definition().load.duration, LoadDuration::Auto);
    assert_eq!(
        benchmarks[0].definition().parameters["batch_bytes"].as_integer(),
        Some(1_048_576)
    );

    let Implementation::Container(ContainerImplementation {
        image,
        config_path,
        command,
        ..
    }) = &benchmarks[0].definition().implementations["flink"]
    else {
        panic!("flink should load as a container implementation");
    };
    assert_eq!(image, "flink:2");
    assert_eq!(config_path, Path::new("/opt/flink/conf/flink-conf.yaml"));
    assert_eq!(
        command
            .as_deref()
            .map(|arguments| arguments.iter().map(String::as_str).collect::<Vec<_>>()),
        Some(vec!["bin/flink", "run"])
    );
}

#[test]
fn renders_upon_templates_with_kafka_lanes_and_arbitrary_parameters() {
    let repository = tempfile::tempdir().expect("temporary repository should be created");
    write_benchmark(repository.path(), "kafka-forward", "\"auto\"");
    let benchmark = BenchmarkCatalog::from_repository_root(repository.path())
        .load("kafka-forward")
        .expect("benchmark should load");
    let dependency_endpoints = BTreeMap::from([(
        "kafka_docker_addr".to_string(),
        "kafka-container:9093".to_string(),
    )]);

    let rendered = benchmark
        .render_implementation(
            "nervix",
            KafkaRenderInputs {
                kafka_bootstrap_servers: "kafka:9092",
                input_topic: "bench-input",
                output_topic: "bench-output",
                consumer_group: "bench-consumer",
                lane_count: 2,
                dependency_endpoints: &dependency_endpoints,
            },
        )
        .expect("template should render");

    assert_eq!(
        rendered,
        concat!(
            "bootstrap=kafka:9092\n",
            "input=bench-input\n",
            "output=bench-output\n",
            "group=bench-consumer\n",
            "docker=kafka-container:9093\n",
            "lanes=[0][1]\n",
            "batch=1048576\n",
            "codec=json\n",
        )
    );
}

#[test]
fn nervix_catalog_workloads_use_a_dedicated_idempotent_kafka_producer() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("benchmark crate should live below the repository root");
    let catalog = BenchmarkCatalog::from_repository_root(repository);
    let dependency_endpoints = BTreeMap::new();

    for slug in ["kafka-dedup-window", "kafka-filter-map"] {
        let benchmark = catalog.load(slug).expect("catalog benchmark should load");
        let rendered = benchmark
            .render_implementation(
                "nervix",
                KafkaRenderInputs {
                    kafka_bootstrap_servers: "kafka:9092",
                    input_topic: "bench-input",
                    output_topic: "bench-output",
                    consumer_group: "bench-consumer",
                    lane_count: 1,
                    dependency_endpoints: &dependency_endpoints,
                },
            )
            .expect("Nervix benchmark template should render");

        assert!(rendered.contains("CREATE CLIENT kafka_input"));
        assert!(rendered.contains("CREATE CLIENT kafka_output"));
        assert!(rendered.contains("'enable.idempotence' = 'true'"));
        assert!(rendered.contains("'acks' = 'all'"));
        assert!(rendered.contains("FROM KAFKA kafka_input"));
        assert!(rendered.contains("TO KAFKA kafka_output"));
    }
}

#[test]
fn catalog_workloads_declare_a_timed_warmup() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("benchmark crate should live below the repository root");
    let catalog = BenchmarkCatalog::from_repository_root(repository);

    for slug in ["kafka-dedup-window", "kafka-filter-map"] {
        let benchmark = catalog.load(slug).expect("catalog benchmark should load");
        let manifest = fs::read_to_string(benchmark.directory().join("benchmark.toml"))
            .expect("catalog manifest should be readable");
        assert!(manifest.contains("warmup_seconds = 10"));
    }
}

#[test]
fn rejects_invalid_slugs_bounds_missing_implementations_and_escaping_templates() {
    let repository = tempfile::tempdir().expect("temporary repository should be created");
    let catalog = BenchmarkCatalog::from_repository_root(repository.path());
    assert!(matches!(
        catalog.load("../escape"),
        Err(BenchmarkError::InvalidSlug { .. })
    ));

    for (slug, dependencies) in [
        ("missing-kafka", "[]"),
        ("duplicate-kafka", "[\"kafka\", \"kafka\"]"),
    ] {
        let directory = repository.path().join("benches/benchmarks").join(slug);
        write(
            &directory.join("benchmark.toml"),
            &format!(
                r#"
name = "Invalid dependencies"
description = "Dependency declarations are exact"
dependencies = {dependencies}
[load]
duration = "auto"
partitions = 1
value_bytes = 1
max_backlog_messages = 1
wait_timeout_seconds = 1
warmup_seconds = 1

[load.shape]
kind = "uniform-passthrough"
[parameters]
[implementations.nervix]
kind = "nervix"
template = "graph.nspl.upon"
"#,
            ),
        );
        write(&directory.join("graph.nspl.upon"), "BEGIN; COMMIT;");
        assert!(matches!(
            catalog.load(slug),
            Err(BenchmarkError::InvalidDefinition { .. })
        ));
    }

    let unsupported_dependency = repository
        .path()
        .join("benches/benchmarks/unsupported-dependency");
    write(
        &unsupported_dependency.join("benchmark.toml"),
        r#"
name = "Unsupported dependency"
description = "Only typed benchmark dependencies are accepted"
dependencies = ["redis"]
[load]
duration = "auto"
partitions = 1
value_bytes = 1
max_backlog_messages = 1
wait_timeout_seconds = 1
warmup_seconds = 1

[load.shape]
kind = "uniform-passthrough"
[parameters]
[implementations.nervix]
kind = "nervix"
template = "graph.nspl.upon"
"#,
    );
    write(
        &unsupported_dependency.join("graph.nspl.upon"),
        "BEGIN; COMMIT;",
    );
    assert!(matches!(
        catalog.load("unsupported-dependency"),
        Err(BenchmarkError::ParseManifest { .. })
    ));

    let invalid_directory = repository.path().join("benches/benchmarks/invalid-bounds");
    write(
        &invalid_directory.join("benchmark.toml"),
        r#"
name = "Invalid"
description = "Invalid load bounds"
dependencies = ["kafka"]
[load]
duration = "auto"
partitions = 0
value_bytes = 0
max_backlog_messages = 0
wait_timeout_seconds = 0
warmup_seconds = 0

[load.shape]
kind = "uniform-passthrough"
[parameters]
"#,
    );
    let error = catalog
        .load("invalid-bounds")
        .expect_err("invalid benchmark should fail");
    assert!(matches!(error, BenchmarkError::InvalidDefinition { .. }));

    let invalid_duration_directory = repository
        .path()
        .join("benches/benchmarks/invalid-duration");
    write(
        &invalid_duration_directory.join("benchmark.toml"),
        r#"
name = "Invalid duration"
description = "Duration must be positive"
dependencies = ["kafka"]
[load]
duration = 0
partitions = 1
value_bytes = 1
max_backlog_messages = 1
wait_timeout_seconds = 1
warmup_seconds = 1

[load.shape]
kind = "uniform-passthrough"
[parameters]
[implementations.nervix]
kind = "nervix"
template = "graph.nspl.upon"
"#,
    );
    write(
        &invalid_duration_directory.join("graph.nspl.upon"),
        "BEGIN; COMMIT;",
    );
    assert!(matches!(
        catalog.load("invalid-duration"),
        Err(BenchmarkError::ParseManifest { .. })
    ));

    let invalid_partition_directory = repository
        .path()
        .join("benches/benchmarks/invalid-partitions");
    write(
        &invalid_partition_directory.join("benchmark.toml"),
        r#"
name = "Invalid partitions"
description = "Kafka partition ranges are validated before startup"
dependencies = ["kafka"]
[load]
duration = "auto"
partitions = 2147483648
value_bytes = 1
max_backlog_messages = 1
wait_timeout_seconds = 1
warmup_seconds = 1

[load.shape]
kind = "uniform-passthrough"
[parameters]
[implementations.nervix]
kind = "nervix"
template = "graph.nspl.upon"
"#,
    );
    write(
        &invalid_partition_directory.join("graph.nspl.upon"),
        "BEGIN; COMMIT;",
    );
    assert!(matches!(
        catalog.load("invalid-partitions"),
        Err(BenchmarkError::InvalidDefinition { .. })
    ));

    for (slug, config_path, readiness_port) in [
        ("invalid-config-path", "relative.yaml", 8686),
        ("invalid-readiness-port", "/etc/vector/vector.yaml", 0),
    ] {
        let directory = repository.path().join("benches/benchmarks").join(slug);
        write(
            &directory.join("benchmark.toml"),
            &format!(
                r#"
name = "Invalid container"
description = "Container paths and ports are validated before startup"
dependencies = ["kafka"]
[load]
duration = "auto"
partitions = 1
value_bytes = 1
max_backlog_messages = 1
wait_timeout_seconds = 1
warmup_seconds = 1

[load.shape]
kind = "uniform-passthrough"
[parameters]
[implementations.vector]
kind = "container"
image = "vector:tag"
template = "vector.yaml"
config_path = "{config_path}"
readiness_port = {readiness_port}
readiness_path = "/health"
"#,
            ),
        );
        write(&directory.join("vector.yaml"), "data_dir: /tmp/vector");
        assert!(matches!(
            catalog.load(slug),
            Err(BenchmarkError::InvalidDefinition { .. })
        ));
    }

    let escaping_directory = repository
        .path()
        .join("benches/benchmarks/escaping-template");
    write(
        &escaping_directory.join("benchmark.toml"),
        r#"
name = "Escaping"
description = "Escaping template"
dependencies = ["kafka"]
[load]
duration = "auto"
partitions = 1
value_bytes = 1
max_backlog_messages = 1
wait_timeout_seconds = 1
warmup_seconds = 1

[load.shape]
kind = "uniform-passthrough"
[parameters]
[implementations.nervix]
kind = "nervix"
template = "../outside.nspl"
"#,
    );
    write(
        &escaping_directory
            .parent()
            .expect("benchmark should have a parent")
            .join("outside.nspl"),
        "BEGIN; COMMIT;",
    );
    let error = catalog
        .load("escaping-template")
        .expect_err("escaping template should fail");
    assert!(matches!(error, BenchmarkError::InvalidTemplatePath { .. }));
}

fn write_keyed_benchmark(repository: &Path, slug: &str, shape: &str) {
    let directory = repository.join("benches/benchmarks").join(slug);
    write(
        &directory.join("benchmark.toml"),
        &format!(
            r#"
name = "Keyed load"
description = "Deduplicated and windowed records"
dependencies = ["kafka"]

[load]
duration = "auto"
partitions = 2
value_bytes = 128
max_backlog_messages = 4096
wait_timeout_seconds = 30
warmup_seconds = 1

[load.shape]
kind = "keyed-windowed"
{shape}

[parameters]

[implementations.nervix]
kind = "nervix"
template = "graph.nspl.upon"
"#,
        ),
    );
    write(&directory.join("graph.nspl.upon"), "BEGIN; COMMIT;");
}

#[test]
fn loads_the_keyed_windowed_shape_and_its_output_contract() {
    let repository = tempfile::tempdir().expect("temporary repository should be created");
    write_keyed_benchmark(
        repository.path(),
        "keyed-load",
        concat!(
            "keys_per_cycle = 8\n",
            "retained_keys = 6\n",
            "copies_per_key = 2\n",
            "count_field = \"record_count\"\n",
        ),
    );

    let benchmark = BenchmarkCatalog::from_repository_root(repository.path())
        .load("keyed-load")
        .expect("keyed benchmark should load");
    let shape = &benchmark.definition().load.shape;

    assert_eq!(
        shape,
        &LoadShape::KeyedWindowed {
            keys_per_cycle: 8,
            retained_keys: 6,
            copies_per_key: 2,
            count_field: "record_count".to_string(),
        }
    );
    assert_eq!(shape.messages_per_cycle(), 16);
    assert_eq!(shape.output_records_per_cycle(), 6);
    assert_eq!(shape.expected_output_records(100), 600);
    assert_eq!(shape.input_messages_for_output_records(600), 1_600);
}

#[test]
fn rejects_keyed_shapes_that_cannot_state_an_exact_drop_rate() {
    let repository = tempfile::tempdir().expect("temporary repository should be created");
    let catalog = BenchmarkCatalog::from_repository_root(repository.path());

    for (slug, shape) in [
        (
            "zero-keys",
            concat!(
                "keys_per_cycle = 0\n",
                "retained_keys = 1\n",
                "copies_per_key = 1\n",
                "count_field = \"record_count\"\n",
            ),
        ),
        (
            "zero-copies",
            concat!(
                "keys_per_cycle = 4\n",
                "retained_keys = 1\n",
                "copies_per_key = 0\n",
                "count_field = \"record_count\"\n",
            ),
        ),
        (
            "zero-retained",
            concat!(
                "keys_per_cycle = 4\n",
                "retained_keys = 0\n",
                "copies_per_key = 1\n",
                "count_field = \"record_count\"\n",
            ),
        ),
        (
            "retains-more-than-it-produces",
            concat!(
                "keys_per_cycle = 4\n",
                "retained_keys = 5\n",
                "copies_per_key = 1\n",
                "count_field = \"record_count\"\n",
            ),
        ),
        (
            "unnamed-count-field",
            concat!(
                "keys_per_cycle = 4\n",
                "retained_keys = 3\n",
                "copies_per_key = 1\n",
                "count_field = \"Record Count\"\n",
            ),
        ),
        (
            "cycle-exceeds-the-backlog-cap",
            concat!(
                "keys_per_cycle = 4096\n",
                "retained_keys = 3072\n",
                "copies_per_key = 2\n",
                "count_field = \"record_count\"\n",
            ),
        ),
    ] {
        write_keyed_benchmark(repository.path(), slug, shape);
        assert!(
            matches!(
                catalog.load(slug),
                Err(BenchmarkError::InvalidDefinition { .. })
            ),
            "benchmark '{slug}' should be rejected"
        );
    }
}
