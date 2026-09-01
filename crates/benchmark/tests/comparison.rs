use std::{fs, path::Path};

use nervix_benchmark::{
    BenchmarkComparison, BenchmarkRunFailure, BenchmarkSuiteReport, ComparisonError,
};

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("fixture parent should be created");
    }
    fs::write(path, contents).expect("fixture should be written");
}

struct Fixture<'a> {
    implementation: &'a str,
    image: &'a str,
    input_messages: u64,
    expected_output_records: u64,
    output_records: u64,
    generation_rate: f64,
    end_to_end_rate: f64,
    payload_rate: f64,
    drain_seconds: f64,
    peak_backlog: u64,
}

fn write_run(root: &Path, fixture: Fixture<'_>) -> std::path::PathBuf {
    let directory = root
        .join("kafka-filter-map")
        .join(fixture.implementation)
        .join(format!("run-{}", fixture.implementation));
    write(
        &directory.join("run.toml"),
        &format!(
            r#"benchmark = "kafka-filter-map"
consumer_group = "benchmark-consumer"
description = "Kafka JSON ingestion, contains filter, uppercase map, and Kafka emission"
duration_seconds = 30
git_dirty = false
git_revision = "0123456789abcdef"
image = "{}"
implementation = "{}"
input_topic = "benchmark-input"
max_backlog_messages = 4096
output_topic = "benchmark-output"
partitions = 16
subject = "container"
value_bytes = 128
wait_timeout_seconds = 120
warmup_seconds = 10

[parameters]
emitter_flush_each = "10ms"
emitter_flush_seconds = 0.01
emitter_max_batch_bytes = 8388608
emitter_max_batch_size = "8MiB"
ingestor_flush_each = "10ms"
ingestor_max_batch_size = "8MiB"
"#,
            fixture.image, fixture.implementation
        ),
    );
    write(
        &directory.join("load-report.txt"),
        &format!(
            r#"target_duration_seconds=30.000000
warmup_target_seconds=10.000000
warmup_generation_seconds=10.000001
warmup_parity_stability_seconds=0.500000
generation_seconds=30.000000
producer_flush_seconds=0.100000
drain_seconds={:.6}
end_to_end_seconds=30.500000
parity_stability_seconds=0.500000
wire_bytes_per_message=140
partitions=16
warmup_messages=16
max_backlog_messages=4096
peak_backlog_messages={}
input_messages={}
expected_output_records={}
output_messages={}
output_records={}
output_records_at_generation_end={}
backlog_messages_at_generation_end=0
output_records_at_flush={}
backlog_messages_at_flush=0
input_messages_per_second={:.3}
output_records_per_second_during_generation={:.3}
end_to_end_messages_per_second={:.3}
input_payload_mib_per_second={:.3}
end_to_end_payload_mib_per_second={:.3}
"#,
            fixture.drain_seconds,
            fixture.peak_backlog,
            fixture.input_messages,
            fixture.expected_output_records,
            fixture.output_records,
            fixture.output_records,
            fixture.output_records,
            fixture.output_records,
            fixture.generation_rate,
            fixture.generation_rate,
            fixture.end_to_end_rate,
            fixture.payload_rate,
            fixture.payload_rate,
        ),
    );
    write(&directory.join("status.txt"), "pass\n");
    write(
        &directory.join("image.txt"),
        &format!(
            "image={}\nid=sha256:{}\n",
            fixture.image, fixture.implementation
        ),
    );
    if fixture.implementation == "nervix" {
        write(
            &directory.join("nervix-metrics.toml"),
            r#"[[batch_targets]]
domain = "benchmark_run"
target_kind = "INGESTOR"
target = "kafka_in_0"
physical_node_id = "node-1"
direction = "sent"
relay = "benchmark_ingested_0"
messages_total = 36000
batches_total = 36
p50 = 500.0
p90 = 1000.0
p99 = 2048.0

[[relay_buffers]]
domain = "benchmark_run"
relay = "benchmark_ingested_0"
physical_node_id = "node-1"
direction = "concrete"
observations = 100
p50 = 1.0
p90 = 8.0
p99 = 32.0
"#,
        );
    }
    directory
}

#[test]
fn renders_a_deterministic_markdown_comparison_from_exact_run_directories() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "ghcr.io/nervix-io/nervix:pr-109",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            generation_rate: 1_250.0,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            drain_seconds: 4.5,
            peak_backlog: 4_096,
        },
    );
    let vector = write_run(
        artifacts.path(),
        Fixture {
            implementation: "vector",
            image: "timberio/vector:0.57.0-debian",
            input_messages: 30_000,
            expected_output_records: 11_250,
            output_records: 11_250,
            generation_rate: 1_020.0,
            end_to_end_rate: 1_000.0,
            payload_rate: 0.13,
            drain_seconds: 0.1,
            peak_backlog: 512,
        },
    );

    let comparison = BenchmarkComparison::from_run_directories(&[vector, nervix])
        .expect("matching run artifacts should compare");
    let markdown = comparison.render_markdown();

    assert!(markdown.starts_with("## Benchmark comparison\n"));
    assert!(markdown.contains(
        "**Configuration:** 30 s + 10 s warm-up · 16 partitions · 128 B values (140 B wire) · \
         backlog cap 4,096"
    ));
    assert!(markdown.contains(
        "| Nervix | **1,200 msg/s** | **0.16 MiB/s** | **1,250 rec/s** | 4.500 s | ✅ 36,000 in / \
         13,500 rec | ⚠️ 4,096 (100.0%) | baseline |"
    ));
    assert!(markdown.contains(
        "| Vector | 1,000 msg/s | 0.13 MiB/s | 1,020 rec/s | **0.100 s** | ✅ 30,000 in / 11,250 \
         rec | 512 (12.5%) | −16.7% |"
    ));
    assert!(markdown.contains("Nervix reached the configured backlog cap"));
    assert!(markdown.contains("<summary>Nervix runtime observations</summary>"));
    assert!(markdown.contains(
        "| INGESTOR `kafka_in_0` | sent | `benchmark_ingested_0` | 1,000.00 | ≤500 | ≤1,000 | \
         ≤2,048 | 36,000 / 36 |"
    ));
    assert!(markdown.contains("| `benchmark_ingested_0` | concrete | ≤1 | ≤8 | ≤32 | 100 |"));
    assert!(markdown.contains(
        "Means use `messages_total / batches_total`; percentiles are upper bounds from the \
         scraped Prometheus histogram buckets."
    ));
    assert!(markdown.contains("`ghcr.io/nervix-io/nervix:pr-109`"));
    assert!(markdown.contains("`timberio/vector:0.57.0-debian`"));
    assert_eq!(markdown, comparison.render_markdown());
}

#[test]
fn reports_every_benchmark_group_in_one_comparison() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let filter_map = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            generation_rate: 1_250.0,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            drain_seconds: 4.5,
            peak_backlog: 4_096,
        },
    );
    let second_root = artifacts.path().join("second-benchmark");
    let dedup_window = write_run(
        &second_root,
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            generation_rate: 1_250.0,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            drain_seconds: 4.5,
            peak_backlog: 4_096,
        },
    );
    let manifest_path = dedup_window.join("run.toml");
    let manifest = fs::read_to_string(&manifest_path)
        .expect("fixture manifest should exist")
        .replace("kafka-filter-map", "kafka-dedup-window")
        .replace(
            "Kafka JSON ingestion, contains filter, uppercase map, and Kafka emission",
            "Kafka deduplication and window aggregation",
        );
    write(&manifest_path, &manifest);

    let markdown = BenchmarkComparison::from_run_directories(&[dedup_window, filter_map])
        .expect("all benchmark groups should compare")
        .render_markdown();

    assert!(markdown.contains("### Kafka Dedup Window"));
    assert!(markdown.contains("### Kafka Filter Map"));
}

#[test]
fn rejects_a_successful_nervix_run_without_observed_metrics() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            generation_rate: 1_250.0,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            drain_seconds: 4.5,
            peak_backlog: 4_096,
        },
    );
    fs::remove_file(nervix.join("nervix-metrics.toml"))
        .expect("metrics fixture should be removable");

    let error = BenchmarkComparison::from_run_directories(&[nervix])
        .expect_err("a successful Nervix run must include scraped metrics");
    assert!(matches!(
        error,
        ComparisonError::MissingMetricsReport { .. }
    ));
}

#[test]
fn suite_report_keeps_successes_and_failed_catalog_entries_together() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            generation_rate: 1_250.0,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            drain_seconds: 4.5,
            peak_backlog: 4_096,
        },
    );
    let vector = write_run(
        artifacts.path(),
        Fixture {
            implementation: "vector",
            image: "vector:test",
            input_messages: 30_000,
            expected_output_records: 11_250,
            output_records: 11_250,
            generation_rate: 1_020.0,
            end_to_end_rate: 1_000.0,
            payload_rate: 0.13,
            drain_seconds: 0.1,
            peak_backlog: 512,
        },
    );
    let report = BenchmarkSuiteReport::from_run_directories(
        &[nervix, vector],
        vec![
            BenchmarkRunFailure::new("kafka-dedup-window", "nervix", "output parity exceeded"),
            BenchmarkRunFailure::new(
                "kafka-dedup-window",
                "vector",
                "subject exited before parity",
            ),
        ],
    )
    .expect("partial benchmark results should remain reportable");
    let markdown = report.render_markdown();

    assert!(markdown.starts_with("## Benchmark comparison\n"));
    assert!(markdown.contains(
        "**Execution:** 2 of 4 catalog executions succeeded; all 4 were attempted across 2 \
         workloads."
    ));
    assert!(markdown.contains("### Kafka Filter Map"));
    assert!(markdown.contains("### Execution status"));
    assert!(markdown.contains("| Kafka Filter Map | Nervix | ✅ Passed |"));
    assert!(markdown.contains("| Kafka Filter Map | Vector | ✅ Passed |"));
    assert!(markdown.contains("| Kafka Dedup Window | Nervix | ❌ Failed |"));
    assert!(markdown.contains("| Kafka Dedup Window | Vector | ❌ Failed |"));
    assert!(markdown.contains("### Failed benchmark implementations"));
    assert!(markdown.contains("| Kafka Dedup Window | Vector | subject exited before parity |"));
}

#[test]
fn rejects_runs_with_different_workload_configuration() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            generation_rate: 1_250.0,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            drain_seconds: 4.5,
            peak_backlog: 4_096,
        },
    );
    let vector = write_run(
        artifacts.path(),
        Fixture {
            implementation: "vector",
            image: "vector:test",
            input_messages: 30_000,
            expected_output_records: 11_250,
            output_records: 11_250,
            generation_rate: 1_020.0,
            end_to_end_rate: 1_000.0,
            payload_rate: 0.13,
            drain_seconds: 0.1,
            peak_backlog: 512,
        },
    );
    let manifest = vector.join("run.toml");
    let changed = fs::read_to_string(&manifest)
        .expect("fixture manifest should exist")
        .replace("partitions = 16", "partitions = 8");
    write(&manifest, &changed);
    let report = vector.join("load-report.txt");
    let changed = fs::read_to_string(&report)
        .expect("fixture report should exist")
        .replace("partitions=16", "partitions=8");
    write(&report, &changed);

    let error = BenchmarkComparison::from_run_directories(&[nervix, vector])
        .expect_err("different partition counts must not compare");
    assert!(matches!(
        error,
        ComparisonError::MismatchedConfiguration {
            field: "partitions",
            ..
        }
    ));
}

#[test]
fn rejects_a_successful_run_without_messages() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 0,
            expected_output_records: 0,
            output_records: 0,
            generation_rate: 0.0,
            end_to_end_rate: 0.0,
            payload_rate: 0.0,
            drain_seconds: 0.0,
            peak_backlog: 0,
        },
    );

    let error = BenchmarkComparison::from_run_directories(&[nervix])
        .expect_err("a run without measured messages must not compare");
    assert!(matches!(error, ComparisonError::InvalidReport { .. }));
}

#[test]
fn rejects_a_run_that_missed_the_output_records_its_shape_expects() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_499,
            generation_rate: 1_250.0,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            drain_seconds: 4.5,
            peak_backlog: 4_096,
        },
    );

    let error = BenchmarkComparison::from_run_directories(&[nervix])
        .expect_err("a run short of its expected output records must not compare");
    assert!(matches!(error, ComparisonError::InvalidReport { .. }));
}
