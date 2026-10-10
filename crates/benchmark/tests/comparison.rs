use std::{fs, path::Path};

use nervix_benchmark::{
    BenchmarkComparison, BenchmarkRunFailure, BenchmarkSuiteReport, ComparisonError,
    ImageIdentityError, LoadReportError, MetricsReportError,
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
    end_to_end_rate: f64,
    payload_rate: f64,
    completion_seconds: f64,
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
subject_nodes = 1
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
completion_seconds={:.6}
end_to_end_seconds={:.6}
parity_stability_seconds=0.500000
wire_bytes_per_message=164
partitions=16
warmup_messages=16
max_backlog_messages=4096
peak_backlog_messages={}
input_messages={}
expected_output_records={}
output_messages={}
output_records={}
output_validation="ids-and-values"
output_records_at_generation_end={}
backlog_messages_at_generation_end=0
output_records_at_flush={}
backlog_messages_at_flush=0
input_messages_per_second={:.3}
end_to_end_messages_per_second={:.3}
input_payload_mib_per_second={:.3}
end_to_end_payload_mib_per_second={:.3}
"#,
            fixture.completion_seconds,
            30.0 + fixture.completion_seconds,
            fixture.peak_backlog,
            fixture.input_messages,
            fixture.expected_output_records,
            fixture.output_records,
            fixture.output_records,
            fixture.output_records,
            fixture.output_records,
            fixture.end_to_end_rate,
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
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
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
            end_to_end_rate: 1_000.0,
            payload_rate: 0.13,
            completion_seconds: 0.1,
            peak_backlog: 512,
        },
    );

    let comparison = BenchmarkComparison::from_run_directories(&[vector, nervix])
        .expect("matching run artifacts should compare");
    let markdown = comparison.render_markdown();

    assert!(markdown.starts_with("## Benchmark comparison\n"));
    assert!(markdown.contains(
        "**Configuration:** 30 s + 10 s warm-up · 16 partitions · 128 B values (164 B wire) · \
         backlog cap 4,096"
    ));
    assert!(markdown.contains(
        "| Nervix | **1,200 msg/s** | **0.16 MiB/s** | 4.500 s | ✅ IDs and values: 36,000 in / \
         13,500 rec | ⚠️ 4,096 / 0 (100.0% peak) | baseline |"
    ));
    assert!(markdown.contains(
        "| Vector | 1,000 msg/s | 0.13 MiB/s | 0.100 s | ✅ IDs and values: 30,000 in / 11,250 \
         rec | 512 / 0 (12.5% peak) | −16.7% |"
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
fn does_not_rank_one_container_references_against_a_two_node_run() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 36_000,
            output_records: 36_000,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
            peak_backlog: 512,
        },
    );
    let vector = write_run(
        artifacts.path(),
        Fixture {
            implementation: "vector",
            image: "vector:test",
            input_messages: 30_000,
            expected_output_records: 30_000,
            output_records: 30_000,
            end_to_end_rate: 1_000.0,
            payload_rate: 0.13,
            completion_seconds: 0.1,
            peak_backlog: 512,
        },
    );
    let manifest_path = nervix.join("run.toml");
    let manifest = fs::read_to_string(&manifest_path).expect("fixture manifest should be readable");
    write(
        &manifest_path,
        &manifest.replace("subject_nodes = 1", "subject_nodes = 2"),
    );

    let markdown = BenchmarkComparison::from_run_directories(&[nervix, vector])
        .expect("both valid runs should be reportable")
        .render_markdown();
    assert!(markdown.contains("reference only"));
    assert!(
        markdown.contains("Implementations with different node counts are shown as references")
    );
    assert!(!markdown.contains("**1,200 msg/s**"));
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
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
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
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
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
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
            peak_backlog: 4_096,
        },
    );
    fs::remove_file(nervix.join("nervix-metrics.toml"))
        .expect("metrics fixture should be removable");

    let error = BenchmarkComparison::from_run_directories(&[nervix])
        .expect_err("a successful Nervix run must include scraped metrics");
    assert!(matches!(
        error.current_context(),
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
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
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
            end_to_end_rate: 1_000.0,
            payload_rate: 0.13,
            completion_seconds: 0.1,
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
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
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
            end_to_end_rate: 1_000.0,
            payload_rate: 0.13,
            completion_seconds: 0.1,
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
        error.current_context(),
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
            end_to_end_rate: 0.0,
            payload_rate: 0.0,
            completion_seconds: 0.1,
            peak_backlog: 0,
        },
    );

    let error = BenchmarkComparison::from_run_directories(&[nervix])
        .expect_err("a run without measured messages must not compare");
    assert!(matches!(
        error.current_context(),
        ComparisonError::InvalidReport { .. }
    ));
    assert!(error.contains::<LoadReportError>());
}

#[test]
fn rejects_a_completion_tail_that_excludes_the_producer_flush() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 0.001,
            peak_backlog: 512,
        },
    );

    let error = BenchmarkComparison::from_run_directories(std::slice::from_ref(&nervix))
        .expect_err("completion must include the measured producer flush");
    assert!(matches!(
        error.current_context(),
        ComparisonError::InvalidReport { path } if *path == nervix
    ));
    assert!(matches!(
        error.downcast_ref::<LoadReportError>(),
        Some(LoadReportError::CompletionShorterThanFlush)
    ));
    assert_eq!(
        format!("{error:#}"),
        format!(
            "benchmark run {} has an invalid load report: completion tail is shorter than \
             producer flush",
            nervix.display()
        )
    );
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
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
            peak_backlog: 4_096,
        },
    );

    let error = BenchmarkComparison::from_run_directories(&[nervix])
        .expect_err("a run short of its expected output records must not compare");
    assert!(matches!(
        error.current_context(),
        ComparisonError::InvalidReport { .. }
    ));
    assert!(matches!(
        error.downcast_ref::<LoadReportError>(),
        Some(LoadReportError::OutputParity {
            expected: 13_500,
            measured: 13_499
        })
    ));
}

#[test]
fn names_a_run_directory_without_artifacts_and_keeps_the_read_failure() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let missing = artifacts.path().join("kafka-filter-map/nervix/never-ran");

    let error = BenchmarkComparison::from_run_directories(std::slice::from_ref(&missing))
        .expect_err("a run directory that was never written must not compare");

    let status_path = missing.join("status.txt");
    assert!(matches!(
        error.current_context(),
        ComparisonError::Read { path } if *path == status_path
    ));
    let cause = error
        .downcast_ref::<std::io::Error>()
        .expect("the read failure should stay beneath the artifact context");
    assert_eq!(cause.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(
        format!("{error:#}"),
        format!(
            "failed to read benchmark artifact {}: {cause}",
            status_path.display()
        )
    );
}

#[test]
fn names_the_line_of_an_image_identity_that_is_not_a_pair() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
            peak_backlog: 512,
        },
    );
    let image_path = nervix.join("image.txt");
    write(&image_path, "image=nervix:test\ndigest\n");

    let error = BenchmarkComparison::from_run_directories(&[nervix])
        .expect_err("an image identity line without '=' must not compare");

    assert!(matches!(
        error.current_context(),
        ComparisonError::InvalidImageIdentity { path } if *path == image_path
    ));
    assert!(matches!(
        error.downcast_ref::<ImageIdentityError>(),
        Some(ImageIdentityError::NotAPair { line }) if line == "digest"
    ));
    assert_eq!(
        format!("{error:#}"),
        format!(
            "benchmark run {} has an invalid image identity: line 'digest' is not a name=value \
             pair",
            image_path.display()
        )
    );
}

fn valid_vector_run(root: &Path) -> std::path::PathBuf {
    write_run(
        root,
        Fixture {
            implementation: "vector",
            image: "timberio/vector:0.57.0-debian",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
            peak_backlog: 512,
        },
    )
}

/// Rewrites the `key=value` line of a run's load report with `value`.
fn set_report_value(run: &Path, key: &str, value: &str) {
    let path = run.join("load-report.txt");
    let report = fs::read_to_string(&path).expect("the fixture load report should exist");
    let prefix = format!("{key}=");
    let mut rewritten = String::new();
    let mut replaced = false;
    for line in report.lines() {
        if line.starts_with(&prefix) {
            rewritten.push_str(&format!("{key}={value}\n"));
            replaced = true;
        } else {
            rewritten.push_str(line);
            rewritten.push('\n');
        }
    }
    assert!(replaced, "the fixture load report has no '{key}'");
    write(&path, &rewritten);
}

#[test]
fn names_every_load_report_property_a_run_violates() {
    let cases = [
        (
            "producer_flush_seconds",
            "-1.0",
            "producer_flush_seconds must be finite and non-negative",
        ),
        (
            "generation_seconds",
            "0.0",
            "generation and end-to-end durations must be positive",
        ),
        (
            "end_to_end_seconds",
            "30.0",
            "generation plus completion exceeds end-to-end duration",
        ),
        (
            "target_duration_seconds",
            "31.0",
            "target duration does not match run.toml",
        ),
        (
            "warmup_target_seconds",
            "11.0",
            "warm-up target does not match run.toml",
        ),
        (
            "warmup_generation_seconds",
            "9.0",
            "warm-up generation ended before its target duration",
        ),
        (
            "warmup_messages",
            "0",
            "a successful benchmark must warm up with at least one message",
        ),
        ("partitions", "8", "partition count does not match run.toml"),
        (
            "max_backlog_messages",
            "2048",
            "backlog cap does not match run.toml",
        ),
        (
            "peak_backlog_messages",
            "5000",
            "peak backlog exceeds its configured cap",
        ),
        (
            "backlog_messages_at_generation_end",
            "600",
            "backlog after generation exceeds observed peak",
        ),
        (
            "backlog_messages_at_flush",
            "1",
            "backlog increased after producer flush",
        ),
        (
            "end_to_end_messages_per_second",
            "0.0",
            "end-to-end message rate must be positive",
        ),
        (
            "expected_output_records",
            "0",
            "a successful benchmark must expect at least one output record",
        ),
        (
            "wire_bytes_per_message",
            "0",
            "wire message size must be positive",
        ),
    ];

    for (key, value, expected) in cases {
        let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
        let run = valid_vector_run(artifacts.path());
        set_report_value(&run, key, value);

        let error = BenchmarkComparison::from_run_directories(std::slice::from_ref(&run))
            .expect_err("the violated load report must not compare");

        assert!(
            matches!(
                error.current_context(),
                ComparisonError::InvalidReport { path } if *path == run
            ),
            "{key}: {error:?}"
        );
        let violation = error
            .downcast_ref::<LoadReportError>()
            .expect("the violated property should be beneath the run");
        assert_eq!(violation.to_string(), expected, "{key}");
    }
}

#[test]
fn refuses_a_run_manifest_without_subject_nodes() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let run = valid_vector_run(artifacts.path());
    let manifest_path = run.join("run.toml");
    let manifest = fs::read_to_string(&manifest_path).expect("fixture manifest should exist");
    write(
        &manifest_path,
        &manifest.replace("subject_nodes = 1", "subject_nodes = 0"),
    );

    let error = BenchmarkComparison::from_run_directories(&[run])
        .expect_err("a run without subject nodes must not compare");

    assert!(matches!(
        error.downcast_ref::<LoadReportError>(),
        Some(LoadReportError::NoSubjectNodes)
    ));
}

#[test]
fn refuses_two_runs_of_one_implementation_for_one_benchmark() {
    let first = tempfile::tempdir().expect("temporary artifacts should be created");
    let second = tempfile::tempdir().expect("temporary artifacts should be created");
    let runs = [
        valid_vector_run(first.path()),
        valid_vector_run(second.path()),
    ];

    let error = BenchmarkComparison::from_run_directories(&runs)
        .expect_err("duplicate implementations must not compare");

    assert!(matches!(
        error.current_context(),
        ComparisonError::DuplicateImplementation { benchmark, implementation }
            if benchmark == "kafka-filter-map" && implementation == "vector"
    ));
}

#[test]
fn refuses_to_compare_or_report_nothing() {
    let error = BenchmarkComparison::from_run_directories(&[])
        .expect_err("no run directories leave nothing to compare");
    assert!(matches!(error.current_context(), ComparisonError::Empty));

    let error = BenchmarkSuiteReport::from_run_directories(&[], Vec::new())
        .expect_err("no runs and no failures leave nothing to report");
    assert!(matches!(error.current_context(), ComparisonError::Empty));
}

#[test]
fn writes_comparisons_and_names_the_path_it_cannot_write() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let run = valid_vector_run(artifacts.path());
    let comparison = BenchmarkComparison::from_run_directories(std::slice::from_ref(&run))
        .expect("a valid run should compare");
    let suite = BenchmarkSuiteReport::from_run_directories(
        &[run],
        vec![BenchmarkRunFailure::new(
            "kafka-dedup-window",
            "vector",
            "subject exited before parity",
        )],
    )
    .expect("a run and a failure should report");

    let written = artifacts.path().join("benchmark-comparison.md");
    comparison
        .write_markdown(&written)
        .expect("the comparison should be written");
    assert_eq!(
        fs::read_to_string(&written).expect("the written comparison should be readable"),
        comparison.render_markdown()
    );
    suite
        .write_markdown(&written)
        .expect("the suite report should be written");
    assert_eq!(
        fs::read_to_string(&written).expect("the written report should be readable"),
        suite.render_markdown()
    );

    let unwritable = artifacts
        .path()
        .join("absent-directory/benchmark-comparison.md");
    for error in [
        comparison
            .write_markdown(&unwritable)
            .expect_err("a comparison cannot be written below a missing directory"),
        suite
            .write_markdown(&unwritable)
            .expect_err("a suite report cannot be written below a missing directory"),
    ] {
        assert!(matches!(
            error.current_context(),
            ComparisonError::Write { path } if *path == unwritable
        ));
        assert!(error.contains::<std::io::Error>());
    }
}

#[test]
fn names_the_image_identity_fields_a_run_lacks_or_repeats() {
    for (identity, expected) in [
        (
            "image=nervix:test\nid=sha256:a\nid=sha256:b\n",
            "unexpected or duplicate field 'id'",
        ),
        ("image=nervix:test\n", "both image and id are required"),
    ] {
        let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
        let run = valid_vector_run(artifacts.path());
        write(&run.join("image.txt"), identity);

        let error = BenchmarkComparison::from_run_directories(&[run])
            .expect_err("an invalid image identity must not compare");

        let issue = error
            .downcast_ref::<ImageIdentityError>()
            .expect("the identity issue should be beneath the run");
        assert_eq!(issue.to_string(), expected);
    }
}

#[test]
fn keeps_the_metrics_report_failure_beneath_a_nervix_run() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let nervix = write_run(
        artifacts.path(),
        Fixture {
            implementation: "nervix",
            image: "nervix:test",
            input_messages: 36_000,
            expected_output_records: 13_500,
            output_records: 13_500,
            end_to_end_rate: 1_200.0,
            payload_rate: 0.16,
            completion_seconds: 4.5,
            peak_backlog: 512,
        },
    );
    let metrics_path = nervix.join("nervix-metrics.toml");
    write(&metrics_path, "batch_targets = 7\n");

    let error = BenchmarkComparison::from_run_directories(&[nervix])
        .expect_err("an unreadable metrics report must not compare");

    assert!(matches!(
        error.current_context(),
        ComparisonError::MetricsReport { path } if *path == metrics_path
    ));
    assert!(matches!(
        error.downcast_ref::<MetricsReportError>(),
        Some(MetricsReportError::Parse { .. })
    ));
}

#[test]
fn names_a_run_manifest_that_is_not_toml() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let run = valid_vector_run(artifacts.path());
    let manifest_path = run.join("run.toml");
    write(&manifest_path, "benchmark = [\n");

    let error = BenchmarkComparison::from_run_directories(&[run])
        .expect_err("a run manifest that is not TOML must not compare");

    assert!(matches!(
        error.current_context(),
        ComparisonError::ParseManifest { path } if *path == manifest_path
    ));
    assert!(error.contains::<toml::de::Error>());
}
