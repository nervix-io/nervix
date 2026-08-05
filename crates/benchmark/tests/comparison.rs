use std::{fs, path::Path};

use nervix_benchmark::{BenchmarkComparison, ComparisonError};

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
output_messages={}
output_messages_at_generation_end={}
backlog_messages_at_generation_end=0
output_messages_at_flush={}
backlog_messages_at_flush=0
input_messages_per_second={:.3}
output_messages_per_second_during_generation={:.3}
end_to_end_messages_per_second={:.3}
input_payload_mib_per_second={:.3}
end_to_end_payload_mib_per_second={:.3}
"#,
            fixture.drain_seconds,
            fixture.peak_backlog,
            fixture.input_messages,
            fixture.input_messages,
            fixture.input_messages,
            fixture.input_messages,
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
        "**Configuration:** 30 s · 16 partitions · 128 B values (140 B wire) · backlog cap 4,096"
    ));
    assert!(markdown.contains(
        "| Nervix | **1,200 msg/s** | **0.16 MiB/s** | **1,250 msg/s** | 4.500 s | ✅ 36,000 | ⚠️ \
         4,096 (100.0%) | baseline |"
    ));
    assert!(markdown.contains(
        "| Vector | 1,000 msg/s | 0.13 MiB/s | 1,020 msg/s | **0.100 s** | ✅ 30,000 | 512 \
         (12.5%) | −16.7% |"
    ));
    assert!(markdown.contains("Nervix reached the configured backlog cap"));
    assert!(markdown.contains("`ghcr.io/nervix-io/nervix:pr-109`"));
    assert!(markdown.contains("`timberio/vector:0.57.0-debian`"));
    assert_eq!(markdown, comparison.render_markdown());
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
