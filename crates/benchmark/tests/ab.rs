use std::{
    fs,
    path::{Path, PathBuf},
};

use nervix_benchmark::{AbArm, AbError, AbSummary, ComparisonError};

const BASELINE_LABEL: &str = "main @ 0123abc";
const CANDIDATE_LABEL: &str = "working tree";

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("fixture parent should be created");
    }
    fs::write(path, contents).expect("fixture should be written");
}

struct Fixture<'a> {
    arm: &'a str,
    run: usize,
    end_to_end_rate: f64,
    peak_backlog: u64,
}

fn write_run(root: &Path, fixture: Fixture<'_>) -> PathBuf {
    let directory = root
        .join(fixture.arm)
        .join("kafka-filter-map")
        .join("nervix")
        .join(format!("run-{}-{}", fixture.arm, fixture.run));
    write(
        &directory.join("run.toml"),
        r#"benchmark = "kafka-filter-map"
consumer_group = "benchmark-consumer"
description = "Kafka JSON ingestion, contains filter, uppercase map, and Kafka emission"
duration_seconds = 30
git_dirty = false
git_revision = "0123456789abcdef"
implementation = "nervix"
input_topic = "benchmark-input"
max_backlog_messages = 4096
output_topic = "benchmark-output"
partitions = 16
subject = "nervix-local"
value_bytes = 128
wait_timeout_seconds = 120

[parameters]
emitter_flush_each = "10ms"
emitter_max_batch_size = "8MiB"
"#,
    );
    write(
        &directory.join("load-report.txt"),
        &format!(
            r#"target_duration_seconds=30.000000
generation_seconds=30.000000
producer_flush_seconds=0.100000
drain_seconds=1.500000
end_to_end_seconds=30.500000
parity_stability_seconds=0.500000
wire_bytes_per_message=140
partitions=16
warmup_messages=16
max_backlog_messages=4096
peak_backlog_messages={}
input_messages=36000
output_messages=36000
output_messages_at_generation_end=36000
backlog_messages_at_generation_end=0
output_messages_at_flush=36000
backlog_messages_at_flush=0
input_messages_per_second={:.3}
output_messages_per_second_during_generation={:.3}
end_to_end_messages_per_second={:.3}
input_payload_mib_per_second=0.160
end_to_end_payload_mib_per_second=0.160
"#,
            fixture.peak_backlog,
            fixture.end_to_end_rate,
            fixture.end_to_end_rate,
            fixture.end_to_end_rate,
        ),
    );
    write(&directory.join("status.txt"), "pass\n");
    directory
}

fn arm(root: &Path, name: &str, label: &str, rates_and_peaks: &[(f64, u64)]) -> AbArm {
    AbArm {
        label: label.to_string(),
        server_binary: root.join(name).join("nervix-server"),
        run_directories: rates_and_peaks
            .iter()
            .enumerate()
            .map(|(run, (end_to_end_rate, peak_backlog))| {
                write_run(
                    root,
                    Fixture {
                        arm: name,
                        run,
                        end_to_end_rate: *end_to_end_rate,
                        peak_backlog: *peak_backlog,
                    },
                )
            })
            .collect(),
    }
}

#[test]
fn summarizes_per_arm_statistics_and_the_mean_delta() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let baseline = arm(
        artifacts.path(),
        "baseline",
        BASELINE_LABEL,
        &[(1_000.0, 512), (1_100.0, 512), (1_050.0, 512)],
    );
    let candidate = arm(
        artifacts.path(),
        "candidate",
        CANDIDATE_LABEL,
        &[(1_150.0, 512), (1_250.0, 512), (1_200.0, 512)],
    );

    let summary = AbSummary::from_arms(baseline, candidate).expect("matching arms should compare");
    let markdown = summary.render_markdown();

    assert!(markdown.starts_with("## A/B benchmark comparison — Kafka Filter Map\n"));
    assert!(markdown.contains(
        "**Configuration:** 30 s · 16 partitions · 128 B values (140 B wire) · backlog cap 4,096"
    ));
    assert!(
        markdown.contains("| main @ 0123abc | 3 | 1,050 msg/s | 1,000 msg/s | 1,100 msg/s | 0/3 |")
    );
    assert!(
        markdown
            .contains("| working tree | 3 | **1,200 msg/s** | 1,150 msg/s | 1,250 msg/s | 0/3 |")
    );
    assert!(markdown.contains("**working tree vs main @ 0123abc (mean end-to-end): +14.3%**"));
    assert!(markdown.contains("run-baseline-0"));
    assert!(markdown.contains("run-candidate-2"));
    assert!(markdown.contains("512 (12.5%)"));
    assert!(!markdown.contains("[!WARNING]"));
    assert_eq!(markdown, summary.render_markdown());

    let summary_path = artifacts.path().join("ab-comparison.md");
    summary
        .write_markdown(&summary_path)
        .expect("summary should be written");
    assert_eq!(
        fs::read_to_string(&summary_path).expect("summary should be readable"),
        markdown
    );
}

#[test]
fn flags_backlog_cap_saturation_per_arm() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let baseline = arm(
        artifacts.path(),
        "baseline",
        BASELINE_LABEL,
        &[(1_000.0, 512), (1_100.0, 512), (1_050.0, 512)],
    );
    let candidate = arm(
        artifacts.path(),
        "candidate",
        CANDIDATE_LABEL,
        &[(1_150.0, 512), (1_250.0, 4_096), (1_200.0, 512)],
    );

    let summary = AbSummary::from_arms(baseline, candidate).expect("matching arms should compare");
    let markdown = summary.render_markdown();

    assert!(
        markdown.contains(
            "| working tree | 3 | **1,200 msg/s** | 1,150 msg/s | 1,250 msg/s | ⚠️ 1/3 |"
        )
    );
    assert!(markdown.contains("⚠️ 4,096 (100.0%)"));
    assert!(markdown.contains("[!WARNING]"));
    assert!(markdown.contains("working tree reached the configured backlog cap in 1 of 3 runs"));
    assert!(!markdown.contains("main @ 0123abc reached the configured backlog cap"));
}

#[test]
fn rejects_arms_with_mismatched_workload_configuration() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let baseline = arm(
        artifacts.path(),
        "baseline",
        BASELINE_LABEL,
        &[(1_000.0, 512)],
    );
    let candidate = arm(
        artifacts.path(),
        "candidate",
        CANDIDATE_LABEL,
        &[(1_150.0, 512)],
    );
    for (file, from, to) in [
        ("run.toml", "partitions = 16", "partitions = 8"),
        ("load-report.txt", "partitions=16", "partitions=8"),
    ] {
        let path = candidate.run_directories[0].join(file);
        let changed = fs::read_to_string(&path)
            .expect("fixture should exist")
            .replace(from, to);
        write(&path, &changed);
    }

    let error = AbSummary::from_arms(baseline, candidate)
        .expect_err("different partition counts must not compare");
    assert!(matches!(
        &error,
        AbError::MismatchedConfiguration { arm, source, .. }
            if arm == CANDIDATE_LABEL
                && matches!(
                    **source,
                    ComparisonError::MismatchedConfiguration {
                        field: "partitions",
                        ..
                    }
                )
    ));
}

#[test]
fn rejects_an_empty_arm() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let baseline = arm(
        artifacts.path(),
        "baseline",
        BASELINE_LABEL,
        &[(1_000.0, 512)],
    );
    let candidate = AbArm {
        label: CANDIDATE_LABEL.to_string(),
        server_binary: artifacts.path().join("candidate/nervix-server"),
        run_directories: Vec::new(),
    };

    let error =
        AbSummary::from_arms(baseline, candidate).expect_err("an empty arm must not compare");
    assert!(matches!(
        &error,
        AbError::EmptyArm { arm } if arm == CANDIDATE_LABEL
    ));
}

#[test]
fn rejects_a_failed_run_with_its_arm_named() {
    let artifacts = tempfile::tempdir().expect("temporary artifacts should be created");
    let baseline = arm(
        artifacts.path(),
        "baseline",
        BASELINE_LABEL,
        &[(1_000.0, 512)],
    );
    let candidate = arm(
        artifacts.path(),
        "candidate",
        CANDIDATE_LABEL,
        &[(1_150.0, 512)],
    );
    write(&candidate.run_directories[0].join("status.txt"), "fail\n");

    let error = AbSummary::from_arms(baseline, candidate)
        .expect_err("a failed run must not contribute to a summary");
    assert!(matches!(
        &error,
        AbError::Artifact { arm, source, .. }
            if arm == CANDIDATE_LABEL
                && matches!(**source, ComparisonError::UnsuccessfulRun { .. })
    ));
}
