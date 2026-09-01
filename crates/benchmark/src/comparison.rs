use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;

use crate::{MetricsReportError, NERVIX_METRICS_REPORT_FILE, NervixMetricsReport};

#[derive(Debug)]
pub struct BenchmarkComparison {
    benchmarks: Vec<BenchmarkRuns>,
}

#[derive(Debug)]
pub struct BenchmarkSuiteReport {
    comparison: Option<BenchmarkComparison>,
    failures: Vec<BenchmarkRunFailure>,
}

#[derive(Debug)]
pub struct BenchmarkRunFailure {
    benchmark: String,
    implementation: String,
    message: String,
}

#[derive(Debug)]
struct BenchmarkRuns {
    slug: String,
    description: String,
    runs: Vec<RunArtifact>,
}

#[derive(Debug)]
pub(crate) struct RunArtifact {
    pub(crate) directory: PathBuf,
    pub(crate) manifest: RunManifest,
    pub(crate) report: LoadReport,
    metrics: Option<NervixMetricsReport>,
    image_identity: Option<ImageIdentity>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RunManifest {
    pub(crate) benchmark: String,
    pub(crate) description: String,
    pub(crate) duration_seconds: u64,
    pub(crate) warmup_seconds: u64,
    pub(crate) git_dirty: Option<bool>,
    pub(crate) git_revision: Option<String>,
    image: Option<String>,
    pub(crate) implementation: String,
    pub(crate) max_backlog_messages: u64,
    pub(crate) partitions: u32,
    subject: String,
    pub(crate) value_bytes: u64,
    pub(crate) parameters: toml::Table,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LoadReport {
    target_duration_seconds: f64,
    warmup_target_seconds: f64,
    warmup_generation_seconds: f64,
    warmup_parity_stability_seconds: f64,
    generation_seconds: f64,
    drain_seconds: f64,
    end_to_end_seconds: f64,
    pub(crate) wire_bytes_per_message: u64,
    partitions: u32,
    warmup_messages: u64,
    pub(crate) max_backlog_messages: u64,
    pub(crate) peak_backlog_messages: u64,
    input_messages: u64,
    expected_output_records: u64,
    output_records: u64,
    output_records_per_second_during_generation: f64,
    pub(crate) end_to_end_messages_per_second: f64,
    end_to_end_payload_mib_per_second: f64,
}

impl LoadReport {
    pub(crate) fn saturated_backlog(&self) -> bool {
        self.peak_backlog_messages == self.max_backlog_messages
    }
}

#[derive(Debug)]
struct ImageIdentity {
    image: String,
    id: String,
}

#[derive(Debug, Error)]
pub enum ComparisonError {
    #[error("no benchmark run directories were provided")]
    Empty,

    #[error("failed to read benchmark artifact {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to parse benchmark run manifest {path}")]
    ParseManifest {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("failed to parse benchmark load report {path}")]
    ParseReport {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("benchmark run {path} has status '{status}', expected 'pass'")]
    UnsuccessfulRun { path: PathBuf, status: String },

    #[error("benchmark run {path} has an invalid image identity: {reason}")]
    InvalidImageIdentity { path: PathBuf, reason: String },

    #[error("benchmark run {path} has an invalid load report: {reason}")]
    InvalidReport { path: PathBuf, reason: String },

    #[error("successful Nervix benchmark run {path} has no scraped metrics report")]
    MissingMetricsReport { path: PathBuf },

    #[error("failed to load Nervix metrics report {path}")]
    MetricsReport {
        path: PathBuf,
        #[source]
        source: Box<MetricsReportError>,
    },

    #[error(
        "benchmark '{benchmark}' has duplicate artifacts for implementation '{implementation}'"
    )]
    DuplicateImplementation {
        benchmark: String,
        implementation: String,
    },

    #[error("benchmark '{benchmark}' implementation '{implementation}' has a different {field}")]
    MismatchedConfiguration {
        benchmark: String,
        implementation: String,
        field: &'static str,
    },

    #[error("failed to write benchmark comparison {path}")]
    Write {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl BenchmarkComparison {
    pub fn from_run_directories(run_directories: &[PathBuf]) -> Result<Self, ComparisonError> {
        if run_directories.is_empty() {
            return Err(ComparisonError::Empty);
        }
        let mut grouped = BTreeMap::<String, Vec<RunArtifact>>::new();
        for directory in run_directories {
            let artifact = RunArtifact::load(directory)?;
            grouped
                .entry(artifact.manifest.benchmark.clone())
                .or_default()
                .push(artifact);
        }

        let mut benchmarks = Vec::with_capacity(grouped.len());
        for (slug, mut runs) in grouped {
            runs.sort_by(|left, right| {
                implementation_sort_key(&left.manifest.implementation)
                    .cmp(&implementation_sort_key(&right.manifest.implementation))
            });
            let mut implementations = BTreeSet::new();
            for run in &runs {
                if !implementations.insert(run.manifest.implementation.as_str()) {
                    return Err(ComparisonError::DuplicateImplementation {
                        benchmark: slug,
                        implementation: run.manifest.implementation.clone(),
                    });
                }
            }
            validate_matching_configuration(&slug, &runs)?;
            benchmarks.push(BenchmarkRuns {
                description: runs[0].manifest.description.clone(),
                slug,
                runs,
            });
        }
        Ok(Self { benchmarks })
    }

    #[must_use]
    pub fn render_markdown(&self) -> String {
        let mut markdown = String::from("## Benchmark comparison\n");
        self.render_benchmarks(&mut markdown);
        markdown
    }

    fn render_benchmarks(&self, markdown: &mut String) {
        for benchmark in &self.benchmarks {
            markdown.push_str(&benchmark.render_markdown());
        }
    }

    pub fn write_markdown(&self, path: impl AsRef<Path>) -> Result<(), ComparisonError> {
        let path = path.as_ref();
        fs::write(path, self.render_markdown()).map_err(|source| ComparisonError::Write {
            path: path.to_path_buf(),
            source,
        })
    }
}

impl BenchmarkRunFailure {
    #[must_use]
    pub fn new(
        benchmark: impl Into<String>,
        implementation: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            benchmark: benchmark.into(),
            implementation: implementation.into(),
            message: message.into(),
        }
    }
}

impl BenchmarkSuiteReport {
    pub fn from_run_directories(
        run_directories: &[PathBuf],
        failures: Vec<BenchmarkRunFailure>,
    ) -> Result<Self, ComparisonError> {
        if run_directories.is_empty() && failures.is_empty() {
            return Err(ComparisonError::Empty);
        }
        let comparison = if run_directories.is_empty() {
            None
        } else {
            Some(BenchmarkComparison::from_run_directories(run_directories)?)
        };
        Ok(Self {
            comparison,
            failures,
        })
    }

    #[must_use]
    pub fn failed_runs(&self) -> usize {
        self.failures.len()
    }

    #[must_use]
    pub fn total_runs(&self) -> usize {
        self.successful_runs() + self.failed_runs()
    }

    #[must_use]
    pub fn render_markdown(&self) -> String {
        let successful = self.successful_runs();
        let total = self.total_runs();
        let benchmark_count = self.benchmark_count();
        let mut markdown = format!(
            "## Benchmark comparison\n\n**Execution:** {successful} of {total} catalog executions \
             succeeded; all {total} were attempted across {benchmark_count} workloads.\n"
        );
        let mut statuses = Vec::with_capacity(total);
        if let Some(comparison) = &self.comparison {
            for benchmark in &comparison.benchmarks {
                for run in &benchmark.runs {
                    statuses.push((
                        benchmark.slug.as_str(),
                        run.manifest.implementation.as_str(),
                        true,
                    ));
                }
            }
        }
        for failure in &self.failures {
            statuses.push((
                failure.benchmark.as_str(),
                failure.implementation.as_str(),
                false,
            ));
        }
        statuses.sort_by(|left, right| {
            left.0.cmp(right.0).then_with(|| {
                implementation_sort_key(left.1).cmp(&implementation_sort_key(right.1))
            })
        });
        markdown.push_str("\n### Execution status\n\n| Workload | Implementation | Status |\n");
        markdown.push_str("|:--|:--|:--|\n");
        for (benchmark, implementation, succeeded) in statuses {
            markdown.push_str(&format!(
                "| {} | {} | {} |\n",
                display_name(benchmark),
                display_name(implementation),
                if succeeded {
                    "✅ Passed"
                } else {
                    "❌ Failed"
                }
            ));
        }
        if let Some(comparison) = &self.comparison {
            comparison.render_benchmarks(&mut markdown);
        }
        if !self.failures.is_empty() {
            markdown.push_str(
                "\n> [!WARNING]\n> One or more benchmark implementations failed. Successful \
                 measurements are retained below, and CI remains failed.\n\n### Failed benchmark \
                 implementations\n\n| Benchmark | Implementation | Failure |\n|:--|:--|:--|\n",
            );
            for failure in &self.failures {
                markdown.push_str(&format!(
                    "| {} | {} | {} |\n",
                    display_name(&failure.benchmark),
                    display_name(&failure.implementation),
                    escape_markdown(&single_line(&failure.message)),
                ));
            }
        }
        markdown
    }

    pub fn write_markdown(&self, path: impl AsRef<Path>) -> Result<(), ComparisonError> {
        let path = path.as_ref();
        fs::write(path, self.render_markdown()).map_err(|source| ComparisonError::Write {
            path: path.to_path_buf(),
            source,
        })
    }

    fn successful_runs(&self) -> usize {
        self.comparison
            .as_ref()
            .map(|comparison| {
                comparison
                    .benchmarks
                    .iter()
                    .map(|benchmark| benchmark.runs.len())
                    .sum()
            })
            .unwrap_or(0)
    }

    fn benchmark_count(&self) -> usize {
        let mut benchmarks = BTreeSet::new();
        if let Some(comparison) = &self.comparison {
            benchmarks.extend(
                comparison
                    .benchmarks
                    .iter()
                    .map(|benchmark| benchmark.slug.as_str()),
            );
        }
        benchmarks.extend(
            self.failures
                .iter()
                .map(|failure| failure.benchmark.as_str()),
        );
        benchmarks.len()
    }
}

impl BenchmarkRuns {
    fn render_markdown(&self) -> String {
        let baseline = self
            .runs
            .iter()
            .position(|run| run.manifest.implementation == "nervix")
            .unwrap_or(0);
        let baseline_name = display_name(&self.runs[baseline].manifest.implementation);
        let baseline_rate = self.runs[baseline].report.end_to_end_messages_per_second;
        let best_end_to_end = self
            .runs
            .iter()
            .map(|run| run.report.end_to_end_messages_per_second)
            .fold(f64::NEG_INFINITY, f64::max);
        let best_payload = self
            .runs
            .iter()
            .map(|run| run.report.end_to_end_payload_mib_per_second)
            .fold(f64::NEG_INFINITY, f64::max);
        let best_generation = self
            .runs
            .iter()
            .map(|run| run.report.output_records_per_second_during_generation)
            .fold(f64::NEG_INFINITY, f64::max);
        let best_drain = self
            .runs
            .iter()
            .map(|run| run.report.drain_seconds)
            .fold(f64::INFINITY, f64::min);
        let first = &self.runs[0];

        let mut markdown = format!(
            "\n### {}\n\n{}\n\n**Configuration:** {} s + {} s warm-up · {} partitions · {} B \
             values ({} B wire) · backlog cap {}\n\n",
            display_name(&self.slug),
            single_line(&self.description),
            first.manifest.duration_seconds,
            first.manifest.warmup_seconds,
            first.manifest.partitions,
            format_count(first.manifest.value_bytes),
            format_count(first.report.wire_bytes_per_message),
            format_count(first.manifest.max_backlog_messages),
        );
        markdown.push_str(&format!(
            "| Implementation | End-to-end | Payload | During generation | Drain ↓ | Parity | \
             Peak backlog | vs. {baseline_name} |\n"
        ));
        markdown.push_str("|:--|--:|--:|--:|--:|--:|--:|--:|\n");
        for (index, run) in self.runs.iter().enumerate() {
            let end_to_end = format!(
                "{} msg/s",
                format_count(run.report.end_to_end_messages_per_second.round() as u64)
            );
            let payload = format!("{:.2} MiB/s", run.report.end_to_end_payload_mib_per_second);
            let generation = format!(
                "{} rec/s",
                format_count(
                    run.report
                        .output_records_per_second_during_generation
                        .round() as u64
                )
            );
            let drain = format!("{:.3} s", run.report.drain_seconds);
            let parity = format!(
                "{} in / {} rec",
                format_count(run.report.input_messages),
                format_count(run.report.output_records)
            );
            let backlog_percentage = run.report.peak_backlog_messages as f64
                / run.report.max_backlog_messages as f64
                * 100.0;
            let cap_marker = if run.report.saturated_backlog() {
                "⚠️ "
            } else {
                ""
            };
            let relative = if index == baseline {
                "baseline".to_string()
            } else {
                format_percentage(
                    (run.report.end_to_end_messages_per_second / baseline_rate - 1.0) * 100.0,
                )
            };
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {} | ✅ {} | {}{} ({backlog_percentage:.1}%) | {relative} \
                 |\n",
                display_name(&run.manifest.implementation),
                emphasize_best(
                    end_to_end,
                    run.report.end_to_end_messages_per_second,
                    best_end_to_end
                ),
                emphasize_best(
                    payload,
                    run.report.end_to_end_payload_mib_per_second,
                    best_payload
                ),
                emphasize_best(
                    generation,
                    run.report.output_records_per_second_during_generation,
                    best_generation,
                ),
                emphasize_best(drain, run.report.drain_seconds, best_drain),
                parity,
                cap_marker,
                format_count(run.report.peak_backlog_messages),
            ));
        }

        let saturated = self
            .runs
            .iter()
            .filter(|run| run.report.saturated_backlog())
            .map(|run| display_name(&run.manifest.implementation))
            .collect::<Vec<_>>();
        if !saturated.is_empty() {
            markdown.push_str(&format!(
                "\n> [!WARNING]\n> {} reached the configured backlog cap. Treat this as a \
                 bounded-pressure comparison, not a definitive maximum-throughput result.\n",
                human_list(&saturated)
            ));
        }
        for run in &self.runs {
            if let Some(metrics) = &run.metrics {
                markdown.push_str(&metrics.render_markdown());
            }
        }
        markdown.push_str(
            "\n> [!NOTE]\n> Single-host end-to-end rates include Kafka and the load driver. \
             Implementations retain their native delivery and batch-size semantics.\n",
        );
        markdown.push_str("\n<details>\n<summary>Parameters and provenance</summary>\n\n");
        markdown.push_str(&format!(
            "- Parameters: {}\n",
            render_parameters(&first.manifest.parameters)
        ));
        for run in &self.runs {
            let mut provenance = format!(
                "- {}: subject `{}`, run `{}`",
                display_name(&run.manifest.implementation),
                run.manifest.subject,
                run.directory
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("unknown"),
            );
            if let Some(identity) = &run.image_identity {
                provenance.push_str(&format!(
                    ", image `{}`, digest `{}`",
                    identity.image, identity.id
                ));
            } else if let Some(image) = &run.manifest.image {
                provenance.push_str(&format!(", image `{image}`"));
            }
            provenance.push('\n');
            markdown.push_str(&provenance);
        }
        if let Some(revision) = first.manifest.git_revision.as_deref() {
            markdown.push_str(&format!("- Harness revision: `{revision}`\n"));
        }
        if self
            .runs
            .iter()
            .any(|run| run.manifest.git_dirty == Some(true))
        {
            markdown.push_str("- Harness worktree was recorded as dirty.\n");
        }
        markdown.push_str("\n</details>\n");
        markdown
    }
}

impl RunArtifact {
    pub(crate) fn load(directory: &Path) -> Result<Self, ComparisonError> {
        let status_path = directory.join("status.txt");
        let status = read(&status_path)?;
        if status.trim() != "pass" {
            return Err(ComparisonError::UnsuccessfulRun {
                path: directory.to_path_buf(),
                status: status.trim().to_string(),
            });
        }
        let manifest_path = directory.join("run.toml");
        let manifest = toml::from_str::<RunManifest>(&read(&manifest_path)?).map_err(|source| {
            ComparisonError::ParseManifest {
                path: manifest_path,
                source,
            }
        })?;
        let report_path = directory.join("load-report.txt");
        let report = toml::from_str::<LoadReport>(&read(&report_path)?).map_err(|source| {
            ComparisonError::ParseReport {
                path: report_path,
                source,
            }
        })?;
        validate_report(directory, &manifest, &report)?;
        let metrics_path = directory.join(NERVIX_METRICS_REPORT_FILE);
        let metrics = if manifest.implementation == "nervix" {
            if !metrics_path.is_file() {
                return Err(ComparisonError::MissingMetricsReport {
                    path: directory.to_path_buf(),
                });
            }
            Some(NervixMetricsReport::read(&metrics_path).map_err(|source| {
                ComparisonError::MetricsReport {
                    path: metrics_path,
                    source: Box::new(source),
                }
            })?)
        } else {
            None
        };
        let image_path = directory.join("image.txt");
        let image_identity = if image_path.exists() {
            Some(ImageIdentity::parse(&image_path, &read(&image_path)?)?)
        } else {
            None
        };
        Ok(Self {
            directory: directory.to_path_buf(),
            manifest,
            report,
            metrics,
            image_identity,
        })
    }
}

impl NervixMetricsReport {
    fn render_markdown(&self) -> String {
        let mut markdown = String::from(
            "\n<details>\n<summary>Nervix runtime observations</summary>\n\nMeans use \
             `messages_total / batches_total`; percentiles are upper bounds from the scraped \
             Prometheus histogram buckets.\n\n#### Messages per batch\n\n| Target | Direction | \
             Relay | Mean | p50 | p90 | p99 | Messages / batches \
             |\n|:--|:--|:--|--:|--:|--:|--:|--:|\n",
        );
        for target in &self.batch_targets {
            markdown.push_str(&format!(
                "| {} `{}` | {} | `{}` | {} | ≤{} | ≤{} | ≤{} | {} / {} |\n",
                escape_markdown(&target.target_kind),
                escape_code(&target.target),
                escape_markdown(&target.direction),
                escape_code(&target.relay),
                format_metric_decimal(target.mean_messages_per_batch(), 2),
                format_metric_value(target.p50),
                format_metric_value(target.p90),
                format_metric_value(target.p99),
                format_count(target.messages_total),
                format_count(target.batches_total),
            ));
        }
        markdown.push_str(
            "\n#### Relay buffer length\n\n| Relay | Direction | p50 | p90 | p99 | Observations \
             |\n|:--|:--|--:|--:|--:|--:|\n",
        );
        for relay in &self.relay_buffers {
            markdown.push_str(&format!(
                "| `{}` | {} | ≤{} | ≤{} | ≤{} | {} |\n",
                escape_code(&relay.relay),
                escape_markdown(&relay.direction),
                format_metric_value(relay.p50),
                format_metric_value(relay.p90),
                format_metric_value(relay.p99),
                format_count(relay.observations),
            ));
        }
        markdown.push_str("\n</details>\n");
        markdown
    }
}

impl ImageIdentity {
    fn parse(path: &Path, contents: &str) -> Result<Self, ComparisonError> {
        let mut image = None;
        let mut id = None;
        for line in contents.lines() {
            let Some((name, value)) = line.split_once('=') else {
                return Err(ComparisonError::InvalidImageIdentity {
                    path: path.to_path_buf(),
                    reason: format!("line '{line}' is not a name=value pair"),
                });
            };
            match name {
                "image" if image.is_none() => image = Some(value.to_string()),
                "id" if id.is_none() => id = Some(value.to_string()),
                _ => {
                    return Err(ComparisonError::InvalidImageIdentity {
                        path: path.to_path_buf(),
                        reason: format!("unexpected or duplicate field '{name}'"),
                    });
                }
            }
        }
        let image = image.filter(|value| !value.is_empty());
        let id = id.filter(|value| !value.is_empty());
        match (image, id) {
            (Some(image), Some(id)) => Ok(Self { image, id }),
            _ => Err(ComparisonError::InvalidImageIdentity {
                path: path.to_path_buf(),
                reason: "both image and id are required".to_string(),
            }),
        }
    }
}

fn validate_report(
    directory: &Path,
    manifest: &RunManifest,
    report: &LoadReport,
) -> Result<(), ComparisonError> {
    let invalid = |reason: String| ComparisonError::InvalidReport {
        path: directory.to_path_buf(),
        reason,
    };
    for (name, value) in [
        ("target_duration_seconds", report.target_duration_seconds),
        ("warmup_target_seconds", report.warmup_target_seconds),
        (
            "warmup_generation_seconds",
            report.warmup_generation_seconds,
        ),
        (
            "warmup_parity_stability_seconds",
            report.warmup_parity_stability_seconds,
        ),
        ("generation_seconds", report.generation_seconds),
        ("drain_seconds", report.drain_seconds),
        ("end_to_end_seconds", report.end_to_end_seconds),
        (
            "output_records_per_second_during_generation",
            report.output_records_per_second_during_generation,
        ),
        (
            "end_to_end_messages_per_second",
            report.end_to_end_messages_per_second,
        ),
        (
            "end_to_end_payload_mib_per_second",
            report.end_to_end_payload_mib_per_second,
        ),
    ] {
        if !value.is_finite() || value < 0.0 {
            return Err(invalid(format!("{name} must be finite and non-negative")));
        }
    }
    if report.generation_seconds == 0.0 || report.end_to_end_seconds == 0.0 {
        return Err(invalid(
            "generation and end-to-end durations must be positive".to_string(),
        ));
    }
    if (report.target_duration_seconds - manifest.duration_seconds as f64).abs() > 0.000_001 {
        return Err(invalid(
            "target duration does not match run.toml".to_string(),
        ));
    }
    if (report.warmup_target_seconds - manifest.warmup_seconds as f64).abs() > 0.000_001 {
        return Err(invalid(
            "warm-up target does not match run.toml".to_string(),
        ));
    }
    if report.warmup_generation_seconds < report.warmup_target_seconds {
        return Err(invalid(
            "warm-up generation ended before its target duration".to_string(),
        ));
    }
    if report.warmup_messages == 0 {
        return Err(invalid(
            "a successful benchmark must warm up with at least one message".to_string(),
        ));
    }
    if report.partitions != manifest.partitions {
        return Err(invalid(
            "partition count does not match run.toml".to_string(),
        ));
    }
    if report.max_backlog_messages != manifest.max_backlog_messages {
        return Err(invalid("backlog cap does not match run.toml".to_string()));
    }
    if report.peak_backlog_messages > report.max_backlog_messages {
        return Err(invalid(
            "peak backlog exceeds its configured cap".to_string(),
        ));
    }
    if report.input_messages == 0 {
        return Err(invalid(
            "a successful benchmark must measure at least one message".to_string(),
        ));
    }
    if report.end_to_end_messages_per_second == 0.0 {
        return Err(invalid(
            "end-to-end message rate must be positive".to_string(),
        ));
    }
    if report.expected_output_records == 0 {
        return Err(invalid(
            "a successful benchmark must expect at least one output record".to_string(),
        ));
    }
    if report.output_records != report.expected_output_records {
        return Err(invalid(format!(
            "output parity failed: the workload's shape expects {} records, the run measured {}",
            report.expected_output_records, report.output_records
        )));
    }
    if report.wire_bytes_per_message == 0 {
        return Err(invalid("wire message size must be positive".to_string()));
    }
    Ok(())
}

fn validate_matching_configuration(
    benchmark: &str,
    runs: &[RunArtifact],
) -> Result<(), ComparisonError> {
    let baseline = &runs[0];
    for run in &runs[1..] {
        ensure_matching_run_configuration(benchmark, baseline, run)?;
    }
    Ok(())
}

pub(crate) fn ensure_matching_run_configuration(
    benchmark: &str,
    baseline: &RunArtifact,
    run: &RunArtifact,
) -> Result<(), ComparisonError> {
    let mismatch = |field| ComparisonError::MismatchedConfiguration {
        benchmark: benchmark.to_string(),
        implementation: run.manifest.implementation.clone(),
        field,
    };
    if run.manifest.description != baseline.manifest.description {
        return Err(mismatch("description"));
    }
    if run.manifest.duration_seconds != baseline.manifest.duration_seconds {
        return Err(mismatch("duration_seconds"));
    }
    if run.manifest.warmup_seconds != baseline.manifest.warmup_seconds {
        return Err(mismatch("warmup_seconds"));
    }
    if run.manifest.partitions != baseline.manifest.partitions {
        return Err(mismatch("partitions"));
    }
    if run.manifest.value_bytes != baseline.manifest.value_bytes {
        return Err(mismatch("value_bytes"));
    }
    if run.manifest.max_backlog_messages != baseline.manifest.max_backlog_messages {
        return Err(mismatch("max_backlog_messages"));
    }
    if run.manifest.parameters != baseline.manifest.parameters {
        return Err(mismatch("parameters"));
    }
    if run.report.wire_bytes_per_message != baseline.report.wire_bytes_per_message {
        return Err(mismatch("wire_bytes_per_message"));
    }
    Ok(())
}

fn read(path: &Path) -> Result<String, ComparisonError> {
    fs::read_to_string(path).map_err(|source| ComparisonError::Read {
        path: path.to_path_buf(),
        source,
    })
}

fn implementation_sort_key(implementation: &str) -> (bool, &str) {
    (implementation != "nervix", implementation)
}

pub(crate) fn display_name(value: &str) -> String {
    value
        .split('-')
        .map(|word| match word {
            "json" => "JSON".to_string(),
            "kafka" => "Kafka".to_string(),
            "nervix" => "Nervix".to_string(),
            "vector" => "Vector".to_string(),
            _ => {
                let mut characters = word.chars();
                characters
                    .next()
                    .map(|first| first.to_uppercase().chain(characters).collect())
                    .unwrap_or_default()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
        .replace('|', "\\|")
}

pub(crate) fn single_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn human_list(values: &[String]) -> String {
    match values {
        [] => String::new(),
        [value] => value.clone(),
        [left, right] => format!("{left} and {right}"),
        values => format!(
            "{}, and {}",
            values[..values.len() - 1].join(", "),
            values.last().expect("non-empty list has a last value")
        ),
    }
}

pub(crate) fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let first_group = digits.len() % 3;
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    if first_group > 0 {
        formatted.push_str(&digits[..first_group]);
    }
    for (index, chunk) in digits.as_bytes()[first_group..].chunks(3).enumerate() {
        if first_group > 0 || index > 0 {
            formatted.push(',');
        }
        formatted.push_str(std::str::from_utf8(chunk).expect("decimal digits are valid UTF-8"));
    }
    formatted
}

fn format_metric_value(value: f64) -> String {
    if value.fract() == 0.0 {
        format_count(value as u64)
    } else {
        format_metric_decimal(value, 3)
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

fn format_metric_decimal(value: f64, precision: usize) -> String {
    let rendered = format!("{value:.precision$}");
    let Some((whole, fraction)) = rendered.split_once('.') else {
        return rendered;
    };
    let whole = whole
        .parse::<u64>()
        .map(format_count)
        .unwrap_or_else(|_| whole.to_string());
    format!("{whole}.{fraction}")
}

fn escape_markdown(value: &str) -> String {
    value.replace('|', "\\|")
}

fn escape_code(value: &str) -> String {
    escape_markdown(value).replace('`', "\\`")
}

pub(crate) fn format_percentage(value: f64) -> String {
    if value < 0.0 {
        format!("−{:.1}%", value.abs())
    } else {
        format!("+{value:.1}%")
    }
}

pub(crate) fn emphasize_best(rendered: String, value: f64, best: f64) -> String {
    if value == best {
        format!("**{rendered}**")
    } else {
        rendered
    }
}

pub(crate) fn render_parameters(parameters: &toml::Table) -> String {
    let mut entries = parameters
        .iter()
        .filter(|entry| {
            let name = entry.0;
            !(*name == "emitter_flush_seconds" && parameters.contains_key("emitter_flush_each"))
                && !(*name == "emitter_max_batch_bytes"
                    && parameters.contains_key("emitter_max_batch_size"))
                && !(*name == "window_max_delay_ms" && parameters.contains_key("window_max_delay"))
        })
        .map(|(name, value)| {
            let value = match value {
                toml::Value::String(value) => value.clone(),
                value => value.to_string(),
            };
            format!("`{name}={value}`")
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries.join(", ")
}
