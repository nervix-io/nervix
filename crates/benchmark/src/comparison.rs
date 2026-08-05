use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug)]
pub struct BenchmarkComparison {
    benchmarks: Vec<BenchmarkRuns>,
}

#[derive(Debug)]
struct BenchmarkRuns {
    slug: String,
    description: String,
    runs: Vec<RunArtifact>,
}

#[derive(Debug)]
struct RunArtifact {
    directory: PathBuf,
    manifest: RunManifest,
    report: LoadReport,
    image_identity: Option<ImageIdentity>,
}

#[derive(Debug, Deserialize)]
struct RunManifest {
    benchmark: String,
    description: String,
    duration_seconds: u64,
    git_dirty: Option<bool>,
    git_revision: Option<String>,
    image: Option<String>,
    implementation: String,
    max_backlog_messages: u64,
    partitions: u32,
    subject: String,
    value_bytes: u64,
    parameters: toml::Table,
}

#[derive(Debug, Deserialize)]
struct LoadReport {
    target_duration_seconds: f64,
    generation_seconds: f64,
    drain_seconds: f64,
    end_to_end_seconds: f64,
    wire_bytes_per_message: u64,
    partitions: u32,
    max_backlog_messages: u64,
    peak_backlog_messages: u64,
    input_messages: u64,
    output_messages: u64,
    output_messages_per_second_during_generation: f64,
    end_to_end_messages_per_second: f64,
    end_to_end_payload_mib_per_second: f64,
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

    #[error(
        "benchmark '{benchmark}' has duplicate artifacts for implementation '{implementation}'"
    )]
    DuplicateImplementation {
        benchmark: String,
        implementation: String,
    },

    #[error(
        "benchmark '{benchmark}' implementation '{implementation}' has a different {field}"
    )]
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
        for benchmark in &self.benchmarks {
            markdown.push_str(&benchmark.render_markdown());
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
}

impl BenchmarkRuns {
    fn render_markdown(&self) -> String {
        let baseline = self
            .runs
            .iter()
            .position(|run| run.manifest.implementation == "nervix")
            .unwrap_or(0);
        let baseline_name = display_name(&self.runs[baseline].manifest.implementation);
        let baseline_rate = self.runs[baseline]
            .report
            .end_to_end_messages_per_second;
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
            .map(|run| run.report.output_messages_per_second_during_generation)
            .fold(f64::NEG_INFINITY, f64::max);
        let best_drain = self
            .runs
            .iter()
            .map(|run| run.report.drain_seconds)
            .fold(f64::INFINITY, f64::min);
        let first = &self.runs[0];

        let mut markdown = format!(
            "\n### {}\n\n{}\n\n**Configuration:** {} s · {} partitions · {} B values ({} B wire) · backlog cap {}\n\n",
            display_name(&self.slug),
            single_line(&self.description),
            first.manifest.duration_seconds,
            first.manifest.partitions,
            format_count(first.manifest.value_bytes),
            format_count(first.report.wire_bytes_per_message),
            format_count(first.manifest.max_backlog_messages),
        );
        markdown.push_str(&format!(
            "| Implementation | End-to-end | Payload | During generation | Drain ↓ | Parity | Peak backlog | vs. {baseline_name} |\n"
        ));
        markdown.push_str("|:--|--:|--:|--:|--:|--:|--:|--:|\n");
        for (index, run) in self.runs.iter().enumerate() {
            let end_to_end = format!(
                "{} msg/s",
                format_count(run.report.end_to_end_messages_per_second.round() as u64)
            );
            let payload = format!(
                "{:.2} MiB/s",
                run.report.end_to_end_payload_mib_per_second
            );
            let generation = format!(
                "{} msg/s",
                format_count(
                    run.report
                        .output_messages_per_second_during_generation
                        .round() as u64
                )
            );
            let drain = format!("{:.3} s", run.report.drain_seconds);
            let backlog_percentage = run.report.peak_backlog_messages as f64
                / run.report.max_backlog_messages as f64
                * 100.0;
            let cap_marker = if run.report.peak_backlog_messages
                == run.report.max_backlog_messages
            {
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
                "| {} | {} | {} | {} | {} | ✅ {} | {}{} ({backlog_percentage:.1}%) | {relative} |\n",
                display_name(&run.manifest.implementation),
                emphasize_best(end_to_end, run.report.end_to_end_messages_per_second, best_end_to_end),
                emphasize_best(payload, run.report.end_to_end_payload_mib_per_second, best_payload),
                emphasize_best(
                    generation,
                    run.report.output_messages_per_second_during_generation,
                    best_generation,
                ),
                emphasize_best(drain, best_drain, run.report.drain_seconds),
                format_count(run.report.input_messages),
                cap_marker,
                format_count(run.report.peak_backlog_messages),
            ));
        }

        let saturated = self
            .runs
            .iter()
            .filter(|run| run.report.peak_backlog_messages == run.report.max_backlog_messages)
            .map(|run| display_name(&run.manifest.implementation))
            .collect::<Vec<_>>();
        if !saturated.is_empty() {
            markdown.push_str(&format!(
                "\n> [!WARNING]\n> {} reached the configured backlog cap. Treat this as a bounded-pressure comparison, not a definitive maximum-throughput result.\n",
                saturated.join(", ")
            ));
        }
        markdown.push_str(
            "\n> [!NOTE]\n> Single-host end-to-end rates include Kafka and the load driver. Implementations retain their native delivery and batch-size semantics.\n",
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
    fn load(directory: &Path) -> Result<Self, ComparisonError> {
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
            image_identity,
        })
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
        ("generation_seconds", report.generation_seconds),
        ("drain_seconds", report.drain_seconds),
        ("end_to_end_seconds", report.end_to_end_seconds),
        (
            "output_messages_per_second_during_generation",
            report.output_messages_per_second_during_generation,
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
    if report.partitions != manifest.partitions {
        return Err(invalid("partition count does not match run.toml".to_string()));
    }
    if report.max_backlog_messages != manifest.max_backlog_messages {
        return Err(invalid("backlog cap does not match run.toml".to_string()));
    }
    if report.peak_backlog_messages > report.max_backlog_messages {
        return Err(invalid("peak backlog exceeds its configured cap".to_string()));
    }
    if report.input_messages != report.output_messages {
        return Err(invalid(format!(
            "input/output parity failed: {} input, {} output",
            report.input_messages, report.output_messages
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

fn display_name(value: &str) -> String {
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

fn single_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let first_group = digits.len() % 3;
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    if first_group > 0 {
        formatted.push_str(&digits[..first_group]);
    }
    for (index, chunk) in digits[first_group..].as_bytes().chunks(3).enumerate() {
        if first_group > 0 || index > 0 {
            formatted.push(',');
        }
        formatted.push_str(std::str::from_utf8(chunk).expect("decimal digits are valid UTF-8"));
    }
    formatted
}

fn format_percentage(value: f64) -> String {
    if value < 0.0 {
        format!("−{:.1}%", value.abs())
    } else {
        format!("+{value:.1}%")
    }
}

fn emphasize_best(rendered: String, value: f64, best: f64) -> String {
    if value == best {
        format!("**{rendered}**")
    } else {
        rendered
    }
}

fn render_parameters(parameters: &toml::Table) -> String {
    let mut entries = parameters
        .iter()
        .filter(|(name, _)| {
            !(*name == "emitter_flush_seconds" && parameters.contains_key("emitter_flush_each"))
                && !(*name == "emitter_max_batch_bytes"
                    && parameters.contains_key("emitter_max_batch_size"))
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
