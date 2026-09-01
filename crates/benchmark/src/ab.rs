use std::{
    fs, io,
    path::{Path, PathBuf},
};

use thiserror::Error;

use crate::comparison::{
    ComparisonError, RunArtifact, display_name, emphasize_best, ensure_matching_run_configuration,
    format_count, format_percentage, render_parameters, single_line,
};

/// One side of a local A/B comparison: a labelled server binary and the run directories that
/// measured it.
#[derive(Debug)]
pub struct AbArm {
    pub label: String,
    pub server_binary: PathBuf,
    pub run_directories: Vec<PathBuf>,
}

#[derive(Debug, Error)]
pub enum AbError {
    #[error("A/B arm '{arm}' has no benchmark runs")]
    EmptyArm { arm: String },

    #[error("A/B arm '{arm}' has an unusable run {}", directory.display())]
    Artifact {
        arm: String,
        directory: PathBuf,
        #[source]
        source: Box<ComparisonError>,
    },

    #[error(
        "A/B arm '{arm}' run {} does not match the baseline workload configuration",
        directory.display()
    )]
    MismatchedConfiguration {
        arm: String,
        directory: PathBuf,
        #[source]
        source: Box<ComparisonError>,
    },

    #[error("failed to write A/B summary {}", path.display())]
    Write {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// A local same-hardware A/B comparison between a baseline and a candidate server build running
/// the same workload.
#[derive(Debug)]
pub struct AbSummary {
    baseline: ArmSummary,
    candidate: ArmSummary,
}

#[derive(Debug)]
struct ArmSummary {
    label: String,
    server_binary: PathBuf,
    runs: Vec<RunArtifact>,
}

impl AbSummary {
    pub fn from_arms(baseline: AbArm, candidate: AbArm) -> Result<Self, AbError> {
        let baseline = ArmSummary::load(baseline)?;
        let candidate = ArmSummary::load(candidate)?;
        let reference = &baseline.runs[0];
        let slug = reference.manifest.benchmark.clone();
        for arm in [&baseline, &candidate] {
            for run in &arm.runs {
                let mismatch = |source: ComparisonError| AbError::MismatchedConfiguration {
                    arm: arm.label.clone(),
                    directory: run.directory.clone(),
                    source: Box::new(source),
                };
                if run.manifest.benchmark != slug {
                    return Err(mismatch(ComparisonError::MismatchedConfiguration {
                        benchmark: slug,
                        implementation: run.manifest.implementation.clone(),
                        field: "benchmark",
                    }));
                }
                ensure_matching_run_configuration(&slug, reference, run).map_err(mismatch)?;
            }
        }
        Ok(Self {
            baseline,
            candidate,
        })
    }

    #[must_use]
    pub fn render_markdown(&self) -> String {
        let reference = &self.baseline.runs[0];
        let manifest = &reference.manifest;
        let mut markdown = format!(
            "## A/B benchmark comparison — {}\n\n{}\n\n**Configuration:** {} s · {} partitions · \
             {} B values ({} B wire) · backlog cap {}\n\n",
            display_name(&manifest.benchmark),
            single_line(&manifest.description),
            manifest.duration_seconds,
            manifest.partitions,
            format_count(manifest.value_bytes),
            format_count(reference.report.wire_bytes_per_message),
            format_count(manifest.max_backlog_messages),
        );

        let best_mean = self.baseline.mean_rate().max(self.candidate.mean_rate());
        markdown.push_str("| Arm | Runs | End-to-end mean | Min | Max | Saturated runs |\n");
        markdown.push_str("|:--|--:|--:|--:|--:|--:|\n");
        for arm in [&self.baseline, &self.candidate] {
            let saturated = arm.saturated_runs();
            let marker = if saturated > 0 { "⚠️ " } else { "" };
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {} | {marker}{saturated}/{} |\n",
                arm.label,
                arm.runs.len(),
                emphasize_best(format_rate(arm.mean_rate()), arm.mean_rate(), best_mean),
                format_rate(arm.min_rate()),
                format_rate(arm.max_rate()),
                arm.runs.len(),
            ));
        }

        markdown.push_str(&format!(
            "\n**{} vs {} (mean end-to-end): {}**\n\n",
            self.candidate.label,
            self.baseline.label,
            format_percentage(
                (self.candidate.mean_rate() / self.baseline.mean_rate() - 1.0) * 100.0
            ),
        ));

        markdown.push_str("| Arm | End-to-end | Peak backlog | Run directory |\n");
        markdown.push_str("|:--|--:|--:|:--|\n");
        for arm in [&self.baseline, &self.candidate] {
            for run in &arm.runs {
                let backlog_percentage = run.report.peak_backlog_messages as f64
                    / run.report.max_backlog_messages as f64
                    * 100.0;
                let marker = if run.report.saturated_backlog() {
                    "⚠️ "
                } else {
                    ""
                };
                markdown.push_str(&format!(
                    "| {} | {} | {marker}{} ({backlog_percentage:.1}%) | {} |\n",
                    arm.label,
                    format_rate(run.report.end_to_end_messages_per_second),
                    format_count(run.report.peak_backlog_messages),
                    run.directory.display(),
                ));
            }
        }

        let saturated_arms = [&self.baseline, &self.candidate]
            .into_iter()
            .filter(|arm| arm.saturated_runs() > 0)
            .collect::<Vec<_>>();
        if !saturated_arms.is_empty() {
            markdown.push_str("\n> [!WARNING]\n");
            for arm in &saturated_arms {
                markdown.push_str(&format!(
                    "> {} reached the configured backlog cap in {} of {} runs.\n",
                    arm.label,
                    arm.saturated_runs(),
                    arm.runs.len(),
                ));
            }
            markdown.push_str(
                "> Treat capped rates as bounded-pressure results, not maximum throughput.\n",
            );
        }

        markdown.push_str(
            "\n> [!NOTE]\n> Arms ran interleaved on one host in a single invocation. Rates \
             include Kafka and the load driver; compare arms only against each other.\n",
        );

        markdown.push_str("\n<details>\n<summary>Parameters and provenance</summary>\n\n");
        markdown.push_str(&format!(
            "- Parameters: {}\n",
            render_parameters(&manifest.parameters)
        ));
        for arm in [&self.baseline, &self.candidate] {
            markdown.push_str(&format!(
                "- {}: server binary `{}`\n",
                arm.label,
                arm.server_binary.display(),
            ));
        }
        if let Some(revision) = manifest.git_revision.as_deref() {
            markdown.push_str(&format!("- Harness revision: `{revision}`\n"));
        }
        if [&self.baseline, &self.candidate]
            .into_iter()
            .flat_map(|arm| &arm.runs)
            .any(|run| run.manifest.git_dirty == Some(true))
        {
            markdown.push_str("- Harness worktree was recorded as dirty.\n");
        }
        markdown.push_str("\n</details>\n");
        markdown
    }

    pub fn write_markdown(&self, path: impl AsRef<Path>) -> Result<(), AbError> {
        let path = path.as_ref();
        fs::write(path, self.render_markdown()).map_err(|source| AbError::Write {
            path: path.to_path_buf(),
            source,
        })
    }
}

impl ArmSummary {
    fn load(arm: AbArm) -> Result<Self, AbError> {
        if arm.run_directories.is_empty() {
            return Err(AbError::EmptyArm { arm: arm.label });
        }
        let mut runs = Vec::with_capacity(arm.run_directories.len());
        for directory in &arm.run_directories {
            runs.push(
                RunArtifact::load(directory).map_err(|source| AbError::Artifact {
                    arm: arm.label.clone(),
                    directory: directory.clone(),
                    source: Box::new(source),
                })?,
            );
        }
        Ok(Self {
            label: arm.label,
            server_binary: arm.server_binary,
            runs,
        })
    }

    fn rates(&self) -> impl Iterator<Item = f64> + '_ {
        self.runs
            .iter()
            .map(|run| run.report.end_to_end_messages_per_second)
    }

    fn mean_rate(&self) -> f64 {
        self.rates().sum::<f64>() / self.runs.len() as f64
    }

    fn min_rate(&self) -> f64 {
        self.rates().fold(f64::INFINITY, f64::min)
    }

    fn max_rate(&self) -> f64 {
        self.rates().fold(f64::NEG_INFINITY, f64::max)
    }

    fn saturated_runs(&self) -> usize {
        self.runs
            .iter()
            .filter(|run| run.report.saturated_backlog())
            .count()
    }
}

fn format_rate(rate: f64) -> String {
    format!("{} msg/s", format_count(rate.round() as u64))
}
