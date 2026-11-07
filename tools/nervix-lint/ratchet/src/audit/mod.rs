//! Repository tooling, outside the product layer order.
//! Owns: one in-process analysis-to-audit conversion and publication of review reports.
//! Depends on: the matrix runner, typed provenance owners and filesystem publication.
//! Must not know: product graph execution or enforcement decisions.

use std::{collections::BTreeMap, fs, path::PathBuf};

use error_stack::{Report as ErrorReport, ResultExt as _};

mod attribution;
mod blame;
mod candidates;
mod catalog;
mod encoding;
mod git;
mod history;
mod hunks;
mod line_history;
mod pull_requests;
mod render;
mod report;
mod spellings;

#[cfg(test)]
mod tests;

use attribution::Attributor;
use candidates::Collector;
use encoding::Encoding;
use git::Git;
use report::{AttributedCandidate, Introduction, Report, Summary};

use crate::{
    error::{Error, Result},
    evidence::MatrixReport,
    matrix::Analyze,
};

#[derive(Clone, Debug, clap::Args)]
pub struct Options {
    #[arg(long)]
    pub root: Option<PathBuf>,
    #[arg(long)]
    pub target_dir: Option<PathBuf>,
    /// Analyze with warning levels, so current diagnostics do not stop the audit.
    #[arg(long)]
    pub inventory: bool,
    /// Audit selected declared configurations as an inventory.
    #[arg(long)]
    pub configuration: Vec<String>,
    /// Also read the commits squash merges folded away, through gh.
    #[arg(long)]
    pub pull_requests: bool,
    #[arg(long)]
    pub output_dir: Option<PathBuf>,
}

impl Options {
    fn analysis(&self) -> Result<Analyze> {
        let mut analysis = Analyze {
            root: self.root.clone(),
            target_dir: self.target_dir.clone(),
            configuration: self.configuration.clone(),
            turmoil_child: false,
            fixture_mode: None,
            fresh: false,
            recompile: false,
            inventory: self.inventory || !self.configuration.is_empty(),
            output: None,
            show: false,
            jobs: None,
        };
        let root = fs::canonicalize(analysis.root()).change_context(Error::Io {
            operation: "resolve audit root",
            path: analysis.root(),
        })?;
        analysis.root = Some(root);
        let target = analysis.target();
        fs::create_dir_all(&target).change_context(Error::Io {
            operation: "create audit target directory",
            path: target.clone(),
        })?;
        analysis.target_dir = Some(fs::canonicalize(&target).change_context(Error::Io {
            operation: "resolve audit target directory",
            path: target,
        })?);
        let relative = if analysis.inventory {
            "lint-audit/inventory.json"
        } else {
            "typed-ratchet/gate.json"
        };
        analysis.output = Some(analysis.target().join(relative));
        Ok(analysis)
    }

    pub fn run(&self) -> Result<()> {
        let analysis = self.analysis()?;
        let matrix = analysis.run()?;
        let git = Git::new(&analysis.root());
        let report = build(git, matrix, self.pull_requests)?;
        let directory = match &self.output_dir {
            Some(directory) => directory.clone(),
            None => analysis
                .target()
                .join("lint-audit")
                .join(render::prefix(&report.source_identity, 16)),
        };
        publish(&directory, &report)?;
        for (kind, counts) in &report.summary.0 {
            println!(
                "lint audit: {kind}: {} candidates, {} recurring, {} suspected",
                counts.total, counts.recurring, counts.suspected
            );
        }
        println!(
            "lint audit: {} candidates at {}; report: {}",
            report.candidates.len(),
            render::prefix(&report.revision, 12),
            directory.join("audit.md").display()
        );
        Ok(())
    }
}

fn build(git: Git, matrix: MatrixReport, pull_requests: bool) -> Result<Report> {
    if !matrix.complete {
        return Err(ErrorReport::new(Error::AuditIncomplete));
    }
    let revision = git.text(&["rev-parse", "HEAD"])?;
    let mut candidates = Collector::new().collect(matrix.evidence)?;
    let mut attributor = Attributor::new(git, pull_requests)?;
    candidates.extend(attributor.relocated(matrix.findings)?);
    attributor.prepare(&candidates)?;
    let mut attributed = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let attribution = attributor.attribute(&candidate)?;
        attributed.push(AttributedCandidate {
            candidate,
            attribution,
        });
    }
    let mut rules = BTreeMap::new();
    for (name, commit) in attributor.history.introduced {
        rules.insert(
            name,
            Introduction {
                introduced: commit.sha,
                date: commit.date,
            },
        );
    }
    Ok(Report {
        revision: revision.trim().into(),
        source_identity: matrix.source_identity,
        matrix_complete: matrix.matrix_complete,
        configurations: matrix
            .configurations
            .into_iter()
            .map(|configuration| configuration.name)
            .collect(),
        pull_requests,
        rules,
        summary: Summary::of(&attributed),
        candidates: attributed,
        complete: (),
    })
}

fn publish(directory: &std::path::Path, report: &Report) -> Result<()> {
    fs::create_dir_all(directory).change_context(Error::Io {
        operation: "create audit report directory",
        path: directory.into(),
    })?;
    let path = directory.join("audit.json");
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let bytes = Encoding::new().report(report)?;
    fs::write(&temporary, bytes).change_context(Error::Io {
        operation: "write audit report",
        path: temporary.clone(),
    })?;
    fs::rename(&temporary, &path).change_context(Error::Io {
        operation: "publish audit report",
        path,
    })?;
    let path = directory.join("audit.md");
    fs::write(&path, render::markdown(report)).change_context(Error::Io {
        operation: "write audit Markdown",
        path,
    })
}
