//! Repository qualification tooling, outside the product layer order.
//! Owns: two-worktree cache, changed-input and interrupted-run qualification.
//! Depends on: production analysis owners, Git, kache and current fixture sources.
//! Must not know: product execution or source-site policy.

use std::{
    fs,
    path::{Path, PathBuf},
};

use error_stack::{Report, ResultExt as _};
use serde::Serialize;

use crate::{
    configuration::{Configuration, TOOLCHAIN},
    error::{Error, Result},
    evidence::{CargoMessage, Completion},
    io, process,
    runner::Runner,
};

mod changes;
mod interruption;
mod worktree;

#[derive(clap::Args)]
pub struct QualifyCache {
    #[arg(long)]
    pub root: PathBuf,
    #[arg(long)]
    pub target_dir: PathBuf,
}

#[derive(Debug, thiserror::Error)]
enum Problem {
    #[error("Cargo-fresh analysis changed its complete reports")]
    ChangedFreshReports,
    #[error("repeated analysis did not observe a Cargo-fresh artifact")]
    NoFreshArtifact,
    #[error("the configured cache did not restore the warmed fixture dependency")]
    NoCacheRestore,
    #[error("missing side reports did not re-establish complete analysis in a fresh namespace")]
    MissingReportRecovery,
    #[error("worktree identity was not part of the cache key")]
    SharedWorktreeIdentity,
    #[error("a side report from another worktree was accepted")]
    ForeignReport,
    #[error("changed source, contract, rule, dependency or configuration reused stale analysis")]
    ChangedInput,
    #[error("an interrupted analysis published complete evidence")]
    InterruptedEvidence,
    #[error("the analysis never reached Cargo before its interruption budget")]
    InterruptionReadiness,
}

impl Problem {
    fn require(self, condition: bool) -> Result<()> {
        if condition {
            Ok(())
        } else {
            Err(Report::new(self).change_context(Error::Qualification))
        }
    }
}

impl Runner {
    fn warm_dependency(&self, configuration: &Configuration, first: &Completion) -> Result<()> {
        let (mut child, log) = self.start_cargo(
            configuration,
            &first.build_directory,
            &first.report_directory,
            &first.cache_namespace,
        )?;
        let status = child.wait().change_context(Error::Qualification)?;
        if !status.success() {
            return Err(self.failure(configuration, crate::error::Issue::Compilation, &log));
        }
        let messages = process::messages(&log)?;
        self.validate_reports(
            configuration,
            &first.expected,
            &messages,
            &first.report_directory,
        )?;
        Ok(())
    }
}

#[derive(Serialize)]
struct CacheRun {
    root: PathBuf,
    identity: String,
    fresh: Completion,
    cargo_fresh: Completion,
    kache_hits: Vec<serde_json::Value>,
    missing_side_report_recovered: bool,
    recovered: Completion,
    paired_doctests: &'static str,
}

fn qualify(root: &Path, target: &Path) -> Result<CacheRun> {
    let runner = Runner::new(root, target, false)?;
    let configuration = Configuration::fixture("ordinary");
    let first = runner.analyze_one(&configuration, true)?;
    let repeated = runner.analyze_one(&configuration, true)?;
    Problem::ChangedFreshReports.require(repeated.reports == first.reports)?;
    let messages = process::messages(&repeated.cargo_log)?;
    Problem::NoFreshArtifact.require(messages.iter().any(
        |message| matches!(message, CargoMessage::CompilerArtifact(artifact) if artifact.fresh),
    ))?;
    let started = io::unix_ns() / 1_000_000;
    let mut hits = Vec::new();
    for _ in 0..3 {
        process::output(
            "cargo",
            &[
                format!("+{TOOLCHAIN}"),
                "clean".into(),
                "--manifest-path".into(),
                configuration.manifest.clone(),
                "--target-dir".into(),
                first.build_directory.display().to_string(),
                "--package".into(),
                "nervix-lint-fixture-macros".into(),
            ],
            root,
            &runner.environment,
        )?;
        runner.warm_dependency(&configuration, &first)?;
        let report = process::text(
            "kache",
            &[
                "report".into(),
                "--format".into(),
                "json".into(),
                "--since".into(),
                "15m".into(),
                "--root".into(),
                target.join("typed-ratchet").display().to_string(),
            ],
            root,
            &runner.environment,
        )?;
        let report: serde_json::Value =
            serde_json::from_str(&report).change_context(Error::Qualification)?;
        if let Some(events) = report["all_events"].as_array() {
            for event in events {
                if event["crate_name"] == "nervix_lint_fixture_macros"
                    && event["result"]
                        .as_str()
                        .is_some_and(|result| result.contains("hit"))
                    && event["start_unix_ms"]
                        .as_i64()
                        .is_some_and(|time| i128::from(time) >= started)
                {
                    hits.push(event.clone());
                }
            }
        }
        if !hits.is_empty() {
            break;
        }
    }
    Problem::NoCacheRestore.require(!hits.is_empty())?;
    for entry in fs::read_dir(&first.report_directory).change_context(Error::Qualification)? {
        let path = entry.change_context(Error::Qualification)?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            io::remove_if_present(&path)?;
        }
    }
    let recovered = runner.analyze_one(&configuration, true)?;
    Problem::MissingReportRecovery
        .require(recovered.complete && recovered.cache_namespace != first.cache_namespace)?;
    for _ in 0..2 {
        process::output(
            "just",
            &["test-typed-ratchet-docs".into()],
            root,
            &runner.environment,
        )?;
    }
    Ok(CacheRun {
        root: root.into(),
        identity: runner.identity,
        fresh: first,
        cargo_fresh: repeated,
        kache_hits: hits,
        missing_side_report_recovered: true,
        recovered,
        paired_doctests: "passed twice with the configured wrapper",
    })
}

#[derive(Serialize)]
struct Qualification {
    complete: bool,
    first: CacheRun,
    second: CacheRun,
    changes: changes::Changes,
    interruption: interruption::Interruption,
    cross_worktree_rejected: bool,
    wrapper: &'static str,
}

impl QualifyCache {
    pub fn run(&self) -> Result<()> {
        let directory = self
            .target_dir
            .join("typed-ratchet/cache-qualification")
            .join(uuid::Uuid::new_v4().simple().to_string());
        fs::create_dir_all(&directory).change_context(Error::Qualification)?;
        let first = qualify(&self.root, &self.target_dir)?;
        let second_root = directory.join("worktree");
        worktree::add(&self.root, &second_root)?;
        let result = self.qualify_second(&directory, &second_root, first);
        let cleanup = worktree::remove(&self.root, &second_root);
        match result {
            Ok(report) => {
                cleanup?;
                let path = directory.join("qualification.json");
                io::atomic_json(&path, &report)?;
                println!("typed ratchet cache qualification: {}", path.display());
                Ok(())
            }
            Err(error) => match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.attach(format!("worktree cleanup also failed: {cleanup:#}")))
                }
            },
        }
    }

    fn qualify_second(
        &self,
        directory: &Path,
        root: &Path,
        first: CacheRun,
    ) -> Result<Qualification> {
        worktree::copy_current(&self.root, root)?;
        let target = directory.join("second-target");
        for (relative, name) in [
            ("driver", "nervix-lint-driver"),
            ("driver", "nervix-lint-report"),
            ("source", "nervix-ratchet"),
        ] {
            let relative = Path::new("typed-ratchet")
                .join(relative)
                .join("debug")
                .join(name);
            worktree::copy(&self.target_dir.join(&relative), &target.join(relative))?;
        }
        let second = qualify(root, &target)?;
        Problem::SharedWorktreeIdentity.require(first.identity != second.identity)?;
        worktree::reject_foreign_report(root, &target, &second.recovered, &first.identity)?;
        let changes = changes::qualify(root, &target)?;
        let interruption = interruption::qualify(root, &target, directory)?;
        Ok(Qualification {
            complete: true,
            first,
            second,
            changes,
            interruption,
            cross_worktree_rejected: true,
            wrapper: "configured RUSTC_WRAPPER preserved",
        })
    }
}
