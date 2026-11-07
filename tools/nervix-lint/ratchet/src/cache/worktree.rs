//! Repository qualification tooling, outside the product layer order.
//! Owns: isolated checkout setup and cross-worktree evidence rejection probes.
//! Depends on: Git, filesystem copies and production report validation.
//! Must not know: product execution or compiler policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use error_stack::ResultExt as _;
use nervix_lint_report::CompilerReport;

use super::Problem;
use crate::{
    configuration::Configuration,
    error::{Error, Result},
    evidence::{CargoMessage, Completion},
    io, process,
    runner::Runner,
};

pub fn add(root: &Path, destination: &Path) -> Result<()> {
    process::output(
        "git",
        &[
            "worktree".into(),
            "add".into(),
            "--detach".into(),
            destination.display().to_string(),
            "HEAD".into(),
        ],
        root,
        &BTreeMap::new(),
    )?;
    Ok(())
}

pub fn remove(root: &Path, destination: &Path) -> Result<()> {
    process::output(
        "git",
        &[
            "worktree".into(),
            "remove".into(),
            "--force".into(),
            destination.display().to_string(),
        ],
        root,
        &BTreeMap::new(),
    )?;
    Ok(())
}

pub fn copy(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).change_context(Error::Qualification)?;
    }
    fs::copy(source, destination).change_context(Error::Qualification)?;
    Ok(())
}

pub fn copy_tooling(source: &Path, destination: &Path) -> Result<()> {
    for (relative, name) in [
        ("driver", "nervix-lint-driver"),
        ("driver", "nervix-lint-report"),
        ("source", "nervix-ratchet"),
    ] {
        let relative = Path::new("typed-ratchet")
            .join(relative)
            .join("debug")
            .join(name);
        copy(&source.join(&relative), &destination.join(relative))?;
    }
    Ok(())
}

pub fn copy_current(root: &Path, destination: &Path) -> Result<()> {
    let changed = process::text(
        "git",
        &[
            "diff".into(),
            "--name-only".into(),
            "-z".into(),
            "HEAD".into(),
        ],
        root,
        &BTreeMap::new(),
    )?;
    let untracked = process::text(
        "git",
        &[
            "ls-files".into(),
            "-z".into(),
            "--others".into(),
            "--exclude-standard".into(),
        ],
        root,
        &BTreeMap::new(),
    )?;
    let names: BTreeSet<_> = changed.split('\0').chain(untracked.split('\0')).collect();
    for name in names {
        if name.is_empty() {
            continue;
        }
        let source = root.join(name);
        let target = destination.join(name);
        if source.is_file() {
            copy(&source, &target)?;
        } else {
            io::remove_if_present(&target)?;
        }
    }
    Ok(())
}

pub fn reject_foreign_report(
    root: &Path,
    target: &Path,
    evidence: &Completion,
    identity: &str,
) -> Result<()> {
    let runner = Runner::new(root, target, false)?;
    let messages = process::messages(&evidence.cargo_log)?;
    let mut changed = false;
    for message in &messages {
        let CargoMessage::CompilerArtifact(artifact) = message else {
            continue;
        };
        if evidence.reports.contains_key(&artifact.key()) {
            let paths = crate::runner::report_candidates(artifact, &evidence.report_directory);
            Problem::ForeignReport.require(paths.len() == 1)?;
            let path = &paths[0];
            let mut report: CompilerReport = io::read(path)?;
            report.identity = identity.into();
            io::atomic_json(path, &report)?;
            changed = true;
            break;
        }
    }
    Problem::ForeignReport.require(changed)?;
    let result = runner.validate_reports(
        &Configuration::fixture("ordinary"),
        &evidence.expected,
        &messages,
        &evidence.report_directory,
    );
    let rejected = matches!(result, Err(ref error) if matches!(error.current_context(), Error::Evidence {issue:crate::error::Issue::Identity, ..}));
    Problem::ForeignReport.require(rejected)
}
