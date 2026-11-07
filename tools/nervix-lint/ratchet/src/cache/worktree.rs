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
    fs::copy(source, destination)
        .change_context(Error::Io {
            operation: "copy cache qualification input",
            path: source.into(),
        })
        .change_context(Error::Qualification)?;
    Ok(())
}

pub fn copy_tooling(source: &Path, destination: &Path) -> Result<()> {
    for (relative, name) in [
        ("driver", "nervix-lint-driver"),
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

#[cfg(test)]
mod tests {
    //! Repository test harness, outside the product layer order.
    //! Owns: current qualification-tool copies and required-input refusal witnesses.
    //! Depends on: the production copy owner and private filesystem fixtures.
    //! Must not know: Cargo execution or product graph policy.

    use std::{fs, path::Path};

    use meticulous::{OptionExt as _, ResultExt as _};
    use tempfile::tempdir;

    use super::copy_tooling;
    use crate::error::Error;

    #[test]
    fn the_current_driver_and_runner_are_copied_with_their_paths() {
        let source = tempdir().assured("the source fixture directory is available");
        let destination = tempdir().assured("the destination fixture directory is available");
        let driver = Path::new("typed-ratchet/driver/debug/nervix-lint-driver");
        let runner = Path::new("typed-ratchet/source/debug/nervix-ratchet");
        for (path, bytes) in [
            (driver, b"driver".as_slice()),
            (runner, b"runner".as_slice()),
        ] {
            let path = source.path().join(path);
            fs::create_dir_all(
                path.parent()
                    .assured("a tool path has its parent directory"),
            )
            .assured("the fixture parent directory is available");
            fs::write(path, bytes).assured("the fixture tool can be written");
        }

        copy_tooling(source.path(), destination.path())
            .assured("the current driver and runner are the complete tooling inputs");

        assert_eq!(
            fs::read(destination.path().join(driver)).assured("the driver was copied"),
            b"driver"
        );
        assert_eq!(
            fs::read(destination.path().join(runner)).assured("the runner was copied"),
            b"runner"
        );
    }

    #[test]
    fn a_missing_required_runner_keeps_its_typed_cause_and_input_path() {
        let source = tempdir().assured("the source fixture directory is available");
        let destination = tempdir().assured("the destination fixture directory is available");
        let driver = source
            .path()
            .join("typed-ratchet/driver/debug/nervix-lint-driver");
        fs::create_dir_all(driver.parent().assured("the fixture driver has its parent"))
            .assured("the fixture parent directory is available");
        fs::write(driver, b"driver").assured("the fixture driver can be written");

        let error = copy_tooling(source.path(), destination.path())
            .err()
            .assured("qualification requires its runner");

        assert!(matches!(error.current_context(), Error::Qualification));
        assert!(error.contains::<std::io::Error>());
        let runner = source
            .path()
            .join("typed-ratchet/source/debug/nervix-ratchet");
        assert!(format!("{error:#}").contains(&runner.display().to_string()));
    }
}
