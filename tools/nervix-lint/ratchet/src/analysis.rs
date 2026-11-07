//! Repository tooling, outside the product layer order.
//! Owns: a configuration's Cargo process, report re-establishment and completion publication.
//! Depends on: the runner's validated inputs and typed compiler evidence.
//! Must not know: product execution or the order of other configurations.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use error_stack::ResultExt as _;
use nervix_primitives::thread::sleep;

use crate::{
    configuration::{Configuration, TOOLCHAIN},
    error::{Error, Issue, Result},
    evidence::{CargoMessage, Completion, ExpectedTarget},
    io, process,
    runner::Runner,
};

#[cfg(test)]
mod tests;

pub enum Preparation {
    Complete(Completion),
    Running(Analysis),
}

pub struct Analysis {
    pub configuration: Configuration,
    expected: BTreeMap<String, ExpectedTarget>,
    directory: PathBuf,
    completion: PathBuf,
    build: PathBuf,
    reports: PathBuf,
    salt: String,
    log: PathBuf,
    child: Child,
    reestablished: bool,
}

impl Runner {
    pub fn analyze_one(&self, configuration: &Configuration, fresh: bool) -> Result<Completion> {
        match self.prepare(configuration, fresh, false)? {
            Preparation::Complete(evidence) => Ok(evidence),
            Preparation::Running(mut analysis) => loop {
                if let Some(evidence) = analysis.poll(self)? {
                    return Ok(evidence);
                }
                sleep(Duration::from_millis(10));
            },
        }
    }

    pub fn prepare(
        &self,
        configuration: &Configuration,
        fresh: bool,
        recompile: bool,
    ) -> Result<Preparation> {
        let expected = self.expected_targets(configuration)?;
        let directory = self.work.join(&configuration.name);
        fs::create_dir_all(&directory).change_context(Error::Io {
            operation: "create analysis directory",
            path: directory.clone(),
        })?;
        let completion = directory.join("completion.json");
        let mut previous = None;
        if completion.is_file() {
            let evidence: Completion = io::read(&completion)?;
            if evidence.configuration != *configuration {
                return Err(self.failure(configuration, Issue::Completion, &completion));
            }
            if evidence.complete && evidence.expected == expected && !evidence.files.is_empty() {
                let mut unchanged = true;
                for (path, digest) in &evidence.files {
                    if !path.is_file() || io::digest(path)? != *digest {
                        unchanged = false;
                        break;
                    }
                }
                if unchanged {
                    let messages = process::messages(&evidence.cargo_log)?;
                    let validated = self.validate_reports(
                        configuration,
                        &expected,
                        &messages,
                        &evidence.report_directory,
                    )?;
                    if validated.reports != evidence.reports {
                        return Err(self.failure(configuration, Issue::Completion, &completion));
                    }
                    if !fresh && !recompile {
                        return Ok(Preparation::Complete(evidence));
                    }
                    previous = Some(evidence);
                }
            }
        }
        io::remove_if_present(&completion)?;
        let prefix = match self.environment.get("KACHE_KEY_SALT") {
            Some(prefix) => prefix.as_str(),
            None => "",
        };
        let mut salt = format!("{prefix}:nervix-analysis:{}", self.identity);
        let mut build = self
            .target
            .join("typed-ratchet/build")
            .join(&configuration.name);
        let mut reports = directory.join("reports");
        if recompile {
            self.clean_authored_artifacts(configuration, &expected, &build)?;
        }
        if let Some(evidence) = previous {
            build = evidence.build_directory;
            reports = evidence.report_directory;
            salt = evidence.cache_namespace;
        }
        let (child, log) = self.start_cargo(configuration, &build, &reports, &salt)?;
        Ok(Preparation::Running(Analysis {
            configuration: configuration.clone(),
            expected,
            directory,
            completion,
            build,
            reports,
            salt,
            log,
            child,
            reestablished: false,
        }))
    }

    pub(crate) fn start_cargo(
        &self,
        configuration: &Configuration,
        build: &Path,
        reports: &Path,
        salt: &str,
    ) -> Result<(Child, PathBuf)> {
        fs::create_dir_all(reports).change_context(Error::Io {
            operation: "create report directory",
            path: reports.into(),
        })?;
        let records = reports.join("compilations");
        if records.is_dir() {
            for entry in fs::read_dir(&records).change_context(Error::Io {
                operation: "read compilation records",
                path: records.clone(),
            })? {
                let path = entry
                    .change_context(Error::Io {
                        operation: "read compilation record",
                        path: records.clone(),
                    })?
                    .path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "json")
                {
                    io::remove_if_present(&path)?;
                }
            }
        }
        let mut environment = self.environment.clone();
        environment.extend([
            ("CARGO_TARGET_DIR".into(), build.display().to_string()),
            (
                "RUSTC_WORKSPACE_WRAPPER".into(),
                self.wrapper.display().to_string(),
            ),
            (
                "NERVIX_LINT_DRIVER".into(),
                self.driver.display().to_string(),
            ),
            ("NERVIX_LINT_ROOT".into(), self.root.display().to_string()),
            ("NERVIX_LINT_IDENTITY".into(), self.identity.clone()),
            (
                "NERVIX_LINT_CONFIGURATION".into(),
                configuration.name.clone(),
            ),
            ("NERVIX_LINT_REPORTS".into(), reports.display().to_string()),
            (
                "NERVIX_LINT_ADMISSION".into(),
                serde_json::to_string(&self.policy).change_context(Error::Json {
                    path: "admission policy".into(),
                })?,
            ),
            ("KACHE_KEY_SALT".into(), salt.into()),
        ]);
        let mut arguments = vec![format!("+{TOOLCHAIN}"), "check".into()];
        arguments.extend(configuration.cargo_arguments());
        arguments.push("--message-format=json".into());
        let log = reports
            .parent()
            .ok_or_else(|| self.failure(configuration, Issue::Report, reports))?
            .join("cargo.jsonl");
        let output = File::create(&log).change_context(Error::Io {
            operation: "create Cargo log",
            path: log.clone(),
        })?;
        println!(
            "typed ratchet: {}: cargo {}",
            configuration.name,
            arguments.join(" ")
        );
        let child = Command::new("cargo")
            .args(arguments)
            .current_dir(&self.root)
            .envs(&environment)
            .stdout(Stdio::from(output))
            .spawn()
            .change_context(Error::Io {
                operation: "start compiler analysis",
                path: "cargo".into(),
            })?;
        Ok((child, log))
    }
}

impl Analysis {
    pub fn poll(&mut self, runner: &Runner) -> Result<Option<Completion>> {
        let Some(status) = self.child.try_wait().change_context(Error::Io {
            operation: "wait for compiler analysis",
            path: self.log.clone(),
        })?
        else {
            return Ok(None);
        };
        let messages = process::messages(&self.log)?;
        for message in &messages {
            if let CargoMessage::CompilerMessage { message, .. } = message
                && message.level == "error"
            {
                eprintln!(
                    "{}",
                    message.rendered.as_deref().unwrap_or(&message.message)
                );
            }
        }
        if !status.success()
            || !messages
                .iter()
                .any(|message| matches!(message, CargoMessage::BuildFinished { success: true }))
        {
            crate::measurement::print_failures(&messages, &self.reports)?;
            return Err(runner.failure(&self.configuration, Issue::Compilation, &self.log));
        }
        runner.check_diagnostics(&self.configuration, &messages)?;
        let validated = match runner.validate_reports(
            &self.configuration,
            &self.expected,
            &messages,
            &self.reports,
        ) {
            Ok(validated) => validated,
            Err(report) if !self.reestablished => {
                self.reestablished = true;
                let attempt = uuid::Uuid::new_v4().simple().to_string();
                println!(
                    "typed ratchet: {report:?}; re-establishing compiler analysis in cache \
                     namespace {attempt}"
                );
                runner.clean_authored_artifacts(
                    &self.configuration,
                    &self.expected,
                    &self.build,
                )?;
                self.reports = self
                    .directory
                    .join("reanalysis")
                    .join(&attempt)
                    .join("reports");
                self.salt.push(':');
                self.salt.push_str(&attempt);
                let (child, log) = runner.start_cargo(
                    &self.configuration,
                    &self.build,
                    &self.reports,
                    &self.salt,
                )?;
                self.child = child;
                self.log = log;
                return Ok(None);
            }
            Err(report) => return Err(report),
        };
        let compilations = match runner.measure_compilations(
            &self.configuration,
            &self.expected,
            &messages,
            &self.reports,
        ) {
            Ok(compilations) => compilations,
            Err(error) => {
                runner.clean_authored_artifacts(
                    &self.configuration,
                    &self.expected,
                    &self.build,
                )?;
                return Err(error);
            }
        };
        runner.inputs_current()?;
        let mut files = validated.files;
        files.insert(self.log.clone(), io::digest(&self.log)?);
        let evidence = Completion {
            configuration: self.configuration.clone(),
            expected: self.expected.clone(),
            reports: validated.reports,
            files,
            complete: true,
            build_directory: self.build.clone(),
            report_directory: self.reports.clone(),
            cache_namespace: self.salt.clone(),
            cargo_log: self.log.clone(),
            compilations,
        };
        io::atomic_json(&self.completion, &evidence)?;
        let required = evidence
            .expected
            .values()
            .filter(|target| target.required)
            .count();
        println!(
            "typed ratchet: {}: {required} required targets and {} observed workspace targets \
             complete",
            self.configuration.name,
            evidence.reports.len()
        );
        for line in crate::measurement::lines(&self.configuration, &evidence.compilations) {
            println!("{line}");
        }
        Ok(Some(evidence))
    }
}
