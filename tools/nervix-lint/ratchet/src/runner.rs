//! Repository tooling, outside the product layer order.
//! Owns: one worktree's compiler identity, complete target coverage and evidence reuse.
//! Depends on: typed Cargo/report evidence, process orchestration and current declarations.
//! Must not know: product graph policy or manually maintained source findings.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use error_stack::{Report, ResultExt as _};
use nervix_lint_report::{CompilerReport, origin::SourceOrigins};

use crate::{
    admission::{Compiled, Policy, target_kind},
    configuration::{Configuration, Declaration, TOOLCHAIN},
    error::{Error, Issue, Result},
    evidence::{CargoArtifact, CargoMessage, ExpectedTarget, Measurement, Metadata},
    identity,
    identity::Identity,
    io, process,
};

pub struct Runner {
    pub root: PathBuf,
    pub target: PathBuf,
    pub driver: PathBuf,
    pub wrapper: PathBuf,
    pub policy: Policy,
    pub compiler: String,
    pub identity: String,
    pub inputs: BTreeMap<String, String>,
    pub environment: BTreeMap<String, String>,
    pub work: PathBuf,
}

impl Runner {
    pub fn new(root: &Path, target: &Path, inventory: bool) -> Result<Self> {
        // Commands inherit the OS environment. Unrelated non-text values must not panic.
        let mut environment = BTreeMap::new();
        for (key, value) in std::env::vars_os() {
            if let (Ok(key), Ok(value)) = (key.into_string(), value.into_string()) {
                environment.insert(key, value);
            }
        }
        Self::from_environment(root, target, inventory, environment)
    }

    pub(crate) fn from_environment(
        root: &Path,
        target: &Path,
        inventory: bool,
        mut environment: BTreeMap<String, String>,
    ) -> Result<Self> {
        let root = fs::canonicalize(root).change_context(Error::Io {
            operation: "resolve worktree",
            path: root.into(),
        })?;
        fs::create_dir_all(target).change_context(Error::Io {
            operation: "create target directory",
            path: target.into(),
        })?;
        let target = fs::canonicalize(target).change_context(Error::Io {
            operation: "resolve target directory",
            path: target.into(),
        })?;
        let driver = target.join("typed-ratchet/driver/debug/nervix-lint-driver");
        if !driver.is_file() {
            return Err(Report::new(Error::MissingDriver { path: driver }));
        }
        let wrapper = std::env::current_exe().change_context(Error::Io {
            operation: "locate runner",
            path: "nervix-ratchet".into(),
        })?;
        let policy = Declaration::load(&root)?.policy(&root)?;
        let compiler = process::text(
            "rustup",
            &["run".into(), TOOLCHAIN.into(), "rustc".into(), "-vV".into()],
            &root,
            &environment,
        )?;
        let inputs = identity::source_inputs(&root)?;
        environment.insert(
            "NERVIX_LINT_MODE".into(),
            if inventory { "inventory" } else { "gate" }.into(),
        );
        environment.insert("NERVIX_RATCHET_ROOT".into(), root.display().to_string());
        environment.insert("CARGO_TARGET_DIR".into(), target.display().to_string());
        if environment
            .get("NERVIX_NATIVE_COVERAGE_ATTEMPT")
            .is_some_and(|value| !value.is_empty())
        {
            // The runner's collected runtime profile belongs to this process. Cargo build
            // scripts keep the producer's build-time path; wrapper and driver collect explicitly.
            environment.insert("LLVM_PROFILE_FILE".into(), "/dev/null".into());
        }
        if environment
            .get("RUSTC_WORKSPACE_WRAPPER")
            .is_some_and(|value| !value.is_empty())
        {
            return Err(Report::new(Error::WorkspaceWrapper));
        }
        let sysroot = process::text(
            "rustup",
            &[
                "run".into(),
                TOOLCHAIN.into(),
                "rustc".into(),
                "--print".into(),
                "sysroot".into(),
            ],
            &root,
            &environment,
        )?;
        let library = Path::new(sysroot.trim()).join("lib");
        let library_path = match environment.get("LD_LIBRARY_PATH") {
            Some(previous) if !previous.is_empty() => format!("{}:{previous}", library.display()),
            _ => library.display().to_string(),
        };
        environment.insert("LD_LIBRARY_PATH".into(), library_path);
        let payload = Identity {
            mode: environment["NERVIX_LINT_MODE"].clone(),
            root: root.clone(),
            compiler: compiler.clone(),
            driver: io::digest(&driver)?,
            workspace_wrapper: io::digest(&wrapper)?,
            inputs: inputs.clone(),
            cargo_configuration: identity::cargo_configuration(&root, &environment)?,
            environment: identity::compiler_environment(&environment),
        };
        let identity = io::fingerprint(&payload)?;
        let work = target.join("typed-ratchet/evidence").join(&identity);
        io::atomic_json(&work.join("identity.json"), &payload)?;
        Ok(Self {
            root,
            target,
            driver,
            wrapper,
            policy,
            compiler,
            identity,
            inputs,
            environment,
            work,
        })
    }

    pub fn failure(
        &self,
        configuration: &Configuration,
        issue: Issue,
        path: &Path,
    ) -> Report<Error> {
        Report::new(Error::Evidence {
            configuration: configuration.name.clone(),
            issue,
            path: path.into(),
        })
    }

    pub fn expected_targets(
        &self,
        configuration: &Configuration,
    ) -> Result<BTreeMap<String, ExpectedTarget>> {
        let args = vec![
            format!("+{TOOLCHAIN}"),
            "metadata".into(),
            "--format-version".into(),
            "1".into(),
            "--no-deps".into(),
            "--manifest-path".into(),
            configuration.manifest.clone(),
        ];
        let text = process::text("cargo", &args, &self.root, &self.environment)?;
        let metadata: Metadata = serde_json::from_str(&text).change_context(Error::Json {
            path: "Cargo metadata".into(),
        })?;
        let members: BTreeSet<_> = metadata.workspace_members.into_iter().collect();
        let requested: BTreeSet<_> = configuration.packages.iter().collect();
        let mut selected = BTreeSet::new();
        let kinds: BTreeSet<_> = configuration.kinds.iter().map(String::as_str).collect();
        let mut targets = BTreeMap::new();
        for package in metadata.packages {
            if !members.contains(&package.id) {
                continue;
            }
            let required = requested.is_empty() || requested.contains(&package.name);
            if required {
                selected.insert(package.name.clone());
            }
            for target in package.targets {
                let kind = target_kind(&target.kind);
                if !kinds.contains(kind) || (!required && kind != "lib") {
                    continue;
                }
                let key = format!("{}::{}::{kind}", package.id, target.name);
                targets.insert(
                    key,
                    ExpectedTarget {
                        package: package.name.clone(),
                        source: target.src_path,
                        kind: kind.into(),
                        required,
                    },
                );
            }
        }
        let mut missing_packages = Vec::new();
        for name in requested {
            if !selected.contains(name) {
                missing_packages.push(name);
            }
        }
        if !missing_packages.is_empty() {
            return Err(self
                .failure(
                    configuration,
                    Issue::Packages,
                    Path::new(&configuration.manifest),
                )
                .attach(format!("unknown requested packages: {missing_packages:?}")));
        }
        if targets.is_empty() {
            return Err(self.failure(
                configuration,
                Issue::NoTargets,
                Path::new(&configuration.manifest),
            ));
        }
        Ok(targets)
    }

    pub fn check_diagnostics(
        &self,
        configuration: &Configuration,
        messages: &[CargoMessage],
    ) -> Result<()> {
        if self
            .environment
            .get("NERVIX_LINT_MODE")
            .is_some_and(|mode| mode == "inventory")
        {
            return Ok(());
        }
        for message in messages {
            if let CargoMessage::CompilerMessage { message, .. } = message
                && message
                    .code
                    .as_ref()
                    .is_some_and(|code| code.code.starts_with("nervix::"))
            {
                return Err(self.failure(
                    configuration,
                    Issue::Diagnostics,
                    Path::new(&configuration.manifest),
                ));
            }
        }
        Ok(())
    }

    pub fn validate_reports(
        &self,
        configuration: &Configuration,
        expected: &BTreeMap<String, ExpectedTarget>,
        messages: &[CargoMessage],
        reports: &Path,
    ) -> Result<Validated> {
        self.check_diagnostics(configuration, messages)?;
        let mut covered = BTreeSet::new();
        let mut accepted = BTreeMap::new();
        let mut files = BTreeMap::new();
        let mut sources = BTreeMap::new();
        for message in messages {
            let CargoMessage::CompilerArtifact(artifact) = message else {
                continue;
            };
            let key = artifact.key();
            if !expected.contains_key(&key) {
                continue;
            }
            covered.insert(key.clone());
            for path in &artifact.filenames {
                if !path.is_file() {
                    return Err(self.failure(configuration, Issue::Artifact, path));
                }
                files.insert(path.clone(), io::digest(path)?);
            }
            let candidates = report_candidates(artifact, reports);
            if candidates.len() != 1 {
                return Err(self
                    .failure(configuration, Issue::Report, reports)
                    .attach(format!("target: {key}")));
            }
            let path = &candidates[0];
            let mut report: CompilerReport = io::read(path)?;
            self.validate_report(configuration, artifact, &report, path)?;
            for finding in &mut report.findings {
                let site = &finding.span.site;
                let source_path = self.root.join(&site.path);
                if !sources.contains_key(&site.path) {
                    let source = fs::read_to_string(&source_path).change_context(Error::Io {
                        operation: "read authored source",
                        path: source_path.clone(),
                    })?;
                    let origins = SourceOrigins::parse(&site.path, &source).change_context(
                        Error::Evidence {
                            configuration: configuration.name.clone(),
                            issue: Issue::Source,
                            path: source_path,
                        },
                    )?;
                    sources.insert(site.path.clone(), origins);
                }
                for origin in sources[&site.path].at(usize::try_from(site.start).change_context(
                    Error::Evidence {
                        configuration: configuration.name.clone(),
                        issue: Issue::Source,
                        path: path.into(),
                    },
                )?) {
                    if !finding.expansion.contains(&origin) {
                        finding.expansion.push(origin);
                    }
                }
            }
            accepted.insert(key, report);
            files.insert(path.clone(), io::digest(path)?);
        }
        let mut missing_targets = Vec::new();
        for (key, target) in expected {
            if target.required && !covered.contains(key) {
                missing_targets.push(key);
            }
        }
        if !missing_targets.is_empty() {
            return Err(self
                .failure(configuration, Issue::Targets, reports)
                .attach(format!("missing targets: {missing_targets:?}")));
        }
        Ok(Validated {
            reports: accepted,
            files,
        })
    }

    fn validate_report(
        &self,
        configuration: &Configuration,
        artifact: &CargoArtifact,
        report: &CompilerReport,
        path: &Path,
    ) -> Result<()> {
        if !report.complete || report.identity != self.identity {
            return Err(self.failure(configuration, Issue::Identity, path));
        }
        if report.compiler != self.compiler || report.configuration != configuration.name {
            return Err(self.failure(configuration, Issue::Compiler, path));
        }
        if report.crate_name != artifact.crate_name() {
            return Err(self.failure(configuration, Issue::Crate, path));
        }
        let source =
            fs::canonicalize(self.root.join(&report.crate_source)).change_context(Error::Io {
                operation: "resolve reported source",
                path: report.crate_source.clone().into(),
            })?;
        let expected = fs::canonicalize(&artifact.target.src_path).change_context(Error::Io {
            operation: "resolve Cargo source",
            path: artifact.target.src_path.clone(),
        })?;
        if source != expected {
            return Err(self.failure(configuration, Issue::Source, path));
        }
        Ok(())
    }

    pub fn measure_compilations(
        &self,
        configuration: &Configuration,
        expected: &BTreeMap<String, ExpectedTarget>,
        messages: &[CargoMessage],
        reports: &Path,
    ) -> Result<BTreeMap<String, Measurement>> {
        let records = reports.join("compilations");
        let declaration = self.root.join("tools/nervix-lint/configurations.toml");
        let mut measured = BTreeMap::new();
        let reservations = self.policy.keyed_reservations();
        for message in messages {
            let CargoMessage::CompilerArtifact(artifact) = message else {
                continue;
            };
            if artifact.fresh {
                continue;
            }
            let key = artifact.key();
            let name = artifact.crate_name();
            let kind = target_kind(&artifact.target.kind);
            let candidates = report_candidates(artifact, &records);
            if candidates.len() > 1 || (candidates.is_empty() && expected.contains_key(&key)) {
                return Err(self
                    .failure(configuration, Issue::Measurement, &records)
                    .attach(format!("target: {key}")));
            }
            let Some(path) = candidates.first() else {
                continue;
            };
            let record: crate::evidence::Compilation = io::read(path)?;
            if !record.ending.succeeded() {
                return Err(self
                    .failure(configuration, Issue::FailedMeasurement, path)
                    .attach(format!("target: {key}")));
            }
            let compiled = Compiled {
                crate_name: name.clone(),
                kind: kind.into(),
            };
            if let Some(reservation) = reservations.get(&compiled) {
                if record.peak_resident_bytes > *reservation {
                    return Err(self
                        .failure(configuration, Issue::Reservation, &declaration)
                        .attach(format!(
                            "{name} ({kind}) peaked at {} bytes, reservation {reservation}",
                            record.peak_resident_bytes
                        )));
                }
            } else if record.peak_resident_bytes > self.policy.threshold_bytes {
                return Err(self
                    .failure(configuration, Issue::Threshold, &declaration)
                    .attach(format!(
                        "{name} ({kind}) peaked at {} bytes, threshold {}",
                        record.peak_resident_bytes, self.policy.threshold_bytes
                    )));
            }
            measured.insert(
                key,
                Measurement {
                    crate_name: name,
                    kind: kind.into(),
                    peak_resident_bytes: record.peak_resident_bytes,
                    started_unix_ns: record.started_unix_ns,
                    seconds: record.seconds,
                    admission: record.admission,
                },
            );
        }
        Ok(measured)
    }

    pub fn clean_authored_artifacts(
        &self,
        configuration: &Configuration,
        expected: &BTreeMap<String, ExpectedTarget>,
        build: &Path,
    ) -> Result<()> {
        let packages: BTreeSet<_> = expected.values().map(|target| &target.package).collect();
        let mut arguments = vec![
            format!("+{TOOLCHAIN}"),
            "clean".into(),
            "--manifest-path".into(),
            configuration.manifest.clone(),
            "--target-dir".into(),
            build.to_string_lossy().into_owned(),
        ];
        if let Some(target) = &configuration.target {
            arguments.extend(["--target".into(), target.clone()]);
        }
        for package in packages {
            arguments.extend(["--package".into(), package.clone()]);
        }
        process::output("cargo", &arguments, &self.root, &self.environment)?;
        Ok(())
    }

    pub fn inputs_current(&self) -> Result<()> {
        if identity::source_inputs(&self.root)? != self.inputs {
            return Err(Report::new(Error::SourceChanged));
        }
        Ok(())
    }
}

pub struct Validated {
    pub reports: BTreeMap<String, CompilerReport>,
    pub files: BTreeMap<PathBuf, String>,
}

pub fn report_candidates(artifact: &CargoArtifact, reports: &Path) -> Vec<PathBuf> {
    let name = artifact.crate_name();
    let mut candidates = BTreeSet::new();
    for artifact in &artifact.filenames {
        let Some(filename) = artifact.file_name() else {
            continue;
        };
        let filename = filename.to_string_lossy();
        let stem = filename.split('.').next().unwrap_or("");
        let stem = stem.strip_prefix("lib").unwrap_or(stem);
        if stem.starts_with(&format!("{name}-")) {
            let candidate = reports.join(format!("{stem}.json"));
            if candidate.is_file() {
                candidates.insert(candidate);
            }
        }
    }
    candidates.into_iter().collect()
}

#[cfg(test)]
mod tests;
