//! Repository qualification tooling, outside the product layer order.
//! Owns: compiling semantic fixtures before assertion-only Rust tests read their evidence.
//! Depends on: the actual wrapper/driver, complete fixture evidence and current sources.
//! Must not know: product execution or expected diagnostic verdicts.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use error_stack::{Report, ResultExt as _};

use crate::{
    configuration::{Configuration, TOOLCHAIN, TOOLING},
    error::{Error, Issue, Result},
    evidence::MatrixReport,
    io, process,
    runner::Runner,
};

#[derive(clap::Args)]
pub struct PrepareContracts {
    #[arg(long)]
    pub root: PathBuf,
    #[arg(long)]
    pub target_dir: PathBuf,
    #[arg(long, default_value = "ordinary", value_parser = ["ordinary", "shuttle", "loom", "turmoil", "deloxide"])]
    pub mode: String,
}

pub struct ContractCompiler {
    runner: Runner,
    compiler: String,
    mode: String,
    fixture: MatrixReport,
    dependencies: BTreeSet<PathBuf>,
    metadata: PathBuf,
}

impl ContractCompiler {
    pub fn new(root: &Path, target: &Path, mode: &str) -> Result<Self> {
        let runner = Runner::new(root, target, false)?;
        let compiler = process::text(
            "rustup",
            &[
                "which".into(),
                "--toolchain".into(),
                TOOLCHAIN.into(),
                "rustc".into(),
            ],
            root,
            &runner.environment,
        )?
        .trim()
        .to_owned();
        let fixture_path = target.join(format!("typed-ratchet/fixture-{mode}.json"));
        let fixture: MatrixReport = io::read(&fixture_path)?;
        if !fixture.complete
            || fixture.root != runner.root
            || fixture.compiler != runner.compiler
            || fixture.source_identity != io::fingerprint(&runner.inputs)?
            || fixture.configurations != [Configuration::fixture(mode)]
        {
            return Err(Report::new(Error::Evidence {
                configuration: format!("fixture-{mode}"),
                issue: Issue::Completion,
                path: fixture_path,
            }));
        }
        let mut metadata = None;
        let mut dependencies = BTreeSet::new();
        for evidence in &fixture.evidence {
            for path in evidence.files.keys() {
                if path.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .starts_with("libnervix_lint_fixtures-")
                }) && path
                    .extension()
                    .is_some_and(|extension| extension == "rmeta")
                {
                    metadata = Some(path.clone());
                }
            }
            // Cargo keeps each library among its build unit's results, so every directory holding
            // one is a dependency search path.
            let debug = evidence.build_directory.join("debug");
            for entry in walkdir::WalkDir::new(&debug) {
                let entry = entry.change_context(Error::Io {
                    operation: "discover fixture dependencies",
                    path: debug.clone(),
                })?;
                if entry.file_type().is_file()
                    && entry.file_name().to_string_lossy().starts_with("lib")
                    && entry
                        .path()
                        .extension()
                        .is_some_and(|extension| extension == "rmeta" || extension == "so")
                    && let Some(parent) = entry.path().parent()
                {
                    dependencies.insert(parent.into());
                }
            }
        }
        let metadata = metadata.ok_or_else(|| {
            Report::new(Error::Evidence {
                configuration: format!("fixture-{mode}"),
                issue: Issue::Artifact,
                path: fixture_path,
            })
        })?;
        Ok(Self {
            runner,
            compiler,
            mode: mode.into(),
            fixture,
            dependencies,
            metadata,
        })
    }

    pub fn directory(&self, name: &str) -> PathBuf {
        let name = if self.mode == "ordinary" {
            name.to_owned()
        } else {
            format!("{name}-{}", self.mode)
        };
        self.runner
            .target
            .join("typed-ratchet/contracts")
            .join(name)
    }

    pub fn compile(
        &self,
        name: &str,
        crate_name: &str,
        source: &Path,
        source_root: &Path,
        arguments: &[String],
    ) -> Result<()> {
        let output = self.directory(name);
        fs::create_dir_all(&output).change_context(Error::Io {
            operation: "create contract output",
            path: output.clone(),
        })?;
        let mut environment = self.runner.environment.clone();
        environment.extend([
            ("NERVIX_LINT_IDENTITY".into(), self.runner.identity.clone()),
            ("NERVIX_LINT_ROOT".into(), source_root.display().to_string()),
            (
                "NERVIX_LINT_CONFIGURATION".into(),
                "compiler-contract".into(),
            ),
            ("NERVIX_LINT_REPORTS".into(), output.display().to_string()),
            (
                "NERVIX_LINT_DRIVER".into(),
                self.runner.driver.display().to_string(),
            ),
            (
                "NERVIX_LINT_ADMISSION".into(),
                serde_json::to_string(&self.runner.policy).change_context(Error::Json {
                    path: "admission policy".into(),
                })?,
            ),
            (
                "KACHE_KEY_SALT".into(),
                format!("{}:contract:{name}:{}", self.runner.identity, self.mode),
            ),
        ]);
        let wrapper = environment
            .get("RUSTC_WRAPPER")
            .map(String::as_str)
            .unwrap_or("kache");
        let mut command = Command::new(wrapper);
        command
            .arg(&self.runner.wrapper)
            .arg(&self.compiler)
            .args([
                "--edition=2024",
                "--crate-name",
                crate_name,
                "--crate-type=lib",
                "--emit=metadata",
                "--error-format=json",
                "--out-dir",
            ])
            .arg(&output)
            .arg(source)
            .args(arguments)
            .arg("--extern")
            .arg(format!("nervix_lint_fixtures={}", self.metadata.display()));
        for directory in &self.dependencies {
            command
                .arg("-L")
                .arg(format!("dependency={}", directory.display()));
        }
        for evidence in &self.fixture.evidence {
            for report in evidence.reports.values() {
                for pair in report.arguments.windows(2) {
                    if pair[0] == "-L" {
                        command.arg("-L").arg(&pair[1]);
                    }
                }
            }
        }
        let result = command
            .envs(environment)
            .current_dir(&self.runner.root)
            .output()
            .change_context(Error::Io {
                operation: "compile semantic fixture",
                path: source.into(),
            })?;
        fs::write(output.join("stderr"), result.stderr).change_context(Error::Io {
            operation: "record fixture diagnostics",
            path: output.clone(),
        })?;
        fs::write(output.join("stdout"), result.stdout).change_context(Error::Io {
            operation: "record fixture output",
            path: output.clone(),
        })?;
        io::atomic_json(
            &output.join("ending.json"),
            &crate::wrapper::ending(result.status),
        )
    }

    fn semantic_source(&self, name: &str) -> PathBuf {
        self.runner
            .root
            .join(TOOLING)
            .join("fixtures/tests/semantics")
            .join(format!("{name}.rs"))
    }

    pub fn prepare(&self) -> Result<()> {
        if self.mode != "ordinary" {
            for name in ["error_replacement", "error_replacement_allowed"] {
                self.compile(
                    name,
                    "contract",
                    &self.semantic_source(name),
                    &self.runner.root,
                    &[],
                )?;
            }
            return Ok(());
        }
        let callees = [
            "architecture_metadata",
            "architecture_foreign",
            "error_replacement_metadata",
            "error_metadata_callee",
            "error_metadata_failure",
            "cross_crate_callee",
        ];
        for name in callees {
            self.compile(
                name,
                "callee",
                &self.semantic_source(name),
                &self.runner.root,
                &[],
            )?;
        }
        let callees: BTreeSet<_> = callees.into_iter().collect();
        let mut sources = BTreeMap::new();
        let directory = self
            .runner
            .root
            .join(TOOLING)
            .join("fixtures/tests/semantics");
        for entry in fs::read_dir(&directory).change_context(Error::Io {
            operation: "discover semantic fixtures",
            path: directory.clone(),
        })? {
            let path = entry
                .change_context(Error::Io {
                    operation: "read semantic fixture",
                    path: directory.clone(),
                })?
                .path();
            if path.extension().is_some_and(|extension| extension == "rs")
                && let Some(name) = path.file_stem().and_then(|name| name.to_str())
            {
                sources.insert(name.to_owned(), path);
            }
        }
        for (name, source) in sources {
            if callees.contains(name.as_str()) {
                continue;
            }
            let callee = match name.as_str() {
                "architecture_metadata_use" => Some("architecture_metadata"),
                "architecture_foreign_use" => Some("architecture_foreign"),
                "error_replacement_metadata_allowed" | "error_replacement_metadata_rejected" => {
                    Some("error_replacement_metadata")
                }
                "error_metadata_caller" => Some("error_metadata_callee"),
                "error_metadata_rejected" => Some("error_metadata_failure"),
                "cross_crate_caller" | "cross_crate_expected" => Some("cross_crate_callee"),
                _ => None,
            };
            let mut arguments = Vec::new();
            if let Some(callee) = callee {
                arguments.push(format!(
                    "--extern=callee={}",
                    self.directory(callee).join("libcallee.rmeta").display()
                ));
            }
            if name == "architecture_metadata_use" {
                let moved = self.directory("architecture_metadata_source");
                fs::create_dir_all(&moved).change_context(Error::Io {
                    operation: "create relocated fixture",
                    path: moved.clone(),
                })?;
                let relocated = moved.join("relocated.rs");
                fs::copy(&source, &relocated).change_context(Error::Io {
                    operation: "relocate fixture source",
                    path: relocated.clone(),
                })?;
                self.compile(&name, "contract", &relocated, &moved, &arguments)?;
            } else {
                self.compile(&name, "contract", &source, &self.runner.root, &arguments)?;
            }
        }
        for (name, level) in [
            ("recurring_warn", "-Wnervix::sync_acquisition"),
            ("recurring_deny", "-Dnervix::sync_acquisition"),
            ("recurring_allow", "-Anervix::sync_acquisition"),
            ("warnings_allow", "-Awarnings"),
            ("cap_allow", "--cap-lints=allow"),
            ("cap_warn", "--cap-lints=warn"),
        ] {
            self.compile(
                name,
                "contract",
                &self.semantic_source("recurring_lock"),
                &self.runner.root,
                &[level.into()],
            )?;
        }
        self.prepare_changes()?;
        self.prepare_stable()
    }

    fn prepare_changes(&self) -> Result<()> {
        let directory = self.directory("changed_sources");
        fs::create_dir_all(&directory).change_context(Error::Io {
            operation: "create changing-source fixture",
            path: directory.clone(),
        })?;
        let source = directory.join("owner.rs");
        let cold = "#![cfg_attr(nervix_lint, nervix::context(lifecycle, reason = \"installation \
                    default\"))]\npub fn helper(map: &nervix_lint_fixtures::MapAlias) { \
                    drop(map.get(&1)); }\npub fn caller(map: &nervix_lint_fixtures::MapAlias) { \
                    helper(map); }\n";
        fs::write(&source, cold).change_context(Error::Io {
            operation: "write cold fixture",
            path: source.clone(),
        })?;
        self.compile("changed_source_cold", "contract", &source, &directory, &[])?;
        let hot = cold.replace(
            "pub fn caller",
            "#[cfg_attr(nervix_lint, nervix::context(recurring, reason = \"handles each \
             batch\"))]\npub fn caller",
        );
        fs::write(&source, &hot).change_context(Error::Io {
            operation: "change caller contract",
            path: source.clone(),
        })?;
        self.compile("changed_source_hot", "contract", &source, &directory, &[])?;
        let moved = directory.join("relocated.rs");
        fs::write(&moved, hot.replace("helper", "renamed_helper")).change_context(Error::Io {
            operation: "move current fixture",
            path: moved.clone(),
        })?;
        self.compile("changed_source_moved", "contract", &moved, &directory, &[])?;
        let callee = directory.join("callee.rs");
        let declaration = "#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = \
                           \"publishes a lifetime\"))]\npub fn install() {}\n";
        fs::write(&callee, declaration).change_context(Error::Io {
            operation: "write callee fixture",
            path: callee.clone(),
        })?;
        self.compile("changed_callee", "changed_callee", &callee, &directory, &[])?;
        let caller = directory.join("caller.rs");
        fs::write(
            &caller,
            "#[cfg_attr(nervix_lint, nervix::context(recurring, reason = \"handles each \
             batch\"))]\npub fn run() { use changed_callee::install as renamed; renamed(); }\n",
        )
        .change_context(Error::Io {
            operation: "write caller fixture",
            path: caller.clone(),
        })?;
        let argument = format!(
            "--extern=changed_callee={}",
            self.directory("changed_callee")
                .join("libchanged_callee.rmeta")
                .display()
        );
        self.compile(
            "changed_caller_refused",
            "contract",
            &caller,
            &directory,
            std::slice::from_ref(&argument),
        )?;
        fs::write(&callee, declaration.replace("lifecycle", "recurring")).change_context(
            Error::Io {
                operation: "change callee contract",
                path: callee.clone(),
            },
        )?;
        self.compile("changed_callee", "changed_callee", &callee, &directory, &[])?;
        self.compile(
            "changed_caller_current",
            "contract",
            &caller,
            &directory,
            &[argument],
        )
    }

    fn prepare_stable(&self) -> Result<()> {
        let directory = self.directory("stable_contracts");
        fs::create_dir_all(&directory).change_context(Error::Io {
            operation: "create stable fixture",
            path: directory.clone(),
        })?;
        let source = fs::read_to_string(self.semantic_source("expected_expression"))
            .change_context(Error::Io {
                operation: "read stable fixture",
                path: directory.clone(),
            })?;
        let source = source
            .replace(
                "nervix_lint_fixtures::MapAlias",
                "std::collections::BTreeMap<u32, u32>",
            )
            .replace("nervix_lint_fixtures::expect_lint!", "expect_lint!")
            .replace("drop(value);", "assert!(value.is_none());")
            .replace(
                "//! A single acquisition expression carries its own reviewed exception.",
                "",
            );
        let macros = fs::read_to_string(self.runner.root.join("crates/primitives/src/lint.rs"))
            .change_context(Error::Io {
                operation: "read stable contract macros",
                path: directory.clone(),
            })?;
        let path = directory.join("contract.rs");
        fs::write(&path, format!("{macros}\n{source}")).change_context(Error::Io {
            operation: "write stable contract fixture",
            path: path.clone(),
        })?;
        let compiler = process::text(
            "rustup",
            &[
                "which".into(),
                "--toolchain".into(),
                "1.100".into(),
                "rustc".into(),
            ],
            &self.runner.root,
            &self.runner.environment,
        )?;
        let wrapper = self
            .runner
            .environment
            .get("RUSTC_WRAPPER")
            .map(String::as_str)
            .unwrap_or("kache");
        let result = Command::new(wrapper)
            .arg(compiler.trim())
            .args([
                "--edition=2024",
                "--crate-type=lib",
                "--emit=metadata",
                "-Dwarnings",
                "--check-cfg=cfg(nervix_lint)",
                "--out-dir",
            ])
            .arg(&directory)
            .arg(path)
            .output()
            .change_context(Error::Io {
                operation: "compile stable contract fixture",
                path: directory.clone(),
            })?;
        fs::write(directory.join("stderr"), result.stderr).change_context(Error::Io {
            operation: "record stable diagnostics",
            path: directory.clone(),
        })?;
        io::atomic_json(
            &directory.join("ending.json"),
            &crate::wrapper::ending(result.status),
        )
    }
}

impl PrepareContracts {
    pub fn run(&self) -> Result<()> {
        ContractCompiler::new(&self.root, &self.target_dir, &self.mode)?.prepare()
    }
}
