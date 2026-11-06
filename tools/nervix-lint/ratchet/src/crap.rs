//! Repository tooling, outside the product layer order.
//! Owns: the pinned complexity analyzer, its installation, complete source walk and coverage runs.
//! Depends on: Cargo, Git, the analyzer's versioned JSON report and filesystem I/O.
//! Must not know: debt thresholds, source-owned policies or product graph execution.

use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
    process::Command,
};

use anyhow::{Context as _, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const CRATE: &str = "cargo-crap";
pub const VERSION: &str = "0.6.1";
const CONFIGURATION: &str = ".cargo-crap.toml";

#[derive(Clone, Debug, clap::Args)]
pub struct Options {
    #[command(subcommand)]
    pub command: Action,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Action {
    Version,
    Install {
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long)]
        target_dir: Option<PathBuf>,
    },
    Run {
        #[arg(last = true)]
        arguments: Vec<String>,
    },
    Analyze {
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long)]
        target_dir: Option<PathBuf>,
        #[arg(long)]
        exclude: Vec<String>,
    },
    Score {
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long)]
        target_dir: Option<PathBuf>,
        #[arg(long)]
        exclude: Vec<String>,
        #[arg(long)]
        lcov: PathBuf,
    },
    Unreachable {
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long = "path")]
        paths: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Function {
    pub path: String,
    pub name: String,
    pub line: usize,
    pub complexity: f64,
}

impl Function {
    fn from_entry(entry: &Value) -> Result<Self> {
        Ok(Self {
            path: Path::new(
                entry["file"]
                    .as_str()
                    .context("cargo-crap entry has no file")?,
            )
            .components()
            .filter(|component| !matches!(component, Component::CurDir))
            .collect::<PathBuf>()
            .to_string_lossy()
            .replace('\\', "/"),
            name: entry["function"]
                .as_str()
                .context("cargo-crap entry has no function")?
                .into(),
            line: usize::try_from(
                entry["line"]
                    .as_u64()
                    .context("cargo-crap entry has no line")?,
            )?,
            complexity: entry["cyclomatic"]
                .as_f64()
                .context("cargo-crap entry has no cyclomatic complexity")?,
        })
    }
}

pub fn target(root: &Path, explicit: Option<&Path>) -> PathBuf {
    explicit.map(Path::to_path_buf).unwrap_or_else(|| {
        std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join("target"))
    })
}

pub fn executable(target: &Path) -> PathBuf {
    target
        .join("tools")
        .join(format!("{CRATE}-{VERSION}"))
        .join("bin")
        .join(CRATE)
}

fn installed_version(path: &Path) -> Result<Option<String>> {
    if !path.is_file() {
        return Ok(None);
    }
    let result = Command::new(path)
        .arg("--version")
        .output()
        .with_context(|| format!("{} could not be run", path.display()))?;
    let stdout = String::from_utf8_lossy(&result.stdout);
    let words: Vec<_> = stdout.split_whitespace().collect();
    ensure!(
        result.status.success() && words.len() == 2 && words[0] == CRATE,
        "{} did not report a {CRATE} version (exit {}):\n{}{}",
        path.display(),
        result.status,
        stdout,
        String::from_utf8_lossy(&result.stderr),
    );
    Ok(Some(words[1].into()))
}

pub fn verified_executable(target: &Path) -> Result<PathBuf> {
    let path = executable(target);
    let Some(found) = installed_version(&path)? else {
        bail!(
            "{CRATE} {VERSION} is not installed at {}; run `just install-cargo-crap`",
            path.display()
        );
    };
    ensure!(
        found == VERSION,
        "{} is {CRATE} {found}, but this repository pins {VERSION}; run `just install-cargo-crap`",
        path.display(),
    );
    Ok(path)
}

pub fn install(root: &Path, target: &Path) -> Result<()> {
    if installed_version(&executable(target))?.as_deref() == Some(VERSION) {
        return Ok(());
    }
    let status = Command::new("cargo")
        .current_dir(root)
        .args([
            "install",
            "--locked",
            "--force",
            "--version",
            VERSION,
            "--root",
        ])
        .arg(target.join("tools").join(format!("{CRATE}-{VERSION}")))
        .arg(CRATE)
        .status()
        .context("cargo could not be run")?;
    ensure!(
        status.success(),
        "installing {CRATE} {VERSION} failed with exit {status}"
    );
    verified_executable(target)?;
    Ok(())
}

pub fn configuration_in_reach(root: &Path) -> Option<PathBuf> {
    root.ancestors()
        .map(|directory| directory.join(CONFIGURATION))
        .find(|path| path.exists())
}

pub fn unreachable_sources(root: &Path, paths: &[String]) -> Result<Vec<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--ignored",
            "--exclude-standard",
            "--",
            "*.rs",
        ])
        .output()
        .context("cannot inspect Git-ignored Rust sources")?;
    ensure!(
        output.status.success(),
        "Git could not list ignored Rust sources: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let ignored: BTreeSet<_> = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| String::from_utf8_lossy(entry).into_owned())
        .collect();
    Ok(paths.iter().filter(|path| {
        Path::new(path).components().any(|part| matches!(part, Component::Normal(name) if name.to_string_lossy().starts_with('.')))
            || root.join(path).is_symlink() || ignored.contains(*path)
    }).cloned().collect())
}

fn analysis_command(root: &Path, target: &Path, excludes: &[String]) -> Result<Command> {
    let path = verified_executable(target)?;
    if let Some(configuration) = configuration_in_reach(root) {
        bail!(
            "{} would change what the analyzer measures; the analysis reads no configuration \
             file, so remove it",
            configuration.display()
        );
    }
    let mut command = Command::new(path);
    command
        .current_dir(root)
        .args(["--path", ".", "--no-default-excludes"]);
    for pattern in excludes {
        command.args(["--exclude", pattern]);
    }
    command.args(["--format", "json", "--sort", "file"]);
    Ok(command)
}

fn entries(output: &[u8]) -> Result<Vec<Value>> {
    let report: Value =
        serde_json::from_slice(output).context("cargo-crap wrote no JSON report")?;
    ensure!(
        report["version"] == VERSION,
        "cargo-crap wrote a version {} report, not {VERSION}",
        report["version"]
    );
    ensure!(
        report.get("try_weight").is_none(),
        "cargo-crap weighted `?` at {}, not the classical 1",
        report["try_weight"]
    );
    Ok(report["entries"]
        .as_array()
        .context("cargo-crap report has no entries")?
        .clone())
}

fn run_analysis(
    root: &Path,
    target: &Path,
    excludes: &[String],
    lcov: Option<&Path>,
) -> Result<Vec<Value>> {
    let mut command = analysis_command(root, target, excludes)?;
    if let Some(path) = lcov {
        command.arg("--lcov").arg(path);
    }
    let output = command.output().context("cargo-crap could not be run")?;
    ensure!(
        output.status.success(),
        "cargo-crap failed with exit {}:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    if lcov.is_none() {
        ensure!(
            output.stderr.is_empty(),
            "cargo-crap skipped sources it could not analyze:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let values = entries(&output.stdout)?;
    ensure!(
        !values.is_empty(),
        "cargo-crap measured no function below {}",
        root.display()
    );
    Ok(values)
}

pub fn analyze(root: &Path, target: &Path, excludes: &[String]) -> Result<Vec<Function>> {
    run_analysis(root, target, excludes, None)?
        .iter()
        .map(Function::from_entry)
        .collect()
}

pub fn score(root: &Path, target: &Path, excludes: &[String], report: &Path) -> Result<Vec<Value>> {
    let alone = analyze(root, target, excludes)?;
    let body =
        fs::read_to_string(report).with_context(|| format!("cannot read {}", report.display()))?;
    let mut located =
        tempfile::NamedTempFile::new().context("cannot create located LCOV report")?;
    use std::io::Write as _;
    for line in body.lines() {
        if let Some(source) = line.strip_prefix("SF:") {
            ensure!(
                !Path::new(source).is_absolute(),
                "{} names {source}, not a source relative to {}",
                report.display(),
                root.display()
            );
            writeln!(located, "SF:{}", root.join(source).display())?;
        } else {
            writeln!(located, "{line}")?;
        }
    }
    located.flush()?;
    let entries = run_analysis(root, target, excludes, Some(located.path()))?;
    let mut before = alone;
    let mut after: Vec<_> = entries
        .iter()
        .map(Function::from_entry)
        .collect::<Result<Vec<_>>>()?;
    let compare = |left: &Function, right: &Function| {
        (&left.path, &left.name, left.line)
            .cmp(&(&right.path, &right.name, right.line))
            .then_with(|| left.complexity.total_cmp(&right.complexity))
    };
    before.sort_by(compare);
    after.sort_by(compare);
    ensure!(
        before == after,
        "cargo-crap measured other functions against {} than without it",
        report.display()
    );
    Ok(entries)
}

pub fn run(options: Options, repository: &Path) -> Result<i32> {
    match options.command {
        Action::Version => println!("{CRATE} {VERSION}"),
        Action::Install { root, target_dir } => {
            let root = root.as_deref().unwrap_or(repository);
            install(root, &target(root, target_dir.as_deref()))?;
        }
        Action::Run { arguments } => {
            let executable = verified_executable(&target(repository, None))?;
            let status = Command::new(executable).args(arguments).status()?;
            return Ok(status.code().unwrap_or(1));
        }
        Action::Analyze {
            root,
            target_dir,
            exclude,
        } => {
            let root = root.as_deref().unwrap_or(repository);
            let result = run_analysis(root, &target(root, target_dir.as_deref()), &exclude, None)?;
            println!(
                "{}",
                serde_json::json!({"version": VERSION, "entries": result})
            );
        }
        Action::Score {
            root,
            target_dir,
            exclude,
            lcov,
        } => {
            let root = root.as_deref().unwrap_or(repository);
            let result = score(root, &target(root, target_dir.as_deref()), &exclude, &lcov)?;
            println!(
                "{}",
                serde_json::json!({"version": VERSION, "entries": result})
            );
        }
        Action::Unreachable { root, paths } => {
            let root = root.as_deref().unwrap_or(repository);
            println!(
                "{}",
                serde_json::to_string(&unreachable_sources(root, &paths)?)?
            );
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    fn analyzer(target: &Path, version: &str, output: &str, stderr: &str) {
        let path = executable(target);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'cargo-crap {version}'; exit 0; \
             fi\nprintf '%s' '{output}'\nprintf '%s' '{stderr}' >&2\n"
        );
        let ready = path.with_extension("ready");
        fs::write(&ready, script).unwrap();
        fs::set_permissions(&ready, fs::Permissions::from_mode(0o755)).unwrap();
        fs::rename(ready, path).unwrap();
    }

    #[test]
    fn pinned_binary_refuses_missing_and_other_versions() {
        let directory = tempfile::tempdir().unwrap();
        assert!(
            verified_executable(directory.path())
                .unwrap_err()
                .to_string()
                .contains("just install-cargo-crap")
        );
        analyzer(directory.path(), "0.5.0", "", "");
        assert!(
            verified_executable(directory.path())
                .unwrap_err()
                .to_string()
                .contains("pins 0.6.1")
        );
        analyzer(directory.path(), VERSION, "", "");
        assert!(verified_executable(directory.path()).is_ok());
    }

    #[test]
    fn analysis_refuses_weight_version_empty_warning_and_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let target = root.join("target");
        let cases = [
            (r#"{"version":"0.5.0","entries":[{}]}"#, "", "version"),
            (
                r#"{"version":"0.6.1","try_weight":0.5,"entries":[{}]}"#,
                "",
                "weighted",
            ),
            (
                r#"{"version":"0.6.1","entries":[]}"#,
                "",
                "measured no function",
            ),
            (
                r#"{"version":"0.6.1","entries":[{}]}"#,
                "skipped source",
                "skipped sources",
            ),
        ];
        for (json, warning, expected) in cases {
            analyzer(&target, VERSION, json, warning);
            assert!(
                analyze(root, &target, &[])
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
        fs::write(root.join(CONFIGURATION), "try-weight = 0.5").unwrap();
        assert!(
            analyze(root, &target, &[])
                .unwrap_err()
                .to_string()
                .contains("would change")
        );
    }

    #[test]
    fn coverage_score_rewrites_relative_sources_and_preserves_function_identity() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let target = root.join("target");
        let output = r#"{"version":"0.6.1","entries":[{"file":"./src/lib.rs","function":"one","line":1,"cyclomatic":1.0,"coverage":100.0,"crap":1.0}]}"#;
        analyzer(&target, VERSION, output, "");
        let report = root.join("report.info");
        fs::write(&report, "SF:src/lib.rs\nDA:1,1\nend_of_record\n").unwrap();
        assert_eq!(score(root, &target, &[], &report).unwrap().len(), 1);
        fs::write(&report, "SF:/absolute.rs\n").unwrap();
        assert!(
            score(root, &target, &[], &report)
                .unwrap_err()
                .to_string()
                .contains("not a source relative")
        );
    }

    #[test]
    fn hidden_ignored_and_linked_sources_are_unreachable() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(root)
            .status()
            .unwrap();
        fs::create_dir(root.join("src")).unwrap();
        fs::write(root.join("src/real.rs"), "fn real() {}\n").unwrap();
        std::os::unix::fs::symlink("real.rs", root.join("src/linked.rs")).unwrap();
        fs::write(root.join(".gitignore"), "src/ignored.rs\n").unwrap();
        fs::write(root.join("src/ignored.rs"), "fn ignored() {}\n").unwrap();
        Command::new("git")
            .args(["add", "-f", "src/ignored.rs"])
            .current_dir(root)
            .status()
            .unwrap();
        let paths = [
            "src/.hidden.rs",
            "src/linked.rs",
            "src/ignored.rs",
            "src/real.rs",
        ]
        .map(str::to_string);
        assert_eq!(unreachable_sources(root, &paths).unwrap(), paths[..3]);
    }

    fn branching(name: &str, complexity: usize) -> String {
        let decisions = (0..complexity.saturating_sub(1))
            .map(|index| format!("if value == {index} {{ return {index}; }}\n"))
            .collect::<String>();
        format!("fn {name}(value: u32) -> u32 {{\n{decisions}0\n}}\n")
    }

    #[test]
    fn pinned_analyzer_keeps_threshold_inactive_and_eligibility_semantics() {
        let repository = crate::gate::repository_root();
        let target = std::env::var_os("NERVIX_RATCHET_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| repository.join("target"));
        verified_executable(&target).expect("just install-cargo-crap prepares the unit suite");
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::create_dir(root.join("src")).unwrap();
        fs::create_dir(root.join("tests")).unwrap();
        fs::write(
            root.join("src/lib.rs"),
            format!(
                "{}\n#[cfg(any())]\n{}\nmacro_rules! generated {{ () => {{ {} }} }}\ntrait \
                 Provided {{ {} }}\nfn outer() {{ {} let _ = || {{ {} }}; }}\n",
                branching("at_limit", 30),
                branching("inactive", 31),
                branching("in_macro", 31),
                branching("provided", 31),
                branching("nested", 31),
                branching("closure_body", 31)
            ),
        )
        .unwrap();
        fs::write(
            root.join("src/lib_tests.rs"),
            branching("in_crate_test", 31),
        )
        .unwrap();
        fs::write(root.join("tests/suite.rs"), branching("test_tree", 31)).unwrap();
        let excludes =
            ["benches", "examples", "tests"].map(|directory| format!("**/{directory}/**"));
        let found = analyze(root, &target, &excludes).unwrap();
        let above: BTreeSet<_> = found
            .iter()
            .filter(|function| function.complexity > 30.0)
            .map(|function| function.name.as_str())
            .collect();
        assert!(above.contains("inactive"), "{above:?}");
        assert!(above.contains("in_crate_test"), "{above:?}");
        for excluded in [
            "at_limit",
            "in_macro",
            "provided",
            "nested",
            "closure_body",
            "test_tree",
        ] {
            assert!(!above.contains(excluded), "{excluded}: {above:?}");
        }
    }
}
