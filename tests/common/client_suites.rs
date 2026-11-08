//! Language-level client suites run against a scenario's cluster.
//!
//! Outside the layer order: a test harness. Product code must not name it.
//!
//! - **Owns.** Starting one host's suite runner on the cluster tier, with the environment a probe
//!   reads and what a suite needs beside it, reading the one result line per test the runner
//!   prints, confirming declared scenario actions, and holding those results to the inventory
//!   for the host's cluster tier.
//! - **Depends on.** The suite sources under `tests/client_conformance/<host>/suite`, the
//!   artifacts `just build-client-conformance` builds for them, and the inventory.
//! - **Must not know.** What a suite's tests check: a run is held only to which registered tests
//!   ran and passed, and to nothing else running.
//!
//! A runner prints `NERVIX-SUITE <outcome> <identity>` once a test ends, where the outcome is
//! `passed`, `failed` or `skipped` and the identity is the test's stable name in the inventory.
//! Every other line it prints is kept for the diagnostic of a failed run.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    path::PathBuf,
    process::Stdio,
    str::FromStr,
    time::Duration,
};

use nervix_primitives::{sync::mpsc, task::JoinHandle, time::Instant};
use nervix_test_environment::workspace_root;
use serde::Deserialize;
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader},
    process::{Child, ChildStdin, Command},
};

mod actions;

pub(crate) use actions::SuiteAction;
use actions::SuiteActions;

/// The prefix of every result line a runner prints.
const RESULT_PREFIX: &str = "NERVIX-SUITE ";

/// The directory `just build-client-conformance` builds the hosts' artifacts into.
const ARTIFACTS_ENV: &str = "NERVIX_CLIENT_CONFORMANCE_DIR";

/// The inventory every suite test is registered in.
const INVENTORY: &str = "tests/client-suites-inventory.toml";

/// The hosts whose suites a scenario can run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SuiteHost {
    /// CPython over the Python package, `nervix_client`, with pytest.
    Python,
    /// Java 21 over the JVM library, with the pinned JUnit 5 console launcher.
    Java,
}

impl FromStr for SuiteHost {
    type Err = io::Error;

    fn from_str(host: &str) -> io::Result<Self> {
        match host {
            "python" => Ok(Self::Python),
            "java" => Ok(Self::Java),
            other => Err(io::Error::other(format!(
                "unknown client suite host '{other}'"
            ))),
        }
    }
}

impl fmt::Display for SuiteHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

impl SuiteHost {
    /// The host's name in the inventory.
    fn name(self) -> &'static str {
        match self {
            Self::Python => "python",
            Self::Java => "java",
        }
    }

    /// The suite's root, which its runner treats as the root of every test identity.
    fn suite(self) -> PathBuf {
        workspace_root()
            .join("tests/client_conformance")
            .join(self.name())
            .join("suite")
    }

    /// The command that runs the host's cluster tier.
    fn command(self) -> io::Result<Command> {
        let artifacts = artifacts()?;
        match self {
            Self::Python => {
                // The CPython the package was installed into, with pytest, pyarrow and polars.
                let mut command = Command::new(artifacts.join("python/bin/python"));
                command
                    .args(["-m", "cluster_runner", "cluster"])
                    .current_dir(self.suite())
                    .env("PYTHONDONTWRITEBYTECODE", "1");
                Ok(command)
            }
            Self::Java => {
                let java = artifacts.join("java");
                let classpath = std::env::join_paths([
                    java.join("nervix-client.jar"),
                    java.join("tests"),
                    java.join("junit-platform-console-standalone.jar"),
                ])
                .map_err(io::Error::other)?;
                let mut command = Command::new("java");
                command
                    .args(["-Xcheck:jni", "-cp"])
                    .arg(classpath)
                    .args([
                        "org.junit.platform.console.ConsoleLauncher",
                        "execute",
                        "--scan-class-path",
                    ])
                    .arg(java.join("tests"))
                    .args([
                        "--include-tag",
                        "cluster",
                        "--include-classname",
                        ".*",
                        "--fail-if-no-tests",
                        "--disable-banner",
                        "--details",
                        "summary",
                        "--config",
                        "junit.jupiter.testclass.order.default=org.junit.jupiter.api.\
                         ClassOrderer$OrderAnnotation",
                    ])
                    .env("NERVIX_CLIENT_JVM_JAR", java.join("nervix-client.jar"));
                Ok(command)
            }
        }
    }
}

fn artifacts() -> io::Result<PathBuf> {
    match std::env::var_os(ARTIFACTS_ENV) {
        Some(directory) => Ok(PathBuf::from(directory)),
        None => Err(io::Error::other(format!(
            "{ARTIFACTS_ENV} is not set; run the suites with `just test-client-conformance`"
        ))),
    }
}

/// Where a suite's cluster tier connects, and what it needs beside the probe's target.
#[derive(Debug, Clone)]
pub(crate) struct SuiteTarget {
    /// The session listener of the node the suite starts on.
    pub(crate) grpc_uri: String,
    /// The session listener of the node that led when the suite started.
    pub(crate) leader_grpc_uri: String,
    /// The session listener of every node.
    pub(crate) seeds: Vec<String>,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) domain: String,
    /// The certificate authority the session listeners' certificates chain to.
    pub(crate) ca_certificate: PathBuf,
    /// The directory the suite writes backup archives to, removed once the run is read.
    pub(crate) backups: PathBuf,
}

impl SuiteTarget {
    fn environment(&self) -> BTreeMap<&'static str, String> {
        BTreeMap::from([
            ("NERVIX_PROBE_GRPC_URI", self.grpc_uri.clone()),
            ("NERVIX_SUITE_LEADER_GRPC_URI", self.leader_grpc_uri.clone()),
            ("NERVIX_SUITE_SEEDS", self.seeds.join(",")),
            ("NERVIX_PROBE_USERNAME", self.username.clone()),
            ("NERVIX_PROBE_PASSWORD", self.password.clone()),
            ("NERVIX_PROBE_DOMAIN", self.domain.clone()),
            (
                "NERVIX_SUITE_CA_CERTIFICATE",
                self.ca_certificate.display().to_string(),
            ),
            (
                "NERVIX_SUITE_BACKUP_DIRECTORY",
                self.backups.display().to_string(),
            ),
        ])
    }
}

/// One registered suite test.
#[derive(Debug, Deserialize)]
struct RegisteredTest {
    host: String,
    tier: String,
    identity: String,
}

#[derive(Debug, Deserialize)]
struct Inventory {
    test: Vec<RegisteredTest>,
}

/// The identities of the host's tests the inventory registers for `tier`.
fn registered(host: SuiteHost, tier: &str) -> io::Result<BTreeSet<String>> {
    let path = workspace_root().join(INVENTORY);
    let text = std::fs::read_to_string(&path)?;
    let inventory: Inventory = toml::from_str(&text).map_err(|error| {
        io::Error::other(format!("{} is not readable: {error}", path.display()))
    })?;
    let mut tests = BTreeSet::new();
    for test in inventory.test {
        if test.host == host.name() && test.tier == tier && !tests.insert(test.identity.clone()) {
            return Err(io::Error::other(format!(
                "{INVENTORY} registers {} twice",
                test.identity
            )));
        }
    }
    Ok(tests)
}

/// A suite runner that is running against a cluster.
pub(crate) struct ClientSuite {
    host: SuiteHost,
    lines: mpsc::UnboundedReceiver<String>,
    input: ChildStdin,
    child: Child,
    stdout: JoinHandle<io::Result<()>>,
    stderr: JoinHandle<io::Result<String>>,
    /// The target's backup directory, kept until the run is read.
    _backups: TempDir,
}

impl fmt::Debug for ClientSuite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientSuite")
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

impl ClientSuite {
    /// Starts the host's cluster tier against `target`, whose backup directory is `backups`.
    pub(crate) fn start(
        host: SuiteHost,
        target: &SuiteTarget,
        backups: TempDir,
    ) -> io::Result<Self> {
        let mut command = host.command()?;
        command
            .envs(target.environment())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            io::Error::other(format!("failed to start the {host} client suite: {error}"))
        })?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("the suite's stdin was not captured"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("the suite's stdout was not captured"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("the suite's stderr was not captured"))?;
        let (sender, lines) = mpsc::unbounded_channel();
        let stdout = nervix_primitives::task::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            loop {
                nervix_primitives::task::consume_budget().await;
                let Some(line) = reader.next_line().await? else {
                    break;
                };
                if sender.send(line).is_err() {
                    break;
                }
            }
            Ok(())
        });
        let stderr = nervix_primitives::task::spawn(async move {
            let mut captured = String::new();
            stderr.read_to_string(&mut captured).await?;
            Ok(captured)
        });
        Ok(Self {
            host,
            lines,
            input,
            child,
            stdout,
            stderr,
            _backups: backups,
        })
    }

    /// Waits for the runner to end and holds its results to the inventory's cluster tier.
    pub(crate) async fn finish(
        mut self,
        within: Duration,
        mut perform: impl FnMut(SuiteAction) -> io::Result<()>,
    ) -> io::Result<SuiteReport> {
        let deadline = Instant::now() + within;
        let mut output = Vec::new();
        let mut actions = SuiteActions::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            match nervix_primitives::time::timeout_at(deadline, self.lines.recv()).await {
                Ok(Some(line)) => {
                    actions
                        .handle(&line, &mut self.input, deadline, &mut perform)
                        .await?;
                    output.push(line);
                }
                Ok(None) => break,
                Err(_) => {
                    return Err(io::Error::other(format!(
                        "the {} client suite did not finish within {within:?}\noutput so far:\n{}",
                        self.host,
                        output.join("\n")
                    )));
                }
            }
        }
        nervix_primitives::time::timeout_at(deadline, self.stdout)
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)??;
        let status = nervix_primitives::time::timeout_at(deadline, self.child.wait())
            .await
            .map_err(|_| {
                io::Error::other(format!(
                    "the {} client suite closed its output but did not exit within {within:?}",
                    self.host
                ))
            })??;
        let stderr = match self.stderr.await {
            Ok(Ok(captured)) => captured,
            Ok(Err(error)) => format!("<stderr unreadable: {error}>"),
            Err(error) => format!("<stderr reader failed: {error}>"),
        };
        Ok(SuiteReport {
            host: self.host,
            success: status.success(),
            status: status.to_string(),
            output,
            stderr,
        })
    }
}

/// What a suite runner printed, and how it exited.
#[derive(Debug)]
pub(crate) struct SuiteReport {
    host: SuiteHost,
    success: bool,
    status: String,
    output: Vec<String>,
    stderr: String,
}

impl SuiteReport {
    /// Holds the run to the inventory: every registered cluster test ran once and passed, no
    /// test ran that the inventory does not register for the tier, and the runner succeeded.
    pub(crate) fn check(&self) -> io::Result<()> {
        let expected = registered(self.host, "cluster")?;
        let mut results: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for line in &self.output {
            let Some(result) = line.strip_prefix(RESULT_PREFIX) else {
                continue;
            };
            let Some((outcome, identity)) = result.trim().split_once(' ') else {
                return Err(self.failure(&[format!("unreadable result line: {line}")]));
            };
            results.entry(identity).or_default().push(outcome);
        }
        let mut problems = Vec::new();
        for identity in &expected {
            if !results.contains_key(identity.as_str()) {
                problems.push(format!("registered cluster test {identity} did not run"));
            }
        }
        for (identity, outcomes) in &results {
            if !expected.contains(*identity) {
                problems.push(format!(
                    "{identity} ran but is not a registered cluster test"
                ));
            } else if outcomes.as_slice() != ["passed"] {
                problems.push(format!("{identity} reported {}", outcomes.join(", ")));
            }
        }
        if !self.success {
            problems.push(format!("the runner exited with {}", self.status));
        }
        if problems.is_empty() {
            return Ok(());
        }
        Err(self.failure(&problems))
    }

    fn failure(&self, problems: &[String]) -> io::Error {
        io::Error::other(format!(
            "the {} client suite's cluster tier failed:\n{}\noutput:\n{}\nstderr:\n{}",
            self.host,
            problems.join("\n"),
            self.output.join("\n"),
            self.stderr
        ))
    }

    /// The number of registered tests that passed, for the scenario's log.
    pub(crate) fn passed(&self) -> usize {
        self.output
            .iter()
            .filter(|line| line.starts_with("NERVIX-SUITE passed "))
            .count()
    }
}

/// The fixture certificate authority every TLS session listener of a scenario chains to.
pub(crate) fn fixture_certificate_authority() -> PathBuf {
    workspace_root().join("tls/dev/ca.pem")
}
