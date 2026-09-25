//! Cross-language client probes run against a scenario's cluster.
//!
//! Outside the layer order: a test harness. Product code must not name it.
//!
//! - **Owns.** Starting a probe of one client runtime against a node, the environment a probe
//!   reads its target from, collecting the report a probe prints, and holding that report to the
//!   report a scenario expects.
//! - **Depends on.** The probe programs under `tests/client_conformance`, the artifacts
//!   `just test-client-conformance` builds for them, and the shared Rust binding, which the
//!   in-process probe drives through its C ABI.
//! - **Must not know.** How any probe talks to the server; every probe prints the same report,
//!   and that report is all the harness compares.
//!
//! A probe prints one report line per observation. Every runtime prints the same report for the
//! same data, so one expected report is the oracle for every language: a difference in any line
//! is a difference in how that runtime read the protocol.

mod c_abi_probe;

use std::{
    collections::BTreeMap,
    env, fmt, io,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader},
    process::{Child, Command},
    sync::mpsc,
    task::JoinHandle,
    time::Instant,
};

/// The checked-in frames the Rust encoder wrote, and the report of them every reader prints.
pub(crate) fn corpus_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("crates/client-wire/conformance")
}

/// The report every implementation prints for the corpus.
pub(crate) fn corpus_report() -> io::Result<String> {
    std::fs::read_to_string(corpus_directory().join("corpus.report"))
}

/// The line a probe prints once its subscription is open, before any row reaches it.
pub(crate) const SUBSCRIBED_LINE: &str = "SUBSCRIBED";

/// The directory `just test-client-conformance` builds the probe artifacts into.
const ARTIFACTS_ENV: &str = "NERVIX_CLIENT_CONFORMANCE_DIR";

/// The shared Rust binding the binding probes load.
const LIBRARY_ENV: &str = "NERVIX_CLIENT_LIBRARY";

/// The client runtimes a scenario can probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeRuntime {
    /// The C ABI of the shared Rust binding, driven from the harness's own process.
    CAbiInProcess,
    /// C over the shared Rust binding.
    C,
    /// C++ over the shared Rust binding.
    Cpp,
    /// CPython over the shared Rust binding, through ctypes.
    Python,
    /// Java over the shared Rust binding, through the Foreign Function and Memory API.
    Java,
    /// Ruby over the shared Rust binding, through Fiddle.
    Ruby,
    /// An independent Go implementation over native gRPC.
    Go,
    /// An independent TypeScript implementation over the binary WebSocket, run by Node.js.
    Node,
    /// The same TypeScript implementation, run by Bun.
    Bun,
}

impl FromStr for ProbeRuntime {
    type Err = io::Error;

    fn from_str(runtime: &str) -> io::Result<Self> {
        match runtime {
            "c-abi-in-process" => Ok(Self::CAbiInProcess),
            "c" => Ok(Self::C),
            "c++" => Ok(Self::Cpp),
            "python" => Ok(Self::Python),
            "java" => Ok(Self::Java),
            "ruby" => Ok(Self::Ruby),
            "go" => Ok(Self::Go),
            "node" => Ok(Self::Node),
            "bun" => Ok(Self::Bun),
            other => Err(io::Error::other(format!(
                "unknown client probe runtime '{other}'"
            ))),
        }
    }
}

impl ProbeRuntime {
    /// The command that runs this runtime's probe, or `None` for the in-process probe.
    fn command(self) -> io::Result<Option<Command>> {
        let sources = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/client_conformance");
        let command = match self {
            Self::CAbiInProcess => return Ok(None),
            Self::C => Command::new(Self::artifacts()?.join("c-probe")),
            Self::Cpp => Command::new(Self::artifacts()?.join("cpp-probe")),
            Self::Python => {
                let mut command = Command::new("python3");
                command.arg(sources.join("python/probe.py"));
                command
            }
            Self::Java => {
                let mut command = Command::new("java");
                command
                    .arg("--enable-native-access=ALL-UNNAMED")
                    .arg(sources.join("java/Probe.java"));
                command
            }
            Self::Ruby => {
                let mut command = Command::new("ruby");
                command.arg(sources.join("ruby/probe.rb"));
                command
            }
            Self::Go => Command::new(Self::artifacts()?.join("go-probe")),
            Self::Node => {
                let mut command = Command::new("node");
                command.arg(Self::artifacts()?.join("node/probe.mjs"));
                command
            }
            Self::Bun => {
                // The Bun release the probe's lockfile pins, not whichever Bun a machine has.
                let artifacts = Self::artifacts()?;
                let mut command = Command::new(artifacts.join("node-src/node_modules/.bin/bun"));
                command.arg(artifacts.join("node/probe.mjs"));
                command
            }
        };
        Ok(Some(command))
    }

    fn artifacts() -> io::Result<PathBuf> {
        match env::var_os(ARTIFACTS_ENV) {
            Some(directory) => Ok(PathBuf::from(directory)),
            None => Err(io::Error::other(format!(
                "{ARTIFACTS_ENV} is not set; run the probes with `just test-client-conformance`"
            ))),
        }
    }

    fn library() -> io::Result<PathBuf> {
        match env::var_os(LIBRARY_ENV) {
            Some(library) => Ok(PathBuf::from(library)),
            None => Err(io::Error::other(format!(
                "{LIBRARY_ENV} is not set; run the probes with `just test-client-conformance`"
            ))),
        }
    }

    /// Whether the probe implements the protocol itself, and so can decode the corpus frames.
    fn decodes_corpus(self) -> bool {
        match self {
            Self::Go | Self::Node | Self::Bun => true,
            Self::CAbiInProcess | Self::C | Self::Cpp | Self::Python | Self::Java | Self::Ruby => {
                false
            }
        }
    }

    /// Whether the probe loads the shared Rust binding rather than implementing the protocol.
    fn loads_binding(self) -> bool {
        match self {
            Self::C | Self::Cpp | Self::Python | Self::Java | Self::Ruby => true,
            Self::CAbiInProcess | Self::Go | Self::Node | Self::Bun => false,
        }
    }
}

/// Where a probe connects and what it subscribes to.
#[derive(Debug, Clone)]
pub(crate) struct ProbeTarget {
    /// The gRPC session URI of the node the probe starts on.
    pub(crate) grpc_uri: String,
    /// The binary WebSocket session URI of the same node, with its credentials.
    pub(crate) websocket_uri: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) domain: String,
    pub(crate) relay: String,
    pub(crate) subscription: String,
    /// How many rows the probe reads before it closes its subscription.
    pub(crate) rows: usize,
}

impl ProbeTarget {
    /// The environment every probe reads its target from.
    fn environment(&self) -> BTreeMap<&'static str, String> {
        BTreeMap::from([
            ("NERVIX_PROBE_GRPC_URI", self.grpc_uri.clone()),
            ("NERVIX_PROBE_WEBSOCKET_URI", self.websocket_uri.clone()),
            ("NERVIX_PROBE_USERNAME", self.username.clone()),
            ("NERVIX_PROBE_PASSWORD", self.password.clone()),
            ("NERVIX_PROBE_DOMAIN", self.domain.clone()),
            ("NERVIX_PROBE_RELAY", self.relay.clone()),
            ("NERVIX_PROBE_SUBSCRIPTION", self.subscription.clone()),
            ("NERVIX_PROBE_ROWS", self.rows.to_string()),
        ])
    }
}

/// How a running probe ends.
enum Completion {
    Process {
        child: Child,
        stderr: JoinHandle<io::Result<String>>,
    },
    InProcess(JoinHandle<io::Result<()>>),
}

/// A probe that is running against a cluster.
pub(crate) struct ClientProbe {
    runtime: ProbeRuntime,
    lines: mpsc::UnboundedReceiver<String>,
    received: Vec<String>,
    completion: Completion,
}

impl fmt::Debug for ClientProbe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientProbe")
            .field("runtime", &self.runtime)
            .field("received", &self.received)
            .finish_non_exhaustive()
    }
}

impl ClientProbe {
    /// Starts an independent implementation's probe decoding the checked-in conformance corpus.
    pub(crate) async fn start_corpus(runtime: ProbeRuntime) -> io::Result<Self> {
        if !runtime.decodes_corpus() {
            return Err(io::Error::other(format!(
                "the {runtime:?} probe uses the shared Rust binding and reads no frames itself"
            )));
        }
        let Some(mut command) = runtime.command()? else {
            return Err(io::Error::other(
                "the in-process probe reads no frames itself",
            ));
        };
        command.arg("corpus").arg(corpus_directory());
        Self::spawn(runtime, command)
    }

    pub(crate) async fn start(runtime: ProbeRuntime, target: ProbeTarget) -> io::Result<Self> {
        let (sender, lines) = mpsc::unbounded_channel();
        let Some(mut command) = runtime.command()? else {
            let completion = Completion::InProcess(tokio::task::spawn_blocking(move || {
                c_abi_probe::run(&target, &mut |line| {
                    sender.send(line.to_string()).map_err(|_| {
                        io::Error::other("the scenario stopped reading the probe's report")
                    })
                })
            }));
            return Ok(Self {
                runtime,
                lines,
                received: Vec::new(),
                completion,
            });
        };
        command.envs(target.environment());
        if runtime.loads_binding() {
            command.env(LIBRARY_ENV, ProbeRuntime::library()?);
        }
        Self::spawn(runtime, command)
    }

    fn spawn(runtime: ProbeRuntime, mut command: Command) -> io::Result<Self> {
        let (sender, lines) = mpsc::unbounded_channel();
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            io::Error::other(format!(
                "failed to start the {runtime:?} client probe: {error}"
            ))
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("the probe's stdout was not captured"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("the probe's stderr was not captured"))?;
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                tokio::task::consume_budget().await;
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let stderr = tokio::spawn(async move {
            let mut captured = String::new();
            stderr.read_to_string(&mut captured).await?;
            Ok(captured)
        });
        Ok(Self {
            runtime,
            lines,
            received: Vec::new(),
            completion: Completion::Process { child, stderr },
        })
    }

    /// Waits until the probe prints `expected`, keeping every line it printed on the way.
    pub(crate) async fn wait_for_line(
        &mut self,
        expected: &str,
        within: Duration,
    ) -> io::Result<()> {
        let deadline = Instant::now() + within;
        loop {
            tokio::task::consume_budget().await;
            match tokio::time::timeout_at(deadline, self.lines.recv()).await {
                Ok(Some(line)) => {
                    let found = line == expected;
                    self.received.push(line);
                    if found {
                        return Ok(());
                    }
                }
                Ok(None) => {
                    let ending = self.ending().await;
                    return Err(io::Error::other(format!(
                        "the {:?} client probe ended before printing '{expected}': \
                         {ending}\nreport so far:\n{}",
                        self.runtime,
                        self.received.join("\n")
                    )));
                }
                Err(_) => {
                    return Err(io::Error::other(format!(
                        "the {:?} client probe did not print '{expected}' within \
                         {within:?}\nreport so far:\n{}",
                        self.runtime,
                        self.received.join("\n")
                    )));
                }
            }
        }
    }

    /// Waits for the probe to end and returns its whole report, or why it failed.
    pub(crate) async fn finish(mut self, within: Duration) -> io::Result<ProbeReport> {
        let deadline = Instant::now() + within;
        loop {
            tokio::task::consume_budget().await;
            match tokio::time::timeout_at(deadline, self.lines.recv()).await {
                Ok(Some(line)) => self.received.push(line),
                Ok(None) => break,
                Err(_) => {
                    return Err(io::Error::other(format!(
                        "the {:?} client probe did not finish within {within:?}\nreport so \
                         far:\n{}",
                        self.runtime,
                        self.received.join("\n")
                    )));
                }
            }
        }
        let ending = tokio::time::timeout_at(deadline, self.ended()).await;
        match ending {
            Ok(Ok(())) => Ok(ProbeReport {
                lines: self.received,
            }),
            Ok(Err(error)) => Err(io::Error::other(format!(
                "the {:?} client probe failed: {error}\nreport:\n{}",
                self.runtime,
                self.received.join("\n")
            ))),
            Err(_) => Err(io::Error::other(format!(
                "the {:?} client probe closed its report but did not exit within {within:?}",
                self.runtime
            ))),
        }
    }

    /// Why a probe that stopped reporting ended, for a diagnostic.
    async fn ending(&mut self) -> String {
        match tokio::time::timeout(Duration::from_secs(30), self.ended()).await {
            Ok(Ok(())) => "it exited successfully".to_string(),
            Ok(Err(error)) => error.to_string(),
            Err(_) => "it closed its report and kept running".to_string(),
        }
    }

    /// Waits for the probe to end, succeeding only when it exited successfully.
    async fn ended(&mut self) -> io::Result<()> {
        match &mut self.completion {
            Completion::InProcess(task) => match task.await {
                Ok(result) => result,
                Err(error) => Err(io::Error::other(format!(
                    "the in-process probe panicked: {error}"
                ))),
            },
            Completion::Process { child, stderr } => {
                let status = child.wait().await?;
                let captured = match stderr.await {
                    Ok(Ok(captured)) => captured,
                    Ok(Err(error)) => format!("<stderr unreadable: {error}>"),
                    Err(error) => format!("<stderr reader failed: {error}>"),
                };
                if status.success() {
                    return Ok(());
                }
                Err(io::Error::other(format!(
                    "it exited with {status}; stderr:\n{captured}"
                )))
            }
        }
    }
}

/// The lines a probe printed.
#[derive(Debug, Clone)]
pub(crate) struct ProbeReport {
    lines: Vec<String>,
}

impl ProbeReport {
    /// Holds the report to `expected`. Row lines are compared as a set, because rows of
    /// different branches reach a subscriber in no fixed order; every other line is compared in
    /// order.
    pub(crate) fn check(&self, expected: &str) -> io::Result<()> {
        let expected_lines: Vec<String> = expected
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect();
        let actual = ReportShape::of(&self.lines);
        let wanted = ReportShape::of(&expected_lines);
        if actual == wanted {
            return Ok(());
        }
        Err(io::Error::other(format!(
            "the probe's report differs from the expected report\nexpected:\n{}\nactual:\n{}",
            expected_lines.join("\n"),
            self.lines.join("\n")
        )))
    }
}

/// A report with its row lines separated out and sorted.
#[derive(Debug, PartialEq, Eq)]
struct ReportShape {
    ordered: Vec<String>,
    rows: Vec<String>,
}

impl ReportShape {
    fn of(lines: &[String]) -> Self {
        let mut ordered = Vec::new();
        let mut rows = Vec::new();
        for line in lines {
            if line.starts_with("ROW ") {
                rows.push(line.clone());
            } else {
                ordered.push(line.clone());
            }
        }
        rows.sort();
        Self { ordered, rows }
    }
}
