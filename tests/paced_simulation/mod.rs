//! Public scenarios for the runnable paced simulation drivers under `examples/paced-simulation`.
//!
//! Layer: test harness.
//! - **Owns.** Running a published driver as its own process against a node of the scenario's
//!   cluster, the ledger and effect files the driver's runs in one scenario share, the report each
//!   run prints, and holding the files and the report to the behavior a scenario expects.
//! - **Depends on.** The Rust driver binary `tests-deps` builds, the Python driver with the shared
//!   binding it loads, the published example graph, and the scenario's cluster.
//! - **Must not know.** How either driver talks to the server. Both drivers print the same report
//!   and write the same files, so one set of checks holds both to the same contract.

use std::collections::BTreeSet;

use cucumber::{given, then, when};
use nervix_models::Timestamp;
use nervix_primitives::{sync::mpsc, task::JoinHandle};
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt as _, BufReader},
    process::{Child, Command as ProcessCommand},
};

use super::*;

/// The published example graph every scenario of the drivers loads.
const EXAMPLE_GRAPH: &str = "examples/paced-simulation/paced_simulation.nspl";

/// The Python driver, run from its published source.
const PYTHON_DRIVER: &str = "examples/paced-simulation/python/paced_simulation.py";

/// The instrumented Rust driver a coverage run builds, when the run builds one.
const RUST_DRIVER_ENV: &str = "NERVIX_PACED_SIMULATION_PATH";

/// The shared Rust binding the Python driver loads.
const LIBRARY_ENV: &str = "NERVIX_CLIENT_LIBRARY";

/// The admission error a reading stamped outside every reached tick window is rejected with.
const OUTSIDE_WINDOW: &str = "outside any reached logical tick window";

/// The drivers' runs in one scenario and the files they share.
#[derive(Default)]
pub(crate) struct PacedSimulation {
    /// Holds the ledger and the effect store every run of the scenario reads and appends to.
    directory: Option<TempDir>,
    running: Option<DriverRun>,
    finished: Option<FinishedRun>,
}

/// The two published drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Driver {
    /// The Rust driver, built on the Rust client library.
    Rust,
    /// The Python driver, built on the shared C binding through ctypes.
    Python,
}

impl FromStr for Driver {
    type Err = String;

    fn from_str(driver: &str) -> Result<Self, Self::Err> {
        match driver {
            "rust" => Ok(Self::Rust),
            "python" => Ok(Self::Python),
            other => Err(format!("unknown paced simulation driver '{other}'")),
        }
    }
}

impl Driver {
    /// The command that runs this driver.
    fn command(self) -> ProcessCommand {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
        match self {
            Self::Rust => {
                let program = match std::env::var_os(RUST_DRIVER_ENV) {
                    Some(path) => PathBuf::from(path),
                    None => build_directory().join("nervix-paced-simulation"),
                };
                assert!(
                    program.exists(),
                    "the Rust paced simulation driver is missing at {}; `just tests-deps` builds \
                     it",
                    program.display()
                );
                ProcessCommand::new(program)
            }
            Self::Python => {
                let library = match std::env::var_os(LIBRARY_ENV) {
                    Some(path) => PathBuf::from(path),
                    None => build_directory().join("libnervix_client_ffi.so"),
                };
                assert!(
                    library.exists(),
                    "the shared binding the Python paced simulation driver loads is missing at \
                     {}; `just tests-deps` builds it",
                    library.display()
                );
                let mut command = ProcessCommand::new("python3");
                command.arg(repository.join(PYTHON_DRIVER));
                command.env(LIBRARY_ENV, library);
                command
            }
        }
    }
}

/// The directory the debug build of the workspace writes its binaries and libraries to.
fn build_directory() -> PathBuf {
    let target = match std::env::var_os("CARGO_TARGET_DIR") {
        Some(path) => PathBuf::from(path),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join("target"),
    };
    target.join("debug")
}

/// A driver process the scenario started and has not seen end yet.
struct DriverRun {
    driver: Driver,
    child: Child,
    lines: mpsc::UnboundedReceiver<String>,
    received: Vec<String>,
    stderr: JoinHandle<std::io::Result<String>>,
    started: Instant,
}

/// A driver process that ended, with everything it printed.
struct FinishedRun {
    driver: Driver,
    status: Option<i32>,
    lines: Vec<String>,
    stderr: String,
    elapsed: Duration,
}

impl FinishedRun {
    /// The values of the driver's last `SUMMARY` line, by key.
    fn summary(&self) -> BTreeMap<String, String> {
        let line = self
            .lines
            .iter()
            .rev()
            .find_map(|line| line.strip_prefix("SUMMARY "))
            .unwrap_or_else(|| {
                panic!(
                    "the {:?} driver printed no summary:\n{}\nstderr:\n{}",
                    self.driver,
                    self.lines.join("\n"),
                    self.stderr
                )
            });
        let mut summary = BTreeMap::new();
        for pair in line.split_whitespace() {
            let (key, value) = pair
                .split_once('=')
                .unwrap_or_else(|| panic!("summary entry '{pair}' is not key=value: {line}"));
            summary.insert(key.to_string(), value.to_string());
        }
        summary
    }

    /// The clock the driver reported first: the generation, period and skew it paced by.
    fn first_paced_clock(&self) -> ReportedClock {
        let line = self
            .lines
            .iter()
            .find(|line| line.starts_with("CLOCK ") && line.contains(" state=paced "))
            .unwrap_or_else(|| {
                panic!(
                    "the {:?} driver reported no paced clock:\n{}",
                    self.driver,
                    self.lines.join("\n")
                )
            });
        let mut period = None;
        let mut skew = None;
        for pair in line.split_whitespace() {
            match pair.split_once('=') {
                Some(("period", value)) => {
                    period = Some(parse_duration_text(value).expect("the period is a duration"));
                }
                Some(("skew", value)) => {
                    skew = Some(parse_duration_text(value).expect("the skew is a duration"));
                }
                _ => {}
            }
        }
        ReportedClock {
            period: period.unwrap_or_else(|| panic!("the clock line names no period: {line}")),
            skew: skew.unwrap_or_else(|| panic!("the clock line names no skew: {line}")),
        }
    }

    fn describe(&self) -> String {
        format!(
            "the {:?} driver exited with {:?} after {:?}\nstdout:\n{}\nstderr:\n{}",
            self.driver,
            self.status,
            self.elapsed,
            self.lines.join("\n"),
            self.stderr
        )
    }
}

/// The period and skew of the paced clock a driver reported.
struct ReportedClock {
    period: Duration,
    skew: Duration,
}

/// One line of the application's input ledger.
#[derive(Debug, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum LedgerRecord {
    /// The driver began planning readings in a START generation.
    Generation { generation: u64 },
    /// One reading the driver planned, as it submits it.
    Reading(LedgerReading),
    /// The terminal outcome of the batch that carried these readings.
    Outcome {
        reading_ids: Vec<String>,
        outcome: String,
        cause: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerReading {
    reading_id: String,
    generation: u64,
    tick: u64,
    sensor: String,
    occurred_at: String,
    value: i64,
    ingestor: String,
    timestamps: String,
    stamp: String,
}

impl LedgerReading {
    fn occurred_at(&self) -> Timestamp {
        parse_timestamp(&self.occurred_at)
    }

    /// Whether the ingestor the reading was submitted to rejects it: only a `TIMESTAMP AT`
    /// ingestor checks the event time, and only a stamp before the window fails it.
    fn expects_rejection(&self) -> bool {
        self.timestamps == "at" && self.stamp == "before_window"
    }
}

/// One line of the application's effect store.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum EffectRecord {
    Reading {
        reading_id: String,
        sensor: String,
        tick: u64,
        occurred_at: String,
        admitted_at: String,
        timestamp_source: String,
        value: i64,
        generation: u64,
    },
    Rejection {
        reading_id: String,
        occurred_at: String,
        error_code: String,
        error_message: String,
        generation: u64,
    },
}

/// The terminal outcome the ledger records for one reading's batch, and its cause.
#[derive(Debug)]
struct LedgerOutcome {
    outcome: String,
    cause: String,
}

impl LedgerOutcome {
    fn completed(&self) -> bool {
        self.outcome == "completed"
    }
}

fn parse_timestamp(text: &str) -> Timestamp {
    Timestamp::from_str(text).unwrap_or_else(|error| panic!("'{text}' is not RFC 3339: {error}"))
}

/// Everything the drivers of one scenario wrote: the ledger in order, and the effects.
struct Records {
    ledger: Vec<LedgerRecord>,
    effects: Vec<EffectRecord>,
}

impl Records {
    fn read(directory: &Path) -> Self {
        let ledger = read_json_lines(&directory.join("ledger.jsonl"));
        let effects = read_json_lines(&directory.join("effects.jsonl"));
        Self { ledger, effects }
    }

    /// Every reading the ledger holds, by identity, as last planned.
    fn readings(&self) -> BTreeMap<String, LedgerReading> {
        let mut readings = BTreeMap::new();
        for record in &self.ledger {
            if let LedgerRecord::Reading(reading) = record {
                readings.insert(reading.reading_id.clone(), reading.clone());
            }
        }
        readings
    }

    /// The last outcome the ledger records for each reading.
    fn outcomes(&self) -> BTreeMap<String, LedgerOutcome> {
        let mut outcomes = BTreeMap::new();
        for record in &self.ledger {
            if let LedgerRecord::Outcome {
                reading_ids,
                outcome,
                cause,
            } = record
            {
                for reading_id in reading_ids {
                    let recorded = LedgerOutcome {
                        outcome: outcome.clone(),
                        cause: cause.clone(),
                    };
                    outcomes.insert(reading_id.clone(), recorded);
                }
            }
        }
        outcomes
    }

    /// The reading effects by reading, failing on a reading applied twice.
    fn reading_effects(&self) -> BTreeMap<String, &EffectRecord> {
        let mut effects = BTreeMap::new();
        for effect in &self.effects {
            if let EffectRecord::Reading { reading_id, .. } = effect {
                let previous = effects.insert(reading_id.clone(), effect);
                assert!(
                    previous.is_none(),
                    "the effect store applied reading {reading_id} twice"
                );
            }
        }
        effects
    }

    /// The rejection notices by reading, failing on a notice recorded twice.
    fn rejection_notices(&self) -> BTreeMap<String, &EffectRecord> {
        let mut notices = BTreeMap::new();
        for effect in &self.effects {
            if let EffectRecord::Rejection { reading_id, .. } = effect {
                let previous = notices.insert(reading_id.clone(), effect);
                assert!(
                    previous.is_none(),
                    "the effect store recorded the rejection of {reading_id} twice"
                );
            }
        }
        notices
    }
}

fn read_json_lines<T: for<'de> Deserialize<'de>>(path: &Path) -> Vec<T> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => panic!("cannot read {}: {error}", path.display()),
    };
    let mut records = Vec::new();
    for line in text.lines() {
        let record = serde_json::from_str(line)
            .unwrap_or_else(|error| panic!("{}: '{line}' is malformed: {error}", path.display()));
        records.push(record);
    }
    records
}

impl PacedSimulation {
    fn directory(&mut self) -> PathBuf {
        if self.directory.is_none() {
            self.directory = Some(TempDir::new().expect("a scenario can create a directory"));
        }
        self.directory
            .as_ref()
            .verified("the directory was created above")
            .path()
            .to_path_buf()
    }

    fn records(&self) -> Records {
        let directory = self
            .directory
            .as_ref()
            .expect("a driver ran in this scenario and wrote its files");
        Records::read(directory.path())
    }

    fn running(&mut self) -> &mut DriverRun {
        self.running
            .as_mut()
            .expect("a paced simulation driver is running")
    }

    fn finished(&self) -> &FinishedRun {
        self.finished
            .as_ref()
            .expect("a paced simulation driver ran to its end")
    }
}

impl DriverRun {
    fn start(driver: Driver, server: &str, arguments: &[String], directory: &Path) -> Self {
        let mut command = driver.command();
        command
            .arg("--server")
            .arg(server)
            .arg("--username")
            .arg(TEST_AUTH_USERNAME)
            .arg("--password")
            .arg(TEST_AUTH_PASSWORD)
            .arg("--ledger")
            .arg(directory.join("ledger.jsonl"))
            .arg("--effects")
            .arg(directory.join("effects.jsonl"))
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .unwrap_or_else(|error| panic!("the {driver:?} driver could not start: {error}"));
        let stdout = child
            .stdout
            .take()
            .verified("the driver's stdout is piped above");
        let mut stderr = child
            .stderr
            .take()
            .verified("the driver's stderr is piped above");
        let (sender, lines) = mpsc::unbounded_channel();
        nervix_primitives::task::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                nervix_primitives::task::consume_budget().await;
                append_cucumber_log_line(&format!("paced simulation {driver:?}: {line}"));
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let stderr = nervix_primitives::task::spawn(async move {
            let mut captured = String::new();
            stderr.read_to_string(&mut captured).await?;
            Ok(captured)
        });
        Self {
            driver,
            child,
            lines,
            received: Vec::new(),
            stderr,
            started: Instant::now(),
        }
    }

    /// Waits until the driver has printed a line `matches` accepts, keeping every line on the way.
    /// A line printed before the wait began counts: the clock, producer and consumer report their
    /// events independently, so their lines arrive in no fixed order.
    async fn wait_for_line(
        &mut self,
        description: &str,
        within: Duration,
        matches: impl Fn(&str) -> bool,
    ) {
        for line in &self.received {
            if matches(line) {
                return;
            }
        }
        let deadline = Instant::now() + within;
        loop {
            nervix_primitives::task::consume_budget().await;
            match nervix_primitives::time::timeout_at(deadline, self.lines.recv()).await {
                Ok(Some(line)) => {
                    let found = matches(&line);
                    self.received.push(line);
                    if found {
                        return;
                    }
                }
                Ok(None) => panic!(
                    "the {:?} driver ended before printing a line {description}:\n{}",
                    self.driver,
                    self.received.join("\n")
                ),
                Err(_) => panic!(
                    "the {:?} driver printed no line {description} within {within:?}:\n{}",
                    self.driver,
                    self.received.join("\n")
                ),
            }
        }
    }

    /// Waits for the driver to exit and keeps everything it printed.
    async fn finish(mut self, within: Duration) -> FinishedRun {
        let deadline = Instant::now() + within;
        loop {
            nervix_primitives::task::consume_budget().await;
            match nervix_primitives::time::timeout_at(deadline, self.lines.recv()).await {
                Ok(Some(line)) => self.received.push(line),
                Ok(None) => break,
                Err(_) => panic!(
                    "the {:?} driver did not finish within {within:?}:\n{}",
                    self.driver,
                    self.received.join("\n")
                ),
            }
        }
        let status = nervix_primitives::time::timeout_at(deadline, self.child.wait())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the {:?} driver closed its output but did not exit within {within:?}",
                    self.driver
                )
            })
            .unwrap_or_else(|error| {
                panic!("waiting for the {:?} driver failed: {error}", self.driver)
            });
        let elapsed = self.started.elapsed();
        let stderr = match self.stderr.await {
            Ok(Ok(captured)) => captured,
            Ok(Err(error)) => format!("<stderr unreadable: {error}>"),
            Err(error) => format!("<stderr reader failed: {error}>"),
        };
        append_cucumber_log_line(&format!(
            "paced simulation {:?} exited with {status} after {elapsed:?}; stderr:\n{stderr}",
            self.driver
        ));
        FinishedRun {
            driver: self.driver,
            status: status.code(),
            lines: self.received,
            stderr,
            elapsed,
        }
    }
}

/// Splits a step's argument text into the arguments it passes the driver.
fn driver_arguments(world: &ScenarioWorld, arguments: &str) -> Vec<String> {
    expand_placeholders(world, arguments)
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

fn start_driver(world: &mut ScenarioWorld, driver: &str, server: String, arguments: &str) {
    let driver: Driver = driver
        .parse()
        .unwrap_or_else(|error: String| panic!("{error}"));
    let mut arguments = driver_arguments(world, arguments);
    if !arguments.iter().any(|argument| argument == "--domain") {
        arguments.push("--domain".to_string());
        arguments.push(world.domain.clone());
    }
    assert!(
        world.paced_simulation.running.is_none(),
        "a paced simulation driver is already running"
    );
    let directory = world.paced_simulation.directory();
    append_cucumber_log_line(&format!(
        "paced simulation {driver:?}: server={server} arguments={arguments:?}"
    ));
    world.paced_simulation.finished = None;
    world.paced_simulation.running =
        Some(DriverRun::start(driver, &server, &arguments, &directory));
}

#[given("the leader node is configured with the paced simulation example graph")]
async fn given_the_leader_node_is_configured_with_the_example_graph(world: &mut ScenarioWorld) {
    world.last_command_error = None;
    world.last_command_output = None;
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(EXAMPLE_GRAPH);
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
    // `USE` is client-local: a client selects the domain itself and sends the rest. The harness's
    // session runs in the scenario's active domain, so the graph must select that same domain.
    let statements = nervix_nspl::client_statement::parse_client_statement_sources(&source)
        .unwrap_or_else(|error| panic!("the example graph does not parse: {error:?}"));
    let mut commands = String::new();
    for statement in &statements {
        if let nervix_nspl::client_statement::ClientStatement::UseDomain(domain) =
            &statement.statement
        {
            assert_eq!(
                domain.as_str(),
                world.domain,
                "the example graph selects another domain than the scenario's active one"
            );
            continue;
        }
        let text = statement.source(&source).trim();
        commands.push_str(text);
        if !text.ends_with(';') {
            commands.push(';');
        }
        commands.push('\n');
    }
    let leader = current_leader_node(world).await;
    let server = world
        .cluster()
        .grpc_uri(&leader)
        .expect("the leader has a gRPC endpoint");
    let client = Client::connect_with_options(
        &server,
        client_domain(&world.domain),
        client_connect_options(&server).expect("the cluster has valid client options"),
    )
    .await
    .unwrap_or_else(|error| panic!("the example graph client did not connect: {error}"));
    // One native client retains BEGIN/COMMIT state and each command's execution reference across
    // a leader change. A raw wire probe deliberately exposes OutcomeUnknown to its caller.
    for command in nspl_statements(&commands) {
        append_cucumber_log_line(&format!("paced example graph command: {command}"));
        let outcome = client
            .execute(command)
            .await
            .unwrap_or_else(|error| panic!("the example graph did not load: {error}"));
        assert!(
            outcome.succeeded(),
            "the example graph did not load: {outcome:?}"
        );
        world.last_command_output =
            Some(crate::common::cluster::flatten_outcome_messages(&outcome));
    }
    let leader = current_leader_node(world).await;
    let session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .unwrap_or_else(|error| {
            panic!("the configured example graph session did not open: {error}")
        });
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = false;
}

#[when(
    expr = "the {word} paced simulation driver runs through node {string} with arguments {string}"
)]
async fn when_the_driver_runs_through_node(
    world: &mut ScenarioWorld,
    driver: String,
    node_id: String,
    arguments: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let server = world
        .cluster()
        .grpc_uri(&node_id)
        .expect("the driver's node belongs to the cluster");
    start_driver(world, &driver, server, &arguments);
}

/// Starts a driver through the TCP forwarder a preceding step stood in front of `node_id`'s gRPC
/// endpoint, so the scenario can cut the driver's session by stopping the forwarder.
#[when(
    expr = "the {word} paced simulation driver runs through the forwarded gRPC endpoint of node \
            {string} with arguments {string}"
)]
async fn when_the_driver_runs_through_the_forwarder(
    world: &mut ScenarioWorld,
    driver: String,
    _node_id: String,
    arguments: String,
) {
    let server = world
        .placeholders
        .get("forwarded_grpc")
        .expect("a preceding step forwarded the node's gRPC endpoint")
        .clone();
    start_driver(world, &driver, server, &arguments);
}

#[then(expr = "within {string} the paced simulation driver prints a line starting with {string}")]
async fn then_the_driver_prints_a_line_starting_with(
    world: &mut ScenarioWorld,
    within: String,
    prefix: String,
) {
    let within = parse_duration_text(&within).expect("the step names a duration");
    let prefix = expand_placeholders(world, &prefix);
    let description = format!("starting with '{prefix}'");
    world
        .paced_simulation
        .running()
        .wait_for_line(&description, within, |line| line.starts_with(&prefix))
        .await;
}

#[then(expr = "within {string} the paced simulation driver prints a line containing {string}")]
async fn then_the_driver_prints_a_line_containing(
    world: &mut ScenarioWorld,
    within: String,
    text: String,
) {
    let within = parse_duration_text(&within).expect("the step names a duration");
    let text = expand_placeholders(world, &text);
    let description = format!("containing '{text}'");
    world
        .paced_simulation
        .running()
        .wait_for_line(&description, within, |line| line.contains(&text))
        .await;
}

async fn finish_driver<'world>(
    world: &'world mut ScenarioWorld,
    within: &str,
) -> &'world FinishedRun {
    let within = parse_duration_text(within).expect("the step names a duration");
    let run = world
        .paced_simulation
        .running
        .take()
        .expect("a paced simulation driver is running");
    let finished = run.finish(within).await;
    world.paced_simulation.finished = Some(finished);
    world.paced_simulation.finished()
}

#[then(expr = "within {string} the paced simulation driver exits with status {int}")]
async fn then_the_driver_exits_with_status(world: &mut ScenarioWorld, within: String, status: i32) {
    let finished = finish_driver(world, &within).await;
    assert_eq!(
        finished.status,
        Some(status),
        "unexpected exit: {}",
        finished.describe()
    );
}

/// The driver exits with 0 once every reading it submitted completed, and with 3 when an outcome
/// left a reading unresolved, so its status must agree with the outcomes its summary counts.
#[then(
    expr = "within {string} the paced simulation driver finishes with the status its outcomes \
            imply"
)]
async fn then_the_driver_finishes_with_the_status_its_outcomes_imply(
    world: &mut ScenarioWorld,
    within: String,
) {
    let finished = finish_driver(world, &within).await;
    let summary = finished.summary();
    let count = |key: &str| -> u64 {
        summary
            .get(key)
            .unwrap_or_else(|| panic!("the summary has no '{key}': {}", finished.describe()))
            .parse()
            .unwrap_or_else(|error| panic!("summary '{key}' is not a count: {error}"))
    };
    let readings = count("readings");
    let completed = count("completed");
    let resolved =
        completed + count("not_admitted") + count("processing_failed") + count("outcome_unknown");
    assert_eq!(
        resolved,
        readings,
        "every reading has exactly one outcome: {}",
        finished.describe()
    );
    let expected = if completed == readings { 0 } else { 3 };
    assert_eq!(
        finished.status,
        Some(expected),
        "the exit status contradicts the outcomes: {}",
        finished.describe()
    );
}

#[then("the paced simulation driver reports")]
async fn then_the_driver_reports(world: &mut ScenarioWorld, #[step] step: &Step) {
    let finished = world.paced_simulation.finished();
    let summary = finished.summary();
    let table = step.table.as_ref().expect("the step has a table");
    for row in &table.rows {
        let [key, expected] = row.as_slice() else {
            panic!("each row names one summary key and its value: {row:?}");
        };
        let actual = summary
            .get(key)
            .unwrap_or_else(|| panic!("the summary has no '{key}': {}", finished.describe()));
        let holds = if let Some(bound) = expected.strip_prefix(">= ") {
            parse_count(actual) >= parse_count(bound)
        } else if let Some(bound) = expected.strip_prefix("<= ") {
            parse_count(actual) <= parse_count(bound)
        } else {
            actual == expected
        };
        assert!(
            holds,
            "summary '{key}' is {actual}, expected {expected}: {}",
            finished.describe()
        );
    }
}

fn parse_count(text: &str) -> u64 {
    text.parse()
        .unwrap_or_else(|error| panic!("'{text}' is not a count: {error}"))
}

#[then(expr = "the paced simulation driver's errors contain {string}")]
async fn then_the_driver_errors_contain(world: &mut ScenarioWorld, text: String) {
    let text = expand_placeholders(world, &text);
    let finished = world.paced_simulation.finished();
    assert!(
        finished.stderr.contains(&text),
        "the driver's errors do not contain '{text}': {}",
        finished.describe()
    );
}

/// Every reading the ledger planned completed, and the effect store holds exactly one record of
/// it with exactly its content: a reading effect for an admitted reading, or a rejection notice
/// for one its ingestor rejected. Nothing else is in the store.
#[then("the paced simulation effects hold every reading of its ledger exactly once")]
async fn then_the_effects_hold_every_reading_exactly_once(world: &mut ScenarioWorld) {
    let records = world.paced_simulation.records();
    let readings = records.readings();
    let outcomes = records.outcomes();
    let effects = records.reading_effects();
    let notices = records.rejection_notices();
    assert!(!readings.is_empty(), "the ledger holds no reading");
    for (reading_id, reading) in &readings {
        let outcome = outcomes
            .get(reading_id)
            .unwrap_or_else(|| panic!("reading {reading_id} has no outcome"));
        assert!(
            outcome.completed(),
            "reading {reading_id} did not complete: {} {}",
            outcome.outcome,
            outcome.cause
        );
        if reading.expects_rejection() {
            let Some(EffectRecord::Rejection { generation, .. }) = notices.get(reading_id).copied()
            else {
                panic!("rejected reading {reading_id} has no rejection notice");
            };
            assert_eq!(
                *generation, reading.generation,
                "the rejection of {reading_id} was recorded under another generation"
            );
            assert!(
                !effects.contains_key(reading_id),
                "rejected reading {reading_id} was applied as an admitted reading"
            );
            continue;
        }
        assert!(
            !notices.contains_key(reading_id),
            "admitted reading {reading_id} has a rejection notice"
        );
        let Some(EffectRecord::Reading {
            sensor,
            tick,
            occurred_at,
            timestamp_source,
            value,
            ..
        }) = effects.get(reading_id).copied()
        else {
            panic!("admitted reading {reading_id} has no effect");
        };
        assert_eq!(
            sensor, &reading.sensor,
            "reading {reading_id} changed sensor"
        );
        assert_eq!(*tick, reading.tick, "reading {reading_id} changed tick");
        assert_eq!(
            parse_timestamp(occurred_at),
            reading.occurred_at(),
            "reading {reading_id} changed its event time"
        );
        assert_eq!(*value, reading.value, "reading {reading_id} changed value");
        assert_eq!(
            timestamp_source, &reading.timestamps,
            "reading {reading_id} submitted to {} was admitted by another timestamp source",
            reading.ingestor
        );
    }
    let applied: BTreeSet<&String> = effects.keys().chain(notices.keys()).collect();
    let planned: BTreeSet<&String> = readings.keys().collect();
    assert_eq!(
        applied, planned,
        "the effect store holds a record of a reading the ledger never planned"
    );
}

/// The readings each sensor reported are stamped with consecutive tick centers one clock period
/// apart: the driver stepped through the ticks the clock reached without skipping one.
#[then("the paced simulation readings of every sensor occur one clock period apart")]
async fn then_the_readings_of_every_sensor_occur_one_period_apart(world: &mut ScenarioWorld) {
    let clock = world.paced_simulation.finished().first_paced_clock();
    let records = world.paced_simulation.records();
    let mut by_sensor: BTreeMap<String, BTreeMap<u64, Timestamp>> = BTreeMap::new();
    for reading in records.readings().into_values() {
        if reading.stamp != "center" {
            continue;
        }
        let occurred_at = reading.occurred_at();
        by_sensor
            .entry(reading.sensor)
            .or_default()
            .insert(reading.tick, occurred_at);
    }
    assert!(
        !by_sensor.is_empty(),
        "the ledger holds no reading stamped at a tick center"
    );
    for (sensor, ticks) in by_sensor {
        let mut previous: Option<(u64, Timestamp)> = None;
        for (tick, occurred_at) in ticks {
            if let Some((previous_tick, previous_at)) = previous {
                assert_eq!(tick, previous_tick + 1, "sensor {sensor} skipped a tick");
                assert_eq!(
                    occurred_at.duration_since(previous_at),
                    Some(clock.period),
                    "sensor {sensor} readings of ticks {previous_tick} and {tick} are not one \
                     period apart"
                );
            }
            previous = Some((tick, occurred_at));
        }
    }
}

/// The ingestor stamps `admitted_at` with the domain time it admitted a reading at. A reading
/// admitted no earlier than it occurred was not submitted before the clock reached its tick.
#[then("every paced simulation reading was admitted no earlier than it occurred")]
async fn then_every_reading_was_admitted_no_earlier_than_it_occurred(world: &mut ScenarioWorld) {
    let records = world.paced_simulation.records();
    let effects = records.reading_effects();
    assert!(!effects.is_empty(), "the effect store holds no reading");
    for (reading_id, effect) in effects {
        let EffectRecord::Reading {
            occurred_at,
            admitted_at,
            ..
        } = effect
        else {
            continue;
        };
        assert!(
            parse_timestamp(admitted_at) >= parse_timestamp(occurred_at),
            "reading {reading_id} occurred at {occurred_at} and was admitted earlier, at \
             {admitted_at}"
        );
    }
}

/// A driver that steps through `periods` further tick centers cannot finish before the clock
/// reaches the last of them, which at `time_rate` takes `periods × period / time_rate` of wall
/// time. Load only delays the driver, so the elapsed time only grows.
#[then(
    expr = "the paced simulation driver ran for at least {int} clock periods at time rate {string}"
)]
async fn then_the_driver_ran_for_at_least(
    world: &mut ScenarioWorld,
    periods: u32,
    time_rate: String,
) {
    let finished = world.paced_simulation.finished();
    let clock = finished.first_paced_clock();
    let time_rate: f64 = time_rate.parse().expect("the step names a time rate");
    let logical = clock.period * periods;
    let wall = logical.div_f64(time_rate);
    assert!(
        finished.elapsed >= wall,
        "{periods} periods of {:?} at time rate {time_rate} take at least {wall:?} of wall time, \
         and the driver finished in {:?}",
        clock.period,
        finished.elapsed
    );
}

/// Every reading a `TIMESTAMP AT` run stamped before the admission window reached the rejection
/// emitter as a validation notice naming it, and nothing else did.
#[then(
    "the paced simulation rejection notices name every reading its ledger stamped before the \
     admission window"
)]
async fn then_the_rejection_notices_name_every_reading_stamped_before_the_window(
    world: &mut ScenarioWorld,
) {
    let records = world.paced_simulation.records();
    let readings = records.readings();
    let notices = records.rejection_notices();
    let expected: BTreeSet<&String> = readings
        .values()
        .filter(|reading| reading.expects_rejection())
        .map(|reading| &reading.reading_id)
        .collect();
    assert!(
        !expected.is_empty(),
        "the ledger stamped no reading before the window"
    );
    let noticed: BTreeSet<&String> = notices.keys().collect();
    assert_eq!(
        noticed, expected,
        "the rejection notices name other readings"
    );
    for (reading_id, notice) in notices {
        let EffectRecord::Rejection {
            occurred_at,
            error_code,
            error_message,
            ..
        } = notice
        else {
            continue;
        };
        let reading = &readings[&reading_id];
        assert_eq!(parse_timestamp(occurred_at), reading.occurred_at());
        assert_eq!(error_code, "validation", "notice of {reading_id}");
        assert!(
            error_message.contains(OUTSIDE_WINDOW),
            "notice of {reading_id} says '{error_message}'"
        );
    }
}

/// A `TIMESTAMP NOW` ingestor ignores the event time, so a reading stamped before the admission
/// window is admitted at the domain time it arrived, later than its window allowed.
#[then(
    "the paced simulation effects show TIMESTAMP NOW admitting the readings stamped before the \
     admission window"
)]
async fn then_timestamp_now_admits_the_readings_stamped_before_the_window(
    world: &mut ScenarioWorld,
) {
    let clock = world.paced_simulation.finished().first_paced_clock();
    let records = world.paced_simulation.records();
    let effects = records.reading_effects();
    let stale: Vec<LedgerReading> = records
        .readings()
        .into_values()
        .filter(|reading| reading.timestamps == "now" && reading.stamp == "before_window")
        .collect();
    assert!(
        !stale.is_empty(),
        "no TIMESTAMP NOW run stamped a reading before the window"
    );
    for reading in stale {
        let Some(EffectRecord::Reading {
            occurred_at,
            admitted_at,
            timestamp_source,
            ..
        }) = effects.get(&reading.reading_id).copied()
        else {
            panic!("TIMESTAMP NOW did not admit {}", reading.reading_id);
        };
        assert_eq!(timestamp_source, "now");
        let occurred_at = parse_timestamp(occurred_at);
        let admitted_at = parse_timestamp(admitted_at);
        let late = admitted_at
            .duration_since(occurred_at)
            .unwrap_or_else(|| panic!("{} was admitted before it occurred", reading.reading_id));
        assert!(
            late > clock.skew,
            "{} was admitted {late:?} after it occurred, within the skew",
            reading.reading_id
        );
    }
}

/// Each reading that completed was applied exactly once, by a consumer attached under the START
/// generation the reading was planned in.
#[then("every completed paced simulation reading has exactly one effect from its own generation")]
async fn then_every_completed_reading_has_one_effect_from_its_generation(
    world: &mut ScenarioWorld,
) {
    let records = world.paced_simulation.records();
    let readings = records.readings();
    let outcomes = records.outcomes();
    let effects = records.reading_effects();
    let mut completed = 0_u32;
    for (reading_id, reading) in &readings {
        let outcome = outcomes
            .get(reading_id)
            .unwrap_or_else(|| panic!("reading {reading_id} has no outcome"));
        if !outcome.completed() {
            continue;
        }
        completed += 1;
        let Some(EffectRecord::Reading { generation, .. }) = effects.get(reading_id).copied()
        else {
            panic!("completed reading {reading_id} has no effect");
        };
        assert_eq!(
            *generation, reading.generation,
            "reading {reading_id} of generation {} was applied under generation {generation}",
            reading.generation
        );
    }
    assert!(completed > 0, "no reading completed");
    for reading_id in effects.keys() {
        assert!(
            readings.contains_key(reading_id),
            "the effect store applied {reading_id}, which the ledger never planned"
        );
    }
}

#[then(
    expr = "the paced simulation ledger submits no reading of generation {int} after generation \
            {int} begins"
)]
async fn then_the_ledger_submits_no_earlier_generation(
    world: &mut ScenarioWorld,
    earlier: u64,
    later: u64,
) {
    let records = world.paced_simulation.records();
    let mut later_began = false;
    let mut later_readings = 0_u32;
    for record in &records.ledger {
        match record {
            LedgerRecord::Generation { generation } if *generation == later => later_began = true,
            LedgerRecord::Reading(reading) if later_began => {
                assert_ne!(
                    reading.generation, earlier,
                    "reading {} of generation {earlier} was submitted after generation {later} \
                     began",
                    reading.reading_id
                );
                later_readings += 1;
            }
            LedgerRecord::Generation { .. }
            | LedgerRecord::Reading(_)
            | LedgerRecord::Outcome { .. } => {}
        }
    }
    assert!(later_began, "generation {later} never began");
    assert!(
        later_readings > 0,
        "no reading was submitted in generation {later}"
    );
}

/// The emitter holds output nobody can take: no consumer is attached, and it retains batches.
#[then(
    expr = "within {string} the leader node describes emitter {string} retaining output for no \
            consumer"
)]
async fn then_the_emitter_retains_output_for_no_consumer(
    world: &mut ScenarioWorld,
    within: String,
    emitter: String,
) {
    let within = parse_duration_text(&within).expect("the step names a duration");
    let emitter = expand_placeholders(world, &emitter);
    let deadline = Instant::now() + within;
    loop {
        nervix_primitives::task::consume_budget().await;
        let leader = running_leader_node(world).await;
        let output = world
            .cluster()
            .run_command(
                &leader,
                &world.domain,
                &format!("DESCRIBE EMITTER {emitter};"),
            )
            .await
            .unwrap_or_else(|error| format!("DESCRIBE failed: {error}"));
        let mut consumers = None;
        let mut retained = None;
        for line in output.lines() {
            let line = line.trim();
            if let Some(count) = line.strip_prefix("consumers: ") {
                consumers = count.parse::<u64>().ok();
            }
            if let Some(count) = line.strip_prefix("retained batches: ") {
                retained = count.parse::<u64>().ok();
            }
        }
        if consumers == Some(0) && retained.is_some_and(|retained| retained > 0) {
            world.last_command_output = Some(output);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "DESCRIBE EMITTER {emitter} never retained output for no consumer; last \
             output:\n{output}"
        );
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    }
}
