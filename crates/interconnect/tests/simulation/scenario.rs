//! Seed selection, failure records and fresh-process replay for the interconnect simulations.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** Each scenario's identity and committed regression seeds, the wider seed sweep, the
//!   determinism check that runs every attempt in a fresh process of the test binary, the record a
//!   failed run leaves behind, the replay of that record in a fresh process, and the harness failure
//!   injected to prove that replay works.
//! - **Depends on.** The bounded runner and its semantic trace, and the source revision, lockfile
//!   and toolchain of the build that ran.
//! - **Must not know.** What a scenario asserts, or its fixtures' payloads and credentials.

use std::{
    env::{self, VarError},
    fmt::{self, Write as _},
    fs::{self, File},
    io,
    ops::Range,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_recovery::Discarded as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::runner::{PanicReport, SemanticTrace, SimulationConfig, SimulationError, TraceEvent};

/// The directory failure records are written to. Unset, they go under the build's scratch
/// directory, `CARGO_TARGET_TMPDIR`.
const FAILURES_VARIABLE: &str = "NERVIX_TURMOIL_FAILURES";
/// A failure record to replay instead of running any seeds.
const REPLAY_VARIABLE: &str = "NERVIX_TURMOIL_REPLAY";
/// A seed range, `<first>..<end>`, that replaces every scenario's committed seeds.
const SWEEP_VARIABLE: &str = "NERVIX_TURMOIL_SWEEP";
/// A simulated time, such as `5s`, at which an extra harness host fails every run.
const INJECT_VARIABLE: &str = "NERVIX_TURMOIL_INJECT_FAILURE";
/// Set by the driver on the fresh process it starts for one attempt: the attempt to run.
const ATTEMPT_VARIABLE: &str = "NERVIX_TURMOIL_ATTEMPT";
/// Real time an attempt's process has beyond its run's own wall bound: to start, to report, and to
/// exit. Past it the driver kills the process.
const ATTEMPT_PROCESS_MARGIN: Duration = Duration::from_secs(30);
/// How often the driver checks whether an attempt's process has exited.
const ATTEMPT_POLL: Duration = Duration::from_millis(5);
/// How much of an unreported attempt's output its record keeps.
const OUTPUT_TAIL_LINES: usize = 40;
/// The workspace root, where the lockfile and the toolchain file live.
const WORKSPACE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
/// The line a replay prints when it reproduced the recorded outcome and every recorded event.
const REPRODUCED: &str = "turmoil replay: reproduced the recorded outcome and trace";

/// Why the driver could not run a scenario at all, as opposed to a scenario that ran and failed.
#[derive(Debug, Error)]
enum DriverError {
    #[error("{variable} is not valid Unicode")]
    NotUnicode { variable: &'static str },
    #[error("{SWEEP_VARIABLE}={value:?} is not a nonempty seed range `<first>..<end>`")]
    InvalidSweep { value: String },
    #[error("{INJECT_VARIABLE}={value:?} is not a nonzero simulated time such as `5s`")]
    InvalidInjection { value: String },
    #[error(
        "{REPLAY_VARIABLE} replays exactly the recorded inputs, so {conflicting} must be unset"
    )]
    ReplayConflict { conflicting: &'static str },
    #[error("scenario {case:?} has no committed regression seeds")]
    NoCommittedSeeds { case: &'static str },
    #[error("cannot read failure record {path}: {source}")]
    ReadRecord {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{path} is not a failure record this build reads: {source}")]
    ParseRecord {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("cannot encode the failure record for {path}: {source}")]
    EncodeRecord {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("cannot write failure record {path}: {source}")]
    WriteRecord {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot prepare an attempt of {test}: {source}")]
    PrepareAttempt {
        test: String,
        #[source]
        source: io::Error,
    },
    #[error("cannot encode the attempt of {test}: {source}")]
    EncodeAttempt {
        test: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("cannot supervise the attempt process of {test}: {source}")]
    SuperviseAttempt {
        test: String,
        #[source]
        source: io::Error,
    },
    #[error("{test}: the wall bound {wall_duration:?} leaves the attempt process no deadline")]
    UnboundedAttempt {
        test: String,
        wall_duration: Duration,
    },
    #[error("the attempt process of {test} passed without running case {case:?}")]
    AttemptNotRun { test: String, case: String },
    #[error("cannot read the attempt {path}: {source}")]
    ReadAttempt {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{path} is not an attempt this build reads: {source}")]
    ParseAttempt {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("cannot report the attempt to {path}: {source}")]
    ReportAttempt {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// One replayable scenario of a simulation test.
pub(super) struct Scenario {
    /// The case's name, unique within its test. A failure record selects the case by it.
    pub name: &'static str,
    /// What the case does to links and hosts, kept in a failure record for its reader.
    pub fault_plan: &'static str,
    /// The committed regression seeds every ordinary run checks.
    pub seeds: &'static [u64],
}

impl Scenario {
    /// Run the scenario in the mode the environment selects.
    ///
    /// Ordinarily each committed seed runs twice, each time in a fresh process of this test binary,
    /// and the two traces must agree; a sweep does the same over its seed range instead. A failed
    /// run leaves a failure record and fails the test with the command that replays the record. A
    /// replay runs only the recorded case, once, with exactly the recorded inputs, and compares what
    /// it observes with the record. `configure` builds a seed's inputs and `exercise` runs them.
    pub fn check<C, E>(&self, configure: C, exercise: E)
    where
        C: Fn(u64) -> SimulationConfig,
        E: Fn(ScenarioRun) -> Result<(), SimulationError>,
    {
        if let Err(error) = self.drive(&configure, &exercise) {
            panic!("simulation scenario {:?}: {error}", self.name);
        }
    }

    fn drive<C, E>(&self, configure: &C, exercise: &E) -> Result<(), DriverError>
    where
        C: Fn(u64) -> SimulationConfig,
        E: Fn(ScenarioRun) -> Result<(), SimulationError>,
    {
        PanicReport::capture();
        let identity = ScenarioIdentity::of(self);
        match Mode::from_variables(&Mode::environment_variable)? {
            Mode::Explore { seeds, injection } => {
                let store = RecordStore::from_variables(&Mode::environment_variable)?;
                for seed in seeds.for_scenario(self)? {
                    let inputs = RecordedInputs {
                        config: configure(seed),
                        injected_failure: injection,
                    };
                    self.explore(&identity, &store, inputs)?;
                }
            }
            Mode::Replay(replay) => {
                if replay.record.scenario.selects(&identity) {
                    replay.run(self, &identity, exercise);
                }
            }
            Mode::Attempt(request) => {
                if request.scenario.selects(&identity) {
                    let attempt = self.attempt(request.inputs, exercise);
                    request.report(&attempt)?;
                }
            }
        }
        Ok(())
    }

    /// Run one seed twice. The record written first marks the run in progress, so a process
    /// ended by its real-time budget still leaves the inputs of the run it was in.
    fn explore(
        &self,
        identity: &ScenarioIdentity,
        store: &RecordStore,
        inputs: RecordedInputs,
    ) -> Result<(), DriverError> {
        let path = store.path(identity, inputs.config.seed);
        let mut record = FailureRecord {
            scenario: identity.clone(),
            build: BuildFingerprint::current().clone(),
            inputs,
            run: RunOrdinal::First,
            outcome: Outcome::Running,
            events: Vec::new(),
        };
        store.write(&path, &record)?;

        let first = AttemptRequest::isolate(identity, inputs)?;
        if !first.outcome.passed() {
            record.outcome = first.outcome;
            record.events = first.events;
            record.fail(store, &path);
        }
        let repeat = AttemptRequest::isolate(identity, inputs)?;
        if !repeat.outcome.passed() {
            record.run = RunOrdinal::Repeat;
            record.outcome = repeat.outcome;
            record.events = repeat.events;
            record.fail(store, &path);
        }
        if let Some(at_event) = RecordedEvent::first_difference(&first.events, &repeat.events) {
            record.outcome = Outcome::Diverged {
                at_event,
                first: first.events.get(at_event).cloned(),
                repeat: repeat.events.get(at_event).cloned(),
            };
            record.events = first.events;
            record.fail(store, &path);
        }
        store.remove(&path)
    }

    /// Run the scenario once with `inputs`, supervising panics on this thread as well as the
    /// simulation's own failures.
    fn attempt<E>(&self, inputs: RecordedInputs, exercise: &E) -> Attempt
    where
        E: Fn(ScenarioRun) -> Result<(), SimulationError>,
    {
        let trace = SemanticTrace::default();
        let run = ScenarioRun {
            name: self.name,
            inputs,
            trace: trace.clone(),
        };
        PanicReport::take().discarded("a panic before this attempt belongs to an earlier one");
        let result = panic::catch_unwind(AssertUnwindSafe(|| exercise(run)));
        let outcome = match result {
            Ok(Ok(())) => Outcome::Passed,
            Ok(Err(error)) => Outcome::Failed {
                error: error.to_string(),
            },
            Err(payload) => Outcome::Panicked {
                panic: PanicReport::caught(&*payload),
            },
        };
        let events = trace.events().iter().map(RecordedEvent::from).collect();
        Attempt { outcome, events }
    }
}

/// One run of a scenario: its exact inputs and the trace its hosts record into.
pub(super) struct ScenarioRun {
    name: &'static str,
    inputs: RecordedInputs,
    trace: SemanticTrace,
}

impl ScenarioRun {
    pub fn seed(&self) -> u64 {
        self.inputs.config.seed
    }

    /// A handle to the run's trace, for the hosts that record into it and for checks after it.
    pub fn trace(&self) -> SemanticTrace {
        self.trace.clone()
    }

    pub fn simulate<F>(self, setup: F) -> Result<(), SimulationError>
    where
        F: for<'a> FnOnce(&mut turmoil::Sim<'a>) + Send + 'static,
    {
        self.simulate_with_control(setup, |_| {})
    }

    /// Run the simulation, observing each completed step as the runner's control does.
    pub fn simulate_with_control<F, C>(self, setup: F, control: C) -> Result<(), SimulationError>
    where
        F: for<'a> FnOnce(&mut turmoil::Sim<'a>) + Send + 'static,
        C: for<'a> FnMut(&mut turmoil::Sim<'a>) + Send + 'static,
    {
        let injection = self.inputs.injected_failure;
        self.inputs.config.run_with_control(
            self.name,
            move |simulation| {
                setup(simulation);
                if let Some(injection) = injection {
                    injection.install(simulation);
                }
            },
            control,
        )
    }
}

/// What the environment asks the driver to do.
enum Mode {
    /// Run seeds and record any failure.
    Explore {
        seeds: Seeds,
        injection: Option<InjectedFailure>,
    },
    /// Run one recorded failure again.
    Replay(Box<Replay>),
    /// Run one attempt for the driver that started this process, and report what it observed.
    Attempt(Box<AttemptRequest>),
}

impl Mode {
    /// The mode `variable` selects; the driver reads the process environment through it.
    fn from_variables(
        variable: &impl Fn(&'static str) -> Result<Option<String>, DriverError>,
    ) -> Result<Self, DriverError> {
        // The driver clears every other variable when it starts an attempt's process.
        if let Some(path) = variable(ATTEMPT_VARIABLE)? {
            let request = AttemptRequest::load(PathBuf::from(path))?;
            return Ok(Self::Attempt(Box::new(request)));
        }
        let replay = variable(REPLAY_VARIABLE)?;
        let sweep = variable(SWEEP_VARIABLE)?;
        let injection = variable(INJECT_VARIABLE)?;
        if let Some(path) = replay {
            if sweep.is_some() {
                return Err(DriverError::ReplayConflict {
                    conflicting: SWEEP_VARIABLE,
                });
            }
            if injection.is_some() {
                return Err(DriverError::ReplayConflict {
                    conflicting: INJECT_VARIABLE,
                });
            }
            let replay = Replay::load(PathBuf::from(path))?;
            return Ok(Self::Replay(Box::new(replay)));
        }
        let seeds = match sweep {
            Some(value) => Seeds::Sweep(Seeds::parse_sweep(&value)?),
            None => Seeds::Committed,
        };
        let injection = match injection {
            Some(value) => Some(InjectedFailure::parse(&value)?),
            None => None,
        };
        Ok(Self::Explore { seeds, injection })
    }

    fn environment_variable(name: &'static str) -> Result<Option<String>, DriverError> {
        match env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(VarError::NotPresent) => Ok(None),
            Err(VarError::NotUnicode(_)) => Err(DriverError::NotUnicode { variable: name }),
        }
    }
}

/// Which seeds an exploring run checks.
enum Seeds {
    /// Each scenario's committed regression seeds.
    Committed,
    /// One range of seeds for every scenario.
    Sweep(Range<u64>),
}

impl Seeds {
    fn parse_sweep(value: &str) -> Result<Range<u64>, DriverError> {
        let invalid = || DriverError::InvalidSweep {
            value: value.to_string(),
        };
        let Some((first, end)) = value.split_once("..") else {
            return Err(invalid());
        };
        let first = first.trim().parse::<u64>().map_err(|_| invalid())?;
        let end = end.trim().parse::<u64>().map_err(|_| invalid())?;
        if first >= end {
            return Err(invalid());
        }
        Ok(first..end)
    }

    fn for_scenario(&self, scenario: &Scenario) -> Result<Vec<u64>, DriverError> {
        match self {
            Self::Committed => {
                if scenario.seeds.is_empty() {
                    return Err(DriverError::NoCommittedSeeds {
                        case: scenario.name,
                    });
                }
                Ok(scenario.seeds.to_vec())
            }
            Self::Sweep(range) => Ok(range.clone().collect()),
        }
    }
}

/// An extra harness host that fails at a fixed simulated time.
///
/// It proves that a failed run leaves a record and that the record reproduces the failure in a
/// fresh process. It is one of the run's inputs, so a replay injects it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct InjectedFailure {
    at: Duration,
}

impl InjectedFailure {
    /// The injected host's name. Turmoil derives addresses and ordering from the host names, so
    /// the name is part of what a replay repeats.
    const HOST: &'static str = "injected-failure";

    fn parse(value: &str) -> Result<Self, DriverError> {
        let invalid = || DriverError::InvalidInjection {
            value: value.to_string(),
        };
        let at = humantime::parse_duration(value).map_err(|_| invalid())?;
        if at.is_zero() {
            return Err(invalid());
        }
        Ok(Self { at })
    }

    fn install(self, simulation: &mut turmoil::Sim<'_>) {
        simulation.client(Self::HOST, async move {
            tokio::time::sleep(self.at).await;
            Err(format!("injected harness failure at {:?} simulated time", self.at).into())
        });
    }
}

/// The inputs a run is built from, all of which a replay repeats.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct RecordedInputs {
    config: SimulationConfig,
    injected_failure: Option<InjectedFailure>,
}

/// Which scenario a record belongs to, and how Cargo and libtest select it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ScenarioIdentity {
    /// The Cargo package, as `--package` names it.
    package: String,
    /// The test target, as `--test` names it.
    target: String,
    /// The libtest name of the test, as `--exact` selects it.
    test: String,
    /// The scenario within that test.
    case: String,
    fault_plan: String,
}

impl ScenarioIdentity {
    fn of(scenario: &Scenario) -> Self {
        let test = std::thread::current()
            .name()
            .assured("libtest runs every test on a thread named after the test")
            .to_string();
        Self {
            package: env!("CARGO_PKG_NAME").to_string(),
            target: env!("CARGO_CRATE_NAME").to_string(),
            test,
            case: scenario.name.to_string(),
            fault_plan: scenario.fault_plan.to_string(),
        }
    }

    /// Whether `other` is the same case of the same test in the same test binary.
    fn selects(&self, other: &Self) -> bool {
        self.package == other.package
            && self.target == other.target
            && self.test == other.test
            && self.case == other.case
    }

    /// Lowercase ASCII letters and digits, with each run of other characters as one dash.
    fn slug(text: &str) -> String {
        let mut slug = String::new();
        let mut separated = false;
        for character in text.chars() {
            if character.is_ascii_alphanumeric() {
                if separated && !slug.is_empty() {
                    slug.push('-');
                }
                separated = false;
                slug.push(character.to_ascii_lowercase());
            } else {
                separated = true;
            }
        }
        slug
    }
}

/// The build a record was written by. A replay compares it with its own build and reports every
/// difference, because a different source, lockfile or toolchain may legitimately change a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BuildFingerprint {
    /// The checked-out commit, when Git can say.
    source: Option<SourceRevision>,
    /// SHA-256 of the workspace `Cargo.lock`.
    lockfile_sha256: Option<String>,
    /// `rustc -vV` of the toolchain the workspace selects.
    toolchain: Option<String>,
    /// Whether Tokio's seeded scheduling and panic shutdown were compiled in.
    tokio_unstable: bool,
    /// Whether debug assertions, and with them overflow checks, were compiled in.
    debug_assertions: bool,
}

impl BuildFingerprint {
    /// This process's build, computed once.
    fn current() -> &'static Self {
        static CURRENT: OnceLock<BuildFingerprint> = OnceLock::new();
        CURRENT.get_or_init(|| Self {
            source: SourceRevision::current(),
            lockfile_sha256: Self::lockfile_sha256(),
            toolchain: Self::probe("rustc", &["-vV"]),
            tokio_unstable: cfg!(tokio_unstable),
            debug_assertions: cfg!(debug_assertions),
        })
    }

    fn lockfile_sha256() -> Option<String> {
        let lockfile = fs::read(Path::new(WORKSPACE).join("Cargo.lock")).ok()?;
        Some(format!("{:x}", Sha256::digest(lockfile)))
    }

    /// The trimmed standard output of a command that succeeded in the workspace.
    fn probe(program: &str, arguments: &[&str]) -> Option<String> {
        let output = Command::new(program)
            .args(arguments)
            .current_dir(WORKSPACE)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let stdout = String::from_utf8(output.stdout).ok()?;
        Some(stdout.trim().to_string())
    }

    /// One line for each way `current` differs from this recorded build.
    fn differences(&self, current: &Self) -> Vec<String> {
        let mut differences = Vec::new();
        if self.source != current.source {
            differences.push(format!(
                "source: recorded {}, current {}",
                or_unknown(self.source.as_ref()),
                or_unknown(current.source.as_ref())
            ));
        }
        if self.lockfile_sha256 != current.lockfile_sha256 {
            differences.push(format!(
                "Cargo.lock SHA-256: recorded {}, current {}",
                or_unknown(self.lockfile_sha256.as_ref()),
                or_unknown(current.lockfile_sha256.as_ref())
            ));
        }
        if self.toolchain != current.toolchain {
            differences.push(format!(
                "toolchain: recorded {:?}, current {:?}",
                self.toolchain, current.toolchain
            ));
        }
        if self.tokio_unstable != current.tokio_unstable {
            differences.push(format!(
                "tokio_unstable: recorded {}, current {}",
                self.tokio_unstable, current.tokio_unstable
            ));
        }
        if self.debug_assertions != current.debug_assertions {
            differences.push(format!(
                "debug assertions: recorded {}, current {}",
                self.debug_assertions, current.debug_assertions
            ));
        }
        differences
    }
}

/// A commit and whether tracked files differed from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SourceRevision {
    commit: String,
    /// Tracked files had uncommitted changes, so the commit alone does not rebuild the source.
    modified: bool,
}

impl SourceRevision {
    fn current() -> Option<Self> {
        let commit = BuildFingerprint::probe("git", &["rev-parse", "HEAD"])?;
        let status =
            BuildFingerprint::probe("git", &["status", "--porcelain", "--untracked-files=no"])?;
        Some(Self {
            commit,
            modified: !status.is_empty(),
        })
    }
}

impl fmt::Display for SourceRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.modified {
            write!(formatter, "{} with modified tracked files", self.commit)
        } else {
            write!(formatter, "{}", self.commit)
        }
    }
}

/// How one run ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Outcome {
    /// The run had started when its process ended, for example at the real-time budget of the
    /// whole test run; the record holds its inputs.
    Running,
    Passed,
    /// The runner returned an error: a failed host, an exhausted bound, or a scheduler panic.
    Failed {
        error: String,
    },
    /// The scenario panicked on the test thread, around the simulation.
    Panicked {
        panic: PanicReport,
    },
    /// Two runs of the same inputs recorded different traces, first at event `at_event`.
    Diverged {
        at_event: usize,
        first: Option<RecordedEvent>,
        repeat: Option<RecordedEvent>,
    },
    /// The attempt's process ended without reporting what it observed: it aborted, or it outlived
    /// its real-time bound and was killed. `output` is the end of what it printed.
    Unreported {
        ending: String,
        output: String,
    },
}

impl Outcome {
    fn passed(&self) -> bool {
        matches!(self, Self::Passed)
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => write!(formatter, "still running when its process ended"),
            Self::Passed => write!(formatter, "passed"),
            Self::Failed { error } => write!(formatter, "failed: {error}"),
            Self::Panicked { panic } => write!(formatter, "panicked: {panic}"),
            Self::Diverged {
                at_event,
                first,
                repeat,
            } => write!(
                formatter,
                "two runs diverged at event {at_event}: first {}, repeat {}",
                or_unknown(first.as_ref()),
                or_unknown(repeat.as_ref())
            ),
            Self::Unreported { ending, output } => write!(
                formatter,
                "its process {ending} without reporting an outcome; its output ended \
                 with:\n{output}"
            ),
        }
    }
}

/// Which run of a seed's determinism check a record describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum RunOrdinal {
    First,
    /// The second run, after the first passed: its failure did not happen on the first run.
    Repeat,
}

/// A semantic trace event as a record stores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RecordedEvent {
    at: Duration,
    host: String,
    event: String,
}

impl RecordedEvent {
    /// The index of the first event at which two traces differ, if they differ at all.
    fn first_difference(first: &[Self], second: &[Self]) -> Option<usize> {
        for (index, (left, right)) in first.iter().zip(second).enumerate() {
            if left != right {
                return Some(index);
            }
        }
        if first.len() == second.len() {
            return None;
        }
        Some(first.len().min(second.len()))
    }
}

impl From<&TraceEvent> for RecordedEvent {
    fn from(event: &TraceEvent) -> Self {
        Self {
            at: event.at,
            host: event.host.to_string(),
            event: event.event.clone(),
        }
    }
}

impl fmt::Display for RecordedEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:.3}s {} {}",
            self.at.as_secs_f64(),
            self.host,
            self.event
        )
    }
}

/// What one run observed.
#[derive(Debug, Serialize, Deserialize)]
struct Attempt {
    outcome: Outcome,
    events: Vec<RecordedEvent>,
}

/// Everything needed to understand a failed run and to repeat it in a fresh process.
///
/// It holds identities, inputs, outcomes and semantic events only: never a fixture's certificate
/// or key material, and never a payload value.
#[derive(Debug, Serialize, Deserialize)]
struct FailureRecord {
    scenario: ScenarioIdentity,
    build: BuildFingerprint,
    inputs: RecordedInputs,
    run: RunOrdinal,
    outcome: Outcome,
    /// The trace of the run named by `run`, as far as it got.
    events: Vec<RecordedEvent>,
}

impl FailureRecord {
    /// Write the record and fail the test with a summary and the command that replays it.
    fn fail(&self, store: &RecordStore, path: &Path) -> ! {
        let summary = self.summary();
        match store.write(path, self) {
            Ok(()) => panic!(
                "{summary}failure record: {}\nreplay it in a fresh process: just \
                 test-turmoil-replay {}",
                path.display(),
                path.display()
            ),
            Err(error) => panic!("{summary}{error}"),
        }
    }

    fn summary(&self) -> String {
        let mut summary = String::new();
        writeln!(
            summary,
            "simulation {} / {:?} seed {} ({} run) {}",
            self.scenario.test, self.scenario.case, self.inputs.config.seed, self.run, self.outcome
        )
        .assured("writing to a String cannot fail");
        for event in &self.events {
            writeln!(summary, "  {event}").assured("writing to a String cannot fail");
        }
        summary
    }
}

/// Where failure records are written.
struct RecordStore {
    directory: PathBuf,
}

impl RecordStore {
    fn from_variables(
        variable: &impl Fn(&'static str) -> Result<Option<String>, DriverError>,
    ) -> Result<Self, DriverError> {
        let directory = match variable(FAILURES_VARIABLE)? {
            Some(directory) => PathBuf::from(directory),
            None => Path::new(env!("CARGO_TARGET_TMPDIR")).join("turmoil-failures"),
        };
        Ok(Self { directory })
    }

    fn path(&self, identity: &ScenarioIdentity, seed: u64) -> PathBuf {
        self.directory
            .join(ScenarioIdentity::slug(&identity.test))
            .join(format!(
                "{}-seed-{seed}.json",
                ScenarioIdentity::slug(&identity.case)
            ))
    }

    fn write(&self, path: &Path, record: &FailureRecord) -> Result<(), DriverError> {
        let directory = path
            .parent()
            .assured("a record path is inside its test's directory");
        let written = fs::create_dir_all(directory);
        written.map_err(|source| DriverError::WriteRecord {
            path: path.to_path_buf(),
            source,
        })?;
        let encoded =
            serde_json::to_string_pretty(record).map_err(|source| DriverError::EncodeRecord {
                path: path.to_path_buf(),
                source,
            })?;
        fs::write(path, encoded).map_err(|source| DriverError::WriteRecord {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Remove the in-progress record of a run that passed, and its test's directory once empty.
    fn remove(&self, path: &Path) -> Result<(), DriverError> {
        fs::remove_file(path).map_err(|source| DriverError::WriteRecord {
            path: path.to_path_buf(),
            source,
        })?;
        let directory = path
            .parent()
            .assured("a record path is inside its test's directory");
        fs::remove_dir(directory)
            .discarded("a test's directory still holding another seed's record stays");
        Ok(())
    }
}

/// One attempt the driver runs in a fresh process of this test binary.
///
/// Tokio numbers tasks across the whole process and tears a runtime's tasks down in an order
/// derived from those numbers, so a simulated crash closes a host's connections in an order that
/// depends on every task the process created before. A fresh process gives every attempt, and
/// every replay, the same starting state; it also lets the driver kill an attempt that outlives
/// its bound.
#[derive(Debug, Serialize, Deserialize)]
struct AttemptRequest {
    scenario: ScenarioIdentity,
    inputs: RecordedInputs,
    /// Where the attempt's process writes what it observed.
    result: PathBuf,
}

impl AttemptRequest {
    /// Run `inputs` once in a fresh process and collect what that process observed.
    fn isolate(
        identity: &ScenarioIdentity,
        inputs: RecordedInputs,
    ) -> Result<Attempt, DriverError> {
        let prepare = |source| DriverError::PrepareAttempt {
            test: identity.test.clone(),
            source,
        };
        let scratch = tempfile::tempdir().map_err(prepare)?;
        let request = Self {
            scenario: identity.clone(),
            inputs,
            result: scratch.path().join("attempt.json"),
        };
        let encoded =
            serde_json::to_string(&request).map_err(|source| DriverError::EncodeAttempt {
                test: identity.test.clone(),
                source,
            })?;
        let request_path = scratch.path().join("request.json");
        fs::write(&request_path, encoded).map_err(prepare)?;
        let output_path = scratch.path().join("output.log");
        let ending = request.run_process(&request_path, &output_path)?;
        request.collect(ending, &output_path)
    }

    /// Start this test binary on the request and wait for it within the run's wall bound plus the
    /// process margin, killing it past that.
    fn run_process(
        &self,
        request_path: &Path,
        output_path: &Path,
    ) -> Result<AttemptEnding, DriverError> {
        let test = &self.scenario.test;
        let prepare = |source| DriverError::PrepareAttempt {
            test: test.clone(),
            source,
        };
        let supervise = |source| DriverError::SuperviseAttempt {
            test: test.clone(),
            source,
        };
        let output = File::create(output_path).map_err(prepare)?;
        let errors = output.try_clone().map_err(prepare)?;
        let executable = env::current_exe().map_err(prepare)?;
        let mut command = Command::new(executable);
        command
            .args([
                test.as_str(),
                "--exact",
                "--include-ignored",
                "--test-threads=1",
                "--nocapture",
            ])
            .stdin(Stdio::null())
            .stdout(output)
            .stderr(errors)
            .env(ATTEMPT_VARIABLE, request_path);
        for variable in [
            FAILURES_VARIABLE,
            REPLAY_VARIABLE,
            SWEEP_VARIABLE,
            INJECT_VARIABLE,
        ] {
            command.env_remove(variable);
        }

        let wall_duration = self.inputs.config.bounds.wall_duration;
        let unbounded = || DriverError::UnboundedAttempt {
            test: test.clone(),
            wall_duration,
        };
        let bound = wall_duration
            .checked_add(ATTEMPT_PROCESS_MARGIN)
            .ok_or_else(unbounded)?;
        let deadline = Instant::now().checked_add(bound).ok_or_else(unbounded)?;
        let mut child = command.spawn().map_err(supervise)?;
        loop {
            if let Some(status) = child.try_wait().map_err(supervise)? {
                return Ok(AttemptEnding::Exited(status));
            }
            if Instant::now() >= deadline {
                child.kill().map_err(supervise)?;
                child.wait().map_err(supervise)?;
                return Ok(AttemptEnding::Killed(bound));
            }
            thread::sleep(ATTEMPT_POLL);
        }
    }

    /// Read what the attempt's process reported. A process that ended without a report yields an
    /// unreported outcome with the end of its output; one that passed without running the case is
    /// a driver defect.
    fn collect(&self, ending: AttemptEnding, output_path: &Path) -> Result<Attempt, DriverError> {
        let text = match fs::read_to_string(&self.result) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if let AttemptEnding::Exited(status) = ending
                    && status.success()
                {
                    return Err(DriverError::AttemptNotRun {
                        test: self.scenario.test.clone(),
                        case: self.scenario.case.clone(),
                    });
                }
                return Ok(Attempt {
                    outcome: Outcome::Unreported {
                        ending: ending.to_string(),
                        output: Self::output_tail(output_path),
                    },
                    events: Vec::new(),
                });
            }
            Err(source) => {
                return Err(DriverError::ReadAttempt {
                    path: self.result.clone(),
                    source,
                });
            }
        };
        serde_json::from_str(&text).map_err(|source| DriverError::ParseAttempt {
            path: self.result.clone(),
            source,
        })
    }

    fn load(path: PathBuf) -> Result<Self, DriverError> {
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(source) => return Err(DriverError::ReadAttempt { path, source }),
        };
        match serde_json::from_str(&text) {
            Ok(request) => Ok(request),
            Err(source) => Err(DriverError::ParseAttempt { path, source }),
        }
    }

    /// Write what this process observed where the driver reads it.
    fn report(&self, attempt: &Attempt) -> Result<(), DriverError> {
        let encoded =
            serde_json::to_string(attempt).map_err(|source| DriverError::EncodeAttempt {
                test: self.scenario.test.clone(),
                source,
            })?;
        fs::write(&self.result, encoded).map_err(|source| DriverError::ReportAttempt {
            path: self.result.clone(),
            source,
        })
    }

    /// The last lines an attempt's process printed.
    fn output_tail(path: &Path) -> String {
        let output = match fs::read(path) {
            Ok(output) => String::from_utf8_lossy(&output).into_owned(),
            Err(error) => return format!("its output is unreadable: {error}"),
        };
        let mut tail: Vec<&str> = output.lines().rev().take(OUTPUT_TAIL_LINES).collect();
        tail.reverse();
        tail.join("\n")
    }
}

/// How an attempt's process ended.
#[derive(Debug, Clone, Copy)]
enum AttemptEnding {
    Exited(ExitStatus),
    /// It outlived its real-time bound, and the driver killed it.
    Killed(Duration),
}

impl fmt::Display for AttemptEnding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exited(status) => write!(formatter, "exited with {status}"),
            Self::Killed(bound) => write!(
                formatter,
                "was killed after exceeding its {bound:?} real-time bound"
            ),
        }
    }
}

/// A failure record being replayed.
struct Replay {
    path: PathBuf,
    record: FailureRecord,
}

impl Replay {
    fn load(path: PathBuf) -> Result<Self, DriverError> {
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(source) => return Err(DriverError::ReadRecord { path, source }),
        };
        match serde_json::from_str(&text) {
            Ok(record) => Ok(Self { path, record }),
            Err(source) => Err(DriverError::ParseRecord { path, source }),
        }
    }

    /// Run the recorded inputs once. A replay that fails, whether as recorded or otherwise, fails
    /// the test; one that passes prints why the record did not reproduce.
    fn run<E>(&self, scenario: &Scenario, identity: &ScenarioIdentity, exercise: &E)
    where
        E: Fn(ScenarioRun) -> Result<(), SimulationError>,
    {
        let attempt = scenario.attempt(self.record.inputs, exercise);
        let report = self.report(identity, &attempt);
        if attempt.outcome.passed() {
            println!("{report}");
        } else {
            panic!("{report}");
        }
    }

    fn report(&self, identity: &ScenarioIdentity, attempt: &Attempt) -> String {
        let mut report = String::new();
        let recorded = &self.record;
        let mut line = |text: String| {
            writeln!(report, "{text}").assured("writing to a String cannot fail");
        };
        line(format!(
            "turmoil replay of {}: {} / {:?} seed {}",
            self.path.display(),
            recorded.scenario.test,
            recorded.scenario.case,
            recorded.inputs.config.seed
        ));
        for difference in recorded.build.differences(BuildFingerprint::current()) {
            line(format!("build differs: {difference}"));
        }
        if recorded.scenario.fault_plan != identity.fault_plan {
            line(format!(
                "fault plan differs: recorded {:?}, current {:?}",
                recorded.scenario.fault_plan, identity.fault_plan
            ));
        }
        line(format!(
            "recorded outcome ({} run): {}",
            recorded.run, recorded.outcome
        ));
        line(format!("replayed outcome: {}", attempt.outcome));
        let divergence = RecordedEvent::first_difference(&recorded.events, &attempt.events);
        match divergence {
            None => line(format!(
                "trace: all {} recorded events replayed identically",
                recorded.events.len()
            )),
            Some(index) => line(format!(
                "trace diverges at event {index}: recorded {}, replayed {}",
                or_unknown(recorded.events.get(index)),
                or_unknown(attempt.events.get(index))
            )),
        }
        for event in &attempt.events {
            line(format!("  {event}"));
        }
        let reproduced = divergence.is_none() && attempt.outcome == recorded.outcome;
        if reproduced {
            line(REPRODUCED.to_string());
        } else if attempt.outcome.passed() {
            line("turmoil replay: the recorded failure did not reproduce".to_string());
        } else {
            line("turmoil replay: the replay failed differently from the record".to_string());
        }
        report
    }
}

/// An optional value as a report line shows it.
fn or_unknown(value: Option<&impl fmt::Display>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "unknown".to_string(),
    }
}

mod tests {
    use std::{
        collections::BTreeMap, num::NonZeroUsize, os::unix::process::ExitStatusExt as _,
        time::SystemTime,
    };

    use super::{
        super::runner::{NetworkParameters, SimulationBounds, Topology},
        *,
    };

    type Variable = dyn Fn(&'static str) -> Result<Option<String>, DriverError>;

    fn variables(pairs: &[(&'static str, &str)]) -> Box<Variable> {
        let mut values = BTreeMap::new();
        for (name, value) in pairs {
            values.insert(*name, (*value).to_string());
        }
        Box::new(move |name| Ok(values.get(name).cloned()))
    }

    fn mode(pairs: &[(&'static str, &str)]) -> Result<Mode, DriverError> {
        Mode::from_variables(&variables(pairs))
    }

    fn inputs() -> RecordedInputs {
        RecordedInputs {
            config: SimulationConfig {
                seed: 63,
                epoch: SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
                topology: Topology::Ipv6,
                network: NetworkParameters::LOSSLESS,
                bounds: SimulationBounds {
                    simulated_duration: Duration::from_secs(30),
                    tick: Duration::from_millis(1),
                    max_steps: NonZeroUsize::new(5_000).assured("5000 is nonzero"),
                    wall_duration: Duration::from_secs(60),
                },
            },
            injected_failure: Some(InjectedFailure {
                at: Duration::from_millis(1_500),
            }),
        }
    }

    fn event(millis: u64, event: &str) -> RecordedEvent {
        RecordedEvent {
            at: Duration::from_millis(millis),
            host: "client".to_string(),
            event: event.to_string(),
        }
    }

    fn identity() -> ScenarioIdentity {
        ScenarioIdentity {
            package: "nervix-interconnect".to_string(),
            target: "simulation".to_string(),
            test: "transport::held_links_expire".to_string(),
            case: "held exchange".to_string(),
            fault_plan: "hold the link past both deadlines".to_string(),
        }
    }

    fn record(outcome: Outcome, events: Vec<RecordedEvent>) -> FailureRecord {
        FailureRecord {
            scenario: identity(),
            build: BuildFingerprint::current().clone(),
            inputs: inputs(),
            run: RunOrdinal::First,
            outcome,
            events,
        }
    }

    fn failed(error: &str) -> Outcome {
        Outcome::Failed {
            error: error.to_string(),
        }
    }

    fn replay(record: FailureRecord) -> Replay {
        Replay {
            path: PathBuf::from("held-exchange-seed-63.json"),
            record,
        }
    }

    #[test]
    fn exploring_runs_committed_seeds_or_one_sweep_range() {
        let scenario = Scenario {
            name: "case",
            fault_plan: "none",
            seeds: &[5, 9],
        };
        let Ok(Mode::Explore { seeds, injection }) = mode(&[]) else {
            panic!("no variables explore the committed seeds");
        };
        assert_eq!(
            seeds.for_scenario(&scenario).assured("seeds are committed"),
            [5, 9]
        );
        assert_eq!(injection, None);

        let Ok(Mode::Explore { seeds, injection }) =
            mode(&[(SWEEP_VARIABLE, " 3..6 "), (INJECT_VARIABLE, "1500ms")])
        else {
            panic!("a sweep explores its range");
        };
        assert_eq!(
            seeds.for_scenario(&scenario).assured("a sweep has seeds"),
            [3, 4, 5]
        );
        assert_eq!(
            injection,
            Some(InjectedFailure {
                at: Duration::from_millis(1_500)
            })
        );

        let uncommitted = Scenario {
            name: "uncommitted",
            fault_plan: "none",
            seeds: &[],
        };
        assert!(matches!(
            Seeds::Committed.for_scenario(&uncommitted),
            Err(DriverError::NoCommittedSeeds {
                case: "uncommitted"
            })
        ));
    }

    #[test]
    fn malformed_sweeps_and_injections_are_refused() {
        for sweep in ["6..6", "6..3", "a..b", "7", ""] {
            assert!(
                matches!(
                    mode(&[(SWEEP_VARIABLE, sweep)]),
                    Err(DriverError::InvalidSweep { .. })
                ),
                "{sweep:?}"
            );
        }
        for injection in ["0s", "soon", ""] {
            assert!(
                matches!(
                    mode(&[(INJECT_VARIABLE, injection)]),
                    Err(DriverError::InvalidInjection { .. })
                ),
                "{injection:?}"
            );
        }
    }

    #[test]
    fn a_replay_takes_only_its_recorded_inputs() {
        for conflicting in [SWEEP_VARIABLE, INJECT_VARIABLE] {
            let error = mode(&[(REPLAY_VARIABLE, "record.json"), (conflicting, "5s")]);
            let Err(DriverError::ReplayConflict { conflicting: named }) = error else {
                panic!("a replay with {conflicting} set must be refused");
            };
            assert_eq!(named, conflicting);
        }

        let directory = tempfile::tempdir().assured("the test creates a directory");
        let missing = directory.path().join("missing.json");
        let missing = missing.to_str().assured("the temporary path is Unicode");
        assert!(matches!(
            mode(&[(REPLAY_VARIABLE, missing)]),
            Err(DriverError::ReadRecord { .. })
        ));
        let foreign = directory.path().join("foreign.json");
        fs::write(&foreign, r#"{"seed": 63}"#).assured("the test writes a file");
        let foreign = foreign.to_str().assured("the temporary path is Unicode");
        assert!(matches!(
            mode(&[(REPLAY_VARIABLE, foreign)]),
            Err(DriverError::ParseRecord { .. })
        ));
    }

    #[test]
    fn records_round_trip_and_leave_nothing_once_their_seed_passes() {
        let directory = tempfile::tempdir().assured("the test creates a directory");
        let root = directory
            .path()
            .to_str()
            .assured("the temporary path is Unicode");
        let store = RecordStore::from_variables(&variables(&[(FAILURES_VARIABLE, root)]))
            .assured("the failure directory is given");
        let path = store.path(&identity(), 63);
        assert_eq!(
            path,
            directory
                .path()
                .join("transport-held-links-expire")
                .join("held-exchange-seed-63.json")
        );

        let written = record(
            failed("simulation held exchange seed 63: host or client failed"),
            vec![event(1, "exchange completed"), event(1_001, "link held")],
        );
        store
            .write(&path, &written)
            .assured("the record is written");
        let path_text = path.to_str().assured("the temporary path is Unicode");
        let Ok(Mode::Replay(loaded)) = mode(&[(REPLAY_VARIABLE, path_text)]) else {
            panic!("the written record loads for replay");
        };
        assert_eq!(loaded.record.scenario, written.scenario);
        assert_eq!(loaded.record.build, written.build);
        assert_eq!(loaded.record.inputs, written.inputs);
        assert_eq!(loaded.record.run, written.run);
        assert_eq!(loaded.record.outcome, written.outcome);
        assert_eq!(loaded.record.events, written.events);
        assert!(loaded.record.scenario.selects(&identity()));
        let mut other_case = identity();
        other_case.case = "partitioned exchange".to_string();
        assert!(!loaded.record.scenario.selects(&other_case));

        store.remove(&path).assured("the record is removed");
        assert!(!path.exists());
        assert!(
            !directory
                .path()
                .join("transport-held-links-expire")
                .exists(),
            "the emptied test directory is removed"
        );
    }

    #[test]
    fn slugs_keep_letters_and_digits_and_collapse_the_rest() {
        assert_eq!(
            ScenarioIdentity::slug("transport::relay::Relay case  (7)!"),
            "transport-relay-relay-case-7"
        );
        assert_eq!(ScenarioIdentity::slug("::"), "");
    }

    #[test]
    fn first_difference_names_changed_missing_and_extra_events() {
        let first = [event(1, "a"), event(2, "b")];
        assert_eq!(RecordedEvent::first_difference(&first, &first), None);
        assert_eq!(
            RecordedEvent::first_difference(&first, &[event(1, "a"), event(3, "b")]),
            Some(1)
        );
        assert_eq!(
            RecordedEvent::first_difference(&first, &first[..1]),
            Some(1)
        );
        assert_eq!(RecordedEvent::first_difference(&[], &first), Some(0));
    }

    #[test]
    fn replay_reports_its_verdict() {
        let events = vec![event(1, "exchange completed")];
        let recorded = replay(record(failed("host failed"), events.clone()));
        let verdict = |outcome: Outcome, events: Vec<RecordedEvent>| {
            recorded.report(&identity(), &Attempt { outcome, events })
        };

        let reproduced = verdict(failed("host failed"), events.clone());
        assert!(reproduced.contains(REPRODUCED), "{reproduced}");
        assert!(
            reproduced.contains("trace: all 1 recorded events replayed identically"),
            "{reproduced}"
        );
        assert!(!reproduced.contains("build differs"), "{reproduced}");

        let passed = verdict(Outcome::Passed, events.clone());
        assert!(
            passed.contains("turmoil replay: the recorded failure did not reproduce"),
            "{passed}"
        );

        let different = verdict(
            Outcome::Panicked {
                panic: PanicReport {
                    message: "assertion failed".to_string(),
                    location: Some("transport.rs:9:5".to_string()),
                },
            },
            vec![event(2, "exchange completed")],
        );
        assert!(
            different.contains("turmoil replay: the replay failed differently from the record"),
            "{different}"
        );
        assert!(
            different.contains("replayed outcome: panicked: assertion failed at transport.rs:9:5"),
            "{different}"
        );
        assert!(
            different.contains("trace diverges at event 0: recorded 0.001s client exchange"),
            "{different}"
        );
    }

    #[test]
    fn replay_reports_every_way_its_build_and_plan_differ() {
        let mut changed = record(Outcome::Running, Vec::new());
        changed.build = BuildFingerprint {
            source: Some(SourceRevision {
                commit: "0123abc".to_string(),
                modified: true,
            }),
            lockfile_sha256: None,
            toolchain: Some("rustc 0.0.0".to_string()),
            tokio_unstable: !cfg!(tokio_unstable),
            debug_assertions: !cfg!(debug_assertions),
        };
        let recorded = replay(changed);
        let mut current = identity();
        current.fault_plan = "partition the link".to_string();
        let report = recorded.report(
            &current,
            &Attempt {
                outcome: Outcome::Passed,
                events: Vec::new(),
            },
        );
        for expected in [
            "build differs: source: recorded 0123abc with modified tracked files, current",
            "build differs: Cargo.lock SHA-256: recorded unknown, current",
            "build differs: toolchain: recorded Some(\"rustc 0.0.0\")",
            "build differs: tokio_unstable",
            "build differs: debug assertions",
            "fault plan differs: recorded \"hold the link past both deadlines\"",
            "recorded outcome (first run): still running when its process ended",
        ] {
            assert!(report.contains(expected), "{expected:?} missing:\n{report}");
        }
    }

    fn request(directory: &Path) -> AttemptRequest {
        AttemptRequest {
            scenario: identity(),
            inputs: inputs(),
            result: directory.join("attempt.json"),
        }
    }

    #[test]
    fn an_attempt_process_reports_to_its_driver() {
        let directory = tempfile::tempdir().assured("the test creates a directory");
        let request = request(directory.path());
        let request_path = directory.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_string(&request).assured("a request encodes"),
        )
        .assured("the test writes the request");
        let request_path = request_path
            .to_str()
            .assured("the temporary path is Unicode");
        let Ok(Mode::Attempt(loaded)) = mode(&[
            (ATTEMPT_VARIABLE, request_path),
            (SWEEP_VARIABLE, "not a range"),
        ]) else {
            panic!("an attempt's process runs its request whatever else is set");
        };
        assert!(loaded.scenario.selects(&identity()));
        assert_eq!(loaded.inputs, request.inputs);

        let observed = Attempt {
            outcome: failed("host failed"),
            events: vec![event(3, "exchange completed")],
        };
        loaded.report(&observed).assured("the attempt reports");
        let exited = AttemptEnding::Exited(ExitStatus::from_raw(0));
        let collected = request
            .collect(exited, &directory.path().join("output.log"))
            .assured("the driver reads the report");
        assert_eq!(collected.outcome, observed.outcome);
        assert_eq!(collected.events, observed.events);
    }

    #[test]
    fn an_attempt_process_that_never_reports_is_named_by_its_ending() {
        let directory = tempfile::tempdir().assured("the test creates a directory");
        let request = request(directory.path());
        let output_path = directory.path().join("output.log");
        let output: Vec<String> = (1..=45).map(|line| format!("line {line}")).collect();
        fs::write(&output_path, output.join("\n")).assured("the test writes output");

        let killed = request
            .collect(AttemptEnding::Killed(Duration::from_secs(90)), &output_path)
            .assured("an unreported attempt is an outcome");
        let Outcome::Unreported { ending, output } = &killed.outcome else {
            panic!("a killed attempt reports nothing: {:?}", killed.outcome);
        };
        assert_eq!(ending, "was killed after exceeding its 90s real-time bound");
        assert!(output.starts_with("line 6\n"), "{output}");
        assert!(output.ends_with("line 45"), "{output}");
        assert!(
            killed
                .outcome
                .to_string()
                .starts_with("its process was killed after exceeding its 90s real-time bound")
        );

        let aborted = request
            .collect(
                AttemptEnding::Exited(ExitStatus::from_raw(134 << 8)),
                &output_path,
            )
            .assured("an unreported attempt is an outcome");
        assert!(matches!(aborted.outcome, Outcome::Unreported { .. }));

        let passed = request.collect(AttemptEnding::Exited(ExitStatus::from_raw(0)), &output_path);
        assert!(matches!(passed, Err(DriverError::AttemptNotRun { .. })));

        let unreadable = AttemptRequest::output_tail(&directory.path().join("missing.log"));
        assert!(
            unreadable.starts_with("its output is unreadable"),
            "{unreadable}"
        );
    }

    #[test]
    fn a_divergence_names_both_runs_first_differing_events() {
        let outcome = Outcome::Diverged {
            at_event: 1,
            first: Some(event(5, "admitted")),
            repeat: None,
        };
        assert_eq!(
            outcome.to_string(),
            "two runs diverged at event 1: first 0.005s client admitted, repeat unknown"
        );
        let summary = record(outcome, vec![event(1, "ready")]).summary();
        assert!(
            summary.starts_with(
                "simulation transport::held_links_expire / \"held exchange\" seed 63 (first run) \
                 two runs diverged"
            ),
            "{summary}"
        );
        assert!(summary.contains("  0.001s client ready"), "{summary}");
    }
}
