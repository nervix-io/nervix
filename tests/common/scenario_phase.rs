//! The phase one Cucumber scenario is in, published for as long as that scenario is active.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The phases a scenario passes through, the registry of the scenarios that have
//!   started and not yet finished, which attempt of a scenario each of them is, what length it
//!   declares and how many run slots it holds, when each of them entered the phase it is in, and
//!   the suite summary of their times.
//! - **Depends on.** Tokio's monotonic clock, and the admission vocabulary of the run slots.
//! - **Must not know.** What a phase does, how a cluster is torn down, or scenario state.

use std::{collections::BTreeMap, fmt, num::NonZeroU32, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use nervix_primitives::{
    sync::blocking::{LazyLock, Mutex},
    time::Instant,
};

use super::{
    runner_lag::{RunnerLag, RunnerThread, RunnerTick, WorkerBusy},
    scenario_schedule::{
        AdmissionWait, LONG_RUNNING_ATTEMPT, LONG_RUNNING_TAG, ScenarioLength, ScenarioRun,
    },
    scenario_shard::ScenarioShard,
};

/// Where one scenario is between its first step and the end of its cleanup.
///
/// A scenario publishes the phase it is entering, so a reader sees the work in flight rather than
/// the last work that finished. `Finished` is published only once cleanup has completed, which is
/// what makes a scenario still holding any other phase one that has not finished.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::Display)]
pub(crate) enum ScenarioPhase {
    /// The suite has taken the scenario up, and it is waiting for the permits that let it run
    /// beside the scenarios already running.
    #[strum(serialize = "queued")]
    Queued,
    /// The scenario's own steps are running.
    #[strum(serialize = "started")]
    Body,
    /// The steps have ended and their result is known.
    #[strum(serialize = "body complete")]
    BodyComplete,
    /// Cleanup has begun: the fault injection a scenario installed is being released.
    #[strum(serialize = "teardown started")]
    TeardownStarted,
    /// The bounded diagnostics that explain what the scenario left behind are being collected.
    #[strum(serialize = "teardown diagnostics")]
    Diagnostics,
    /// The scenario's fixtures and cluster nodes are being stopped.
    #[strum(serialize = "stopping")]
    Stopping,
    /// The scenario and its cleanup have both ended.
    #[strum(serialize = "finished")]
    Finished,
}

impl ScenarioPhase {
    /// Whether a scenario in this phase holds its run slots: from its first step through the end
    /// of its cleanup.
    pub(crate) fn holds_run_slots(self) -> bool {
        match self {
            Self::Queued | Self::Finished => false,
            Self::Body
            | Self::BodyComplete
            | Self::TeardownStarted
            | Self::Diagnostics
            | Self::Stopping => true,
        }
    }
}

/// Which scenario of which feature an entry belongs to.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ScenarioIdentity {
    pub(crate) feature: String,
    pub(crate) scenario: String,
    /// Where the scenario begins in its feature file. An outline is expanded into one scenario per
    /// example row, and those rows usually share the outline's name, so the line is what tells
    /// them apart.
    pub(crate) line: usize,
}

impl fmt::Display for ScenarioIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "feature={:?} scenario={:?} line={}",
            self.feature, self.scenario, self.line
        )
    }
}

/// A scenario that has started and has not yet finished its cleanup.
#[derive(Clone, Debug)]
pub(crate) struct ActiveScenario {
    pub(crate) identity: ScenarioIdentity,
    /// Which run of this scenario is in flight, counted from one.
    pub(crate) attempt: u32,
    pub(crate) phase: ScenarioPhase,
    declared: ScenarioLength,
    waiting_for: Option<AdmissionWait>,
    started_at: Instant,
    phase_started_at: Instant,
    slot_started_at: Option<Instant>,
    /// The run slots this attempt holds while it runs, from its weight and retry share.
    run_slots: u32,
}

impl ActiveScenario {
    /// The slot time an attempt holds while it runs for `running`: that time for each run slot it
    /// holds.
    fn slot_work(&self, running: Duration) -> Duration {
        running
            .checked_mul(self.run_slots)
            .assured("a run holds a few dozen slots for at most the suite budget")
    }

    /// The part of [`Self::slot_work`] beyond the one slot every attempt holds: what larger
    /// reservations cost the suite.
    fn extra_slot_work(&self, running: Duration) -> Duration {
        let extra_slots = self
            .run_slots
            .checked_sub(1)
            .assured("a run's share is at least one slot, and a registration starts at one");
        running
            .checked_mul(extra_slots)
            .assured("a run holds a few dozen slots for at most the suite budget")
    }

    /// How long ago this scenario started.
    pub(crate) fn age(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// How long ago this scenario entered the phase it is in.
    pub(crate) fn phase_age(&self) -> Duration {
        self.phase_started_at.elapsed()
    }

    /// Every scenario the suite has taken up and not yet dropped, oldest registration first. A
    /// finished attempt stays here while Cucumber holds its world, as it does for a failed attempt
    /// it reports at the end of the run.
    pub(crate) fn active() -> Vec<Self> {
        ACTIVE_SCENARIOS.lock().entries.values().cloned().collect()
    }

    /// Every scenario waiting for its permits or running, oldest registration first: the active
    /// scenarios less the attempts that have finished.
    pub(crate) fn unfinished() -> Vec<Self> {
        let registry = ACTIVE_SCENARIOS.lock();
        let mut unfinished = Vec::new();
        for active in registry.entries.values() {
            if active.phase != ScenarioPhase::Finished {
                unfinished.push(active.clone());
            }
        }
        unfinished
    }

    /// Every scenario that holds its run slots, oldest registration first: the scenarios whose
    /// steps or cleanup run, without the ones still queued for their permits.
    pub(crate) fn running() -> Vec<Self> {
        let registry = ACTIVE_SCENARIOS.lock();
        let mut running = Vec::new();
        for active in registry.entries.values() {
            if active.phase.holds_run_slots() {
                running.push(active.clone());
            }
        }
        running
    }

    pub(crate) fn suite_age() -> Duration {
        MEASUREMENTS
            .lock()
            .began
            .map(|began| began.elapsed())
            .unwrap_or_default()
    }
}

impl fmt::Display for ActiveScenario {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} attempt={} phase={} phase_age={:?} age={:?}",
            self.identity,
            self.attempt,
            self.phase,
            self.phase_age(),
            self.age()
        )?;
        if let Some(reason) = self.waiting_for {
            write!(formatter, " waiting_for={reason}")?;
        }
        Ok(())
    }
}

/// Every scenario currently registered, and the registration the next one takes.
#[derive(Default)]
struct ActiveScenarios {
    /// Keyed by the registration that owns each entry. Scenarios run many at a time in one test
    /// binary and a name repeats across outlines and retries, so the key is the registration
    /// rather than anything the feature file supplies.
    entries: BTreeMap<u64, ActiveScenario>,
    next_registration: u64,
}

impl ActiveScenarios {
    /// Publishes `active` under a registration no earlier scenario of the run took.
    fn register(&mut self, active: ActiveScenario) -> u64 {
        let registration = self.next_registration;
        self.next_registration = registration
            .checked_add(1)
            .assured("a suite takes up far fewer than 2^64 scenarios");
        self.entries.insert(registration, active);
        registration
    }
}

static ACTIVE_SCENARIOS: LazyLock<Mutex<ActiveScenarios>> =
    LazyLock::new(|| Mutex::new(ActiveScenarios::default()));

/// How many times the suite has taken up each scenario.
///
/// Cucumber retries a failed scenario by running it again, and the hook that registers a scenario
/// is not told which run it is in. Counting here is exact: a retry is taken up only once the run
/// before it has ended, so the count a registration reads is its own attempt.
static SCENARIO_ATTEMPTS: LazyLock<Mutex<BTreeMap<ScenarioIdentity, u32>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

#[derive(Clone, Debug)]
struct AttemptTime {
    identity: ScenarioIdentity,
    attempt: u32,
    declared: ScenarioLength,
    length: Duration,
}

impl AttemptTime {
    /// The attempt as a row of the summary's attempt tables.
    fn table_row(&self) -> String {
        format!(
            "| {:.1}s | {} | {} / {} (line {}) |",
            self.length.as_secs_f64(),
            self.attempt,
            self.identity.feature,
            self.identity.scenario,
            self.identity.line
        )
    }
}

#[derive(Default)]
struct SuiteMeasurements {
    began: Option<Instant>,
    slots: usize,
    budget: Duration,
    /// The slot time finished attempts held: each attempt's running time for every run slot its
    /// admission held.
    slot_work: Duration,
    /// The part of `slot_work` reservations held beyond one slot per attempt.
    extra_slot_work: Duration,
    waiting: BTreeMap<AdmissionWait, Duration>,
    completed: Vec<AttemptTime>,
    /// How late the runner task polled its ticks over the run.
    runner_lag: RunnerLag,
    /// The thread that polls the run, named when the run began.
    runner_thread: Option<RunnerThread>,
    /// How busy the runtime's workers had been when the run began, when it began inside one.
    workers_began: Option<WorkerBusy>,
    /// The shard this run executes, when it is one shard of a sharded run.
    shard: Option<ScenarioShard>,
}

static MEASUREMENTS: LazyLock<Mutex<SuiteMeasurements>> =
    LazyLock::new(|| Mutex::new(SuiteMeasurements::default()));

impl SuiteMeasurements {
    /// How busy the runner task was over a run of `length`: how late its ticks were polled, and
    /// how long the thread that polls every scenario ran on a CPU and waited for one.
    fn runner_task_line(&self, length: Duration) -> String {
        let lag = &self.runner_lag;
        let Some(thread) = &self.runner_thread else {
            return format!("Runner task: {lag}; no measurement named its thread.");
        };
        let times = match thread.since_began() {
            Ok(times) => times,
            Err(kind) => {
                return format!(
                    "Runner task: {lag}; its thread's CPU time could not be read: {kind}."
                );
            }
        };
        let share = if length.is_zero() {
            0.0
        } else {
            100.0 * times.on_cpu.as_secs_f64() / length.as_secs_f64()
        };
        format!(
            "Runner task: {lag}; its thread ran {:.1}s on a CPU, {share:.1}% of the run, and \
             waited {:.1}s for one.",
            times.on_cpu.as_secs_f64(),
            times.run_queue_wait.as_secs_f64()
        )
    }

    /// Which runs this run executed: one shard's, whose count the scenario log names, or every
    /// run it selected.
    fn shard_line(&self) -> String {
        match self.shard {
            Some(shard) => format!("Shard: {shard}; the scenario log names the runs it kept."),
            None => "Shard: none; the run executed every scenario run it selected.".to_string(),
        }
    }

    /// How busy the runtime's workers, which run every in-process node, were over a run of
    /// `length`.
    fn workers_line(&self, length: Duration) -> String {
        let Some(began) = self.workers_began else {
            return "Runtime workers: no runtime was measured when the run began.".to_string();
        };
        let Some(now) = WorkerBusy::of_current_runtime() else {
            return "Runtime workers: the summary was taken outside the run's runtime.".to_string();
        };
        let busy = now.since(began);
        let capacity = length.as_secs_f64() * now.workers.approx_into::<f64>();
        let share = if capacity > 0.0 {
            100.0 * busy.as_secs_f64() / capacity
        } else {
            0.0
        };
        format!(
            "Runtime workers: {} busy for {:.1}s together, {share:.1}% of their time over the run.",
            now.workers,
            busy.as_secs_f64()
        )
    }
}

/// Starts measuring a run of `slots` run slots and `budget`, executing `shard` when it is one
/// shard of a sharded run, polled on the calling thread.
pub(crate) fn begin_suite_measurement(
    slots: usize,
    budget: Duration,
    shard: Option<ScenarioShard>,
) {
    *MEASUREMENTS.lock() = SuiteMeasurements {
        began: Some(Instant::now()),
        slots,
        budget,
        shard,
        runner_thread: Some(RunnerThread::current()),
        workers_began: WorkerBusy::of_current_runtime(),
        ..SuiteMeasurements::default()
    };
}

/// Adds one tick of the runner task to the run's measurement.
pub(crate) fn record_runner_tick(tick: RunnerTick) {
    MEASUREMENTS.lock().runner_lag.record(tick);
}

/// Snapshot completed and still-running work before the watchdog drops a timed-out run.
pub(crate) fn suite_summary() -> String {
    let active = ActiveScenario::active();
    let attempts = SCENARIO_ATTEMPTS.lock().clone();
    let measurements = MEASUREMENTS.lock();
    let length = measurements
        .began
        .map(|began| began.elapsed())
        .unwrap_or_default();
    let mut waiting = measurements.waiting.clone();
    let mut slot_work = measurements.slot_work;
    let mut extra_slot_work = measurements.extra_slot_work;
    let mut longest = measurements.completed.clone();
    for scenario in &active {
        if scenario.phase == ScenarioPhase::Finished {
            continue;
        }
        if let Some(reason) = scenario.waiting_for {
            *waiting.entry(reason).or_default() += scenario.phase_age();
        }
        if let Some(started) = scenario.slot_started_at {
            slot_work += scenario.slot_work(started.elapsed());
            extra_slot_work += scenario.extra_slot_work(started.elapsed());
        }
        if let Some(started) = scenario.slot_started_at {
            longest.push(AttemptTime {
                identity: scenario.identity.clone(),
                attempt: scenario.attempt,
                declared: scenario.declared,
                length: started.elapsed(),
            });
        }
    }
    longest.sort_by_key(|attempt| std::cmp::Reverse(attempt.length));
    let capacity = length.as_secs_f64() * measurements.slots.approx_into::<f64>();
    let utilization = if capacity > 0.0 {
        100.0 * slot_work.as_secs_f64() / capacity
    } else {
        0.0
    };
    let margin = if length <= measurements.budget {
        format!("{:.1}s", (measurements.budget - length).as_secs_f64())
    } else {
        format!("-{:.1}s", (length - measurements.budget).as_secs_f64())
    };
    let retries: u32 = attempts.values().map(|attempts| attempts - 1).sum();
    let mut lines = vec![
        "## Scenario suite".to_string(),
        String::new(),
        format!(
            "Length: {:.1}s; budget: {:.1}s; margin left: {margin}.",
            length.as_secs_f64(),
            measurements.budget.as_secs_f64()
        ),
        measurements.shard_line(),
        format!(
            "Run slots: {}; utilization: {utilization:.1}%; scenario work: {:.1} slot-seconds.",
            measurements.slots,
            slot_work.as_secs_f64()
        ),
        format!(
            "Attempts: {} completed, {} active; retries: {retries}.",
            measurements.completed.len(),
            active
                .iter()
                .filter(|scenario| scenario.phase != ScenarioPhase::Finished)
                .count(),
        ),
        format!(
            "Reservations: {:.1} slot-seconds beyond one slot per attempt.",
            extra_slot_work.as_secs_f64()
        ),
        measurements.runner_task_line(length),
        measurements.workers_line(length),
        String::new(),
        "| Waiting reason | Cumulative time |".to_string(),
        "| --- | ---: |".to_string(),
    ];
    for reason in [
        AdmissionWait::WebConsole,
        AdmissionWait::WasmStateReset,
        AdmissionWait::RunSlot,
    ] {
        lines.push(format!(
            "| {reason} | {:.1}s |",
            waiting
                .get(&reason)
                .copied()
                .unwrap_or_default()
                .as_secs_f64()
        ));
    }
    lines.extend([
        String::new(),
        "### 10 longest attempts".to_string(),
        String::new(),
        "| Time | Attempt | Feature / scenario |".to_string(),
        "| ---: | ---: | --- |".to_string(),
    ]);
    for attempt in longest.iter().take(10) {
        lines.push(attempt.table_row());
    }
    // A scenario taken up late in feature order without its declaration can start in the run's
    // last minutes and keep the suite running after the rest has ended; naming it here keeps the
    // declared set current.
    let mut undeclared = Vec::new();
    for attempt in &longest {
        if attempt.declared.lacks_declaration_for(attempt.length) {
            undeclared.push(attempt);
        }
    }
    lines.extend([
        String::new(),
        format!(
            "### Attempts of at least {}s without @{LONG_RUNNING_TAG}",
            LONG_RUNNING_ATTEMPT.as_secs()
        ),
        String::new(),
    ]);
    if undeclared.is_empty() {
        lines.push("None.".to_string());
    } else {
        lines.extend([
            "| Time | Attempt | Feature / scenario |".to_string(),
            "| ---: | ---: | --- |".to_string(),
        ]);
        for attempt in undeclared {
            lines.push(attempt.table_row());
        }
    }
    lines.join("\n") + "\n"
}

/// One scenario's entry in the active-scenario registry.
///
/// The entry exists for exactly as long as the registration does: a scenario appears when it
/// starts and disappears when the world holding this registration is dropped, whether the scenario
/// passed, failed or ended in a panic.
#[derive(Debug)]
pub(crate) struct ActiveScenarioRegistration {
    registration: u64,
    identity: ScenarioIdentity,
    run: ScenarioRun,
}

impl ActiveScenarioRegistration {
    /// Publishes a scenario the suite has taken up, before it holds the permits it runs under.
    ///
    /// Registering here rather than once the scenario is running is what makes a scenario that
    /// never gets its permits visible, and it gives every scenario an entry its own cleanup can
    /// reach.
    pub(crate) fn start(
        feature: &str,
        scenario: &str,
        line: usize,
        declared: ScenarioLength,
    ) -> Self {
        let identity = ScenarioIdentity {
            feature: feature.to_string(),
            scenario: scenario.to_string(),
            line,
        };
        let attempt = Self::take_up(&identity);
        let started_at = Instant::now();
        let active = ActiveScenario {
            identity: identity.clone(),
            attempt,
            phase: ScenarioPhase::Queued,
            declared,
            waiting_for: None,
            started_at,
            phase_started_at: started_at,
            slot_started_at: None,
            run_slots: 1,
        };
        let registration = ACTIVE_SCENARIOS.lock().register(active);
        let run = NonZeroU32::new(attempt).verified("take_up counts runs from one");
        Self {
            registration,
            identity,
            run: ScenarioRun::new(run),
        }
    }

    /// Counts one more run of `identity` and returns which run it is.
    fn take_up(identity: &ScenarioIdentity) -> u32 {
        let mut attempts = SCENARIO_ATTEMPTS.lock();
        let taken_up = attempts.entry(identity.clone()).or_insert(0);
        *taken_up = taken_up
            .checked_add(1)
            .assured("cucumber retries a scenario a handful of times, not four billion");
        *taken_up
    }

    pub(crate) fn identity(&self) -> &ScenarioIdentity {
        &self.identity
    }

    /// Which run of its scenario this registration is, counted from one.
    pub(crate) fn run(&self) -> ScenarioRun {
        self.run
    }

    /// Publishes that the scenario's admission holds `run_slots` run slots and its steps are
    /// starting, and returns what a reader of the registry sees.
    pub(crate) fn start_running(&self, run_slots: usize) -> ActiveScenario {
        let run_slots = u32::try_from(run_slots)
            .assured("a run holds at most the run slots of one machine, far fewer than 2^32");
        {
            let mut registry = ACTIVE_SCENARIOS.lock();
            let active = registry
                .entries
                .get_mut(&self.registration)
                .verified("a registration holds its registry entry until it is dropped");
            active.run_slots = run_slots;
        }
        self.enter(ScenarioPhase::Body)
    }

    /// Publishes `phase` as the work now in flight and returns what a reader of the registry sees.
    pub(crate) fn enter(&self, phase: ScenarioPhase) -> ActiveScenario {
        let mut registry = ACTIVE_SCENARIOS.lock();
        let active = registry
            .entries
            .get_mut(&self.registration)
            .verified("a registration holds its registry entry until it is dropped");
        let now = Instant::now();
        let mut measurement = MEASUREMENTS.lock();
        if let Some(reason) = active.waiting_for.take() {
            *measurement.waiting.entry(reason).or_default() += now - active.phase_started_at;
        }
        if phase == ScenarioPhase::Body {
            active.slot_started_at = Some(now);
        }
        if phase == ScenarioPhase::Finished {
            let work = active
                .slot_started_at
                .take()
                .map(|started| now - started)
                .unwrap_or_default();
            measurement.slot_work += active.slot_work(work);
            measurement.extra_slot_work += active.extra_slot_work(work);
            measurement.completed.push(AttemptTime {
                identity: active.identity.clone(),
                attempt: active.attempt,
                declared: active.declared,
                length: work,
            });
        }
        active.phase = phase;
        active.phase_started_at = now;
        active.clone()
    }

    pub(crate) fn wait_for(&self, reason: AdmissionWait) -> ActiveScenario {
        let mut registry = ACTIVE_SCENARIOS.lock();
        let active = registry
            .entries
            .get_mut(&self.registration)
            .verified("a registration holds its registry entry until it is dropped");
        let now = Instant::now();
        if let Some(previous) = active.waiting_for.replace(reason) {
            *MEASUREMENTS.lock().waiting.entry(previous).or_default() +=
                now - active.phase_started_at;
        }
        active.phase_started_at = now;
        active.clone()
    }
}

impl Drop for ActiveScenarioRegistration {
    fn drop(&mut self) {
        if let Some(active) = ACTIVE_SCENARIOS.lock().entries.remove(&self.registration)
            && active.phase != ScenarioPhase::Finished
        {
            let mut measurement = MEASUREMENTS.lock();
            if let Some(reason) = active.waiting_for {
                *measurement.waiting.entry(reason).or_default() += active.phase_age();
            }
            let length = active
                .slot_started_at
                .map(|started| started.elapsed())
                .unwrap_or_default();
            measurement.slot_work += active.slot_work(length);
            measurement.extra_slot_work += active.extra_slot_work(length);
            measurement.completed.push(AttemptTime {
                identity: active.identity,
                attempt: active.attempt,
                declared: active.declared,
                length,
            });
        }
    }
}
