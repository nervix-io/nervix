//! The phase one Cucumber scenario is in, published for as long as that scenario is active.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The phases a scenario passes through, the registry of the scenarios that have
//!   started and not yet finished, which attempt of a scenario each of them is, and when each of
//!   them entered the phase it is in.
//! - **Depends on.** Tokio's monotonic clock.
//! - **Must not know.** What a phase does, how a cluster is torn down, or scenario state.

use std::{collections::BTreeMap, fmt};

use meticulous::OptionExt as _;
use nervix_approx_into::ApproxInto as _;
use nervix_primitives::sync::{
    atomic::{AtomicU64, Ordering},
    blocking::{LazyLock, Mutex},
};
use tokio::time::{Duration, Instant};

use super::scenario_schedule::AdmissionWait;

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
    waiting_for: Option<AdmissionWait>,
    started_at: Instant,
    phase_started_at: Instant,
    slot_started_at: Option<Instant>,
}

impl ActiveScenario {
    /// How long ago this scenario started.
    pub(crate) fn age(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// How long ago this scenario entered the phase it is in.
    pub(crate) fn phase_age(&self) -> Duration {
        self.phase_started_at.elapsed()
    }

    /// Every scenario that has started and not yet finished, oldest registration first.
    pub(crate) fn active() -> Vec<Self> {
        ACTIVE_SCENARIOS.lock().values().cloned().collect()
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

/// Every scenario currently registered, keyed by the registration that owns it. Scenarios run many
/// at a time in one test binary and a name repeats across outlines and retries, so the key is the
/// registration rather than anything the feature file supplies.
static ACTIVE_SCENARIOS: LazyLock<Mutex<BTreeMap<u64, ActiveScenario>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
static NEXT_REGISTRATION: AtomicU64 = AtomicU64::new(0);

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
    length: Duration,
}

#[derive(Default)]
struct SuiteMeasurements {
    began: Option<Instant>,
    slots: usize,
    budget: Duration,
    slot_work: Duration,
    waiting: BTreeMap<AdmissionWait, Duration>,
    completed: Vec<AttemptTime>,
}

static MEASUREMENTS: LazyLock<Mutex<SuiteMeasurements>> =
    LazyLock::new(|| Mutex::new(SuiteMeasurements::default()));

pub(crate) fn begin_suite_measurement(slots: usize, budget: Duration) {
    *MEASUREMENTS.lock() = SuiteMeasurements {
        began: Some(Instant::now()),
        slots,
        budget,
        ..SuiteMeasurements::default()
    };
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
    let mut longest = measurements.completed.clone();
    for scenario in &active {
        if scenario.phase == ScenarioPhase::Finished {
            continue;
        }
        if let Some(reason) = scenario.waiting_for {
            *waiting.entry(reason).or_default() += scenario.phase_age();
        }
        if let Some(started) = scenario.slot_started_at {
            slot_work += started.elapsed();
        }
        if let Some(started) = scenario.slot_started_at {
            longest.push(AttemptTime {
                identity: scenario.identity.clone(),
                attempt: scenario.attempt,
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
                .count()
        ),
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
        lines.push(format!(
            "| {:.1}s | {} | {} / {} (line {}) |",
            attempt.length.as_secs_f64(),
            attempt.attempt,
            attempt.identity.feature,
            attempt.identity.scenario,
            attempt.identity.line
        ));
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
}

impl ActiveScenarioRegistration {
    /// Publishes a scenario the suite has taken up, before it holds the permits it runs under.
    ///
    /// Registering here rather than once the scenario is running is what makes a scenario that
    /// never gets its permits visible, and it gives every scenario an entry its own cleanup can
    /// reach.
    pub(crate) fn start(feature: &str, scenario: &str, line: usize) -> Self {
        let registration = NEXT_REGISTRATION.fetch_add(1, Ordering::Relaxed);
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
            waiting_for: None,
            started_at,
            phase_started_at: started_at,
            slot_started_at: None,
        };
        ACTIVE_SCENARIOS.lock().insert(registration, active);
        Self {
            registration,
            identity,
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

    /// Publishes `phase` as the work now in flight and returns what a reader of the registry sees.
    pub(crate) fn enter(&self, phase: ScenarioPhase) -> ActiveScenario {
        let mut registry = ACTIVE_SCENARIOS.lock();
        let active = registry
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
            measurement.slot_work += work;
            measurement.completed.push(AttemptTime {
                identity: active.identity.clone(),
                attempt: active.attempt,
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
        if let Some(active) = ACTIVE_SCENARIOS.lock().remove(&self.registration)
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
            measurement.slot_work += length;
            measurement.completed.push(AttemptTime {
                identity: active.identity,
                attempt: active.attempt,
                length,
            });
        }
    }
}
