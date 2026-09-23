//! The phase one Cucumber scenario is in, published for as long as that scenario is active.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The phases a scenario passes through, the registry of the scenarios that have
//!   started and not yet finished, which attempt of a scenario each of them is, and when each of
//!   them entered the phase it is in.
//! - **Depends on.** Tokio's monotonic clock.
//! - **Must not know.** What a phase does, how a cluster is torn down, or scenario state.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
};

use meticulous::OptionExt as _;
use parking_lot::Mutex;
use tokio::time::{Duration, Instant};

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
    started_at: Instant,
    phase_started_at: Instant,
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
        )
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
            started_at,
            phase_started_at: started_at,
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
        active.phase = phase;
        active.phase_started_at = Instant::now();
        active.clone()
    }
}

impl Drop for ActiveScenarioRegistration {
    fn drop(&mut self) {
        ACTIVE_SCENARIOS.lock().remove(&self.registration);
    }
}
