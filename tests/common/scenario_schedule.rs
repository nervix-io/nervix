//! Admission to the scenario suite's run slots and its limited feature groups.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Feature-group admission, run-slot admission, feature take-up priority, the
//!   declared scenario weights and lengths, the retry count, and the share of the run slots each
//!   attempt holds.
//! - **Depends on.** Tokio channels, a short synchronous queue lock, and Gherkin feature names and
//!   inherited tags supplied by the parser.
//! - **Must not know.** Scenario steps, node state, or Cucumber's result writer.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    num::{NonZeroU32, NonZeroUsize, ParseIntError},
    time::Duration,
};

use arch_into::ArchInto as _;
use cucumber::gherkin::{Feature, Rule, Scenario};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::{StdArc, blocking::Mutex, oneshot, watch};

pub(crate) const WEB_CONSOLE_FEATURE_NAMES: [&str; 4] = [
    "Web console Plexus REPL",
    "Web console execution graph",
    "Web console transaction inspector",
    "Web console domain clock",
];
pub(crate) const WASM_STATE_RESET_FEATURE_NAME: &str = "Coordinated WASM processor state reset";

/// The Gherkin tag, without its `@`, that declares a scenario long-running. It counts on the
/// scenario, its outline's examples, its rule, or its feature.
pub(crate) const LONG_RUNNING_TAG: &str = "long_running";

/// How long an attempt runs before the suite summary names its scenario as one that should
/// declare itself long-running. The scenarios declared so far are those whose attempts reached two
/// minutes in the canonical producers of 17 October 2026: about one attempt in a hundred, in runs
/// whose median attempt took 25 seconds.
pub(crate) const LONG_RUNNING_ATTEMPT: Duration = Duration::from_secs(120);

/// How long one attempt of a scenario is declared to run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScenarioLength {
    /// Declared with `@long_running`: an attempt runs for minutes.
    LongRunning,
    /// Every scenario without the declaration.
    Ordinary,
}

impl ScenarioLength {
    /// The length a scenario's tags declare, given the tags of the scenario, its rule and its
    /// feature together.
    pub(crate) fn declared_by<'a>(tags: impl IntoIterator<Item = &'a str>) -> Self {
        for tag in tags {
            if tag == LONG_RUNNING_TAG {
                return Self::LongRunning;
            }
        }
        Self::Ordinary
    }

    /// Whether an attempt that ran for `time` shows a scenario of this declared length to be
    /// long-running without the declaration.
    pub(crate) fn lacks_declaration_for(self, time: Duration) -> bool {
        match self {
            Self::LongRunning => false,
            Self::Ordinary => time >= LONG_RUNNING_ATTEMPT,
        }
    }
}

/// The run-slot load a scenario declares, before the pool's quarter-share cap is applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScenarioWeight(NonZeroUsize);

#[derive(Debug, thiserror::Error)]
#[error("invalid scenario run-slot tag @{tag}: expected @slots_<positive integer>")]
pub(crate) struct ScenarioWeightError {
    tag: String,
    #[source]
    source: ParseIntError,
}

impl ScenarioWeight {
    /// The declaration of a scenario without a weight tag.
    pub(crate) const ONE: Self = Self(NonZeroUsize::MIN);

    /// Read all inherited declarations and retain the largest, independently of their order.
    pub(crate) fn declared_by<'a>(
        tags: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, ScenarioWeightError> {
        let mut weight = Self::ONE.0;
        for tag in tags {
            let Some(number) = tag.strip_prefix("slots_") else {
                continue;
            };
            let declared: NonZeroUsize = number.parse().map_err(|source| ScenarioWeightError {
                tag: tag.to_string(),
                source,
            })?;
            weight = weight.max(declared);
        }
        Ok(Self(weight))
    }

    /// Examples' tags have already been carried into `scenario` by Cucumber's expansion.
    pub(crate) fn for_scenario(
        feature: &Feature,
        rule: Option<&Rule>,
        scenario: &Scenario,
    ) -> Result<Self, ScenarioWeightError> {
        let rule_tags = rule.into_iter().flat_map(|rule| &rule.tags);
        let tags = feature.tags.iter().chain(rule_tags).chain(&scenario.tags);
        Self::declared_by(tags.map(String::as_str))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FeatureLimit {
    WebConsole,
    WasmStateReset,
    Unlimited,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScenarioAttempt {
    First,
    Retry,
}

/// How many times Cucumber runs a failed scenario again before it reports the scenario failed.
pub(crate) const SCENARIO_RETRIES: usize = 2;

/// The first retry of a scenario holds this divisor's share of the run slots, and each later retry
/// holds twice the share of the run before it. A scenario that failed beside the rest of the suite
/// therefore runs again beside fewer other scenarios, and never alone.
const FIRST_RETRY_SLOT_DIVISOR: usize = 16;

/// Every admission leaves room for other scenarios by reserving at most a quarter of the pool.
const MAX_RUN_SLOT_SHARE_DIVISOR: usize = 4;

/// The share of the run slots the last retry holds, as a divisor of the pool: the first retry's
/// share, doubled once for each retry after it.
const LAST_RETRY_SLOT_DIVISOR: usize = FIRST_RETRY_SLOT_DIVISOR >> (SCENARIO_RETRIES - 1);

const _: () = assert!(
    SCENARIO_RETRIES >= 1,
    "a failed scenario runs again before the suite reports it"
);
const _: () = assert!(
    LAST_RETRY_SLOT_DIVISOR << (SCENARIO_RETRIES - 1) == FIRST_RETRY_SLOT_DIVISOR,
    "each retry holds exactly twice the share of the run before it"
);
const _: () = assert!(
    LAST_RETRY_SLOT_DIVISOR >= MAX_RUN_SLOT_SHARE_DIVISOR,
    "no run holds more than a quarter of the run slots, so a retry never runs a scenario alone"
);

/// Which run of a scenario an admission is for, counted from one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScenarioRun(NonZeroU32);

impl ScenarioRun {
    /// A scenario's first run.
    pub(crate) const FIRST: Self = Self(NonZeroU32::MIN);

    pub(crate) fn new(run: NonZeroU32) -> Self {
        Self(run)
    }

    pub(crate) fn number(self) -> u32 {
        self.0.get()
    }

    /// Whether this is the scenario's first run or a run after a failed one.
    pub(crate) fn attempt(self) -> ScenarioAttempt {
        if self == Self::FIRST {
            ScenarioAttempt::First
        } else {
            ScenarioAttempt::Retry
        }
    }

    /// How many of a pool of `slots` run slots this run holds from admission to the end of its
    /// teardown.
    ///
    /// The larger of the declared weight and retry share, capped at a quarter of the pool and at
    /// least one slot. Even a declaration above the pool's size admits in a small local run.
    pub(crate) fn run_slots(self, slots: usize, weight: ScenarioWeight) -> usize {
        let requested = self.retry_slots(slots).max(weight.0.get());
        let largest_share = (slots / MAX_RUN_SLOT_SHARE_DIVISOR).max(1);
        requested.min(largest_share)
    }

    /// A first run's minimum is one slot. The first retry holds a sixteenth of the pool and each
    /// later retry twice the share of the run before it, up to the configured last retry's share.
    fn retry_slots(self, slots: usize) -> usize {
        let run: usize = self.number().arch_into();
        if run == 1 {
            return 1;
        }
        let retry = run
            .checked_sub(1)
            .verified("the first run returned above, so this run is a retry");
        let doublings = retry
            .min(SCENARIO_RETRIES)
            .checked_sub(1)
            .verified("a retry counts from one");
        let divisor = FIRST_RETRY_SLOT_DIVISOR >> doublings;
        let share = slots / divisor;
        share.max(1)
    }
}

impl FeatureLimit {
    pub(crate) fn for_name(name: &str) -> Self {
        if WEB_CONSOLE_FEATURE_NAMES.contains(&name) {
            Self::WebConsole
        } else if name == WASM_STATE_RESET_FEATURE_NAME {
            Self::WasmStateReset
        } else {
            Self::Unlimited
        }
    }

    fn priority(self) -> u8 {
        match self {
            Self::WasmStateReset => 0,
            Self::WebConsole => 1,
            Self::Unlimited => 2,
        }
    }
}

pub(crate) fn prioritize_features<T>(features: &mut [T], name: impl Fn(&T) -> Option<&str>) {
    features.sort_by_key(|feature| {
        name(feature)
            .map(|name| FeatureLimit::for_name(name).priority())
            .unwrap_or(u8::MAX)
    });
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum AdmissionWait {
    WebConsole,
    WasmStateReset,
    RunSlot,
}

impl fmt::Display for AdmissionWait {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WebConsole => "web console feature limit",
            Self::WasmStateReset => "WASM state reset feature limit",
            Self::RunSlot => "run slot",
        })
    }
}

#[derive(Debug)]
pub(crate) struct ScenarioAdmission {
    limit: FeatureLimit,
    /// The run slots this admission holds, all given back together when it ends.
    slots: usize,
    pool: StdArc<AdmissionPool>,
    active: bool,
}

impl ScenarioAdmission {
    pub(crate) fn limit(&self) -> FeatureLimit {
        self.limit
    }

    pub(crate) fn slots(&self) -> usize {
        self.slots
    }
}

impl Drop for ScenarioAdmission {
    fn drop(&mut self) {
        if self.active {
            self.pool.release(self.limit, self.slots);
        }
    }
}

pub(crate) struct ScenarioRunSlots {
    /// The size of the pool, from which each run's share is taken.
    slots: usize,
    pool: StdArc<AdmissionPool>,
}

struct Waiter {
    feature_name: String,
    /// The run slots this waiter's run holds once admitted.
    slots: usize,
    grant: oneshot::Sender<ScenarioAdmission>,
    reason: watch::Sender<AdmissionWait>,
}

/// What the waiter a queue offers next allows admission to do.
enum Offer {
    /// The queue has no waiter to offer.
    Empty,
    /// The offered waiter needs more run slots than are free. Admission keeps every slot that
    /// frees up for it, rather than for a smaller waiter behind it, until its share is free.
    Waiting,
    Admitted(Waiter),
}

#[derive(Default)]
struct AttemptQueues {
    first: VecDeque<Waiter>,
    retries: VecDeque<Waiter>,
}

impl AttemptQueues {
    fn for_attempt(&mut self, attempt: ScenarioAttempt) -> &mut VecDeque<Waiter> {
        match attempt {
            ScenarioAttempt::First => &mut self.first,
            ScenarioAttempt::Retry => &mut self.retries,
        }
    }

    /// Takes the waiter at `index` of the `attempt` queue when the free run slots cover its share.
    fn offer(&mut self, attempt: ScenarioAttempt, index: usize, available_slots: usize) -> Offer {
        let queue = self.for_attempt(attempt);
        let Some(waiter) = queue.get(index) else {
            return Offer::Empty;
        };
        if waiter.slots > available_slots {
            return Offer::Waiting;
        }
        let waiter = queue
            .remove(index)
            .verified("the waiter at this index was read above under the same lock");
        Offer::Admitted(waiter)
    }

    fn waiting(&self) -> impl Iterator<Item = &Waiter> {
        self.retries.iter().chain(&self.first)
    }
}

#[derive(Default)]
struct AdmissionState {
    available_slots: usize,
    available_web_console: usize,
    available_wasm_state_reset: usize,
    web_console: AttemptQueues,
    web_console_grants: BTreeMap<String, usize>,
    wasm_state_reset: AttemptQueues,
    /// Declared long-running scenarios of unlimited features. They take a free run slot before
    /// ordinary first attempts so that one taken up late in feature order cannot start in a run's
    /// last minutes and keep the suite running after every other scenario has ended.
    long_running: AttemptQueues,
    ordinary: AttemptQueues,
}

impl AdmissionState {
    fn feature_available(&self, limit: FeatureLimit) -> bool {
        match limit {
            FeatureLimit::WebConsole => self.available_web_console > 0,
            FeatureLimit::WasmStateReset => self.available_wasm_state_reset > 0,
            FeatureLimit::Unlimited => true,
        }
    }

    fn waiting_for(&self, limit: FeatureLimit) -> AdmissionWait {
        match limit {
            FeatureLimit::WebConsole if !self.feature_available(limit) => AdmissionWait::WebConsole,
            FeatureLimit::WasmStateReset if !self.feature_available(limit) => {
                AdmissionWait::WasmStateReset
            }
            _ => AdmissionWait::RunSlot,
        }
    }

    /// A limited feature's chain keeps its own order whatever its scenarios declare: the feature
    /// limit already takes it up first and paces it.
    fn queue(
        &mut self,
        limit: FeatureLimit,
        length: ScenarioLength,
        attempt: ScenarioAttempt,
        waiter: Waiter,
    ) {
        let queue = match (limit, length) {
            (FeatureLimit::WebConsole, _) => &mut self.web_console,
            (FeatureLimit::WasmStateReset, _) => &mut self.wasm_state_reset,
            (FeatureLimit::Unlimited, ScenarioLength::LongRunning) => &mut self.long_running,
            (FeatureLimit::Unlimited, ScenarioLength::Ordinary) => &mut self.ordinary,
        };
        queue.for_attempt(attempt).push_back(waiter);
    }

    /// Takes the waiter admission serves next, if `available_slots` cover its share.
    ///
    /// Waiters are offered in priority order, and the first eligible one decides: when its share
    /// is not yet free, nothing is admitted, so the slots that free up gather for it instead of
    /// going one at a time to the waiters behind it.
    fn next_eligible(&mut self, available_slots: usize) -> Option<(FeatureLimit, Waiter)> {
        for attempt in [ScenarioAttempt::Retry, ScenarioAttempt::First] {
            if self.available_wasm_state_reset > 0 {
                match self.wasm_state_reset.offer(attempt, 0, available_slots) {
                    Offer::Admitted(waiter) => return Some((FeatureLimit::WasmStateReset, waiter)),
                    Offer::Waiting => return None,
                    Offer::Empty => {}
                }
            }
            if self.available_web_console > 0 {
                let queue = self.web_console.for_attempt(attempt);
                let fairest = queue
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, waiter)| {
                        self.web_console_grants
                            .get(&waiter.feature_name)
                            .copied()
                            .unwrap_or_default()
                    })
                    .map(|(index, _)| index);
                if let Some(index) = fairest {
                    match self.web_console.offer(attempt, index, available_slots) {
                        Offer::Admitted(waiter) => return Some((FeatureLimit::WebConsole, waiter)),
                        Offer::Waiting => return None,
                        Offer::Empty => {}
                    }
                }
            }
            match self.long_running.offer(attempt, 0, available_slots) {
                Offer::Admitted(waiter) => return Some((FeatureLimit::Unlimited, waiter)),
                Offer::Waiting => return None,
                Offer::Empty => {}
            }
            match self.ordinary.offer(attempt, 0, available_slots) {
                Offer::Admitted(waiter) => return Some((FeatureLimit::Unlimited, waiter)),
                Offer::Waiting => return None,
                Offer::Empty => {}
            }
        }
        None
    }

    fn take_feature(&mut self, limit: FeatureLimit) {
        match limit {
            FeatureLimit::WebConsole => self.available_web_console -= 1,
            FeatureLimit::WasmStateReset => self.available_wasm_state_reset -= 1,
            FeatureLimit::Unlimited => {}
        }
    }

    fn release_feature(&mut self, limit: FeatureLimit) {
        match limit {
            FeatureLimit::WebConsole => self.available_web_console += 1,
            FeatureLimit::WasmStateReset => self.available_wasm_state_reset += 1,
            FeatureLimit::Unlimited => {}
        }
    }

    fn refresh_wait_reasons(&self) {
        for (limit, queue) in [
            (FeatureLimit::WasmStateReset, &self.wasm_state_reset),
            (FeatureLimit::WebConsole, &self.web_console),
            (FeatureLimit::Unlimited, &self.long_running),
            (FeatureLimit::Unlimited, &self.ordinary),
        ] {
            let reason = self.waiting_for(limit);
            for waiter in queue.waiting() {
                let previous = *waiter.reason.borrow();
                if previous != reason {
                    waiter.reason.send_replace(reason);
                }
            }
        }
    }
}

struct AdmissionPool {
    state: Mutex<AdmissionState>,
}

impl fmt::Debug for AdmissionPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("AdmissionPool").finish()
    }
}

impl AdmissionPool {
    fn dispatch(self: &StdArc<Self>, state: &mut AdmissionState) {
        while state.available_slots > 0 {
            let Some((limit, waiter)) = state.next_eligible(state.available_slots) else {
                break;
            };
            let slots = waiter.slots;
            state.available_slots = state
                .available_slots
                .checked_sub(slots)
                .verified("a waiter is offered only when the free run slots cover its share");
            state.take_feature(limit);
            let feature_name = waiter.feature_name;
            let admission = ScenarioAdmission {
                limit,
                slots,
                pool: self.clone(),
                active: true,
            };
            if let Err(mut abandoned) = waiter.grant.send(admission) {
                // A canceled receiver returns the admission. Disarm it before dropping it under
                // this lock, then offer the same capacity to the next waiter.
                abandoned.active = false;
                drop(abandoned);
                state.available_slots += slots;
                state.release_feature(limit);
            } else if limit == FeatureLimit::WebConsole {
                *state.web_console_grants.entry(feature_name).or_default() += 1;
            }
        }
        state.refresh_wait_reasons();
    }

    fn release(self: &StdArc<Self>, limit: FeatureLimit, slots: usize) {
        let mut state = self.state.lock();
        // Give the feature capacity and its run slots back in one decision. Otherwise an ordinary
        // waiter can consume a slot before a feature waiter awakened by the release is polled.
        state.available_slots += slots;
        state.release_feature(limit);
        self.dispatch(&mut state);
    }
}

impl ScenarioRunSlots {
    pub(crate) fn new(slots: usize) -> Self {
        assert!(slots > 0, "a scenario suite needs at least one run slot");
        Self {
            slots,
            pool: StdArc::new(AdmissionPool {
                state: Mutex::new(AdmissionState {
                    available_slots: slots,
                    available_web_console: 2,
                    available_wasm_state_reset: 1,
                    ..AdmissionState::default()
                }),
            }),
        }
    }

    /// Admits `run` of a scenario once its feature capacity and its share of the run slots are
    /// both free, reporting through `waiting_for` what it waits for meanwhile.
    pub(crate) async fn admit_with(
        &self,
        limit: FeatureLimit,
        length: ScenarioLength,
        weight: ScenarioWeight,
        feature_name: &str,
        run: ScenarioRun,
        mut waiting_for: impl FnMut(AdmissionWait),
    ) -> ScenarioAdmission {
        let (grant, mut granted) = oneshot::channel();
        let (mut reason_changes, mut reason) = {
            let mut state = self.pool.state.lock();
            let reason = state.waiting_for(limit);
            let (reason_sender, reason_changes) = watch::channel(reason);
            state.queue(
                limit,
                length,
                run.attempt(),
                Waiter {
                    feature_name: feature_name.to_owned(),
                    slots: run.run_slots(self.slots, weight),
                    grant,
                    reason: reason_sender,
                },
            );
            self.pool.dispatch(&mut state);
            (reason_changes, reason)
        };
        waiting_for(reason);
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                biased;
                admission = &mut granted => {
                    return admission.assured(
                        "the admission pool holds every queued sender until it grants or the run ends"
                    );
                }
                changed = reason_changes.changed() => {
                    if changed.is_ok() {
                        let current = *reason_changes.borrow_and_update();
                        if current != reason {
                            waiting_for(current);
                            reason = current;
                        }
                    }
                }
            }
        }
    }
}
