//! Admission to the scenario suite's run slots and its limited feature groups.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Feature-group admission, run-slot admission, feature take-up priority, and the
//!   long-running declaration that orders run-slot admission.
//! - **Depends on.** Tokio channels, a short synchronous queue lock, and feature names and tags
//!   supplied by the parser.
//! - **Must not know.** Scenario steps, node state, or Cucumber's result writer.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::{StdArc, blocking::Mutex, oneshot, watch};

pub(crate) const WEB_CONSOLE_FEATURE_NAMES: [&str; 4] = [
    "Web console NSPL REPL",
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
    pool: StdArc<AdmissionPool>,
    active: bool,
}

impl ScenarioAdmission {
    pub(crate) fn limit(&self) -> FeatureLimit {
        self.limit
    }
}

impl Drop for ScenarioAdmission {
    fn drop(&mut self) {
        if self.active {
            self.pool.release(self.limit);
        }
    }
}

pub(crate) struct ScenarioRunSlots {
    pool: StdArc<AdmissionPool>,
}

struct Waiter {
    feature_name: String,
    grant: oneshot::Sender<ScenarioAdmission>,
    reason: watch::Sender<AdmissionWait>,
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

    fn next_eligible(&mut self) -> Option<(FeatureLimit, Waiter)> {
        for attempt in [ScenarioAttempt::Retry, ScenarioAttempt::First] {
            if self.available_wasm_state_reset > 0
                && let Some(waiter) = self.wasm_state_reset.for_attempt(attempt).pop_front()
            {
                return Some((FeatureLimit::WasmStateReset, waiter));
            }
            if self.available_web_console > 0 {
                let queue = self.web_console.for_attempt(attempt);
                let next = queue.iter().enumerate().min_by_key(|(_, waiter)| {
                    self.web_console_grants
                        .get(&waiter.feature_name)
                        .copied()
                        .unwrap_or_default()
                });
                if let Some((index, _)) = next {
                    let waiter = queue
                        .remove(index)
                        .verified("the selected web console waiter is still at its queue index");
                    return Some((FeatureLimit::WebConsole, waiter));
                }
            }
            if let Some(waiter) = self.long_running.for_attempt(attempt).pop_front() {
                return Some((FeatureLimit::Unlimited, waiter));
            }
            if let Some(waiter) = self.ordinary.for_attempt(attempt).pop_front() {
                return Some((FeatureLimit::Unlimited, waiter));
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
            let Some((limit, waiter)) = state.next_eligible() else {
                break;
            };
            state.available_slots -= 1;
            state.take_feature(limit);
            let feature_name = waiter.feature_name;
            let admission = ScenarioAdmission {
                limit,
                pool: self.clone(),
                active: true,
            };
            if let Err(mut abandoned) = waiter.grant.send(admission) {
                // A canceled receiver returns the admission. Disarm it before dropping it under
                // this lock, then offer the same capacity to the next waiter.
                abandoned.active = false;
                drop(abandoned);
                state.available_slots += 1;
                state.release_feature(limit);
            } else if limit == FeatureLimit::WebConsole {
                *state.web_console_grants.entry(feature_name).or_default() += 1;
            }
        }
        state.refresh_wait_reasons();
    }

    fn release(self: &StdArc<Self>, limit: FeatureLimit) {
        let mut state = self.state.lock();
        // Give the feature capacity and its run slot back in one decision. Otherwise an ordinary
        // waiter can consume the slot before a feature waiter awakened by the release is polled.
        state.available_slots += 1;
        state.release_feature(limit);
        self.dispatch(&mut state);
    }
}

impl ScenarioRunSlots {
    pub(crate) fn new(slots: usize) -> Self {
        assert!(slots > 0, "a scenario suite needs at least one run slot");
        Self {
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

    pub(crate) async fn admit_with(
        &self,
        limit: FeatureLimit,
        length: ScenarioLength,
        feature_name: &str,
        attempt: ScenarioAttempt,
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
                attempt,
                Waiter {
                    feature_name: feature_name.to_owned(),
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
