//! Admission to the scenario suite's run slots and its limited feature groups.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Feature-group admission, run-slot admission, and feature take-up priority.
//! - **Depends on.** Tokio channels, a short synchronous queue lock, and feature names supplied by
//!   the parser.
//! - **Must not know.** Scenario steps, node state, or Cucumber's result writer.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FeatureLimit {
    WebConsole,
    WasmStateReset,
    Unlimited,
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
struct AdmissionState {
    available_slots: usize,
    available_web_console: usize,
    available_wasm_state_reset: usize,
    web_console: VecDeque<Waiter>,
    web_console_grants: BTreeMap<String, usize>,
    wasm_state_reset: VecDeque<Waiter>,
    ordinary: VecDeque<Waiter>,
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

    fn queue(&mut self, limit: FeatureLimit, waiter: Waiter) {
        match limit {
            FeatureLimit::WebConsole => self.web_console.push_back(waiter),
            FeatureLimit::WasmStateReset => self.wasm_state_reset.push_back(waiter),
            FeatureLimit::Unlimited => self.ordinary.push_back(waiter),
        }
    }

    fn next_eligible(&mut self) -> Option<(FeatureLimit, Waiter)> {
        if self.available_wasm_state_reset > 0
            && let Some(waiter) = self.wasm_state_reset.pop_front()
        {
            return Some((FeatureLimit::WasmStateReset, waiter));
        }
        if self.available_web_console > 0
            && let Some(index) = self
                .web_console
                .iter()
                .enumerate()
                .min_by_key(|(_, waiter)| {
                    self.web_console_grants
                        .get(&waiter.feature_name)
                        .copied()
                        .unwrap_or_default()
                })
                .map(|(index, _)| index)
        {
            let waiter = self
                .web_console
                .remove(index)
                .verified("the selected web console waiter is still at its queue index");
            return Some((FeatureLimit::WebConsole, waiter));
        }
        self.ordinary
            .pop_front()
            .map(|waiter| (FeatureLimit::Unlimited, waiter))
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
            (FeatureLimit::Unlimited, &self.ordinary),
        ] {
            let reason = self.waiting_for(limit);
            for waiter in queue {
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
        feature_name: &str,
        mut waiting_for: impl FnMut(AdmissionWait),
    ) -> ScenarioAdmission {
        let (grant, mut granted) = oneshot::channel();
        let (mut reason_changes, mut reason) = {
            let mut state = self.pool.state.lock();
            let reason = state.waiting_for(limit);
            let (reason_sender, reason_changes) = watch::channel(reason);
            state.queue(
                limit,
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
