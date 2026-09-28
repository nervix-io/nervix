//! Admission to the scenario suite's run slots and its limited feature groups.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Feature-group admission, run-slot admission, and feature take-up priority.
//! - **Depends on.** Tokio semaphores and channels, a short synchronous queue lock, and feature
//!   names supplied by the parser.
//! - **Must not know.** Scenario steps, node state, or Cucumber's result writer.

use std::{collections::VecDeque, fmt, sync::Arc};

use meticulous::ResultExt as _;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

pub(crate) const WEB_CONSOLE_FEATURE_NAMES: [&str; 3] = [
    "Web console NSPL REPL",
    "Web console execution graph",
    "Web console transaction inspector",
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
    _feature: Option<OwnedSemaphorePermit>,
    _slot: RunSlotPermit,
}

impl ScenarioAdmission {
    pub(crate) fn limit(&self) -> FeatureLimit {
        self.limit
    }
}

pub(crate) struct ScenarioRunSlots {
    slots: Arc<SlotPool>,
    web_console: Arc<Semaphore>,
    wasm_state_reset: Arc<Semaphore>,
}

#[derive(Default)]
struct SlotState {
    available: usize,
    limited: VecDeque<oneshot::Sender<RunSlotPermit>>,
    ordinary: VecDeque<oneshot::Sender<RunSlotPermit>>,
}

struct SlotPool {
    state: Mutex<SlotState>,
}

/// One occupied run slot. A canceled waiter receives this guard in its channel, and dropping the
/// channel gives the slot back even when the waiter was canceled just after it was selected.
pub(crate) struct RunSlotPermit {
    pool: Arc<SlotPool>,
    active: bool,
}

impl fmt::Debug for RunSlotPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("RunSlotPermit").finish()
    }
}

impl Drop for RunSlotPermit {
    fn drop(&mut self) {
        if self.active {
            self.pool.release();
        }
    }
}

impl SlotPool {
    fn dispatch(self: &Arc<Self>, state: &mut SlotState) {
        while state.available > 0 {
            let next = state
                .limited
                .pop_front()
                .or_else(|| state.ordinary.pop_front());
            let Some(waiter) = next else {
                break;
            };
            state.available -= 1;
            let permit = RunSlotPermit {
                pool: self.clone(),
                active: true,
            };
            if let Err(mut abandoned) = waiter.send(permit) {
                // Keep the slot in this dispatch loop. Disarm before drop so a canceled receiver
                // cannot recursively lock the same pool.
                abandoned.active = false;
                drop(abandoned);
                state.available += 1;
            }
        }
    }

    async fn acquire(self: &Arc<Self>, limit: FeatureLimit) -> RunSlotPermit {
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.state.lock();
            if limit == FeatureLimit::Unlimited {
                state.ordinary.push_back(sender);
            } else {
                state.limited.push_back(sender);
            }
            self.dispatch(&mut state);
        }
        receiver
            .await
            .assured("run slot pool holds every queued sender until it grants or the run ends")
    }

    fn release(self: &Arc<Self>) {
        let mut state = self.state.lock();
        state.available += 1;
        self.dispatch(&mut state);
    }
}

impl ScenarioRunSlots {
    pub(crate) fn new(slots: usize) -> Self {
        Self {
            slots: Arc::new(SlotPool {
                state: Mutex::new(SlotState {
                    available: slots,
                    ..SlotState::default()
                }),
            }),
            web_console: Arc::new(Semaphore::new(2)),
            wasm_state_reset: Arc::new(Semaphore::new(1)),
        }
    }

    pub(crate) async fn acquire_feature(
        &self,
        feature: FeatureLimit,
    ) -> Option<OwnedSemaphorePermit> {
        let permits = match feature {
            FeatureLimit::WebConsole => &self.web_console,
            FeatureLimit::WasmStateReset => &self.wasm_state_reset,
            FeatureLimit::Unlimited => return None,
        };
        Some(
            permits
                .clone()
                .acquire_owned()
                .await
                .assured("scenario feature semaphores remain open for the run"),
        )
    }

    pub(crate) async fn admit_with(
        &self,
        limit: FeatureLimit,
        mut waiting_for: impl FnMut(AdmissionWait),
    ) -> ScenarioAdmission {
        if let Some(reason) = match limit {
            FeatureLimit::WebConsole => Some(AdmissionWait::WebConsole),
            FeatureLimit::WasmStateReset => Some(AdmissionWait::WasmStateReset),
            FeatureLimit::Unlimited => None,
        } {
            waiting_for(reason);
        }
        let feature = self.acquire_feature(limit).await;
        waiting_for(AdmissionWait::RunSlot);
        let slot = self.slots.acquire(limit).await;
        ScenarioAdmission {
            limit,
            _feature: feature,
            _slot: slot,
        }
    }
}
