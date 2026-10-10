//! Shared observation and release barriers for the fault harness.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Watch-backed reached, released and delivered observations and one-shot claims.
//! - **Depends on.** Execution-sensitive watch channels and atomics from the primitive boundary.
//! - **Must not know.** Fault selection, runtime ownership, schedules or product policy.

use meticulous::ResultExt as _;
use nervix_primitives::sync::{atomic::AtomicBool, watch};

#[derive(Debug, Clone, Copy, Default)]
struct TestPauseState {
    reached: bool,
    released: bool,
    delivered: bool,
}

#[derive(Debug)]
pub(super) struct TestPause {
    state: watch::Sender<TestPauseState>,
    /// Command pauses are one-shot: later matching requests must be able to drive the failure
    /// while the selected request remains at its barrier.
    pub(super) claimed: AtomicBool,
}

impl Default for TestPause {
    fn default() -> Self {
        Self {
            state: watch::channel(TestPauseState::default()).0,
            claimed: AtomicBool::new(false),
        }
    }
}

impl TestPause {
    pub(super) fn reach(&self) {
        self.state.send_modify(|state| state.reached = true);
    }

    pub(super) async fn wait_until_reached(&self) {
        self.state
            .subscribe()
            .wait_for(|state| state.reached)
            .await
            .assured("the pause owns its state sender for the full wait");
    }

    pub(super) async fn wait_until_released(&self) {
        self.state
            .subscribe()
            .wait_for(|state| state.released)
            .await
            .assured("the pause owns its state sender for the full wait");
    }

    pub(super) async fn wait_until_delivered(&self) {
        self.state
            .subscribe()
            .wait_for(|state| state.delivered)
            .await
            .assured("the pause owns its state sender for the full wait");
    }

    pub(super) fn release(&self) {
        self.state.send_modify(|state| state.released = true);
    }

    pub(super) fn mark_delivered(&self) {
        self.state.send_modify(|state| state.delivered = true);
    }
}
