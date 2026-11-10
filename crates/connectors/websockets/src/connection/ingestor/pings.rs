//! Configured client ping cadence and its one bounded response opportunity after a missed tick.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The cadence, relevant successful ping write, and an unwritten or unanswered probe.
//! - **Depends on.** Durations and the primitive boundary's monotonic clock and interval.
//! - **Must not know.** Transport frames, configuration Models, graph state or host admission.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns connection-local ping and response-window state"
    )
)]

use std::time::Duration;

use meticulous::OptionExt as _;
use nervix_primitives::time::{Instant, Interval, MissedTickBehavior, interval};

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "one scheduled interval plus one whole overdue interval define a missed ping"
    )
)]
const MISSED_PING_PERIODS: u32 = 2;

enum PingActivity {
    /// The inbound-activity anchor is newer than any relevant configured ping write.
    Fresh,
    WrittenAt(Instant),
    ProbePending,
    AwaitingReplyUntil(Instant),
}

pub(super) struct WebsocketPings {
    cadence: Interval,
    activity: PingActivity,
}

impl WebsocketPings {
    pub(super) fn new(period: Duration) -> Self {
        let mut cadence = interval(period);
        cadence.set_missed_tick_behavior(MissedTickBehavior::Delay);
        cadence.reset();
        Self {
            cadence,
            activity: PingActivity::Fresh,
        }
    }

    pub(super) async fn tick(&mut self) {
        self.cadence.tick().await;
    }

    pub(super) fn response_deadline(&self) -> Option<Instant> {
        match self.activity {
            PingActivity::AwaitingReplyUntil(deadline) => Some(deadline),
            PingActivity::Fresh | PingActivity::WrittenAt(_) | PingActivity::ProbePending => None,
        }
    }

    /// A received frame becomes the newest activity anchor, and ends any outstanding probe.
    pub(super) fn received(&mut self) {
        self.activity = PingActivity::Fresh;
    }

    pub(super) fn written(&mut self, now: Instant, idle_bound: Duration) {
        match self.activity {
            PingActivity::ProbePending => {
                let deadline = now
                    .checked_add(idle_bound)
                    .verified("client deadlines are held to the schedulable duration bound");
                self.activity = PingActivity::AwaitingReplyUntil(deadline);
                // The probe replaces the overdue tick. It must not be followed by another
                // immediate ping from that same tick when reading starts again.
                self.cadence.reset();
            }
            PingActivity::AwaitingReplyUntil(_) => {}
            PingActivity::Fresh | PingActivity::WrittenAt(_) => {
                self.activity = PingActivity::WrittenAt(now);
            }
        }
    }

    /// Retains an unwritten probe across cancellation, but never renews an unanswered one.
    pub(super) fn offer_after_idle(&mut self, last_received: Instant, now: Instant) -> bool {
        let mut latest_activity = last_received;
        match self.activity {
            PingActivity::ProbePending => return true,
            PingActivity::AwaitingReplyUntil(_) => return false,
            PingActivity::WrittenAt(written) => latest_activity = written,
            PingActivity::Fresh => {}
        }
        let missed = self
            .cadence
            .period()
            .checked_mul(MISSED_PING_PERIODS)
            .assured("twice a schedulable client period fits Duration's u64 seconds");
        if now.duration_since(latest_activity) < missed {
            return false;
        }
        self.activity = PingActivity::ProbePending;
        true
    }
}
