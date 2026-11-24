//! Relay-tab restoration policy.
//! Layer: edges.
//! - **Owns.** The acknowledged schema and bounded waits between restoration attempts.
//! - **Depends on.** The client wire schema and the restoration backoff of the pacing the console
//!   follows, whose first wait is also the period of the session's retry timer.
//! - **Must not know.** Browser transport, rendered output or server graph ownership.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        edge,
        reason = "owns relay-tab restoration contracts and retry availability"
    )
)]

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::RowSchema;
use nervix_primitives::sync::Arc;

use crate::pacing::Backoff;

/// A page-local count of completed retry timer intervals, independent of physical UTC.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RetryTick(u64);

impl RetryTick {
    pub(crate) fn advance(&mut self) {
        self.0 = self.0.checked_add(1).assured(
            "a page cannot remain open for the u64 range of retry intervals of at least a \
             millisecond",
        );
    }

    /// The tick at which a wait of `delay` that starts now has passed, counted in intervals of
    /// `period`, the session's retry timer.
    fn after(self, delay: Duration, period: Duration) -> Self {
        let intervals = delay.as_nanos().div_ceil(period.as_nanos());
        let intervals = u64::try_from(intervals).assured(
            "a wait of at most a day counted in timer intervals of at least a millisecond fits a \
             u64",
        );
        // A refusal may arrive immediately before the next tick. Skip that partial interval so
        // the next attempt waits at least the delay its notice reports.
        let intervals = intervals
            .checked_add(1)
            .assured("the bounded wait has room for a tick");
        Self(self.0.checked_add(intervals).assured(
            "a page and its bounded retry wait cannot exhaust the u64 timer interval range",
        ))
    }
}

/// The desired tab's original contract and the wait after its last refusal. State clones share the
/// schema while a request gets its own wire value; no retry rebuilds it from a relay's current
/// schema.
#[derive(Clone)]
pub(crate) struct TabRestoration {
    schema: Arc<RowSchema>,
    /// The wait the last refusal of this sequence took, or `None` before its first refusal.
    last_delay: Option<Duration>,
}

impl TabRestoration {
    pub(crate) fn new(schema: RowSchema) -> Self {
        Self {
            schema: Arc::new(schema),
            last_delay: None,
        }
    }

    pub(crate) fn schema(&self) -> RowSchema {
        self.schema.as_ref().clone()
    }

    /// A replacement session tries immediately with its own refusal sequence.
    pub(crate) fn reconnect(&mut self) {
        self.last_delay = None;
    }

    /// Waits the first wait of `backoff` after a sequence's first refusal, and twice the last
    /// wait, up to the backoff's ceiling, after every further one. The session's retry timer ticks
    /// at the backoff's first wait, so the wait is counted in its ticks.
    pub(crate) fn refused(&mut self, now: RetryTick, backoff: Backoff) -> RetryWait {
        let delay = match self.last_delay {
            Some(previous) => backoff.after(previous),
            None => backoff.first(),
        };
        self.last_delay = Some(delay);
        RetryWait {
            delay,
            ready_at: now.after(delay, backoff.first()),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RetryWait {
    pub(crate) delay: Duration,
    pub(crate) ready_at: RetryTick,
}
