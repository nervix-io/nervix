//! Relay-tab restoration policy.
//! Layer: edges.
//! - **Owns.** The acknowledged schema and bounded waits between restoration attempts.
//! - **Depends on.** The client wire schema and the console's existing timer cadence and ceiling.
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

use crate::{SUBSCRIPTION_RETRY_DELAY, WEBSOCKET_MAX_RECONNECT_DELAY};

const _: () = assert!(!SUBSCRIPTION_RETRY_DELAY.is_zero());
const _: () =
    assert!(SUBSCRIPTION_RETRY_DELAY.as_nanos() <= WEBSOCKET_MAX_RECONNECT_DELAY.as_nanos());
const _: () = assert!(WEBSOCKET_MAX_RECONNECT_DELAY.checked_mul(2).is_some());

/// A page-local count of completed retry timer intervals, independent of physical UTC.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RetryTick(u64);

impl RetryTick {
    pub(crate) fn advance(&mut self) {
        self.0 = self
            .0
            .checked_add(1)
            .assured("a page cannot remain open for the u64 range of one-second retry intervals");
    }

    fn after(self, delay: Duration) -> Self {
        let intervals = delay
            .as_nanos()
            .div_ceil(SUBSCRIPTION_RETRY_DELAY.as_nanos());
        let intervals = u64::try_from(intervals)
            .assured("the console's retry ceiling fits a u64 count of timer intervals");
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

/// The desired tab's original contract and its next refusal delay. State clones share the schema
/// while a request gets its own wire value; no retry rebuilds it from a relay's current schema.
#[derive(Clone)]
pub(crate) struct TabRestoration {
    schema: Arc<RowSchema>,
    next_delay: Duration,
}

impl TabRestoration {
    pub(crate) fn new(schema: RowSchema) -> Self {
        Self {
            schema: Arc::new(schema),
            next_delay: SUBSCRIPTION_RETRY_DELAY,
        }
    }

    pub(crate) fn schema(&self) -> RowSchema {
        self.schema.as_ref().clone()
    }

    /// A replacement session tries immediately with its own refusal sequence.
    pub(crate) fn reconnect(&mut self) {
        self.next_delay = SUBSCRIPTION_RETRY_DELAY;
    }

    pub(crate) fn refused(&mut self, now: RetryTick) -> RetryWait {
        let delay = self.next_delay;
        let doubled = delay
            .checked_mul(2)
            .assured("twice the console's bounded retry ceiling fits Duration");
        self.next_delay = doubled.min(WEBSOCKET_MAX_RECONNECT_DELAY);
        RetryWait {
            delay,
            ready_at: now.after(delay),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RetryWait {
    pub(crate) delay: Duration,
    pub(crate) ready_at: RetryTick,
}
