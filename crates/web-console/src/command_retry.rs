//! Retry availability of uncertain browser commands.
//!
//! Layer: edges.
//! - **Owns.** One bounded delay and waiting/ready state per retained command's original issue order.
//! - **Depends on.** The console's issue identity and existing reconnect-delay policy.
//! - **Must not know.** Sockets, browser timers, query text, replies or server execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns browser command retry availability")
)]

use std::{collections::BTreeMap, time::Duration};

use meticulous::OptionExt as _;

use super::{IssueOrder, WEBSOCKET_INITIAL_RECONNECT_DELAY, WEBSOCKET_MAX_RECONNECT_DELAY};

const _: () = assert!(!WEBSOCKET_INITIAL_RECONNECT_DELAY.is_zero());
const _: () = assert!(
    WEBSOCKET_INITIAL_RECONNECT_DELAY.as_nanos() <= WEBSOCKET_MAX_RECONNECT_DELAY.as_nanos()
);
const _: () = assert!(WEBSOCKET_MAX_RECONNECT_DELAY.checked_mul(2).is_some());

#[derive(Clone, Copy)]
enum RetryState {
    Waiting(Duration),
    Ready(Duration),
}

impl RetryState {
    fn is_waiting(self) -> bool {
        matches!(self, Self::Waiting(_))
    }
}

/// Timer work owned by the connection that received the uncertain reply. It carries no payload.
pub(super) struct CommandRetryWait {
    pub(super) order: IssueOrder,
    pub(super) delay: Duration,
}

#[derive(Default)]
pub(super) struct CommandRetries {
    /// Only retained commands have entries, so the session's outstanding-request bound applies.
    retries: BTreeMap<IssueOrder, RetryState>,
}

impl CommandRetries {
    pub(super) fn wait(&mut self, order: IssueOrder) -> CommandRetryWait {
        let delay;
        match self.retries.entry(order) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                delay = WEBSOCKET_INITIAL_RECONNECT_DELAY;
                entry.insert(RetryState::Waiting(delay));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let RetryState::Ready(previous) = *entry.get() else {
                    panic!(
                        "only a command whose retry wait ended can have another attempt answered"
                    );
                };
                let doubled = previous
                    .checked_mul(2)
                    .assured("the reconnect ceiling's checked compile-time bound permits doubling");
                delay = doubled.min(WEBSOCKET_MAX_RECONNECT_DELAY);
                entry.insert(RetryState::Waiting(delay));
            }
        }
        CommandRetryWait { order, delay }
    }

    pub(super) fn is_uncertain(&self, order: IssueOrder) -> bool {
        self.retries.contains_key(&order)
    }

    pub(super) fn is_waiting(&self, order: IssueOrder) -> bool {
        if let Some(state) = self.retries.get(&order) {
            state.is_waiting()
        } else {
            false
        }
    }

    pub(super) fn ready(&mut self, order: IssueOrder) {
        if let Some(state) = self.retries.get_mut(&order)
            && let RetryState::Waiting(delay) = *state
        {
            *state = RetryState::Ready(delay);
        }
    }

    pub(super) fn finished(&mut self, order: IssueOrder) {
        self.retries.remove(&order);
    }

    /// The ended connection drops its timer futures. Its retained commands can be replayed
    /// after the replacement's setup; further uncertain answers retain their growing backoff.
    pub(super) fn connection_ended(&mut self) {
        for state in self.retries.values_mut() {
            if let RetryState::Waiting(delay) = *state {
                *state = RetryState::Ready(delay);
            }
        }
    }

    pub(super) fn clear(&mut self) {
        self.retries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_answers_grow_the_existing_delay_up_to_its_ceiling() {
        let mut retries = CommandRetries::default();
        let order = IssueOrder(1);
        for millis in [250, 500, 1000, 2000, 4000, 5000, 5000] {
            let wait = retries.wait(order);
            assert_eq!(wait.order, order);
            assert_eq!(wait.delay, Duration::from_millis(millis));
            assert!(retries.is_waiting(order));
            retries.ready(order);
            assert!(!retries.is_waiting(order));
        }
    }

    #[test]
    fn commands_keep_independent_delays_and_finish_releases_their_state() {
        let mut retries = CommandRetries::default();
        let first = IssueOrder(1);
        let second = IssueOrder(2);
        retries.wait(first);
        retries.ready(first);
        assert_eq!(retries.wait(first).delay, Duration::from_millis(500));
        assert_eq!(
            retries.wait(second).delay,
            WEBSOCKET_INITIAL_RECONNECT_DELAY
        );
        retries.finished(first);
        assert_eq!(retries.retries.len(), 1);
        assert!(retries.is_waiting(second));
        assert_eq!(
            retries.wait(IssueOrder(3)).delay,
            WEBSOCKET_INITIAL_RECONNECT_DELAY
        );
        retries.clear();
        assert!(retries.retries.is_empty());
    }

    #[test]
    fn replacement_connections_release_waits_and_retain_the_next_delay() {
        let mut retries = CommandRetries::default();
        let order = IssueOrder(1);
        retries.wait(order);
        retries.connection_ended();
        assert!(!retries.is_waiting(order));
        assert_eq!(retries.wait(order).delay, Duration::from_millis(500));
    }
}
