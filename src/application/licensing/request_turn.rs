//! Serialize licensing requests and coalesce operator and notification retry wakeups.
//!
//! Layer: control plane.
//! - **Owns.** One request turn and independent coalescing operator/notification wakeups per node.
//! - **Depends on.** Selected async mutual exclusion, notification and pure future polling.
//! - **Must not know.** HTTP, signed documents, policy grants or graph execution.

use futures_util::FutureExt as _;
use nervix_primitives::sync::{Mutex, MutexGuard, Notify};
use nervix_recovery::Discarded as _;

#[derive(Default)]
pub(in crate::application) struct LicensingRequests {
    turn: Mutex<()>,
    refresh: Notify,
    notifications: Notify,
}

impl LicensingRequests {
    pub(super) fn expedite(&self) {
        self.notifications.notify_one();
    }

    fn consume_notification(&self) {
        self.notifications.notified().now_or_never().discarded(
            "the new attempt already represents a preceding notification, if one remains",
        );
    }
    pub(super) async fn background(&self) -> MutexGuard<'_, ()> {
        let turn = self.turn.lock().await;
        // A queued attempt already represents every hint that preceded its turn. A signal left
        // by a completed request must not shorten this attempt's first automatic retry.
        self.consume_notification();
        turn
    }

    pub(super) async fn foreground(&self) -> MutexGuard<'_, ()> {
        // The current owner may be sleeping in retry backoff while holding the turn.
        self.refresh.notify_one();
        let turn = self.turn.lock().await;
        // An uncontended foreground request is itself the requested attempt. Do not leave its
        // stored permit to bypass the next automatic backoff.
        self.refresh.notified().now_or_never().discarded(
            "the foreground attempt owns the turn whether its stored wakeup was consumed or absent",
        );
        self.consume_notification();
        turn
    }

    pub(super) async fn refresh_requested(&self) {
        nervix_primitives::select! {
            _ = self.refresh.notified() => {},
            _ = self.notifications.notified() => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use meticulous::ResultExt as _;
    use nervix_primitives::time::timeout;

    use super::*;

    #[nervix_primitives::test]
    async fn foreground_refresh_wakes_the_current_retry_before_waiting_for_its_turn() {
        let requests = LicensingRequests::default();
        let background = requests.background().await;
        let mut foreground = std::pin::pin!(requests.foreground());
        assert!(futures_util::poll!(&mut foreground).is_pending());
        timeout(Duration::from_secs(1), requests.refresh_requested())
            .await
            .assured("the foreground request wakes the retry despite the held turn");
        assert!(requests.turn.try_lock().is_err());
        drop(background);
        let foreground = timeout(Duration::from_secs(1), foreground)
            .await
            .assured("the foreground request takes the released turn");
        drop(foreground);
        assert!(requests.refresh_requested().now_or_never().is_none());
    }

    #[nervix_primitives::test]
    async fn an_uncontended_foreground_request_leaves_no_automatic_retry_wakeup() {
        let requests = LicensingRequests::default();
        let turn = requests.foreground().await;
        assert!(requests.refresh_requested().now_or_never().is_none());
        drop(turn);
    }

    #[nervix_primitives::test]
    async fn a_queued_background_attempt_consumes_the_hint_that_requested_it() {
        let requests = LicensingRequests::default();
        let foreground = requests.foreground().await;
        let mut background = std::pin::pin!(requests.background());
        assert!(futures_util::poll!(&mut background).is_pending());
        requests.expedite();
        drop(foreground);
        let background = background.await;
        assert!(
            requests.refresh_requested().now_or_never().is_none(),
            "the queued attempt already represents the hint; its first retry keeps its backoff"
        );
        drop(background);
    }

    #[nervix_primitives::test]
    async fn a_hint_wakes_a_foreground_retry_before_the_queued_background_attempt() {
        let requests = LicensingRequests::default();
        let foreground = requests.foreground().await;
        let mut background = std::pin::pin!(requests.background());
        assert!(futures_util::poll!(&mut background).is_pending());
        requests.expedite();
        timeout(Duration::from_secs(1), requests.refresh_requested())
            .await
            .assured("a hint wakes whichever refresh currently holds the turn");
        assert!(futures_util::poll!(&mut background).is_pending());
        drop(foreground);
        let background = background.await;
        assert!(requests.refresh_requested().now_or_never().is_none());
        drop(background);
    }

    #[nervix_primitives::test]
    async fn a_queued_background_attempt_preserves_another_waiting_operators_wakeup() {
        let requests = LicensingRequests::default();
        let current = requests.foreground().await;
        let mut background = std::pin::pin!(requests.background());
        assert!(futures_util::poll!(&mut background).is_pending());
        let mut foreground = std::pin::pin!(requests.foreground());
        assert!(futures_util::poll!(&mut foreground).is_pending());
        drop(current);
        let background = background.await;
        assert!(
            requests.refresh_requested().now_or_never().is_some(),
            "the next operator must still be able to expedite the owner ahead of its turn"
        );
        drop(background);
        let foreground = foreground.await;
        assert!(requests.refresh_requested().now_or_never().is_none());
        drop(foreground);
    }
}
