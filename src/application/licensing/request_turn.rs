//! Serialize licensing requests while letting an operator expedite a sleeping refresh retry.
//!
//! Layer: control plane.
//! - **Owns.** One request turn and one coalescing refresh wakeup per node.
//! - **Depends on.** Selected async mutual exclusion, notification and pure future polling.
//! - **Must not know.** HTTP, signed documents, policy grants or graph execution.

use futures_util::FutureExt as _;
use nervix_primitives::sync::{Mutex, MutexGuard, Notify};

#[derive(Default)]
pub(in crate::application) struct LicensingRequests {
    turn: Mutex<()>,
    refresh: Notify,
}

impl LicensingRequests {
    pub(super) async fn background(&self) -> MutexGuard<'_, ()> {
        self.turn.lock().await
    }

    pub(super) async fn foreground(&self) -> MutexGuard<'_, ()> {
        // The current owner may be sleeping in retry backoff while holding the turn.
        self.refresh.notify_one();
        let turn = self.turn.lock().await;
        // An uncontended foreground request is itself the requested attempt. Do not leave its
        // stored permit to bypass the next automatic backoff.
        match self.refresh.notified().now_or_never() {
            Some(()) | None => {}
        }
        turn
    }

    pub(super) async fn refresh_requested(&self) {
        self.refresh.notified().await;
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
}
