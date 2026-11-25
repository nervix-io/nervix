//! Serialize licensing requests, give the turn to waiting operators and hurry retries.
//!
//! Layer: control plane.
//! - **Owns.** One request turn per node, the operators waiting for it, and the retry wakeups of
//!   the attempt that holds it.
//! - **Depends on.** Selected async mutual exclusion, value publication, notification and pure
//!   future polling.
//! - **Must not know.** HTTP, signed documents, policy grants or graph execution.
//!
//! Every licensing request takes the one turn: an operator's statement through `foreground`, and
//! an automatic refresh, a resumed release or an activation's preparation through `background`. An
//! operator's request counts from the moment it asks until it holds the turn or its statement stops
//! waiting, and the count is published, so the attempt that holds the turn sees every operator
//! waiting behind it however their requests interleave. While one waits, a release gives the turn
//! back (`foreground_requested`), and a refresh between two retries retries at once
//! (`refresh_requested`), once for each operator request it has not been hurried for, so its backoff
//! still spaces every later retry. An attempt that takes the turn while operators already wait is
//! hurried once for all of them, and an operator's own request never hurries the attempt it makes.
//!
//! A service notification hurries a refresh between retries the same way but never makes a release
//! give its turn away, and an attempt that takes the turn represents every notification that
//! preceded it.

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::{
    Mutex, MutexGuard, Notify,
    atomic::{AtomicU64, Ordering},
    watch,
};
use nervix_recovery::Discarded as _;

#[derive(Default)]
pub(in crate::application) struct LicensingRequests {
    turn: Mutex<()>,
    /// The operator requests for the turn, published to the attempt that holds it.
    operators: watch::Sender<OperatorRequests>,
    /// The operator requests the attempt holding the turn has already been hurried for. Only that
    /// attempt reads or writes it, and the turn orders one holder after the next.
    hurried: AtomicU64,
    notifications: Notify,
}

/// The operator requests for the request turn.
#[derive(Clone, Copy, Default)]
struct OperatorRequests {
    /// Requests whose operators neither hold the turn yet nor stopped waiting for it.
    waiting: usize,
    /// Every operator request made on this node, held, waiting or given up. It only grows.
    made: u64,
}

impl OperatorRequests {
    /// The operator requests an attempt that takes the turn now has already been hurried for:
    /// every one when no operator waits behind it, and all but one while operators wait, so their
    /// requests hurry its first retry once.
    fn already_hurried(&self) -> u64 {
        if self.waiting == 0 {
            return self.made;
        }
        self.made
            .checked_sub(1)
            .verified("every waiting operator counted its request, so at least one was made")
    }
}

/// An operator's request, from the moment it asks for the turn until it holds the turn or its
/// statement stops waiting.
struct WaitingOperator<'a> {
    /// The requests this one is counted among as waiting, until it stops waiting.
    requests: Option<&'a watch::Sender<OperatorRequests>>,
}

impl<'a> WaitingOperator<'a> {
    fn ask(requests: &'a watch::Sender<OperatorRequests>) -> Self {
        requests.send_modify(|operators| {
            operators.waiting = operators
                .waiting
                .checked_add(1)
                .assured("a node runs far fewer than usize::MAX operator statements at once");
            operators.made = operators
                .made
                .checked_add(1)
                .assured("a node serves far fewer than u64::MAX operator requests");
        });
        Self {
            requests: Some(requests),
        }
    }

    /// The operator holds the turn and stops waiting. Returns the requests as it left them.
    fn take_turn(mut self) -> OperatorRequests {
        self.stop_waiting()
            .verified("an operator waits until it takes the turn, and it takes the turn once")
    }

    fn stop_waiting(&mut self) -> Option<OperatorRequests> {
        let requests = self.requests.take()?;
        let mut left = OperatorRequests::default();
        requests.send_modify(|operators| {
            operators.waiting = operators
                .waiting
                .checked_sub(1)
                .verified("this operator's own request is still counted as waiting");
            left = *operators;
        });
        Some(left)
    }
}

impl Drop for WaitingOperator<'_> {
    fn drop(&mut self) {
        self.stop_waiting().discarded(
            "a statement that gave up stops waiting here; one that took the turn already did",
        );
    }
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

    /// Begin the turn of the attempt that just took it, with the operators it found waiting.
    fn begin_turn(&self, operators: OperatorRequests) {
        self.hurried
            .store(operators.already_hurried(), Ordering::SeqCst);
    }

    pub(super) async fn background(&self) -> MutexGuard<'_, ()> {
        let turn = self.turn.lock().await;
        // A queued attempt already represents every notification that preceded its turn. A signal
        // left by a completed request must not shorten this attempt's first automatic retry.
        self.consume_notification();
        let operators = *self.operators.borrow();
        self.begin_turn(operators);
        turn
    }

    pub(super) async fn foreground(&self) -> MutexGuard<'_, ()> {
        // The attempt holding the turn may be releasing, or sleeping in retry backoff: asking
        // shows it this request.
        let waiting = WaitingOperator::ask(&self.operators);
        let turn = self.turn.lock().await;
        // This attempt is the request it made. Only operators still waiting behind it hurry it.
        let operators = waiting.take_turn();
        self.begin_turn(operators);
        self.consume_notification();
        turn
    }

    /// Completes when the attempt holding the turn should retry at once: for an operator request
    /// it has not been hurried for, or for a service notification.
    pub(super) async fn refresh_requested(&self) {
        let hurried = self.hurried.load(Ordering::SeqCst);
        let mut operators = self.operators.subscribe();
        nervix_primitives::select! {
            requested = operators.wait_for(|operators| operators.made > hurried) => {
                let made = requested
                    .assured("the request turn owns the sender its operators publish to")
                    .made;
                self.hurried.store(made, Ordering::SeqCst);
            }
            () = self.notifications.notified() => {}
        }
    }

    /// Completes while an operator waits for the turn.
    pub(super) async fn foreground_requested(&self) {
        let mut operators = self.operators.subscribe();
        let waiting = operators
            .wait_for(|operators| operators.waiting > 0)
            .await
            .assured("the request turn owns the sender its operators publish to");
        drop(waiting);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
    async fn a_notification_preserves_the_release_turn_until_an_operator_requests_it() {
        let requests = LicensingRequests::default();
        let release = requests.background().await;
        requests.expedite();
        assert!(requests.foreground_requested().now_or_never().is_none());
        assert!(requests.refresh_requested().now_or_never().is_some());
        let mut operator = std::pin::pin!(requests.foreground());
        assert!(futures_util::poll!(&mut operator).is_pending());
        timeout(Duration::from_secs(1), requests.foreground_requested())
            .await
            .assured("the waiting operator, rather than a notification, yields release's turn");
        drop(release);
        let operator = timeout(Duration::from_secs(1), operator)
            .await
            .assured("the operator takes the released turn");
        drop(operator);
        assert!(requests.refresh_requested().now_or_never().is_none());
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

    /// Two operators ask while another attempt holds the turn, and a background attempt queues
    /// between them. The first operator takes the turn and ends; the queued attempt that takes it
    /// next still has the second operator waiting behind it.
    async fn the_second_of_two_operators_waits_behind_a_queued_attempt(
        requests: &LicensingRequests,
        while_it_holds_the_turn: impl FnOnce(&LicensingRequests),
    ) {
        let holder = requests.background().await;
        let mut first = std::pin::pin!(requests.foreground());
        assert!(futures_util::poll!(&mut first).is_pending());
        let mut queued = std::pin::pin!(requests.background());
        assert!(futures_util::poll!(&mut queued).is_pending());
        let mut second = std::pin::pin!(requests.foreground());
        assert!(futures_util::poll!(&mut second).is_pending());
        drop(holder);
        let first = first.await;
        drop(first);
        let queued = queued.await;
        while_it_holds_the_turn(requests);
        drop(queued);
        let second = second.await;
        drop(second);
    }

    #[nervix_primitives::test]
    async fn a_release_gives_its_turn_to_the_second_of_two_operators_that_asked_together() {
        let requests = LicensingRequests::default();
        the_second_of_two_operators_waits_behind_a_queued_attempt(&requests, |requests| {
            assert!(
                requests.foreground_requested().now_or_never().is_some(),
                "the second operator still waits for the turn, so the release gives it back"
            );
        })
        .await;
    }

    #[nervix_primitives::test]
    async fn a_refresh_retry_is_hurried_for_the_second_of_two_operators_that_asked_together() {
        let requests = LicensingRequests::default();
        the_second_of_two_operators_waits_behind_a_queued_attempt(&requests, |requests| {
            assert!(
                requests.refresh_requested().now_or_never().is_some(),
                "the second operator still waits for the turn, so the refresh retries at once"
            );
        })
        .await;
    }

    #[nervix_primitives::test]
    async fn a_retry_is_hurried_once_for_each_operator_request() {
        let requests = LicensingRequests::default();
        let attempt = requests.background().await;
        let mut first = std::pin::pin!(requests.foreground());
        assert!(futures_util::poll!(&mut first).is_pending());
        assert!(
            requests.refresh_requested().now_or_never().is_some(),
            "the first operator's request hurries the retry"
        );
        assert!(
            requests.refresh_requested().now_or_never().is_none(),
            "the next retry keeps its backoff until another operator asks"
        );
        let mut second = std::pin::pin!(requests.foreground());
        assert!(futures_util::poll!(&mut second).is_pending());
        assert!(
            requests.refresh_requested().now_or_never().is_some(),
            "the second operator's request hurries the retry again"
        );
        assert!(requests.refresh_requested().now_or_never().is_none());
        drop(attempt);
        let first = first.await;
        drop(first);
        let second = second.await;
        drop(second);
    }

    #[nervix_primitives::test]
    async fn a_release_keeps_its_turn_once_the_only_waiting_operator_gave_up() {
        let requests = LicensingRequests::default();
        let release = requests.background().await;
        {
            let mut operator = std::pin::pin!(requests.foreground());
            assert!(futures_util::poll!(&mut operator).is_pending());
        }
        assert!(
            requests.foreground_requested().now_or_never().is_none(),
            "the operator's statement stopped waiting for the turn, so the release keeps it"
        );
        drop(release);
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "request_turn_shuttle_tests.rs"]
mod shuttle_tests;
