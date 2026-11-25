//! The licensing request turn, explored under Shuttle.
//!
//! Layer: test harness.
//!
//! - **Owns.** The invariants of the request turn: a background attempt that holds the turn never
//!   loses an operator's request, an uncontended operator request leaves no wakeup behind, a
//!   service notification hurries a refresh retry but never takes an operator's place, and a
//!   queued background attempt consumes the notification that preceded its turn.
//! - **Depends on.** The production `LicensingRequests` and the model harness's Shuttle runner.
//! - **Must not know.** HTTP, signed documents, or what an attempt asks the licensing service.
//!
//! Each attempt is a task that takes the turn the way its production caller does: an operator's
//! statement through `foreground`, an automatic refresh, a resumed release or an activation's
//! preparation through `background`. An attempt that holds the turn waits the way the licensing
//! client waits while it holds it: a release gives the turn back once `foreground_requested`
//! completes, and a refresh between two of its retries retries at once once `refresh_requested`
//! completes. The model's attempt then ends.
//!
//! The model has no clock. In place of the timer that ends a retry backoff, an attempt's wait also
//! ends once every operator of the model has had its turn. An operator can only have had its turn
//! before the attempt took it, so a request that reaches no attempt leaves an operator waiting for
//! the turn behind an attempt that never wakes, and Shuttle reports the deadlock.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use futures_util::FutureExt as _;
use meticulous::ResultExt as _;
use nervix_model_harness::shuttle::{check_pct, check_random};
use nervix_primitives::{
    sync::{
        Arc, CancellationToken,
        atomic::{AtomicUsize, Ordering},
    },
    task,
};
use shuttle::future::block_on;

use super::LicensingRequests;

const MODEL_TASKS_JOIN: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";
const PCT_DEPTH: usize = 3;
const PCT_ITERATIONS: usize = 1_000;
const RANDOM_ITERATIONS: usize = 1_000;

/// Explore a model under 1,000 random schedules and 1,000 PCT schedules of depth three. A
/// depth-first search cannot explore these models: the request turn's own `select!` asks the
/// scheduler for random data, which a depth-first search refuses.
fn explore(model: impl Fn() + Clone + Send + Sync + 'static) {
    check_random(model.clone(), RANDOM_ITERATIONS);
    check_pct(model, PCT_ITERATIONS, PCT_DEPTH);
}

/// What a background attempt that holds the turn waits for, as the licensing client waits.
#[derive(Clone, Copy)]
enum HeldWait {
    /// A release gives the turn back to an operator that asks for it
    /// (`LicensingClient::perform`).
    Release,
    /// A refresh between two retries retries at once when an operator or a notification asks
    /// (`LicensingClient::perform_request`).
    RefreshRetry,
}

impl HeldWait {
    async fn requested(self, requests: &LicensingRequests) {
        match self {
            Self::Release => requests.foreground_requested().await,
            Self::RefreshRetry => requests.refresh_requested().await,
        }
    }
}

/// The operators of a model: one or two.
struct Operators {
    /// Operators that asked for the turn, holding it or not.
    asked: AtomicUsize,
    /// Operators that asked for the turn and do not hold it yet.
    waiting: AtomicUsize,
    /// The first operator held the turn and gave it back.
    first_served: CancellationToken,
    /// The second operator held the turn and gave it back, or the model has no second operator.
    second_served: CancellationToken,
}

impl Operators {
    fn two() -> Self {
        Self {
            asked: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
            first_served: CancellationToken::new(),
            second_served: CancellationToken::new(),
        }
    }

    fn one() -> Self {
        let operators = Self::two();
        operators.second_served.cancel();
        operators
    }

    /// Completes once every operator of the model has had its turn.
    async fn every_one_served(&self) {
        self.first_served.cancelled().await;
        self.second_served.cancelled().await;
    }

    fn waiting(&self) -> usize {
        self.waiting.load(Ordering::SeqCst)
    }

    fn served(&self, which: Operator) {
        match which {
            Operator::First => self.first_served.cancel(),
            Operator::Second => self.second_served.cancel(),
        }
    }
}

#[derive(Clone, Copy)]
enum Operator {
    First,
    Second,
}

/// What an operator's attempt does once it holds the turn.
#[derive(Clone, Copy)]
enum OperatorAttempt {
    /// The attempt ends at once.
    EndsAtOnce,
    /// The attempt's request failed and it waits to retry. Only a request another operator made
    /// may hurry that retry, never the operator's own.
    WaitsToRetry,
}

/// An operator's statement: it asks for the turn in the foreground and gives it back once its
/// attempt ends.
async fn operator(
    requests: Arc<LicensingRequests>,
    operators: Arc<Operators>,
    which: Operator,
    attempt: OperatorAttempt,
) {
    operators.asked.fetch_add(1, Ordering::SeqCst);
    operators.waiting.fetch_add(1, Ordering::SeqCst);
    let turn = requests.foreground().await;
    operators.waiting.fetch_sub(1, Ordering::SeqCst);
    if let OperatorAttempt::WaitsToRetry = attempt {
        let hurried = requests.refresh_requested().now_or_never().is_some();
        if hurried {
            // Another operator counts itself here before the turn can see its request, so a retry
            // that another operator hurried finds more than one operator counted.
            assert!(
                operators.asked.load(Ordering::SeqCst) > 1,
                "an operator's own request hurried the retry of the attempt it made"
            );
        }
    }
    drop(turn);
    operators.served(which);
}

/// A background attempt that takes the turn and holds it until an operator asks for it, or until
/// every operator has had its turn.
async fn background_attempt(
    requests: Arc<LicensingRequests>,
    operators: Arc<Operators>,
    held: HeldWait,
) {
    let turn = requests.background().await;
    nervix_primitives::select! {
        () = held.requested(&requests) => {
            assert!(
                operators.waiting() > 0,
                "a background attempt holding the turn was asked to give way while no operator \
                 waited for it"
            );
        }
        () = operators.every_one_served() => {}
    }
    drop(turn);
}

/// A background attempt that holds the turn in its wait races two operators. Each operator takes
/// the turn once it is given back, so every task ends.
fn operators_racing_a_background_attempt_that_holds_the_turn(held: HeldWait) {
    block_on(async move {
        let requests = Arc::new(LicensingRequests::default());
        let operators = Arc::new(Operators::two());
        let attempt = task::spawn(background_attempt(
            requests.clone(),
            operators.clone(),
            held,
        ));
        let first_operator = task::spawn(operator(
            requests.clone(),
            operators.clone(),
            Operator::First,
            OperatorAttempt::EndsAtOnce,
        ));
        let second_operator = task::spawn(operator(
            requests.clone(),
            operators.clone(),
            Operator::Second,
            OperatorAttempt::EndsAtOnce,
        ));
        attempt.await.assured(MODEL_TASKS_JOIN);
        first_operator.await.assured(MODEL_TASKS_JOIN);
        second_operator.await.assured(MODEL_TASKS_JOIN);
        assert_eq!(operators.waiting(), 0, "every operator held the turn");
    });
}

#[test]
fn shuttle_a_release_holding_the_turn_always_gives_it_back_to_a_waiting_operator() {
    explore(|| operators_racing_a_background_attempt_that_holds_the_turn(HeldWait::Release));
}

#[test]
fn shuttle_a_refresh_retry_holding_the_turn_is_always_hurried_for_a_waiting_operator() {
    explore(|| operators_racing_a_background_attempt_that_holds_the_turn(HeldWait::RefreshRetry));
}

/// An operator alone asks for the turn, then a background attempt that holds the turn and ends
/// without a retry wait races two more operators. Every operator's attempt waits to retry, and an
/// operator's own request never hurries the attempt it makes. Once all of them have ended, nobody
/// waits behind the next attempt, so nothing may shorten its first retry or make it give its turn
/// away.
fn operators_racing_an_attempt_that_ends_at_once() {
    block_on(async {
        let requests = Arc::new(LicensingRequests::default());
        operator(
            requests.clone(),
            Arc::new(Operators::one()),
            Operator::First,
            OperatorAttempt::WaitsToRetry,
        )
        .await;

        let operators = Arc::new(Operators::two());
        let attempt = task::spawn({
            let requests = requests.clone();
            async move {
                let turn = requests.background().await;
                drop(turn);
            }
        });
        let first_operator = task::spawn(operator(
            requests.clone(),
            operators.clone(),
            Operator::First,
            OperatorAttempt::WaitsToRetry,
        ));
        let second_operator = task::spawn(operator(
            requests.clone(),
            operators.clone(),
            Operator::Second,
            OperatorAttempt::WaitsToRetry,
        ));
        attempt.await.assured(MODEL_TASKS_JOIN);
        first_operator.await.assured(MODEL_TASKS_JOIN);
        second_operator.await.assured(MODEL_TASKS_JOIN);

        let next = requests.background().await;
        assert!(
            requests.refresh_requested().now_or_never().is_none(),
            "an operator request that found no attempt to hurry left a wakeup that shortens the \
             next attempt's first retry"
        );
        assert!(
            requests.foreground_requested().now_or_never().is_none(),
            "an operator request that found no attempt to hurry left a wakeup that makes the next \
             release give its turn away"
        );
        drop(next);
    });
}

#[test]
fn shuttle_an_uncontended_operator_request_leaves_no_wakeup_for_the_next_attempt() {
    explore(operators_racing_an_attempt_that_ends_at_once);
}

/// A release that holds the turn races two service notifications and an operator: only the
/// operator makes it give the turn back. Then a refresh that holds the turn waits between two
/// retries while a notification arrives: the notification hurries it.
fn notifications_racing_a_release_and_a_refresh_retry() {
    block_on(async {
        let requests = Arc::new(LicensingRequests::default());
        let operators = Arc::new(Operators::one());
        let release = task::spawn({
            let requests = requests.clone();
            let operators = operators.clone();
            async move {
                let turn = requests.background().await;
                nervix_primitives::select! {
                    () = requests.foreground_requested() => {
                        assert!(
                            operators.waiting() > 0,
                            "a release gave its turn away to a service notification"
                        );
                    }
                    () = operators.every_one_served() => {}
                }
                drop(turn);
            }
        });
        let notifier = task::spawn({
            let requests = requests.clone();
            async move {
                requests.expedite();
                requests.expedite();
            }
        });
        let only_operator = task::spawn(operator(
            requests.clone(),
            operators.clone(),
            Operator::First,
            OperatorAttempt::EndsAtOnce,
        ));
        release.await.assured(MODEL_TASKS_JOIN);
        notifier.await.assured(MODEL_TASKS_JOIN);
        only_operator.await.assured(MODEL_TASKS_JOIN);

        // Earlier notifications belong to attempts that took the turn before them. A refresh that
        // holds the turn now waits only for the one sent while it holds it.
        let refresh_holds = CancellationToken::new();
        let refresh = task::spawn({
            let requests = requests.clone();
            let refresh_holds = refresh_holds.clone();
            async move {
                let turn = requests.background().await;
                refresh_holds.cancel();
                requests.refresh_requested().await;
                drop(turn);
            }
        });
        let notifier = task::spawn({
            let requests = requests.clone();
            async move {
                refresh_holds.cancelled().await;
                requests.expedite();
            }
        });
        refresh.await.assured(MODEL_TASKS_JOIN);
        notifier.await.assured(MODEL_TASKS_JOIN);
    });
}

#[test]
fn shuttle_a_notification_hurries_a_refresh_retry_but_never_takes_an_operators_place() {
    explore(notifications_racing_a_release_and_a_refresh_retry);
}

/// An operator's refresh holds the turn and waits between two retries while a background attempt
/// queues behind it and a service notification arrives. The notification hurries the operator's
/// retry or waits for the next attempt to take the turn, and that attempt already represents it.
fn a_notification_preceding_a_queued_attempts_turn() {
    block_on(async {
        let requests = Arc::new(LicensingRequests::default());
        let operator_holds = CancellationToken::new();
        let notified = CancellationToken::new();
        let refreshing_operator = task::spawn({
            let requests = requests.clone();
            let operator_holds = operator_holds.clone();
            let notified = notified.clone();
            async move {
                let turn = requests.foreground().await;
                operator_holds.cancel();
                nervix_primitives::select! {
                    () = requests.refresh_requested() => {}
                    () = notified.cancelled() => {}
                }
                drop(turn);
            }
        });
        let queued_attempt = task::spawn({
            let requests = requests.clone();
            let operator_holds = operator_holds.clone();
            async move {
                operator_holds.cancelled().await;
                let turn = requests.background().await;
                assert!(
                    requests.refresh_requested().now_or_never().is_none(),
                    "a queued attempt kept the notification that preceded its turn, so it would \
                     shorten its first retry"
                );
                drop(turn);
            }
        });
        let notifier = task::spawn({
            let requests = requests.clone();
            async move {
                operator_holds.cancelled().await;
                requests.expedite();
                notified.cancel();
            }
        });
        refreshing_operator.await.assured(MODEL_TASKS_JOIN);
        queued_attempt.await.assured(MODEL_TASKS_JOIN);
        notifier.await.assured(MODEL_TASKS_JOIN);
    });
}

#[test]
fn shuttle_a_queued_background_attempt_consumes_the_notification_that_preceded_its_turn() {
    explore(a_notification_preceding_a_queued_attempts_turn);
}
