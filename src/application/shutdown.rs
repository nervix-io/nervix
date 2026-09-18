//! The process-wide application shutdown lifecycle.
//!
//! Layer: control plane.
//!
//! - **Owns.** The ordered transition from a stop request through admission shutdown, drain
//!   support, terminal teardown, and the outcome reported to the process boundary, together with
//!   the one deadline that bounds all of them.
//! - **Depends on.** Tokio signaling primitives and monotonic time.
//! - **Must not know.** Which listeners, control-plane services, or runtime tasks observe each
//!   lifecycle signal.

use meticulous::{OptionExt as _, ResultExt as _};
use tokio::{
    sync::watch,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::info;
use triomphe::Arc;

pub(super) const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a shutdown may take from its first stop request until the process exits. It covers
/// the default drain timeout and leaves the rest to the services that stop after the drain.
pub(super) const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(50);
const _: () = assert!(
    DEFAULT_DRAIN_TIMEOUT.as_nanos() < DEFAULT_SHUTDOWN_TIMEOUT.as_nanos(),
    "the default shutdown timeout must leave terminal teardown time after the default drain"
);

/// The one monotonic deadline that bounds every phase of a shutdown, fixed when its first stop
/// request is accepted.
///
/// It keeps the instant of that request and the timeout measured from it rather than a single
/// instant, so a configured timeout of any length cannot overflow the monotonic clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownDeadline {
    requested_at: Instant,
    timeout: Duration,
}

impl ShutdownDeadline {
    /// The time left before the deadline, which is zero once it has passed.
    pub fn remaining(self) -> Duration {
        self.timeout
            .checked_sub(self.requested_at.elapsed())
            .unwrap_or(Duration::ZERO)
    }

    pub fn has_passed(self) -> bool {
        self.requested_at.elapsed() >= self.timeout
    }

    /// Runs `work` until it finishes or the deadline passes, whichever comes first.
    pub(in crate::application) async fn bound<F>(self, work: F) -> BeforeDeadline<F::Output>
    where
        F: Future,
    {
        let bounded = tokio::time::timeout(self.remaining(), work).await;
        match bounded {
            Ok(output) => BeforeDeadline::Finished(output),
            Err(_) => BeforeDeadline::Expired,
        }
    }
}

/// Whether shutdown work finished before the deadline passed.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::application) enum BeforeDeadline<T> {
    Finished(T),
    Expired,
}

/// The immutable request that starts one application shutdown lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownRequest {
    deadline: ShutdownDeadline,
}

impl ShutdownRequest {
    pub fn deadline(self) -> ShutdownDeadline {
        self.deadline
    }
}

/// Whether this caller started shutdown or observed an existing request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownRequestOutcome {
    Accepted(ShutdownRequest),
    AlreadyRequested(ShutdownRequest),
}

/// The application phase currently owned by the coordinator.
///
/// Phases are declared in the order a shutdown moves through them, so a later phase compares
/// greater than every phase before it.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ShutdownPhase {
    Serving,
    StopRequested,
    DrainSupport,
    TerminalTeardown,
    Finished,
}

/// How a shutdown phase ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownPhaseOutcome {
    /// The phase finished its contract before the shutdown deadline.
    Completed,
    /// The phase finished before the shutdown deadline but had to leave work behind.
    Abandoned,
    /// The shutdown deadline passed before the phase finished, and the work it still owned was
    /// cancelled.
    Forced,
}

impl ShutdownPhaseOutcome {
    pub(in crate::application) fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Forced, _) | (_, Self::Forced) => Self::Forced,
            (Self::Abandoned, _) | (_, Self::Abandoned) => Self::Abandoned,
            (Self::Completed, Self::Completed) => Self::Completed,
        }
    }

    /// This outcome for a phase that finishes now, or `Forced` when the shutdown deadline has
    /// already passed.
    pub(in crate::application) fn unless_deadline_passed(self, deadline: ShutdownDeadline) -> Self {
        if deadline.has_passed() {
            return Self::Forced;
        }
        self
    }
}

/// The terminal report shared with signal handlers and other process boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownOutcome {
    pub stop_admission: ShutdownPhaseOutcome,
    pub drain_support: ShutdownPhaseOutcome,
    pub terminal_teardown: ShutdownPhaseOutcome,
}

impl ShutdownOutcome {
    /// Whether the shutdown deadline cut any phase short.
    pub fn deadline_expired(self) -> bool {
        self.stop_admission == ShutdownPhaseOutcome::Forced
            || self.drain_support == ShutdownPhaseOutcome::Forced
            || self.terminal_teardown == ShutdownPhaseOutcome::Forced
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoordinatorState {
    Serving,
    StopRequested {
        request: ShutdownRequest,
    },
    DrainSupport {
        request: ShutdownRequest,
        stop_admission: ShutdownPhaseOutcome,
    },
    TerminalTeardown {
        request: ShutdownRequest,
        stop_admission: ShutdownPhaseOutcome,
        drain_support: ShutdownPhaseOutcome,
    },
    Finished {
        request: ShutdownRequest,
        outcome: ShutdownOutcome,
    },
}

impl CoordinatorState {
    fn phase(self) -> ShutdownPhase {
        match self {
            Self::Serving => ShutdownPhase::Serving,
            Self::StopRequested { .. } => ShutdownPhase::StopRequested,
            Self::DrainSupport { .. } => ShutdownPhase::DrainSupport,
            Self::TerminalTeardown { .. } => ShutdownPhase::TerminalTeardown,
            Self::Finished { .. } => ShutdownPhase::Finished,
        }
    }

    fn request(self) -> Option<ShutdownRequest> {
        match self {
            Self::Serving => None,
            Self::StopRequested { request }
            | Self::DrainSupport { request, .. }
            | Self::TerminalTeardown { request, .. }
            | Self::Finished { request, .. } => Some(request),
        }
    }

    fn outcome(self) -> Option<ShutdownOutcome> {
        match self {
            Self::Finished { outcome, .. } => Some(outcome),
            Self::Serving
            | Self::StopRequested { .. }
            | Self::DrainSupport { .. }
            | Self::TerminalTeardown { .. } => None,
        }
    }
}

struct ShutdownCoordinatorInner {
    /// How long shutdown may take from its first stop request.
    timeout: Duration,
    state: watch::Sender<CoordinatorState>,
    stop_requested: CancellationToken,
    admission_shutdown: CancellationToken,
    drain_support_shutdown: CancellationToken,
}

/// A cloneable handle over the single owner of the application shutdown lifecycle.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    inner: Arc<ShutdownCoordinatorInner>,
}

impl std::fmt::Debug for ShutdownCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShutdownCoordinator")
            .field("timeout", &self.inner.timeout)
            .field("phase", &self.phase())
            .field("deadline", &self.request().map(ShutdownRequest::deadline))
            .finish_non_exhaustive()
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new(DEFAULT_SHUTDOWN_TIMEOUT)
    }
}

impl ShutdownCoordinator {
    /// A coordinator whose shutdown, once requested, must finish within `timeout`.
    pub fn new(timeout: Duration) -> Self {
        let (state, _) = watch::channel(CoordinatorState::Serving);
        Self {
            inner: Arc::new(ShutdownCoordinatorInner {
                timeout,
                state,
                stop_requested: CancellationToken::new(),
                admission_shutdown: CancellationToken::new(),
                drain_support_shutdown: CancellationToken::new(),
            }),
        }
    }

    /// Starts shutdown exactly once and fixes its deadline at the configured timeout after this
    /// first request. Every later request observes that same deadline, so none can restart or
    /// extend it.
    pub fn request_stop(&self) -> ShutdownRequestOutcome {
        let mut outcome = None;
        self.inner.state.send_if_modified(|state| {
            let existing = state.request();
            match existing {
                Some(existing) => {
                    outcome = Some(ShutdownRequestOutcome::AlreadyRequested(existing));
                    false
                }
                None => {
                    let requested = ShutdownRequest {
                        deadline: ShutdownDeadline {
                            requested_at: Instant::now(),
                            timeout: self.inner.timeout,
                        },
                    };
                    *state = CoordinatorState::StopRequested { request: requested };
                    outcome = Some(ShutdownRequestOutcome::Accepted(requested));
                    true
                }
            }
        });
        let outcome = outcome.assured(
            "the state update closure always records either the accepted or existing request",
        );
        if let ShutdownRequestOutcome::Accepted(_) = outcome {
            self.inner.stop_requested.cancel();
        }
        outcome
    }

    pub fn phase(&self) -> ShutdownPhase {
        self.inner.state.borrow().phase()
    }

    pub fn request(&self) -> Option<ShutdownRequest> {
        self.inner.state.borrow().request()
    }

    pub fn outcome(&self) -> Option<ShutdownOutcome> {
        self.inner.state.borrow().outcome()
    }

    /// Waits for the first stop request and returns its immutable deadline.
    pub async fn requested(&self) -> ShutdownRequest {
        self.inner.stop_requested.cancelled().await;
        self.request()
            .assured("the coordinator stores the request before publishing its stop signal")
    }

    /// Waits until all shutdown phases have reported their outcome.
    pub async fn completion(&self) -> ShutdownOutcome {
        let mut state = self.inner.state.subscribe();
        loop {
            tokio::task::consume_budget().await;
            let outcome = state.borrow().outcome();
            if let Some(outcome) = outcome {
                return outcome;
            }
            state.changed().await.assured(
                "this coordinator handle retains the lifecycle state sender while it waits",
            );
        }
    }

    pub(in crate::application) fn admission_token(&self) -> CancellationToken {
        self.inner.admission_shutdown.clone()
    }

    pub(in crate::application) fn drain_support_token(&self) -> CancellationToken {
        self.inner.drain_support_shutdown.clone()
    }

    pub(in crate::application) fn stop_admission(&self) {
        self.request()
            .assured("the composition root stops admission only after a stop request");
        self.inner.admission_shutdown.cancel();
    }

    pub(in crate::application) fn begin_drain_support(&self, stop_admission: ShutdownPhaseOutcome) {
        let transitioned = self.inner.state.send_if_modified(|state| {
            let CoordinatorState::StopRequested { request } = *state else {
                return false;
            };
            *state = CoordinatorState::DrainSupport {
                request,
                stop_admission,
            };
            true
        });
        if !transitioned {
            None::<()>.assured(
                "the composition root begins drain support once after it completes admission \
                 shutdown",
            );
        }
        info!(
            outcome = ?stop_admission,
            "shutdown admission phase finished"
        );
    }

    pub(in crate::application) fn begin_terminal_teardown(
        &self,
        drain_support: ShutdownPhaseOutcome,
    ) {
        let transitioned = self.inner.state.send_if_modified(|state| {
            let CoordinatorState::DrainSupport {
                request,
                stop_admission,
            } = *state
            else {
                return false;
            };
            *state = CoordinatorState::TerminalTeardown {
                request,
                stop_admission,
                drain_support,
            };
            true
        });
        if !transitioned {
            None::<()>.assured(
                "the composition root begins terminal teardown once after drain support finishes",
            );
        }
        self.inner.drain_support_shutdown.cancel();
        info!(
            outcome = ?drain_support,
            "shutdown drain-support phase finished"
        );
    }

    pub(in crate::application) fn finish(&self, terminal_teardown: ShutdownPhaseOutcome) {
        let transitioned = self.inner.state.send_if_modified(|state| {
            let CoordinatorState::TerminalTeardown {
                request,
                stop_admission,
                drain_support,
            } = *state
            else {
                return false;
            };
            *state = CoordinatorState::Finished {
                request,
                outcome: ShutdownOutcome {
                    stop_admission,
                    drain_support,
                    terminal_teardown,
                },
            };
            true
        });
        if !transitioned {
            None::<()>.assured(
                "the composition root reports terminal teardown once after that phase finishes",
            );
        }
        info!(
            outcome = ?terminal_teardown,
            "shutdown terminal-teardown phase finished"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requested_deadline(coordinator: &ShutdownCoordinator) -> ShutdownDeadline {
        coordinator
            .request()
            .expect("the scenario requested shutdown first")
            .deadline()
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_stop_requests_keep_the_first_deadline() {
        let coordinator = ShutdownCoordinator::new(Duration::from_secs(30));
        let ShutdownRequestOutcome::Accepted(first) = coordinator.request_stop() else {
            panic!("the first stop request must start shutdown");
        };

        tokio::time::advance(Duration::from_secs(10)).await;
        let repeated = coordinator.request_stop();

        assert_eq!(repeated, ShutdownRequestOutcome::AlreadyRequested(first));
        assert_eq!(first.deadline().remaining(), Duration::from_secs(20));
        assert!(!first.deadline().has_passed());

        tokio::time::advance(Duration::from_secs(20)).await;
        let after_the_deadline = coordinator.request_stop();

        assert_eq!(
            after_the_deadline,
            ShutdownRequestOutcome::AlreadyRequested(first)
        );
        assert_eq!(first.deadline().remaining(), Duration::ZERO);
        assert!(first.deadline().has_passed());
    }

    #[tokio::test(start_paused = true)]
    async fn work_still_running_at_the_deadline_is_cut_off_there() {
        let coordinator = ShutdownCoordinator::new(Duration::from_secs(5));
        coordinator.request_stop();
        let deadline = requested_deadline(&coordinator);
        let started = Instant::now();

        let stalled = deadline.bound(std::future::pending::<()>()).await;

        assert_eq!(stalled, BeforeDeadline::Expired);
        assert_eq!(started.elapsed(), Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn work_that_finishes_before_the_deadline_reports_its_output() {
        let coordinator = ShutdownCoordinator::new(Duration::from_secs(5));
        coordinator.request_stop();
        let deadline = requested_deadline(&coordinator);

        let finished = deadline
            .bound(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                7
            })
            .await;

        assert_eq!(finished, BeforeDeadline::Finished(7));
        assert_eq!(deadline.remaining(), Duration::from_secs(4));
    }

    #[test]
    fn a_forced_phase_outweighs_every_other_outcome() {
        assert_eq!(
            ShutdownPhaseOutcome::Completed.combine(ShutdownPhaseOutcome::Completed),
            ShutdownPhaseOutcome::Completed
        );
        assert_eq!(
            ShutdownPhaseOutcome::Completed.combine(ShutdownPhaseOutcome::Abandoned),
            ShutdownPhaseOutcome::Abandoned
        );
        assert_eq!(
            ShutdownPhaseOutcome::Abandoned.combine(ShutdownPhaseOutcome::Forced),
            ShutdownPhaseOutcome::Forced
        );
        assert_eq!(
            ShutdownPhaseOutcome::Forced.combine(ShutdownPhaseOutcome::Completed),
            ShutdownPhaseOutcome::Forced
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_phase_that_finishes_after_the_deadline_is_forced() {
        let coordinator = ShutdownCoordinator::new(Duration::from_secs(5));
        coordinator.request_stop();
        let deadline = requested_deadline(&coordinator);

        assert_eq!(
            ShutdownPhaseOutcome::Completed.unless_deadline_passed(deadline),
            ShutdownPhaseOutcome::Completed
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(
            ShutdownPhaseOutcome::Abandoned.unless_deadline_passed(deadline),
            ShutdownPhaseOutcome::Forced
        );
        let outcome = ShutdownOutcome {
            stop_admission: ShutdownPhaseOutcome::Completed,
            drain_support: ShutdownPhaseOutcome::Forced,
            terminal_teardown: ShutdownPhaseOutcome::Completed,
        };
        assert!(outcome.deadline_expired());
    }

    #[tokio::test]
    async fn drain_support_stays_live_until_terminal_teardown() {
        let coordinator = ShutdownCoordinator::default();
        let admission = coordinator.admission_token();
        let drain_support = coordinator.drain_support_token();

        coordinator.request_stop();
        coordinator.stop_admission();
        assert!(admission.is_cancelled());
        assert!(!drain_support.is_cancelled());

        coordinator.begin_drain_support(ShutdownPhaseOutcome::Completed);
        assert_eq!(coordinator.phase(), ShutdownPhase::DrainSupport);
        assert!(!drain_support.is_cancelled());

        coordinator.begin_terminal_teardown(ShutdownPhaseOutcome::Completed);
        assert!(drain_support.is_cancelled());
        coordinator.finish(ShutdownPhaseOutcome::Completed);

        let outcome = coordinator.completion().await;
        assert_eq!(
            outcome,
            ShutdownOutcome {
                stop_admission: ShutdownPhaseOutcome::Completed,
                drain_support: ShutdownPhaseOutcome::Completed,
                terminal_teardown: ShutdownPhaseOutcome::Completed,
            }
        );
        assert!(!outcome.deadline_expired());
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use shuttle::{future::block_on, thread};

    use super::*;
    use crate::{
        application::test_fixtures::{FAR_FUTURE_SHUTDOWN_TIMEOUT, shut_down_in_phase_order},
        shuttle_test::{check_pct, check_random},
    };

    const MODEL_THREAD_JOINS: &str =
        "Shuttle fails the whole execution when a model thread panics, so no join observes one";
    const PCT_DEPTH: usize = 3;
    const PCT_ITERATIONS: usize = 1_000;
    const RANDOM_ITERATIONS: usize = 1_000;

    /// Samples the phase of `shutdown` until it has finished, and fails when a sample precedes the
    /// one taken before it.
    fn observe_phases_until_finished(shutdown: &ShutdownCoordinator) {
        let mut previous = shutdown.phase();
        while previous != ShutdownPhase::Finished {
            // The observer waits on nothing, so it yields between samples; otherwise a PCT schedule
            // that ranks it first would spin until the step bound instead of moving shutdown on.
            thread::yield_now();
            let phase = shutdown.phase();
            assert!(
                phase >= previous,
                "shutdown moved back from {previous:?} to {phase:?}"
            );
            previous = phase;
        }
    }

    /// Two stop requests race each other and a task waiting for the first request.
    fn racing_stop_requests() {
        let shutdown = ShutdownCoordinator::new(FAR_FUTURE_SHUTDOWN_TIMEOUT);
        let waiting = shutdown.clone();
        let waiter = thread::spawn(move || block_on(waiting.requested()));
        let first_requester = shutdown.clone();
        let first = thread::spawn(move || first_requester.request_stop());
        let second_requester = shutdown.clone();
        let second = thread::spawn(move || second_requester.request_stop());

        let first = first.join().assured(MODEL_THREAD_JOINS);
        let second = second.join().assured(MODEL_THREAD_JOINS);
        let accepted = match (first, second) {
            (
                ShutdownRequestOutcome::Accepted(accepted),
                ShutdownRequestOutcome::AlreadyRequested(observed),
            )
            | (
                ShutdownRequestOutcome::AlreadyRequested(observed),
                ShutdownRequestOutcome::Accepted(accepted),
            ) => {
                assert_eq!(
                    observed, accepted,
                    "the stop request that lost the race must observe the accepted request and \
                     its deadline"
                );
                accepted
            }
            outcomes => {
                panic!("exactly one of two racing stop requests must be accepted, got {outcomes:?}")
            }
        };
        let awaited = waiter.join().assured(MODEL_THREAD_JOINS);
        assert_eq!(
            awaited, accepted,
            "a task waiting for the stop request must observe the accepted request and its \
             deadline"
        );
        assert_eq!(shutdown.request(), Some(accepted));
        assert_eq!(
            shutdown.request_stop(),
            ShutdownRequestOutcome::AlreadyRequested(accepted),
            "a stop request after the race must observe the accepted deadline rather than replace \
             it"
        );
    }

    /// The composition root moves shutdown through its phases while a phase observer samples them
    /// and completion waiters subscribe before and during the transitions.
    fn phases_racing_their_observers() {
        let shutdown = ShutdownCoordinator::new(FAR_FUTURE_SHUTDOWN_TIMEOUT);
        let early_waiting = shutdown.clone();
        let early_completion = thread::spawn(move || block_on(early_waiting.completion()));
        let observing = shutdown.clone();
        let phase_observer = thread::spawn(move || observe_phases_until_finished(&observing));
        let composing = shutdown.clone();
        let composition_root = thread::spawn(move || block_on(shut_down_in_phase_order(composing)));
        let late_waiting = shutdown.clone();
        let late_completion = thread::spawn(move || block_on(late_waiting.completion()));
        shutdown.request_stop();

        composition_root.join().assured(MODEL_THREAD_JOINS);
        phase_observer.join().assured(MODEL_THREAD_JOINS);
        let outcome = shutdown
            .outcome()
            .verified("the composition root finished shutdown before it was joined");
        assert_eq!(
            outcome,
            ShutdownOutcome {
                stop_admission: ShutdownPhaseOutcome::Completed,
                drain_support: ShutdownPhaseOutcome::Abandoned,
                terminal_teardown: ShutdownPhaseOutcome::Completed,
            }
        );
        for completion in [early_completion, late_completion] {
            let observed = completion.join().assured(MODEL_THREAD_JOINS);
            assert_eq!(
                observed, outcome,
                "every completion waiter must observe the one outcome shutdown finished with"
            );
        }
        assert_eq!(
            block_on(shutdown.completion()),
            outcome,
            "a completion waiter that subscribes after shutdown finished must observe that outcome"
        );
    }

    #[test]
    fn shuttle_racing_stop_requests_accept_exactly_one_and_keep_its_deadline() {
        check_random(racing_stop_requests, RANDOM_ITERATIONS);
        check_pct(racing_stop_requests, PCT_ITERATIONS, PCT_DEPTH);
    }

    #[test]
    fn shuttle_phases_only_advance_and_every_completion_waiter_observes_the_one_outcome() {
        check_random(phases_racing_their_observers, RANDOM_ITERATIONS);
        check_pct(phases_racing_their_observers, PCT_ITERATIONS, PCT_DEPTH);
    }
}
