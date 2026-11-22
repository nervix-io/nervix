//! The process-wide application shutdown lifecycle.
//!
//! Layer: control plane.
//!
//! - **Owns.** The ordered transition from a stop request through admission shutdown and the cut
//!   of the client connections it leaves open, the save of what the node holds, the moves of its
//!   saved work to other nodes under the shutdown timeout, terminal teardown, and the outcome
//!   reported to the process boundary.
//! - **Depends on.** Tokio signaling primitives and monotonic time.
//! - **Must not know.** Which listeners, control-plane services, or runtime tasks observe each
//!   lifecycle signal.
//!
//! Only the moves have a deadline. The save waits only for work already in motion, which ends on
//! its own, and every task terminal teardown stops is given its own grace. A save or a teardown
//! that hangs is ended by the platform's supervisor, not by the node.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    sync::{Arc, CancellationToken, watch},
    time::Instant,
};
use tracing::info;

mod settings;
pub use settings::{ShutdownSettings, ShutdownTimeout, TaskGracePeriod};

/// How long a stopping node's work may take to move to other nodes once the node has saved it.
pub(super) const DEFAULT_SHUTDOWN_TIMEOUT: Duration = nervix_models::ShutdownTimeout::DEFAULT;

/// The deadline of a stopping node's moves: the shutdown timeout, counted from the moment the node
/// finished saving what it holds.
///
/// It keeps the instant the moves began and the timeout measured from it rather than a single
/// instant, so a configured timeout of any length cannot overflow the monotonic clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownDeadline {
    started_at: Instant,
    timeout: Duration,
}

impl ShutdownDeadline {
    /// The deadline of moves that begin now and may take `timeout`.
    pub(in crate::application) fn starting_now(timeout: Duration) -> Self {
        Self {
            started_at: Instant::now(),
            timeout,
        }
    }

    /// The time left before the deadline, which is zero once it has passed.
    pub fn remaining(self) -> Duration {
        self.timeout
            .checked_sub(self.started_at.elapsed())
            .unwrap_or(Duration::ZERO)
    }

    pub fn has_passed(self) -> bool {
        self.started_at.elapsed() >= self.timeout
    }

    /// The shutdown timeout this deadline was counted with.
    pub fn timeout(self) -> Duration {
        self.timeout
    }
}

/// The immutable request that starts one application shutdown lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownRequest {
    requested_at: Instant,
}

impl ShutdownRequest {
    /// When the first stop request was accepted.
    pub fn requested_at(self) -> Instant {
        self.requested_at
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
    /// The node takes no new work, lets the work in motion come to rest and saves what it holds.
    Saving,
    /// The node's saved work moves to other nodes, within the shutdown timeout.
    Moving,
    TerminalTeardown,
    Finished,
}

/// How a shutdown phase without a deadline ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::EnumString)]
pub enum ShutdownPhaseOutcome {
    /// The phase finished its contract.
    Completed,
    /// The phase finished but had to leave work behind, such as a task that outlived its grace or
    /// a leader that did not answer.
    Abandoned,
}

impl ShutdownPhaseOutcome {
    pub(in crate::application) fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Abandoned, _) | (_, Self::Abandoned) => Self::Abandoned,
            (Self::Completed, Self::Completed) => Self::Completed,
        }
    }
}

/// How the moves of a stopping node's saved work ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::EnumString)]
pub enum ShutdownMoveOutcome {
    /// Every unit of the node moved to another node.
    Moved,
    /// No other node can take the node's work, so every unit stays saved on this node, and a
    /// restart restores it. The stop is a success.
    NowhereToMove,
    /// The shutdown timeout passed, or a move failed, while another node could have taken the
    /// work: the units that did not move stay saved on this node, and the cluster is disrupted
    /// until it restarts.
    Disrupted,
}

/// The terminal report shared with signal handlers and other process boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownOutcome {
    pub stop_admission: ShutdownPhaseOutcome,
    pub save: ShutdownPhaseOutcome,
    pub moves: ShutdownMoveOutcome,
    pub terminal_teardown: ShutdownPhaseOutcome,
}

impl ShutdownOutcome {
    /// Whether the node left work it could have moved, which a process reports as a failed
    /// shutdown.
    pub fn disrupted(self) -> bool {
        self.moves == ShutdownMoveOutcome::Disrupted
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoordinatorState {
    Serving,
    StopRequested {
        request: ShutdownRequest,
    },
    Saving {
        request: ShutdownRequest,
        stop_admission: ShutdownPhaseOutcome,
    },
    Moving {
        request: ShutdownRequest,
        stop_admission: ShutdownPhaseOutcome,
        save: ShutdownPhaseOutcome,
        deadline: ShutdownDeadline,
    },
    TerminalTeardown {
        request: ShutdownRequest,
        stop_admission: ShutdownPhaseOutcome,
        save: ShutdownPhaseOutcome,
        moves: ShutdownMoveOutcome,
        deadline: ShutdownDeadline,
    },
    Finished {
        request: ShutdownRequest,
        outcome: ShutdownOutcome,
        deadline: ShutdownDeadline,
    },
}

impl CoordinatorState {
    fn phase(self) -> ShutdownPhase {
        match self {
            Self::Serving => ShutdownPhase::Serving,
            Self::StopRequested { .. } => ShutdownPhase::StopRequested,
            Self::Saving { .. } => ShutdownPhase::Saving,
            Self::Moving { .. } => ShutdownPhase::Moving,
            Self::TerminalTeardown { .. } => ShutdownPhase::TerminalTeardown,
            Self::Finished { .. } => ShutdownPhase::Finished,
        }
    }

    fn request(self) -> Option<ShutdownRequest> {
        match self {
            Self::Serving => None,
            Self::StopRequested { request }
            | Self::Saving { request, .. }
            | Self::Moving { request, .. }
            | Self::TerminalTeardown { request, .. }
            | Self::Finished { request, .. } => Some(request),
        }
    }

    fn outcome(self) -> Option<ShutdownOutcome> {
        match self {
            Self::Finished { outcome, .. } => Some(outcome),
            Self::Serving
            | Self::StopRequested { .. }
            | Self::Saving { .. }
            | Self::Moving { .. }
            | Self::TerminalTeardown { .. } => None,
        }
    }

    fn moves_deadline(self) -> Option<ShutdownDeadline> {
        match self {
            Self::Serving | Self::StopRequested { .. } | Self::Saving { .. } => None,
            Self::Moving { deadline, .. }
            | Self::TerminalTeardown { deadline, .. }
            | Self::Finished { deadline, .. } => Some(deadline),
        }
    }
}

struct ShutdownCoordinatorInner {
    task_grace_period: Duration,
    /// How long the moves may take once the node has saved what it holds.
    timeout: Duration,
    state: watch::Sender<CoordinatorState>,
    stop_requested: CancellationToken,
    admission_shutdown: CancellationToken,
    /// Cancelled once the client connections the public listeners still hold after admission
    /// closed are cut.
    connections_cut: CancellationToken,
    /// Cancelled when terminal teardown begins. The services the save and the moves rely on stay
    /// alive until then.
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
            .field("request", &self.request())
            .finish_non_exhaustive()
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new(
            DEFAULT_SHUTDOWN_TIMEOUT,
            ShutdownSettings::default().task_grace_period(),
        )
    }
}

impl ShutdownCoordinator {
    /// A coordinator whose moves, once a stop is requested and the node has saved what it holds,
    /// must finish within `timeout`, and whose tasks each have `task_grace_period` to stop.
    pub fn new(timeout: Duration, task_grace_period: Duration) -> Self {
        let (state, _) = watch::channel(CoordinatorState::Serving);
        Self {
            inner: Arc::new(ShutdownCoordinatorInner {
                task_grace_period,
                timeout,
                state,
                stop_requested: CancellationToken::new(),
                admission_shutdown: CancellationToken::new(),
                connections_cut: CancellationToken::new(),
                drain_support_shutdown: CancellationToken::new(),
            }),
        }
    }

    /// Starts shutdown exactly once. Every later request observes that same request, so none can
    /// restart it.
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
                        requested_at: Instant::now(),
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

    /// The deadline of the node's moves, once they have begun.
    pub fn moves_deadline(&self) -> Option<ShutdownDeadline> {
        self.inner.state.borrow().moves_deadline()
    }

    /// The time each listener or background task has to stop cooperatively.
    pub(in crate::application) fn task_grace_period(&self) -> Duration {
        self.inner.task_grace_period
    }

    /// Waits for the first stop request.
    pub async fn requested(&self) -> ShutdownRequest {
        self.inner.stop_requested.cancelled().await;
        self.request()
            .assured("the coordinator stores the request before publishing its stop signal")
    }

    /// Waits until all shutdown phases have reported their outcome.
    pub async fn completion(&self) -> ShutdownOutcome {
        let mut state = self.inner.state.subscribe();
        loop {
            nervix_primitives::task::consume_budget().await;
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

    /// The signal that cuts the client connections a public listener accepted.
    pub(in crate::application) fn connections_cut_token(&self) -> CancellationToken {
        self.inner.connections_cut.clone()
    }

    /// The signal that ends the services the save and the moves rely on, cancelled when terminal
    /// teardown begins.
    pub(in crate::application) fn drain_support_token(&self) -> CancellationToken {
        self.inner.drain_support_shutdown.clone()
    }

    pub(in crate::application) fn stop_admission(&self) {
        self.request()
            .assured("the composition root stops admission only after a stop request");
        self.inner.admission_shutdown.cancel();
    }

    /// Cuts every client connection the public listeners still hold. Admission closed before, so
    /// the sessions those connections carry have already been told why they end.
    pub(in crate::application) fn cut_connections(&self) {
        if !self.inner.admission_shutdown.is_cancelled() {
            None::<()>.assured(
                "the composition root cuts the public listeners' connections only after it closed \
                 admission",
            );
        }
        self.inner.connections_cut.cancel();
    }

    pub(in crate::application) fn begin_save(&self, stop_admission: ShutdownPhaseOutcome) {
        let transitioned = self.inner.state.send_if_modified(|state| {
            let CoordinatorState::StopRequested { request } = *state else {
                return false;
            };
            *state = CoordinatorState::Saving {
                request,
                stop_admission,
            };
            true
        });
        if !transitioned {
            None::<()>.assured(
                "the composition root begins the save once after it completes admission shutdown",
            );
        }
        info!(
            outcome = ?stop_admission,
            "shutdown admission phase finished"
        );
    }

    /// Begins the moves once the node has saved what it holds, and returns their deadline: the
    /// shutdown timeout, counted from now.
    pub(in crate::application) fn begin_moves(
        &self,
        save: ShutdownPhaseOutcome,
    ) -> ShutdownDeadline {
        let deadline = ShutdownDeadline::starting_now(self.inner.timeout);
        let transitioned = self.inner.state.send_if_modified(|state| {
            let CoordinatorState::Saving {
                request,
                stop_admission,
            } = *state
            else {
                return false;
            };
            *state = CoordinatorState::Moving {
                request,
                stop_admission,
                save,
                deadline,
            };
            true
        });
        if !transitioned {
            None::<()>.assured("the composition root begins the moves once after the save");
        }
        info!(outcome = ?save, "shutdown save phase finished");
        deadline
    }

    pub(in crate::application) fn begin_terminal_teardown(&self, moves: ShutdownMoveOutcome) {
        let transitioned = self.inner.state.send_if_modified(|state| {
            let CoordinatorState::Moving {
                request,
                stop_admission,
                save,
                deadline,
            } = *state
            else {
                return false;
            };
            *state = CoordinatorState::TerminalTeardown {
                request,
                stop_admission,
                save,
                moves,
                deadline,
            };
            true
        });
        if !transitioned {
            None::<()>
                .assured("the composition root begins terminal teardown once after the moves end");
        }
        self.inner.drain_support_shutdown.cancel();
        info!(outcome = ?moves, "shutdown move phase finished");
    }

    pub(in crate::application) fn finish(&self, terminal_teardown: ShutdownPhaseOutcome) {
        let transitioned = self.inner.state.send_if_modified(|state| {
            let CoordinatorState::TerminalTeardown {
                request,
                stop_admission,
                save,
                moves,
                deadline,
            } = *state
            else {
                return false;
            };
            *state = CoordinatorState::Finished {
                request,
                outcome: ShutdownOutcome {
                    stop_admission,
                    save,
                    moves,
                    terminal_teardown,
                },
                deadline,
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

    fn coordinator_with_timeout(timeout: Duration) -> ShutdownCoordinator {
        ShutdownCoordinator::new(timeout, ShutdownSettings::default().task_grace_period())
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn repeated_stop_requests_keep_the_first_request() {
        let coordinator = coordinator_with_timeout(Duration::from_secs(30));
        let ShutdownRequestOutcome::Accepted(first) = coordinator.request_stop() else {
            panic!("the first stop request must start shutdown");
        };

        nervix_primitives::time::advance(Duration::from_secs(10)).await;
        let repeated = coordinator.request_stop();

        assert_eq!(repeated, ShutdownRequestOutcome::AlreadyRequested(first));
        assert_eq!(coordinator.request(), Some(first));
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn the_shutdown_timeout_starts_when_the_save_finishes() {
        let coordinator = coordinator_with_timeout(Duration::from_secs(30));
        coordinator.request_stop();
        coordinator.stop_admission();
        coordinator.begin_save(ShutdownPhaseOutcome::Completed);
        assert_eq!(coordinator.moves_deadline(), None);

        nervix_primitives::time::advance(Duration::from_secs(100)).await;
        let deadline = coordinator.begin_moves(ShutdownPhaseOutcome::Completed);

        assert_eq!(coordinator.phase(), ShutdownPhase::Moving);
        assert_eq!(coordinator.moves_deadline(), Some(deadline));
        assert_eq!(deadline.remaining(), Duration::from_secs(30));
        assert_eq!(deadline.timeout(), Duration::from_secs(30));
        assert!(!deadline.has_passed());

        nervix_primitives::time::advance(Duration::from_secs(30)).await;
        assert_eq!(deadline.remaining(), Duration::ZERO);
        assert!(deadline.has_passed());
    }

    #[test]
    fn an_abandoned_phase_outweighs_a_completed_one() {
        assert_eq!(
            ShutdownPhaseOutcome::Completed.combine(ShutdownPhaseOutcome::Completed),
            ShutdownPhaseOutcome::Completed
        );
        assert_eq!(
            ShutdownPhaseOutcome::Completed.combine(ShutdownPhaseOutcome::Abandoned),
            ShutdownPhaseOutcome::Abandoned
        );
        assert_eq!(
            ShutdownPhaseOutcome::Abandoned.combine(ShutdownPhaseOutcome::Completed),
            ShutdownPhaseOutcome::Abandoned
        );
    }

    #[test]
    fn only_moves_left_unfinished_make_a_disrupted_shutdown() {
        let finished = |moves| ShutdownOutcome {
            stop_admission: ShutdownPhaseOutcome::Completed,
            save: ShutdownPhaseOutcome::Completed,
            moves,
            terminal_teardown: ShutdownPhaseOutcome::Abandoned,
        };
        assert!(!finished(ShutdownMoveOutcome::Moved).disrupted());
        assert!(!finished(ShutdownMoveOutcome::NowhereToMove).disrupted());
        assert!(finished(ShutdownMoveOutcome::Disrupted).disrupted());
    }

    #[nervix_primitives::test]
    async fn the_services_the_save_and_the_moves_rely_on_stay_live_until_terminal_teardown() {
        let coordinator = ShutdownCoordinator::default();
        let admission = coordinator.admission_token();
        let drain_support = coordinator.drain_support_token();

        coordinator.request_stop();
        coordinator.stop_admission();
        assert!(admission.is_cancelled());
        assert!(!drain_support.is_cancelled());

        coordinator.begin_save(ShutdownPhaseOutcome::Completed);
        assert_eq!(coordinator.phase(), ShutdownPhase::Saving);
        assert!(!drain_support.is_cancelled());

        coordinator.begin_moves(ShutdownPhaseOutcome::Completed);
        assert_eq!(coordinator.phase(), ShutdownPhase::Moving);
        assert!(!drain_support.is_cancelled());

        coordinator.begin_terminal_teardown(ShutdownMoveOutcome::NowhereToMove);
        assert!(drain_support.is_cancelled());
        coordinator.finish(ShutdownPhaseOutcome::Completed);

        let outcome = coordinator.completion().await;
        assert_eq!(
            outcome,
            ShutdownOutcome {
                stop_admission: ShutdownPhaseOutcome::Completed,
                save: ShutdownPhaseOutcome::Completed,
                moves: ShutdownMoveOutcome::NowhereToMove,
                terminal_teardown: ShutdownPhaseOutcome::Completed,
            }
        );
        assert!(!outcome.disrupted());
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use nervix_model_harness::shuttle::{check_pct, check_random};
    use nervix_primitives::thread;
    use shuttle::future::block_on;

    use super::*;
    use crate::application::test_fixtures::{
        FAR_FUTURE_SHUTDOWN_TIMEOUT, shut_down_in_phase_order,
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
        let shutdown = ShutdownCoordinator::new(
            FAR_FUTURE_SHUTDOWN_TIMEOUT,
            ShutdownSettings::default().task_grace_period(),
        );
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
                    "the stop request that lost the race must observe the accepted request"
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
            "a task waiting for the stop request must observe the accepted request"
        );
        assert_eq!(shutdown.request(), Some(accepted));
        assert_eq!(
            shutdown.request_stop(),
            ShutdownRequestOutcome::AlreadyRequested(accepted),
            "a stop request after the race must observe the accepted request rather than replace \
             it"
        );
    }

    /// The composition root moves shutdown through its phases while a phase observer samples them
    /// and completion waiters subscribe before and during the transitions.
    fn phases_racing_their_observers() {
        let shutdown = ShutdownCoordinator::new(
            FAR_FUTURE_SHUTDOWN_TIMEOUT,
            ShutdownSettings::default().task_grace_period(),
        );
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
                save: ShutdownPhaseOutcome::Completed,
                moves: ShutdownMoveOutcome::NowhereToMove,
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
    fn shuttle_racing_stop_requests_accept_exactly_one() {
        check_random(racing_stop_requests, RANDOM_ITERATIONS);
        check_pct(racing_stop_requests, PCT_ITERATIONS, PCT_DEPTH);
    }

    #[test]
    fn shuttle_phases_only_advance_and_every_completion_waiter_observes_the_one_outcome() {
        check_random(phases_racing_their_observers, RANDOM_ITERATIONS);
        check_pct(phases_racing_their_observers, PCT_ITERATIONS, PCT_DEPTH);
    }
}
