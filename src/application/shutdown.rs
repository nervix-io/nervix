//! The process-wide application shutdown lifecycle.
//!
//! Layer: control plane.
//!
//! - **Owns.** The ordered transition from a stop request through admission shutdown, drain
//!   support, terminal teardown, and the outcome reported to the process boundary.
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

/// The absolute monotonic deadline shared by every phase of one shutdown request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ShutdownDeadline {
    /// The caller did not impose a process-wide deadline.
    #[default]
    Unbounded,
    /// Every phase must complete before this monotonic instant.
    At(Instant),
}

impl ShutdownDeadline {
    /// Returns the time left before the deadline, clamped at zero after it expires.
    pub fn remaining(self) -> Option<Duration> {
        match self {
            Self::Unbounded => None,
            Self::At(deadline) => Some(deadline.saturating_duration_since(Instant::now())),
        }
    }
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownPhase {
    Serving,
    StopRequested,
    DrainSupport,
    TerminalTeardown,
    Finished,
}

/// Whether a shutdown phase completed its contract or had to leave work behind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownPhaseOutcome {
    Completed,
    Abandoned,
}

impl ShutdownPhaseOutcome {
    pub(in crate::application) fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Completed, Self::Completed) => Self::Completed,
            (Self::Completed, Self::Abandoned)
            | (Self::Abandoned, Self::Completed)
            | (Self::Abandoned, Self::Abandoned) => Self::Abandoned,
        }
    }
}

/// The terminal report shared with signal handlers and other process boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownOutcome {
    pub stop_admission: ShutdownPhaseOutcome,
    pub drain_support: ShutdownPhaseOutcome,
    pub terminal_teardown: ShutdownPhaseOutcome,
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
            .field("phase", &self.phase())
            .field("deadline", &self.request().map(ShutdownRequest::deadline))
            .finish_non_exhaustive()
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        let (state, _) = watch::channel(CoordinatorState::Serving);
        Self {
            inner: Arc::new(ShutdownCoordinatorInner {
                state,
                stop_requested: CancellationToken::new(),
                admission_shutdown: CancellationToken::new(),
                drain_support_shutdown: CancellationToken::new(),
            }),
        }
    }
}

impl ShutdownCoordinator {
    /// Starts shutdown exactly once. Repeated requests retain the first absolute deadline.
    pub fn request_stop(&self, deadline: ShutdownDeadline) -> ShutdownRequestOutcome {
        let requested = ShutdownRequest { deadline };
        let mut outcome = None;
        self.inner.state.send_if_modified(|state| {
            let existing = state.request();
            match existing {
                Some(existing) => {
                    outcome = Some(ShutdownRequestOutcome::AlreadyRequested(existing));
                    false
                }
                None => {
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

    #[test]
    fn repeated_stop_requests_keep_the_first_deadline() {
        let coordinator = ShutdownCoordinator::default();
        let first_deadline = ShutdownDeadline::At(Instant::now() + Duration::from_secs(30));
        let second_deadline = ShutdownDeadline::At(Instant::now() + Duration::from_secs(5));

        assert_eq!(
            coordinator.request_stop(first_deadline),
            ShutdownRequestOutcome::Accepted(ShutdownRequest {
                deadline: first_deadline,
            })
        );
        assert_eq!(
            coordinator.request_stop(second_deadline),
            ShutdownRequestOutcome::AlreadyRequested(ShutdownRequest {
                deadline: first_deadline,
            })
        );
        assert_eq!(
            coordinator.request().map(ShutdownRequest::deadline),
            Some(first_deadline)
        );
    }

    #[tokio::test]
    async fn drain_support_stays_live_until_terminal_teardown() {
        let coordinator = ShutdownCoordinator::default();
        let admission = coordinator.admission_token();
        let drain_support = coordinator.drain_support_token();

        coordinator.request_stop(ShutdownDeadline::Unbounded);
        coordinator.stop_admission();
        assert!(admission.is_cancelled());
        assert!(!drain_support.is_cancelled());

        coordinator.begin_drain_support(ShutdownPhaseOutcome::Completed);
        assert_eq!(coordinator.phase(), ShutdownPhase::DrainSupport);
        assert!(!drain_support.is_cancelled());

        coordinator.begin_terminal_teardown(ShutdownPhaseOutcome::Completed);
        assert!(drain_support.is_cancelled());
        coordinator.finish(ShutdownPhaseOutcome::Completed);

        assert_eq!(
            coordinator.completion().await,
            ShutdownOutcome {
                stop_admission: ShutdownPhaseOutcome::Completed,
                drain_support: ShutdownPhaseOutcome::Completed,
                terminal_teardown: ShutdownPhaseOutcome::Completed,
            }
        );
    }
}
