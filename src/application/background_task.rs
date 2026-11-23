//! A task the node supervises for as long as it serves, and how it is stopped.
//!
//! Layer: edges.
//!
//! - **Owns.** The handle the composition root keeps for each spawned task, the bounded wait that
//!   shutdown gives it, and the join of the public listeners once admission closes, including when
//!   the client connections they still hold are cut.
//! - **Depends on.** The cancellation token or shutdown coordinator the root advances, and the
//!   task grace period that bounds every wait.
//! - **Must not know.** What any supervised task does.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::time::Duration;

use error_stack::Report;
use nervix_primitives::{sync::CancellationToken, task::JoinHandle};
use tracing::{info, warn};

use super::{
    error,
    error::AppError,
    shutdown::{ShutdownCoordinator, ShutdownPhaseOutcome},
};
use crate::task_shutdown::JoinShutdown as _;

/// A running background task and the token that stops it.
pub(in crate::application) struct BackgroundTask {
    pub(in crate::application) cancel: CancellationToken,
    pub(in crate::application) handle: JoinHandle<()>,
}

impl BackgroundTask {
    pub(in crate::application) fn request_stop(&self) {
        self.cancel.cancel();
    }

    async fn join(self) {
        self.handle.join_after_shutdown("background task").await;
    }

    /// Stops the task and waits for it to finish, so the caller never drops a task that is still
    /// touching the state it is about to replace.
    pub(in crate::application) async fn stop(self) {
        self.request_stop();
        self.join().await;
    }
}

pub(in crate::application) async fn request_shutdown_on_completion<F>(
    server: F,
    shutdown: ShutdownCoordinator,
) -> Result<(), Report<AppError>>
where
    F: Future<Output = Result<(), Report<AppError>>>,
{
    let result = server.await;
    shutdown.request_stop();
    result
}

/// Requests the node's stop once a listener that cannot fail returns, which it does only once the
/// node closes admission.
pub(in crate::application) async fn request_shutdown_on_return<F>(
    listener: F,
    shutdown: ShutdownCoordinator,
) where
    F: Future<Output = ()>,
{
    listener.await;
    shutdown.request_stop();
}

/// What joining the public listeners reported once admission closed.
pub(in crate::application) struct JoinedListeners {
    /// The error a listener returned, or the failure of the task that ran the listeners.
    pub(in crate::application) result: Result<(), Report<AppError>>,
    /// How closing the listeners ended for the admission phase.
    pub(in crate::application) outcome: ShutdownPhaseOutcome,
}

/// Waits for the public listeners to stop accepting and to close the connections they had
/// accepted. The connections they still hold when their grace period ends are cut, and listeners
/// that have not closed one more grace period after that are aborted.
pub(in crate::application) async fn join_public_listeners(
    mut listeners: JoinHandle<Result<(), Report<AppError>>>,
    shutdown: &ShutdownCoordinator,
) -> JoinedListeners {
    let grace_period = shutdown.task_grace_period();
    let closed_within_grace = nervix_primitives::time::timeout(grace_period, &mut listeners).await;
    let joined = match closed_within_grace {
        Ok(joined) => joined,
        Err(_) => {
            info!(
                grace_period = %humantime::format_duration(grace_period),
                "public listeners still hold client connections after their grace period; cutting \
                 them"
            );
            shutdown.cut_connections();
            let closed_after_cut =
                nervix_primitives::time::timeout(grace_period, &mut listeners).await;
            match closed_after_cut {
                Ok(joined) => joined,
                Err(_) => {
                    warn!(
                        grace_period = %humantime::format_duration(grace_period),
                        "public listeners did not close within their grace period after their \
                         connections were cut; aborting them"
                    );
                    listeners.abort();
                    listeners.join_after_shutdown("public listeners").await;
                    return JoinedListeners {
                        result: Ok(()),
                        outcome: ShutdownPhaseOutcome::Abandoned,
                    };
                }
            }
        }
    };
    match joined {
        Ok(Ok(())) => JoinedListeners {
            result: Ok(()),
            outcome: ShutdownPhaseOutcome::Completed,
        },
        Ok(Err(error)) => JoinedListeners {
            result: Err(error),
            outcome: ShutdownPhaseOutcome::Abandoned,
        },
        Err(error) => JoinedListeners {
            result: Err(Report::new(error).change_context(AppError::JoinPublicListeners)),
            outcome: ShutdownPhaseOutcome::Abandoned,
        },
    }
}

/// Waits for a task that has been asked to stop, for at most its grace period, and aborts it if it
/// is still running then.
pub(in crate::application) async fn await_background_task_shutdown(
    mut task: JoinHandle<()>,
    task_kind: &'static str,
    grace_period: Duration,
) -> ShutdownPhaseOutcome {
    let joined = nervix_primitives::time::timeout(grace_period, &mut task).await;
    match joined {
        Ok(Ok(())) => ShutdownPhaseOutcome::Completed,
        Ok(Err(error)) => {
            if error.is_cancelled() {
                warn!(task_kind, "shutdown task was cancelled");
            } else {
                error!(task_kind, error = %error, "shutdown task join failed");
            }
            ShutdownPhaseOutcome::Abandoned
        }
        Err(_) => {
            warn!(
                task_kind,
                grace_period = %humantime::format_duration(grace_period),
                "shutdown task exceeded grace period; aborting"
            );
            task.abort();
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                error!(task_kind, error = %error, "aborted shutdown task join failed");
            }
            ShutdownPhaseOutcome::Abandoned
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_primitives::time::Instant;

    use super::*;
    use crate::application::shutdown::ShutdownSettings;

    fn grace_period() -> Duration {
        ShutdownSettings::default().task_grace_period()
    }

    /// A coordinator whose shutdown has closed admission.
    fn closed_admission() -> ShutdownCoordinator {
        let coordinator = ShutdownCoordinator::default();
        coordinator.request_stop();
        coordinator.stop_admission();
        coordinator
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_background_task_that_stops_within_its_grace_period_completes() {
        let task = nervix_primitives::task::spawn(async {});

        let outcome = await_background_task_shutdown(task, "test task", grace_period()).await;

        assert_eq!(outcome, ShutdownPhaseOutcome::Completed);
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_background_task_that_ignores_its_stop_is_aborted_after_its_grace_period() {
        let task = nervix_primitives::task::spawn(std::future::pending::<()>());
        let started = Instant::now();

        let outcome = await_background_task_shutdown(task, "test task", grace_period()).await;

        assert_eq!(outcome, ShutdownPhaseOutcome::Abandoned);
        assert_eq!(started.elapsed(), grace_period());
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn public_listeners_that_close_within_their_grace_period_keep_their_connections() {
        let coordinator = closed_admission();
        let listeners = nervix_primitives::task::spawn(async { Ok::<(), Report<AppError>>(()) });
        let cut = coordinator.connections_cut_token();

        let joined = join_public_listeners(listeners, &coordinator).await;

        assert_eq!(joined.outcome, ShutdownPhaseOutcome::Completed);
        assert!(joined.result.is_ok());
        assert!(
            !cut.is_cancelled(),
            "listeners that closed their connections themselves must not have them cut"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn public_listeners_still_open_after_their_grace_period_have_their_connections_cut() {
        let coordinator = closed_admission();
        let cut = coordinator.connections_cut_token();
        // A listener whose client holds its connection open closes only once the connection is cut.
        let listeners = nervix_primitives::task::spawn(async move {
            cut.cancelled().await;
            Ok::<(), Report<AppError>>(())
        });
        let started = Instant::now();

        let joined = join_public_listeners(listeners, &coordinator).await;

        assert_eq!(joined.outcome, ShutdownPhaseOutcome::Completed);
        assert!(joined.result.is_ok());
        assert_eq!(started.elapsed(), grace_period());
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn public_listeners_that_outlive_the_cut_by_a_grace_period_are_aborted_and_joined() {
        let coordinator = closed_admission();
        let listeners =
            nervix_primitives::task::spawn(std::future::pending::<Result<(), Report<AppError>>>());
        let cut = coordinator.connections_cut_token();
        let started = Instant::now();

        let joined = join_public_listeners(listeners, &coordinator).await;

        assert_eq!(joined.outcome, ShutdownPhaseOutcome::Abandoned);
        assert!(joined.result.is_ok());
        assert!(
            cut.is_cancelled(),
            "the connections are cut when the first grace period ends, before the listeners are \
             aborted"
        );
        assert_eq!(
            started.elapsed(),
            grace_period()
                .checked_mul(2)
                .expect("twice the default grace period is a valid duration")
        );
    }
}
