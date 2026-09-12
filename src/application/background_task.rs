//! A task the node supervises for as long as it serves, and how it is stopped.
//!
//! Layer: edges.
//!
//! - **Owns.** The handle the composition root keeps for each spawned task and the bounded wait
//!   that shutdown gives it.
//! - **Depends on.** The cancellation token the root cancels.
//! - **Must not know.** What any supervised task does.

use error_stack::Report;
use tokio::{task::JoinHandle, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{error, error::AppError};
use crate::task_shutdown::JoinShutdown as _;

const BACKGROUND_TASK_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

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

pub(in crate::application) async fn cancel_shutdown_on_completion<F>(
    server: F,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>>
where
    F: Future<Output = Result<(), Report<AppError>>>,
{
    let result = server.await;
    shutdown.cancel();
    result
}

pub(in crate::application) async fn await_background_task_shutdown(
    mut task: JoinHandle<()>,
    task_kind: &'static str,
) {
    match tokio::time::timeout(BACKGROUND_TASK_SHUTDOWN_GRACE_PERIOD, &mut task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            if error.is_cancelled() {
                warn!(task_kind, "shutdown task was cancelled");
            } else {
                error!(task_kind, error = %error, "shutdown task join failed");
            }
        }
        Err(_) => {
            warn!(
                task_kind,
                grace_period = %humantime::format_duration(BACKGROUND_TASK_SHUTDOWN_GRACE_PERIOD),
                "shutdown task exceeded grace period; aborting"
            );
            task.abort();
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                error!(task_kind, error = %error, "aborted shutdown task join failed");
            }
        }
    }
}
