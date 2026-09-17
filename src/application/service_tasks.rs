//! The work that sessions, requests and commands start on this node, and how shutdown ends it.
//!
//! Layer: edges.
//!
//! - **Owns.** Tracking every service task, waiting for them during terminal teardown, and
//!   cancelling the ones still running when the shutdown deadline passes.
//! - **Depends on.** Tokio task tracking and cancellation, and the shutdown deadline.
//! - **Must not know.** What any service task does.

use tokio::task::JoinHandle;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::warn;
use triomphe::Arc;

use super::shutdown::{BeforeDeadline, ShutdownDeadline, ShutdownPhaseOutcome};

#[cfg(not(feature = "shuttle"))]
fn run_until_cancelled_owned<F>(
    cancellation: CancellationToken,
    task: F,
) -> impl Future<Output = Option<F::Output>>
where
    F: Future,
{
    cancellation.run_until_cancelled_owned(task)
}

#[cfg(feature = "shuttle")]
async fn run_until_cancelled_owned<F>(cancellation: CancellationToken, task: F) -> Option<F::Output>
where
    F: Future,
{
    cancellation.run_until_cancelled(task).await
}

/// The tracker and the cancellation every service task shares.
struct ServiceTasksInner {
    tracker: TaskTracker,
    /// Cancelled only when the shutdown deadline passes while service tasks are still running.
    deadline_cancellation: CancellationToken,
}

/// A cloneable handle over the tasks this node's services spawn.
#[derive(Clone)]
pub(in crate::application) struct ServiceTasks {
    inner: Arc<ServiceTasksInner>,
}

impl Default for ServiceTasks {
    fn default() -> Self {
        Self {
            inner: Arc::new(ServiceTasksInner {
                tracker: TaskTracker::new(),
                deadline_cancellation: CancellationToken::new(),
            }),
        }
    }
}

impl ServiceTasks {
    /// Spawns `task`. Its handle resolves to `None` when shutdown cancelled the task at the
    /// deadline before it finished.
    pub(in crate::application) fn spawn<F>(&self, task: F) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let cancellation = self.inner.deadline_cancellation.clone();
        self.inner
            .tracker
            .spawn(run_until_cancelled_owned(cancellation, task))
    }

    /// Waits for every service task until the shutdown deadline, then cancels the ones still
    /// running and waits for them to end.
    ///
    /// A cancelled task ends at its next await, so the second wait lasts only as long as that
    /// cancellation takes to reach each task.
    pub(in crate::application) async fn shut_down(
        &self,
        deadline: ShutdownDeadline,
    ) -> ShutdownPhaseOutcome {
        self.inner.tracker.close();
        let finished = deadline.bound(self.inner.tracker.wait()).await;
        match finished {
            BeforeDeadline::Finished(()) => ShutdownPhaseOutcome::Completed,
            BeforeDeadline::Expired => {
                warn!(
                    tasks = self.inner.tracker.len(),
                    "shutdown deadline expired; cancelling the service tasks still running"
                );
                self.inner.deadline_cancellation.cancel();
                self.inner.tracker.wait().await;
                ShutdownPhaseOutcome::Forced
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, Instant};

    use super::*;
    use crate::application::ShutdownCoordinator;

    fn deadline_after(timeout: Duration) -> ShutdownDeadline {
        let coordinator = ShutdownCoordinator::new(timeout);
        coordinator.request_stop();
        coordinator
            .request()
            .expect("the stop request above was accepted")
            .deadline()
    }

    #[tokio::test(start_paused = true)]
    async fn service_tasks_that_finish_before_the_deadline_complete_their_shutdown() {
        let tasks = ServiceTasks::default();
        let deadline = deadline_after(Duration::from_secs(5));
        let task = tasks.spawn(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            7
        });

        let outcome = tasks.shut_down(deadline).await;

        assert_eq!(outcome, ShutdownPhaseOutcome::Completed);
        assert_eq!(task.await.expect("the task must not panic"), Some(7));
    }

    #[tokio::test(start_paused = true)]
    async fn a_service_task_still_running_at_the_deadline_is_cancelled_and_joined() {
        let tasks = ServiceTasks::default();
        let deadline = deadline_after(Duration::from_secs(5));
        let task = tasks.spawn(std::future::pending::<()>());
        let started = Instant::now();

        let outcome = tasks.shut_down(deadline).await;

        assert_eq!(outcome, ShutdownPhaseOutcome::Forced);
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        assert_eq!(
            task.await.expect("a cancelled service task does not panic"),
            None
        );
    }
}
