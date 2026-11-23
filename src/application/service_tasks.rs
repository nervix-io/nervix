//! The work that sessions, requests, commands and cluster coordination start on this node, and how
//! a stopping node stops taking part in it.
//!
//! Layer: edges.
//!
//! - **Owns.** Tracking every service task, ending all of them at once when the node begins
//!   turning off, and the wait for them to end during terminal teardown.
//! - **Depends on.** Tokio task tracking and cancellation.
//! - **Must not know.** What any service task does.
//!
//! A node that is turning off takes part in no service work: neither a commit, command or restore
//! a client started, nor the coordination it runs for the cluster. Each task ends at its next
//! await, and what it left undone is completed by the recovery that completes the work of a node
//! that stopped: after a restart, or by the leader that follows.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_primitives::{
    sync::{Arc, CancellationToken},
    task::{JoinHandle, TaskTracker},
};
use tracing::info;

/// The tracker and the cancellation every service task shares.
struct ServiceTasksInner {
    tracker: TaskTracker,
    /// Cancelled once, when the node begins turning off.
    participation: CancellationToken,
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
                participation: CancellationToken::new(),
            }),
        }
    }
}

impl ServiceTasks {
    /// Spawns `task`. Its handle resolves to `None` when the node began turning off before the task
    /// finished, including when it had already begun before the task was spawned.
    pub(in crate::application) fn spawn<F>(&self, task: F) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let cancellation = self.inner.participation.clone();
        self.inner
            .tracker
            .spawn(cancellation.run_until_cancelled_owned(task))
    }

    /// Stops this node taking part in service work: every service task ends at its next await, and
    /// a task spawned from now on ends before it starts.
    pub(in crate::application) fn stop(&self) {
        let running = self.inner.tracker.len();
        self.inner.tracker.close();
        self.inner.participation.cancel();
        info!(
            tasks = running,
            "this node is turning off and stopped taking part in service work"
        );
    }

    /// Waits for every service task to end once [`Self::stop`] ended them. A cancelled task ends at
    /// its next await, so the wait lasts only as long as that takes to reach each task.
    pub(in crate::application) async fn wait_stopped(&self) {
        self.inner.tracker.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nervix_primitives::time::Instant;

    use super::*;

    #[nervix_primitives::test(start_paused = true)]
    async fn a_service_task_that_finished_keeps_its_output_when_the_node_stops() {
        let tasks = ServiceTasks::default();
        let task = tasks.spawn(async { 7 });
        let finished = task.await.expect("the task must not panic");

        tasks.stop();
        tasks.wait_stopped().await;

        assert_eq!(finished, Some(7));
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_node_that_turns_off_ends_every_running_service_task_at_once() {
        let tasks = ServiceTasks::default();
        let running = tasks.spawn(async {
            nervix_primitives::time::sleep(Duration::from_secs(60)).await;
            7
        });
        let started = Instant::now();

        tasks.stop();
        tasks.wait_stopped().await;

        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(
            running
                .await
                .expect("a cancelled service task does not panic"),
            None
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_service_task_spawned_after_the_node_began_turning_off_never_runs() {
        let tasks = ServiceTasks::default();
        tasks.stop();
        let ran = Arc::new(nervix_primitives::sync::atomic::AtomicBool::new(false));
        let witness = ran.clone();

        let late = tasks.spawn(async move {
            witness.store(true, nervix_primitives::sync::atomic::Ordering::SeqCst);
        });

        assert_eq!(
            late.await.expect("a cancelled service task does not panic"),
            None
        );
        assert!(!ran.load(nervix_primitives::sync::atomic::Ordering::SeqCst));
        tasks.wait_stopped().await;
    }
}
