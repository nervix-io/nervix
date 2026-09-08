//! How a node joins a task it is taking down.
//!
//! Layer: data plane and control plane, over the tasks both of them spawn.
//!
//! - **Owns.** The one reading of a [`JoinError`] a shutdown path is allowed to make: which
//!   outcomes the shutdown asked for, and which one is a defect that has to stay visible.
//! - **Depends on.** Tokio's join handles and the tracing macros. Nothing about what the joined
//!   task was doing.
//! - **Must not know.** Domains, branches, relays, or any entity a task belongs to. A caller that
//!   needs those in the record logs them itself.
//!
//! Awaiting a handle can fail two ways, and they are not the same failure. A handle that was
//! aborted resolves to a cancellation, which is precisely the outcome the abort asked for and
//! carries nothing a caller did not already know. A panic is the opposite: the task died on its
//! own, the join is the last place that fact exists, and discarding it loses the only evidence the
//! node has that its own invariant broke.
//!
//! Dropping the join result with `let _` collapses the two into silence. These methods keep them
//! apart.

use tokio::task::{JoinError, JoinHandle};
use tracing::error;

/// Joining a task the node is stopping on purpose.
pub(crate) trait JoinShutdown {
    /// Await a task that has been asked to stop, reporting a panic and accepting a cancellation.
    ///
    /// `task` names what was joined so the report identifies it without the caller having to log
    /// separately.
    async fn join_after_shutdown(self, task: &str);
}

impl<T> JoinShutdown for JoinHandle<T> {
    async fn join_after_shutdown(self, task: &str) {
        if let Err(error) = self.await {
            report_join_failure(&error, task);
        }
    }
}

impl<T> JoinShutdown for &mut JoinHandle<T> {
    async fn join_after_shutdown(self, task: &str) {
        if let Err(error) = self.await {
            report_join_failure(&error, task);
        }
    }
}

/// Report a join failure at the severity its cause deserves.
///
/// A cancellation is the shutdown working, so it is left unreported. A panic escaped a task the
/// node owns, which is a broken invariant the node could not turn into an error, and `error` is
/// the level that says so.
fn report_join_failure(error: &JoinError, task: &str) {
    if error.is_cancelled() {
        return;
    }
    error!(%error, task, "task panicked before the node could join it");
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use meticulous::ResultExt as _;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    /// Collects everything a scoped subscriber writes so a test can assert on what was reported.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn contents(&self) -> String {
            String::from_utf8_lossy(
                &self
                    .0
                    .lock()
                    .verified("no test holds this lock across a panic")
                    .clone(),
            )
            .into_owned()
        }
    }

    impl io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .verified("no test holds this lock across a panic")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    async fn join_capturing(handle: JoinHandle<()>, task: &str) -> String {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish();
        // `#[tokio::test]` runs the whole future on the calling thread, so the thread-local
        // default the guard installs stays in force across the await.
        let guard = tracing::subscriber::set_default(subscriber);
        handle.join_after_shutdown(task).await;
        drop(guard);
        logs.contents()
    }

    #[tokio::test]
    async fn an_aborted_task_is_joined_without_a_report() {
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        handle.abort();

        assert_eq!(join_capturing(handle, "aborted task").await, "");
    }

    #[tokio::test]
    async fn a_panicking_task_is_reported_when_it_is_joined() {
        let handle = tokio::spawn(async {
            panic!("the task broke its own invariant");
        });

        let logs = join_capturing(handle, "panicking task").await;
        assert!(
            logs.contains("task panicked before the node could join it"),
            "a panic must survive the join, got: {logs}"
        );
        assert!(
            logs.contains("panicking task"),
            "the report must name the task, got: {logs}"
        );
    }

    #[tokio::test]
    async fn a_task_that_finished_is_joined_without_a_report() {
        let handle = tokio::spawn(async {});

        assert_eq!(join_capturing(handle, "completed task").await, "");
    }
}
