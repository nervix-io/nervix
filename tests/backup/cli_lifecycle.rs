//! Cancellation assertions through the scenario's public background-backup step.
//!
//! Layer: test harness.
//! - **Owns.** Cancelling a paused CLI backup and observing the exit of its exact child process.
//! - **Depends on.** The scenario's CLI task, OS process status, and bounded harness timers.
//! - **Must not know.** The client's retry logic or the server's backup implementation.

use super::*;

#[given(expr = "a fresh backup execution reference is saved as placeholder {string}")]
fn given_backup_execution_reference(world: &mut ScenarioWorld, placeholder: String) {
    let reference = crate::common::raw_session::fresh_execution_reference();
    world
        .placeholders
        .insert(placeholder, reference.to_string());
}

impl ScenarioWorld {
    /// Join cancellation before releasing a backup gate or the cluster's listener ports. The
    /// task's child is killed when its output future is dropped, even if the task never polled.
    pub(crate) async fn cancel_background_backup(
        &mut self,
    ) -> Option<Result<Output, nervix_primitives::task::JoinError>> {
        let task = self.background_backup.take()?;
        task.abort();
        Some(task.await)
    }
}

#[when("the scenario cancels its background CLI backup")]
async fn when_background_backup_is_cancelled(world: &mut ScenarioWorld) {
    let ended = world
        .cancel_background_backup()
        .await
        .assured("a preceding step started a background backup");
    assert!(
        matches!(ended, Err(error) if error.is_cancelled()),
        "the paused background backup task must end through cancellation"
    );
}

#[then("the cancelled background CLI process has ended")]
async fn then_background_cli_has_ended(world: &mut ScenarioWorld) {
    let pid = world
        .background_backup_pid
        .take()
        .assured("the background-backup step recorded its child process");
    let ended = nervix_primitives::time::timeout(Duration::from_secs(30), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            match nix::sys::signal::kill(pid, None) {
                Err(nix::errno::Errno::ESRCH) => return,
                Ok(()) => {}
                Err(error) => panic!("cannot observe background CLI {pid}: {error}"),
            }
            nervix_primitives::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if ended.is_err() {
        // Keep a failing regression from contaminating another scenario while retaining the
        // failure: this signal cannot turn the assertion below into a pass.
        nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL)
            .assured("the leaked CLI is killed before reporting its survival");
        panic!("background CLI {pid} survived cancellation of its owning task");
    }
}
