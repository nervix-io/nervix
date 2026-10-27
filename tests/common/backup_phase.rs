//! Bounded backup-fixture operations whose progress must survive a delayed scenario runner.
//!
//! Layer: test harness.
//! - **Owns.** One phase's captured deadline and failure identity, and its operation's lifetime.
//! - **Depends on.** Phase deadlines and independently scheduled observations.
//! - **Must not know.** Scenario state, restore installation or producer delivery policy.

use std::{future::Future, time::Duration};

use super::phase_deadline::{BeforeDeadline, PhaseDeadline};

pub(crate) struct BackupPhase {
    identity: String,
    deadline: PhaseDeadline,
}

impl BackupPhase {
    pub(crate) fn new(identity: String, budget: Duration) -> Self {
        Self {
            identity,
            deadline: PhaseDeadline::after(budget),
        }
    }

    pub(crate) fn deadline(&self) -> PhaseDeadline {
        self.deadline
    }

    pub(crate) fn observe<F>(self, operation: F) -> impl Future<Output = F::Output> + Send
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // Dispatch before returning the result future. The phase keeps its original deadline
        // while the runner is busy polling other scenarios, and its abort guard owns cancellation.
        super::runner_observation::observe_off_the_runner(async move {
            eprintln!(
                "backup fixture phase started: {}, budget={:?}, elapsed={:?}",
                self.identity,
                self.deadline.budget(),
                self.deadline.elapsed()
            );
            match self.deadline.bound(operation).await {
                BeforeDeadline::Finished(output) => {
                    eprintln!(
                        "backup fixture phase completed: {}, elapsed={:?}",
                        self.identity,
                        self.deadline.elapsed()
                    );
                    output
                }
                BeforeDeadline::Passed => panic!(
                    "backup fixture phase exceeded {:?} after {:?}: {}",
                    self.deadline.budget(),
                    self.deadline.elapsed(),
                    self.identity
                ),
            }
        })
    }
}
