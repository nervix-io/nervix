//! How long the runner task goes without polling the scenarios it drives.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** A periodic tick polled beside a run on the same task, how late each tick is polled,
//!   and handing every stall at or above a bound to the run's reporter.
//! - **Depends on.** The binary's runtime and its monotonic timers.
//! - **Must not know.** Which step held the task, or what a reporter does with a stall.
//!
//! Cucumber polls every running scenario, and the suite watchdog around them, from one runner task.
//! A step that blocks inside its poll holds every other scenario, and nothing else in the process
//! sees it: the watchdog is polled on that same task. A tick polled on the task, however, is polled
//! only once the blocking step returns, so the lateness of the tick is the length of the stall.

use std::{fmt, time::Duration};

use meticulous::OptionExt as _;
use nervix_primitives::time::Instant;

/// A period during which the runner task polled nothing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunnerStall {
    /// How long after its due time the tick was polled.
    pub(crate) late: Duration,
}

impl fmt::Display for RunnerStall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "runner task stalled for {:?}", self.late)
    }
}

/// The tick a run is observed with.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunnerLagObserver {
    /// How often the task is expected to poll the tick. A stall is measured to within one period.
    pub(crate) period: Duration,
    /// The shortest stall handed to the reporter.
    pub(crate) reported: Duration,
}

impl RunnerLagObserver {
    /// Polls `run` to its end on the calling task, and hands `report` every stall of that task at
    /// or above the observer's bound. The tick is polled before `run` on each wake-up, so a stall
    /// inside the poll that ends `run` is reported too.
    pub(crate) async fn observe<F: Future>(
        self,
        run: F,
        mut report: impl FnMut(RunnerStall),
    ) -> F::Output {
        let mut run = std::pin::pin!(run);
        loop {
            nervix_primitives::task::consume_budget().await;
            let due = Instant::now()
                .checked_add(self.period)
                .assured("a tick period of a fraction of a second fits after the present instant");
            nervix_primitives::select! {
                biased;
                () = nervix_primitives::time::sleep_until(due) => {
                    let late = due.elapsed();
                    if late >= self.reported {
                        report(RunnerStall { late });
                    }
                }
                output = &mut run => return output,
            }
        }
    }
}
