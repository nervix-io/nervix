//! Regressions for the runner-lag observer.
//!
//! Layer: test harness.
//! - **Owns.** A stand-in run that blocks the task it is polled on, and the stalls observed of it.
//! - **Depends on.** The runner-lag observer, a current-thread runtime and a blocking channel.
//! - **Must not know.** Which scenario step a real stall comes from.

use std::time::Duration;

use meticulous::OptionExt as _;
use nervix_primitives::sync::blocking::mpsc;

use crate::runner_lag::{RunnerLagObserver, RunnerStall};

/// How long the stand-in run blocks the task it is polled on.
const STALL: Duration = Duration::from_millis(400);

/// An observer whose bound sits well inside the stand-in stall.
const OBSERVER: RunnerLagObserver = RunnerLagObserver {
    period: Duration::from_millis(10),
    reported: Duration::from_millis(100),
};

/// The runner task is the only thread of a current-thread runtime here. A run that blocks inside
/// its poll stands in for a step doing synchronous work on the runner task.
#[nervix_primitives::test(flavor = "current_thread")]
async fn a_step_that_blocks_the_runner_task_is_reported_with_its_length() {
    let mut stalls: Vec<RunnerStall> = Vec::new();
    let run = async {
        // The observer's tick is due before the run blocks, so its lateness spans the stall.
        nervix_primitives::task::yield_now().await;
        let (_held, never_sent) = mpsc::sync_channel::<()>(1);
        // Nothing sends, so this blocks the task for the whole stall, as a synchronous step does.
        let blocked = never_sent.recv_timeout(STALL);
        assert!(blocked.is_err(), "nothing ends the stand-in stall early");
        nervix_primitives::task::yield_now().await;
        "finished"
    };
    let output = OBSERVER.observe(run, |stall| stalls.push(stall)).await;
    assert_eq!(output, "finished");
    let longest = stalls.iter().map(|stall| stall.late).max();
    let Some(longest) = longest else {
        panic!("the observer reports the blocked runner task");
    };
    // The tick was due at most one period after the stall began.
    let shortest_measurable = STALL
        .checked_sub(OBSERVER.period)
        .assured("the stand-in stall is longer than one tick period");
    assert!(
        longest >= shortest_measurable,
        "the reported stall of {longest:?} spans the {STALL:?} the task was blocked"
    );
}
