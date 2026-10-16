//! Regressions for synchronous step work handed off the runner task.
//!
//! Layer: test harness.
//! - **Owns.** A stand-in runner task and a stand-in synchronous job that can finish only while
//!   that runner task keeps polling another scenario.
//! - **Depends on.** The runner offload owner, a current-thread runtime and a blocking channel.
//! - **Must not know.** What a backup step reads, writes or asserts.

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_primitives::sync::blocking::mpsc;

use crate::runner_offload::off_the_runner;

/// How long the stand-in job waits for the runner task to poll another scenario. A job run on the
/// runner task itself never sees that poll, however long it waits.
const RUNNER_POLL_BOUND: Duration = Duration::from_secs(30);

/// The runner task is the only thread of a current-thread runtime here, as Cucumber's is the one
/// thread that polls every scenario. The job waits until another scenario's future on that task
/// has run, which only happens if the job is not occupying the task.
#[nervix_primitives::test(flavor = "current_thread")]
async fn a_step_job_leaves_the_runner_task_polling_other_scenarios() {
    let (polled, poll_announced) = mpsc::sync_channel(1);
    let job = off_the_runner(move || poll_announced.recv_timeout(RUNNER_POLL_BOUND));
    let other_scenario = async move {
        polled
            .send(())
            .assured("the stand-in job waits for the other scenario's poll");
    };
    let (seen, ()) = tokio::join!(job, other_scenario);
    seen.assured("the runner task polled another scenario while the step's job ran");
}

#[nervix_primitives::test(flavor = "current_thread")]
async fn a_step_job_returns_its_result_to_the_step() {
    let length = off_the_runner(|| vec![7_u8; 1024].len()).await;
    assert_eq!(length, 1024);
}

#[nervix_primitives::test(flavor = "current_thread")]
#[should_panic(expected = "the archive verifies")]
async fn a_step_job_fails_its_step_with_its_own_panic() {
    off_the_runner(|| panic!("the archive verifies")).await;
}
