//! Deadline regressions for observations independent of the scenario runner.
//!
//! Layer: test harness.
//! - **Owns.** A caller stalled beyond the deadline of its bounded observation.
//! - **Depends on.** The observation owner, runtime workers, timers and a blocking channel.
//! - **Must not know.** Scenario steps or product execution.

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_primitives::{
    sync::{blocking::mpsc, oneshot},
    time::{Instant, sleep},
};

use crate::runner_observation::observe_off_the_runner;

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_observation_meets_its_deadline_while_the_caller_is_stalled() {
    let deadline = Instant::now() + Duration::from_millis(250);
    let observation = observe_off_the_runner(async move {
        sleep(Duration::from_millis(20)).await;
        assert!(
            Instant::now() < deadline,
            "the observation finishes within its own deadline despite a stalled caller"
        );
        42
    });

    let (_held, never_sent) = mpsc::sync_channel::<()>(1);
    assert!(matches!(
        never_sent.recv_timeout(Duration::from_secs(1)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    assert_eq!(observation.await, 42);
}

#[nervix_primitives::test]
async fn ending_a_step_cancels_the_observation_it_owns() {
    let (held, released) = oneshot::channel::<()>();
    let observation = observe_off_the_runner(async move {
        let _held = held;
        std::future::pending::<()>().await;
    });
    drop(observation);
    let ended = nervix_primitives::time::timeout(Duration::from_secs(1), released)
        .await
        .assured("cancelling the observation releases the values it owns");
    assert!(
        ended.is_err(),
        "the observation drops its sender without sending"
    );
}

#[nervix_primitives::test]
async fn an_observation_returns_its_original_panic_to_the_step() {
    use std::panic::AssertUnwindSafe;

    use futures_util::FutureExt as _;

    async fn panicking_observation() {
        panic!("the observation failed");
    }

    let observation = observe_off_the_runner(panicking_observation());
    let Err(failure) = AssertUnwindSafe(observation).catch_unwind().await else {
        panic!("the step receives the observation's panic");
    };
    assert_eq!(
        failure.downcast_ref::<&str>(),
        Some(&"the observation failed")
    );
}
