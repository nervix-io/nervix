//! Deadline regressions for observations independent of the scenario runner.
//!
//! Layer: test harness.
//! - **Owns.** A caller stalled beyond the deadline of its bounded observation.
//! - **Depends on.** The observation owner, runtime workers, timers and a blocking channel.
//! - **Must not know.** Scenario steps or product execution.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    sync::{blocking::mpsc, oneshot},
    time::{Instant, sleep},
};

use crate::runner_observation::{ArrivalObservation, observe_off_the_runner};

/// Control the input and the assertion independently, with no wall-clock scheduling tolerance.
async fn measured_arrival(
    release_after: Duration,
    assertion_after: Duration,
) -> crate::runner_observation::ObservedArrival<&'static str> {
    let (release, released) = oneshot::channel();
    let (received, receipt) = oneshot::channel();
    let observation = ArrivalObservation::start(async move {
        let value = released
            .await
            .assured("the controlled publisher sends one row");
        received
            .send(())
            .assured("the test waits for the observer to receive its row");
        value
    })
    .await;
    nervix_primitives::time::advance(release_after).await;
    release
        .send("alpha released")
        .assured("the observer is polling before publication");
    receipt
        .await
        .assured("the observer confirms receipt before the assertion is delayed");
    nervix_primitives::time::advance(assertion_after).await;
    observation.finish().await
}

#[nervix_primitives::test(start_paused = true)]
async fn a_late_assertion_keeps_the_on_time_receipt_timestamp() {
    let published_at = Instant::now();
    let arrival = measured_arrival(Duration::from_secs(3), Duration::from_secs(10)).await;
    assert_eq!(arrival.value, "alpha released");
    assert_eq!(
        arrival.received_at.duration_since(published_at),
        Duration::from_secs(3)
    );
    assert!(arrival.arrived_no_sooner_than(published_at, Duration::from_secs(3)));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_late_assertion_still_refuses_an_early_receipt() {
    let published_at = Instant::now();
    let arrival = measured_arrival(Duration::from_secs(1), Duration::from_secs(10)).await;
    assert_eq!(
        arrival.received_at.duration_since(published_at),
        Duration::from_secs(1)
    );
    assert!(!arrival.arrived_no_sooner_than(published_at, Duration::from_secs(3)));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_row_received_before_publication_cannot_satisfy_its_delay() {
    let arrival = ArrivalObservation::start(async { "already queued" })
        .await
        .finish()
        .await;
    nervix_primitives::time::advance(Duration::from_secs(1)).await;
    assert!(!arrival.arrived_no_sooner_than(Instant::now(), Duration::ZERO));
}

#[nervix_primitives::test]
async fn dropping_a_receipt_observer_releases_its_session() {
    let (held, released) = oneshot::channel::<()>();
    let observation = ArrivalObservation::start(async move {
        let _held = held;
        std::future::pending::<()>().await;
    })
    .await;
    drop(observation);
    assert!(
        nervix_primitives::time::timeout(Duration::from_secs(1), released)
            .await
            .assured("the dropped observation cancels its session reader")
            .is_err()
    );
}

#[nervix_primitives::test]
async fn a_receipt_observer_preserves_a_panic_from_its_first_poll() {
    use std::panic::AssertUnwindSafe;

    use futures_util::FutureExt as _;

    let observation: ArrivalObservation<()> =
        ArrivalObservation::start(async { panic!("receipt read failed") }).await;
    let Err(failure) = AssertUnwindSafe(observation.finish()).catch_unwind().await else {
        panic!("the step must receive the original observation panic");
    };
    assert_eq!(failure.downcast_ref::<&str>(), Some(&"receipt read failed"));
}

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

#[nervix_primitives::test(start_paused = true)]
async fn sequential_command_replies_progress_before_delayed_result_collection() {
    #[derive(Debug)]
    struct Request {
        position: usize,
        reply: oneshot::Sender<usize>,
    }

    let started = Instant::now();
    let deadline = started + Duration::from_secs(10);
    let (requests, mut pending) = nervix_primitives::sync::mpsc::channel::<Request>(1);
    let (finished, completion) = oneshot::channel();
    let observation = observe_off_the_runner(async move {
        for position in 0..6 {
            nervix_primitives::task::consume_budget().await;
            let (reply, received) = oneshot::channel();
            requests
                .send(Request { position, reply })
                .await
                .assured("the controlled server receives every request");
            let answer = nervix_primitives::time::timeout_at(deadline, received)
                .await
                .assured("the controlled server replies within the probe deadline")
                .assured("the controlled server answers each command");
            assert_eq!(answer, position);
        }
        let completed_at = Instant::now();
        finished
            .send(())
            .assured("the test waits for command completion before delaying collection");
        completed_at
    });

    // This task plays the server while the scenario never polls its observation future.
    for position in 0..6 {
        let request = nervix_primitives::time::timeout_at(deadline, pending.recv())
            .await
            .assured("the independent probe dispatches every command without runner polling")
            .assured("the independent probe retains its request sender");
        assert_eq!(request.position, position);
        nervix_primitives::time::advance(Duration::from_secs(1)).await;
        request
            .reply
            .send(position)
            .assured("the independent probe keeps polling every reply");
    }
    completion
        .await
        .assured("the independent probe completes all six commands");
    nervix_primitives::time::advance(Duration::from_secs(11)).await;
    let completed_at = observation.await;
    assert_eq!(completed_at.duration_since(started), Duration::from_secs(6));
    assert!(Instant::now() > deadline);
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
