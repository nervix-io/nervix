//! Regressions for holds a scenario injects while the runner task is stalled.
//!
//! Layer: test harness.
//! - **Owns.** A stand-in gate and the release of its hold while the runner thread is blocked.
//! - **Depends on.** The injected hold owner, watch channels and a blocking channel.
//! - **Must not know.** Which product deadline a hold races, or scenario state.

use std::{fmt, future::Future, time::Duration};

use meticulous::ResultExt as _;
use nervix_primitives::sync::{blocking::mpsc, watch};

use crate::injected_hold::{HeldGate, InjectedHold, InjectedHoldError, hold_in_order};

/// How long the stalled runner thread waits for an injected hold to release its gate. A hold
/// timed on the runner task cannot release it before the stall ends, however long that is.
const RUNNER_STALL_BOUND: Duration = Duration::from_secs(30);

/// An operation the harness holds at a gate, standing in for a server waiting at a controlled
/// cut. The test marks it reached, and its release is announced on a blocking channel, which a
/// stalled runner thread can wait on without polling anything.
struct StandInGate {
    reached: watch::Receiver<bool>,
    released: mpsc::SyncSender<()>,
}

impl fmt::Display for StandInGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the stand-in gate")
    }
}

impl HeldGate for StandInGate {
    fn reached(&self) -> impl Future<Output = ()> + Send {
        let mut reached = self.reached.clone();
        async move {
            reached
                .wait_for(|reached| *reached)
                .await
                .assured("the test keeps the gate's sender for the whole hold");
        }
    }

    fn release(&self) {
        self.released
            .send(())
            .assured("the stalled runner thread waits for the release");
    }
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_injected_hold_releases_its_gate_while_the_runner_task_is_stalled() {
    let (reach, reached) = watch::channel(false);
    let (released, release_announced) = mpsc::sync_channel(1);
    let hold = InjectedHold {
        gate: StandInGate { reached, released },
        duration: Duration::from_millis(50),
    };
    let held = hold_in_order(vec![hold], Duration::from_secs(30));
    reach.send_replace(true);
    // A synchronous step of another scenario stalls the runner task, which this test's thread
    // stands in for: nothing polls `held` until the gate has been released.
    release_announced
        .recv_timeout(RUNNER_STALL_BOUND)
        .assured("the injected hold releases its gate while the runner task is stalled");
    held.await
        .assured("the stand-in operation reached its gate before the hold began");
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_injected_hold_reports_a_gate_its_operation_never_reached() {
    let (reach, reached) = watch::channel(false);
    let (released, release_announced) = mpsc::sync_channel(1);
    let hold = InjectedHold {
        gate: StandInGate { reached, released },
        duration: Duration::from_millis(50),
    };
    let arrival = Duration::from_millis(100);
    let held = hold_in_order(vec![hold], arrival).await;
    let Err(report) = held else {
        panic!("a gate the operation never reached ends the holds with a failure");
    };
    let InjectedHoldError::NotReached {
        gate,
        arrival: waited,
    } = report.current_context();
    assert_eq!(gate, "the stand-in gate");
    assert_eq!(*waited, arrival);
    assert!(
        release_announced.try_recv().is_err(),
        "a gate the operation never reached is never released"
    );
    drop(reach);
}
