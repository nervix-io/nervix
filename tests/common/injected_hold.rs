//! Holds a scenario injects at the gates of controlled operations, timed on a task of their own.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Waiting for each gate of a sequence to be reached, holding it for the duration the
//!   scenario injects after observing it, releasing it, and the typed failure of a gate the
//!   controlled operation never reached, all on one task the holds' future owns.
//! - **Depends on.** The gates a scenario armed, and the binary's runtime and monotonic timers.
//! - **Must not know.** What a gate holds back, or the product deadline its hold races.
//!
//! An injected hold races the deadline the controlled operation keeps while it waits at the gate.
//! The runner task polls every scenario's steps, and a synchronous step of another scenario can
//! stall it for longer than that deadline's margin, so a hold never waits on it: from observing a
//! gate to releasing it, the holds run on a worker of the runtime, and the step only awaits them.

use std::{fmt, future::Future, panic::resume_unwind, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_primitives::task::AbortOnDropHandle;
use thiserror::Error;

/// A gate a controlled operation waits at until the scenario releases it.
pub(crate) trait HeldGate: fmt::Display + Send + 'static {
    /// Resolves once the controlled operation waits at the gate.
    fn reached(&self) -> impl Future<Output = ()> + Send;

    /// Lets the controlled operation continue.
    fn release(&self);
}

/// One gate of a sequence, and how long the scenario holds it after observing it.
pub(crate) struct InjectedHold<G> {
    pub(crate) gate: G,
    pub(crate) duration: Duration,
}

/// Why a sequence of injected holds ended before it released every gate.
#[derive(Debug, Error)]
pub(crate) enum InjectedHoldError {
    #[error("the controlled operation did not reach {gate} within {arrival:?}")]
    NotReached { gate: String, arrival: Duration },
}

/// Starts holding each gate of `holds` in order, on a task of its own: waits up to `arrival` for
/// the controlled operation to reach a gate, holds it for its duration, and releases it before
/// waiting for the next.
///
/// The holds run whether or not the returned future is polled, so a stalled runner task delays
/// only when its step learns of their end. Dropping the future aborts the holds that remain.
pub(crate) fn hold_in_order<G: HeldGate>(
    holds: Vec<InjectedHold<G>>,
    arrival: Duration,
) -> impl Future<Output = Result<(), Report<InjectedHoldError>>> + Send {
    let task = AbortOnDropHandle::new(nervix_primitives::task::spawn(hold_each(holds, arrival)));
    async move {
        match task.await {
            Ok(held) => held,
            Err(failure) => {
                let panic = failure
                    .try_into_panic()
                    .assured("the future owns the task's only handle, so nothing cancelled it");
                resume_unwind(panic)
            }
        }
    }
}

async fn hold_each<G: HeldGate>(
    holds: Vec<InjectedHold<G>>,
    arrival: Duration,
) -> Result<(), Report<InjectedHoldError>> {
    for InjectedHold { gate, duration } in holds {
        nervix_primitives::task::consume_budget().await;
        let reached = nervix_primitives::time::timeout(arrival, gate.reached()).await;
        if reached.is_err() {
            return Err(Report::new(InjectedHoldError::NotReached {
                gate: gate.to_string(),
                arrival,
            }));
        }
        // The injected workload duration after an observed gate; the notification establishes
        // that the operation waits there, so this is not a readiness sleep.
        nervix_primitives::time::sleep(duration).await;
        gate.release();
    }
    Ok(())
}
