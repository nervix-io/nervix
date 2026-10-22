//! Bounded observations a scenario keeps independent of its runner task.
//!
//! Layer: test harness.
//! - **Owns.** An observation task, receipt timestamps, cancellation when its owner drops it, and
//!   returning its result or original panic to that owner.
//! - **Depends on.** An observation future, monotonic instants and the binary's runtime.
//! - **Must not know.** The observed product, its protocol, or its deadline policy.

use std::{
    future::{Future, poll_fn},
    panic::resume_unwind,
    pin::Pin,
    task::Poll,
    time::Duration,
};

use meticulous::ResultExt as _;
use nervix_primitives::{sync::oneshot, task::AbortOnDropHandle, time::Instant};
use nervix_recovery::Discarded as _;

/// A receipt timestamp retained even when the scenario reads the result much later.
pub(crate) struct ObservedArrival<T> {
    pub(crate) value: T,
    pub(crate) received_at: Instant,
}

impl<T> ObservedArrival<T> {
    pub(crate) fn arrived_no_sooner_than(&self, published_at: Instant, delay: Duration) -> bool {
        let Some(elapsed) = self.received_at.checked_duration_since(published_at) else {
            return false;
        };
        elapsed >= delay
    }
}

/// An owned receipt observation started before the scenario performs its triggering action.
///
/// The future owns its session until it returns. There is no intervening step that can defer
/// polling an already queued row and replace its receipt time with the assertion's start time.
pub(crate) struct ArrivalObservation<T> {
    completion: Pin<Box<dyn Future<Output = ObservedArrival<T>> + Send>>,
}

impl<T> std::fmt::Debug for ArrivalObservation<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArrivalObservation")
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> ArrivalObservation<T> {
    /// Return only after the observer has polled its input at least once.
    pub(crate) async fn start<F>(observation: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        let (started, polling) = oneshot::channel();
        let completion = Box::pin(observe_off_the_runner(async move {
            let mut started = Some(started);
            let mut observation = std::pin::pin!(observation);
            poll_fn(|context| {
                let result = match observation.as_mut().poll(context) {
                    Poll::Ready(value) => Poll::Ready(ObservedArrival {
                        value,
                        received_at: Instant::now(),
                    }),
                    Poll::Pending => Poll::Pending,
                };
                if let Some(started) = started.take()
                    && started.send(()).is_err()
                {
                    // The caller cancelled startup and is dropping this task's abort guard.
                    return Poll::Pending;
                }
                result
            })
            .await
        }));
        polling.await.discarded(
            "the completion future retains a panic raised while polling the observer's input",
        );
        Self { completion }
    }

    pub(crate) async fn finish(self) -> ObservedArrival<T> {
        self.completion.await
    }
}

/// Starts polling `observation` on a runtime worker before returning its owned future.
///
/// The caller owns the observation's deadline policy. A stalled runner delays learning the result
/// while its observation keeps polling. Dropping the returned future aborts the task and releases
/// the session or observer the observation owns.
pub(crate) fn observe_off_the_runner<F>(observation: F) -> impl Future<Output = F::Output> + Send
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let task = AbortOnDropHandle::new(nervix_primitives::task::spawn(observation));
    async move {
        match task.await {
            Ok(output) => output,
            Err(failure) => {
                let panic = failure.try_into_panic().assured(
                    "the step owns the observation's only handle, so nothing cancelled it",
                );
                resume_unwind(panic)
            }
        }
    }
}
