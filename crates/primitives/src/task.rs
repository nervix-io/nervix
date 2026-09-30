//! Async tasks, selected for the build's execution mode: spawning and joining them, yielding and
//! the cooperative budget, aborting them, and tracking a group of them.
//!
//! Every async loop whose body performs async work calls [`consume_budget`] once per iteration near
//! the top of its body, so a loop that always has work ready still lets the scheduler run other
//! tasks.
//!
//! [`spawn_blocking`] is the runtime's mechanism, not a policy: variable-size and blocking work
//! belongs to the bounded executor, which admits, charges and cancels it and runs its storage jobs
//! here.
//!
//! [`spawn_cpu`] is the mechanism that runs a CPU job the bounded executor admitted. It is
//! [`spawn_blocking`] in every mode but Turmoil's, where the job runs as one task of the simulated
//! host's scheduler, so its synchronous body is a single scheduling step of the simulation instead
//! of a thread outside it. `just validate-primitive-boundary` rejects it outside the executor's
//! worker pools, so it gives no caller a way around admission either.

#[cfg(feature = "shuttle")]
pub use shuttle_tokio::task::spawn_blocking as spawn_cpu;
#[cfg(feature = "shuttle")]
pub use shuttle_tokio::task::{
    AbortHandle, JoinError, JoinHandle, JoinSet, consume_budget, spawn, spawn_blocking, yield_now,
};
#[cfg(feature = "shuttle")]
pub use shuttle_tokio_util::task::TaskTracker;
#[cfg(not(any(feature = "shuttle", feature = "turmoil")))]
pub use tokio::task::spawn_blocking as spawn_cpu;
#[cfg(not(feature = "shuttle"))]
pub use tokio::task::{
    AbortHandle, JoinError, JoinHandle, JoinSet, block_in_place, consume_budget, spawn,
    spawn_blocking, yield_now,
};
#[cfg(not(feature = "shuttle"))]
pub use tokio_util::task::{AbortOnDropHandle, TaskTracker};

/// A task handle that aborts its task when dropped.
///
/// Tokio Util's handle wraps Tokio's own join handle, so a Shuttle build supplies its own over
/// Shuttle's.
#[cfg(feature = "shuttle")]
#[must_use = "dropping the handle aborts the task immediately"]
pub struct AbortOnDropHandle<T>(JoinHandle<T>);

#[cfg(feature = "shuttle")]
impl<T> AbortOnDropHandle<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self(handle)
    }

    /// End the task now, while keeping the handle to await its cancellation.
    pub fn abort(&self) {
        self.0.abort();
    }
}

#[cfg(feature = "shuttle")]
impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(feature = "shuttle")]
impl<T> std::future::Future for AbortOnDropHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(context)
    }
}

/// Run one CPU job the bounded executor admitted as a task of the simulated host's scheduler.
///
/// The job's synchronous body is one scheduling step of the simulation: Turmoil can order the tasks
/// around it, but nothing interleaves inside it, and it never runs on a thread outside the
/// simulation, whose completion the simulation's clock could not order.
#[cfg(all(feature = "turmoil", not(feature = "shuttle")))]
pub fn spawn_cpu<F, R>(job: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    spawn(async move { job() })
}
