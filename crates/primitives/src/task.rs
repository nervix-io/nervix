//! Async tasks, selected for the build's execution mode: spawning and joining them, yielding and
//! the cooperative budget, aborting them, and tracking a group of them.
//!
//! Every async loop whose body performs async work calls [`consume_budget`] once per iteration near
//! the top of its body, so a loop that always has work ready still lets the scheduler run other
//! tasks.
//!
//! [`unconstrained`] polls a future outside the cooperative budget. Once a task has spent its
//! budget, Tokio's channels and timers read as not ready even when their value has arrived, so an
//! owner that must learn what has already arrived without waiting for anything polls through it:
//! a confirmation window settling every answer its broker gave before a publish failed is one. A
//! future polled this way is never made to yield, so its owner bounds how much it polls. Shuttle
//! has no cooperative budget, and there it only forwards the poll.
//!
//! [`spawn_blocking`] is the runtime's mechanism, not a policy: variable-size and blocking work
//! belongs to the bounded executor, which admits, charges and cancels it and runs its storage jobs
//! here. `just validate-primitive-boundary` rejects it outside the executor's worker pools unless
//! `crates/primitives/blocking-permissions.toml` lists it for the file that names it, with the
//! owner, why that owner stays outside the executor, and what bounds its work instead.
//!
//! `block_in_place` blocks the runtime worker thread that calls it without any of the executor's
//! admission, charges or cancellation, so no file owns it by default: the check rejects it unless a
//! blocking permission lists it for the file that names it, with that owner's reason and bound. A
//! Shuttle build does not provide it.
//!
//! [`spawn_cpu`] runs an admitted CPU worker. Each callback runs one bounded job and says whether
//! another is ready. Ordinary and Shuttle workers drain on the blocking pool; Turmoil workers
//! yield on the simulated scheduler between jobs. The executor owns admission and termination.
//! `just validate-primitive-boundary` confines this mechanism to the executor's worker pools.

#[cfg(feature = "shuttle")]
pub use shuttle_tokio::task::coop::unconstrained;
#[cfg(feature = "shuttle")]
pub use shuttle_tokio::task::{
    AbortHandle, JoinError, JoinHandle, JoinSet, consume_budget, spawn, spawn_blocking, yield_now,
};
#[cfg(feature = "shuttle")]
pub use shuttle_tokio_util::task::TaskTracker;
#[cfg(not(feature = "shuttle"))]
pub use tokio::task::coop::unconstrained;
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

/// Run the bounded executor's CPU worker, calling `run_next` once per admitted job until it
/// returns false. The callback owns the queue's empty-to-idle transition.
///
/// Ordinary and Shuttle builds retain one blocking-pool dispatch for the worker's whole backlog.
/// Turmoil keeps each callback on the simulated host and yields between callbacks, so each bounded
/// job remains a separate simulation step. No worker thread waits for more work after it is idle.
#[cfg(not(feature = "turmoil"))]
pub fn spawn_cpu(mut run_next: impl FnMut() -> bool + Send + 'static) -> JoinHandle<()> {
    spawn_blocking(move || while run_next() {})
}

/// Run an admitted CPU worker on the simulated host, yielding between its bounded jobs.
#[cfg(all(feature = "turmoil", not(feature = "shuttle")))]
pub fn spawn_cpu(mut run_next: impl FnMut() -> bool + Send + 'static) -> JoinHandle<()> {
    spawn(async move {
        loop {
            consume_budget().await;
            if !run_next() {
                break;
            }
            yield_now().await;
        }
    })
}
