//! Async tasks, selected for the build's execution mode: spawning and joining them, yielding and
//! the cooperative budget, aborting them, and tracking a group of them.
//!
//! Every async loop whose body performs async work calls [`consume_budget`] once per iteration near
//! the top of its body, so a loop that always has work ready still lets the scheduler run other
//! tasks.
//!
//! [`spawn_blocking`] is the runtime's mechanism, not a policy: variable-size and blocking work is
//! admitted through the bounded executor, which charges and cancels it, and this path gives no
//! caller a way around that owner.

#[cfg(feature = "shuttle")]
pub use shuttle_tokio::task::{
    AbortHandle, JoinError, JoinHandle, JoinSet, consume_budget, spawn, spawn_blocking, yield_now,
};
#[cfg(feature = "shuttle")]
pub use shuttle_tokio_util::task::TaskTracker;
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
