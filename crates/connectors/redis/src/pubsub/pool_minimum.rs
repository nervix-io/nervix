//! The declared minimum of one node's Redis command pool, counted in established connections.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The task that keeps one command pool at `POOL SIZE MIN` established connections,
//!   counting the ones publishers have checked out with the idle ones, the signal each pooled
//!   connection gives that task when it closes, and what the task asks its pool for to restore the
//!   minimum.
//! - **Depends on.** `error-stack`, and the primitives boundary's notification and tasks. The pool
//!   it keeps is behind [`KeptPool`].
//! - **Must not know.** Emitters and their publishes, the host's lease, how a connection is opened,
//!   or the driver's pool internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(
        plans,
        reason = "receives the pool it keeps and the minimum the client's plan declares"
    )
)]

#[cfg(all(test, feature = "shuttle"))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod shuttle_tests;
#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;

use std::{fmt, future::Future, num::NonZeroU32, pin::pin};

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_primitives::{
    sync::{Arc, Notify},
    task::{AbortOnDropHandle, consume_budget, spawn},
};
use nervix_recovery::Reported as _;
use thiserror::Error;

/// Tells the keeper of one pool's minimum that a connection of that pool closed.
///
/// Every connection the pool opens holds one and signals it when it closes: when the pool retires
/// it at a lifetime, drops it after a failed check, or closes with it.
#[derive(Clone, Debug, Default)]
pub(super) struct ConnectionClosures {
    notify: Arc<Notify>,
}

impl ConnectionClosures {
    /// Record that one connection closed, so the keeper reads its pool again.
    pub(super) fn closed(&self) {
        self.notify.notify_one();
    }
}

/// The connections one reading of a pool counts as established, idle and checked out together.
/// A connection the pool is still opening is not counted yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct EstablishedConnections {
    pub(super) total: u32,
    /// The ones waiting in the pool for a checkout, which are counted in `total`.
    pub(super) idle: u32,
}

impl EstablishedConnections {
    /// What restoring the pool to `minimum` takes, or nothing while it holds that many.
    pub(super) fn restoration(self, minimum: NonZeroU32) -> Option<Restoration> {
        if self.total >= minimum.get() {
            return None;
        }
        let in_use = self
            .total
            .checked_sub(self.idle)
            .assured("a pool counts its idle connections among the ones it holds");
        let missing = minimum
            .get()
            .checked_sub(in_use)
            .verified("the connections in use are within the total, which is below the minimum");
        let checkouts = NonZeroU32::new(missing)
            .verified("the connections in use are within the total, which is below the minimum");
        Some(Restoration { minimum, checkouts })
    }
}

/// Bringing a pool back to its minimum: hold `checkouts` connections checked out at once. They take
/// every idle connection, so the pool hands over none it already holds, and the pool opens one for
/// each checkout it has no idle connection for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Restoration {
    pub(super) minimum: NonZeroU32,
    pub(super) checkouts: NonZeroU32,
}

/// Why the keeper could not bring its pool back to its declared minimum.
#[derive(Debug, Error)]
pub(super) enum PoolMinimumError {
    /// One of the checkouts did not finish; the cause beneath is the deadline it ran out of or the
    /// connection that failed.
    #[error("restoring the Redis command pool to its minimum of {minimum} connections failed")]
    NotRestored { minimum: NonZeroU32 },
}

/// What the keeper reads from the pool it keeps, and asks of it.
pub(super) trait KeptPool: Send + Sync + 'static {
    /// The pool's established connections now.
    fn established(&self) -> EstablishedConnections;

    /// Hold `restoration`'s checkouts at once and return them together, within the pool's checkout
    /// deadline.
    fn restore(
        &self,
        restoration: Restoration,
    ) -> impl Future<Output = Result<(), Report<PoolMinimumError>>> + Send;
}

/// The task that keeps one pool at its declared minimum, stopped when this handle is dropped.
pub(super) struct PoolMinimum {
    _keeper: AbortOnDropHandle<()>,
}

impl fmt::Debug for PoolMinimum {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PoolMinimum")
            .finish_non_exhaustive()
    }
}

impl PoolMinimum {
    /// Keep `pool` at `minimum` established connections, reading it again whenever `closures`
    /// says one of its connections closed.
    pub(super) fn keep(
        pool: impl KeptPool,
        minimum: NonZeroU32,
        closures: ConnectionClosures,
    ) -> Self {
        Self {
            _keeper: AbortOnDropHandle::new(spawn(Self::run(pool, minimum, closures))),
        }
    }

    async fn run(pool: impl KeptPool, minimum: NonZeroU32, closures: ConnectionClosures) {
        loop {
            consume_budget().await;
            // Registered before the pool is read, so a connection that closes after the reading
            // wakes the wait below instead of leaving the pool short until the next closure.
            let mut closed = pin!(closures.notify.notified());
            closed.as_mut().enable();
            let established = pool.established();
            let Some(restoration) = established.restoration(minimum) else {
                closed.await;
                continue;
            };
            // A failed restoration is retried at once: each of its checkouts already waited out
            // the pool's checkout deadline, which paces the next attempt.
            pool.restore(restoration)
                .await
                .reported("keep the Redis command pool at its declared minimum");
        }
    }
}
