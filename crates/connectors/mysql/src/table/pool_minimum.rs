//! The declared minimum of one node's MySQL pool, counted in established connections.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The task that keeps one MySQL pool at `POOL SIZE MIN` established connections, and
//!   the pass that brings the pool back to that minimum.
//! - **Depends on.** `error-stack`, and the primitives boundary's timers and tasks. The pool it
//!   keeps is behind [`KeptPool`].
//! - **Must not know.** Emitters and their inserts, the host's lease, how a connection is opened,
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

use std::{future::Future, time::Duration};

use error_stack::Report;
use nervix_primitives::task::{AbortOnDropHandle, consume_budget, spawn};
use nervix_recovery::Reported as _;
use thiserror::Error;

/// Why the keeper could not bring its pool back to the declared minimum.
#[derive(Debug, Error)]
pub(super) enum PoolMinimumError {
    /// The pool could not hand over a connection; the driver's cause is beneath.
    #[error("checking a connection out of the MySQL pool failed")]
    CheckOut,
    /// A pass ended below the minimum because one of its checkouts failed, which is beneath.
    #[error("restoring the MySQL pool to its minimum of {minimum} connections failed")]
    NotRestored { minimum: usize },
}

/// What the keeper reads from the pool it keeps, and asks of it.
pub(super) trait KeptPool: Send + Sync + 'static {
    /// A connection checked out of the pool, which returns to it when dropped.
    type Checkout: Send;

    /// The connections the pool has established now, idle and checked out together.
    fn established(&self) -> usize;

    /// Check one connection out: one the pool holds idle when it has one, otherwise a new one.
    fn check_out(
        &self,
    ) -> impl Future<Output = Result<Self::Checkout, Report<PoolMinimumError>>> + Send;
}

/// Check the pool's shortfall from `minimum` out of it together, and return the checkouts.
///
/// The checkouts are held until the pass ends, so the pool cannot hand the same connection to the
/// next one.
pub(super) async fn restore_minimum<P: KeptPool>(
    pool: &P,
    minimum: usize,
) -> Result<Vec<P::Checkout>, Report<PoolMinimumError>> {
    let Some(shortfall) = minimum.checked_sub(pool.established()) else {
        return Ok(Vec::new());
    };
    let mut held = Vec::with_capacity(shortfall);
    for _ in 0..shortfall {
        consume_budget().await;
        match pool.check_out().await {
            Ok(checkout) => held.push(checkout),
            Err(report) => {
                return Err(report.change_context(PoolMinimumError::NotRestored { minimum }));
            }
        }
    }
    Ok(held)
}

/// The task that keeps one pool at its declared minimum, stopped when this handle is dropped, which
/// stops it with the pool it keeps rather than leaving it reconnecting to a database nothing is
/// writing to.
pub(super) struct PoolMinimum {
    _keeper: AbortOnDropHandle<()>,
}

impl PoolMinimum {
    /// Restore `pool` to `minimum` established connections after every `interval`.
    ///
    /// The driver keeps its minimum only as a retention floor: it keeps that many idle connections
    /// once they have been returned, but never opens one. A client whose emitters are idle would
    /// therefore sit below its declared minimum indefinitely, so each pass opens the missing ones
    /// and releases them straight back to the pool.
    pub(super) fn keep<P: KeptPool>(pool: P, minimum: usize, interval: Duration) -> Self {
        let keeper = spawn(async move {
            loop {
                consume_budget().await;
                nervix_primitives::time::sleep(interval).await;
                // Reporting the pass releases the checkouts it held, which it held only so that each
                // reached a distinct connection. A failed pass keeps its diagnostic before the next
                // one tries again; a writer still reports the result of its own checkout separately.
                restore_minimum(&pool, minimum)
                    .await
                    .reported("maintain MySQL connection pool");
            }
        });
        Self {
            _keeper: AbortOnDropHandle::new(keeper),
        }
    }
}
