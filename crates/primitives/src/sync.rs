//! Synchronization, selected for the build's execution mode.
//!
//! The items at this level synchronize async tasks: they suspend a task instead of blocking the
//! thread that runs it. The locks, notification, semaphores, one-time cells and channels are
//! Tokio's, and cancellation tokens are Tokio Util's. Each channel module carries its whole endpoint
//! family, with its permits and errors, so a producer and its consumer always come from the same
//! backend. [`atomic`] is the portable atomic family, and `blocking` holds the primitives that block
//! the calling thread.
//!
//! A Shuttle build takes Shuttle's modeled Tokio for the locks, semaphores, one-time cells and the
//! `mpsc`, `oneshot` and `broadcast` channels. `Notify` and `watch` are this crate's own, with
//! Tokio's semantics and a scheduling point before every waiter registration, because Shuttle's
//! keep their registrations out of the scheduler's sight. The cancellation token wraps Shuttle's
//! to give it Tokio Util's clone identity and owned operations. Loom models none of these: a Loom
//! build takes the ordinary libraries, outside every model, and its model code may not name them.

pub mod atomic;
#[cfg(feature = "native")]
pub mod blocking;

#[cfg(all(feature = "native", not(feature = "shuttle")))]
pub use tokio::sync::{
    AcquireError, Mutex, MutexGuard, Notify, OnceCell, OwnedMutexGuard, OwnedSemaphorePermit,
    Semaphore, SemaphorePermit, TryAcquireError, broadcast, futures, mpsc, oneshot, watch,
};
#[cfg(all(feature = "native", not(feature = "shuttle")))]
pub use tokio_util::sync::{CancellationToken, DropGuard, WaitForCancellationFutureOwned};

#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
mod cancellation;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
mod notify;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub mod watch;

#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub use cancellation::CancellationToken;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub use notify::Notify;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub use shuttle_tokio::sync::{
    AcquireError, Mutex, MutexGuard, OnceCell, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore,
    SemaphorePermit, TryAcquireError, broadcast, mpsc, oneshot,
};
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub use shuttle_tokio_util::sync::{DropGuard, WaitForCancellationFutureOwned};

/// The futures of the synchronization primitives above.
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub mod futures {
    pub use super::notify::Notified;
}
