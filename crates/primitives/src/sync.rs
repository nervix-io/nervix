//! Synchronization, selected for the build's execution mode.
//!
//! The items at this level synchronize async tasks: they suspend a task instead of blocking the
//! thread that runs it. The locks, notification, semaphores, one-time cells and channels are
//! Tokio's, and cancellation tokens are Tokio Util's. Each channel module carries its whole endpoint
//! family, with its permits and errors, so a producer and its consumer always come from the same
//! backend. [`atomic`] is the portable atomic family, and `blocking` holds the primitives that block
//! the calling thread.

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

#[cfg(all(feature = "native", feature = "shuttle"))]
mod cancellation;
#[cfg(all(feature = "native", feature = "shuttle"))]
pub use cancellation::CancellationToken;
#[cfg(all(feature = "native", feature = "shuttle"))]
pub use shuttle_tokio::sync::{
    AcquireError, Mutex, MutexGuard, Notify, OnceCell, OwnedMutexGuard, OwnedSemaphorePermit,
    Semaphore, SemaphorePermit, TryAcquireError, broadcast, futures, mpsc, oneshot, watch,
};
#[cfg(all(feature = "native", feature = "shuttle"))]
pub use shuttle_tokio_util::sync::{DropGuard, WaitForCancellationFutureOwned};
