//! Synchronization that blocks the calling thread, selected for the build's execution mode.
//!
//! The locks and the condition variable have `parking_lot`'s interface: a lock is never poisoned,
//! so acquiring one returns its guard. Barriers, one-time initialization and the synchronous channel
//! have the standard library's. None of these may be held or waited on across an `.await`; an async
//! task that must wait uses the primitives of the parent module instead.
//!
//! A `deloxide` build takes the locks and the condition variable as adapters over Deloxide's tracked
//! locks, with the same interface where Deloxide keeps its meaning, so the deadlock detector sees
//! every acquisition that waits; it keeps the standard library's barriers, one-time initialization
//! and channel, which it does not track. Its tracked locks hold sized values, are constructed at run
//! time and have no timed, upgradable, mapped or reentrant acquisition; its condition variable has
//! `wait` and `notify_all`. Code that uses another operation fails to compile in that build.
//!
//! A Shuttle build models the locks, the condition variable, barriers, `Once` and the channel.
//! `OnceLock` stays the standard library's, with a scheduling point on each side of each read and
//! write, and without its initializing read. `LazyLock` is not available under Shuttle: a lazily
//! initialized static outlives every model execution, so it cannot be one execution's state. A
//! process-wide value initialized once, such as a detected CPU level, is a real primitive outside
//! every model and comes from the unmodeled path.

#[cfg(not(feature = "shuttle"))]
pub use std::sync::{Barrier, LazyLock, Once, OnceLock, mpsc};

#[cfg(not(any(feature = "shuttle", feature = "deloxide")))]
pub use parking_lot::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(feature = "shuttle")]
mod condvar;
#[cfg(feature = "shuttle")]
mod once_lock;
#[cfg(all(feature = "deloxide", not(feature = "shuttle")))]
pub(crate) mod tracked;

#[cfg(feature = "shuttle")]
pub use condvar::Condvar;
#[cfg(feature = "shuttle")]
pub use once_lock::OnceLock;
#[cfg(feature = "shuttle")]
pub use shuttle::sync::{Barrier, Once, mpsc};
#[cfg(feature = "shuttle")]
pub use shuttle_parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(all(feature = "deloxide", not(feature = "shuttle")))]
pub use tracked::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
