//! Synchronization that blocks the calling thread, selected for the build's execution mode.
//!
//! The locks and the condition variable have `parking_lot`'s interface: a lock is never poisoned,
//! so acquiring one returns its guard. None of these may be held or waited on across an `.await`;
//! an async task that must wait uses the primitives of the parent module instead.

#[cfg(not(feature = "shuttle"))]
pub use parking_lot::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "shuttle")]
pub use shuttle_parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
