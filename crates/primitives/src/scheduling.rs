//! The scheduling points the Shuttle adapters of this crate take.
//!
//! Shuttle interleaves tasks only where an operation lets it choose which task runs next. Its own
//! modeled atomics and locks are such points; a primitive built on real synchronization, or a
//! registration kept under a real lock, is not, and a race that lives inside it is unreachable. An
//! adapter takes a point where the scheduler has to be able to run another task: immediately
//! before an operation that registers, publishes or reads a decision, and, around an opaque
//! operation, immediately after it too.

use std::time::Duration;

/// Let the scheduler choose which task runs next, without asking it to deprioritize this one.
///
/// Shuttle does not model time: its thread sleep is exactly one scheduling point, the same one each
/// modeled atomic operation takes before it acts.
pub(crate) fn point() {
    shuttle::thread::sleep(Duration::ZERO);
}

/// Run one opaque operation between two scheduling points, so the scheduler can run another task
/// just before it and just after it. What the operation does inside stays unmodeled.
pub(crate) fn around<R>(operation: impl FnOnce() -> R) -> R {
    point();
    let result = operation();
    point();
    result
}
