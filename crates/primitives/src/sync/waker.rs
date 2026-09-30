//! The waker registration of one waiting task, with the `futures` crate's semantics and a
//! scheduling point around every operation.
//!
//! An atomic waker holds the waker of the one task that waits for something, and the task that
//! changes that thing wakes it. Its registration is lock-free and opaque to Shuttle: without a
//! scheduling point between a waiter's read of the state and its registration, no schedule could
//! place the change and its wake in that window, and a waiter that reads before it registers would
//! look correct in a check while it loses the wake in production. This adapter runs each
//! registration, wake and take between two scheduling points, so a check reaches that window and
//! orders the registration against the wake. What the registration does inside stays unmodeled.

use std::task::Waker;

use crate::scheduling;

/// The waker of one waiting task, which the task that changes what it waits for wakes.
#[derive(Debug, Default)]
pub struct AtomicWaker {
    inner: futures_util::task::AtomicWaker,
}

impl AtomicWaker {
    pub const fn new() -> Self {
        Self {
            inner: futures_util::task::AtomicWaker::new(),
        }
    }

    /// Register `waker`, replacing any waker registered before it.
    pub fn register(&self, waker: &Waker) {
        scheduling::around(|| self.inner.register(waker));
    }

    /// Wake the registered waker, if there is one, and clear the registration.
    pub fn wake(&self) {
        scheduling::around(|| self.inner.wake());
    }

    /// Take the registered waker, if there is one, without waking it.
    pub fn take(&self) -> Option<Waker> {
        scheduling::around(|| self.inner.take())
    }
}
