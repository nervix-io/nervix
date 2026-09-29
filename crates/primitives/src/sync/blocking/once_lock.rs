//! A cell written once, for a Shuttle build.
//!
//! Shuttle has no modeled `OnceLock`. This one keeps the standard library's and takes a scheduling
//! point immediately before and after each read and write, so a check can order an owner's
//! check-then-set against the other tasks that race it. The cell's own synchronization stays
//! opaque. It offers no initializing read: an initializer that took a scheduling point would leave
//! a racing task blocked inside the standard library's cell, where no scheduler can run it, so a
//! value is computed first and then offered with [`OnceLock::set`].

use crate::scheduling;

/// A value set at most once.
#[derive(Debug, Default)]
pub struct OnceLock<T> {
    inner: std::sync::OnceLock<T>,
}

impl<T> OnceLock<T> {
    pub const fn new() -> Self {
        Self {
            inner: std::sync::OnceLock::new(),
        }
    }

    /// The value, once one was set.
    pub fn get(&self) -> Option<&T> {
        scheduling::around(|| self.inner.get())
    }

    /// Set the value, or return `value` when one was already set.
    pub fn set(&self, value: T) -> Result<(), T> {
        scheduling::around(|| self.inner.set(value))
    }
}
