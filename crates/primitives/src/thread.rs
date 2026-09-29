//! Threads, selected for the build's execution mode. The `native` capability.
//!
//! A model's participants are threads of that model: Loom and Shuttle can only interleave and
//! reorder the operations of threads they started. In ordinary execution and under Turmoil these
//! are the operating system's threads.

#[cfg(not(any(feature = "loom", feature = "shuttle")))]
pub use std::thread::{JoinHandle, spawn};

#[cfg(feature = "loom")]
pub use loom::thread::{JoinHandle, spawn};
// See the atomic module: one backend stays selected when both modes are enabled by mistake.
#[cfg(all(feature = "shuttle", not(feature = "loom")))]
pub use shuttle::thread::{JoinHandle, spawn};
