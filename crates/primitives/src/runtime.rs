//! The async runtime, selected for the build's execution mode: building one, and a handle to the
//! runtime a task runs on.
//!
//! A test or a binary builds its runtime through `#[nervix_primitives::test]` or
//! `#[nervix_primitives::main]`, which always construct this runtime; code that builds one by hand
//! uses [`Builder`].

#[cfg(feature = "shuttle")]
pub use shuttle_tokio::runtime::{Builder, Handle, Runtime};
#[cfg(not(feature = "shuttle"))]
pub use tokio::runtime::{Builder, Handle, Runtime};
