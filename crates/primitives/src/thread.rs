//! Threads, selected for the build's execution mode. The `native` capability.
//!
//! A model's participants are threads of that model: Loom and Shuttle can only interleave and
//! reorder the operations of threads they started. In ordinary execution and under Turmoil these
//! are the operating system's threads. [`yield_now`] is how a synchronous spin wait gives the
//! scheduler of the build, the operating system's or a model's, the chance to run what it waits on.
//!
//! Loom provides the threads its models spawn and join, and nothing else of this module: a Loom
//! model has no sleeping, parking with a timeout, scoped or detached threads. Neither model checker
//! models elapsed time, so under Shuttle `sleep` and `park_timeout` are scheduling points that do
//! not wait for their duration. [`available_parallelism`] reports the host in every mode.

/// How many threads the host runs in parallel. A query of the host, the same in every mode.
pub use std::thread::available_parallelism;
#[cfg(not(any(feature = "loom", feature = "shuttle")))]
pub use std::thread::{
    Builder, JoinHandle, current, park, park_timeout, scope, sleep, spawn, yield_now,
};

#[cfg(feature = "loom")]
pub use loom::thread::{JoinHandle, spawn, yield_now};
// See the atomic module: one backend stays selected when both modes are enabled by mistake.
#[cfg(all(feature = "shuttle", not(feature = "loom")))]
pub use shuttle::thread::{
    Builder, JoinHandle, current, park, park_timeout, scope, sleep, spawn, yield_now,
};

/// Start `body` on a thread named `name` that nothing joins, so it runs until it returns or the
/// process exits.
///
/// Shuttle cannot detach a thread: a thread still parked when a model's main thread returns is
/// reported as a deadlock. A Shuttle build therefore runs `body` as a detached Shuttle task, which
/// the model abandons wherever it is parked when its main thread returns, as a process that exits
/// abandons a thread nothing joins.
#[cfg(not(feature = "loom"))]
pub fn spawn_detached<F>(name: &str, body: F) -> std::io::Result<()>
where
    F: FnOnce() + Send + 'static,
{
    #[cfg(not(feature = "shuttle"))]
    let started = Builder::new().name(name.to_string()).spawn(body)?;
    #[cfg(feature = "shuttle")]
    let started = {
        let name = name.to_string();
        shuttle::future::spawn(async move {
            // The name labels the task in Shuttle's traces, as it labels the thread in production.
            shuttle::current::set_name_for_task(shuttle::current::me(), name);
            body();
        })
    };
    drop(started);
    Ok(())
}
