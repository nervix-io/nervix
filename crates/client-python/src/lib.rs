//! The Python package of the Nervix client, `nervix_client`, on pyo3 over the host session runtime.
//!
//! Layer: edges.
//!
//! - **Owns.** The Python classes, enumerations and exceptions a CPython host reads and calls:
//!   sessions with their connection options, prepared commands and their outcomes, completion
//!   pages, subscription events with their frames and columns, domain clock events and followed
//!   clocks, producers, consumers and deliveries of client endpoints, typed batches and batch
//!   builders, the Arrow PyCapsule exchange of batches, and cancellation tokens with deadlines;
//!   releasing the GIL for every wait, and tying each object's lifetime to Python's.
//! - **Depends on.** The host session runtime every binding shares, the Rust client vocabulary it
//!   names, Arrow's C data interface, and pyo3.
//! - **Must not know.** How a session reaches its server, recovers or decides an outcome, which
//!   the Rust client decides; how a failure is classified, which the host session runtime
//!   decides; or the C ABI other hosts load.
//!
//! Every object owns its share of the runtime and releases it when Python collects it, and a
//! borrowed view keeps its owner alive. A blocking call runs on the calling Python thread with the
//! GIL released, and the library never calls Python from a thread of its own.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "a Python host's session, its waits and its objects belong to the client edge"
    )
)]

mod arrow;
mod batch;
mod clock;
mod endpoint;
mod enums;
mod errors;
mod event;
mod schema;
mod session;
mod values;

use pyo3::prelude::*;

/// The `nervix_client` module: every class, enumeration and exception of the package, each module
/// adding its own.
#[pymodule]
fn nervix_client(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::register(module)?;
    enums::register(module)?;
    session::register(module)?;
    schema::register(module)?;
    event::register(module)?;
    clock::register(module)?;
    endpoint::register(module)?;
    batch::register(module)?;
    values::register(module)?;
    Ok(())
}

#[cfg(test)]
mod tests;
