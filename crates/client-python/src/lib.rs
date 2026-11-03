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

/// The `nervix_client` module: every class, enumeration and exception of the package.
#[pymodule]
fn nervix_client(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::register(module)?;
    enums::register(module)?;
    module.add_class::<session::Cancel>()?;
    module.add_class::<session::Session>()?;
    module.add_class::<session::Execution>()?;
    module.add_class::<session::Outcome>()?;
    module.add_class::<session::Diagnostic>()?;
    module.add_class::<session::Backup>()?;
    module.add_class::<session::Restore>()?;
    module.add_class::<session::Suggestions>()?;
    module.add_class::<session::Suggestion>()?;
    module.add_class::<schema::Schema>()?;
    module.add_class::<schema::Field>()?;
    module.add_class::<schema::Level>()?;
    module.add_class::<schema::Fields>()?;
    module.add_class::<event::Event>()?;
    module.add_class::<event::Subscription>()?;
    module.add_class::<event::RestorationFailure>()?;
    module.add_class::<clock::ClockEvent>()?;
    module.add_class::<clock::DomainClock>()?;
    module.add_class::<clock::PacedClock>()?;
    module.add_class::<clock::Tick>()?;
    module.add_class::<endpoint::Producer>()?;
    module.add_class::<endpoint::Consumer>()?;
    module.add_class::<endpoint::Delivery>()?;
    module.add_class::<endpoint::SubmissionOutcome>()?;
    module.add_class::<endpoint::Grant>()?;
    module.add_class::<endpoint::Policy>()?;
    module.add_class::<endpoint::Reopen>()?;
    module.add_class::<endpoint::PendingSubmission>()?;
    module.add_class::<batch::Batch>()?;
    module.add_class::<batch::BatchBuilder>()?;
    module.add_class::<values::Redacted>()?;
    module.add("REDACTED", values::Redacted)?;
    Ok(())
}

#[cfg(test)]
mod tests;
