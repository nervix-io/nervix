//! A paced sensor simulation that produces into and consumes from a Nervix graph through the Rust
//! client library.
//!
//! Layer: edges.
//!
//! - **Owns.** The simulation application: its command line, the readings it plans at each tick
//!   center the domain clock reaches, its application-owned input ledger and idempotent effect
//!   store, the producer, consumer, clock and inspection loops it runs on one session, and the
//!   report it prints.
//! - **Depends on.** The Rust client library, Arrow arrays, and the vocabulary's names, schema
//!   fields, clock observations and timestamps.
//! - **Must not know.** The server. Everything it learns arrives over its session.
//!
//! The program is the Rust half of the runnable paced simulation under `examples/paced-simulation`;
//! the Python driver beside it does the same through the shared C binding, and both print the same
//! report and write the same files. `examples/paced-simulation/README.md` describes how to run it.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "an example client application owns its session, its loops and its files"
    )
)]

mod clock;
mod consumers;
mod effects;
#[cfg(test)]
mod file_records_tests;
mod ledger;
mod options;
mod readings;
mod refusal;
mod report;
mod simulation;
#[cfg(test)]
mod vocabulary_tests;

pub use options::Options;
pub use simulation::{Finish, run};
