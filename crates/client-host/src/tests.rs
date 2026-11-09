//! Tests of the representations every binding exchanges with the host session runtime: batches
//! through the Arrow C data interface, and field descriptions through the typed field list.
//!
//! Layer: test harness.
//! - **Owns.** Generators of bounded host columns and field descriptions, and the complete
//!   equality of what the runtime exports and takes back.
//! - **Depends on.** The runtime's batches, builders, schemas and field lists, Arrow's C data
//!   interface, and the vocabulary's test generators.
//! - **Must not know.** Any binding or host language, sessions, or what a server does with a
//!   batch.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

mod arrow;
mod fields;
mod inspection;
