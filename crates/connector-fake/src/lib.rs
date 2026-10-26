//! Scripted connectors for tests of Nervix hosts.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Scripted transport answers and complete observations of connector-boundary values.
//! - **Depends on.** The connector contract, vocabulary, Arrow and selected execution primitives.
//! - **Must not know.** Server runtime types, graph planning, registry state or real drivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "scripted fixtures are dev-dependencies outside product execution"
    )
)]

mod emitter;
mod ingestor;

pub use emitter::*;
pub use ingestor::*;

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
mod tests;
