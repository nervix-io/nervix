//! Scripted connectors for tests of Nervix hosts.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Scripted transport answers, scripted host services behind both host handles, and
//!   complete observations of connector-boundary values.
//! - **Depends on.** The connector contract, vocabulary, Arrow, the node resolver's type and
//!   selected execution primitives.
//! - **Must not know.** Server runtime types, graph planning, registry state or real drivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "scripted fixtures are dev-dependencies outside product execution"
    )
)]

mod emitter;
mod emitter_host;
mod ingestor;
mod ingestor_host;
mod pooled_client;

pub use emitter::*;
pub use emitter_host::*;
pub use ingestor::*;
pub use ingestor_host::*;
pub use pooled_client::*;

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
mod tests;

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
mod host_tests;
