//! ZeroMqPullIngestor, ZeroMqPushEmitter connector entities.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** ZeroMQ push- and pull-socket configuration, whether a socket binds or
//!   connects, a PUSH socket's connection within its client's `connect_timeout_ms` and each send
//!   within its `timeout_ms`, reading the first frame of each received message, and per-record
//!   publication.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio, and
//!   `zeromq`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

pub mod pull;
pub mod push;
pub use pull::{
    ZeroMqPullIngestor, ZeroMqPullIngestorError, ZeroMqPullIngestorMessage, ZeroMqPullIngestorPlan,
};
pub use push::{ZeroMqPushEmitter, ZeroMqPushEmitterConfig};
