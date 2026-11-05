//! NATS JetStream ingestor connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The durable pull consumer a NATS STREAM ingestor instance reads, the pull requests
//!   it sends and the deliveries they bring, and the acknowledgement and negative acknowledgement
//!   of each delivery.
//! - **Depends on.** The connector contract, vocabulary values, the crate's NATS client
//!   connection, `error-stack`, the primitive boundary and `async-nats`.
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

mod ingestor;
mod pull;

pub use ingestor::{
    NatsPullRefusal, NatsStreamIngestor, NatsStreamIngestorError, NatsStreamIngestorMessage,
    NatsStreamIngestorPlan, NatsStreamIngestorPosition,
};
