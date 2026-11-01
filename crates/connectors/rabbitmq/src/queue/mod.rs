//! RabbitMQ queue entities: the queue a source consumes and the queue an emitter publishes to.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The queue ingestor: the consumer and prefetch window it reads through, the AMQP
//!   headers it reads and per-delivery acknowledgement and requeue; and the queue emitter, which
//!   publishes through the shared publisher to the default exchange.
//! - **Depends on.** The connector contract, vocabulary values, the node resolver, the crate's
//!   connection and publisher, `error-stack`, Tokio and `lapin`.
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

mod emitter;
mod ingestor;

pub use emitter::{RabbitMqQueueEmitter, RabbitMqQueueEmitterConfig};
pub use ingestor::{
    RabbitMqConsumerMode, RabbitMqDeliveryHeaders, RabbitMqQueueIngestor,
    RabbitMqQueueIngestorError, RabbitMqQueueIngestorMessage, RabbitMqQueueIngestorPlan,
    RabbitMqQueueIngestorPosition,
};
