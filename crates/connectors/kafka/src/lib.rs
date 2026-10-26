//! KafkaTopicIngestor and KafkaTopicEmitter connector entities.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Kafka consumer and producer configuration, ingestor polling and offset handling,
//!   consumer and producer retirement off the async workers, record and header publication, native
//!   queue capacity and admission backpressure, delivery-report classification, the records the
//!   producer holds until their delivery reports are served, the delivery bound a confirmed publish
//!   gives the producer, and producer queue shutdown.
//! - **Depends on.** The connector contract and its confirmation window, vocabulary values, the
//!   bounded executor, `error-stack`, Tokio, and `rust-rdkafka`.
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

pub mod topic;
pub use topic::*;
