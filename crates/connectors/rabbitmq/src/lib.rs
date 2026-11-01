//! RabbitMqQueueIngestor, RabbitMqQueueEmitter and RabbitMqExchangeEmitter connector entities.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The public queue connector implementation surface.
//! - **Depends on.** The connector contract, vocabulary values and its own driver stack.
//! - **Must not know.** Graph routing, registry state, placement or other connector engines.

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

pub mod exchange;
pub mod queue;
pub use exchange::*;
pub use queue::*;

mod address;
mod channel_closure;
mod connection;
mod offline_config;
mod publisher;
pub use connection::RabbitMqConnectError;
pub use offline_config::validate_client_configuration;
