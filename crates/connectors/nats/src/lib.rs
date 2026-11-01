//! NatsSubjectIngestor, NatsSubjectEmitter and NatsStreamIngestor connector entities.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The public subject and stream connector implementation surface.
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

pub mod stream;
pub mod subject;
pub use stream::*;
pub use subject::*;

mod offline_config;
pub use offline_config::validate_client_configuration;
