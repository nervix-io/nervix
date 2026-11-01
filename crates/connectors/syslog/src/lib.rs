//! SyslogListenerIngestor, SyslogCollectorEmitter connector entities.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Syslog TLS loading, UDP/TCP/TLS listener and sender transports, RFC 6587 framing,
//!   peer metadata, ingestor lifecycle operations, and record publication.
//! - **Depends on.** The connector contract, typed client configuration entries,
//!   execution-sensitive sockets, `memchr`, and rustls.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or Plexus.

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

pub mod collector;
mod config;
pub mod listener;

pub use collector::{SyslogCollectorEmitter, SyslogCollectorEmitterConfig};
pub use config::SyslogConfigError;
#[cfg(feature = "testing")]
pub use listener::SyslogListenerFault;
#[cfg(feature = "benchmarks")]
pub use listener::{SyslogFrameError, frame_stream};
pub use listener::{
    SyslogListenerIngestor, SyslogListenerIngestorMessage, SyslogListenerIngestorPlan,
};
