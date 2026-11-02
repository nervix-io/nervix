//! SyslogListenerIngestor, SyslogCollectorEmitter connector entities.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Syslog client configuration, UDP/TCP/TLS listener and sender transports, RFC
//!   6587 framing, peer metadata, ingestor lifecycle operations, and record publication.
//! - **Depends on.** The connector contract, typed client configuration entries,
//!   execution-sensitive sockets, `memchr`, rustls and X.509 client-subject inspection.
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
pub use config::{
    DEFAULT_MAX_MESSAGE_SIZE, SyslogClientConfig, SyslogConfigError, SyslogDeadlineScope,
    SyslogDirection, SyslogFraming, SyslogProtocol,
};
#[cfg(feature = "testing")]
pub use listener::SyslogListenerFault;
#[cfg(feature = "benchmarks")]
pub use listener::{SyslogFrameError, frame_stream};
pub use listener::{
    SyslogListenerIngestor, SyslogListenerIngestorMessage, SyslogListenerIngestorPlan,
};
