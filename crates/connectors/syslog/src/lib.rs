//! The Syslog connector transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Syslog client configuration, UDP/TCP/TLS listener and sender transports, RFC
//!   6587 framing, peer metadata, source lifecycle operations, and record publication.
//! - **Depends on.** The connector contract, typed client configuration entries, Tokio sockets,
//!   `memchr`, and rustls.
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

mod config;
mod sink;
mod source;

pub use config::{
    DEFAULT_MAX_MESSAGE_SIZE, MAX_UDP_PAYLOAD_SIZE, SyslogClientConfig, SyslogConfigError,
    SyslogDeadlineScope, SyslogDirection, SyslogFraming, SyslogProtocol,
};
pub use sink::{SyslogSink, SyslogSinkConfig};
#[cfg(feature = "benchmarks")]
pub use source::{SyslogFrameError, frame_stream};
pub use source::{SyslogSource, SyslogSourceMessage, SyslogSourcePlan};
