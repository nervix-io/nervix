//! The Syslog connector transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Syslog client configuration, UDP/TCP/TLS listener and sender transports, RFC
//!   6587 framing, peer metadata, source lifecycle operations, and record publication.
//! - **Depends on.** The connector contract, typed client configuration entries, Tokio sockets,
//!   and rustls.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

mod config;
mod sink;
mod source;

pub use config::{
    DEFAULT_MAX_MESSAGE_SIZE, MAX_UDP_PAYLOAD_SIZE, SyslogClientConfig, SyslogConfigError,
    SyslogDirection, SyslogFraming, SyslogProtocol,
};
pub use sink::{SyslogSink, SyslogSinkConfig};
pub use source::{SyslogSource, SyslogSourceMessage, SyslogSourcePlan};
