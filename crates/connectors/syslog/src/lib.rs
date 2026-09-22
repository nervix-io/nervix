//! The Syslog connector transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Syslog client configuration, UDP/TCP/TLS listener transport, RFC 6587 framing,
//!   peer metadata, and source lifecycle operations.
//! - **Depends on.** The connector contract, typed client configuration entries, Tokio sockets,
//!   and rustls.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

mod config;
mod source;

pub use config::{
    DEFAULT_MAX_MESSAGE_SIZE, MAX_UDP_PAYLOAD_SIZE, SyslogClientConfig, SyslogConfigError,
    SyslogDirection, SyslogFraming, SyslogProtocol,
};
pub use source::{SyslogSource, SyslogSourceMessage, SyslogSourcePlan};
