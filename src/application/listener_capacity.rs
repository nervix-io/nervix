//! A refusal at a declared listening boundary.
//!
//! Layer: edges.
//! - **Owns.** Stable typed reasons for listener input refusals and connection expiry.
//! - **Depends on.** HTTP response statuses.
//! - **Must not know.** Routes, graph execution or admission implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns listener refusal outcomes")
)]

use std::io;

use error_stack::Report;
use hyper::StatusCode;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(super) enum ConnectionExpiry {
    #[error("listener idle timeout")]
    Idle,
    #[error("gRPC TLS handshake timeout")]
    TlsHandshake,
    #[error("the listener idle deadline cannot be represented by its clock")]
    Unrepresentable,
}
impl ConnectionExpiry {
    pub(super) fn into_io_error(self) -> io::Error {
        let kind = match self {
            Self::Idle | Self::TlsHandshake => io::ErrorKind::TimedOut,
            Self::Unrepresentable => io::ErrorKind::InvalidInput,
        };
        io::Error::new(kind, ConnectionExpiryReport(Report::new(self)))
    }
}

#[derive(Debug, Error)]
#[error("{0:#}")]
pub(super) struct ConnectionExpiryReport(pub(super) Report<ConnectionExpiry>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(super) enum InputRefusal {
    #[error("listener_connection_capacity")]
    ConnectionCapacity,
    #[error("http_body_capacity")]
    BodyCapacity,
    #[error("http_body_timeout")]
    BodyTimeout,
    #[error("http_input_memory_capacity")]
    MemoryCapacity,
    #[error("http_input_worker_capacity")]
    WorkerCapacity,
    #[error("http_body_invalid")]
    InvalidBody,
}
impl InputRefusal {
    pub(super) fn status(self) -> StatusCode {
        match self {
            Self::BodyCapacity => StatusCode::PAYLOAD_TOO_LARGE,
            Self::BodyTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::ConnectionCapacity | Self::MemoryCapacity | Self::WorkerCapacity => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::InvalidBody => StatusCode::BAD_REQUEST,
        }
    }
}
