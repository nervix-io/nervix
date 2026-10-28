//! A RabbitMQ client's `addr`: the AMQP URI that names its broker.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading `addr` into the broker it names, the same way before a client is committed
//!   and for every connection: its scheme, its host as the URL grammar reads it and its port, which
//!   Nervix checks, and the credentials, virtual host and query options Lapin's URI grammar reads.
//! - **Depends on.** The URL grammar, Lapin's AMQP URI grammar and `error-stack`.
//! - **Must not know.** Connections, TLS, queues or Models.
//!
//! # Hosts
//!
//! Lapin's URI grammar takes `localhost` for an address that names no host and for an IPv6 literal
//! host, so the host is read with the URL grammar instead: an address that names none is refused,
//! a literal IPv4 or IPv6 address is kept as written, and a name keeps Lapin's percent-decoded
//! reading. Lapin's grammar also quotes an address it cannot read as a hierarchical URL, which can
//! carry credentials, so such an address is refused before Lapin reads it, and no refusal repeats
//! the address.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::str::FromStr as _;

use error_stack::Report;
use lapin::uri::{AMQPScheme, AMQPUri};
use thiserror::Error;
use url::{Host, Url};

/// Why a RabbitMQ client's `addr` names no broker. No variant repeats the address.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RabbitMqAddressError {
    /// The address is not a URL.
    #[error("the RabbitMQ address is not a URL")]
    NotUrl,
    /// The address's scheme is neither `amqp` nor `amqps`. The scheme is not repeated: an address
    /// written without one, such as `user:password@host`, reads its user name as the scheme.
    #[error("the RabbitMQ address scheme is neither 'amqp' nor 'amqps'")]
    Scheme,
    /// The address names no host, which Lapin would read as `localhost`.
    #[error("the RabbitMQ address names no host")]
    MissingHost,
    /// The address names port 0, which no broker listens on.
    #[error("the RabbitMQ address names port 0")]
    ZeroPort,
    /// Lapin's URI grammar cannot read the address's credentials, virtual host or query options.
    #[error("the RabbitMQ address has credentials, a virtual host or options Lapin cannot read")]
    Driver,
}

/// The broker a RabbitMQ client's `addr` names, as Lapin connects to it. It has no `Debug`, since
/// its URI carries the client's credentials.
pub(crate) struct RabbitMqAddress {
    uri: AMQPUri,
}

impl RabbitMqAddress {
    /// The broker `addr` names, refused unless it is an `amqp` or `amqps` URL that names a host
    /// and, when it names a port, one other than 0, and Lapin's grammar reads the rest of it.
    pub(crate) fn parse(addr: &str) -> Result<Self, Report<RabbitMqAddressError>> {
        let url = Url::parse(addr)
            .map_err(|error| Report::new(RabbitMqAddressError::NotUrl).attach_printable(error))?;
        if url.scheme() != "amqp" && url.scheme() != "amqps" {
            return Err(Report::new(RabbitMqAddressError::Scheme));
        }
        let literal = match url.host() {
            None => return Err(Report::new(RabbitMqAddressError::MissingHost)),
            Some(Host::Domain("")) => return Err(Report::new(RabbitMqAddressError::MissingHost)),
            Some(Host::Domain(_)) => None,
            Some(Host::Ipv4(address)) => Some(address.to_string()),
            Some(Host::Ipv6(address)) => Some(address.to_string()),
        };
        if url.port() == Some(0) {
            return Err(Report::new(RabbitMqAddressError::ZeroPort));
        }
        // The URL has an authority, so Lapin's one failure that quotes the address cannot occur.
        let mut uri = AMQPUri::from_str(addr)
            .map_err(|reason| Report::new(RabbitMqAddressError::Driver).attach_printable(reason))?;
        if let Some(literal) = literal {
            uri.authority.host = literal;
        }
        Ok(Self { uri })
    }

    /// The host the address names, as it is resolved and as an `amqps` broker certificate has to
    /// name it.
    pub(crate) fn host(&self) -> &str {
        &self.uri.authority.host
    }

    /// The port the address names, or its scheme's default.
    pub(crate) fn port(&self) -> u16 {
        self.uri.authority.port
    }

    pub(crate) fn scheme(&self) -> &AMQPScheme {
        &self.uri.scheme
    }

    /// The URI Lapin runs the AMQP handshake for.
    pub(crate) fn uri(&self) -> &AMQPUri {
        &self.uri
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;
