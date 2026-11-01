//! A `host:port` client address.
//!
//! Layer: vocabulary.
//! - **Owns.** The shape of an address that names a host and a port and nothing else, and whether
//!   its host is a literal IP address.
//! - **Depends on.** The URL parser.
//! - **Must not know.** Name resolution, sockets or what a connector does with an address.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr};

use error_stack::{Report, ResultExt as _};
use thiserror::Error;
use url::{Host, Url};

/// The scheme that lets the URL parser read a `host:port` address. A non-special scheme keeps a
/// host's spelling and applies no default port.
const ADDRESS_SCHEME: &str = "address";

/// Why a value is not a `host:port` address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum HostPortError {
    #[error("the address does not parse")]
    Parse,
    #[error("the address holds more than a host and a port")]
    Shape,
    #[error("the address names no host")]
    MissingHost,
    #[error("the address names no port")]
    MissingPort,
}

/// The host a [`HostPort`] names.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AddressHost {
    Name(String),
    Literal(IpAddr),
}

impl AddressHost {
    /// A host the URL parser left as written, which it does for every host of a non-special
    /// scheme but a bracketed IPv6 address: an IPv4 address in dotted decimal is a literal, and
    /// anything else is a name.
    fn of_written(host: &str) -> Self {
        match Self::ipv4_literal(host) {
            Ok(address) => Self::Literal(IpAddr::V4(address)),
            Err(_) => Self::Name(host.to_string()),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "a host that is not an IPv4 address in dotted decimal is a name, which a \
                      resolver reads"
        )
    )]
    fn ipv4_literal(host: &str) -> Result<Ipv4Addr, AddrParseError> {
        host.parse()
    }
}

/// An address that names a host, a DNS name or a literal IP address, and a port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPort {
    host: AddressHost,
    port: u16,
}

impl HostPort {
    pub fn parse(address: &str) -> Result<Self, Report<HostPortError>> {
        let parsed = Url::parse(&format!("{ADDRESS_SCHEME}://{address}"))
            .change_context(HostPortError::Parse)?;
        if !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !parsed.path().is_empty()
        {
            return Err(Report::new(HostPortError::Shape));
        }
        let host = match parsed.host() {
            Some(Host::Domain(host)) => AddressHost::of_written(host),
            Some(Host::Ipv4(address)) => AddressHost::Literal(IpAddr::V4(address)),
            Some(Host::Ipv6(address)) => AddressHost::Literal(IpAddr::V6(address)),
            None => return Err(Report::new(HostPortError::MissingHost)),
        };
        let Some(port) = parsed.port() else {
            return Err(Report::new(HostPortError::MissingPort));
        };
        Ok(Self { host, port })
    }

    /// The host as a resolver or a TLS server name reads it: a name as written, and a literal IP
    /// address without the brackets an IPv6 address needs beside a port.
    pub fn host(&self) -> String {
        match &self.host {
            AddressHost::Name(name) => name.clone(),
            AddressHost::Literal(address) => address.to_string(),
        }
    }

    pub const fn port(&self) -> u16 {
        self.port
    }

    /// The socket address this names, when its host is a literal IP address that needs no
    /// resolver to read.
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        match self.host {
            AddressHost::Name(_) => None,
            AddressHost::Literal(address) => Some(SocketAddr::new(address, self.port)),
        }
    }
}

#[cfg(test)]
#[path = "host_port_tests.rs"]
mod tests;
