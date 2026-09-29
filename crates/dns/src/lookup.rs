//! What a failed lookup means.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The closed set of lookup failures, their classification from Hickory's errors, and
//!   finding a failed lookup among the causes of a client library's error.
//! - **Depends on.** Hickory's error types.
//! - **Must not know.** Whether a caller retries, or what it would have connected to.

use std::{error::Error, sync::Arc};

use hickory_resolver::net::{DnsError, NetError};
use strum::AsRefStr;
use thiserror::Error;

/// Why a host did not resolve to an address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum DnsLookupFailure {
    /// The name servers answered that the name does not exist.
    #[error("the name does not exist")]
    NameNotFound,
    /// The name exists but has neither an IPv4 nor an IPv6 address.
    #[error("the name has no IPv4 or IPv6 address")]
    NoAddresses,
    /// No answer arrived within the lookup's budget or the name servers' own timeouts.
    #[error("no answer arrived in time")]
    Timeout,
    /// A name server answered the query with an error, such as a server failure or a refusal.
    #[error("a name server refused or failed the query")]
    Refused,
    /// No name server could be asked, or none answered with a usable response.
    #[error("no name server could be reached")]
    Unreachable,
    /// The host is neither an IP address nor a valid DNS name.
    #[error("the host is not a valid DNS name")]
    InvalidName,
}

impl DnsLookupFailure {
    /// The failure `error` reports. With a search list, Hickory reports the last name it tried.
    pub(crate) fn of(error: &NetError) -> Self {
        match error {
            NetError::Dns(DnsError::NoRecordsFound(_)) if error.is_nx_domain() => {
                Self::NameNotFound
            }
            NetError::Dns(DnsError::NoRecordsFound(_)) => Self::NoAddresses,
            NetError::Dns(_) => Self::Refused,
            NetError::Timeout => Self::Timeout,
            _ => Self::Unreachable,
        }
    }
}

/// A host that did not resolve, and why.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("resolving '{name}' failed: {failure}")]
pub struct DnsLookupError {
    name: String,
    failure: DnsLookupFailure,
}

impl DnsLookupError {
    pub fn new(name: impl Into<String>, failure: DnsLookupFailure) -> Self {
        Self {
            name: name.into(),
            failure,
        }
    }

    /// The host as the caller wrote it.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn failure(&self) -> DnsLookupFailure {
        self.failure
    }

    /// The failed lookup among `error` and its causes, if resolving a host is what failed.
    ///
    /// A client library that resolved through one of the resolver's DNS hooks keeps the lookup's
    /// own failure among the causes of the connection error it reports, however many errors of its
    /// own it wraps around it.
    pub fn find_in<'a>(error: &'a (dyn Error + 'static)) -> Option<&'a Self> {
        let mut current = Some(error);
        while let Some(cause) = current {
            if let Some(lookup) = cause.downcast_ref::<Self>() {
                return Some(lookup);
            }
            // Redis exposes its stored cause as an `Arc<dyn Error>` rather than the
            // value inside the Arc, so normal `source` traversal stops at the wrapper.
            if let Some(inner) = cause.downcast_ref::<Arc<dyn Error + Send + Sync>>()
                && let Some(lookup) = Self::find_in(inner.as_ref())
            {
                return Some(lookup);
            }
            // `std::io::Error::other` holds its custom error in `get_ref`, but its `source`
            // implementation does not expose that value. Redis's DNS hook crosses this boundary.
            if let Some(inner) = cause
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::get_ref)
                && let Some(lookup) = Self::find_in(inner)
            {
                return Some(lookup);
            }
            current = cause.source();
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lookup_wrapped_by_an_io_error_remains_discoverable() {
        let io = std::io::Error::other(DnsLookupError::new(
            "redis.nervix.test",
            DnsLookupFailure::NameNotFound,
        ));
        let arc: Arc<dyn Error + Send + Sync> = Arc::new(io);
        let lookup = DnsLookupError::find_in(&arc)
            .expect("the typed cause survives Redis's Arc and io::Error wrappers");
        assert_eq!(lookup.name(), "redis.nervix.test");
        assert_eq!(lookup.failure(), DnsLookupFailure::NameNotFound);
    }
}
