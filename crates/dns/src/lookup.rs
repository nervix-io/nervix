//! What a failed lookup means.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The closed set of lookup failures, their classification from Hickory's errors, the
//!   report a failed lookup reaches a client library's DNS hook in, and finding a failed lookup
//!   among the causes of a client library's error.
//! - **Depends on.** Hickory's error types and `error-stack`.
//! - **Must not know.** Whether a caller retries, or what it would have connected to.

use std::{error::Error, fmt};

use error_stack::Report;
use hickory_resolver::net::{DnsError, NetError};
use nervix_primitives::sync::StdArc;
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
    /// report among the causes of the connection error it reports, however many errors of its own
    /// it wraps around it.
    pub fn find_in<'a>(error: &'a (dyn Error + 'static)) -> Option<&'a Self> {
        let carried = DnsLookupReport::find_in(error)?;
        Some(carried.report().current_context())
    }
}

/// A failed lookup as a client library's DNS hook hands it to the library.
///
/// Every hook's trait takes a standard error, and a [`Report`] is not one. This keeps the lookup's
/// whole report through that boundary, with what the name servers answered and the budget that ran
/// out, rather than a copy of its top context. It shows the lookup's message, its alternate form
/// shows the report's chain, and its `Debug` form shows every frame with its attachments.
///
/// A hook requiring a standard error accepts the complete carrier:
///
/// ```
/// use error_stack::Report;
/// use nervix_dns::{DnsLookupError, DnsLookupReport};
/// fn hook(report: Report<DnsLookupError>) -> impl std::error::Error {
///     DnsLookupReport::from(report)
/// }
/// ```
///
/// A report needs that carrier at the standard-error boundary:
///
/// ```compile_fail
/// use error_stack::Report;
/// use nervix_dns::DnsLookupError;
/// fn hook(report: Report<DnsLookupError>) -> impl std::error::Error {
///     report
/// }
/// ```
#[derive(Debug)]
pub struct DnsLookupReport(Report<DnsLookupError>);

impl DnsLookupReport {
    /// The report of the lookup that failed.
    pub fn report(&self) -> &Report<DnsLookupError> {
        &self.0
    }

    /// The failed lookup's report among `error` and its causes, if resolving a host is what failed.
    pub fn find_in<'a>(error: &'a (dyn Error + 'static)) -> Option<&'a Self> {
        let mut current = Some(error);
        while let Some(cause) = current {
            if let Some(lookup) = cause.downcast_ref::<Self>() {
                return Some(lookup);
            }
            // Redis exposes its stored cause as an `Arc<dyn Error>` rather than the
            // value inside the Arc, so normal `source` traversal stops at the wrapper.
            if let Some(inner) = cause.downcast_ref::<StdArc<dyn Error + Send + Sync>>()
                && let Some(lookup) = Self::find_in(inner.as_ref())
            {
                return Some(lookup);
            }
            // `std::io::Error::other` holds its custom error in `get_ref`, but its `source`
            // implementation does not expose that value. The MQTT and Redis DNS hooks cross this
            // boundary.
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

impl From<Report<DnsLookupError>> for DnsLookupReport {
    fn from(report: Report<DnsLookupError>) -> Self {
        Self(report)
    }
}

impl fmt::Display for DnsLookupReport {
    /// The report's own rendering: the lookup's message, or with `{:#}` the chain beneath it.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

/// The report's frames are not standard errors, so nothing is offered as a source; the lookup is
/// found through [`DnsLookupError::find_in`] instead, and naming it as a source would print its
/// message twice where a library renders a chain.
impl Error for DnsLookupReport {}

#[cfg(test)]
mod tests {
    use error_stack::{AttachmentKind, FrameKind};

    use super::*;

    fn name_not_found() -> Report<DnsLookupError> {
        Report::new(DnsLookupError::new(
            "redis.nervix.test",
            DnsLookupFailure::NameNotFound,
        ))
        .attach_printable("the name servers answered NXDOMAIN")
    }

    #[test]
    fn a_lookup_wrapped_by_an_io_error_remains_discoverable() {
        let io = std::io::Error::other(DnsLookupReport::from(name_not_found()));
        let arc: StdArc<dyn Error + Send + Sync> = StdArc::new(io);
        let lookup = DnsLookupError::find_in(&arc)
            .expect("the typed cause survives Redis's Arc and io::Error wrappers");
        assert_eq!(lookup.name(), "redis.nervix.test");
        assert_eq!(lookup.failure(), DnsLookupFailure::NameNotFound);
    }

    #[test]
    fn the_report_keeps_what_the_lookup_recorded_through_a_hook() {
        let io = std::io::Error::other(DnsLookupReport::from(name_not_found()));

        let carried = DnsLookupReport::find_in(&io).expect("the hook's report is a cause");
        let mut attachments = Vec::new();
        for frame in carried.report().frames() {
            if let FrameKind::Attachment(AttachmentKind::Printable(attachment)) = frame.kind() {
                attachments.push(attachment.to_string());
            }
        }
        assert_eq!(attachments, ["the name servers answered NXDOMAIN"]);
        assert_eq!(
            carried.to_string(),
            "resolving 'redis.nervix.test' failed: the name does not exist"
        );
        assert!(
            format!("{carried:?}").contains("the name servers answered NXDOMAIN"),
            "{carried:?}"
        );
    }
}
