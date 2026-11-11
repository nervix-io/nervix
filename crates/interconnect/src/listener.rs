//! Shared interpretation of failed socket accepts and their declared retry pause.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Connection-local accept failures and the positive node-local retry setting.
//! - **Depends on.** I/O error kinds, setting vocabulary and the selected monotonic clock.
//! - **Must not know.** Graphs, listening protocols, leadership or task lifetimes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns socket accept classification and its retry declaration"
    )
)]

use std::{io, time::Duration};

use error_stack::Report;
use nervix_models::{NodeCapabilities, SettingDefinition};
use nervix_primitives::time::Instant;
use thiserror::Error;

/// Whether retrying immediately affects only the connection which failed to arrive.
pub fn is_connection_accept_failure(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::Interrupted
    )
}

/// Why a node cannot enforce its accept retry pause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AcceptRetryDelayError {
    #[error("listener-accept-retry-delay must be greater than zero")]
    Empty,
    #[error("listener-accept-retry-delay does not fit the monotonic clock")]
    Unrepresentable,
}

/// The pause after an accept failed for a reason other than its peer leaving.
pub struct AcceptRetryDelay;

impl SettingDefinition for AcceptRetryDelay {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = AcceptRetryDelayError;
    const NAME: &'static str = "listener-accept-retry-delay";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_millis(100)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        if value.is_zero() {
            return Err(Report::new(AcceptRetryDelayError::Empty));
        }
        if Instant::now().checked_add(*value).is_none() {
            return Err(Report::new(AcceptRetryDelayError::Unrepresentable));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::Setting;

    use super::*;

    #[test]
    fn defaults_and_explicit_pauses_share_the_validation_boundary() {
        assert_eq!(
            *Setting::<AcceptRetryDelay>::resolve(None, &())
                .assured("default pause")
                .value(),
            Duration::from_millis(100)
        );
        assert!(matches!(
            Setting::<AcceptRetryDelay>::new(Duration::ZERO, &())
                .expect_err("zero pause")
                .current_context(),
            AcceptRetryDelayError::Empty
        ));
        assert!(matches!(
            Setting::<AcceptRetryDelay>::new(Duration::MAX, &())
                .expect_err("unrepresentable pause")
                .current_context(),
            AcceptRetryDelayError::Unrepresentable
        ));
    }

    #[test]
    fn peer_failures_retry_immediately_while_node_failures_wait() {
        for kind in [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::Interrupted,
        ] {
            assert!(is_connection_accept_failure(&io::Error::from(kind)));
        }
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::OutOfMemory,
            io::ErrorKind::PermissionDenied,
        ] {
            assert!(!is_connection_accept_failure(&io::Error::from(kind)));
        }
    }
}
