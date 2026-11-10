//! Operation deadlines projected from this transport's configured budgets.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Request-relative budgets and the selected source of a duplex opening budget.
//! - **Depends on.** Transport setup, request and committed liveness settings.
//! - **Must not know.** Message payloads, graph state or application retries.
#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns relative RPC deadlines")
)]
use std::time::Duration;

use meticulous::OptionExt as _;

/// An operation's relationship to the configured request budget, never another local timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestDeadline {
    Liveness,
    Control,
    Coordination,
    Setup,
    Request,
    Snapshot,
    Transfer,
    Recovery,
    /// The remaining caller-owned operation deadline, or an explicit fixture budget.
    Explicit(Duration),
}
impl RequestDeadline {
    pub fn resolve(self, request: Duration, liveness: super::InterconnectLiveness) -> Duration {
        match self {
            Self::Liveness => liveness.probe_timeout(),
            Self::Control => (request / 10).max(Duration::from_nanos(1)),
            Self::Coordination => (request / 5).max(Duration::from_nanos(1)),
            Self::Setup => (request / 2).max(Duration::from_nanos(1)),
            Self::Request => request,
            Self::Snapshot => request
                .checked_mul(3)
                .verified("the request budget fits twelvefold"),
            Self::Transfer => request
                .checked_mul(6)
                .verified("the request budget fits twelvefold"),
            Self::Recovery => request
                .checked_mul(12)
                .verified("the request budget fits twelvefold"),
            Self::Explicit(value) => value,
        }
    }
    /// Resolve a deterministic fixture against the same defaults as a constructed transport.
    pub fn declared_default(self) -> Duration {
        use nervix_models::SettingDefinition as _;
        self.resolve(
            super::RequestTimeout::default_value(&()),
            super::InterconnectLiveness::default(),
        )
    }
}

/// The settings owner that bounds opening one duplex stream, independently of its lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplexSetupDeadline {
    /// Open a client link within this transport's complete connection-setup budget.
    ConnectionSetup,
    /// Keep an engine operation's existing relationship to the request budget.
    Request(RequestDeadline),
}

impl DuplexSetupDeadline {
    pub fn resolve(
        self,
        connection_setup: Duration,
        request: Duration,
        liveness: super::InterconnectLiveness,
    ) -> Duration {
        match self {
            Self::ConnectionSetup => connection_setup,
            Self::Request(deadline) => deadline.resolve(request, liveness),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplex_opening_uses_its_selected_transport_budget() {
        let connection_setup = Duration::from_secs(3);
        let request = Duration::from_secs(14);
        let liveness = super::super::InterconnectLiveness::default();
        assert_eq!(
            DuplexSetupDeadline::ConnectionSetup.resolve(connection_setup, request, liveness),
            connection_setup
        );
        assert_eq!(
            DuplexSetupDeadline::Request(RequestDeadline::Setup).resolve(
                connection_setup,
                request,
                liveness
            ),
            Duration::from_secs(7)
        );
    }
}
