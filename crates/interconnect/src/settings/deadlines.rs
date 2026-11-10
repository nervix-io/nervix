//! Operation deadlines projected from this transport's configured request budget.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The relative budgets of control, coordination, setup and transfer requests.
//! - **Depends on.** Transport request and committed liveness settings.
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
