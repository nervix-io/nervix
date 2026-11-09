//! Layer: engines and infrastructure.
//! - **Owns.** Opaque retained event recorders and construction refusal for undeclared events.
//! - **Depends on.** The vocabulary's closed declarations and typed reports.
//! - **Must not know.** Metrics, drivers, Models, graph state or delivery decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns connector event host services")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies declared event capabilities")
)]

use nervix_models::{ConnectorEvent, DeclaredEvents};

/// The requested event is outside this host's entity declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "construction refuses a capability; no record is judged"
    )
)]
#[error("host does not declare connector event {event}")]
pub struct UndeclaredConnectorEvent {
    pub event: ConnectorEvent,
}

/// A resolved recording operation. Construction validates the identity; recording only counts.
pub struct ConnectorEventCounter {
    event: ConnectorEvent,
    record: Box<dyn Fn() + Send + Sync>,
}

impl std::fmt::Debug for ConnectorEventCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorEventCounter")
            .field("event", &self.event)
            .finish_non_exhaustive()
    }
}

impl ConnectorEventCounter {
    /// Resolves a declared identity once. `record` must be one nonblocking counter increment,
    /// retaining its child without registry access, allocation, waits or rate calculation.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "validates and retains one event recorder at connector construction"
        )
    )]
    pub fn new(
        declared: DeclaredEvents,
        event: ConnectorEvent,
        record: impl Fn() + Send + Sync + 'static,
    ) -> error_stack::Result<Self, UndeclaredConnectorEvent> {
        if !declared.contains(event) {
            return Err(UndeclaredConnectorEvent { event }.into());
        }
        Ok(Self {
            event,
            record: Box::new(record),
        })
    }

    /// Counts one observation without changing its record's delivery outcome.
    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "records through one retained counter child")
    )]
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the host-resolved callback performs one nonblocking counter increment"
        )
    )]
    pub fn increment(&self) {
        (self.record)();
    }
}

/// Resolves event children during connector construction. A host with no events refuses all.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "connector construction resolves declared event children"
    )
)]
pub trait ConnectorEventProvider: Send + 'static {
    fn event_counter(
        &self,
        event: ConnectorEvent,
    ) -> error_stack::Result<ConnectorEventCounter, UndeclaredConnectorEvent> {
        Err(UndeclaredConnectorEvent { event }.into())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests remain outside product execution")
)]
mod tests;
