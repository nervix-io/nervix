//! Fault controls at the coordinator's entity drain observation.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Domain-scoped forced entity drain timeouts.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** Entity gates, transactions, command recovery or the runtime.

#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "owns test drain controls")
)]

use nervix_models::DomainName;

use super::FaultInjection;

impl FaultInjection {
    /// Forces the next entity drain in `domain` to report its normal timeout outcome after one
    /// complete status observation. The one-shot seam lets timeout recovery tests exercise that
    /// outcome without making successful cluster work race a short duration.
    pub fn force_next_entity_drain_timeout(&self, domain: DomainName) {
        self.inner.forced_entity_drain_timeouts.insert(domain, ());
    }

    /// Consumes the domain-scoped timeout only at the coordinator's drain-observation seam.
    pub(crate) fn take_forced_entity_drain_timeout(&self, domain: &DomainName) -> bool {
        self.inner
            .forced_entity_drain_timeouts
            .remove(domain)
            .is_some()
    }
}
