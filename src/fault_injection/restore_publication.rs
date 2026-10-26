//! Fault controls at the durable publication boundary of restored domain state.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Domain-scoped failures of durable restored-state publication and their delivery
//!   observations. Checkpoint staging failures and publication pauses never consume them.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** Restore steps, command recovery, consensus or the state store.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns test publication controls and their observations"
    )
)]

use nervix_models::DomainName;

use super::FaultInjection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RestorePublicationFailure {
    Armed,
    Delivered,
}

impl FaultInjection {
    /// Keeps every durable publication of `domain`'s restored state failing, after the store made
    /// it durable, until the failure is cleared. A restore that a new leader or a retry resumes
    /// under the same execution reference meets the same failure, so the failure stays the
    /// restore's outcome when recording it is interrupted.
    pub fn fail_restore_state_publications(&self, domain: DomainName) {
        self.inner
            .failed_restore_state_publications
            .insert(domain, RestorePublicationFailure::Armed);
    }

    /// Ends the injected condition without changing a recorded restore outcome.
    pub fn clear_restore_state_publication_failure(&self, domain: &DomainName) {
        self.inner.failed_restore_state_publications.remove(domain);
    }

    /// Distinguishes an unarmed control from a pending or delivered failure.
    pub fn restore_state_publication_failure_delivered(&self, domain: &DomainName) -> Option<bool> {
        self.inner
            .failed_restore_state_publications
            .get(domain)
            .map(|state| *state == RestorePublicationFailure::Delivered)
    }

    /// Whether the durable publication of `domain`'s restored state the store just made fails,
    /// recording that a publication met the failure.
    pub(crate) fn restore_state_publication_fails(&self, domain: &DomainName) -> bool {
        let Some(mut state) = self.inner.failed_restore_state_publications.get_mut(domain) else {
            return false;
        };
        *state = RestorePublicationFailure::Delivered;
        true
    }
}
