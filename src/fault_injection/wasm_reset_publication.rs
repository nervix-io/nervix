//! Fault controls at the WASM reset generation publication boundary.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Domain-scoped publication refusals, pending genuine failures and their delivery
//!   observations. Ordinary schedule publications cannot consume these controls.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** Transaction progress, reset preparation, consensus or command recovery.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns test publication controls and their observations"
    )
)]

use std::num::NonZeroUsize;

use meticulous::OptionExt as _;
use nervix_models::DomainName;

use super::FaultInjection;

#[derive(Debug)]
pub(super) struct WasmResetPublicationAuthorityLoss {
    remaining: usize,
    delivered: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WasmResetPublicationFailure {
    Armed,
    Delivered,
}

impl FaultInjection {
    /// Refuses a bounded number of WASM generation publications at their consensus boundary.
    pub fn lose_wasm_reset_publication_authority(&self, domain: DomainName, count: NonZeroUsize) {
        self.inner.wasm_reset_publication_authority_losses.insert(
            domain,
            WasmResetPublicationAuthorityLoss {
                remaining: count.get(),
                delivered: 0,
            },
        );
    }

    /// Keeps an armed refusal distinct from a seam that no scenario armed.
    pub fn wasm_reset_publication_authority_losses(&self, domain: &DomainName) -> Option<usize> {
        self.inner
            .wasm_reset_publication_authority_losses
            .get(domain)
            .map(|state| state.delivered)
    }

    pub(crate) fn take_wasm_reset_publication_authority_loss(&self, domain: &DomainName) -> bool {
        let Some(mut state) = self
            .inner
            .wasm_reset_publication_authority_losses
            .get_mut(domain)
        else {
            return false;
        };
        if state.remaining == 0 {
            return false;
        }
        state.remaining = state
            .remaining
            .checked_sub(1)
            .assured("a pending refusal remains");
        state.delivered = state
            .delivered
            .checked_add(1)
            .assured("delivery is bounded by the armed count");
        true
    }

    /// Keeps reset generation publications failing until explicitly cleared. Delivery is observed
    /// after unpublished preparation is aborted; interrupted outcome recording leaves the
    /// condition active for recovery under the same command reference.
    pub fn fail_wasm_reset_publications(&self, domain: DomainName) {
        self.inner
            .failed_wasm_reset_publications
            .insert(domain, WasmResetPublicationFailure::Armed);
    }

    /// Ends the injected condition without changing a retained command outcome.
    pub fn clear_wasm_reset_publication_failure(&self, domain: &DomainName) {
        self.inner.failed_wasm_reset_publications.remove(domain);
    }

    /// Distinguishes an unarmed control from a pending or delivered failure.
    pub fn wasm_reset_publication_failure_delivered(&self, domain: &DomainName) -> Option<bool> {
        self.inner
            .failed_wasm_reset_publications
            .get(domain)
            .map(|state| *state == WasmResetPublicationFailure::Delivered)
    }

    pub(crate) fn wasm_reset_publication_failure_is_enabled(&self, domain: &DomainName) -> bool {
        self.inner
            .failed_wasm_reset_publications
            .contains_key(domain)
    }

    pub(crate) fn deliver_wasm_reset_publication_failure(&self, domain: &DomainName) {
        if let Some(mut state) = self.inner.failed_wasm_reset_publications.get_mut(domain) {
            *state = WasmResetPublicationFailure::Delivered;
        }
    }
}
