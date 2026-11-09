//! A domain pause injected before an admitted model command's preflight.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The domain-scoped pause fault, its claim and confirmed delivery observation.
//! - **Depends on.** Vocabulary identities and the shared injector facade.
//! - **Must not know.** Consensus writes, transaction planning or recovery policy.

use ahash::RandomState;
use meticulous::OptionExt as _;
use nervix_models::DomainName;
use nervix_primitives::collections::DashMap;

use super::FaultInjection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrphanedDomainPause {
    Armed,
    Claimed,
    Delivered,
}

/// One scenario's pause faults, retained until its shared injector ends.
#[derive(Debug, Default)]
pub(super) struct OrphanedDomainPauses {
    pauses: DashMap<DomainName, OrphanedDomainPause, RandomState>,
}

impl OrphanedDomainPauses {
    fn arm(&self, domain: DomainName) {
        self.pauses.insert(domain, OrphanedDomainPause::Armed);
    }

    fn delivered(&self, domain: &DomainName) -> bool {
        let Some(pause) = self.pauses.get(domain) else {
            return false;
        };
        *pause == OrphanedDomainPause::Delivered
    }

    fn claim(&self, domain: &DomainName) -> bool {
        let Some(mut pause) = self.pauses.get_mut(domain) else {
            return false;
        };
        if *pause != OrphanedDomainPause::Armed {
            return false;
        }
        *pause = OrphanedDomainPause::Claimed;
        true
    }

    fn mark_delivered(&self, domain: &DomainName) {
        let mut pause = self
            .pauses
            .get_mut(domain)
            .verified("the injection was claimed before its pause was proposed");
        assert_eq!(*pause, OrphanedDomainPause::Claimed);
        *pause = OrphanedDomainPause::Delivered;
    }
}

impl FaultInjection {
    /// Leaves a pause for the next admitted model command to encounter before preflight.
    pub fn orphan_next_model_command_pause(&self, domain: DomainName) {
        self.inner.orphaned_domain_pauses.arm(domain);
    }

    /// Whether the armed command encountered its confirmed pause injection.
    pub fn orphaned_model_command_pause_delivered(&self, domain: &DomainName) -> bool {
        self.inner.orphaned_domain_pauses.delivered(domain)
    }

    pub(crate) fn take_orphaned_model_command_pause(&self, domain: &DomainName) -> bool {
        self.inner.orphaned_domain_pauses.claim(domain)
    }

    pub(crate) fn mark_orphaned_model_command_pause_delivered(&self, domain: &DomainName) {
        self.inner.orphaned_domain_pauses.mark_delivered(domain);
    }
}
