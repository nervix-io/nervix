//! Pauses automatic scheduling after a domain's first forced recovery preparation round.
//!
//! Layer: test harness.
//! - **Owns.** The pause and the count of preparation rounds observed by a scenario.
//! - **Depends on.** Domain identities and the shared scenario pause.
//! - **Must not know.** Schedule decisions, health policy or state recovery internals.

use nervix_models::DomainName;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::{FaultInjection, TestPause};

#[derive(Debug, Default)]
pub(super) struct ForcedRecoveryPause {
    pause: TestPause,
    rounds: AtomicUsize,
}

impl FaultInjection {
    pub fn pause_first_forced_recovery_publication(&self, domain: DomainName) {
        self.inner
            .forced_recovery_pauses
            .insert(domain, Arc::new(ForcedRecoveryPause::default()));
    }

    pub async fn wait_for_forced_recovery_publication_pause(&self, domain: &DomainName) {
        self.forced_recovery_pause(domain)
            .pause
            .wait_until_reached()
            .await;
    }

    pub fn release_forced_recovery_publication(&self, domain: &DomainName) {
        self.forced_recovery_pause(domain).pause.release();
    }

    pub async fn wait_for_forced_recovery_discard(&self, domain: &DomainName) {
        self.forced_recovery_pause(domain)
            .pause
            .wait_until_delivered()
            .await;
    }

    pub(crate) fn observe_forced_recovery_discard(&self, domain: &DomainName) {
        if let Some(observation) = self.inner.forced_recovery_pauses.get(domain) {
            observation.pause.mark_delivered();
        }
    }

    pub fn forced_recovery_preparation_rounds(&self, domain: &DomainName) -> usize {
        self.forced_recovery_pause(domain)
            .rounds
            .load(Ordering::Relaxed)
    }

    pub(crate) async fn observe_forced_recovery_preparation(
        &self,
        domain: &DomainName,
        confirmed: bool,
    ) {
        let Some(observation) = self
            .inner
            .forced_recovery_pauses
            .get(domain)
            .map(|observation| observation.value().clone())
        else {
            return;
        };
        if observation.rounds.fetch_add(1, Ordering::Relaxed) == 0 {
            assert!(
                confirmed,
                "no destination confirmed staging the first forced recovery in '{domain}'"
            );
            observation.pause.reach();
            observation.pause.wait_until_released().await;
        }
    }

    fn forced_recovery_pause(&self, domain: &DomainName) -> Arc<ForcedRecoveryPause> {
        self.inner
            .forced_recovery_pauses
            .get(domain)
            .unwrap_or_else(|| {
                panic!("no forced recovery publication pause was armed for '{domain}'")
            })
            .value()
            .clone()
    }
}
