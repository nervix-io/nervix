//! Scenario pauses between retaining a relay batch and asking its peer for a grant.
//!
//! Layer: test harness.
//! - **Owns.** One scenario-domain pause and its reached/released observations.
//! - **Depends on.** The common pause primitive and typed domain names.
//! - **Must not know.** Receiver bounds, payloads, acknowledgement owners or graph schedules.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns a scenario relay grant pause")
)]

use super::*;

#[derive(Debug, Default)]
pub(super) struct RelayCapabilityProbe {
    pause: TestPause,
    accepted: nervix_primitives::sync::blocking::Mutex<Vec<(ClusterNodeName, usize, usize)>>,
}

impl FaultInjection {
    pub fn pause_relay_before_grant(&self, domain: String) {
        self.inner.relay_before_grant_pauses.insert(
            domain.to_ascii_lowercase(),
            Arc::new(RelayCapabilityProbe::default()),
        );
    }
    pub async fn wait_for_relay_before_grant_pause(&self, domain: &str) {
        let pause = self
            .inner
            .relay_before_grant_pauses
            .get(domain)
            .verified("the scenario armed this domain")
            .value()
            .clone();
        pause.pause.wait_until_reached().await;
    }
    pub fn release_relay_before_grant_pause(&self, domain: &str) {
        let pause = self
            .inner
            .relay_before_grant_pauses
            .get(domain)
            .verified("the scenario armed this domain")
            .value()
            .clone();
        pause.pause.release();
    }
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one scenario-owned relay grant pause",
            key = "scenario domain",
            bound = "one keyed read whose guard ends before waiting"
        )
    )]
    pub(crate) async fn pause_relay_before_grant_if_armed(&self, domain: &DomainName) {
        let pause = self
            .inner
            .relay_before_grant_pauses
            .get(domain.as_str())
            .map(|entry| entry.value().clone());
        let Some(pause) = pause else {
            return;
        };
        pause.pause.reach();
        pause.pause.wait_until_released().await;
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one scenario-owned relay admission observation",
            key = "scenario domain",
            bound = "one keyed read and one guard over at most 32 observations; no await"
        )
    )]
    pub(crate) fn observe_capability_relay_admission(
        &self,
        domain: &DomainName,
        node: &ClusterNodeName,
        rows: usize,
        bytes: usize,
    ) {
        let Some(probe) = self
            .inner
            .relay_before_grant_pauses
            .get(domain.as_str())
            .map(|entry| entry.value().clone())
        else {
            return;
        };
        let mut accepted = probe.accepted.lock();
        assert!(
            accepted.len() < 32,
            "the bounded eight-row scenario records at most sixteen relay pieces"
        );
        accepted.push((node.clone(), rows, bytes));
    }

    pub fn capability_relay_admissions(
        &self,
        domain: &str,
    ) -> Vec<(ClusterNodeName, usize, usize)> {
        let probe = self
            .inner
            .relay_before_grant_pauses
            .get(domain)
            .verified("the scenario armed this domain")
            .value()
            .clone();
        probe.accepted.lock().clone()
    }
}
