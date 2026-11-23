//! The scenario-held failure of one ingestor and how often its task rechecks it.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Holding an ingestor failed, clearing it, and the recheck cadence its task reads.
//! - **Depends on.** Vocabulary ingestor identities and the shared test injector.
//! - **Must not know.** Connector drivers, intake decisions, placement or metric storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the scenario-held ingestor failure seam")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the harness owns ingestor failure control and observations"
    )
)]

use std::time::Duration;

use nervix_models::IngestorName;

use super::FaultInjection;

impl FaultInjection {
    /// Holds `ingestor` failed until [`Self::clear_ingestor_fault`]. Its task reports the failed
    /// intake with `recheck` as its retry backoff and checks every `recheck` whether it may run
    /// again.
    pub fn fail_ingestor(&self, ingestor: &str, recheck: Duration) {
        self.inner
            .failed_ingestors
            .insert(ingestor.to_ascii_lowercase(), recheck);
    }

    pub fn clear_ingestor_fault(&self, ingestor: &str) {
        self.inner
            .failed_ingestors
            .remove(&ingestor.to_ascii_lowercase());
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario selects one retained ingestor failure seam",
            key = "one test ingestor",
            bound = "one keyed presence read with no retained guard"
        )
    )]
    pub(crate) fn ingestor_is_failed(&self, ingestor: &IngestorName) -> bool {
        self.inner
            .failed_ingestors
            .contains_key(&ingestor.as_str().to_ascii_lowercase())
    }

    /// How often `ingestor` rechecks whether it may run again while the scenario holds it failed,
    /// absent while it is not failed.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario selects one retained ingestor failure seam",
            key = "one test ingestor",
            bound = "one keyed read of a copied recheck cadence with no retained guard"
        )
    )]
    pub(crate) fn failed_ingestor_recheck(&self, ingestor: &IngestorName) -> Option<Duration> {
        let recheck = self
            .inner
            .failed_ingestors
            .get(&ingestor.as_str().to_ascii_lowercase())?;
        Some(*recheck)
    }
}
