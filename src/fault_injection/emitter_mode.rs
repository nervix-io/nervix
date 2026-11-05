//! Scenario-owned modes for one emitter and its record-answer substitution.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Emitter mode vocabulary, arming, clearing, queries and one-shot record answers.
//! - **Depends on.** Vocabulary emitter identities, connector outcomes and the shared test injector.
//! - **Must not know.** Connector drivers, delivery decisions, placement or metric storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns scenario-controlled emitter modes")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the harness owns emitter mode control and observations"
    )
)]

use nervix_models::EmitterName;

use super::FaultInjection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EmitterFaultMode {
    Fail,
    Stall,
    UnknownRecord,
}

impl FaultInjection {
    pub fn fail_emitter(&self, emitter: &str) {
        self.inner
            .emitter_faults
            .insert(emitter.to_ascii_lowercase(), EmitterFaultMode::Fail);
    }

    pub fn stall_emitter(&self, emitter: &str) {
        self.inner
            .emitter_faults
            .insert(emitter.to_ascii_lowercase(), EmitterFaultMode::Stall);
    }

    pub fn clear_emitter_fault(&self, emitter: &str) {
        self.inner
            .emitter_faults
            .remove(&emitter.to_ascii_lowercase());
    }

    /// Replaces the next record write's answers with an identity outside that exact write.
    pub fn answer_next_emitter_with_unknown_record(&self, emitter: &str) {
        self.inner.emitter_faults.insert(
            emitter.to_ascii_lowercase(),
            EmitterFaultMode::UnknownRecord,
        );
    }

    pub(crate) fn received_emitter_record_outcome(
        &self,
        emitter: &EmitterName,
        records: usize,
        outcome: nervix_connector::PerRecordOutcome<nervix_connector::EmitterRecordId>,
    ) -> nervix_connector::PerRecordOutcome<nervix_connector::EmitterRecordId> {
        if records == 0 {
            return outcome;
        }
        let armed = self
            .inner
            .emitter_faults
            .remove_if(emitter.as_str(), |_, mode| {
                *mode == EmitterFaultMode::UnknownRecord
            });
        if armed.is_none() {
            return outcome;
        }
        let mut answer = nervix_connector::PerRecordOutcome::with_capacity(1);
        answer.deliver(nervix_connector::EmitterRecordId::new(records));
        answer
    }

    pub fn clear_all_emitter_faults(&self) {
        self.inner.emitter_faults.clear();
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario selects one retained emitter fault mode",
            key = "one test emitter",
            bound = "one keyed read; the guard ends with the synchronous predicate"
        )
    )]
    pub(crate) fn emitter_should_fail(&self, emitter: &EmitterName) -> bool {
        self.inner
            .emitter_faults
            .get(&emitter.as_str().to_ascii_lowercase())
            .is_some_and(|mode| *mode == EmitterFaultMode::Fail)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario selects one retained emitter stall mode",
            key = "one test emitter",
            bound = "one keyed read; the guard ends with the synchronous predicate"
        )
    )]
    pub(crate) fn emitter_should_stall(&self, emitter: &EmitterName) -> bool {
        self.inner
            .emitter_faults
            .get(&emitter.as_str().to_ascii_lowercase())
            .is_some_and(|mode| *mode == EmitterFaultMode::Stall)
    }
}
