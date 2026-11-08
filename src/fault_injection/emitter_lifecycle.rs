//! Scenario observations of emitter construction.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Counts for explicitly observed emitter construction boundaries.
//! - **Depends on.** The scenario's injection state and typed emitter names.
//! - **Must not know.** Drivers, publication, retry scheduling or graph configuration.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns cold scenario construction observations")
)]

use meticulous::OptionExt as _;

use super::{EmitterName, FaultInjection};

impl FaultInjection {
    pub fn observe_emitter_opens(&self, emitter: EmitterName) {
        assert!(
            self.inner
                .observed_emitter_opens
                .insert(emitter, 0)
                .is_none()
        );
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "records one construction at the cold emitter composition boundary"
        )
    )]
    pub(crate) fn record_emitter_open(&self, emitter: &EmitterName) {
        if let Some(mut count) = self.inner.observed_emitter_opens.get_mut(emitter) {
            *count = count
                .checked_add(1)
                .assured("a bounded scenario cannot exhaust the construction count");
        }
    }

    pub fn emitter_open_count(&self, emitter: &EmitterName) -> Option<usize> {
        self.inner
            .observed_emitter_opens
            .get(emitter)
            .map(|count| *count)
    }
}
