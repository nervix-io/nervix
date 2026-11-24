//! The licensing clock offset a licensing scenario moves its nodes' committed time by.
//!
//! Layer: test harness.
//! - **Owns.** The scenario fixture's shared licensing clock offset.
//! - **Depends on.** The fault handle's publication primitive.
//! - **Must not know.** Admission, policy evaluation, consensus or service requests.

use super::FaultInjection;

impl FaultInjection {
    pub(crate) fn licensing_clock_changes(
        &self,
    ) -> Option<nervix_primitives::sync::watch::Receiver<std::time::Duration>> {
        Some(self.inner.licensing_clock_offset.subscribe())
    }

    pub fn set_licensing_clock_offset(&self, offset: std::time::Duration) {
        self.inner.licensing_clock_offset.send_replace(offset);
    }
    pub(crate) fn licensing_clock_offset(&self) -> std::time::Duration {
        *self.inner.licensing_clock_offset.borrow()
    }
}
