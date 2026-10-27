//! Observing the actual storage admission future of an offline client inspection.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** A domain's first-poll observation through the existing pause registry.
//! - **Depends on.** Typed domain names, shared observation barriers and governed atomics.
//! - **Must not know.** Configuration values, connector drivers or admission outcomes.

use nervix_models::DomainName;
use nervix_primitives::sync::atomic::Ordering;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    pub fn observe_client_inspection_on(&self, domain: DomainName) {
        self.arm_command_pause(CommandPausePoint::ClientInspectionAdmission(domain));
    }

    pub async fn wait_for_client_inspection_admission(&self, domain: &DomainName) {
        self.wait_for_command_pause(&CommandPausePoint::ClientInspectionAdmission(
            domain.clone(),
        ))
        .await;
    }

    /// Poll the production future unchanged and publish after its first poll.
    pub(crate) async fn observe_client_inspection_admission<F: Future>(
        &self,
        domain: &DomainName,
        future: F,
    ) -> F::Output {
        let point = CommandPausePoint::ClientInspectionAdmission(domain.clone());
        let pause = self
            .inner
            .command_pauses
            .get(&point)
            .map(|pause| pause.value().clone());
        let Some(pause) = pause else {
            return future.await;
        };
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|cx| {
            let result = future.as_mut().poll(cx);
            if !pause.claimed.swap(true, Ordering::AcqRel) {
                pause.reach();
            }
            result
        })
        .await
    }
}
