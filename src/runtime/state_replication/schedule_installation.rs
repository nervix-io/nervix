//! Layer: data plane.
//! Owns: selecting the installed execution that a forced-recovery preparation validates.
//! May depend on: runtime installation publication, its application lock and a caller's deadline.
//! Must not know: placement decisions, checkpoint contents or state recreation policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        data_plane,
        reason = "owns the installation boundary of runtime-state recovery"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "one recovery waits for its destination's installed graph"
    )
)]

use super::*;

impl Runtime {
    pub(super) async fn forced_recovery_node_until(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        expected: [u8; 32],
        deadline: Instant,
    ) -> OwnershipHandoffResult<ExecutionNode> {
        let installation = nervix_primitives::time::timeout_at(
            deadline,
            self.installed_forced_recovery_node(domain, entity, expected),
        )
        .await;
        match installation {
            Ok(node) => node,
            Err(_) => Err(Report::new(
                OwnershipHandoffError::ScheduleInstallationDeadline {
                    domain: domain.clone(),
                    node: entity.clone(),
                },
            )),
        }
    }

    /// A lifecycle notification can precede graph construction. Read only after the application
    /// that published it releases its lock, and subscribe before checking so a later publication
    /// cannot be missed. Release the lock before checkpoint transfer or guest restoration begins.
    pub(super) async fn installed_forced_recovery_node(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        expected: [u8; 32],
    ) -> OwnershipHandoffResult<ExecutionNode> {
        let mut installations = self.subscribe_domain_state();
        loop {
            nervix_primitives::task::consume_budget().await;
            let installation = self.inner.schedule_application.lock().await;
            if self.inner.executions.contains_key(domain) {
                self.verify_local_handoff_schedule(domain, expected)?;
                return self.ownership_handoff_scheduled_node(domain, entity);
            }
            #[cfg(feature = "testing")]
            if let Some(dispatcher) = self.inner.remote_dispatcher.load_full() {
                self.inner
                    .fault_injection
                    .missing_recovery_schedule_if_armed(dispatcher.local_node_id());
            }
            drop(installation);
            installations.changed().await.assured(
                "this waiter borrows the runtime that owns the domain-installation sender",
            );
        }
    }
}
