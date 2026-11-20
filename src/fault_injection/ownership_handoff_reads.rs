//! Ownership handoff checkpoint reads a scenario slowed, for the public scenario that grows a
//! regrouped reader's topology while every node answers slowly.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The domains whose ownership handoff checkpoint reads a scenario slowed, and the
//!   wait before each of those reads.
//! - **Depends on.** Vocabulary identities and execution-sensitive timers.
//! - **Must not know.** Handoff capture, preparation or checkpoint formats.

#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "owns test ownership handoff checkpoint read delays")
)]

use std::time::Duration;

use nervix_models::DomainName;

use super::FaultInjection;

impl FaultInjection {
    /// Makes every read of an ownership handoff checkpoint in `domain` wait `delay` before it
    /// starts, on whichever node reads it: a source projecting a unit it captured, and a
    /// destination preparing the state it receives. A planned handoff's preparation then takes as
    /// long as its longest chain of reads, as it does when every node answers slowly.
    pub fn delay_ownership_handoff_checkpoint_reads(
        &self,
        domain: impl Into<String>,
        delay: Duration,
    ) {
        self.inner
            .ownership_handoff_checkpoint_read_delays
            .insert(domain.into().to_ascii_lowercase(), delay);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario-owned checkpoint read delay is read once before every ownership \
                      handoff checkpoint read",
            key = "one test domain",
            bound = "one keyed read; its guard ends before the delay"
        )
    )]
    pub(crate) async fn delay_ownership_handoff_checkpoint_read_if_armed(
        &self,
        domain: &DomainName,
    ) {
        let key = domain.as_str().to_ascii_lowercase();
        let delay = self
            .inner
            .ownership_handoff_checkpoint_read_delays
            .get(&key)
            .map(|delay| *delay.value());
        if let Some(delay) = delay {
            nervix_primitives::time::sleep(delay).await;
        }
    }
}
