//! Materialized dependency reads a scenario slowed, and the messages their processor parked on
//! `REQUIRED WAIT` meanwhile, for the public parked-wait scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The relay processors whose dependency reads a scenario slowed, the wait before each
//!   of their reads, and the count of messages each first parked on `REQUIRED WAIT`.
//! - **Depends on.** Vocabulary identities and execution-sensitive timers and watch channels.
//! - **Must not know.** Materialized state formats, how a processor resolves its dependencies or
//!   how a drain treats a parked message.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns test materialized dependency read delays and park observations"
    )
)]

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{DomainName, DomainNodeRef, ModelKind, ModelName};
use nervix_primitives::sync::watch;

use super::FaultInjection;

/// A relay processor whose reads of its materialized dependencies a scenario slowed.
#[derive(Debug)]
pub(super) struct DelayedMaterializedReads {
    /// What the processor waits before each read, as a read slow to reach the node that holds the
    /// state would.
    delay: Duration,
    /// The messages the processor has parked on `REQUIRED WAIT`, each counted when it first parks.
    parked: watch::Sender<u64>,
}

impl FaultInjection {
    /// Makes `processor`, a relay processor, wait `delay` before each read of its materialized
    /// dependencies, as a read slow to reach the node that holds the state would. Every read waits,
    /// including each re-evaluation of a message the processor parked on `REQUIRED WAIT`.
    pub fn delay_materialized_dependency_reads(&self, processor: DomainNodeRef, delay: Duration) {
        let (parked, _) = watch::channel(0);
        self.inner
            .delayed_materialized_dependency_reads
            .insert(processor, DelayedMaterializedReads { delay, parked });
    }

    /// Waits until `processor`, whose reads the scenario slowed, has parked a message on `REQUIRED
    /// WAIT`, so a step that follows meets the message parked rather than still being read.
    pub async fn wait_for_materialized_wait(&self, processor: &DomainNodeRef) {
        let mut parked = self
            .inner
            .delayed_materialized_dependency_reads
            .get(processor)
            .assured("the scenario slowed this processor's reads before waiting for its park")
            .parked
            .subscribe();
        parked
            .wait_for(|parked| *parked != 0)
            .await
            .assured("the seam keeps the count's sender for as long as the scenario runs");
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario-owned dependency read delay is read before every materialized \
                      dependency read of the processor it names",
            key = "one test processor",
            bound = "one keyed read; its guard ends before the test-controlled wait"
        )
    )]
    pub(crate) async fn delay_materialized_dependency_read_if_armed(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        processor: &ModelName,
    ) {
        let processor = DomainNodeRef::node_in(domain.clone(), kind, processor.clone());
        let delay = self
            .inner
            .delayed_materialized_dependency_reads
            .get(&processor)
            .map(|reads| reads.delay);
        if let Some(delay) = delay {
            nervix_primitives::time::sleep(delay).await;
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario-owned park count is updated when a processor whose reads the \
                      scenario slowed first parks a message on REQUIRED WAIT",
            key = "one test processor",
            bound = "one keyed read; its guard ends with the count's update"
        )
    )]
    pub(crate) fn record_materialized_wait_if_armed(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        processor: &ModelName,
    ) {
        let processor = DomainNodeRef::node_in(domain.clone(), kind, processor.clone());
        let Some(reads) = self
            .inner
            .delayed_materialized_dependency_reads
            .get(&processor)
        else {
            return;
        };
        reads.parked.send_modify(|parked| {
            *parked = parked
                .checked_add(1)
                .assured("a scenario parks far fewer than 2^64 messages");
        });
    }
}
