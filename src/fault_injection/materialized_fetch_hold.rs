//! A materialized snapshot fetch held on its first received chunk until its node's restore
//! maintenance has asked for bulk memory, for the public bulk contention scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The nodes armed to hold their next materialized snapshot fetch, the one fetch each
//!   hold claims, and its held, released and resumed observations.
//! - **Depends on.** Vocabulary identities and the shared pause barriers.
//! - **Must not know.** Snapshot formats, staging, bulk budgets, restore staging or how
//!   maintenance is admitted.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns test materialized snapshot fetch holds and their observations"
    )
)]

use nervix_models::ClusterNodeName;
use nervix_primitives::sync::{Arc, atomic::Ordering};

use super::{FaultInjection, pause::TestPause};

impl FaultInjection {
    /// Holds the next materialized snapshot fetch `node_id` makes once its first received chunk is
    /// charged to the node's bulk memory, until the node's restore checkpoint maintenance next asks
    /// bulk memory for its working set. Fetches after the held one are not held.
    pub fn hold_materialized_snapshot_fetch_on(&self, node_id: ClusterNodeName) {
        self.inner
            .materialized_snapshot_fetch_holds
            .insert(node_id, Arc::new(TestPause::default()));
    }

    /// Waits until the held fetch of `node_id` went on after the node's maintenance asked for bulk
    /// memory while the fetch held its chunk.
    pub async fn wait_for_materialized_snapshot_fetch_resumption(&self, node_id: &ClusterNodeName) {
        let Some(hold) = self.materialized_snapshot_fetch_hold(node_id) else {
            panic!("no materialized snapshot fetch hold is armed on '{node_id}'");
        };
        hold.wait_until_delivered().await;
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario-owned hold is read once per materialized snapshot fetch, after \
                      its first chunk arrived",
            key = "one test node",
            bound = "one keyed read; its guard ends before the test-controlled wait"
        )
    )]
    pub(crate) async fn hold_materialized_snapshot_fetch_if_armed(
        &self,
        node_id: &ClusterNodeName,
    ) {
        let Some(hold) = self.materialized_snapshot_fetch_hold(node_id) else {
            return;
        };
        if hold.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        hold.reach();
        hold.wait_until_released().await;
        hold.mark_delivered();
    }

    /// Releases a fetch of `node_id` that holds its chunk, once the node's restore checkpoint
    /// maintenance has asked bulk memory for its working set, whether it was charged, deferred or
    /// left waiting.
    pub(crate) fn restore_maintenance_asked_for_bulk_memory(&self, node_id: &ClusterNodeName) {
        let Some(hold) = self.materialized_snapshot_fetch_hold(node_id) else {
            return;
        };
        if hold.is_reached() {
            hold.release();
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario arms one hold per test node, which a fetch reads once after its \
                      first chunk and the node's maintenance once per sweep",
            key = "one test node",
            bound = "one keyed read whose guard ends once the hold's handle is cloned"
        )
    )]
    fn materialized_snapshot_fetch_hold(
        &self,
        node_id: &ClusterNodeName,
    ) -> Option<Arc<TestPause>> {
        self.inner
            .materialized_snapshot_fetch_holds
            .get(node_id)
            .map(|hold| hold.value().clone())
    }
}
