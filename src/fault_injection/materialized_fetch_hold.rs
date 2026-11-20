//! Pauses at the opening and first chunk of a materialized snapshot fetch.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The one-shot barriers that hold a fetch before opening a described snapshot or
//!   after receiving its first chunk, and their observations and releases.
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

use nervix_models::{ClusterNodeName, DomainNodeRef};
use nervix_primitives::sync::{Arc, atomic::Ordering};

use super::{CommandPausePoint, FaultInjection, pause::TestPause};

impl FaultInjection {
    /// Pause the next remote fetch of this relay after it has described its snapshot.
    pub fn pause_materialized_snapshot_opening(&self, relay: DomainNodeRef) {
        self.arm_command_pause(CommandPausePoint::MaterializedSnapshotOpening(relay));
    }

    pub async fn wait_for_materialized_snapshot_opening(&self, relay: &DomainNodeRef) {
        self.wait_for_command_pause(&CommandPausePoint::MaterializedSnapshotOpening(
            relay.clone(),
        ))
        .await;
    }

    pub fn release_materialized_snapshot_opening(&self, relay: &DomainNodeRef) {
        self.release_command_pause(&CommandPausePoint::MaterializedSnapshotOpening(
            relay.clone(),
        ));
    }

    pub(crate) async fn pause_materialized_snapshot_opening_if_armed(&self, relay: DomainNodeRef) {
        self.pause_command_if_armed(CommandPausePoint::MaterializedSnapshotOpening(relay))
            .await;
    }

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
