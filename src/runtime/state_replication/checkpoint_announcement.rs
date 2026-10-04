//! Layer: data plane.
//! Owns: offering a placement's newest checkpoint to the replicas that lag behind it, and routing a
//! replica's acknowledgement to the replication of the state it names and an owner's announcement
//! to the replica task that keeps the state current.
//! May depend on: the replicated states this node holds, checkpoint replication, the committed
//! schedule and the interconnect dispatcher.
//! Must not know: what a checkpoint holds, how a replica installs it, NSPL parsing, or
//! control-plane transactions.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "replica frames and steady checkpoint-announcer steps resolve placement state \
                  repeatedly"
    )
)]

use std::future::Future;

use super::*;

/// Announcement tasks retain their exact state until they finish or terminal teardown cancels
/// them. Closing the owner also interrupts a pending dispatch or retry wait.
#[derive(Default)]
pub(in crate::runtime) struct CheckpointAnnouncementTasks {
    tracker: TaskTracker,
    shutdown: CancellationToken,
}

impl CheckpointAnnouncementTasks {
    pub(in crate::runtime) fn spawn(
        &self,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> JoinHandle<()> {
        let shutdown = self.shutdown.clone();
        self.tracker.spawn(async move {
            nervix_primitives::select! {
                biased;
                _ = shutdown.cancelled() => {}
                _ = task => {}
            }
        })
    }

    pub(in crate::runtime) fn is_closed(&self) -> bool {
        self.tracker.is_closed()
    }

    pub(in crate::runtime) fn close(&self) {
        self.shutdown.cancel();
        self.tracker.close();
    }

    pub(in crate::runtime) async fn wait(&self) {
        self.tracker.wait().await;
    }
}

impl Runtime {
    /// Offer revision `lsm` of `placement` to its replicas through `replication`, the replication of
    /// the state that holds it, and start the placement's announcer when none is offering.
    pub(in crate::runtime) fn announce_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        replication: &CheckpointReplication,
        lsm: u64,
    ) {
        let Some(route) = self.inner.state_replication_routing.resolve(placement) else {
            return;
        };
        let Some(announcer) = route.offer(replication, lsm) else {
            return;
        };
        let runtime = self.clone();
        self.inner.state_replication_tasks.spawn(async move {
            runtime.offer_to_lagging_replicas(route, announcer).await;
        });
    }

    /// Announce a checkpoint of `placement` this node has just written, through the replication of
    /// the state it holds for the placement. A placement it holds no state for has no replication to
    /// announce through; its replicas catch up through their own synchronization.
    pub(super) fn announce_stored_checkpoint(&self, placement: &RuntimeStatePlacement, lsm: u64) {
        self.with_placement_replication(placement, |replication| {
            self.announce_checkpoint(placement, replication, lsm);
        });
    }

    /// Offer the announcer's revision to the replicas that do not hold it yet, and again every retry
    /// interval, until every replica the committed schedule assigns holds it, this node stops being
    /// the placement's primary, the replicated state goes away, or the runtime stops.
    async fn offer_to_lagging_replicas(
        &self,
        route: Arc<routing::StateReplicationRoute>,
        mut announcer: Announcer,
    ) {
        loop {
            nervix_primitives::task::consume_budget().await;
            // A stopping runtime closes the tracker this task runs under and waits for it, so an
            // announcer ends there. Dropping it hands its announcement back.
            if self.inner.state_replication_tasks.is_closed() {
                return;
            }
            // A runtime that has not joined a cluster has no replicas to offer anything to.
            let Some(dispatcher) = self.inner.remote_dispatcher.load_full() else {
                return;
            };
            let Some(replicas) = route.owned_replicas(dispatcher.local_node_id()) else {
                return;
            };
            let AnnouncerStep::Offer { revision, lagging } = announcer.next(&replicas) else {
                return;
            };
            let checkpoint = nervix_interconnect::StateCheckpointAvailable {
                placement: route.placement().to_remote(),
                lsm: revision,
            };
            for replica in lagging {
                nervix_primitives::task::consume_budget().await;
                if let Err(error) = dispatcher
                    .dispatch(
                        &replica,
                        Envelope::Control(
                            nervix_interconnect::ControlEnvelope::StateCheckpointAvailable(
                                checkpoint.clone(),
                            ),
                        ),
                    )
                    .await
                {
                    warn!(
                        destination = %replica,
                        error = %error,
                        "failed to announce runtime state checkpoint"
                    );
                }
            }
            sleep(STATE_CHECKPOINT_ANNOUNCEMENT_RETRY_INTERVAL).await;
        }
    }

    /// Use the installed state's retained replication handle without opening a state registry.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies a synchronous action on its retained replication handle"
        )
    )]
    pub(in crate::runtime) fn with_placement_replication(
        &self,
        placement: &RuntimeStatePlacement,
        use_replication: impl FnOnce(&CheckpointReplication),
    ) {
        self.inner
            .state_replication_routing
            .with_replication(placement, use_replication);
    }

    /// Record that `node_id` holds revision `ack.lsm` of `ack.placement` on its stable storage.
    pub(crate) fn handle_state_replication_ack(
        &self,
        node_id: &ClusterNodeName,
        ack: StateSyncAck,
    ) {
        self.inner
            .state_replication_routing
            .acknowledge(&ack.placement, node_id, ack.lsm);
    }

    /// Act on `source`'s announcement that it holds a newer checkpoint of a placement this node
    /// replicates.
    ///
    /// The branch lifecycle and the branch states of a branch-keyed entity are kept current by one
    /// replica task for the whole entity: the announcement is left with the entity's lifecycle,
    /// which wakes that task, and the task fetches or acknowledges the announced checkpoint in its
    /// next round. Any other state wakes the task that keeps this node's copy of it current.
    pub(crate) fn handle_state_checkpoint_available(
        &self,
        source: &ClusterNodeName,
        checkpoint: nervix_interconnect::StateCheckpointAvailable,
    ) {
        if self
            .inner
            .fault_injection
            .state_checkpoint_announcements_are_lost()
        {
            return;
        }
        let placement = match RuntimeStatePlacement::from_remote(checkpoint.placement) {
            Ok(placement) => placement,
            Err(error) => {
                warn!(
                    error = %error,
                    "ignored invalid runtime state checkpoint notification"
                );
                return;
            }
        };
        if !self.state_replica_assignment_is_current(&placement, source) {
            return;
        }
        trace!(
            domain = placement.domain.as_str(),
            kind = placement.kind.as_str(),
            name = placement.identifier.as_str(),
            lsm = checkpoint.lsm,
            "runtime state checkpoint is available"
        );
        match placement.state.kind() {
            RuntimeStateKind::BranchLru => {
                if let Some(lifecycle) = self.branch_lifecycle(&placement) {
                    lifecycle.announce_lifecycle(checkpoint.lsm);
                }
            }
            RuntimeStateKind::Deduplicator
            | RuntimeStateKind::WasmProcessor
            | RuntimeStateKind::WindowProcessor => {
                let Some(entity) = placement.branch_lifecycle() else {
                    return;
                };
                let Some(lifecycle) = self.branch_lifecycle(&entity) else {
                    return;
                };
                lifecycle.announce_branch(
                    placement.branch_key,
                    AnnouncedCheckpoint {
                        state: placement.state,
                        lsm: checkpoint.lsm,
                    },
                );
            }
            RuntimeStateKind::BranchAggregated
            | RuntimeStateKind::Correlator
            | RuntimeStateKind::KafkaOffset
            | RuntimeStateKind::MaterializedRelay => {
                self.with_placement_replication(&placement, CheckpointReplication::announced);
            }
        }
    }
}

#[cfg(test)]
#[path = "checkpoint_announcement_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "checkpoint_announcement_shuttle_tests.rs"]
mod shuttle_tests;
