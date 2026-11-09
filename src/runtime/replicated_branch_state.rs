//! Layer: data plane.
//! Owns: one branch's published generations, replica progress, checkpoint-catalog registration,
//! publication requests, and persistence through its snapshot codec.
//! May depend on: typed state placement, generation publication, checkpoint replication, the
//! state store, and processor snapshot requests.
//! Must not know: live processor state, snapshot encoding or restore rules, control-plane
//! transactions, or Plexus parsing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "branch tasks publish immutable state through this owner; checkpoint waits \
                  declare their lifecycle context"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(
        plans,
        reason = "state is installed from typed placements and snapshots, never configuration \
                  Models"
    )
)]

use std::{fmt::Debug, future::Future};

use nervix_checkpoint_replication::CheckpointReplication;
use nervix_execution::Executor;
use nervix_primitives::sync::{mpsc, oneshot};

use super::{
    ProcessorSnapshotRequest, RuntimeStateOperationError, RuntimeStatePlacement,
    RuntimeStateResult, RuntimeStateStore,
    branch_checkpoint_catalog::{BranchCheckpointCatalog, CatalogRegistration},
    published_generation::PublishedGenerations,
    snapshot_staging::SnapshotStaging,
};

/// The state-specific persistence boundary for an immutable branch generation.
///
/// The codec owns its published representation and how it writes that representation, including
/// bounded sealing when needed. The branch-state owner records persistence only after it succeeds.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "snapshot codecs write immutable generations at periodic and final checkpoint \
                  boundaries"
    )
)]
pub(super) trait BranchSnapshotCodec: Debug + Send + Sync + Sized + 'static {
    type Published: Debug + Send + Sync + 'static;

    fn persist_after(
        state: &ReplicatedBranchState<Self>,
        after_lsm: u64,
        store: &RuntimeStateStore,
        executor: &Executor,
        staging: &SnapshotStaging,
    ) -> impl Future<Output = RuntimeStateResult<Option<u64>>> + Send;
}

/// The state one branch keeps beyond the task that alone changes its live processor state.
///
/// The task publishes immutable generations here; snapshots, replicas, handoff and replacement
/// tasks never borrow its live state. The carrier keeps the same placement, generation, progress
/// and catalog lifetimes for every snapshot codec, without another allocation or shared handle.
#[derive(Debug)]
pub(super) struct ReplicatedBranchState<C: BranchSnapshotCodec> {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) generations: PublishedGenerations<C::Published>,
    replication: CheckpointReplication,
    /// Absent for a state that only encodes or checks a checkpoint.
    catalog: Option<CatalogRegistration>,
}

impl<C: BranchSnapshotCodec> ReplicatedBranchState<C> {
    /// A cold read identifies the exact registered state as well as its published revision.
    pub(super) fn inspection_identity(&self) -> Option<[u8; 16]> {
        self.catalog
            .as_ref()
            .map(CatalogRegistration::inspection_identity)
    }

    /// Retain the codec's initial publication under its exact placement.
    pub(super) fn from_generations(
        placement: RuntimeStatePlacement,
        generations: PublishedGenerations<C::Published>,
    ) -> Self {
        Self {
            placement,
            generations,
            replication: CheckpointReplication::new(),
            catalog: None,
        }
    }

    /// Register an owned branch's current generation and every later publication with its entity.
    pub(super) fn cataloged(mut self, catalog: &BranchCheckpointCatalog) -> Self {
        let revision = self.generations.load().revision;
        self.catalog = Some(catalog.register(
            self.placement.branch_key.clone(),
            self.placement.state,
            revision,
        ));
        self
    }

    pub(super) fn replication(&self) -> &CheckpointReplication {
        &self.replication
    }

    /// Publish the branch task's current value before recording its revision in the catalog.
    pub(super) fn publish(&self, value: C::Published) {
        let revision = self.generations.publish(value);
        if let Some(catalog) = &self.catalog {
            catalog.record(revision);
        }
    }

    /// Ask the branch task to publish only when its live state changed since its last publication.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "periodic and stopping checkpoint tasks request publication outside record \
                      execution"
        )
    )]
    pub(super) async fn request_publication(
        &self,
        snapshot_requests: &mpsc::Sender<ProcessorSnapshotRequest>,
    ) -> RuntimeStateResult<()> {
        if !self.generations.is_live_dirty() {
            return Ok(());
        }
        let placement = &self.placement;
        let (response_tx, response_rx) = oneshot::channel();
        snapshot_requests.send(response_tx).await.map_err(|_| {
            RuntimeStateOperationError::checkpoint(format!(
                "{} '{}' snapshot owner is unavailable",
                placement.kind.as_str(),
                placement.identifier.as_str()
            ))
        })?;
        let response = response_rx.await.map_err(|_| {
            RuntimeStateOperationError::checkpoint(format!(
                "{} '{}' snapshot owner dropped its response",
                placement.kind.as_str(),
                placement.identifier.as_str()
            ))
        })?;
        response.map_err(|error| RuntimeStateOperationError::checkpoint(format!("{error:#}")))
    }

    /// Persist a generation newer than the last successful write, without borrowing live state.
    ///
    /// This remains usable after the branch task has ended. An unsuccessful write never advances
    /// persistence progress, and writing an earlier captured generation cannot move it backwards.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "periodic and final checkpoint tasks persist an immutable generation through \
                      its codec"
        )
    )]
    pub(super) async fn persist_published(
        &self,
        store: &RuntimeStateStore,
        executor: &Executor,
        staging: &SnapshotStaging,
    ) -> RuntimeStateResult<Option<u64>> {
        let after_lsm = self.generations.last_persisted_lsm();
        let persisted = C::persist_after(self, after_lsm, store, executor, staging).await?;
        if let Some(revision) = persisted {
            self.generations.record_persisted(revision);
        }
        Ok(persisted)
    }
}
