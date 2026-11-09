//! Layer: data plane.
//! Owns: selecting the typed published state of a deduplicator, inferencer or window branch.
//! May depend on: the shared replicated branch-state owner, the state store, and snapshot requests.
//! Must not know: how a branch task changes its live state, control-plane transactions, or edge
//! protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

/// A branch-local runtime state whose branch task publishes what everything outside that task
/// reads.
#[derive(Debug, Clone)]
pub(in crate::runtime) enum PublishedBranchState {
    Deduplicator(Arc<ReplicatedDeduplicatorState>),
    Inferencer(Arc<ReplicatedInferencerState>),
    WindowProcessor(Arc<ReplicatedWindowProcessorState>),
}

impl PublishedBranchState {
    pub(super) fn placement(&self) -> &RuntimeStatePlacement {
        match self {
            Self::Deduplicator(state) => &state.placement,
            Self::Inferencer(state) => &state.placement,
            Self::WindowProcessor(state) => &state.placement,
        }
    }

    pub(super) fn replication(&self) -> &CheckpointReplication {
        match self {
            Self::Deduplicator(state) => state.replication(),
            Self::Inferencer(state) => state.replication(),
            Self::WindowProcessor(state) => state.replication(),
        }
    }

    /// Ask the selected state owner to publish its branch task's changed live state.
    pub(super) async fn request_publication(
        &self,
        snapshot_requests: &mpsc::Sender<ProcessorSnapshotRequest>,
    ) -> RuntimeStateResult<()> {
        match self {
            Self::Deduplicator(state) => state.request_publication(snapshot_requests).await,
            Self::Inferencer(state) => state.request_publication(snapshot_requests).await,
            Self::WindowProcessor(state) => state.request_publication(snapshot_requests).await,
        }
    }

    /// Persist the selected owner's latest publication, including after its branch task ends.
    pub(super) async fn persist_published(
        &self,
        store: &RuntimeStateStore,
        executor: &Executor,
        staging: &SnapshotStaging,
    ) -> RuntimeStateResult<Option<u64>> {
        match self {
            Self::Deduplicator(state) => state.persist_published(store, executor, staging).await,
            Self::Inferencer(state) => state.persist_published(store, executor, staging).await,
            Self::WindowProcessor(state) => state.persist_published(store, executor, staging).await,
        }
    }
}
