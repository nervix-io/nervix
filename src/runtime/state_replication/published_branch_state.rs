//! Layer: data plane.
//! Owns: asking the branch task that owns a deduplicator or window processor state to publish it,
//! and persisting the generation that task published.
//! May depend on: published branch states, the state store, and processor snapshot requests.
//! Must not know: how a branch task changes its live state, control-plane transactions, or edge
//! protocols.

use super::*;

/// A branch-local runtime state whose branch task publishes what everything outside that task
/// reads.
#[derive(Debug, Clone)]
pub(in crate::runtime) enum PublishedBranchState {
    Deduplicator(Arc<ReplicatedDeduplicatorState>),
    WindowProcessor(Arc<ReplicatedWindowProcessorState>),
}

impl PublishedBranchState {
    pub(super) fn placement(&self) -> &RuntimeStatePlacement {
        match self {
            Self::Deduplicator(state) => &state.placement,
            Self::WindowProcessor(state) => &state.placement,
        }
    }

    pub(super) fn replication(&self) -> &CheckpointReplication {
        match self {
            Self::Deduplicator(state) => state.replication(),
            Self::WindowProcessor(state) => state.replication(),
        }
    }

    fn is_live_dirty(&self) -> bool {
        match self {
            Self::Deduplicator(state) => state.generations.is_live_dirty(),
            Self::WindowProcessor(state) => state.generations.is_live_dirty(),
        }
    }

    fn last_persisted_lsm(&self) -> u64 {
        match self {
            Self::Deduplicator(state) => state.generations.last_persisted_lsm(),
            Self::WindowProcessor(state) => state.generations.last_persisted_lsm(),
        }
    }

    fn record_persisted(&self, lsm: u64) {
        match self {
            Self::Deduplicator(state) => state.generations.record_persisted(lsm),
            Self::WindowProcessor(state) => state.generations.record_persisted(lsm),
        }
    }

    /// Ask the branch task that owns this state to publish its live state, when that changed after
    /// the last publication.
    pub(super) async fn request_publication(
        &self,
        snapshot_requests: &mpsc::Sender<ProcessorSnapshotRequest>,
    ) -> RuntimeStateResult<()> {
        if !self.is_live_dirty() {
            return Ok(());
        }
        let placement = self.placement();
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

    /// Persist what the branch task published last when it is newer than the persisted snapshot.
    ///
    /// This does not need the branch task: one that is gone can no longer publish, and what it
    /// published before is then the newest state anyone can restore. The encode reads the published
    /// value, so a branch that is still running keeps processing while it runs. A window too large
    /// to encode in memory is sealed on quota-owned disk and published in bounded segments.
    pub(super) async fn persist_published(
        &self,
        store: &RuntimeStateStore,
        executor: &Executor,
        staging: &SnapshotStaging,
    ) -> RuntimeStateResult<Option<u64>> {
        let revision = match self {
            Self::Deduplicator(state) => {
                let snapshot = state
                    .snapshot_after(Some(self.last_persisted_lsm()))
                    .change_context(RuntimeStateOperationError::Persistence)?;
                let Some(snapshot) = snapshot else {
                    return Ok(None);
                };
                store
                    .persist_latest_snapshot(self.placement(), snapshot.lsm, &snapshot.payload)
                    .change_context(RuntimeStateOperationError::Persistence)?;
                snapshot.lsm
            }
            Self::WindowProcessor(state) => {
                let persistence = state
                    .persistence_after(self.last_persisted_lsm(), executor, staging)
                    .await
                    .change_context(RuntimeStateOperationError::Persistence)?;
                match persistence {
                    None => return Ok(None),
                    Some(WindowPersistence::Resident(snapshot)) => {
                        store
                            .persist_latest_snapshot(
                                self.placement(),
                                snapshot.lsm,
                                &snapshot.payload,
                            )
                            .change_context(RuntimeStateOperationError::Persistence)?;
                        snapshot.lsm
                    }
                    Some(WindowPersistence::Sealed { revision, artifact }) => {
                        store
                            .publish_checkpoint_artifact(
                                self.placement(),
                                revision,
                                Arc::new(artifact),
                            )
                            .await
                            .change_context(RuntimeStateOperationError::Persistence)?;
                        revision
                    }
                }
            }
        };
        self.record_persisted(revision);
        Ok(Some(revision))
    }
}
