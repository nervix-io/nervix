//! Snapshotting the three runtime state kinds a backup carries at a domain cut.
//!
//! Layer: data plane.
//! - **Owns.** Forcing Kafka and branch-lifecycle publications, then reading one database
//!   snapshot and reattaching typed branch keys from lifecycle checkpoints.
//! - **Depends on.** Branch-local state, the runtime state store, and typed placement envelopes.
//! - **Must not know.** Archive records, backup command execution, or restore planning.

use error_stack::{Report, ResultExt as _};
use nervix_interconnect::StatePlacementEnvelope;
use nervix_models::{
    BranchKeyFingerprint, DomainName, ModelKind, ModelName, RemoteRuntimeField, Timestamp,
};
use thiserror::Error;

use super::{
    BranchInstanceSnapshotEntry, BranchKey, OwnershipHandoffError, OwnershipHandoffResult,
    ReplicatedKafkaOffsetState, Runtime, RuntimeStateKind, RuntimeStatePlacement,
    ScheduledNodeTask,
    backup_capture_fence::{BackupCaptureFence, BackupPublication},
    decode_branch_lru_snapshot, encode_branch_lru_snapshot,
    kafka_offset_state::{backup_offset_positions, restore_offset_payload},
    state_store::StoredPlacement,
};

#[derive(Debug, Clone)]
pub(crate) struct CapturedRuntimeState {
    pub(crate) placement: StatePlacementEnvelope,
    pub(crate) branch_fingerprint: Option<BranchKeyFingerprint>,
    pub(crate) revision: u64,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct BackupBranchLifecycleEntry {
    pub(crate) key: Option<Vec<RemoteRuntimeField>>,
    pub(crate) last_ingestion: Timestamp,
    pub(crate) incarnation: u64,
}

#[derive(Debug, Error)]
pub(crate) enum BackupStateCaptureError {
    #[error("runtime state storage is unavailable")]
    Unavailable,
    #[error("runtime state could not be read or published")]
    Storage,
    #[error("branch lifecycle of processor '{entity}' could not be decoded")]
    Lifecycle { entity: ModelName },
    #[error("saved state of processor '{entity}' has no typed branch key in its lifecycle")]
    MissingBranch { entity: ModelName },
    #[error("restored guest state branch fingerprint does not match its typed branch key")]
    BranchFingerprint,
    #[error("a restored state placement is invalid")]
    Placement,
}

impl Runtime {
    /// Ask every locally owned branch supervisor for its current lifecycle before a quiesced
    /// backup reads the state store. A branch may have appeared since its periodic snapshot, so
    /// reading the published map alone can omit it from an otherwise complete cut.
    pub(crate) async fn checkpoint_backup_branch_lifecycles(
        &self,
        domain: &DomainName,
    ) -> OwnershipHandoffResult<()> {
        let local_node_id = {
            let dispatcher = self.inner.remote_dispatcher.load();
            let Some(dispatcher) = dispatcher.as_deref() else {
                return Err(OwnershipHandoffError::checkpoint(
                    "local node identity is unavailable for backup checkpoint",
                ));
            };
            dispatcher.local_node_id().clone()
        };
        let Some(execution) = self.inner.executions.get(domain) else {
            // A stopped domain has no tasks to checkpoint; its stored state is the cut.
            return Ok(());
        };
        if execution.passive_only {
            return Ok(());
        }
        let entities = execution
            .revision
            .nodes
            .iter()
            .filter(|(entity, node)| {
                Self::node_has_branch_lifecycle(entity.kind) && node.is_primary_on(&local_node_id)
            })
            .map(|(entity, _)| entity.clone())
            .collect::<Vec<_>>();
        drop(execution);

        for entity in entities {
            tokio::task::consume_budget().await;
            let snapshot = if entity.kind.is_processor() {
                let commands = {
                    let execution = self.inner.executions.get(domain).ok_or_else(|| {
                        OwnershipHandoffError::checkpoint(format!(
                            "domain '{}' has no execution during backup checkpoint",
                            domain.as_str()
                        ))
                    })?;
                    let task = execution.node_tasks.get(&entity).ok_or_else(|| {
                        OwnershipHandoffError::checkpoint(format!(
                            "{} '{}' has no local task during backup checkpoint",
                            entity.kind.as_str(),
                            entity.identifier.as_str()
                        ))
                    })?;
                    task.commands.clone()
                };
                ScheduledNodeTask::checkpoint_via(&commands).await?
            } else {
                self.checkpoint_entrypoint_branch_lifecycle(domain, &entity)
                    .await?
                    .ok_or_else(|| {
                        OwnershipHandoffError::checkpoint(format!(
                            "{} '{}' has no branch lifecycle during backup checkpoint",
                            entity.kind.as_str(),
                            entity.identifier.as_str()
                        ))
                    })?
            };
            let placement = self
                .state_placement(
                    domain,
                    RuntimeStateKind::BranchLru,
                    entity.kind,
                    entity.identifier.clone(),
                    None,
                )
                .change_context_lazy(|| OwnershipHandoffError::StatePlacement {
                    kind: entity.kind,
                    identifier: entity.identifier.clone(),
                })?;
            self.persist_branch_lru_snapshot(placement, snapshot)
                .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?;
        }
        Ok(())
    }

    pub(super) fn backup_publication(&self, domain: &DomainName) -> BackupPublication {
        BackupCaptureFence::publication(&self.backup_capture_fence(domain))
    }

    fn backup_capture_fence(&self, domain: &DomainName) -> super::Arc<BackupCaptureFence> {
        if let Some(fence) = self.inner.backup_capture_fences.load().get(domain) {
            return fence.clone();
        }
        let candidate = super::Arc::new(BackupCaptureFence::default());
        loop {
            let current = self.inner.backup_capture_fences.load();
            if let Some(fence) = current.get(domain) {
                return fence.clone();
            }
            drop(current);
            // First use is rare. Publish one immutable registry revision so concurrent first
            // publishers and a capture resolve the same fence without a data-plane map lock.
            self.inner.backup_capture_fences.rcu(|current| {
                let mut next = (**current).clone();
                if !next.contains_key(domain) {
                    next.insert(domain.clone(), candidate.clone());
                }
                next
            });
        }
    }

    pub(crate) fn purge_restored_domain_state(
        &self,
        domain: &DomainName,
    ) -> error_stack::Result<(), BackupStateCaptureError> {
        let Some(store) = self.inner.state_store.as_ref() else {
            return Err(Report::new(BackupStateCaptureError::Unavailable));
        };
        store
            .purge_domain(domain)
            .change_context(BackupStateCaptureError::Storage)?;
        self.clear_runtime_state_for_domain(domain);
        Ok(())
    }

    /// Installs one archive checkpoint in its recomputed placement's state lifetime. The caller
    /// purges the stopped domain first and installs branch lifecycle before branch guest saves.
    pub(crate) fn install_restored_domain_state(
        &self,
        checkpoint: CapturedRuntimeState,
    ) -> error_stack::Result<(), BackupStateCaptureError> {
        let Some(store) = self.inner.state_store.as_ref() else {
            return Err(Report::new(BackupStateCaptureError::Unavailable));
        };
        let placement = RuntimeStatePlacement::from_remote(checkpoint.placement)
            .change_context(BackupStateCaptureError::Placement)?;
        if placement.branch_key.as_ref().map(BranchKey::fingerprint)
            != checkpoint.branch_fingerprint
        {
            return Err(Report::new(BackupStateCaptureError::BranchFingerprint));
        }
        store
            .publish_sealed_snapshot(&placement, checkpoint.revision, &checkpoint.payload)
            .change_context(BackupStateCaptureError::Storage)?;
        // A stopped domain may already have built a state handle when its model schedule was
        // published. Its next START must reload the newly installed checkpoint from storage.
        self.clear_runtime_state_for_domain(&placement.domain);
        Ok(())
    }

    /// Publishes volatile state and reads it with durable WASM saves from one database snapshot.
    /// The caller holds the domain cut or accepts a live checkpoint.
    pub(crate) fn capture_backup_state(
        &self,
        domain: &DomainName,
        quiesced: bool,
    ) -> error_stack::Result<Vec<CapturedRuntimeState>, BackupStateCaptureError> {
        let Some(store) = self.inner.state_store.as_ref() else {
            return Err(Report::new(BackupStateCaptureError::Unavailable));
        };
        // Closing first waits for publishers registered before the cut. The forced Kafka and
        // lifecycle snapshots therefore include all of their completed changes, and publishers
        // admitted into the next generation cannot alter this database view.
        let _cut = quiesced.then(|| BackupCaptureFence::close(&self.backup_capture_fence(domain)));
        for state in self.inner.replicated_kafka_offset_states.iter() {
            let placement = state.key();
            if &placement.domain != domain
                || !self.runtime_state_placement_is_assigned_locally(placement)
            {
                continue;
            }
            let read = ReplicatedKafkaOffsetState::read(state.value());
            let snapshot = read
                .latest_snapshot()
                .change_context(BackupStateCaptureError::Storage)?;
            store
                .publish_sealed_snapshot(placement, snapshot.lsm, &snapshot.payload)
                .change_context(BackupStateCaptureError::Storage)?;
        }
        for state in self.inner.replicated_branch_lru_snapshots.iter() {
            let placement = state.key();
            if &placement.domain != domain
                || !self.runtime_state_placement_is_assigned_locally(placement)
            {
                continue;
            }
            store
                .publish_sealed_snapshot(placement, state.value().lsm, &state.value().payload)
                .change_context(BackupStateCaptureError::Storage)?;
        }
        let snapshots = store
            .snapshot_backup_domain(domain)
            .change_context(BackupStateCaptureError::Storage)?;
        let mut keys =
            ahash::HashMap::<(ModelKind, ModelName, BranchKeyFingerprint), BranchKey>::default();
        for (placement, snapshot) in &snapshots {
            if placement.state.kind() != RuntimeStateKind::BranchLru {
                continue;
            }
            let branches =
                decode_branch_lru_snapshot(&snapshot.payload).change_context_lazy(|| {
                    BackupStateCaptureError::Lifecycle {
                        entity: placement.identifier.clone(),
                    }
                })?;
            for branch in branches {
                if let Some(key) = branch.key {
                    keys.insert(
                        (
                            placement.kind,
                            placement.identifier.clone(),
                            key.fingerprint(),
                        ),
                        key,
                    );
                }
            }
        }
        snapshots
            .into_iter()
            .map(|(stored, snapshot)| {
                let placement = restored_placement(domain, stored, &keys)?;
                Ok(CapturedRuntimeState {
                    placement: placement.to_remote(),
                    branch_fingerprint: placement.branch_key.as_ref().map(BranchKey::fingerprint),
                    revision: snapshot.lsm,
                    payload: snapshot.payload,
                })
            })
            .collect()
    }
}

pub(crate) fn decode_backup_kafka_offsets(
    payload: &[u8],
) -> error_stack::Result<Vec<(String, i32, i64)>, BackupStateCaptureError> {
    backup_offset_positions(payload).change_context(BackupStateCaptureError::Storage)
}

pub(crate) fn encode_restored_kafka_offsets(
    offsets: Vec<(String, i32, i64)>,
) -> error_stack::Result<Vec<u8>, BackupStateCaptureError> {
    restore_offset_payload(offsets).change_context(BackupStateCaptureError::Storage)
}

pub(crate) fn decode_backup_branch_lifecycle(
    payload: &[u8],
    entity: &ModelName,
) -> error_stack::Result<Vec<BackupBranchLifecycleEntry>, BackupStateCaptureError> {
    let entries =
        decode_branch_lru_snapshot(payload).change_context(BackupStateCaptureError::Lifecycle {
            entity: entity.clone(),
        })?;
    Ok(entries
        .into_iter()
        .map(|entry| BackupBranchLifecycleEntry {
            key: BranchKey::to_remote_key(&entry.key),
            last_ingestion: entry.last_ingestion,
            incarnation: entry.incarnation,
        })
        .collect())
}

pub(crate) fn encode_restored_branch_lifecycle(
    entries: Vec<BackupBranchLifecycleEntry>,
    entity: &ModelName,
) -> error_stack::Result<Vec<u8>, BackupStateCaptureError> {
    let entries = entries
        .into_iter()
        .map(|entry| {
            Ok(BranchInstanceSnapshotEntry {
                key: BranchKey::from_remote_key(entry.key).change_context(
                    BackupStateCaptureError::Lifecycle {
                        entity: entity.clone(),
                    },
                )?,
                last_ingestion: entry.last_ingestion,
                incarnation: entry.incarnation,
            })
        })
        .collect::<error_stack::Result<Vec<_>, BackupStateCaptureError>>()?;
    encode_branch_lru_snapshot(&entries).change_context(BackupStateCaptureError::Lifecycle {
        entity: entity.clone(),
    })
}

fn restored_placement(
    domain: &DomainName,
    stored: StoredPlacement,
    keys: &ahash::HashMap<(ModelKind, ModelName, BranchKeyFingerprint), BranchKey>,
) -> error_stack::Result<RuntimeStatePlacement, BackupStateCaptureError> {
    let branch_key = match stored.branch {
        Some(fingerprint) => Some(
            keys.get(&(stored.kind, stored.identifier.clone(), fingerprint))
                .cloned()
                .ok_or_else(|| {
                    Report::new(BackupStateCaptureError::MissingBranch {
                        entity: stored.identifier.clone(),
                    })
                })?,
        ),
        None => None,
    };
    Ok(RuntimeStatePlacement {
        domain: domain.clone(),
        state: stored.state,
        kind: stored.kind,
        identifier: stored.identifier,
        branch_key,
    })
}
