//! Capturing the runtime checkpoint kinds and fresh materialized generations at a domain cut.
//!
//! Layer: data plane.
//! - **Owns.** Forcing Kafka and branch-lifecycle publications, then reading one database
//!   snapshot, reattaching typed branch keys from lifecycle checkpoints, and selecting stored
//!   materialized readers for stopped domains or fresh shared Arrow rows under the assignment
//!   barrier for running and paused domains.
//! - **Depends on.** Branch-local state, the runtime state store, and typed placement envelopes.
//! - **Must not know.** Archive records, backup command execution, or restore planning.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "backup capture and stopped-domain restore inspect or install one domain state \
                  generation; checkpoint publishers override this lifecycle default"
    )
)]

use std::io::{Read, Write};

use error_stack::{Report, ResultExt as _};
use nervix_execution::Cancellation;
use nervix_interconnect::StatePlacementEnvelope;
use nervix_models::{
    BranchKeyFingerprint, DomainName, DomainStatus, ModelName, NodeRef, RemoteRuntimeField,
    Timestamp,
};
use thiserror::Error;

use super::{
    BranchKey, MaterializedGeneration, OwnershipHandoffError, OwnershipHandoffResult,
    ReplicatedKafkaOffsetState, ReplicatedMaterializedRelayState, Runtime, RuntimeStateKind,
    RuntimeStatePlacement, ScheduledNodeTask,
    backup_capture_fence::{BackupCaptureFence, BackupPublication},
    branch_lru_state::write_branch_lru_snapshot,
    decode_branch_lru_snapshot,
    kafka_offset_state::{backup_offset_positions, write_offset_payload},
    state_store::{RuntimePersistenceError, StoredPlacement, generation::CheckpointMetadata},
};

#[derive(Debug, Clone)]
pub(crate) struct CapturedRuntimeState {
    pub(crate) placement: StatePlacementEnvelope,
    pub(crate) branch_fingerprint: Option<BranchKeyFingerprint>,
    pub(crate) revision: u64,
    pub(crate) payload: Vec<u8>,
}

/// One domain cut, including the materialized source selected by the domain's lifecycle.
pub(crate) struct CapturedDomainState {
    pub(crate) checkpoints: Vec<CapturedRuntimeState>,
    pub(crate) materialized: CapturedMaterializedState,
}

pub(crate) enum CapturedMaterializedState {
    Current(Vec<CapturedMaterializedRelay>),
    Stored(Vec<CapturedStoredMaterializedRelay>),
}

pub(crate) struct CapturedStoredMaterializedRelay {
    pub(crate) placement: StatePlacementEnvelope,
    pub(crate) checkpoint: super::materialized_snapshot::CapturedMaterializedCheckpoint,
}

#[derive(Debug, Clone)]
pub(crate) struct CapturedMaterializedRelay {
    pub(crate) placement: StatePlacementEnvelope,
    pub(crate) generation: MaterializedGeneration,
    pub(crate) charge: super::Arc<nervix_execution::Reservation>,
}

/// A checkpoint's identity and byte contract, independent of how its bytes are delivered.
#[derive(Debug, Clone)]
pub(crate) struct RestoredRuntimeState {
    pub(crate) placement: StatePlacementEnvelope,
    pub(crate) branch_fingerprint: Option<BranchKeyFingerprint>,
    pub(crate) revision: u64,
    pub(crate) length: u64,
    pub(crate) digest: [u8; 32],
}

pub(crate) use super::state_store::generation::{
    RESTORE_STATE_CHUNK_BYTES, RESTORE_STATE_WORKING_BYTES,
};

#[derive(Debug, Clone)]
pub struct BackupBranchLifecycleEntry {
    pub key: Option<Vec<RemoteRuntimeField>>,
    pub last_ingestion: Timestamp,
    pub incarnation: u64,
}

#[derive(Debug, Error)]
pub enum BackupStateCaptureError {
    #[error("runtime state storage is unavailable")]
    Unavailable,
    #[error("runtime state could not be read or published")]
    Storage,
    #[error("branch lifecycle of processor '{entity}' could not be decoded")]
    Lifecycle { entity: ModelName },
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
            nervix_primitives::task::consume_budget().await;
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

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "checkpoint callbacks register before publishing their retained state"
        )
    )]
    pub(super) fn backup_publication(&self, domain: &DomainName) -> BackupPublication {
        BackupCaptureFence::publication(&self.backup_capture_fence(domain))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "checkpoint callbacks register before publishing their retained state"
        )
    )]
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

    /// Validates and stages one archive checkpoint without changing published runtime state.
    pub(crate) fn stage_restored_domain_state(
        &self,
        authority: &nervix_models::RestoreStateAuthority,
        checkpoint: RestoredRuntimeState,
        reader: impl Read,
        cancellation: &Cancellation,
    ) -> error_stack::Result<(), BackupStateCaptureError> {
        #[cfg(feature = "testing")]
        if (checkpoint.placement.state.kind() == RuntimeStateKind::WasmProcessor
            && self
                .inner
                .fault_injection
                .restored_wasm_checkpoint_fails(&checkpoint.placement.domain))
            || (checkpoint.placement.state.kind() == RuntimeStateKind::MaterializedRelay
                && self
                    .inner
                    .fault_injection
                    .restored_materialized_checkpoint_fails(&checkpoint.placement.domain))
        {
            return Err(Report::new(BackupStateCaptureError::Storage));
        }
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
            .stage_restored_checkpoint(
                authority,
                &placement,
                CheckpointMetadata {
                    lsm: checkpoint.revision,
                    length: checkpoint.length,
                    digest: checkpoint.digest,
                },
                reader,
                || {
                    cancellation
                        .check()
                        .change_context(RuntimePersistenceError::Cancelled)
                },
            )
            .change_context(BackupStateCaptureError::Storage)?;
        Ok(())
    }

    /// The caller holds the applied installation authority across this synchronous publication.
    pub(crate) fn publish_restored_domain_state(
        &self,
        domain: &DomainName,
        authority: &nervix_models::RestoreStateAuthority,
        inventory: nervix_interconnect::backup::RestoreStateInventory,
        cancellation: &Cancellation,
    ) -> error_stack::Result<(), BackupStateCaptureError> {
        let store = self
            .inner
            .state_store
            .as_ref()
            .ok_or_else(|| Report::new(BackupStateCaptureError::Unavailable))?;
        store
            .publish_restored_state(domain, authority, inventory, || {
                cancellation
                    .check()
                    .change_context(RuntimePersistenceError::Cancelled)
            })
            .change_context(BackupStateCaptureError::Storage)?;
        #[cfg(feature = "testing")]
        if self
            .inner
            .fault_injection
            .durable_restore_publication_fails(domain)
        {
            return Err(Report::new(BackupStateCaptureError::Storage));
        }
        self.clear_runtime_state_for_domain(domain);
        Ok(())
    }

    pub(crate) fn reclaim_restore_checkpoint_staging(
        &self,
        retains: impl Fn(&DomainName, u64) -> bool,
        cancellation: &Cancellation,
    ) -> error_stack::Result<crate::metrics::RestoreStagingObservation, BackupStateCaptureError>
    {
        let Some(store) = self.inner.state_store.as_ref() else {
            return Ok(crate::metrics::RestoreStagingObservation::default());
        };
        store
            .reclaim_restore_staging(retains, || {
                cancellation
                    .check()
                    .change_context(RuntimePersistenceError::Cancelled)
            })
            .change_context(BackupStateCaptureError::Storage)
    }

    /// Publishes volatile state and reads it with durable WASM saves from one database snapshot.
    /// The caller holds the domain cut or accepts a live checkpoint.
    pub(crate) fn capture_backup_state(
        &self,
        domain: &DomainName,
        quiesced: bool,
        status: DomainStatus,
    ) -> error_stack::Result<CapturedDomainState, BackupStateCaptureError> {
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
        for state in self.inner.replicated_branch_lifecycles.iter() {
            let placement = state.key();
            if &placement.domain != domain
                || !self.runtime_state_placement_is_assigned_locally(placement)
            {
                continue;
            }
            let Some(checkpoint) = state.value().latest() else {
                continue;
            };
            let snapshot = checkpoint.snapshot();
            store
                .publish_sealed_snapshot(placement, snapshot.lsm, &snapshot.payload)
                .change_context(BackupStateCaptureError::Storage)?;
        }
        let kinds: &[RuntimeStateKind] = match status {
            DomainStatus::Stopped => &[
                RuntimeStateKind::WasmProcessor,
                RuntimeStateKind::KafkaOffset,
                RuntimeStateKind::BranchLru,
                RuntimeStateKind::MaterializedRelay,
            ],
            DomainStatus::Running | DomainStatus::Paused => &[
                RuntimeStateKind::WasmProcessor,
                RuntimeStateKind::KafkaOffset,
                RuntimeStateKind::BranchLru,
            ],
        };
        let stored_snapshots = store
            .snapshot_backup_domain(domain, kinds)
            .change_context(BackupStateCaptureError::Storage)?;
        let mut snapshots = Vec::new();
        for (placement, snapshot) in stored_snapshots.checkpoints {
            let node = NodeRef::new(placement.kind, placement.identifier.clone()).in_domain(domain);
            let Some(slot) = self.inner.state_identities.get(&node) else {
                continue;
            };
            let Some(assignment) = slot.load_full() else {
                continue;
            };
            if assignment
                .identity
                .names(placement.state, placement.branch.as_ref())
            {
                snapshots.push((placement, snapshot));
            }
        }
        let mut keys =
            ahash::HashMap::<(NodeRef, Option<BranchKeyFingerprint>), Option<BranchKey>>::default();
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
                keys.insert(
                    (
                        NodeRef::new(placement.kind, placement.identifier.clone()),
                        branch.key.as_ref().map(BranchKey::fingerprint),
                    ),
                    branch.key,
                );
            }
        }
        let mut captured = Vec::new();
        for (stored, snapshot) in snapshots {
            let Some(placement) = restored_placement(domain, stored, &keys) else {
                continue;
            };
            captured.push(CapturedRuntimeState {
                placement: placement.to_remote(),
                branch_fingerprint: placement.branch_key.as_ref().map(BranchKey::fingerprint),
                revision: snapshot.lsm,
                payload: snapshot.payload,
            });
        }
        let materialized = match status {
            DomainStatus::Stopped => {
                let mut stored_materialized = Vec::new();
                for (placement, reader) in stored_snapshots.materialized {
                    let node = NodeRef::new(placement.kind, placement.identifier.clone())
                        .in_domain(domain);
                    let Some(slot) = self.inner.state_identities.get(&node) else {
                        continue;
                    };
                    let Some(assignment) = slot.load_full() else {
                        continue;
                    };
                    if !assignment
                        .identity
                        .names(placement.state, placement.branch.as_ref())
                    {
                        continue;
                    }
                    let placement = RuntimeStatePlacement {
                        domain: domain.clone(),
                        state: placement.state,
                        kind: placement.kind,
                        identifier: placement.identifier,
                        branch_key: None,
                    };
                    stored_materialized.push(CapturedStoredMaterializedRelay {
                        placement: placement.to_remote(),
                        checkpoint:
                            super::materialized_snapshot::CapturedMaterializedCheckpoint::new(
                                self.executor().clone(),
                                reader,
                            ),
                    });
                }
                CapturedMaterializedState::Stored(stored_materialized)
            }
            DomainStatus::Running | DomainStatus::Paused => {
                let mut materialized = Vec::new();
                for state in self.inner.replicated_materialized_stream_states.iter() {
                    let placement = state.key();
                    if &placement.domain != domain
                        || !self.runtime_state_placement_is_assigned_locally(placement)
                    {
                        continue;
                    }
                    let (generation, charge) =
                        ReplicatedMaterializedRelayState::read(state.value())
                            .capture_for_backup(self.executor())
                            .change_context(BackupStateCaptureError::Storage)?;
                    materialized.push(CapturedMaterializedRelay {
                        placement: placement.to_remote(),
                        generation,
                        charge: super::Arc::new(charge),
                    });
                }
                CapturedMaterializedState::Current(materialized)
            }
        };
        Ok(CapturedDomainState {
            checkpoints: captured,
            materialized,
        })
    }
}

pub(crate) fn decode_backup_kafka_offsets(
    payload: &[u8],
) -> error_stack::Result<Vec<(String, i32, i64)>, BackupStateCaptureError> {
    backup_offset_positions(payload).change_context(BackupStateCaptureError::Storage)
}

pub(crate) fn write_restored_kafka_offsets(
    offsets: impl ExactSizeIterator<Item = (String, i32, i64)> + Clone,
    writer: &mut dyn Write,
    cancellation: &Cancellation,
) -> error_stack::Result<(), BackupStateCaptureError> {
    write_offset_payload(offsets, writer, cancellation)
        .change_context(BackupStateCaptureError::Storage)
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

pub(crate) fn write_restored_branch_lifecycle(
    entries: impl ExactSizeIterator<Item = BackupBranchLifecycleEntry> + Clone,
    entity: &ModelName,
    writer: &mut dyn Write,
    cancellation: &Cancellation,
) -> error_stack::Result<(), BackupStateCaptureError> {
    write_branch_lru_snapshot(entries, writer, cancellation).change_context(
        BackupStateCaptureError::Lifecycle {
            entity: entity.clone(),
        },
    )
}

fn restored_placement(
    domain: &DomainName,
    stored: StoredPlacement,
    keys: &ahash::HashMap<(NodeRef, Option<BranchKeyFingerprint>), Option<BranchKey>>,
) -> Option<RuntimeStatePlacement> {
    let branch_key = if stored.state.kind() == RuntimeStateKind::WasmProcessor {
        // Eviction removes the lifecycle entry, while its durable guest checkpoint may remain.
        // Only a currently active execution has a typed key worth reconstructing for this cut.
        keys.get(&(
            NodeRef::new(stored.kind, stored.identifier.clone()),
            stored.branch,
        ))?
        .clone()
    } else {
        None
    };
    Some(RuntimeStatePlacement {
        domain: domain.clone(),
        state: stored.state,
        kind: stored.kind,
        identifier: stored.identifier,
        branch_key,
    })
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_interconnect::RuntimeState;
    use nervix_models::{ModelKind, SchemaFingerprint, WasmStateGeneration};

    use super::*;
    use crate::runtime::encode_branch_lru_snapshot;

    #[nervix_primitives::test]
    async fn backup_omits_a_guest_checkpoint_after_its_branch_leaves_the_lifecycle() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let db = fjall::Database::builder(dir.path())
            .open()
            .assured("database opens");
        let runtime = Runtime::with_persistence(Some(db), std::time::Duration::from_secs(60))
            .assured("runtime opens");
        let store = runtime
            .inner
            .state_store
            .as_ref()
            .assured("state store is configured");
        let domain = DomainName::parse("orders").assured("domain is valid");
        let entity = ModelName::parse("accumulator").assured("processor is valid");
        let schema = SchemaFingerprint::from_digest([4; 32]);
        runtime.publish_state_assignment(
            nervix_models::DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::WasmProcessor,
                entity.clone(),
            ),
            super::super::ScheduledStateAssignment {
                identity: super::super::ScheduledStateIdentity {
                    schema_fingerprint: schema,
                    wasm_state_generations: Some(nervix_models::WasmStateGenerations::first()),
                },
                checkpoint_owners: None,
            },
        );
        let key = super::super::string_branch_key("tenant", "alpha");
        let lifecycle = RuntimeStatePlacement {
            domain: domain.clone(),
            kind: ModelKind::WasmProcessor,
            identifier: entity,
            state: RuntimeState::BranchLru { schema },
            branch_key: None,
        };
        let guest = RuntimeStatePlacement {
            state: RuntimeState::WasmProcessor {
                schema,
                generation: WasmStateGeneration::FIRST,
            },
            branch_key: key,
            ..lifecycle.clone()
        };
        store
            .publish_sealed_snapshot(&guest, 8, b"saved-alpha")
            .assured("guest persists");
        store
            .publish_sealed_snapshot(
                &lifecycle,
                9,
                &encode_branch_lru_snapshot(&[]).assured("empty current lifecycle encodes"),
            )
            .assured("eviction persists");
        let captured = runtime
            .capture_backup_state(&domain, true, DomainStatus::Stopped)
            .assured("a retained inactive guest does not fail the cut");
        assert_eq!(
            captured.checkpoints.len(),
            1,
            "the scheduled current lifecycle is retained"
        );
        assert!(
            captured
                .checkpoints
                .iter()
                .all(|entry| entry.placement.state.kind() != RuntimeStateKind::WasmProcessor)
        );
        runtime.clear_state_identities(&domain);
        assert!(
            runtime
                .capture_backup_state(&domain, true, DomainStatus::Stopped)
                .assured("removed entities do not contribute retained checkpoints")
                .checkpoints
                .is_empty()
        );
    }
}
