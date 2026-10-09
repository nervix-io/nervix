//! Capturing the runtime checkpoint kinds and fresh materialized generations at a domain cut.
//!
//! Layer: data plane.
//! - **Owns.** Forcing Kafka and branch-lifecycle publications, then listing one database
//!   snapshot and reading only the checkpoints the cut archives, reattaching typed branch keys
//!   from lifecycle checkpoints for the branches that need them, handing Kafka offset and
//!   lifecycle checkpoints on in that snapshot and reading each one whole under its own admitted
//!   charge, selecting stored materialized readers for stopped domains or fresh shared Arrow rows
//!   under the assignment barrier for running and paused domains, and taking each active
//!   deduplicator and window branch from the generation its branch task published or, without
//!   one, from its checkpoint.
//! - **Depends on.** Branch-local state, the runtime state store, the bounded executor's memory
//!   classes, and typed placement envelopes.
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

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use nervix_execution::{Cancellation, MemoryClass};
use nervix_interconnect::StatePlacementEnvelope;
use nervix_models::{
    BranchKeyFingerprint, DomainName, DomainStatus, ModelName, NodeRef, RemoteRuntimeField,
    Timestamp,
};
use thiserror::Error;

use super::{
    BranchInstanceSnapshotEntry, BranchKey, CapturedDeduplicatorKeyspace, CapturedWindow,
    MaterializedGeneration, OwnershipHandoffError, OwnershipHandoffResult,
    ReplicatedKafkaOffsetState, ReplicatedMaterializedRelayState, Runtime, RuntimeStateKind,
    RuntimeStatePlacement, ScheduledNodeTask,
    backup_capture_fence::{BackupCaptureFence, BackupPublication},
    branch_lru_state::{LifecycleBranches, NativeBranchLifecycle, write_branch_lru_snapshot},
    decode_branch_lru_snapshot,
    kafka_offset_state::{
        KafkaOffsetPositions, NativeKafkaOffsets, backup_offset_positions, write_offset_payload,
    },
    state_store::{
        RuntimePersistenceError, StoredPlacement,
        checkpoint_reader::{AlignedCheckpoint, ListedCheckpoint},
        generation::CheckpointMetadata,
    },
};

/// One WASM branch's durable guest save at the cut, read whole.
#[derive(Debug, Clone)]
pub(crate) struct CapturedGuestSave {
    pub(crate) placement: StatePlacementEnvelope,
    pub(crate) branch_fingerprint: Option<BranchKeyFingerprint>,
    pub(crate) revision: u64,
    pub(crate) payload: Vec<u8>,
}

/// One domain cut, including the materialized source selected by the domain's lifecycle.
pub(crate) struct CapturedDomainState {
    pub(crate) guest_saves: Vec<CapturedGuestSave>,
    /// The Kafka offset and branch lifecycle checkpoints, still in the cut's database snapshot.
    pub(crate) native_metadata: Vec<CapturedNativeMetadata>,
    pub(crate) materialized: CapturedMaterializedState,
    /// The deduplicator keyspaces and windows of the branches the captured lifecycles hold.
    pub(crate) branch_states: Vec<CapturedBranchState>,
}

/// A Kafka offset or branch lifecycle checkpoint of the cut. It stays in the cut's database
/// snapshot until its section is written, and is then read whole under an admitted charge, one
/// checkpoint at a time, rather than with the rest of the cut.
pub(crate) struct CapturedNativeMetadata {
    pub(crate) placement: StatePlacementEnvelope,
    checkpoint: ListedCheckpoint,
}

impl CapturedNativeMetadata {
    /// The memory reading this checkpoint and converting it takes, which its reader's
    /// `restore_metadata` charge must cover.
    pub(crate) fn conversion_bytes(&self) -> error_stack::Result<u64, BackupStateCaptureError> {
        native_conversion_bytes(self.checkpoint.stored_bytes()).ok_or_else(|| {
            Report::new(BackupStateCaptureError::Admission {
                entity: self.checkpoint.placement.identifier.clone(),
            })
        })
    }

    /// Reads and validates a branch lifecycle checkpoint. The caller holds its conversion charge.
    pub(crate) fn read_branch_lifecycle(
        &self,
        cancellation: &Cancellation,
    ) -> error_stack::Result<NativeLifecycleCheckpoint, BackupStateCaptureError> {
        read_native_lifecycle(&self.checkpoint, cancellation)
    }

    /// Reads and validates a Kafka offset checkpoint and orders its partitions. The caller holds
    /// its conversion charge.
    pub(crate) fn read_kafka_offsets(
        &self,
        cancellation: &Cancellation,
    ) -> error_stack::Result<NativeKafkaCheckpoint, BackupStateCaptureError> {
        let entity = &self.checkpoint.placement.identifier;
        let aligned = self
            .checkpoint
            .read_aligned(|| {
                cancellation
                    .check()
                    .change_context(RuntimePersistenceError::Cancelled)
            })
            .change_context_lazy(|| BackupStateCaptureError::KafkaOffsets {
                entity: entity.clone(),
            })?;
        NativeKafkaCheckpoint::validate(aligned, entity, cancellation)
    }
}

/// A stored branch lifecycle checkpoint read for one section: its revision and its validated
/// branches.
pub(crate) struct NativeLifecycleCheckpoint {
    pub(crate) revision: u64,
    lifecycle: NativeBranchLifecycle,
}

impl NativeLifecycleCheckpoint {
    /// Validates the lifecycle checkpoint of `entity` read into alignment.
    pub(in crate::runtime) fn validate(
        checkpoint: AlignedCheckpoint,
        entity: &ModelName,
        cancellation: &Cancellation,
    ) -> error_stack::Result<Self, BackupStateCaptureError> {
        let lifecycle = NativeBranchLifecycle::validate(checkpoint.payload, cancellation)
            .change_context_lazy(|| BackupStateCaptureError::Lifecycle {
                entity: entity.clone(),
            })?;
        Ok(Self {
            revision: checkpoint.lsm,
            lifecycle,
        })
    }

    /// Hands `visit` the branches in LRU order, through an iterator it may clone and walk again.
    pub(crate) fn with_branches<R>(
        &self,
        visit: impl FnOnce(BackupLifecycleBranches<'_>) -> R,
    ) -> R {
        self.lifecycle
            .with_branches(|branches| visit(BackupLifecycleBranches(branches)))
    }
}

/// The branches of a captured lifecycle in LRU order, each converted when it is reached.
#[derive(Clone)]
pub(crate) struct BackupLifecycleBranches<'a>(LifecycleBranches<'a>);

impl Iterator for BackupLifecycleBranches<'_> {
    type Item = BackupBranchLifecycleEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let branch = self.0.next()?;
        Some(branch.stored())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for BackupLifecycleBranches<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// A stored Kafka offset checkpoint read for one section: its revision and its validated
/// partitions in topic and partition order.
pub(crate) struct NativeKafkaCheckpoint {
    pub(crate) revision: u64,
    offsets: NativeKafkaOffsets,
}

impl NativeKafkaCheckpoint {
    /// Validates the Kafka offset checkpoint of `entity` read into alignment, and orders its
    /// partitions.
    pub(in crate::runtime) fn validate(
        checkpoint: AlignedCheckpoint,
        entity: &ModelName,
        cancellation: &Cancellation,
    ) -> error_stack::Result<Self, BackupStateCaptureError> {
        let offsets = NativeKafkaOffsets::validate(checkpoint.payload, || {
            cancellation
                .check()
                .change_context(RuntimePersistenceError::Cancelled)
        })
        .change_context_lazy(|| BackupStateCaptureError::KafkaOffsets {
            entity: entity.clone(),
        })?;
        Ok(Self {
            revision: checkpoint.lsm,
            offsets,
        })
    }

    /// Hands `visit` every partition's position in topic and partition order, through an
    /// iterator it may clone and walk again.
    pub(crate) fn with_positions<R>(&self, visit: impl FnOnce(KafkaOffsetPositions<'_>) -> R) -> R {
        self.offsets.with_positions(visit)
    }
}

/// One active branch's deduplicator keyspace or window at the cut.
pub(crate) struct CapturedBranchState {
    pub(crate) placement: StatePlacementEnvelope,
    pub(crate) branch_fingerprint: Option<BranchKeyFingerprint>,
    pub(crate) state: CapturedBranchStateKind,
}

pub(crate) enum CapturedBranchStateKind {
    Deduplicator(CapturedDeduplicatorKeyspace),
    Window(CapturedWindow),
}

/// A branch-local state's entity and branch, which names it once in a cut.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BranchStateIdentity {
    node: NodeRef,
    branch: Option<BranchKeyFingerprint>,
}

impl BranchStateIdentity {
    fn of_stored(stored: &StoredPlacement) -> Self {
        Self {
            node: NodeRef::new(stored.kind, stored.identifier.clone()),
            branch: stored.branch,
        }
    }
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

impl From<BranchInstanceSnapshotEntry<Option<BranchKey>>> for BackupBranchLifecycleEntry {
    fn from(branch: BranchInstanceSnapshotEntry<Option<BranchKey>>) -> Self {
        Self {
            key: BranchKey::to_remote_key(&branch.key),
            last_ingestion: branch.last_ingestion,
            incarnation: branch.incarnation,
        }
    }
}

/// One Kafka source partition's next offset in a captured checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackupKafkaPartitionOffset {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) next_offset: i64,
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
    #[error("the deduplicator keyspace of '{entity}' could not be read")]
    Keyspace { entity: ModelName },
    #[error("the Kafka domain offsets of ingestor '{entity}' could not be read")]
    KafkaOffsets { entity: ModelName },
    #[error("reading the native metadata of '{entity}' could not be admitted")]
    Admission { entity: ModelName },
    #[error("the cut was cancelled between bounded units")]
    Cancelled,
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
        {
            let faults = &self.inner.fault_injection;
            let domain = &checkpoint.placement.domain;
            let injected = match checkpoint.placement.state.kind() {
                RuntimeStateKind::WasmProcessor => faults.restored_wasm_checkpoint_fails(domain),
                RuntimeStateKind::MaterializedRelay => {
                    faults.restored_materialized_checkpoint_fails(domain)
                }
                RuntimeStateKind::Deduplicator | RuntimeStateKind::WindowProcessor => {
                    faults.restored_branch_state_checkpoint_fails(domain)
                }
                RuntimeStateKind::BranchAggregated
                | RuntimeStateKind::Correlator
                | RuntimeStateKind::KafkaOffset
                | RuntimeStateKind::BranchLru => false,
            };
            if injected {
                return Err(Report::new(BackupStateCaptureError::Storage));
            }
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
        cancellation: &Cancellation,
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
                RuntimeStateKind::Deduplicator,
                RuntimeStateKind::WindowProcessor,
            ],
            DomainStatus::Running | DomainStatus::Paused => &[
                RuntimeStateKind::WasmProcessor,
                RuntimeStateKind::KafkaOffset,
                RuntimeStateKind::BranchLru,
                RuntimeStateKind::Deduplicator,
                RuntimeStateKind::WindowProcessor,
            ],
        };
        let listed = store
            .snapshot_backup_domain(domain, kinds)
            .change_context(BackupStateCaptureError::Storage)?;
        let mut current = Vec::new();
        for checkpoint in listed.checkpoints {
            if self.names_stored_state(domain, &checkpoint.placement) {
                current.push(checkpoint);
            }
        }
        // A branch-local checkpoint takes its typed key from its entity's captured lifecycle. Only
        // the branches with such a checkpoint, or with a published generation, are looked up.
        let mut needed = ahash::HashSet::default();
        for checkpoint in &current {
            if checkpoint.placement.state.kind().is_branch_local() {
                needed.insert(BranchStateIdentity::of_stored(&checkpoint.placement));
            }
        }
        for identity in self.published_branch_state_identities(domain) {
            needed.insert(identity);
        }
        let keys = self.lifecycle_keys(&current, &needed, cancellation)?;
        let mut branch_states = self.capture_published_branch_states(domain, &keys);
        let mut published = ahash::HashSet::default();
        for captured in &branch_states {
            published.insert(BranchStateIdentity {
                node: NodeRef::new(
                    captured.placement.kind,
                    captured.placement.identifier.clone(),
                ),
                branch: captured.branch_fingerprint,
            });
        }
        let mut guest_saves = Vec::new();
        let mut native_metadata = Vec::new();
        let mut stored_materialized = Vec::new();
        for checkpoint in current {
            let kind = checkpoint.placement.state.kind();
            let publishes = matches!(
                kind,
                RuntimeStateKind::Deduplicator | RuntimeStateKind::WindowProcessor
            );
            // A stored keyspace or window that a published generation supersedes is never read.
            if publishes
                && published.contains(&BranchStateIdentity::of_stored(&checkpoint.placement))
            {
                continue;
            }
            let Some(placement) = restored_placement(domain, &checkpoint.placement, &keys) else {
                continue;
            };
            let branch_fingerprint = placement.branch_key.as_ref().map(BranchKey::fingerprint);
            match kind {
                RuntimeStateKind::KafkaOffset | RuntimeStateKind::BranchLru => {
                    native_metadata.push(CapturedNativeMetadata {
                        placement: placement.to_remote(),
                        checkpoint,
                    });
                }
                RuntimeStateKind::WasmProcessor => {
                    let snapshot = checkpoint
                        .read_entry()
                        .change_context(BackupStateCaptureError::Storage)?;
                    guest_saves.push(CapturedGuestSave {
                        placement: placement.to_remote(),
                        branch_fingerprint,
                        revision: snapshot.lsm,
                        payload: snapshot.payload,
                    });
                }
                RuntimeStateKind::Deduplicator => {
                    let snapshot = checkpoint
                        .read_entry()
                        .change_context(BackupStateCaptureError::Storage)?;
                    let keyspace =
                        CapturedDeduplicatorKeyspace::stored(snapshot.lsm, &snapshot.payload)
                            .change_context_lazy(|| BackupStateCaptureError::Keyspace {
                                entity: placement.identifier.clone(),
                            })?;
                    branch_states.push(CapturedBranchState {
                        placement: placement.to_remote(),
                        branch_fingerprint,
                        state: CapturedBranchStateKind::Deduplicator(keyspace),
                    });
                }
                RuntimeStateKind::WindowProcessor => {
                    let snapshot = checkpoint
                        .read_entry()
                        .change_context(BackupStateCaptureError::Storage)?;
                    branch_states.push(CapturedBranchState {
                        placement: placement.to_remote(),
                        branch_fingerprint,
                        state: CapturedBranchStateKind::Window(CapturedWindow::stored(
                            snapshot.lsm,
                            snapshot.payload,
                        )),
                    });
                }
                RuntimeStateKind::MaterializedRelay => {
                    let reader = checkpoint
                        .open()
                        .change_context(BackupStateCaptureError::Storage)?;
                    stored_materialized.push(CapturedStoredMaterializedRelay {
                        placement: placement.to_remote(),
                        checkpoint:
                            super::materialized_snapshot::CapturedMaterializedCheckpoint::new(
                                self.executor().clone(),
                                reader,
                            ),
                    });
                }
                RuntimeStateKind::BranchAggregated | RuntimeStateKind::Correlator => {}
            }
        }
        let materialized = match status {
            DomainStatus::Stopped => CapturedMaterializedState::Stored(stored_materialized),
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
            guest_saves,
            native_metadata,
            materialized,
            branch_states,
        })
    }

    /// Whether a stored checkpoint of `domain` is state its entity's current identity names.
    fn names_stored_state(&self, domain: &DomainName, stored: &StoredPlacement) -> bool {
        let node = NodeRef::new(stored.kind, stored.identifier.clone()).in_domain(domain);
        let Some(slot) = self.inner.state_identities.get(&node) else {
            return false;
        };
        let Some(assignment) = slot.load_full() else {
            return false;
        };
        assignment
            .identity
            .names(stored.state, stored.branch.as_ref())
    }

    /// The typed keys of the `needed` branches, read from the captured lifecycles of `current`.
    /// A lifecycle is read only when one of its branches is needed, one lifecycle at a time and
    /// under its own conversion charge, and only the needed keys are kept. A branch its lifecycle
    /// no longer holds was evicted and has no key.
    fn lifecycle_keys(
        &self,
        current: &[ListedCheckpoint],
        needed: &ahash::HashSet<BranchStateIdentity>,
        cancellation: &Cancellation,
    ) -> error_stack::Result<
        ahash::HashMap<BranchStateIdentity, Option<BranchKey>>,
        BackupStateCaptureError,
    > {
        let mut nodes = ahash::HashSet::default();
        for identity in needed {
            nodes.insert(&identity.node);
        }
        let mut keys = ahash::HashMap::default();
        for checkpoint in current {
            let stored = &checkpoint.placement;
            let node = NodeRef::new(stored.kind, stored.identifier.clone());
            if stored.state.kind() != RuntimeStateKind::BranchLru || !nodes.contains(&node) {
                continue;
            }
            let entity = || BackupStateCaptureError::Lifecycle {
                entity: stored.identifier.clone(),
            };
            let Some(conversion) = native_conversion_bytes(checkpoint.stored_bytes()) else {
                return Err(Report::new(entity()));
            };
            let _charge = self
                .executor()
                .try_reserve(MemoryClass::RestoreMetadata, conversion)
                .change_context_lazy(|| BackupStateCaptureError::Admission {
                    entity: stored.identifier.clone(),
                })?;
            let lifecycle = read_native_lifecycle(checkpoint, cancellation)?;
            lifecycle.lifecycle.with_branches(|branches| {
                for branch in branches {
                    cancellation
                        .check()
                        .change_context(BackupStateCaptureError::Cancelled)?;
                    let branch = branch.typed();
                    let identity = BranchStateIdentity {
                        node: node.clone(),
                        branch: branch.key.as_ref().map(BranchKey::fingerprint),
                    };
                    if needed.contains(&identity) {
                        keys.insert(identity, branch.key);
                    }
                }
                Ok::<_, Report<BackupStateCaptureError>>(())
            })?;
        }
        Ok(keys)
    }

    /// The identities of the deduplicator and window generations this node's branch tasks of
    /// `domain` published, whichever branches the cut's lifecycles hold.
    fn published_branch_state_identities(&self, domain: &DomainName) -> Vec<BranchStateIdentity> {
        let mut identities = Vec::new();
        for state in self.inner.replicated_deduplicator_states.iter() {
            if let Some(identity) = self.names_published_branch_state(domain, state.key()) {
                identities.push(identity);
            }
        }
        for state in self.inner.replicated_window_processor_states.iter() {
            if let Some(identity) = self.names_published_branch_state(domain, state.key()) {
                identities.push(identity);
            }
        }
        identities
    }

    /// The generation each deduplicator and window branch task of `domain` on this node published
    /// last, for every branch the captured lifecycles in `keys` hold. A quiesced cut asked those
    /// tasks to publish before it closed, so each generation holds every change before the cut.
    fn capture_published_branch_states(
        &self,
        domain: &DomainName,
        keys: &ahash::HashMap<BranchStateIdentity, Option<BranchKey>>,
    ) -> Vec<CapturedBranchState> {
        let mut deduplicators = Vec::new();
        for state in self.inner.replicated_deduplicator_states.iter() {
            if self.captures_published_branch_state(domain, state.key(), keys) {
                deduplicators.push(state.value().clone());
            }
        }
        let mut windows = Vec::new();
        for state in self.inner.replicated_window_processor_states.iter() {
            if self.captures_published_branch_state(domain, state.key(), keys) {
                windows.push(state.value().clone());
            }
        }
        let mut captured = Vec::with_capacity(deduplicators.len() + windows.len());
        for state in deduplicators {
            captured.push(CapturedBranchState {
                placement: state.placement.to_remote(),
                branch_fingerprint: state
                    .placement
                    .branch_key
                    .as_ref()
                    .map(BranchKey::fingerprint),
                state: CapturedBranchStateKind::Deduplicator(
                    CapturedDeduplicatorKeyspace::published(&state),
                ),
            });
        }
        for state in windows {
            let generation = state.generations.load();
            let Some(window) = CapturedWindow::published(&generation) else {
                continue;
            };
            captured.push(CapturedBranchState {
                placement: state.placement.to_remote(),
                branch_fingerprint: state
                    .placement
                    .branch_key
                    .as_ref()
                    .map(BranchKey::fingerprint),
                state: CapturedBranchStateKind::Window(window),
            });
        }
        captured
    }

    /// Whether a published branch state belongs to this cut: it is this node's state of `domain`,
    /// named by its entity's current identity, for a branch a captured lifecycle holds.
    fn captures_published_branch_state(
        &self,
        domain: &DomainName,
        placement: &RuntimeStatePlacement,
        keys: &ahash::HashMap<BranchStateIdentity, Option<BranchKey>>,
    ) -> bool {
        let Some(identity) = self.names_published_branch_state(domain, placement) else {
            return false;
        };
        keys.contains_key(&identity)
    }

    /// The identity of a published branch state that is this node's state of `domain`, named by
    /// its entity's current identity.
    fn names_published_branch_state(
        &self,
        domain: &DomainName,
        placement: &RuntimeStatePlacement,
    ) -> Option<BranchStateIdentity> {
        if &placement.domain != domain
            || !self.runtime_state_placement_is_assigned_locally(placement)
        {
            return None;
        }
        let branch = placement.branch_key.as_ref().map(BranchKey::fingerprint);
        let node = NodeRef::new(placement.kind, placement.identifier.clone());
        let slot = self
            .inner
            .state_identities
            .get(&node.clone().in_domain(domain))?;
        let assignment = slot.load_full()?;
        if !assignment.identity.names(placement.state, branch.as_ref()) {
            return None;
        }
        Some(BranchStateIdentity { node, branch })
    }
}

/// The memory reading one native checkpoint into alignment and converting it takes: an inline
/// payload and its aligned copy at once, or a segmented payload beside the stored chunk being
/// copied, and afterwards the payload with the order of a Kafka checkpoint's partitions, which is
/// never larger than the payload. Absent when that is not addressable.
fn native_conversion_bytes(stored_bytes: u64) -> Option<u64> {
    let chunk: u64 = RESTORE_STATE_CHUNK_BYTES.arch_into();
    stored_bytes.checked_mul(2)?.checked_add(chunk)
}

fn read_native_lifecycle(
    checkpoint: &ListedCheckpoint,
    cancellation: &Cancellation,
) -> error_stack::Result<NativeLifecycleCheckpoint, BackupStateCaptureError> {
    let entity = &checkpoint.placement.identifier;
    let aligned = checkpoint
        .read_aligned(|| {
            cancellation
                .check()
                .change_context(RuntimePersistenceError::Cancelled)
        })
        .change_context_lazy(|| BackupStateCaptureError::Lifecycle {
            entity: entity.clone(),
        })?;
    NativeLifecycleCheckpoint::validate(aligned, entity, cancellation)
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
        .map(BackupBranchLifecycleEntry::from)
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
    stored: &StoredPlacement,
    keys: &ahash::HashMap<BranchStateIdentity, Option<BranchKey>>,
) -> Option<RuntimeStatePlacement> {
    let branch_key = if stored.state.kind().is_branch_local() {
        // Eviction removes the lifecycle entry, while a branch's durable checkpoint may remain.
        // Only a currently active execution has a typed key worth reconstructing for this cut.
        keys.get(&BranchStateIdentity::of_stored(stored))?.clone()
    } else {
        None
    };
    Some(RuntimeStatePlacement {
        domain: domain.clone(),
        state: stored.state,
        kind: stored.kind,
        identifier: stored.identifier.clone(),
        branch_key,
    })
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "branch_state_capture_shuttle_tests.rs"]
mod branch_state_shuttle_tests;

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_execution::StorageClass;
    use nervix_interconnect::RuntimeState;
    use nervix_models::{ModelKind, SchemaFingerprint, WasmStateGeneration};

    use super::*;
    use crate::runtime::encode_branch_lru_snapshot;

    /// Runs `work` as one admitted storage job, as the owner's capture runs it.
    async fn in_storage_job<T: Send + 'static>(
        runtime: &Runtime,
        work: impl FnOnce(&Cancellation) -> T + Send + 'static,
    ) -> T {
        let executor = runtime.executor().clone();
        let reservation = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .assured("the storage job is admitted");
        executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| work(cancellation),
            )
            .await
            .assured("the storage job runs to completion")
    }

    async fn capture(
        runtime: &Runtime,
        domain: &DomainName,
    ) -> error_stack::Result<CapturedDomainState, BackupStateCaptureError> {
        let capturing = runtime.clone();
        let domain = domain.clone();
        in_storage_job(runtime, move |cancellation| {
            capturing.capture_backup_state(&domain, true, DomainStatus::Stopped, cancellation)
        })
        .await
    }

    fn schedule(runtime: &Runtime, domain: &DomainName, kind: ModelKind, entity: &ModelName) {
        runtime.publish_state_assignment(
            nervix_models::DomainNodeRef::node_in(domain.clone(), kind, entity.clone()),
            super::super::ScheduledStateAssignment {
                identity: super::super::ScheduledStateIdentity {
                    schema_fingerprint: SchemaFingerprint::from_digest([4; 32]),
                    wasm_state_generations: Some(nervix_models::WasmStateGenerations::first()),
                },
                checkpoint_owners: None,
            },
        );
    }

    fn persisted_runtime(dir: &std::path::Path) -> Runtime {
        let db = fjall::Database::builder(dir)
            .open()
            .assured("database opens");
        Runtime::with_persistence(Some(db), std::time::Duration::from_secs(60))
            .assured("runtime opens")
    }

    fn lifecycle_of(
        domain: &DomainName,
        kind: ModelKind,
        entity: &ModelName,
    ) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: domain.clone(),
            kind,
            identifier: entity.clone(),
            state: RuntimeState::BranchLru {
                schema: SchemaFingerprint::from_digest([4; 32]),
            },
            branch_key: None,
        }
    }

    fn guest_of(lifecycle: &RuntimeStatePlacement, tenant: &str) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            state: RuntimeState::WasmProcessor {
                schema: SchemaFingerprint::from_digest([4; 32]),
                generation: WasmStateGeneration::FIRST,
            },
            branch_key: super::super::string_branch_key("tenant", tenant),
            ..lifecycle.clone()
        }
    }

    fn branch(tenant: &str, incarnation: u64) -> BranchInstanceSnapshotEntry<Option<BranchKey>> {
        BranchInstanceSnapshotEntry {
            key: super::super::string_branch_key("tenant", tenant),
            last_ingestion: Timestamp::from_unix_nanos(
                1_000 + i64::try_from(incarnation).assured("a small incarnation fits"),
            ),
            incarnation,
        }
    }

    #[nervix_primitives::test]
    async fn backup_omits_a_guest_checkpoint_after_its_branch_leaves_the_lifecycle() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let runtime = persisted_runtime(dir.path());
        let store = runtime
            .inner
            .state_store
            .as_ref()
            .assured("state store is configured");
        let domain = DomainName::parse("orders").assured("domain is valid");
        let entity = ModelName::parse("accumulator").assured("processor is valid");
        schedule(&runtime, &domain, ModelKind::WasmProcessor, &entity);
        let lifecycle = lifecycle_of(&domain, ModelKind::WasmProcessor, &entity);
        store
            .publish_sealed_snapshot(&guest_of(&lifecycle, "alpha"), 8, b"saved-alpha")
            .assured("guest persists");
        store
            .publish_sealed_snapshot(
                &lifecycle,
                9,
                &encode_branch_lru_snapshot(&[]).assured("empty current lifecycle encodes"),
            )
            .assured("eviction persists");
        let captured = capture(&runtime, &domain)
            .await
            .assured("a retained inactive guest does not fail the cut");
        assert!(
            captured.guest_saves.is_empty(),
            "the evicted branch's guest save is not archived"
        );
        assert_eq!(
            captured.native_metadata.len(),
            1,
            "the scheduled current lifecycle is retained"
        );
        assert_eq!(
            captured.native_metadata[0].placement.state.kind(),
            RuntimeStateKind::BranchLru
        );
        runtime.clear_state_identities(&domain);
        let captured = capture(&runtime, &domain)
            .await
            .assured("removed entities do not contribute retained checkpoints");
        assert!(captured.guest_saves.is_empty());
        assert!(captured.native_metadata.is_empty());
    }

    #[nervix_primitives::test]
    async fn a_cut_reattaches_only_the_keys_its_branch_local_checkpoints_need() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let runtime = persisted_runtime(dir.path());
        let store = runtime
            .inner
            .state_store
            .as_ref()
            .assured("state store is configured");
        let domain = DomainName::parse("orders").assured("domain is valid");
        let processor = ModelName::parse("accumulator").assured("processor is valid");
        let ingestor = ModelName::parse("source").assured("ingestor is valid");
        schedule(&runtime, &domain, ModelKind::WasmProcessor, &processor);
        schedule(&runtime, &domain, ModelKind::Ingestor, &ingestor);
        let lifecycle = lifecycle_of(&domain, ModelKind::WasmProcessor, &processor);
        let branches = [branch("alpha", 3), branch("beta", 5), branch("gamma", 7)];
        store
            .publish_sealed_snapshot(
                &lifecycle,
                9,
                &encode_branch_lru_snapshot(&branches).assured("the lifecycle encodes"),
            )
            .assured("the lifecycle persists");
        store
            .publish_sealed_snapshot(&guest_of(&lifecycle, "beta"), 8, b"saved-beta")
            .assured("the guest persists");
        // No branch-local state of the ingestor needs its lifecycle, so the cut never reads it.
        store
            .publish_sealed_snapshot(
                &lifecycle_of(&domain, ModelKind::Ingestor, &ingestor),
                4,
                b"not a lifecycle snapshot",
            )
            .assured("the ingestor lifecycle persists");
        let captured = capture(&runtime, &domain)
            .await
            .assured("an unread lifecycle does not fail the cut");
        let [guest] = captured.guest_saves.as_slice() else {
            panic!("exactly the saved guest is archived");
        };
        assert_eq!(guest.payload, b"saved-beta");
        assert_eq!(guest.revision, 8);
        let beta = super::super::string_branch_key("tenant", "beta");
        assert_eq!(
            guest.placement.branch_key,
            BranchKey::to_remote_key(&beta),
            "the guest takes its typed key from the lifecycle"
        );
        assert_eq!(
            guest.branch_fingerprint,
            beta.as_ref().map(BranchKey::fingerprint)
        );
        let mut native = captured.native_metadata;
        native.sort_by_key(|metadata| metadata.placement.kind == ModelKind::Ingestor);
        let Ok([processor_lifecycle, ingestor_lifecycle]) =
            <[CapturedNativeMetadata; 2]>::try_from(native)
        else {
            panic!("both lifecycles are captured for their sections");
        };
        /// What reading both captured lifecycles produced.
        struct ReadLifecycles {
            revision: u64,
            branches: Vec<BackupBranchLifecycleEntry>,
            ingestor: error_stack::Result<NativeLifecycleCheckpoint, BackupStateCaptureError>,
        }
        let read = in_storage_job(&runtime, move |cancellation| {
            let read = processor_lifecycle
                .read_branch_lifecycle(cancellation)
                .assured("the processor lifecycle reads");
            ReadLifecycles {
                revision: read.revision,
                branches: read.with_branches(|branches| branches.collect()),
                ingestor: ingestor_lifecycle.read_branch_lifecycle(cancellation),
            }
        })
        .await;
        assert_eq!(read.revision, 9);
        assert_eq!(read.branches.len(), branches.len());
        for (actual, expected) in read.branches.iter().zip(&branches) {
            assert_eq!(actual.key, BranchKey::to_remote_key(&expected.key));
            assert_eq!(actual.last_ingestion, expected.last_ingestion);
            assert_eq!(actual.incarnation, expected.incarnation);
        }
        let failure = read
            .ingestor
            .err()
            .assured("an invalid lifecycle fails when its section reads it");
        assert!(matches!(
            failure.current_context(),
            BackupStateCaptureError::Lifecycle { entity } if *entity == ingestor
        ));
    }

    #[nervix_primitives::test]
    async fn a_lifecycle_the_restore_metadata_class_cannot_hold_refuses_the_cut() {
        let dir = tempfile::tempdir().assured("state directory opens");
        let runtime = persisted_runtime(dir.path());
        let store = runtime
            .inner
            .state_store
            .as_ref()
            .assured("state store is configured");
        let domain = DomainName::parse("orders").assured("domain is valid");
        let processor = ModelName::parse("accumulator").assured("processor is valid");
        schedule(&runtime, &domain, ModelKind::WasmProcessor, &processor);
        let lifecycle = lifecycle_of(&domain, ModelKind::WasmProcessor, &processor);
        store
            .publish_sealed_snapshot(
                &lifecycle,
                9,
                &encode_branch_lru_snapshot(&[branch("alpha", 3)]).assured("the lifecycle encodes"),
            )
            .assured("the lifecycle persists");
        store
            .publish_sealed_snapshot(&guest_of(&lifecycle, "alpha"), 8, b"saved-alpha")
            .assured("the guest persists");
        let capacity = runtime
            .executor()
            .snapshot()
            .restore_metadata_memory
            .capacity_bytes;
        let held = runtime
            .executor()
            .try_reserve(MemoryClass::RestoreMetadata, capacity)
            .assured("the test holds the whole class");
        let failure = capture(&runtime, &domain)
            .await
            .err()
            .assured("a lifecycle read is refused rather than read uncharged");
        assert!(matches!(
            failure.current_context(),
            BackupStateCaptureError::Admission { entity } if *entity == processor
        ));
        drop(held);
        let captured = capture(&runtime, &domain)
            .await
            .assured("the same cut is admitted once the class has room");
        assert_eq!(captured.guest_saves.len(), 1);
        assert_eq!(
            runtime
                .executor()
                .snapshot()
                .restore_metadata_memory
                .reserved_bytes,
            0,
            "the lifecycle charge ends with its read"
        );
    }
}
