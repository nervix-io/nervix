//! Streaming a node's captured branch lifecycle and Kafka offset checkpoints into archive sections.
//!
//! Layer: control plane.
//! - **Owns.** Admitting each captured native checkpoint's conversion, the conversion of its
//!   entries into public archive values, and streaming its record into a quota-owned staged
//!   section, one checkpoint at a time.
//! - **Depends on.** Captured native checkpoints, the archive's streamed records, the executor's
//!   memory classes and the runtime's staging area.
//! - **Must not know.** Native checkpoint encodings, database keys, or how a staged section
//!   reaches the coordinator.
//!
//! A section holds neither its decoded entries nor its encoded bytes whole. The native checkpoint
//! is read into one aligned allocation under a `restore_metadata` charge; its entries convert one
//! at a time while the record streams through a fixed bulk buffer into the staged file, which
//! seals the section's exact length and digest. The record's serializer scratch, one resolver per
//! entry, is charged to `restore_metadata` beside the checkpoint.

use std::{cell::Cell, io::Write, mem::MaybeUninit};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_backup::{
    ArchiveRecord as _, BranchLifecycleEntry, BranchLifecycleRecord, KafkaOffsetsRecord,
    KafkaPartitionOffset, MAX_RECORD_BYTES, RecordKind, SectionContent, SectionPath, StateField,
    StreamedBranchLifecycle, StreamedKafkaOffsets,
};
use nervix_execution::{Cancellation, Executor, MemoryClass};
use nervix_interconnect::{RemoteOperationFailure, RuntimeState};
use nervix_models::{DomainName, DomainSchedule, ModelKind, ModelName, SchemaFingerprint};
use thiserror::Error;

use super::{
    CaptureSectionKey, CapturedSection,
    interconnect::{CaptureDomainStateRequest, failed},
    state_sections::archived_node,
};
use crate::{
    application::session_service::SessionServiceImpl,
    runtime::{
        BackupBranchLifecycleEntry, BackupKafkaPartitionOffset, CapturedNativeMetadata,
        SnapshotStagingError, StagedArtifact,
    },
};

/// The bulk memory streaming one section holds: the staged file's buffered block.
const SECTION_WRITER_BYTES: u64 = 64 * 1024;

/// Why a captured native checkpoint did not become its archive section. No variant carries a
/// value from the checkpoint.
#[derive(Debug, Error)]
enum NativeSectionError {
    #[error("the {kind} checkpoint of '{entity}' could not be read")]
    Read { kind: RecordKind, entity: ModelName },
    #[error("the serializer scratch of the {kind} section of '{entity}' could not be admitted")]
    Scratch { kind: RecordKind, entity: ModelName },
    #[error("the {kind} section of '{entity}' could not be written")]
    Write { kind: RecordKind, entity: ModelName },
}

/// One captured checkpoint this node archives, and what its record says beside its entries.
struct NativeSection {
    captured: CapturedNativeMetadata,
    record: NativeRecord,
    domain: DomainName,
    entity: ModelName,
}

/// The record a native section streams, with the schema it records.
enum NativeRecord {
    /// Kafka offsets depend on no schema; the record names its ingestor's committed one.
    KafkaOffsets { schema: SchemaFingerprint },
    BranchLifecycle {
        owner_kind: ModelKind,
        schema: SchemaFingerprint,
    },
}

impl NativeRecord {
    fn kind(&self) -> RecordKind {
        match self {
            Self::KafkaOffsets { .. } => KafkaOffsetsRecord::KIND,
            Self::BranchLifecycle { .. } => BranchLifecycleRecord::KIND,
        }
    }
}

impl NativeSection {
    fn path(&self) -> SectionPath {
        match &self.record {
            NativeRecord::KafkaOffsets { .. } => {
                SectionPath::kafka_offsets(&self.domain, &self.entity)
            }
            NativeRecord::BranchLifecycle { owner_kind, .. } => {
                SectionPath::branch_lifecycle(&self.domain, *owner_kind, &self.entity)
            }
        }
    }
}

/// The failure a capture coordinator reads, rendering every context of `error`.
fn refusal<C>(domain: &DomainName, error: &Report<C>) -> RemoteOperationFailure {
    failed(domain, &format!("{error:#}"))
}

impl SessionServiceImpl {
    /// Streams every captured native checkpoint this node is the scheduled primary of into its
    /// staged section, one at a time.
    pub(super) async fn stage_native_metadata_sections(
        &self,
        captured: Vec<CapturedNativeMetadata>,
        schedule: Option<&DomainSchedule>,
        request: &CaptureDomainStateRequest,
    ) -> Result<Vec<CapturedSection>, RemoteOperationFailure> {
        let local_node = self.inner.consensus.local_node_id();
        let mut staged = Vec::new();
        for captured in captured {
            nervix_primitives::task::consume_budget().await;
            let Some(node) = archived_node(&captured.placement, schedule, local_node) else {
                continue;
            };
            let placement = &captured.placement;
            let record = match placement.state {
                RuntimeState::KafkaOffset => NativeRecord::KafkaOffsets {
                    schema: node.schema_fingerprint,
                },
                RuntimeState::BranchLru { schema } => NativeRecord::BranchLifecycle {
                    owner_kind: placement.kind,
                    schema,
                },
                _ => continue,
            };
            let section = NativeSection {
                domain: placement.domain.clone(),
                entity: placement.identifier.clone(),
                captured,
                record,
            };
            let key = CaptureSectionKey {
                coordination: request.coordination.clone(),
                domain: request.domain.clone(),
                path: section.path().as_str().to_string(),
            };
            let content = SectionContent::Record(section.record.kind());
            let artifact = self.stage_native_section(section, &request.domain).await?;
            staged.push(CapturedSection {
                key,
                content,
                artifact,
            });
        }
        Ok(staged)
    }

    /// Admits the section's conversion, then streams its record into a staged file of at most
    /// one record's length. A failed or cancelled section drops its file and returns its quota and
    /// charges; nothing of it is staged for the coordinator.
    async fn stage_native_section(
        &self,
        section: NativeSection,
        domain: &DomainName,
    ) -> Result<StagedArtifact, RemoteOperationFailure> {
        let executor = self.inner.runtime.executor().clone();
        let conversion_bytes = section
            .captured
            .conversion_bytes()
            .map_err(|error| refusal(domain, &error))?;
        let conversion = executor
            .reserve(MemoryClass::RestoreMetadata, conversion_bytes)
            .await
            .map_err(|error| refusal(domain, &error))?;
        let working = executor
            .reserve(MemoryClass::Bulk, SECTION_WRITER_BYTES)
            .await
            .map_err(|error| refusal(domain, &error))?;
        let writer = self
            .inner
            .runtime
            .try_stage_artifact(MAX_RECORD_BYTES)
            .await
            .map_err(|error| refusal(domain, &error))?;
        let interruption = self
            .inner
            .runtime
            .native_metadata_capture_interruption(domain, &section.entity);
        writer
            .encode_artifact(working, move |output, cancellation| {
                let _conversion = conversion;
                section
                    .write(&executor, output, cancellation, interruption)
                    .change_context(SnapshotStagingError::Encode)
            })
            .await
            .map_err(|error| refusal(domain, &error))
    }
}

impl NativeSection {
    /// Reads the captured checkpoint and streams its record into `output`. The record stops when
    /// the staging job is cancelled, or once it has written `interruption` entries when a test
    /// interrupts it.
    fn write(
        &self,
        executor: &Executor,
        output: &mut dyn Write,
        cancellation: &Cancellation,
        interruption: Option<u64>,
    ) -> error_stack::Result<(), NativeSectionError> {
        let kind = self.record.kind();
        let entity = || self.entity.clone();
        let entries = Cell::new(0_u64);
        let stop = || {
            if cancellation.is_cancelled() {
                return true;
            }
            let Some(limit) = interruption else {
                return false;
            };
            let reached = entries.get();
            entries.set(
                reached
                    .checked_add(1)
                    .assured("a record asks fewer times than 64 bits count"),
            );
            reached >= limit
        };
        match &self.record {
            NativeRecord::KafkaOffsets { schema } => {
                let offsets = self
                    .captured
                    .read_kafka_offsets(cancellation)
                    .change_context_lazy(|| NativeSectionError::Read {
                        kind,
                        entity: entity(),
                    })?;
                offsets.with_positions(|positions| {
                    let record = StreamedKafkaOffsets {
                        domain: self.domain.clone(),
                        entity: entity(),
                        schema: *schema,
                        revision: offsets.revision,
                        offsets: positions.map(KafkaPartitionOffset::from),
                    };
                    let scratch_bytes = record.scratch_bytes().change_context_lazy(|| {
                        NativeSectionError::Write {
                            kind,
                            entity: entity(),
                        }
                    })?;
                    let mut scratch = self.admitted_scratch(executor, scratch_bytes)?;
                    record
                        .write(&mut scratch.bytes, output, &stop)
                        .change_context_lazy(|| NativeSectionError::Write {
                            kind,
                            entity: entity(),
                        })?;
                    Ok(())
                })
            }
            NativeRecord::BranchLifecycle { owner_kind, schema } => {
                let lifecycle = self
                    .captured
                    .read_branch_lifecycle(cancellation)
                    .change_context_lazy(|| NativeSectionError::Read {
                        kind,
                        entity: entity(),
                    })?;
                lifecycle.with_branches(|branches| {
                    let record = StreamedBranchLifecycle {
                        domain: self.domain.clone(),
                        owner_kind: *owner_kind,
                        entity: entity(),
                        schema: *schema,
                        revision: lifecycle.revision,
                        branches: branches.map(BranchLifecycleEntry::from),
                    };
                    let scratch_bytes = record.scratch_bytes().change_context_lazy(|| {
                        NativeSectionError::Write {
                            kind,
                            entity: entity(),
                        }
                    })?;
                    let mut scratch = self.admitted_scratch(executor, scratch_bytes)?;
                    record
                        .write(&mut scratch.bytes, output, &stop)
                        .change_context_lazy(|| NativeSectionError::Write {
                            kind,
                            entity: entity(),
                        })?;
                    Ok(())
                })
            }
        }
    }

    /// Serializer scratch of `bytes` bytes under its own `restore_metadata` charge. The section's
    /// job already holds its checkpoint's charge, so this one is refused rather than awaited when
    /// the class has no room for it now.
    fn admitted_scratch(
        &self,
        executor: &Executor,
        bytes: usize,
    ) -> error_stack::Result<AdmittedScratch, NativeSectionError> {
        let charge = executor
            .try_reserve(MemoryClass::RestoreMetadata, bytes.arch_into())
            .change_context_lazy(|| NativeSectionError::Scratch {
                kind: self.record.kind(),
                entity: self.entity.clone(),
            })?;
        Ok(AdmittedScratch {
            bytes: vec![MaybeUninit::uninit(); bytes],
            _charge: charge,
        })
    }
}

/// Serializer scratch and the charge that backs it, released together.
struct AdmittedScratch {
    bytes: Vec<MaybeUninit<u8>>,
    _charge: nervix_execution::Reservation,
}

impl From<BackupKafkaPartitionOffset> for KafkaPartitionOffset {
    fn from(position: BackupKafkaPartitionOffset) -> Self {
        Self {
            topic: position.topic,
            partition: position.partition,
            next_offset: position.next_offset,
        }
    }
}

impl From<BackupBranchLifecycleEntry> for BranchLifecycleEntry {
    fn from(branch: BackupBranchLifecycleEntry) -> Self {
        Self {
            key: branch
                .key
                .map(|fields| fields.into_iter().map(StateField::from_remote).collect()),
            last_ingestion: branch.last_ingestion,
            incarnation: branch.incarnation,
        }
    }
}
