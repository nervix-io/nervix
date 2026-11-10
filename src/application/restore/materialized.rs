//! Bounded conversion from archive materialized sections to a native sealed checkpoint.
//!
//! Layer: control plane.
//! - **Owns.** Verifying exact-schema Arrow groups and translating archive scalar identities
//!   once, then assembling a quota-owned checkpoint file from bounded pieces.
//! - **Depends on.** Archive-owned records, the runtime snapshot codec and bounded executor.
//! - **Must not know.** Database keys, placement ownership or consensus activation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeMap;

use ahash::HashSetExt as _;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_backup::{
    ArchiveRecord, DescribedMaterializedGroup, DescribedRuntimeState, DescribedSection,
    MATERIALIZED_COLUMNS_BYTES, MATERIALIZED_IDENTITIES_BYTES, MaterializedIdentitiesRecord,
    MaterializedRelayDescriptor, SectionPath,
};
use nervix_execution::{ChargedBytes, CpuClass, MemoryClass};
use nervix_models::{
    BranchKeyFingerprint, ModelKind, NodeRef, RemoteRuntimeField, RemoteRuntimeRecordMetadata,
    ScheduledNode, Unit,
};
use nervix_primitives::sync::{Arc, StdArc};
use thiserror::Error;

use super::{
    partitions::RestoredPartitions,
    prepare::{ArchiveSectionReadError, RestoredBranchDeclarations, VerifiedArchive},
    steps::remote_branch_key,
};
use crate::{
    runtime::{
        Runtime, StagedArtifact, materialized_columns_frame, materialized_container_header,
        materialized_identity_section,
    },
    runtime_schema::{ArrowBodyError, RuntimeRecordBatch},
};

#[derive(Debug, Error)]
pub(super) enum MaterializedRestoreError {
    #[error("materialized restore conversion could not be admitted")]
    Admission,
    #[error("materialized restore conversion was cancelled")]
    Cancelled,
    #[error("materialized archive section length is invalid")]
    SectionLength,
    #[error("materialized archive identity record is invalid")]
    Identities,
    #[error(
        "the branch key of record identity {record} in archive section '{section}' does not \
         belong to the relay's branching"
    )]
    BranchKey { section: SectionPath, record: usize },
    #[error("materialized Arrow columns do not match the restored relay schema")]
    Columns,
    #[error("materialized Arrow row count does not match its identities")]
    RecordCount,
    #[error("materialized checkpoint framing could not be encoded")]
    Encoding,
    #[error("materialized checkpoint file could not be staged")]
    Storage,
    #[error("materialized branch identity metadata exceeds its 8 MiB limit")]
    MetadataTooLarge,
}

async fn read_section(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    section: &DescribedSection,
    limit: u64,
) -> Result<ChargedBytes, Report<MaterializedRestoreError>> {
    {
        let mapping_input = archive.read_bounded_section(runtime, section, limit).await;
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let context = match error.current_context() {
                    ArchiveSectionReadError::TooLong { .. } => {
                        MaterializedRestoreError::SectionLength
                    }
                    ArchiveSectionReadError::Admission => MaterializedRestoreError::Admission,
                    ArchiveSectionReadError::Read => MaterializedRestoreError::Storage,
                };
                error.change_context(context)
            }),
        }
    }
}

async fn stage_piece(
    runtime: &Runtime,
    bytes: ChargedBytes,
) -> Result<StagedArtifact, Report<MaterializedRestoreError>> {
    let mut writer = runtime
        .try_stage_artifact(u64::try_from(bytes.len()).verified("bounded piece fits"))
        .await
        .change_context(MaterializedRestoreError::Storage)?;
    writer
        .write_chunk(bytes)
        .await
        .change_context(MaterializedRestoreError::Storage)?;
    writer
        .finish_artifact()
        .await
        .change_context(MaterializedRestoreError::Storage)
}

/// The native sealed checkpoint of one archived materialized relay. Every record identity's branch
/// key is admitted under `declarations`, the branching of the restored relay, before it is staged.
pub(super) async fn prepare_materialized_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &MaterializedRelayDescriptor,
    groups: &[DescribedMaterializedGroup],
    schema: StdArc<arrow_schema::Schema>,
    declarations: Arc<RestoredBranchDeclarations>,
) -> Result<StagedArtifact, Report<MaterializedRestoreError>> {
    let executor = runtime.executor();
    let identity_bytes = groups
        .iter()
        .try_fold(0_u64, |total, group| {
            total.checked_add(group.identities.length)
        })
        .ok_or_else(|| Report::new(MaterializedRestoreError::MetadataTooLarge))?;
    let identity_bytes = identity_bytes
        .checked_mul(8)
        .ok_or_else(|| Report::new(MaterializedRestoreError::MetadataTooLarge))?;
    let table_bytes = descriptor
        .record_count
        .checked_mul(64)
        .ok_or_else(|| Report::new(MaterializedRestoreError::MetadataTooLarge))?;
    let identity_bytes = identity_bytes
        .checked_add(table_bytes)
        .ok_or_else(|| Report::new(MaterializedRestoreError::MetadataTooLarge))?;
    if identity_bytes > 8 * 1024 * 1024 {
        return Err(Report::new(MaterializedRestoreError::MetadataTooLarge));
    }
    let _identity_charge = executor
        .reserve(MemoryClass::Commands, identity_bytes.max(1))
        .await
        .change_context(MaterializedRestoreError::Admission)?;
    let mut branches = ahash::HashSet::new();
    // The archived fence describes its source process. The restored assignment establishes its
    // own authority; source fence numbers cannot authorize or block a different cluster.
    let header = materialized_container_header(
        descriptor.revision,
        0,
        descriptor.branch_generation,
        descriptor.record_count,
        descriptor.groups,
    )
    .change_context(MaterializedRestoreError::Encoding)?;
    let header = executor
        .charge_owned(MemoryClass::Bulk, header)
        .await
        .change_context(MaterializedRestoreError::Admission)?;
    let mut pieces = vec![stage_piece(runtime, header).await?];
    for group in groups {
        nervix_primitives::task::consume_budget().await;
        let identities = read_section(
            runtime,
            archive,
            &group.identities,
            MATERIALIZED_IDENTITIES_BYTES,
        )
        .await?;
        let section = group.identities.path.clone();
        let charge = executor
            .reserve(MemoryClass::Bulk, 4 * MATERIALIZED_IDENTITIES_BYTES)
            .await
            .change_context(MaterializedRestoreError::Admission)?;
        let descriptor_count = descriptor.record_count;
        let declarations = declarations.clone();
        let decoded = executor
            .run_cpu(CpuClass::Bulk, charge, move |charge, cancellation| {
                cancellation
                    .check()
                    .change_context(MaterializedRestoreError::Cancelled)?;
                let record = MaterializedIdentitiesRecord::decode(section.as_str(), &identities)
                    .change_context(MaterializedRestoreError::Identities)?;
                let mut native_identities = Vec::with_capacity(record.identities.len());
                for (index, identity) in record.identities.into_iter().enumerate() {
                    cancellation
                        .check()
                        .change_context(MaterializedRestoreError::Cancelled)?;
                    if !branches.insert(identity.branch.clone())
                        || (identity.branch.is_none() && descriptor_count != 1)
                    {
                        return Err(Report::new(MaterializedRestoreError::Identities));
                    }
                    let record_number = index.checked_add(1).assured(
                        "an identity's index is below the length of the list that holds it",
                    );
                    declarations
                        .admit(identity.branch.as_ref())
                        .change_context_lazy(|| MaterializedRestoreError::BranchKey {
                            section: section.clone(),
                            record: record_number,
                        })?;
                    native_identities.push((
                        identity.branch.map(|fields| {
                            fields
                                .into_iter()
                                .map(|field| field.into_remote())
                                .collect()
                        }),
                        identity.watermarks,
                    ));
                }
                let bytes = materialized_identity_section(native_identities)
                    .change_context(MaterializedRestoreError::Encoding)?;
                Ok::<_, Report<MaterializedRestoreError>>((
                    ChargedBytes::from_owned(bytes, charge),
                    branches,
                ))
            })
            .await
            .change_context(MaterializedRestoreError::Admission)??;
        branches = decoded.1;
        pieces.push(stage_piece(runtime, decoded.0).await?);
        let columns =
            read_section(runtime, archive, &group.columns, MATERIALIZED_COLUMNS_BYTES).await?;
        let batch = {
            let mapping_input = RuntimeRecordBatch::decode_arrow_snapshot_section(
                executor,
                schema.clone(),
                columns.clone(),
            )
            .await;
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let context = match error.current_context() {
                        ArrowBodyError::Admission | ArrowBodyError::Execution => {
                            MaterializedRestoreError::Admission
                        }
                        ArrowBodyError::Cancelled => MaterializedRestoreError::Cancelled,
                        _ => MaterializedRestoreError::Columns,
                    };
                    error.change_context(context)
                }),
            }
        }?;
        if u64::try_from(batch.batch().num_rows()).ok() != Some(group.record_count) {
            return Err(Report::new(MaterializedRestoreError::RecordCount));
        }
        drop(batch);
        let frame = materialized_columns_frame(columns.len())
            .change_context(MaterializedRestoreError::Encoding)?;
        let frame = executor
            .charge_owned(MemoryClass::Bulk, frame)
            .await
            .change_context(MaterializedRestoreError::Admission)?;
        pieces.push(stage_piece(runtime, frame).await?);
        pieces.push(stage_piece(runtime, columns).await?);
    }
    assemble_checkpoint(runtime, pieces).await
}

/// Writes `pieces`, a checkpoint's header and sections in order, into one staged checkpoint file.
async fn assemble_checkpoint(
    runtime: &Runtime,
    pieces: Vec<StagedArtifact>,
) -> Result<StagedArtifact, Report<MaterializedRestoreError>> {
    let total = pieces
        .iter()
        .try_fold(0_u64, |total, piece| total.checked_add(piece.length()))
        .ok_or_else(|| Report::new(MaterializedRestoreError::Encoding))?;
    let mut writer = runtime
        .try_stage_artifact(total)
        .await
        .change_context(MaterializedRestoreError::Storage)?;
    for piece in pieces {
        nervix_primitives::task::consume_budget().await;
        let mut reader = piece
            .open_reader()
            .await
            .change_context(MaterializedRestoreError::Storage)?;
        while let Some(chunk) = reader
            .next_chunk(
                u64::try_from(crate::runtime::RESTORE_STATE_CHUNK_BYTES)
                    .verified("chunk size fits"),
            )
            .await
            .change_context(MaterializedRestoreError::Storage)?
        {
            nervix_primitives::task::consume_budget().await;
            writer
                .write_chunk(chunk)
                .await
                .change_context(MaterializedRestoreError::Storage)?;
        }
    }
    writer
        .finish_artifact()
        .await
        .change_context(MaterializedRestoreError::Storage)
}

/// One archived unit of a materialized relay.
pub(super) struct ArchivedMaterializedUnit<'a> {
    pub(super) descriptor: &'a MaterializedRelayDescriptor,
    pub(super) groups: &'a [DescribedMaterializedGroup],
}

impl<'a> ArchivedMaterializedUnit<'a> {
    /// The archived units of every materialized relay a branch's partitions divide under
    /// `partitions`, by relay. A unit archived under a schema the restored relay no longer has is
    /// left out, as installation leaves out any such state.
    pub(super) fn of_divided_relays(
        states: &'a [DescribedRuntimeState],
        partitions: &RestoredPartitions<'_>,
    ) -> BTreeMap<NodeRef, Vec<Self>> {
        let mut relays = BTreeMap::<NodeRef, Vec<Self>>::new();
        for archived in states {
            let DescribedRuntimeState::Materialized {
                descriptor, groups, ..
            } = archived
            else {
                continue;
            };
            let reference = NodeRef::new(ModelKind::Relay, descriptor.entity.clone());
            let Some(node) = partitions.divided(&reference) else {
                continue;
            };
            if descriptor.schema != node.schema_fingerprint {
                continue;
            }
            relays
                .entry(reference)
                .or_default()
                .push(Self { descriptor, groups });
        }
        relays
    }
}

/// The rows of one archived group a regrouped checkpoint keeps, with the identity of each.
struct SelectedRows {
    /// The kept rows' places in the group's Arrow order.
    rows: Vec<usize>,
    identities: Vec<(Option<Vec<RemoteRuntimeField>>, RemoteRuntimeRecordMetadata)>,
}

/// What a group's identity scan hands back: its kept rows, and the keys kept and seen so far.
struct ScannedIdentities {
    selected: SelectedRows,
    kept: ahash::HashSet<BranchKeyFingerprint>,
    seen: ahash::HashSet<BranchKeyFingerprint>,
}

/// The native sealed checkpoint of `unit` of the materialized relay `node`, a node a branch's
/// partitions divide: every row archived in any of `archived` whose key the restored count places
/// in `unit`. `archived` lists the archived unit that is `unit` first, so a key archived in two
/// units keeps the row of its own unit, where its claim installed it. None when no archived row
/// lands in `unit`. Every kept row's key is admitted under `declarations` before it is staged.
pub(super) async fn prepare_regrouped_materialized_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    archived: &[ArchivedMaterializedUnit<'_>],
    unit: Unit,
    node: &ScheduledNode,
    schema: StdArc<arrow_schema::Schema>,
    declarations: Arc<RestoredBranchDeclarations>,
) -> Result<Option<StagedArtifact>, Report<MaterializedRestoreError>> {
    let executor = runtime.executor();
    let mut kept = ahash::HashSet::new();
    let mut pieces = Vec::new();
    let mut records = 0_u64;
    let mut groups = 0_u32;
    let mut revision = 0_u64;
    let mut branch_generation = 0_u64;
    for source in archived {
        revision = revision.max(source.descriptor.revision);
        branch_generation = branch_generation.max(source.descriptor.branch_generation);
        // One archived unit holds each key once; a second row of one key in it is a damaged
        // archive.
        let mut seen = ahash::HashSet::new();
        for group in source.groups {
            nervix_primitives::task::consume_budget().await;
            let identities = read_section(
                runtime,
                archive,
                &group.identities,
                MATERIALIZED_IDENTITIES_BYTES,
            )
            .await?;
            let section = group.identities.path.clone();
            let charge = executor
                .reserve(MemoryClass::Bulk, 4 * MATERIALIZED_IDENTITIES_BYTES)
                .await
                .change_context(MaterializedRestoreError::Admission)?;
            let declarations = declarations.clone();
            let node = node.clone();
            let scan = ScannedIdentities {
                selected: SelectedRows {
                    rows: Vec::new(),
                    identities: Vec::new(),
                },
                kept,
                seen,
            };
            let scanned = executor
                .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                    scan.select(
                        &identities,
                        &section,
                        &declarations,
                        &node,
                        unit,
                        cancellation,
                    )
                })
                .await
                .change_context(MaterializedRestoreError::Admission)??;
            kept = scanned.kept;
            seen = scanned.seen;
            let selected = scanned.selected;
            if selected.rows.is_empty() {
                continue;
            }
            let columns =
                read_section(runtime, archive, &group.columns, MATERIALIZED_COLUMNS_BYTES).await?;
            let decoded = RuntimeRecordBatch::decode_arrow_snapshot_section(
                executor,
                schema.clone(),
                columns,
            )
            .await
            .map_err(MaterializedRestoreError::columns)?;
            if u64::try_from(decoded.batch().num_rows()).ok() != Some(group.record_count) {
                return Err(Report::new(MaterializedRestoreError::RecordCount));
            }
            let regrouped = decoded
                .take(&selected.rows)
                .change_context(MaterializedRestoreError::Columns)?;
            drop(decoded);
            let encoded = regrouped
                .encode_arrow_snapshot_section(executor)
                .await
                .map_err(MaterializedRestoreError::columns)?;
            let identity_section = materialized_identity_section(selected.identities)
                .change_context(MaterializedRestoreError::Encoding)?;
            let identity_section = executor
                .charge_owned(MemoryClass::Bulk, identity_section)
                .await
                .change_context(MaterializedRestoreError::Admission)?;
            pieces.push(stage_piece(runtime, identity_section).await?);
            let frame = materialized_columns_frame(encoded.len())
                .change_context(MaterializedRestoreError::Encoding)?;
            let frame = executor
                .charge_owned(MemoryClass::Bulk, frame)
                .await
                .change_context(MaterializedRestoreError::Admission)?;
            pieces.push(stage_piece(runtime, frame).await?);
            pieces.push(stage_piece(runtime, encoded).await?);
            let rows = u64::try_from(selected.rows.len())
                .verified("a group holds no more rows than its record count, a u64");
            records = records
                .checked_add(rows)
                .ok_or_else(|| Report::new(MaterializedRestoreError::MetadataTooLarge))?;
            groups = groups
                .checked_add(1)
                .ok_or_else(|| Report::new(MaterializedRestoreError::MetadataTooLarge))?;
        }
    }
    if records == 0 {
        return Ok(None);
    }
    // The archived fences describe their source process; the restored assignment establishes its
    // own authority.
    let header = materialized_container_header(revision, 0, branch_generation, records, groups)
        .change_context(MaterializedRestoreError::Encoding)?;
    let header = executor
        .charge_owned(MemoryClass::Bulk, header)
        .await
        .change_context(MaterializedRestoreError::Admission)?;
    pieces.insert(0, stage_piece(runtime, header).await?);
    assemble_checkpoint(runtime, pieces).await.map(Some)
}

impl ScannedIdentities {
    /// Decodes one archived group's identities and keeps every row whose key the restored count
    /// places in `unit` and no earlier archived unit kept.
    fn select(
        mut self,
        identities: &ChargedBytes,
        section: &SectionPath,
        declarations: &RestoredBranchDeclarations,
        node: &ScheduledNode,
        unit: Unit,
        cancellation: &nervix_execution::Cancellation,
    ) -> Result<Self, Report<MaterializedRestoreError>> {
        cancellation
            .check()
            .change_context(MaterializedRestoreError::Cancelled)?;
        let record = MaterializedIdentitiesRecord::decode(section.as_str(), identities)
            .change_context(MaterializedRestoreError::Identities)?;
        for (index, identity) in record.identities.into_iter().enumerate() {
            cancellation
                .check()
                .change_context(MaterializedRestoreError::Cancelled)?;
            let record_number = index
                .checked_add(1)
                .assured("an identity's index is below the length of the list that holds it");
            let key = declarations
                .admit(identity.branch.as_ref())
                .change_context_lazy(|| MaterializedRestoreError::BranchKey {
                    section: section.clone(),
                    record: record_number,
                })?;
            // A branch's partitions divide only concrete keys.
            let Some(key) = key else {
                return Err(Report::new(MaterializedRestoreError::Identities));
            };
            let fingerprint = key.fingerprint();
            if !self.seen.insert(fingerprint) {
                return Err(Report::new(MaterializedRestoreError::Identities));
            }
            if node.unit_of_branch(Some(&fingerprint)) != Some(unit) {
                continue;
            }
            if !self.kept.insert(fingerprint) {
                continue;
            }
            self.selected.rows.push(index);
            self.selected.identities.push((
                remote_branch_key(identity.branch.as_ref()),
                identity.watermarks,
            ));
        }
        Ok(self)
    }
}

impl MaterializedRestoreError {
    /// The failure an Arrow section of an archived group met, as what it means for the restore.
    fn columns(error: Report<ArrowBodyError>) -> Report<Self> {
        let context = match error.current_context() {
            ArrowBodyError::Admission | ArrowBodyError::Execution => Self::Admission,
            ArrowBodyError::Cancelled => Self::Cancelled,
            _ => Self::Columns,
        };
        error.change_context(context)
    }
}
