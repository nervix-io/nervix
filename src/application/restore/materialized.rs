//! Bounded conversion from archive materialized sections to a native sealed checkpoint.
//!
//! Layer: control plane.
//! - **Owns.** Verifying exact-schema Arrow groups and translating archive scalar identities
//!   once, then assembling a quota-owned checkpoint file from bounded pieces.
//! - **Depends on.** Archive-owned records, the runtime snapshot codec and bounded executor.
//! - **Must not know.** Database keys, placement ownership or consensus activation.

use ahash::HashSetExt as _;
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_backup::{
    ArchiveRecord, DescribedMaterializedGroup, DescribedSection, MATERIALIZED_COLUMNS_BYTES,
    MATERIALIZED_IDENTITIES_BYTES, MaterializedIdentitiesRecord, MaterializedRelayDescriptor,
};
use nervix_execution::{ChargedBytes, CpuClass, MemoryClass};
use nervix_primitives::sync::StdArc;
use thiserror::Error;

use super::prepare::{ArchiveSectionReadError, VerifiedArchive};
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
    archive
        .read_bounded_section(runtime, section, limit)
        .await
        .map_err(|error| {
            let context = match error.current_context() {
                ArchiveSectionReadError::TooLong { .. } => MaterializedRestoreError::SectionLength,
                ArchiveSectionReadError::Admission => MaterializedRestoreError::Admission,
                ArchiveSectionReadError::Read => MaterializedRestoreError::Storage,
            };
            error.change_context(context)
        })
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

pub(super) async fn prepare_materialized_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &MaterializedRelayDescriptor,
    groups: &[DescribedMaterializedGroup],
    schema: StdArc<arrow_schema::Schema>,
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
        let path = group.identities.path.to_string();
        let charge = executor
            .reserve(MemoryClass::Bulk, 4 * MATERIALIZED_IDENTITIES_BYTES)
            .await
            .change_context(MaterializedRestoreError::Admission)?;
        let descriptor_count = descriptor.record_count;
        let decoded = executor
            .run_cpu(CpuClass::Bulk, charge, move |charge, cancellation| {
                cancellation
                    .check()
                    .change_context(MaterializedRestoreError::Cancelled)?;
                let record = MaterializedIdentitiesRecord::decode(&path, &identities)
                    .change_context(MaterializedRestoreError::Identities)?;
                let mut native_identities = Vec::with_capacity(record.identities.len());
                for identity in record.identities {
                    cancellation
                        .check()
                        .change_context(MaterializedRestoreError::Cancelled)?;
                    if !branches.insert(identity.branch.clone())
                        || (identity.branch.is_none() && descriptor_count != 1)
                    {
                        return Err(Report::new(MaterializedRestoreError::Identities));
                    }
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
        let batch = RuntimeRecordBatch::decode_arrow_snapshot_section(
            executor,
            schema.clone(),
            columns.clone(),
        )
        .await
        .map_err(|error| {
            let context = match error.current_context() {
                ArrowBodyError::Admission | ArrowBodyError::Execution => {
                    MaterializedRestoreError::Admission
                }
                ArrowBodyError::Cancelled => MaterializedRestoreError::Cancelled,
                _ => MaterializedRestoreError::Columns,
            };
            error.change_context(context)
        })?;
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
