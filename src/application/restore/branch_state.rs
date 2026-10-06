//! Bounded conversion from archived deduplicator and window sections to native checkpoints.
//!
//! Layer: control plane.
//! - **Owns.** Decoding each archived key or row group under the exact shape the restored models
//!   give it, rebuilding the native keyspace or window checkpoint from them, and staging that
//!   checkpoint in a quota-owned file for the streamed installer.
//! - **Depends on.** Archive-owned records, the decision layer's restored shapes, the runtime's
//!   keyspace and window checkpoint codecs, and the bounded executor.
//! - **Must not know.** Database keys, placement ownership or consensus activation.

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_backup::{
    BRANCH_STATE_GROUP_BYTES, DeduplicatorStateDescriptor, DescribedSection, DescribedWindowGroup,
    WindowAccumulatorRecord, WindowStateDescriptor,
};
use nervix_execution::{ChargedBytes, CpuClass, MemoryClass, Reservation};
use nervix_primitives::sync::StdArc;
use thiserror::Error;

use super::prepare::{ArchiveSectionReadError, VerifiedArchive};
use crate::{
    registry::{WindowAccumulatorShape, WindowStateSchemas},
    runtime::{
        ArchivedDeduplicatorKeys, ArchivedWindow, BranchKey, Runtime, StagedArtifact,
        WindowAccumulatorState, WindowCheckpointBuilder, WindowDelayedRemoval,
    },
    runtime_schema::{ArrowBodyError, RuntimeRecordBatch},
};

#[derive(Debug, Error)]
pub(super) enum BranchStateRestoreError {
    #[error("deduplicator or window restore conversion could not be admitted")]
    Admission,
    #[error("deduplicator or window restore conversion was cancelled")]
    Cancelled,
    #[error("an archived deduplicator or window group is longer than its bound")]
    SectionLength,
    #[error("an archived deduplicator or window group could not be read")]
    Storage,
    #[error("archived deduplicator keys do not have the restored key shape")]
    Keys,
    #[error("the archived keyspace holds {found} keys where its descriptor lists {expected}")]
    KeyCount { expected: u64, found: u64 },
    #[error("archived window rows do not have the restored window's input or argument shape")]
    Rows,
    #[error("the archived window's aggregate state does not fit the restored aggregates")]
    Accumulators,
    #[error("the archived branch key does not decode")]
    BranchKey,
    #[error("the restored checkpoint could not be encoded")]
    Encoding,
}

impl From<&ArchiveSectionReadError> for BranchStateRestoreError {
    fn from(error: &ArchiveSectionReadError) -> Self {
        match error {
            ArchiveSectionReadError::TooLong { .. } => Self::SectionLength,
            ArchiveSectionReadError::Admission => Self::Admission,
            ArchiveSectionReadError::Read => Self::Storage,
        }
    }
}

/// Reads one archived group and decodes it as exactly `schema`.
async fn decode_group(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
    mismatch: BranchStateRestoreError,
) -> Result<RuntimeRecordBatch, Report<BranchStateRestoreError>> {
    let bytes = archive
        .read_bounded_section(runtime, section, BRANCH_STATE_GROUP_BYTES)
        .await
        .map_err(|error| {
            let context = BranchStateRestoreError::from(error.current_context());
            error.change_context(context)
        })?;
    RuntimeRecordBatch::decode_arrow_snapshot_section(
        runtime.executor(),
        StdArc::clone(schema),
        bytes,
    )
    .await
    .map_err(|error| {
        let context = match error.current_context() {
            ArrowBodyError::Admission | ArrowBodyError::Execution => {
                BranchStateRestoreError::Admission
            }
            ArrowBodyError::Cancelled => BranchStateRestoreError::Cancelled,
            _ => mismatch,
        };
        error.change_context(context)
    })
}

/// Charges what a decoded group keeps alive until its checkpoint is encoded. A refusal fails the
/// conversion rather than waiting, because the groups it already holds cannot free the charge.
fn retain_group(
    runtime: &Runtime,
    batch: &RuntimeRecordBatch,
) -> Result<Reservation, Report<BranchStateRestoreError>> {
    let bytes = batch.estimated_bytes().max(1);
    runtime
        .executor()
        .try_reserve(MemoryClass::Bulk, bytes)
        .change_context(BranchStateRestoreError::Admission)
}

/// Stages a rebuilt checkpoint in a quota-owned file for the streamed installer.
async fn stage_checkpoint(
    runtime: &Runtime,
    bytes: ChargedBytes,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let length = u64::try_from(bytes.len()).verified("an encoded checkpoint fits 64 bits");
    let mut writer = runtime
        .try_stage_artifact(length)
        .await
        .change_context(BranchStateRestoreError::Storage)?;
    writer
        .write_chunk(bytes)
        .await
        .change_context(BranchStateRestoreError::Storage)?;
    writer
        .finish_artifact()
        .await
        .change_context(BranchStateRestoreError::Storage)
}

/// The native keyspace checkpoint of one archived deduplicator branch, every key normalized as the
/// restored branch task normalizes the values its `DEDUPLICATE ON` expressions produce.
pub(super) async fn prepare_deduplicator_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &DeduplicatorStateDescriptor,
    groups: &[DescribedSection],
    key_schema: &StdArc<arrow_schema::Schema>,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let executor = runtime.executor();
    let mut keys = ArchivedDeduplicatorKeys::new();
    let mut retained = Vec::with_capacity(groups.len());
    // The native checkpoint encodes about as many bytes as the archived groups hold.
    let mut archived_bytes = 64 * 1024_u64;
    for group in groups {
        nervix_primitives::task::consume_budget().await;
        let batch = decode_group(
            runtime,
            archive,
            group,
            key_schema,
            BranchStateRestoreError::Keys,
        )
        .await?;
        retained.push(retain_group(runtime, &batch)?);
        archived_bytes = archived_bytes
            .checked_add(group.length)
            .ok_or_else(|| Report::new(BranchStateRestoreError::SectionLength))?;
        let charge = executor
            .reserve(MemoryClass::Bulk, batch.estimated_bytes().max(1))
            .await
            .change_context(BranchStateRestoreError::Admission)?;
        keys = executor
            .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(BranchStateRestoreError::Cancelled)?;
                keys.admit_group(batch.batch())
                    .change_context(BranchStateRestoreError::Keys)?;
                Ok::<_, Report<BranchStateRestoreError>>(keys)
            })
            .await
            .change_context(BranchStateRestoreError::Admission)??;
    }
    let found = u64::try_from(keys.len()).verified("an addressable key count fits 64 bits");
    if found != descriptor.keys {
        return Err(Report::new(BranchStateRestoreError::KeyCount {
            expected: descriptor.keys,
            found,
        }));
    }
    let charge = executor
        .reserve(MemoryClass::Bulk, archived_bytes)
        .await
        .change_context(BranchStateRestoreError::Admission)?;
    let encoded = executor
        .run_cpu(CpuClass::Bulk, charge, move |charge, cancellation| {
            cancellation
                .check()
                .change_context(BranchStateRestoreError::Cancelled)?;
            let bytes = keys
                .encode()
                .change_context(BranchStateRestoreError::Encoding)?;
            Ok::<_, Report<BranchStateRestoreError>>(ChargedBytes::from_owned(bytes, charge))
        })
        .await
        .change_context(BranchStateRestoreError::Admission)??;
    drop(retained);
    stage_checkpoint(runtime, encoded).await
}

/// The native window checkpoint of one archived window branch. The restored branch task re-admits
/// its rows in order to rebuild every aggregate structure, then applies the delayed removals.
pub(super) async fn prepare_window_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &WindowStateDescriptor,
    groups: &[DescribedWindowGroup],
    schemas: &WindowStateSchemas,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let accumulators = window_accumulators(descriptor, schemas)?;
    let branch = super::steps::remote_branch_key(descriptor.branch.as_ref());
    let branch =
        BranchKey::from_remote_key(branch).change_context(BranchStateRestoreError::BranchKey)?;
    let mut builder = WindowCheckpointBuilder::new(ArchivedWindow {
        revision: descriptor.revision,
        incarnation: descriptor.incarnation,
        branch,
        first_sequence: descriptor.first_sequence,
        next_sequence: descriptor.next_sequence,
        rows: descriptor.rows.clone(),
        accumulators,
    });
    let mut retained = Vec::with_capacity(groups.len());
    for group in groups {
        nervix_primitives::task::consume_budget().await;
        let input = decode_group(
            runtime,
            archive,
            &group.input,
            &schemas.input,
            BranchStateRestoreError::Rows,
        )
        .await?;
        retained.push(retain_group(runtime, &input)?);
        let arguments = decode_group(
            runtime,
            archive,
            &group.arguments,
            &schemas.arguments,
            BranchStateRestoreError::Rows,
        )
        .await?;
        retained.push(retain_group(runtime, &arguments)?);
        builder
            .admit_group(input, arguments)
            .change_context(BranchStateRestoreError::Rows)?;
    }
    let encoded = builder
        .encode(runtime.executor())
        .await
        .change_context(BranchStateRestoreError::Encoding)?;
    drop(retained);
    let encoded = runtime
        .executor()
        .charge_owned(MemoryClass::Bulk, encoded)
        .await
        .change_context(BranchStateRestoreError::Admission)?;
    stage_checkpoint(runtime, encoded).await
}

/// The archived aggregate state of each demand, checked against the restored window's demands:
/// a linear histogram where the window keeps one, with every delayed bucket inside its range.
fn window_accumulators(
    descriptor: &WindowStateDescriptor,
    schemas: &WindowStateSchemas,
) -> Result<Vec<WindowAccumulatorState>, Report<BranchStateRestoreError>> {
    if descriptor.accumulators.len() != schemas.accumulators.len() {
        return Err(Report::new(BranchStateRestoreError::Accumulators));
    }
    let mut accumulators = Vec::with_capacity(descriptor.accumulators.len());
    for (archived, shape) in descriptor.accumulators.iter().zip(&schemas.accumulators) {
        let state = match (archived, shape) {
            (WindowAccumulatorRecord::Retained, WindowAccumulatorShape::Retained) => {
                WindowAccumulatorState::Retained
            }
            (
                WindowAccumulatorRecord::LinearHistogram { delayed_removals },
                WindowAccumulatorShape::LinearHistogram { buckets },
            ) => {
                let buckets = u64::try_from(buckets.get()).verified("a bucket count fits 64 bits");
                let mut removals = Vec::with_capacity(delayed_removals.len());
                for removal in delayed_removals {
                    if removal.bucket >= buckets {
                        return Err(Report::new(BranchStateRestoreError::Accumulators));
                    }
                    removals.push(WindowDelayedRemoval {
                        expires_at: removal.expires_at,
                        bucket: removal.bucket,
                    });
                }
                WindowAccumulatorState::LinearHistogram {
                    delayed_removals: removals,
                }
            }
            _ => return Err(Report::new(BranchStateRestoreError::Accumulators)),
        };
        accumulators.push(state);
    }
    Ok(accumulators)
}
