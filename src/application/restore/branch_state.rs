//! Bounded conversion from archived deduplicator and window sections to native checkpoints.
//!
//! Layer: control plane.
//! - **Owns.** Decoding each archived key or row group under the exact shape the restored models
//!   give it, one bounded group at a time, and converting them into the native keyspace or window
//!   checkpoint a quota-owned file holds for the streamed installer.
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
        ArchivedDeduplicatorKeys, ArchivedRows, ArchivedWindow, BranchKey,
        DeduplicatorArchiveError, RESTORE_STATE_WORKING_BYTES, Runtime, SnapshotStagingError,
        StagedArtifact, WindowAccumulatorState, WindowArchiveError, WindowDelayedRemoval,
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

/// What a keyspace conversion failure means for the restore: a refusal, a cancellation and an
/// encoding failure keep their cause, and everything else is archived keys of another shape.
impl From<&DeduplicatorArchiveError> for BranchStateRestoreError {
    fn from(error: &DeduplicatorArchiveError) -> Self {
        match error {
            DeduplicatorArchiveError::Admission => Self::Admission,
            DeduplicatorArchiveError::Cancelled => Self::Cancelled,
            DeduplicatorArchiveError::Encode => Self::Encoding,
            DeduplicatorArchiveError::SeenAtColumn
            | DeduplicatorArchiveError::KeyPart { .. }
            | DeduplicatorArchiveError::KeyArity { .. }
            | DeduplicatorArchiveError::KeyColumn { .. }
            | DeduplicatorArchiveError::DuplicateKey
            | DeduplicatorArchiveError::MissingSeenAt
            | DeduplicatorArchiveError::Columns
            | DeduplicatorArchiveError::Decode => Self::Keys,
        }
    }
}

/// What a window conversion failure means for the restore: a checkpoint that could not be sealed
/// is an encoding failure, and everything else is archived rows of another shape.
impl From<&WindowArchiveError> for BranchStateRestoreError {
    fn from(error: &WindowArchiveError) -> Self {
        match error {
            WindowArchiveError::Checkpoint => Self::Encoding,
            WindowArchiveError::Open
            | WindowArchiveError::Encode
            | WindowArchiveError::GroupRows
            | WindowArchiveError::RowCount { .. }
            | WindowArchiveError::Sequence
            | WindowArchiveError::Bucket => Self::Rows,
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
    decode_bytes(runtime, bytes, schema, mismatch).await
}

/// Decodes one archived Arrow section as exactly `schema`.
async fn decode_bytes(
    runtime: &Runtime,
    bytes: ChargedBytes,
    schema: &StdArc<arrow_schema::Schema>,
    mismatch: BranchStateRestoreError,
) -> Result<RuntimeRecordBatch, Report<BranchStateRestoreError>> {
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

/// Charges what a decoded group keeps alive while it converts. A refusal fails the conversion
/// rather than waiting, because the group's archived bytes are already held.
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

/// The native keyspace checkpoint of one archived deduplicator branch, every key normalized as the
/// restored branch task normalizes the values its `DEDUPLICATE ON` expressions produce.
///
/// Groups decode and admit one at a time. The resident keyspace grows under its restore metadata
/// charge, and the checkpoint streams from it into a quota-owned file with a fixed bulk working
/// set, so a keyspace larger than the bulk budget converts.
pub(super) async fn prepare_deduplicator_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &DeduplicatorStateDescriptor,
    groups: &[DescribedSection],
    key_schema: &StdArc<arrow_schema::Schema>,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let executor = runtime.executor();
    let mut keys = ArchivedDeduplicatorKeys::new(executor)
        .change_context(BranchStateRestoreError::Admission)?;
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
        // The archived bytes were released by the decode, so waiting for this group's charge
        // holds nothing else of the bulk budget.
        let charge = executor
            .reserve(MemoryClass::Bulk, batch.estimated_bytes().max(1))
            .await
            .change_context(BranchStateRestoreError::Admission)?;
        keys = executor
            .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                keys.admit_group(batch.batch(), cancellation)
                    .map_err(|error| {
                        let context = BranchStateRestoreError::from(error.current_context());
                        error.change_context(context)
                    })?;
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
    keys.admit_encoding()
        .change_context(BranchStateRestoreError::Admission)?;
    let writer = runtime
        .stage_artifact(keys.encoded_bound())
        .await
        .change_context(BranchStateRestoreError::Storage)?;
    let charge = executor
        .reserve(MemoryClass::Bulk, RESTORE_STATE_WORKING_BYTES)
        .await
        .change_context(BranchStateRestoreError::Admission)?;
    writer
        .encode_artifact(charge, move |output, cancellation| {
            keys.write_checkpoint(output, cancellation)
                .change_context(SnapshotStagingError::Encode)
        })
        .await
        .change_context(BranchStateRestoreError::Encoding)
}

/// Reads one archived window group section and decodes it as exactly `schema`, keeping its
/// archived bytes for the checkpoint.
async fn read_rows(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
) -> Result<ArchivedRows, Report<BranchStateRestoreError>> {
    let bytes = archive
        .read_bounded_section(runtime, section, BRANCH_STATE_GROUP_BYTES)
        .await
        .map_err(|error| {
            let context = BranchStateRestoreError::from(error.current_context());
            error.change_context(context)
        })?;
    let batch = decode_bytes(
        runtime,
        bytes.clone(),
        schema,
        BranchStateRestoreError::Rows,
    )
    .await?;
    Ok(ArchivedRows { bytes, batch })
}

/// The native window checkpoint of one archived window branch. The restored branch task re-admits
/// its rows in order to rebuild every aggregate structure, then applies the delayed removals.
///
/// Each group is sealed into quota-owned pieces as it is admitted, so only one decoded group is
/// held at a time and a window larger than the bulk budget converts.
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
    let mut checkpoint = runtime.restored_window_checkpoint(ArchivedWindow {
        revision: descriptor.revision,
        incarnation: descriptor.incarnation,
        branch,
        first_sequence: descriptor.first_sequence,
        next_sequence: descriptor.next_sequence,
        rows: descriptor.rows.clone(),
        accumulators,
    });
    for group in groups {
        nervix_primitives::task::consume_budget().await;
        // The input rows are sealed and released before the argument columns are read, so a
        // conversion holds one archived section and its decoded batch at a time.
        let input = read_rows(runtime, archive, &group.input, &schemas.input).await?;
        let input_charge = retain_group(runtime, &input.batch)?;
        let pending = checkpoint.admit_input(input).await.map_err(|error| {
            let context = BranchStateRestoreError::from(error.current_context());
            error.change_context(context)
        })?;
        drop(input_charge);
        let arguments = read_rows(runtime, archive, &group.arguments, &schemas.arguments).await?;
        let argument_charge = retain_group(runtime, &arguments.batch)?;
        checkpoint
            .admit_arguments(pending, arguments)
            .await
            .map_err(|error| {
                let context = BranchStateRestoreError::from(error.current_context());
                error.change_context(context)
            })?;
        drop(argument_charge);
    }
    checkpoint.finish().await.map_err(|error| {
        let context = BranchStateRestoreError::from(error.current_context());
        error.change_context(context)
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A keyspace conversion the node refused, cancelled or could not encode is reported as that,
    /// so that only archived keys the restored shape does not accept read as a damaged archive.
    #[test]
    fn a_keyspace_conversion_failure_keeps_its_cause() {
        assert!(matches!(
            BranchStateRestoreError::from(&DeduplicatorArchiveError::Admission),
            BranchStateRestoreError::Admission
        ));
        assert!(matches!(
            BranchStateRestoreError::from(&DeduplicatorArchiveError::Cancelled),
            BranchStateRestoreError::Cancelled
        ));
        assert!(matches!(
            BranchStateRestoreError::from(&DeduplicatorArchiveError::Encode),
            BranchStateRestoreError::Encoding
        ));
        for shape in [
            DeduplicatorArchiveError::SeenAtColumn,
            DeduplicatorArchiveError::KeyPart { column: 1 },
            DeduplicatorArchiveError::KeyArity {
                expected: 2,
                found: 1,
            },
            DeduplicatorArchiveError::KeyColumn { column: 0 },
            DeduplicatorArchiveError::DuplicateKey,
            DeduplicatorArchiveError::MissingSeenAt,
            DeduplicatorArchiveError::Columns,
            DeduplicatorArchiveError::Decode,
        ] {
            assert!(
                matches!(
                    BranchStateRestoreError::from(&shape),
                    BranchStateRestoreError::Keys
                ),
                "{shape} is a key shape failure"
            );
        }
    }

    /// A window checkpoint that could not be sealed is reported as an encoding failure, and rows
    /// the restored window does not accept as rows of another shape.
    #[test]
    fn a_window_conversion_failure_keeps_its_cause() {
        assert!(matches!(
            BranchStateRestoreError::from(&WindowArchiveError::Checkpoint),
            BranchStateRestoreError::Encoding
        ));
        for shape in [
            WindowArchiveError::Open,
            WindowArchiveError::Encode,
            WindowArchiveError::GroupRows,
            WindowArchiveError::RowCount {
                expected: 2,
                found: 1,
            },
            WindowArchiveError::Sequence,
            WindowArchiveError::Bucket,
        ] {
            assert!(
                matches!(
                    BranchStateRestoreError::from(&shape),
                    BranchStateRestoreError::Rows
                ),
                "{shape} is a row shape failure"
            );
        }
    }
}
