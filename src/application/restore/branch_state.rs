//! Bounded conversion from archived deduplicator and window sections to native checkpoints.
//!
//! Layer: control plane.
//! - **Owns.** Decoding each archived key or row group under the exact shape the restored models
//!   give it, one bounded group at a time, converting them into the native keyspace or window
//!   checkpoint a quota-owned file holds for the streamed installer, and converting again what
//!   the node refused only for room.
//! - **Depends on.** Archive-owned records, the decision layer's restored shapes, the runtime's
//!   keyspace and window checkpoint codecs, and the bounded executor.
//! - **Must not know.** Database keys, placement ownership or consensus activation.

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_backup::{
    BRANCH_STATE_GROUP_BYTES, DeduplicatorStateDescriptor, DescribedSection, DescribedWindowGroup,
    WindowAccumulatorRecord, WindowStateDescriptor,
};
use nervix_execution::{
    AdmissionError, ChargedBytes, CpuClass, ExecutionError, MemoryClass, QueueAdmission,
    Reservation,
};
use nervix_primitives::{sync::StdArc, time::Instant};
use thiserror::Error;

use super::prepare::{ArchiveSectionReadError, VerifiedArchive};
use crate::{
    registry::{WindowAccumulatorShape, WindowStateSchemas},
    runtime::{
        ArchivedDeduplicatorKeys, ArchivedRows, ArchivedWindow, BranchKey,
        DeduplicatorArchiveError, PendingArguments, RESTORE_STATE_WORKING_BYTES, Runtime,
        SnapshotStagingError, StagedArtifact, WindowAccumulatorState, WindowArchiveError,
        WindowCheckpointBuilder, WindowDelayedRemoval,
    },
    runtime_schema::{ArrowBodyError, RuntimeRecordBatch},
};

/// How long one unit of a conversion, a window section or a keyspace, is converted again after the
/// node refused it only for room, before that refusal ends the conversion.
const ADMISSION_WAIT: Duration = Duration::from_secs(30);

/// How long a refused unit waits before it is converted again.
const ADMISSION_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Debug, Error)]
pub(super) enum BranchStateRestoreError {
    #[error("deduplicator or window restore conversion could not be admitted")]
    Admission,
    #[error("the node had no room for a deduplicator or window restore conversion for {waited:?}")]
    NoRoom { waited: Duration },
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

/// Whether `failure` ended a conversion only because the node had no room for it now: a memory
/// budget with nothing left, a worker queue that is full, or a staging quota that cannot hold
/// another piece. Such a refusal judged nothing of the archive.
fn refused_for_room(failure: &Report<BranchStateRestoreError>) -> bool {
    // A refusal that already outlasted the wait of one unit is not waited for again by the
    // conversion that unit belongs to.
    if let BranchStateRestoreError::NoRoom { .. } = failure.current_context() {
        return false;
    }
    // A report holds one frame for each layer the failure crossed.
    for frame in failure.frames() {
        let memory = frame.downcast_ref::<AdmissionError>();
        if let Some(AdmissionError::BudgetExhausted { .. }) = memory {
            return true;
        }
        let queue = frame.downcast_ref::<ExecutionError>();
        if let Some(ExecutionError::QueueFull { .. }) = queue {
            return true;
        }
        let staging = frame.downcast_ref::<SnapshotStagingError>();
        if let Some(SnapshotStagingError::Full { .. }) = staging {
            return true;
        }
    }
    false
}

/// The bounded wait of one conversion unit for room on the node.
///
/// A unit the node refused only for room releases what it holds and is converted again rather than
/// failing the restore, because the refusal judged nothing. The wait is bounded because the room
/// may never come: what the restore itself holds can be what fills the budget.
#[derive(Debug, Default)]
struct AdmissionWait {
    /// When the node first refused the unit.
    refused_since: Option<Instant>,
}

impl AdmissionWait {
    /// Waits before the unit is converted again after `failure`, or returns the failure that ends
    /// the conversion: any failure but a refusal for room, and a refusal for room once the unit
    /// has been refused for [`ADMISSION_WAIT`], which is then reported as the node having had no
    /// room, whatever the layer that met the refusal called it.
    async fn retry_after(
        &mut self,
        failure: Report<BranchStateRestoreError>,
    ) -> Result<(), Report<BranchStateRestoreError>> {
        if !refused_for_room(&failure) {
            return Err(failure);
        }
        let now = Instant::now();
        let refused_since = *self.refused_since.get_or_insert(now);
        if now.duration_since(refused_since) >= ADMISSION_WAIT {
            return Err(failure.change_context(BranchStateRestoreError::NoRoom {
                waited: ADMISSION_WAIT,
            }));
        }
        nervix_primitives::time::sleep(ADMISSION_RETRY_DELAY).await;
        Ok(())
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

/// Charges what a decoded group keeps alive while it converts. The charge is refused rather than
/// awaited, because the group's archived bytes are already held: the attempt then ends, releasing
/// them, and its unit is converted again.
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
///
/// A keyspace the node refused only for room is converted again from its first group: the keys
/// admitted so far ended with the refused attempt, which is what released their charge.
pub(super) async fn prepare_deduplicator_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &DeduplicatorStateDescriptor,
    groups: &[DescribedSection],
    key_schema: &StdArc<arrow_schema::Schema>,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let mut wait = AdmissionWait::default();
    loop {
        nervix_primitives::task::consume_budget().await;
        let converted = convert_keyspace(runtime, archive, descriptor, groups, key_schema).await;
        match converted {
            Ok(checkpoint) => return Ok(checkpoint),
            Err(failure) => wait.retry_after(failure).await?,
        }
    }
}

/// One attempt to convert an archived keyspace. Everything it holds ends with it.
async fn convert_keyspace(
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
        // The keys admitted so far cannot be presented again short of converting the keyspace
        // from its first group, so this job waits for a place in a full queue instead of being
        // refused.
        keys = executor
            .run_cpu_with(
                CpuClass::Bulk,
                QueueAdmission::WaitForPlace,
                charge,
                move |_charge, cancellation| {
                    keys.admit_group(batch.batch(), cancellation)
                        .map_err(|error| {
                            let context = BranchStateRestoreError::from(error.current_context());
                            error.change_context(context)
                        })?;
                    Ok::<_, Report<BranchStateRestoreError>>(keys)
                },
            )
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
///
/// A section the node refused only for room is sealed again in place. A window refused while its
/// sealed pieces are finished into one checkpoint is converted again from its first group.
pub(super) async fn prepare_window_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &WindowStateDescriptor,
    groups: &[DescribedWindowGroup],
    schemas: &WindowStateSchemas,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let mut wait = AdmissionWait::default();
    loop {
        nervix_primitives::task::consume_budget().await;
        let converted = convert_window(runtime, archive, descriptor, groups, schemas).await;
        match converted {
            Ok(checkpoint) => return Ok(checkpoint),
            Err(failure) => wait.retry_after(failure).await?,
        }
    }
}

/// One attempt to convert an archived window. Everything it holds ends with it.
async fn convert_window(
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
        let pending = seal_input_rows(
            runtime,
            archive,
            &mut checkpoint,
            &group.input,
            &schemas.input,
        )
        .await?;
        seal_argument_columns(
            runtime,
            archive,
            &mut checkpoint,
            &pending,
            &group.arguments,
            &schemas.arguments,
        )
        .await?;
    }
    checkpoint.finish().await.map_err(|error| {
        let context = BranchStateRestoreError::from(error.current_context());
        error.change_context(context)
    })
}

/// Seals one group's input rows into `checkpoint`, reading and decoding them again whenever the
/// node refused an attempt only for room.
async fn seal_input_rows(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    checkpoint: &mut WindowCheckpointBuilder,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
) -> Result<PendingArguments, Report<BranchStateRestoreError>> {
    let mut wait = AdmissionWait::default();
    loop {
        nervix_primitives::task::consume_budget().await;
        let sealed = try_seal_input_rows(runtime, archive, checkpoint, section, schema).await;
        match sealed {
            Ok(pending) => return Ok(pending),
            Err(failure) => wait.retry_after(failure).await?,
        }
    }
}

/// One attempt to seal a group's input rows. The archived bytes, the decoded batch and its charge
/// end with the attempt, and a failed attempt leaves `checkpoint` as it was.
async fn try_seal_input_rows(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    checkpoint: &mut WindowCheckpointBuilder,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
) -> Result<PendingArguments, Report<BranchStateRestoreError>> {
    let input = read_rows(runtime, archive, section, schema).await?;
    let retained = retain_group(runtime, &input.batch)?;
    let admitted = checkpoint.admit_input(input).await;
    drop(retained);
    admitted.map_err(|error| {
        let context = BranchStateRestoreError::from(error.current_context());
        error.change_context(context)
    })
}

/// Seals the argument columns of the group `pending` names into `checkpoint`, reading and decoding
/// them again whenever the node refused an attempt only for room.
async fn seal_argument_columns(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    checkpoint: &mut WindowCheckpointBuilder,
    pending: &PendingArguments,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
) -> Result<(), Report<BranchStateRestoreError>> {
    let mut wait = AdmissionWait::default();
    loop {
        nervix_primitives::task::consume_budget().await;
        let sealed =
            try_seal_argument_columns(runtime, archive, checkpoint, pending, section, schema).await;
        match sealed {
            Ok(()) => return Ok(()),
            Err(failure) => wait.retry_after(failure).await?,
        }
    }
}

/// One attempt to seal a group's argument columns. The archived bytes, the decoded batch and its
/// charge end with the attempt, and a failed attempt leaves `checkpoint` as it was.
async fn try_seal_argument_columns(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    checkpoint: &mut WindowCheckpointBuilder,
    pending: &PendingArguments,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
) -> Result<(), Report<BranchStateRestoreError>> {
    let arguments = read_rows(runtime, archive, section, schema).await?;
    let retained = retain_group(runtime, &arguments.batch)?;
    let admitted = checkpoint.admit_arguments(pending, arguments).await;
    drop(retained);
    admitted.map_err(|error| {
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

    fn out_of_memory() -> Report<BranchStateRestoreError> {
        Report::new(AdmissionError::BudgetExhausted {
            class: "bulk",
            requested: 8,
        })
        .change_context(BranchStateRestoreError::Encoding)
    }

    /// A conversion is converted again only after a refusal that judged nothing: a memory budget
    /// with no room left, a full worker queue, or a staging quota that is full now. A request no
    /// budget can ever hold, a closed pool and archived content of another shape end it.
    #[test]
    fn only_a_refusal_for_room_is_converted_again() {
        assert!(refused_for_room(&out_of_memory()));
        let queue_full = Report::new(ExecutionError::QueueFull {
            class: "bulk",
            pending: 4,
        })
        .change_context(BranchStateRestoreError::Admission);
        assert!(refused_for_room(&queue_full));
        let staging_full = Report::new(SnapshotStagingError::Full { requested: 8 })
            .change_context(BranchStateRestoreError::Encoding);
        assert!(refused_for_room(&staging_full));

        let beyond_the_budget = Report::new(AdmissionError::ExceedsBudget {
            class: "bulk",
            requested: 9,
            capacity: 8,
        })
        .change_context(BranchStateRestoreError::Admission);
        assert!(!refused_for_room(&beyond_the_budget));
        let closed = Report::new(ExecutionError::PoolClosed { class: "bulk" })
            .change_context(BranchStateRestoreError::Admission);
        assert!(!refused_for_room(&closed));
        let beyond_the_quota = Report::new(SnapshotStagingError::QuotaExceeded {
            actual: 9,
            limit: 8,
        })
        .change_context(BranchStateRestoreError::Encoding);
        assert!(!refused_for_room(&beyond_the_quota));
        assert!(!refused_for_room(&Report::new(
            BranchStateRestoreError::Rows
        )));
    }

    /// A unit the node refused for room is converted again until it has been refused for the
    /// whole wait. The refusal then ends the conversion and is reported as the node having had no
    /// room, whatever the layer that met it called it. Any other failure ends the conversion at
    /// once.
    #[nervix_primitives::test(start_paused = true)]
    async fn a_refusal_for_room_is_retried_until_its_wait_ends() {
        let mut wait = AdmissionWait::default();
        wait.retry_after(out_of_memory())
            .await
            .assured("the first refusal is retried");
        wait.retry_after(out_of_memory())
            .await
            .assured("a refusal inside the wait is retried");
        nervix_primitives::time::advance(ADMISSION_WAIT).await;
        let ended = wait
            .retry_after(out_of_memory())
            .await
            .expect_err("a refusal that outlasts the wait ends the conversion");
        assert!(matches!(
            ended.current_context(),
            BranchStateRestoreError::NoRoom { waited } if *waited == ADMISSION_WAIT
        ));
        assert!(
            ended.contains::<AdmissionError>(),
            "the report still names the refusal that ended it"
        );
        assert!(
            !refused_for_room(&ended),
            "the conversion the unit belongs to does not wait for the same refusal again"
        );

        let shape = AdmissionWait::default()
            .retry_after(Report::new(BranchStateRestoreError::Keys))
            .await
            .expect_err("archived keys of another shape are not retried");
        assert!(matches!(
            shape.current_context(),
            BranchStateRestoreError::Keys
        ));
    }

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
