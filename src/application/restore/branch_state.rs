//! Bounded conversion from archived deduplicator and window sections to native checkpoints.
//!
//! Layer: control plane.
//! - **Owns.** Decoding each archived key or row group under the exact shape the restored models
//!   give it, one bounded group at a time, converting them into the native keyspace or window
//!   checkpoint a quota-owned file holds for the streamed installer, each conversion within its own
//!   shares of the memory classes it charges, and converting again what the staging quota had no
//!   room for.
//! - **Depends on.** Archive-owned records, the decision layer's restored shapes, the runtime's
//!   keyspace and window checkpoint codecs, and the bounded executor.
//! - **Must not know.** Database keys, placement ownership or consensus activation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_backup::{
    BRANCH_STATE_GROUP_BYTES, DeduplicatorStateDescriptor, DescribedSection, DescribedWindowGroup,
    WindowAccumulatorRecord, WindowStateDescriptor,
};
use nervix_execution::{ChargedBytes, CpuClass, Executor, MemoryClass, Reservation};
use nervix_primitives::{sync::StdArc, time::Instant};
use thiserror::Error;

use super::prepare::{ArchiveSectionReadError, VerifiedArchive};
use crate::{
    registry::{WindowAccumulatorShape, WindowStateSchemas},
    runtime::{
        ArchivedDeduplicatorKeys, ArchivedRows, ArchivedWindow, BranchKey,
        DeduplicatorArchiveError, PendingArguments, Runtime, SnapshotStagingError, StagedArtifact,
        WindowAccumulatorState, WindowArchiveError, WindowCheckpointBuilder, WindowDelayedRemoval,
    },
    runtime_schema::{ArrowBodyError, RuntimeRecordBatch},
};

/// Immutable local conversion policy captured at the restore boundary: how long a conversion
/// waits for room in the staging quota.
#[derive(Debug, Clone, Copy)]
pub(super) struct RestoreAdmissionPolicy {
    wait: Duration,
    retry: Duration,
}
impl RestoreAdmissionPolicy {
    pub(super) fn new(settings: &nervix_models::ControlPlaneCapabilities) -> Self {
        Self {
            wait: settings.restore_admission_wait(),
            retry: settings.restore_admission_retry_interval(),
        }
    }
}

#[derive(Debug, Error)]
pub(super) enum BranchStateRestoreError {
    #[error("deduplicator or window restore conversion could not be admitted")]
    Admission,
    #[error(
        "the staging quota had no room for a deduplicator or window restore conversion for \
         {waited:?}"
    )]
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

/// Whether `failure` ended a conversion only because the staging quota cannot hold another piece
/// now. Such a refusal judged nothing of the archive. A memory class never refuses a conversion
/// for room: each conversion runs within its own shares, which it waits for.
fn refused_for_room(failure: &Report<BranchStateRestoreError>) -> bool {
    // A refusal that already outlasted the wait is not waited for again.
    if let BranchStateRestoreError::NoRoom { .. } = failure.current_context() {
        return false;
    }
    // A report holds one frame for each layer the failure crossed.
    for frame in failure.frames() {
        let staging = frame.downcast_ref::<SnapshotStagingError>();
        if let Some(SnapshotStagingError::Full { .. }) = staging {
            return true;
        }
    }
    false
}

/// The bounded wait of one conversion for room in the staging quota.
///
/// A conversion the staging quota refused releases what it holds, its shares of memory included,
/// and is converted again rather than failing the restore, because the refusal judged nothing.
/// The wait is bounded because the room may never come: what the restore itself stages can be
/// what fills the quota.
#[derive(Debug)]
struct AdmissionWait {
    policy: RestoreAdmissionPolicy,
    /// When the node first refused the unit.
    refused_since: Option<Instant>,
}

impl AdmissionWait {
    fn new(policy: RestoreAdmissionPolicy) -> Self {
        Self {
            policy,
            refused_since: None,
        }
    }
    /// Waits before the unit is converted again after `failure`, or returns the failure that ends
    /// the conversion: any failure but a refusal for room, and a refusal for room once the unit
    /// has been refused for its captured wait, which is then reported as the node having had no
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
        if now.duration_since(refused_since) >= self.policy.wait {
            return Err(failure.change_context(BranchStateRestoreError::NoRoom {
                waited: self.policy.wait,
            }));
        }
        nervix_primitives::time::sleep(self.policy.retry).await;
        Ok(())
    }
}

/// Reads one archived group and decodes it as exactly `schema`, charging through `executor`.
async fn decode_group(
    executor: &Executor,
    archive: &VerifiedArchive,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
    mismatch: BranchStateRestoreError,
) -> Result<RuntimeRecordBatch, Report<BranchStateRestoreError>> {
    let bytes = {
        let mapping_input = archive
            .read_bounded_section(executor, section, BRANCH_STATE_GROUP_BYTES)
            .await;
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let context = BranchStateRestoreError::from(error.current_context());
                error.change_context(context)
            }),
        }
    }?;
    decode_bytes(executor, bytes, schema, mismatch).await
}

/// Decodes one archived Arrow section as exactly `schema`, charging through `executor`.
async fn decode_bytes(
    executor: &Executor,
    bytes: ChargedBytes,
    schema: &StdArc<arrow_schema::Schema>,
    mismatch: BranchStateRestoreError,
) -> Result<RuntimeRecordBatch, Report<BranchStateRestoreError>> {
    {
        let mapping_input = RuntimeRecordBatch::decode_arrow_snapshot_section(
            executor,
            StdArc::clone(schema),
            bytes,
        )
        .await;
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let context = match error.current_context() {
                    ArrowBodyError::Admission | ArrowBodyError::Execution => {
                        BranchStateRestoreError::Admission
                    }
                    ArrowBodyError::Cancelled => BranchStateRestoreError::Cancelled,
                    _ => mismatch,
                };
                error.change_context(context)
            }),
        }
    }
}

/// Charges what a decoded group keeps alive while it converts, from the conversion's bulk share:
/// the group's archived bytes are already held, so the charge never waits, and a share without
/// room for it ends the attempt with the share the conversion needs.
async fn retain_group(
    executor: &Executor,
    batch: &RuntimeRecordBatch,
) -> Result<Reservation, Report<BranchStateRestoreError>> {
    let bytes = batch.estimated_bytes().max(1);
    executor
        .reserve(MemoryClass::Bulk, bytes)
        .await
        .change_context(BranchStateRestoreError::Admission)
}

/// The bulk share a branch state conversion starts from: its largest archived group with the
/// batch it decodes into and that batch's charge, about three times the group, or the native
/// checkpoint's encoding working set when that is larger, up to the whole class. A conversion
/// that needs more gives everything back and runs again within a larger share.
fn conversion_bulk_share<'a>(
    executor: &Executor,
    sections: impl IntoIterator<Item = &'a DescribedSection>,
) -> u64 {
    let mut largest = 0_u64;
    for section in sections {
        largest = largest.max(section.length);
    }
    let encoding = VerifiedArchive::native_checkpoint_encoding_bytes(executor);
    nervix_primitives::expect_lint!(
        nervix::clamped_arithmetic,
        "a first share beyond u64 bytes is capped at the class below",
        largest.saturating_mul(3)
    )
    .max(encoding)
    .min(executor.memory(MemoryClass::Bulk).capacity_bytes)
}

/// The native keyspace checkpoint of one archived deduplicator branch, every key normalized as the
/// restored branch task normalizes the values its `DEDUPLICATE ON` expressions produce.
///
/// Groups decode and admit one at a time. The resident keyspace grows under its restore metadata
/// charge, and the checkpoint streams from it into a quota-owned file with a fixed bulk working
/// set, so a keyspace larger than the bulk budget converts.
///
/// The keyspace converts within its own shares of restore metadata, for its keys, and of bulk
/// memory, for its groups and its encoding, which it waits for while it holds nothing of either.
/// A keyspace that needs more than a share gives everything back and is converted again from its
/// first group within a larger one: the keys admitted so far ended with that attempt, which is
/// what released their charge. So a full class holds the conversion back and never refuses it. A
/// keyspace the staging quota refused is converted again after a wait.
pub(super) async fn prepare_deduplicator_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &DeduplicatorStateDescriptor,
    groups: &[DescribedSection],
    key_schema: &StdArc<arrow_schema::Schema>,
    policy: RestoreAdmissionPolicy,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let mut wait = AdmissionWait::new(policy);
    let executor = runtime.executor();
    let bulk_share = conversion_bulk_share(executor, groups);
    loop {
        nervix_primitives::task::consume_budget().await;
        let converted = executor
            .within_allowance(
                MemoryClass::RestoreMetadata,
                ArchivedDeduplicatorKeys::FIRST_CHARGE_BYTES,
                |metadata| async move {
                    metadata
                        .within_allowance(MemoryClass::Bulk, bulk_share, |scoped| async move {
                            convert_keyspace(
                                runtime, &scoped, archive, descriptor, groups, key_schema,
                            )
                            .await
                        })
                        .await
                        .change_context(BranchStateRestoreError::Admission)?
                },
            )
            .await
            .change_context(BranchStateRestoreError::Admission)
            .and_then(|converted| converted);
        match converted {
            Ok(checkpoint) => return Ok(checkpoint),
            Err(failure) => wait.retry_after(failure).await?,
        }
    }
}

/// One attempt to convert an archived keyspace, charging everything through `executor`, which
/// draws from the conversion's shares. Everything it holds ends with it.
async fn convert_keyspace(
    runtime: &Runtime,
    executor: &Executor,
    archive: &VerifiedArchive,
    descriptor: &DeduplicatorStateDescriptor,
    groups: &[DescribedSection],
    key_schema: &StdArc<arrow_schema::Schema>,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let mut keys = ArchivedDeduplicatorKeys::new(executor)
        .change_context(BranchStateRestoreError::Admission)?;
    for group in groups {
        nervix_primitives::task::consume_budget().await;
        let batch = decode_group(
            executor,
            archive,
            group,
            key_schema,
            BranchStateRestoreError::Keys,
        )
        .await?;
        // The archived bytes were released by the decode; the group's charge is drawn from the
        // conversion's share.
        let charge = executor
            .reserve(MemoryClass::Bulk, batch.estimated_bytes().max(1))
            .await
            .change_context(BranchStateRestoreError::Admission)?;
        // The keys admitted so far cannot be presented again short of converting the keyspace
        // from its first group, so this job waits for a place in a full queue.
        keys = executor
            .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                {
                    let mapping_input = keys.admit_group(batch.batch(), cancellation);
                    match mapping_input {
                        Ok(mapped_value) => Ok(mapped_value),
                        Err(error) => Err({
                            let context = BranchStateRestoreError::from(error.current_context());
                            error.change_context(context)
                        }),
                    }
                }?;
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
    let charge = VerifiedArchive::reserve_native_checkpoint_encoding(executor)
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
/// archived bytes for the checkpoint, and charging through `executor`.
async fn read_rows(
    executor: &Executor,
    archive: &VerifiedArchive,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
) -> Result<ArchivedRows, Report<BranchStateRestoreError>> {
    let bytes = {
        let mapping_input = archive
            .read_bounded_section(executor, section, BRANCH_STATE_GROUP_BYTES)
            .await;
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let context = BranchStateRestoreError::from(error.current_context());
                error.change_context(context)
            }),
        }
    }?;
    let batch = decode_bytes(
        executor,
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
/// The window converts within its own share of bulk memory, which it waits for while it holds
/// nothing of the class, and every group releases its charges back into the share before the next.
/// A window that needs more than the share gives everything back and is converted again from its
/// first group within a larger one, so a full class holds the conversion back and never refuses
/// it. A window the staging quota refused is converted again after a wait.
pub(super) async fn prepare_window_checkpoint(
    runtime: &Runtime,
    archive: &VerifiedArchive,
    descriptor: &WindowStateDescriptor,
    groups: &[DescribedWindowGroup],
    schemas: &WindowStateSchemas,
    policy: RestoreAdmissionPolicy,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let mut wait = AdmissionWait::new(policy);
    let executor = runtime.executor();
    let share = conversion_bulk_share(
        executor,
        groups
            .iter()
            .flat_map(|group| [&group.input, &group.arguments]),
    );
    loop {
        nervix_primitives::task::consume_budget().await;
        let converted = executor
            .within_allowance(MemoryClass::Bulk, share, |bulk| async move {
                convert_window(runtime, &bulk, archive, descriptor, groups, schemas).await
            })
            .await
            .change_context(BranchStateRestoreError::Admission)
            .and_then(|converted| converted);
        match converted {
            Ok(checkpoint) => return Ok(checkpoint),
            Err(failure) => wait.retry_after(failure).await?,
        }
    }
}

/// One attempt to convert an archived window, charging everything through `executor`, which
/// draws from the conversion's share. Everything it holds ends with it.
async fn convert_window(
    runtime: &Runtime,
    executor: &Executor,
    archive: &VerifiedArchive,
    descriptor: &WindowStateDescriptor,
    groups: &[DescribedWindowGroup],
    schemas: &WindowStateSchemas,
) -> Result<StagedArtifact, Report<BranchStateRestoreError>> {
    let accumulators = window_accumulators(descriptor, schemas)?;
    let branch = super::steps::remote_branch_key(&descriptor.branch);
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
            executor,
            archive,
            &mut checkpoint,
            &group.input,
            &schemas.input,
        )
        .await?;
        seal_argument_columns(
            executor,
            archive,
            &mut checkpoint,
            &pending,
            &group.arguments,
            &schemas.arguments,
        )
        .await?;
    }
    {
        let mapping_input = checkpoint.finish(executor).await;
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let context = BranchStateRestoreError::from(error.current_context());
                error.change_context(context)
            }),
        }
    }
}

/// Seals one group's input rows into `checkpoint`. The archived bytes, the decoded batch and its
/// charge end before this returns, and a failure leaves `checkpoint` as it was.
async fn seal_input_rows(
    executor: &Executor,
    archive: &VerifiedArchive,
    checkpoint: &mut WindowCheckpointBuilder,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
) -> Result<PendingArguments, Report<BranchStateRestoreError>> {
    let input = read_rows(executor, archive, section, schema).await?;
    let retained = retain_group(executor, &input.batch).await?;
    let admitted = checkpoint.admit_input(executor, input).await;
    drop(retained);
    {
        let mapping_input = admitted;
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let context = BranchStateRestoreError::from(error.current_context());
                error.change_context(context)
            }),
        }
    }
}

/// Seals the argument columns of the group `pending` names into `checkpoint`. The archived bytes,
/// the decoded batch and its charge end before this returns, and a failure leaves `checkpoint` as
/// it was.
async fn seal_argument_columns(
    executor: &Executor,
    archive: &VerifiedArchive,
    checkpoint: &mut WindowCheckpointBuilder,
    pending: &PendingArguments,
    section: &DescribedSection,
    schema: &StdArc<arrow_schema::Schema>,
) -> Result<(), Report<BranchStateRestoreError>> {
    let arguments = read_rows(executor, archive, section, schema).await?;
    let retained = retain_group(executor, &arguments.batch).await?;
    let admitted = checkpoint
        .admit_arguments(executor, pending, arguments)
        .await;
    drop(retained);
    {
        let mapping_input = admitted;
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let context = BranchStateRestoreError::from(error.current_context());
                error.change_context(context)
            }),
        }
    }
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
impl Default for AdmissionWait {
    fn default() -> Self {
        Self::new(RestoreAdmissionPolicy::new(
            &nervix_models::ControlPlaneCapabilities::default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use nervix_execution::{AdmissionError, ExecutionError};

    use super::*;

    fn staging_full() -> Report<BranchStateRestoreError> {
        Report::new(SnapshotStagingError::Full { requested: 8 })
            .change_context(BranchStateRestoreError::Encoding)
    }

    /// A conversion is converted again after a wait only when the staging quota is full now, a
    /// refusal that judged nothing. A memory class never refuses a conversion for room, so a
    /// memory refusal that reaches here is not waited for. A request no budget can ever hold, a
    /// closed pool and archived content of another shape end it.
    #[test]
    fn only_a_refusal_for_room_is_converted_again() {
        assert!(refused_for_room(&staging_full()));
        let out_of_memory = Report::new(AdmissionError::BudgetExhausted {
            class: "bulk",
            requested: 8,
        })
        .change_context(BranchStateRestoreError::Encoding);
        assert!(!refused_for_room(&out_of_memory));

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

    /// A conversion the staging quota refused for room is converted again until it has been
    /// refused for the whole wait. The refusal then ends the conversion and is reported as the
    /// quota having had no room, whatever the layer that met it called it. Any other failure ends
    /// the conversion at once.
    #[nervix_primitives::test(start_paused = true)]
    async fn a_refusal_for_room_is_retried_until_its_wait_ends() {
        let mut wait = AdmissionWait::default();
        wait.retry_after(staging_full())
            .await
            .assured("the first refusal is retried");
        wait.retry_after(staging_full())
            .await
            .assured("a refusal inside the wait is retried");
        nervix_primitives::time::advance(
            nervix_models::ControlPlaneCapabilities::default().restore_admission_wait(),
        )
        .await;
        let ended = wait
            .retry_after(staging_full())
            .await
            .expect_err("a refusal that outlasts the wait ends the conversion");
        assert!(matches!(
            ended.current_context(),
            BranchStateRestoreError::NoRoom { waited } if *waited == nervix_models::ControlPlaneCapabilities::default().restore_admission_wait()
        ));
        assert!(
            ended.contains::<SnapshotStagingError>(),
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
