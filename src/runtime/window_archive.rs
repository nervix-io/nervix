//! One window branch's retained rows as archive Arrow groups, and the window rebuilt from them.
//!
//! Layer: data plane.
//! - **Owns.** Opening a published or persisted window against its schemas, bounded row-aligned
//!   groups of input rows and argument columns, and the native window checkpoint rebuilt from
//!   archived groups for the branch task to re-admit.
//! - **Depends on.** The window snapshot codec, the shared columnar generation codec and Arrow
//!   row views.
//! - **Must not know.** Archive records or paths, capture fencing, or restore placement.

use std::ops::Range;

use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_execution::{ChargedBytes, Executor};
use nervix_models::{RemoteRuntimeRecordMetadata, Timestamp};
use nervix_primitives::sync::{Arc, StdArc};

use super::{
    BranchKey, LinearHistogramDelayedRemovalSnapshot, MaterializedGeneration,
    MaterializedGenerationRecord, Runtime, StagedArtifact, WindowAccumulatorSnapshot,
    WindowProcessorStateSnapshot,
    published_generation::Generation,
    snapshot_staging::SnapshotStaging,
    window_state::{
        WindowCheckpointPieces, WindowPublishedSnapshot, WindowSnapshotLifetime,
        WindowSnapshotSchemas, decode_window_processor_snapshot,
    },
};
use crate::runtime_schema::{RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow};

/// Why a window could not become archive groups, or archive groups a window checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WindowArchiveError {
    #[error("the persisted window checkpoint could not be opened against its schemas")]
    Open,
    #[error("the window's retained rows could not be encoded as Arrow columns")]
    Encode,
    #[error("a window group's input rows and argument columns hold different row counts")]
    GroupRows,
    #[error("the archived window groups hold {found} rows where its descriptor lists {expected}")]
    RowCount { expected: usize, found: usize },
    #[error("the archived window's row sequences overflow")]
    Sequence,
    #[error("an archived histogram bucket does not fit this node's address space")]
    Bucket,
    #[error("the restored window checkpoint could not be encoded")]
    Checkpoint,
}

/// A stepped row a linear histogram still counts, as the archive carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WindowDelayedRemoval {
    pub(crate) expires_at: Timestamp,
    pub(crate) bucket: u64,
}

/// What one aggregate structure keeps beyond the rows its window retains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WindowAccumulatorState {
    Retained,
    LinearHistogram {
        delayed_removals: Vec<WindowDelayedRemoval>,
    },
}

/// A window a backup captured: its branch task's last publication or its persisted checkpoint,
/// at the revision it stands at.
#[derive(Debug, Clone)]
pub(crate) struct CapturedWindow {
    revision: u64,
    source: CapturedWindowSource,
}

#[derive(Debug, Clone)]
enum CapturedWindowSource {
    /// The shared row views the branch task published.
    Live(WindowProcessorStateSnapshot),
    /// A checkpoint that opens only against the window's input and argument schemas.
    Sealed(Vec<u8>),
}

impl CapturedWindow {
    /// What a published generation holds, or nothing for a window that never published.
    pub(super) fn published(
        generation: &Generation<Option<WindowPublishedSnapshot>>,
    ) -> Option<Self> {
        let source = match &generation.value {
            None => return None,
            Some(WindowPublishedSnapshot::Live(snapshot)) => {
                CapturedWindowSource::Live(snapshot.clone())
            }
            Some(WindowPublishedSnapshot::Sealed(payload)) => {
                CapturedWindowSource::Sealed(payload.clone())
            }
        };
        Some(Self {
            revision: generation.revision,
            source,
        })
    }

    /// A persisted checkpoint at `revision`.
    pub(super) fn stored(revision: u64, payload: Vec<u8>) -> Self {
        Self {
            revision,
            source: CapturedWindowSource::Sealed(payload),
        }
    }

    /// Opens the window against `input` and `arguments`, the schemas its model gives its retained
    /// rows. A checkpoint with no branch lifetime, an empty forced-recovery window, opens as nothing.
    pub(crate) async fn open(
        self,
        executor: &Executor,
        input: &StdArc<ArrowSchema>,
        arguments: &StdArc<ArrowSchema>,
    ) -> error_stack::Result<Option<CapturedWindowState>, WindowArchiveError> {
        let snapshot = match self.source {
            CapturedWindowSource::Live(snapshot) => snapshot,
            CapturedWindowSource::Sealed(payload) => {
                let decoded = decode_window_processor_snapshot(
                    &payload,
                    executor,
                    WindowSnapshotSchemas { input, arguments },
                    WindowSnapshotLifetime::Recorded,
                )
                .await
                .change_context(WindowArchiveError::Open)?;
                let Some(decoded) = decoded else {
                    return Ok(None);
                };
                decoded
            }
        };
        let Some(incarnation) = snapshot.incarnation else {
            return Ok(None);
        };
        Ok(Some(CapturedWindowState::new(
            self.revision,
            incarnation,
            snapshot,
        )))
    }
}

/// A captured window with a branch lifetime, ready to be written as archive groups.
#[derive(Debug, Clone)]
pub(crate) struct CapturedWindowState {
    revision: u64,
    incarnation: u64,
    snapshot: WindowProcessorStateSnapshot,
    input: MaterializedGeneration,
    arguments: MaterializedGeneration,
}

impl CapturedWindowState {
    fn new(revision: u64, incarnation: u64, snapshot: WindowProcessorStateSnapshot) -> Self {
        let mut input_records = Vec::with_capacity(snapshot.entries.len());
        let mut argument_records = Vec::with_capacity(snapshot.entries.len());
        for entry in &snapshot.entries {
            input_records.push(MaterializedGenerationRecord {
                branch: entry.key.clone(),
                row: entry.record.clone(),
            });
            argument_records.push(MaterializedGenerationRecord {
                branch: None,
                row: entry.arguments.clone(),
            });
        }
        let (input_schema, argument_schema) = match snapshot.entries.first() {
            Some(entry) => (
                entry.record.one_row_batch().schema(),
                entry.arguments.one_row_batch().schema(),
            ),
            None => (
                StdArc::new(ArrowSchema::empty()),
                StdArc::new(ArrowSchema::empty()),
            ),
        };
        Self {
            revision,
            incarnation,
            input: MaterializedGeneration::new(
                revision,
                0,
                incarnation,
                input_schema,
                input_records,
            ),
            arguments: MaterializedGeneration::new(
                revision,
                0,
                incarnation,
                argument_schema,
                argument_records,
            ),
            snapshot,
        }
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn incarnation(&self) -> u64 {
        self.incarnation
    }

    pub(crate) fn next_sequence(&self) -> u64 {
        self.snapshot.next_sequence
    }

    /// The admission sequence of the oldest retained row.
    pub(crate) fn first_sequence(&self) -> Option<u64> {
        let first = self.snapshot.entries.first()?;
        Some(first.sequence)
    }

    /// Each retained row's ingestion watermarks, oldest first.
    pub(crate) fn row_watermarks(&self) -> Vec<RemoteRuntimeRecordMetadata> {
        self.snapshot
            .entries
            .iter()
            .map(|entry| entry.record.metadata().to_remote())
            .collect()
    }

    /// What every aggregate structure keeps beyond the retained rows, in demand order.
    pub(crate) fn accumulators(
        &self,
    ) -> error_stack::Result<Vec<WindowAccumulatorState>, WindowArchiveError> {
        let mut accumulators = Vec::with_capacity(self.snapshot.accumulators.len());
        for accumulator in &self.snapshot.accumulators {
            let state = match accumulator {
                WindowAccumulatorSnapshot::Retained => WindowAccumulatorState::Retained,
                WindowAccumulatorSnapshot::LinearHistogram { delayed_removals } => {
                    let mut removals = Vec::with_capacity(delayed_removals.len());
                    for removal in delayed_removals {
                        let bucket = u64::try_from(removal.bucket)
                            .map_err(|_| Report::new(WindowArchiveError::Bucket))?;
                        removals.push(WindowDelayedRemoval {
                            expires_at: removal.expires_at,
                            bucket,
                        });
                    }
                    WindowAccumulatorState::LinearHistogram {
                        delayed_removals: removals,
                    }
                }
            };
            accumulators.push(state);
        }
        Ok(accumulators)
    }

    /// Consecutive retained rows whose input and argument payloads each stay within `limit`,
    /// oldest first. A row larger than the limit occupies a group of its own.
    pub(crate) fn groups(&self, limit: u64) -> Vec<Range<usize>> {
        let mut groups = Vec::new();
        let mut start = 0;
        let mut input = 0_u64;
        let mut arguments = 0_u64;
        for (index, entry) in self.snapshot.entries.iter().enumerate() {
            let input_bytes = entry.record.one_row_batch().estimated_bytes();
            let argument_bytes = entry.arguments.one_row_batch().estimated_bytes();
            let next_input = input.checked_add(input_bytes);
            let next_arguments = arguments.checked_add(argument_bytes);
            let overruns = match (next_input, next_arguments) {
                (Some(next_input), Some(next_arguments)) => {
                    next_input > limit || next_arguments > limit
                }
                _ => true,
            };
            if index > start && overruns {
                groups.push(start..index);
                start = index;
                input = input_bytes;
                arguments = argument_bytes;
                continue;
            }
            input = next_input.unwrap_or(limit);
            arguments = next_arguments.unwrap_or(limit);
        }
        if start < self.snapshot.entries.len() {
            groups.push(start..self.snapshot.entries.len());
        }
        groups
    }

    /// The retained input rows in `group` as one Arrow IPC stream of the window's input schema.
    pub(crate) async fn encode_input_group(
        &self,
        executor: &Executor,
        group: &Range<usize>,
    ) -> error_stack::Result<ChargedBytes, WindowArchiveError> {
        self.input
            .encode_columns(executor, group)
            .await
            .change_context(WindowArchiveError::Encode)
    }

    /// The argument columns of the rows in `group`, row-aligned with their input rows.
    pub(crate) async fn encode_argument_group(
        &self,
        executor: &Executor,
        group: &Range<usize>,
    ) -> error_stack::Result<ChargedBytes, WindowArchiveError> {
        self.arguments
            .encode_columns(executor, group)
            .await
            .change_context(WindowArchiveError::Encode)
    }
}

/// Everything of an archived window except its rows, as a restore converts it.
#[derive(Debug, Clone)]
pub(crate) struct ArchivedWindow {
    pub(crate) revision: u64,
    pub(crate) incarnation: u64,
    pub(crate) branch: Option<BranchKey>,
    pub(crate) first_sequence: Option<u64>,
    pub(crate) next_sequence: u64,
    pub(crate) rows: Vec<RemoteRuntimeRecordMetadata>,
    pub(crate) accumulators: Vec<WindowAccumulatorState>,
}

/// One archived Arrow section of window rows and the batch it decoded into under its exact schema.
pub(crate) struct ArchivedRows {
    pub(crate) bytes: ChargedBytes,
    pub(crate) batch: RuntimeRecordBatch,
}

/// A group whose input rows were sealed and whose argument columns are still to come.
#[derive(Debug)]
#[must_use = "a group's argument columns complete it"]
pub(crate) struct PendingArguments {
    /// The position of the group's first row among the window's retained rows.
    first_row: usize,
    rows: usize,
}

/// A window checkpoint being rebuilt from archived groups, oldest row first.
///
/// Each group's input rows and then its argument columns are sealed into quota-owned pieces as
/// soon as they are admitted, keeping their archived Arrow sections whenever the rows' identities
/// fit one identity record, so the window never exists whole in memory however much it retains.
pub(crate) struct WindowCheckpointBuilder {
    executor: Executor,
    window: ArchivedWindow,
    /// How many of the window's rows the admitted input rows and argument columns covered.
    admitted_input: usize,
    admitted_arguments: usize,
    pieces: WindowCheckpointPieces,
}

impl Runtime {
    /// A builder for the native checkpoint of the archived window `window`, staged under this
    /// node's staging quota.
    pub(crate) fn restored_window_checkpoint(
        &self,
        window: ArchivedWindow,
    ) -> WindowCheckpointBuilder {
        WindowCheckpointBuilder::new(&self.inner.executor, &self.inner.snapshot_staging, window)
    }
}

impl WindowCheckpointBuilder {
    pub(in crate::runtime) fn new(
        executor: &Executor,
        staging: &SnapshotStaging,
        window: ArchivedWindow,
    ) -> Self {
        let pieces =
            WindowCheckpointPieces::new(staging, window.revision, Some(window.incarnation));
        Self {
            executor: executor.clone(),
            window,
            admitted_input: 0,
            admitted_arguments: 0,
            pieces,
        }
    }

    /// The watermarks the descriptor lists for the rows from `first_row`, `rows` of them, each as
    /// the metadata its input row and its argument row carry.
    fn row_metadata(
        &self,
        first_row: usize,
        rows: usize,
    ) -> error_stack::Result<Vec<RuntimeRecordMetadata>, WindowArchiveError> {
        let end = first_row
            .checked_add(rows)
            .assured("an admitted row count is shorter than the address space");
        let Some(watermarks) = self.window.rows.get(first_row..end) else {
            return Err(Report::new(WindowArchiveError::RowCount {
                expected: self.window.rows.len(),
                found: end,
            }));
        };
        let mut metadata = Vec::with_capacity(rows);
        for watermark in watermarks {
            metadata.push(RuntimeRecordMetadata::from_remote(watermark.clone()));
        }
        Ok(metadata)
    }

    /// Admits one group's input rows and seals them before this returns. The group's argument
    /// columns, which must hold the same rows, complete it.
    pub(crate) async fn admit_input(
        &mut self,
        input: ArchivedRows,
    ) -> error_stack::Result<PendingArguments, WindowArchiveError> {
        if self.admitted_arguments != self.admitted_input {
            return Err(Report::new(WindowArchiveError::GroupRows));
        }
        let first_row = self.admitted_input;
        let rows = input.batch.batch().num_rows();
        let metadata = self.row_metadata(first_row, rows)?;
        let Some(first) = self.window.first_sequence else {
            return Err(Report::new(WindowArchiveError::Sequence));
        };
        let first_offset =
            u64::try_from(first_row).map_err(|_| Report::new(WindowArchiveError::Sequence))?;
        let Some(first_sequence) = first.checked_add(first_offset) else {
            return Err(Report::new(WindowArchiveError::Sequence));
        };
        let rows_u64 =
            u64::try_from(rows).map_err(|_| Report::new(WindowArchiveError::Sequence))?;
        if let Some(last_offset) = rows_u64.checked_sub(1)
            && first_sequence.checked_add(last_offset).is_none()
        {
            return Err(Report::new(WindowArchiveError::Sequence));
        }
        let schema = input.batch.schema();
        let batch = Arc::new(input.batch);
        let mut records = Vec::with_capacity(rows);
        for (row, metadata) in metadata.into_iter().enumerate() {
            let record = RuntimeRow::new(Arc::clone(&batch), row, metadata)
                .change_context(WindowArchiveError::GroupRows)?;
            records.push(MaterializedGenerationRecord {
                branch: self.window.branch.clone(),
                row: record,
            });
        }
        let generation = self.pieces.nested_generation(schema, records);
        self.pieces
            .append_encoded_input(&self.executor, first_sequence, &generation, input.bytes)
            .await
            .change_context(WindowArchiveError::Checkpoint)?;
        self.admitted_input = first_row
            .checked_add(rows)
            .assured("an admitted row count is shorter than the address space");
        Ok(PendingArguments { first_row, rows })
    }

    /// Admits the argument columns of the group whose input rows `pending` names, which must hold
    /// the same number of rows, and seals them before this returns.
    pub(crate) async fn admit_arguments(
        &mut self,
        pending: PendingArguments,
        arguments: ArchivedRows,
    ) -> error_stack::Result<(), WindowArchiveError> {
        if arguments.batch.batch().num_rows() != pending.rows
            || pending.first_row != self.admitted_arguments
        {
            return Err(Report::new(WindowArchiveError::GroupRows));
        }
        let metadata = self.row_metadata(pending.first_row, pending.rows)?;
        let schema = arguments.batch.schema();
        let batch = Arc::new(arguments.batch);
        let mut records = Vec::with_capacity(pending.rows);
        for (row, metadata) in metadata.into_iter().enumerate() {
            let record = RuntimeRow::new(Arc::clone(&batch), row, metadata)
                .change_context(WindowArchiveError::GroupRows)?;
            records.push(MaterializedGenerationRecord {
                branch: None,
                row: record,
            });
        }
        let generation = self.pieces.nested_generation(schema, records);
        self.pieces
            .append_encoded_arguments(&self.executor, &generation, arguments.bytes)
            .await
            .change_context(WindowArchiveError::Checkpoint)?;
        self.admitted_arguments = pending
            .first_row
            .checked_add(pending.rows)
            .assured("an admitted row count is shorter than the address space");
        Ok(())
    }

    /// The native checkpoint the window's branch task restores and re-admits, in one quota-owned
    /// artifact.
    pub(crate) async fn finish(self) -> error_stack::Result<StagedArtifact, WindowArchiveError> {
        if self.admitted_arguments != self.admitted_input {
            return Err(Report::new(WindowArchiveError::GroupRows));
        }
        if self.admitted_input != self.window.rows.len() {
            return Err(Report::new(WindowArchiveError::RowCount {
                expected: self.window.rows.len(),
                found: self.admitted_input,
            }));
        }
        let mut accumulators = Vec::with_capacity(self.window.accumulators.len());
        for accumulator in self.window.accumulators {
            let snapshot = match accumulator {
                WindowAccumulatorState::Retained => WindowAccumulatorSnapshot::Retained,
                WindowAccumulatorState::LinearHistogram { delayed_removals } => {
                    let mut removals = Vec::with_capacity(delayed_removals.len());
                    for removal in delayed_removals {
                        let bucket = usize::try_from(removal.bucket)
                            .map_err(|_| Report::new(WindowArchiveError::Bucket))?;
                        removals.push(LinearHistogramDelayedRemovalSnapshot {
                            expires_at: removal.expires_at,
                            bucket,
                        });
                    }
                    WindowAccumulatorSnapshot::LinearHistogram {
                        delayed_removals: removals,
                    }
                }
            };
            accumulators.push(snapshot);
        }
        self.pieces
            .finish(&self.executor, self.window.next_sequence, &accumulators)
            .await
            .change_context(WindowArchiveError::Checkpoint)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use meticulous::ResultExt as _;
    use nervix_arbitrary::Entropy;
    use nervix_execution::ExecutionConfig;
    use nervix_models::ParseAsType;
    use nonzero_ext::nonzero;
    use ordered_float::OrderedFloat;

    use super::*;
    use crate::{
        runtime::{
            OptionalTestField, TestWindow, WindowArgumentColumns, WindowProcessorState,
            batch_value, snapshot_staging::SnapshotStagingLimits,
            window_state::WindowPublishedSnapshot,
        },
        runtime_schema::RuntimeValue,
    };

    /// Every exact aggregate family, a histogram percentile with a delay, and nullable string and
    /// boolean arguments, over small integers whose sums and means are exact in `F64`.
    const AGGREGATES: &str = "SET samples = COUNT(input.latency), total = SUM(input.latency), \
                              first_label = FIRST(input.label), last_label = LAST(input.label), \
                              smallest = MIN(input.latency), largest = MAX(input.latency), \
                              mean_latency = AVG(input.latency), healthy = \
                              BOOL_AND(input.healthy), p50 = \
                              PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s')";

    /// The outputs whose aggregates are exact, so a restored window answers them as the live
    /// window did.
    const EXACT_OUTPUTS: [&str; 8] = [
        "samples",
        "total",
        "first_label",
        "last_label",
        "smallest",
        "largest",
        "healthy",
        "p50",
    ];

    /// The mean merges each admission run's result in floating point, so its last digits depend
    /// on how rows were grouped into runs, which archive groups change.
    const MERGED_OUTPUT: &str = "mean_latency";

    fn float_output(batch: &RuntimeRecordBatch, field: &str) -> Option<f64> {
        let value = batch_value(batch, field)?;
        let RuntimeValue::F64(OrderedFloat(value)) = value else {
            panic!("{field} is a floating-point output, not {value:?}");
        };
        Some(value)
    }

    /// Asserts the merged mean of `restored` stays within rounding of `expected`'s.
    fn assert_mean_close(restored: &RuntimeRecordBatch, expected: &RuntimeRecordBatch) {
        let restored = float_output(restored, MERGED_OUTPUT);
        let expected = float_output(expected, MERGED_OUTPUT);
        match (restored, expected) {
            (Some(restored), Some(expected)) => assert!(
                (restored - expected).abs() <= expected.abs() * 1e-12,
                "the restored mean {restored} drifted from {expected}"
            ),
            (None, None) => {}
            (restored, expected) => panic!(
                "the restored mean {restored:?} and {expected:?} disagree on whether the window \
                 holds a latency"
            ),
        }
    }

    fn window() -> TestWindow {
        let input = [
            OptionalTestField {
                name: "latency",
                ty: ParseAsType::I64,
                optional: true,
            },
            OptionalTestField {
                name: "label",
                ty: ParseAsType::String,
                optional: true,
            },
            OptionalTestField {
                name: "healthy",
                ty: ParseAsType::Bool,
                optional: true,
            },
        ];
        let output = [
            OptionalTestField {
                name: "samples",
                ty: ParseAsType::I64,
                optional: false,
            },
            OptionalTestField {
                name: "total",
                ty: ParseAsType::I64,
                optional: true,
            },
            OptionalTestField {
                name: "first_label",
                ty: ParseAsType::String,
                optional: true,
            },
            OptionalTestField {
                name: "last_label",
                ty: ParseAsType::String,
                optional: true,
            },
            OptionalTestField {
                name: "smallest",
                ty: ParseAsType::I64,
                optional: true,
            },
            OptionalTestField {
                name: "largest",
                ty: ParseAsType::I64,
                optional: true,
            },
            OptionalTestField {
                name: "mean_latency",
                ty: ParseAsType::F64,
                optional: true,
            },
            OptionalTestField {
                name: "healthy",
                ty: ParseAsType::Bool,
                optional: true,
            },
            OptionalTestField {
                name: "p50",
                ty: ParseAsType::F64,
                optional: true,
            },
        ];
        TestWindow::new(AGGREGATES, &input, &output)
    }

    /// Admits generated batches and steps over some of their rows, as a branch task would.
    async fn fill(window: &mut TestWindow, entropy: &mut Entropy<'_>) {
        let mut clock = 0_i64;
        for _ in 0..entropy.count(4) {
            let rows = entropy.positive_count(nonzero!(4_usize));
            let mut latencies = Vec::with_capacity(rows);
            let mut labels = Vec::with_capacity(rows);
            let mut healthy = Vec::with_capacity(rows);
            let mut timestamps = Vec::with_capacity(rows);
            for _ in 0..rows {
                let latency = i64::try_from(entropy.between(0..=100))
                    .verified("the latency was drawn below 101");
                let latency = if entropy.flag() { None } else { Some(latency) };
                let label = entropy.pick(["alpha", "beta", "", "é"]);
                let label = if entropy.flag() { None } else { Some(label) };
                let health = entropy.flag();
                let health = if entropy.flag() { None } else { Some(health) };
                latencies.push(latency);
                labels.push(label);
                healthy.push(health);
                let advance = i64::try_from(entropy.between(0..=3))
                    .verified("the advance was drawn below four");
                clock = clock
                    .checked_add(advance)
                    .verified("a few small advances stay far below i64::MAX");
                timestamps.push(clock);
            }
            let columns: Vec<ArrayRef> = vec![
                StdArc::new(Int64Array::from(latencies)),
                StdArc::new(StringArray::from(labels)),
                StdArc::new(BooleanArray::from(healthy)),
            ];
            let refused = window.admit(columns, &timestamps).await;
            assert!(
                refused.is_empty(),
                "generated rows fit the window: {refused:?}"
            );
            if entropy.flag() {
                let retained = window.state.entries.len();
                window.step(entropy.count(retained), clock);
            }
        }
    }

    /// The window `published` archives as groups bounded by `limit`, then rebuilt from them as
    /// the branch task restores it.
    async fn archived_and_restored(
        executor: &Executor,
        window: &TestWindow,
        published: WindowProcessorStateSnapshot,
        revision: u64,
        limit: u64,
    ) -> WindowProcessorStateSnapshot {
        let input = window.input_schema.arrow_schema();
        let arguments = WindowArgumentColumns::snapshot_schema(&window.plan);
        let generation = Generation {
            revision,
            value: Some(WindowPublishedSnapshot::Live(published.clone())),
        };
        let captured =
            CapturedWindow::published(&generation).verified("the generation holds a window");
        let opened = captured
            .open(executor, &input, &arguments)
            .await
            .assured("a published window opens against its own schemas")
            .verified("a window the branch task published has a branch lifetime");
        assert_eq!(opened.revision(), revision);
        assert_eq!(Some(opened.incarnation()), published.incarnation);
        assert_eq!(opened.next_sequence(), published.next_sequence);

        let groups = opened.groups(limit);
        let mut next = 0;
        for group in &groups {
            assert_eq!(group.start, next, "groups cover the rows in order");
            assert!(group.end > group.start, "no group is empty");
            next = group.end;
        }
        assert_eq!(next, published.entries.len(), "groups cover every row");

        let directory = tempfile::tempdir().assured("the staging directory opens");
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let mut builder = WindowCheckpointBuilder::new(
            executor,
            &staging,
            ArchivedWindow {
                revision: opened.revision(),
                incarnation: opened.incarnation(),
                branch: None,
                first_sequence: opened.first_sequence(),
                next_sequence: opened.next_sequence(),
                rows: opened.row_watermarks(),
                accumulators: opened
                    .accumulators()
                    .assured("a bounded window's buckets fit 64 bits"),
            },
        );
        for group in &groups {
            let input_rows = archived_rows(
                executor,
                &input,
                opened
                    .encode_input_group(executor, group)
                    .await
                    .assured("a bounded input group encodes"),
            )
            .await;
            let argument_rows = archived_rows(
                executor,
                &arguments,
                opened
                    .encode_argument_group(executor, group)
                    .await
                    .assured("a bounded argument group encodes"),
            )
            .await;
            let pending = builder
                .admit_input(input_rows)
                .await
                .assured("a group of the archived input rows is admitted");
            builder
                .admit_arguments(pending, argument_rows)
                .await
                .assured("the group's row-aligned argument columns are admitted");
        }
        let artifact = builder
            .finish()
            .await
            .assured("the rebuilt window seals as a native checkpoint");
        let checkpoint = std::fs::read(artifact.path()).assured("the sealed checkpoint reads");
        assert_eq!(
            artifact.digest(),
            *blake3::hash(&checkpoint).as_bytes(),
            "the artifact names the digest of its bytes"
        );
        decode_window_processor_snapshot(
            &checkpoint,
            executor,
            WindowSnapshotSchemas {
                input: &input,
                arguments: &arguments,
            },
            WindowSnapshotLifetime::Branch(window.state.incarnation),
        )
        .await
        .assured("the rebuilt checkpoint opens for its own branch lifetime")
        .verified("the rebuilt checkpoint names the branch lifetime it was captured in")
    }

    /// One archived Arrow section of `schema` and the batch it decodes into.
    async fn archived_rows(
        executor: &Executor,
        schema: &StdArc<ArrowSchema>,
        bytes: ChargedBytes,
    ) -> ArchivedRows {
        let batch = RuntimeRecordBatch::decode_arrow_snapshot_section(
            executor,
            StdArc::clone(schema),
            bytes.clone(),
        )
        .await
        .assured("archived rows decode under their exact schema");
        ArchivedRows { bytes, batch }
    }

    /// An executor whose identity records hold a few rows, so archived groups split into groups
    /// of their own and their columns are projected again.
    fn narrow_identity_executor() -> Executor {
        Executor::new(ExecutionConfig {
            limits: nervix_execution::OperationLimits {
                snapshot_record_bytes: ubyte::ByteUnit::Byte(512),
                ..nervix_execution::OperationLimits::default()
            },
            ..ExecutionConfig::default()
        })
        .assured("a narrow identity record is a valid limit")
    }

    /// Asserts `restored` holds exactly the rows, sequences, watermarks, argument values and
    /// delayed removals of `published`.
    fn assert_same_window(
        restored: &WindowProcessorStateSnapshot,
        published: &WindowProcessorStateSnapshot,
    ) {
        assert_eq!(restored.next_sequence, published.next_sequence);
        assert_eq!(restored.incarnation, published.incarnation);
        assert_eq!(restored.entries.len(), published.entries.len());
        for (restored, published) in restored.entries.iter().zip(&published.entries) {
            assert_eq!(restored.sequence, published.sequence);
            assert_eq!(restored.timestamp, published.timestamp);
            assert_eq!(restored.key, published.key);
            assert_eq!(
                restored.record.one_row_batch().batch(),
                published.record.one_row_batch().batch(),
                "the retained input row survives with its exact schema and values"
            );
            assert_eq!(
                restored.record.metadata().to_remote(),
                published.record.metadata().to_remote()
            );
            assert_eq!(
                restored.arguments.one_row_batch().batch(),
                published.arguments.one_row_batch().batch(),
                "the row's aggregate arguments survive with their exact values"
            );
        }
        assert_eq!(restored.accumulators.len(), published.accumulators.len());
        for (restored, published) in restored.accumulators.iter().zip(&published.accumulators) {
            match (restored, published) {
                (WindowAccumulatorSnapshot::Retained, WindowAccumulatorSnapshot::Retained) => {}
                (
                    WindowAccumulatorSnapshot::LinearHistogram {
                        delayed_removals: restored,
                    },
                    WindowAccumulatorSnapshot::LinearHistogram {
                        delayed_removals: published,
                    },
                ) => assert_eq!(restored, published),
                (restored, published) => {
                    panic!("accumulator {restored:?} restored where {published:?} was published")
                }
            }
        }
    }

    #[test]
    fn bolero_window_archive_groups_rebuild_the_published_window() {
        bolero::check!()
            .with_iterations(64)
            .with_max_len(1024)
            .for_each(|input| {
                let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .assured("property runtime opens");
                let mut entropy = Entropy::new(input);
                // Archived groups keep their sections when their identities fit one identity
                // record, and split into groups of their own otherwise.
                let executor = if entropy.flag() {
                    narrow_identity_executor()
                } else {
                    Executor::new(ExecutionConfig::default()).assured("default bounds are valid")
                };
                runtime.block_on(async {
                    let mut window = window();
                    fill(&mut window, &mut entropy).await;
                    let before = window.emitted().await;
                    let published = window
                        .state
                        .to_snapshot()
                        .assured("a bounded window publishes");
                    let revision = entropy.any_u64();
                    let limit = entropy.between(1..=512);
                    let restored = archived_and_restored(
                        &executor,
                        &window,
                        published.clone(),
                        revision,
                        limit,
                    )
                    .await;
                    assert_same_window(&restored, &published);

                    let native = WindowProcessorState::from_snapshot(
                        &window.plan,
                        window.input_schema.as_ref(),
                        &published,
                    )
                    .assured("the published snapshot re-admits under its own plan");
                    let archived = WindowProcessorState::from_snapshot(
                        &window.plan,
                        window.input_schema.as_ref(),
                        &restored,
                    )
                    .assured("the restored snapshot re-admits under its own plan");
                    assert_eq!(
                        archived.next_timeout_deadline(),
                        native.next_timeout_deadline(),
                        "pending delayed removals keep their expiry"
                    );
                    window.state = native;
                    let from_native = window.emitted().await;
                    window.state = archived;
                    let from_archive = window.emitted().await;
                    for field in EXACT_OUTPUTS {
                        assert_eq!(
                            batch_value(&from_archive, field),
                            batch_value(&from_native, field),
                            "{field} answers as the native checkpoint of the same window does"
                        );
                        assert_eq!(
                            batch_value(&from_archive, field),
                            batch_value(&before, field),
                            "{field} answers as the live window did"
                        );
                    }
                    assert_mean_close(&from_archive, &from_native);
                    assert_mean_close(&from_archive, &before);
                });
            });
    }

    #[nervix_primitives::test]
    async fn archived_sketch_windows_answer_as_before_the_backup() {
        let mut window = TestWindow::new(
            "SET distinct_values = APPROX_COUNT_DISTINCT(input.value, 10), median_value = \
             APPROX_QUANTILE(input.value, 50, 128), frequent_values = APPROX_TOP_K(input.value, \
             2, 16)",
            &[OptionalTestField {
                name: "value",
                ty: ParseAsType::I64,
                optional: false,
            }],
            &[
                OptionalTestField {
                    name: "distinct_values",
                    ty: ParseAsType::I64,
                    optional: false,
                },
                OptionalTestField {
                    name: "median_value",
                    ty: ParseAsType::F64,
                    optional: true,
                },
                OptionalTestField {
                    name: "frequent_values",
                    ty: ParseAsType::Vec {
                        element: Box::new(ParseAsType::I64),
                    },
                    optional: false,
                },
            ],
        );
        let values: ArrayRef = StdArc::new(Int64Array::from(vec![5, 7, 5, 9, 5, 11]));
        let refused = window
            .admit(
                vec![values],
                &[
                    0,
                    400_000_000,
                    800_000_000,
                    1_200_000_000,
                    1_500_000_000,
                    1_900_000_000,
                ],
            )
            .await;
        assert!(
            refused.is_empty(),
            "every row fits the sketch window: {refused:?}"
        );
        let before = window.emitted().await;
        let executor = Executor::default();
        let published = window
            .state
            .to_snapshot()
            .assured("the sketch window publishes");
        let restored = archived_and_restored(&executor, &window, published.clone(), 3, 64).await;
        assert_same_window(&restored, &published);
        window.state = WindowProcessorState::from_snapshot(
            &window.plan,
            window.input_schema.as_ref(),
            &restored,
        )
        .assured("the restored sketch window re-admits its rows");
        let after = window.emitted().await;
        for field in ["distinct_values", "median_value", "frequent_values"] {
            assert_eq!(
                batch_value(&after, field),
                batch_value(&before, field),
                "{field} is rebuilt from the same rows in the same order"
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_window_without_a_branch_lifetime_archives_nothing() {
        let window = window();
        let executor = Executor::default();
        let input = window.input_schema.arrow_schema();
        let arguments = WindowArgumentColumns::snapshot_schema(&window.plan);
        let mut never_admitted = window
            .state
            .to_snapshot()
            .assured("an empty window publishes");
        never_admitted.incarnation = None;
        let generation = Generation {
            revision: 4,
            value: Some(WindowPublishedSnapshot::Live(never_admitted)),
        };
        let opened = CapturedWindow::published(&generation)
            .verified("the generation holds a window")
            .open(&executor, &input, &arguments)
            .await
            .assured("an empty window opens");
        assert!(
            opened.is_none(),
            "an empty forced-recovery window restores empty"
        );
        let unpublished = Generation {
            revision: 0,
            value: None,
        };
        assert!(CapturedWindow::published(&unpublished).is_none());
    }

    #[nervix_primitives::test]
    async fn rebuilding_a_window_refuses_groups_that_disagree_with_its_descriptor() {
        let mut window = window();
        let latencies: ArrayRef = StdArc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let labels: ArrayRef = StdArc::new(StringArray::from(vec![Some("a"), None]));
        let healthy: ArrayRef = StdArc::new(BooleanArray::from(vec![Some(true), Some(false)]));
        let refused = window
            .admit(vec![latencies, labels, healthy], &[1, 2])
            .await;
        assert!(refused.is_empty(), "both rows fit the window: {refused:?}");
        let executor = Executor::default();
        let published = window.state.to_snapshot().assured("the window publishes");
        let input = window.input_schema.arrow_schema();
        let arguments = WindowArgumentColumns::snapshot_schema(&window.plan);
        let generation = Generation {
            revision: 2,
            value: Some(WindowPublishedSnapshot::Live(published)),
        };
        let opened = CapturedWindow::published(&generation)
            .verified("the generation holds a window")
            .open(&executor, &input, &arguments)
            .await
            .assured("the window opens")
            .verified("the window has a branch lifetime");
        let archived = || ArchivedWindow {
            revision: opened.revision(),
            incarnation: opened.incarnation(),
            branch: None,
            first_sequence: opened.first_sequence(),
            next_sequence: opened.next_sequence(),
            rows: opened.row_watermarks(),
            accumulators: opened.accumulators().assured("bounded buckets fit"),
        };
        let directory = tempfile::tempdir().assured("the staging directory opens");
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let builder = |window| WindowCheckpointBuilder::new(&executor, &staging, window);
        let all_rows = 0..2;
        let input_rows = archived_rows(
            &executor,
            &input,
            opened
                .encode_input_group(&executor, &all_rows)
                .await
                .assured("the input rows encode"),
        )
        .await;
        let one_row = 0..1;
        let one_argument_row = archived_rows(
            &executor,
            &arguments,
            opened
                .encode_argument_group(&executor, &one_row)
                .await
                .assured("the argument columns encode"),
        )
        .await;
        let same_rows = |rows: &ArchivedRows| ArchivedRows {
            bytes: rows.bytes.clone(),
            batch: rows.batch.clone(),
        };

        let mut misaligned = builder(archived());
        let pending = misaligned
            .admit_input(same_rows(&input_rows))
            .await
            .assured("the group's input rows are admitted");
        let error = misaligned
            .admit_arguments(pending, one_argument_row)
            .await
            .expect_err("a group's input rows and arguments are row-aligned");
        assert_eq!(error.current_context(), &WindowArchiveError::GroupRows);

        let mut incomplete = builder(archived());
        let _pending = incomplete
            .admit_input(same_rows(&input_rows))
            .await
            .assured("the group's input rows are admitted");
        let error = incomplete
            .finish()
            .await
            .expect_err("a group's argument columns complete it");
        assert_eq!(error.current_context(), &WindowArchiveError::GroupRows);

        let mut short = archived();
        short.rows.truncate(1);
        let error = builder(short)
            .admit_input(same_rows(&input_rows))
            .await
            .expect_err("groups hold no more rows than the descriptor lists");
        assert_eq!(
            error.current_context(),
            &WindowArchiveError::RowCount {
                expected: 1,
                found: 2
            }
        );

        let error = builder(archived())
            .finish()
            .await
            .expect_err("every row the descriptor lists arrives in some group");
        assert_eq!(
            error.current_context(),
            &WindowArchiveError::RowCount {
                expected: 2,
                found: 0
            }
        );

        let mut overflowing = archived();
        overflowing.first_sequence = Some(u64::MAX);
        let error = builder(overflowing)
            .admit_input(input_rows)
            .await
            .expect_err("row sequences never pass u64::MAX");
        assert_eq!(error.current_context(), &WindowArchiveError::Sequence);
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
    }
}
