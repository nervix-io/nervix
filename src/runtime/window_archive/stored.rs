//! Bounded archive groups of a stored window, decoded selectively from an admitted native view.
//!
//! Layer: data plane.
//! - **Owns.** Current window metadata, canonical archive group boundaries and one decoded input
//!   or argument group at a time, retaining exact sequences, branches and typed removals.
//! - **Depends on.** The native window layout, indexed column groups and Arrow generation codec.
//! - **Must not know.** Archive paths, configuration Models, placement or runtime activation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns bounded stored window archive conversion")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "backup converts one selected stored window")
)]

use std::ops::Range;

use arrow_schema::Schema as ArrowSchema;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_execution::{ChargedBytes, Executor, MemoryClass, Reservation};
use nervix_models::RemoteRuntimeRecordMetadata;
use nervix_primitives::sync::StdArc;

use super::{WindowAccumulatorState, WindowArchiveError, WindowDelayedRemoval};
use crate::runtime::{
    MaterializedGeneration, MaterializedGenerationRecord, WindowAccumulatorSnapshot,
    materialized_snapshot::column_index::ColumnarCheckpointIndex,
    snapshot_staging::MappedArtifact,
    window_state::layout::{AdmittedWindowLayout, WindowCheckpointLayout},
};

pub(in crate::runtime) struct StoredWindowState {
    revision: u64,
    view: MappedArtifact,
    layout: AdmittedWindowLayout,
    input: ColumnarCheckpointIndex,
    arguments: ColumnarCheckpointIndex,
    input_schema: StdArc<ArrowSchema>,
    argument_schema: StdArc<ArrowSchema>,
    row_sizes: Vec<RowSizes>,
    _size_memory: Reservation,
}

struct RowSizes {
    input: u64,
    arguments: u64,
}

impl StoredWindowState {
    pub(in crate::runtime) async fn open(
        executor: &Executor,
        revision: u64,
        view: MappedArtifact,
        input_schema: &StdArc<ArrowSchema>,
        argument_schema: &StdArc<ArrowSchema>,
    ) -> error_stack::Result<Option<Self>, WindowArchiveError> {
        let Some(layout) = WindowCheckpointLayout::open(executor, &view)
            .await
            .change_context(WindowArchiveError::Open)?
        else {
            return Ok(None);
        };
        let input = ColumnarCheckpointIndex::open(executor, &view, layout.layout.input.clone())
            .await
            .change_context(WindowArchiveError::Open)?;
        let arguments =
            ColumnarCheckpointIndex::open(executor, &view, layout.layout.arguments.clone())
                .await
                .change_context(WindowArchiveError::Open)?;
        let header = &layout.layout.header;
        let incarnation = header
            .incarnation
            .verified("recorded layout has a branch lifetime");
        if input.summary.revision != header.revision
            || arguments.summary.revision != header.revision
            || input.summary.branch_generation != incarnation
            || arguments.summary.branch_generation != incarnation
            || input.summary.records != header.rows
            || arguments.summary.records != header.rows
        {
            return Err(Report::new(WindowArchiveError::GroupRows));
        }
        let rows = input.identities.len();
        if rows == 0 && header.first_sequence.is_some()
            || rows != 0 && header.first_sequence.is_none()
        {
            return Err(Report::new(WindowArchiveError::Sequence));
        }
        if let Some(first) = header.first_sequence {
            let tail = u64::try_from(
                rows.checked_sub(1)
                    .verified("a nonempty window has a last index"),
            )
            .map_err(|_| Report::new(WindowArchiveError::Sequence))?;
            first
                .checked_add(tail)
                .ok_or_else(|| Report::new(WindowArchiveError::Sequence))?;
        }
        for identity in &arguments.identities {
            if identity.branch.is_some() {
                return Err(Report::new(WindowArchiveError::GroupRows));
            }
        }
        let bytes = rows
            .checked_mul(std::mem::size_of::<RowSizes>() + std::mem::size_of::<u64>())
            .ok_or_else(|| Report::new(WindowArchiveError::Open))?;
        let size_memory = executor
            .try_reserve(
                MemoryClass::RestoreMetadata,
                u64::try_from(bytes)
                    .map_err(|_| Report::new(WindowArchiveError::Open))?
                    .max(1),
            )
            .change_context(WindowArchiveError::Open)?;
        let mut input_sizes = Vec::with_capacity(rows);
        let mut row_sizes = Vec::with_capacity(rows);
        for group in input.group_ranges() {
            nervix_primitives::task::consume_budget().await;
            let decoded = input
                .read_rows(executor, &view, input_schema, group)
                .await
                .change_context(WindowArchiveError::Open)?;
            for row in &decoded.rows {
                input_sizes.push(row.one_row_batch().estimated_bytes());
            }
        }
        for group in arguments.group_ranges() {
            nervix_primitives::task::consume_budget().await;
            let first = group.start;
            let decoded = arguments
                .read_rows(executor, &view, argument_schema, group)
                .await
                .change_context(WindowArchiveError::Open)?;
            for (offset, row) in decoded.rows.iter().enumerate() {
                let index = first
                    .checked_add(offset)
                    .verified("a validated native group is addressable");
                row_sizes.push(RowSizes {
                    input: input_sizes[index],
                    arguments: row.one_row_batch().estimated_bytes(),
                });
            }
        }
        Ok(Some(Self {
            revision,
            view,
            layout,
            input,
            arguments,
            input_schema: StdArc::clone(input_schema),
            argument_schema: StdArc::clone(argument_schema),
            row_sizes,
            _size_memory: size_memory,
        }))
    }

    pub(in crate::runtime) fn revision(&self) -> u64 {
        self.revision
    }
    pub(in crate::runtime) fn incarnation(&self) -> u64 {
        self.layout
            .layout
            .header
            .incarnation
            .verified("an opened stored window has a branch lifetime")
    }
    pub(in crate::runtime) fn next_sequence(&self) -> u64 {
        self.layout.layout.header.next_sequence
    }
    pub(in crate::runtime) fn first_sequence(&self) -> Option<u64> {
        self.layout.layout.header.first_sequence
    }
    pub(in crate::runtime) fn row_watermarks(&self) -> Vec<RemoteRuntimeRecordMetadata> {
        self.input
            .identities
            .iter()
            .map(|identity| identity.metadata.to_remote())
            .collect()
    }
    pub(in crate::runtime) fn accumulators(
        &self,
    ) -> error_stack::Result<Vec<WindowAccumulatorState>, WindowArchiveError> {
        let mut output = Vec::with_capacity(self.layout.layout.accumulators.len());
        for accumulator in &self.layout.layout.accumulators {
            let state = match accumulator {
                WindowAccumulatorSnapshot::Retained => WindowAccumulatorState::Retained,
                WindowAccumulatorSnapshot::LinearHistogram { delayed_removals } => {
                    let mut removals = Vec::with_capacity(delayed_removals.len());
                    for removal in delayed_removals {
                        removals.push(WindowDelayedRemoval {
                            expires_at: removal.expires_at,
                            bucket: u64::try_from(removal.bucket)
                                .map_err(|_| Report::new(WindowArchiveError::Bucket))?,
                        });
                    }
                    WindowAccumulatorState::LinearHistogram {
                        delayed_removals: removals,
                    }
                }
            };
            output.push(state);
        }
        Ok(output)
    }
    pub(in crate::runtime) fn groups(&self, limit: u64) -> Vec<Range<usize>> {
        let mut groups = Vec::new();
        let mut start = 0;
        let mut input = 0_u64;
        let mut arguments = 0_u64;
        for (index, row) in self.row_sizes.iter().enumerate() {
            let next_input = input.checked_add(row.input);
            let next_arguments = arguments.checked_add(row.arguments);
            let overruns = match (next_input, next_arguments) {
                (Some(input), Some(arguments)) => input > limit || arguments > limit,
                _ => true,
            };
            if index > start && overruns {
                groups.push(start..index);
                start = index;
                input = row.input;
                arguments = row.arguments;
            } else {
                input = next_input.unwrap_or(limit);
                arguments = next_arguments.unwrap_or(limit);
            }
        }
        if start < self.row_sizes.len() {
            groups.push(start..self.row_sizes.len());
        }
        groups
    }
    pub(in crate::runtime) async fn encode_input_group(
        &self,
        executor: &Executor,
        group: &Range<usize>,
    ) -> error_stack::Result<ChargedBytes, WindowArchiveError> {
        self.encode_group(executor, &self.input, &self.input_schema, group)
            .await
    }
    pub(in crate::runtime) async fn encode_argument_group(
        &self,
        executor: &Executor,
        group: &Range<usize>,
    ) -> error_stack::Result<ChargedBytes, WindowArchiveError> {
        self.encode_group(executor, &self.arguments, &self.argument_schema, group)
            .await
    }
    async fn encode_group(
        &self,
        executor: &Executor,
        index: &ColumnarCheckpointIndex,
        schema: &StdArc<ArrowSchema>,
        group: &Range<usize>,
    ) -> error_stack::Result<ChargedBytes, WindowArchiveError> {
        let decoded = index
            .read_rows(executor, &self.view, schema, group.clone())
            .await
            .change_context(WindowArchiveError::Open)?;
        let mut records = Vec::with_capacity(decoded.rows.len());
        for (offset, row) in decoded.rows.iter().enumerate() {
            let position = group
                .start
                .checked_add(offset)
                .verified("an indexed group stays within its native rows");
            records.push(MaterializedGenerationRecord {
                branch: index.identities[position].branch.clone(),
                row: row.clone(),
            });
        }
        let generation = MaterializedGeneration::new(
            self.revision,
            0,
            self.incarnation(),
            StdArc::clone(schema),
            records,
        );
        let encoded = generation
            .encode_columns(executor, &(0..decoded.rows.len()))
            .await
            .change_context(WindowArchiveError::Encode);
        drop(decoded);
        encoded
    }
}
