//! A window's retained argument column collected in FIRST-to-LAST order.
//!
//! Layer: data plane.
//! - **Owns.** Charged immutable row selection, Arrow column gathering and list offset bounds.
//! - **Depends on.** Retained window rows, their shared arrival order, Arrow and the executor.
//! - **Must not know.** Models, routing, acknowledgements, snapshots or publication policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns retained window collection")
)]

use std::{
    collections::BTreeMap,
    mem::{size_of, size_of_val},
    ops::Range,
};

use arrow_array::{Array, ArrayRef, ListArray, cast::AsArray as _, make_array};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_data::{
    ArrayData,
    transform::{Capacities, MutableArrayData},
};
use arrow_schema::{DataType, FieldRef};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass, QueueAdmission};
use nervix_primitives::{sync::StdArc, task::consume_budget};

use super::{RetainedWindowRows, WindowArrival, retained_runs};
use crate::runtime::WindowProcessorError;

/// Immutable addresses into retained Arrow columns; no payload value is materialized per row.
struct SelectedRow {
    arrival: WindowArrival,
    column: usize,
    row: usize,
}

/// One emission's selected rows and the columns keeping their values alive while its worker runs.
pub(in crate::runtime) struct CollectedSequence {
    columns: Vec<ArrayRef>,
    rows: Vec<SelectedRow>,
    element: FieldRef,
    length: i32,
    capacities: Capacities,
}

impl CollectedSequence {
    /// Capture cooperatively, then gather on the bounded data workers under a relay-memory charge.
    pub(in crate::runtime) async fn evaluate<R: RetainedWindowRows>(
        executor: &Executor,
        demand: usize,
        output_type: &DataType,
        rows: &R,
    ) -> error_stack::Result<ArrayRef, WindowProcessorError> {
        let count = rows.retained();
        if count == 0 {
            return Err(Report::new(WindowProcessorError::EmptyWindow));
        }
        let length = i32::try_from(count)
            .change_context(WindowProcessorError::CollectionRows { rows: count })?;
        let DataType::List(element) = output_type else {
            return Err(Report::new(WindowProcessorError::CollectionOutputType {
                data_type: output_type.clone(),
            }));
        };
        let first = rows
            .retained_row(0)
            .arguments
            .demand(demand)
            .first()
            .array();
        let header_bytes = SelectionShape::header_bytes(first.as_ref());
        let per_row = size_of::<SelectedRow>()
            .checked_add(size_of::<ArrayRef>())
            .assured("the metadata of an addressable Arrow type fits usize");
        let per_row = per_row
            .checked_add(header_bytes)
            .assured("the metadata of an addressable Arrow type fits usize");
        let metadata = u64::try_from(count)
            .assured("the collection row count fits i32")
            .checked_mul(u64::try_from(per_row).assured("metadata sizes fit u64"))
            .ok_or_else(|| Report::new(WindowProcessorError::CollectionSizeOverflow))?;
        let mut shape = SelectionShape::default();
        for run in retained_runs(rows, 0..count) {
            consume_budget().await;
            let array = run.arguments.demand(demand).first().array();
            shape.include(array.as_ref(), run.rows, 0)?;
        }
        let working = shape
            .bytes
            .checked_add(metadata)
            .ok_or_else(|| Report::new(WindowProcessorError::CollectionSizeOverflow))?;
        let offsets = u64::try_from(size_of::<[i32; 2]>()).assured("two list offsets fit u64");
        let working = working
            .checked_add(offsets)
            .ok_or_else(|| Report::new(WindowProcessorError::CollectionSizeOverflow))?;
        // Reserve the complete operation together: queued gathers never hold a partial grant
        // while waiting for the rest of their own working set.
        let charge = executor
            .reserve(MemoryClass::Relay, working)
            .await
            .change_context(WindowProcessorError::CollectionAdmission)?;
        let mut selected = Self {
            columns: Vec::new(),
            rows: Vec::with_capacity(count),
            element: StdArc::clone(element),
            length,
            capacities: shape.capacities(first.data_type(), 0),
        };
        for run in retained_runs(rows, 0..count) {
            consume_budget().await;
            let array = run.arguments.demand(demand).first().array();
            let column = selected.columns.len();
            selected.columns.push(StdArc::clone(array));
            for positioned in run.positioned_rows() {
                consume_budget().await;
                selected.rows.push(SelectedRow {
                    arrival: rows.retained_row(positioned.position).arrival(),
                    column,
                    row: positioned.row,
                });
            }
        }
        executor
            .run_cpu_with(
                CpuClass::Data,
                QueueAdmission::WaitForPlace,
                charge,
                move |_charge, cancellation| selected.gather(cancellation),
            )
            .await
            .change_context(WindowProcessorError::CollectionExecution)?
    }

    fn gather(
        mut self,
        cancellation: &Cancellation,
    ) -> error_stack::Result<ArrayRef, WindowProcessorError> {
        cancellation
            .check()
            .change_context(WindowProcessorError::CollectionExecution)?;
        self.rows.sort_unstable_by_key(|row| row.arrival);
        cancellation
            .check()
            .change_context(WindowProcessorError::CollectionExecution)?;
        let data = self
            .columns
            .iter()
            .map(|array| array.to_data())
            .collect::<Vec<_>>();
        let sources = data.iter().collect::<Vec<_>>();
        let mut gathered = MutableArrayData::with_capacities(sources, false, self.capacities);
        let mut position = 0;
        while position < self.rows.len() {
            cancellation
                .check()
                .change_context(WindowProcessorError::CollectionExecution)?;
            let first = &self.rows[position];
            let mut end = first
                .row
                .checked_add(1)
                .assured("the row indexes a retained array");
            position = position
                .checked_add(1)
                .assured("the position indexes selected rows");
            while let Some(next) = self.rows.get(position)
                && next.column == first.column
                && next.row == end
            {
                end = end
                    .checked_add(1)
                    .assured("the run indexes a retained array");
                position = position
                    .checked_add(1)
                    .assured("the position indexes selected rows");
            }
            gathered.extend(first.column, first.row, end);
        }
        cancellation
            .check()
            .change_context(WindowProcessorError::CollectionExecution)?;
        let values = make_array(gathered.freeze());
        let list = ListArray::try_new(
            self.element,
            OffsetBuffer::new(vec![0, self.length].into()),
            values,
            None,
        )
        .change_context(WindowProcessorError::BuildCollection)?;
        Ok(StdArc::new(list))
    }
}

/// Selected payload bytes and cumulative child counts at each list depth, before Arrow allocates.
#[derive(Default)]
struct SelectionShape {
    bytes: u64,
    offset_values: BTreeMap<usize, u64>,
    rows: BTreeMap<usize, usize>,
}

impl SelectionShape {
    /// Conservatively cover source data, destination transforms and sliced-array metadata for
    /// every type level, even when sorting leaves one gather run per row.
    fn header_bytes(mut array: &dyn Array) -> usize {
        let mut bytes = 0_usize;
        let mut depth = 1_usize;
        loop {
            let node = size_of_val(array)
                .checked_add(size_of::<ArrayData>())
                .assured("Arrow view and transform metadata fit usize");
            let node = node
                .checked_add(size_of::<MutableArrayData<'_>>())
                .assured("Arrow view and transform metadata fit usize");
            let node = node
                .checked_add(size_of::<[Buffer; 2]>())
                .assured("Arrow view and transform metadata fit usize");
            // Arrow clones each remaining child-capacity subtree while descending, so their
            // construction stack holds a triangular number of capacity nodes at peak depth.
            let capacities = depth
                .checked_mul(size_of::<Capacities>())
                .assured("capacity metadata for an addressable Arrow type fits usize");
            let node = node
                .checked_add(capacities)
                .assured("Arrow view, transform and capacity metadata fit usize");
            depth = depth
                .checked_add(1)
                .assured("an addressable Arrow type depth fits usize");
            bytes = bytes
                .checked_add(node)
                .assured("an addressable Arrow type's metadata fits usize");
            array = match array.data_type() {
                DataType::List(_) => array.as_list::<i32>().values().as_ref(),
                DataType::FixedSizeList(_, _) => array.as_fixed_size_list().values().as_ref(),
                _ => return bytes,
            };
        }
    }

    fn include(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        depth: usize,
    ) -> error_stack::Result<(), WindowProcessorError> {
        let count = rows.len();
        let total = self.rows.entry(depth).or_default();
        *total = total
            .checked_add(count)
            .ok_or_else(|| Report::new(WindowProcessorError::CollectionSizeOverflow))?;
        let next_depth = depth
            .checked_add(1)
            .assured("a retained Arrow type's depth fits usize");
        match array.data_type() {
            DataType::List(_) => {
                let list = array.as_list::<i32>();
                let offsets = list.value_offsets();
                let start = usize::try_from(offsets[rows.start])
                    .assured("validated list offsets are nonnegative");
                let end = usize::try_from(offsets[rows.end])
                    .assured("validated list offsets are nonnegative");
                let children =
                    u64::try_from(end.checked_sub(start).assured("list offsets are ordered"))
                        .assured("an addressable child array length fits u64");
                self.include_offsets(depth, children)?;
                self.add_bytes(
                    u64::try_from(count)
                        .assured("a collection count fits u64")
                        .checked_mul(
                            u64::try_from(size_of::<i32>()).assured("an offset size fits u64"),
                        )
                        .assured("an addressable offset buffer size fits u64"),
                )?;
                self.include_nulls(array, count)?;
                self.include(list.values().as_ref(), start..end, next_depth)
            }
            DataType::FixedSizeList(_, _) => {
                let list = array.as_fixed_size_list();
                let width = usize::try_from(list.value_length())
                    .assured("validated fixed-list widths are positive");
                let start = rows
                    .start
                    .checked_mul(width)
                    .assured("the selected rows fit the fixed-list child array");
                let end = rows
                    .end
                    .checked_mul(width)
                    .assured("the selected rows fit the fixed-list child array");
                self.include_nulls(array, count)?;
                self.include(list.values().as_ref(), start..end, next_depth)
            }
            _ => {
                if array.data_type() == &DataType::Utf8 {
                    let offsets = array.as_string::<i32>().value_offsets();
                    let bytes = offsets[rows.end]
                        .checked_sub(offsets[rows.start])
                        .assured("validated string offsets are ordered");
                    self.include_offsets(
                        depth,
                        u64::try_from(bytes).assured("validated string offsets are nonnegative"),
                    )?;
                }
                let bytes = array
                    .slice(rows.start, count)
                    .to_data()
                    .get_slice_memory_size()
                    .change_context(WindowProcessorError::BuildCollection)?;
                self.add_bytes(u64::try_from(bytes).assured("an addressable Arrow slice fits u64"))
            }
        }
    }

    /// Explicit child capacities prevent Arrow's parent-row guess from allocating values that
    /// an empty nested vector does not contain, especially for a wide fixed-size element.
    fn capacities(&self, data_type: &DataType, depth: usize) -> Capacities {
        let rows = match self.rows.get(&depth) {
            Some(rows) => *rows,
            None => 0,
        };
        match data_type {
            DataType::Utf8 => {
                let bytes = match self.offset_values.get(&depth) {
                    Some(bytes) => *bytes,
                    None => 0,
                };
                Capacities::Binary(
                    rows,
                    Some(usize::try_from(bytes).assured("validated string offsets fit usize")),
                )
            }
            DataType::List(field) | DataType::FixedSizeList(field, _) => {
                let next = depth
                    .checked_add(1)
                    .assured("an addressable Arrow type depth fits usize");
                Capacities::List(
                    rows,
                    Some(Box::new(self.capacities(field.data_type(), next))),
                )
            }
            _ => Capacities::Array(rows),
        }
    }

    fn include_offsets(
        &mut self,
        depth: usize,
        values: u64,
    ) -> error_stack::Result<(), WindowProcessorError> {
        let total = self.offset_values.entry(depth).or_default();
        *total = total
            .checked_add(values)
            .ok_or_else(|| Report::new(WindowProcessorError::CollectionOffsets { depth }))?;
        if *total > u64::try_from(i32::MAX).assured("positive i32 bounds fit u64") {
            return Err(Report::new(WindowProcessorError::CollectionOffsets {
                depth,
            }));
        }
        Ok(())
    }

    fn include_nulls(
        &mut self,
        array: &dyn Array,
        count: usize,
    ) -> error_stack::Result<(), WindowProcessorError> {
        if array.nulls().is_some() {
            let bytes = count.div_ceil(u8::BITS.try_into().assured("eight bits fit usize"));
            self.add_bytes(
                u64::try_from(bytes).assured("an addressable validity bitmap fits u64"),
            )?;
        }
        Ok(())
    }

    fn add_bytes(&mut self, bytes: u64) -> error_stack::Result<(), WindowProcessorError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| Report::new(WindowProcessorError::CollectionSizeOverflow))?;
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks collection offset admission without large allocations"
    )
)]
mod tests {
    //! Layer: harness.
    //! - **Owns.** Cumulative Arrow offset refusal at each independent type depth.
    //! - **Depends on.** The production collection shape and its typed error.
    //! - **Must not know.** Runtime scheduling or window publication.

    use meticulous::ResultExt as _;

    use super::SelectionShape;
    use crate::runtime::WindowProcessorError;

    #[test]
    fn collection_checks_cumulative_offsets_at_each_depth_before_allocating() {
        let mut shape = SelectionShape::default();
        let limit = u64::try_from(i32::MAX).assured("positive i32 bounds fit u64");
        shape
            .include_offsets(0, limit)
            .assured("the last representable offset is valid");
        shape
            .include_offsets(1, 1)
            .assured("nested offsets have an independent bound");
        let error = shape
            .include_offsets(0, 1)
            .expect_err("one more value exceeds Arrow's offset");
        assert!(matches!(
            error.current_context(),
            WindowProcessorError::CollectionOffsets { depth: 0 }
        ));
        let error = shape
            .include_offsets(1, u64::MAX)
            .expect_err("offset accumulation itself cannot wrap");
        assert!(matches!(
            error.current_context(),
            WindowProcessorError::CollectionOffsets { depth: 1 }
        ));
    }
}
