//! Layer: data plane.
//! Owns: memory plans for processor inspection's row selection, Arrow columns and resident runs.
//! Depends on: row views, Arrow storage and the page's executor allowance.
//! Must not know: processor execution, public statement rendering or how a reply is transported.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "charges processor state page allocations")
)]

use std::{mem::size_of, ops::Range};

use arrow_array::{Array, ArrayRef, cast::AsArray as _};
use arrow_schema::DataType;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::Reservation;

use super::{ProcessorStatePageError, ProcessorStateSectionColumns, ReorderKeyPart, RuntimeRow};

type ReadResult<T> = error_stack::Result<T, ProcessorStatePageError>;

fn overflow() -> Report<ProcessorStatePageError> {
    Report::new(ProcessorStatePageError::Admission)
}

pub(in crate::runtime) fn add(left: u64, right: u64) -> ReadResult<u64> {
    left.checked_add(right).ok_or_else(overflow)
}

pub(in crate::runtime) fn slots<T>(length: usize) -> ReadResult<u64> {
    let bytes = length.checked_mul(size_of::<T>()).ok_or_else(overflow)?;
    Ok(u64::try_from(bytes).assured("resident allocation sizes fit u64"))
}

pub(in crate::runtime) fn require(memory: &mut Reservation, bytes: u64) -> ReadResult<()> {
    let target = add(memory.bytes(), bytes)?;
    memory
        .grow_to(target)
        .change_context(ProcessorStatePageError::Admission)
}

/// A Vec grows to an explicitly admitted capacity. Growing geometrically keeps pushes amortized,
/// while the charge follows the allocated slots even in a short vector or a partially full page.
pub(in crate::runtime) struct VecGrowth {
    capacity: usize,
    pub(in crate::runtime) bytes: u64,
}

impl VecGrowth {
    pub(in crate::runtime) fn next<T>(values: &Vec<T>, additional: usize) -> ReadResult<Self> {
        let length = values.len().checked_add(additional).ok_or_else(overflow)?;
        let current = values.capacity();
        if length <= current {
            return Ok(Self {
                capacity: current,
                bytes: 0,
            });
        }
        // Preserve Vec's normal initial allocation before doubling the capacity on each growth.
        let initial = match size_of::<T>() {
            1 => 8,
            2..=1024 => 4,
            _ => 1,
        };
        let capacity = length
            .checked_next_power_of_two()
            .ok_or_else(overflow)?
            .max(initial);
        let extra = capacity
            .checked_sub(current)
            .assured("the next capacity is larger");
        Ok(Self {
            capacity,
            bytes: slots::<T>(extra)?,
        })
    }

    pub(in crate::runtime) fn allocate<T>(self, values: &mut Vec<T>) {
        if values.capacity() < self.capacity {
            let extra = self
                .capacity
                .checked_sub(values.len())
                .assured("the Vec length fits its allocation");
            values.reserve_exact(extra);
        }
        assert_eq!(values.capacity(), self.capacity);
    }

    pub(in crate::runtime) fn reserve<T>(
        memory: &mut Reservation,
        values: &mut Vec<T>,
    ) -> ReadResult<()> {
        let growth = Self::next(values, 1)?;
        require(memory, growth.bytes)?;
        growth.allocate(values);
        Ok(())
    }
}

/// Primitive builders and validity bitmaps use Arrow's 64-byte allocation quantum. A bitmap can
/// span multiple rows, so charge the selected bits and set aside the final buffer's padding once.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "Arrow MutableBuffer rounds its allocations to 64 bytes"
    )
)]
const ARROW_BUFFER_QUANTUM: u64 = 64;

pub(in crate::runtime) fn aligned(bytes: u64) -> ReadResult<u64> {
    bytes
        .checked_next_multiple_of(ARROW_BUFFER_QUANTUM)
        .ok_or_else(overflow)
}

pub(in crate::runtime) fn bitmap(length: usize, first: bool) -> ReadResult<u64> {
    let bytes = slots::<u8>(length.div_ceil(8))?;
    add(bytes, if first { ARROW_BUFFER_QUANTUM - 1 } else { 0 })
}

pub(in crate::runtime) fn offset_slots(length: usize, first: bool) -> ReadResult<u64> {
    let count = length
        .checked_add(usize::from(first))
        .ok_or_else(overflow)?;
    slots::<i32>(count)
}

/// The selected values copied by Arrow, plus the row views, source references and indices it uses
/// to copy them. The source views borrow immutable buffers rather than duplicating their values.
pub(in crate::runtime) fn payload_row(row: &RuntimeRow, first: bool) -> ReadResult<u64> {
    let batch = row.batch().batch();
    let start = row.row_index();
    let end = start
        .checked_add(1)
        .assured("a row index is within an addressable batch");
    let mut buffers = 0;
    let mut scratch = 0;
    for column in batch.columns() {
        let gather = ColumnGather::measure(column.as_ref(), start..end, first, None)?;
        buffers = add(buffers, gather.buffers)?;
        scratch = scratch.max(gather.scratch);
    }
    let row_views = slots::<&RuntimeRow>(1)?;
    let row_indices = slots::<(usize, usize)>(1)?;
    add(add(buffers, scratch)?, add(row_views, row_indices)?)
}

struct ColumnGather {
    buffers: u64,
    scratch: u64,
}

#[derive(Clone, Copy)]
struct MutableCapacity {
    /// Number of elements reserved by MutableArrayData before it reads their offsets.
    initial: usize,
    /// Variable list offsets can name more elements than the initial reservation.
    may_grow: bool,
}

impl MutableCapacity {
    fn for_child(self, width: usize) -> ReadResult<Self> {
        let initial = self.initial.checked_mul(width).ok_or_else(overflow)?;
        Ok(Self {
            initial,
            may_grow: self.may_grow,
        })
    }
}

impl ColumnGather {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "reads type and offsets from immutable Arrow arrays")
    )]
    fn measure(
        array: &dyn Array,
        rows: Range<usize>,
        first: bool,
        inherited: Option<MutableCapacity>,
    ) -> ReadResult<Self> {
        let data_type = array.data_type();
        let length = rows.len();
        let mutable = match (inherited, data_type) {
            (Some(capacity), _) => Some(capacity),
            (None, DataType::Boolean | DataType::FixedSizeList(..)) => Some(MutableCapacity {
                initial: length,
                may_grow: false,
            }),
            _ => None,
        };
        let span = match mutable {
            Some(capacity) => length.max(capacity.initial),
            None => length,
        };
        let grows = mutable.is_some_and(|capacity| capacity.may_grow);
        // A source can have nulls outside this selection. MutableArrayData can also allocate the
        // validity bitmap of a child when a parent is nullable.
        let validity = Self::buffer(slots::<u8>(span.div_ceil(8))?, first, grows)?;
        let mut scratch = Self::source_slots(first)?;
        if mutable.is_some() {
            scratch = add(
                scratch,
                Self::mutable_sources(data_type, array.nulls().is_some())?,
            )?;
            if first && inherited.is_some() {
                scratch = add(
                    scratch,
                    slots::<arrow_data::transform::MutableArrayData<'_>>(1)?,
                )?;
            }
        }
        let values = Self::values(array, data_type, rows, span, first, mutable)?;
        Ok(Self {
            buffers: add(validity, values.buffers)?,
            scratch: add(scratch, values.scratch)?,
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "identifies the value layout of immutable Arrow arrays")
    )]
    fn values(
        array: &dyn Array,
        data_type: &DataType,
        rows: Range<usize>,
        span: usize,
        first: bool,
        mutable: Option<MutableCapacity>,
    ) -> ReadResult<Self> {
        let grows = mutable.is_some_and(|capacity| capacity.may_grow);
        let buffers = match data_type {
            DataType::Utf8 => {
                let offsets = array.as_string::<i32>().value_offsets();
                Self::text(offsets, rows, span, first, mutable)?
            }
            DataType::Binary => {
                let offsets = array.as_binary::<i32>().value_offsets();
                Self::text(offsets, rows, span, first, mutable)?
            }
            DataType::List(_) => {
                return Self::list(array.as_list::<i32>(), rows, span, first, mutable);
            }
            DataType::FixedSizeList(_, width) => {
                let capacity = mutable.assured("fixed-size lists use MutableArrayData");
                let child = array.as_fixed_size_list().values();
                return Self::fixed_list(child, *width, rows, first, capacity);
            }
            DataType::Boolean => Self::buffer(slots::<u8>(span.div_ceil(8))?, first, grows)?,
            DataType::Null => 0,
            scalar => {
                let width = scalar
                    .primitive_width()
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
                let raw = span.checked_mul(width).ok_or_else(overflow)?;
                let bytes = slots::<u8>(raw)?;
                match mutable {
                    Some(_) => Self::buffer(bytes, first, grows)?,
                    None => bytes,
                }
            }
        };
        Ok(Self {
            buffers,
            scratch: 0,
        })
    }

    /// A variable list gathers its offsets and the selected elements of its child column.
    fn list(
        list: &arrow_array::ListArray,
        rows: Range<usize>,
        span: usize,
        first: bool,
        mutable: Option<MutableCapacity>,
    ) -> ReadResult<Self> {
        let child = Self::span(list.value_offsets(), rows)?;
        let offsets = offset_slots(span, first)?;
        let (offsets, indices, child_capacity) = match mutable {
            Some(parent) => (
                Self::buffer(offsets, first, parent.may_grow)?,
                0,
                Some(MutableCapacity {
                    initial: parent.initial,
                    may_grow: true,
                }),
            ),
            None => (offsets, slots::<(usize, usize)>(child.len())?, None),
        };
        let nested = Self::measure(list.values().as_ref(), child, first, child_capacity)?;
        Ok(Self {
            buffers: add(nested.buffers, offsets)?,
            scratch: add(indices, nested.scratch)?,
        })
    }

    /// A fixed list addresses its child directly, with no offset or index allocation of its own.
    fn fixed_list(
        child: &ArrayRef,
        width: i32,
        rows: Range<usize>,
        first: bool,
        parent: MutableCapacity,
    ) -> ReadResult<Self> {
        let width = usize::try_from(width).change_context(ProcessorStatePageError::Columns)?;
        let start = rows.start.checked_mul(width).ok_or_else(overflow)?;
        let end = rows.end.checked_mul(width).ok_or_else(overflow)?;
        Self::measure(
            child.as_ref(),
            start..end,
            first,
            Some(parent.for_child(width)?),
        )
    }

    /// MutableBuffer grows to the larger of the aligned length and twice its prior capacity.
    /// Before growth the prior capacity was smaller than the requested length.
    fn buffer(bytes: u64, first: bool, grows: bool) -> ReadResult<u64> {
        let bytes = if grows { add(bytes, bytes)? } else { bytes };
        add(bytes, if first { ARROW_BUFFER_QUANTUM - 1 } else { 0 })
    }

    fn source_slots(first: bool) -> ReadResult<u64> {
        // The caller's source array reference and Arrow's downcast or ArrayData reference.
        let mut bytes = slots::<&dyn Array>(1)?;
        bytes = add(bytes, slots::<&arrow_data::ArrayData>(1)?)?;
        if first {
            bytes = add(bytes, slots::<ArrayRef>(1)?)?;
        }
        Ok(bytes)
    }

    /// MutableArrayData takes one ArrayData view per input, including buffer references and
    /// its value and validity writers. The closures borrow the input's slices or its offsets.
    fn mutable_sources(data_type: &DataType, nullable: bool) -> ReadResult<u64> {
        use arrow_buffer::{Buffer, NullBuffer};
        use arrow_data::ArrayData;

        let (buffers, values) = match data_type {
            DataType::Utf8 | DataType::Binary => (2, slots::<(&[i32], &[u8])>(1)?),
            DataType::List(_) => (1, slots::<&[i32]>(1)?),
            DataType::FixedSizeList(..) => (0, slots::<(usize, usize)>(1)?),
            DataType::Boolean => (1, slots::<(&[u8], &ArrayData)>(1)?),
            DataType::Null => (0, 0),
            _ => (1, slots::<&[u8]>(1)?),
        };
        let nulls = slots::<(&[u8], &NullBuffer)>(usize::from(nullable))?;
        let mut bytes = slots::<ArrayData>(1)?;
        bytes = add(bytes, slots::<Buffer>(buffers)?)?;
        bytes = add(bytes, slots::<Box<dyn Fn()>>(2)?)?;
        add(add(bytes, values)?, nulls)
    }

    fn span(offsets: &[i32], rows: Range<usize>) -> ReadResult<Range<usize>> {
        let start = usize::try_from(offsets[rows.start])
            .change_context(ProcessorStatePageError::Columns)?;
        let end =
            usize::try_from(offsets[rows.end]).change_context(ProcessorStatePageError::Columns)?;
        Ok(start..end)
    }

    fn text(
        offsets: &[i32],
        rows: Range<usize>,
        span: usize,
        first: bool,
        mutable: Option<MutableCapacity>,
    ) -> ReadResult<u64> {
        let bytes = slots::<u8>(Self::span(offsets, rows)?.len())?;
        let offsets = offset_slots(span, first)?;
        match mutable {
            Some(capacity) => {
                // The variable value buffer starts with one byte per slot. Copying the selected
                // values can grow it beyond that initial capacity.
                let initial = slots::<u8>(capacity.initial)?;
                let values = Self::buffer(bytes.max(initial), first, true)?;
                add(values, Self::buffer(offsets, first, capacity.may_grow)?)
            }
            None => add(bytes, offsets),
        }
    }
}

pub(in crate::runtime) fn key_part(
    part: &ReorderKeyPart,
    ty: &DataType,
    first: bool,
) -> ReadResult<u64> {
    let values = match ty {
        DataType::Boolean => bitmap(1, first)?,
        DataType::Utf8 | DataType::Binary => add(part.value_bytes(), offset_slots(1, first)?)?,
        DataType::Null | DataType::List(_) | DataType::FixedSizeList(..) => 0,
        ty => {
            let width = ty
                .primitive_width()
                .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
            add(
                slots::<u8>(width)?,
                if first { ARROW_BUFFER_QUANTUM - 1 } else { 0 },
            )?
        }
    };
    add(values, bitmap(1, first)?)
}

pub(in crate::runtime) fn fixed_columns(bytes: u64, columns: u64, first: bool) -> ReadResult<u64> {
    // Each fixed-width builder may round its buffer capacity up independently.
    if first {
        let padding = columns
            .checked_mul(ARROW_BUFFER_QUANTUM - 1)
            .ok_or_else(overflow)?;
        return add(bytes, padding);
    }
    Ok(bytes)
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(reason = "Arrow arrays report their allocated buffer sizes")
)]
pub(in crate::runtime) fn assert_buffers(sections: &[ProcessorStateSectionColumns], admitted: u64) {
    let mut actual = 0_u64;
    for section in sections {
        for column in section.payload.iter().chain(&section.appended) {
            let bytes = u64::try_from(column.get_buffer_memory_size())
                .assured("a resident buffer size fits u64");
            actual = actual
                .checked_add(bytes)
                .assured("resident buffers are addressable");
        }
    }
    assert!(
        actual <= admitted,
        "Arrow columns allocated {actual} bytes from a {admitted}-byte page charge"
    );
}
