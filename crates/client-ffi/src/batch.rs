//! A typed Arrow batch of a client endpoint: `nx_batch`, and copying its columns one level at a
//! time.
//!
//! - **Owns.** The record batch a delivery carried or a builder finished, with the schema of its
//!   endpoint and its canonical Arrow IPC stream, the shared ownership a host retains and
//!   releases, and copying the states, list offsets and values of one level of one column.
//! - **Depends on.** Arrow's array data, the schema handle, and the Rust client's canonical
//!   stream encoding.
//! - **Must not know.** Where a batch goes or came from, or how its stream is encoded.
//!
//! A column is read one level of its type at a time. Level 0 holds one cell per row; a `LIST` or
//! `FIXED_LIST` level holds lists whose elements are the cells of the next level, and the last
//! level holds the scalar values. Only a row can be null: list elements never are. Every copy
//! writes one whole level in one call, and a list's offsets start at zero.

use std::{mem::ManuallyDrop, ops::Range};

use arch_into::ArchInto as _;
use arrow_array::RecordBatch;
use arrow_buffer::BooleanBuffer;
use arrow_data::ArrayData;
use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{EmitterDelivery, ProducerBatch};
use nervix_models::{ParseAsType, SchemaField};
use nervix_primitives::sync::{Arc, blocking::OnceLock};
use nervix_recovery::Discarded as _;

use crate::{
    abi,
    event::CellState,
    failure::{Failure, FailureKind},
    schema::{FieldType, Part, Schema, TypeLevels},
};

/// One typed batch of an endpoint, shared by every reference a host holds.
pub struct Batch {
    batch: RecordBatch,
    schema: Schema,
    ipc: Ipc,
}

/// The canonical Arrow IPC stream of a batch.
enum Ipc {
    /// The stream a delivery carried, exactly as it arrived.
    Delivered(Bytes),
    /// The stream of a batch a host built, written the first time a host reads it.
    Built(OnceLock<Result<Bytes, Failure>>),
}

/// The cells of one level of one column: their type, and the Arrow data holding them.
struct LevelData<'a> {
    ty: &'a ParseAsType,
    data: ArrayData,
}

impl<'a> LevelData<'a> {
    /// The offsets of this level's own cells, one more than it holds: into the next level for a
    /// list, into the value bytes for a string or bytes level.
    fn offsets(&self) -> &[i32] {
        let buffer = self
            .data
            .buffers()
            .first()
            .verified("a list, string or bytes level keeps its offsets in its first buffer");
        let cells_end = self
            .data
            .offset()
            .checked_add(self.data.len())
            .verified("Arrow validated the offsets of every level it holds");
        let end = cells_end
            .checked_add(1)
            .verified("Arrow validated the offsets of every level it holds");
        buffer
            .typed_data::<i32>()
            .get(self.data.offset()..end)
            .verified("Arrow validated the offsets of every level it holds")
    }

    /// The positions from this level's first offset to its last.
    fn offset_range(&self) -> Range<usize> {
        let offsets = self.offsets();
        let first = offsets
            .first()
            .verified("a level's offsets hold at least its end");
        let last = offsets
            .last()
            .verified("a level's offsets hold at least its end");
        let first =
            usize::try_from(*first).verified("Arrow validated that offsets are not negative");
        let last = usize::try_from(*last).verified("Arrow validated that offsets are not negative");
        first..last
    }

    /// The level below this one: the elements of the lists it holds, or `None` for a scalar
    /// level.
    fn elements(&self) -> Option<LevelData<'a>> {
        match self.ty {
            ParseAsType::Vec { element } => {
                let range = self.offset_range();
                let child = self.data.child_data().first()?;
                let len = range
                    .end
                    .checked_sub(range.start)
                    .verified("Arrow validated that offsets never decrease");
                Some(LevelData {
                    ty: element,
                    data: child.slice(range.start, len),
                })
            }
            ParseAsType::Array { element, len } => {
                let per_list: usize = len.get().arch_into();
                let child = self.data.child_data().first()?;
                let start = self
                    .data
                    .offset()
                    .checked_mul(per_list)
                    .verified("Arrow validated a fixed-size list's elements");
                let elements = self
                    .data
                    .len()
                    .checked_mul(per_list)
                    .verified("Arrow validated a fixed-size list's elements");
                Some(LevelData {
                    ty: element,
                    data: child.slice(start, elements),
                })
            }
            ParseAsType::U8
            | ParseAsType::I8
            | ParseAsType::U16
            | ParseAsType::I16
            | ParseAsType::U32
            | ParseAsType::I32
            | ParseAsType::U64
            | ParseAsType::I64
            | ParseAsType::Bool
            | ParseAsType::String
            | ParseAsType::Datetime
            | ParseAsType::F32
            | ParseAsType::F64
            | ParseAsType::Bytes => None,
        }
    }
}

impl Batch {
    /// Decodes the batch a delivery carried, held to its consumer's schema and its row count.
    pub(crate) fn delivered(delivery: &EmitterDelivery, schema: Schema) -> Result<Self, Failure> {
        let batch = delivery.record_batch().map_err(Failure::from)?;
        Ok(Self {
            batch,
            schema,
            ipc: Ipc::Delivered(delivery.batch.clone()),
        })
    }

    /// A batch a host built for `schema`, which its columns already conform to.
    pub(crate) fn built(batch: RecordBatch, schema: Schema) -> Self {
        Self {
            batch,
            schema,
            ipc: Ipc::Built(OnceLock::new()),
        }
    }

    /// Hands the batch to a host as its first reference.
    pub(crate) fn into_shared(self) -> *mut Self {
        Arc::into_raw(Arc::new(self)).cast_mut()
    }

    pub(crate) fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The canonical Arrow IPC stream of the batch. A batch a host built is written the first
    /// time its stream is read, and every reader borrows the stream that was kept.
    pub fn ipc(&self) -> Result<&[u8], Failure> {
        let written = match &self.ipc {
            Ipc::Delivered(bytes) => return Ok(bytes),
            Ipc::Built(written) => written,
        };
        let stream = match written.get() {
            Some(stream) => stream,
            None => {
                let encoded = match ProducerBatch::from_record_batch(&self.batch) {
                    Ok(encoded) => Ok(encoded.arrow_ipc().clone()),
                    Err(report) => Err(Failure::from(report)),
                };
                written.set(encoded).discarded(
                    "a reader that wrote the same batch concurrently kept the same stream first",
                );
                written
                    .get()
                    .verified("this reader or a concurrent one kept the stream just above")
            }
        };
        match stream {
            Ok(bytes) => Ok(bytes),
            Err(failure) => Err(failure.clone()),
        }
    }

    fn field(&self, column: usize) -> Result<&SchemaField, Failure> {
        self.schema.field(Part::Rows, column)
    }

    /// The cells of `level` of `column`.
    fn level(&self, column: usize, level: usize) -> Result<LevelData<'_>, Failure> {
        let field = self.field(column)?;
        TypeLevels::of(&field.ty).level(level)?;
        let mut current = LevelData {
            ty: &field.ty,
            data: self.batch.column(column).to_data(),
        };
        for _ in 0..level {
            current = current
                .elements()
                .verified("the level was checked to lie within the field's type");
        }
        Ok(current)
    }

    /// How many cells `level` of `column` holds.
    pub fn cells(&self, column: usize, level: usize) -> Result<usize, Failure> {
        Ok(self.level(column, level)?.data.len())
    }

    /// Writes one state per row of `column`.
    pub fn states(&self, column: usize, states: &mut [u8]) -> Result<(), Failure> {
        let rows = self.level(column, 0)?;
        Self::check_len(states.len(), rows.data.len(), "states_len")?;
        let nulls = rows.data.nulls();
        for (index, state) in states.iter_mut().enumerate() {
            let valid = match nulls {
                Some(nulls) => nulls.is_valid(index),
                None => true,
            };
            let cell = if valid {
                CellState::Value
            } else {
                CellState::Null
            };
            *state = u8::from(cell);
        }
        Ok(())
    }

    /// Copies the offsets of the lists `level` of `column` holds into the level below it.
    pub fn offsets(&self, column: usize, level: usize, target: &mut [u64]) -> Result<(), Failure> {
        let cells = self.level(column, level)?;
        let ParseAsType::Vec { .. } = cells.ty else {
            return Err(Failure::new(
                FailureKind::Type,
                format!("level {level} of column {column} holds no variable-length lists"),
            ));
        };
        Self::copy_offsets(&cells, target, "offsets_len")
    }

    /// Writes a level's offsets, starting at zero, into `target`.
    fn copy_offsets(
        cells: &LevelData<'_>,
        target: &mut [u64],
        argument: &'static str,
    ) -> Result<(), Failure> {
        let offsets = cells.offsets();
        Self::check_len(target.len(), offsets.len(), argument)?;
        let first = cells.offset_range().start;
        for (target, offset) in target.iter_mut().zip(offsets) {
            let offset =
                usize::try_from(*offset).verified("Arrow validated that offsets are not negative");
            let relative = offset
                .checked_sub(first)
                .verified("Arrow validated that offsets never decrease");
            *target = relative.arch_into();
        }
        Ok(())
    }

    /// Copies the fixed-width values of `level` of `column` in native byte order.
    pub fn fixed(&self, column: usize, level: usize, values: &mut [u8]) -> Result<(), Failure> {
        let cells = self.level(column, level)?;
        let field_type = FieldType::from(cells.ty);
        let Some(width) = field_type.fixed_width() else {
            return Err(Failure::new(
                FailureKind::Type,
                format!(
                    "level {level} of column {column} holds {field_type:?}, which is not \
                     fixed-width"
                ),
            ));
        };
        let expected = cells
            .data
            .len()
            .checked_mul(width)
            .ok_or_else(|| Failure::invalid_argument("values_len", "overflows"))?;
        Self::check_len(values.len(), expected, "values_len")?;
        let buffer = cells
            .data
            .buffers()
            .first()
            .verified("a fixed-width level keeps its values in its first buffer");
        if let ParseAsType::Bool = cells.ty {
            let flags = BooleanBuffer::new(buffer.clone(), cells.data.offset(), cells.data.len());
            for (target, flag) in values.iter_mut().zip(flags.iter()) {
                *target = u8::from(flag);
            }
            return Ok(());
        }
        let start = cells
            .data
            .offset()
            .checked_mul(width)
            .verified("Arrow validated a fixed-width level's values");
        let end = start
            .checked_add(expected)
            .verified("Arrow validated a fixed-width level's values");
        let bytes = buffer
            .as_slice()
            .get(start..end)
            .verified("Arrow validated a fixed-width level's values");
        values.copy_from_slice(bytes);
        Ok(())
    }

    /// The value bytes of a string or bytes level, from its first offset to its last.
    fn varlen_bytes<'a>(cells: &'a LevelData<'_>) -> &'a [u8] {
        let range = cells.offset_range();
        cells
            .data
            .buffers()
            .get(1)
            .verified("a string or bytes level keeps its values in its second buffer")
            .as_slice()
            .get(range)
            .verified("Arrow validated a string or bytes level's offsets against its values")
    }

    fn varlen_level(&self, column: usize, level: usize) -> Result<LevelData<'_>, Failure> {
        let cells = self.level(column, level)?;
        match cells.ty {
            ParseAsType::String | ParseAsType::Bytes => Ok(cells),
            other => Err(Failure::new(
                FailureKind::Type,
                format!(
                    "level {level} of column {column} holds {:?}, not STRING or BYTES",
                    FieldType::from(other)
                ),
            )),
        }
    }

    /// The bytes the string or bytes values of `level` of `column` take.
    pub fn varlen_len(&self, column: usize, level: usize) -> Result<usize, Failure> {
        let cells = self.varlen_level(column, level)?;
        Ok(Self::varlen_bytes(&cells).len())
    }

    /// Copies the string or bytes values of `level` of `column`: `offsets` receives one offset per
    /// cell and a final end, starting at zero, and `data` the concatenated values, which must be
    /// exactly as long as they are.
    pub fn varlen(
        &self,
        column: usize,
        level: usize,
        offsets: &mut [u64],
        data: &mut [u8],
    ) -> Result<(), Failure> {
        let cells = self.varlen_level(column, level)?;
        Self::copy_offsets(&cells, offsets, "offsets_len")?;
        let bytes = Self::varlen_bytes(&cells);
        Self::check_len(data.len(), bytes.len(), "data_capacity")?;
        data.copy_from_slice(bytes);
        Ok(())
    }

    fn check_len(actual: usize, expected: usize, argument: &'static str) -> Result<(), Failure> {
        if actual != expected {
            return Err(Failure::invalid_argument(
                argument,
                &format!("must be {expected} for this level, not {actual}"),
            ));
        }
        Ok(())
    }
}

/// # Safety
///
/// `batch` is a live batch this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_row_count(batch: *const Batch) -> usize {
    // SAFETY: the header requires a live batch.
    unsafe { abi::accessor(batch) }.row_count()
}

/// # Safety
///
/// `batch` is a live batch; a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_schema(
    batch: *const Batch,
    out: *mut *mut Schema,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_schema(batch, out) })
}

/// # Safety
///
/// As [`nx_batch_schema`].
unsafe fn write_schema(batch: *const Batch, out: *mut *mut Schema) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live batch.
    let schema = unsafe { abi::handle(batch, "batch") }?.schema().clone();
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(schema)) };
    Ok(())
}

/// # Safety
///
/// `batch` is a live batch; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_ipc(
    batch: *const Batch,
    ipc: *mut *const u8,
    ipc_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_ipc(batch, ipc, ipc_len) })
}

/// # Safety
///
/// As [`nx_batch_ipc`].
unsafe fn write_ipc(
    batch: *const Batch,
    ipc: *mut *const u8,
    ipc_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(ipc, "ipc")?;
    // SAFETY: the caller guarantees a live batch.
    let bytes = unsafe { abi::handle(batch, "batch") }?.ipc()?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe { abi::write_bytes(ipc, ipc_len, bytes) };
    Ok(())
}

/// # Safety
///
/// `batch` is a live batch; a non-null `cells` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_cells(
    batch: *const Batch,
    column: usize,
    level: usize,
    cells: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_cells(batch, column, level, cells) })
}

/// # Safety
///
/// As [`nx_batch_cells`].
unsafe fn write_cells(
    batch: *const Batch,
    column: usize,
    level: usize,
    cells: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(cells, "cells")?;
    // SAFETY: the caller guarantees a live batch.
    let count = unsafe { abi::handle(batch, "batch") }?.cells(column, level)?;
    // SAFETY: `cells` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(cells, count) };
    Ok(())
}

/// # Safety
///
/// `batch` is a live batch and a non-null `states` addresses `states_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_states(
    batch: *const Batch,
    column: usize,
    states: *mut u8,
    states_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_states(batch, column, states, states_len) })
}

/// # Safety
///
/// As [`nx_batch_states`].
unsafe fn write_states(
    batch: *const Batch,
    column: usize,
    states: *mut u8,
    states_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live batch and writable states.
    let (batch, states) = unsafe {
        (
            abi::handle(batch, "batch")?,
            abi::output_slice(states, states_len, "states")?,
        )
    };
    batch.states(column, states)
}

/// # Safety
///
/// `batch` is a live batch and a non-null `offsets` addresses `offsets_len` writable entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_offsets(
    batch: *const Batch,
    column: usize,
    level: usize,
    offsets: *mut u64,
    offsets_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_offsets(batch, column, level, offsets, offsets_len) })
}

/// # Safety
///
/// As [`nx_batch_offsets`].
unsafe fn write_offsets(
    batch: *const Batch,
    column: usize,
    level: usize,
    offsets: *mut u64,
    offsets_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live batch and writable offsets.
    let (batch, offsets) = unsafe {
        (
            abi::handle(batch, "batch")?,
            abi::output_slice(offsets, offsets_len, "offsets")?,
        )
    };
    batch.offsets(column, level, offsets)
}

/// # Safety
///
/// `batch` is a live batch and a non-null `values` addresses `values_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_fixed(
    batch: *const Batch,
    column: usize,
    level: usize,
    values: *mut std::ffi::c_void,
    values_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_fixed(batch, column, level, values.cast::<u8>(), values_len) })
}

/// # Safety
///
/// As [`nx_batch_fixed`].
unsafe fn write_fixed(
    batch: *const Batch,
    column: usize,
    level: usize,
    values: *mut u8,
    values_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live batch and writable values.
    let (batch, values) = unsafe {
        (
            abi::handle(batch, "batch")?,
            abi::output_slice(values, values_len, "values")?,
        )
    };
    batch.fixed(column, level, values)
}

/// # Safety
///
/// `batch` is a live batch; a non-null `offsets` addresses `offsets_len` writable entries, a
/// non-null `data` addresses `data_capacity` writable bytes, and a non-null `data_len` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_varlen(
    batch: *const Batch,
    column: usize,
    level: usize,
    offsets: *mut u64,
    offsets_len: usize,
    data: *mut u8,
    data_capacity: usize,
    data_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_varlen(
            batch,
            column,
            level,
            offsets,
            offsets_len,
            data,
            data_capacity,
            data_len,
        )
    })
}

/// # Safety
///
/// As [`nx_batch_varlen`].
#[expect(
    clippy::too_many_arguments,
    reason = "the C ABI passes each caller buffer as a pointer and a length"
)]
unsafe fn write_varlen(
    batch: *const Batch,
    column: usize,
    level: usize,
    offsets: *mut u64,
    offsets_len: usize,
    data: *mut u8,
    data_capacity: usize,
    data_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(data_len, "data_len")?;
    // SAFETY: the caller guarantees a live batch.
    let batch = unsafe { abi::handle(batch, "batch") }?;
    let needed = batch.varlen_len(column, level)?;
    // SAFETY: `data_len` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(data_len, needed) };
    if data.is_null() {
        return Ok(());
    }
    if data_capacity < needed {
        return Err(Failure::invalid_argument(
            "data_capacity",
            &format!("is {data_capacity} bytes, and the level needs {needed}"),
        ));
    }
    // SAFETY: the caller guarantees writable offsets, and `data` addresses at least `needed`
    // writable bytes.
    let (offsets, data) = unsafe {
        (
            abi::output_slice(offsets, offsets_len, "offsets")?,
            abi::output_slice(data, needed, "data")?,
        )
    };
    batch.varlen(column, level, offsets, data)
}

/// # Safety
///
/// `batch` is a live reference to a batch this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_retain(batch: *mut Batch) -> *mut Batch {
    // SAFETY: the header requires a live reference, which `into_shared` or this function made
    // from an `Arc`. Wrapping it in `ManuallyDrop` leaves the caller's reference in place.
    let shared = ManuallyDrop::new(unsafe { Arc::from_raw(batch.cast_const()) });
    Arc::into_raw(Arc::clone(&shared)).cast_mut()
}

/// # Safety
///
/// A non-null `batch` is a reference this library returned that has not been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_release(batch: *mut Batch) {
    if batch.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased reference, which `into_shared` or
    // `nx_batch_retain` made from an `Arc`.
    drop(unsafe { Arc::from_raw(batch.cast_const()) });
}

/// Hands out a new reference to a shared batch, for a delivery that keeps its decoded batch.
pub(crate) fn share(batch: &Arc<Batch>) -> *mut Batch {
    Arc::into_raw(Arc::clone(batch)).cast_mut()
}

#[cfg(test)]
impl Batch {
    /// A batch decoded from a stream a test wrote, as a delivery's would be.
    pub(crate) fn decoded(batch: RecordBatch, schema: Schema, ipc: Bytes) -> Self {
        Self {
            batch,
            schema,
            ipc: Ipc::Delivered(ipc),
        }
    }
}
