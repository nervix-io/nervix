//! Building a typed Arrow batch for a client endpoint from a host's columns:
//! `nx_batch_builder`.
//!
//! - **Owns.** Copying a host's row states, list offsets and values one level of one column at a
//!   time, refusing what the column's type or nullability cannot hold, and finishing the columns
//!   as the Arrow arrays of the batch's schema.
//! - **Depends on.** Arrow's buffers and array data, the schema handle, and the batch it finishes.
//! - **Must not know.** Which producer the batch is for, or how its stream is encoded.
//!
//! Every call copies what it is given before it returns, so a host may reuse or free its buffers
//! as soon as the call returns. Values are never converted: a level takes exactly its type's
//! width or text, a boolean is 0 or 1, and a string is UTF-8.

use std::{num::NonZeroU32, sync::Arc as StdArc};

use arch_into::ArchInto as _;
use arrow_array::{ArrayRef, RecordBatch, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, MutableBuffer};
use arrow_data::ArrayData;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ParseAsType, SchemaField};

use crate::{
    abi,
    batch::Batch,
    event::CellState,
    failure::{Failure, FailureKind},
    schema::{FieldType, Part, Schema, TypeLevels},
};

/// The batch a host is building.
pub struct BatchBuilder {
    schema: Schema,
    rows: usize,
    columns: Vec<ColumnParts>,
}

/// What a host has set of one column.
struct ColumnParts {
    /// Whether each row holds a value, once set; unset, every row does.
    valid: Option<Vec<bool>>,
    /// The list levels of the column's type, from the outside in.
    lists: Vec<ListPart>,
    /// The values of the innermost level, once set.
    values: Option<LeafValues>,
}

/// One list level of a column.
enum ListPart {
    /// Variable-length lists: their offsets into the next level, once set.
    List { offsets: Option<Vec<i32>> },
    /// Fixed-size lists of `per_list` elements each.
    FixedList { per_list: NonZeroU32 },
}

/// The values of a column's innermost level.
enum LeafValues {
    /// Fixed-width values, in Arrow's layout: native-order values, or one bit per boolean.
    Fixed { buffer: Buffer, count: usize },
    /// String or bytes values: one offset per value and a final end, into `data`.
    Varlen { offsets: Vec<i32>, data: Buffer },
}

impl LeafValues {
    fn count(&self) -> usize {
        match self {
            Self::Fixed { count, .. } => *count,
            Self::Varlen { offsets, .. } => offsets
                .len()
                .checked_sub(1)
                .verified("checked offsets hold at least the final end"),
        }
    }
}

impl ColumnParts {
    /// The empty parts of a column of type `ty`.
    fn of(ty: &ParseAsType) -> Self {
        let mut lists = Vec::new();
        let mut current = ty;
        loop {
            match current {
                ParseAsType::Vec { element } => {
                    lists.push(ListPart::List { offsets: None });
                    current = element;
                }
                ParseAsType::Array { element, len } => {
                    lists.push(ListPart::FixedList { per_list: *len });
                    current = element;
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
                | ParseAsType::Bytes => break,
            }
        }
        Self {
            valid: None,
            lists,
            values: None,
        }
    }
}

/// Checks offsets a host passed: at least the end, starting at zero, never decreasing, and each
/// within the range Arrow's 32-bit offsets hold.
fn checked_offsets(offsets: &[u64], argument: &'static str) -> Result<Vec<i32>, Failure> {
    let Some(first) = offsets.first() else {
        return Err(Failure::invalid_argument(
            argument,
            "must hold at least the final end",
        ));
    };
    if *first != 0 {
        return Err(Failure::invalid_argument(argument, "must start at zero"));
    }
    let mut checked = Vec::with_capacity(offsets.len());
    let mut previous = 0_u64;
    for offset in offsets {
        if *offset < previous {
            return Err(Failure::invalid_argument(argument, "must never decrease"));
        }
        let Ok(offset32) = i32::try_from(*offset) else {
            return Err(Failure::invalid_argument(
                argument,
                "exceeds the range of Arrow's 32-bit offsets",
            ));
        };
        checked.push(offset32);
        previous = *offset;
    }
    Ok(checked)
}

/// The end the last of checked offsets names.
fn end_of(offsets: &[i32]) -> usize {
    let last = offsets
        .last()
        .verified("checked offsets hold at least the final end");
    usize::try_from(*last).verified("checked offsets are not negative")
}

impl BatchBuilder {
    /// Starts a batch of `rows` rows of `schema`'s row fields.
    pub fn new(schema: Schema, rows: usize) -> Self {
        let columns = schema
            .fields(Part::Rows)
            .iter()
            .map(|field| ColumnParts::of(&field.ty))
            .collect();
        Self {
            schema,
            rows,
            columns,
        }
    }

    fn field(&self, column: usize) -> Result<&SchemaField, Failure> {
        self.schema.field(Part::Rows, column)
    }

    fn parts(&mut self, column: usize) -> Result<&mut ColumnParts, Failure> {
        let fields = self.columns.len();
        self.columns.get_mut(column).ok_or_else(|| {
            Failure::new(
                FailureKind::InvalidArgument,
                format!("column {column} is past the {fields} fields of the batch"),
            )
        })
    }

    /// Sets whether each row of `column` holds a value.
    pub fn states(&mut self, column: usize, states: &[u8]) -> Result<(), Failure> {
        let field = self.field(column)?;
        let optional = field.optional;
        let name = field.name.as_str().to_string();
        if states.len() != self.rows {
            return Err(Failure::invalid_argument(
                "states_len",
                &format!("must be {} for this batch, not {}", self.rows, states.len()),
            ));
        }
        let mut valid = Vec::with_capacity(states.len());
        for state in states {
            let holds_value = match CellState::from_host(*state)? {
                CellState::Value => true,
                CellState::Null if optional => false,
                CellState::Null => {
                    return Err(Failure::invalid_argument(
                        "states",
                        &format!("marks a row of required field '{name}' null"),
                    ));
                }
                CellState::Redacted => {
                    return Err(Failure::invalid_argument(
                        "states",
                        "marks a row redacted, which only a server withholding it can",
                    ));
                }
            };
            valid.push(holds_value);
        }
        self.parts(column)?.valid = Some(valid);
        Ok(())
    }

    /// Sets the offsets of the lists `level` of `column` holds into the level below it.
    pub fn offsets(&mut self, column: usize, level: usize, offsets: &[u64]) -> Result<(), Failure> {
        let checked = checked_offsets(offsets, "offsets")?;
        let parts = self.parts(column)?;
        match parts.lists.get_mut(level) {
            Some(ListPart::List { offsets }) => {
                *offsets = Some(checked);
                Ok(())
            }
            Some(ListPart::FixedList { .. }) | None => Err(Failure::new(
                FailureKind::Type,
                format!("level {level} of column {column} holds no variable-length lists"),
            )),
        }
    }

    /// The type of the innermost level of `column`, when `level` is that level.
    fn leaf(&self, column: usize, level: usize) -> Result<&ParseAsType, Failure> {
        let field = self.field(column)?;
        let levels = TypeLevels::of(&field.ty);
        let innermost = levels
            .count()
            .checked_sub(1)
            .verified("a type has at least its own level");
        if level != innermost {
            return Err(Failure::new(
                FailureKind::Type,
                format!("level {level} of column {column} holds lists, not values"),
            ));
        }
        levels.level(level)
    }

    /// Sets the fixed-width values of the innermost level of `column`, in native byte order.
    pub fn fixed(&mut self, column: usize, level: usize, values: &[u8]) -> Result<(), Failure> {
        let ty = self.leaf(column, level)?;
        let field_type = FieldType::from(ty);
        let Some(width) = field_type.fixed_width() else {
            return Err(Failure::new(
                FailureKind::Type,
                format!("column {column} holds {field_type:?}, which is not fixed-width"),
            ));
        };
        if !values.len().is_multiple_of(width) {
            return Err(Failure::invalid_argument(
                "values_len",
                &format!("must be a multiple of the {width}-byte width of the column's values"),
            ));
        }
        let count = values.len() / width;
        let buffer = match ty {
            ParseAsType::Bool => {
                let mut flags = Vec::with_capacity(count);
                for value in values {
                    let flag = match value {
                        0 => false,
                        1 => true,
                        _ => {
                            return Err(Failure::invalid_argument(
                                "values",
                                "holds a BOOL byte that is neither 0 nor 1",
                            ));
                        }
                    };
                    flags.push(flag);
                }
                BooleanBuffer::from(flags).into_inner()
            }
            _ => {
                let mut buffer = MutableBuffer::with_capacity(values.len());
                buffer.extend_from_slice(values);
                Buffer::from(buffer)
            }
        };
        self.parts(column)?.values = Some(LeafValues::Fixed { buffer, count });
        Ok(())
    }

    /// Sets the string or bytes values of the innermost level of `column`: one offset per value
    /// and a final end into `data`.
    pub fn varlen(
        &mut self,
        column: usize,
        level: usize,
        offsets: &[u64],
        data: &[u8],
    ) -> Result<(), Failure> {
        let ty = self.leaf(column, level)?;
        if !matches!(ty, ParseAsType::String | ParseAsType::Bytes) {
            return Err(Failure::new(
                FailureKind::Type,
                format!(
                    "column {column} holds {:?}, not STRING or BYTES",
                    FieldType::from(ty)
                ),
            ));
        }
        let checked = checked_offsets(offsets, "offsets")?;
        if end_of(&checked) != data.len() {
            return Err(Failure::invalid_argument(
                "offsets",
                &format!("must end at the {} bytes of `data`", data.len()),
            ));
        }
        let mut buffer = MutableBuffer::with_capacity(data.len());
        buffer.extend_from_slice(data);
        self.parts(column)?.values = Some(LeafValues::Varlen {
            offsets: checked,
            data: Buffer::from(buffer),
        });
        Ok(())
    }

    /// Finishes the batch from every column the host set, leaving the builder empty for another
    /// batch of the same rows.
    pub fn finish(&mut self) -> Result<Batch, Failure> {
        let fields = self.schema.fields(Part::Rows);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (column, (field, parts)) in fields.iter().zip(&self.columns).enumerate() {
            let data = Self::column_data(field, parts, self.rows, column)?;
            arrays.push(make_array(data));
        }
        let arrow_schema = StdArc::new(SchemaField::arrow_schema(fields));
        let batch = RecordBatch::try_new(arrow_schema, arrays).map_err(|error| {
            Failure::invalid_argument("builder", &format!("does not make a valid batch: {error}"))
        })?;
        self.columns = fields
            .iter()
            .map(|field| ColumnParts::of(&field.ty))
            .collect();
        Ok(Batch::built(batch, self.schema.clone()))
    }

    /// The Arrow data of one column, built from its innermost level up to its rows.
    fn column_data(
        field: &SchemaField,
        parts: &ColumnParts,
        rows: usize,
        column: usize,
    ) -> Result<ArrayData, Failure> {
        let levels = TypeLevels::of(&field.ty);
        // How many cells each level holds, from the rows down to the innermost level.
        let mut cells = Vec::with_capacity(levels.count());
        let mut count = rows;
        for (level, list) in parts.lists.iter().enumerate() {
            cells.push(count);
            count = match list {
                ListPart::List { offsets: None } => {
                    return Err(Self::missing(column, level, "its list offsets"));
                }
                ListPart::List {
                    offsets: Some(offsets),
                } => {
                    let expected = count
                        .checked_add(1)
                        .ok_or_else(|| Failure::invalid_argument("offsets_len", "overflows"))?;
                    if offsets.len() != expected {
                        return Err(Failure::invalid_argument(
                            "offsets_len",
                            &format!(
                                "must be {expected} for level {level} of column {column}, not {}",
                                offsets.len()
                            ),
                        ));
                    }
                    end_of(offsets)
                }
                ListPart::FixedList { per_list } => {
                    let per_list: usize = per_list.get().arch_into();
                    count.checked_mul(per_list).ok_or_else(|| {
                        Failure::invalid_argument("rows", "hold more elements than a usize counts")
                    })?
                }
            };
        }
        let innermost = parts.lists.len();
        let Some(values) = &parts.values else {
            return Err(Self::missing(column, innermost, "its values"));
        };
        if values.count() != count {
            return Err(Failure::invalid_argument(
                "values",
                &format!(
                    "hold {} values where level {innermost} of column {column} has {count} cells",
                    values.count()
                ),
            ));
        }
        let nulls = match (&parts.valid, field.optional) {
            (Some(valid), true) => Some(BooleanBuffer::from(valid.clone()).into_inner()),
            (Some(_), false) | (None, _) => None,
        };
        let leaf_type = levels.level(innermost)?;
        let leaf_nulls = if innermost == 0 { nulls.clone() } else { None };
        let buffers = match values {
            LeafValues::Fixed { buffer, .. } => vec![buffer.clone()],
            LeafValues::Varlen { offsets, data } => {
                vec![Buffer::from_slice_ref(offsets), data.clone()]
            }
        };
        let mut data = Self::array_data(leaf_type, count, leaf_nulls, buffers, Vec::new())?;
        for level in (0..innermost).rev() {
            let level_type = levels.level(level)?;
            let level_cells = *cells
                .get(level)
                .verified("a count was kept for every list level");
            let level_nulls = if level == 0 { nulls.clone() } else { None };
            let buffers = match parts.lists.get(level) {
                Some(ListPart::List {
                    offsets: Some(offsets),
                }) => vec![Buffer::from_slice_ref(offsets)],
                Some(ListPart::FixedList { .. }) => Vec::new(),
                Some(ListPart::List { offsets: None }) | None => {
                    return Err(Self::missing(column, level, "its list offsets"));
                }
            };
            data = Self::array_data(level_type, level_cells, level_nulls, buffers, vec![data])?;
        }
        Ok(data)
    }

    /// The validated Arrow data of one level of `ty`'s Arrow type.
    fn array_data(
        ty: &ParseAsType,
        len: usize,
        nulls: Option<Buffer>,
        buffers: Vec<Buffer>,
        children: Vec<ArrayData>,
    ) -> Result<ArrayData, Failure> {
        ArrayData::try_new(ty.arrow_data_type(), len, nulls, 0, buffers, children).map_err(
            |error| {
                Failure::invalid_argument("builder", &format!("holds an invalid level: {error}"))
            },
        )
    }

    fn missing(column: usize, level: usize, what: &str) -> Failure {
        Failure::invalid_argument(
            "builder",
            &format!("has not been given {what} for level {level} of column {column}"),
        )
    }
}

/// # Safety
///
/// `schema` is a live schema and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_new(
    schema: *const Schema,
    rows: usize,
    out: *mut *mut BatchBuilder,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_new(schema, rows, out) })
}

/// # Safety
///
/// As [`nx_batch_builder_new`].
unsafe fn write_new(
    schema: *const Schema,
    rows: usize,
    out: *mut *mut BatchBuilder,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live schema.
    let schema = unsafe { abi::handle(schema, "schema") }?.clone();
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(BatchBuilder::new(schema, rows))) };
    Ok(())
}

/// # Safety
///
/// `builder` is a live builder no other thread uses, and a non-null `states` addresses
/// `states_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_states(
    builder: *mut BatchBuilder,
    column: usize,
    states: *const u8,
    states_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { set_states(builder, column, states, states_len) })
}

/// # Safety
///
/// As [`nx_batch_builder_states`].
unsafe fn set_states(
    builder: *mut BatchBuilder,
    column: usize,
    states: *const u8,
    states_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live builder no other thread uses and readable states.
    let (builder, states) = unsafe {
        (
            abi::handle_mut(builder, "builder")?,
            abi::input_slice(states, states_len, "states")?,
        )
    };
    builder.states(column, states)
}

/// # Safety
///
/// `builder` is a live builder no other thread uses, and a non-null `offsets` addresses
/// `offsets_len` readable entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_offsets(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    offsets: *const u64,
    offsets_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { set_offsets(builder, column, level, offsets, offsets_len) })
}

/// # Safety
///
/// As [`nx_batch_builder_offsets`].
unsafe fn set_offsets(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    offsets: *const u64,
    offsets_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live builder no other thread uses and readable offsets.
    let (builder, offsets) = unsafe {
        (
            abi::handle_mut(builder, "builder")?,
            abi::input_slice(offsets, offsets_len, "offsets")?,
        )
    };
    builder.offsets(column, level, offsets)
}

/// # Safety
///
/// `builder` is a live builder no other thread uses, and a non-null `values` addresses
/// `values_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_fixed(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    values: *const std::ffi::c_void,
    values_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { set_fixed(builder, column, level, values.cast::<u8>(), values_len) })
}

/// # Safety
///
/// As [`nx_batch_builder_fixed`].
unsafe fn set_fixed(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    values: *const u8,
    values_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live builder no other thread uses and readable values.
    let (builder, values) = unsafe {
        (
            abi::handle_mut(builder, "builder")?,
            abi::input_slice(values, values_len, "values")?,
        )
    };
    builder.fixed(column, level, values)
}

/// # Safety
///
/// `builder` is a live builder no other thread uses; a non-null `offsets` addresses
/// `offsets_len` readable entries and a non-null `data` addresses `data_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_varlen(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    offsets: *const u64,
    offsets_len: usize,
    data: *const u8,
    data_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        set_varlen(builder, column, level, offsets, offsets_len, data, data_len)
    })
}

/// # Safety
///
/// As [`nx_batch_builder_varlen`].
unsafe fn set_varlen(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    offsets: *const u64,
    offsets_len: usize,
    data: *const u8,
    data_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live builder no other thread uses and readable buffers.
    let (builder, offsets, data) = unsafe {
        (
            abi::handle_mut(builder, "builder")?,
            abi::input_slice(offsets, offsets_len, "offsets")?,
            abi::input_slice(data, data_len, "data")?,
        )
    };
    builder.varlen(column, level, offsets, data)
}

/// # Safety
///
/// `builder` is a live builder no other thread uses, and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_finish(
    builder: *mut BatchBuilder,
    out: *mut *mut Batch,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_finished(builder, out) })
}

/// # Safety
///
/// As [`nx_batch_builder_finish`].
unsafe fn write_finished(builder: *mut BatchBuilder, out: *mut *mut Batch) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live builder no other thread uses.
    let batch = unsafe { abi::handle_mut(builder, "builder") }?.finish()?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, batch.into_shared()) };
    Ok(())
}

/// # Safety
///
/// A non-null `builder` is a builder this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_free(builder: *mut BatchBuilder) {
    // SAFETY: the header requires an unreleased builder or null.
    unsafe { abi::release(builder) };
}
