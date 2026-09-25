//! A subscription event a host holds: `nx_event`, and bulk access to the rows it carries.
//!
//! - **Owns.** The event kinds and cell states the header names, the shared ownership a host
//!   retains and releases, and copying or borrowing the cells of a batch column by column.
//! - **Depends on.** The Rust client's subscription events, the wire contract's row views, and
//!   the schema handle.
//! - **Must not know.** How events are routed or restored; the session decides that.
//!
//! A rows event owns the verified frame it was decoded from and nothing else, so retaining one
//! keeps exactly that frame alive. Every accessor reads the frame through the wire contract's
//! checked views; a batch is held to its schema once, when the event is created, which is why
//! reading a cell the schema describes cannot fail.

use std::mem::ManuallyDrop;

use arch_into::ArchInto as _;
use meticulous::OptionExt as _;
use nervix_client_core::{
    SubscriptionEvent,
    wire::{CellView, CellsView, RowBatchView},
};
use triomphe::Arc;

use crate::{
    abi,
    failure::{Failure, FailureKind},
    schema::{FieldType, Part, Schema},
};

/// What a subscription event reports, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum EventKind {
    Rows = 1,
    DeliveryLost = 2,
    RowsSkipped = 3,
    Ended = 4,
    Interrupted = 5,
    ConsumerOverflow = 6,
}

/// What a cell holds, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellState {
    Value,
    Null,
    Redacted,
}

impl From<CellState> for u8 {
    fn from(state: CellState) -> Self {
        match state {
            CellState::Value => 1,
            CellState::Null => 2,
            CellState::Redacted => 3,
        }
    }
}

impl From<CellView<'_>> for CellState {
    fn from(cell: CellView<'_>) -> Self {
        match cell {
            CellView::Null => Self::Null,
            CellView::Redacted => Self::Redacted,
            _ => Self::Value,
        }
    }
}

/// One subscription event, shared by every reference a host holds.
#[derive(Debug)]
pub struct Event {
    event: SubscriptionEvent,
}

/// The cells of one part of a rows event: its rows, or its single branch key.
#[derive(Clone, Copy)]
enum PartCells<'a> {
    Rows(RowBatchView<'a>),
    BranchKey(CellsView<'a>),
}

impl<'a> PartCells<'a> {
    /// How many cells a column of the part holds.
    fn len(&self) -> usize {
        match self {
            Self::Rows(batch) => batch.len(),
            Self::BranchKey(_) => 1,
        }
    }

    /// The cells of `column`, in row order.
    ///
    /// The column is one the schema describes, and the batch conformed to that schema when the
    /// event was created, so every row holds it.
    fn for_each_cell(
        &self,
        column: usize,
        mut visit: impl FnMut(usize, CellView<'a>) -> Result<(), Failure>,
    ) -> Result<(), Failure> {
        match self {
            Self::Rows(batch) => {
                for (index, row) in batch.rows().enumerate() {
                    let cell = row
                        .get(column)
                        .assured("a conforming row holds one cell per schema field");
                    visit(index, cell)?;
                }
                Ok(())
            }
            Self::BranchKey(key) => {
                let cell = key
                    .get(column)
                    .assured("a conforming branch key holds one cell per key field");
                visit(0, cell)
            }
        }
    }

    /// One cell of the part.
    fn cell(&self, row: usize, column: usize) -> Result<CellView<'a>, Failure> {
        let cells = match self {
            Self::Rows(batch) => batch.row(row),
            Self::BranchKey(key) => match row {
                0 => Some(*key),
                _ => None,
            },
        };
        let Some(cells) = cells else {
            return Err(Failure::new(
                FailureKind::InvalidArgument,
                format!("row {row} is past the {} cells of the column", self.len()),
            ));
        };
        Ok(cells
            .get(column)
            .assured("a conforming row holds one cell per schema field"))
    }
}

impl Event {
    /// Wraps an event a session delivered, holding a rows event to its schema once.
    pub(crate) fn new(event: SubscriptionEvent) -> Result<Self, Failure> {
        if let SubscriptionEvent::Rows(rows) = &event
            && let Err(error) = rows.rows.batch().conform(&rows.schema)
        {
            return Err(Failure::new(
                FailureKind::Protocol,
                format!("a batch does not conform to its subscription schema: {error:#}"),
            ));
        }
        Ok(Self { event })
    }

    /// Hands the event to a host as its first reference.
    pub(crate) fn into_shared(self) -> *mut Self {
        Arc::into_raw(Arc::new(self)).cast_mut()
    }

    pub fn kind(&self) -> EventKind {
        match &self.event {
            SubscriptionEvent::Rows(_) => EventKind::Rows,
            SubscriptionEvent::DeliveryLost(_) => EventKind::DeliveryLost,
            SubscriptionEvent::RowsSkipped(_) => EventKind::RowsSkipped,
            SubscriptionEvent::Ended(_) => EventKind::Ended,
            SubscriptionEvent::Interrupted(_) => EventKind::Interrupted,
            SubscriptionEvent::ConsumerOverflow(_) => EventKind::ConsumerOverflow,
        }
    }

    /// The rows a rows event carries, or the rows a loss or skip counts.
    pub fn row_count(&self) -> u64 {
        match &self.event {
            SubscriptionEvent::Rows(rows) => rows.rows.batch().len().arch_into(),
            SubscriptionEvent::DeliveryLost(lost) => lost.dropped_rows.get(),
            SubscriptionEvent::RowsSkipped(skipped) => skipped.skipped_rows.get(),
            SubscriptionEvent::Ended(_)
            | SubscriptionEvent::Interrupted(_)
            | SubscriptionEvent::ConsumerOverflow(_) => 0,
        }
    }

    pub fn subscription_event(&self) -> &SubscriptionEvent {
        &self.event
    }

    fn rows(&self) -> Result<&nervix_client_core::SubscriptionRowsEvent, Failure> {
        match &self.event {
            SubscriptionEvent::Rows(rows) => Ok(rows),
            SubscriptionEvent::DeliveryLost(_)
            | SubscriptionEvent::RowsSkipped(_)
            | SubscriptionEvent::Ended(_)
            | SubscriptionEvent::Interrupted(_)
            | SubscriptionEvent::ConsumerOverflow(_) => {
                Err(Failure::new(FailureKind::Type, "the event carries no rows"))
            }
        }
    }

    pub fn schema(&self) -> Result<Schema, Failure> {
        Ok(Schema::new(self.rows()?.schema.clone()))
    }

    pub fn frame(&self) -> Result<&[u8], Failure> {
        Ok(self.rows()?.rows.frame().bytes())
    }

    /// The cells of `part` and the type of `column` within it, after checking the column exists.
    fn column(&self, part: Part, column: usize) -> Result<(PartCells<'_>, FieldType), Failure> {
        let rows = self.rows()?;
        let schema = Schema::new(rows.schema.clone());
        let field_type = FieldType::from(&schema.field(part, column)?.ty);
        let batch = rows.rows.batch();
        let cells = match part {
            Part::Rows => PartCells::Rows(batch),
            Part::BranchKey => PartCells::BranchKey(
                batch
                    .branch_key()
                    .assured("a conforming batch of a branched relay carries its branch key"),
            ),
        };
        Ok((cells, field_type))
    }

    /// Writes one state per cell of a column.
    pub fn column_states(
        &self,
        part: Part,
        column: usize,
        states: &mut [u8],
    ) -> Result<(), Failure> {
        let (cells, _) = self.column(part, column)?;
        Self::check_cells(&cells, states.len(), "states_len")?;
        cells.for_each_cell(column, |index, cell| {
            let state = states
                .get_mut(index)
                .verified("the states were checked to hold one entry per cell");
            *state = u8::from(CellState::from(cell));
            Ok(())
        })
    }

    /// Copies a fixed-width column in native byte order.
    pub fn column_fixed(
        &self,
        part: Part,
        column: usize,
        values: &mut [u8],
    ) -> Result<(), Failure> {
        let (cells, field_type) = self.column(part, column)?;
        let Some(width) = field_type.fixed_width() else {
            return Err(Failure::new(
                FailureKind::Type,
                format!("column {column} holds {field_type:?}, which is not fixed-width"),
            ));
        };
        let expected = cells
            .len()
            .checked_mul(width)
            .ok_or_else(|| Failure::invalid_argument("values_len", "overflows"))?;
        if values.len() != expected {
            return Err(Failure::invalid_argument(
                "values_len",
                &format!(
                    "must be {expected} bytes for this column, not {}",
                    values.len()
                ),
            ));
        }
        let mut targets = values.chunks_exact_mut(width);
        cells.for_each_cell(column, |_, cell| {
            let target = targets
                .next()
                .verified("the values were checked to hold one width per cell");
            Self::write_fixed(cell, target)
        })
    }

    /// Writes a fixed-width cell into a target of its field's width, or zeroes for a null or
    /// redacted cell. A conforming cell of a fixed-width field always holds a fixed-width value.
    fn write_fixed(cell: CellView<'_>, target: &mut [u8]) -> Result<(), Failure> {
        match cell {
            CellView::Null | CellView::Redacted => target.fill(0),
            CellView::U8(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::I8(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::U16(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::I16(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::U32(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::I32(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::U64(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::I64(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::F32(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::F64(value) => target.copy_from_slice(&value.to_ne_bytes()),
            CellView::Bool(value) => target.copy_from_slice(&[u8::from(value)]),
            CellView::Datetime(value) => {
                target.copy_from_slice(&value.unix_nanos().to_ne_bytes());
            }
            CellView::String(_) | CellView::Bytes(_) | CellView::List(_) => {
                return Err(Failure::new(
                    FailureKind::Type,
                    format!("a {} cell is not fixed-width", cell.kind()),
                ));
            }
        }
        Ok(())
    }

    /// The bytes of a string or bytes cell, or nothing for a null or redacted one.
    fn varlen(cell: CellView<'_>) -> Option<&[u8]> {
        match cell {
            CellView::String(value) => Some(value.as_bytes()),
            CellView::Bytes(value) => Some(value),
            _ => None,
        }
    }

    /// The bytes a string or bytes column needs.
    pub fn column_varlen_len(&self, part: Part, column: usize) -> Result<usize, Failure> {
        let (cells, field_type) = self.column(part, column)?;
        Self::check_varlen(field_type, column)?;
        let mut total = 0_usize;
        cells.for_each_cell(column, |_, cell| {
            let length = match Self::varlen(cell) {
                Some(value) => value.len(),
                None => 0,
            };
            total = total
                .checked_add(length)
                .verified("the column's values all lie inside one frame, whose length is a usize");
            Ok(())
        })?;
        Ok(total)
    }

    /// Copies a string or bytes column: `offsets` receives one offset per cell and a final end,
    /// and `data` the concatenated values, which must be exactly the column's length.
    pub fn column_varlen(
        &self,
        part: Part,
        column: usize,
        offsets: &mut [u64],
        data: &mut [u8],
    ) -> Result<(), Failure> {
        let (cells, field_type) = self.column(part, column)?;
        Self::check_varlen(field_type, column)?;
        let expected_offsets = cells
            .len()
            .checked_add(1)
            .ok_or_else(|| Failure::invalid_argument("offsets_len", "overflows"))?;
        if offsets.len() != expected_offsets {
            return Err(Failure::invalid_argument(
                "offsets_len",
                &format!(
                    "must be {expected_offsets} for this column, not {}",
                    offsets.len()
                ),
            ));
        }
        let mut written = 0_usize;
        cells.for_each_cell(column, |index, cell| {
            let offset = offsets
                .get_mut(index)
                .verified("the offsets were checked to hold one entry per cell and an end");
            *offset = written.arch_into();
            let Some(value) = Self::varlen(cell) else {
                return Ok(());
            };
            let end = written
                .checked_add(value.len())
                .verified("the column's values all lie inside one frame, whose length is a usize");
            let Some(target) = data.get_mut(written..end) else {
                return Err(Failure::invalid_argument(
                    "data_capacity",
                    "is smaller than the column's data",
                ));
            };
            target.copy_from_slice(value);
            written = end;
            Ok(())
        })?;
        let end = offsets
            .last_mut()
            .verified("the offsets were checked to hold at least the final end");
        *end = written.arch_into();
        Ok(())
    }

    /// Borrows one string or bytes value from the frame.
    pub fn cell_varlen(&self, part: Part, row: usize, column: usize) -> Result<&[u8], Failure> {
        let (cells, field_type) = self.column(part, column)?;
        Self::check_varlen(field_type, column)?;
        let cell = cells.cell(row, column)?;
        match Self::varlen(cell) {
            Some(value) => Ok(value),
            None => Err(Failure::new(
                FailureKind::Type,
                format!("row {row} of column {column} holds no value"),
            )),
        }
    }

    fn check_cells(
        cells: &PartCells<'_>,
        len: usize,
        argument: &'static str,
    ) -> Result<(), Failure> {
        if len != cells.len() {
            return Err(Failure::invalid_argument(
                argument,
                &format!("must be {} for this column, not {len}", cells.len()),
            ));
        }
        Ok(())
    }

    fn check_varlen(field_type: FieldType, column: usize) -> Result<(), Failure> {
        match field_type {
            FieldType::String | FieldType::Bytes => Ok(()),
            other => Err(Failure::new(
                FailureKind::Type,
                format!("column {column} holds {other:?}, not STRING or BYTES"),
            )),
        }
    }
}

/// # Safety
///
/// `event` is a live event this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_kind_of(event: *const Event) -> EventKind {
    // SAFETY: the header requires a live event.
    unsafe { abi::accessor(event) }.kind()
}

/// # Safety
///
/// `event` is a live event this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_subscription(
    event: *const Event,
    name: *mut *const u8,
    name_len: *mut usize,
    generation: *mut u64,
) {
    // SAFETY: the header requires a live event and writable out-parameters.
    unsafe {
        let subscription = abi::accessor(event).event.subscription();
        abi::write_bytes(name, name_len, subscription.name.as_str().as_bytes());
        abi::write(generation, subscription.generation.get());
    }
}

/// # Safety
///
/// `event` is a live event this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_row_count(event: *const Event) -> u64 {
    // SAFETY: the header requires a live event.
    unsafe { abi::accessor(event) }.row_count()
}

/// # Safety
///
/// `event` is a live reference to an event this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_retain(event: *mut Event) -> *mut Event {
    // SAFETY: the header requires a live reference, which `into_shared` or this function made
    // from an `Arc`. Wrapping it in `ManuallyDrop` leaves the caller's reference in place.
    let shared = ManuallyDrop::new(unsafe { Arc::from_raw(event.cast_const()) });
    Arc::into_raw(Arc::clone(&shared)).cast_mut()
}

/// # Safety
///
/// A non-null `event` is a reference this library returned that has not been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_release(event: *mut Event) {
    if event.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased reference, which `into_shared` or
    // `nx_event_retain` made from an `Arc`.
    drop(unsafe { Arc::from_raw(event.cast_const()) });
}

/// # Safety
///
/// `event` is a live event; a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_schema(
    event: *const Event,
    out: *mut *mut Schema,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_schema(event, out) })
}

/// # Safety
///
/// As [`nx_event_schema`].
unsafe fn write_schema(event: *const Event, out: *mut *mut Schema) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live event.
    let schema = unsafe { abi::handle(event, "event") }?.schema()?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(schema)) };
    Ok(())
}

/// # Safety
///
/// `event` is a live event; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_frame(
    event: *const Event,
    frame: *mut *const u8,
    frame_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_frame(event, frame, frame_len) })
}

/// # Safety
///
/// As [`nx_event_frame`].
unsafe fn write_frame(
    event: *const Event,
    frame: *mut *const u8,
    frame_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(frame, "frame")?;
    // SAFETY: the caller guarantees a live event.
    let bytes = unsafe { abi::handle(event, "event") }?.frame()?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe { abi::write_bytes(frame, frame_len, bytes) };
    Ok(())
}

/// # Safety
///
/// `event` is a live event and a non-null `states` addresses `states_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_column_states(
    event: *const Event,
    part: i32,
    column: usize,
    states: *mut u8,
    states_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_states(event, part, column, states, states_len) })
}

/// # Safety
///
/// As [`nx_event_column_states`].
unsafe fn write_states(
    event: *const Event,
    part: i32,
    column: usize,
    states: *mut u8,
    states_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live event and writable states.
    let (event, states) = unsafe {
        (
            abi::handle(event, "event")?,
            abi::output_slice(states, states_len, "states")?,
        )
    };
    event.column_states(Part::from_host(part)?, column, states)
}

/// # Safety
///
/// `event` is a live event and a non-null `values` addresses `values_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_column_fixed(
    event: *const Event,
    part: i32,
    column: usize,
    values: *mut std::ffi::c_void,
    values_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_fixed(event, part, column, values.cast::<u8>(), values_len) })
}

/// # Safety
///
/// As [`nx_event_column_fixed`].
unsafe fn write_fixed(
    event: *const Event,
    part: i32,
    column: usize,
    values: *mut u8,
    values_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live event and writable values.
    let (event, values) = unsafe {
        (
            abi::handle(event, "event")?,
            abi::output_slice(values, values_len, "values")?,
        )
    };
    event.column_fixed(Part::from_host(part)?, column, values)
}

/// # Safety
///
/// `event` is a live event; a non-null `offsets` addresses `offsets_len` writable entries, a
/// non-null `data` addresses `data_capacity` writable bytes, and a non-null `data_len` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_column_varlen(
    event: *const Event,
    part: i32,
    column: usize,
    offsets: *mut u64,
    offsets_len: usize,
    data: *mut u8,
    data_capacity: usize,
    data_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_varlen(
            event,
            part,
            column,
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
/// As [`nx_event_column_varlen`].
#[expect(
    clippy::too_many_arguments,
    reason = "the C ABI passes each caller buffer as a pointer and a length"
)]
unsafe fn write_varlen(
    event: *const Event,
    part: i32,
    column: usize,
    offsets: *mut u64,
    offsets_len: usize,
    data: *mut u8,
    data_capacity: usize,
    data_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(data_len, "data_len")?;
    // SAFETY: the caller guarantees a live event.
    let event = unsafe { abi::handle(event, "event") }?;
    let part = Part::from_host(part)?;
    let needed = event.column_varlen_len(part, column)?;
    // SAFETY: `data_len` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(data_len, needed) };
    if data.is_null() {
        return Ok(());
    }
    if data_capacity < needed {
        return Err(Failure::invalid_argument(
            "data_capacity",
            &format!("is {data_capacity} bytes, and the column needs {needed}"),
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
    event.column_varlen(part, column, offsets, data)
}

/// # Safety
///
/// `event` is a live event; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_cell_varlen(
    event: *const Event,
    part: i32,
    row: usize,
    column: usize,
    value: *mut *const u8,
    value_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_cell(event, part, row, column, value, value_len) })
}

/// # Safety
///
/// As [`nx_event_cell_varlen`].
unsafe fn write_cell(
    event: *const Event,
    part: i32,
    row: usize,
    column: usize,
    value: *mut *const u8,
    value_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(value, "value")?;
    // SAFETY: the caller guarantees a live event.
    let event = unsafe { abi::handle(event, "event") }?;
    let bytes = event.cell_varlen(Part::from_host(part)?, row, column)?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe { abi::write_bytes(value, value_len, bytes) };
    Ok(())
}
