//! A subscription event a host reads, and bulk access to the rows it carries.
//!
//! - **Owns.** The event kinds and cell states a host reads, and copying or borrowing the cells of
//!   a batch column by column.
//! - **Depends on.** The Rust client's subscription events, the wire contract's row views, and
//!   the schema.
//! - **Must not know.** How events are routed or restored; the session decides that.
//!
//! A rows event owns the verified frame it was decoded from and nothing else, so holding one
//! keeps exactly that frame alive. Every accessor reads the frame through the wire contract's
//! checked views; a batch is held to its schema once, when the event is created, which is why
//! reading a cell the schema describes cannot fail.

use arch_into::ArchInto as _;
use meticulous::OptionExt as _;
use nervix_client_core::{
    SubscriptionEvent,
    wire::{CellView, CellsView, RowBatchView},
};

use crate::{
    failure::{Failure, FailureKind},
    schema::{FieldType, Part, Schema},
};

/// What a subscription event reports, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum EventKind {
    Rows = 1,
    DeliveryLost = 2,
    RowsSkipped = 3,
    Ended = 4,
    Interrupted = 5,
    ConsumerOverflow = 6,
    RestorationFailed = 7,
}

/// What a cell holds, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellState {
    Value,
    Null,
    Redacted,
}

impl CellState {
    /// Reads a state a host passed as a byte.
    pub fn from_host(state: u8) -> Result<Self, Failure> {
        match state {
            1 => Ok(Self::Value),
            2 => Ok(Self::Null),
            3 => Ok(Self::Redacted),
            _ => Err(Failure::invalid_argument(
                "states",
                "holds a byte that is not an nx_cell_state",
            )),
        }
    }
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
    pub fn new(event: SubscriptionEvent) -> Result<Self, Failure> {
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

    pub fn kind(&self) -> EventKind {
        match &self.event {
            SubscriptionEvent::Rows(_) => EventKind::Rows,
            SubscriptionEvent::DeliveryLost(_) => EventKind::DeliveryLost,
            SubscriptionEvent::RowsSkipped(_) => EventKind::RowsSkipped,
            SubscriptionEvent::Ended(_) => EventKind::Ended,
            SubscriptionEvent::Interrupted(_) => EventKind::Interrupted,
            SubscriptionEvent::RestorationFailed(_) => EventKind::RestorationFailed,
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
            | SubscriptionEvent::RestorationFailed(_)
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
            | SubscriptionEvent::RestorationFailed(_)
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
