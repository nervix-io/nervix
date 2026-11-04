//! A subscription event a host reads, and bulk access to the rows it carries.
//!
//! - **Owns.** The event kinds and cell states a host reads, copying or borrowing the cells of a
//!   batch column by column, and building a rows event's rows as one batch of its row fields.
//! - **Depends on.** The Rust client's subscription events, the wire contract's row views, the
//!   schema, and the batch builder.
//! - **Must not know.** How events are routed or restored; the session decides that.
//!
//! A rows event owns the verified frame it was decoded from and nothing else, so holding one
//! keeps exactly that frame alive. Every accessor reads the frame through the wire contract's
//! checked views; a batch is held to its schema once, when the event is created, which is why
//! reading a cell the schema describes cannot fail.

use arch_into::ArchInto as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{
    SubscriptionEvent,
    wire::{CellView, CellsView, RowBatchView},
};
use nervix_models::{ParseAsType, SchemaField};

use crate::{
    batch::Batch,
    batch_builder::BatchBuilder,
    failure::{Failure, FailureKind},
    schema::{FieldType, Part, Schema, TypeLevels},
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

    /// The cells of `column` of `part`, in row order: one per row, or the one cell of the branch
    /// key.
    pub fn cells(&self, part: Part, column: usize) -> Result<Vec<CellView<'_>>, Failure> {
        let (cells, _) = self.column(part, column)?;
        let mut collected = Vec::with_capacity(cells.len());
        cells.for_each_cell(column, |_, cell| {
            collected.push(cell);
            Ok(())
        })?;
        Ok(collected)
    }

    /// The bytes a fixed-width column takes in native byte order: one width per cell.
    pub fn fixed_len(&self, part: Part, column: usize) -> Result<usize, Failure> {
        let (cells, field_type) = self.column(part, column)?;
        let Some(width) = field_type.fixed_width() else {
            return Err(Failure::new(
                FailureKind::Type,
                format!("column {column} holds {field_type:?}, which is not fixed-width"),
            ));
        };
        cells
            .len()
            .checked_mul(width)
            .ok_or_else(|| Failure::invalid_argument("column", "overflows"))
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

/// One list level of a rows event's column, laid out the way a host builds a batch.
enum ListLevel {
    /// Variable-length lists: their offsets into the next level, starting at zero.
    Variable(Vec<u64>),
    /// Fixed-size lists, whose element count the type gives, so they have no offsets.
    Fixed,
}

impl ListLevel {
    /// The offsets of a variable-length level.
    fn offsets_mut(&mut self) -> Option<&mut Vec<u64>> {
        match self {
            Self::Variable(offsets) => Some(offsets),
            Self::Fixed => None,
        }
    }
}

/// One column of a rows event laid out the way a host builds a batch: its row states, every list
/// level, and the values of its innermost level.
struct ColumnLevels {
    states: Vec<u8>,
    /// The list levels of the column's type, from the outside in.
    lists: Vec<ListLevel>,
    fixed: Vec<u8>,
    varlen_offsets: Vec<u64>,
    varlen: Vec<u8>,
}

impl ColumnLevels {
    /// The empty levels of a column of type `ty`.
    fn of(ty: &ParseAsType) -> Self {
        let mut lists = Vec::new();
        let mut current = ty;
        loop {
            match current {
                ParseAsType::Vec { element } => {
                    lists.push(ListLevel::Variable(vec![0]));
                    current = element;
                }
                ParseAsType::Array { element, .. } => {
                    lists.push(ListLevel::Fixed);
                    current = element;
                }
                _ => break,
            }
        }
        Self {
            states: Vec::new(),
            lists,
            fixed: Vec::new(),
            varlen_offsets: vec![0],
            varlen: Vec::new(),
        }
    }

    /// Lays out one cell of `level`, whose type is `ty`. A cell without a value — a null or
    /// withheld row — is an empty list, a fixed-size list of placeholder elements, or a zero
    /// value, which only its row's state marks.
    fn push(&mut self, level: usize, ty: &ParseAsType, cell: Option<CellView<'_>>) {
        let next = level
            .checked_add(1)
            .assured("a type nests fewer lists than a usize counts");
        match ty {
            ParseAsType::Vec { element } => {
                let mut items = 0_usize;
                if let Some(CellView::List(elements)) = cell {
                    for item in elements.iter() {
                        self.push(next, element, Some(item));
                        items = items.checked_add(1).assured(
                            "a list's elements lie inside one frame, whose length is a usize",
                        );
                    }
                }
                let list = self
                    .lists
                    .get_mut(level)
                    .assured("every list level of the type was laid out");
                let offsets = list
                    .offsets_mut()
                    .assured("a variable-length level of the type was laid out with offsets");
                let last = *offsets.last().assured("a level's offsets start with zero");
                let items: u64 = items.arch_into();
                let end = last
                    .checked_add(items)
                    .assured("a level's elements lie inside one frame, whose length is a usize");
                offsets.push(end);
            }
            ParseAsType::Array { element, len } => {
                if let Some(CellView::List(elements)) = cell {
                    for item in elements.iter() {
                        self.push(next, element, Some(item));
                    }
                    return;
                }
                for _ in 0..len.get() {
                    self.push(next, element, None);
                }
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
            | ParseAsType::Bytes => self.push_value(ty, cell),
        }
    }

    /// Lays out one value of a scalar level: its native-order bytes, or its text.
    fn push_value(&mut self, ty: &ParseAsType, cell: Option<CellView<'_>>) {
        match (FieldType::from(ty).fixed_width(), cell) {
            (Some(width), Some(cell)) => {
                let start = self.fixed.len();
                let end = start
                    .checked_add(width)
                    .assured("a column's values lie inside one frame, whose length is a usize");
                self.fixed.resize(end, 0);
                let target = self
                    .fixed
                    .get_mut(start..end)
                    .verified("the values were just extended by one width");
                Event::write_fixed(cell, target)
                    .verified("a conforming cell of a fixed-width field holds a fixed-width value");
            }
            (Some(width), None) => {
                let end = self
                    .fixed
                    .len()
                    .checked_add(width)
                    .assured("a column's values lie inside one frame, whose length is a usize");
                self.fixed.resize(end, 0);
            }
            (None, cell) => {
                if let Some(value) = cell.and_then(Event::varlen) {
                    self.varlen.extend_from_slice(value);
                }
                self.varlen_offsets.push(self.varlen.len().arch_into());
            }
        }
    }
}

impl Event {
    /// A rows event's rows as one batch of its row fields. A sensitive field may hold cells the
    /// server withheld, so it is nullable in the batch, and a withheld cell is null there;
    /// `column_states` tells it apart from a null.
    pub fn rows_batch(&self) -> Result<Batch, Failure> {
        let schema = self.schema()?;
        let fields: Vec<SchemaField> = schema
            .fields(Part::Rows)
            .iter()
            .map(|field| SchemaField {
                optional: field.optional || field.sensitive,
                ..field.clone()
            })
            .collect();
        let rows = usize::try_from(self.row_count())
            .assured("a rows event's rows were counted from a usize");
        let mut builder = BatchBuilder::new(Schema::of_fields(fields.clone()), rows);
        for (column, field) in fields.iter().enumerate() {
            let (cells, _) = self.column(Part::Rows, column)?;
            let mut levels = ColumnLevels::of(&field.ty);
            cells.for_each_cell(column, |_, cell| {
                let value = match cell {
                    CellView::Null | CellView::Redacted => None,
                    _ => Some(cell),
                };
                let state = match value {
                    Some(_) => CellState::Value,
                    None => CellState::Null,
                };
                levels.states.push(u8::from(state));
                levels.push(0, &field.ty, value);
                Ok(())
            })?;
            builder.states(column, &levels.states)?;
            for (level, list) in levels.lists.iter().enumerate() {
                if let ListLevel::Variable(offsets) = list {
                    builder.offsets(column, level, offsets)?;
                }
            }
            let innermost = levels.lists.len();
            let scalar = TypeLevels::of(&field.ty).level(innermost)?;
            match FieldType::from(scalar).fixed_width() {
                Some(_) => builder.fixed(column, innermost, &levels.fixed)?,
                None => {
                    builder.varlen(column, innermost, &levels.varlen_offsets, &levels.varlen)?
                }
            }
        }
        builder.finish()
    }
}
