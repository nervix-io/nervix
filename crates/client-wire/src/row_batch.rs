//! Shared bounded construction of typed Row batches.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Row-cell writing, branch-key encoding and exact frame-growth reservations.
//! - **Depends on.** The bounded FlatBuffers encoder and shared Row tables.
//! - **Must not know.** Subscription handles, state owners or how a caller frames a completed batch.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the boundary declared above")
)]

use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;

use crate::{
    codec::{Encoder, WireEncodeError},
    limits::MIN_FRAME_BYTES,
    row::CellWriter,
    wire,
};

/// The depth of a row's or branch key's cell tables in a rows frame: the server message, the rows
/// event, the batch, the row or branch key, and the cell.
const ROW_CELL_DEPTH: usize = 5;

/// The bytes a rows frame keeps free for what it writes once the last row is accepted: the rows
/// vector's length prefix, the batch, rows event and server message tables with their vtables, the
/// root offset and identifier, and alignment padding, with room for the one row table a refused
/// row writes before it is checked. The subscription handle and branch key are written first and
/// counted as they are written. A test fills batches up to the frame limit and finishes them.
const ROWS_ENVELOPE_BYTES: usize = 256;

/// The offset each row adds to the batch's rows vector.
const ROW_OFFSET_BYTES: usize = 4;

const _: () = assert!(ROWS_ENVELOPE_BYTES < MIN_FRAME_BYTES);

pub(crate) struct RowBatchBuilder {
    encoder: Encoder<'static>,
    branch_key: Option<WIPOffset<wire::BranchKey<'static>>>,
    rows: Vec<WIPOffset<wire::Row<'static>>>,
}

impl RowBatchBuilder {
    pub(crate) fn new(encoder: Encoder<'static>) -> Self {
        Self {
            encoder,
            branch_key: None,
            rows: Vec::new(),
        }
    }

    pub(crate) fn branch_key(
        &mut self,
        write_key: impl FnOnce(&mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>>,
    ) -> Result<(), Report<WireEncodeError>> {
        self.encoder.keep_free(Self::free_bytes(1));
        let mut key = CellWriter::new(&mut self.encoder, "BranchKey.cells", ROW_CELL_DEPTH);
        write_key(&mut key)?;
        let cells = key.finish()?;
        let branch_key = wire::BranchKey::create(
            self.encoder.fbb(),
            &wire::BranchKeyArgs { cells: Some(cells) },
        );
        self.encoder.within_limit("RowBatch.branch_key")?;
        self.branch_key = Some(branch_key);
        Ok(())
    }
    /// Appends a row whose cells `write_cells` writes in field order.
    pub(crate) fn push_row(
        &mut self,
        write_cells: impl FnOnce(&mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>>,
    ) -> Result<(), Report<WireEncodeError>> {
        let rows = self
            .rows
            .len()
            .checked_add(1)
            .assured("a vector of four-byte offsets holds at most isize::MAX / 4 entries");
        self.encoder.entries("RowBatch.rows", rows)?;
        self.encoder.keep_free(Self::free_bytes(rows));
        let mut cells = CellWriter::new(&mut self.encoder, "Row.cells", ROW_CELL_DEPTH);
        write_cells(&mut cells)?;
        let cells = cells.finish()?;
        let row = wire::Row::create(self.encoder.fbb(), &wire::RowArgs { cells: Some(cells) });
        self.encoder.within_limit("RowBatch.rows")?;
        self.rows.push(row);
        Ok(())
    }

    /// The bytes the frame keeps free while `rows` rows are written: their offsets and the
    /// envelope.
    fn free_bytes(rows: usize) -> usize {
        let offsets = rows.checked_mul(ROW_OFFSET_BYTES).assured(
            "the row count is at most one past the length of an in-memory vector of four-byte \
             offsets, whose byte size is at most isize::MAX",
        );
        offsets
            .checked_add(ROWS_ENVELOPE_BYTES)
            .assured("the offsets take at most isize::MAX bytes, leaving room for the envelope")
    }

    /// The rows written so far.
    pub(crate) fn is_branched(&self) -> bool {
        self.branch_key.is_some()
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows.len()
    }

    /// The bytes written so far, for deciding when to start the next batch.
    pub(crate) fn encoded_bytes(&self) -> usize {
        self.encoder.encoded_bytes()
    }

    pub(crate) fn finish(
        mut self,
    ) -> Result<(Encoder<'static>, WIPOffset<wire::RowBatch<'static>>), Report<WireEncodeError>>
    {
        if self.rows.is_empty() {
            return Err(Report::new(WireEncodeError::EmptyCollection {
                field: "RowBatch.rows",
            }));
        }
        self.encoder.keep_free(0);
        let rows = self.encoder.tables("RowBatch.rows", &self.rows)?;
        let batch = wire::RowBatch::create(
            self.encoder.fbb(),
            &wire::RowBatchArgs {
                branch_key: self.branch_key,
                rows: Some(rows),
            },
        );
        Ok((self.encoder, batch))
    }
}
