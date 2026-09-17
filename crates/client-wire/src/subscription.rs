//! Subscription identity, the rows a subscription delivers, and the server's reports about its
//! delivery and end.

use std::{fmt, num::NonZeroU64};

use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::SubscriptionName;

use crate::{
    codec::{DecodeError, Decoder, EncodeError, EncodedUnion, Encoder, wire_enum},
    frame::{EncodedFrame, ServerFrame, VerifiedFrame},
    limits::{MIN_FRAME_BYTES, SessionLimits},
    row::{CellWriter, RowBatchView},
    server::finish_server_message,
    wire,
};

/// How a subscription represents the records it delivers. A subscription's type never changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionType {
    /// Typed rows against the schema sent when the subscription opens.
    Row,
}

wire_enum!(ALL_SUBSCRIPTION_TYPES: SubscriptionType => wire::SubscriptionType { Row });

/// One subscription within a session. A name reused after deletion has a new generation, so
/// messages about the earlier subscription cannot be taken for the later one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle {
    pub name: SubscriptionName,
    pub generation: NonZeroU64,
}

impl SubscriptionHandle {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::SubscriptionHandle<'fbb>>, Report<EncodeError>> {
        let name = encoder.text("SubscriptionHandle.name", self.name.as_str())?;
        Ok(wire::SubscriptionHandle::create(
            encoder.fbb(),
            &wire::SubscriptionHandleArgs {
                name: Some(name),
                generation: self.generation.get(),
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        handle: wire::SubscriptionHandle<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let name = decoder.name("SubscriptionHandle.name", handle.name())?;
        let generation = decoder.non_zero("SubscriptionHandle.generation", handle.generation())?;
        Ok(Self { name, generation })
    }
}

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

/// Writes one batch of a subscription's rows straight into a server frame.
///
/// Every row of a batch belongs to one concrete branch, chosen when the encoder is created. Rows
/// are checked against the frame limit as they are written, with room kept for the rest of the
/// frame, so a batch whose rows were accepted always finishes. A refused row leaves unreferenced
/// bytes behind that still fit the frame: finish the rows accepted before it, or discard the
/// encoder.
pub struct SubscriptionRowsEncoder {
    encoder: Encoder<'static>,
    subscription: SubscriptionHandle,
    handle: WIPOffset<wire::SubscriptionHandle<'static>>,
    branch_key: Option<WIPOffset<wire::BranchKey<'static>>>,
    rows: Vec<WIPOffset<wire::Row<'static>>>,
}

impl SubscriptionRowsEncoder {
    /// A batch of an unbranched relay.
    pub fn unbranched(
        subscription: SubscriptionHandle,
        limits: &SessionLimits,
    ) -> Result<Self, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let handle = subscription.encode(&mut encoder)?;
        Ok(Self {
            encoder,
            subscription,
            handle,
            branch_key: None,
            rows: Vec::new(),
        })
    }

    /// A batch of one concrete branch, whose key cells `write_key` writes in key order.
    pub fn branched(
        subscription: SubscriptionHandle,
        limits: &SessionLimits,
        write_key: impl FnOnce(&mut CellWriter<'_, 'static>) -> Result<(), Report<EncodeError>>,
    ) -> Result<Self, Report<EncodeError>> {
        let mut batch = Self::unbranched(subscription, limits)?;
        batch.encoder.keep_free(Self::free_bytes(1));
        let mut key = CellWriter::new(&mut batch.encoder, "BranchKey.cells", ROW_CELL_DEPTH);
        write_key(&mut key)?;
        let cells = key.finish()?;
        let branch_key = wire::BranchKey::create(
            batch.encoder.fbb(),
            &wire::BranchKeyArgs { cells: Some(cells) },
        );
        batch.encoder.within_limit("RowBatch.branch_key")?;
        batch.branch_key = Some(branch_key);
        Ok(batch)
    }

    /// Appends a row whose cells `write_cells` writes in field order.
    pub fn push_row(
        &mut self,
        write_cells: impl FnOnce(&mut CellWriter<'_, 'static>) -> Result<(), Report<EncodeError>>,
    ) -> Result<(), Report<EncodeError>> {
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
    pub fn rows(&self) -> usize {
        self.rows.len()
    }

    /// The bytes written so far, for deciding when to start the next batch.
    pub fn encoded_bytes(&self) -> usize {
        self.encoder.encoded_bytes()
    }

    pub fn finish(mut self) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        if self.rows.is_empty() {
            return Err(Report::new(EncodeError::EmptyCollection {
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
        let message = wire::SubscriptionRows::create(
            self.encoder.fbb(),
            &wire::SubscriptionRowsArgs {
                subscription: Some(self.handle),
                batch: Some(batch),
            },
        );
        finish_server_message(
            self.encoder,
            EncodedUnion::new(wire::ServerBody::SubscriptionRows, message),
        )
    }
}

impl fmt::Debug for SubscriptionRowsEncoder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubscriptionRowsEncoder")
            .field("subscription", &self.subscription)
            .field("branched", &self.branch_key.is_some())
            .field("rows", &self.rows.len())
            .field("encoded_bytes", &self.encoder.encoded_bytes())
            .finish()
    }
}

/// A batch of a subscription's rows, read in place from the frame that carried it.
///
/// It keeps that frame alive. A receiver that retains rows beyond the frame's lifetime copies the
/// cells it needs out of the views.
#[derive(Debug, Clone)]
pub struct SubscriptionRows {
    frame: VerifiedFrame<ServerFrame>,
    subscription: SubscriptionHandle,
}

impl SubscriptionRows {
    pub(crate) fn decode(
        frame: &VerifiedFrame<ServerFrame>,
        decoder: Decoder<'_>,
        rows: wire::SubscriptionRows<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let subscription = SubscriptionHandle::decode(decoder, rows.subscription())?;
        RowBatchView::check(decoder, rows.batch())?;
        Ok(Self {
            frame: frame.clone(),
            subscription,
        })
    }

    pub fn subscription(&self) -> &SubscriptionHandle {
        &self.subscription
    }

    pub fn batch(&self) -> RowBatchView<'_> {
        let rows = self
            .frame
            .root()
            .body_as_subscription_rows()
            .assured("this value is only decoded from a frame whose body is subscription rows");
        RowBatchView::checked(rows.batch())
    }

    /// The frame the rows are read from, and the bytes they keep alive.
    pub fn frame(&self) -> &VerifiedFrame<ServerFrame> {
        &self.frame
    }
}

/// A dropping subscription discarded rows its session could not take in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionDeliveryLost {
    pub subscription: SubscriptionHandle,
    pub dropped_rows: NonZeroU64,
}

impl SubscriptionDeliveryLost {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let subscription = self.subscription.encode(&mut encoder)?;
        let lost = wire::SubscriptionDeliveryLost::create(
            encoder.fbb(),
            &wire::SubscriptionDeliveryLostArgs {
                subscription: Some(subscription),
                dropped_rows: self.dropped_rows.get(),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::SubscriptionDeliveryLost, lost),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        lost: wire::SubscriptionDeliveryLost<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let subscription = SubscriptionHandle::decode(decoder, lost.subscription())?;
        let dropped_rows =
            decoder.non_zero("SubscriptionDeliveryLost.dropped_rows", lost.dropped_rows())?;
        Ok(Self {
            subscription,
            dropped_rows,
        })
    }
}

/// Why rows of an open subscription were skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowsSkippedCause {
    /// The subscription filter failed to evaluate.
    FilterFailed,
    /// The domain's execution time was unavailable to the filter.
    DomainTimeUnavailable,
    /// The rows could not be encoded against the subscription schema.
    EncodingFailed,
}

wire_enum!(ALL_ROWS_SKIPPED_CAUSES: RowsSkippedCause => wire::RowsSkippedCause {
    FilterFailed,
    DomainTimeUnavailable,
    EncodingFailed,
});

/// Rows were skipped. The subscription stays open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionRowsSkipped {
    pub subscription: SubscriptionHandle,
    pub cause: RowsSkippedCause,
    pub skipped_rows: NonZeroU64,
    pub message: String,
}

impl SubscriptionRowsSkipped {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let subscription = self.subscription.encode(&mut encoder)?;
        let message = encoder.text("SubscriptionRowsSkipped.message", &self.message)?;
        let skipped = wire::SubscriptionRowsSkipped::create(
            encoder.fbb(),
            &wire::SubscriptionRowsSkippedArgs {
                subscription: Some(subscription),
                cause: Some(self.cause.into()),
                skipped_rows: self.skipped_rows.get(),
                message: Some(message),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::SubscriptionRowsSkipped, skipped),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        skipped: wire::SubscriptionRowsSkipped<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let subscription = SubscriptionHandle::decode(decoder, skipped.subscription())?;
        let cause =
            decoder.required_enumeration("SubscriptionRowsSkipped.cause", skipped.cause())?;
        let skipped_rows = decoder.non_zero(
            "SubscriptionRowsSkipped.skipped_rows",
            skipped.skipped_rows(),
        )?;
        let message = decoder.text("SubscriptionRowsSkipped.message", skipped.message())?;
        Ok(Self {
            subscription,
            cause,
            skipped_rows,
            message,
        })
    }
}

/// Why the server closed a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionEndReason {
    /// The relay was rebuilt, stopped or removed. Subscribe again against the current schema.
    RelayClosed,
}

wire_enum!(ALL_SUBSCRIPTION_END_REASONS: SubscriptionEndReason => wire::SubscriptionEndReason {
    RelayClosed,
});

/// The server closed the subscription. No further rows follow for its generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionEnded {
    pub subscription: SubscriptionHandle,
    pub reason: SubscriptionEndReason,
    pub message: String,
}

impl SubscriptionEnded {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let subscription = self.subscription.encode(&mut encoder)?;
        let message = encoder.text("SubscriptionEnded.message", &self.message)?;
        let ended = wire::SubscriptionEnded::create(
            encoder.fbb(),
            &wire::SubscriptionEndedArgs {
                subscription: Some(subscription),
                reason: Some(self.reason.into()),
                message: Some(message),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::SubscriptionEnded, ended),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        ended: wire::SubscriptionEnded<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let subscription = SubscriptionHandle::decode(decoder, ended.subscription())?;
        let reason = decoder.required_enumeration("SubscriptionEnded.reason", ended.reason())?;
        let message = decoder.text("SubscriptionEnded.message", ended.message())?;
        Ok(Self {
            subscription,
            reason,
            message,
        })
    }
}
