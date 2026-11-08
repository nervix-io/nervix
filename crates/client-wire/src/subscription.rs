//! Subscription identity, the rows a subscription delivers, and the server's reports about its
//! delivery and end.

use std::{fmt, num::NonZeroU64};

use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::SubscriptionName;

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    frame::{EncodedFrame, ServerFrame, VerifiedFrame},
    limits::SessionLimits,
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
    ) -> Result<WIPOffset<wire::SubscriptionHandle<'fbb>>, Report<WireEncodeError>> {
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
    ) -> Result<Self, Report<WireDecodeError>> {
        let name = decoder.name("SubscriptionHandle.name", handle.name())?;
        let generation = decoder.non_zero("SubscriptionHandle.generation", handle.generation())?;
        Ok(Self { name, generation })
    }
}

/// Writes one batch of a subscription's rows straight into a server frame.
///
/// Every row of a batch belongs to one concrete branch, chosen when the encoder is created. Rows
/// are checked against the frame limit as they are written, with room kept for the rest of the
/// frame, so a batch whose rows were accepted always finishes. A refused row leaves unreferenced
/// bytes behind that still fit the frame: finish the rows accepted before it, or discard the
/// encoder.
pub struct SubscriptionRowsEncoder {
    batch: crate::row_batch::RowBatchBuilder,
    subscription: SubscriptionHandle,
    handle: WIPOffset<wire::SubscriptionHandle<'static>>,
}

impl SubscriptionRowsEncoder {
    /// A batch of an unbranched relay.
    pub fn unbranched(
        subscription: SubscriptionHandle,
        limits: &SessionLimits,
    ) -> Result<Self, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let handle = subscription.encode(&mut encoder)?;
        Ok(Self {
            batch: crate::row_batch::RowBatchBuilder::new(encoder),
            subscription,
            handle,
        })
    }

    /// A batch of one concrete branch, whose key cells `write_key` writes in key order.
    pub fn branched(
        subscription: SubscriptionHandle,
        limits: &SessionLimits,
        write_key: impl FnOnce(&mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>>,
    ) -> Result<Self, Report<WireEncodeError>> {
        let mut batch = Self::unbranched(subscription, limits)?;
        batch.batch.branch_key(write_key)?;
        Ok(batch)
    }

    /// Appends a row whose cells `write_cells` writes in field order.
    pub fn push_row(
        &mut self,
        write_cells: impl FnOnce(&mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>>,
    ) -> Result<(), Report<WireEncodeError>> {
        self.batch.push_row(write_cells)
    }

    /// The rows written so far.
    pub fn rows(&self) -> usize {
        self.batch.rows()
    }

    /// The bytes written so far, for deciding when to start the next batch.
    pub fn encoded_bytes(&self) -> usize {
        self.batch.encoded_bytes()
    }

    pub fn finish(self) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
        let (mut encoder, batch) = self.batch.finish()?;
        let message = wire::SubscriptionRows::create(
            encoder.fbb(),
            &wire::SubscriptionRowsArgs {
                subscription: Some(self.handle),
                batch: Some(batch),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::SubscriptionRows, message),
        )
    }
}

impl fmt::Debug for SubscriptionRowsEncoder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubscriptionRowsEncoder")
            .field("subscription", &self.subscription)
            .field("branched", &self.batch.is_branched())
            .field("rows", &self.batch.rows())
            .field("encoded_bytes", &self.batch.encoded_bytes())
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
    ) -> Result<Self, Report<WireDecodeError>> {
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
    ) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
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
    ) -> Result<Self, Report<WireDecodeError>> {
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
    ) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
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
    ) -> Result<Self, Report<WireDecodeError>> {
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

/// Why the server ended a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionEndReason {
    /// The relay, or the domain that held it, no longer exists.
    RelayRemoved,
    /// The relay was redefined, or removed and declared again, so the schema the subscription
    /// announced no longer describes its rows. Subscribe again to read them under the current one.
    RelayChanged,
}

wire_enum!(ALL_SUBSCRIPTION_END_REASONS: SubscriptionEndReason => wire::SubscriptionEndReason {
    RelayRemoved,
    RelayChanged,
});

impl fmt::Display for SubscriptionEndReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RelayRemoved => formatter.write_str("its relay no longer exists"),
            Self::RelayChanged => formatter.write_str("its relay was redefined"),
        }
    }
}

/// The server ended the subscription. It is the last frame about its generation.
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
    ) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
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
    ) -> Result<Self, Report<WireDecodeError>> {
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
