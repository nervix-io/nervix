//! Independently verified Row frames embedded in state inspection sections.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Bounded state Row frame construction and borrowed access to verified cells.
//! - **Depends on.** The shared Row batch builder, frame verification and schema conformance.
//! - **Must not know.** Subscriptions, graph state, snapshots or owner routing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the boundary declared above")
)]

use bytes::Bytes;
use error_stack::{Report, ResultExt as _};

use crate::{
    CellWriter, RowBatchView, RowSchema, SessionLimits, WireDecodeError, WireEncodeError,
    codec::{Decoder, Encoder},
    frame::{StateRowRoot, VerifiedFrame},
    row_batch::RowBatchBuilder,
};

/// Writes one bounded batch of a single concrete branch without a subscription identity.
pub struct StateRowsEncoder {
    batch: RowBatchBuilder,
}

impl StateRowsEncoder {
    pub fn unbranched(limits: &SessionLimits) -> Self {
        Self {
            batch: RowBatchBuilder::new(Encoder::new(limits.transfer_bytes(), limits)),
        }
    }

    pub fn branched(
        limits: &SessionLimits,
        write_key: impl FnOnce(&mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>>,
    ) -> Result<Self, Report<WireEncodeError>> {
        let mut encoder = Self::unbranched(limits);
        encoder.batch.branch_key(write_key)?;
        Ok(encoder)
    }

    pub fn push_row(
        &mut self,
        write_cells: impl FnOnce(&mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>>,
    ) -> Result<(), Report<WireEncodeError>> {
        self.batch.push_row(write_cells)
    }

    pub fn rows(&self) -> usize {
        self.batch.rows()
    }

    pub fn finish(self, limits: &SessionLimits) -> Result<StateRowFrame, Report<WireEncodeError>> {
        let (encoder, batch) = self.batch.finish()?;
        let encoded = encoder.finish::<StateRowRoot>(batch)?;
        StateRowFrame::from_bytes(encoded.into_bytes(), limits).change_context(
            WireEncodeError::InvalidValue {
                field: "StateRowFrame",
                kind: "verified Row batch",
            },
        )
    }
}

/// Immutable backing storage for positional typed Row cells. Cloning shares its bytes.
#[derive(Debug, Clone)]
pub struct StateRowFrame {
    frame: VerifiedFrame<StateRowRoot>,
}

impl PartialEq for StateRowFrame {
    fn eq(&self, other: &Self) -> bool {
        self.bytes() == other.bytes()
    }
}

impl Eq for StateRowFrame {}

impl StateRowFrame {
    pub fn from_bytes(
        bytes: Bytes,
        limits: &SessionLimits,
    ) -> Result<Self, Report<WireDecodeError>> {
        let frame =
            VerifiedFrame::<StateRowRoot>::verify_within(bytes, limits.transfer_bytes(), limits)
                .change_context(WireDecodeError::InvalidValue {
                    field: "StateRowFrame",
                    kind: "Row frame",
                })?;
        RowBatchView::check(Decoder::new(limits), frame.root())?;
        Ok(Self { frame })
    }

    pub fn bytes(&self) -> &Bytes {
        self.frame.bytes()
    }

    pub fn batch(&self) -> RowBatchView<'_> {
        RowBatchView::checked(self.frame.root())
    }

    pub fn conform(&self, schema: &RowSchema) -> Result<(), Report<WireDecodeError>> {
        self.batch()
            .conform(schema)
            .change_context(WireDecodeError::InvalidValue {
                field: "StateRowFrame",
                kind: "rows conforming to the section schema",
            })
    }
}
