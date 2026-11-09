//! The state charge that bounds a branch-local window before admission.
//!
//! Layer: data plane.
//! - **Owns.** Counting retained Arrow allocations, row handles, branch keys and reserved sketch
//!   panes under the window's existing MAX STATE SIZE accounting rule.
//! - **Depends on.** Prepared window plans, Arrow row views and evaluated argument columns.
//! - **Must not know.** Models, Plexus, placement, snapshots, acknowledgement policy or rendering.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns window state accounting")
)]

use std::collections::BTreeSet;

use ahash::{HashSet, HashSetExt as _};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::Arc;

use super::{
    RelayMessage, WindowAccumulatorPlan, WindowArgumentColumns, WindowProcessorError,
    window_processor::message_timestamp,
};
use crate::runtime_schema::RuntimeRecordBatch;

/// One admission's complete accounting cut, sharing each Arrow allocation's charge once.
pub(super) struct WindowStateCharge<'plan> {
    plan: &'plan WindowAccumulatorPlan,
    required: u128,
    panes: BTreeSet<i64>,
    charged_arguments: HashSet<*const WindowArgumentColumns>,
    charged_batches: HashSet<*const RuntimeRecordBatch>,
}

impl<'plan> WindowStateCharge<'plan> {
    pub(super) fn new(plan: &'plan WindowAccumulatorPlan) -> Self {
        Self {
            plan,
            required: 1024,
            panes: BTreeSet::new(),
            charged_arguments: HashSet::new(),
            charged_batches: HashSet::new(),
        }
    }

    pub(super) fn admit(&mut self, message: &RelayMessage, arguments: &Arc<WindowArgumentColumns>) {
        self.required = self
            .required
            .checked_add(Self::admission_bytes(
                message,
                arguments,
                &mut self.charged_arguments,
                &mut self.charged_batches,
            ))
            .assured("addressable retained or incoming allocations fit u128");
        if let Some(layout) = self.plan.sketch_layout {
            self.panes
                .insert(layout.pane_of(message_timestamp(message).unix_nanos()));
        }
    }

    pub(super) fn finish(mut self) -> error_stack::Result<u128, WindowProcessorError> {
        if let Some(layout) = self.plan.sketch_layout {
            let panes = self.panes.len();
            if u64::try_from(panes).assured("addressable pane count fits u64")
                > layout.maximum_panes
            {
                return Err(Report::new(WindowProcessorError::PaneLimitExceeded {
                    panes,
                    limit: layout.maximum_panes,
                }));
            }
            let mut reserved = 0_u128;
            for sketch in self
                .plan
                .demands()
                .iter()
                .filter_map(|demand| demand.sketch)
            {
                reserved = reserved
                    .checked_add(sketch.reserved_bytes())
                    .assured("addressable sketch demands fit a u128 byte allowance");
            }
            // Emission merges the retained panes into one more sketch while they remain live.
            let pane_allocations = u128::try_from(panes)
                .assured("addressable pane count fits u128")
                .checked_add(1)
                .assured("addressable panes and one merged sketch fit u128");
            self.required = self
                .required
                .checked_add(
                    reserved
                        .checked_mul(pane_allocations)
                        .assured("configured sketch bounds fit u128"),
                )
                .assured("configured sketch bounds and addressable rows fit u128");
        }
        Ok(self.required)
    }

    fn admission_bytes(
        message: &RelayMessage,
        arguments: &Arc<WindowArgumentColumns>,
        charged_arguments: &mut HashSet<*const WindowArgumentColumns>,
        charged_batches: &mut HashSet<*const RuntimeRecordBatch>,
    ) -> u128 {
        // A restored snapshot may hold each retained input row in its own one-row Arrow batch.
        // Reserve array and batch headers per row so an admitted branch also fits after handoff.
        let record_columns = u128::try_from(message.record.batch().batch().num_columns())
            .assured("an addressable Arrow schema has fewer than 2^128 fields");
        let row_handles = record_columns
            .checked_add(1)
            .assured("an addressable Arrow schema's field count fits u128");
        let row_headers = row_handles
            .checked_mul(256)
            .assured("an addressable Arrow schema's row headers fit u128");
        let mut bytes = row_headers;
        if charged_arguments.insert(Arc::as_ptr(arguments)) {
            bytes = bytes
                .checked_add(arguments.allocated_bytes())
                .assured("addressable Arrow argument allocations fit u128");
        }
        let batch = message.record.batch();
        if charged_batches.insert(Arc::as_ptr(batch)) {
            let mut actual = 0_u128;
            let mut logical = 0_u128;
            for column in batch.batch().columns() {
                actual = actual
                    .checked_add(
                        u128::try_from(column.get_array_memory_size())
                            .assured("an addressable Arrow allocation fits u128"),
                    )
                    .assured("addressable Arrow record allocations fit u128");
                logical = logical
                    .checked_add(
                        u128::try_from(
                            column
                                .to_data()
                                .get_slice_memory_size()
                                .assured("a validated Arrow record has bounded slice memory"),
                        )
                        .assured("an addressable Arrow slice fits u128"),
                    )
                    .assured("addressable Arrow record slices fit u128");
            }
            let rows = u128::try_from(batch.batch().num_rows())
                .assured("an addressable Arrow batch length fits u128");
            let columns = u128::try_from(batch.batch().num_columns())
                .assured("an addressable Arrow schema has fewer than 2^128 fields");
            let elements = rows
                .checked_mul(columns)
                .assured("an addressable Arrow batch's element count fits u128");
            let headers = elements
                .checked_mul(128)
                .assured("an addressable Arrow batch's array headers fit u128");
            let payload = logical
                .checked_mul(2)
                .assured("an addressable Arrow batch's doubled payload fits u128");
            let estimated = payload
                .checked_add(headers)
                .assured("an addressable Arrow batch's restored shape fits u128");
            let batch_bytes = actual.max(estimated);
            bytes = bytes
                .checked_add(batch_bytes)
                .assured("addressable Arrow record allocations fit u128");
        }
        let key_bytes = u128::try_from(format!("{:?}", message.key).len())
            .assured("an addressable branch key fits u128");
        bytes
            .checked_add(key_bytes)
            .assured("addressable branch state fits u128")
    }
}
