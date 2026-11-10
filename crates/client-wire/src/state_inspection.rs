//! Typed, bounded state inspection at the session boundary.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Inspection identities, complete counts, bounded Row sections and typed refusals.
//! - **Depends on.** Shared vocabulary identities and verified Row frames.
//! - **Must not know.** Graph execution, snapshot storage, owner selection or Plexus parsing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the boundary declared above")
)]

use nervix_models::{ClusterNodeName, Unit};
pub use nervix_models::{
    StateInspectionCounts, StateInspectionEntity, StateInspectionOrigin, StateInspectionRejection,
    StateInspectionStage,
};

use crate::{RowSchema, StateRowFrame};
mod codec;
mod presentation;

/// One read, with either a page from its captured generations or a typed refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateInspection {
    pub entity: StateInspectionEntity,
    pub result: StateInspectionResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateInspectionResult {
    Page(StateInspectionPage),
    Rejected(StateInspectionRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateInspectionPage {
    pub counts: StateInspectionCounts,
    pub unit_count: u64,
    pub units: Vec<StateInspectionUnit>,
    pub continuation: Option<String>,
}

/// One execution unit's exact owner, replicas and captured read origin.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StateInspectionUnit {
    pub unit: Unit,
    pub owner: ClusterNodeName,
    pub replicas: Vec<ClusterNodeName>,
    pub origin: StateInspectionOrigin,
    pub counts: StateInspectionCounts,
    pub sections: Vec<StateInspectionSection>,
}

/// A section's frames retain typed cells and their independently verified backing bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateInspectionSection {
    pub name: String,
    pub schema: RowSchema,
    pub frames: Vec<StateRowFrame>,
    pub omitted_rows: u64,
    pub watermarks: Option<StateWatermarkColumns>,
}

/// The watermark columns are identified positionally, so payload field names never collide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct StateWatermarkColumns {
    pub low: u32,
    pub high: u32,
}

#[cfg(test)]
pub(crate) mod tests;

impl StateInspectionSection {
    fn valid_watermarks(&self) -> bool {
        valid_watermark_columns(&self.schema, self.watermarks)
    }
}

fn valid_watermark_columns(schema: &RowSchema, columns: Option<StateWatermarkColumns>) -> bool {
    let Some(columns) = columns else {
        return true;
    };
    if columns.low == columns.high {
        return false;
    }
    [columns.low, columns.high].into_iter().all(|index| {
        let Ok(index) = usize::try_from(index) else {
            return false;
        };
        let Some(field) = schema.fields.get(index) else {
            return false;
        };
        field.ty == nervix_models::ParseAsType::Datetime && !field.optional && !field.sensitive
    })
}
