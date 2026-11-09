//! A host-owned state inspection and its typed Row frames.
//!
//! Layer: edges.
//! - **Owns.** Host access to the typed envelope, its lossless JSON and retained section frames.
//! - **Depends on.** The verified wire representation and shared schema handle.
//! - **Must not know.** Commands, routing, state storage or display-message parsing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the boundary declared above")
)]

use arch_into::ArchInto as _;
use nervix_client_core::wire::{
    StateInspection, StateInspectionCounts, StateInspectionResult, StateInspectionSection,
    StateInspectionUnit, StateRowFrame,
};
pub use nervix_client_core::wire::{
    StateInspectionRejection as InspectionRejection, StateInspectionStage as InspectionStage,
};
use nervix_models::UnitKind;
use nervix_primitives::sync::Arc;

use crate::{Failure, FailureKind, Schema};

/// The kind of an inspected execution unit; its index stays in the existing unit identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InspectionUnitKind {
    Single,
    Partition,
    Instance,
}

impl From<UnitKind> for InspectionUnitKind {
    fn from(kind: UnitKind) -> Self {
        match kind {
            UnitKind::Single => Self::Single,
            UnitKind::Partition(_) => Self::Partition,
            UnitKind::Instance(_) => Self::Instance,
        }
    }
}

/// One independently retained inspection; its Row frame clones share verified backing bytes.
#[derive(Debug, Clone)]
pub struct Inspection {
    state: StateInspection,
    json: String,
}

impl Inspection {
    pub fn new(state: StateInspection) -> Self {
        let json = state.to_json();
        Self { state, json }
    }
    pub fn state(&self) -> &StateInspection {
        &self.state
    }
    pub fn domain(&self) -> &str {
        self.state.entity.domain.as_str()
    }
    pub fn entity_kind(&self) -> &str {
        self.state.entity.node.kind.keyword_phrase()
    }
    pub fn entity_name(&self) -> &str {
        self.state.entity.node.identifier.as_str()
    }
    pub fn rejection(&self) -> Option<InspectionRejection> {
        match self.state.result {
            StateInspectionResult::Page(_) => None,
            StateInspectionResult::Rejected(reason) => Some(reason),
        }
    }
    pub fn unit_count(&self) -> Option<u64> {
        match &self.state.result {
            StateInspectionResult::Page(page) => Some(page.unit_count),
            StateInspectionResult::Rejected(_) => None,
        }
    }
    pub fn continuation(&self) -> Option<&str> {
        match &self.state.result {
            StateInspectionResult::Page(page) => page.continuation.as_deref(),
            StateInspectionResult::Rejected(_) => None,
        }
    }
    pub fn units(&self) -> &[StateInspectionUnit] {
        match &self.state.result {
            StateInspectionResult::Page(page) => &page.units,
            StateInspectionResult::Rejected(_) => &[],
        }
    }
    pub fn unit(&self, index: usize) -> Result<&StateInspectionUnit, Failure> {
        self.units().get(index).ok_or_else(|| {
            Failure::new(
                FailureKind::InvalidArgument,
                "the inspection has no unit at that index",
            )
        })
    }
    pub fn unit_kind(&self, index: usize) -> Result<InspectionUnitKind, Failure> {
        Ok(InspectionUnitKind::from(self.unit(index)?.unit.kind()))
    }
    pub fn unit_index(&self, index: usize) -> Result<Option<u64>, Failure> {
        match self.unit(index)?.unit.kind() {
            UnitKind::Single => Ok(None),
            UnitKind::Partition(index) => Ok(Some(u64::from(index))),
            UnitKind::Instance(index) => Ok(Some(index)),
        }
    }
    pub fn json(&self) -> &str {
        &self.json
    }
    pub fn counts(&self) -> Option<StateInspectionCounts> {
        match &self.state.result {
            StateInspectionResult::Page(page) => Some(page.counts),
            StateInspectionResult::Rejected(_) => None,
        }
    }
    pub fn section(&self, unit: usize, section: usize) -> Result<&StateInspectionSection, Failure> {
        let StateInspectionResult::Page(page) = &self.state.result else {
            return Err(Failure::new(
                FailureKind::InvalidArgument,
                "the inspection was refused",
            ));
        };
        let missing = || {
            Failure::new(
                FailureKind::InvalidArgument,
                "the inspection has no section at those indices",
            )
        };
        let unit = page.units.get(unit).ok_or_else(missing)?;
        unit.sections.get(section).ok_or_else(missing)
    }
    pub fn schema(&self, unit: usize, section: usize) -> Result<Schema, Failure> {
        Ok(Schema::new(Arc::new(
            self.section(unit, section)?.schema.clone(),
        )))
    }
    pub fn frame(
        &self,
        unit: usize,
        section: usize,
        frame: usize,
    ) -> Result<&StateRowFrame, Failure> {
        self.section(unit, section)?
            .frames
            .get(frame)
            .ok_or_else(|| {
                Failure::new(
                    FailureKind::InvalidArgument,
                    "the section has no frame at that index",
                )
            })
    }

    /// Retain precisely one verified Row frame and its schema for a native host view.
    pub fn row_frame(
        &self,
        unit: usize,
        section: usize,
        frame: usize,
    ) -> Result<InspectionFrame, Failure> {
        let rows = self.frame(unit, section, frame)?;
        let section = self.section(unit, section)?;
        if let Err(error) = rows.conform(&section.schema) {
            return Err(Failure::new(
                FailureKind::Protocol,
                format!("an inspection frame does not conform to its section schema: {error:#}"),
            ));
        }
        Ok(InspectionFrame {
            frame: rows.clone(),
            schema: Schema::new(Arc::new(section.schema.clone())),
        })
    }
}

/// An independently retained verified frame; cloning shares its bytes and schema.
#[derive(Debug, Clone)]
pub struct InspectionFrame {
    frame: StateRowFrame,
    // A native Schema returned to the caller can outlive its frame view.
    schema: Schema,
}

impl InspectionFrame {
    pub fn bytes(&self) -> &[u8] {
        self.frame.bytes().as_ref()
    }
    pub fn row_count(&self) -> u64 {
        self.frame.batch().len().arch_into()
    }
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }
}
