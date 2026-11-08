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

use nervix_client_core::wire::{
    StateInspection, StateInspectionCounts, StateInspectionResult, StateInspectionSection,
    StateRowFrame,
};
use nervix_primitives::sync::Arc;

use crate::{Failure, FailureKind, Schema};

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
    pub fn json(&self) -> &str {
        &self.json
    }
    pub fn counts(&self) -> Option<StateInspectionCounts> {
        match &self.state.result {
            StateInspectionResult::Page(page) => Some(page.counts),
            StateInspectionResult::Rejected(_) => None,
        }
    }
    fn section(&self, unit: usize, section: usize) -> Result<&StateInspectionSection, Failure> {
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
}
