//! Independently retained host access to captured entity labels.
//!
//! Layer: edges.
//! - **Owns.** Stable host getters and indexed access to one typed metadata read.
//! - **Depends on.** The verified inspection vocabulary and host failures.
//! - **Must not know.** Display parsing, command routing, persistence or running execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns retained metadata host handles")
)]

use arch_into::ArchInto as _;
use meticulous::OptionExt as _;
use nervix_models::{EntityLabelView, LabelKey};
use nervix_primitives::sync::Arc;

use crate::{Failure, FailureKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum EntityLabelViewKind {
    Committed = 1,
    Staged = 2,
}

/// The index makes repeated native indexed reads O(log n), without copying values.
#[derive(Debug)]
struct CapturedLabels {
    value: nervix_models::EntityLabelsInspection,
    keys: Vec<LabelKey>,
}

#[derive(Debug, Clone)]
pub struct EntityLabelsInspection {
    captured: Arc<CapturedLabels>,
}

impl EntityLabelsInspection {
    pub fn new(value: nervix_models::EntityLabelsInspection) -> Self {
        let keys = value.labels().iter().map(|(key, _)| key.clone()).collect();
        Self {
            captured: Arc::new(CapturedLabels { value, keys }),
        }
    }
    pub fn value(&self) -> &nervix_models::EntityLabelsInspection {
        &self.captured.value
    }
    pub fn domain(&self) -> &str {
        self.value().entity().domain.as_str()
    }
    pub fn entity_kind(&self) -> &str {
        self.value().entity().node.kind.keyword_phrase()
    }
    pub fn entity_name(&self) -> &str {
        self.value().entity().node.identifier.as_str()
    }
    pub fn labels(&self) -> &nervix_models::EntityLabels {
        self.value().labels()
    }
    pub fn count(&self) -> usize {
        self.labels().len()
    }
    pub fn entry(&self, index: usize) -> Result<(&str, &str), Failure> {
        let key = self.captured.keys.get(index).ok_or_else(|| {
            Failure::new(
                FailureKind::InvalidArgument,
                "the label view has no entry at that index",
            )
        })?;
        let value = self
            .labels()
            .get(key.as_str())
            .assured("the index was constructed from this immutable map's keys");
        Ok((key.as_str(), value))
    }
    pub fn view(&self) -> EntityLabelViewKind {
        match self.value().view() {
            EntityLabelView::Committed { .. } => EntityLabelViewKind::Committed,
            EntityLabelView::Staged { .. } => EntityLabelViewKind::Staged,
        }
    }
    pub fn revision(&self) -> Option<u64> {
        match self.value().view() {
            EntityLabelView::Committed { revision } => Some(*revision),
            EntityLabelView::Staged { .. } => None,
        }
    }
    pub fn transaction_id(&self) -> Option<&str> {
        match self.value().view() {
            EntityLabelView::Committed { .. } => None,
            EntityLabelView::Staged { preview } => Some(&preview.transaction_id),
        }
    }
    pub fn position(&self) -> Option<u64> {
        match self.value().view() {
            EntityLabelView::Committed { .. } => None,
            EntityLabelView::Staged { preview } => {
                Some(preview.position.accepted_operations().arch_into())
            }
        }
    }
    pub fn planning_basis(&self) -> Option<&[u8; 32]> {
        match self.value().view() {
            EntityLabelView::Committed { .. } => None,
            EntityLabelView::Staged { preview } => Some(preview.planning_basis.fingerprint()),
        }
    }
    pub fn policy_revision(&self) -> u64 {
        self.value().policy().revision.get()
    }
    pub fn max_count(&self) -> u64 {
        self.value().policy().limits.count
    }
    pub fn max_key_bytes(&self) -> u64 {
        self.value().policy().limits.key_bytes
    }
    pub fn max_value_bytes(&self) -> u64 {
        self.value().policy().limits.value_bytes
    }
    pub fn max_bytes(&self) -> u64 {
        self.value().policy().limits.bytes
    }
}
