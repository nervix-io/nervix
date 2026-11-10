//! A captured view of one entity's metadata.
//!
//! Layer: vocabulary.
//! - **Owns.** Typed inspection identity, exact values, revision view and captured policy.
//! - **Depends on.** Existing entity and transaction identities and label values.
//! - **Must not know.** Registry lookups, transport framing, client handles or execution tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns captured metadata inspection values")
)]

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use super::{EntityLabelLimits, EntityLabelMutation, EntityLabels};
use crate::{DomainNodeRef, InspectionFormat, NodeRef, TransactionPreviewIdentity};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct MutateEntityLabels {
    pub entity: NodeRef,
    pub mutation: EntityLabelMutation,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct ShowLabels {
    pub entity: NodeRef,
    pub format: InspectionFormat,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct EntityLabelPolicy {
    pub revision: NonZeroU64,
    pub limits: EntityLabelLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum EntityLabelView {
    Committed { revision: u64 },
    Staged { preview: TransactionPreviewIdentity },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "InspectionFields")]
pub struct EntityLabelsInspection {
    entity: DomainNodeRef,
    labels: EntityLabels,
    view: EntityLabelView,
    policy: EntityLabelPolicy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectionFields {
    entity: DomainNodeRef,
    labels: EntityLabels,
    view: EntityLabelView,
    policy: EntityLabelPolicy,
}

impl TryFrom<InspectionFields> for EntityLabelsInspection {
    type Error = error_stack::Report<super::EntityLabelError>;
    fn try_from(fields: InspectionFields) -> Result<Self, Self::Error> {
        Self::new(fields.entity, fields.labels, fields.view, fields.policy)
    }
}

impl EntityLabelsInspection {
    pub fn new(
        entity: DomainNodeRef,
        labels: EntityLabels,
        view: EntityLabelView,
        policy: EntityLabelPolicy,
    ) -> error_stack::Result<Self, super::EntityLabelError> {
        labels.validate(&policy.limits)?;
        Ok(Self {
            entity,
            labels,
            view,
            policy,
        })
    }
    pub fn entity(&self) -> &DomainNodeRef {
        &self.entity
    }
    pub fn labels(&self) -> &EntityLabels {
        &self.labels
    }
    pub fn view(&self) -> &EntityLabelView {
        &self.view
    }
    pub fn policy(&self) -> EntityLabelPolicy {
        self.policy
    }
    pub fn validate(&self) -> error_stack::Result<(), super::EntityLabelError> {
        self.labels.validate(&self.policy.limits)
    }
}

impl From<&crate::ClusterParameterRecord> for EntityLabelPolicy {
    fn from(record: &crate::ClusterParameterRecord) -> Self {
        Self {
            revision: record.revision,
            limits: EntityLabelLimits::from(&record.values),
        }
    }
}
