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
use crate::{
    DomainName, DomainNodeRef, InspectionFormat, NodeRef, ResourceCatalogKey, ResourceName,
    TransactionPreviewIdentity, UserName,
};

/// Administrative targets carry their full identity in the statement.
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
pub enum AdministrativeLabelTarget {
    Domain(DomainName),
    User(UserName),
}

impl std::fmt::Display for AdministrativeLabelTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Domain(domain) => write!(f, "DOMAIN {domain}"),
            Self::User(user) => write!(f, "USER {user}"),
        }
    }
}

/// The target as written in Plexus. Models and resources use the selected domain.
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
pub enum EntityLabelTarget {
    Model(NodeRef),
    Resource(ResourceName),
    Administrative(AdministrativeLabelTarget),
}

impl EntityLabelTarget {
    pub fn requires_selected_domain(&self) -> bool {
        matches!(self, Self::Model(_) | Self::Resource(_))
    }

    pub fn resolve(&self, domain: Option<&DomainName>) -> Option<EntityLabelOwner> {
        match self {
            Self::Model(model) => Some(EntityLabelOwner::Model(DomainNodeRef::new(
                domain?.clone(),
                model.clone(),
            ))),
            Self::Resource(resource) => Some(EntityLabelOwner::Resource(ResourceCatalogKey::new(
                domain?, resource,
            ))),
            Self::Administrative(target) => Some(target.clone().into()),
        }
    }
}

impl std::fmt::Display for EntityLabelTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Model(model) => write!(f, "{} {}", model.kind.keyword_phrase(), model.identifier),
            Self::Resource(resource) => write!(f, "RESOURCE {resource}"),
            Self::Administrative(target) => target.fmt(f),
        }
    }
}

/// The fully addressed owner of one label map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EntityLabelOwner {
    Model(DomainNodeRef),
    Domain { name: DomainName },
    Resource(ResourceCatalogKey),
    User { name: UserName },
}

impl From<AdministrativeLabelTarget> for EntityLabelOwner {
    fn from(target: AdministrativeLabelTarget) -> Self {
        match target {
            AdministrativeLabelTarget::Domain(name) => Self::Domain { name },
            AdministrativeLabelTarget::User(name) => Self::User { name },
        }
    }
}

impl EntityLabelOwner {
    pub fn domain(&self) -> Option<&DomainName> {
        match self {
            Self::Model(model) => Some(&model.domain),
            Self::Resource(resource) => Some(&resource.domain),
            Self::Domain { name } => Some(name),
            Self::User { .. } => None,
        }
    }

    pub fn keyword_phrase(&self) -> &'static str {
        match self {
            Self::Model(model) => model.node.kind.keyword_phrase(),
            Self::Domain { .. } => "DOMAIN",
            Self::Resource(_) => "RESOURCE",
            Self::User { .. } => "USER",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Model(model) => model.node.identifier.as_str(),
            Self::Domain { name } => name.as_str(),
            Self::Resource(resource) => resource.identifier.as_str(),
            Self::User { name } => name.as_str(),
        }
    }

    pub fn supports_staged_view(&self) -> bool {
        matches!(self, Self::Model(_) | Self::Resource(_))
    }
}

impl std::fmt::Display for EntityLabelOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.keyword_phrase(), self.name())?;
        let domain = match self {
            Self::Model(model) => &model.domain,
            Self::Resource(resource) => &resource.domain,
            Self::Domain { .. } | Self::User { .. } => return Ok(()),
        };
        write!(f, " in domain {domain}")
    }
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
pub struct MutateEntityLabels {
    pub entity: EntityLabelTarget,
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
    pub entity: EntityLabelTarget,
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
    entity: EntityLabelOwner,
    labels: EntityLabels,
    view: EntityLabelView,
    policy: EntityLabelPolicy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectionFields {
    entity: EntityLabelOwner,
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
        entity: EntityLabelOwner,
        labels: EntityLabels,
        view: EntityLabelView,
        policy: EntityLabelPolicy,
    ) -> error_stack::Result<Self, super::EntityLabelError> {
        labels.validate(&policy.limits)?;
        if matches!(&view, EntityLabelView::Staged { .. }) && !entity.supports_staged_view() {
            return Err(error_stack::Report::new(
                super::EntityLabelError::AdministrativeStagedView,
            ));
        }
        Ok(Self {
            entity,
            labels,
            view,
            policy,
        })
    }
    pub fn entity(&self) -> &EntityLabelOwner {
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
