//! Placement rule declarations and atomic alterations.
//!
//! Layer: vocabulary.
//! - **Owns.** Placement rule forms, scopes, policies, member normalization and typed alterations.
//! - **Depends on.** Vocabulary names, selector expressions and serializable configuration values.
//! - **Must not know.** Graph validation, selector evaluation, scheduling, parser recovery or
//!   runtime tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use thiserror::Error;

use crate::{Expression, ModelName, PlacementName};

/// How a colocation rule relates the units on either side of each hop it covers.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Default,
    AsRefStr,
    strum::Display,
)]
pub enum PlacementPolicy {
    #[strum(serialize = "REQUIRE COLOCATION")]
    RequireColocation,
    #[strum(serialize = "PREFER COLOCATION")]
    PreferColocation,
    #[default]
    #[strum(serialize = "NEUTRAL")]
    Neutral,
    #[strum(serialize = "SUGGEST SEPARATION")]
    SuggestSeparation,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreatePlacement {
    pub labels: crate::EntityLabels,
    pub name: PlacementName,
    pub rule: PlacementRule,
    pub rank: Option<NonZeroU64>,
}

/// What a placement rule decides. Its form is fixed when the rule is created: an alteration
/// changes what the rule covers or how it decides, never which of the two it is.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum PlacementRule {
    /// Whether the units on either side of each covered hop share a cluster node.
    Colocation {
        scope: PlacementScope,
        policy: PlacementPolicy,
    },
    /// Which cluster nodes every unit of each covered runtime node may run on, or should prefer,
    /// judged by the committed record of each node.
    Nodes {
        scope: NodeRuleScope,
        policy: NodePolicy,
    },
}

/// The directed corridor or entire domain whose direct hops a colocation rule claims.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum PlacementScope {
    Corridor {
        from: Vec<ModelName>,
        to: Vec<ModelName>,
    },
    DomainWide,
}

/// The runtime nodes whose units a node rule governs: the listed members, or every
/// placement-eligible runtime node of the domain.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NodeRuleScope {
    Members(Vec<ModelName>),
    DomainWide,
}

/// How a node rule judges a cluster node for the units it covers.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NodePolicy {
    /// A unit runs only on a node whose record the selector admits.
    Require(Expression),
    /// A node whose record the selector admits ranks ahead of one it does not.
    Prefer(Expression),
    /// Lifts every weaker requirement: the covered units may run on any node.
    Any,
}

impl NodePolicy {
    /// The selector this policy evaluates, absent for `ANY NODE`.
    pub const fn selector(&self) -> Option<&Expression> {
        match self {
            Self::Require(selector) | Self::Prefer(selector) => Some(selector),
            Self::Any => None,
        }
    }

    /// The policy's name without its selector, as inspection lists it.
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Require(_) => "REQUIRE NODES",
            Self::Prefer(_) => "PREFER NODES",
            Self::Any => "ANY NODE",
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterPlacement {
    pub placement: PlacementName,
    pub operations: Vec<AlterPlacementOperation>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterPlacementOperation {
    /// Replaces a colocation rule's policy.
    SetPolicy {
        policy: PlacementPolicy,
    },
    /// Replaces a node rule's policy and selector.
    SetNodePolicy {
        policy: NodePolicy,
    },
    SetRank {
        rank: NonZeroU64,
    },
    DropRank,
    /// Replaces a colocation rule's coverage with a corridor.
    SetMembers {
        from: Vec<ModelName>,
        to: Vec<ModelName>,
    },
    /// Replaces a node rule's coverage with the listed members.
    SetNodeMembers {
        members: Vec<ModelName>,
    },
    /// Covers the whole domain, keeping the rule's form.
    SetDomainWide,
    RenameTo {
        name: PlacementName,
    },
}

impl AlterPlacementOperation {
    /// The operation as its statement writes it, for a refusal that names it.
    pub const fn spelling(&self) -> &'static str {
        match self {
            Self::SetPolicy { .. } | Self::SetNodePolicy { .. } => "SET POLICY",
            Self::SetRank { .. } => "SET RANK",
            Self::DropRank => "DROP RANK",
            Self::SetMembers { .. } => "SET FROM ... TO ...",
            Self::SetNodeMembers { .. } => "SET FOR",
            Self::SetDomainWide => "SET DOMAIN WIDE",
            Self::RenameTo { .. } => "RENAME TO",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterPlacementError {
    #[error("ALTER targets placement `{requested}`, but the stored placement is `{stored}`")]
    PlacementNameMismatch {
        stored: PlacementName,
        requested: PlacementName,
    },
    #[error("a placement must declare at least one FROM member")]
    EmptyFrom,
    #[error("a placement must declare at least one TO member")]
    EmptyTo,
    #[error("a node rule must declare at least one FOR member")]
    EmptyMembers,
    #[error("placement '{placement}' is a node rule; SET POLICY needs a node policy")]
    ColocationPolicyOnNodeRule { placement: PlacementName },
    #[error("placement '{placement}' is a colocation rule; SET POLICY needs a colocation policy")]
    NodePolicyOnColocationRule { placement: PlacementName },
    #[error(
        "placement '{placement}' is a node rule; {operation} applies only to a colocation rule"
    )]
    ColocationOperationOnNodeRule {
        placement: PlacementName,
        operation: &'static str,
    },
    #[error(
        "placement '{placement}' is a colocation rule; {operation} applies only to a node rule"
    )]
    NodeOperationOnColocationRule {
        placement: PlacementName,
        operation: &'static str,
    },
}

impl CreatePlacement {
    /// A colocation rule over the corridor from every `from` member to every `to` member.
    pub fn new(
        name: PlacementName,
        from: Vec<ModelName>,
        to: Vec<ModelName>,
        policy: PlacementPolicy,
        rank: Option<NonZeroU64>,
    ) -> error_stack::Result<Self, AlterPlacementError> {
        let mut placement = Self {
            labels: crate::EntityLabels::default(),
            name,
            rule: PlacementRule::Colocation {
                scope: PlacementScope::Corridor { from, to },
                policy,
            },
            rank,
        };
        placement.normalize_members();
        placement.validate()?;
        Ok(placement)
    }

    /// A colocation rule over every eligible hop of the domain.
    pub fn domain_wide(
        name: PlacementName,
        policy: PlacementPolicy,
        rank: Option<NonZeroU64>,
    ) -> Self {
        Self {
            labels: crate::EntityLabels::default(),
            name,
            rule: PlacementRule::Colocation {
                scope: PlacementScope::DomainWide,
                policy,
            },
            rank,
        }
    }

    /// A node rule over `scope`.
    pub fn node_rule(
        name: PlacementName,
        scope: NodeRuleScope,
        policy: NodePolicy,
        rank: Option<NonZeroU64>,
    ) -> error_stack::Result<Self, AlterPlacementError> {
        let mut placement = Self {
            labels: crate::EntityLabels::default(),
            name,
            rule: PlacementRule::Nodes { scope, policy },
            rank,
        };
        placement.normalize_members();
        placement.validate()?;
        Ok(placement)
    }

    /// Whether this rule selects cluster nodes rather than relating hops.
    pub const fn is_node_rule(&self) -> bool {
        matches!(self.rule, PlacementRule::Nodes { .. })
    }

    pub fn apply_alter(
        &mut self,
        alter: &AlterPlacement,
    ) -> error_stack::Result<(), AlterPlacementError> {
        if self.name != alter.placement {
            return Err(Report::new(AlterPlacementError::PlacementNameMismatch {
                stored: self.name.clone(),
                requested: alter.placement.clone(),
            }));
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.apply_operation(operation)?;
            candidate.validate()?;
        }
        *self = candidate;
        Ok(())
    }

    fn apply_operation(
        &mut self,
        operation: &AlterPlacementOperation,
    ) -> error_stack::Result<(), AlterPlacementError> {
        match operation {
            AlterPlacementOperation::SetRank { rank } => self.rank = Some(*rank),
            AlterPlacementOperation::DropRank => self.rank = None,
            AlterPlacementOperation::RenameTo { name } => self.name = name.clone(),
            AlterPlacementOperation::SetDomainWide => match &mut self.rule {
                PlacementRule::Colocation { scope, .. } => *scope = PlacementScope::DomainWide,
                PlacementRule::Nodes { scope, .. } => *scope = NodeRuleScope::DomainWide,
            },
            AlterPlacementOperation::SetPolicy { policy } => {
                let PlacementRule::Colocation {
                    policy: current, ..
                } = &mut self.rule
                else {
                    return Err(Report::new(
                        AlterPlacementError::ColocationPolicyOnNodeRule {
                            placement: self.name.clone(),
                        },
                    ));
                };
                *current = *policy;
            }
            AlterPlacementOperation::SetMembers { from, to } => {
                let PlacementRule::Colocation { scope, .. } = &mut self.rule else {
                    return Err(Report::new(
                        AlterPlacementError::ColocationOperationOnNodeRule {
                            placement: self.name.clone(),
                            operation: operation.spelling(),
                        },
                    ));
                };
                *scope = PlacementScope::Corridor {
                    from: from.clone(),
                    to: to.clone(),
                };
                self.normalize_members();
            }
            AlterPlacementOperation::SetNodePolicy { policy } => {
                let PlacementRule::Nodes {
                    policy: current, ..
                } = &mut self.rule
                else {
                    return Err(Report::new(
                        AlterPlacementError::NodePolicyOnColocationRule {
                            placement: self.name.clone(),
                        },
                    ));
                };
                *current = policy.clone();
            }
            AlterPlacementOperation::SetNodeMembers { members } => {
                let PlacementRule::Nodes { scope, .. } = &mut self.rule else {
                    return Err(Report::new(
                        AlterPlacementError::NodeOperationOnColocationRule {
                            placement: self.name.clone(),
                            operation: operation.spelling(),
                        },
                    ));
                };
                *scope = NodeRuleScope::Members(members.clone());
                self.normalize_members();
            }
        }
        Ok(())
    }

    pub fn validate(&self) -> error_stack::Result<(), AlterPlacementError> {
        match &self.rule {
            PlacementRule::Colocation {
                scope: PlacementScope::Corridor { from, to },
                ..
            } => {
                if from.is_empty() {
                    return Err(Report::new(AlterPlacementError::EmptyFrom));
                }
                if to.is_empty() {
                    return Err(Report::new(AlterPlacementError::EmptyTo));
                }
            }
            PlacementRule::Nodes {
                scope: NodeRuleScope::Members(members),
                ..
            } => {
                if members.is_empty() {
                    return Err(Report::new(AlterPlacementError::EmptyMembers));
                }
            }
            PlacementRule::Colocation {
                scope: PlacementScope::DomainWide,
                ..
            }
            | PlacementRule::Nodes {
                scope: NodeRuleScope::DomainWide,
                ..
            } => {}
        }
        Ok(())
    }

    fn normalize_members(&mut self) {
        match &mut self.rule {
            PlacementRule::Colocation {
                scope: PlacementScope::Corridor { from, to },
                ..
            } => {
                deduplicate_identifiers(from);
                deduplicate_identifiers(to);
            }
            PlacementRule::Nodes {
                scope: NodeRuleScope::Members(members),
                ..
            } => deduplicate_identifiers(members),
            PlacementRule::Colocation {
                scope: PlacementScope::DomainWide,
                ..
            }
            | PlacementRule::Nodes {
                scope: NodeRuleScope::DomainWide,
                ..
            } => {}
        }
    }
}

fn deduplicate_identifiers(identifiers: &mut Vec<ModelName>) {
    let mut seen = std::collections::BTreeSet::new();
    identifiers.retain(|identifier| seen.insert(identifier.clone()));
}
