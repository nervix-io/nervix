//! Placement rule declarations and atomic alterations.
//!
//! Layer: vocabulary.
//! - **Owns.** Placement scopes, policies, member normalization and typed alterations.
//! - **Depends on.** Vocabulary names and serializable configuration values.
//! - **Must not know.** Graph validation, scheduling, parser recovery or runtime tasks.

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

use crate::{ModelName, PlacementName};

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
    pub name: PlacementName,
    pub scope: PlacementScope,
    pub policy: PlacementPolicy,
    pub rank: Option<NonZeroU64>,
}

/// The directed corridor or entire domain whose direct hops a rule claims.
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
    SetPolicy {
        policy: PlacementPolicy,
    },
    SetRank {
        rank: NonZeroU64,
    },
    DropRank,
    SetMembers {
        from: Vec<ModelName>,
        to: Vec<ModelName>,
    },
    SetDomainWide,
    RenameTo {
        name: PlacementName,
    },
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
}

impl CreatePlacement {
    pub fn new(
        name: PlacementName,
        from: Vec<ModelName>,
        to: Vec<ModelName>,
        policy: PlacementPolicy,
        rank: Option<NonZeroU64>,
    ) -> error_stack::Result<Self, AlterPlacementError> {
        let mut placement = Self {
            name,
            scope: PlacementScope::Corridor { from, to },
            policy,
            rank,
        };
        placement.normalize_members();
        placement.validate()?;
        Ok(placement)
    }

    pub fn domain_wide(
        name: PlacementName,
        policy: PlacementPolicy,
        rank: Option<NonZeroU64>,
    ) -> Self {
        Self {
            name,
            scope: PlacementScope::DomainWide,
            policy,
            rank,
        }
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
            match operation {
                AlterPlacementOperation::SetPolicy { policy } => candidate.policy = *policy,
                AlterPlacementOperation::SetRank { rank } => candidate.rank = Some(*rank),
                AlterPlacementOperation::DropRank => candidate.rank = None,
                AlterPlacementOperation::SetMembers { from, to } => {
                    candidate.scope = PlacementScope::Corridor {
                        from: from.clone(),
                        to: to.clone(),
                    };
                    candidate.normalize_members();
                }
                AlterPlacementOperation::SetDomainWide => {
                    candidate.scope = PlacementScope::DomainWide
                }
                AlterPlacementOperation::RenameTo { name } => candidate.name = name.clone(),
            }
            candidate.validate()?;
        }
        *self = candidate;
        Ok(())
    }

    pub fn validate(&self) -> error_stack::Result<(), AlterPlacementError> {
        if let PlacementScope::Corridor { from, to } = &self.scope {
            if from.is_empty() {
                return Err(Report::new(AlterPlacementError::EmptyFrom));
            }
            if to.is_empty() {
                return Err(Report::new(AlterPlacementError::EmptyTo));
            }
        }
        Ok(())
    }

    fn normalize_members(&mut self) {
        if let PlacementScope::Corridor { from, to } = &mut self.scope {
            deduplicate_identifiers(from);
            deduplicate_identifiers(to);
        }
    }
}

fn deduplicate_identifiers(identifiers: &mut Vec<ModelName>) {
    let mut seen = Vec::new();
    identifiers.retain(|identifier| {
        if seen.contains(identifier) {
            false
        } else {
            seen.push(identifier.clone());
            true
        }
    });
}
