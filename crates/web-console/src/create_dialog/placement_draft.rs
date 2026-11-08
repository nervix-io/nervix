//! Domain-wide placement creation drafts.
//!
//! Layer: edges.
//! - **Owns.** Validating operator text into a named placement Model.
//! - **Depends on.** Vocabulary names, policies and exact integer parsing.
//! - **Must not know.** Graph validation, scheduling or browser transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::{Report, ResultExt as _};
use nervix_models::{CreatePlacement, PlacementName, PlacementPolicy};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct PlacementDraft {
    pub name: String,
    pub if_not_exists: bool,
    pub policy: PlacementPolicy,
    pub rank: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum PlacementDraftError {
    #[error("Placement name is invalid")]
    Name,
    #[error("Rank must be a positive 64-bit integer")]
    Rank,
}

impl PlacementDraft {
    pub fn build(&self) -> Result<CreatePlacement, Report<PlacementDraftError>> {
        let name =
            PlacementName::parse(self.name.trim()).change_context(PlacementDraftError::Name)?;
        let rank = if self.rank.trim().is_empty() {
            None
        } else {
            Some(
                self.rank
                    .trim()
                    .parse::<NonZeroU64>()
                    .change_context(PlacementDraftError::Rank)?,
            )
        };
        Ok(CreatePlacement::domain_wide(name, self.policy, rank))
    }
}
