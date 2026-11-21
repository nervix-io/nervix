//! Forced ownership-recovery activation decisions.
//!
//! Layer: decisions.
//! - **Owns.** Validation of the scheduled outcome that authorizes checkpoint activation or reset.
//! - **Depends on.** Typed schedule models and runtime-state activation capabilities.
//! - **Must not know.** Persistence mechanisms, asynchronous tasks or connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use super::*;

impl ForcedRuntimeStateRecoveryAuthorization {
    pub(super) fn for_scheduled_node(
        node: &ExecutionNode,
        transition: &nervix_models::OwnershipTransition,
    ) -> Result<Option<Self>, Report<RuntimePersistenceError>> {
        let expected = node
            .ownership_state_components()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let reset_components = transition
            .resets
            .iter()
            .map(|reset| reset.component)
            .collect::<BTreeSet<_>>();
        let reset_components_are_unique = reset_components.len() == transition.resets.len();
        let reset_components_belong_to_node = reset_components.is_subset(&expected);
        if !reset_components_are_unique || !reset_components_belong_to_node {
            return Err(Report::new(
                RuntimePersistenceError::InvalidForcedRecoveryDecision,
            ));
        }

        let allows_empty_inventory = match transition.state_recovery {
            OwnershipStateRecoveryOutcome::Complete if reset_components.is_empty() => {
                return Ok(None);
            }
            OwnershipStateRecoveryOutcome::Unverified if reset_components.is_empty() => {
                expected.is_empty()
            }
            OwnershipStateRecoveryOutcome::Reset
                if !reset_components.is_empty() || expected.is_empty() =>
            {
                reset_components == expected
            }
            _ => {
                return Err(Report::new(
                    RuntimePersistenceError::InvalidForcedRecoveryDecision,
                ));
            }
        };
        Ok(Some(if allows_empty_inventory {
            Self::AllowEmptyInventory
        } else {
            Self::PreparedCheckpoints
        }))
    }
}
