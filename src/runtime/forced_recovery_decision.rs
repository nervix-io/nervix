//! Forced ownership-recovery activation decisions.
//!
//! Layer: decisions.
//! - **Owns.** Validation of the scheduled outcome that authorizes checkpoint activation or reset.
//! - **Depends on.** Typed schedule models and runtime-state activation capabilities.
//! - **Must not know.** Persistence mechanisms, asynchronous tasks or connector behavior.

use super::*;

impl ForcedRuntimeStateRecoveryAuthorization {
    pub(super) fn for_scheduled_node(
        node: &ScheduledNode,
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

        match transition.state_recovery {
            OwnershipStateRecoveryOutcome::Complete => {
                if reset_components.is_empty() {
                    Ok(None)
                } else {
                    Err(Report::new(
                        RuntimePersistenceError::InvalidForcedRecoveryDecision,
                    ))
                }
            }
            OwnershipStateRecoveryOutcome::Unverified => {
                if reset_components.is_empty() {
                    Ok(Some(Self::PreparedCheckpoints))
                } else {
                    Err(Report::new(
                        RuntimePersistenceError::InvalidForcedRecoveryDecision,
                    ))
                }
            }
            OwnershipStateRecoveryOutcome::Reset => {
                if reset_components.is_empty() && !expected.is_empty() {
                    return Err(Report::new(
                        RuntimePersistenceError::InvalidForcedRecoveryDecision,
                    ));
                }
                if reset_components == expected {
                    Ok(Some(Self::RecreateState))
                } else {
                    Ok(Some(Self::PreparedCheckpoints))
                }
            }
        }
    }
}
