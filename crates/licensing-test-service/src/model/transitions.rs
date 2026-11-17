//! The scenario service's bounded transition acknowledgement and administrator extension.
//!
//! Layer: test harness.
//! - **Owns.** Receipt-window clamping and signed transition echoes for public scenarios.
//! - **Depends on.** The fixture's activation decisions and shared transition vocabulary.
//! - **Must not know.** Cluster policy, storage or graph execution.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_licensing_protocol::{
    conditions::QUOTA_GRACE,
    documents::{Decision, Transition},
    problems::ProblemKind,
};
use uuid::Uuid;

use super::{HeldActivation, Model};

pub(super) fn acknowledge(
    held: &HeldActivation,
    decision: &mut Decision,
    reported: &[Transition],
    now: DateTime<Utc>,
) -> Result<(), ProblemKind> {
    let mut accepted: BTreeMap<Uuid, Transition> = held
        .decision
        .transitions
        .iter()
        .chain(&decision.transitions)
        .map(|transition| (transition.transition_id, transition.clone()))
        .collect();
    let skew = TimeDelta::seconds(300);
    let upper = now
        .checked_add_signed(skew)
        .ok_or(ProblemKind::BindingMismatch)?;
    for transition in reported {
        if accepted.contains_key(&transition.transition_id) {
            continue;
        }
        let (introduced, grace) = if let Some(authority) = held
            .transition_authority
            .get(&transition.to_conditions_revision)
        {
            *authority
        } else if transition.to_conditions_revision == decision.conditions.conditions_revision {
            (
                decision.issued_at,
                decision
                    .conditions
                    .operating
                    .duration(QUOTA_GRACE)
                    .assured("fixture operating conditions were validated"),
            )
        } else {
            return Err(ProblemKind::BindingMismatch);
        };
        if transition.affected.is_empty()
            || transition
                .affected
                .iter()
                .any(|entry| entry.additional_allowance().is_err())
        {
            return Err(ProblemKind::BindingMismatch);
        }
        let lower = introduced
            .checked_sub_signed(skew)
            .ok_or(ProblemKind::BindingMismatch)?;
        let mut acknowledged = transition.clone();
        acknowledged.started_at = acknowledged.started_at.clamp(lower, upper);
        let nervix_licensing_protocol::conditions::ConditionDuration::Bounded { seconds } = grace
        else {
            return Err(ProblemKind::BindingMismatch);
        };
        let seconds = i64::try_from(seconds).map_err(|_| ProblemKind::BindingMismatch)?;
        let grace = TimeDelta::try_seconds(seconds).ok_or(ProblemKind::BindingMismatch)?;
        acknowledged.deadline = acknowledged
            .started_at
            .checked_add_signed(grace)
            .ok_or(ProblemKind::BindingMismatch)?;
        accepted.insert(acknowledged.transition_id, acknowledged);
    }
    decision.transitions = accepted.into_values().collect();
    Ok(())
}

impl Model {
    pub fn extend_transitions(&mut self, duration: std::time::Duration) {
        let delta = TimeDelta::from_std(duration).assured("scenario extension fits chrono");
        for activation in self.activations.values_mut() {
            for transition in &mut activation.decision.transitions {
                transition.deadline = transition
                    .deadline
                    .checked_add_signed(delta)
                    .verified("bounded scenario deadline fits chrono");
            }
        }
    }
}
