//! Atomically install a verified rotated decision and its pending cluster key.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Rotation request admission, exact operation fences and paired key/decision commits.
//! - **Depends on.** Cluster identity transitions and shared signed decision/request verification.
//! - **Must not know.** HTTP, scheduling, physical clocks or graph execution.

use nervix_licensing_protocol::{requests::Request, validation::DecisionOperation};
use nervix_primitives::sync::Arc;

use super::{LicenseDecisionError, LicensingMode};
use crate::{
    AppliedConsensusCommand, ConsensusCommand, ConsensusConflict, StateMachineChanges,
    StateMachineData,
};

pub(super) fn apply(
    state: &mut StateMachineData,
    command: &ConsensusCommand,
) -> AppliedConsensusCommand {
    let Some(identity) = state.cluster_identity.clone() else {
        return rejected(LicenseDecisionError::NotActivated);
    };
    if let ConsensusCommand::AbandonClusterKeyRotation {
        reference,
        request_id,
        code,
    } = command
    {
        return match identity.abandon_rotation(*request_id) {
            Ok(identity) => {
                state.cluster_identity = Some(Arc::new(identity));
                state.record_licensing_command_outcome(
                    reference,
                    crate::LicensingCommandOutcome::Refused { code: *code },
                );
                AppliedConsensusCommand::applied(StateMachineChanges::default())
            }
            Err(error) => {
                AppliedConsensusCommand::conflict(ConsensusConflict::ClusterKeyRotation {
                    reason: *error.current_context(),
                })
            }
        };
    }
    let Some(rotation) = identity.pending_rotation() else {
        return AppliedConsensusCommand::conflict(ConsensusConflict::ClusterKeyRotation {
            reason: crate::ClusterIdentityError::NoPendingRotation,
        });
    };
    let Some(record) = state.licensing.clone() else {
        return rejected(LicenseDecisionError::NotActivated);
    };
    let LicensingMode::Activated(activation) = &record.mode else {
        return rejected(LicenseDecisionError::NotActivated);
    };
    match command {
        ConsensusCommand::PrepareClusterKeyRotationRequest {
            request_id,
            request,
        } => {
            if *request_id != rotation.request_id {
                return rejected(LicenseDecisionError::Binding);
            }
            let verified = request.verify_rotation(
                &identity.public(),
                &rotation.next_identity,
                *request_id,
                state.licensing_environment.limits(),
            );
            let request_value = match verified {
                Ok(request) => request,
                Err(error) => return rejected(*error.current_context()),
            };
            let Request::RotateKey {
                common,
                activation_id,
                state: committed,
                ..
            } = request_value
            else {
                return rejected(LicenseDecisionError::Request);
            };
            if activation_id != activation.binding.activation_id
                || &common.aud != state.licensing_environment.service()
                || committed.applied_decision_revision != activation.decision.revision
                || committed.applied_conditions_revision != activation.decision.conditions_revision
                || committed.licensing_time > record.licensing_time.into_datetime()
            {
                return rejected(LicenseDecisionError::Binding);
            }
            match identity.stage_rotation_request(*request_id, request) {
                Ok(identity) => {
                    state.cluster_identity = Some(Arc::new(identity));
                    AppliedConsensusCommand::applied(StateMachineChanges::default())
                }
                Err(error) => {
                    AppliedConsensusCommand::conflict(ConsensusConflict::ClusterKeyRotation {
                        reason: *error.current_context(),
                    })
                }
            }
        }
        ConsensusCommand::CompleteClusterKeyRotation {
            request_id,
            decision,
        } => {
            if *request_id != rotation.request_id
                || decision.request_id != *request_id
                || rotation.prepared.is_none()
            {
                return rejected(LicenseDecisionError::Binding);
            }
            let accepted = record.accept_operation(
                &identity.public(),
                &activation.binding,
                decision,
                Some(activation.decision.revision),
                DecisionOperation::CompleteClusterKeyRotation {
                    pending_key: &rotation.next_identity.kid,
                },
                &state.licensing_environment,
            );
            let accepted = match accepted {
                Ok(accepted) => accepted,
                Err(error) => return rejected(*error.current_context()),
            };
            let identity = match identity.complete_rotation(*request_id) {
                Ok(identity) => identity,
                Err(error) => {
                    return AppliedConsensusCommand::conflict(
                        ConsensusConflict::ClusterKeyRotation {
                            reason: *error.current_context(),
                        },
                    );
                }
            };
            if let (Some(Ok(prior)), Some(Ok(next))) = (
                &state.licensing_verification,
                accepted.verify(&identity.public(), &state.licensing_environment),
            ) && (prior.decision.timing != next.decision.timing
                || prior.decision.activation.ephemeral_expires_at
                    != next.decision.activation.ephemeral_expires_at)
            {
                return rejected(LicenseDecisionError::Metadata);
            }
            state.licensing_verification =
                accepted.verify(&identity.public(), &state.licensing_environment);
            state.cluster_identity = Some(Arc::new(identity));
            state.licensing = Some(Arc::new(accepted));
            state.record_licensing_command_outcome(
                &rotation.reference,
                crate::LicensingCommandOutcome::Applied,
            );
            AppliedConsensusCommand::applied(StateMachineChanges::default())
        }
        _ => unreachable!("only rotation request, completion and abandonment enter their owner"),
    }
}

fn rejected(reason: LicenseDecisionError) -> AppliedConsensusCommand {
    AppliedConsensusCommand::conflict(ConsensusConflict::LicenseDecision { reason })
}
