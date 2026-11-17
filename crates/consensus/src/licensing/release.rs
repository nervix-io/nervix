//! Committed license release before remote capacity confirmation.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Release grants, the original bounded signing capability and confirmation fences.
//! - **Depends on.** Shared impact arithmetic, identity ownership and signed decision validation.
//! - **Must not know.** HTTP, physical clocks, retry cadence or graph execution.

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_licensing_protocol::{
    conditions::FREE_POLICY,
    document::DocumentLimits,
    documents::{DecisionStatus, Transition, TransitionReason, TransitionTrigger},
    impact::{
        EffectiveUsage, ImpactContext, ImpactTarget, TransitionPolicy, UsageSnapshot, assess_impact,
    },
    requests::Request,
};
use nervix_models::Timestamp;
use nervix_primitives::sync::Arc;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ActivationState, LicenseDecisionError, LicensingMode, LicensingRecord, LicensingRecordError,
    PreparedLicensingRequest, ReceivedLicenseDecision,
};
use crate::{ClusterIdentity, ClusterIdentityRecord, ConsensusCommand, ConsensusError, Proposer};

/// One stored projection of protocol transitions. The maintained bounded JSON encoder owns syntax;
/// consumers receive the shared semantic type, including every amount, identity and deadline.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StoredLicenseTransitions(#[rkyv(with = super::archive::Transitions)] Vec<Transition>);

impl StoredLicenseTransitions {
    pub fn new(
        transitions: &[Transition],
        limits: DocumentLimits,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        let written = nervix_bounded_write::BoundedWriter::write_with(limits.bytes, |writer| {
            serde_json::to_writer(writer, transitions)
        })
        .change_context(LicenseDecisionError::Bounds)?;
        let nervix_bounded_write::BoundedWrite::Complete(bytes) = written else {
            return Err(Report::new(LicenseDecisionError::Bounds));
        };
        drop(bytes);
        Ok(Self(transitions.to_vec()))
    }

    pub fn values(&self) -> &[Transition] {
        &self.0
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[serde(deny_unknown_fields)]
pub struct AwaitingLicenseRelease {
    pub(super) key: crate::cluster_identity::ClusterKey,
    pub prepared: Option<PreparedLicensingRequest>,
}

impl std::fmt::Debug for AwaitingLicenseRelease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AwaitingLicenseRelease")
            .field("kid", &self.key.kid)
            .finish_non_exhaustive()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ReleaseConfirmation {
    Awaiting(Box<AwaitingLicenseRelease>),
    Confirmed,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ReleaseState {
    pub reference: nervix_models::CommandExecutionReference,
    pub activation: ActivationState,
    pub identity: ClusterIdentity,
    #[rkyv(with = crate::cluster_identity::archive::UuidBytes)]
    pub request_id: Uuid,
    pub released_at: Timestamp,
    pub grants_until: Option<Timestamp>,
    pub transitions: StoredLicenseTransitions,
    pub confirmation: ReleaseConfirmation,
}

impl LicensingMode {
    pub(super) fn decision_state(&self) -> Option<&ActivationState> {
        match self {
            Self::Free { .. } => None,
            Self::Activated(activation) => Some(activation),
            Self::Released(release) => Some(&release.activation),
        }
    }
}

impl ReleaseState {
    pub fn awaiting(&self) -> bool {
        matches!(self.confirmation, ReleaseConfirmation::Awaiting(_))
    }

    pub(super) fn validate(
        &self,
        current: &ClusterIdentity,
    ) -> error_stack::Result<(), LicensingRecordError> {
        if self.identity.cluster_uid != current.cluster_uid
            || self.identity.created_at != current.created_at
            || self.identity.key_generation > current.key_generation
            || self.released_at < self.activation.binding.created_at
        {
            return Err(Report::new(LicensingRecordError::Activation));
        }
        let values = self.transitions.values();
        let mut identities = std::collections::BTreeSet::new();
        for transition in values {
            if transition.deadline < transition.started_at
                || transition.affected.is_empty()
                || !identities.insert(transition.transition_id)
            {
                return Err(Report::new(LicensingRecordError::Activation));
            }
            Timestamp::try_from(transition.started_at)
                .change_context(LicensingRecordError::Activation)?;
            Timestamp::try_from(transition.deadline)
                .change_context(LicensingRecordError::Activation)?;
            for affected in &transition.affected {
                affected
                    .additional_allowance()
                    .change_context(LicensingRecordError::Activation)?;
            }
        }
        let deadline = values.iter().map(|value| value.deadline).max();
        if deadline != self.grants_until.map(Timestamp::into_datetime) {
            return Err(Report::new(LicensingRecordError::Activation));
        }
        if let ReleaseConfirmation::Awaiting(awaiting) = &self.confirmation {
            awaiting
                .key
                .validate()
                .change_context(LicensingRecordError::Activation)?;
            if awaiting.key.kid != self.identity.kid
                || awaiting.key.public_jwk != self.identity.public_jwk
            {
                return Err(Report::new(LicensingRecordError::Activation));
            }
            if let Some(prepared) = &awaiting.prepared {
                self.verify_request(
                    prepared,
                    prepared
                        .stored_limits()
                        .change_context(LicensingRecordError::Activation)?,
                )
                .change_context(LicensingRecordError::Activation)?;
            }
        }
        Ok(())
    }

    fn verify_request(
        &self,
        prepared: &PreparedLicensingRequest,
        limits: DocumentLimits,
    ) -> error_stack::Result<nervix_licensing_protocol::requests::RequestCommon, LicenseDecisionError>
    {
        let parsed = prepared.verify(&self.identity, limits)?;
        let Request::Release {
            common,
            activation_id,
            state: committed,
            grants_until,
            ..
        } = parsed
        else {
            return Err(Report::new(LicenseDecisionError::Request));
        };
        if common.request_id != self.request_id
            || activation_id != self.activation.binding.activation_id
            || committed.applied_decision_revision != self.activation.decision.revision
            || committed.applied_conditions_revision != self.activation.decision.conditions_revision
            || committed.licensing_time != self.released_at.into_datetime()
            || grants_until
                != self
                    .grants_until
                    .unwrap_or(self.released_at)
                    .into_datetime()
        {
            return Err(Report::new(LicenseDecisionError::Binding));
        }
        Ok(common)
    }

    pub(crate) fn signing_key(&self) -> Option<&crate::cluster_identity::ClusterKey> {
        match &self.confirmation {
            ReleaseConfirmation::Awaiting(awaiting) => Some(&awaiting.key),
            ReleaseConfirmation::Confirmed => None,
        }
    }
}

struct ReleaseAdmission<'a> {
    binding: &'a super::ActivationBinding,
    reference: &'a nervix_models::CommandExecutionReference,
    request_id: Uuid,
    released_at: Timestamp,
}

impl LicensingRecord {
    fn release(
        &self,
        identity: &ClusterIdentityRecord,
        admission: ReleaseAdmission<'_>,
        usage: UsageSnapshot,
        environment: &super::LicensingEnvironment,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        let ReleaseAdmission {
            binding,
            reference,
            request_id,
            released_at,
        } = admission;
        match &self.mode {
            LicensingMode::Released(release) if release.request_id == request_id => {
                return Ok(self.clone());
            }
            LicensingMode::Free {
                last_release: Some(completed),
            } if *completed == request_id => return Ok(self.clone()),
            _ => {}
        }
        let LicensingMode::Activated(activation) = &self.mode else {
            return Err(Report::new(LicenseDecisionError::NotActivated));
        };
        if &activation.binding != binding {
            return Err(Report::new(LicenseDecisionError::Binding));
        }
        // The release establishes its own licensing time, and an activation the stop gate's
        // predicate has expired by then cannot be released.
        let released_at = released_at.max(self.licensing_time);
        if activation.binding.expiry_stop(released_at).is_some() {
            return Err(Report::new(LicenseDecisionError::Expired));
        }
        let mut transitions = Vec::new();
        if let Some(Ok(verified)) = self.verify(&identity.public(), environment) {
            let policy = TransitionPolicy::from_operating(&verified.decision.conditions.operating)
                .change_context(LicenseDecisionError::Metadata)?;
            transitions = verified
                .decision
                .transitions
                .iter()
                .filter(|transition| transition.deadline > released_at.into_datetime())
                .cloned()
                .collect();
            let to_conditions_revision = activation
                .decision
                .conditions_revision
                .checked_add(1)
                .ok_or_else(|| Report::new(LicenseDecisionError::Metadata))?;
            transitions.extend(
                assess_impact(
                    EffectiveUsage {
                        conditions: &verified.decision.conditions.usage,
                        transitions: &verified.decision.transitions,
                    },
                    ImpactTarget::Usage(&FREE_POLICY.usage()),
                    usage,
                    policy,
                    ImpactContext {
                        from_conditions_revision: activation.decision.conditions_revision,
                        to_conditions_revision,
                        started_at: released_at.into_datetime(),
                        trigger: TransitionTrigger::Release { request_id },
                        reason: TransitionReason::Released,
                    },
                    environment.limits().bytes,
                )
                .change_context(LicenseDecisionError::Metadata)?,
            );
        }
        let grants_until = match transitions.iter().map(|value| value.deadline).max() {
            Some(at) => {
                Some(Timestamp::try_from(at).change_context(LicenseDecisionError::Timestamp)?)
            }
            None => None,
        };
        Ok(Self {
            licensing_time: released_at,
            trust: self.trust.clone(),
            mode: LicensingMode::Released(Box::new(ReleaseState {
                reference: reference.clone(),
                activation: activation.as_ref().clone(),
                identity: identity.public(),
                request_id,
                released_at,
                grants_until,
                transitions: StoredLicenseTransitions::new(&transitions, environment.limits())?,
                confirmation: ReleaseConfirmation::Awaiting(Box::new(AwaitingLicenseRelease {
                    key: identity.retain_release_key(),
                    prepared: None,
                })),
            })),
        })
    }

    pub(super) fn finish_release_if_unneeded(&mut self) {
        if let LicensingMode::Released(release) = &self.mode
            && !release.awaiting()
            && release
                .grants_until
                .is_none_or(|at| at <= self.licensing_time)
        {
            self.mode = LicensingMode::Free {
                last_release: Some(release.request_id),
            };
        }
    }

    fn confirm_release(
        &self,
        request_id: Uuid,
        decision: Option<&ReceivedLicenseDecision>,
        environment: &super::LicensingEnvironment,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        let LicensingMode::Released(release) = &self.mode else {
            if let LicensingMode::Free {
                last_release: Some(completed),
            } = self.mode
                && completed == request_id
            {
                return Ok(self.clone());
            }
            return Err(Report::new(LicenseDecisionError::NotActivated));
        };
        if release.request_id != request_id {
            return Err(Report::new(LicenseDecisionError::Binding));
        }
        if !release.awaiting() {
            return Ok(self.clone());
        }
        let mut record = self.clone();
        if let Some(received) = decision {
            if received.request_id != request_id {
                return Err(Report::new(LicenseDecisionError::Binding));
            }
            let activated = Self {
                licensing_time: self.licensing_time,
                trust: self.trust.clone(),
                mode: LicensingMode::Activated(Box::new(release.activation.clone())),
            };
            let accepted = activated.accept(
                &release.identity,
                &release.activation.binding,
                received,
                Some(release.activation.decision.revision),
                environment,
            )?;
            let Some(Ok(verified)) = accepted.verify(&release.identity, environment) else {
                return Err(Report::new(LicenseDecisionError::Metadata));
            };
            if verified.decision.status != DecisionStatus::Released {
                return Err(Report::new(LicenseDecisionError::Binding));
            }
            let retained = release.transitions.values();
            if verified
                .decision
                .transitions
                .iter()
                .any(|transition| !retained.contains(transition))
            {
                return Err(Report::new(LicenseDecisionError::Binding));
            }
            let LicensingMode::Activated(activation) = accepted.mode else {
                unreachable!("decision acceptance produces an activated representation");
            };
            let LicensingMode::Released(current) = &mut record.mode else {
                unreachable!("record clones the released mode");
            };
            current.activation = *activation;
            record.trust = accepted.trust;
            record.licensing_time = accepted.licensing_time;
        }
        let LicensingMode::Released(current) = &mut record.mode else {
            unreachable!("confirmation retains the released mode until grants end");
        };
        current.confirmation = ReleaseConfirmation::Confirmed;
        record.finish_release_if_unneeded();
        Ok(record)
    }
}

pub(super) fn apply(
    state: &mut crate::StateMachineData,
    command: &ConsensusCommand,
) -> crate::AppliedConsensusCommand {
    if let ConsensusCommand::ReleaseLicense { reference, .. } = command
        && state.licensing_command_outcome(reference).is_some()
    {
        return crate::AppliedConsensusCommand::applied(crate::StateMachineChanges::default());
    }
    let (Some(identity), Some(record)) = (&state.cluster_identity, &state.licensing) else {
        return rejected(LicenseDecisionError::NotActivated);
    };
    let accepted = match command {
        ConsensusCommand::ReleaseLicense {
            reference,
            binding,
            request_id,
            released_at,
        } => record.release(
            identity,
            ReleaseAdmission {
                binding,
                reference,
                request_id: *request_id,
                released_at: *released_at,
            },
            UsageSnapshot {
                cluster_nodes: u64::try_from(state.last_membership.membership().nodes().count())
                    .assured("materialized membership fits u64"),
                cluster_domains: u64::try_from(state.domains.len())
                    .assured("materialized domains fit u64"),
            },
            &state.licensing_environment,
        ),
        ConsensusCommand::ConfirmRelease {
            request_id,
            decision,
        } => record.confirm_release(
            *request_id,
            decision.as_deref(),
            &state.licensing_environment,
        ),
        ConsensusCommand::PrepareLicenseReleaseRequest {
            request_id,
            request,
        } => {
            let LicensingMode::Released(release) = &record.mode else {
                return rejected(LicenseDecisionError::NotActivated);
            };
            let common = match release.verify_request(request, state.licensing_environment.limits())
            {
                Ok(common) => common,
                Err(error) => return rejected(*error.current_context()),
            };
            if *request_id != release.request_id
                || &common.aud != state.licensing_environment.service()
            {
                return rejected(LicenseDecisionError::Binding);
            }
            let mut accepted = record.as_ref().clone();
            let LicensingMode::Released(release) = &mut accepted.mode else {
                unreachable!("accepted record clones the released mode");
            };
            let ReleaseConfirmation::Awaiting(awaiting) = &mut release.confirmation else {
                return rejected(LicenseDecisionError::NotActivated);
            };
            awaiting.prepared = Some(request.as_ref().clone());
            Ok(accepted)
        }
        _ => unreachable!("only release commands enter their owner"),
    };
    match accepted {
        Ok(record) => {
            state.licensing_verification =
                record.verify(&identity.public(), &state.licensing_environment);
            state.licensing = Some(Arc::new(record));
            if let ConsensusCommand::ReleaseLicense { reference, .. } = command {
                state.record_licensing_command_outcome(
                    reference,
                    crate::LicensingCommandOutcome::Applied,
                );
            }
            crate::AppliedConsensusCommand::applied(crate::StateMachineChanges::default())
        }
        Err(error) => rejected(*error.current_context()),
    }
}

fn rejected(reason: LicenseDecisionError) -> crate::AppliedConsensusCommand {
    crate::AppliedConsensusCommand::conflict(crate::ConsensusConflict::LicenseDecision { reason })
}

impl Proposer {
    pub async fn release_license(
        &self,
        binding: super::ActivationBinding,
        reference: nervix_models::CommandExecutionReference,
        request_id: Uuid,
        released_at: Timestamp,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::ReleaseLicense {
                reference,
                binding: Box::new(binding),
                request_id,
                released_at,
            })
            .await?;
        Self::applied_response(response.data)
    }
    pub async fn prepare_license_release_request(
        &self,
        request_id: Uuid,
        request: PreparedLicensingRequest,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::PrepareLicenseReleaseRequest {
                request_id,
                request: Box::new(request),
            })
            .await?;
        Self::applied_response(response.data)
    }
    pub async fn confirm_license_release(
        &self,
        request_id: Uuid,
        decision: Option<ReceivedLicenseDecision>,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::ConfirmRelease {
                request_id,
                decision: decision.map(Box::new),
            })
            .await?;
        Self::applied_response(response.data)
    }
}
