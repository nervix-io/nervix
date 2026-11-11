//! The committed licensing singleton and a coherent observation of its usage.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Current licensing records, archive validation and committed usage capture.
//! - **Depends on.** Consensus state, shared licensing vocabulary and signed-document identities.
//! - **Must not know.** Policy evaluation, physical clocks, service connectivity or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns replicated licensing state")
)]

mod activation;
mod archive;
mod environment;
#[cfg(test)]
mod operation_tests;
mod release;
mod request;
mod rotation;
#[cfg(test)]
mod transition_tests;
mod transitions;
pub use activation::{
    ActivationBinding, ActivationState, LicenseDecisionError, LicenseVerification,
    ReceivedLicenseDecision, StoredDecision, StoredDecisionOperation, VerifiedLicenseDecision,
};
pub use environment::{LICENSING_SERVICE, LicensingEnvironment};
pub use release::{
    AwaitingLicenseRelease, ReleaseConfirmation, ReleaseState, StoredLicenseTransitions,
};
pub use request::PreparedLicensingRequest;
pub use transitions::{CommittedLicenseTransitions, CommittedTransition};
#[cfg(test)]
pub(crate) mod tests;
#[cfg(test)]
pub(crate) mod verification_tests;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_licensing_protocol::{document::DocumentType, impact::UsageSnapshot, keys::KeyId};
use nervix_models::Timestamp;
use nervix_primitives::sync::Arc;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ClusterIdentity, Observer};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum LicensingMode {
    Free {
        #[rkyv(with = rkyv::with::Map<crate::cluster_identity::archive::UuidBytes>)]
        last_release: Option<uuid::Uuid>,
    },
    Activated(Box<ActivationState>),
    Released(Box<ReleaseState>),
}

/// Exact externally signed bytes and their retained document identity. Succession has no working kid.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct StoredTrustDocument {
    pub bytes: Vec<u8>,
    #[rkyv(with = archive::DocumentKind)]
    pub typ: DocumentType,
    pub serial: u64,
    #[rkyv(with = rkyv::with::Map<crate::cluster_identity::archive::KeyIdentity>)]
    pub kid: Option<KeyId>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct LicensingRecord {
    pub licensing_time: Timestamp,
    pub trust: Vec<StoredTrustDocument>,
    pub mode: LicensingMode,
}

#[derive(Debug, Error)]
pub(crate) enum LicensingRecordError {
    #[error("licensing time precedes the cluster identity's creation")]
    Time,
    #[error("stored trust document metadata does not identify a trust document")]
    TrustIdentity,
    #[error("stored activation binding or decision metadata is structurally inconsistent")]
    Activation,
}

impl LicensingRecord {
    pub(crate) fn free(licensing_time: Timestamp) -> Self {
        Self {
            licensing_time,
            trust: Vec::new(),
            mode: LicensingMode::Free { last_release: None },
        }
    }

    pub(crate) fn validate(
        &self,
        identity: &ClusterIdentity,
    ) -> Result<(), Report<LicensingRecordError>> {
        if self.licensing_time < identity.created_at {
            return Err(Report::new(LicensingRecordError::Time));
        }
        if let Some(activation) = self.mode.decision_state() {
            let binding = &activation.binding;
            let lifetime_valid = match (binding.kind, binding.ephemeral_expires_at) {
                (nervix_licensing_vocabulary::ActivationKind::Persistent, None) => true,
                (nervix_licensing_vocabulary::ActivationKind::Ephemeral, Some(until)) => {
                    until > binding.created_at
                }
                _ => false,
            };
            if !lifetime_valid
                || binding.created_at > self.licensing_time
                || activation.decision.issued_at > self.licensing_time
                || activation.decision.received_at > self.licensing_time
                || activation.decision.bytes.is_empty()
                || activation.decision.revision == 0
                || activation.decision.conditions_revision == 0
                || !activation.transitions.validate()
            {
                return Err(Report::new(LicensingRecordError::Activation));
            }
        }
        if let LicensingMode::Released(release) = &self.mode {
            release.validate(identity)?;
        }
        for document in &self.trust {
            let valid = match document.typ {
                DocumentType::WorkingKey | DocumentType::Revocation => document.kid.is_some(),
                DocumentType::Succession => document.kid.is_none(),
                _ => false,
            };
            if !valid || document.bytes.is_empty() {
                return Err(Report::new(LicensingRecordError::TrustIdentity));
            }
        }
        Ok(())
    }
}

/// Bootstrap absence is one state; an installed identity always has its licensing record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterLicensingState {
    pub identity: ClusterIdentity,
    pub record: Arc<LicensingRecord>,
    pub verification: Option<LicenseVerification>,
    pub rotation: Option<crate::ClusterKeyRotation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicensingSnapshot {
    pub initialized: Option<ClusterLicensingState>,
    pub revision: Option<u64>,
    pub usage: UsageSnapshot,
}

impl Observer {
    /// These are committed membership/domain records, captured with the licensing singleton.
    pub fn licensing_snapshot(&self) -> LicensingSnapshot {
        let state = self.inner.store.inner.state();
        let initialized = match (&state.cluster_identity, &state.licensing) {
            (Some(identity), Some(record)) => Some(ClusterLicensingState {
                identity: identity.public(),
                record: record.clone(),
                verification: state.licensing_verification.clone(),
                rotation: identity.pending_rotation(),
            }),
            (None, None) => None,
            _ => unreachable!("installation and recovery preserve the licensing singleton pair"),
        };
        LicensingSnapshot {
            initialized,
            revision: state.last_applied_log_id.as_ref().map(|log| log.index),
            usage: UsageSnapshot {
                cluster_nodes: u64::try_from(state.last_membership.membership().nodes().count())
                    .assured("a materialized membership has fewer than u64 entries"),
                cluster_domains: u64::try_from(state.domains.len())
                    .assured("a materialized domain collection has fewer than u64 entries"),
            },
        }
    }
}

pub(super) fn admit_domain(
    state: &crate::StateMachineData,
    mut quota: nervix_licensing_vocabulary::Quota,
    replacement: bool,
) -> error_stack::Result<(), nervix_licensing_vocabulary::LicensingRefusal> {
    use nervix_licensing_vocabulary::{QuotaConstraint, QuotaName};
    if let (Some(record), Some(Ok(verified))) = (&state.licensing, &state.licensing_verification)
        && let LicensingMode::Activated(activation) = &record.mode
    {
        let decision = &verified.decision;
        let signed_target = decision.status
            == nervix_licensing_protocol::documents::DecisionStatus::Active
            && record.licensing_time.into_datetime() < decision.timing.term_ends_at;
        let lapsed = signed_target
            && decision
                .timing
                .valid_until
                .is_some_and(|until| record.licensing_time.into_datetime() >= until);
        let refusal = nervix_licensing_vocabulary::LicensingRefusal {
            state: nervix_licensing_vocabulary::AdmissionState::Lapsed,
            condition: QuotaName::ClusterDomains,
            limit: u64::try_from(state.domains.len()).assured("materialized domains fit u64"),
            current_usage: u64::try_from(state.domains.len())
                .assured("materialized domains fit u64"),
            requested: 1,
        };
        let recovery = if replacement {
            domain_recovery_quota(record, activation, &verified.decision).change_context(refusal)?
        } else {
            None
        };
        if lapsed && recovery.is_none() {
            return Err(Report::new(refusal));
        }
        if replacement {
            let live = match recovery {
                Some(recovery) => recovery,
                None => {
                    let free = nervix_licensing_protocol::conditions::FREE_POLICY.usage();
                    let target = if signed_target {
                        &decision.conditions.usage
                    } else {
                        &free
                    };
                    target
                        .quota(QuotaName::ClusterDomains.as_str())
                        .assured("verified conditions supply the current cluster domain quota")
                }
            };
            quota = match (quota, live) {
                (nervix_licensing_vocabulary::Quota::Unlimited, quota)
                | (quota, nervix_licensing_vocabulary::Quota::Unlimited) => quota,
                (
                    nervix_licensing_vocabulary::Quota::Bounded { value: captured },
                    nervix_licensing_vocabulary::Quota::Bounded { value: live },
                ) => nervix_licensing_vocabulary::Quota::Bounded {
                    value: captured.min(live),
                },
            };
        }
    }

    QuotaConstraint {
        name: QuotaName::ClusterDomains,
        quota,
    }
    .admit(
        u64::try_from(state.domains.len()).assured("a materialized domain collection fits u64"),
        1,
        replacement,
    )
    .map(|_| ())
}

fn domain_recovery_quota(
    record: &LicensingRecord,
    activation: &ActivationState,
    decision: &nervix_licensing_protocol::documents::Decision,
) -> error_stack::Result<
    Option<nervix_licensing_vocabulary::Quota>,
    nervix_licensing_protocol::impact::ImpactError,
> {
    let now = record.licensing_time;
    if activation
        .binding
        .ephemeral_expires_at
        .is_some_and(|expiry| now >= expiry)
    {
        return Ok(None);
    }
    if !activation.transitions.values().any(|entry| {
        entry.until() > now
            && entry.transition.affected.iter().any(|affected| {
                affected.condition
                    == nervix_licensing_vocabulary::QuotaName::ClusterDomains.as_str()
            })
    }) {
        return Ok(None);
    }
    let transitions = activation.transitions.effective_values();
    nervix_licensing_protocol::impact::EffectiveUsage {
        conditions: &decision.conditions.usage,
        transitions: &transitions,
    }
    .quota(
        nervix_licensing_vocabulary::QuotaName::ClusterDomains.as_str(),
        now.into_datetime(),
    )
    .map(Some)
}

pub(super) fn apply(
    state: &mut crate::StateMachineData,
    command: &crate::ConsensusCommand,
) -> crate::AppliedConsensusCommand {
    use crate::{
        AppliedConsensusCommand, ConsensusCommand, ConsensusConflict, StateMachineChanges,
    };

    if matches!(
        command,
        ConsensusCommand::ReleaseLicense { .. }
            | ConsensusCommand::PrepareLicenseReleaseRequest { .. }
            | ConsensusCommand::ConfirmRelease { .. }
    ) {
        return release::apply(state, command);
    }
    if matches!(
        command,
        ConsensusCommand::PrepareClusterKeyRotationRequest { .. }
            | ConsensusCommand::CompleteClusterKeyRotation { .. }
            | ConsensusCommand::AbandonClusterKeyRotation { .. }
    ) {
        return rotation::apply(state, command);
    }

    let (Some(identity), Some(record)) = (&state.cluster_identity, &state.licensing) else {
        return AppliedConsensusCommand::conflict(ConsensusConflict::LicenseDecision {
            reason: LicenseDecisionError::NotActivated,
        });
    };
    let identity = identity.public();
    let usage = UsageSnapshot {
        cluster_nodes: u64::try_from(state.last_membership.membership().nodes().count())
            .assured("materialized membership fits u64"),
        cluster_domains: u64::try_from(state.domains.len()).assured("materialized domains fit u64"),
    };
    let result = match command {
        ConsensusCommand::ActivateLicense { binding, decision } => record
            .accept(
                &identity,
                binding,
                decision,
                None,
                &state.licensing_environment,
            )
            .and_then(|accepted| {
                accepted.assess_decision(record, &identity, usage, &state.licensing_environment)
            }),
        ConsensusCommand::ApplyLicenseDecision {
            decision,
            expected_revision,
        } => {
            let LicensingMode::Activated(activation) = &record.mode else {
                return AppliedConsensusCommand::conflict(ConsensusConflict::LicenseDecision {
                    reason: LicenseDecisionError::NotActivated,
                });
            };
            record
                .accept(
                    &identity,
                    &activation.binding,
                    decision,
                    Some(*expected_revision),
                    &state.licensing_environment,
                )
                .and_then(|accepted| {
                    accepted.assess_decision(record, &identity, usage, &state.licensing_environment)
                })
        }
        ConsensusCommand::InstallTrustDocuments { documents } => {
            record.install_trust(documents, &state.licensing_environment)
        }
        ConsensusCommand::AdvanceLicensingTime { observed_at } => {
            let mut advanced = record.as_ref().clone();
            advanced.licensing_time = advanced.licensing_time.max(*observed_at);
            if let Err(error) = advanced.assess_term_tick(
                &identity,
                record.licensing_time,
                usage,
                &state.licensing_environment,
            ) {
                return AppliedConsensusCommand::conflict(ConsensusConflict::LicenseDecision {
                    reason: *error.current_context(),
                });
            }
            advanced.finish_release_if_unneeded();
            Ok(advanced)
        }
        _ => unreachable!("only licensing commands enter their owner"),
    };
    let mut accepted = match result {
        Ok(record) => record,
        Err(error) => {
            return AppliedConsensusCommand::conflict(ConsensusConflict::LicenseDecision {
                reason: *error.current_context(),
            });
        }
    };
    if let LicensingMode::Activated(activation) = &mut accepted.mode {
        activation.transitions.protect_nodes(
            &state
                .last_membership
                .membership()
                .nodes()
                .map(|(name, _)| name.clone())
                .collect(),
        );
    }
    state.licensing_verification = accepted.verify(&identity, &state.licensing_environment);
    state.licensing = Some(Arc::new(accepted));
    if matches!(command, ConsensusCommand::ActivateLicense { .. }) {
        let mut identity = state
            .cluster_identity
            .as_ref()
            .verified("the licensing command checked its installed identity before applying")
            .as_ref()
            .clone();
        identity.mark_activated();
        state.cluster_identity = Some(Arc::new(identity));
    }
    AppliedConsensusCommand::applied(StateMachineChanges::default())
}

impl crate::Proposer {
    pub async fn activate_license(
        &self,
        binding: ActivationBinding,
        decision: ReceivedLicenseDecision,
    ) -> error_stack::Result<(), crate::ConsensusError> {
        let response = self
            .inner
            .client_write(crate::ConsensusCommand::ActivateLicense {
                binding: Box::new(binding),
                decision: Box::new(decision),
            })
            .await?;
        Self::applied_response(response.data)
    }

    pub async fn apply_license_decision(
        &self,
        decision: ReceivedLicenseDecision,
        expected_revision: u64,
    ) -> error_stack::Result<(), crate::ConsensusError> {
        let response = self
            .inner
            .client_write(crate::ConsensusCommand::ApplyLicenseDecision {
                decision: Box::new(decision),
                expected_revision,
            })
            .await?;
        Self::applied_response(response.data)
    }

    pub async fn install_license_trust(
        &self,
        documents: Vec<StoredTrustDocument>,
    ) -> error_stack::Result<(), crate::ConsensusError> {
        let response = self
            .inner
            .client_write(crate::ConsensusCommand::InstallTrustDocuments { documents })
            .await?;
        Self::applied_response(response.data)
    }

    pub async fn advance_licensing_time(
        &self,
        observed_at: Timestamp,
    ) -> error_stack::Result<(), crate::ConsensusError> {
        let response = self
            .inner
            .client_write(crate::ConsensusCommand::AdvanceLicensingTime { observed_at })
            .await?;
        Self::applied_response(response.data)
    }
}
