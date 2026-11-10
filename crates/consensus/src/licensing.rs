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
pub use activation::{
    ActivationBinding, ActivationState, LicenseDecisionError, LicenseVerification,
    ReceivedLicenseDecision, StoredDecision, VerifiedLicenseDecision,
};
pub use environment::{LICENSING_SERVICE, LicensingEnvironment};
#[cfg(test)]
pub(crate) mod tests;
#[cfg(test)]
pub(crate) mod verification_tests;

use error_stack::Report;
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
    Free,
    Activated(Box<ActivationState>),
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
            mode: LicensingMode::Free,
        }
    }

    pub(crate) fn validate(
        &self,
        identity: &ClusterIdentity,
    ) -> Result<(), Report<LicensingRecordError>> {
        if self.licensing_time < identity.created_at {
            return Err(Report::new(LicensingRecordError::Time));
        }
        if let LicensingMode::Activated(activation) = &self.mode {
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
            {
                return Err(Report::new(LicensingRecordError::Activation));
            }
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
    quota: nervix_licensing_vocabulary::Quota,
) -> error_stack::Result<(), nervix_licensing_vocabulary::LicensingRefusal> {
    use nervix_licensing_vocabulary::{QuotaConstraint, QuotaName};
    if let (Some(record), Some(Ok(verified))) = (&state.licensing, &state.licensing_verification) {
        if verified
            .decision
            .timing
            .valid_until
            .is_some_and(|until| record.licensing_time.into_datetime() >= until)
        {
            return Err(Report::new(nervix_licensing_vocabulary::LicensingRefusal {
                state: nervix_licensing_vocabulary::AdmissionState::Lapsed,
                condition: QuotaName::ClusterDomains,
                limit: u64::try_from(state.domains.len()).assured("materialized domains fit u64"),
                current_usage: u64::try_from(state.domains.len())
                    .assured("materialized domains fit u64"),
                requested: 1,
            }));
        }
    }
    QuotaConstraint {
        name: QuotaName::ClusterDomains,
        quota,
    }
    .admit(
        u64::try_from(state.domains.len()).assured("a materialized domain collection fits u64"),
        1,
        false,
    )
    .map(|_| ())
}

pub(super) fn apply(
    state: &mut crate::StateMachineData,
    command: &crate::ConsensusCommand,
) -> crate::AppliedConsensusCommand {
    use crate::{
        AppliedConsensusCommand, ConsensusCommand, ConsensusConflict, StateMachineChanges,
    };

    let (Some(identity), Some(record)) = (&state.cluster_identity, &state.licensing) else {
        return AppliedConsensusCommand::conflict(ConsensusConflict::LicenseDecision {
            reason: LicenseDecisionError::NotActivated,
        });
    };
    let identity = identity.public();
    let result = match command {
        ConsensusCommand::ActivateLicense { binding, decision } => record.accept(
            &identity,
            binding,
            decision,
            None,
            &state.licensing_environment,
        ),
        ConsensusCommand::ApplyLicenseDecision {
            decision,
            expected_revision,
        } => {
            let LicensingMode::Activated(activation) = &record.mode else {
                return AppliedConsensusCommand::conflict(ConsensusConflict::LicenseDecision {
                    reason: LicenseDecisionError::NotActivated,
                });
            };
            record.accept(
                &identity,
                &activation.binding,
                decision,
                Some(*expected_revision),
                &state.licensing_environment,
            )
        }
        ConsensusCommand::InstallTrustDocuments { documents } => {
            record.install_trust(documents, &state.licensing_environment)
        }
        ConsensusCommand::AdvanceLicensingTime { observed_at } => {
            let mut advanced = record.as_ref().clone();
            advanced.licensing_time = advanced.licensing_time.max(*observed_at);
            Ok(advanced)
        }
        _ => unreachable!("only licensing commands enter their owner"),
    };
    let accepted = match result {
        Ok(record) => record,
        Err(error) => {
            return AppliedConsensusCommand::conflict(ConsensusConflict::LicenseDecision {
                reason: *error.current_context(),
            });
        }
    };
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
