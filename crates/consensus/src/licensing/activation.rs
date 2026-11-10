//! Current activation bindings, signed decision storage and deterministic verification.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The binding independent of signed bytes, decision acceptance and recovered views.
//! - **Depends on.** Cluster identity, shared protocol verification and current timestamp values.
//! - **Must not know.** Clocks, service requests, graph execution or policy publication.

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use indexmap::IndexMap;
use nervix_licensing_protocol::{
    documents::{Activation, Decision, DecisionResponse, SignedTrustDocument, TrustDocuments},
    trust::{DecisionVerification, TrustDocumentContent, TrustState},
    validation::{DecisionContext, DecisionDisposition, DecisionOperation, DecisionRejection},
};
use nervix_licensing_vocabulary::ActivationKind;
use nervix_models::Timestamp;
use nervix_primitives::sync::Arc;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use super::{
    LicensingMode, LicensingRecord, StoredTrustDocument, environment::LicensingEnvironment,
};
use crate::ClusterIdentity;

fn transition_revisions(decision: &Decision) -> BTreeSet<u64> {
    decision
        .transitions
        .iter()
        .flat_map(|transition| {
            [
                transition.from_conditions_revision,
                transition.to_conditions_revision,
            ]
        })
        .collect()
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ActivationBinding {
    #[rkyv(with = crate::cluster_identity::archive::UuidBytes)]
    pub activation_id: Uuid,
    pub kind: ActivationKind,
    pub created_at: Timestamp,
    pub ephemeral_expires_at: Option<Timestamp>,
    pub license_key_id: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct StoredDecision {
    pub bytes: String,
    pub revision: u64,
    pub conditions_revision: u64,
    pub transition_conditions_revisions: Vec<u64>,
    pub operation: StoredDecisionOperation,
    pub issued_at: Timestamp,
    pub received_at: Timestamp,
    #[rkyv(with = crate::cluster_identity::archive::UuidBytes)]
    pub request_id: Uuid,
}

/// The accepted command's semantic context is required to verify preserved rotation deadlines
/// again after recovery. It is constructed at acceptance, never supplied as a verification flag.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum StoredDecisionOperation {
    Apply,
    ClusterKeyRotation,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ActivationState {
    pub binding: ActivationBinding,
    pub decision: StoredDecision,
}

/// A command carries received data, never a preverified flag. Every replica verifies it again.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ReceivedLicenseDecision {
    pub bytes: String,
    pub trust: Vec<StoredTrustDocument>,
    pub received_at: Timestamp,
    #[rkyv(with = crate::cluster_identity::archive::UuidBytes)]
    pub request_id: Uuid,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Error,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum LicenseDecisionError {
    #[error("licensing decision or trust documents exceed their admitted bounds")]
    Bounds,
    #[error("licensing trust documents do not verify")]
    Trust,
    #[error("licensing decision was rejected: {reason}")]
    Rejected { reason: DecisionRejection },
    #[error("licensing decision does not match its independent stored binding")]
    Binding,
    #[error("licensing decision metadata disagrees with its signed content")]
    Metadata,
    #[error("licensing timestamp is outside the supported coordinate")]
    Timestamp,
    #[error("the cluster already holds an activation")]
    AlreadyActivated,
    #[error("the cluster has no activation")]
    NotActivated,
    #[error("the licensing decision proposal is stale")]
    StaleDecision,
    #[error("the prepared licensing request does not verify")]
    Request,
    #[error("an expired ephemeral activation cannot be released")]
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedLicenseDecision {
    pub decision: Arc<Decision>,
    pub signing_key_revoked: bool,
    pub conditions_not_understood: usize,
}

pub type LicenseVerification = Result<VerifiedLicenseDecision, LicenseDecisionError>;

impl LicenseDecisionError {
    fn from_rejection(report: Report<DecisionRejection>) -> Report<Self> {
        let reason = *report.current_context();
        report.change_context(Self::Rejected { reason })
    }
}

impl ActivationBinding {
    fn protocol_binding(&self, identity: &ClusterIdentity) -> Activation {
        Activation {
            activation_id: self.activation_id,
            cluster_uid: identity.cluster_uid,
            cluster_key_kid: identity.kid.clone(),
            generation: identity.key_generation.get(),
            kind: self.kind,
            created_at: self.created_at.into_datetime(),
            ephemeral_expires_at: self.ephemeral_expires_at.map(Timestamp::into_datetime),
        }
    }

    pub fn from_verified(
        decision: &Decision,
        license_key_id: String,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        Ok(Self {
            activation_id: decision.activation.activation_id,
            kind: decision.activation.kind,
            created_at: Timestamp::try_from(decision.activation.created_at)
                .change_context(LicenseDecisionError::Timestamp)?,
            ephemeral_expires_at: match decision.activation.ephemeral_expires_at {
                Some(at) => {
                    Some(Timestamp::try_from(at).change_context(LicenseDecisionError::Timestamp)?)
                }
                None => None,
            },
            license_key_id,
        })
    }
}

impl ReceivedLicenseDecision {
    pub fn from_response(
        response: DecisionResponse,
        received_at: Timestamp,
        request_id: Uuid,
        held: &[StoredTrustDocument],
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        let mut documents = StoredTrustDocument::protocol_documents(held, environment)?;
        documents.documents.extend(response.trust.documents);
        let trust = StoredTrustDocument::verified_documents(&documents, environment)?;
        Ok(Self {
            bytes: response.decision,
            trust,
            received_at,
            request_id,
        })
    }

    fn stored(
        &self,
        decision: &Decision,
        operation: DecisionOperation<'_>,
    ) -> error_stack::Result<StoredDecision, LicenseDecisionError> {
        Ok(StoredDecision {
            bytes: self.bytes.clone(),
            revision: decision.decision_revision,
            conditions_revision: decision.conditions.conditions_revision,
            transition_conditions_revisions: transition_revisions(decision).into_iter().collect(),
            operation: match operation {
                DecisionOperation::Apply => StoredDecisionOperation::Apply,
                DecisionOperation::CompleteClusterKeyRotation { .. } => {
                    StoredDecisionOperation::ClusterKeyRotation
                }
            },
            issued_at: Timestamp::try_from(decision.issued_at)
                .change_context(LicenseDecisionError::Timestamp)?,
            received_at: self.received_at,
            request_id: self.request_id,
        })
    }
}

impl StoredTrustDocument {
    fn verified_documents(
        documents: &TrustDocuments,
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<Vec<Self>, LicenseDecisionError> {
        let trust = environment.trust(documents)?;
        Self::retained(&trust)
    }

    fn retained(trust: &TrustState) -> error_stack::Result<Vec<Self>, LicenseDecisionError> {
        let mut output = Vec::new();
        for document in trust.documents() {
            let (typ, serial, kid) = match document.content() {
                TrustDocumentContent::Authorization(value) => (
                    nervix_licensing_protocol::document::DocumentType::WorkingKey,
                    value.serial,
                    Some(value.kid.clone()),
                ),
                TrustDocumentContent::Revocation(value) => (
                    nervix_licensing_protocol::document::DocumentType::Revocation,
                    value.serial,
                    Some(value.kid.clone()),
                ),
                TrustDocumentContent::Succession(value) => (
                    nervix_licensing_protocol::document::DocumentType::Succession,
                    value.serial,
                    None,
                ),
            };
            let bytes = serde_json::to_vec(document.signed())
                .change_context(LicenseDecisionError::Trust)?;
            output.push(Self {
                bytes,
                typ,
                serial,
                kid,
            });
        }
        Ok(output)
    }

    pub(super) fn protocol_documents(
        documents: &[Self],
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<TrustDocuments, LicenseDecisionError> {
        if documents.len() > environment.limits().trust_documents.get() {
            return Err(Report::new(LicenseDecisionError::Bounds));
        }
        let mut total = 0usize;
        let mut result = Vec::new();
        for document in documents {
            total = total
                .checked_add(document.bytes.len())
                .ok_or_else(|| Report::new(LicenseDecisionError::Bounds))?;
            if total > environment.limits().bytes.get() {
                return Err(Report::new(LicenseDecisionError::Bounds));
            }
            let read: SignedTrustDocument = serde_json::from_slice(&document.bytes)
                .change_context(LicenseDecisionError::Trust)?;
            result.push(read);
        }
        Ok(TrustDocuments { documents: result })
    }
}

impl LicensingRecord {
    pub fn verify(
        &self,
        identity: &ClusterIdentity,
        environment: &LicensingEnvironment,
    ) -> Option<LicenseVerification> {
        let (activation, identity) = match &self.mode {
            LicensingMode::Free { .. } => return None,
            LicensingMode::Activated(activation) => (activation.as_ref(), identity),
            LicensingMode::Released(release) => (&release.activation, &release.identity),
        };
        Some(
            self.verify_stored(activation, identity, environment)
                .map_err(|report| *report.current_context()),
        )
    }

    fn verify_stored(
        &self,
        activation: &ActivationState,
        identity: &ClusterIdentity,
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<VerifiedLicenseDecision, LicenseDecisionError> {
        let documents = StoredTrustDocument::protocol_documents(&self.trust, environment)?;
        let trust = environment.trust(&documents)?;
        if StoredTrustDocument::retained(&trust)? != self.trust {
            return Err(Report::new(LicenseDecisionError::Metadata));
        }
        let mut binding = activation.binding.protocol_binding(identity);
        let operation = match activation.decision.operation {
            StoredDecisionOperation::Apply => DecisionOperation::Apply,
            StoredDecisionOperation::ClusterKeyRotation => {
                binding.generation = match binding.generation.checked_sub(1) {
                    Some(generation) if generation > 0 => generation,
                    _ => return Err(Report::new(LicenseDecisionError::Metadata)),
                };
                DecisionOperation::CompleteClusterKeyRotation {
                    pending_key: &identity.kid,
                }
            }
        };
        let validated = trust
            .validate_decision(
                &activation.decision.bytes,
                DecisionVerification::Reload,
                DecisionContext {
                    service: environment.service(),
                    binding: &binding,
                    operation,
                    request_id: activation.decision.request_id,
                    committed: None,
                    known_conditions_revisions: &activation
                        .decision
                        .transition_conditions_revisions
                        .iter()
                        .copied()
                        .collect(),
                },
                environment.limits(),
            )
            .map_err(LicenseDecisionError::from_rejection)?;
        let verified = validated.verified();
        let decision = verified.decision();
        validate_coordinates(decision)?;
        let stored = &activation.decision;
        if decision.decision_revision != stored.revision
            || decision.conditions.conditions_revision != stored.conditions_revision
            || transition_revisions(decision)
                .into_iter()
                .collect::<Vec<_>>()
                != stored.transition_conditions_revisions
            || decision.issued_at != stored.issued_at.into_datetime()
        {
            return Err(Report::new(LicenseDecisionError::Metadata));
        }
        Ok(VerifiedLicenseDecision {
            decision: Arc::new(decision.clone()),
            signing_key_revoked: verified.signing_key_revoked(),
            conditions_not_understood: match validated.disposition() {
                DecisionDisposition::Apply { unknown_conditions } => unknown_conditions,
                DecisionDisposition::AlreadyApplied => 0,
            },
        })
    }

    pub fn accept(
        &self,
        identity: &ClusterIdentity,
        binding: &ActivationBinding,
        received: &ReceivedLicenseDecision,
        expected_revision: Option<u64>,
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        self.accept_operation(
            identity,
            binding,
            received,
            expected_revision,
            DecisionOperation::Apply,
            environment,
        )
    }

    pub(super) fn accept_operation(
        &self,
        identity: &ClusterIdentity,
        binding: &ActivationBinding,
        received: &ReceivedLicenseDecision,
        expected_revision: Option<u64>,
        operation: DecisionOperation<'_>,
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        let previous = match &self.mode {
            LicensingMode::Free { .. } | LicensingMode::Released(_) => {
                if expected_revision.is_some() {
                    return Err(Report::new(LicenseDecisionError::NotActivated));
                }
                None
            }
            LicensingMode::Activated(activation) => {
                if expected_revision.is_none() {
                    return Err(Report::new(LicenseDecisionError::AlreadyActivated));
                }
                if &activation.binding != binding {
                    return Err(Report::new(LicenseDecisionError::Binding));
                }
                Some(activation)
            }
        };
        let documents = merge_documents(&self.trust, &received.trust)?;
        let protocol = StoredTrustDocument::protocol_documents(&documents, environment)?;
        let trust = environment.trust(&protocol)?;
        if StoredTrustDocument::retained(&trust)? != documents {
            return Err(Report::new(LicenseDecisionError::Metadata));
        }
        let committed = match (previous, self.verify(identity, environment)) {
            (Some(_), Some(Ok(verified))) => Some(verified),
            _ => None,
        };
        let protocol_binding = binding.protocol_binding(identity);
        let mut known_revisions = BTreeSet::new();
        if let Some(previous) = previous {
            known_revisions.insert(previous.decision.conditions_revision);
            known_revisions.extend(
                previous
                    .decision
                    .transition_conditions_revisions
                    .iter()
                    .copied(),
            );
        }
        let verified = trust
            .verify_decision(
                &received.bytes,
                DecisionVerification::Apply,
                environment.limits(),
            )
            .change_context(DecisionRejection::Envelope)
            .map_err(LicenseDecisionError::from_rejection)?;
        if let Some(previous) = previous {
            if verified.decision().decision_revision < previous.decision.revision {
                return Err(Report::new(LicenseDecisionError::StaleDecision));
            }
            if verified.decision().decision_revision > previous.decision.revision
                && expected_revision != Some(previous.decision.revision)
            {
                return Err(Report::new(LicenseDecisionError::StaleDecision));
            }
        }
        let disposition = verified
            .validate(DecisionContext {
                service: environment.service(),
                binding: &protocol_binding,
                operation,
                request_id: received.request_id,
                committed: committed.as_ref().map(|view| view.decision.as_ref()),
                known_conditions_revisions: &known_revisions,
            })
            .map_err(LicenseDecisionError::from_rejection)?;
        let decision = verified.decision();
        validate_coordinates(decision)?;
        if let Some(previous) = previous {
            if decision.conditions.conditions_revision < previous.decision.conditions_revision
                || decision.issued_at < previous.decision.issued_at.into_datetime()
            {
                return Err(Report::new(LicenseDecisionError::Metadata));
            }
            if decision.decision_revision == previous.decision.revision {
                if disposition == DecisionDisposition::AlreadyApplied {
                    return Ok(self.clone());
                }
                return Err(Report::new(LicenseDecisionError::StaleDecision));
            }
            if decision.decision_revision < previous.decision.revision
                || expected_revision != Some(previous.decision.revision)
            {
                return Err(Report::new(LicenseDecisionError::StaleDecision));
            }
        }
        let decision = received.stored(decision, operation)?;
        let time = self
            .licensing_time
            .max(decision.received_at)
            .max(decision.issued_at);
        Ok(Self {
            licensing_time: time,
            trust: documents,
            mode: LicensingMode::Activated(Box::new(ActivationState {
                binding: binding.clone(),
                decision,
            })),
        })
    }
}

#[derive(PartialEq, Eq, Hash)]
struct TrustIdentity {
    typ: &'static str,
    serial: u64,
    kid: Option<nervix_licensing_protocol::keys::KeyId>,
}

fn merge_documents(
    held: &[StoredTrustDocument],
    received: &[StoredTrustDocument],
) -> error_stack::Result<Vec<StoredTrustDocument>, LicenseDecisionError> {
    let mut documents = IndexMap::<TrustIdentity, StoredTrustDocument>::new();
    for document in held.iter().chain(received) {
        let key = TrustIdentity {
            typ: document.typ.as_str(),
            serial: document.serial,
            kid: document.kid.clone(),
        };
        match documents.entry(key) {
            indexmap::map::Entry::Occupied(entry) => {
                if entry.get().bytes != document.bytes {
                    return Err(Report::new(LicenseDecisionError::Trust));
                }
            }
            indexmap::map::Entry::Vacant(entry) => {
                entry.insert(document.clone());
            }
        }
    }
    Ok(documents.into_values().collect())
}

impl LicensingRecord {
    pub fn install_trust(
        &self,
        received: &[StoredTrustDocument],
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        let documents = merge_documents(&self.trust, received)?;
        let protocol = StoredTrustDocument::protocol_documents(&documents, environment)?;
        let verified = StoredTrustDocument::verified_documents(&protocol, environment)?;
        if verified != documents {
            return Err(Report::new(LicenseDecisionError::Metadata));
        }
        let mut record = self.clone();
        record.trust = documents;
        Ok(record)
    }
}

impl LicensingRecord {
    /// Receipt verification uses the same owner and independent cluster/kind facts as apply.
    pub fn prepare_activation(
        &self,
        identity: &ClusterIdentity,
        kind: ActivationKind,
        license_key_id: String,
        received: &ReceivedLicenseDecision,
        environment: &LicensingEnvironment,
    ) -> error_stack::Result<ActivationBinding, LicenseDecisionError> {
        let documents = merge_documents(&self.trust, &received.trust)?;
        let protocol = StoredTrustDocument::protocol_documents(&documents, environment)?;
        let trust = environment.trust(&protocol)?;
        let verified = trust
            .verify_decision(
                &received.bytes,
                DecisionVerification::Apply,
                environment.limits(),
            )
            .change_context(DecisionRejection::Envelope)
            .map_err(LicenseDecisionError::from_rejection)?;
        if verified.decision().activation.kind != kind {
            return Err(Report::new(LicenseDecisionError::Binding));
        }
        let binding = ActivationBinding::from_verified(verified.decision(), license_key_id)?;
        self.accept(identity, &binding, received, None, environment)?;
        Ok(binding)
    }
}

fn validate_coordinates(decision: &Decision) -> error_stack::Result<(), LicenseDecisionError> {
    for at in [
        Some(decision.issued_at),
        Some(decision.activation.created_at),
        decision.activation.ephemeral_expires_at,
        Some(decision.timing.refresh_after),
        Some(decision.timing.overdue_at),
        decision.timing.valid_until,
        Some(decision.timing.term_ends_at),
        decision.timing.ephemeral_expires_at,
        Some(decision.license.term_starts_at),
        Some(decision.license.term_ends_at),
    ]
    .into_iter()
    .flatten()
    {
        Timestamp::try_from(at).change_context(LicenseDecisionError::Timestamp)?;
    }
    Ok(())
}
