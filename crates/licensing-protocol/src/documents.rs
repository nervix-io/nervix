//! The current signed decision and trust-document payloads.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Exact document fields, typed status, timing, binding and transition payloads.
//! - **Depends on.** Public identities, UTC values, condition values and document signing contracts.
//! - **Must not know.** Runtime policy, graphs, consensus, databases, clocks or HTTP.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::{
    conditions::{ActivationKind, OperatingConditions, Quota, UsageConditions},
    document::{Document, DocumentType, SignatureSet},
    keys::{KeyId, PublicKey},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    LicenseDecision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationKind {
    WorkingKeyAuthorization,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevocationKind {
    WorkingKeyRevocation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuccessionKind {
    RootSuccession,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkingKeyUse {
    Decision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkingKeyAuthorization {
    pub doc: AuthorizationKind,
    pub protocol: u8,
    pub serial: u64,
    pub issued_at: DateTime<Utc>,
    pub root_epoch: u64,
    pub kid: KeyId,
    pub public_jwk: PublicKey,
    pub uses: Vec<WorkingKeyUse>,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
}

impl Document for WorkingKeyAuthorization {
    const TYPE: DocumentType = DocumentType::WorkingKey;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkingKeyRevocation {
    pub doc: RevocationKind,
    pub protocol: u8,
    pub serial: u64,
    pub issued_at: DateTime<Utc>,
    pub root_epoch: u64,
    pub kid: KeyId,
    pub revoked_at: DateTime<Utc>,
    pub reason: String,
}

impl Document for WorkingKeyRevocation {
    const TYPE: DocumentType = DocumentType::Revocation;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootSuccession {
    pub doc: SuccessionKind,
    pub protocol: u8,
    pub serial: u64,
    pub issued_at: DateTime<Utc>,
    pub from_epoch: u64,
    pub to_epoch: u64,
    pub roots: Vec<PublicKey>,
    pub threshold: std::num::NonZeroUsize,
    pub effective_at: DateTime<Utc>,
}

impl Document for RootSuccession {
    const TYPE: DocumentType = DocumentType::Succession;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootSet {
    pub epoch: u64,
    pub roots: Vec<PublicKey>,
    pub threshold: std::num::NonZeroUsize,
}

/// Anchors are a caller-owned immutable build input, never learned from an unsigned response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddedTrustBundle {
    pub epochs: Vec<RootSet>,
    pub successions: Vec<SignatureSet>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Activation {
    pub activation_id: Uuid,
    pub cluster_uid: Uuid,
    pub cluster_key_kid: KeyId,
    pub generation: u64,
    pub kind: ActivationKind,
    pub created_at: DateTime<Utc>,
    pub ephemeral_expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanVersion {
    pub plan_code: String,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct License {
    pub license_id: Uuid,
    pub organization_id: Uuid,
    pub term_starts_at: DateTime<Utc>,
    pub term_ends_at: DateTime<Utc>,
    pub plan: PlanVersion,
    pub assignment_revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionStatus {
    Active,
    TermEnded,
    Suspended,
    Revoked,
    Released,
    Transferred,
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionConditions {
    pub conditions_revision: u64,
    pub usage: UsageConditions,
    pub operating: OperatingConditions,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionTiming {
    pub refresh_after: DateTime<Utc>,
    pub overdue_at: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub term_ends_at: DateTime<Utc>,
    pub ephemeral_expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransitionTrigger {
    Decision { decision_id: Uuid },
    Tick { decision_id: Uuid },
    Release { request_id: Uuid },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionReason {
    PlanChanged,
    TermEnded,
    Suspended,
    Revoked,
    Released,
    Transferred,
    OfflineLapse,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AffectedQuota {
    pub condition: String,
    pub previous: Quota,
    pub target: Quota,
    pub usage: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transition {
    pub transition_id: Uuid,
    pub from_conditions_revision: u64,
    pub to_conditions_revision: u64,
    pub started_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub trigger: TransitionTrigger,
    pub reason: TransitionReason,
    pub affected: Vec<AffectedQuota>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Notice {
    pub code: String,
    pub params: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub doc: DecisionKind,
    pub protocol: u8,
    pub decision_id: Uuid,
    pub decision_revision: u64,
    pub issued_at: DateTime<Utc>,
    pub service: Url,
    pub activation: Activation,
    pub license: License,
    pub status: DecisionStatus,
    pub conditions: DecisionConditions,
    pub timing: DecisionTiming,
    pub transitions: Vec<Transition>,
    pub notices: Vec<Notice>,
    pub in_reply_to: Uuid,
}

impl Document for Decision {
    const TYPE: DocumentType = DocumentType::Decision;
}

/// Trust response members retain their exact original signed form. Each member is a root-signed
/// signature set; its document type is established only by signature verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustDocuments {
    pub documents: Vec<SignedTrustDocument>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SignedTrustDocument {
    Compact(String),
    Signatures(SignatureSet),
}

impl SignedTrustDocument {
    pub fn signature_set(&self) -> SignatureSet {
        match self {
            Self::Compact(compact) => SignatureSet {
                signatures: vec![compact.clone()],
            },
            Self::Signatures(set) => set.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionResponse {
    pub decision: String,
    pub trust: TrustDocuments,
    pub server_time: DateTime<Utc>,
}
