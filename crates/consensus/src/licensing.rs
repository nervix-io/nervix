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

mod archive;
#[cfg(test)]
pub(crate) mod tests;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_licensing_protocol::{document::DocumentType, impact::UsageSnapshot, keys::KeyId};
use nervix_models::Timestamp;
use nervix_primitives::sync::Arc;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ClusterIdentity, Observer};

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
pub enum LicensingMode {
    Free,
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
