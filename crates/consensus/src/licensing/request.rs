//! Durable signed requests for an operation whose answer may be lost after remote commit.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Bounded original request bytes and their challenge lifetime, with redacted diagnostics.
//! - **Depends on.** Protocol request verification, the shared key vocabulary and archive codecs.
//! - **Must not know.** HTTP, retry scheduling, leadership or graph execution.

use error_stack::{Report, ResultExt as _};
use nervix_licensing_protocol::{
    document::DocumentLimits,
    requests::{Request, RequestSigner},
};
use nervix_models::Timestamp;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize as _;

use super::LicenseDecisionError;
use crate::ClusterIdentity;

/// The exact request is committed before its first send. Its challenge may have expired when a
/// new leader replays it: the service checks saved operation answers before challenge admission.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedLicensingRequest {
    body: String,
    pub expires_at: Timestamp,
}

impl std::fmt::Debug for PreparedLicensingRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedLicensingRequest")
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl Drop for PreparedLicensingRequest {
    fn drop(&mut self) {
        self.body.zeroize();
    }
}

impl PreparedLicensingRequest {
    pub fn new(
        body: String,
        expires_at: Timestamp,
        limits: DocumentLimits,
    ) -> error_stack::Result<Self, LicenseDecisionError> {
        let prepared = Self { body, expires_at };
        if prepared.body.len() > limits.bytes.get() || prepared.body.is_empty() {
            return Err(Report::new(LicenseDecisionError::Bounds));
        }
        Ok(prepared)
    }

    pub fn body(&self) -> &str {
        &self.body
    }

    pub(crate) fn stored_limits(
        &self,
    ) -> error_stack::Result<DocumentLimits, LicenseDecisionError> {
        let bytes = std::num::NonZeroUsize::new(self.body.len())
            .ok_or_else(|| Report::new(LicenseDecisionError::Bounds))?;
        Ok(DocumentLimits {
            bytes,
            signatures: bytes,
            trust_documents: bytes,
        })
    }

    pub(super) fn verify(
        &self,
        identity: &ClusterIdentity,
        limits: DocumentLimits,
    ) -> error_stack::Result<Request, LicenseDecisionError> {
        let request = Request::verify(
            &self.body,
            RequestSigner::Bound(&identity.public_jwk),
            limits,
        )
        .change_context(LicenseDecisionError::Request)?;
        let common = request.common();
        if common.protocol != 1
            || common.cluster_uid != identity.cluster_uid
            || common.key_generation != identity.key_generation.get()
            || common.sent_at >= self.expires_at.into_datetime()
        {
            return Err(Report::new(LicenseDecisionError::Binding));
        }
        Ok(request)
    }

    pub(crate) fn verify_rotation(
        &self,
        identity: &ClusterIdentity,
        next: &ClusterIdentity,
        request_id: uuid::Uuid,
        limits: DocumentLimits,
    ) -> error_stack::Result<Request, LicenseDecisionError> {
        let request = self.verify(identity, limits)?;
        let Request::RotateKey {
            common,
            new_key,
            activation_id,
            proof,
            ..
        } = &request
        else {
            return Err(Report::new(LicenseDecisionError::Request));
        };
        if common.request_id != request_id || new_key != &next.public_jwk {
            return Err(Report::new(LicenseDecisionError::Binding));
        }
        let proof: nervix_licensing_protocol::requests::KeyRotationProof =
            nervix_licensing_protocol::document::verify(proof, new_key, limits)
                .change_context(LicenseDecisionError::Request)?;
        if proof.challenge != common.challenge
            || proof.cluster_uid != identity.cluster_uid
            || proof.activation_id != *activation_id
            || proof.new_kid != next.kid
        {
            return Err(Report::new(LicenseDecisionError::Binding));
        }
        Ok(request)
    }
}
