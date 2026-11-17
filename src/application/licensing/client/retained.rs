//! Durable request projections for cluster key rotation and release.
//!
//! Layer: control plane.
//! - **Owns.** Signing capability selection and committing exact bytes before remote submission.
//! - **Depends on.** Public consensus operation facts and the shared protocol request vocabulary.
//! - **Must not know.** Private keys, storage encoding or transport implementation.

use error_stack::{Report, ResultExt as _};
use nervix_consensus::{
    ClusterIdentity, ClusterLicensingState, ClusterSigner, LicensingMode, PreparedLicensingRequest,
    ReleaseConfirmation,
};
use nervix_execution::MemoryClass;
use nervix_licensing_protocol::requests::{CommittedState, KeyRotationProof, RequestCommon};
use uuid::Uuid;

use super::{LicensingClient, LicensingRequestError, Operation, SignedRequest};

impl Operation {
    pub(super) fn prepared(&self) -> Option<&PreparedLicensingRequest> {
        match self {
            Self::RotateKey { rotation, .. } => rotation.prepared.as_ref(),
            Self::Release { prepared, .. } => prepared.as_ref(),
            Self::Create { .. } | Self::Refresh { .. } => None,
        }
    }

    pub(super) fn retains_request(&self) -> bool {
        matches!(self, Self::RotateKey { .. } | Self::Release { .. })
    }

    pub(super) fn identity<'a>(
        &'a self,
        cluster: &'a ClusterLicensingState,
    ) -> &'a ClusterIdentity {
        match self {
            Self::Release { identity, .. } => identity,
            _ => &cluster.identity,
        }
    }

    pub(super) async fn signer(
        &self,
        client: &LicensingClient<'_>,
        request_id: Uuid,
    ) -> Result<ClusterSigner, Report<LicensingRequestError>> {
        let proposer = &client.service.inner.consensus;
        let signer = match self {
            Self::Release { .. } => proposer.released_cluster_signer(request_id).await,
            _ => proposer.cluster_signer().await,
        }
        .change_context(LicensingRequestError::Signing)?;
        signer.ok_or_else(|| Report::new(LicensingRequestError::Signing))
    }

    pub(super) async fn retain(
        &self,
        client: &LicensingClient<'_>,
        request_id: Uuid,
        signed: &SignedRequest,
    ) -> Result<(), Report<LicensingRequestError>> {
        if !self.retains_request() {
            return Ok(());
        }
        let prepared = PreparedLicensingRequest::new(
            signed.body.to_string(),
            signed.expires_at,
            client.environment.limits(),
        )
        .change_context(LicensingRequestError::Bounds)?;
        let proposer = &client.service.inner.consensus;
        match self {
            Self::RotateKey { .. } => {
                proposer
                    .stage_cluster_key_rotation_request(request_id, prepared)
                    .await
            }
            Self::Release { .. } => {
                proposer
                    .prepare_license_release_request(request_id, prepared)
                    .await
            }
            _ => unreachable!("only durable operations retain signed requests"),
        }
        .change_context(LicensingRequestError::Consensus)
    }

    pub(super) async fn rotation_proof(
        &self,
        client: &LicensingClient<'_>,
        common: &RequestCommon,
    ) -> Result<String, Report<LicensingRequestError>> {
        let Self::RotateKey {
            activation_id,
            rotation,
            ..
        } = self
        else {
            unreachable!("only key rotation asks for a possession proof");
        };
        let signer = client
            .service
            .inner
            .consensus
            .pending_cluster_signer(common.request_id)
            .await
            .change_context(LicensingRequestError::Signing)?
            .ok_or_else(|| Report::new(LicensingRequestError::Signing))?;
        let executor = client.service.inner.runtime.executor();
        let reservation = executor
            .try_reserve(
                MemoryClass::Management,
                executor.limits().management_event_bytes.as_u64(),
            )
            .change_context(LicensingRequestError::Admission)?;
        signer
            .sign(
                KeyRotationProof {
                    challenge: common.challenge.clone(),
                    cluster_uid: common.cluster_uid,
                    activation_id: *activation_id,
                    new_kid: rotation.next_identity.kid.clone(),
                },
                client.environment.limits(),
                reservation,
            )
            .await
            .change_context(LicensingRequestError::Signing)
    }
}

pub(in crate::application::licensing) fn rotation(
    cluster: &ClusterLicensingState,
) -> Option<Operation> {
    let LicensingMode::Activated(activation) = &cluster.record.mode else {
        return None;
    };
    Some(Operation::RotateKey {
        activation_id: activation.binding.activation_id,
        state: CommittedState {
            applied_decision_revision: activation.decision.revision,
            applied_conditions_revision: activation.decision.conditions_revision,
            licensing_time: cluster.record.licensing_time.into_datetime(),
        },
        rotation: cluster.rotation.clone()?,
    })
}

pub(in crate::application::licensing) fn release(
    cluster: &ClusterLicensingState,
) -> Option<Operation> {
    let LicensingMode::Released(release) = &cluster.record.mode else {
        return None;
    };
    let ReleaseConfirmation::Awaiting(awaiting) = &release.confirmation else {
        return None;
    };
    Some(Operation::Release {
        activation_id: release.activation.binding.activation_id,
        state: CommittedState {
            applied_decision_revision: release.activation.decision.revision,
            applied_conditions_revision: release.activation.decision.conditions_revision,
            licensing_time: release.released_at.into_datetime(),
        },
        identity: release.identity.clone(),
        grants_until: release.grants_until.unwrap_or(release.released_at),
        prepared: awaiting.prepared.clone(),
    })
}
