//! Generation-fenced cluster-key rotation and its retained remote request.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Pending keys, exact request replay, local completion and atomic activated completion.
//! - **Depends on.** Identity key ownership, signed protocol validation and consensus state.
//! - **Must not know.** HTTP, retry cadence, licensing policy or graph execution.

use std::num::NonZeroU64;

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_execution::{CpuClass, MemoryClass};
use nervix_licensing_protocol::document::DocumentSigner;
use nervix_models::Timestamp;
use nervix_primitives::sync::Arc;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ClusterIdentity, ClusterIdentityError, ClusterIdentityRecord, ClusterKey, ClusterPrivateKey,
    RetiredClusterKey,
};
use crate::{ConsensusCommand, ConsensusError, PreparedLicensingRequest, Proposer};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingClusterKey {
    pub(super) reference: nervix_models::CommandExecutionReference,
    pub(super) key: ClusterKey,
    pub(super) requested_at: Timestamp,
    pub(super) generation: NonZeroU64,
    #[rkyv(with = super::archive::UuidBytes)]
    pub(super) request_id: Uuid,
    pub(super) prepared: Option<PreparedLicensingRequest>,
}

impl std::fmt::Debug for PendingClusterKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingClusterKey")
            .field("kid", &self.key.kid)
            .field("generation", &self.generation)
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

/// Public operation facts; private material stays with the identity's signing owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterKeyRotation {
    pub reference: nervix_models::CommandExecutionReference,
    pub request_id: Uuid,
    pub next_identity: ClusterIdentity,
    pub prepared: Option<PreparedLicensingRequest>,
}

impl PendingClusterKey {
    pub(super) fn validate(
        &self,
        record: &ClusterIdentityRecord,
    ) -> error_stack::Result<(), ClusterIdentityError> {
        self.key.validate()?;
        let expected = record
            .key_generation
            .get()
            .checked_add(1)
            .ok_or_else(|| Report::new(ClusterIdentityError::GenerationExhausted))?;
        if self.generation.get() != expected || self.key.kid == record.current_key.kid {
            return Err(Report::new(ClusterIdentityError::StaleRotation));
        }
        if let Some(prepared) = &self.prepared {
            let mut next = record.public();
            next.kid = self.key.kid.clone();
            next.public_jwk = self.key.public_jwk.clone();
            next.key_generation = self.generation;
            prepared
                .verify_rotation(
                    &record.public(),
                    &next,
                    self.request_id,
                    prepared
                        .stored_limits()
                        .change_context(ClusterIdentityError::StaleRotation)?,
                )
                .change_context(ClusterIdentityError::StaleRotation)?;
        }
        Ok(())
    }

    fn same_operation(&self, candidate: &Self) -> bool {
        self.reference == candidate.reference
            && self.request_id == candidate.request_id
            && self.key == candidate.key
            && self.generation == candidate.generation
            && self.requested_at == candidate.requested_at
    }
}

impl ClusterIdentityRecord {
    fn check_rotation_basis(
        &self,
        basis: &ClusterIdentity,
    ) -> error_stack::Result<(), ClusterIdentityError> {
        if self.cluster_uid != basis.cluster_uid
            || self.key_generation != basis.key_generation
            || self.current_key.kid != basis.kid
        {
            return Err(Report::new(ClusterIdentityError::StaleRotation));
        }
        Ok(())
    }

    pub(crate) fn pending_rotation(&self) -> Option<ClusterKeyRotation> {
        let pending = self.pending_key.as_ref()?;
        let mut next_identity = self.public();
        next_identity.key_generation = pending.generation;
        next_identity.kid = pending.key.kid.clone();
        next_identity.public_jwk = pending.key.public_jwk.clone();
        Some(ClusterKeyRotation {
            reference: pending.reference.clone(),
            request_id: pending.request_id,
            next_identity,
            prepared: pending.prepared.clone(),
        })
    }

    pub(super) fn begin_rotation(
        &self,
        pending: &PendingClusterKey,
        local: bool,
    ) -> error_stack::Result<Self, ClusterIdentityError> {
        if self
            .retired_key
            .as_ref()
            .is_some_and(|key| key.request_id == pending.request_id)
        {
            if self
                .retired_key
                .as_ref()
                .is_some_and(|key| key.reference != pending.reference)
            {
                return Err(Report::new(ClusterIdentityError::StaleRotation));
            }
            return Ok(self.clone());
        }
        if let Some(current) = &self.pending_key {
            if current.same_operation(pending) {
                return Ok(self.clone());
            }
            return Err(Report::new(ClusterIdentityError::RotationInProgress));
        }
        pending.validate(self)?;
        let mut record = self.clone();
        record.pending_key = Some(pending.clone());
        if local {
            record = record.complete_rotation(pending.request_id)?;
        }
        Ok(record)
    }

    pub(crate) fn complete_rotation(
        &self,
        request_id: Uuid,
    ) -> error_stack::Result<Self, ClusterIdentityError> {
        let mut record = self.clone();
        let pending = record
            .pending_key
            .take()
            .ok_or_else(|| Report::new(ClusterIdentityError::NoPendingRotation))?;
        if pending.request_id != request_id {
            return Err(Report::new(ClusterIdentityError::StaleRotation));
        }
        pending.validate(self)?;
        record.retired_key = Some(RetiredClusterKey {
            reference: pending.reference,
            key: std::mem::replace(&mut record.current_key, pending.key),
            retired_at: pending.requested_at,
            request_id,
        });
        record.key_generation = pending.generation;
        Ok(record)
    }

    pub(crate) fn stage_rotation_request(
        &self,
        request_id: Uuid,
        prepared: &PreparedLicensingRequest,
    ) -> error_stack::Result<Self, ClusterIdentityError> {
        let mut record = self.clone();
        let pending = record
            .pending_key
            .as_mut()
            .ok_or_else(|| Report::new(ClusterIdentityError::NoPendingRotation))?;
        if pending.request_id != request_id {
            return Err(Report::new(ClusterIdentityError::StaleRotation));
        }
        pending.prepared = Some(prepared.clone());
        Ok(record)
    }

    pub(crate) fn abandon_rotation(
        &self,
        request_id: Uuid,
    ) -> error_stack::Result<Self, ClusterIdentityError> {
        let Some(pending) = &self.pending_key else {
            return Ok(self.clone());
        };
        if pending.request_id != request_id {
            return Err(Report::new(ClusterIdentityError::StaleRotation));
        }
        let mut record = self.clone();
        record.pending_key = None;
        Ok(record)
    }

    pub(super) fn prepare_rotation(
        &self,
        basis: &ClusterIdentity,
        reference: &nervix_models::CommandExecutionReference,
        request_id: Uuid,
        requested_at: Timestamp,
    ) -> error_stack::Result<Option<PendingClusterKey>, ClusterIdentityError> {
        if self
            .retired_key
            .as_ref()
            .is_some_and(|key| key.request_id == request_id)
        {
            if self
                .retired_key
                .as_ref()
                .is_some_and(|key| &key.reference != reference)
            {
                return Err(Report::new(ClusterIdentityError::StaleRotation));
            }
            return Ok(None);
        }
        if let Some(pending) = &self.pending_key {
            if pending.request_id == request_id && &pending.reference == reference {
                return Ok(Some(pending.clone()));
            }
            return Err(Report::new(ClusterIdentityError::RotationInProgress));
        }
        self.check_rotation_basis(basis)?;
        let generation = self
            .key_generation
            .get()
            .checked_add(1)
            .ok_or_else(|| Report::new(ClusterIdentityError::GenerationExhausted))?;
        let private = nervix_crypto::ecdsa::EcdsaP256KeyPair::generate_pkcs8()
            .change_context(ClusterIdentityError::Generate)?;
        let signer = DocumentSigner::from_pkcs8(private.as_ref())
            .change_context(ClusterIdentityError::PrivateKey)?;
        Ok(Some(PendingClusterKey {
            reference: reference.clone(),
            key: ClusterKey {
                kid: signer.key_id().clone(),
                public_jwk: signer.public_key().clone(),
                private_pkcs8: ClusterPrivateKey(private.as_ref().to_vec()),
            },
            requested_at,
            generation: NonZeroU64::new(generation)
                .verified("checked increment of a nonzero generation remains nonzero"),
            request_id,
            prepared: None,
        }))
    }
}

impl Proposer {
    pub async fn begin_cluster_key_rotation(
        &self,
        basis: ClusterIdentity,
        reference: nervix_models::CommandExecutionReference,
        request_id: Uuid,
        requested_at: Timestamp,
    ) -> error_stack::Result<(), ConsensusError> {
        let state = self.inner.store.inner.state();
        if state.licensing_command_outcome(&reference).is_some() {
            return Ok(());
        }
        let identity = state
            .cluster_identity
            .ok_or_else(|| Report::new(ClusterIdentityError::NoPendingRotation))
            .change_context(ConsensusError::ClusterIdentity)?;
        let executor = self.inner.store.inner.executor.clone();
        let reservation = executor
            .try_reserve(
                MemoryClass::Management,
                executor.limits().management_event_bytes.as_u64(),
            )
            .change_context(ClusterIdentityError::Admission)
            .change_context(ConsensusError::ClusterIdentity)?;
        let expected = basis.clone();
        let pending = executor
            .run_cpu(
                CpuClass::Control,
                reservation,
                move |_reservation, cancellation| {
                    cancellation
                        .check()
                        .change_context(ClusterIdentityError::Cancelled)?;
                    let pending =
                        identity.prepare_rotation(&basis, &reference, request_id, requested_at)?;
                    cancellation
                        .check()
                        .change_context(ClusterIdentityError::Cancelled)?;
                    Ok::<_, Report<ClusterIdentityError>>(pending)
                },
            )
            .await
            .change_context(ClusterIdentityError::Execution)
            .change_context(ConsensusError::ClusterIdentity)?
            .change_context(ConsensusError::ClusterIdentity)?;
        let Some(pending_key) = pending else {
            return Ok(());
        };
        let response = self
            .inner
            .client_write(ConsensusCommand::BeginClusterKeyRotation {
                identity: Box::new(expected),
                pending_key: Box::new(pending_key),
            })
            .await?;
        Self::applied_response(response.data)
    }

    pub async fn stage_cluster_key_rotation_request(
        &self,
        request_id: Uuid,
        request: PreparedLicensingRequest,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::PrepareClusterKeyRotationRequest {
                request_id,
                request: Box::new(request),
            })
            .await?;
        Self::applied_response(response.data)
    }

    pub async fn abandon_cluster_key_rotation(
        &self,
        reference: nervix_models::CommandExecutionReference,
        request_id: Uuid,
        code: nervix_licensing_vocabulary::ProblemCode,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::AbandonClusterKeyRotation {
                reference,
                request_id,
                code,
            })
            .await?;
        Self::applied_response(response.data)
    }

    pub async fn complete_cluster_key_rotation(
        &self,
        request_id: Uuid,
        decision: crate::ReceivedLicenseDecision,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::CompleteClusterKeyRotation {
                request_id,
                decision: Box::new(decision),
            })
            .await?;
        Self::applied_response(response.data)
    }
}

pub(crate) fn begin(
    state: &mut crate::StateMachineData,
    basis: &ClusterIdentity,
    pending: &PendingClusterKey,
) -> crate::AppliedConsensusCommand {
    if state
        .licensing_command_outcome(&pending.reference)
        .is_some()
    {
        return crate::AppliedConsensusCommand::applied(crate::StateMachineChanges::default());
    }
    let Some(identity) = &state.cluster_identity else {
        return failure(ClusterIdentityError::NoPendingRotation);
    };
    let same_operation = identity
        .retired_key
        .as_ref()
        .is_some_and(|key| key.request_id == pending.request_id)
        || identity
            .pending_key
            .as_ref()
            .is_some_and(|key| key.request_id == pending.request_id);
    if !same_operation && let Err(error) = identity.check_rotation_basis(basis) {
        return failure(*error.current_context());
    }
    let local = !matches!(
        state.licensing.as_ref().map(|record| &record.mode),
        Some(crate::LicensingMode::Activated(_))
    );
    match identity.begin_rotation(pending, local) {
        Ok(identity) => {
            state.cluster_identity = Some(Arc::new(identity));
            if local {
                state.record_licensing_command_outcome(
                    &pending.reference,
                    crate::LicensingCommandOutcome::Applied,
                );
            }
            crate::AppliedConsensusCommand::applied(crate::StateMachineChanges::default())
        }
        Err(error) => failure(*error.current_context()),
    }
}

pub(crate) fn failure(reason: ClusterIdentityError) -> crate::AppliedConsensusCommand {
    crate::AppliedConsensusCommand::conflict(crate::ConsensusConflict::ClusterKeyRotation {
        reason,
    })
}
