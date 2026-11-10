//! Permanent cluster identity and its first committed installation.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Validated identity records, private-key ownership and idempotent installation.
//! - **Depends on.** The cryptography boundary, shared licensing key types and consensus state.
//! - **Must not know.** Bootstrap timing, licensing policy, sessions, backups or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns replicated cluster identity")
)]

pub(crate) mod archive;
mod signer;
#[cfg(test)]
pub(crate) mod tests;

use std::{fmt, num::NonZeroU64};

use error_stack::{Report, ResultExt as _};
use nervix_crypto::{ecdsa::EcdsaP256KeyPair, random};
use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass};
use nervix_licensing_protocol::{
    document::DocumentSigner,
    keys::{KeyId, PublicKey},
};
use nervix_models::Timestamp;
use nervix_primitives::sync::Arc;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
pub use signer::ClusterSigner;
use thiserror::Error;
use uuid::{Uuid, Variant, Version};
use zeroize::Zeroize as _;

use crate::{
    AppliedConsensusCommand, ConsensusCommand, ConsensusError, Observer, Proposer,
    StateMachineChanges, StateMachineData,
};

#[derive(Debug, PartialEq, Eq, Error)]
pub enum ClusterIdentityError {
    #[error("the cluster UID is not a version-4 RFC 4122 UUID")]
    Uid,
    #[error("failed to generate the cluster private key")]
    Generate,
    #[error("failed to import the cluster private key")]
    PrivateKey,
    #[error("the cluster public key disagrees with its private key")]
    PublicKeyMismatch,
    #[error("the cluster key identifier disagrees with its public key")]
    KeyIdMismatch,
    #[error("cluster identity work could not acquire its memory reservation")]
    Admission,
    #[error("cluster identity work was refused by the executor")]
    Execution,
    #[error("cluster identity work was cancelled")]
    Cancelled,
    #[error("failed to sign the cluster document")]
    Sign,
}

/// The public projection an observer may read. It carries no private-key owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterIdentity {
    pub cluster_uid: Uuid,
    pub created_at: Timestamp,
    pub key_generation: NonZeroU64,
    pub ever_activated: bool,
    pub kid: KeyId,
    pub public_jwk: PublicKey,
}

/// One replicated singleton. Its current, retired and pending private allocations clear on drop.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize)]
#[serde(try_from = "ClusterIdentityFields")]
pub struct ClusterIdentityRecord {
    #[rkyv(with = archive::UuidBytes)]
    cluster_uid: Uuid,
    created_at: Timestamp,
    key_generation: NonZeroU64,
    ever_activated: bool,
    current_key: ClusterKey,
    retired_key: Option<RetiredClusterKey>,
    pending_key: Option<PendingClusterKey>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterIdentityFields {
    cluster_uid: Uuid,
    created_at: Timestamp,
    key_generation: NonZeroU64,
    ever_activated: bool,
    current_key: ClusterKey,
    retired_key: Option<RetiredClusterKey>,
    pending_key: Option<PendingClusterKey>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[serde(deny_unknown_fields)]
struct ClusterKey {
    #[rkyv(with = archive::KeyIdentity)]
    kid: KeyId,
    #[rkyv(with = archive::PublicCoordinates)]
    public_jwk: PublicKey,
    private_pkcs8: ClusterPrivateKey,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
struct RetiredClusterKey {
    #[serde(flatten)]
    key: ClusterKey,
    retired_at: Timestamp,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
struct PendingClusterKey {
    #[serde(flatten)]
    key: ClusterKey,
    requested_at: Timestamp,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
struct ClusterPrivateKey(Vec<u8>);

impl Drop for ClusterPrivateKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for ClusterIdentityRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClusterIdentityRecord")
            .field("cluster_uid", &self.cluster_uid)
            .field("created_at", &self.created_at)
            .field("key_generation", &self.key_generation)
            .field("kid", &self.current_key.kid)
            .finish_non_exhaustive()
    }
}

impl ClusterKey {
    fn validate(&self) -> Result<(), Report<ClusterIdentityError>> {
        let signer = DocumentSigner::from_pkcs8(&self.private_pkcs8.0)
            .change_context(ClusterIdentityError::PrivateKey)?;
        if signer.public_key() != &self.public_jwk {
            return Err(Report::new(ClusterIdentityError::PublicKeyMismatch));
        }
        if signer.key_id() != &self.kid {
            return Err(Report::new(ClusterIdentityError::KeyIdMismatch));
        }
        Ok(())
    }
}

impl ClusterIdentityRecord {
    pub(crate) fn mark_activated(&mut self) {
        self.ever_activated = true;
    }

    pub(crate) fn cluster_uid(&self) -> Uuid {
        self.cluster_uid
    }

    pub(crate) fn public(&self) -> ClusterIdentity {
        ClusterIdentity {
            cluster_uid: self.cluster_uid,
            created_at: self.created_at,
            key_generation: self.key_generation,
            ever_activated: self.ever_activated,
            kid: self.current_key.kid.clone(),
            public_jwk: self.current_key.public_jwk.clone(),
        }
    }

    fn validate(&self) -> Result<(), Report<ClusterIdentityError>> {
        if self.cluster_uid.get_version() != Some(Version::Random)
            || self.cluster_uid.get_variant() != Variant::RFC4122
        {
            return Err(Report::new(ClusterIdentityError::Uid));
        }
        self.current_key.validate()?;
        if let Some(retired) = &self.retired_key {
            retired.key.validate()?;
        }
        if let Some(pending) = &self.pending_key {
            pending.key.validate()?;
        }
        Ok(())
    }

    async fn generate(
        executor: &Executor,
        created_at: Timestamp,
    ) -> Result<Self, Report<ClusterIdentityError>> {
        let reservation = executor
            .try_reserve(
                MemoryClass::Management,
                executor.limits().management_event_bytes.as_u64(),
            )
            .change_context(ClusterIdentityError::Admission)?;
        executor
            .run_cpu(
                CpuClass::Control,
                reservation,
                move |_reservation, cancellation| Self::generate_key(created_at, cancellation),
            )
            .await
            .change_context(ClusterIdentityError::Execution)?
    }

    fn generate_key(
        created_at: Timestamp,
        cancellation: &Cancellation,
    ) -> Result<Self, Report<ClusterIdentityError>> {
        cancellation
            .check()
            .change_context(ClusterIdentityError::Cancelled)?;
        let cluster_uid = uuid::Builder::from_random_bytes(random::bytes()).into_uuid();
        let private =
            EcdsaP256KeyPair::generate_pkcs8().change_context(ClusterIdentityError::Generate)?;
        cancellation
            .check()
            .change_context(ClusterIdentityError::Cancelled)?;
        let signer = DocumentSigner::from_pkcs8(private.as_ref())
            .change_context(ClusterIdentityError::PrivateKey)?;
        cancellation
            .check()
            .change_context(ClusterIdentityError::Cancelled)?;
        Ok(Self {
            cluster_uid,
            created_at,
            key_generation: NonZeroU64::MIN,
            ever_activated: false,
            current_key: ClusterKey {
                kid: signer.key_id().clone(),
                public_jwk: signer.public_key().clone(),
                private_pkcs8: ClusterPrivateKey(private.as_ref().to_vec()),
            },
            retired_key: None,
            pending_key: None,
        })
    }
}

impl TryFrom<ClusterIdentityFields> for ClusterIdentityRecord {
    type Error = Report<ClusterIdentityError>;

    fn try_from(fields: ClusterIdentityFields) -> Result<Self, Self::Error> {
        let record = Self {
            cluster_uid: fields.cluster_uid,
            created_at: fields.created_at,
            key_generation: fields.key_generation,
            ever_activated: fields.ever_activated,
            current_key: fields.current_key,
            retired_key: fields.retired_key,
            pending_key: fields.pending_key,
        };
        record.validate()?;
        Ok(record)
    }
}

pub(super) fn install(
    state: &mut StateMachineData,
    identity: &ClusterIdentityRecord,
) -> AppliedConsensusCommand {
    if state.cluster_identity.is_none() {
        state.licensing = Some(Arc::new(crate::LicensingRecord::free(identity.created_at)));
        state.cluster_identity = Some(Arc::new(identity.clone()));
    }
    AppliedConsensusCommand::applied(StateMachineChanges::default())
}

impl Observer {
    pub fn cluster_identity(&self) -> Option<ClusterIdentity> {
        self.inner
            .store
            .inner
            .state()
            .cluster_identity
            .as_ref()
            .map(|record| record.public())
    }
}

impl Proposer {
    /// Generate one unused candidate and propose it. The first committed candidate wins.
    pub async fn install_cluster_identity(
        &self,
        created_at: Timestamp,
    ) -> error_stack::Result<(), ConsensusError> {
        let identity =
            ClusterIdentityRecord::generate(&self.inner.store.inner.executor, created_at)
                .await
                .change_context(ConsensusError::ClusterIdentity)?;
        let response = self
            .inner
            .client_write(ConsensusCommand::InstallClusterIdentity {
                identity: Box::new(identity),
            })
            .await?;
        Self::applied_response(response.data)
    }
}
