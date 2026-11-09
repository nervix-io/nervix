//! Signing-only access granted to a leader's coordinator.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** A private signing handle and bounded execution of typed document signing.
//! - **Depends on.** The shared protocol signer, execution reservations and consensus leadership.
//! - **Must not know.** Licensing requests, HTTP, retry cadence or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns cluster signing access")
)]

use std::fmt;

use error_stack::{Report, ResultExt as _};
use nervix_execution::{CpuClass, Executor, MemoryClass, Reservation};
use nervix_licensing_protocol::document::{Document, DocumentLimits, DocumentSigner};
use nervix_primitives::sync::Arc;

use super::ClusterIdentityError;
use crate::{ConsensusError, LeadershipObservation, Proposer};

struct SigningIdentity {
    signer: DocumentSigner,
    executor: Executor,
}

/// A leader coordinator's signing capability. It has no private-key export operation.
/// The coordinator owns its leadership lifetime and ends use when that tenure ends.
/// The maintained signer clears its owned private DER and native key on drop.
#[derive(Clone)]
pub struct ClusterSigner {
    inner: Arc<SigningIdentity>,
}

impl fmt::Debug for ClusterSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClusterSigner")
            .field("kid", self.inner.signer.key_id())
            .finish()
    }
}

impl ClusterSigner {
    pub(super) fn from_record(
        record: &super::ClusterIdentityRecord,
        executor: Executor,
    ) -> Result<Self, Report<ClusterIdentityError>> {
        let signer = DocumentSigner::from_pkcs8(&record.current_key.private_pkcs8.0)
            .change_context(ClusterIdentityError::PrivateKey)?;
        Ok(Self {
            inner: Arc::new(SigningIdentity { signer, executor }),
        })
    }

    /// Sign within the caller's admitted document working set. The reservation must cover the
    /// document and the shared protocol encoder's scratch and output under `limits`.
    pub async fn sign<T: Document + Send + 'static>(
        &self,
        document: T,
        limits: DocumentLimits,
        reservation: Reservation,
    ) -> Result<String, Report<ClusterIdentityError>> {
        let inner = self.inner.clone();
        self.inner
            .executor
            .run_cpu(
                CpuClass::Control,
                reservation,
                move |_reservation, cancellation| -> Result<String, Report<ClusterIdentityError>> {
                    cancellation
                        .check()
                        .change_context(ClusterIdentityError::Cancelled)?;
                    let signed = inner
                        .signer
                        .sign(&document, limits)
                        .change_context(ClusterIdentityError::Sign)?;
                    cancellation
                        .check()
                        .change_context(ClusterIdentityError::Cancelled)?;
                    Ok(signed)
                },
            )
            .await
            .change_context(ClusterIdentityError::Execution)?
    }
}

impl Proposer {
    /// Grant the current private key only to this node's leader coordinator. Observers receive
    /// only the public identity. The coordinator remains responsible for its leadership fence.
    pub async fn cluster_signer(
        &self,
    ) -> error_stack::Result<Option<ClusterSigner>, ConsensusError> {
        let tenure = self.current_leader_tenure().ok_or_else(|| {
            Report::new(ConsensusError::LeadershipLost {
                observed: LeadershipObservation::NoLeader,
            })
        })?;
        if tenure.leader_id() != self.local_node_id() {
            return Err(Report::new(ConsensusError::LeadershipLost {
                observed: LeadershipObservation::Leader(tenure.leader_id().clone()),
            }));
        }
        let Some(record) = self.inner.store.inner.state().cluster_identity else {
            return Ok(None);
        };
        let executor = self.inner.store.inner.executor.clone();
        let reservation = executor
            .try_reserve(
                MemoryClass::Management,
                executor.limits().management_event_bytes.as_u64(),
            )
            .change_context(ClusterIdentityError::Admission)
            .change_context(ConsensusError::ClusterIdentity)?;
        let signer_executor = executor.clone();
        let signer = executor
            .run_cpu(
                CpuClass::Control,
                reservation,
                move |_reservation,
                      cancellation|
                      -> Result<ClusterSigner, Report<ClusterIdentityError>> {
                    cancellation
                        .check()
                        .change_context(ClusterIdentityError::Cancelled)?;
                    let signer = ClusterSigner::from_record(&record, signer_executor)?;
                    cancellation
                        .check()
                        .change_context(ClusterIdentityError::Cancelled)?;
                    Ok(signer)
                },
            )
            .await
            .change_context(ClusterIdentityError::Execution)
            .change_context(ConsensusError::ClusterIdentity)?
            .change_context(ConsensusError::ClusterIdentity)?;
        Ok(Some(signer))
    }
}
