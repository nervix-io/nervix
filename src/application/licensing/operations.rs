//! Execute durable key rotation and notify the service of committed releases.
//!
//! Layer: control plane.
//! - **Owns.** Foreground operation results and leadership-scoped resumption of committed work.
//! - **Depends on.** Consensus operation proposals, bounded request transport and response decoding.
//! - **Must not know.** Private keys, policy arithmetic or storage representations.

use error_stack::{Report, ResultExt as _};
use nervix_consensus::{ClusterLicensingState, LicensingMode, ReceivedLicenseDecision};
use nervix_execution::{CpuClass, MemoryClass};
use nervix_licensing_protocol::{
    documents::DecisionResponse, problems::ProblemKind, requests::Operation,
};
use nervix_models::CommandExecutionReference;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("licensing service refused the request: {code}", code = .code.as_str())]
struct CommittedLicensingRefusal {
    code: nervix_licensing_protocol::problems::ProblemCode,
}

use super::{LicensingOperationError, activation_request_id, client, clock};
use crate::application::{
    command_result::CommandResult,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};

impl SessionServiceImpl {
    async fn licensing_command_result(
        &self,
        reference: &CommandExecutionReference,
    ) -> Option<Result<(), Report<LicensingOperationError>>> {
        let execution = self
            .inner
            .consensus
            .current_command_execution(reference)
            .await?;
        let outcome = match execution.effect()? {
            nervix_consensus::CommandExecutionEffect::RotateLicenseClusterKey {
                outcome, ..
            }
            | nervix_consensus::CommandExecutionEffect::ReleaseLicense { outcome, .. } => *outcome,
            _ => None,
        }?;
        Some(match outcome {
            nervix_consensus::LicensingCommandOutcome::Applied => Ok(()),
            nervix_consensus::LicensingCommandOutcome::Refused { code } => {
                Err(Report::new(CommittedLicensingRefusal { code })
                    .change_context(LicensingOperationError::Request))
            }
        })
    }
    pub(in crate::application) fn prepare_cluster_key_rotation(
        &self,
    ) -> Result<nervix_consensus::CommandExecutionEffect, Report<LicensingOperationError>> {
        Ok(
            nervix_consensus::CommandExecutionEffect::RotateLicenseClusterKey {
                outcome: None,
                identity: Box::new(self.licensing_state()?.identity),
            },
        )
    }

    pub(in crate::application) fn prepare_license_release(
        &self,
    ) -> Result<nervix_consensus::CommandExecutionEffect, Report<LicensingOperationError>> {
        let state = self.licensing_state()?;
        let LicensingMode::Activated(activation) = &state.record.mode else {
            return Err(
                Report::new(nervix_consensus::LicenseDecisionError::NotActivated)
                    .change_context(LicensingOperationError::Rejected),
            );
        };
        Ok(nervix_consensus::CommandExecutionEffect::ReleaseLicense {
            outcome: None,
            binding: Box::new(activation.binding.clone()),
        })
    }
    fn licensing_state(&self) -> Result<ClusterLicensingState, Report<LicensingOperationError>> {
        self.inner
            .consensus
            .observer()
            .licensing_snapshot()
            .initialized
            .ok_or_else(|| Report::new(LicensingOperationError::Uninitialized))
    }

    pub(in crate::application) async fn rotate_license_cluster_key(
        &self,
        reference: &CommandExecutionReference,
        identity: nervix_consensus::ClusterIdentity,
    ) -> CommandResult {
        let _turn = self.inner.licensing_requests.foreground().await;
        let result = async {
            if let Some(result) = self.licensing_command_result(reference).await {
                return result;
            }
            let request_id = activation_request_id(reference)?;
            if let Some(rotation) = self.licensing_state()?.rotation
                && rotation.request_id != request_id
            {
                self.finish_cluster_key_rotation().await?;
            }
            self.inner
                .consensus
                .begin_cluster_key_rotation(
                    identity,
                    reference.clone(),
                    request_id,
                    clock::observe(self.inner.runtime.fault_injection()),
                )
                .await
                .change_context(LicensingOperationError::Consensus)?;
            if let Some(result) = self.licensing_command_result(reference).await {
                return result;
            }
            self.finish_cluster_key_rotation().await?;
            self.licensing_command_result(reference)
                .await
                .unwrap_or(Ok(()))
        }
        .await;
        match result {
            Ok(()) => command_ok("License cluster key rotated".to_owned()),
            Err(error) => command_error(format!("{error:#}")),
        }
    }

    pub(in crate::application) async fn release_license(
        &self,
        reference: &CommandExecutionReference,
        binding: nervix_consensus::ActivationBinding,
    ) -> CommandResult {
        let _turn = self.inner.licensing_requests.foreground().await;
        let result = async {
            if let Some(result) = self.licensing_command_result(reference).await {
                return result;
            }
            let request_id = activation_request_id(reference)?;
            if self.licensing_state()?.rotation.is_some() {
                self.finish_cluster_key_rotation().await?;
            }
            self.inner
                .consensus
                .release_license(
                    binding,
                    reference.clone(),
                    request_id,
                    clock::observe(self.inner.runtime.fault_injection()),
                )
                .await
                .change_context(LicensingOperationError::Consensus)
        }
        .await;
        match result {
            Ok(()) => command_ok("License released; awaiting service confirmation".to_owned()),
            Err(error) => command_error(format!("{error:#}")),
        }
    }

    /// The request turn serializes resumption with foreground activation/refresh commands.
    pub(super) async fn resume_licensing_operations(
        &self,
    ) -> Result<(), Report<LicensingOperationError>> {
        let _turn = self.inner.licensing_requests.background().await;
        self.finish_cluster_key_rotation().await?;
        self.confirm_license_release().await
    }

    pub(super) async fn finish_cluster_key_rotation(
        &self,
    ) -> Result<(), Report<LicensingOperationError>> {
        let state = self.licensing_state()?;
        let Some(rotation) = &state.rotation else {
            return Ok(());
        };
        let request_id = rotation.request_id;
        let Some(operation) = client::rotation(&state) else {
            return Ok(());
        };
        let client = client::LicensingClient::new(self)
            .await
            .change_context(LicensingOperationError::Request)?;
        let response = match client.perform(&state, operation, request_id).await {
            Ok(response) => response,
            Err(error) => {
                if let client::LicensingRequestError::Refused { kind } = error.current_context()
                    && !kind.code().retryable()
                {
                    self.inner
                        .consensus
                        .abandon_cluster_key_rotation(
                            rotation.reference.clone(),
                            request_id,
                            kind.code(),
                        )
                        .await
                        .change_context(LicensingOperationError::Consensus)?;
                }
                return Err(error.change_context(LicensingOperationError::Request));
            }
        };
        let received = self
            .receive_licensing_response(response, &state, request_id, client.environment)
            .await?;
        self.inner
            .consensus
            .complete_cluster_key_rotation(request_id, received)
            .await
            .change_context(LicensingOperationError::Consensus)?;
        self.inner.licensing.record_request(
            Operation::RotateKey,
            crate::licensing::RequestEvent::Committed,
            clock::observe(self.inner.runtime.fault_injection()),
        );
        Ok(())
    }

    pub(super) async fn confirm_license_release(
        &self,
    ) -> Result<(), Report<LicensingOperationError>> {
        let state = self.licensing_state()?;
        let LicensingMode::Released(release) = &state.record.mode else {
            return Ok(());
        };
        let Some(operation) = client::release(&state) else {
            return Ok(());
        };
        let request_id = release.request_id;
        let client = client::LicensingClient::new(self)
            .await
            .change_context(LicensingOperationError::Request)?;
        let response = client.perform(&state, operation, request_id).await;
        let decision = match response {
            Ok(response) => Some(
                self.receive_licensing_response(response, &state, request_id, client.environment)
                    .await?,
            ),
            Err(error)
                if matches!(
                    error.current_context(),
                    client::LicensingRequestError::Refused {
                        kind: ProblemKind::ActivationUnknown
                    }
                ) =>
            {
                None
            }
            Err(error) => return Err(error.change_context(LicensingOperationError::Request)),
        };
        self.inner
            .consensus
            .confirm_license_release(request_id, decision)
            .await
            .change_context(LicensingOperationError::Consensus)?;
        self.inner.licensing.record_request(
            Operation::Release,
            crate::licensing::RequestEvent::Committed,
            clock::observe(self.inner.runtime.fault_injection()),
        );
        Ok(())
    }

    async fn receive_licensing_response(
        &self,
        response: DecisionResponse,
        state: &ClusterLicensingState,
        request_id: Uuid,
        environment: nervix_consensus::LicensingEnvironment,
    ) -> Result<ReceivedLicenseDecision, Report<LicensingOperationError>> {
        let held = state.record.trust.clone();
        let received_at = clock::observe(self.inner.runtime.fault_injection());
        let executor = self.inner.runtime.executor();
        let reservation = executor
            .try_reserve(
                MemoryClass::Management,
                executor.limits().management_event_bytes.as_u64(),
            )
            .change_context(LicensingOperationError::Admission)?;
        let received = executor
            .run_cpu(
                CpuClass::Control,
                reservation,
                move |_reservation, cancellation| {
                    cancellation
                        .check()
                        .change_context(LicensingOperationError::Execution)?;
                    let received = ReceivedLicenseDecision::from_response(
                        response,
                        received_at,
                        request_id,
                        &held,
                        &environment,
                    )
                    .change_context(LicensingOperationError::Rejected)?;
                    cancellation
                        .check()
                        .change_context(LicensingOperationError::Execution)?;
                    Ok::<_, Report<LicensingOperationError>>(received)
                },
            )
            .await
            .change_context(LicensingOperationError::Execution)??;
        self.commit_licensing_trust(&received).await?;
        Ok(received)
    }

    pub(super) async fn commit_licensing_trust(
        &self,
        received: &ReceivedLicenseDecision,
    ) -> Result<(), Report<LicensingOperationError>> {
        if self.licensing_state()?.record.trust != received.trust {
            self.inner
                .consensus
                .install_license_trust(received.trust.clone())
                .await
                .change_context(LicensingOperationError::Consensus)?;
        }
        Ok(())
    }
}
