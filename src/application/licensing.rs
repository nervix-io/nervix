//! Leader-owned licensing requests and committed time coordination.
//!
//! Layer: control plane.
//! - **Owns.** Activation, refresh, retry and deadline proposal lifetimes.
//! - **Depends on.** Typed protocol requests, consensus, node DNS/TLS and policy observations.
//! - **Must not know.** Private keys, raw parsing or graph execution internals.

mod client;
mod clock;
mod coordinator;
mod notifications;
mod request_turn;
pub(in crate::application) mod stop_gate;
mod stream;
pub(in crate::application) use coordinator::run;
pub(in crate::application) use request_turn::LicensingRequests;
pub mod test_support;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_consensus::{ActivationBinding, CommandExecutionEffect, ReceivedLicenseDecision};
use nervix_execution::{CpuClass, MemoryClass};
use nervix_licensing_protocol::{
    problems::ProblemKind,
    requests::{RefreshState, Secret},
};
use nervix_models::{ActivateLicense, CommandExecutionReference};
pub use test_support::ConfiguredLicensingTestSupport;
use thiserror::Error;

use super::session_service::SessionServiceImpl;

#[derive(Debug, Error)]
pub(super) enum LicensingOperationError {
    #[error("cluster licensing identity is not initialized")]
    Uninitialized,
    #[error("licensing protocol request failed")]
    Request,
    #[error("licensing response was rejected by the cluster")]
    Rejected,
    #[error("licensing operation could not acquire its execution reservation")]
    Admission,
    #[error("licensing verification could not execute")]
    Execution,
    #[error("licensing consensus proposal failed")]
    Consensus,
}

impl SessionServiceImpl {
    pub(in crate::application) async fn prepare_license_activation(
        &self,
        reference: &CommandExecutionReference,
        activate: &ActivateLicense,
    ) -> Result<CommandExecutionEffect, Report<LicensingOperationError>> {
        let _turn = self.inner.licensing_requests.background().await;
        let state = self
            .inner
            .consensus
            .observer()
            .licensing_snapshot()
            .initialized
            .ok_or_else(|| Report::new(LicensingOperationError::Uninitialized))?;
        // An ephemeral activation whose stop gate closed activates again (specification §7.6);
        // every other existing activation refuses a new one.
        if let nervix_consensus::LicensingMode::Activated(_) = state.record.mode
            && state.record.processing_stop().is_none()
        {
            return Err(
                Report::new(nervix_consensus::LicenseDecisionError::AlreadyActivated)
                    .change_context(LicensingOperationError::Rejected),
            );
        }
        let license_key_id = public_key_id(&activate.license_key)
            .change_context(LicensingOperationError::Request)?;
        let client = client::LicensingClient::new(self)
            .await
            .change_context(LicensingOperationError::Request)?;
        let mut request_id = activation_request_id(reference)?;
        let mut adoption_id = None;
        let first = client
            .perform(
                &state,
                client::Operation::Create {
                    kind: activate.kind,
                    license_key: Secret::new(activate.license_key.clone()),
                },
                request_id,
            )
            .await;
        let response = match first {
            Ok(response) => response,
            Err(error) => match error.current_context() {
                client::LicensingRequestError::Refused {
                    kind: ProblemKind::AlreadyActivated { activation_id },
                } => {
                    adoption_id = Some(*activation_id);
                    request_id = clock::request_id(self.inner.runtime.fault_injection());
                    client
                        .perform(
                            &state,
                            client::Operation::Refresh {
                                activation_id: *activation_id,
                                state: RefreshState::Adoption,
                            },
                            request_id,
                        )
                        .await
                        .change_context(LicensingOperationError::Request)?
                }
                _ => return Err(error.change_context(LicensingOperationError::Request)),
            },
        };
        let environment = client.environment;
        let received_at = clock::observe(self.inner.runtime.fault_injection());
        let executor = self.inner.runtime.executor();
        let reservation = executor
            .try_reserve(
                MemoryClass::Management,
                executor.limits().management_event_bytes.as_u64(),
            )
            .change_context(LicensingOperationError::Admission)?;
        let kind = activate.kind;
        let prepared = executor
            .run_cpu(
                CpuClass::Control,
                reservation,
                move |_reservation, cancellation| -> Result<CommandExecutionEffect, Report<LicensingOperationError>> {
                    cancellation
                        .check()
                        .change_context(LicensingOperationError::Execution)?;
                    let received = ReceivedLicenseDecision::from_response(
                        response,
                        received_at,
                        request_id,
                        &state.record.trust,
                        &environment,
                    )
                    .change_context(LicensingOperationError::Rejected)?;
                    let binding = state
                        .record
                        .prepare_activation(
                            &state.identity,
                            kind,
                            license_key_id,
                            &received,
                            &environment,
                        )
                        .change_context(LicensingOperationError::Rejected)?;
                    if adoption_id.is_some_and(|expected| binding.activation_id != expected) {
                        return Err(Report::new(nervix_consensus::LicenseDecisionError::Binding)
                            .change_context(LicensingOperationError::Rejected));
                    }
                    Ok(CommandExecutionEffect::ActivateLicense {
                        binding: Box::new(binding),
                        decision: Box::new(received),
                    })
                },
            )
            .await
            .change_context(LicensingOperationError::Execution)?;
        match prepared {
            Ok(effect) => Ok(effect),
            Err(error) => {
                self.report_license_rejection(
                    if adoption_id.is_some() {
                        nervix_licensing_protocol::requests::Operation::Refresh
                    } else {
                        nervix_licensing_protocol::requests::Operation::Create
                    },
                    received_at,
                    &error,
                );
                Err(error)
            }
        }
    }

    pub(in crate::application) async fn refresh_license(
        &self,
    ) -> Result<(), Report<LicensingOperationError>> {
        let _turn = self.inner.licensing_requests.foreground().await;
        self.refresh_license_once().await
    }

    pub(in crate::application) async fn refresh_license_in_background(
        &self,
    ) -> Result<(), Report<LicensingOperationError>> {
        let _turn = self.inner.licensing_requests.background().await;
        self.refresh_license_once().await
    }

    async fn refresh_license_once(&self) -> Result<(), Report<LicensingOperationError>> {
        let state = self
            .inner
            .consensus
            .observer()
            .licensing_snapshot()
            .initialized
            .ok_or_else(|| Report::new(LicensingOperationError::Uninitialized))?;
        let operation = client::refresh(&state)
            .ok_or_else(|| Report::new(nervix_consensus::LicenseDecisionError::NotActivated))
            .change_context(LicensingOperationError::Rejected)?;
        let nervix_consensus::LicensingMode::Activated(activation) = &state.record.mode else {
            unreachable!("refresh operation requires activation")
        };
        let expected_revision = activation.decision.revision;
        let binding = activation.binding.clone();
        let request_id = clock::request_id(self.inner.runtime.fault_injection());
        let client = client::LicensingClient::new(self)
            .await
            .change_context(LicensingOperationError::Request)?;
        let response = client
            .perform(&state, operation, request_id)
            .await
            .change_context(LicensingOperationError::Request)?;
        let environment = client.environment;
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
                move |_reservation, cancellation| -> Result<ReceivedLicenseDecision, Report<LicensingOperationError>> {
                    cancellation
                        .check()
                        .change_context(LicensingOperationError::Execution)?;
                    let received = ReceivedLicenseDecision::from_response(
                        response,
                        received_at,
                        request_id,
                        &state.record.trust,
                        &environment,
                    )
                    .change_context(LicensingOperationError::Rejected)?;
                    state
                        .record
                        .accept(
                            &state.identity,
                            &binding,
                            &received,
                            Some(expected_revision),
                            &environment,
                        )
                        .change_context(LicensingOperationError::Rejected)?;
                    Ok(received)
                },
            )
            .await
            .change_context(LicensingOperationError::Execution)?;
        let received = match received {
            Ok(received) => received,
            Err(error) => {
                self.report_license_rejection(
                    nervix_licensing_protocol::requests::Operation::Refresh,
                    received_at,
                    &error,
                );
                return Err(error);
            }
        };
        match self
            .inner
            .consensus
            .apply_license_decision(received, expected_revision)
            .await
        {
            Ok(()) => {
                self.inner.licensing.record_request(
                    nervix_licensing_protocol::requests::Operation::Refresh,
                    crate::licensing::RequestEvent::Committed,
                    clock::observe(self.inner.runtime.fault_injection()),
                );
                tracing::info!("licensing decision refresh committed");
                Ok(())
            }
            Err(error) => {
                let error = error.change_context(LicensingOperationError::Consensus);
                self.report_license_rejection(
                    nervix_licensing_protocol::requests::Operation::Refresh,
                    clock::observe(self.inner.runtime.fault_injection()),
                    &error,
                );
                Err(error)
            }
        }
    }

    fn report_license_rejection(
        &self,
        operation: nervix_licensing_protocol::requests::Operation,
        at: nervix_models::Timestamp,
        error: &Report<LicensingOperationError>,
    ) {
        self.inner.licensing.record_request(
            operation,
            crate::licensing::RequestEvent::RejectedByCluster,
            at,
        );
        tracing::warn!(operation = operation.as_str(), error = %format_args!("{error:#}"), "licensing response rejected by cluster");
    }

    pub(in crate::application) async fn apply_license_activation(
        &self,
        reference: &CommandExecutionReference,
        binding: ActivationBinding,
        decision: ReceivedLicenseDecision,
    ) -> super::command_result::CommandResult {
        let state = self.inner.consensus.observer().licensing_snapshot();
        if let Some(initialized) = state.initialized
            && let nervix_consensus::LicensingMode::Activated(current) = &initialized.record.mode
            && current.binding == binding
            && current.decision.bytes == decision.bytes
        {
            return super::model_mutation::command_ok("License activated".to_owned());
        }
        let creation_id = match activation_request_id(reference) {
            Ok(id) => id,
            Err(error) => return super::model_mutation::command_error(format!("{error:#}")),
        };
        let operation = if decision.request_id == creation_id {
            nervix_licensing_protocol::requests::Operation::Create
        } else {
            nervix_licensing_protocol::requests::Operation::Refresh
        };
        match self
            .inner
            .consensus
            .activate_license(binding, decision)
            .await
        {
            Ok(()) => {
                self.inner.licensing.record_request(
                    operation,
                    crate::licensing::RequestEvent::Committed,
                    clock::observe(self.inner.runtime.fault_injection()),
                );
                tracing::info!("license activation committed");
                super::model_mutation::command_ok("License activated".to_owned())
            }
            Err(error) => {
                let error = error.change_context(LicensingOperationError::Consensus);
                self.report_license_rejection(
                    operation,
                    clock::observe(self.inner.runtime.fault_injection()),
                    &error,
                );
                super::model_mutation::command_error(format!("{error:#}"))
            }
        }
    }
}

fn public_key_id(key: &str) -> Result<String, Report<client::LicensingRequestError>> {
    let invalid = || Report::new(client::LicensingRequestError::LicenseKey);
    let body = key.strip_prefix("NVXK-").ok_or_else(invalid)?;
    let (id, secret) = body.split_once('-').ok_or_else(invalid)?;
    if id.len() != 8
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || (b'2'..=b'7').contains(&byte))
        || secret.is_empty()
    {
        return Err(invalid());
    }
    Ok(id.to_owned())
}

fn activation_request_id(
    reference: &CommandExecutionReference,
) -> Result<uuid::Uuid, Report<LicensingOperationError>> {
    reference
        .retry_issued_at()
        .change_context(LicensingOperationError::Request)?;
    Ok(uuid::Uuid::parse_str(reference.as_str())
        .assured("retry_issued_at checked this UUIDv7 reference"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_reference_is_checked_before_network_work() {
        for value in ["fixture-reference", "00000000-0000-4000-8000-000000000001"] {
            let reference =
                CommandExecutionReference::parse(value).assured("reference syntax is valid");
            assert!(activation_request_id(&reference).is_err());
        }
        let id = uuid::Builder::from_unix_timestamp_millis(1_700_000_000_000, &[0; 10]).into_uuid();
        let reference = CommandExecutionReference::parse(id.to_string())
            .assured("UUID reference syntax is valid");
        assert_eq!(
            activation_request_id(&reference).assured("checked UUIDv7 is accepted"),
            id
        );
    }
}
