//! Bounded HTTPS transport of typed signed licensing requests.
//!
//! Layer: control plane.
//! - **Owns.** Node-resolved requests, challenge refresh, exact retries and typed service refusals.
//! - **Depends on.** Maintained HTTP/TLS clients, the signing capability and protocol encoders.
//! - **Must not know.** Private key bytes, policy grants or graph execution.

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{ClusterLicensingState, LicensingEnvironment, LicensingMode};
use nervix_execution::MemoryClass;
use nervix_licensing_protocol::{
    documents::DecisionResponse,
    problems::{ProblemDetails, ProblemKind},
    requests::{
        ChallengeRequest, ChallengeResponse, ClientDescription, ClientProduct, CommittedState,
        RefreshState, Request, RequestCommon, Secret, TrustHeld,
    },
};
use nervix_models::Timestamp;
use nervix_primitives::time::{sleep, timeout};
use serde::de::DeserializeOwned;
use thiserror::Error;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::clock;
use crate::application::session_service::SessionServiceImpl;

#[derive(Debug, Error)]
pub(super) enum LicensingRequestError {
    #[error("licensing HTTP client could not be configured")]
    Client,
    #[error("licensing service request failed")]
    Transport,
    #[error("licensing response exceeds its admitted body capacity")]
    Bounds,
    #[error("licensing response does not match the protocol")]
    Response,
    #[error("licensing request exceeded its deadline")]
    Timeout,
    #[error("licensing service refused the request: {code}", code = .kind.code().as_str())]
    Refused { kind: ProblemKind },
    #[error("licensing request signing failed")]
    Signing,
    #[error("licensing work could not acquire its execution reservation")]
    Admission,
    #[error("licensing trust anchors have not been provisioned in this build")]
    Unprovisioned,
    #[error("the license key does not have the declared public identifier and secret format")]
    LicenseKey,
    #[error("licensing request ended with its leadership tenure")]
    Leadership,
    #[error("licensing request ended during node shutdown")]
    Shutdown,
}

pub(super) enum Operation {
    Create {
        kind: nervix_models::ActivationKind,
        license_key: Secret,
    },
    Refresh {
        activation_id: Uuid,
        state: RefreshState,
    },
}

pub(super) struct LicensingClient<'a> {
    service: &'a SessionServiceImpl,
    pub(super) environment: LicensingEnvironment,
    client: reqwest::Client,
    budget: Duration,
    backoff_max: Duration,
}

struct SignedRequest {
    body: Zeroizing<String>,
    expires_at: Timestamp,
}

impl Operation {
    fn name(&self) -> nervix_licensing_protocol::requests::Operation {
        match self {
            Self::Create { .. } => nervix_licensing_protocol::requests::Operation::Create,
            Self::Refresh { .. } => nervix_licensing_protocol::requests::Operation::Refresh,
        }
    }
}

impl<'a> LicensingClient<'a> {
    pub(super) async fn new(
        service: &'a SessionServiceImpl,
    ) -> Result<Self, Report<LicensingRequestError>> {
        let executor = service.inner.runtime.executor();
        let environment = service
            .inner
            .licensing_test_support
            .environment(executor.limits().management_event_bytes.as_u64());
        if !environment.is_provisioned() {
            return Err(Report::new(LicensingRequestError::Unprovisioned));
        }
        let dns = service
            .inner
            .runtime
            .dns()
            .ok_or_else(|| Report::new(LicensingRequestError::Client))?
            .clone();
        let entries = service.inner.licensing_test_support.tls_entries();
        let policy = service.inner.runtime.outward_tls().clone();
        let reservation = executor
            .try_reserve(
                MemoryClass::Management,
                executor.limits().management_event_bytes.as_u64(),
            )
            .change_context(LicensingRequestError::Admission)?;
        let client = executor
            .run_storage(
                nervix_execution::StorageClass::Filesystem,
                reservation,
                move |_reservation,
                      cancellation|
                      -> Result<reqwest::Client, Report<LicensingRequestError>> {
                    cancellation
                        .check()
                        .change_context(LicensingRequestError::Client)?;
                    let client = nervix_connector::HttpClientConfig::new(
                        &entries,
                        "licensing",
                        &dns,
                        &policy,
                    )
                    .build()
                    .change_context(LicensingRequestError::Client)?;
                    cancellation
                        .check()
                        .change_context(LicensingRequestError::Client)?;
                    Ok(client)
                },
            )
            .await
            .change_context(LicensingRequestError::Client)??;
        let parameters = service.cluster_parameters();
        Ok(Self {
            service,
            environment,
            client,
            budget: parameters.licensing_request_timeout(),
            backoff_max: parameters.licensing_retry_backoff_max(),
        })
    }

    async fn body<T: DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, Report<LicensingRequestError>> {
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            Report::new(error.without_url()).change_context(LicensingRequestError::Transport)
        })? {
            nervix_primitives::task::consume_budget().await;
            let Some(total) = bytes.len().checked_add(chunk.len()) else {
                return Err(Report::new(LicensingRequestError::Bounds));
            };
            if total > self.environment.limits().bytes.get() {
                return Err(Report::new(LicensingRequestError::Bounds));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).change_context(LicensingRequestError::Response)
    }

    async fn challenge(
        &self,
        cluster: &ClusterLicensingState,
    ) -> Result<ChallengeResponse, Report<LicensingRequestError>> {
        let request = self
            .client
            .post(
                self.environment
                    .service()
                    .join("/v1/challenges")
                    .assured("absolute protocol path"),
            )
            .json(&ChallengeRequest {
                cluster_uid: cluster.identity.cluster_uid,
            });
        timeout(self.budget, async {
            let response = request.send().await.map_err(|error| {
                Report::new(error.without_url()).change_context(LicensingRequestError::Transport)
            })?;
            if !response.status().is_success() {
                let problem: ProblemDetails = self.body(response).await?;
                return Err(Report::new(LicensingRequestError::Refused {
                    kind: problem.kind,
                }));
            }
            self.body(response).await
        })
        .await
        .change_context(LicensingRequestError::Timeout)?
    }

    pub(super) async fn perform(
        &self,
        cluster: &ClusterLicensingState,
        operation: Operation,
        request_id: Uuid,
    ) -> Result<DecisionResponse, Report<LicensingRequestError>> {
        let Some(tenure) = self.service.inner.consensus.current_leader_tenure() else {
            return Err(Report::new(LicensingRequestError::Leadership));
        };
        if tenure.leader_id() != self.service.inner.consensus.local_node_id() {
            return Err(Report::new(LicensingRequestError::Leadership));
        }
        let mut topology = self.service.inner.consensus.observer().subscribe_topology();
        let mut work = std::pin::pin!(self.perform_request(cluster, operation, request_id));
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                result = &mut work => return result,
                _ = self.service.inner.drain_support_shutdown.cancelled() => return Err(Report::new(LicensingRequestError::Shutdown)),
                changed = topology.changed() => {
                    if !changed || self.service.inner.consensus.current_leader_tenure().as_ref() != Some(&tenure) {
                        return Err(Report::new(LicensingRequestError::Leadership));
                    }
                },
            }
        }
    }

    async fn perform_request(
        &self,
        cluster: &ClusterLicensingState,
        operation: Operation,
        request_id: Uuid,
    ) -> Result<DecisionResponse, Report<LicensingRequestError>> {
        let mut signed: Option<SignedRequest> = None;
        let mut backoff = self.budget.min(self.backoff_max);
        loop {
            nervix_primitives::task::consume_budget().await;
            let now = clock::observe(self.service.inner.runtime.fault_injection());
            let attempt = async {
                if !matches!(signed.as_ref(), Some(request) if now < request.expires_at) {
                    let challenge = self.challenge(cluster).await?;
                    let expires_at = Timestamp::try_from(challenge.expires_at)
                        .change_context(LicensingRequestError::Response)?;
                    let request = self.request(cluster, &operation, request_id, challenge, now);
                    let executor = self.service.inner.runtime.executor();
                    let reservation = executor
                        .try_reserve(
                            MemoryClass::Management,
                            executor.limits().management_event_bytes.as_u64(),
                        )
                        .change_context(LicensingRequestError::Admission)?;
                    let signer = self
                        .service
                        .inner
                        .consensus
                        .cluster_signer()
                        .await
                        .change_context(LicensingRequestError::Signing)?
                        .verified("initialized licensing requests hold an identity");
                    signed = Some(SignedRequest {
                        body: Zeroizing::new(
                            signer
                                .sign(request, self.environment.limits(), reservation)
                                .await
                                .change_context(LicensingRequestError::Signing)?,
                        ),
                        expires_at,
                    });
                }
                let route = match &operation {
                    Operation::Create { .. } => "/v1/activations".to_owned(),
                    Operation::Refresh { activation_id, .. } => {
                        format!("/v1/activations/{activation_id}/refresh")
                    }
                };
                timeout(self.budget, async {
                    let response = self
                        .client
                        .post(
                            self.environment
                                .service()
                                .join(&route)
                                .assured("absolute protocol path"),
                        )
                        .header(reqwest::header::CONTENT_TYPE, "application/jose")
                        .body(
                            signed
                                .as_ref()
                                .verified("request was signed")
                                .body
                                .as_bytes()
                                .to_vec(),
                        )
                        .send()
                        .await
                        .map_err(|error| {
                            Report::new(error.without_url())
                                .change_context(LicensingRequestError::Transport)
                        })?;
                    if !response.status().is_success() {
                        let problem: ProblemDetails = self.body(response).await?;
                        return Err(Report::new(LicensingRequestError::Refused {
                            kind: problem.kind,
                        }));
                    }
                    self.body(response).await
                })
                .await
                .change_context(LicensingRequestError::Timeout)?
            }
            .await;
            match attempt {
                Ok(response) => return Ok(response),
                Err(error) => {
                    let (retry, outcome) = failed_attempt(error.current_context());
                    if let Some(outcome) = outcome {
                        self.service.inner.licensing.record_request(
                            operation.name(),
                            outcome,
                            clock::observe(self.service.inner.runtime.fault_injection()),
                        );
                    }
                    tracing::warn!(error = %format_args!("{error:#}"), retry, "licensing request failed");
                    if !retry {
                        return Err(error);
                    }
                    let nanos = u64::try_from(backoff.as_nanos())
                        .assured("validated u32-seconds backoff fits u64 nanoseconds");
                    let jitter = Duration::from_nanos(nervix_crypto::random::u64() % nanos);
                    nervix_primitives::select! {
                        _ = sleep(jitter) => {},
                        _ = self.service.inner.licensing_requests.refresh_requested(), if matches!(&operation, Operation::Refresh { .. }) => {},
                    }
                    backoff = backoff
                        .checked_mul(2)
                        .assured("doubling a validated u32-seconds backoff fits Duration")
                        .min(self.backoff_max);
                }
            }
        }
    }

    fn request(
        &self,
        cluster: &ClusterLicensingState,
        operation: &Operation,
        request_id: Uuid,
        challenge: ChallengeResponse,
        now: Timestamp,
    ) -> Request {
        let common = RequestCommon {
            protocol: 1,
            request_id,
            challenge: challenge.challenge,
            sent_at: now.into_datetime(),
            aud: self.environment.service().clone(),
            cluster_uid: cluster.identity.cluster_uid,
            display_name: cluster.identity.cluster_uid.to_string(),
            client: ClientDescription {
                product: ClientProduct::NervixServer,
                version: env!("CARGO_PKG_VERSION").to_owned(),
            },
            key_generation: cluster.identity.key_generation.get(),
            trust_held: held(cluster),
        };
        match operation {
            Operation::Create { kind, license_key } => Request::Create {
                common,
                kind: *kind,
                license_key: license_key.clone(),
                cluster_key: cluster.identity.public_jwk.clone(),
            },
            Operation::Refresh {
                activation_id,
                state,
            } => Request::Refresh {
                common,
                activation_id: *activation_id,
                state: state.clone(),
                transitions: Vec::new(),
            },
        }
    }
}

fn held(cluster: &ClusterLicensingState) -> TrustHeld {
    let mut held = TrustHeld {
        working_keys: Vec::new(),
        revocation_serials: Vec::new(),
        succession_serials: Vec::new(),
    };
    for document in &cluster.record.trust {
        match document.typ {
            nervix_licensing_protocol::document::DocumentType::WorkingKey => {
                held.working_keys.extend(document.kid.clone())
            }
            nervix_licensing_protocol::document::DocumentType::Revocation => {
                held.revocation_serials.push(document.serial)
            }
            nervix_licensing_protocol::document::DocumentType::Succession => {
                held.succession_serials.push(document.serial)
            }
            _ => unreachable!("stored trust identities validate before publication"),
        }
    }
    held
}

fn failed_attempt(error: &LicensingRequestError) -> (bool, Option<crate::licensing::RequestEvent>) {
    use crate::licensing::RequestEvent;
    match error {
        LicensingRequestError::Transport => (true, Some(RequestEvent::Unreachable)),
        LicensingRequestError::Timeout => (true, Some(RequestEvent::Timeout)),
        LicensingRequestError::Refused { kind } => (
            kind.code().retryable(),
            Some(RequestEvent::RefusedByService(kind.code())),
        ),
        LicensingRequestError::Response | LicensingRequestError::Bounds => {
            (false, Some(RequestEvent::RejectedByCluster))
        }
        LicensingRequestError::Admission | LicensingRequestError::Signing => (true, None),
        LicensingRequestError::Client
        | LicensingRequestError::Unprovisioned
        | LicensingRequestError::LicenseKey
        | LicensingRequestError::Leadership
        | LicensingRequestError::Shutdown => (false, None),
    }
}

pub(super) fn refresh(cluster: &ClusterLicensingState) -> Option<Operation> {
    let LicensingMode::Activated(activation) = &cluster.record.mode else {
        return None;
    };
    Some(Operation::Refresh {
        activation_id: activation.binding.activation_id,
        state: RefreshState::Committed(CommittedState {
            applied_decision_revision: activation.decision.revision,
            applied_conditions_revision: activation.decision.conditions_revision,
            licensing_time: cluster.record.licensing_time.into_datetime(),
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_admission_refusal_is_retryable_without_claiming_a_service_outage() {
        for error in [
            LicensingRequestError::Admission,
            LicensingRequestError::Signing,
        ] {
            let (retry, observation) = failed_attempt(&error);
            assert!(
                retry,
                "local work that judged nothing must remain retryable"
            );
            assert!(
                observation.is_none(),
                "local failure cannot change service connectivity or its HTTP counters"
            );
        }
    }
}
