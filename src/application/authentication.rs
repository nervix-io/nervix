//! Who a request belongs to, and whether it may act.
//!
//! Layer: control plane.
//!
//! - **Owns.** Basic credentials on every transport, the password hash, the user record, and the
//!   rate limit an unauthenticated caller is held to.
//! - **Depends on.** Consensus, the bounded executor and the cryptography boundary.
//! - **Must not know.** What an authenticated caller goes on to do.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::num::NonZeroU32;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use error_stack::{Report, ResultExt as _};
use http_body_util::Full;
use hyper::{
    Request as HyperRequest, Response as HyperResponse, StatusCode,
    body::{Bytes, Incoming as HyperIncoming},
    header::{AUTHORIZATION, WWW_AUTHENTICATE},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::UserCredentials;
use nervix_execution::{AdmissionError, CpuClass, ExecutionError, Executor, MemoryClass};
use nervix_models::{CreateStatement, CreateUser, PasswordCost, PasswordCredential, UserName};
use thiserror::Error;
use tonic::{Status, metadata::MetadataMap};
use tracing::{debug, warn};

use super::{
    command_result::CommandResult,
    model_mutation::{command_error, command_ok, command_ok_already_existed},
    session_service::SessionServiceImpl,
    web_console::{WEB_CONSOLE_AUTH_QUERY_PARAM, web_console_query_param},
};

pub(in crate::application) const DEFAULT_USER: &str = "default";

const BASIC_AUTH_REALM: &str = "Nervix";

const AUTH_RATE_LIMIT_PER_SECOND: u32 = 10;

mod pacing;

pub(in crate::application) use pacing::AuthenticationPacing;

/// The credentials a `Basic` authorization token carries, or `None` when it carries none.
///
/// A token that is not base64, not UTF-8, or not `user:password` is a malformed header rather than
/// a wrong password, and the caller answers both the same way. Nothing about the token is reported,
/// because it is the secret.
fn credentials_from_basic_token(token: &str) -> Option<BasicAuthCredentials> {
    let decoded = BASE64_STANDARD.decode(token).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    if username.is_empty() {
        return None;
    }
    Some(BasicAuthCredentials {
        username: username.to_string(),
        password: password.to_string(),
    })
}

fn credentials_from_basic_authorization(value: &str) -> Option<BasicAuthCredentials> {
    let (scheme, token) = value.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Basic") {
        return None;
    }
    credentials_from_basic_token(token.trim())
}

pub(in crate::application) fn credentials_from_metadata(
    metadata: &MetadataMap,
) -> Option<BasicAuthCredentials> {
    let value = metadata.get("authorization")?;
    let Ok(value) = value.to_str() else {
        return None;
    };
    credentials_from_basic_authorization(value)
}

pub(in crate::application) fn credentials_from_web_console_request(
    request: &HyperRequest<HyperIncoming>,
) -> Option<BasicAuthCredentials> {
    if let Some(value) = request.headers().get(AUTHORIZATION)
        && let Ok(value) = value.to_str()
        && let Some(credentials) = credentials_from_basic_authorization(value)
    {
        return Some(credentials);
    }
    let token = web_console_query_param(request.uri().query(), WEB_CONSOLE_AUTH_QUERY_PARAM)?;
    credentials_from_basic_token(&token)
}

pub(in crate::application) fn unauthorized_basic_response() -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            WWW_AUTHENTICATE,
            format!("Basic realm=\"{BASIC_AUTH_REALM}\""),
        )
        .body(Full::new(Bytes::from_static(b"authentication failed")))
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

/// The answer to credentials the node could not verify now: they were not judged, so the caller
/// may present them again once the node has room.
pub(in crate::application) fn busy_authentication_response() -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(Full::new(Bytes::from_static(
            b"the node could not verify credentials now; retry",
        )))
        .assured("the status is a typed constant, which the http builder always accepts")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct BasicAuthCredentials {
    pub(in crate::application) username: String,
    pub(in crate::application) password: String,
}

#[derive(Debug, Error)]
pub(in crate::application) enum GrpcAuthenticationError {
    #[error("authentication required")]
    Required,
    #[error("authentication failed")]
    Failed,
    #[error("the node could not verify credentials now; retry")]
    Busy,
}

/// Why a call did not authenticate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "credential checks expose one deliberate rejection for invalid user names, \
                  unknown users and wrong passwords; verification failures have their own \
                  operator diagnostic and never reveal credential details to the caller"
    )
)]
pub(in crate::application) enum AuthenticationRefusal {
    /// The call presented no readable Basic credentials.
    Missing,
    /// The user is unknown, the name is invalid, or the password is wrong.
    Rejected,
    /// The node's bounded execution could not take the verification now, so the credentials were
    /// not judged at all.
    Unavailable,
}

impl From<AuthenticationRefusal> for GrpcAuthenticationError {
    fn from(refusal: AuthenticationRefusal) -> Self {
        match refusal {
            AuthenticationRefusal::Missing => Self::Required,
            AuthenticationRefusal::Rejected => Self::Failed,
            AuthenticationRefusal::Unavailable => Self::Busy,
        }
    }
}

#[derive(Debug, Error)]
pub(in crate::application) enum PasswordHashError {
    #[error("the node's bounded execution could not take the password hash now")]
    Busy,
    #[error("the password hash needs more working memory than the node's credentials budget holds")]
    ExceedsBudget,
    #[error("the caller stopped waiting for the password hash before it started")]
    Cancelled,
    #[error("password hash task failed")]
    Task,
}

impl PasswordHashError {
    /// A charge the credentials budget could not grant: waiting for room is the budget's own
    /// backpressure,
    /// so only a hash larger than the whole budget or a closed budget reaches here.
    fn from_admission(error: Report<AdmissionError>) -> Report<Self> {
        let failure = match error.current_context() {
            AdmissionError::ExceedsBudget { .. } | AdmissionError::DifferentBudget { .. } => {
                Self::ExceedsBudget
            }
            AdmissionError::BudgetExhausted { .. } | AdmissionError::BudgetClosed { .. } => {
                Self::Busy
            }
        };
        error.change_context(failure)
    }

    fn from_execution(error: Report<ExecutionError>) -> Report<Self> {
        let failure = match error.current_context() {
            ExecutionError::QueueFull { .. } | ExecutionError::PoolClosed { .. } => Self::Busy,
            ExecutionError::JobPanicked { .. } => Self::Task,
        };
        error.change_context(failure)
    }
}

impl GrpcAuthenticationError {
    /// The status a call whose credentials did not authenticate it ends with. It names the
    /// refusal and nothing beneath it.
    pub(in crate::application) fn status(report: Report<Self>) -> Status {
        let refusal = report.current_context();
        match refusal {
            Self::Required | Self::Failed => Status::unauthenticated(refusal.to_string()),
            Self::Busy => Status::unavailable(refusal.to_string()),
        }
    }
}

/// Derive a credential on the isolated credentials worker, under the fixed workspace charge.
async fn hash_password(
    executor: &Executor,
    password: String,
    cost: PasswordCost,
) -> error_stack::Result<PasswordCredential, PasswordHashError> {
    let reservation = executor
        .reserve(
            MemoryClass::Credentials,
            executor.limits().credential_working_bytes.as_u64(),
        )
        .await
        .map_err(PasswordHashError::from_admission)?;
    let hashed = executor
        .run_cpu(
            CpuClass::Credentials,
            reservation,
            move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(PasswordHashError::Cancelled)?;
                let salt = nervix_crypto::random::bytes::<16>();
                let digest =
                    nervix_crypto::password::derive(cost.get(), &salt, password.as_bytes());
                Ok(PasswordCredential::Pbkdf2HmacSha512 { cost, salt, digest })
            },
        )
        .await;
    match hashed {
        Ok(value) => value,
        Err(error) => Err(PasswordHashError::from_execution(error)),
    }
}

/// Replay compares only the admitted credential; public authentication performs uniform work.
enum PasswordVerification {
    Admitted(PasswordCredential),
    Authentication(nervix_consensus::AuthenticationInput),
}

impl PasswordVerification {
    fn verify(
        self,
        password: &[u8],
        cancellation: &nervix_execution::Cancellation,
    ) -> error_stack::Result<bool, PasswordHashError> {
        match self {
            Self::Admitted(credential) => Ok(nervix_crypto::password::verify(
                credential.cost().get(),
                credential.salt(),
                password,
                credential.digest(),
            )),
            Self::Authentication(input) => {
                let work = input.work();
                let (matched, cost) = match input.into_user() {
                    Some(user) => {
                        let credential = user.password_credential;
                        let matched = nervix_crypto::password::verify(
                            credential.cost().get(),
                            credential.salt(),
                            password,
                            credential.digest(),
                        );
                        (matched, credential.cost())
                    }
                    None => {
                        // No user is fabricated: this is only the same cryptographic workload.
                        std::hint::black_box(nervix_crypto::password::verify(
                            work.get(),
                            &[0; 16],
                            password,
                            &[0; 64],
                        ));
                        (false, work)
                    }
                };
                cancellation
                    .check()
                    .change_context(PasswordHashError::Cancelled)?;
                // Every authentication does two native calls and the same total iteration work,
                // including a credential written before the creation policy increased.
                let padding = work
                    .get()
                    .get()
                    .checked_sub(cost.get().get())
                    .assured("the captured work count covers every credential in its publication")
                    .checked_add(1)
                    .assured("subtracting a positive cost leaves room for one iteration");
                let padding =
                    NonZeroU32::new(padding).assured("one padding iteration is always included");
                std::hint::black_box(nervix_crypto::password::derive(padding, &[0; 16], password));
                Ok(matched)
            }
        }
    }
}

async fn verify_password(
    executor: &Executor,
    verification: PasswordVerification,
    password: String,
) -> error_stack::Result<bool, PasswordHashError> {
    let reservation = executor
        .reserve(
            MemoryClass::Credentials,
            executor.limits().credential_working_bytes.as_u64(),
        )
        .await
        .map_err(PasswordHashError::from_admission)?;
    let verified = executor
        .run_cpu(
            CpuClass::Credentials,
            reservation,
            move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(PasswordHashError::Cancelled)?;
                verification.verify(password.as_bytes(), cancellation)
            },
        )
        .await;
    match verified {
        Ok(value) => value,
        Err(error) => Err(PasswordHashError::from_execution(error)),
    }
}

pub(in crate::application) async fn verify_password_credential(
    executor: &Executor,
    credential: PasswordCredential,
    password: String,
) -> error_stack::Result<bool, PasswordHashError> {
    verify_password(
        executor,
        PasswordVerification::Admitted(credential),
        password,
    )
    .await
}

pub(in crate::application) async fn user_credentials(
    executor: &Executor,
    name: UserName,
    password: String,
    cost: PasswordCost,
) -> error_stack::Result<UserCredentials, PasswordHashError> {
    let password_credential = hash_password(executor, password, cost).await?;
    Ok(UserCredentials {
        name,
        password_credential,
    })
}

impl SessionServiceImpl {
    pub(in crate::application) fn password_creation_cost(&self) -> PasswordCost {
        PasswordCost::try_from(self.cluster_parameters().password_iterations())
            .assured("the committed parameter validates its cost")
    }

    pub(in crate::application) fn new_authentication_pacing() -> AuthenticationPacing {
        AuthenticationPacing::new(
            NonZeroU32::new(AUTH_RATE_LIMIT_PER_SECOND)
                .assured("AUTH_RATE_LIMIT_PER_SECOND is a positive constant"),
        )
    }

    /// Authenticates the Basic credentials a call presented, or refuses a call that presented none.
    pub(in crate::application) async fn authenticate(
        &self,
        credentials: Option<&BasicAuthCredentials>,
    ) -> Result<UserName, AuthenticationRefusal> {
        let Some(credentials) = credentials else {
            return Err(AuthenticationRefusal::Missing);
        };
        self.authenticate_basic_credentials(credentials).await
    }

    /// Whether `credentials` name a user and that user's password.
    ///
    /// Every name receives the common bounded verification workload captured with its user
    /// publication. Unknown and invalid names use the same credentials worker and memory charge
    /// as known names; lower-cost credentials receive padding to that captured work count.
    async fn authenticate_basic_credentials(
        &self,
        credentials: &BasicAuthCredentials,
    ) -> Result<UserName, AuthenticationRefusal> {
        let user_name = UserName::parse(&credentials.username).ok();
        let name = match &user_name {
            Some(name) => name.as_str().to_owned(),
            None => credentials.username.to_ascii_lowercase(),
        };
        let admission = self
            .inner
            .authentication_pacing
            .before_verification(&name)
            .await;
        let outcome = self.verify_basic_credentials(credentials, user_name).await;
        match &outcome {
            Ok(_) => self.inner.authentication_pacing.authenticated(&name).await,
            Err(AuthenticationRefusal::Rejected) => {
                self.inner
                    .authentication_pacing
                    .refused(name, admission)
                    .await;
            }
            Err(AuthenticationRefusal::Missing | AuthenticationRefusal::Unavailable) => {}
        }
        outcome
    }

    /// Verifies credentials only after their presented name's quota has admitted the attempt.
    async fn verify_basic_credentials(
        &self,
        credentials: &BasicAuthCredentials,
        user_name: Option<UserName>,
    ) -> Result<UserName, AuthenticationRefusal> {
        let Some(input) = self
            .inner
            .consensus
            .authentication_input(user_name.as_ref())
        else {
            return Err(AuthenticationRefusal::Unavailable);
        };
        let verified = verify_password(
            self.inner.runtime.executor(),
            PasswordVerification::Authentication(input),
            credentials.password.clone(),
        )
        .await;
        let matched = match verified {
            Ok(matched) => matched,
            Err(error) => {
                if let PasswordHashError::Busy = error.current_context() {
                    debug!(error = %format_args!("{error:#}"), "credential verification was not admitted");
                } else {
                    warn!(error = %format_args!("{error:#}"), "credential verification failed");
                }
                // Only a completed verifier can judge a password. A worker failure neither
                // paces the user nor clears a pacing it already has.
                return Err(AuthenticationRefusal::Unavailable);
            }
        };
        if matched {
            return Ok(user_name.assured("only a captured user can match a password"));
        }
        Err(AuthenticationRefusal::Rejected)
    }

    pub(in crate::application) async fn create_user(
        &self,
        create: CreateStatement<CreateUser>,
    ) -> CommandResult {
        let if_not_exists = create.if_not_exists;
        let create = create.body;
        if self
            .inner
            .consensus
            .current_user(&create.name)
            .await
            .is_some()
        {
            if if_not_exists {
                return match self.wait_for_authoritative_visibility().await {
                    Ok(()) => command_ok_already_existed(format!(
                        "user '{}' already exists",
                        create.name.as_str()
                    )),
                    Err(error) => command_error(format!(
                        "user '{}' exists, but authoritative visibility did not complete: {error}",
                        create.name.as_str()
                    )),
                };
            }
            return command_error(format!("user '{}' already exists", create.name.as_str()));
        }
        let user = match user_credentials(
            self.inner.runtime.executor(),
            create.name.clone(),
            create.password,
            self.password_creation_cost(),
        )
        .await
        {
            Ok(user) => user,
            Err(error) => {
                return command_error(format!(
                    "failed to hash password for user '{}': {error}",
                    create.name.as_str()
                ));
            }
        };
        match self.inner.consensus.create_user(user).await {
            Ok(()) => match self.wait_for_authoritative_visibility().await {
                Ok(()) => command_ok(format!("created user '{}'", create.name.as_str())),
                Err(error) => command_error(format!(
                    "created user '{}', but authoritative visibility did not complete: {error}",
                    create.name.as_str()
                )),
            },
            Err(error) => {
                self.consensus_report_response(
                    &error,
                    format!("failed to create user '{}'", create.name.as_str()),
                )
                .await
            }
        }
    }

    pub(in crate::application) async fn apply_persistent_user_creation(
        &self,
        if_not_exists: bool,
        user: UserCredentials,
    ) -> CommandResult {
        if let Some(existing) = self.inner.consensus.current_user(&user.name).await {
            if existing == user {
                return match self.wait_for_authoritative_visibility().await {
                    Ok(()) => command_ok(format!("created user '{}'", existing.name.as_str())),
                    Err(error) => command_error(format!(
                        "created user '{}', but authoritative visibility did not complete: {error}",
                        existing.name.as_str()
                    )),
                };
            }
            if if_not_exists {
                return match self.wait_for_authoritative_visibility().await {
                    Ok(()) => command_ok_already_existed(format!(
                        "user '{}' already exists",
                        user.name.as_str()
                    )),
                    Err(error) => command_error(format!(
                        "user '{}' exists, but authoritative visibility did not complete: {error}",
                        user.name.as_str()
                    )),
                };
            }
            return command_error(format!("user '{}' already exists", user.name.as_str()));
        }

        let name = user.name.clone();
        match self.inner.consensus.create_user(user).await {
            Ok(()) => match self.wait_for_authoritative_visibility().await {
                Ok(()) => command_ok(format!("created user '{}'", name.as_str())),
                Err(error) => command_error(format!(
                    "created user '{}', but authoritative visibility did not complete: {error}",
                    name.as_str()
                )),
            },
            Err(error) => {
                self.consensus_report_response(
                    &error,
                    format!("failed to create user '{}'", name.as_str()),
                )
                .await
            }
        }
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use std::time::Duration;

    #[cfg(feature = "testing")]
    use nervix_consensus::{ConsensusTestProbe, StorageBoundary};
    use nervix_execution::{ExecutionConfig, MemoryBudgets, OperationLimits};
    use nervix_models::{CreateStatement, CreateUser, PasswordCost, UserName};
    use nervix_recovery::Discarded as _;
    use ubyte::ByteUnit;

    use super::*;
    #[cfg(feature = "testing")]
    use crate::application::test_fixtures::build_test_service_with_probe;
    use crate::{
        application::test_fixtures::{
            TestService, build_test_service, build_test_service_with_executor,
        },
        runtime::{FilledCpuClass, single_worker_executor},
    };

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn user_creation_storage_failure_keeps_user_absent() {
        let probe = ConsensusTestProbe::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_probe(false, probe.clone()).await;
        let user = UserName::parse("report_user").expect("valid test user name");
        probe.storage_fault().fail_next(
            "create-user:report_user".to_string(),
            StorageBoundary::BeforeCommit,
        );

        let result = service
            .create_user(CreateStatement::new(
                CreateUser {
                    name: user.clone(),
                    password: "secret-password".to_string(),
                },
                false,
            ))
            .await;
        assert!(!result.succeeded(), "{result:?}");
        assert!(result.message.contains("consensus storage"), "{result:?}");
        assert!(service.inner.consensus.current_user(&user).await.is_none());

        drop(service);
        drop(registry);
        let _ = std::fs::remove_dir_all(path);
    }

    #[nervix_primitives::test]
    async fn user_creation_retries_preserve_the_admitted_credentials() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        let direct_name = UserName::parse("direct_user")
            .assured("the test user name is an identifier-shaped literal");

        let created = service
            .create_user(CreateStatement::new(
                CreateUser {
                    name: direct_name.clone(),
                    password: "direct-secret".to_string(),
                },
                false,
            ))
            .await;
        assert!(
            created.succeeded(),
            "user creation must succeed: {created:?}"
        );
        let duplicate = service
            .create_user(CreateStatement::new(
                CreateUser {
                    name: direct_name,
                    password: "ignored-secret".to_string(),
                },
                true,
            ))
            .await;
        assert!(
            duplicate.succeeded(),
            "idempotent creation must succeed: {duplicate:?}"
        );
        assert!(duplicate.found_existing());

        let retained_name = UserName::parse("retained_user")
            .assured("the test user name is an identifier-shaped literal");
        let retained = user_credentials(
            service.inner.runtime.executor(),
            retained_name.clone(),
            "retained-secret".to_string(),
            service.password_creation_cost(),
        )
        .await
        .assured("the admitted password cost is within its declared bound");
        let applied = service
            .apply_persistent_user_creation(false, retained.clone())
            .await;
        assert!(
            applied.succeeded(),
            "admitted user creation must succeed: {applied:?}"
        );

        let resumed = service
            .apply_persistent_user_creation(false, retained)
            .await;
        assert_eq!(resumed, applied);

        let conflicting = user_credentials(
            service.inner.runtime.executor(),
            retained_name,
            "different-secret".to_string(),
            service.password_creation_cost(),
        )
        .await
        .assured("the admitted password cost is within its declared bound");
        let ignored_conflict = service
            .apply_persistent_user_creation(true, conflicting.clone())
            .await;
        assert!(ignored_conflict.succeeded());
        assert!(ignored_conflict.found_existing());

        let rejected_conflict = service
            .apply_persistent_user_creation(false, conflicting)
            .await;
        assert!(!rejected_conflict.succeeded());
        assert!(rejected_conflict.message.contains("already exists"));

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn credentials_the_node_could_not_verify_are_answered_as_unavailable() {
        assert_eq!(
            GrpcAuthenticationError::status(Report::new(GrpcAuthenticationError::Busy)).code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            GrpcAuthenticationError::status(Report::new(GrpcAuthenticationError::Failed)).code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            busy_authentication_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn credential_operations_charge_one_profile_digest() {
        assert_eq!(
            64,
            OperationLimits::default().credential_working_bytes.as_u64()
        );
    }

    #[nervix_primitives::test]
    async fn passwords_are_hashed_and_verified_on_the_credentials_worker() {
        let executor = Executor::new(ExecutionConfig {
            budgets: MemoryBudgets {
                credentials: ByteUnit::Byte(64),
                ..Default::default()
            },
            ..Default::default()
        })
        .assured("one profile digest is enough working memory for password operations");
        let password_credential = hash_password(
            &executor,
            "secret".to_string(),
            PasswordCost::try_from(210000).assured("the test cost is within its definition"),
        )
        .await
        .expect("the credentials worker hashes the password");
        let matched =
            verify_password_credential(&executor, password_credential, "secret".to_string())
                .await
                .expect("the credentials worker verifies the password");

        assert!(matched);
        let snapshot = executor.snapshot();
        assert_eq!(snapshot.credentials_cpu.admitted, 2);
        assert_eq!(snapshot.credentials_cpu.completed, 2);
        assert_eq!(snapshot.credentials_memory.granted, 2);
        assert_eq!(snapshot.credentials_memory.reserved_bytes, 0);
        assert_eq!(snapshot.bulk_cpu.admitted, 0);
    }

    #[nervix_primitives::test]
    async fn a_node_without_room_to_verify_credentials_does_not_judge_them() {
        let executor = single_worker_executor();
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service_with_executor(false, executor.clone()).await;
        let created = service
            .create_user(CreateStatement::new(
                CreateUser {
                    name: UserName::parse("busy_user")
                        .assured("the test user name is an identifier-shaped literal"),
                    password: "busy-secret".to_string(),
                },
                false,
            ))
            .await;
        assert!(
            created.succeeded(),
            "user creation must succeed: {created:?}"
        );
        let credentials = BasicAuthCredentials {
            username: "busy_user".to_string(),
            password: "busy-secret".to_string(),
        };

        let filled = FilledCpuClass::fill(&executor, CpuClass::Credentials).await;
        let refused = service.authenticate_basic_credentials(&credentials).await;
        assert_eq!(refused, Err(AuthenticationRefusal::Unavailable));
        // Nothing judged the credentials, so the user is not paced as after a failed attempt.
        assert!(service.inner.authentication_pacing.is_empty().await);

        let unknown = BasicAuthCredentials {
            username: "unknown_user".to_string(),
            password: "busy-secret".to_string(),
        };
        let refused = service.authenticate_basic_credentials(&unknown).await;
        assert_eq!(
            refused,
            Err(AuthenticationRefusal::Unavailable),
            "a name that names no user is not judged either while the node has no room"
        );
        assert!(service.inner.authentication_pacing.is_empty().await);
        let invalid = BasicAuthCredentials {
            username: "not a user".to_string(),
            password: "busy-secret".to_string(),
        };
        assert_eq!(
            service.authenticate_basic_credentials(&invalid).await,
            Err(AuthenticationRefusal::Unavailable)
        );
        assert!(service.inner.authentication_pacing.is_empty().await);

        filled.release().await;
        let accepted = service.authenticate_basic_credentials(&credentials).await;
        assert_eq!(
            accepted,
            Ok(UserName::parse("busy_user").assured("the name parsed above"))
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    /// The credentials work an executor has done.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct CredentialsWork {
        /// Jobs the credentials worker admitted.
        jobs: u64,
        /// Charges the credentials memory granted.
        charges: u64,
    }

    impl CredentialsWork {
        fn of(executor: &Executor) -> Self {
            let snapshot = executor.snapshot();
            Self {
                jobs: snapshot.credentials_cpu.admitted,
                charges: snapshot.credentials_memory.granted,
            }
        }

        fn after_one_verification(self) -> Self {
            Self {
                jobs: self.jobs.checked_add(1).assured("a test admits few jobs"),
                charges: self
                    .charges
                    .checked_add(1)
                    .assured("a test grants few charges"),
            }
        }
    }

    #[nervix_primitives::test]
    async fn every_presented_name_costs_the_credentials_work_of_one_verification() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        let created = service
            .create_user(CreateStatement::new(
                CreateUser {
                    name: UserName::parse("known_user")
                        .assured("the test user name is an identifier-shaped literal"),
                    password: "known-secret".to_string(),
                },
                false,
            ))
            .await;
        assert!(
            created.succeeded(),
            "user creation must succeed: {created:?}"
        );
        let executor = service.inner.runtime.executor().clone();

        for presented in [
            BasicAuthCredentials {
                username: "known_user".to_string(),
                password: "wrong-secret".to_string(),
            },
            BasicAuthCredentials {
                username: "unknown_user".to_string(),
                password: "any-secret".to_string(),
            },
            BasicAuthCredentials {
                username: "not a user name".to_string(),
                password: "any-secret".to_string(),
            },
        ] {
            let before = CredentialsWork::of(&executor);
            let refused = service.authenticate(Some(&presented)).await;
            assert_eq!(
                refused,
                Err(AuthenticationRefusal::Rejected),
                "{:?} does not authenticate",
                presented.username
            );
            assert_eq!(
                CredentialsWork::of(&executor),
                before.after_one_verification(),
                "{:?} costs the node one verification",
                presented.username
            );
        }

        let before = CredentialsWork::of(&executor);
        let missing = service.authenticate(None).await;
        assert_eq!(missing, Err(AuthenticationRefusal::Missing));
        assert_eq!(
            CredentialsWork::of(&executor),
            before,
            "a call without credentials has nothing to verify"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    #[nervix_primitives::test]
    async fn unknown_names_are_paced_after_their_first_refusal() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        for username in ["unknown_user", "not a user name"] {
            let credentials = BasicAuthCredentials {
                username: username.to_string(),
                password: "any-secret".to_string(),
            };
            assert_eq!(
                service.authenticate(Some(&credentials)).await,
                Err(AuthenticationRefusal::Rejected)
            );
            let started = nervix_primitives::time::Instant::now();
            for _ in 0..21 {
                nervix_primitives::task::consume_budget().await;
                assert_eq!(
                    service.authenticate(Some(&credentials)).await,
                    Err(AuthenticationRefusal::Rejected)
                );
            }
            assert!(
                started.elapsed() >= Duration::from_secs(1),
                "repeated refusals for {username:?} must wait on that name's quota"
            );
        }
        tokio::fs::remove_dir_all(&path)
            .await
            .discarded("the test's temporary database is cleaned up on a best-effort basis");
    }
}
