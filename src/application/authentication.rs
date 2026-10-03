//! Who a request belongs to, and whether it may act.
//!
//! Layer: control plane.
//!
//! - **Owns.** Basic credentials on every transport, the password hash, the user record, and the
//!   rate limit an unauthenticated caller is held to.
//! - **Depends on.** Consensus for the stored user credentials.
//! - **Must not know.** What an authenticated caller goes on to do.

use std::num::NonZeroU32;

#[cfg(feature = "testing")]
use argon2::Algorithm;
#[cfg(feature = "testing")]
use argon2::Version;
use argon2::{
    Argon2, Params, PasswordHasher, PasswordVerifier,
    password_hash::{PasswordHash, SaltString, rand_core::OsRng},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use error_stack::{Report, ResultExt as _};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use http_body_util::Full;
use hyper::{
    Request as HyperRequest, Response as HyperResponse, StatusCode,
    body::{Bytes, Incoming as HyperIncoming},
    header::{AUTHORIZATION, WWW_AUTHENTICATE},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::UserCredentials;
use nervix_execution::{AdmissionError, CpuClass, ExecutionError, Executor, MemoryClass};
use nervix_models::{CreateStatement, CreateUser, UserName};
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

pub(in crate::application) type AuthRateLimiter = DefaultKeyedRateLimiter<String>;

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

fn credentials_from_metadata(metadata: &MetadataMap) -> Option<BasicAuthCredentials> {
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

/// Why presented credentials did not authenticate a caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum CredentialRejection {
    /// The user is unknown, the password is wrong, or the stored hash could not be verified.
    Failed,
    /// The node's bounded execution could not take the verification now, so the credentials were
    /// not judged at all.
    Busy,
}

#[derive(Debug, Error)]
pub(in crate::application) enum PasswordHashError {
    #[error("password hash computation failed")]
    Compute,
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

impl From<GrpcAuthenticationError> for Status {
    fn from(error: GrpcAuthenticationError) -> Self {
        match error {
            GrpcAuthenticationError::Required | GrpcAuthenticationError::Failed => {
                Self::unauthenticated(error.to_string())
            }
            GrpcAuthenticationError::Busy => Self::unavailable(error.to_string()),
        }
    }
}

/// The working memory Argon2 allocates for one hash with `params`, whose memory cost is in KiB.
fn argon2_working_bytes(params: &Params) -> u64 {
    u64::from(params.m_cost())
        .checked_mul(1024)
        .assured("a KiB count held in a u32 is far below u64::MAX / 1024")
}

/// Hash `password` on the node's credentials worker, charged the working memory Argon2 allocates.
///
/// Argon2 is deliberately expensive in both time and memory, and anyone who can reach a listener
/// can make a node hash or verify. The credentials class keeps that work apart from every other
/// class: a burst of attempts never takes what control, data or bulk work needs, and saturated work
/// there never keeps an operator from authenticating. Its budget bounds how much Argon2 memory the
/// node holds at once, so a burst of hashes waits for room instead of allocating past it.
async fn hash_password(
    executor: &Executor,
    password: String,
) -> error_stack::Result<String, PasswordHashError> {
    let argon2 = password_argon2();
    let reservation = executor
        .reserve(
            MemoryClass::Credentials,
            argon2_working_bytes(argon2.params()),
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
                let salt = SaltString::generate(&mut OsRng);
                match argon2.hash_password(password.as_bytes(), &salt) {
                    Ok(hash) => Ok(hash.to_string()),
                    Err(_) => Err(Report::new(PasswordHashError::Compute)),
                }
            },
        )
        .await;
    match hashed {
        Ok(hashed) => hashed,
        Err(error) => Err(PasswordHashError::from_execution(error)),
    }
}

/// Whether `password` matches `password_hash`, verified on the node's credentials worker under a
/// charge of the working memory the stored hash's own parameters make Argon2 allocate. A hash that
/// does not parse matches nothing, and neither does one whose parameters need more memory than the
/// credentials budget holds.
pub(in crate::application) async fn verify_password_hash(
    executor: &Executor,
    password_hash: String,
    password: String,
) -> error_stack::Result<bool, PasswordHashError> {
    let Ok(parsed_hash) = PasswordHash::new(&password_hash) else {
        return Ok(false);
    };
    let Ok(params) = Params::try_from(&parsed_hash) else {
        return Ok(false);
    };
    let reservation = executor
        .reserve(MemoryClass::Credentials, argon2_working_bytes(&params))
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
                let parsed_hash = PasswordHash::new(&password_hash)
                    .verified("the same hash parsed before its verification was admitted");
                let matched = password_argon2()
                    .verify_password(password.as_bytes(), &parsed_hash)
                    .is_ok();
                Ok(matched)
            },
        )
        .await;
    match verified {
        Ok(verified) => verified,
        Err(error) => Err(PasswordHashError::from_execution(error)),
    }
}

#[cfg(feature = "testing")]
const TESTING_ARGON2_MEMORY_COST: u32 = 8;

#[cfg(feature = "testing")]
const TESTING_ARGON2_TIME_COST: u32 = 1;

#[cfg(feature = "testing")]
const TESTING_ARGON2_PARALLELISM: u32 = 1;

#[cfg(not(feature = "testing"))]
fn password_argon2() -> Argon2<'static> {
    Argon2::default()
}

#[cfg(feature = "testing")]
fn password_argon2() -> Argon2<'static> {
    let params = Params::new(
        TESTING_ARGON2_MEMORY_COST,
        TESTING_ARGON2_TIME_COST,
        TESTING_ARGON2_PARALLELISM,
        None,
    )
    .assured("the testing cost constants are inside the ranges Argon2 accepts");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

pub(in crate::application) async fn user_credentials(
    executor: &Executor,
    name: UserName,
    password: String,
) -> error_stack::Result<UserCredentials, PasswordHashError> {
    let password_hash = hash_password(executor, password).await?;
    Ok(UserCredentials {
        name,
        password_hash,
    })
}

impl SessionServiceImpl {
    pub(in crate::application) fn new_auth_rate_limiter() -> AuthRateLimiter {
        let quota = Quota::per_second(
            NonZeroU32::new(AUTH_RATE_LIMIT_PER_SECOND)
                .assured("AUTH_RATE_LIMIT_PER_SECOND is a positive constant"),
        );
        RateLimiter::keyed(quota)
    }

    pub(in crate::application) async fn authenticate_grpc_metadata(
        &self,
        metadata: &MetadataMap,
    ) -> Result<UserName, GrpcAuthenticationError> {
        let Some(credentials) = credentials_from_metadata(metadata) else {
            return Err(GrpcAuthenticationError::Required);
        };
        match self.authenticate_basic_credentials(&credentials).await {
            Ok(user) => Ok(user),
            Err(CredentialRejection::Failed) => Err(GrpcAuthenticationError::Failed),
            Err(CredentialRejection::Busy) => Err(GrpcAuthenticationError::Busy),
        }
    }

    pub(in crate::application) async fn authenticate_basic_credentials(
        &self,
        credentials: &BasicAuthCredentials,
    ) -> Result<UserName, CredentialRejection> {
        let Ok(user_name) = UserName::parse(&credentials.username) else {
            return Err(CredentialRejection::Failed);
        };
        let Some(user) = self.inner.consensus.current_user(&user_name).await else {
            return Err(CredentialRejection::Failed);
        };
        let auth_rate_limit_key = user_name.as_str().to_string();
        if self
            .inner
            .failed_auth_rate_limit_keys
            .contains_key(&auth_rate_limit_key)
        {
            self.inner
                .auth_rate_limiter
                .until_key_ready(&auth_rate_limit_key)
                .await;
        }
        let verified = verify_password_hash(
            self.inner.runtime.executor(),
            user.password_hash,
            credentials.password.clone(),
        )
        .await;
        let matched = match verified {
            Ok(matched) => matched,
            Err(error) => {
                if let PasswordHashError::Busy = error.current_context() {
                    // Nothing judged the credentials, so the attempt neither paces the user nor
                    // clears a pacing it already has.
                    debug!(user = user_name.as_str(), error = ?error, "credential verification was not admitted");
                    return Err(CredentialRejection::Busy);
                }
                warn!(user = user_name.as_str(), error = ?error, "credential verification failed");
                false
            }
        };
        if matched {
            self.inner
                .failed_auth_rate_limit_keys
                .remove(&auth_rate_limit_key);
            return Ok(user_name);
        }
        self.inner
            .failed_auth_rate_limit_keys
            .insert(auth_rate_limit_key, ());
        Err(CredentialRejection::Failed)
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
    #[cfg(feature = "testing")]
    use nervix_consensus::{ConsensusTestProbe, StorageBoundary};
    use nervix_execution::OperationLimits;
    use nervix_models::{CreateStatement, CreateUser, UserName};

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
    async fn testing_feature_hashes_passwords_with_lean_argon2_params() {
        let executor = Executor::default();
        let password_hash = hash_password(&executor, "secret".to_string())
            .await
            .expect("password hash should be created");
        let parsed_hash =
            PasswordHash::new(&password_hash).expect("password hash should parse as PHC");

        assert_eq!(
            parsed_hash
                .params
                .get("m")
                .and_then(|value| value.decimal().ok()),
            Some(TESTING_ARGON2_MEMORY_COST)
        );
        assert_eq!(
            parsed_hash
                .params
                .get("t")
                .and_then(|value| value.decimal().ok()),
            Some(TESTING_ARGON2_TIME_COST)
        );
        assert_eq!(
            parsed_hash
                .params
                .get("p")
                .and_then(|value| value.decimal().ok()),
            Some(TESTING_ARGON2_PARALLELISM)
        );
        assert!(
            verify_password_hash(&executor, password_hash, "secret".to_string())
                .await
                .expect("the verification should be admitted")
        );
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
        )
        .await
        .assured("the testing Argon2 parameters accept this password");
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
        )
        .await
        .assured("the testing Argon2 parameters accept this password");
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
    fn argon2_is_charged_its_whole_working_memory() {
        let argon2 = password_argon2();
        assert_eq!(
            argon2_working_bytes(argon2.params()),
            u64::from(TESTING_ARGON2_MEMORY_COST) * 1024
        );
    }

    #[test]
    fn credentials_the_node_could_not_verify_are_answered_as_unavailable() {
        assert_eq!(
            Status::from(GrpcAuthenticationError::Busy).code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            Status::from(GrpcAuthenticationError::Failed).code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            busy_authentication_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn the_credentials_budget_holds_one_default_hash() {
        assert_eq!(
            argon2_working_bytes(&Params::default()),
            OperationLimits::default().credential_working_bytes.as_u64()
        );
    }

    #[nervix_primitives::test]
    async fn passwords_are_hashed_and_verified_on_the_credentials_worker() {
        let executor = Executor::default();
        let password_hash = hash_password(&executor, "secret".to_string())
            .await
            .expect("the credentials worker hashes the password");
        let matched = verify_password_hash(&executor, password_hash, "secret".to_string())
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
        assert_eq!(refused, Err(CredentialRejection::Busy));
        // Nothing judged the credentials, so the user is not paced as after a failed attempt.
        assert!(service.inner.failed_auth_rate_limit_keys.is_empty());

        filled.release().await;
        let accepted = service.authenticate_basic_credentials(&credentials).await;
        assert_eq!(
            accepted,
            Ok(UserName::parse("busy_user").assured("the name parsed above"))
        );
        let _ = std::fs::remove_dir_all(&path);
    }
}
