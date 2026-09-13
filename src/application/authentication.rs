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
use argon2::Params;
#[cfg(feature = "testing")]
use argon2::Version;
use argon2::{
    Argon2, PasswordHasher, PasswordVerifier,
    password_hash::{PasswordHash, SaltString, rand_core::OsRng},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use http_body_util::Full;
use hyper::{
    Request as HyperRequest, Response as HyperResponse, StatusCode,
    body::{Bytes, Incoming as HyperIncoming},
    header::{AUTHORIZATION, WWW_AUTHENTICATE},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::UserCredentials;
use nervix_models::{CreateStatement, CreateUser, UserName};
use thiserror::Error;
use tonic::{Status, metadata::MetadataMap};

use super::{
    model_mutation::{command_error, command_ok, command_ok_already_existed},
    session_service::SessionServiceImpl,
    web_console::{WEB_CONSOLE_AUTH_QUERY_PARAM, web_console_query_param},
};
use crate::proto::CommandResult;

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
}

impl From<GrpcAuthenticationError> for Status {
    fn from(error: GrpcAuthenticationError) -> Self {
        Self::unauthenticated(error.to_string())
    }
}

async fn hash_password(password: String) -> Result<String, String> {
    tokio::task::spawn_blocking(move || {
        let mut rng = OsRng;
        let salt = SaltString::generate(&mut rng);
        password_argon2()
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("password hash task failed: {error}"))?
}

async fn verify_password_hash(password_hash: String, password: String) -> bool {
    tokio::task::spawn_blocking(move || {
        let Ok(parsed_hash) = PasswordHash::new(&password_hash) else {
            return false;
        };
        password_argon2()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    })
    .await
    .unwrap_or(false)
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
    name: UserName,
    password: String,
) -> Result<UserCredentials, String> {
    let password_hash = hash_password(password).await?;
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
        self.authenticate_basic_credentials(&credentials)
            .await
            .ok_or(GrpcAuthenticationError::Failed)
    }

    pub(in crate::application) async fn authenticate_basic_credentials(
        &self,
        credentials: &BasicAuthCredentials,
    ) -> Option<UserName> {
        let Ok(user_name) = UserName::parse(&credentials.username) else {
            return None;
        };
        let user = self.inner.consensus.current_user(&user_name).await?;
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
        let verified = verify_password_hash(user.password_hash, credentials.password.clone()).await;
        if verified {
            self.inner
                .failed_auth_rate_limit_keys
                .remove(&auth_rate_limit_key);
        } else {
            self.inner
                .failed_auth_rate_limit_keys
                .insert(auth_rate_limit_key, ());
        }
        verified.then_some(user_name)
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
                return command_ok_already_existed(format!(
                    "user '{}' already exists",
                    create.name.as_str()
                ));
            }
            return command_error(format!("user '{}' already exists", create.name.as_str()));
        }
        let user = match user_credentials(create.name.clone(), create.password).await {
            Ok(user) => user,
            Err(error) => {
                return command_error(format!(
                    "failed to hash password for user '{}': {error}",
                    create.name.as_str()
                ));
            }
        };
        match self.inner.consensus.create_user(user).await {
            Ok(()) => command_ok(format!("created user '{}'", create.name.as_str())),
            Err(error) => {
                self.consensus_error_response(
                    &error,
                    format!("failed to create user '{}': {error}", create.name.as_str()),
                )
                .await
            }
        }
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn testing_feature_hashes_passwords_with_lean_argon2_params() {
        let password_hash = hash_password("secret".to_string())
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
        assert!(verify_password_hash(password_hash, "secret".to_string()).await);
    }
}
