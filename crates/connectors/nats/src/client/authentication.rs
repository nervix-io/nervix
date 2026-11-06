//! NATS user identities and nonce authentication.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** NATS user identity selection and the driver auth callback.
//! - **Depends on.** The cryptography boundary, maintained NKey/credentials codecs and primitive ownership.
//! - **Must not know.** Models, graph routing, connector lifecycle or TLS policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the NATS authentication wire boundary")
)]

use async_nats::{Auth, AuthError, ConnectOptions, auth_utils::UserCredentials};
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_crypto::ed25519::Ed25519SigningKey;
use nervix_primitives::sync::StdArc;
use nkeys_codec::{KeyPairType, error::ErrorKind};
use thiserror::Error;

#[derive(Debug, Error)]
pub(super) enum NatsAuthenticationError {
    #[error("a NATS user seed must have its complete encoded length")]
    SeedLength,
    #[error("invalid NATS user seed encoding")]
    SeedEncoding,
    #[error("invalid NATS user seed checksum")]
    SeedChecksum,
    #[error("the NATS seed must identify a user")]
    SeedKind,
    #[error("failed to prepare the NATS signing key")]
    SigningKey,
    #[error("the NATS credentials file must contain a complete user JWT and user seed")]
    Credentials,
}

struct SigningIdentity {
    key: Ed25519SigningKey,
    public: String,
}

impl SigningIdentity {
    fn from_seed(seed: &str) -> Result<Self, Report<NatsAuthenticationError>> {
        let raw =
            nkeys_codec::decode_seed_for(seed, KeyPairType::User).map_err(Self::seed_error)?;
        let key = Ed25519SigningKey::from_seed(raw.as_ref())
            .change_context(NatsAuthenticationError::SigningKey)?;
        let public = key
            .public_key()
            .try_into()
            .assured("the cryptography boundary returns a 32-byte Ed25519 public key");
        let public = nkeys_codec::encode(&KeyPairType::User, public);
        Ok(Self { key, public })
    }

    fn seed_error(error: nkeys_codec::error::Error) -> Report<NatsAuthenticationError> {
        let context = match error.kind() {
            ErrorKind::InvalidKeyLength => NatsAuthenticationError::SeedLength,
            ErrorKind::InvalidPrefix | ErrorKind::IncorrectKeyType => {
                NatsAuthenticationError::SeedKind
            }
            ErrorKind::ChecksumFailure => NatsAuthenticationError::SeedChecksum,
            _ => NatsAuthenticationError::SeedEncoding,
        };
        Report::new(error).change_context(context)
    }
}

enum AuthenticationIdentity {
    Nkey(SigningIdentity),
    Credentials {
        jwt: String,
        signer: SigningIdentity,
    },
}

impl AuthenticationIdentity {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            library,
            reason = "async-nats requires AuthError as its authentication callback error"
        )
    )]
    fn authenticate(&self, nonce: &[u8]) -> Result<Auth, AuthError> {
        let (signer, mut auth) = match self {
            Self::Nkey(signer) => {
                let mut auth = Auth::new();
                auth.nkey = Some(signer.public.clone());
                (signer, auth)
            }
            Self::Credentials { jwt, signer } => {
                let mut auth = Auth::new();
                auth.jwt = Some(jwt.clone());
                (signer, auth)
            }
        };
        // AuthError is the driver's required callback result. Preserve the boundary's typed
        // signing report, which contains no credential material.
        auth.signature = Some(
            signer
                .key
                .sign(nonce)
                .map_err(|error| AuthError::new(error.into_error()))?
                .as_ref()
                .to_vec(),
        );
        Ok(auth)
    }

    fn into_options(self) -> ConnectOptions {
        let identity = StdArc::new(self);
        ConnectOptions::with_auth_callback(move |nonce| {
            let identity = identity.clone();
            async move { identity.authenticate(&nonce) }
        })
    }
}

pub(super) fn nkey_options(seed: &str) -> Result<ConnectOptions, Report<NatsAuthenticationError>> {
    Ok(AuthenticationIdentity::Nkey(SigningIdentity::from_seed(seed)?).into_options())
}

fn credentials_identity(
    contents: &str,
) -> Result<AuthenticationIdentity, Report<NatsAuthenticationError>> {
    let credentials =
        UserCredentials::parse(contents).change_context(NatsAuthenticationError::Credentials)?;
    Ok(AuthenticationIdentity::Credentials {
        jwt: credentials.jwt().to_owned(),
        signer: SigningIdentity::from_seed(credentials.seed())?,
    })
}

pub(super) fn credentials_options(
    contents: &str,
) -> Result<ConnectOptions, Report<NatsAuthenticationError>> {
    Ok(credentials_identity(contents)?.into_options())
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "interoperability tests are outside product execution"
    )
)]
mod tests;
