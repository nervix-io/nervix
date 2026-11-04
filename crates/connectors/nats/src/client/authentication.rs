//! NATS user identities and nonce authentication.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** NATS seed and credentials decoding, public-key encoding and the driver auth callback.
//! - **Depends on.** AWS-LC signing, wire encodings, primitive shared ownership and the NATS driver.
//! - **Must not know.** Models, graph routing, connector lifecycle or TLS policy.
//!
//! AWS-LC signing moves to `nervix-crypto` when that boundary is established (FIPS Prep 02).

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the NATS authentication wire boundary")
)]

use async_nats::{Auth, AuthError, ConnectOptions};
use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair as _};
use data_encoding::BASE32_NOPAD;
use error_stack::{Report, ResultExt as _};
use nervix_primitives::sync::StdArc;
use thiserror::Error;
use zeroize::Zeroizing;

// NATS nkeys encode a 32-byte seed with a two-byte user-seed prefix and a little-endian
// CRC-16/XMODEM. The CRC detects transcription errors; it establishes no authenticity.
#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "NATS nkeys user-seed wire prefix")
)]
const USER_SEED_PREFIX: [u8; 2] = [0x95, 0x00];
#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "NATS nkeys user public-key wire prefix")
)]
const USER_PUBLIC_KEY_PREFIX: u8 = 0xa0;
#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "base32 length of the NATS nkeys seed wire record")
)]
const ENCODED_SEED_BYTES: usize = 58;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "NATS nkeys two-byte prefix, 32-byte seed and two-byte checksum"
    )
)]
const DECODED_SEED_BYTES: usize = 36;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "NATS nkeys one-byte prefix, 32-byte public key and two-byte checksum"
    )
)]
const PUBLIC_KEY_BYTES: usize = 35;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "NATS nkeys specifies CRC-16/XMODEM for transcription errors"
    )
)]
const CHECKSUM: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);

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
    key: Ed25519KeyPair,
    public: String,
}

impl SigningIdentity {
    fn from_seed(seed: &str) -> Result<Self, Report<NatsAuthenticationError>> {
        if seed.len() != ENCODED_SEED_BYTES {
            return Err(Report::new(NatsAuthenticationError::SeedLength));
        }
        let raw = Zeroizing::new(
            BASE32_NOPAD
                .decode(seed.as_bytes())
                .change_context(NatsAuthenticationError::SeedEncoding)?,
        );
        let raw: &[u8; DECODED_SEED_BYTES] = raw
            .as_slice()
            .try_into()
            .map_err(|_| Report::new(NatsAuthenticationError::SeedLength))?;
        if raw[..2] != USER_SEED_PREFIX {
            return Err(Report::new(NatsAuthenticationError::SeedKind));
        }
        if CHECKSUM.checksum(&raw[..34]) != u16::from_le_bytes([raw[34], raw[35]]) {
            return Err(Report::new(NatsAuthenticationError::SeedChecksum));
        }
        let key = Ed25519KeyPair::from_seed_unchecked(&raw[2..34])
            .change_context(NatsAuthenticationError::SigningKey)?;
        let mut public = [0u8; PUBLIC_KEY_BYTES];
        public[0] = USER_PUBLIC_KEY_PREFIX;
        public[1..33].copy_from_slice(key.public_key().as_ref());
        let checksum = CHECKSUM.checksum(&public[..33]).to_le_bytes();
        public[33..].copy_from_slice(&checksum);
        Ok(Self {
            key,
            public: BASE32_NOPAD.encode(&public),
        })
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
        // AuthError is the driver's required callback result. Its opaque AWS-LC cause has no
        // credential material; decoding and file failures retain their typed reports above it.
        auth.signature = Some(
            signer
                .key
                .try_sign(nonce)
                .map_err(AuthError::new)?
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
    let section = |begin: &str, end: &str| {
        let mut lines = contents.lines().map(str::trim);
        lines.find(|line| *line == begin)?;
        let value = lines.next().filter(|line| !line.is_empty())?;
        (lines.next()? == end).then_some(value)
    };
    let jwt = section(
        "-----BEGIN NATS USER JWT-----",
        "------END NATS USER JWT------",
    )
    .ok_or_else(|| Report::new(NatsAuthenticationError::Credentials))?;
    let seed = section(
        "-----BEGIN USER NKEY SEED-----",
        "------END USER NKEY SEED------",
    )
    .ok_or_else(|| Report::new(NatsAuthenticationError::Credentials))?;
    Ok(AuthenticationIdentity::Credentials {
        jwt: jwt.to_owned(),
        signer: SigningIdentity::from_seed(seed)?,
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
