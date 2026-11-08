//! ECDSA P-256/SHA-256 with fixed-size signatures and zeroizing private-key ownership.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** P-256 key generation/import/export, public coordinates, signing and verification.
//! - **Depends on.** AWS-LC's maintained ECDSA/PKCS#8 implementation and typed reports.
//! - **Must not know.** JWK, JWS, licensing, Models, credentials formats or runtime policy.

use std::fmt;

use aws_lc_rs::{
    pkcs8::Document,
    rand::SystemRandom,
    signature::{
        ECDSA_P256_SHA256_FIXED, ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair as _,
        UnparsedPublicKey,
    },
};
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use thiserror::Error;

/// The operation that failed; backend causes remain beneath this value-free context.
#[derive(Debug, Error)]
pub enum EcdsaP256Error {
    #[error("failed to generate a P-256 private key")]
    GenerateKey,
    #[error("failed to import a P-256 PKCS#8 private key")]
    ImportKey,
    #[error("failed to export a P-256 PKCS#8 private key")]
    ExportKey,
    #[error("failed to sign with the P-256 key")]
    Sign,
    #[error("P-256 signature verification failed")]
    Verify,
}

/// Unencrypted PKCS#8 v1 DER. The maintained document zeroizes its allocation on drop.
///
/// Borrowed bytes must be kept confidential by their consumer. This owner neither clones the
/// secret allocation nor includes its bytes in diagnostics.
pub struct Pkcs8PrivateKey {
    document: Document,
}

impl AsRef<[u8]> for Pkcs8PrivateKey {
    fn as_ref(&self) -> &[u8] {
        self.document.as_ref()
    }
}

impl fmt::Debug for Pkcs8PrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pkcs8PrivateKey").finish_non_exhaustive()
    }
}

/// A private signing key. AWS-LC clears the private scalar when its key ownership ends.
pub struct EcdsaP256KeyPair {
    key: EcdsaKeyPair,
}

impl fmt::Debug for EcdsaP256KeyPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EcdsaP256KeyPair").finish_non_exhaustive()
    }
}

/// Public, big-endian, fixed-width affine coordinates of an uncompressed P-256 point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct P256PublicCoordinates {
    pub x: [u8; 32],
    pub y: [u8; 32],
}

/// Exactly 64 bytes: the unsigned 32-byte big-endian `r` followed by `s`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EcdsaP256Signature([u8; 64]);

impl EcdsaP256Signature {
    /// Carry a fixed-width signature. Cryptographic validity is checked by [`verify`].
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}

impl EcdsaP256KeyPair {
    /// Generate unencrypted PKCS#8 v1 DER using AWS-LC's system-seeded random generator.
    pub fn generate_pkcs8() -> Result<Pkcs8PrivateKey, Report<EcdsaP256Error>> {
        let document =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .change_context(EcdsaP256Error::GenerateKey)?;
        Ok(Pkcs8PrivateKey { document })
    }

    /// Import a P-256 PKCS#8 private key; AWS-LC validates its curve and key components.
    /// The caller retains responsibility for clearing its input storage.
    pub fn from_pkcs8(bytes: &[u8]) -> Result<Self, Report<EcdsaP256Error>> {
        let key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, bytes)
            .change_context(EcdsaP256Error::ImportKey)?;
        Ok(Self { key })
    }

    /// Export this exact key as unencrypted PKCS#8 v1 DER in a zeroizing owner.
    pub fn to_pkcs8(&self) -> Result<Pkcs8PrivateKey, Report<EcdsaP256Error>> {
        let document = self
            .key
            .to_pkcs8v1()
            .change_context(EcdsaP256Error::ExportKey)?;
        Ok(Pkcs8PrivateKey { document })
    }

    /// Borrow AWS-LC's SEC 1 uncompressed public point (65 bytes, including the prefix).
    #[must_use]
    pub fn public_key(&self) -> &[u8] {
        self.key.public_key().as_ref()
    }

    /// Extract coordinates from AWS-LC's maintained point encoding without re-encoding it.
    #[must_use]
    pub fn public_coordinates(&self) -> P256PublicCoordinates {
        let point = self.public_key();
        let x = point[1..33]
            .try_into()
            .assured("AWS-LC P-256 points have a 32-byte x coordinate");
        let y = point[33..]
            .try_into()
            .assured("AWS-LC P-256 points have a 32-byte y coordinate");
        P256PublicCoordinates { x, y }
    }

    /// Sign the complete message with SHA-256 and a fresh AWS-LC nonce, returning fixed `r‖s`.
    pub fn sign(&self, message: &[u8]) -> Result<EcdsaP256Signature, Report<EcdsaP256Error>> {
        let signature = self
            .key
            .sign(&SystemRandom::new(), message)
            .change_context(EcdsaP256Error::Sign)?;
        let bytes = signature
            .as_ref()
            .try_into()
            .assured("AWS-LC fixed P-256 signatures are 64 bytes");
        Ok(EcdsaP256Signature(bytes))
    }
}

/// Verify fixed `r‖s` against a SEC 1 uncompressed public point and the complete message.
/// AWS-LC owns point validation, signature parsing and SHA-256; malformed inputs are refusals.
pub fn verify(
    public_key: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<(), Report<EcdsaP256Error>> {
    UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, public_key)
        .verify(message, signature)
        .change_context(EcdsaP256Error::Verify)
}
