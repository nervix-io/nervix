//! Maintained ES256 JWS cryptography through the one AWS-LC provider.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The maintained JWS library's provider, signing, verification and JWK thumbprints.
//! - **Depends on.** jsonwebtoken on AWS-LC, P-256 keys and process initialization.
//! - **Must not know.** Licensing documents, trust policy, Models, graphs or runtime decisions.

use error_stack::{Report, ResultExt as _};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
pub use jsonwebtoken::{
    errors::{Error as LibraryError, ErrorKind as LibraryErrorKind},
    jwk::{
        AlgorithmParameters, CommonParameters, EllipticCurve, EllipticCurveKeyParameters,
        EllipticCurveKeyType, Jwk,
    },
};
use nervix_primitives::unmodeled::sync::OnceLock;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::ecdsa::{EcdsaP256KeyPair, P256PublicCoordinates};

#[derive(Debug, Error)]
pub enum JwsError {
    #[error("another JWS cryptography provider was installed before the AWS-LC boundary")]
    ProviderUnavailable,
    #[error("failed to import the JWS signing key")]
    ImportKey,
    #[error("failed to sign the JWS document")]
    Sign,
    #[error("JWS signature verification failed")]
    Verify,
    #[error("failed to prepare the JWS verification key")]
    VerificationKey,
    #[error("failed to compute the JWK thumbprint")]
    Thumbprint,
}

// jsonwebtoken owns a process-global provider. This real initialization contains no protocol
// state and must survive distinct model executions just as that external process default does.
static INSTALLED: OnceLock<bool> = OnceLock::new();

fn ensure_provider() -> Result<(), Report<JwsError>> {
    let installed = INSTALLED.get_or_init(|| {
        jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER
            .install_default()
            .is_ok()
    });
    if *installed {
        Ok(())
    } else {
        Err(Report::new(JwsError::ProviderUnavailable))
    }
}

/// A reusable maintained signing-key owner; its private DER zeroizes on drop.
pub struct SigningKey {
    key: EncodingKey,
    coordinates: P256PublicCoordinates,
}

impl std::fmt::Debug for SigningKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SigningKey").finish_non_exhaustive()
    }
}

impl SigningKey {
    pub fn from_pkcs8(bytes: &[u8]) -> Result<Self, Report<JwsError>> {
        ensure_provider()?;
        let validated = EcdsaP256KeyPair::from_pkcs8(bytes).change_context(JwsError::ImportKey)?;
        Ok(Self {
            key: EncodingKey::from_ec_der(bytes),
            coordinates: validated.public_coordinates(),
        })
    }

    pub const fn public_coordinates(&self) -> P256PublicCoordinates {
        self.coordinates
    }

    pub fn sign<T: Serialize>(
        &self,
        typ: &str,
        kid: &str,
        payload: &T,
    ) -> Result<String, Report<JwsError>> {
        ensure_provider()?;
        let mut header = Header::new(Algorithm::ES256);
        header.typ = Some(typ.to_owned());
        header.kid = Some(kid.to_owned());
        jsonwebtoken::encode(&header, payload, &self.key).change_context(JwsError::Sign)
    }
}

/// Verify the original signing input before deserializing the payload. All time, audience and
/// binding policy belongs to the caller; neither expiry nor not-before enables a physical read.
pub fn verify<T: DeserializeOwned>(compact: &str, public_jwk: &Jwk) -> Result<T, Report<JwsError>> {
    ensure_provider()?;
    let key = DecodingKey::from_jwk(public_jwk).change_context(JwsError::VerificationKey)?;
    let mut validation = Validation::new(Algorithm::ES256);
    validation.required_spec_claims.clear();
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.validate_aud = false;
    let verified =
        jsonwebtoken::decode::<T>(compact, &key, &validation).change_context(JwsError::Verify)?;
    Ok(verified.claims)
}

pub fn thumbprint(public_jwk: &Jwk) -> Result<String, Report<JwsError>> {
    ensure_provider()?;
    public_jwk
        .thumbprint(jsonwebtoken::jwk::ThumbprintHash::SHA256)
        .change_context(JwsError::Thumbprint)
}
