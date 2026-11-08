//! Public P-256 keys and their content-bound RFC 7638 identities.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The public-only JWK profile and canonical key identifiers.
//! - **Depends on.** Maintained base64 and JWK encodings and the cryptography boundary.
//! - **Must not know.** Private-key storage, trust grants, Models or service state.

use std::fmt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_crypto::{ecdsa::P256PublicCoordinates, jws};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PublicKeyError {
    #[error("the public coordinate or key identifier is not canonical base64url")]
    Encoding,
    #[error("the public coordinate or key identifier is not 32 bytes")]
    Width,
    #[error("failed to derive the public key's content identity")]
    Identity,
}

/// An RFC 7638 SHA-256 thumbprint, with exactly one canonical base64url spelling.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct KeyId(String);

impl TryFrom<String> for KeyId {
    type Error = Report<PublicKeyError>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let bytes = URL_SAFE_NO_PAD
            .decode(&value)
            .change_context(PublicKeyError::Encoding)?;
        if bytes.len() != 32 {
            return Err(Report::new(PublicKeyError::Width));
        }
        if URL_SAFE_NO_PAD.encode(&bytes) != value {
            return Err(Report::new(PublicKeyError::Encoding));
        }
        Ok(Self(value))
    }
}

impl From<KeyId> for String {
    fn from(value: KeyId) -> Self {
        value.0
    }
}

impl AsRef<str> for KeyId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum KeyType {
    EC,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Curve {
    #[serde(rename = "P-256")]
    P256,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicKeyWire {
    kty: KeyType,
    crv: Curve,
    x: String,
    y: String,
}

/// Public-only P-256 coordinates. Private parameters and other curves cannot be deserialized.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PublicKeyWire", into = "PublicKeyWire")]
pub struct PublicKey {
    coordinates: P256PublicCoordinates,
}

impl PublicKey {
    pub const fn from_coordinates(coordinates: P256PublicCoordinates) -> Self {
        Self { coordinates }
    }

    pub const fn coordinates(&self) -> P256PublicCoordinates {
        self.coordinates
    }

    pub fn key_id(&self) -> Result<KeyId, Report<PublicKeyError>> {
        let encoded = jws::thumbprint(&self.jwk()).change_context(PublicKeyError::Identity)?;
        encoded.try_into()
    }

    pub(crate) fn jwk(&self) -> jws::Jwk {
        jws::Jwk {
            common: jws::CommonParameters::default(),
            algorithm: jws::AlgorithmParameters::EllipticCurve(jws::EllipticCurveKeyParameters {
                key_type: jws::EllipticCurveKeyType::EC,
                curve: jws::EllipticCurve::P256,
                x: URL_SAFE_NO_PAD.encode(self.coordinates.x),
                y: URL_SAFE_NO_PAD.encode(self.coordinates.y),
            }),
        }
    }
}

impl TryFrom<PublicKeyWire> for PublicKey {
    type Error = Report<PublicKeyError>;

    fn try_from(wire: PublicKeyWire) -> Result<Self, Self::Error> {
        fn coordinate(text: &str) -> Result<[u8; 32], Report<PublicKeyError>> {
            let decoded = URL_SAFE_NO_PAD
                .decode(text)
                .change_context(PublicKeyError::Encoding)?;
            if URL_SAFE_NO_PAD.encode(&decoded) != text {
                return Err(Report::new(PublicKeyError::Encoding));
            }
            if decoded.len() != 32 {
                return Err(Report::new(PublicKeyError::Width));
            }
            Ok(decoded
                .try_into()
                .assured("the coordinate's width was checked to equal 32"))
        }
        Ok(Self {
            coordinates: P256PublicCoordinates {
                x: coordinate(&wire.x)?,
                y: coordinate(&wire.y)?,
            },
        })
    }
}

impl From<PublicKey> for PublicKeyWire {
    fn from(key: PublicKey) -> Self {
        Self {
            kty: KeyType::EC,
            crv: Curve::P256,
            x: URL_SAFE_NO_PAD.encode(key.coordinates.x),
            y: URL_SAFE_NO_PAD.encode(key.coordinates.y),
        }
    }
}
