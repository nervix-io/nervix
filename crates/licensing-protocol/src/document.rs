//! Bounded signed documents under the exact licensing JWS profile.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Profile checks, original signed bytes, signature sets and bounded JSON writes.
//! - **Depends on.** Maintained compact JWS/JSON encodings and the cryptography boundary.
//! - **Must not know.** Trust decisions, networking, clocks, Models or persistence.

use std::num::NonZeroUsize;

use error_stack::{Report, ResultExt as _};
use nervix_bounded_write::{BoundedWrite, BoundedWriter};
use nervix_crypto::jws;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::keys::{KeyId, PublicKey};

/// Allocation and work bounds supplied by the transport or stored-record owner.
#[derive(Clone, Copy, Debug)]
pub struct DocumentLimits {
    pub bytes: NonZeroUsize,
    pub signatures: NonZeroUsize,
    pub trust_documents: NonZeroUsize,
}

macro_rules! document_types {
    ($($variant:ident => $name:literal),* $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum DocumentType { $(#[serde(rename = $name)] $variant,)* }
        impl DocumentType {
            pub const fn as_str(self) -> &'static str { match self { $(Self::$variant => $name,)* } }
        }
    };
}

document_types! {
    Decision => "nervix.licensing.decision",
    Request => "nervix.licensing.request",
    StreamTicket => "nervix.licensing.stream-ticket",
    WorkingKey => "nervix.licensing.trust.working-key",
    Revocation => "nervix.licensing.trust.revocation",
    Succession => "nervix.licensing.trust.succession",
}

pub trait Document: Serialize + DeserializeOwned {
    const TYPE: DocumentType;
}

#[derive(Debug, Error)]
pub enum DocumentError {
    #[error("the document exceeds its {limit}-byte bound")]
    Size { limit: NonZeroUsize },
    #[error("the compact JWS is malformed")]
    Compact,
    #[error("the protected header must contain exactly ES256, the expected type and a key id")]
    Header,
    #[error("the signing key identity does not match the protected header")]
    KeyIdentity,
    #[error("the document signature or payload is invalid")]
    Verification,
    #[error("failed to encode the document")]
    Encoding,
    #[error("failed to prepare the signing key")]
    SigningKey,
    #[error("a signature set is empty or exceeds its {limit}-signature bound")]
    SignatureCount { limit: NonZeroUsize },
    #[error("signature-set members have different payload segments")]
    PayloadMismatch,
    #[error("signature-set members repeat a signing key")]
    RepeatedSigner,
}

/// jose-b64 exposes typed refusals without implementing the standard Error trait. Project that
/// exact closed outcome once, retaining its JSON error as the report's original source.
#[derive(Debug, Error)]
enum CompactDecodeError {
    #[error("invalid compact JWS segment length")]
    Length,
    #[error("invalid compact JWS segment encoding")]
    Value,
}

#[derive(Debug, Deserialize)]
enum Algorithm {
    ES256,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtectedHeader {
    alg: Algorithm,
    typ: DocumentType,
    kid: KeyId,
}

/// Parsed framing only. No JSON payload is interpreted before signature verification.
pub(crate) struct UnverifiedDocument {
    compact: String,
    header: ProtectedHeader,
    payload: Vec<u8>,
}

impl UnverifiedDocument {
    pub(crate) fn parse(
        compact: &str,
        expected: DocumentType,
        limits: DocumentLimits,
    ) -> Result<Self, Report<DocumentError>> {
        let parsed = Self::parse_kind(compact, limits)?;
        if parsed.header.typ != expected {
            return Err(Report::new(DocumentError::Header));
        }
        Ok(parsed)
    }

    fn parse_kind(compact: &str, limits: DocumentLimits) -> Result<Self, Report<DocumentError>> {
        if compact.len() > limits.bytes.get() {
            return Err(Report::new(DocumentError::Size {
                limit: limits.bytes,
            }));
        }
        let decoded =
            compact
                .parse::<jose_jws::Flattened>()
                .map_err(|error| match error {
                    jose_b64::stream::Error::Inner(error) => {
                        Report::new(error).change_context(DocumentError::Compact)
                    }
                    jose_b64::stream::Error::Length => Report::new(CompactDecodeError::Length)
                        .change_context(DocumentError::Compact),
                    jose_b64::stream::Error::Value => Report::new(CompactDecodeError::Value)
                        .change_context(DocumentError::Compact),
                })?;
        let Some(protected) = decoded.signature.protected else {
            return Err(Report::new(DocumentError::Header));
        };
        let header: ProtectedHeader =
            serde_json::from_slice(protected.as_ref()).change_context(DocumentError::Header)?;
        let Algorithm::ES256 = header.alg;
        let Some(payload) = decoded.payload else {
            return Err(Report::new(DocumentError::Compact));
        };
        Ok(Self {
            compact: compact.to_owned(),
            header,
            payload: payload.as_ref().to_vec(),
        })
    }

    pub(crate) fn key_id(&self) -> &KeyId {
        &self.header.kid
    }

    pub(crate) fn same_payload(&self, other: &Self) -> bool {
        // jose-b64 requires canonical unpadded base64url, so equal decoded bytes have exactly
        // equal payload segments. It retains the protected JSON separately, without rewriting it.
        self.payload == other.payload
    }

    pub(crate) fn verify<T: Document>(&self, key: &PublicKey) -> Result<T, Report<DocumentError>> {
        if key.key_id().change_context(DocumentError::KeyIdentity)? != self.header.kid {
            return Err(Report::new(DocumentError::KeyIdentity));
        }
        jws::verify(&self.compact, &key.jwk()).change_context(DocumentError::Verification)
    }
}

/// The exact compact strings supplied by the signers, with a byte-identical payload segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignatureSet {
    pub signatures: Vec<String>,
}

impl SignatureSet {
    pub(crate) fn check_bounds(&self, limits: DocumentLimits) -> Result<(), Report<DocumentError>> {
        if self.signatures.is_empty() || self.signatures.len() > limits.signatures.get() {
            return Err(Report::new(DocumentError::SignatureCount {
                limit: limits.signatures,
            }));
        }
        for compact in &self.signatures {
            if compact.len() > limits.bytes.get() {
                return Err(Report::new(DocumentError::Size {
                    limit: limits.bytes,
                }));
            }
        }
        Ok(())
    }

    pub(crate) fn document_type(
        &self,
        limits: DocumentLimits,
    ) -> Result<DocumentType, Report<DocumentError>> {
        let Some(first) = self.signatures.first() else {
            return Err(Report::new(DocumentError::SignatureCount {
                limit: limits.signatures,
            }));
        };
        let parsed = UnverifiedDocument::parse_kind(first, limits)?;
        Ok(parsed.header.typ)
    }

    pub(crate) fn parse(
        &self,
        expected: DocumentType,
        limits: DocumentLimits,
    ) -> Result<Vec<UnverifiedDocument>, Report<DocumentError>> {
        self.check_bounds(limits)?;
        let mut documents: Vec<UnverifiedDocument> = Vec::with_capacity(self.signatures.len());
        let mut signers = std::collections::BTreeSet::new();
        for compact in &self.signatures {
            let parsed = UnverifiedDocument::parse(compact, expected, limits)?;
            if !signers.insert(parsed.key_id().clone()) {
                return Err(Report::new(DocumentError::RepeatedSigner));
            }
            if let Some(first) = documents.first()
                && !first.same_payload(&parsed)
            {
                return Err(Report::new(DocumentError::PayloadMismatch));
            }
            documents.push(parsed);
        }
        Ok(documents)
    }
}

/// A signer whose public identity is always derived from its actual private key.
#[derive(Debug)]
pub struct DocumentSigner {
    key: jws::SigningKey,
    public: PublicKey,
    kid: KeyId,
}

impl DocumentSigner {
    pub fn from_pkcs8(bytes: &[u8]) -> Result<Self, Report<DocumentError>> {
        let key = jws::SigningKey::from_pkcs8(bytes).change_context(DocumentError::SigningKey)?;
        let public = PublicKey::from_coordinates(key.public_coordinates());
        let kid = public.key_id().change_context(DocumentError::SigningKey)?;
        Ok(Self { key, public, kid })
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.public
    }

    pub fn key_id(&self) -> &KeyId {
        &self.kid
    }

    pub fn sign<T: Document>(
        &self,
        document: &T,
        limits: DocumentLimits,
    ) -> Result<String, Report<DocumentError>> {
        // The first serialization bounds every payload allocation made by the maintained signer.
        // Re-reading this trusted JSON before signing is not verification of a received document.
        let encoded = encode_json(document, limits.bytes)?;
        let value: serde_json::Value =
            serde_json::from_slice(&encoded).change_context(DocumentError::Encoding)?;
        let compact = self
            .key
            .sign(T::TYPE.as_str(), self.kid.as_ref(), &value)
            .change_context(DocumentError::Encoding)?;
        if compact.len() > limits.bytes.get() {
            return Err(Report::new(DocumentError::Size {
                limit: limits.bytes,
            }));
        }
        Ok(compact)
    }
}

pub fn verify<T: Document>(
    compact: &str,
    key: &PublicKey,
    limits: DocumentLimits,
) -> Result<T, Report<DocumentError>> {
    UnverifiedDocument::parse(compact, T::TYPE, limits)?.verify(key)
}

pub(crate) fn encode_json<T: Serialize>(
    value: &T,
    limit: NonZeroUsize,
) -> Result<Vec<u8>, Report<DocumentError>> {
    let written = BoundedWriter::write_with(limit, |writer| serde_json::to_writer(writer, value))
        .change_context(DocumentError::Encoding)?;
    match written {
        BoundedWrite::Complete(bytes) => Ok(bytes),
        BoundedWrite::LimitReached => Err(Report::new(DocumentError::Size { limit })),
    }
}
