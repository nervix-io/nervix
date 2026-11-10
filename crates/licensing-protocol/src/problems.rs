//! The closed protocol refusal catalog and RFC 9457 projection.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Typed service refusals and the shared catalog's RFC 9457 projection.
//! - **Depends on.** Maintained serde/URL encodings and typed request identities.
//! - **Must not know.** HTTP servers, storage, retry scheduling, Models or clocks.

pub use nervix_licensing_vocabulary::ProblemCode;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProblemKind {
    InvalidRequest,
    SignatureInvalid,
    ChallengeInvalid,
    LicenseKeyInvalid,
    LicenseKeyRevoked,
    LicenseNotActive,
    KindNotAllowed,
    QuotaExhausted,
    ProtocolUnsupported,
    ActivationUnknown,
    BindingMismatch,
    GenerationMismatch,
    RequestConflict,
    ProofInvalid,
    AlreadyActivated { activation_id: Uuid },
    RateLimited { retry_after_seconds: u64 },
    Unavailable { retry_after_seconds: u64 },
    Internal { retry_after_seconds: Option<u64> },
}

impl ProblemKind {
    pub const fn code(&self) -> ProblemCode {
        match self {
            Self::InvalidRequest => ProblemCode::InvalidRequest,
            Self::SignatureInvalid => ProblemCode::SignatureInvalid,
            Self::ChallengeInvalid => ProblemCode::ChallengeInvalid,
            Self::LicenseKeyInvalid => ProblemCode::LicenseKeyInvalid,
            Self::LicenseKeyRevoked => ProblemCode::LicenseKeyRevoked,
            Self::LicenseNotActive => ProblemCode::LicenseNotActive,
            Self::KindNotAllowed => ProblemCode::KindNotAllowed,
            Self::QuotaExhausted => ProblemCode::QuotaExhausted,
            Self::ProtocolUnsupported => ProblemCode::ProtocolUnsupported,
            Self::ActivationUnknown => ProblemCode::ActivationUnknown,
            Self::BindingMismatch => ProblemCode::BindingMismatch,
            Self::GenerationMismatch => ProblemCode::GenerationMismatch,
            Self::RequestConflict => ProblemCode::RequestConflict,
            Self::ProofInvalid => ProblemCode::ProofInvalid,
            Self::AlreadyActivated { .. } => ProblemCode::AlreadyActivated,
            Self::RateLimited { .. } => ProblemCode::RateLimited,
            Self::Unavailable { .. } => ProblemCode::Unavailable,
            Self::Internal { .. } => ProblemCode::Internal,
        }
    }

    const fn retry_after(&self) -> Option<u64> {
        match self {
            Self::RateLimited {
                retry_after_seconds,
            }
            | Self::Unavailable {
                retry_after_seconds,
            } => Some(*retry_after_seconds),
            Self::Internal {
                retry_after_seconds,
            } => *retry_after_seconds,
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProblemDetails {
    pub kind: ProblemKind,
    pub title: String,
    pub detail: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProblemWire {
    r#type: Url,
    title: String,
    status: u16,
    code: ProblemCode,
    detail: String,
    retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    activation_id: Option<Uuid>,
}

#[derive(Debug, Error)]
#[error("problem details contradict the protocol's refusal code")]
struct ProblemShapeError;

fn problem_type(code: ProblemCode) -> Url {
    use meticulous::ResultExt as _;
    let base = Url::parse("https://licensing.nervix.io/problems/")
        .assured("the compiled service problem namespace is an absolute HTTPS URL");
    base.join(code.as_str())
        .assured("catalog codes are nonempty relative path segments")
}

impl Serialize for ProblemDetails {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let code = self.kind.code();
        let activation_id = match self.kind {
            ProblemKind::AlreadyActivated { activation_id } => Some(activation_id),
            _ => None,
        };
        ProblemWire {
            r#type: problem_type(code),
            title: self.title.clone(),
            status: code.status(),
            code,
            detail: self.detail.clone(),
            retryable: code.retryable(),
            retry_after_seconds: self.kind.retry_after(),
            request_id: self.request_id,
            activation_id,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProblemDetails {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = ProblemWire::deserialize(deserializer)?;
        if wire.r#type != problem_type(wire.code)
            || wire.status != wire.code.status()
            || wire.retryable != wire.code.retryable()
        {
            return Err(D::Error::custom(ProblemShapeError));
        }
        let kind = match wire.code {
            ProblemCode::AlreadyActivated => ProblemKind::AlreadyActivated {
                activation_id: wire
                    .activation_id
                    .ok_or_else(|| D::Error::custom(ProblemShapeError))?,
            },
            ProblemCode::RateLimited => ProblemKind::RateLimited {
                retry_after_seconds: wire
                    .retry_after_seconds
                    .ok_or_else(|| D::Error::custom(ProblemShapeError))?,
            },
            ProblemCode::Unavailable => ProblemKind::Unavailable {
                retry_after_seconds: wire
                    .retry_after_seconds
                    .ok_or_else(|| D::Error::custom(ProblemShapeError))?,
            },
            ProblemCode::Internal => ProblemKind::Internal {
                retry_after_seconds: wire.retry_after_seconds,
            },
            ProblemCode::InvalidRequest => ProblemKind::InvalidRequest,
            ProblemCode::SignatureInvalid => ProblemKind::SignatureInvalid,
            ProblemCode::ChallengeInvalid => ProblemKind::ChallengeInvalid,
            ProblemCode::LicenseKeyInvalid => ProblemKind::LicenseKeyInvalid,
            ProblemCode::LicenseKeyRevoked => ProblemKind::LicenseKeyRevoked,
            ProblemCode::LicenseNotActive => ProblemKind::LicenseNotActive,
            ProblemCode::KindNotAllowed => ProblemKind::KindNotAllowed,
            ProblemCode::QuotaExhausted => ProblemKind::QuotaExhausted,
            ProblemCode::ProtocolUnsupported => ProblemKind::ProtocolUnsupported,
            ProblemCode::ActivationUnknown => ProblemKind::ActivationUnknown,
            ProblemCode::BindingMismatch => ProblemKind::BindingMismatch,
            ProblemCode::GenerationMismatch => ProblemKind::GenerationMismatch,
            ProblemCode::RequestConflict => ProblemKind::RequestConflict,
            ProblemCode::ProofInvalid => ProblemKind::ProofInvalid,
        };
        if (wire.code != ProblemCode::AlreadyActivated && wire.activation_id.is_some())
            || (!wire.code.retryable() && wire.retry_after_seconds.is_some())
        {
            return Err(D::Error::custom(ProblemShapeError));
        }
        Ok(Self {
            kind,
            title: wire.title,
            detail: wire.detail,
            request_id: wire.request_id,
        })
    }
}
