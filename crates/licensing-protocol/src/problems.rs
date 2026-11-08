//! The closed protocol refusal catalog and RFC 9457 projection.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Stable refusal codes and their status, retryability and problem-detail projection.
//! - **Depends on.** Maintained serde/URL encodings and typed request identities.
//! - **Must not know.** HTTP servers, storage, retry scheduling, Models or clocks.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

macro_rules! problem_codes {
    ($($variant:ident => ($code:literal, $status:literal, $title:literal, $retry:literal)),* $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum ProblemCode { $(#[serde(rename = $code)] $variant,)* }
        impl ProblemCode {
            pub const fn as_str(self) -> &'static str { match self { $(Self::$variant => $code,)* } }
            pub const fn status(self) -> u16 { match self { $(Self::$variant => $status,)* } }
            pub const fn title(self) -> &'static str { match self { $(Self::$variant => $title,)* } }
            pub const fn retryable(self) -> bool { match self { $(Self::$variant => $retry,)* } }
        }
    };
}

problem_codes! {
    InvalidRequest => ("invalid_request", 400, "Invalid request", false),
    SignatureInvalid => ("signature_invalid", 401, "Invalid signature", false),
    ChallengeInvalid => ("challenge_invalid", 401, "Invalid challenge", false),
    LicenseKeyInvalid => ("license_key_invalid", 403, "Invalid license key", false),
    LicenseKeyRevoked => ("license_key_revoked", 403, "Revoked license key", false),
    LicenseNotActive => ("license_not_active", 403, "License is not active", false),
    KindNotAllowed => ("kind_not_allowed", 403, "Activation kind is not allowed", false),
    QuotaExhausted => ("quota_exhausted", 409, "Activation quota is exhausted", false),
    AlreadyActivated => ("already_activated", 409, "Cluster is already activated", false),
    ProtocolUnsupported => ("protocol_unsupported", 400, "Unsupported protocol", false),
    RateLimited => ("rate_limited", 429, "Rate limited", true),
    Unavailable => ("unavailable", 503, "Service unavailable", true),
    ActivationUnknown => ("activation_unknown", 404, "Unknown activation", false),
    BindingMismatch => ("binding_mismatch", 409, "Activation binding mismatch", false),
    GenerationMismatch => ("generation_mismatch", 409, "Cluster key generation mismatch", false),
    RequestConflict => ("request_conflict", 409, "Request identity conflict", false),
    ProofInvalid => ("proof_invalid", 401, "Invalid key possession proof", false),
    Internal => ("internal", 500, "Internal service error", true),
}

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

impl ProblemCode {
    fn problem_type(self) -> Url {
        use meticulous::ResultExt as _;
        let base = Url::parse("https://licensing.nervix.io/problems/")
            .assured("the compiled service problem namespace is an absolute HTTPS URL");
        base.join(self.as_str())
            .assured("catalog codes are nonempty relative path segments")
    }
}

impl Serialize for ProblemDetails {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let code = self.kind.code();
        let activation_id = match self.kind {
            ProblemKind::AlreadyActivated { activation_id } => Some(activation_id),
            _ => None,
        };
        ProblemWire {
            r#type: code.problem_type(),
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
        if wire.r#type != wire.code.problem_type()
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
