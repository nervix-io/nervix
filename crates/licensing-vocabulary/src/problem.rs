//! Shared machine-readable licensing service refusal codes.
//!
//! Layer: vocabulary.
//! - **Owns.** The closed refusal catalog, exact public spellings, status and retryability.
//! - **Depends on.** Serialization and primitive values.
//! - **Must not know.** HTTP clients, problem-detail bodies, cryptography, Models or clocks.

use serde::{Deserialize, Serialize};

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
