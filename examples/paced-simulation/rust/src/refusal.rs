//! Why a client endpoint refused to open, in the words both drivers print.
//!
//! - **Owns.** The refusal vocabulary of the example's opens, and its text.
//! - **Depends on.** The client's producer and consumer refusals.
//! - **Must not know.** Sessions or how an open is retried.
//!
//! The shared C binding reports producer and consumer refusals as one set; the Rust client keeps
//! them apart. The driver speaks the binding's set, so the Rust and Python drivers print the same
//! refusal for the same open.

use nervix_client_core::{ClientProducerRefusal, wire::EmitterOpenRefusal};

/// Why an endpoint refused to open, in the shared binding's vocabulary, so both drivers print the
/// same refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    DomainNotFound,
    DomainStopped,
    EndpointNotFound,
    NotClientEndpoint,
    EndpointUnavailable,
    SchemaMismatch,
    TooManyEndpoints,
    SessionCapacityExhausted,
    NodeCapacityExhausted,
    InvalidLimits,
    InTransaction,
}

impl Refusal {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DomainNotFound => "domain not found",
            Self::DomainStopped => "domain stopped",
            Self::EndpointNotFound => "endpoint not found",
            Self::NotClientEndpoint => "not a client endpoint",
            Self::EndpointUnavailable => "endpoint unavailable",
            Self::SchemaMismatch => "schema mismatch",
            Self::TooManyEndpoints => "too many endpoints",
            Self::SessionCapacityExhausted => "session capacity exhausted",
            Self::NodeCapacityExhausted => "node capacity exhausted",
            Self::InvalidLimits => "invalid limits",
            Self::InTransaction => "in transaction",
        }
    }
}

impl From<EmitterOpenRefusal> for Refusal {
    fn from(refusal: EmitterOpenRefusal) -> Self {
        match refusal {
            EmitterOpenRefusal::DomainNotFound => Self::DomainNotFound,
            EmitterOpenRefusal::DomainStopped => Self::DomainStopped,
            EmitterOpenRefusal::EmitterNotFound => Self::EndpointNotFound,
            EmitterOpenRefusal::NotClientEmitter => Self::NotClientEndpoint,
            EmitterOpenRefusal::EndpointUnavailable => Self::EndpointUnavailable,
            EmitterOpenRefusal::SchemaMismatch => Self::SchemaMismatch,
            EmitterOpenRefusal::TooManyConsumers => Self::TooManyEndpoints,
            EmitterOpenRefusal::SessionCapacityExhausted => Self::SessionCapacityExhausted,
            EmitterOpenRefusal::NodeCapacityExhausted => Self::NodeCapacityExhausted,
            EmitterOpenRefusal::InvalidLimits => Self::InvalidLimits,
            EmitterOpenRefusal::InTransaction => Self::InTransaction,
        }
    }
}

impl From<ClientProducerRefusal> for Refusal {
    fn from(refusal: ClientProducerRefusal) -> Self {
        match refusal {
            ClientProducerRefusal::DomainNotFound => Self::DomainNotFound,
            ClientProducerRefusal::DomainStopped => Self::DomainStopped,
            ClientProducerRefusal::IngestorNotFound => Self::EndpointNotFound,
            ClientProducerRefusal::NotClientIngestor => Self::NotClientEndpoint,
            ClientProducerRefusal::EndpointUnavailable => Self::EndpointUnavailable,
            ClientProducerRefusal::SchemaMismatch => Self::SchemaMismatch,
            ClientProducerRefusal::TooManyProducers => Self::TooManyEndpoints,
            ClientProducerRefusal::SessionCapacityExhausted => Self::SessionCapacityExhausted,
            ClientProducerRefusal::NodeCapacityExhausted => Self::NodeCapacityExhausted,
            ClientProducerRefusal::InvalidLimits => Self::InvalidLimits,
            ClientProducerRefusal::InTransaction => Self::InTransaction,
        }
    }
}
