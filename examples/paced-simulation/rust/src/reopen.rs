//! Accepting a changed endpoint contract in the example application.
//!
//! Layer: edges.
//! - **Owns.** Which reopen reasons wait for another START generation, and the reason vocabulary
//!   both drivers print for producers and consumers.
//! - **Depends on.** The Rust client's reopen reasons and the example's refusal vocabulary.
//! - **Must not know.** How endpoints open, how batches finish, or how the clock advances.

use nervix_client_core::{ConsumerReopenReason, ProducerReopenReason};

use crate::refusal::Refusal;

/// Why the application must explicitly open a fresh endpoint handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reopen {
    DomainStopped,
    EndpointRemoved,
    SchemaChanged,
    ContractChanged,
    GenerationChanged,
    ProtocolViolated,
    Refused(Refusal),
}

impl Reopen {
    /// Only a domain lifecycle ending needs another START before opening again.
    pub(crate) const fn waits_for_generation(self) -> bool {
        matches!(self, Self::DomainStopped | Self::GenerationChanged)
    }

    /// The reason in the shared binding's vocabulary.
    pub(crate) fn text(self) -> String {
        match self {
            Self::DomainStopped => "domain_stopped".to_string(),
            Self::EndpointRemoved => "endpoint_removed".to_string(),
            Self::SchemaChanged => "schema_changed".to_string(),
            Self::ContractChanged => "contract_changed".to_string(),
            Self::GenerationChanged => "generation_changed".to_string(),
            Self::ProtocolViolated => "protocol_violated".to_string(),
            Self::Refused(refusal) => format!("refused ({})", refusal.as_str()),
        }
    }
}

impl From<&ConsumerReopenReason> for Reopen {
    fn from(reason: &ConsumerReopenReason) -> Self {
        match reason {
            ConsumerReopenReason::DomainStopped => Self::DomainStopped,
            ConsumerReopenReason::EndpointRemoved => Self::EndpointRemoved,
            ConsumerReopenReason::SchemaChanged => Self::SchemaChanged,
            ConsumerReopenReason::ContractChanged => Self::ContractChanged,
            ConsumerReopenReason::GenerationChanged => Self::GenerationChanged,
            ConsumerReopenReason::ProtocolViolated => Self::ProtocolViolated,
            ConsumerReopenReason::Refused(refusal) => Self::Refused(Refusal::from(*refusal)),
        }
    }
}

impl From<&ProducerReopenReason> for Reopen {
    fn from(reason: &ProducerReopenReason) -> Self {
        match reason {
            ProducerReopenReason::DomainStopped => Self::DomainStopped,
            ProducerReopenReason::EndpointRemoved => Self::EndpointRemoved,
            ProducerReopenReason::SchemaChanged => Self::SchemaChanged,
            ProducerReopenReason::ContractChanged => Self::ContractChanged,
            ProducerReopenReason::GenerationChanged => Self::GenerationChanged,
            ProducerReopenReason::ProtocolViolated => Self::ProtocolViolated,
            ProducerReopenReason::Refused(refusal) => Self::Refused(Refusal::from(*refusal)),
        }
    }
}
