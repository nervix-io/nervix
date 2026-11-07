//! What producers and consumers have in common as a host reads them: the state of an endpoint,
//! why an open is refused or an endpoint has to be opened again, and an endpoint's
//! acknowledgement window.
//!
//! - **Owns.** The endpoint states, open refusals, reopen reasons and acknowledgement windows a
//!   host reads, how the Rust client's producer and consumer vocabulary reads as them, the credit
//!   a host asks for, and the nanoseconds of an endpoint's durations.
//! - **Depends on.** The Rust client's producer and consumer vocabulary.
//! - **Must not know.** How an endpoint is opened, restored or closed; the producer and consumer
//!   decide that.

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use meticulous::ResultExt as _;
use nervix_client_core::{
    AckWindow, ClientProducerRefusal, ConsumerConnection, ConsumerReopenReason, ProducerConnection,
    ProducerReopenReason, wire::EmitterOpenRefusal,
};

use crate::failure::Failure;

/// Whether an endpoint handle is attached, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum EndpointState {
    Active = 1,
    Interrupted = 2,
    Restoring = 3,
    ReopenRequired = 4,
    Closed = 5,
}

impl From<ProducerConnection> for EndpointState {
    fn from(connection: ProducerConnection) -> Self {
        match connection {
            ProducerConnection::Active => Self::Active,
            ProducerConnection::Interrupted => Self::Interrupted,
            ProducerConnection::Restoring => Self::Restoring,
            ProducerConnection::ReopenRequired => Self::ReopenRequired,
            ProducerConnection::Closed => Self::Closed,
        }
    }
}

impl From<ConsumerConnection> for EndpointState {
    fn from(connection: ConsumerConnection) -> Self {
        match connection {
            ConsumerConnection::Active => Self::Active,
            ConsumerConnection::Interrupted => Self::Interrupted,
            ConsumerConnection::Restoring => Self::Restoring,
            ConsumerConnection::ReopenRequired => Self::ReopenRequired,
            ConsumerConnection::Closed => Self::Closed,
        }
    }
}

/// Why the server refused to open a producer or a consumer, with its stable values. A producer
/// and a consumer are refused for the same reasons, each about its own kind of endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum OpenRefusal {
    DomainNotFound = 1,
    DomainStopped = 2,
    EndpointNotFound = 3,
    NotClientEndpoint = 4,
    EndpointUnavailable = 5,
    SchemaMismatch = 6,
    TooManyEndpoints = 7,
    SessionCapacityExhausted = 8,
    NodeCapacityExhausted = 9,
    InvalidLimits = 10,
    InTransaction = 11,
}

impl From<ClientProducerRefusal> for OpenRefusal {
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

impl From<EmitterOpenRefusal> for OpenRefusal {
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

/// Why a handle has to be opened again, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ReopenReason {
    DomainStopped = 1,
    EndpointRemoved = 2,
    SchemaChanged = 3,
    ContractChanged = 4,
    GenerationChanged = 5,
    ProtocolViolated = 6,
    Refused = 7,
}

/// Why a handle has to be opened again, with the refusal of a restoration the server refused for
/// good.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reopen {
    Changed(ReopenReason),
    Refused(OpenRefusal),
}

impl Reopen {
    pub fn reason(self) -> ReopenReason {
        match self {
            Self::Changed(reason) => reason,
            Self::Refused(_) => ReopenReason::Refused,
        }
    }

    pub fn refusal(self) -> Option<OpenRefusal> {
        match self {
            Self::Changed(_) => None,
            Self::Refused(refusal) => Some(refusal),
        }
    }
}

impl From<&ProducerReopenReason> for Reopen {
    fn from(reason: &ProducerReopenReason) -> Self {
        match reason {
            ProducerReopenReason::DomainStopped => Self::Changed(ReopenReason::DomainStopped),
            ProducerReopenReason::EndpointRemoved => Self::Changed(ReopenReason::EndpointRemoved),
            ProducerReopenReason::SchemaChanged => Self::Changed(ReopenReason::SchemaChanged),
            ProducerReopenReason::ContractChanged => Self::Changed(ReopenReason::ContractChanged),
            ProducerReopenReason::GenerationChanged => {
                Self::Changed(ReopenReason::GenerationChanged)
            }
            ProducerReopenReason::ProtocolViolated => Self::Changed(ReopenReason::ProtocolViolated),
            ProducerReopenReason::Refused(refusal) => Self::Refused(OpenRefusal::from(*refusal)),
        }
    }
}

impl From<&ConsumerReopenReason> for Reopen {
    fn from(reason: &ConsumerReopenReason) -> Self {
        match reason {
            ConsumerReopenReason::DomainStopped => Self::Changed(ReopenReason::DomainStopped),
            ConsumerReopenReason::EndpointRemoved => Self::Changed(ReopenReason::EndpointRemoved),
            ConsumerReopenReason::SchemaChanged => Self::Changed(ReopenReason::SchemaChanged),
            ConsumerReopenReason::ContractChanged => Self::Changed(ReopenReason::ContractChanged),
            ConsumerReopenReason::GenerationChanged => {
                Self::Changed(ReopenReason::GenerationChanged)
            }
            ConsumerReopenReason::ProtocolViolated => Self::Changed(ReopenReason::ProtocolViolated),
            ConsumerReopenReason::Refused(refusal) => Self::Refused(OpenRefusal::from(*refusal)),
        }
    }
}

/// How an endpoint's acknowledgements may be outstanding, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum WindowKind {
    Sequential = 1,
    Parallel = 2,
}

/// An endpoint's acknowledgement window: its kind, and how many acknowledgements it lets be
/// outstanding at once, which is one for a sequential window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub kind: WindowKind,
    pub outstanding: u64,
}

impl From<AckWindow> for Window {
    fn from(window: AckWindow) -> Self {
        match window {
            AckWindow::Sequential => Self {
                kind: WindowKind::Sequential,
                outstanding: 1,
            },
            AckWindow::Parallel { max } => Self {
                kind: WindowKind::Parallel,
                outstanding: max.get(),
            },
        }
    }
}

/// The credit one endpoint asks for: how many batches and bytes may be outstanding at once.
pub fn credit(batches: u32, bytes: u64) -> Result<(NonZeroU32, NonZeroU64), Failure> {
    let Some(batches) = NonZeroU32::new(batches) else {
        return Err(Failure::invalid_argument("batches", "must be positive"));
    };
    let Some(bytes) = NonZeroU64::new(bytes) else {
        return Err(Failure::invalid_argument("bytes", "must be positive"));
    };
    Ok((batches, bytes))
}

/// A duration the wire carried, in nanoseconds.
pub fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos())
        .verified("the wire carries every endpoint duration as u64 nanoseconds")
}
