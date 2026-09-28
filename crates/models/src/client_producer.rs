//! What an application producer attached to a client ingestor is told, and what it may ask for.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The limits a producer requests and is granted, the identity of the endpoint contract
//!   it attached under, the typed refusal of an open, the four outcomes of a submitted batch with
//!   their typed causes, the admission state of a producer, and why a producer's attachment ends.
//! - **Depends on.** Serialization and strum.
//! - **Must not know.** How a session or the interconnect carries these values, how a batch is
//!   decoded or admitted, or which node serves a producer.
//!
//! One submitted batch is one source acknowledgement unit, so every outcome describes a whole
//! batch. Transport receipt is never an outcome: a batch is either not admitted, completed, failed
//! after admission, or of unknown outcome.

use std::{
    fmt,
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter, IntoStaticStr};
use uuid::Uuid;

use crate::{AckWindow, SchemaField};

/// The most producers one session may hold open at once.
pub const MAX_CLIENT_PRODUCERS_PER_SESSION: usize = 32;
/// The Arrow IPC bytes every producer of one session may have outstanding together.
pub const CLIENT_PRODUCER_SESSION_BYTES: u64 = 32 * 1024 * 1024;
/// The Arrow IPC bytes every producer served by, or forwarded to, one node may have outstanding
/// together.
pub const CLIENT_PRODUCER_NODE_BYTES: u64 = 128 * 1024 * 1024;
/// The most batches one producer may have outstanding at once.
pub const MAX_CLIENT_PRODUCER_BATCHES: u32 = 1024;
/// The most rows one submitted batch may carry.
pub const MAX_CLIENT_BATCH_ROWS: u32 = 65_536;

const _: () = assert!(
    CLIENT_PRODUCER_SESSION_BYTES <= CLIENT_PRODUCER_NODE_BYTES,
    "one session's producers must fit the budget of the node that serves them",
);
const _: () = assert!(
    MAX_CLIENT_PRODUCERS_PER_SESSION > 0 && MAX_CLIENT_PRODUCER_BATCHES > 0,
    "a session must be able to open a producer that can submit a batch",
);

/// What a producer asks for when it opens: how many submitted batches and how many bytes of
/// Arrow IPC payload it may have outstanding at once.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ClientProducerLimits {
    pub batches: NonZeroU32,
    pub bytes: NonZeroU64,
}

impl ClientProducerLimits {
    /// Whether these limits stay within what one producer may ask for.
    pub fn is_within_bounds(&self) -> bool {
        self.batches.get() <= MAX_CLIENT_PRODUCER_BATCHES
            && self.bytes.get() <= CLIENT_PRODUCER_SESSION_BYTES
    }
}

/// What an opened producer was granted. A submission holds one batch and its payload bytes of the
/// grant from the moment it is sent until its terminal outcome is sent back.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ClientProducerGrant {
    pub batches: NonZeroU32,
    pub bytes: NonZeroU64,
    /// The largest Arrow IPC payload one submission may carry: the smaller of the granted bytes
    /// and what one session frame carries after its envelope.
    pub max_batch_bytes: NonZeroU64,
    /// The most rows one submission may carry.
    pub max_batch_rows: NonZeroU32,
}

/// The identity of the endpoint contract a producer attached under: the ingestor as producers
/// see it, its input schema, and the branch declarations its routes construct. A change to any of
/// them is a new contract; a change of flush cadence is not.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct ClientEndpointContract([u8; 32]);

impl ClientEndpointContract {
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    pub const fn as_digest(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A contract reads as its hexadecimal digest.
impl fmt::Debug for ClientEndpointContract {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ClientEndpointContract({self})")
    }
}

impl fmt::Display for ClientEndpointContract {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// The identity the node executing a client ingestor gives one producer attachment. Reopening the
/// same ingestor creates another, so nothing addressed to an earlier attachment reaches its
/// replacement.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ClientAttachmentId(u128);

impl ClientAttachmentId {
    /// A fresh identity, ordered by the time it was created.
    pub fn new() -> Self {
        Self(Uuid::now_v7().as_u128())
    }

    pub const fn from_u128(value: u128) -> Self {
        Self(value)
    }

    pub const fn as_u128(&self) -> u128 {
        self.0
    }
}

impl Default for ClientAttachmentId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ClientAttachmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ClientAttachmentId({self})")
    }
}

impl fmt::Display for ClientAttachmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&Uuid::from_u128(self.0), formatter)
    }
}

/// The acknowledgement policy every producer of one client ingestor shares: the window of batches
/// that may await their acknowledgement, how long one waits without progress, and the physical
/// backoff a producer applies before sending again a batch refused while admission was suspended.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ClientProducerPolicy {
    pub window: AckWindow,
    pub ack_timeout: Duration,
    pub retry_backoff: Duration,
    pub retry_max_backoff: Duration,
}

/// Everything an opened producer is told: the attachment it holds, the exact input schema its
/// batches carry, the domain generation and endpoint contract it is bound to, the policy it
/// submits under, what it was granted, and whether admission is open right now.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientProducerDescription {
    pub attachment: ClientAttachmentId,
    pub fields: Vec<SchemaField>,
    /// The number of STARTs the domain had committed when the producer attached.
    pub generation: u64,
    pub contract: ClientEndpointContract,
    pub policy: ClientProducerPolicy,
    pub grant: ClientProducerGrant,
    pub admission: ClientProducerAdmission,
}

/// Whether a producer may send new batches.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    IntoStaticStr,
    EnumIter,
)]
pub enum ClientProducerAdmission {
    /// The ingestor admits batches.
    #[strum(serialize = "open")]
    Open,
    /// The ingestor is quiesced or shedding under memory pressure. Admission is stopped, a batch
    /// that arrives is refused as not admitted, and the producer keeps its unsent batches.
    #[strum(serialize = "suspended")]
    Suspended,
}

/// Why a producer could not be opened. Nothing is left attached after any of them.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    IntoStaticStr,
    EnumIter,
)]
pub enum ClientProducerRefusal {
    /// The domain does not exist.
    #[strum(serialize = "domain not found")]
    DomainNotFound,
    /// The domain exists but is not running.
    #[strum(serialize = "domain stopped")]
    DomainStopped,
    /// The domain has no ingestor of that name.
    #[strum(serialize = "ingestor not found")]
    IngestorNotFound,
    /// The ingestor reads an external transport rather than client batches.
    #[strum(serialize = "not a client ingestor")]
    NotClientIngestor,
    /// The ingestor's execution is not running on the node scheduled to own it, or that node could
    /// not be reached. Opening again later may succeed.
    #[strum(serialize = "endpoint unavailable")]
    EndpointUnavailable,
    /// The expected schema differs from the ingestor's input schema in a field, its position, its
    /// exact type, its optionality or its sensitivity.
    #[strum(serialize = "schema mismatch")]
    SchemaMismatch,
    /// The session already holds as many producers as it may.
    #[strum(serialize = "too many producers")]
    TooManyProducers,
    /// The session's producer byte budget cannot hold the requested window.
    #[strum(serialize = "session capacity exhausted")]
    SessionCapacityExhausted,
    /// The byte budget of the serving node, or of the node the producer is forwarded to, cannot
    /// hold the requested window.
    #[strum(serialize = "node capacity exhausted")]
    NodeCapacityExhausted,
    /// The requested limits are zero or larger than one producer may ask for.
    #[strum(serialize = "invalid limits")]
    InvalidLimits,
    /// The session holds a transaction. Producers belong to the session, not to the transaction.
    #[strum(serialize = "in transaction")]
    InTransaction,
}

/// The terminal outcome of one submitted batch.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum ClientSubmissionOutcome {
    /// No row of the batch entered the graph.
    NotAdmitted(ClientSubmissionRefusal),
    /// The batch's source acknowledgement root resolved successfully under the graph's policies.
    Completed,
    /// The batch's source acknowledgement root failed. Some routes or external effects may already
    /// have completed, and admitted work may still complete after a timeout.
    ProcessingFailed(ClientProcessingFailure),
    /// The batch may have been admitted and processed, but no terminal result can be established.
    OutcomeUnknown(ClientOutcomeUncertainty),
}

impl ClientSubmissionOutcome {
    /// The bounded label of the outcome class, as metrics count it.
    pub const fn class_label(&self) -> &'static str {
        match self {
            Self::NotAdmitted(_) => "not_admitted",
            Self::Completed => "completed",
            Self::ProcessingFailed(_) => "processing_failed",
            Self::OutcomeUnknown(_) => "outcome_unknown",
        }
    }

    /// The bounded label of the outcome's cause, or `none` for a completed batch.
    pub fn cause_label(&self) -> &'static str {
        match self {
            Self::NotAdmitted(refusal) => refusal.label(),
            Self::Completed => "none",
            Self::ProcessingFailed(failure) => failure.into(),
            Self::OutcomeUnknown(uncertainty) => uncertainty.into(),
        }
    }
}

/// Why a batch was not admitted.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum ClientSubmissionRefusal {
    /// The batch is not a canonical Arrow IPC batch of the producer's schema within its limits.
    /// Sending it again cannot succeed.
    InvalidBatch(ClientBatchDefect),
    /// Admission is suspended by a quiesce or memory pressure. The producer retries the same batch
    /// on its declared backoff once admission reopens.
    Suspended,
    /// The node executing the ingestor had no capacity to validate the batch right now. The
    /// producer retries the same batch on its declared backoff.
    Busy,
    /// The ingestor's execution is stopping or moving to another node. The producer's attachment
    /// ends; a new producer can be opened once the ingestor runs again.
    Draining,
    /// The producer was closed or its attachment ended before the batch was admitted.
    ProducerEnded,
    /// The batch exceeded the producer's granted credit, which also ends the producer.
    CreditExceeded,
}

impl ClientSubmissionRefusal {
    /// Whether the same batch may be sent again on the same producer and succeed. Only a suspended
    /// admission and a busy node pass; every other refusal is final for this attachment.
    pub const fn is_temporary(&self) -> bool {
        match self {
            Self::Suspended | Self::Busy => true,
            Self::InvalidBatch(_) | Self::Draining | Self::ProducerEnded | Self::CreditExceeded => {
                false
            }
        }
    }

    pub const fn label(&self) -> &'static str {
        match self {
            Self::InvalidBatch(_) => "invalid_batch",
            Self::Suspended => "suspended",
            Self::Busy => "busy",
            Self::Draining => "draining",
            Self::ProducerEnded => "producer_ended",
            Self::CreditExceeded => "credit_exceeded",
        }
    }
}

/// What makes a submitted batch invalid. None of these carries a payload value.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    IntoStaticStr,
    EnumIter,
)]
pub enum ClientBatchDefect {
    /// The payload is not an Arrow IPC stream.
    #[strum(serialize = "malformed")]
    Malformed,
    /// The stream carries a dictionary batch, a tensor or another message besides its schema and
    /// one record batch.
    #[strum(serialize = "unexpected message")]
    UnexpectedMessage,
    /// The record batch is compressed.
    #[strum(serialize = "compressed")]
    Compressed,
    /// The stream declares another schema than the producer's, including field metadata.
    #[strum(serialize = "schema mismatch")]
    SchemaMismatch,
    /// The stream carries no record batch, or more than one.
    #[strum(serialize = "not one batch")]
    NotOneBatch,
    /// The batch carries more rows than one submission may.
    #[strum(serialize = "too many rows")]
    TooManyRows,
    /// The payload, or the columns it decodes to, is larger than one submission may carry.
    #[strum(serialize = "too large")]
    TooLarge,
    /// The columns do not satisfy their declared types, nullability or buffer bounds.
    #[strum(serialize = "invalid data")]
    InvalidData,
}

/// Why an admitted batch's acknowledgement failed.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    IntoStaticStr,
    EnumIter,
)]
pub enum ClientProcessingFailure {
    /// The acknowledgement made no progress within the declared ACK TIMEOUT. The admitted work was
    /// not cancelled and may still complete.
    #[strum(serialize = "ack_timeout")]
    AckTimedOut,
    /// A route, a policy or a downstream node negatively acknowledged the batch.
    #[strum(serialize = "rejected")]
    Rejected,
}

/// Why the server could establish no terminal result for a submitted batch. A client whose own
/// session ends before an outcome arrives knows that without the server telling it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    IntoStaticStr,
    EnumIter,
)]
pub enum ClientOutcomeUncertainty {
    /// The ingestor's execution stopped, or the producer's attachment ended, while the batch's
    /// acknowledgement was unresolved.
    #[strum(serialize = "interrupted")]
    Interrupted,
    /// The node that owns the ingestor's execution, or the connection to it, was lost.
    #[strum(serialize = "owner_lost")]
    OwnerLost,
}

/// Why the server ended a producer's attachment. It is the last event about that producer.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    IntoStaticStr,
    EnumIter,
)]
pub enum ClientProducerEndReason {
    /// An alteration changed the endpoint contract. Opening again validates the new contract.
    #[strum(serialize = "endpoint changed")]
    EndpointChanged,
    /// The ingestor or its domain was removed.
    #[strum(serialize = "endpoint removed")]
    EndpointRemoved,
    /// The domain stopped, or a new START replaced the generation the producer attached under.
    #[strum(serialize = "domain stopped")]
    DomainStopped,
    /// A planned ownership handoff moved the ingestor's execution to another node.
    #[strum(serialize = "relocated")]
    Relocated,
    /// The serving node, or the node that owns the ingestor's execution, is shutting down.
    #[strum(serialize = "shutting down")]
    ShuttingDown,
    /// The node that owns the ingestor's execution, or the connection to it, was lost.
    #[strum(serialize = "owner lost")]
    OwnerLost,
    /// The producer sent a batch beyond its granted credit.
    #[strum(serialize = "protocol violated")]
    ProtocolViolated,
}
