//! What an ingestor reads, and how what it reads becomes records of its input schema.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The two input contracts an ingestor declares — an external transport together with
//!   the codec that decodes its payloads, or the typed batches application clients submit through
//!   their sessions — and the capabilities each contract carries: header reads, execution on every
//!   node, the source it references, its quiesce modes and its acknowledgement.
//! - **Depends on.** The source Models and the schema and codec names they reference.
//! - **Must not know.** How a source is opened, how a batch is decoded or admitted, or which node
//!   executes an ingestor.
//!
//! The codec belongs to the transport contract, so a client ingestor cannot name one and a
//! transport ingestor cannot lack one: neither contradiction is representable.

use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use crate::{
    AckWindow, CodecName, IngestAcknowledgement, IngestQuiesceMode, IngestSource, IngestSourceKind,
    ModelKind, ModelName, RetryPolicy, SchemaName,
};

/// The only quiesce mode a client source honors: admission stops and the producer keeps its
/// unsent batches.
const CLIENT_QUIESCE: IngestQuiesceMode = IngestQuiesceMode::Suspend;

/// What an ingestor reads, and how what it reads becomes records of its input schema.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum IngestorInput {
    /// An external transport whose payloads the codec decodes.
    Transport(TransportIngestorInput),
    /// Typed batches application clients submit through their sessions.
    Client(ClientIngestSource),
}

/// An external transport and the codec that decodes the payloads it delivers.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransportIngestorInput {
    pub source: IngestSource,
    pub codec: CodecName,
}

/// Application clients submitting native Arrow batches of one internal schema.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientIngestSource {
    /// The internal schema every submitted batch matches exactly, before route construction.
    pub schema: SchemaName,
    pub mode: ClientIngestMode,
}

/// How many submitted batches may await their acknowledgement across every producer of one
/// client ingestor, how long one waits without progress, and how a producer retries a batch that
/// was refused before admission.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientIngestMode {
    pub window: AckWindow,
    pub ack_timeout: String,
    pub retry_policy: RetryPolicy,
}

/// Which of the two input contracts an ingestor declares, for diagnostics that name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
pub enum IngestorInputKind {
    #[strum(serialize = "transport")]
    Transport,
    #[strum(serialize = "CLIENT")]
    Client,
}

impl IngestorInput {
    pub const fn kind(&self) -> IngestorInputKind {
        match self {
            Self::Transport(_) => IngestorInputKind::Transport,
            Self::Client(_) => IngestorInputKind::Client,
        }
    }

    /// The source class capability decisions read, such as whether messages carry headers.
    pub const fn source_kind(&self) -> IngestSourceKind {
        match self {
            Self::Transport(input) => input.source.transport_kind(),
            Self::Client(_) => IngestSourceKind::Client,
        }
    }

    /// The NSPL keyword of the source, as `DESCRIBE` and diagnostics name it.
    pub fn source_label(&self) -> &str {
        match self {
            Self::Transport(input) => input.source.transport_label(),
            Self::Client(_) => "CLIENT",
        }
    }

    /// Whether this input executes on every live cluster node rather than on one scheduled owner.
    pub fn executes_on_every_cluster_node(&self) -> bool {
        match self {
            Self::Transport(input) => input.source.executes_on_every_cluster_node(),
            Self::Client(_) => false,
        }
    }

    /// Whether the messages this input delivers carry transport headers that `read_header` and
    /// `read_headers` can read.
    pub const fn reads_headers(&self) -> bool {
        self.source_kind().reads_headers()
    }

    /// The model this input references: the client or endpoint a transport reads through, or the
    /// schema a client source's batches carry.
    pub fn source_ref(&self) -> ModelName {
        match self {
            Self::Transport(input) => input.source.source_ref(),
            Self::Client(source) => ModelName::from(&source.schema),
        }
    }

    /// The kind of the model [`Self::source_ref`] names.
    pub fn source_model_kind(&self) -> ModelKind {
        match self {
            Self::Transport(input) => input.source.source_kind(),
            Self::Client(_) => ModelKind::Schema,
        }
    }

    /// The external transport this input reads. A client source reads none.
    pub fn transport_source(&self) -> Option<&IngestSource> {
        match self {
            Self::Transport(input) => Some(&input.source),
            Self::Client(_) => None,
        }
    }

    /// The codec that decodes a transport's payloads. A client source has none.
    pub fn codec(&self) -> Option<&CodecName> {
        match self {
            Self::Transport(input) => Some(&input.codec),
            Self::Client(_) => None,
        }
    }

    pub fn quiesce(&self) -> &IngestQuiesceMode {
        match self {
            Self::Transport(input) => input.source.quiesce(),
            Self::Client(_) => &CLIENT_QUIESCE,
        }
    }

    pub fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        match self {
            Self::Transport(input) => input.source.supports_quiesce(quiesce),
            Self::Client(_) => matches!(quiesce, IngestQuiesceMode::Suspend),
        }
    }

    /// The acknowledgement a transport's delivery mode declares. A client source acknowledges
    /// through its own window, which [`ClientIngestMode`] describes.
    pub fn transport_acknowledgement(&self) -> Option<IngestAcknowledgement<'_>> {
        match self {
            Self::Transport(input) => Some(input.source.acknowledgement()),
            Self::Client(_) => None,
        }
    }
}

impl ClientIngestMode {
    /// The acknowledgement window as a count: one batch for `ACK SEQUENTIAL`, `max` for
    /// `ACK PARALLEL MAX <max>`.
    pub fn window_size(&self) -> NonZeroU64 {
        match self.window {
            AckWindow::Sequential => NonZeroU64::MIN,
            AckWindow::Parallel { max } => max,
        }
    }
}
