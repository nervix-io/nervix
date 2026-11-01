//! What an ingestor reads, and how what it reads becomes records of its input schema.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The two input contracts an ingestor declares — an external transport together with
//!   the codec that decodes its payloads, or the typed batches application clients submit through
//!   their sessions — and how either one answers what its source declares: header reads, execution
//!   on every node, the model it references, its quiesce mode and its acknowledgement. A client
//!   contract writes no quiesce mode, so the one it honors is fixed here.
//! - **Depends on.** The source Models with what their entities declare, and the schema and codec
//!   names they reference.
//! - **Must not know.** How a source is opened, how a batch is decoded or admitted, or which node
//!   executes an ingestor.
//!
//! The codec belongs to the transport contract, so a client ingestor cannot name one and a
//! transport ingestor cannot lack one: neither contradiction is representable.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{
    ClientIngestorEntity, ClientProducerIngestor, CodecName, IngestAcknowledgement,
    IngestQuiesceMode, IngestorDeclaration, IngestorEntity, IngestorEntityKind, ModelKind,
    ModelName,
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
    Client(ClientProducerIngestor),
}

/// An external transport and the codec that decodes the payloads it delivers.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransportIngestorInput {
    pub source: IngestorEntity,
    pub codec: CodecName,
}

impl IngestorInput {
    /// The connector and entity of the source, which capability decisions read: whether its
    /// messages carry headers, which metadata they expose, and where it executes.
    pub const fn entity_kind(&self) -> IngestorEntityKind {
        match self {
            Self::Transport(input) => input.source.kind(),
            Self::Client(_) => IngestorEntityKind::Client(ClientIngestorEntity::Producer),
        }
    }

    /// The phrase that names the pair this input reads, its connector's keyword then its entity's,
    /// as the statement's head, `DESCRIBE` and diagnostics name it.
    pub fn source_phrase(&self) -> &'static str {
        self.entity_kind().phrase()
    }

    /// Whether this input executes on every live cluster node rather than on one scheduled owner.
    pub fn executes_on_every_cluster_node(&self) -> bool {
        self.entity_kind()
            .capabilities()
            .executes_on_every_cluster_node
    }

    /// Whether the messages this input delivers carry transport headers that `read_header` and
    /// `read_headers` can read.
    pub fn reads_headers(&self) -> bool {
        self.entity_kind().reads_headers()
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
        self.entity_kind().capabilities().reference.model_kind()
    }

    /// The external transport this input reads. A client source reads none.
    pub fn transport_entity(&self) -> Option<&IngestorEntity> {
        match self {
            Self::Transport(input) => Some(&input.source),
            Self::Client(_) => None,
        }
    }

    /// The mutable form of [`Self::transport_entity`], which an alteration sets.
    pub(crate) fn transport_source_mut(&mut self) -> Option<&mut IngestorEntity> {
        match self {
            Self::Transport(input) => Some(&mut input.source),
            Self::Client(_) => None,
        }
    }

    /// The source application clients produce into. A transport input has none.
    pub(crate) fn client_entity_mut(&mut self) -> Option<&mut ClientProducerIngestor> {
        match self {
            Self::Transport(_) => None,
            Self::Client(source) => Some(source),
        }
    }

    /// The codec that decodes a transport's payloads. A client source has none.
    pub fn codec(&self) -> Option<&CodecName> {
        match self {
            Self::Transport(input) => Some(&input.codec),
            Self::Client(_) => None,
        }
    }

    /// The mutable form of [`Self::codec`], which an alteration sets.
    pub(crate) fn codec_mut(&mut self) -> Option<&mut CodecName> {
        match self {
            Self::Transport(input) => Some(&mut input.codec),
            Self::Client(_) => None,
        }
    }

    pub fn quiesce(&self) -> &IngestQuiesceMode {
        match self {
            Self::Transport(input) => input.source.quiesce(),
            Self::Client(_) => &CLIENT_QUIESCE,
        }
    }

    /// Whether this input can honor `quiesce`, as its entity declares.
    pub fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        match self {
            Self::Transport(input) => input.source.supports_quiesce(quiesce),
            Self::Client(_) => ClientProducerIngestor::CAPABILITIES
                .declared_quiesce(quiesce.kind())
                .is_some(),
        }
    }

    /// The requirement of a transport's delivery mode its statement leaves unmet, as its entity
    /// declares the mode. A client source's window requires no option choice.
    pub fn unmet_delivery(&self) -> Option<crate::UnmetDeliveryRequirement> {
        match self {
            Self::Transport(input) => input.source.unmet_delivery(),
            Self::Client(_) => None,
        }
    }

    /// The acknowledgement a transport's delivery mode declares. A client source acknowledges
    /// through its own window, which [`ClientIngestMode`](crate::ClientIngestMode) describes.
    pub fn transport_acknowledgement(&self) -> Option<IngestAcknowledgement<'_>> {
        match self {
            Self::Transport(input) => Some(input.source.acknowledgement()),
            Self::Client(_) => None,
        }
    }
}
