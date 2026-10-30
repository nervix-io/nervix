//! The ZeroMQ connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The ZeroMQ client Model, the pull source and push emitter Models with their delivery
//!   mode, and what each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared source and emitter declarations and the names a body references.
//! - **Must not know.** The ZeroMQ driver, or how a socket is bound,
//!   connected, read or written.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, DEFAULT_CONNECT_TIMEOUT, DeadlineKey, DeclaredBounds, DeclaredBuffer,
        DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_emitters, connector_ingestors},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterClientReference, EmitterDeclaration,
    EmitterEntityBody, EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind,
    IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration, IngestorEntityBody,
    IngestorEntityCapabilities, IngestorMetadataDeclaration, IngestorReference,
    IngestorReferenceKind, PositionClass,
};

configured_client!(ZeroMq: unpooled, config CLIENT_CONFIG, CreateClientZeroMq => ZeroMqConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("addr", V::String),
            K::optional("bind", V::Boolean),
            K::CONNECT_TIMEOUT,
            K::TIMEOUT,
        ],
        &[],
        &[],
    )
};

/// The pull socket of a ZeroMQ client, which the client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ZeroMqPullIngestor {
    pub client: ClientName,
    pub mode: ZeroMqIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a ZEROMQ PULL holds and how long it waits.
const ZEROMQ_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "the frame a peer is sending",
        bound: BufferBound::Unbounded,
    }],
    waits: &[
        DeclaredWait {
            wait: "connecting, retried without limit",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "the ZMTP handshake",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "receiving the next message",
            bound: WaitBound::Unbounded,
        },
    ],
    driver_limits: &["The driver has no high-water mark, maximum message size or heartbeat."],
};

/// What the connector of a ZEROMQ PUSH holds and how long it waits.
const ZEROMQ_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "bytes queued for a peer",
        bound: BufferBound::Unbounded,
    }],
    waits: &[
        DeclaredWait {
            wait: "connecting and the ZMTP handshake",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "a send",
            bound: WaitBound::Key {
                key: DeadlineKey::Timeout,
                omitted: OmittedDeadline::Unbounded,
            },
        },
    ],
    driver_limits: &["The driver has no high-water mark, maximum message size or heartbeat."],
};

impl IngestorDeclaration for ZeroMqPullIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        headers: None,
        metadata: IngestorMetadataDeclaration::NONE,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::ZeroMq),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
        quiesce: &[
            DeclaredQuiesce::SUSPEND,
            DeclaredQuiesce::BUFFER,
            DeclaredQuiesce::DROP,
        ],
        locator: None,
        options: &[],
        bounds: ZEROMQ_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for ZeroMqPullIngestor {
    fn reference(&self) -> IngestorReference<'_> {
        IngestorReference::Client(&self.client)
    }

    fn quiesce(&self) -> &IngestQuiesceMode {
        &self.quiesce
    }

    fn quiesce_mut(&mut self) -> &mut IngestQuiesceMode {
        &mut self.quiesce
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ZeroMqIngestMode {
    NoAck,
}

impl ZeroMqIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_ingestors! {
    connector ZeroMq = "ZEROMQ";
    /// What a ZeroMQ source reads.
    pub enum ZeroMqIngestor;
    /// The alterations of the ingestors that read a ZeroMQ entity.
    pub enum ZeroMqPullIngestorAlteration;
    /// The entities a ZeroMQ source reads.
    pub enum ZeroMqIngestorEntity {
        Pull(ZeroMqPullIngestor) = "PULL", "ZeroMQ";
    }
}

/// The push socket of a ZeroMQ client, which the client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ZeroMqPushEmitter {
    pub mode: ZeroMqPushPublishingMode,
    pub client: ClientName,
}

impl EmitterDeclaration for ZeroMqPushEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::ZeroMq),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: ZeroMqPushPublishingMode::KINDS,
        locator: None,
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: ZEROMQ_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for ZeroMqPushEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::ZeroMq,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector ZeroMq = "ZEROMQ";
    /// What a ZeroMQ emitter writes.
    pub enum ZeroMqEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum ZeroMqPushEmitterMode;
    /// The entities a ZeroMQ emitter writes.
    pub enum ZeroMqEmitterEntity {
        Push(ZeroMqPushEmitter, ZeroMqPushPublishingMode) = "PUSH";
    }
}

super::publishing::emitter_modes! {
    pub enum ZeroMqPushPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
    }
}
