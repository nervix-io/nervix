//! The ZeroMQ connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The ZeroMQ client Model, the pull source and push sink Models with their delivery
//!   mode, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** The ZeroMQ driver, its configuration keys, or how a socket is bound,
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
    entity_union::{connector_sinks, connector_sources},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm, EmitterPublishingModeKind,
    IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities, PositionClass, SinkBody,
    SinkClientReference, SinkEntity, SinkReference, SinkReferenceKind, SourceBody, SourceEntity,
    SourceMetadataScope, SourceReference, SourceReferenceKind,
};

configured_client!(ZeroMq: unpooled, config Nervix, CreateClientZeroMq => ZeroMqConfigEntry);

/// The pull socket of a ZeroMQ client, which the client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ZeroMqPullSource {
    pub client: ClientName,
    pub mode: ZeroMqIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a ZEROMQ PULL holds and how long it waits.
const ZEROMQ_SOURCE_BOUNDS: DeclaredBounds = DeclaredBounds {
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
const ZEROMQ_SINK_BOUNDS: DeclaredBounds = DeclaredBounds {
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

impl SourceEntity for ZeroMqPullSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        headers: None,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::ZeroMq),
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
        bounds: ZEROMQ_SOURCE_BOUNDS,
    };
}

impl SourceBody for ZeroMqPullSource {
    fn reference(&self) -> SourceReference<'_> {
        SourceReference::Client(&self.client)
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

connector_sources! {
    connector ZeroMq = "ZEROMQ";
    /// What a ZeroMQ source reads.
    pub enum ZeroMqSource;
    /// The alterations of the ingestors that read a ZeroMQ entity.
    pub enum ZeroMqIngestorAlteration;
    /// The entities a ZeroMQ source reads.
    pub enum ZeroMqSourceEntity {
        Pull(ZeroMqPullSource) = "PULL", "ZeroMQ";
    }
}

/// The push socket of a ZeroMQ client, which the client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ZeroMqPushSink {
    pub client: ClientName,
}

impl SinkEntity for ZeroMqPushSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        headers: None,
        reference: SinkReferenceKind::Client(ConnectorKind::ZeroMq),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: &[EmitterPublishingModeKind::NoAck],
        locator: None,
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: ZEROMQ_SINK_BOUNDS,
    };
}

impl SinkBody for ZeroMqPushSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::ZeroMq,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector ZeroMq = "ZEROMQ";
    /// What a ZeroMQ sink writes.
    pub enum ZeroMqSink;
    /// The entities a ZeroMQ sink writes.
    pub enum ZeroMqSinkEntity {
        Push(ZeroMqPushSink) = "PUSH";
    }
}
