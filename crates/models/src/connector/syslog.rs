//! The syslog connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The syslog client Model, the listener source Model with its delivery mode, the
//!   collector sink Model, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** Syslog framing, its configuration keys, or how a listener is bound or a
//!   collector is written to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DEFAULT_CONNECT_TIMEOUT, DeadlineKey,
        DeclaredBounds, DeclaredBuffer, DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm, IngestAcknowledgement,
    IngestQuiesceMode, IngestSourceCapabilities, PositionClass, SinkBody, SinkClientReference,
    SinkEntity, SinkReference, SinkReferenceKind, SourceBody, SourceEntity, SourceMetadataScope,
    SourceReference, SourceReferenceKind,
};

configured_client!(Syslog: unpooled, config Nervix, CreateClientSyslog => SyslogConfigEntry);

/// The listener a syslog client's configuration binds, which every node of the cluster serves.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyslogListenerSource {
    pub client: ClientName,
    pub mode: SyslogIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a SYSLOG LISTENER holds and how long it waits.
const SYSLOG_LISTENER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "frames read from TCP and TLS connections",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Items {
                count: 64,
                unit: "frames",
            },
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "the TLS handshake of an accepted connection",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "reading a connection",
            bound: WaitBound::Key {
                key: DeadlineKey::IdleTimeout,
                omitted: OmittedDeadline::Unbounded,
            },
        },
    ],
    driver_limits: &[
        "Datagrams wait in the kernel's UDP receive buffer, which drops what does not fit.",
    ],
};

/// What the connector of a SYSLOG COLLECTOR holds and how long it waits.
const SYSLOG_COLLECTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, address attempts and TLS",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "writing and flushing a frame",
            bound: WaitBound::Key {
                key: DeadlineKey::Timeout,
                omitted: OmittedDeadline::Unbounded,
            },
        },
    ],
    driver_limits: &[],
};

impl SourceEntity for SyslogListenerSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        headers: None,
        metadata: SourceMetadataScope::Syslog,
        executes_on_every_cluster_node: true,
        reference: SourceReferenceKind::Client(ConnectorKind::Syslog),
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
        bounds: SYSLOG_LISTENER_BOUNDS,
    };
}

impl SourceBody for SyslogListenerSource {
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

/// How a listener's messages are delivered. Syslog senders expect no acknowledgement, so none
/// waits on one.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum SyslogIngestMode {
    NoAck,
}

impl SyslogIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector Syslog = "SYSLOG";
    /// What a syslog source reads.
    pub enum SyslogSource;
    /// The alterations of the ingestors that read a syslog entity.
    pub enum SyslogIngestorAlteration;
    /// The entities a syslog source reads.
    pub enum SyslogSourceEntity {
        Listener(SyslogListenerSource) = "LISTENER", "Syslog";
    }
}

/// The collector a syslog client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyslogCollectorSink {
    pub mode: SyslogCollectorPublishingMode,
    pub client: ClientName,
}

impl SinkEntity for SyslogCollectorSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        headers: None,
        reference: SinkReferenceKind::Client(ConnectorKind::Syslog),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: SyslogCollectorPublishingMode::KINDS,
        locator: None,
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: SYSLOG_COLLECTOR_BOUNDS,
    };
}

impl SinkBody for SyslogCollectorSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Syslog,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Syslog = "SYSLOG";
    /// What a syslog sink writes.
    pub enum SyslogSink;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum SyslogEmitterMode;
    /// The entities a syslog sink writes.
    pub enum SyslogSinkEntity {
        Collector(SyslogCollectorSink, SyslogCollectorPublishingMode) = "COLLECTOR";
    }
}

super::publishing::emitter_modes! {
    pub enum SyslogCollectorPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
    }
}
