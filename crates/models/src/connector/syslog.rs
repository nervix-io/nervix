//! The syslog connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The syslog client Model, the listener source Model with its delivery mode, the
//!   collector emitter Model, and what each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared source and emitter declarations and the names a body references.
//! - **Must not know.** Syslog framing or how a listener is bound or a collector is written to.

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
    entity_union::{connector_emitters, connector_ingestors},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterClientReference, EmitterDeclaration,
    EmitterEntityBody, EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind,
    IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration, IngestorEntityBody,
    IngestorEntityCapabilities, IngestorMetadataDeclaration, IngestorMetadataField,
    IngestorMetadataType, IngestorReference, IngestorReferenceKind, PositionClass,
};

configured_client!(Syslog: unpooled, config CLIENT_CONFIG, CreateClientSyslog => SyslogConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("addr", V::String),
            K::CONNECT_TIMEOUT,
            K::optional("framing", V::Choice(&["octet-counting", "non-transparent"])),
            K::IDLE_TIMEOUT,
            K::optional("max_message_size", V::PositiveInteger),
            K::required("protocol", V::Choice(&["udp", "tcp", "tls"])),
            K::TIMEOUT,
            K::TLS_CA,
            K::TLS_CERT,
            K::TLS_KEY,
        ],
        &[],
        &[],
    )
};

/// The listener a syslog client's configuration binds, which every node of the cluster serves.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyslogListenerIngestor {
    pub client: ClientName,
    pub mode: SyslogIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a SYSLOG LISTENER holds and how long it waits.
const SYSLOG_LISTENER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "UDP datagrams admitted after a fixed 65,535-byte receive scratch buffer",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("max_message_size"),
                overflow: BufferOverflow::DropAndCount,
            },
        },
        DeclaredBuffer {
            holds: "TCP and TLS frame assembly across unlimited connections (max_message_size + \
                    11 bytes per connection; an oversized frame closes only that connection)",
            bound: BufferBound::Unbounded,
        },
        DeclaredBuffer {
            holds: "completed TCP and TLS frames waiting for the intake queue (one frame per \
                    connection, at most max_message_size bytes; unlimited connections)",
            bound: BufferBound::Unbounded,
        },
        DeclaredBuffer {
            holds: "frames read from TCP and TLS connections",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 64,
                    unit: "frames",
                },
                overflow: BufferOverflow::Backpressure,
            },
        },
    ],
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

/// The peer a syslog message arrived from.
const SYSLOG_LISTENER_METADATA: IngestorMetadataDeclaration =
    IngestorMetadataDeclaration::new(&[IngestorMetadataField::new(
        "peer_addr",
        IngestorMetadataType::String,
    )]);

impl IngestorDeclaration for SyslogListenerIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        headers: None,
        metadata: SYSLOG_LISTENER_METADATA,
        executes_on_every_cluster_node: true,
        reference: IngestorReferenceKind::Client(ConnectorKind::Syslog),
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

impl IngestorEntityBody for SyslogListenerIngestor {
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

connector_ingestors! {
    connector Syslog = "SYSLOG";
    /// What a syslog source reads.
    pub enum SyslogIngestor;
    /// The alterations of the ingestors that read a syslog entity.
    pub enum SyslogListenerIngestorAlteration;
    /// The entities a syslog source reads.
    pub enum SyslogIngestorEntity {
        Listener(SyslogListenerIngestor) = "LISTENER", "Syslog";
    }
}

/// The collector a syslog client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyslogCollectorEmitter {
    pub mode: SyslogCollectorPublishingMode,
    pub client: ClientName,
}

impl EmitterDeclaration for SyslogCollectorEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::Syslog),
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

impl EmitterEntityBody for SyslogCollectorEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Syslog,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector Syslog = "SYSLOG";
    /// What a syslog emitter writes.
    pub enum SyslogEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum SyslogCollectorEmitterMode;
    /// The entities a syslog emitter writes.
    pub enum SyslogEmitterEntity {
        Collector(SyslogCollectorEmitter, SyslogCollectorPublishingMode) = "COLLECTOR";
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "qualifies the Syslog listener's declared transport bounds"
    )
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** The listener declaration's admission, retention and overflow contract.
    //! - **Depends on.** The Syslog vocabulary and shared buffer declarations.
    //! - **Must not know.** Socket I/O, frame decoding or host execution.

    use super::*;

    #[test]
    fn listener_declares_datagram_admission_and_all_stream_buffers_in_transport_order() {
        let buffers = SyslogListenerIngestor::CAPABILITIES.bounds.buffers;
        assert_eq!(buffers.len(), 4);
        assert_eq!(
            buffers[0].bound,
            BufferBound::Bounded {
                capacity: BufferCapacity::Setting("max_message_size"),
                overflow: BufferOverflow::DropAndCount,
            }
        );
        assert_eq!(buffers[1].bound, BufferBound::Unbounded);
        assert!(
            buffers[1]
                .holds
                .contains("max_message_size + 11 bytes per connection")
        );
        assert!(
            buffers[1]
                .holds
                .contains("oversized frame closes only that connection")
        );
        assert_eq!(buffers[2].bound, BufferBound::Unbounded);
        assert!(buffers[2].holds.contains("one frame per connection"));
        assert!(buffers[2].holds.contains("max_message_size"));
        assert_eq!(
            buffers[3].bound,
            BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 64,
                    unit: "frames",
                },
                overflow: BufferOverflow::Backpressure,
            }
        );
    }
}

super::publishing::emitter_modes! {
    pub enum SyslogCollectorPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
    }
}
