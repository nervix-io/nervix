//! The WebSockets connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The client's closed configuration schema and signaling reference, the connection's
//!   literal upgrade fields and frame kind, the endpoint source Model, and their declarations.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** Shared source/client declarations, HTTP field rules and referenced names.
//! - **Must not know.** Drivers, sockets, TLS file contents, signaling execution, or how endpoints
//!   are published, routed to or served.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::time::Duration;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

mod config;
#[cfg(test)]
mod config_tests;
mod frame;
mod upgrade_headers;
#[cfg(test)]
mod upgrade_headers_tests;

use config::CLIENT_CONFIG;
pub use config::{WebsocketsClientConfig, WebsocketsClientConfigError};
pub use frame::WebsocketsFrameType;
pub use upgrade_headers::{
    WebsocketsUpgradeHeader, WebsocketsUpgradeHeaders, WebsocketsUpgradeHeadersError,
};

/// The existing transport budget shared by ping writes and the close handshake.
pub const WEBSOCKETS_CONTROL_FRAME_BUDGET: Duration = Duration::from_secs(1);

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DEFAULT_CONNECT_TIMEOUT, DeadlineKey,
        DeclaredBounds, DeclaredBuffer, DeclaredWait, OmittedDeadline, WaitBound,
    },
    entity_union::connector_ingestors,
};
use crate::{
    AcknowledgementConfirmation, ClientConfigEntry, ClientDeclaration, ClientName, ClientPooling,
    ClientResourceMount, ConnectorKind, DeclaredDelivery, DeclaredQuiesce, EndpointIngestMode,
    EndpointName, EndpointType, HeaderLayout, HeaderModel, IngestAcknowledgement,
    IngestQuiesceMode, IngestorDeclaration, IngestorEntityBody, IngestorEntityCapabilities,
    IngestorMetadataDeclaration, IngestorMetadataField, IngestorMetadataType, IngestorReference,
    IngestorReferenceKind, OptionDeclaration, OptionForm, OptionPresence, OptionValue,
    PositionClass, SignalingProtocolName,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateClientWebsockets<Version = u64> {
    pub name: ClientName,
    pub mount: Option<ClientResourceMount<Version>>,
    pub signaling_protocol: Option<SignalingProtocolName>,
    pub config: Vec<ClientConfigEntry>,
}

pub type WebsocketsConfigEntry = ClientConfigEntry;

impl<Version> CreateClientWebsockets<Version> {
    /// The connector this client configures.
    pub const CONNECTOR: ConnectorKind = ConnectorKind::Websockets;

    /// What every WebSockets client declares: the signaling protocol its connection may speak.
    pub const DECLARATION: ClientDeclaration = ClientDeclaration {
        pooling: ClientPooling::Unpooled,
        config: CLIENT_CONFIG,
        options: &[OptionDeclaration {
            keywords: "WITH SIGNALING PROTOCOL",
            presence: OptionPresence::Optional { default: None },
            form: OptionForm::Value {
                value: OptionValue::Clause,
                example: "WITH SIGNALING PROTOCOL signaling",
            },
        }],
    };
}

/// The connection a WebSockets client opens to the server its configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WebsocketsConnectionIngestor {
    pub client: ClientName,
    pub headers: Vec<WebsocketsUpgradeHeader>,
    pub mode: WebsocketsIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a WEBSOCKETS CONNECTION holds and how long it waits.
const WEBSOCKETS_CONNECTION_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "accepted signaling frames retained for source reads",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting(
                    "handshake_buffer_capacity messages (default 16), each at most \
                     max_message_size bytes",
                ),
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "one accepted frame waiting for handoff room",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("max_message_size bytes (default 67108864)"),
                overflow: BufferOverflow::Backpressure,
            },
        },
        DeclaredBuffer {
            holds: "the incoming message assembled by the transport",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting(
                    "max_message_size bytes (default 67108864), with each frame bounded by \
                     max_frame_size bytes (default 16777216)",
                ),
                overflow: BufferOverflow::FailRead,
            },
        },
    ],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, address attempts, TLS and the opening upgrade",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "the signaling handshake",
            bound: WaitBound::Clause("the signaling protocol's TIMEOUT"),
        },
        DeclaredWait {
            wait: "reading the next frame",
            bound: WaitBound::Key {
                key: DeadlineKey::IdleTimeout,
                omitted: OmittedDeadline::Derived(
                    "three ping_interval_ms intervals, or 30 s without configured pings",
                ),
            },
        },
        DeclaredWait {
            wait: "writing a configured ping",
            bound: WaitBound::Fixed(WEBSOCKETS_CONTROL_FRAME_BUDGET),
        },
        DeclaredWait {
            wait: "close",
            bound: WaitBound::Fixed(WEBSOCKETS_CONTROL_FRAME_BUDGET),
        },
    ],
    driver_limits: &[],
};

/// What the connector of a WEBSOCKETS ENDPOINT holds and how long it waits.
const WEBSOCKETS_ENDPOINT_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "unresolved data frames under MODE ACK, per node and ingestor",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("MODE ACK MAX"),
            overflow: BufferOverflow::RefuseRequest,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "graph acknowledgement under MODE ACK",
            bound: WaitBound::Clause("ACK TIMEOUT, or the exact intake lifetime ending"),
        },
        DeclaredWait {
            wait: "the signaling handshake",
            bound: WaitBound::Clause("the signaling protocol's TIMEOUT"),
        },
        DeclaredWait {
            wait: "reading the next frame",
            bound: WaitBound::Unbounded,
        },
    ],
    driver_limits: &["A message above 64 MiB or a frame above 16 MiB ends the session."],
};

/// The text or binary kind of one data frame of a WebSockets connection.
const WEBSOCKETS_CONNECTION_METADATA: IngestorMetadataDeclaration =
    IngestorMetadataDeclaration::new(&[IngestorMetadataField::new(
        "frame_type",
        IngestorMetadataType::String,
    )]);

impl IngestorDeclaration for WebsocketsConnectionIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
        metadata: WEBSOCKETS_CONNECTION_METADATA,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Websockets),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
        // Both keep reading: stopping reads would starve ping/pong and disconnect the peer.
        quiesce: &[DeclaredQuiesce::BUFFER, DeclaredQuiesce::DROP],
        locator: None,
        options: &[OptionDeclaration {
            keywords: "HEADERS",
            presence: OptionPresence::Optional { default: None },
            form: OptionForm::Value {
                value: OptionValue::Clause,
                example: "HEADERS { 'authorization' = 'Bearer token' }",
            },
        }],
        bounds: WEBSOCKETS_CONNECTION_BOUNDS,
    };
}

impl IngestorEntityBody for WebsocketsConnectionIngestor {
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
pub enum WebsocketsIngestMode {
    NoAck,
}

impl WebsocketsIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

/// One endpoint of `TYPE WEBSOCKETS` this cluster serves, whose messages are the payloads. Every
/// node of the cluster serves it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WebsocketsEndpointIngestor {
    pub endpoint: EndpointName,
    pub mode: EndpointIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// The text or binary kind of one data frame, the address of the peer that opened its connection and
/// the path the connection's upgrade request named.
const WEBSOCKETS_ENDPOINT_METADATA: IngestorMetadataDeclaration =
    IngestorMetadataDeclaration::new(&[
        IngestorMetadataField::new("frame_type", IngestorMetadataType::String),
        IngestorMetadataField::new("remote_addr", IngestorMetadataType::String),
        IngestorMetadataField::new("path", IngestorMetadataType::String),
    ]);

impl IngestorDeclaration for WebsocketsEndpointIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // The opening HTTP upgrade request's fields, as the server's header map keeps them: names in
        // lowercase, every value of a repeated field together under its name.
        headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
        metadata: WEBSOCKETS_ENDPOINT_METADATA,
        executes_on_every_cluster_node: true,
        reference: IngestorReferenceKind::Endpoint(EndpointType::Websockets),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK, DeclaredDelivery::ACK_REQUEST],
        quiesce: &[DeclaredQuiesce::REJECT, DeclaredQuiesce::ENDPOINT_BUFFER],
        locator: None,
        options: &[],
        bounds: WEBSOCKETS_ENDPOINT_BOUNDS,
    };
}

impl IngestorEntityBody for WebsocketsEndpointIngestor {
    fn reference(&self) -> IngestorReference<'_> {
        IngestorReference::Endpoint(&self.endpoint)
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

connector_ingestors! {
    connector Websockets = "WEBSOCKETS";
    /// What a WebSockets source reads.
    pub enum WebsocketsIngestor;
    /// The alterations of the ingestors that read a WebSockets entity.
    pub enum WebsocketsIngestorAlteration;
    /// The entities a WebSockets source reads.
    pub enum WebsocketsIngestorEntity {
        Connection(WebsocketsConnectionIngestor) = "CONNECTION", "WebSocket client";
        Endpoint(WebsocketsEndpointIngestor) = "ENDPOINT", "WebSocket endpoint";
    }
}
