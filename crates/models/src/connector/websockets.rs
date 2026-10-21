//! The WebSockets connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The client's closed configuration schema and signaling reference, the connection's
//!   literal upgrade fields and frame kind, the endpoint source Model, and their declarations.
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

pub use config::{WebsocketsClientConfig, WebsocketsClientConfigError, WebsocketsClientKey};
pub use frame::WebsocketsFrameType;
pub use upgrade_headers::{
    WebsocketsUpgradeHeader, WebsocketsUpgradeHeaders, WebsocketsUpgradeHeadersError,
};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DEFAULT_CONNECT_TIMEOUT, DeadlineKey,
        DeclaredBounds, DeclaredBuffer, DeclaredWait, OmittedDeadline, WaitBound,
    },
    entity_union::connector_sources,
};
use crate::{
    AcknowledgementConfirmation, ClientConfigEntry, ClientConfigOwner, ClientDeclaration,
    ClientName, ClientPooling, ClientResourceMount, ConnectorKind, DeclaredDelivery,
    DeclaredQuiesce, EndpointIngestMode, EndpointName, EndpointType, IngestAcknowledgement,
    IngestQuiesceMode, IngestSourceCapabilities, OptionDeclaration, OptionForm, OptionPresence,
    OptionValue, PositionClass, SignalingProtocolName, SourceBody, SourceEntity,
    SourceMetadataScope, SourceReference, SourceReferenceKind,
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
        config: ClientConfigOwner::Nervix,
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
pub struct WebsocketsConnectionSource {
    pub client: ClientName,
    pub headers: Vec<WebsocketsUpgradeHeader>,
    pub mode: WebsocketsIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a WEBSOCKETS CONNECTION holds and how long it waits.
const WEBSOCKETS_CONNECTION_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "frames that arrive while the signaling handshake runs",
        bound: BufferBound::Unbounded,
    }],
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
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "writing a configured ping",
            bound: WaitBound::Fixed(Duration::from_secs(1)),
        },
        DeclaredWait {
            wait: "close",
            bound: WaitBound::Fixed(Duration::from_secs(1)),
        },
    ],
    driver_limits: &["A message above 64 MiB or a frame above 16 MiB ends the connection."],
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

impl SourceEntity for WebsocketsConnectionSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: true,
        metadata: SourceMetadataScope::Websockets,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Websockets),
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

impl SourceBody for WebsocketsConnectionSource {
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
pub struct WebsocketsEndpointSource {
    pub endpoint: EndpointName,
    pub mode: EndpointIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for WebsocketsEndpointSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: true,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: true,
        reference: SourceReferenceKind::Endpoint(EndpointType::Websockets),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK, DeclaredDelivery::ACK_REQUEST],
        quiesce: &[DeclaredQuiesce::REJECT, DeclaredQuiesce::ENDPOINT_BUFFER],
        locator: None,
        options: &[],
        bounds: WEBSOCKETS_ENDPOINT_BOUNDS,
    };
}

impl SourceBody for WebsocketsEndpointSource {
    fn reference(&self) -> SourceReference<'_> {
        SourceReference::Endpoint(&self.endpoint)
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

connector_sources! {
    connector Websockets = "WEBSOCKETS";
    /// What a WebSockets source reads.
    pub enum WebsocketsSource;
    /// The alterations of the ingestors that read a WebSockets entity.
    pub enum WebsocketsIngestorAlteration;
    /// The entities a WebSockets source reads.
    pub enum WebsocketsSourceEntity {
        Connection(WebsocketsConnectionSource) = "CONNECTION", "WebSocket client";
        Endpoint(WebsocketsEndpointSource) = "ENDPOINT", "WebSocket endpoint";
    }
}
