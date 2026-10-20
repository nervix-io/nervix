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

use super::entity_union::connector_sources;
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
        delivery: &[DeclaredDelivery::NO_ACK],
        quiesce: &[DeclaredQuiesce::REJECT, DeclaredQuiesce::ENDPOINT_BUFFER],
        locator: None,
        options: &[],
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
