//! The WebSockets connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The WebSockets client Model with the signaling protocol it speaks, the connection
//!   source Model with its delivery mode, the source that reads a WebSockets endpoint this cluster
//!   serves, and what each entity declares.
//! - **Depends on.** The shared source declarations and the names a body references.
//! - **Must not know.** The WebSockets driver, its configuration keys, how a connection is opened,
//!   signaled or read, or how an endpoint is published, routed to or served.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::entity_union::connector_sources;
use crate::{
    ClientConfigEntry, ClientName, ClientResourceMount, ConnectorKind, EndpointIngestMode,
    EndpointName, EndpointType, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    SignalingProtocolName, SourceBody, SourceEntity, SourceMetadataScope, SourceReference,
    SourceReferenceKind,
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
}

/// The connection a WebSockets client opens to the server its configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WebsocketsConnectionSource {
    pub client: ClientName,
    pub mode: WebsocketsIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for WebsocketsConnectionSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: false,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Websockets),
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

    fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        matches!(
            quiesce,
            IngestQuiesceMode::Buffer { .. } | IngestQuiesceMode::Drop
        )
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

    fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        self.mode.supports_quiesce(quiesce)
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
