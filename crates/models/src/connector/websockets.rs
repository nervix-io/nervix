//! The WebSockets connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The WebSockets client Model with the signaling protocol it speaks, the connection
//!   source Model with its delivery mode, and what it declares.
//! - **Depends on.** The shared source declarations and the names a body references.
//! - **Must not know.** The WebSockets driver, its configuration keys, or how a connection is
//!   opened, signaled or read.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::entity_union::connector_sources;
use crate::{
    ClientConfigEntry, ClientName, ClientResourceMount, ConnectorKind, IngestAcknowledgement,
    IngestQuiesceMode, IngestSourceCapabilities, SignalingProtocolName, SourceBody, SourceEntity,
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
    NoAckSequential,
}

impl WebsocketsIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAckSequential => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector Websockets = "WEBSOCKETS";
    /// What a WebSockets source reads.
    pub enum WebsocketsSource;
    /// The entities a WebSockets source reads.
    pub enum WebsocketsSourceEntity {
        Connection(WebsocketsConnectionSource) = "CONNECTION", "WebSocket client";
    }
}
