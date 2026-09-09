//! Typed internode resource replication messages.
//!
//! Layer: control plane.
//!
//! - **Owns.** The current request and response shapes for publishing resource replica state and
//!   reading one bounded archive chunk.
//! - **Depends on.** Resource vocabulary and the authenticated interconnect request contract.
//! - **Must not know.** Resource-store paths, HTTP endpoints, cluster discovery, or handler logic.

use std::time::Duration;

use nervix_interconnect::{InterconnectRequest, PoolClass};
use nervix_models::{ClusterNodeName, ResourceId, ResourceNodeStatus};
use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq, Error)]
pub(crate) enum ResourceInterconnectError {
    #[error("failed to read the resource archive: {0}")]
    ArchiveRead(String),
    #[error("authenticated node '{authenticated}' cannot publish resource state for '{declared}'")]
    ReplicaOrigin {
        authenticated: ClusterNodeName,
        declared: ClusterNodeName,
    },
    #[error("failed to publish resource replica state: {0}")]
    ReplicaPublish(String),
}

impl ResourceInterconnectError {
    pub(crate) fn archive_read(error: impl std::fmt::Display) -> Self {
        Self::ArchiveRead(error.to_string())
    }

    pub(crate) fn replica_publish(error: impl std::fmt::Display) -> Self {
        Self::ReplicaPublish(error.to_string())
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FetchResourceArchiveChunk {
    pub(crate) id: ResourceId,
    pub(crate) offset: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResourceArchiveChunk {
    pub(crate) bytes: Vec<u8>,
    pub(crate) eof: bool,
}

impl InterconnectRequest for FetchResourceArchiveChunk {
    type Response = Result<ResourceArchiveChunk, ResourceInterconnectError>;

    const NAME: &'static str = "fetch_resource_archive_chunk";
    const CLASS: PoolClass = PoolClass::Bulk;
    const TIMEOUT: Duration = Duration::from_secs(30);
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PublishResourceReplica {
    pub(crate) replica: ResourceNodeStatus,
}

impl InterconnectRequest for PublishResourceReplica {
    type Response = Result<(), ResourceInterconnectError>;

    const NAME: &'static str = "publish_resource_replica";
    const CLASS: PoolClass = PoolClass::Commands;
    const TIMEOUT: Duration = Duration::from_secs(5);
}
