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
use nervix_models::{ResourceId, ResourceNodeStatus};
use rkyv::{Archive, Deserialize, Serialize};

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
    type Response = Result<ResourceArchiveChunk, String>;

    const NAME: &'static str = "fetch_resource_archive_chunk";
    const CLASS: PoolClass = PoolClass::Bulk;
    const TIMEOUT: Duration = Duration::from_secs(30);
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PublishResourceReplica {
    pub(crate) replica: ResourceNodeStatus,
}

impl InterconnectRequest for PublishResourceReplica {
    type Response = Result<(), String>;

    const NAME: &'static str = "publish_resource_replica";
    const CLASS: PoolClass = PoolClass::Commands;
    const TIMEOUT: Duration = Duration::from_secs(5);
}
