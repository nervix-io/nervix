//! Typed internode resource replication messages.
//!
//! Layer: control plane.
//!
//! - **Owns.** The current request and response shapes for publishing resource replica state and
//!   opening one bounded archive stream.
//! - **Depends on.** Resource vocabulary and the authenticated interconnect request contract.
//! - **Must not know.** Resource-store paths, HTTP endpoints, cluster discovery, or handler logic.

use std::time::Duration;

use nervix_interconnect::{
    InterconnectRequest, InterconnectStreamRequest, PoolClass, RequestSubquota,
};
use nervix_models::{ClusterNodeName, ResourceId, ResourceNodeStatus};
use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq, Error)]
pub(crate) enum ResourceInterconnectError {
    #[error("authenticated node '{authenticated}' cannot publish resource state for '{declared}'")]
    ReplicaOrigin {
        authenticated: ClusterNodeName,
        declared: ClusterNodeName,
    },
    #[error("failed to publish resource replica state: {0}")]
    ReplicaPublish(String),
}

impl ResourceInterconnectError {
    pub(crate) fn replica_publish(error: impl std::fmt::Display) -> Self {
        Self::ReplicaPublish(error.to_string())
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FetchResourceArchive {
    pub(crate) id: ResourceId,
}

impl InterconnectStreamRequest for FetchResourceArchive {
    const NAME: &'static str = "fetch_resource_archive";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Resource;
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
