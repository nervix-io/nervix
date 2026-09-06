use serde::{Deserialize, Serialize};
use sorted_vec::SortedVec;
use strum::{AsRefStr, EnumString, IntoStaticStr};

use crate::{ClusterNodeName, DomainName, ResourceName, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ResourceId {
    pub domain: DomainName,
    pub identifier: ResourceName,
    pub version: u64,
}

impl ResourceId {
    pub fn new(domain: DomainName, identifier: ResourceName, version: u64) -> Self {
        Self {
            domain,
            identifier,
            version,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ResourceVersion {
    pub id: ResourceId,
    pub root_checksum: String,
    pub manifest_checksum: String,
    pub file_count: u64,
    pub total_bytes: u64,
    pub created_at: Timestamp,
    pub created_by_node: ClusterNodeName,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ResourceVersionKey {
    pub domain: DomainName,
    pub identifier: ResourceName,
    pub version: u64,
}

impl ResourceVersionKey {
    pub fn new(domain: DomainName, identifier: ResourceName, version: u64) -> Self {
        Self {
            domain,
            identifier,
            version,
        }
    }

    pub fn resource_id(&self) -> ResourceId {
        ResourceId::new(self.domain.clone(), self.identifier.clone(), self.version)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    AsRefStr,
    EnumString,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum ResourceNodeState {
    Pending,
    Ready,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ResourceReplicaKey {
    pub domain: DomainName,
    pub identifier: ResourceName,
    pub version: u64,
    pub node_id: ClusterNodeName,
}

impl ResourceReplicaKey {
    pub fn new(
        domain: DomainName,
        identifier: ResourceName,
        version: u64,
        node_id: ClusterNodeName,
    ) -> Self {
        Self {
            domain,
            identifier,
            version,
            node_id,
        }
    }

    pub fn version_key(&self) -> ResourceVersionKey {
        ResourceVersionKey::new(self.domain.clone(), self.identifier.clone(), self.version)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ResourceNodeStatus {
    pub key: ResourceReplicaKey,
    pub state: ResourceNodeState,
    pub root_checksum: Option<String>,
    pub last_verified_at: Option<Timestamp>,
    pub source_node_id: Option<ClusterNodeName>,
    pub error: Option<String>,
}

/// The version counter of one declared resource. Resources are domain-owned, so the counter is
/// keyed by the owning domain as well as the resource name, and the field order is the order the
/// catalog is sorted and searched by.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ResourceVersionCounter {
    pub domain: DomainName,
    pub identifier: ResourceName,
    pub next_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResourceVersionStatus {
    pub next_version_by_resource: SortedVec<ResourceVersionCounter>,
    pub versions: SortedVec<ResourceVersion>,
    pub replicas: SortedVec<ResourceNodeStatus>,
}

impl ResourceVersionStatus {
    /// Returns the next version the named resource would receive in `domain`, which is `None`
    /// until the resource is declared there. Resources are domain-owned, so the same name in two
    /// domains is two independent resources with independent version sequences.
    pub fn next_version(&self, domain: &DomainName, identifier: &ResourceName) -> Option<u64> {
        self.next_version_by_resource.iter().find_map(|counter| {
            (counter.domain == *domain && counter.identifier == *identifier)
                .then_some(counter.next_version)
        })
    }

    /// Returns the highest installed version of the named resource in `domain`, which is `None`
    /// when the resource is declared but has no uploaded version yet.
    pub fn latest_version(&self, domain: &DomainName, identifier: &ResourceName) -> Option<u64> {
        self.next_version(domain, identifier)
            .and_then(|next| next.checked_sub(1))
            .filter(|version| *version > 0)
    }

    pub fn is_declared(&self, domain: &DomainName, identifier: &ResourceName) -> bool {
        self.next_version(domain, identifier).is_some()
    }
}
