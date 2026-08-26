use serde::{Deserialize, Serialize};
use sorted_vec::SortedVec;
use strum::{AsRefStr, EnumString, IntoStaticStr};

use crate::{Domain, Identifier, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ResourceId {
    pub domain: Domain,
    pub identifier: Identifier,
    pub version: u64,
}

impl ResourceId {
    pub fn new(domain: Domain, identifier: Identifier, version: u64) -> Self {
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
    pub created_by_node: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ResourceVersionKey {
    pub domain: Domain,
    pub identifier: Identifier,
    pub version: u64,
}

impl ResourceVersionKey {
    pub fn new(domain: Domain, identifier: Identifier, version: u64) -> Self {
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
    pub domain: Domain,
    pub identifier: Identifier,
    pub version: u64,
    pub node_id: String,
}

impl ResourceReplicaKey {
    pub fn new(
        domain: Domain,
        identifier: Identifier,
        version: u64,
        node_id: impl Into<String>,
    ) -> Self {
        Self {
            domain,
            identifier,
            version,
            node_id: node_id.into(),
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
    pub source_node_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResourceVersionStatus {
    pub next_version_by_resource: SortedVec<(Domain, Identifier, u64)>,
    pub versions: SortedVec<ResourceVersion>,
    pub replicas: SortedVec<ResourceNodeStatus>,
}

impl ResourceVersionStatus {
    /// Returns the next version the named resource would receive in `domain`, which is `None`
    /// until the resource is declared there. Resources are domain-owned, so the same name in two
    /// domains is two independent resources with independent version sequences.
    pub fn next_version(&self, domain: &Domain, identifier: &Identifier) -> Option<u64> {
        self.next_version_by_resource.iter().find_map(
            |(known_domain, known_identifier, next_version)| {
                (known_domain == domain && known_identifier == identifier).then_some(*next_version)
            },
        )
    }

    /// Returns the highest installed version of the named resource in `domain`, which is `None`
    /// when the resource is declared but has no uploaded version yet.
    pub fn latest_version(&self, domain: &Domain, identifier: &Identifier) -> Option<u64> {
        self.next_version(domain, identifier)
            .and_then(|next| next.checked_sub(1))
            .filter(|version| *version > 0)
    }

    pub fn is_declared(&self, domain: &Domain, identifier: &Identifier) -> bool {
        self.next_version(domain, identifier).is_some()
    }
}
