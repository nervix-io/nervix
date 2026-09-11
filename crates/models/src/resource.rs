use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use sorted_vec::SortedVec;
use strum::{AsRefStr, EnumString, IntoStaticStr};

use crate::{ClusterNodeName, DomainName, ResourceName, Timestamp, UserName};

const MAX_UPLOAD_IDENTITY_BYTES: usize = 128;

/// An administrative upload attempt chosen by the client and reused across redirects and retries.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ResourceUploadIdentity(String);

impl ResourceUploadIdentity {
    pub fn parse(value: impl Into<String>) -> Result<Self, ResourceUploadIdentityError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ResourceUploadIdentityError::Empty);
        }
        if value.len() > MAX_UPLOAD_IDENTITY_BYTES {
            return Err(ResourceUploadIdentityError::TooLong {
                actual: value.len(),
                limit: MAX_UPLOAD_IDENTITY_BYTES,
            });
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(ResourceUploadIdentityError::InvalidCharacter);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ResourceUploadIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceUploadIdentityError {
    #[error("resource upload identity must not be empty")]
    Empty,
    #[error("resource upload identity contains {actual} bytes, exceeding the {limit}-byte limit")]
    TooLong { actual: usize, limit: usize },
    #[error("resource upload identity may contain only ASCII letters, digits, '.', '_' and '-'")]
    InvalidCharacter,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
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

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ResourceVersion {
    pub id: ResourceId,
    pub root_checksum: String,
    pub manifest_checksum: String,
    pub file_count: u64,
    pub total_bytes: u64,
    pub archive_bytes: u64,
    pub created_at: Timestamp,
    pub created_by_node: ClusterNodeName,
}

/// The full scope in which an administrative upload identity is unique.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ResourceUploadKey {
    pub owner: UserName,
    pub domain: DomainName,
    pub identifier: ResourceName,
    pub identity: ResourceUploadIdentity,
}

impl ResourceUploadKey {
    pub fn new(
        owner: UserName,
        domain: DomainName,
        identifier: ResourceName,
        identity: ResourceUploadIdentity,
    ) -> Self {
        Self {
            owner,
            domain,
            identifier,
            identity,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum ResourceUploadState {
    Receiving,
    Published { root_checksum: String },
}

impl ResourceUploadState {
    pub fn is_published(&self) -> bool {
        matches!(self, Self::Published { .. })
    }
}

/// The durable assignment and publication outcome of one administrative upload.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ResourceUpload {
    pub key: ResourceUploadKey,
    pub version: u64,
    pub state: ResourceUploadState,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
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
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
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

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
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

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
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
    pub uploads: SortedVec<ResourceUpload>,
}

impl ResourceVersionStatus {
    pub fn version(&self, id: &ResourceId) -> Option<&ResourceVersion> {
        let index = self
            .versions
            .binary_search_by(|resource| resource.id.cmp(id))
            .ok()?;
        self.versions.get(index)
    }

    pub fn upload(&self, key: &ResourceUploadKey) -> Option<&ResourceUpload> {
        let index = self
            .uploads
            .binary_search_by(|upload| upload.key.cmp(key))
            .ok()?;
        self.uploads.get(index)
    }

    /// Locates a resource's catalog slot. `Ok` holds the entry's position and `Err` holds the
    /// position it would be inserted at. The catalog is sorted by domain and then identifier, so
    /// callers resolve a resource by key instead of scanning the catalog.
    pub fn resource_slot(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
    ) -> Result<usize, usize> {
        self.next_version_by_resource.binary_search_by(|counter| {
            counter
                .domain
                .cmp(domain)
                .then_with(|| counter.identifier.cmp(identifier))
        })
    }

    /// Returns the next version the named resource would receive in `domain`, which is `None`
    /// until the resource is declared there. Resources are domain-owned, so the same name in two
    /// domains is two independent resources with independent version sequences.
    pub fn next_version(&self, domain: &DomainName, identifier: &ResourceName) -> Option<u64> {
        let index = self.resource_slot(domain, identifier).ok()?;
        self.next_version_by_resource
            .get(index)
            .map(|counter| counter.next_version)
    }

    /// Returns the highest published version of the named resource in `domain`, which is `None`
    /// when the resource is declared but has no published version yet.
    pub fn latest_version(&self, domain: &DomainName, identifier: &ResourceName) -> Option<u64> {
        let next = self.next_version(domain, identifier)?;
        let latest = next.checked_sub(1)?;
        if latest > 0 { Some(latest) } else { None }
    }

    pub fn is_declared(&self, domain: &DomainName, identifier: &ResourceName) -> bool {
        self.next_version(domain, identifier).is_some()
    }
}
