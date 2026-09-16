use std::collections::{BTreeMap, BTreeSet};

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use sorted_vec::SortedVec;
use strum::{AsRefStr, EnumString, IntoStaticStr};

use crate::{ClusterNodeIdentity, ClusterNodeName, DomainName, ResourceName, Timestamp, UserName};

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
    Applying {
        root_checksum: String,
    },
    Completed {
        root_checksum: String,
        outcome_revision: u64,
    },
    Failed {
        root_checksum: String,
        outcome_revision: u64,
        reason: String,
    },
}

impl ResourceUploadState {
    pub fn root_checksum(&self) -> &str {
        match self {
            Self::Applying { root_checksum }
            | Self::Completed { root_checksum, .. }
            | Self::Failed { root_checksum, .. } => root_checksum,
        }
    }

    pub fn outcome_revision(&self) -> Option<u64> {
        match self {
            Self::Applying { .. } => None,
            Self::Completed {
                outcome_revision, ..
            }
            | Self::Failed {
                outcome_revision, ..
            } => Some(*outcome_revision),
        }
    }
}

/// The durable assignment and installation outcome of one administrative upload.
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

impl ResourceUpload {
    pub fn resource_id(&self) -> ResourceId {
        ResourceId::new(
            self.key.domain.clone(),
            self.key.identifier.clone(),
            self.version,
        )
    }
}

/// Upload outcomes indexed by both administrative identity and assigned resource version.
///
/// The values are serialized once in identity order. The version and completed-version indexes are
/// rebuilt when the collection is decoded, so the indexes cannot disagree with an upload's state.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceUploads {
    by_key: BTreeMap<ResourceUploadKey, ResourceUpload>,
    by_version: BTreeMap<ResourceId, ResourceUploadKey>,
    completed_versions: BTreeSet<ResourceId>,
}

impl ResourceUploads {
    pub fn try_from_uploads(
        uploads: impl IntoIterator<Item = ResourceUpload>,
    ) -> Result<Self, Report<ResourceUploadsError>> {
        let mut by_key = BTreeMap::new();
        let mut by_version = BTreeMap::new();
        let mut completed_versions = BTreeSet::new();
        for upload in uploads {
            let key = upload.key.clone();
            if by_key.contains_key(&key) {
                return Err(Report::new(ResourceUploadsError::DuplicateKey(key)));
            }
            let id = upload.resource_id();
            if by_version.contains_key(&id) {
                return Err(Report::new(ResourceUploadsError::DuplicateVersion(id)));
            }
            if matches!(upload.state, ResourceUploadState::Completed { .. }) {
                completed_versions.insert(id.clone());
            }
            by_version.insert(id, key.clone());
            by_key.insert(key, upload);
        }
        Ok(Self {
            by_key,
            by_version,
            completed_versions,
        })
    }

    pub fn get(&self, key: &ResourceUploadKey) -> Option<&ResourceUpload> {
        self.by_key.get(key)
    }

    pub fn get_version(&self, id: &ResourceId) -> Option<&ResourceUpload> {
        let key = self.by_version.get(id)?;
        self.by_key.get(key)
    }

    pub fn iter(
        &self,
    ) -> std::collections::btree_map::Values<'_, ResourceUploadKey, ResourceUpload> {
        self.by_key.values()
    }

    fn latest_completed_version(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
    ) -> Option<&ResourceId> {
        let first = ResourceId::new(domain.clone(), identifier.clone(), 0);
        let last = ResourceId::new(domain.clone(), identifier.clone(), u64::MAX);
        self.completed_versions.range(first..=last).next_back()
    }
}

impl Serialize for ResourceUploads {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.by_key.values())
    }
}

impl<'de> Deserialize<'de> for ResourceUploads {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let uploads = Vec::<ResourceUpload>::deserialize(deserializer)?;
        match Self::try_from_uploads(uploads) {
            Ok(uploads) => Ok(uploads),
            Err(error) => Err(serde::de::Error::custom(error)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceUploadsError {
    #[error("resource upload identity '{}' appears more than once", .0.identity)]
    DuplicateKey(ResourceUploadKey),
    #[error(
        "resource '{}@{}' in domain '{}' is assigned to more than one upload",
        .0.identifier.as_str(),
        .0.version,
        .0.domain.as_str()
    )]
    DuplicateVersion(ResourceId),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceVersionResolutionError {
    #[error(
        "resource '{}@{}' is not a completed version in domain '{}'",
        .0.identifier.as_str(),
        .0.version,
        .0.domain.as_str()
    )]
    NotCompleted(ResourceId),
    #[error(
        "resource '{}@{}' does not exist in domain '{}'",
        .0.identifier.as_str(),
        .0.version,
        .0.domain.as_str()
    )]
    DoesNotExist(ResourceId),
    #[error(
        "resource '{}' has no completed versions in domain '{}'",
        identifier.as_str(),
        domain.as_str()
    )]
    NoCompletedVersions {
        domain: DomainName,
        identifier: ResourceName,
    },
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
    pub node: ClusterNodeIdentity,
}

impl ResourceReplicaKey {
    pub fn new(
        domain: DomainName,
        identifier: ResourceName,
        version: u64,
        node: ClusterNodeIdentity,
    ) -> Self {
        Self {
            domain,
            identifier,
            version,
            node,
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
    pub source_node: Option<ClusterNodeIdentity>,
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
    pub uploads: ResourceUploads,
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
        self.uploads.get(key)
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

    /// Resolves an explicit version, or the highest completed version when `requested_version` is
    /// absent. Upload outcomes are the sole source of binding eligibility and latest selection.
    pub fn resolve_completed_version(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
        requested_version: Option<u64>,
    ) -> Result<ResourceId, Report<ResourceVersionResolutionError>> {
        let id = match requested_version {
            Some(version) => ResourceId::new(domain.clone(), identifier.clone(), version),
            None => self
                .uploads
                .latest_completed_version(domain, identifier)
                .cloned()
                .ok_or_else(|| {
                    Report::new(ResourceVersionResolutionError::NoCompletedVersions {
                        domain: domain.clone(),
                        identifier: identifier.clone(),
                    })
                })?,
        };
        let Some(upload) = self.uploads.get_version(&id) else {
            return Err(Report::new(ResourceVersionResolutionError::DoesNotExist(
                id,
            )));
        };
        if matches!(upload.state, ResourceUploadState::Completed { .. }) {
            return Ok(id);
        }
        Err(Report::new(ResourceVersionResolutionError::NotCompleted(
            id,
        )))
    }

    pub fn is_declared(&self, domain: &DomainName, identifier: &ResourceName) -> bool {
        self.next_version(domain, identifier).is_some()
    }
}
