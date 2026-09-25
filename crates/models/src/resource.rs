use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use sorted_vec::SortedVec;
use strum::{AsRefStr, EnumString, IntoStaticStr};

use crate::{
    ClusterNodeIdentity, ClusterNodeName, DomainName, NodeRef, ResourceName, Timestamp, UserName,
};

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceManifestEntry {
    pub path: String,
    pub content: ResourceEntryContent,
}

/// What one manifest entry names inside a version.
///
/// A directory has no bytes of its own, so it carries neither a size nor a checksum. A file
/// carries both, and they always describe the same bytes because they are written together.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceEntryContent {
    Directory,
    File { size: u64, checksum: String },
}

impl ResourceEntryContent {
    /// The bytes this entry contributes to its version's total. A directory contributes none.
    pub fn size(&self) -> u64 {
        match self {
            Self::Directory => 0,
            Self::File { size, .. } => *size,
        }
    }

    pub fn is_file(&self) -> bool {
        matches!(self, Self::File { .. })
    }
}

/// What `DESCRIBE RESOURCE` reports about one declared resource: every version with the entries it
/// holds, the newest completed version, and the models bound to each version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceDescription {
    pub resource: ResourceName,
    /// The newest completed version, which `VERSION LATEST` resolves to. Absent while no version
    /// has completed.
    pub latest_version: Option<NonZeroU64>,
    /// Every version of the resource, in ascending version order.
    pub versions: Vec<ResourceVersionDescription>,
    /// Every model bound to a version of the resource, ordered by model.
    pub usages: Vec<ResourceUsage>,
}

/// One version of a described resource. The description it belongs to names the resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceVersionDescription {
    pub version: NonZeroU64,
    pub root_checksum: String,
    pub manifest_checksum: String,
    pub file_count: u64,
    pub total_bytes: u64,
    pub created_at: Timestamp,
    pub created_by_node: ClusterNodeName,
    pub entries: ResourceVersionEntries,
}

/// The entries of one described version, as the store of the node that served the description
/// read them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceVersionEntries {
    /// The version's manifest, in path order.
    Listed(Vec<ResourceManifestEntry>),
    /// The serving node could not read the version's manifest.
    Unavailable { reason: String },
}

/// A model bound to one version of a described resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceUsage {
    pub node: NodeRef,
    pub version: NonZeroU64,
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

/// The resource version a binding statement names before the statement is applied.
///
/// A stored model always holds the resolved number. This is the written form a statement carries
/// until planning resolves it against the completed versions of its domain.
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
)]
pub enum RequestedResourceVersion {
    /// `VERSION <n>`: exactly this version, which has to be completed.
    Number(u64),
    /// `VERSION LATEST`: the highest completed version when the statement is applied.
    Latest,
}

impl std::fmt::Display for RequestedResourceVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Number(version) => write!(formatter, "{version}"),
            Self::Latest => formatter.write_str("LATEST"),
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

    /// The uploads that belong to `domain`, which are the only ones a binding in that domain can
    /// resolve against.
    pub fn in_domain(&self, domain: &DomainName) -> Self {
        let by_key = self
            .by_key
            .iter()
            .filter(|(key, _)| key.domain == *domain)
            .map(|(key, upload)| (key.clone(), upload.clone()))
            .collect();
        let by_version = self
            .by_version
            .iter()
            .filter(|(id, _)| id.domain == *domain)
            .map(|(id, key)| (id.clone(), key.clone()))
            .collect();
        let completed_versions = self
            .completed_versions
            .iter()
            .filter(|id| id.domain == *domain)
            .cloned()
            .collect();
        Self {
            by_key,
            by_version,
            completed_versions,
        }
    }

    /// Every completed version, ordered by domain, resource and version.
    pub fn completed_versions(&self) -> impl Iterator<Item = &ResourceId> {
        self.completed_versions.iter()
    }

    /// The completed versions of one resource in `domain`, ascending.
    pub fn completed_versions_of(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
    ) -> impl DoubleEndedIterator<Item = &ResourceId> {
        let first = ResourceId::new(domain.clone(), identifier.clone(), 0);
        let last = ResourceId::new(domain.clone(), identifier.clone(), u64::MAX);
        self.completed_versions.range(first..=last)
    }

    /// Resolves the version a binding requests in `domain`. Upload outcomes are the sole source
    /// of binding eligibility: an explicit number has to name a completed version, and `LATEST`
    /// selects the highest completed one.
    pub fn resolve_completed_version(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
        requested: RequestedResourceVersion,
    ) -> Result<ResourceId, Report<ResourceVersionResolutionError>> {
        let id = match requested {
            RequestedResourceVersion::Number(version) => {
                ResourceId::new(domain.clone(), identifier.clone(), version)
            }
            RequestedResourceVersion::Latest => {
                let Some(latest) = self.completed_versions_of(domain, identifier).next_back()
                else {
                    return Err(Report::new(
                        ResourceVersionResolutionError::NoCompletedVersions {
                            domain: domain.clone(),
                            identifier: identifier.clone(),
                        },
                    ));
                };
                latest.clone()
            }
        };
        let Some(upload) = self.get_version(&id) else {
            return Err(Report::new(ResourceVersionResolutionError::DoesNotExist(
                id,
            )));
        };
        if let ResourceUploadState::Completed { .. } = upload.state {
            return Ok(id);
        }
        Err(Report::new(ResourceVersionResolutionError::NotCompleted(
            id,
        )))
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

    /// Every published version of one resource in `domain`, ascending. The versions are sorted by
    /// domain, identifier and version, so one resource's versions are a contiguous run located by
    /// binary search.
    pub fn versions_of(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
    ) -> impl Iterator<Item = &ResourceVersion> {
        let start = self.versions.partition_point(|resource| {
            (&resource.id.domain, &resource.id.identifier) < (domain, identifier)
        });
        let end = self.versions.partition_point(|resource| {
            (&resource.id.domain, &resource.id.identifier) <= (domain, identifier)
        });
        self.versions[start..end].iter()
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

    pub fn is_declared(&self, domain: &DomainName, identifier: &ResourceName) -> bool {
        self.next_version(domain, identifier).is_some()
    }

    /// The resources declared in `domain`, in name order. The catalog is sorted by domain first,
    /// so a domain's resources are one contiguous run, located by binary search.
    pub fn resources_in<'a>(
        &'a self,
        domain: &DomainName,
    ) -> impl Iterator<Item = &'a ResourceName> + 'a {
        let start = self
            .next_version_by_resource
            .partition_point(|counter| counter.domain < *domain);
        let end = self
            .next_version_by_resource
            .partition_point(|counter| counter.domain <= *domain);
        self.next_version_by_resource[start..end]
            .iter()
            .map(|counter| &counter.identifier)
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    fn domain(raw: &str) -> DomainName {
        DomainName::parse(raw).assured("test domains are identifier-shaped literals")
    }

    fn resource(raw: &str) -> ResourceName {
        ResourceName::parse(raw).assured("test resources are identifier-shaped literals")
    }

    fn upload(
        domain: &DomainName,
        identity: &str,
        version: u64,
        state: ResourceUploadState,
    ) -> ResourceUpload {
        ResourceUpload {
            key: ResourceUploadKey::new(
                UserName::parse("default")
                    .assured("the test owner is an identifier-shaped literal"),
                domain.clone(),
                resource("model"),
                ResourceUploadIdentity::parse(identity)
                    .assured("test upload identities use accepted characters"),
            ),
            version,
            state,
        }
    }

    fn completed(root_checksum: &str) -> ResourceUploadState {
        ResourceUploadState::Completed {
            root_checksum: root_checksum.to_string(),
            outcome_revision: 1,
        }
    }

    /// Versions 1 and 3 completed, 2 failed after 3 was assigned, and 4 is still applying, all in
    /// `tenant`; `other` completed its own version 7 of a resource with the same name.
    fn uploads() -> ResourceUploads {
        let tenant = domain("tenant");
        let other = domain("other");
        ResourceUploads::try_from_uploads([
            upload(&tenant, "first", 1, completed("first-root")),
            upload(
                &tenant,
                "failed",
                2,
                ResourceUploadState::Failed {
                    root_checksum: "failed-root".to_string(),
                    outcome_revision: 3,
                    reason: "installation failed".to_string(),
                },
            ),
            upload(&tenant, "third", 3, completed("third-root")),
            upload(
                &tenant,
                "applying",
                4,
                ResourceUploadState::Applying {
                    root_checksum: "applying-root".to_string(),
                },
            ),
            upload(&other, "elsewhere", 7, completed("elsewhere-root")),
        ])
        .assured("the test uploads have unique identities and versions")
    }

    #[test]
    fn latest_resolves_to_the_highest_completed_version() {
        let resolved = uploads()
            .resolve_completed_version(
                &domain("tenant"),
                &resource("model"),
                RequestedResourceVersion::Latest,
            )
            .assured("the tenant has completed versions");
        assert_eq!(
            resolved,
            ResourceId::new(domain("tenant"), resource("model"), 3)
        );
    }

    #[test]
    fn a_number_resolves_only_to_a_completed_version_of_its_domain() {
        let uploads = uploads();
        let tenant = domain("tenant");
        let model = resource("model");

        let first = uploads
            .resolve_completed_version(&tenant, &model, RequestedResourceVersion::Number(1))
            .assured("version 1 completed");
        assert_eq!(first.version, 1);

        for incomplete in [2, 4] {
            let error = match uploads.resolve_completed_version(
                &tenant,
                &model,
                RequestedResourceVersion::Number(incomplete),
            ) {
                Ok(id) => panic!("version {incomplete} must not resolve, got {id:?}"),
                Err(error) => error,
            };
            assert_eq!(
                error.current_context().to_string(),
                format!(
                    "resource 'model@{incomplete}' is not a completed version in domain 'tenant'"
                )
            );
        }

        let unknown = match uploads.resolve_completed_version(
            &tenant,
            &model,
            RequestedResourceVersion::Number(7),
        ) {
            Ok(id) => panic!("another domain's version must not resolve, got {id:?}"),
            Err(error) => error,
        };
        assert_eq!(
            unknown.current_context().to_string(),
            "resource 'model@7' does not exist in domain 'tenant'"
        );
    }

    #[test]
    fn latest_without_a_completed_version_is_rejected() {
        let error = match uploads().resolve_completed_version(
            &domain("tenant"),
            &resource("weights"),
            RequestedResourceVersion::Latest,
        ) {
            Ok(id) => panic!("a resource without uploads must not resolve, got {id:?}"),
            Err(error) => error,
        };
        assert_eq!(
            error.current_context().to_string(),
            "resource 'weights' has no completed versions in domain 'tenant'"
        );
    }

    #[test]
    fn domain_uploads_keep_only_that_domain_and_its_completed_versions() {
        let tenant = domain("tenant");
        let scoped = uploads().in_domain(&tenant);

        let completed = scoped
            .completed_versions()
            .map(|id| id.version)
            .collect::<Vec<_>>();
        assert_eq!(completed, vec![1, 3]);
        assert!(
            scoped
                .resolve_completed_version(
                    &domain("other"),
                    &resource("model"),
                    RequestedResourceVersion::Latest
                )
                .is_err(),
            "uploads scoped to one domain resolve nothing in another"
        );
        let applying = scoped
            .get_version(&ResourceId::new(tenant, resource("model"), 4))
            .assured("the applying upload stays in its domain");
        assert!(matches!(
            applying.state,
            ResourceUploadState::Applying { .. }
        ));
    }

    #[test]
    fn the_versions_of_one_resource_exclude_other_resources_and_domains() {
        let version = |domain_name: &str, resource_name: &str, number: u64| ResourceVersion {
            id: ResourceId::new(domain(domain_name), resource(resource_name), number),
            root_checksum: format!("root-{number}"),
            manifest_checksum: format!("manifest-{number}"),
            file_count: 1,
            total_bytes: 1,
            archive_bytes: 1,
            created_at: Timestamp::from_unix_nanos(0),
            created_by_node: ClusterNodeName::parse("node-1").assured("a literal node name"),
        };
        let status = ResourceVersionStatus {
            versions: SortedVec::from_unsorted(vec![
                version("tenant", "zeta", 1),
                version("tenant", "model", 2),
                version("other", "model", 1),
                version("tenant", "alpha", 1),
                version("tenant", "model", 1),
                version("zulu", "model", 3),
            ]),
            ..ResourceVersionStatus::default()
        };

        let described = status
            .versions_of(&domain("tenant"), &resource("model"))
            .map(|resource| resource.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            described,
            vec![
                ResourceId::new(domain("tenant"), resource("model"), 1),
                ResourceId::new(domain("tenant"), resource("model"), 2),
            ]
        );
        assert_eq!(
            status
                .versions_of(&domain("tenant"), &resource("missing"))
                .count(),
            0
        );
    }
}
