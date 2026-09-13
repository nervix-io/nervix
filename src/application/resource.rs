//! Uploading a resource archive and making every node hold a copy.
//!
//! Layer: control plane.
//!
//! - **Owns.** Upload staging, installation into the resource store, cluster replication, and the
//!   readiness a caller waits for.
//! - **Depends on.** The resource store for bytes and the interconnect to publish and fetch them.
//! - **Must not know.** What a model does with the resource once it is installed.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc as StdArc,
};

use ahash::HashMap;
use error_stack::{Report, ResultExt};
use futures_util::{StreamExt, stream};
use nervix_interconnect::Transport;
use nervix_models::{
    ClusterNodeName, CreateResource, CreateStatement, DomainName, ModelName, ResourceId,
    ResourceName, ResourceNodeState, ResourceNodeStatus, ResourceReplicaKey, ResourceUploadKey,
    ResourceUploadState, UploadResource,
};
use thiserror::Error;
use tokio::{
    sync::Mutex as AsyncMutex,
    time::{Duration, sleep_until},
};

use super::{
    domain_clock::current_timestamp,
    model_mutation::{command_error, command_ok, command_ok_already_existed},
    session_service::SessionServiceImpl,
};
use crate::{
    proto::CommandResult,
    resource::{ResourceStore, StagedResourceArchive},
    resource_interconnect::{FetchResourceArchive, PublishResourceReplica},
};

const MAX_CONCURRENT_RESOURCE_REPLICATIONS: usize = 4;

#[derive(Debug, Error)]
pub(in crate::application) enum ResourceUploadError {
    #[error("failed to begin upload for resource '{identifier}'")]
    BeginUpload { identifier: ModelName },
    #[error("failed to install resource '{}@{}'", .id.identifier.as_str(), .id.version)]
    InstallArchive { id: ResourceId },
    #[error("failed to publish resource '{}@{}'", .id.identifier.as_str(), .id.version)]
    Publish { id: ResourceId },
    #[error(
        "resource upload identity '{}' already published version {} with digest {}, not {}",
        .key.identity,
        .version,
        .published_checksum,
        .received_checksum
    )]
    DigestConflict {
        key: ResourceUploadKey,
        version: u64,
        published_checksum: String,
        received_checksum: String,
    },
}

impl ResourceUploadError {
    pub(in crate::application) fn assigned_version(&self) -> Option<u64> {
        match self {
            Self::BeginUpload { .. } => None,
            Self::InstallArchive { id } | Self::Publish { id } => Some(id.version),
            Self::DigestConflict { version, .. } => Some(*version),
        }
    }
}

pub(in crate::application) struct ResourcePublication {
    pub(in crate::application) version: u64,
    pub(in crate::application) cluster_ready: bool,
}

struct ResourceReplication {
    resource: nervix_models::ResourceVersion,
    source_node_id: ClusterNodeName,
    local_key: ResourceReplicaKey,
}

async fn fetch_resource_archive(
    interconnect: &Transport,
    resource_store: &ResourceStore,
    source_node: &ClusterNodeName,
    resource: &nervix_models::ResourceVersion,
) -> Result<StagedResourceArchive, String> {
    resource_store
        .validate_archive_bytes(resource.archive_bytes)
        .map_err(|error| error.to_string())?;
    let mut archive = interconnect
        .request_stream(
            source_node,
            FetchResourceArchive {
                id: resource.id.clone(),
            },
        )
        .await
        .map_err(|error| format!("resource fetch request failed: {error}"))?;
    if archive.content_length() != resource.archive_bytes {
        return Err(format!(
            "resource archive size mismatch: expected {}, source declared {}",
            resource.archive_bytes,
            archive.content_length()
        ));
    }
    let mut staged = resource_store
        .create_archive_stager()
        .await
        .map_err(|error| format!("failed to create temporary resource archive: {error}"))?;
    let mut received = 0_u64;
    while let Some(chunk) = archive
        .next_chunk()
        .await
        .map_err(|error| format!("resource fetch failed: {error}"))?
    {
        tokio::task::consume_budget().await;
        let chunk_bytes = u64::try_from(chunk.len())
            .map_err(|error| format!("resource chunk length is invalid: {error}"))?;
        let next_received = received
            .checked_add(chunk_bytes)
            .ok_or_else(|| "resource archive offset overflowed".to_string())?;
        if next_received > resource.archive_bytes {
            return Err(format!(
                "resource archive size exceeds published size {}",
                resource.archive_bytes
            ));
        }
        resource_store
            .validate_archive_bytes(next_received)
            .map_err(|error| error.to_string())?;
        staged
            .write_chunk(chunk)
            .await
            .map_err(|error| format!("failed to write temporary resource archive: {error}"))?;
        received = next_received;
    }
    if received != resource.archive_bytes {
        return Err(format!(
            "resource archive size mismatch: expected {}, received {received}",
            resource.archive_bytes
        ));
    }
    let staged = staged
        .finish()
        .await
        .map_err(|error| format!("failed to flush temporary resource archive: {error}"))?;
    if staged.archive_bytes() != received {
        return Err(format!(
            "resource archive staging size mismatch: received {received}, wrote {}",
            staged.archive_bytes()
        ));
    }
    Ok(staged)
}

/// Resolves a model's resource reference to the concrete version it binds to. Resources are
/// domain-owned, so a name only resolves against versions published in the referencing domain.
pub(in crate::application) fn resolve_resource_id(
    resources: &nervix_models::ResourceVersionStatus,
    domain: &DomainName,
    identifier: &ResourceName,
    requested_version: Option<u64>,
) -> Result<ResourceId, String> {
    if let Some(version) = requested_version {
        let id = ResourceId::new(domain.clone(), identifier.clone(), version);
        if resources.version(&id).is_some() {
            return Ok(id);
        }
        return Err(format!(
            "resource '{}@{}' does not exist in domain '{}'",
            identifier.as_str(),
            version,
            domain.as_str()
        ));
    }

    let Some(version) = resources.latest_version(domain, identifier) else {
        return Err(format!(
            "resource '{}' has no published versions in domain '{}'",
            identifier.as_str(),
            domain.as_str()
        ));
    };
    Ok(ResourceId::new(domain.clone(), identifier.clone(), version))
}

pub(in crate::application) fn resource_ref_suggestions(
    resources: &nervix_models::ResourceVersionStatus,
    domain: &DomainName,
    prefix: &str,
) -> Vec<String> {
    let mut suggestions = Vec::new();
    for counter in &resources.next_version_by_resource {
        if counter.domain == *domain
            && (prefix.is_empty() || counter.identifier.as_str().starts_with(prefix))
        {
            suggestions.push(counter.identifier.to_string());
        }
    }
    suggestions
}

pub(in crate::application) fn resource_version_suggestions(
    resources: &nervix_models::ResourceVersionStatus,
    domain: &DomainName,
    identifier: &ResourceName,
    prefix: &str,
) -> Vec<String> {
    let mut suggestions = Vec::new();
    for resource in &resources.versions {
        if resource.id.domain != *domain || resource.id.identifier != *identifier {
            continue;
        }
        let version = resource.id.version.to_string();
        if prefix.is_empty() || version.starts_with(prefix) {
            suggestions.push(version);
        }
    }
    suggestions
}

pub(in crate::application) fn requested_resource_versions(
    input: &str,
    cursor: usize,
) -> Option<ResourceName> {
    let safe_cursor = cursor.min(input.len());
    let raw_prefix = &input[..safe_cursor];
    let upper = raw_prefix.to_ascii_uppercase();
    let version_index = upper.find(" VERSION ")?;
    let before_version = raw_prefix[..version_index].trim_end();
    let resource_prefix = "DESCRIBE RESOURCE ";
    if !before_version
        .to_ascii_uppercase()
        .starts_with(resource_prefix)
    {
        return None;
    }
    let identifier = before_version[resource_prefix.len()..].trim();
    if identifier.is_empty() {
        return None;
    }
    ResourceName::parse(identifier).ok()
}

impl SessionServiceImpl {
    async fn publish_resource_replica(&self, replica: ResourceNodeStatus) -> Result<(), String> {
        let Some(leader_id) = self.inner.consensus.current_leader().await else {
            return Err(
                "failed to publish resource replica: cluster leader is unknown".to_string(),
            );
        };

        if leader_id == self.inner.consensus.local_node_id().clone() {
            return self
                .inner
                .consensus
                .put_resource_replica(replica)
                .await
                .map_err(|error| format!("failed to publish resource replica: {error}"));
        }

        self.inner
            .interconnect
            .request(&leader_id, PublishResourceReplica { replica })
            .await
            .map_err(|error| format!("failed to publish resource replica: {error}"))?
            .map_err(|error| format!("failed to publish resource replica: {error}"))
    }

    pub(in crate::application) async fn reconcile_resources_once(&self) {
        let local_node_id = self.inner.consensus.local_node_id().clone();
        let resources = self.inner.consensus.current_resources().await;
        let gossip = self.inner.cluster.availability_state().await;
        let live_node_ids = gossip
            .live_identities()
            .into_iter()
            .map(|identity| identity.node_id().clone())
            .collect::<BTreeSet<_>>();

        // Replicas indexed by the resource version they hold and then by the node holding it. The
        // loop below asks about one resource on one node at a time, so both questions resolve by
        // key instead of scanning every replica for every resource and every live node.
        let mut replicas_by_resource: HashMap<
            ResourceId,
            BTreeMap<ClusterNodeName, ResourceNodeStatus>,
        > = HashMap::default();
        for replica in resources.replicas.iter() {
            replicas_by_resource
                .entry(replica.key.version_key().resource_id())
                .or_default()
                .insert(replica.key.node_id.clone(), replica.clone());
        }

        let mut missing = Vec::new();
        for resource in resources.versions.iter().cloned() {
            tokio::task::consume_budget().await;
            let local_key = ResourceReplicaKey::new(
                resource.id.domain.clone(),
                resource.id.identifier.clone(),
                resource.id.version,
                local_node_id.clone(),
            );
            let resource_replicas = replicas_by_resource.get(&resource.id);
            let holds_current_resource = |node_id: &ClusterNodeName| {
                resource_replicas.is_some_and(|replicas| {
                    replicas.get(node_id).is_some_and(|replica| {
                        replica.state == ResourceNodeState::Ready
                            && replica.root_checksum.as_deref()
                                == Some(resource.root_checksum.as_str())
                    })
                })
            };
            if holds_current_resource(&local_node_id) {
                continue;
            }
            let Some(source_node_id) = resource_replicas.and_then(|replicas| {
                replicas.iter().find_map(|(node_id, replica)| {
                    (node_id != &local_node_id
                        && live_node_ids.contains(node_id)
                        && replica.state == ResourceNodeState::Ready
                        && replica.root_checksum.as_deref()
                            == Some(resource.root_checksum.as_str()))
                    .then(|| node_id.clone())
                })
            }) else {
                continue;
            };
            missing.push(ResourceReplication {
                resource,
                source_node_id,
                local_key,
            });
        }

        stream::iter(missing)
            .for_each_concurrent(
                MAX_CONCURRENT_RESOURCE_REPLICATIONS,
                |replication| async move {
                    self.replicate_resource(replication).await;
                },
            )
            .await;
    }

    async fn replicate_resource(&self, replication: ResourceReplication) {
        let ResourceReplication {
            resource,
            source_node_id,
            local_key,
        } = replication;
        let execution = self
            .inner
            .resource_replication_executions
            .entry(resource.id.clone())
            .or_insert_with(|| StdArc::new(AsyncMutex::new(())))
            .clone();
        let _execution_guard = execution.lock().await;
        let current = self.inner.consensus.current_resources().await;
        if current.replicas.iter().any(|replica| {
            replica.key == local_key
                && replica.state == ResourceNodeState::Ready
                && replica.root_checksum.as_deref() == Some(resource.root_checksum.as_str())
        }) {
            return;
        }
        let failed_replica = |root_checksum: Option<String>, error: String| ResourceNodeStatus {
            key: local_key.clone(),
            state: ResourceNodeState::Failed,
            root_checksum,
            last_verified_at: None,
            source_node_id: Some(source_node_id.clone()),
            error: Some(error),
        };

        if let Err(error) = self
            .publish_resource_replica(ResourceNodeStatus {
                key: local_key.clone(),
                state: ResourceNodeState::Pending,
                root_checksum: None,
                last_verified_at: None,
                source_node_id: Some(source_node_id.clone()),
                error: None,
            })
            .await
        {
            self.broadcast_error(error);
            return;
        }

        let archive = match fetch_resource_archive(
            &self.inner.interconnect,
            &self.inner.resource_store,
            &source_node_id,
            &resource,
        )
        .await
        {
            Ok(archive) => archive,
            Err(error) => {
                if let Err(publish_error) = self
                    .publish_resource_replica(failed_replica(None, error))
                    .await
                {
                    self.broadcast_error(publish_error);
                }
                return;
            }
        };

        if archive.root_checksum() != resource.root_checksum {
            if let Err(error) = self
                .publish_resource_replica(failed_replica(
                    Some(archive.root_checksum().to_string()),
                    format!(
                        "resource checksum mismatch: expected {}, got {}",
                        resource.root_checksum,
                        archive.root_checksum()
                    ),
                ))
                .await
            {
                self.broadcast_error(error);
            }
            return;
        }

        let manifest = match self
            .inner
            .resource_store
            .install_replica_from_archive_path(resource.clone(), archive.path())
            .await
        {
            Ok(manifest) => manifest,
            Err(error) => {
                if let Err(publish_error) = self
                    .publish_resource_replica(failed_replica(None, error.to_string()))
                    .await
                {
                    self.broadcast_error(publish_error);
                }
                return;
            }
        };

        if let Err(error) = self
            .publish_resource_replica(ResourceNodeStatus {
                key: local_key,
                state: ResourceNodeState::Ready,
                root_checksum: Some(manifest.resource.root_checksum),
                last_verified_at: Some(current_timestamp()),
                source_node_id: Some(source_node_id),
                error: None,
            })
            .await
        {
            self.broadcast_error(error);
        } else if let Err(error) = self.refresh_http_tls_server_config().await {
            self.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
        }
    }

    pub(in crate::application) async fn create_resource(
        &self,
        domain: &DomainName,
        create: CreateStatement<CreateResource>,
    ) -> CommandResult {
        let resources = self.inner.consensus.current_resources().await;
        if resources.is_declared(domain, &create.identifier) {
            if create.if_not_exists {
                return command_ok_already_existed(format!(
                    "resource '{}' already exists",
                    create.identifier.as_str()
                ));
            }
            return command_error(format!(
                "resource '{}' already exists",
                create.identifier.as_str()
            ));
        }
        match self
            .inner
            .consensus
            .create_resource_catalog(domain, &create.identifier)
            .await
        {
            Ok(()) => command_ok(format!("created resource '{}'", create.identifier.as_str())),
            Err(error) => {
                self.consensus_error_response(
                    &error,
                    format!(
                        "failed to create resource '{}': {error}",
                        create.identifier.as_str()
                    ),
                )
                .await
            }
        }
    }

    pub(in crate::application) async fn upload_resource_command(
        &self,
        upload: UploadResource,
    ) -> CommandResult {
        command_error(format!(
            "UPLOAD RESOURCE '{}' must be executed by a client that supports local uploads from \
             '{}'",
            upload.identifier.as_str(),
            upload.source_path
        ))
    }

    pub(in crate::application) async fn install_uploaded_resource_archive(
        &self,
        key: ResourceUploadKey,
        archive_path: &Path,
        root_checksum: String,
    ) -> Result<ResourcePublication, Report<ResourceUploadError>> {
        let execution = self
            .inner
            .resource_upload_executions
            .entry(key.clone())
            .or_insert_with(|| StdArc::new(AsyncMutex::new(())))
            .clone();
        let _execution_guard = execution.lock().await;
        let identifier = ModelName::from(&key.identifier);
        let created_at = current_timestamp();
        let upload = self
            .inner
            .consensus
            .begin_resource_upload(key.clone())
            .await
            .change_context(ResourceUploadError::BeginUpload {
                identifier: identifier.clone(),
            })?;
        if let ResourceUploadState::Published {
            root_checksum: published_checksum,
        } = upload.state
        {
            if published_checksum != root_checksum {
                return Err(Report::new(ResourceUploadError::DigestConflict {
                    key,
                    version: upload.version,
                    published_checksum,
                    received_checksum: root_checksum,
                }));
            }
            let id = ResourceId::new(key.domain, key.identifier, upload.version);
            return Ok(ResourcePublication {
                version: upload.version,
                cluster_ready: self.resource_cluster_ready(&id).await,
            });
        }
        let id = ResourceId::new(key.domain.clone(), key.identifier.clone(), upload.version);
        let manifest = self
            .inner
            .resource_store
            .install_from_archive_path(
                id.clone(),
                archive_path,
                root_checksum,
                self.inner.consensus.local_node_id().clone(),
                created_at,
            )
            .await
            .change_context(ResourceUploadError::InstallArchive { id: id.clone() })?;
        let replica = ResourceNodeStatus {
            key: ResourceReplicaKey::new(
                manifest.resource.id.domain.clone(),
                manifest.resource.id.identifier.clone(),
                manifest.resource.id.version,
                self.inner.consensus.local_node_id().clone(),
            ),
            state: ResourceNodeState::Ready,
            root_checksum: Some(manifest.resource.root_checksum.clone()),
            last_verified_at: Some(created_at),
            source_node_id: Some(self.inner.consensus.local_node_id().clone()),
            error: None,
        };
        self.inner
            .consensus
            .publish_resource_upload(key, manifest.resource.clone(), replica)
            .await
            .change_context(ResourceUploadError::Publish {
                id: manifest.resource.id.clone(),
            })?;
        self.inner
            .runtime
            .sync_resource_versions(&self.inner.consensus.current_resources().await);
        if let Err(error) = self.refresh_http_tls_server_config().await {
            self.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
        }
        Ok(ResourcePublication {
            version: manifest.resource.id.version,
            cluster_ready: self.resource_cluster_ready(&manifest.resource.id).await,
        })
    }

    async fn resource_cluster_ready(&self, id: &ResourceId) -> bool {
        let resources = self.inner.consensus.current_resources().await;
        let replicas = resources
            .replicas
            .iter()
            .filter(|replica| replica.key.version_key().resource_id() == *id)
            .collect::<Vec<_>>();
        let gossip = self.inner.cluster.availability_state().await;
        let live_node_ids = gossip
            .live_identities()
            .into_iter()
            .map(|identity| identity.node_id().clone())
            .collect::<BTreeSet<_>>();
        !live_node_ids.is_empty()
            && live_node_ids.iter().all(|node_id| {
                replicas.iter().any(|replica| {
                    replica.key.node_id == *node_id && replica.state == ResourceNodeState::Ready
                })
            })
    }

    pub(in crate::application) async fn wait_for_resource_cluster_ready(
        &self,
        id: &ResourceId,
        deadline: tokio::time::Instant,
    ) -> bool {
        if self.resource_cluster_ready(id).await {
            return true;
        }
        loop {
            tokio::task::consume_budget().await;
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let next_poll = match now.checked_add(Duration::from_millis(100)) {
                Some(next_poll) => next_poll.min(deadline),
                None => deadline,
            };
            sleep_until(next_poll).await;
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            if self.resource_cluster_ready(id).await {
                return true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        ClusterNodeName, CreateResource, CreateStatement, DomainName, ResourceVersion,
        ResourceVersionCounter, ResourceVersionStatus, Timestamp,
    };
    use nervix_nspl::client_statement::upload_resource_path_fragment;
    use sorted_vec::SortedVec;

    use super::{
        super::test_fixtures::{TestService, build_test_service, create_test_domain, named},
        *,
    };

    #[test]
    fn resource_ref_suggestions_expand_known_resource_names_in_the_active_domain() {
        let tenant = DomainName::parse("tenant").expect("valid domain");
        let other = DomainName::parse("other").expect("valid domain");
        let resources = ResourceVersionStatus {
            next_version_by_resource: SortedVec::from_unsorted(vec![
                ResourceVersionCounter {
                    domain: tenant.clone(),
                    identifier: named("fraud_model"),
                    next_version: 2,
                },
                ResourceVersionCounter {
                    domain: tenant.clone(),
                    identifier: named("proto"),
                    next_version: 1,
                },
                ResourceVersionCounter {
                    domain: other.clone(),
                    identifier: named("promo_model"),
                    next_version: 1,
                },
            ]),
            ..Default::default()
        };

        assert_eq!(
            resource_ref_suggestions(&resources, &tenant, "pr"),
            vec!["proto".to_string()]
        );
        assert_eq!(
            resource_ref_suggestions(&resources, &tenant, ""),
            vec!["fraud_model".to_string(), "proto".to_string()]
        );
        assert_eq!(
            resource_ref_suggestions(&resources, &other, ""),
            vec!["promo_model".to_string()]
        );
    }

    #[test]
    fn resource_version_suggestions_expand_known_versions() {
        let tenant = DomainName::parse("tenant").expect("valid domain");
        let other = DomainName::parse("other").expect("valid domain");
        let resources = ResourceVersionStatus {
            versions: SortedVec::from_unsorted(vec![
                ResourceVersion {
                    id: nervix_models::ResourceId::new(tenant.clone(), named("proto"), 1),
                    root_checksum: "a".to_string(),
                    manifest_checksum: "a".to_string(),
                    file_count: 1,
                    total_bytes: 1,
                    archive_bytes: 2048,
                    created_at: Timestamp::from_unix_nanos(1),
                    created_by_node: ClusterNodeName::parse("node-1").expect("valid name"),
                },
                ResourceVersion {
                    id: nervix_models::ResourceId::new(tenant.clone(), named("proto"), 12),
                    root_checksum: "b".to_string(),
                    manifest_checksum: "b".to_string(),
                    file_count: 1,
                    total_bytes: 1,
                    archive_bytes: 2048,
                    created_at: Timestamp::from_unix_nanos(1),
                    created_by_node: ClusterNodeName::parse("node-1").expect("valid name"),
                },
            ]),
            ..Default::default()
        };

        assert_eq!(
            resource_version_suggestions(&resources, &tenant, &named("proto"), ""),
            vec!["1".to_string(), "12".to_string()]
        );
        assert_eq!(
            resource_version_suggestions(&resources, &tenant, &named("proto"), "1"),
            vec!["1".to_string(), "12".to_string()]
        );
        assert!(
            resource_version_suggestions(&resources, &other, &named("proto"), "").is_empty(),
            "another domain must not see this domain's resource versions"
        );
        assert_eq!(
            requested_resource_versions(
                "DESCRIBE RESOURCE proto VERSION ",
                "DESCRIBE RESOURCE proto VERSION ".len()
            ),
            Some(named("proto"))
        );
    }

    #[test]
    fn upload_resource_path_fragment_is_detected_for_upload_resource_path() {
        assert_eq!(
            upload_resource_path_fragment(
                "UPLOAD RESOURCE proto VERSION '/tmp/pro",
                "UPLOAD RESOURCE proto VERSION '/tmp/pro".len(),
            ),
            Some("/tmp/pro")
        );
        assert_eq!(
            upload_resource_path_fragment(
                "UPLOAD RESOURCE proto VERSION ",
                "UPLOAD RESOURCE proto VERSION ".len(),
            ),
            Some("")
        );
        assert_eq!(
            upload_resource_path_fragment(
                "UPLOAD RESOURCE proto VERSION ",
                "UPLOAD RESOURCE proto VERSION '".len(),
            ),
            Some("")
        );
        assert_eq!(
            upload_resource_path_fragment(
                "DESCRIBE RESOURCE proto VERSION ",
                "DESCRIBE RESOURCE proto VERSION ".len(),
            ),
            None
        );
    }

    #[tokio::test]
    async fn create_resource_if_not_exists_returns_already_existed() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let default = DomainName::parse("default").expect("valid domain");
        create_test_domain(&service.inner.consensus, "other").await;
        let other = DomainName::parse("other").expect("valid domain");

        let first = service
            .create_resource(
                &default,
                CreateStatement::new(
                    CreateResource {
                        identifier: named("fraud_model"),
                    },
                    false,
                ),
            )
            .await;
        assert!(first.success);
        assert!(!first.already_existed);

        let duplicate = service
            .create_resource(
                &default,
                CreateStatement::new(
                    CreateResource {
                        identifier: named("fraud_model"),
                    },
                    true,
                ),
            )
            .await;
        assert!(duplicate.success);
        assert!(duplicate.already_existed);
        assert!(duplicate.message.contains("already exists"));

        let same_name_other_domain = service
            .create_resource(
                &other,
                CreateStatement::new(
                    CreateResource {
                        identifier: named("fraud_model"),
                    },
                    false,
                ),
            )
            .await;
        assert!(
            same_name_other_domain.success,
            "resources are domain-owned: {}",
            same_name_other_domain.message
        );
        assert!(!same_name_other_domain.already_existed);

        let _ = std::fs::remove_dir_all(&path);
    }
}
