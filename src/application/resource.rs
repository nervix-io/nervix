//! Uploading a resource archive and making every node hold a copy.
//!
//! Layer: control plane.
//!
//! - **Owns.** Upload staging, installation into the resource store, cluster replication, and the
//!   readiness a caller waits for.
//! - **Depends on.** The resource store for bytes and the interconnect to publish and fetch them.
//! - **Must not know.** What a model does with the resource once it is installed.

use std::{collections::BTreeMap, path::Path, sync::Arc as StdArc};

use ahash::HashMap;
use error_stack::{Report, ResultExt};
use futures_util::{StreamExt, stream};
use nervix_interconnect::Transport;
use nervix_models::{
    ClusterNodeIdentity, CreateResource, CreateStatement, DomainName, ModelName, ResourceId,
    ResourceName, ResourceNodeState, ResourceNodeStatus, ResourceReplicaKey, ResourceUploadKey,
    ResourceUploadState, UploadResource,
};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

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
        "resource upload identity '{}' is already assigned version {} with digest {}, not {}",
        .key.identity,
        .version,
        .expected_checksum,
        .received_checksum
    )]
    DigestConflict {
        key: ResourceUploadKey,
        version: u64,
        expected_checksum: String,
        received_checksum: String,
    },
    #[error(
        "resource '{}@{}' failed to become usable on node '{}': {reason}",
        .id.identifier.as_str(),
        .id.version,
        .node
    )]
    NodeInstallation {
        id: ResourceId,
        node: ClusterNodeIdentity,
        reason: String,
    },
    #[error(
        "resource upload identity '{}' for version {} failed: {}",
        .key.identity,
        .version,
        .reason
    )]
    Terminal {
        key: ResourceUploadKey,
        version: u64,
        reason: String,
    },
}

impl ResourceUploadError {
    pub(in crate::application) fn assigned_version(&self) -> Option<u64> {
        match self {
            Self::BeginUpload { .. } => None,
            Self::InstallArchive { id } | Self::Publish { id } => Some(id.version),
            Self::DigestConflict { version, .. } => Some(*version),
            Self::NodeInstallation { id, .. } => Some(id.version),
            Self::Terminal { version, .. } => Some(*version),
        }
    }
}

pub(in crate::application) struct ResourceInstallation {
    pub(in crate::application) version: u64,
}

struct ResourceReplication {
    resource: nervix_models::ResourceVersion,
    source_node: ClusterNodeIdentity,
    local_key: ResourceReplicaKey,
}

async fn fetch_resource_archive(
    interconnect: &Transport,
    resource_store: &ResourceStore,
    source_node: &ClusterNodeIdentity,
    resource: &nervix_models::ResourceVersion,
) -> Result<StagedResourceArchive, String> {
    resource_store
        .validate_archive_bytes(resource.archive_bytes)
        .map_err(|error| error.to_string())?;
    let mut archive = interconnect
        .request_stream(
            source_node.node_id(),
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
    resources
        .resolve_completed_version(domain, identifier, requested_version)
        .map_err(|error| error.to_string())
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
        let local_node = ClusterNodeIdentity::new(
            local_node_id.clone(),
            self.inner.cluster.local_incarnation(),
        );
        let resources = self.inner.consensus.current_resources().await;
        let gossip = self.inner.cluster.availability_state().await;
        let live_nodes = gossip.live_identities();

        // Replicas indexed by the resource version they hold and then by the node holding it. The
        // loop below asks about one resource on one node at a time, so both questions resolve by
        // key instead of scanning every replica for every resource and every live node.
        let mut replicas_by_resource: HashMap<
            ResourceId,
            BTreeMap<ClusterNodeIdentity, ResourceNodeStatus>,
        > = HashMap::default();
        for replica in resources.replicas.iter() {
            replicas_by_resource
                .entry(replica.key.version_key().resource_id())
                .or_default()
                .insert(replica.key.node.clone(), replica.clone());
        }

        let mut missing = Vec::new();
        for resource in resources.versions.iter().cloned() {
            tokio::task::consume_budget().await;
            let local_key = ResourceReplicaKey::new(
                resource.id.domain.clone(),
                resource.id.identifier.clone(),
                resource.id.version,
                local_node.clone(),
            );
            let resource_replicas = replicas_by_resource.get(&resource.id);
            let holds_current_resource = |node: &ClusterNodeIdentity| {
                resource_replicas.is_some_and(|replicas| {
                    replicas.get(node).is_some_and(|replica| {
                        replica.state == ResourceNodeState::Ready
                            && replica.root_checksum.as_deref()
                                == Some(resource.root_checksum.as_str())
                    })
                })
            };
            if holds_current_resource(&local_node) {
                continue;
            }
            let local_manifest = self.inner.resource_store.read_manifest(&resource.id).await;
            let local_error = match local_manifest {
                Ok(manifest) if manifest.resource == resource => {
                    let replica = ResourceNodeStatus {
                        key: local_key,
                        state: ResourceNodeState::Ready,
                        root_checksum: Some(manifest.resource.root_checksum),
                        last_verified_at: Some(current_timestamp()),
                        source_node: Some(local_node.clone()),
                        error: None,
                    };
                    if let Err(error) = self.publish_resource_replica(replica).await {
                        self.broadcast_error(error);
                    }
                    continue;
                }
                Ok(manifest) => format!(
                    "installed manifest for resource '{}@{}' does not match its durable version; \
                     found root checksum {}",
                    resource.id.identifier.as_str(),
                    resource.id.version,
                    manifest.resource.root_checksum,
                ),
                Err(error) => format!(
                    "failed to read the installed manifest for resource '{}@{}': {error}",
                    resource.id.identifier.as_str(),
                    resource.id.version,
                ),
            };
            let Some(source_node) = resource_replicas.and_then(|replicas| {
                replicas.iter().find_map(|(node, replica)| {
                    (node != &local_node
                        && live_nodes.contains(node)
                        && replica.state == ResourceNodeState::Ready
                        && replica.root_checksum.as_deref()
                            == Some(resource.root_checksum.as_str()))
                    .then(|| node.clone())
                })
            }) else {
                let failed = ResourceNodeStatus {
                    key: local_key,
                    state: ResourceNodeState::Failed,
                    root_checksum: None,
                    last_verified_at: None,
                    source_node: None,
                    error: Some(local_error),
                };
                let already_reported = match resource_replicas {
                    Some(replicas) => match replicas.get(&local_node) {
                        Some(replica) => replica == &failed,
                        None => false,
                    },
                    None => false,
                };
                if !already_reported && let Err(error) = self.publish_resource_replica(failed).await
                {
                    self.broadcast_error(error);
                }
                continue;
            };
            missing.push(ResourceReplication {
                resource,
                source_node,
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

        if self.inner.consensus.current_leader().await.as_ref()
            == Some(self.inner.consensus.local_node_id())
        {
            for upload in resources.uploads.iter() {
                tokio::task::consume_budget().await;
                if !matches!(upload.state, ResourceUploadState::Applying { .. }) {
                    continue;
                }
                let execution = self
                    .inner
                    .resource_upload_executions
                    .entry(upload.key.clone())
                    .or_insert_with(|| StdArc::new(AsyncMutex::new(())))
                    .clone();
                let Ok(execution_guard) = execution.try_lock_owned() else {
                    continue;
                };
                let service = self.clone();
                let upload = upload.clone();
                self.inner.service_tasks.spawn(async move {
                    service
                        .recover_resource_upload(upload, execution_guard)
                        .await;
                });
            }
        }
    }

    async fn recover_resource_upload(
        &self,
        upload: nervix_models::ResourceUpload,
        _execution_guard: OwnedMutexGuard<()>,
    ) {
        let id = ResourceId::new(
            upload.key.domain.clone(),
            upload.key.identifier.clone(),
            upload.version,
        );
        let resources = self.inner.consensus.current_resources().await;
        let outcome = if resources.version(&id).is_none() {
            Err(
                "the admitted archive is unavailable before durable version installation"
                    .to_string(),
            )
        } else {
            self.wait_for_resource_completion(&id)
                .await
                .map_err(|error| error.to_string())
        };
        match outcome {
            Ok(()) => {
                if let Err(error) = self
                    .inner
                    .consensus
                    .complete_resource_upload(upload.key.clone())
                    .await
                {
                    self.broadcast_error(format!(
                        "failed to record completion of resource upload '{}': {error}",
                        upload.key.identity
                    ));
                    return;
                }
            }
            Err(reason) => {
                if let Err(error) = self
                    .inner
                    .consensus
                    .fail_resource_upload(upload.key.clone(), reason)
                    .await
                {
                    self.broadcast_error(format!(
                        "failed to record failure of resource upload '{}': {error}",
                        upload.key.identity
                    ));
                    return;
                }
            }
        }
        self.inner.resource_upload_executions.remove(&upload.key);
    }

    async fn replicate_resource(&self, replication: ResourceReplication) {
        let ResourceReplication {
            resource,
            source_node,
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
            source_node: Some(source_node.clone()),
            error: Some(error),
        };

        if let Err(error) = self
            .publish_resource_replica(ResourceNodeStatus {
                key: local_key.clone(),
                state: ResourceNodeState::Pending,
                root_checksum: None,
                last_verified_at: None,
                source_node: Some(source_node.clone()),
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
            &source_node,
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

        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_resource_installation_if_armed(self.inner.consensus.local_node_id())
            .await;

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
            .refresh_http_tls_server_config(Some(&manifest.resource.id))
            .await
        {
            if let Err(publish_error) = self
                .publish_resource_replica(failed_replica(
                    Some(manifest.resource.root_checksum),
                    format!("failed to refresh HTTP TLS config: {error}"),
                ))
                .await
            {
                self.broadcast_error(publish_error);
            }
            return;
        }

        if let Err(error) = self
            .publish_resource_replica(ResourceNodeStatus {
                key: local_key,
                state: ResourceNodeState::Ready,
                root_checksum: Some(manifest.resource.root_checksum),
                last_verified_at: Some(current_timestamp()),
                source_node: Some(source_node),
                error: None,
            })
            .await
        {
            self.broadcast_error(error);
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
                return match self.wait_for_authoritative_visibility().await {
                    Ok(()) => command_ok_already_existed(format!(
                        "resource '{}' already exists",
                        create.identifier.as_str()
                    )),
                    Err(error) => command_error(format!(
                        "resource '{}' exists, but authoritative visibility did not complete: \
                         {error}",
                        create.identifier.as_str()
                    )),
                };
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
            Ok(()) => match self.wait_for_authoritative_visibility().await {
                Ok(()) => command_ok(format!("created resource '{}'", create.identifier.as_str())),
                Err(error) => command_error(format!(
                    "created resource '{}', but authoritative visibility did not complete: {error}",
                    create.identifier.as_str()
                )),
            },
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
    ) -> Result<ResourceInstallation, Report<ResourceUploadError>> {
        let execution = self
            .inner
            .resource_upload_executions
            .entry(key.clone())
            .or_insert_with(|| StdArc::new(AsyncMutex::new(())))
            .clone();
        let _execution_guard = execution.lock().await;
        let identifier = ModelName::from(&key.identifier);
        let created_at = current_timestamp();
        if let Some(existing) = self.inner.consensus.current_resources().await.upload(&key)
            && existing.state.root_checksum() != root_checksum
        {
            return Err(Report::new(ResourceUploadError::DigestConflict {
                key,
                version: existing.version,
                expected_checksum: existing.state.root_checksum().to_string(),
                received_checksum: root_checksum,
            }));
        }
        let upload = self
            .inner
            .consensus
            .begin_resource_upload(key.clone(), root_checksum.clone())
            .await
            .change_context(ResourceUploadError::BeginUpload {
                identifier: identifier.clone(),
            })?;
        match &upload.state {
            ResourceUploadState::Completed {
                outcome_revision, ..
            } => {
                let id =
                    ResourceId::new(key.domain.clone(), key.identifier.clone(), upload.version);
                self.wait_for_resource_completion(&id).await?;
                self.wait_for_authoritative_revision(*outcome_revision)
                    .await
                    .map_err(|error| {
                        Report::new(ResourceUploadError::Terminal {
                            key: key.clone(),
                            version: upload.version,
                            reason: error.to_string(),
                        })
                    })?;
                return Ok(ResourceInstallation {
                    version: upload.version,
                });
            }
            ResourceUploadState::Failed { reason, .. } => {
                return Err(Report::new(ResourceUploadError::Terminal {
                    key,
                    version: upload.version,
                    reason: reason.clone(),
                }));
            }
            ResourceUploadState::Applying { .. } => {}
        }
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_resource_installation_if_armed(self.inner.consensus.local_node_id())
            .await;
        let id = ResourceId::new(key.domain.clone(), key.identifier.clone(), upload.version);
        let manifest = match self
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
        {
            Ok(manifest) => manifest,
            Err(error) => {
                let reason = error.to_string();
                self.inner
                    .consensus
                    .fail_resource_upload(key.clone(), reason.clone())
                    .await
                    .change_context(ResourceUploadError::InstallArchive { id: id.clone() })?;
                return Err(Report::new(ResourceUploadError::Terminal {
                    key,
                    version: id.version,
                    reason,
                }));
            }
        };
        let replica = ResourceNodeStatus {
            key: ResourceReplicaKey::new(
                manifest.resource.id.domain.clone(),
                manifest.resource.id.identifier.clone(),
                manifest.resource.id.version,
                ClusterNodeIdentity::new(
                    self.inner.consensus.local_node_id().clone(),
                    self.inner.cluster.local_incarnation(),
                ),
            ),
            state: ResourceNodeState::Ready,
            root_checksum: Some(manifest.resource.root_checksum.clone()),
            last_verified_at: Some(created_at),
            source_node: Some(ClusterNodeIdentity::new(
                self.inner.consensus.local_node_id().clone(),
                self.inner.cluster.local_incarnation(),
            )),
            error: None,
        };
        self.inner
            .consensus
            .publish_resource_upload(key.clone(), manifest.resource.clone(), replica)
            .await
            .change_context(ResourceUploadError::Publish {
                id: manifest.resource.id.clone(),
            })?;
        if let Err(error) = self
            .refresh_http_tls_server_config(Some(&manifest.resource.id))
            .await
        {
            let reason = format!("failed to refresh HTTP TLS config: {error}");
            let failed = ResourceNodeStatus {
                key: ResourceReplicaKey::new(
                    manifest.resource.id.domain.clone(),
                    manifest.resource.id.identifier.clone(),
                    manifest.resource.id.version,
                    ClusterNodeIdentity::new(
                        self.inner.consensus.local_node_id().clone(),
                        self.inner.cluster.local_incarnation(),
                    ),
                ),
                state: ResourceNodeState::Failed,
                root_checksum: Some(manifest.resource.root_checksum.clone()),
                last_verified_at: None,
                source_node: None,
                error: Some(reason.clone()),
            };
            self.inner
                .consensus
                .put_resource_replica(failed)
                .await
                .change_context(ResourceUploadError::NodeInstallation {
                    id: manifest.resource.id.clone(),
                    node: ClusterNodeIdentity::new(
                        self.inner.consensus.local_node_id().clone(),
                        self.inner.cluster.local_incarnation(),
                    ),
                    reason: reason.clone(),
                })?;
            self.inner
                .consensus
                .fail_resource_upload(key.clone(), reason.clone())
                .await
                .change_context(ResourceUploadError::NodeInstallation {
                    id: manifest.resource.id.clone(),
                    node: ClusterNodeIdentity::new(
                        self.inner.consensus.local_node_id().clone(),
                        self.inner.cluster.local_incarnation(),
                    ),
                    reason: reason.clone(),
                })?;
            return Err(Report::new(ResourceUploadError::Terminal {
                key,
                version: manifest.resource.id.version,
                reason,
            }));
        }
        if let Err(error) = self
            .wait_for_resource_completion(&manifest.resource.id)
            .await
        {
            let reason = error.to_string();
            self.inner
                .consensus
                .fail_resource_upload(key.clone(), reason.clone())
                .await
                .change_context(ResourceUploadError::NodeInstallation {
                    id: manifest.resource.id.clone(),
                    node: ClusterNodeIdentity::new(
                        self.inner.consensus.local_node_id().clone(),
                        self.inner.cluster.local_incarnation(),
                    ),
                    reason: reason.clone(),
                })?;
            return Err(Report::new(ResourceUploadError::Terminal {
                key,
                version: manifest.resource.id.version,
                reason,
            }));
        }
        let completed = self
            .inner
            .consensus
            .complete_resource_upload(key.clone())
            .await
            .change_context(ResourceUploadError::Publish {
                id: manifest.resource.id.clone(),
            })?;
        let outcome_revision = match &completed.state {
            ResourceUploadState::Completed {
                outcome_revision, ..
            } => *outcome_revision,
            ResourceUploadState::Failed { reason, .. } => {
                return Err(Report::new(ResourceUploadError::Terminal {
                    key,
                    version: manifest.resource.id.version,
                    reason: reason.clone(),
                }));
            }
            ResourceUploadState::Applying { .. } => {
                return Err(Report::new(ResourceUploadError::Publish {
                    id: manifest.resource.id.clone(),
                }));
            }
        };
        self.wait_for_authoritative_revision(outcome_revision)
            .await
            .map_err(|error| {
                Report::new(ResourceUploadError::Terminal {
                    key,
                    version: manifest.resource.id.version,
                    reason: error.to_string(),
                })
            })?;
        // Completion is retained before it is exposed to the caller. Re-evaluate the live set
        // afterwards so a node that joined while the terminal record propagated also becomes an
        // installation obligation for this response.
        self.wait_for_resource_completion(&manifest.resource.id)
            .await?;
        Ok(ResourceInstallation {
            version: manifest.resource.id.version,
        })
    }

    async fn wait_for_resource_completion(
        &self,
        id: &ResourceId,
    ) -> Result<(), Report<ResourceUploadError>> {
        let mut resources_changed = self.inner.consensus.subscribe_resources();
        let mut cluster_changed = self.inner.cluster.subscribe_state_changes().await;
        loop {
            tokio::task::consume_budget().await;
            let resources = self.inner.consensus.current_resources().await;
            let Some(resource) = resources.version(id) else {
                return Err(Report::new(ResourceUploadError::Publish { id: id.clone() }));
            };
            let gossip = self.inner.cluster.availability_state().await;
            let mut live_nodes = gossip.live_identities();
            live_nodes.insert(ClusterNodeIdentity::new(
                self.inner.consensus.local_node_id().clone(),
                self.inner.cluster.local_incarnation(),
            ));
            let mut pending = false;
            for node in live_nodes {
                tokio::task::consume_budget().await;
                let replica = resources.replicas.iter().find(|replica| {
                    replica.key.version_key().resource_id() == *id && replica.key.node == node
                });
                match replica {
                    Some(replica)
                        if replica.state == ResourceNodeState::Ready
                            && replica.root_checksum.as_deref()
                                == Some(resource.root_checksum.as_str()) => {}
                    Some(replica) if replica.state == ResourceNodeState::Failed => {
                        return Err(Report::new(ResourceUploadError::NodeInstallation {
                            id: id.clone(),
                            node,
                            reason: replica
                                .error
                                .clone()
                                .unwrap_or_else(|| "installation failed".to_string()),
                        }));
                    }
                    _ => pending = true,
                }
            }
            if !pending {
                return Ok(());
            }

            tokio::select! {
                changed = resources_changed.changed() => {
                    if changed.is_err() {
                        return Err(Report::new(ResourceUploadError::Publish { id: id.clone() }));
                    }
                }
                _ = cluster_changed.wait_for_change_or_next_unavailability() => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, CreateResource,
        CreateStatement, DomainName, ResourceName, ResourceUpload, ResourceUploadIdentity,
        ResourceUploads, ResourceVersion, ResourceVersionCounter, ResourceVersionStatus, Timestamp,
        UserName,
    };
    use nervix_nspl::client_statement::upload_resource_path_fragment;
    use sorted_vec::SortedVec;

    use super::{
        super::test_fixtures::{TestService, build_test_service, create_test_domain, named},
        *,
    };

    fn resource_upload_key(
        domain: &DomainName,
        identifier: &str,
        identity: &str,
    ) -> ResourceUploadKey {
        ResourceUploadKey::new(
            UserName::parse("default").assured("the test owner is an identifier-shaped literal"),
            domain.clone(),
            ResourceName::parse(identifier)
                .assured("the test resource name is an identifier-shaped literal"),
            ResourceUploadIdentity::parse(identity)
                .assured("the test upload identity uses accepted characters"),
        )
    }

    fn resource_version(
        key: &ResourceUploadKey,
        version: u64,
        source: &ClusterNodeIdentity,
        checksum: &str,
    ) -> ResourceVersion {
        ResourceVersion {
            id: ResourceId::new(key.domain.clone(), key.identifier.clone(), version),
            root_checksum: checksum.to_string(),
            manifest_checksum: format!("manifest-{checksum}"),
            file_count: 1,
            total_bytes: 1,
            archive_bytes: 1,
            created_at: Timestamp::from_unix_nanos(1),
            created_by_node: source.node_id().clone(),
        }
    }

    fn ready_replica(resource: &ResourceVersion, node: ClusterNodeIdentity) -> ResourceNodeStatus {
        ResourceNodeStatus {
            key: ResourceReplicaKey::new(
                resource.id.domain.clone(),
                resource.id.identifier.clone(),
                resource.id.version,
                node.clone(),
            ),
            state: ResourceNodeState::Ready,
            root_checksum: Some(resource.root_checksum.clone()),
            last_verified_at: Some(Timestamp::from_unix_nanos(1)),
            source_node: Some(node),
            error: None,
        }
    }

    async fn declare_resource(service: &SessionServiceImpl, domain: &DomainName, identifier: &str) {
        let created = service
            .create_resource(
                domain,
                CreateStatement::new(
                    CreateResource {
                        identifier: ResourceName::parse(identifier)
                            .assured("the test resource name is an identifier-shaped literal"),
                    },
                    false,
                ),
            )
            .await;
        assert!(
            created.success,
            "resource catalog must be created: {created:?}"
        );
    }

    #[test]
    fn resource_resolution_uses_only_completed_versions() {
        let domain =
            DomainName::parse("tenant").assured("the test domain is an identifier-shaped literal");
        let identifier: ResourceName = named("model");
        let source = ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-1")
                .assured("the test node name is an identifier-shaped literal"),
            ClusterNodeIncarnation::new(1),
        );
        let completed_key = resource_upload_key(&domain, identifier.as_str(), "completed");
        let applying_key = resource_upload_key(&domain, identifier.as_str(), "applying");
        let failed_key = resource_upload_key(&domain, identifier.as_str(), "failed");
        let resources = ResourceVersionStatus {
            next_version_by_resource: SortedVec::from_unsorted(vec![ResourceVersionCounter {
                domain: domain.clone(),
                identifier: identifier.clone(),
                next_version: 4,
            }]),
            versions: SortedVec::from_unsorted(vec![
                resource_version(&completed_key, 1, &source, "completed-root"),
                resource_version(&applying_key, 2, &source, "applying-root"),
                resource_version(&failed_key, 3, &source, "failed-root"),
            ]),
            replicas: SortedVec::new(),
            uploads: ResourceUploads::try_from_uploads([
                ResourceUpload {
                    key: completed_key,
                    version: 1,
                    state: ResourceUploadState::Completed {
                        root_checksum: "completed-root".to_string(),
                        outcome_revision: 1,
                    },
                },
                ResourceUpload {
                    key: applying_key,
                    version: 2,
                    state: ResourceUploadState::Applying {
                        root_checksum: "applying-root".to_string(),
                    },
                },
                ResourceUpload {
                    key: failed_key,
                    version: 3,
                    state: ResourceUploadState::Failed {
                        root_checksum: "failed-root".to_string(),
                        outcome_revision: 2,
                        reason: "installation failed".to_string(),
                    },
                },
            ])
            .assured("the test uploads have unique identities and versions"),
        };

        let latest = resolve_resource_id(&resources, &domain, &identifier, None)
            .assured("the completed version is eligible for an omitted-version binding");
        assert_eq!(latest.version, 1);
        let pinned = resolve_resource_id(&resources, &domain, &identifier, Some(1))
            .assured("the completed version is eligible for an explicit binding");
        assert_eq!(pinned.version, 1);

        for version in [2, 3] {
            let error = match resolve_resource_id(&resources, &domain, &identifier, Some(version)) {
                Ok(_) => panic!("an incomplete version must be ineligible for an explicit binding"),
                Err(error) => error,
            };
            assert_eq!(
                error,
                format!("resource 'model@{version}' is not a completed version in domain 'tenant'")
            );
        }
    }

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

    #[tokio::test]
    async fn interrupted_resource_upload_without_a_durable_version_becomes_terminal() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let identifier = "interrupted_archive";
        declare_resource(&service, &domain, identifier).await;
        let key = resource_upload_key(&domain, identifier, "interrupted-upload");
        let upload = service
            .inner
            .consensus
            .begin_resource_upload(key.clone(), "expected-root".to_string())
            .await
            .assured("the test upload is admitted by the single-node leader");
        let execution = service
            .inner
            .resource_upload_executions
            .entry(key.clone())
            .or_insert_with(|| StdArc::new(AsyncMutex::new(())))
            .clone();
        let execution_guard = execution.lock_owned().await;

        service
            .recover_resource_upload(upload, execution_guard)
            .await;

        let resources = service.inner.consensus.current_resources().await;
        let retained = resources
            .upload(&key)
            .assured("the admitted upload remains in durable state");
        let ResourceUploadState::Failed { reason, .. } = &retained.state else {
            panic!("an interrupted upload without its version must fail: {retained:?}");
        };
        assert!(reason.contains("archive is unavailable"));
        assert!(!service.inner.resource_upload_executions.contains_key(&key));

        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn interrupted_resource_upload_with_ready_replicas_completes() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let identifier = "ready_archive";
        declare_resource(&service, &domain, identifier).await;
        let key = resource_upload_key(&domain, identifier, "ready-upload");
        let upload = service
            .inner
            .consensus
            .begin_resource_upload(key.clone(), "ready-root".to_string())
            .await
            .assured("the test upload is admitted by the single-node leader");
        let local_node = ClusterNodeIdentity::new(
            service.inner.consensus.local_node_id().clone(),
            service.inner.cluster.local_incarnation(),
        );
        let resource = resource_version(&key, upload.version, &local_node, "ready-root");
        service
            .inner
            .consensus
            .publish_resource_upload(
                key.clone(),
                resource.clone(),
                ready_replica(&resource, local_node),
            )
            .await
            .assured("the durable resource version and local replica are published");
        let execution = service
            .inner
            .resource_upload_executions
            .entry(key.clone())
            .or_insert_with(|| StdArc::new(AsyncMutex::new(())))
            .clone();
        let execution_guard = execution.lock_owned().await;

        service
            .recover_resource_upload(upload, execution_guard)
            .await;

        let resources = service.inner.consensus.current_resources().await;
        let retained = resources
            .upload(&key)
            .assured("the completed upload remains in durable state");
        assert!(matches!(
            retained.state,
            ResourceUploadState::Completed { .. }
        ));
        assert!(!service.inner.resource_upload_executions.contains_key(&key));

        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn reconciliation_retains_a_failed_local_replica_when_no_live_source_exists() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let identifier = "remote_archive";
        declare_resource(&service, &domain, identifier).await;
        let key = resource_upload_key(&domain, identifier, "remote-upload");
        let upload = service
            .inner
            .consensus
            .begin_resource_upload(key.clone(), "remote-root".to_string())
            .await
            .assured("the test upload is admitted by the single-node leader");
        let remote_node = ClusterNodeIdentity::new(
            ClusterNodeName::parse("absent-node")
                .assured("the test node name is an identifier-shaped literal"),
            ClusterNodeIncarnation::new(1),
        );
        let resource = resource_version(&key, upload.version, &remote_node, "remote-root");
        service
            .inner
            .consensus
            .publish_resource_upload(
                key.clone(),
                resource.clone(),
                ready_replica(&resource, remote_node),
            )
            .await
            .assured("the durable resource version and remote replica are published");
        service
            .inner
            .consensus
            .complete_resource_upload(key)
            .await
            .assured("the durable upload is marked complete before reconciliation");
        let local_node = ClusterNodeIdentity::new(
            service.inner.consensus.local_node_id().clone(),
            service.inner.cluster.local_incarnation(),
        );
        let local_key = ResourceReplicaKey::new(
            resource.id.domain.clone(),
            resource.id.identifier.clone(),
            resource.id.version,
            local_node,
        );

        service.reconcile_resources_once().await;
        service.reconcile_resources_once().await;

        let resources = service.inner.consensus.current_resources().await;
        let replica_index = resources
            .replicas
            .binary_search_by(|replica| replica.key.cmp(&local_key))
            .assured("reconciliation publishes the local replica outcome");
        let replica = &resources.replicas[replica_index];
        assert_eq!(replica.state, ResourceNodeState::Failed);
        assert!(
            replica
                .error
                .as_deref()
                .is_some_and(|error| error.contains("installed manifest"))
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn invalid_resource_archive_failure_is_retained_across_retry() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let identifier = "invalid_archive";
        declare_resource(&service, &domain, identifier).await;
        let key = resource_upload_key(&domain, identifier, "invalid-upload");
        let missing_archive = path.join("missing-resource-archive.tar.zst");

        let first = service
            .install_uploaded_resource_archive(
                key.clone(),
                &missing_archive,
                "invalid-root".to_string(),
            )
            .await;
        let first_error = match first {
            Ok(_) => panic!("a missing resource archive must fail installation"),
            Err(error) => error,
        };
        assert!(matches!(
            first_error.current_context(),
            ResourceUploadError::Terminal { version: 1, .. }
        ));

        let retried = service
            .install_uploaded_resource_archive(key, &missing_archive, "invalid-root".to_string())
            .await;
        let retried_error = match retried {
            Ok(_) => panic!("a retained resource upload failure must remain terminal"),
            Err(error) => error,
        };
        assert!(matches!(
            retried_error.current_context(),
            ResourceUploadError::Terminal { version: 1, .. }
        ));

        let _ = std::fs::remove_dir_all(&path);
    }
}
