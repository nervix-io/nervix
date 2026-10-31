//! Uploading a resource archive and making every node hold a copy.
//!
//! Layer: control plane.
//!
//! - **Owns.** Upload staging, installation into the resource store, cluster replication, and the
//!   readiness a caller waits for.
//! - **Depends on.** The resource store for bytes and the interconnect to publish and fetch them.
//! - **Must not know.** What a model does with the resource once it is installed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::{collections::BTreeMap, path::Path};

use ahash::HashMap;
use error_stack::{Report, ResultExt};
use futures_util::{StreamExt, stream};
use nervix_consensus::ConsensusError;
use nervix_interconnect::Transport;
use nervix_models::{
    ClusterNodeIdentity, CreateResource, CreateStatement, DomainName, ModelName, ResourceId,
    ResourceName, ResourceNodeState, ResourceNodeStatus, ResourceReplicaKey, ResourceUpload,
    ResourceUploadKey, ResourceUploadState, UploadResource,
};
use nervix_primitives::sync::{Mutex as AsyncMutex, OwnedMutexGuard, StdArc};
use thiserror::Error;

use super::{
    command_result::{CommandOrigin, CommandResult},
    domain_clock::current_timestamp,
    model_mutation::{command_error, command_ok, command_ok_already_existed},
    session_service::SessionServiceImpl,
};
use crate::{
    resource::{ResourceStore, ResourceStoreError, StagedResourceArchive},
    resource_interconnect::{
        FetchResourceArchive, PublishResourceReplica, ResourceInterconnectError,
    },
};

const MAX_CONCURRENT_RESOURCE_REPLICATIONS: usize = 4;

#[derive(Debug, Error)]
enum ResourceFetchError {
    #[error("resource '{}@{}' archive exceeds the configured limit", .id.identifier, .id.version)]
    ArchiveQuota { id: ResourceId },
    #[error("resource '{}@{}' fetch request to node '{node}' failed", .id.identifier, .id.version)]
    Request {
        id: ResourceId,
        node: ClusterNodeIdentity,
    },
    #[error("resource archive size mismatch: expected {expected}, source declared {actual}")]
    SourceSize { expected: u64, actual: u64 },
    #[error("failed to create temporary resource archive")]
    Stage,
    #[error("resource fetch failed")]
    Read,
    #[error("resource chunk length is invalid")]
    ChunkLength,
    #[error("resource archive offset overflowed")]
    OffsetOverflow,
    #[error("resource archive size exceeds published size {published}")]
    ExceedsPublishedSize { published: u64 },
    #[error("failed to write temporary resource archive")]
    Write,
    #[error("resource archive size mismatch: expected {expected}, received {received}")]
    Incomplete { expected: u64, received: u64 },
    #[error("failed to flush temporary resource archive")]
    Finish,
    #[error("resource archive staging size mismatch: received {received}, wrote {written}")]
    StagingSize { received: u64, written: u64 },
}

#[derive(Debug, Error)]
enum ResourceReplicaPublishError {
    #[error("failed to publish resource replica: cluster leader is unknown")]
    UnknownLeader,
    #[error("failed to publish resource replica through consensus")]
    Consensus,
    #[error("failed to publish resource replica to leader '{leader}'")]
    Request {
        leader: nervix_models::ClusterNodeName,
    },
    #[error("leader rejected resource replica")]
    Remote { source: ResourceInterconnectError },
}

/// The reason a scenario-armed installation failure records for the version it fails. The failure
/// happens once the node installed and published the version, before the upload completes.
#[cfg(feature = "testing")]
const INJECTED_RESOURCE_INSTALLATION_FAILURE: &str = "injected resource installation failure";

#[derive(Debug, Error)]
pub(in crate::application) enum ResourceUploadError {
    #[error("failed to begin upload for resource '{identifier}'")]
    BeginUpload { identifier: ModelName },
    #[error("failed to install resource '{}@{}'", .id.identifier.as_str(), .id.version)]
    InstallArchive { id: ResourceId },
    #[error("failed to publish resource '{}@{}'", .id.identifier.as_str(), .id.version)]
    Publish { id: ResourceId },
    #[error("the admitted archive is unavailable before durable version installation")]
    AdmittedArchiveUnavailable { id: ResourceId },
    #[error(
        "failed to read this node's installed copy of resource '{}@{}'",
        .id.identifier.as_str(),
        .id.version
    )]
    ReadInstalledCopy { id: ResourceId },
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
            Self::InstallArchive { id }
            | Self::Publish { id }
            | Self::AdmittedArchiveUnavailable { id }
            | Self::ReadInstalledCopy { id } => Some(id.version),
            Self::DigestConflict { version, .. } => Some(*version),
            Self::NodeInstallation { id, .. } => Some(id.version),
            Self::Terminal { version, .. } => Some(*version),
        }
    }
}

/// How the leader's reconciliation resumes an applying upload whose installing task ended without
/// recording an outcome.
#[derive(Debug)]
enum UploadResumption {
    /// The upload's version is published, so the upload completes once every live node
    /// incarnation holds it.
    Published,
    /// The upload cannot complete, and fails with the report's rendered reason.
    Failed(Report<ResourceUploadError>),
    /// A refusal that decided nothing kept the version unpublished on this pass. The upload stays
    /// applying, and the next reconciliation resumes it again.
    Deferred(Report<ResourceUploadError>),
}

impl UploadResumption {
    /// Resumes from a failed read of this node's installed copy of `id`. A refusal by the node's
    /// executor or a cancellation decided nothing, and any other failure leaves the copy unusable.
    fn from_unreadable_copy(error: Report<ResourceStoreError>, id: &ResourceId) -> Self {
        if let ResourceStoreError::BulkAdmission
        | ResourceStoreError::JoinBlockingTask
        | ResourceStoreError::Cancelled = error.current_context()
        {
            return Self::Deferred(
                error.change_context(ResourceUploadError::ReadInstalledCopy { id: id.clone() }),
            );
        }
        Self::Failed(
            error
                .change_context(ResourceUploadError::AdmittedArchiveUnavailable { id: id.clone() }),
        )
    }

    /// Resumes from a refused publication of this node's installed copy of `id`. A conflict with
    /// the catalog is final. Any other refusal, such as a leader that cannot confirm it still
    /// leads, decided nothing.
    fn from_refused_publication(error: Report<ConsensusError>, id: &ResourceId) -> Self {
        if let ConsensusError::Conflict(_) = error.current_context() {
            return Self::Failed(
                error.change_context(ResourceUploadError::Publish { id: id.clone() }),
            );
        }
        Self::Deferred(error.change_context(ResourceUploadError::Publish { id: id.clone() }))
    }
}

/// A resource version a restore imports under its archived number.
pub(in crate::application) struct ImportedResourceVersion<'a> {
    /// The execution reference of the restore that imports it.
    pub(in crate::application) restore: &'a nervix_models::CommandExecutionReference,
    /// The upload identity the version is recorded under.
    pub(in crate::application) key: ResourceUploadKey,
    /// The version's catalog metadata as the archive recorded it, under the restored domain.
    pub(in crate::application) resource: nervix_models::ResourceVersion,
    /// The staged restore archive that holds the version's original archive.
    pub(in crate::application) archive_path: &'a Path,
    /// Where the version's archive starts in it. Its length is the version's archive size.
    pub(in crate::application) offset: u64,
}

pub(in crate::application) struct ResourceInstallation {
    pub(in crate::application) version: u64,
    /// Whether this call installed the version, or found the upload identity already completed by
    /// an earlier call whose reply was lost.
    pub(in crate::application) origin: CommandOrigin,
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
) -> error_stack::Result<StagedResourceArchive, ResourceFetchError> {
    resource_store
        .validate_archive_bytes(resource.archive_bytes)
        .change_context(ResourceFetchError::ArchiveQuota {
            id: resource.id.clone(),
        })?;
    let mut archive = interconnect
        .request_stream(
            source_node.node_id(),
            FetchResourceArchive {
                id: resource.id.clone(),
            },
        )
        .await
        .change_context(ResourceFetchError::Request {
            id: resource.id.clone(),
            node: source_node.clone(),
        })?;
    if archive.content_length() != resource.archive_bytes {
        return Err(Report::new(ResourceFetchError::SourceSize {
            expected: resource.archive_bytes,
            actual: archive.content_length(),
        }));
    }
    let mut staged = resource_store
        .create_archive_stager()
        .await
        .change_context(ResourceFetchError::Stage)?;
    let mut received = 0_u64;
    while let Some(chunk) = archive
        .next_chunk()
        .await
        .change_context(ResourceFetchError::Read)?
    {
        nervix_primitives::task::consume_budget().await;
        let chunk_bytes =
            u64::try_from(chunk.len()).map_err(|_| Report::new(ResourceFetchError::ChunkLength))?;
        let next_received = received
            .checked_add(chunk_bytes)
            .ok_or_else(|| Report::new(ResourceFetchError::OffsetOverflow))?;
        if next_received > resource.archive_bytes {
            return Err(Report::new(ResourceFetchError::ExceedsPublishedSize {
                published: resource.archive_bytes,
            }));
        }
        resource_store
            .validate_archive_bytes(next_received)
            .change_context(ResourceFetchError::ArchiveQuota {
                id: resource.id.clone(),
            })?;
        staged
            .write_chunk(chunk)
            .await
            .change_context(ResourceFetchError::Write)?;
        received = next_received;
    }
    if received != resource.archive_bytes {
        return Err(Report::new(ResourceFetchError::Incomplete {
            expected: resource.archive_bytes,
            received,
        }));
    }
    let staged = staged
        .finish()
        .await
        .change_context(ResourceFetchError::Finish)?;
    if staged.archive_bytes() != received {
        return Err(Report::new(ResourceFetchError::StagingSize {
            received,
            written: staged.archive_bytes(),
        }));
    }
    Ok(staged)
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

/// The completed versions of one resource that a binding may name, as completion offers them.
pub(in crate::application) fn completed_resource_version_suggestions(
    resources: &nervix_models::ResourceVersionStatus,
    domain: &DomainName,
    identifier: &ResourceName,
    prefix: &str,
) -> Vec<String> {
    let mut suggestions = Vec::new();
    for id in resources.uploads.completed_versions_of(domain, identifier) {
        let version = id.version.to_string();
        if prefix.is_empty() || version.starts_with(prefix) {
            suggestions.push(version);
        }
    }
    suggestions
}

/// The resource a version completion belongs to. Resource bindings write the resource immediately
/// before `VERSION`; `REBIND RESOURCE` inserts `TO` between them.
pub(in crate::application) fn resource_named_before_version(
    input: &str,
    cursor: usize,
) -> Option<ResourceName> {
    let prefix = input.get(..cursor.min(input.len()))?;
    let words = prefix.split_whitespace().collect::<Vec<_>>();
    let version_index = words
        .iter()
        .rposition(|word| word.eq_ignore_ascii_case("VERSION"))?;
    let mut words = words.get(..version_index)?.iter().rev();
    let mut identifier = words.next()?;
    if identifier.eq_ignore_ascii_case("TO") {
        identifier = words.next()?;
    }
    ResourceName::parse(identifier).ok()
}

impl SessionServiceImpl {
    async fn publish_resource_replica(
        &self,
        replica: ResourceNodeStatus,
    ) -> error_stack::Result<(), ResourceReplicaPublishError> {
        let Some(leader_id) = self.inner.consensus.current_leader().await else {
            return Err(Report::new(ResourceReplicaPublishError::UnknownLeader));
        };

        if leader_id == self.inner.consensus.local_node_id().clone() {
            return self
                .inner
                .consensus
                .put_resource_replica(replica)
                .await
                .change_context(ResourceReplicaPublishError::Consensus);
        }

        self.inner
            .interconnect
            .request(&leader_id, PublishResourceReplica { replica })
            .await
            .change_context(ResourceReplicaPublishError::Request { leader: leader_id })?
            .map_err(|source| Report::new(ResourceReplicaPublishError::Remote { source }))
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
            nervix_primitives::task::consume_budget().await;
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
            if let Ok(Some(manifest)) = &local_manifest
                && manifest.resource == resource
            {
                let replica = ResourceNodeStatus {
                    key: local_key,
                    state: ResourceNodeState::Ready,
                    root_checksum: Some(manifest.resource.root_checksum.clone()),
                    last_verified_at: Some(current_timestamp()),
                    source_node: Some(local_node.clone()),
                    error: None,
                };
                if let Err(error) = self.publish_resource_replica(replica).await {
                    self.broadcast_error(format!("{error:#}"));
                }
                continue;
            }
            let source_node = resource_replicas.and_then(|replicas| {
                replicas.iter().find_map(|(node, replica)| {
                    (node != &local_node
                        && live_nodes.contains(node)
                        && replica.state == ResourceNodeState::Ready
                        && replica.root_checksum.as_deref()
                            == Some(resource.root_checksum.as_str()))
                    .then(|| node.clone())
                })
            });
            #[cfg(feature = "testing")]
            let source_node = if source_node.is_some()
                && self
                    .inner
                    .runtime
                    .resource_source_discovery_is_deferred(&local_node_id)
            {
                None
            } else {
                source_node
            };
            let Some(source_node) = source_node else {
                let local_error = match local_manifest {
                    Ok(None) => None,
                    Ok(Some(manifest)) => Some(format!(
                        "installed manifest for resource '{}@{}' does not match its durable \
                         version; found root checksum {}",
                        resource.id.identifier.as_str(),
                        resource.id.version,
                        manifest.resource.root_checksum,
                    )),
                    Err(error) => Some(format!(
                        "failed to read the installed manifest for resource '{}@{}': {error:#}",
                        resource.id.identifier.as_str(),
                        resource.id.version,
                    )),
                };
                let state = match &local_error {
                    Some(_) => ResourceNodeState::Failed,
                    None => ResourceNodeState::Pending,
                };
                let local_status = ResourceNodeStatus {
                    key: local_key,
                    state,
                    root_checksum: None,
                    last_verified_at: None,
                    source_node: None,
                    error: local_error,
                };
                let already_reported = match resource_replicas {
                    Some(replicas) => match replicas.get(&local_node) {
                        Some(replica) => replica == &local_status,
                        None => false,
                    },
                    None => false,
                };
                if !already_reported
                    && let Err(error) = self.publish_resource_replica(local_status).await
                {
                    self.broadcast_error(format!("{error:#}"));
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
                nervix_primitives::task::consume_budget().await;
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

    /// Resumes an applying upload whose installing task ended without recording an outcome. A
    /// published version completes once every live node incarnation holds it, and a version the
    /// task never published is published from this node's installed copy when it holds one.
    async fn recover_resource_upload(
        &self,
        upload: ResourceUpload,
        _execution_guard: OwnedMutexGuard<()>,
    ) {
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .fault_injection()
            .take_up_refused_resource_upload(self.inner.consensus.local_node_id());
        let id = ResourceId::new(
            upload.key.domain.clone(),
            upload.key.identifier.clone(),
            upload.version,
        );
        let resources = self.inner.consensus.current_resources().await;
        let resumption = if resources.version(&id).is_some() {
            UploadResumption::Published
        } else {
            self.publish_installed_upload_copy(&upload, &id).await
        };
        let outcome = match resumption {
            UploadResumption::Published => self
                .wait_for_resource_completion(&id)
                .await
                .map_err(|error| format!("{error:#}")),
            UploadResumption::Failed(error) => Err(format!("{error:#}")),
            UploadResumption::Deferred(error) => {
                self.broadcast_error(format!(
                    "resource upload '{}' stays applying: {error:#}",
                    upload.key.identity
                ));
                return;
            }
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

    /// Publishes the copy of `upload`'s archive that this node installed before the task
    /// installing the upload ended, with this node's ready replica, as that task publishes it.
    /// Only the node that admits an upload installs its first copy, so a leader that holds no copy
    /// with the upload's number and digest cannot complete the upload.
    async fn publish_installed_upload_copy(
        &self,
        upload: &ResourceUpload,
        id: &ResourceId,
    ) -> UploadResumption {
        let manifest = match self.inner.resource_store.read_manifest(id).await {
            Ok(Some(manifest)) => manifest,
            Ok(None) => {
                return UploadResumption::Failed(Report::new(
                    ResourceUploadError::AdmittedArchiveUnavailable { id: id.clone() },
                ));
            }
            Err(error) => return UploadResumption::from_unreadable_copy(error, id),
        };
        if manifest.resource.id != *id
            || manifest.resource.root_checksum != upload.state.root_checksum()
        {
            return UploadResumption::Failed(Report::new(
                ResourceUploadError::AdmittedArchiveUnavailable { id: id.clone() },
            ));
        }
        let local_node = ClusterNodeIdentity::new(
            self.inner.consensus.local_node_id().clone(),
            self.inner.cluster.local_incarnation(),
        );
        let replica = ResourceNodeStatus {
            key: ResourceReplicaKey::new(
                id.domain.clone(),
                id.identifier.clone(),
                id.version,
                local_node.clone(),
            ),
            state: ResourceNodeState::Ready,
            root_checksum: Some(manifest.resource.root_checksum.clone()),
            last_verified_at: Some(current_timestamp()),
            source_node: Some(local_node),
            error: None,
        };
        let published = self
            .inner
            .consensus
            .publish_resource_upload(upload.key.clone(), manifest.resource, replica)
            .await;
        match published {
            Ok(_) => UploadResumption::Published,
            Err(error) => UploadResumption::from_refused_publication(error, id),
        }
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
            self.broadcast_error(format!("{error:#}"));
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
                    .publish_resource_replica(failed_replica(None, format!("{error:#}")))
                    .await
                {
                    self.broadcast_error(format!("{publish_error:#}"));
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
                self.broadcast_error(format!("{error:#}"));
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
                    .publish_resource_replica(failed_replica(None, format!("{error:#}")))
                    .await
                {
                    self.broadcast_error(format!("{publish_error:#}"));
                }
                return;
            }
        };

        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .take_armed_resource_installation_failure(self.inner.consensus.local_node_id())
        {
            if let Err(publish_error) = self
                .publish_resource_replica(failed_replica(
                    Some(manifest.resource.root_checksum),
                    INJECTED_RESOURCE_INSTALLATION_FAILURE.to_string(),
                ))
                .await
            {
                self.broadcast_error(format!("{publish_error:#}"));
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
            self.broadcast_error(format!("{error:#}"));
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
                self.consensus_report_response(
                    &error,
                    format!("failed to create resource '{}'", create.identifier.as_str()),
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
                            reason: format!("{error:#}"),
                        })
                    })?;
                return Ok(ResourceInstallation {
                    version: upload.version,
                    origin: CommandOrigin::Recovered,
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
                let reason = format!("{error:#}");
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
        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .fault_injection()
            .refuses_resource_publication(self.inner.consensus.local_node_id())
        {
            return Err(Report::new(ConsensusError::LeadershipLost {
                observed: nervix_consensus::LeadershipObservation::NoLeader,
            })
            .change_context(ResourceUploadError::Publish {
                id: manifest.resource.id.clone(),
            }));
        }
        self.inner
            .consensus
            .publish_resource_upload(key.clone(), manifest.resource.clone(), replica)
            .await
            .change_context(ResourceUploadError::Publish {
                id: manifest.resource.id.clone(),
            })?;
        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .take_armed_resource_installation_failure(self.inner.consensus.local_node_id())
        {
            let reason = INJECTED_RESOURCE_INSTALLATION_FAILURE.to_string();
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
            let reason = format!("{error:#}");
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
                    reason: format!("{error:#}"),
                })
            })?;
        // Completion is retained before it is exposed to the caller. Re-evaluate the live set
        // afterwards so a node that joined while the terminal record propagated also becomes an
        // installation obligation for this response.
        self.wait_for_resource_completion(&manifest.resource.id)
            .await?;
        Ok(ResourceInstallation {
            version: manifest.resource.id.version,
            origin: CommandOrigin::Executed,
        })
    }

    /// Installs a version a restore imports under its archived number: the leader installs its
    /// own copy from `import`'s section of the restore's archive, verifying every field the archive
    /// recorded, records and publishes the version in one consensus command, and then completes it
    /// exactly as an upload completes, once every live node incarnation holds the same digest.
    ///
    /// Importing a version recorded before, by an earlier attempt of the same restore, joins that
    /// installation: a published version waits for the live set, and a completed one returns once
    /// every live node holds it.
    pub(in crate::application) async fn install_imported_resource_version(
        &self,
        import: ImportedResourceVersion<'_>,
    ) -> Result<(), Report<ResourceUploadError>> {
        let ImportedResourceVersion {
            restore,
            key,
            resource,
            archive_path,
            offset,
        } = import;
        let execution = self
            .inner
            .resource_upload_executions
            .entry(key.clone())
            .or_insert_with(|| StdArc::new(AsyncMutex::new(())))
            .clone();
        let _execution_guard = execution.lock().await;
        let id = resource.id.clone();
        let recorded = self.inner.consensus.current_resources().await;
        match recorded.upload(&key).map(|upload| upload.state.clone()) {
            Some(ResourceUploadState::Completed {
                outcome_revision, ..
            }) => {
                self.wait_for_resource_completion(&id).await?;
                return self
                    .wait_for_authoritative_revision(outcome_revision)
                    .await
                    .map_err(|error| {
                        Report::new(ResourceUploadError::Terminal {
                            key: key.clone(),
                            version: id.version,
                            reason: format!("{error:#}"),
                        })
                    });
            }
            Some(ResourceUploadState::Failed { reason, .. }) => {
                return Err(Report::new(ResourceUploadError::Terminal {
                    key,
                    version: id.version,
                    reason,
                }));
            }
            Some(ResourceUploadState::Applying { .. }) => {}
            None => {
                let manifest = self
                    .inner
                    .resource_store
                    .install_replica_from_archive_section(
                        resource.clone(),
                        archive_path,
                        offset,
                        resource.archive_bytes,
                    )
                    .await
                    .change_context(ResourceUploadError::InstallArchive { id: id.clone() })?;
                let local = ClusterNodeIdentity::new(
                    self.inner.consensus.local_node_id().clone(),
                    self.inner.cluster.local_incarnation(),
                );
                let replica = ResourceNodeStatus {
                    key: ResourceReplicaKey::new(
                        id.domain.clone(),
                        id.identifier.clone(),
                        id.version,
                        local.clone(),
                    ),
                    state: ResourceNodeState::Ready,
                    root_checksum: Some(manifest.resource.root_checksum.clone()),
                    last_verified_at: Some(current_timestamp()),
                    source_node: Some(local),
                    error: None,
                };
                self.inner
                    .consensus
                    .import_resource_version(
                        restore.clone(),
                        key.clone(),
                        manifest.resource,
                        replica,
                    )
                    .await
                    .change_context(ResourceUploadError::Publish { id: id.clone() })?;
            }
        }
        if let Err(error) = self.wait_for_resource_completion(&id).await {
            let reason = format!("{error:#}");
            self.inner
                .consensus
                .fail_resource_upload(key.clone(), reason.clone())
                .await
                .change_context(ResourceUploadError::Publish { id: id.clone() })?;
            return Err(Report::new(ResourceUploadError::Terminal {
                key,
                version: id.version,
                reason,
            }));
        }
        let completed = self
            .inner
            .consensus
            .complete_resource_upload(key.clone())
            .await
            .change_context(ResourceUploadError::Publish { id: id.clone() })?;
        let outcome_revision = match &completed.state {
            ResourceUploadState::Completed {
                outcome_revision, ..
            } => *outcome_revision,
            ResourceUploadState::Failed { reason, .. } => {
                return Err(Report::new(ResourceUploadError::Terminal {
                    key,
                    version: id.version,
                    reason: reason.clone(),
                }));
            }
            ResourceUploadState::Applying { .. } => {
                return Err(Report::new(ResourceUploadError::Publish { id }));
            }
        };
        self.wait_for_authoritative_revision(outcome_revision)
            .await
            .map_err(|error| {
                Report::new(ResourceUploadError::Terminal {
                    key,
                    version: id.version,
                    reason: format!("{error:#}"),
                })
            })?;
        // As for an upload, a node that joined while the outcome propagated also has to hold the
        // version before the import reports it complete.
        self.wait_for_resource_completion(&id).await
    }

    async fn wait_for_resource_completion(
        &self,
        id: &ResourceId,
    ) -> Result<(), Report<ResourceUploadError>> {
        let mut resources_changed = self.inner.consensus.subscribe_resources();
        let mut cluster_changed = self.inner.cluster.subscribe_state_changes().await;
        loop {
            nervix_primitives::task::consume_budget().await;
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
                nervix_primitives::task::consume_budget().await;
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

            nervix_primitives::select! {
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    #[cfg(feature = "testing")]
    use nervix_consensus::{ConsensusTestProbe, StorageBoundary};
    use nervix_models::{
        ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, CreateResource,
        CreateStatement, DescribeResource, DomainName, ResourceName, ResourceUpload,
        ResourceUploadIdentity, ResourceUploads, ResourceVersion, ResourceVersionCounter,
        ResourceVersionStatus, Timestamp, UserName,
    };
    use nervix_plexus::client_statement::local_path_fragment;
    use sorted_vec::SortedVec;

    #[cfg(feature = "testing")]
    use super::super::test_fixtures::build_test_service_with_probe;
    use super::{
        super::test_fixtures::{TestService, build_test_service, create_test_domain, named},
        *,
    };

    #[test]
    fn a_replica_the_leader_rejects_names_the_rejection_once() {
        let rejection = crate::resource_interconnect::ResourceInterconnectError::ReplicaOrigin {
            authenticated: ClusterNodeName::parse("node-b").assured("the test node name is valid"),
            declared: ClusterNodeName::parse("node-a").assured("the test node name is valid"),
        };
        let report = Report::new(ResourceReplicaPublishError::Remote { source: rejection });

        assert_eq!(
            format!("{report:#}"),
            "leader rejected resource replica: authenticated node 'node-b' cannot publish \
             resource state for 'node-a'"
        );
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn resource_catalog_storage_failure_keeps_resource_undeclared() {
        let probe = ConsensusTestProbe::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_probe(true, probe.clone()).await;
        let domain = DomainName::parse("default").assured("the test domain name is valid");
        let resource =
            ResourceName::parse("failed_resource").assured("the test resource name is valid");
        probe.storage_fault().fail_next(
            "create-resource-catalog:default.failed_resource".to_string(),
            StorageBoundary::BeforeCommit,
        );

        let result = service
            .create_resource(
                &domain,
                CreateStatement::new(
                    CreateResource {
                        identifier: resource.clone(),
                    },
                    false,
                ),
            )
            .await;
        assert!(!result.succeeded(), "{result:?}");
        assert!(result.message.contains("consensus storage"), "{result:?}");
        assert!(
            !service
                .inner
                .consensus
                .current_resources()
                .await
                .is_declared(&domain, &resource)
        );

        drop(service);
        drop(registry);
        let _ = std::fs::remove_dir_all(path);
    }

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
            created.succeeded(),
            "resource catalog must be created: {created:?}"
        );
    }

    #[test]
    fn binding_version_suggestions_offer_only_completed_versions() {
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

        assert_eq!(
            completed_resource_version_suggestions(&resources, &domain, &identifier, ""),
            vec!["1".to_string()]
        );
        assert!(
            completed_resource_version_suggestions(&resources, &domain, &identifier, "2")
                .is_empty(),
            "an applying version is not offered to a binding"
        );
        assert!(
            completed_resource_version_suggestions(&resources, &domain, &identifier, "3")
                .is_empty(),
            "a failed version is not offered to a binding"
        );
        let other =
            DomainName::parse("other").assured("the test domain is an identifier-shaped literal");
        assert!(
            completed_resource_version_suggestions(&resources, &other, &identifier, "").is_empty(),
            "another domain must not see this domain's completed versions"
        );
        assert_eq!(
            resource_version_suggestions(&resources, &domain, &identifier, ""),
            vec!["1".to_string(), "2".to_string(), "3".to_string()],
            "DESCRIBE RESOURCE still offers every published version"
        );
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
        for input in [
            "DESCRIBE RESOURCE proto VERSION ",
            "CREATE VHOST edge api.example.com WITH TLS proto VERSION ",
            "CREATE CODEC c FROM PROTOBUF USING RESOURCE proto VERSION 1",
            "CREATE INFERENCER i FROM features USING RESOURCE proto VERSION ",
            "REBIND RESOURCE proto TO VERSION ",
            "REBIND\nRESOURCE\tproto\nTO\tVERSION ",
        ] {
            assert_eq!(
                resource_named_before_version(input, input.len()),
                Some(named("proto")),
                "{input:?} names the resource its version belongs to"
            );
        }
        assert_eq!(
            resource_named_before_version("CREATE VHOST edge api.example.com", 33),
            None
        );
    }

    fn local_path_at_end(input: &str) -> Option<&str> {
        let local_path = local_path_fragment(input, input.len())?;
        Some(local_path.fragment)
    }

    #[test]
    fn a_local_path_is_detected_where_upload_resource_expects_one() {
        assert_eq!(
            local_path_at_end("UPLOAD RESOURCE proto VERSION '/tmp/pro"),
            Some("/tmp/pro")
        );
        assert_eq!(
            local_path_at_end("UPLOAD RESOURCE proto VERSION "),
            Some("")
        );
        assert_eq!(local_path_at_end("DESCRIBE RESOURCE proto VERSION "), None);
    }

    #[nervix_primitives::test]
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
        assert!(first.succeeded());
        assert!(!first.found_existing());

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
        assert!(duplicate.succeeded());
        assert!(duplicate.found_existing());
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
            same_name_other_domain.succeeded(),
            "resources are domain-owned: {}",
            same_name_other_domain.message
        );
        assert!(!same_name_other_domain.found_existing());

        let _ = std::fs::remove_dir_all(&path);
    }

    #[nervix_primitives::test]
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
        assert_eq!(
            reason,
            "the admitted archive is unavailable before durable version installation"
        );
        assert!(!service.inner.resource_upload_executions.contains_key(&key));

        let _ = std::fs::remove_dir_all(&path);
    }

    #[nervix_primitives::test]
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

    /// Installs a one-file directory as the leader's own copy of `id`, as the task installing an
    /// upload does before it publishes the version, and returns the metadata the copy records.
    async fn install_leader_copy(
        service: &SessionServiceImpl,
        id: &ResourceId,
        source: &Path,
    ) -> ResourceVersion {
        std::fs::create_dir_all(source)
            .assured("the test source lives in the test's own directory");
        std::fs::write(source.join("lookup.jsonl"), b"{\"zip\":\"60601\"}\n")
            .assured("the test source lives in the test's own directory");
        let manifest = service
            .inner
            .resource_store
            .install_from_directory(
                id.clone(),
                source,
                service.inner.consensus.local_node_id().clone(),
                Timestamp::from_unix_nanos(1),
            )
            .await
            .assured("the test store installs a one-file directory within its quotas");
        manifest.resource
    }

    /// Takes the execution lock that the leader's reconciliation holds while it recovers `key`.
    async fn recovery_execution(
        service: &SessionServiceImpl,
        key: &ResourceUploadKey,
    ) -> OwnedMutexGuard<()> {
        let execution = service
            .inner
            .resource_upload_executions
            .entry(key.clone())
            .or_insert_with(|| StdArc::new(AsyncMutex::new(())))
            .clone();
        execution.lock_owned().await
    }

    #[nervix_primitives::test]
    async fn an_interrupted_upload_publishes_the_leaders_installed_copy_and_completes() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let identifier = "installed_archive";
        declare_resource(&service, &domain, identifier).await;
        let key = resource_upload_key(&domain, identifier, "installed-upload");
        let id = ResourceId::new(key.domain.clone(), key.identifier.clone(), 1);
        let installed = install_leader_copy(&service, &id, &path.join("installed-source")).await;
        let upload = service
            .inner
            .consensus
            .begin_resource_upload(key.clone(), installed.root_checksum.clone())
            .await
            .assured("the test upload is admitted by the single-node leader");
        assert_eq!(
            upload.version, id.version,
            "the first upload of a resource takes its first number"
        );
        let execution_guard = recovery_execution(&service, &key).await;

        service
            .recover_resource_upload(upload, execution_guard)
            .await;

        let resources = service.inner.consensus.current_resources().await;
        let retained = resources
            .upload(&key)
            .assured("the resumed upload remains in durable state");
        assert!(
            matches!(retained.state, ResourceUploadState::Completed { .. }),
            "an upload whose leader holds its installed copy completes: {retained:?}"
        );
        assert_eq!(
            resources.version(&id),
            Some(&installed),
            "the leader publishes the metadata its installed copy records"
        );
        assert!(!service.inner.resource_upload_executions.contains_key(&key));

        let _ = std::fs::remove_dir_all(&path);
    }

    #[nervix_primitives::test]
    async fn an_interrupted_upload_without_a_local_copy_fails_as_unavailable() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let identifier = "missing_archive";
        declare_resource(&service, &domain, identifier).await;
        let key = resource_upload_key(&domain, identifier, "missing-upload");
        let upload = service
            .inner
            .consensus
            .begin_resource_upload(key.clone(), "unavailable-root".to_string())
            .await
            .assured("the test upload is admitted by the single-node leader");
        let id = ResourceId::new(key.domain.clone(), key.identifier.clone(), upload.version);
        let execution_guard = recovery_execution(&service, &key).await;

        service
            .recover_resource_upload(upload, execution_guard)
            .await;

        let resources = service.inner.consensus.current_resources().await;
        let retained = resources
            .upload(&key)
            .assured("the failed upload remains in durable state");
        let ResourceUploadState::Failed { reason, .. } = &retained.state else {
            panic!("an absent installed copy cannot complete the upload: {retained:?}");
        };
        assert_eq!(
            reason,
            "the admitted archive is unavailable before durable version installation"
        );
        assert_eq!(resources.version(&id), None, "nothing is published");
        assert!(!service.inner.resource_upload_executions.contains_key(&key));

        let _ = std::fs::remove_dir_all(&path);
    }

    #[nervix_primitives::test]
    async fn an_interrupted_upload_fails_when_the_leaders_copy_holds_another_archive() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let identifier = "other_archive";
        declare_resource(&service, &domain, identifier).await;
        let key = resource_upload_key(&domain, identifier, "other-upload");
        let id = ResourceId::new(key.domain.clone(), key.identifier.clone(), 1);
        install_leader_copy(&service, &id, &path.join("other-source")).await;
        let upload = service
            .inner
            .consensus
            .begin_resource_upload(key.clone(), "another-root".to_string())
            .await
            .assured("the test upload is admitted by the single-node leader");
        let execution_guard = recovery_execution(&service, &key).await;

        service
            .recover_resource_upload(upload, execution_guard)
            .await;

        let resources = service.inner.consensus.current_resources().await;
        let retained = resources
            .upload(&key)
            .assured("the failed upload remains in durable state");
        let ResourceUploadState::Failed { reason, .. } = &retained.state else {
            panic!("a copy of another archive cannot complete the upload: {retained:?}");
        };
        assert_eq!(
            reason,
            "the admitted archive is unavailable before durable version installation"
        );
        assert_eq!(resources.version(&id), None, "nothing is published");

        let _ = std::fs::remove_dir_all(&path);
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn an_interrupted_upload_stays_applying_while_consensus_refuses_its_copy() {
        let probe = ConsensusTestProbe::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_probe(true, probe.clone()).await;
        let domain = DomainName::parse("default").assured("the test domain name is valid");
        let identifier = "deferred_archive";
        declare_resource(&service, &domain, identifier).await;
        let key = resource_upload_key(&domain, identifier, "deferred-upload");
        let id = ResourceId::new(key.domain.clone(), key.identifier.clone(), 1);
        let installed = install_leader_copy(&service, &id, &path.join("deferred-source")).await;
        let upload = service
            .inner
            .consensus
            .begin_resource_upload(key.clone(), installed.root_checksum.clone())
            .await
            .assured("the test upload is admitted by the single-node leader");
        probe.storage_fault().fail_next(
            "publish-resource-upload:default.deferred_archive@1:default:deferred-upload"
                .to_string(),
            StorageBoundary::BeforeCommit,
        );
        let execution_guard = recovery_execution(&service, &key).await;

        service
            .recover_resource_upload(upload, execution_guard)
            .await;

        assert!(
            !probe.storage_fault().is_armed(),
            "the resumed publication met the storage failure"
        );
        let resources = service.inner.consensus.current_resources().await;
        let retained = resources
            .upload(&key)
            .assured("the admitted upload remains in durable state");
        assert!(
            matches!(retained.state, ResourceUploadState::Applying { .. }),
            "a publication consensus refused leaves the upload applying: {retained:?}"
        );
        assert_eq!(resources.version(&id), None, "nothing is published");
        assert!(
            service.inner.resource_upload_executions.contains_key(&key),
            "the next reconciliation takes the upload up again"
        );

        drop(service);
        drop(registry);
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn an_unreadable_installed_copy_resumes_by_the_cause_of_its_read() {
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let resource = ResourceName::parse("unreadable")
            .assured("the test resource name is identifier-shaped");
        let id = ResourceId::new(domain, resource, 1);

        let refused = UploadResumption::from_unreadable_copy(
            Report::new(ResourceStoreError::BulkAdmission),
            &id,
        );
        let UploadResumption::Deferred(refused) = refused else {
            panic!("an executor's refusal of the read decides nothing: {refused:?}");
        };
        assert_eq!(
            format!("{refused:#}"),
            "failed to read this node's installed copy of resource 'unreadable@1': the node has \
             no bulk capacity for this resource version"
        );

        let denied = UploadResumption::from_unreadable_copy(
            Report::new(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
                .change_context(ResourceStoreError::ReadFile),
            &id,
        );
        let UploadResumption::Failed(denied) = denied else {
            panic!("a copy that cannot be read is unusable: {denied:?}");
        };
        assert_eq!(
            format!("{denied:#}"),
            "the admitted archive is unavailable before durable version installation: failed to \
             read file: permission denied"
        );
    }

    #[test]
    fn a_refused_publication_of_the_installed_copy_fails_only_on_a_catalog_conflict() {
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let resource =
            ResourceName::parse("refused").assured("the test resource name is identifier-shaped");
        let id = ResourceId::new(domain, resource, 1);

        let lapsed = UploadResumption::from_refused_publication(
            Report::new(ConsensusError::LeadershipLost {
                observed: nervix_consensus::LeadershipObservation::UnconfirmedLease { term: 1 },
            }),
            &id,
        );
        let UploadResumption::Deferred(lapsed) = lapsed else {
            panic!("a leader that cannot confirm it still leads decides nothing: {lapsed:?}");
        };
        assert_eq!(
            format!("{lapsed:#}"),
            "failed to publish resource 'refused@1': this node cannot propose as the leader: this \
             node leads term 1 but could not confirm its lease"
        );

        let conflict = UploadResumption::from_refused_publication(
            Report::new(ConsensusError::Conflict(
                nervix_consensus::ConsensusConflict::Reason("upload changed".to_string()),
            )),
            &id,
        );
        let UploadResumption::Failed(conflict) = conflict else {
            panic!("a conflict with the catalog is final: {conflict:?}");
        };
        assert_eq!(
            format!("{conflict:#}"),
            "failed to publish resource 'refused@1': consensus state changed: upload changed"
        );
    }

    #[derive(Clone, Copy)]
    enum LocalManifestFixture {
        Absent,
        Malformed,
        Mismatched,
    }

    #[rstest::rstest]
    #[case::absent(LocalManifestFixture::Absent, ResourceNodeState::Pending, None)]
    #[case::malformed(
        LocalManifestFixture::Malformed,
        ResourceNodeState::Failed,
        Some("EOF while parsing an object")
    )]
    #[case::mismatched(
        LocalManifestFixture::Mismatched,
        ResourceNodeState::Failed,
        Some("does not match its durable version")
    )]
    #[nervix_primitives::test]
    async fn reconciliation_tracks_local_installation_while_sources_are_unavailable(
        #[case] local_manifest: LocalManifestFixture,
        #[case] expected_state: ResourceNodeState,
        #[case] expected_cause: Option<&str>,
    ) {
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

        match local_manifest {
            LocalManifestFixture::Absent => {}
            LocalManifestFixture::Malformed => {
                let manifest_path = service.inner.resource_store.manifest_path(&resource.id);
                let parent = manifest_path
                    .parent()
                    .assured("the store puts every manifest inside a version directory");
                std::fs::create_dir_all(parent)
                    .assured("the fixture creates its version directory");
                std::fs::write(&manifest_path, "{")
                    .assured("the fixture writes its malformed manifest");
            }
            LocalManifestFixture::Mismatched => {
                let source = path.join("different-content");
                std::fs::create_dir(&source).assured("the fixture creates its source directory");
                std::fs::write(source.join("content"), "x")
                    .assured("the fixture writes its source content");
                service
                    .inner
                    .resource_store
                    .install_from_directory(
                        resource.id.clone(),
                        &source,
                        resource.created_by_node.clone(),
                        resource.created_at,
                    )
                    .await
                    .assured("the production installer creates a valid but different local copy");
            }
        }

        service.reconcile_resources_once().await;
        service.reconcile_resources_once().await;

        let resources = service.inner.consensus.current_resources().await;
        let replica_index = resources
            .replicas
            .binary_search_by(|replica| replica.key.cmp(&local_key))
            .assured("reconciliation publishes the local replica outcome");
        let replica = &resources.replicas[replica_index];
        assert_eq!(replica.state, expected_state);
        match expected_cause {
            Some(cause) => {
                let reason = replica
                    .error
                    .as_deref()
                    .assured("the unusable manifest has a failure reason");
                assert!(
                    reason.contains(cause),
                    "manifest failure cause missing: {reason}"
                );
            }
            None => assert_eq!(replica.error, None),
        }

        if matches!(local_manifest, LocalManifestFixture::Malformed) {
            let description = service
                .describe_resource(
                    &domain,
                    DescribeResource {
                        identifier: resource.id.identifier.clone(),
                        version: Some(resource.id.version),
                    },
                )
                .await;
            assert!(description.succeeded(), "{description:?}");
            let unavailable_entries = description
                .message
                .lines()
                .find(|line| line.starts_with("  - unavailable error="))
                .assured("DESCRIBE reports the local manifest entries as unavailable");
            assert!(
                unavailable_entries.contains("EOF while parsing an object"),
                "DESCRIBE lost the manifest decoding cause: {unavailable_entries}"
            );
        }

        let _ = std::fs::remove_dir_all(&path);
    }

    #[nervix_primitives::test]
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
