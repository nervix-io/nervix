//! Typed node-to-node operations for capturing and installing backup state.
//!
//! Layer: control plane.
//! - **Owns.** Backup drain, capture and inventory handlers, the bulk section fetch, and the
//!   restore state-install handler, all bound to a coordinator process identity.
//! - **Depends on.** The authenticated interconnect contract, shutdown's admitted-work view, and
//!   typed state placements.
//! - **Must not know.** Archive encoding, database keys, or the client's backup destination.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arch_into::ArchInto as _;
use error_stack::Report;
use futures_util::stream;
use meticulous::OptionExt as _;
use nervix_backup::{RecordKind, SectionContent};
use nervix_execution::{ChargedBytes, MemoryClass};
use nervix_interconnect::{
    HandlerRegistrationError, RemoteOperationFailure, RemoteOperationSubject,
    StatePlacementEnvelope, StreamHandlerError, StreamingResponse, Transport,
};
use nervix_models::DomainName;
use tokio::sync::Mutex as AsyncMutex;

use super::{
    CaptureSectionKey, CapturedSectionStage, PlannedContent, state_sections::plan_state_sections,
};
use crate::{
    application::session_service::SessionServiceImpl,
    runtime::{CapturedRuntimeState, StagedArtifact, StagedSnapshotWriter},
};

/// A receiving node keeps one bounded install in its staging area until all chunks verify.
pub(in crate::application) struct RestoreUploadEntry {
    pub(in crate::application) stage: Arc<AsyncMutex<RestoreUploadStage>>,
    pub(in crate::application) expires_at: Instant,
}

pub(in crate::application) struct RestoreUploadStage {
    domain: DomainName,
    placement: StatePlacementEnvelope,
    branch_fingerprint: Option<[u8; 32]>,
    revision: u64,
    length: u64,
    digest: [u8; 32],
    next_offset: u64,
    writer: Option<StagedSnapshotWriter>,
}

use nervix_interconnect::backup::CapturedStateSectionKind;
pub(in crate::application) use nervix_interconnect::backup::{
    BackupDrainAction, BackupDrainStatus, BackupDrainStatusRequest, CaptureDomainStateRequest,
    CaptureInventoryRequest, CapturedSectionInventory, FetchCapturedSection,
    InstallRestoredStateAction, InstallRestoredStateRequest,
};

fn section_kind(content: SectionContent) -> Option<CapturedStateSectionKind> {
    match content {
        SectionContent::Record(RecordKind::WasmStateDescriptor) => {
            Some(CapturedStateSectionKind::WasmDescriptor)
        }
        SectionContent::Record(RecordKind::KafkaOffsets) => {
            Some(CapturedStateSectionKind::KafkaOffsets)
        }
        SectionContent::Record(RecordKind::BranchLifecycle) => {
            Some(CapturedStateSectionKind::BranchLifecycle)
        }
        SectionContent::WasmGuestBlob => Some(CapturedStateSectionKind::WasmGuestBlob),
        _ => None,
    }
}

pub(super) fn section_content(kind: CapturedStateSectionKind) -> SectionContent {
    match kind {
        CapturedStateSectionKind::WasmDescriptor => {
            SectionContent::Record(RecordKind::WasmStateDescriptor)
        }
        CapturedStateSectionKind::KafkaOffsets => SectionContent::Record(RecordKind::KafkaOffsets),
        CapturedStateSectionKind::BranchLifecycle => {
            SectionContent::Record(RecordKind::BranchLifecycle)
        }
        CapturedStateSectionKind::WasmGuestBlob => SectionContent::WasmGuestBlob,
    }
}

impl SessionServiceImpl {
    pub(crate) fn register_backup_state_handlers(
        &self,
        interconnect: &Transport,
    ) -> Result<(), Report<HandlerRegistrationError>> {
        let drain = self.clone();
        interconnect.register_handler::<BackupDrainStatusRequest, _, _>(
            move |context, request| {
                let service = drain.clone();
                let peer = context.peer_node_id().clone();
                async move {
                    service
                        .handle_backup_drain_status_request(&peer, request)
                        .await
                }
            },
        )?;
        let capture = self.clone();
        interconnect.register_handler::<CaptureDomainStateRequest, _, _>(
            move |context, request| {
                let service = capture.clone();
                let peer = context.peer_node_id().clone();
                async move { service.handle_backup_capture_request(&peer, request).await }
            },
        )?;
        let inventory = self.clone();
        interconnect.register_handler::<CaptureInventoryRequest, _, _>(
            move |context, request| {
                let service = inventory.clone();
                let peer = context.peer_node_id().clone();
                async move {
                    service
                        .handle_backup_inventory_request(&peer, request)
                        .await
                }
            },
        )?;
        let fetch = self.clone();
        interconnect.register_stream_handler::<FetchCapturedSection, _, _>(
            move |context, request| {
                let service = fetch.clone();
                let peer = context.peer_node_id().clone();
                async move {
                    if service.inner.consensus.current_leader().await.as_ref() != Some(&peer) {
                        return Err(Report::new(StreamHandlerError::new(
                            "capture coordinator is not the current leader",
                        )));
                    }
                    let artifact = service
                        .take_captured_backup_section(&peer, &request)
                        .ok_or_else(|| {
                            StreamHandlerError::new("captured state section is unavailable")
                        })?;
                    let reader = artifact
                        .open_reader()
                        .await
                        .map_err(StreamHandlerError::with_cause)?;
                    let length = artifact.length();
                    let chunks = stream::unfold(Some((reader, artifact)), |state| async move {
                        let (mut reader, artifact) = state?;
                        match reader.next_chunk(1024 * 1024).await {
                            Ok(Some(chunk)) => Some((Ok(chunk), Some((reader, artifact)))),
                            Ok(None) => None,
                            Err(error) => Some((Err(StreamHandlerError::with_cause(error)), None)),
                        }
                    });
                    Ok(StreamingResponse::new(length, chunks))
                }
            },
        )?;
        let install = self.clone();
        interconnect.register_handler::<InstallRestoredStateRequest, _, _>(
            move |context, request| {
                let service = install.clone();
                let peer = context.peer_node_id().clone();
                async move {
                    service
                        .handle_restore_state_install_request(&peer, request)
                        .await
                }
            },
        )
    }

    pub(crate) async fn handle_backup_drain_status_request(
        &self,
        peer: &nervix_models::ClusterNodeName,
        request: BackupDrainStatusRequest,
    ) -> Result<BackupDrainStatus, RemoteOperationFailure> {
        if self.inner.consensus.current_leader().await.as_ref() != Some(peer) {
            return Err(failed(
                &request.domain,
                "backup drain coordinator is not the current leader",
            ));
        }
        self.apply_current_cluster_state()
            .await
            .map_err(|error| failed(&request.domain, &error.to_string()))?;
        match request.action {
            BackupDrainAction::FlushIfIdle => {
                self.inner
                    .runtime
                    .force_flush_domain_if_idle(&request.domain);
            }
            BackupDrainAction::Confirm => {
                self.inner.runtime.force_flush_domain(&request.domain);
            }
            BackupDrainAction::Observe => {}
        }
        let status = self
            .inner
            .runtime
            .local_domain_drain_status(&request.domain);
        Ok(BackupDrainStatus {
            admitting_ingestors: status.admitting_ingestors.arch_into(),
            active_generators: status.active_generators.arch_into(),
            admitted_acks: status.outstanding_acks.arch_into(),
            buffered_relay_batches: status.buffered_relay_batches.arch_into(),
            node_work_items: status.node_work_items.arch_into(),
            buffered_emitter_messages: status.buffered_emitter_messages.arch_into(),
            publishing_emitters: status.publishing_emitters.arch_into(),
            parked_required_waits: status.required_waits.arch_into(),
            force_flush_obligations: status.force_flush_obligations.arch_into(),
        })
    }

    pub(super) async fn capture_local_state(
        &self,
        domain: &DomainName,
        quiesced: bool,
    ) -> Result<Vec<CapturedRuntimeState>, RemoteOperationFailure> {
        let runtime = self.inner.runtime.clone();
        if quiesced {
            runtime
                .checkpoint_backup_branch_lifecycles(domain)
                .await
                .map_err(|error| failed(domain, &format!("{error:#}")))?;
        }
        let domain_owned = domain.clone();
        let executor = runtime.executor().clone();
        let reservation = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .map_err(|_| failed(domain, "state capture admission failed"))?;
        let read = executor
            .run_storage(
                nervix_execution::StorageClass::Filesystem,
                reservation,
                move |_charge, _cancellation| runtime.capture_backup_state(&domain_owned, quiesced),
            )
            .await
            .map_err(|_| failed(domain, "state capture execution failed"))?;
        read.map_err(|error| failed(domain, &error.to_string()))
    }

    pub(crate) async fn handle_backup_capture_request(
        &self,
        peer: &nervix_models::ClusterNodeName,
        request: CaptureDomainStateRequest,
    ) -> Result<(), RemoteOperationFailure> {
        if self.inner.consensus.current_leader().await.as_ref() != Some(peer) {
            return Err(failed(
                &request.domain,
                "capture coordinator is not the current leader",
            ));
        }
        let Some(capture) = self.inner.consensus.configuration_capture().await else {
            return Err(failed(&request.domain, "configuration is unavailable"));
        };
        if capture.applied.index < request.revision {
            return Err(failed(
                &request.domain,
                "owner has not applied the cut revision",
            ));
        }
        let state = self
            .capture_local_state(&request.domain, request.quiesced)
            .await?;
        let sections = plan_state_sections(
            state,
            capture.schedule.domain(&request.domain),
            self.inner.consensus.local_node_id(),
        )
        .map_err(|error| failed(&request.domain, &error.to_string()))?;
        let mut staged = Vec::with_capacity(sections.len());
        for section in sections {
            let PlannedContent::Held { content, bytes } = section.content else {
                continue;
            };
            let artifact = self.stage_captured_section(bytes, &request.domain).await?;
            staged.push((
                CaptureSectionKey {
                    coordination: request.coordination.clone(),
                    domain: request.domain.clone(),
                    path: section.path.as_str().to_string(),
                },
                content,
                artifact,
            ));
        }
        self.inner.captured_backup_sections.retain(|key, _| {
            key.coordination != request.coordination || key.domain != request.domain
        });
        let expires_at = std::time::Instant::now() + Duration::from_secs(600);
        for (key, content, artifact) in staged {
            self.inner.captured_backup_sections.insert(
                key,
                CapturedSectionStage {
                    artifact: Arc::new(artifact),
                    content,
                    expires_at,
                },
            );
        }
        Ok(())
    }

    async fn stage_captured_section(
        &self,
        bytes: Vec<u8>,
        domain: &DomainName,
    ) -> Result<StagedArtifact, RemoteOperationFailure> {
        let length =
            u64::try_from(bytes.len()).map_err(|_| failed(domain, "state section is too large"))?;
        let mut writer = self
            .inner
            .runtime
            .try_stage_artifact(length)
            .await
            .map_err(|_| failed(domain, "state staging quota is full"))?;
        if !bytes.is_empty() {
            let charge = self
                .inner
                .runtime
                .executor()
                .reserve(MemoryClass::Bulk, length)
                .await
                .map_err(|_| failed(domain, "state section admission failed"))?;
            writer
                .write_chunk(ChargedBytes::from_owned(bytes, charge))
                .await
                .map_err(|_| failed(domain, "state section staging failed"))?;
        }
        writer
            .finish_artifact()
            .await
            .map_err(|_| failed(domain, "state section staging failed"))
    }

    pub(crate) async fn handle_backup_inventory_request(
        &self,
        peer: &nervix_models::ClusterNodeName,
        request: CaptureInventoryRequest,
    ) -> Result<Vec<CapturedSectionInventory>, RemoteOperationFailure> {
        if self.inner.consensus.current_leader().await.as_ref() != Some(peer) {
            return Err(failed(
                &request.domain,
                "inventory coordinator is not the current leader",
            ));
        }
        let mut sections = self
            .inner
            .captured_backup_sections
            .iter()
            .filter(|entry| {
                entry.key().coordination == request.coordination
                    && entry.key().domain == request.domain
            })
            .map(|entry| CapturedSectionInventory {
                path: entry.key().path.clone(),
                length: entry.value().artifact.length(),
                digest: entry.value().artifact.digest(),
                kind: section_kind(entry.value().content)
                    .verified("capture staging holds only runtime state sections"),
            })
            .collect::<Vec<_>>();
        sections.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(sections)
    }

    pub(crate) fn take_captured_backup_section(
        &self,
        peer: &nervix_models::ClusterNodeName,
        request: &FetchCapturedSection,
    ) -> Option<Arc<StagedArtifact>> {
        if request.coordination.coordinator() != peer {
            return None;
        }
        let key = CaptureSectionKey {
            coordination: request.coordination.clone(),
            domain: request.domain.clone(),
            path: request.path.clone(),
        };
        self.inner
            .captured_backup_sections
            .remove(&key)
            .map(|(_, stage)| stage.artifact)
    }

    pub(crate) async fn handle_restore_state_install_request(
        &self,
        peer: &nervix_models::ClusterNodeName,
        request: InstallRestoredStateRequest,
    ) -> Result<(), RemoteOperationFailure> {
        if self.inner.consensus.current_leader().await.as_ref() != Some(peer) {
            return Err(failed(
                &request.domain,
                "state installer is not the current leader",
            ));
        }
        match request.action {
            InstallRestoredStateAction::PurgeDomain => self
                .inner
                .runtime
                .purge_restored_domain_state(&request.domain)
                .map_err(|error| failed(&request.domain, &error.to_string())),
            InstallRestoredStateAction::Begin {
                placement,
                branch_fingerprint,
                revision,
                length,
                digest,
            } => {
                if placement.domain != request.domain {
                    return Err(failed(
                        &request.domain,
                        "restored placement belongs to another domain",
                    ));
                }
                let writer = self
                    .inner
                    .runtime
                    .try_stage_artifact(length)
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                self.inner.restored_state_uploads.insert(
                    request.coordination,
                    RestoreUploadEntry {
                        stage: Arc::new(AsyncMutex::new(RestoreUploadStage {
                            domain: request.domain,
                            placement,
                            branch_fingerprint,
                            revision,
                            length,
                            digest,
                            next_offset: 0,
                            writer: Some(writer),
                        })),
                        expires_at: Instant::now() + Duration::from_secs(600),
                    },
                );
                Ok(())
            }
            InstallRestoredStateAction::Chunk { offset, payload } => {
                if payload.is_empty() || payload.len() > 64 * 1024 {
                    return Err(failed(
                        &request.domain,
                        "restored state chunk has an invalid length",
                    ));
                }
                let entry = self
                    .inner
                    .restored_state_uploads
                    .get(&request.coordination)
                    .ok_or_else(|| {
                        failed(&request.domain, "restored state upload was not started")
                    })?;
                let stage = Arc::clone(&entry.stage);
                drop(entry);
                let mut stage = stage.lock().await;
                if stage.domain != request.domain || stage.next_offset != offset {
                    return Err(failed(
                        &request.domain,
                        "restored state upload offset differs",
                    ));
                }
                let length = u64::try_from(payload.len()).map_err(|_| {
                    failed(
                        &request.domain,
                        "restored state chunk length exceeds address space",
                    )
                })?;
                if offset
                    .checked_add(length)
                    .is_none_or(|end| end > stage.length)
                {
                    return Err(failed(
                        &request.domain,
                        "restored state chunk exceeds declared length",
                    ));
                }
                let charge = self
                    .inner
                    .runtime
                    .executor()
                    .reserve(MemoryClass::Bulk, length)
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                let writer = stage.writer.as_mut().ok_or_else(|| {
                    failed(&request.domain, "restored state upload is already sealed")
                })?;
                writer
                    .write_chunk(ChargedBytes::from_owned(payload, charge))
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                stage.next_offset += length;
                Ok(())
            }
            InstallRestoredStateAction::Finish => {
                let (_, entry) = self
                    .inner
                    .restored_state_uploads
                    .remove(&request.coordination)
                    .ok_or_else(|| {
                        failed(&request.domain, "restored state upload was not started")
                    })?;
                let mut stage = entry.stage.lock().await;
                if stage.domain != request.domain || stage.next_offset != stage.length {
                    return Err(failed(
                        &request.domain,
                        "restored state upload is incomplete",
                    ));
                }
                let writer = stage.writer.take().ok_or_else(|| {
                    failed(&request.domain, "restored state upload is already sealed")
                })?;
                let artifact = writer
                    .finish_artifact()
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                if artifact.length() != stage.length || artifact.digest() != stage.digest {
                    return Err(failed(
                        &request.domain,
                        "restored state upload digest differs",
                    ));
                }
                let _charge = self
                    .inner
                    .runtime
                    .executor()
                    .reserve(MemoryClass::Bulk, stage.length.max(1))
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                let capacity = usize::try_from(stage.length).map_err(|_| {
                    failed(
                        &request.domain,
                        "restored state length exceeds address space",
                    )
                })?;
                let mut payload = Vec::with_capacity(capacity);
                let mut reader = artifact
                    .open_reader()
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                while let Some(chunk) = reader
                    .next_chunk(64 * 1024)
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?
                {
                    payload.extend_from_slice(chunk.as_ref());
                }
                self.inner
                    .runtime
                    .install_restored_domain_state(CapturedRuntimeState {
                        placement: stage.placement.clone(),
                        branch_fingerprint: stage
                            .branch_fingerprint
                            .map(nervix_models::BranchKeyFingerprint::new),
                        revision: stage.revision,
                        payload,
                    })
                    .map_err(|error| failed(&request.domain, &error.to_string()))
            }
        }
    }

    pub(in crate::application) fn sweep_captured_backup_sections(&self) {
        let now = std::time::Instant::now();
        self.inner
            .captured_backup_sections
            .retain(|_, stage| stage.expires_at > now);
        self.inner
            .restored_state_uploads
            .retain(|_, entry| entry.expires_at > now);
    }
}

fn failed(domain: &DomainName, reason: &str) -> RemoteOperationFailure {
    RemoteOperationFailure::failed(RemoteOperationSubject::domain(domain), reason.to_string())
}
