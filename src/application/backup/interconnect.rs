//! Typed node-to-node operations for capturing and installing backup state.
//!
//! Layer: control plane.
//! - **Owns.** Backup drain, capture and inventory handlers, the bulk section fetch, and the
//!   restore state-install handler, all bound to a coordinator process identity.
//! - **Depends on.** The authenticated interconnect contract, shutdown's admitted-work view, and
//!   typed state placements.
//! - **Must not know.** Archive encoding, database keys, or the client's backup destination.

use std::time::Duration;

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
use nervix_models::{DomainName, DomainStatus, RestoreStateAuthority};
use nervix_primitives::{
    sync::{Arc, Mutex as AsyncMutex, watch},
    time::Instant,
};

use super::{
    CaptureSectionKey, CapturedSectionStage, PlannedContent, restore_storage::RestoredStateSource,
    state_sections::plan_state_sections,
};
use crate::{
    application::session_service::SessionServiceImpl,
    runtime::{
        CapturedDomainState, CapturedMaterializedState, RestoredRuntimeState, StagedArtifact,
        StagedSnapshotWriter,
    },
};

/// A receiving node keeps one bounded install in its staging area until all chunks verify.
pub(in crate::application) struct RestoreUploadEntry {
    pub(in crate::application) stage: Arc<AsyncMutex<RestoreUploadStage>>,
    pub(in crate::application) expires_at: Instant,
}

pub(in crate::application) struct RestoreUploadStage {
    domain: DomainName,
    authority: RestoreStateAuthority,
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
    InstallRestoredStateAction, InstallRestoredStateRequest, RestoreStateInventory,
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
        SectionContent::Record(RecordKind::MaterializedRelayDescriptor) => {
            Some(CapturedStateSectionKind::MaterializedDescriptor)
        }
        SectionContent::Record(RecordKind::MaterializedIdentities) => {
            Some(CapturedStateSectionKind::MaterializedIdentities)
        }
        SectionContent::MaterializedColumns => Some(CapturedStateSectionKind::MaterializedColumns),
        SectionContent::Record(RecordKind::DeduplicatorStateDescriptor) => {
            Some(CapturedStateSectionKind::DeduplicatorDescriptor)
        }
        SectionContent::DeduplicatorKeys => Some(CapturedStateSectionKind::DeduplicatorKeys),
        SectionContent::Record(RecordKind::WindowStateDescriptor) => {
            Some(CapturedStateSectionKind::WindowDescriptor)
        }
        SectionContent::WindowInputRows => Some(CapturedStateSectionKind::WindowInputRows),
        SectionContent::WindowArgumentColumns => {
            Some(CapturedStateSectionKind::WindowArgumentColumns)
        }
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
        CapturedStateSectionKind::MaterializedDescriptor => {
            SectionContent::Record(RecordKind::MaterializedRelayDescriptor)
        }
        CapturedStateSectionKind::MaterializedIdentities => {
            SectionContent::Record(RecordKind::MaterializedIdentities)
        }
        CapturedStateSectionKind::MaterializedColumns => SectionContent::MaterializedColumns,
        CapturedStateSectionKind::DeduplicatorDescriptor => {
            SectionContent::Record(RecordKind::DeduplicatorStateDescriptor)
        }
        CapturedStateSectionKind::DeduplicatorKeys => SectionContent::DeduplicatorKeys,
        CapturedStateSectionKind::WindowDescriptor => {
            SectionContent::Record(RecordKind::WindowStateDescriptor)
        }
        CapturedStateSectionKind::WindowInputRows => SectionContent::WindowInputRows,
        CapturedStateSectionKind::WindowArgumentColumns => SectionContent::WindowArgumentColumns,
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
            admitting_relays: status.admitting_relays.arch_into(),
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
        status: DomainStatus,
    ) -> Result<CapturedDomainState, RemoteOperationFailure> {
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
                move |_charge, _cancellation| {
                    runtime.capture_backup_state(&domain_owned, quiesced, status)
                },
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
        wait_for_capture_revision(
            &request.domain,
            request.revision,
            self.inner.consensus.subscribe_applied(),
        )
        .await?;
        self.apply_current_cluster_state()
            .await
            .map_err(|error| failed(&request.domain, &error.to_string()))?;
        if self.inner.consensus.current_leader().await.as_ref() != Some(peer) {
            return Err(failed(
                &request.domain,
                "capture coordinator is not the current leader",
            ));
        }
        let Some(capture) = self.inner.consensus.configuration_capture().await else {
            return Err(failed(&request.domain, "configuration is unavailable"));
        };
        let status = capture
            .domains
            .get(&request.domain)
            .ok_or_else(|| failed(&request.domain, "domain is unavailable"))?
            .status
            .clone();
        let state = self
            .capture_local_state(&request.domain, request.quiesced, status)
            .await?;
        let sections = plan_state_sections(
            state.checkpoints,
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
        match state.materialized {
            CapturedMaterializedState::Current(captured) => {
                staged.extend(
                    self.stage_materialized_sections(
                        captured,
                        capture.schedule.domain(&request.domain),
                        &request,
                    )
                    .await?,
                );
            }
            CapturedMaterializedState::Stored(captured) => {
                staged.extend(
                    self.stage_stored_materialized_sections(
                        captured,
                        capture.schedule.domain(&request.domain),
                        &request,
                    )
                    .await?,
                );
            }
        }
        staged.extend(
            self.stage_branch_state_sections(
                state.branch_states,
                capture.schedule.domain(&request.domain),
                &request,
            )
            .await?,
        );
        self.inner.captured_backup_sections.retain(|key, _| {
            key.coordination != request.coordination || key.domain != request.domain
        });
        let expires_at = Instant::now() + Duration::from_secs(600);
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

    pub(super) async fn stage_captured_section(
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
        if peer != &request.authority.leader {
            return Err(failed(
                &request.domain,
                "state installer differs from the admitted leader",
            ));
        }
        self.wait_for_restore_installation_revision(&request.authority, &request.domain)
            .await?;
        self.inner
            .consensus
            .with_restore_state_installation(&request.domain, &request.authority, || ())
            .map_err(|error| failed(&request.domain, &error.to_string()))?;
        match request.action {
            InstallRestoredStateAction::Publish { inventory } => {
                self.publish_restored_state_generation(
                    &request.domain,
                    &request.authority,
                    inventory,
                )
                .await
            }
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
                            authority: request.authority,
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
                if stage.domain != request.domain
                    || stage.authority != request.authority
                    || stage.next_offset != offset
                {
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
                if stage.domain != request.domain
                    || stage.authority != request.authority
                    || stage.next_offset != stage.length
                {
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
                self.stage_restored_state_checkpoint(
                    &request.authority,
                    RestoredRuntimeState {
                        placement: stage.placement.clone(),
                        branch_fingerprint: stage
                            .branch_fingerprint
                            .map(nervix_models::BranchKeyFingerprint::new),
                        revision: stage.revision,
                        length: stage.length,
                        digest: stage.digest,
                    },
                    RestoredStateSource::Upload(artifact),
                )
                .await
            }
        }
    }

    async fn wait_for_restore_installation_revision(
        &self,
        authority: &RestoreStateAuthority,
        domain: &DomainName,
    ) -> Result<(), RemoteOperationFailure> {
        let mut applied = self.inner.consensus.subscribe_applied();
        nervix_primitives::time::timeout(Duration::from_secs(5), async {
            loop {
                nervix_primitives::task::consume_budget().await;
                if *applied.borrow_and_update() >= authority.generation {
                    return Ok(());
                }
                applied
                    .changed()
                    .await
                    .map_err(|_| failed(domain, "restore installation authority is unavailable"))?;
            }
        })
        .await
        .map_err(|_| failed(domain, "restore installation revision has not applied"))?
    }

    pub(in crate::application) fn sweep_captured_backup_sections(&self) {
        let now = Instant::now();
        self.inner
            .captured_backup_sections
            .retain(|_, stage| stage.expires_at > now);
        self.inner
            .restored_state_uploads
            .retain(|_, entry| entry.expires_at > now);
    }
}

pub(super) fn failed(domain: &DomainName, reason: &str) -> RemoteOperationFailure {
    RemoteOperationFailure::failed(RemoteOperationSubject::domain(domain), reason.to_string())
}

async fn wait_for_capture_revision(
    domain: &DomainName,
    revision: u64,
    mut applied: watch::Receiver<u64>,
) -> Result<(), RemoteOperationFailure> {
    nervix_primitives::time::timeout(Duration::from_secs(5), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if *applied.borrow_and_update() >= revision {
                return Ok(());
            }
            applied
                .changed()
                .await
                .map_err(|_| failed(domain, "capture revision authority is unavailable"))?;
        }
    })
    .await
    .map_err(|_| {
        failed(
            domain,
            "owner has not applied the cut revision within its deadline",
        )
    })?
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::task::Poll;

    use meticulous::ResultExt as _;

    use super::*;

    #[nervix_primitives::test]
    async fn capture_waits_until_the_selected_revision_is_applied() {
        let domain = DomainName::parse("capture_wait").assured("domain is valid");
        let (sender, applied) = watch::channel(7);
        let mut capture = Box::pin(wait_for_capture_revision(&domain, 9, applied));
        assert!(matches!(futures_util::poll!(&mut capture), Poll::Pending));
        sender.send(8).assured("capture retains its receiver");
        assert!(matches!(futures_util::poll!(&mut capture), Poll::Pending));
        sender.send(9).assured("capture retains its receiver");
        capture
            .await
            .assured("the selected revision permits capture");
    }

    #[nervix_primitives::test]
    async fn capture_refuses_when_the_applied_revision_owner_ends() {
        let domain = DomainName::parse("capture_closed").assured("domain is valid");
        let (sender, applied) = watch::channel(7);
        drop(sender);
        let failure = wait_for_capture_revision(&domain, 9, applied)
            .await
            .err()
            .assured("a closed authority cannot reach the selected revision");
        assert!(
            failure
                .to_string()
                .contains("capture revision authority is unavailable")
        );
    }

    #[nervix_primitives::test]
    async fn capture_refuses_when_the_selected_revision_never_applies() {
        let domain = DomainName::parse("capture_deadline").assured("domain is valid");
        let (_sender, applied) = watch::channel(7);
        let failure = wait_for_capture_revision(&domain, 9, applied)
            .await
            .err()
            .assured("an owner that does not catch up reaches its deadline");
        assert!(failure.to_string().contains("within its deadline"));
    }

    #[test]
    fn every_captured_state_kind_names_its_archive_content_and_back() {
        let kinds = [
            CapturedStateSectionKind::WasmDescriptor,
            CapturedStateSectionKind::KafkaOffsets,
            CapturedStateSectionKind::BranchLifecycle,
            CapturedStateSectionKind::WasmGuestBlob,
            CapturedStateSectionKind::MaterializedDescriptor,
            CapturedStateSectionKind::MaterializedIdentities,
            CapturedStateSectionKind::MaterializedColumns,
            CapturedStateSectionKind::DeduplicatorDescriptor,
            CapturedStateSectionKind::DeduplicatorKeys,
            CapturedStateSectionKind::WindowDescriptor,
            CapturedStateSectionKind::WindowInputRows,
            CapturedStateSectionKind::WindowArgumentColumns,
        ];
        for kind in kinds {
            assert_eq!(section_kind(section_content(kind)), Some(kind));
        }
        assert_eq!(
            section_kind(SectionContent::Nspl),
            None,
            "configuration never travels as captured runtime state"
        );
    }
}
