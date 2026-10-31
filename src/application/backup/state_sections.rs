//! Selecting current WASM guest saves and streaming their archive sections one at a time.
//!
//! Layer: control plane.
//! - **Owns.** Checking scheduled guest generations before opening stored payloads, admitting one
//!   save's conversion, and staging its raw bytes and descriptor from the same checkpoint read.
//! - **Depends on.** Typed runtime checkpoints, the archive format, the executor's memory classes,
//!   and vocabulary identities.
//! - **Must not know.** The runtime's stored key encoding or how the archive reaches a client.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use error_stack::{Report, ResultExt as _};
use nervix_backup::{ArchiveRecord, SectionContent, SectionPath, StateField, WasmStateDescriptor};
use nervix_execution::MemoryClass;
use nervix_interconnect::{
    RemoteOperationFailure, RuntimeState, StatePlacementEnvelope, StateSchema,
};
use nervix_models::{ClusterNodeName, DomainName, DomainSchedule, NodeRef, ScheduledNode};

use super::{
    CaptureSectionKey, CapturedSection,
    interconnect::{CaptureDomainStateRequest, failure_context},
};
use crate::{
    application::session_service::SessionServiceImpl,
    runtime::{CapturedGuestSave, SnapshotStagingError, StagedArtifact},
};

/// A segmented checkpoint block, the transfer block and the staged writer's output buffer.
const GUEST_WRITER_BYTES: u64 = 3 * 64 * 1024;

impl SessionServiceImpl {
    /// A save superseded by a scheduled generation never opens its checkpoint payload. Only one
    /// selected save at a time holds a conversion charge, and the descriptor uses the revision
    /// returned by the same read that produced its blob.
    pub(super) async fn stage_guest_state_sections(
        &self,
        captured: Vec<CapturedGuestSave>,
        schedule: Option<&DomainSchedule>,
        request: &CaptureDomainStateRequest,
    ) -> Result<Vec<CapturedSection>, Report<RemoteOperationFailure>> {
        let local_node = self.inner.consensus.local_node_id();
        let mut staged = Vec::new();
        for save in captured {
            nervix_primitives::task::consume_budget().await;
            let Some(node) = archived_node(&save.placement, schedule, local_node) else {
                continue;
            };
            let RuntimeState::WasmProcessor { schema, generation } = save.placement.state else {
                continue;
            };
            let Some(unit) = node.units.get(&save.placement.unit) else {
                continue;
            };
            let Some(generations) = unit.wasm_state_generations.as_ref() else {
                continue;
            };
            if generations.of_branch(save.branch_fingerprint.as_ref()) != generation {
                continue;
            }
            let placement = save.placement.clone();
            let branch_fingerprint = save.branch_fingerprint;
            let (blob, revision) = self.stage_guest_blob(save, &request.domain).await?;
            let descriptor = WasmStateDescriptor {
                domain: placement.domain.clone(),
                entity: placement.identifier.clone(),
                unit: placement.unit,
                schema,
                branch_fingerprint,
                branch: placement
                    .branch_key
                    .map(|fields| fields.into_iter().map(StateField::from_remote).collect()),
                generation,
                revision,
            };
            let descriptor_path = SectionPath::wasm_state_descriptor(
                &placement.domain,
                &placement.identifier,
                placement.unit,
                branch_fingerprint.as_ref(),
            );
            let descriptor_bytes = descriptor.encode().map_err(|error| {
                error.change_context(failure_context(
                    &request.domain,
                    &format!(
                        "guest descriptor of '{}' could not be encoded",
                        placement.identifier.as_str()
                    ),
                ))
            })?;
            let descriptor_artifact = self
                .stage_captured_section(descriptor_bytes, &request.domain)
                .await
                .map_err(|error| {
                    error.change_context(failure_context(
                        &request.domain,
                        &format!(
                            "guest descriptor of '{}' could not be staged",
                            placement.identifier.as_str()
                        ),
                    ))
                })?;
            staged.push(CapturedSection {
                key: CaptureSectionKey {
                    coordination: request.coordination.clone(),
                    domain: request.domain.clone(),
                    path: descriptor_path.as_str().to_string(),
                },
                content: SectionContent::Record(WasmStateDescriptor::KIND),
                artifact: descriptor_artifact,
            });
            let blob_path = SectionPath::wasm_guest_blob(
                &placement.domain,
                &placement.identifier,
                placement.unit,
                branch_fingerprint.as_ref(),
            );
            staged.push(CapturedSection {
                key: CaptureSectionKey {
                    coordination: request.coordination.clone(),
                    domain: request.domain.clone(),
                    path: blob_path.as_str().to_string(),
                },
                content: SectionContent::WasmGuestBlob,
                artifact: blob,
            });
        }
        Ok(staged)
    }

    async fn stage_guest_blob(
        &self,
        save: CapturedGuestSave,
        domain: &DomainName,
    ) -> Result<(StagedArtifact, u64), Report<RemoteOperationFailure>> {
        let executor = self.inner.runtime.executor().clone();
        let entity = save.placement.identifier.clone();
        let (stored_bytes, conversion_bytes) =
            save.size_and_conversion_charge().map_err(|error| {
                error.change_context(failure_context(
                    domain,
                    &format!("guest save of '{}' could not be inspected", entity.as_str()),
                ))
            })?;
        let conversion = executor
            .reserve(MemoryClass::RestoreMetadata, conversion_bytes)
            .await
            .map_err(|error| {
                error.change_context(failure_context(
                    domain,
                    &format!("guest save of '{}' could not be admitted", entity.as_str()),
                ))
            })?;
        let working = executor
            .reserve(MemoryClass::Bulk, GUEST_WRITER_BYTES)
            .await
            .map_err(|error| {
                error.change_context(failure_context(
                    domain,
                    &format!("guest save of '{}' could not be admitted", entity.as_str()),
                ))
            })?;
        let writer = self
            .inner
            .runtime
            .try_stage_artifact(stored_bytes)
            .await
            .map_err(|error| {
                error.change_context(failure_context(
                    domain,
                    &format!("guest save of '{}' could not be staged", entity.as_str()),
                ))
            })?;
        let interruption = self
            .inner
            .runtime
            .guest_save_capture_interruption(domain, &save.placement.identifier);
        writer
            .encode_artifact_with_result(working, move |output, cancellation| {
                let _conversion = conversion;
                save.write(output, cancellation, interruption)
                    .change_context(SnapshotStagingError::Encode)
            })
            .await
            .map_err(|error| {
                error.change_context(failure_context(
                    domain,
                    &format!("guest save of '{}' could not be staged", entity.as_str()),
                ))
            })
    }
}

/// The scheduled node whose state `placement` captured, when this node archives it: this node is
/// the node's primary, and the state carries the node's committed schema or depends on none.
pub(super) fn archived_node<'schedule>(
    placement: &StatePlacementEnvelope,
    schedule: Option<&'schedule DomainSchedule>,
    local_node: &ClusterNodeName,
) -> Option<&'schedule ScheduledNode> {
    let schedule = schedule?;
    let node = schedule
        .nodes
        .get(&NodeRef::new(placement.kind, placement.identifier.clone()))?;
    if node
        .units
        .get(&placement.unit)
        .and_then(|unit| unit.primary_node.as_ref())
        != Some(local_node)
    {
        return None;
    }
    if let StateSchema::Fingerprinted(schema) = placement.state.schema()
        && schema != node.schema_fingerprint
    {
        return None;
    }
    Some(node)
}
