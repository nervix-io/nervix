//! Converting a node's captured WASM guest saves into archive-owned state sections, and choosing
//! which captured state a node archives.
//!
//! Layer: control plane.
//! - **Owns.** The one conversion from captured guest-save placements and payloads to public WASM
//!   descriptor records and raw guest blobs, and the rule that a node archives the state it is the
//!   scheduled primary of, under its entity's committed schema.
//! - **Depends on.** Typed runtime checkpoints, the archive format, and vocabulary identities.
//! - **Must not know.** The runtime's stored key encoding or how the archive reaches a client.

use error_stack::ResultExt as _;
use nervix_backup::{ArchiveRecord, SectionContent, SectionPath, StateField, WasmStateDescriptor};
use nervix_interconnect::{RuntimeState, StatePlacementEnvelope, StateSchema};
use nervix_models::{ClusterNodeName, DomainSchedule, NodeRef, ScheduledNode};

use super::{BackupError, PlannedContent, PlannedSection};
use crate::runtime::CapturedGuestSave;

pub(super) fn plan_state_sections(
    state: Vec<CapturedGuestSave>,
    schedule: Option<&DomainSchedule>,
    local_node: &ClusterNodeName,
) -> error_stack::Result<Vec<PlannedSection>, BackupError> {
    let mut sections = Vec::new();
    for captured in state {
        let placement = captured.placement;
        let Some(node) = archived_node(&placement, schedule, local_node) else {
            continue;
        };
        let RuntimeState::WasmProcessor { schema, generation } = placement.state else {
            continue;
        };
        let current = node
            .wasm_state_generations()
            .map(|generations| generations.of_branch(captured.branch_fingerprint.as_ref()));
        if current != Some(generation) {
            continue;
        }
        let domain = placement.domain;
        let entity = placement.identifier;
        let descriptor = WasmStateDescriptor {
            domain: domain.clone(),
            entity: entity.clone(),
            schema,
            branch_fingerprint: captured.branch_fingerprint,
            branch: placement
                .branch_key
                .map(|fields| fields.into_iter().map(StateField::from_remote).collect()),
            generation,
            revision: captured.revision,
        };
        let path = SectionPath::wasm_state_descriptor(
            &domain,
            &entity,
            captured.branch_fingerprint.as_ref(),
        );
        sections.push(held(
            path,
            domain.clone(),
            SectionContent::Record(WasmStateDescriptor::KIND),
            descriptor.encode().change_context(BackupError::Encoding)?,
        ));
        let path =
            SectionPath::wasm_guest_blob(&domain, &entity, captured.branch_fingerprint.as_ref());
        sections.push(held(
            path,
            domain,
            SectionContent::WasmGuestBlob,
            captured.payload,
        ));
    }
    Ok(sections)
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
    if node.primary_node.as_ref() != Some(local_node) {
        return None;
    }
    if let StateSchema::Fingerprinted(schema) = placement.state.schema()
        && schema != node.schema_fingerprint
    {
        return None;
    }
    Some(node)
}

fn held(
    path: SectionPath,
    domain: nervix_models::DomainName,
    content: SectionContent,
    bytes: Vec<u8>,
) -> PlannedSection {
    PlannedSection {
        path,
        domain: Some(domain),
        content: PlannedContent::Held { content, bytes },
    }
}
