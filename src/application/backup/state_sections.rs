//! Converting a node's captured runtime checkpoints into archive-owned state sections.
//!
//! Layer: control plane.
//! - **Owns.** The one conversion from runtime state placements and payloads to public archive
//!   records and raw WASM guest blobs.
//! - **Depends on.** Typed runtime checkpoints, the archive format, and vocabulary identities.
//! - **Must not know.** The runtime's stored key encoding or how the archive reaches a client.

use error_stack::ResultExt as _;
use nervix_backup::{
    ArchiveRecord, BranchLifecycleEntry, BranchLifecycleRecord, KafkaOffsetsRecord,
    KafkaPartitionOffset, SectionContent, SectionPath, StateField, WasmStateDescriptor,
};
use nervix_interconnect::RuntimeState;
use nervix_models::{ClusterNodeName, DomainSchedule, NodeRef};

use super::{BackupError, PlannedContent, PlannedSection};
use crate::runtime::{
    CapturedRuntimeState, decode_backup_branch_lifecycle, decode_backup_kafka_offsets,
};

pub(super) fn plan_state_sections(
    state: Vec<CapturedRuntimeState>,
    schedule: Option<&DomainSchedule>,
    local_node: &ClusterNodeName,
) -> error_stack::Result<Vec<PlannedSection>, BackupError> {
    let mut sections = Vec::new();
    for captured in state {
        let placement = captured.placement;
        let Some(node) = schedule.and_then(|schedule| {
            schedule
                .nodes
                .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
        }) else {
            continue;
        };
        if node.primary_node.as_ref() != Some(local_node) {
            continue;
        }
        if !matches!(placement.state, RuntimeState::KafkaOffset)
            && placement.state.schema()
                != nervix_interconnect::StateSchema::Fingerprinted(node.schema_fingerprint)
        {
            continue;
        }
        if let RuntimeState::WasmProcessor { generation, .. } = placement.state {
            let current = node
                .wasm_state_generations()
                .map(|generations| generations.of_branch(captured.branch_fingerprint.as_ref()));
            if current != Some(generation) {
                continue;
            }
        }
        let domain = placement.domain;
        let entity = placement.identifier;
        match placement.state {
            RuntimeState::WasmProcessor { schema, generation } => {
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
                let path = SectionPath::wasm_guest_blob(
                    &domain,
                    &entity,
                    captured.branch_fingerprint.as_ref(),
                );
                sections.push(held(
                    path,
                    domain,
                    SectionContent::WasmGuestBlob,
                    captured.payload,
                ));
            }
            RuntimeState::KafkaOffset => {
                let offsets = decode_backup_kafka_offsets(&captured.payload)
                    .change_context(BackupError::Encoding)?
                    .into_iter()
                    .map(|(topic, partition, next_offset)| KafkaPartitionOffset {
                        topic,
                        partition,
                        next_offset,
                    })
                    .collect();
                let record = KafkaOffsetsRecord {
                    domain: domain.clone(),
                    entity: entity.clone(),
                    schema: node.schema_fingerprint,
                    revision: captured.revision,
                    offsets,
                };
                sections.push(held(
                    SectionPath::kafka_offsets(&domain, &entity),
                    domain,
                    SectionContent::Record(KafkaOffsetsRecord::KIND),
                    record.encode().change_context(BackupError::Encoding)?,
                ));
            }
            RuntimeState::BranchLru { schema } => {
                let branches = decode_backup_branch_lifecycle(&captured.payload, &entity)
                    .change_context(BackupError::Encoding)?
                    .into_iter()
                    .map(|branch| BranchLifecycleEntry {
                        key: branch.key.map(|fields| {
                            fields.into_iter().map(StateField::from_remote).collect()
                        }),
                        last_ingestion: branch.last_ingestion,
                        incarnation: branch.incarnation,
                    })
                    .collect();
                let record = BranchLifecycleRecord {
                    domain: domain.clone(),
                    owner_kind: placement.kind,
                    entity: entity.clone(),
                    schema,
                    revision: captured.revision,
                    branches,
                };
                sections.push(held(
                    SectionPath::branch_lifecycle(&domain, placement.kind, &entity),
                    domain,
                    SectionContent::Record(BranchLifecycleRecord::KIND),
                    record.encode().change_context(BackupError::Encoding)?,
                ));
            }
            _ => {}
        }
    }
    Ok(sections)
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
