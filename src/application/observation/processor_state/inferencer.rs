//! Layer: control plane.
//! Owns: admitted inference inspection schemas and their sensitivity at one committed revision.
//! Depends on: owned schedule snapshots, typed configuration and bulk execution admission.
//! Must not know: ONNX values, branch tasks, checkpoint storage or tensor encoding.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns inferencer inspection layouts")
)]

use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass, Reservation};
use nervix_models::CreateInferencer;

use super::*;

pub(super) async fn prepare(
    executor: &Executor,
    mut schedule: ClusterSchedule,
    domain: &DomainName,
    processor: &NodeRef,
) -> Result<ProcessorLayout, StateInspectionRejection> {
    let memory = executor
        .try_reserve(MemoryClass::Bulk, 1)
        .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
    let domain = domain.clone();
    let processor = processor.clone();
    executor
        .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
            cancellation
                .check()
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
            let schedule = schedule
                .domains
                .remove(&domain)
                .ok_or(StateInspectionRejection::UnknownEntity)?;
            layout(schedule, &processor, memory, cancellation)
        })
        .await
        .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?
}

fn layout(
    mut schedule: DomainSchedule,
    processor: &NodeRef,
    memory: Reservation,
    cancellation: &Cancellation,
) -> Result<ProcessorLayout, StateInspectionRejection> {
    let inferencer = schedule
        .scheduled::<CreateInferencer>(processor.identifier.clone())
        .ok_or(StateInspectionRejection::UnknownEntity)?;
    if inferencer.config.state.is_empty() {
        return Err(StateInspectionRejection::NotMaterialized);
    }
    let relay = inferencer
        .config
        .from
        .relays()
        .first()
        .and_then(|relay| schedule.configured::<CreateRelay>(relay))
        .ok_or(StateInspectionRejection::NotReady)?;
    let input = schedule
        .configured::<CreateSchema>(&relay.schema)
        .ok_or(StateInspectionRejection::NotReady)?;
    let (branch, moving_partitions) = match &inferencer.node.resolved_branching {
        Some(ResolvedBranching::Branched {
            branch,
            partitions,
            schema,
            ..
        }) => (
            schema.fields.as_slice(),
            schedule
                .partition_transition_of(branch)
                .map(|_| *partitions),
        ),
        Some(ResolvedBranching::Unbranched) | None => (&[][..], None),
    };
    let mut sensitive = false;
    for field in input.fields.iter().chain(branch) {
        cancellation
            .check()
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
        if field.sensitive {
            sensitive = true;
            break;
        }
    }
    let (sections, memory) = admitted_sections(
        inferencer.config.state.len(),
        sensitive,
        branch,
        memory,
        cancellation,
    )?;
    // Move the node from the owned snapshot instead of copying its model and INITIAL expressions.
    let node = schedule
        .nodes
        .swap_remove(processor)
        .ok_or(StateInspectionRejection::UnknownEntity)?;
    Ok(ProcessorLayout {
        node,
        moving_partitions,
        sections,
        _metadata: Some(memory),
    })
}

fn schema_bytes(
    fields: &[SchemaField],
    cancellation: &Cancellation,
) -> Result<u64, StateInspectionRejection> {
    let mut bytes = 0_u64;
    for field in fields {
        cancellation
            .check()
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
        let slot = std::mem::size_of::<SchemaField>()
            .checked_add(std::mem::size_of::<String>())
            .and_then(|size| size.checked_add(std::mem::size_of::<[usize; 2]>()))
            .and_then(|size| size.checked_add(field.name.as_str().len()))
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        bytes = bytes
            .checked_add(u64::try_from(slot).assured("resident field sizes fit u64"))
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        let mut ty = &field.ty;
        loop {
            cancellation
                .check()
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
            bytes = bytes
                .checked_add(
                    u64::try_from(std::mem::size_of::<ParseAsType>())
                        .assured("a type slot fits u64"),
                )
                .ok_or(StateInspectionRejection::ReplyTooLarge)?;
            match ty {
                ParseAsType::Array { element, .. } | ParseAsType::Vec { element } => ty = element,
                _ => break,
            }
        }
    }
    Ok(bytes)
}

fn admitted_sections(
    entries: usize,
    sensitive: bool,
    branch: &[SchemaField],
    mut memory: Reservation,
    cancellation: &Cancellation,
) -> Result<(Vec<SectionLayout>, Reservation), StateInspectionRejection> {
    let header = ProcessorStateSection::Header.inferencer_fields(false);
    let tensor = ProcessorStateSection::Tensor(0).inferencer_fields(sensitive);
    let branch_bytes = schema_bytes(branch, cancellation)?;
    let header_bytes = schema_bytes(&header, cancellation)?
        .checked_add(branch_bytes)
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    let tensor_bytes = schema_bytes(&tensor, cancellation)?
        .checked_add(branch_bytes)
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    let count = entries
        .checked_add(1)
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    let slots = u64::try_from(std::mem::size_of::<SectionLayout>())
        .assured("a section slot fits u64")
        .checked_mul(u64::try_from(count).assured("a resident declaration count fits u64"))
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    // Retain the layout and cover the working schema copies made by projection and Row encoding.
    let bytes = tensor_bytes
        .checked_mul(u64::try_from(entries).assured("a resident declaration count fits u64"))
        .and_then(|bytes| bytes.checked_add(header_bytes))
        .and_then(|bytes| bytes.checked_add(slots))
        .and_then(|bytes| bytes.checked_mul(4))
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    memory
        .grow_to(bytes)
        .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
    let mut sections = Vec::with_capacity(count);
    sections.push(SectionLayout {
        section: ProcessorStateSection::Header,
        payload: header,
        sensitive_keys: Vec::new(),
    });
    for index in 0..entries {
        cancellation
            .check()
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
        let index = u32::try_from(index)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::ReplyTooLarge))?;
        let section = ProcessorStateSection::Tensor(index);
        sections.push(SectionLayout {
            section,
            payload: tensor.clone(),
            sensitive_keys: Vec::new(),
        });
    }
    Ok((sections, memory))
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
mod tests {
    use super::*;

    #[nervix_primitives::test]
    async fn inferencer_state_inspection_layout_admits_metadata_before_allocating_declared_sections()
     {
        let executor = Executor::default();
        let too_many = usize::try_from(u32::MAX).assured("the native target holds a u32 count");
        for (entries, admitted) in [(6, true), (too_many, false)] {
            let memory = executor
                .try_reserve(MemoryClass::Bulk, 1)
                .assured("fixture admission");
            let result = executor
                .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
                    admitted_sections(entries, true, &[], memory, cancellation)
                })
                .await
                .assured("the bounded metadata job completes");
            if admitted {
                let (sections, memory) = result.assured("six tensor sections fit the class");
                assert_eq!(sections.len(), 7);
                assert!(memory.bytes() > 1);
                for section in sections.iter().skip(1) {
                    assert!(section.payload[3].sensitive);
                }
            } else {
                assert!(matches!(result, Err(StateInspectionRejection::NotReady)));
            }
        }
    }
}
