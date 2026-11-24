//! Layer: control plane.
//! Owns: admitted inference inspection schemas and their sensitivity at one committed revision.
//! Depends on: owned schedule snapshots, typed configuration and bulk execution admission.
//! Must not know: ONNX values, branch tasks, checkpoint storage or tensor encoding.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns inferencer inspection layouts")
)]

use nervix_execution::{
    Attempt, Cancellation, CpuClass, Executor, GrowingCharge, MemoryClass, Reservation,
};
use nervix_models::CreateInferencer;

use super::*;

/// Lay out an inferencer's inspection sections under a charge for their metadata. A bulk class
/// without room for that charge holds the layout back: it runs again, from the same owned schedule,
/// once the whole charge is free.
pub(super) async fn prepare(
    executor: &Executor,
    mut schedule: ClusterSchedule,
    domain: &DomainName,
    processor: &NodeRef,
) -> Result<ProcessorLayout, StateInspectionRejection> {
    let schedule = schedule
        .domains
        .remove(domain)
        .ok_or(StateInspectionRejection::UnknownEntity)?;
    let processor = processor.clone();
    executor
        .run_cpu_growing(
            CpuClass::Bulk,
            MemoryClass::Bulk,
            1,
            schedule,
            move |schedule, memory, cancellation| {
                if let Err(cancelled) = cancellation.check() {
                    return Attempt::Finished(Err(inspection_refusal(
                        cancelled,
                        StateInspectionRejection::Deadline,
                    )));
                }
                layout(schedule, &processor, memory, cancellation)
            },
        )
        .await
        .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?
}

/// The metadata a layout admitted before it takes the processor's node out of its schedule.
struct MeasuredLayout {
    moving_partitions: Option<nervix_models::ResolvedParametrizerPartitions>,
    sections: Vec<SectionLayout>,
    memory: Reservation,
}

/// How measuring a layout's metadata went: admitted, or short of room in the bulk class.
enum Measured {
    Admitted(MeasuredLayout),
    Short { needed: u64 },
}

/// One attempt at a layout. Only the last step takes the node out of `schedule`, so an attempt
/// short of room hands the schedule back whole.
fn layout(
    mut schedule: DomainSchedule,
    processor: &NodeRef,
    memory: Reservation,
    cancellation: &Cancellation,
) -> Attempt<Result<ProcessorLayout, StateInspectionRejection>, DomainSchedule> {
    let measured = match measure(&schedule, processor, memory, cancellation) {
        Ok(Measured::Admitted(measured)) => measured,
        Ok(Measured::Short { needed }) => {
            return Attempt::Short {
                input: schedule,
                needed,
            };
        }
        Err(rejection) => return Attempt::Finished(Err(rejection)),
    };
    // Move the node from the owned snapshot instead of copying its model and INITIAL expressions.
    let Some(node) = schedule.nodes.swap_remove(processor) else {
        return Attempt::Finished(Err(StateInspectionRejection::UnknownEntity));
    };
    Attempt::Finished(Ok(ProcessorLayout {
        node,
        moving_partitions: measured.moving_partitions,
        sections: measured.sections,
        _metadata: Some(measured.memory),
    }))
}

fn measure(
    schedule: &DomainSchedule,
    processor: &NodeRef,
    memory: Reservation,
    cancellation: &Cancellation,
) -> Result<Measured, StateInspectionRejection> {
    let inferencer = schedule
        .scheduled::<CreateInferencer>(processor.identifier.clone())
        .ok_or(StateInspectionRejection::UnknownEntity)?;
    if inferencer.config.state.is_empty() {
        return Err(StateInspectionRejection::NotMaterialized);
    }
    let relay_name = inferencer
        .config
        .from
        .relays()
        .first()
        .ok_or(StateInspectionRejection::NotReady)?;
    let relay = schedule
        .configured::<CreateRelay>(relay_name)
        .ok_or(StateInspectionRejection::NotReady)?;
    let input = schedule
        .configured::<CreateSchema>(&relay.schema)
        .ok_or(StateInspectionRejection::NotReady)?;
    let (branch, moving_partitions) = match &inferencer.node.resolved_branching {
        Some(ResolvedBranching::Branched {
            parametrizer,
            partitions,
            schema,
            ..
        }) => (
            schema.fields.as_slice(),
            schedule
                .partition_transition_of(parametrizer)
                .map(|_| *partitions),
        ),
        // An inherited branch whose external topology is not yet committed has its key fields
        // but no partitions, so no partition of it is moving.
        Some(ResolvedBranching::AwaitingTopology { schema, .. }) => {
            (schema.fields.as_slice(), None)
        }
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
    let admitted = admitted_sections(
        inferencer.config.state.len(),
        sensitive,
        branch,
        memory,
        cancellation,
    )?;
    let (sections, memory) = match admitted {
        AdmittedSections::Admitted { sections, memory } => (sections, memory),
        AdmittedSections::Short { needed } => return Ok(Measured::Short { needed }),
    };
    Ok(Measured::Admitted(MeasuredLayout {
        moving_partitions,
        sections,
        memory,
    }))
}

/// The declared sections under their charge, or the charge they need when the bulk class has no
/// room for it now.
enum AdmittedSections {
    Admitted {
        sections: Vec<SectionLayout>,
        memory: Reservation,
    },
    Short {
        needed: u64,
    },
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
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        let slot = slot
            .checked_add(std::mem::size_of::<[usize; 2]>())
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        let slot = slot
            .checked_add(field.name.as_str().len())
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
    memory: Reservation,
    cancellation: &Cancellation,
) -> Result<AdmittedSections, StateInspectionRejection> {
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
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    let bytes = bytes
        .checked_add(header_bytes)
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    let bytes = bytes
        .checked_add(slots)
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    let bytes = bytes
        .checked_mul(4)
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    let mut charge = GrowingCharge::new(memory);
    if let InspectionGrowth::Short { needed } = grow_inspection(&mut charge, bytes)? {
        return Ok(AdmittedSections::Short { needed });
    }
    let memory = charge.into_reservation();
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
    Ok(AdmittedSections::Admitted { sections, memory })
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
                let AdmittedSections::Admitted { sections, memory } =
                    result.assured("six tensor sections fit the class")
                else {
                    panic!("an empty bulk class has room for six tensor sections");
                };
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

    /// A bulk class without room for the layout's metadata is backpressure: the attempt reports
    /// the charge it needs rather than refusing the inspection.
    #[nervix_primitives::test]
    async fn a_full_bulk_class_holds_the_layout_back_instead_of_refusing_it() {
        let executor = Executor::default();
        let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
        let memory = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .assured("fixture admission");
        let _held = executor
            .try_reserve(MemoryClass::Bulk, capacity - 1)
            .assured("the test holds the rest of the bulk class");
        let result = executor
            .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
                admitted_sections(6, true, &[], memory, cancellation)
            })
            .await
            .assured("the bounded metadata job completes");
        let AdmittedSections::Short { needed } =
            result.assured("a full class is no reason to refuse the layout")
        else {
            panic!("a full bulk class has no room for the layout's metadata");
        };
        assert!(needed > 1);
    }
}
