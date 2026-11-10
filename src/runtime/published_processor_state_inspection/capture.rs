//! Capturing and opening one immutable generation per selected processor branch.
//!
//! Layer: data plane.
//! - **Owns.** Capturing and opening one immutable generation per selected processor branch.
//! - **Depends on.** Published processor snapshots, typed execution plans and bulk admission.
//! - **Must not know.** Models, Plexus, sessions or presentation formats.

use super::*;

impl ReadPlan {
    fn prepare(
        operation: &RelayProcessorOperationTemplate,
        memory: &mut Reservation,
        cancellation: &Cancellation,
    ) -> ReadResult<Self> {
        let plan = match operation {
            RelayProcessorOperationTemplate::Deduplicator {
                compiled_key_program: Some(program),
                max_time,
                ..
            } => {
                let end = program
                    .key_column_offset
                    .checked_add(program.key_count)
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
                reserve_bytes(
                    memory,
                    u64::try_from(program.key_count)
                        .assured("a prepared key count fits u64")
                        .checked_mul(256)
                        .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?,
                )?;
                let fields = program
                    .program
                    .output_schema
                    .fields()
                    .get(program.key_column_offset..end)
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?
                    .to_vec();
                ReadPlan::Keys {
                    fields,
                    max_time: *max_time,
                }
            }
            RelayProcessorOperationTemplate::WindowProcessor {
                input_schema,
                plan,
                width_duration,
                ..
            } => {
                let input = input_schema.arrow_schema();
                let columns =
                    u64::try_from(input.fields().len()).assured("a compiled field count fits u64");
                let bytes = columns
                    .checked_mul(256)
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
                reserve_bytes(memory, bytes)?;
                for demand in plan.demands() {
                    cancellation
                        .check()
                        .change_context(ProcessorStatePageError::Admission)?;
                    reserve_bytes(
                        memory,
                        u64::try_from(
                            std::mem::size_of::<nervix_vm::window::CompiledWindowDemand>(),
                        )
                        .assured("a compiled demand size fits u64"),
                    )?;
                    for argument in demand.arguments.iter() {
                        let bytes = argument
                            .data_type
                            .size()
                            .checked_add(argument.field.len())
                            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
                        let bytes = bytes
                            .checked_add(256)
                            .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
                        reserve_bytes(
                            memory,
                            u64::try_from(bytes)
                                .map_err(|_| Report::new(ProcessorStatePageError::Admission))?,
                        )?;
                    }
                }
                ReadPlan::Window {
                    input,
                    arguments: WindowArgumentColumns::snapshot_schema(plan),
                    plan: plan.clone(),
                    width: *width_duration,
                }
            }
            _ => return Err(Report::new(ProcessorStatePageError::Unrestored)),
        };
        Ok(plan)
    }
}

struct CaptureSelection {
    expected: crate::runtime::RuntimeStatePlacement,
    selection: StateInspectionBranchSelection,
}

impl CaptureSelection {
    fn includes(&self, placement: &crate::runtime::RuntimeStatePlacement) -> bool {
        placement.domain == self.expected.domain
            && placement.kind == self.expected.kind
            && placement.identifier == self.expected.identifier
            && placement.unit == self.expected.unit
            && placement.state == self.expected.state
            && selected(self.selection, &placement.branch_key)
    }
}

impl Runtime {
    pub(crate) async fn local_published_processor_state_page(
        &self,
        domain: &DomainName,
        processor: &NodeRef,
        unit: Unit,
        selection: StateInspectionBranchSelection,
        after: Option<(Option<nervix_models::BranchKeyFingerprint>, u64)>,
        bounds: ProcessorStatePageBounds,
    ) -> ReadResult<ProcessorStatePage> {
        let routing = self
            .domain_routing(domain)
            .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?
            .load_full();
        let state_kind = match processor.kind {
            nervix_models::ModelKind::Deduplicator => RuntimeStateKind::Deduplicator,
            nervix_models::ModelKind::WindowProcessor => RuntimeStateKind::WindowProcessor,
            _ => return Err(Report::new(ProcessorStatePageError::Columns)),
        };
        let expected = self
            .state_placement_for_unit(
                domain,
                state_kind,
                processor.kind,
                &processor.identifier,
                unit,
                None,
            )
            .change_context(ProcessorStatePageError::Unavailable)?;
        let now = self
            .bind_domain_clock(domain)
            .change_context(ProcessorStatePageError::Unavailable)?
            .snapshot()
            .change_context(ProcessorStatePageError::Unavailable)?
            .now();
        let executor = self.inner.executor.clone();
        let memory = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .change_context(ProcessorStatePageError::Admission)?;
        let runtime = self.clone();
        let processor = processor.clone();
        let selection = CaptureSelection {
            expected,
            selection,
        };
        let captured = executor
            .run_cpu(CpuClass::Bulk, memory, move |mut memory, cancellation| {
                let installed = routing
                    .processor_plans
                    .get(&NodeUnitRef::new(processor.clone(), unit))
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
                let template = installed
                    .template
                    .processors
                    .get(&processor.identifier)
                    .ok_or_else(|| Report::new(ProcessorStatePageError::Unavailable))?;
                let plan = ReadPlan::prepare(&template.operation, &mut memory, cancellation)?;
                let mut captured = Vec::new();
                match state_kind {
                    RuntimeStateKind::Deduplicator => {
                        for entry in runtime.inner.replicated_deduplicator_states.iter() {
                            cancellation
                                .check()
                                .change_context(ProcessorStatePageError::Admission)?;
                            let placement = entry.key();
                            if !selection.includes(placement) {
                                continue;
                            }
                            reserve_item::<CapturedBranch>(&mut memory)?;
                            let generation = entry.value().generations.load();
                            captured.push(CapturedBranch {
                                key: placement.branch_key.clone(),
                                registration: entry.value().inspection_identity().ok_or_else(
                                    || Report::new(ProcessorStatePageError::Unrestored),
                                )?,
                                revision: generation.revision,
                                state: CapturedState::Keys(generation),
                            });
                        }
                    }
                    RuntimeStateKind::WindowProcessor => {
                        for entry in runtime.inner.replicated_window_processor_states.iter() {
                            cancellation
                                .check()
                                .change_context(ProcessorStatePageError::Admission)?;
                            let placement = entry.key();
                            if !selection.includes(placement) {
                                continue;
                            }
                            reserve_item::<CapturedBranch>(&mut memory)?;
                            let generation = entry.value().generations.load();
                            captured.push(CapturedBranch {
                                key: placement.branch_key.clone(),
                                registration: entry.value().inspection_identity().ok_or_else(
                                    || Report::new(ProcessorStatePageError::Unrestored),
                                )?,
                                revision: generation.revision,
                                state: CapturedState::Window(generation),
                            });
                        }
                    }
                    _ => return Err(Report::new(ProcessorStatePageError::Columns)),
                }
                captured.sort_by(|left, right| BranchKey::canonical_order(&left.key, &right.key));
                Ok::<_, Report<ProcessorStatePageError>>((captured, plan, memory))
            })
            .await
            .change_context(ProcessorStatePageError::Admission)??;
        let (captured, plan, mut memory) = captured;
        for _ in &captured {
            reserve_item::<OpenBranch>(&mut memory)?;
        }
        let mut opened = Vec::with_capacity(captured.len());
        for branch in captured {
            nervix_primitives::task::consume_budget().await;
            let state = match branch.state {
                CapturedState::Keys(keys) => OpenState::Keys(keys),
                CapturedState::Window(generation) => match (&generation.value, &plan) {
                    (
                        Some(WindowPublishedSnapshot::Sealed(_)),
                        ReadPlan::Window {
                            input, arguments, ..
                        },
                    ) => {
                        let (generation, admitted) = WindowCheckpointLayout::open_owned(
                            &executor,
                            generation,
                            sealed_publication,
                            WindowSnapshotLifetime::Recorded,
                        )
                        .await
                        .change_context(ProcessorStatePageError::Columns)?;
                        let admitted = admitted
                            .ok_or_else(|| Report::new(ProcessorStatePageError::Unrestored))?;
                        let payload = sealed_publication(&generation)
                            .ok_or_else(|| Report::new(ProcessorStatePageError::Columns))?;
                        let mut decoded = decode_window_processor_layout(
                            payload,
                            &executor,
                            WindowSnapshotSchemas { input, arguments },
                            admitted.layout,
                        )
                        .await
                        .change_context(ProcessorStatePageError::Columns)?;
                        decoded.metadata = Some(admitted._memory);
                        reserve_item::<AdmittedWindowSnapshot>(&mut memory)?;
                        OpenState::Window(WindowRead::Decoded(Box::new(decoded)))
                    }
                    _ => OpenState::Window(WindowRead::Published(generation)),
                },
            };
            opened.push(OpenBranch {
                key: branch.key,
                registration: branch.registration,
                revision: branch.revision,
                state,
            });
        }
        executor
            .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
                PublishedPage::build(opened, plan, now, after, bounds, memory, cancellation)
            })
            .await
            .change_context(ProcessorStatePageError::Admission)?
    }
}

fn sealed_publication(
    generation: &StdArc<Generation<Option<WindowPublishedSnapshot>>>,
) -> Option<&[u8]> {
    match &generation.value {
        Some(WindowPublishedSnapshot::Sealed(payload)) => Some(payload),
        Some(WindowPublishedSnapshot::Live(_)) | None => None,
    }
}
