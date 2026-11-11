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
    selection: ProcessorStateSelection,
}

struct CapturedPage {
    branches: Vec<CapturedBranch>,
    plan: ReadPlan,
    memory: Reservation,
    partition_catalog: nervix_models::StateInspectionKeyCatalog,
}

impl CapturedPage {
    fn new(
        mut branches: Vec<CapturedBranch>,
        plan: ReadPlan,
        mut memory: Reservation,
        excluded: Option<&BTreeSet<nervix_models::BranchKeyFingerprint>>,
        cancellation: &Cancellation,
    ) -> ReadResult<Self> {
        branches.sort_by(|left, right| BranchKey::canonical_order(&left.key, &right.key));
        let partition_catalog = match excluded {
            Some(excluded) => {
                let mut catalog = nervix_models::StateInspectionKeyCatalog::requested();
                for branch in &branches {
                    cancellation
                        .check()
                        .change_context(ProcessorStatePageError::Admission)?;
                    if let Some(key) = &branch.key {
                        // A sparse tree may allocate an entire node for this one fingerprint.
                        let bytes = u64::try_from(std::mem::size_of::<
                            nervix_models::BranchKeyFingerprint,
                        >())
                        .assured("a fingerprint's size fits u64")
                        .checked_mul(16)
                        .ok_or_else(|| Report::new(ProcessorStatePageError::Admission))?;
                        reserve_bytes(&mut memory, bytes)?;
                        catalog.insert(key.fingerprint(), key.inherited_origin());
                    }
                }
                branches.retain(|branch| match &branch.key {
                    Some(key) => !excluded.contains(&key.fingerprint()),
                    None => true,
                });
                catalog
            }
            None => nervix_models::StateInspectionKeyCatalog::Unrequested,
        };
        Ok(Self {
            branches,
            plan,
            memory,
            partition_catalog,
        })
    }
}

impl CaptureSelection {
    fn includes(&self, placement: &crate::runtime::RuntimeStatePlacement) -> bool {
        placement.domain == self.expected.domain
            && placement.kind == self.expected.kind
            && placement.identifier == self.expected.identifier
            && placement.unit == self.expected.unit
            && placement.state == self.expected.state
            && selected(self.selection.branch, &placement.branch_key)
    }
}

impl Runtime {
    pub(crate) async fn local_published_processor_state_page(
        &self,
        domain: &DomainName,
        processor: &NodeRef,
        unit: Unit,
        selection: ProcessorStateSelection,
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
                CapturedPage::new(
                    captured,
                    plan,
                    memory,
                    selection.selection.partition_exclusions.as_ref(),
                    cancellation,
                )
            })
            .await
            .change_context(ProcessorStatePageError::Admission)??;
        let CapturedPage {
            branches: captured,
            plan,
            mut memory,
            partition_catalog,
        } = captured;
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
                let mut page =
                    PublishedPage::build(opened, plan, now, after, bounds, memory, cancellation)?;
                page.partition_catalog = partition_catalog;
                Ok::<_, Report<ProcessorStatePageError>>(page)
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

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::*;
    use crate::{
        runtime::{DeduplicatorKey, named},
        runtime_schema::RuntimeValue,
    };

    fn key(tenant: &str) -> BranchKey {
        BranchKey::from_fields([(named("tenant"), RuntimeValue::String(tenant.to_string()))])
            .assured("one typed fixture field forms a concrete key")
    }

    #[nervix_primitives::test]
    async fn excluding_a_claimed_copy_keeps_the_complete_catalog_and_its_checkpoint_unchanged() {
        let executor = nervix_execution::Executor::default();
        let first = key("beta");
        let second = key("eta");
        let generation = StdArc::new(Generation {
            revision: 7,
            value: vec![PublishedDeduplicatorKey {
                key: Arc::new(DeduplicatorKey::new(vec![ReorderKeyPart::UInt64(42)])),
                seen_at: Timestamp::from_unix_nanos(11),
            }],
        });
        let retained = generation.clone();
        let excluded = BTreeSet::from([first.fingerprint()]);
        let expected_catalog = BTreeSet::from([first.fingerprint(), second.fingerprint()]);
        let memory = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .assured("bounded fixture admission");
        let captured = executor
            .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
                let branches = vec![
                    CapturedBranch {
                        key: Some(first),
                        registration: [1; 16],
                        revision: 7,
                        state: CapturedState::Keys(generation.clone()),
                    },
                    CapturedBranch {
                        key: Some(second),
                        registration: [2; 16],
                        revision: 7,
                        state: CapturedState::Keys(generation),
                    },
                ];
                CapturedPage::new(
                    branches,
                    ReadPlan::Keys {
                        fields: vec![StdArc::new(arrow_schema::Field::new(
                            "key",
                            DataType::UInt64,
                            true,
                        ))],
                        max_time: std::time::Duration::from_secs(3600),
                    },
                    memory,
                    Some(&excluded),
                    cancellation,
                )
            })
            .await
            .assured("the bounded capture completes")
            .assured("one copy can be excluded");
        assert_eq!(
            captured.partition_catalog,
            nervix_models::StateInspectionKeyCatalog::Direct(expected_catalog)
        );
        assert_eq!(captured.branches.len(), 1);
        assert_eq!(captured.branches[0].key.as_ref(), Some(&key("eta")));
        let CapturedState::Keys(selected) = &captured.branches[0].state else {
            panic!("the fixture captures deduplicator state");
        };
        assert!(StdArc::ptr_eq(selected, &retained));
        assert_eq!(retained.revision, 7);
        assert_eq!(retained.value.len(), 1);
        assert_eq!(retained.value[0].seen_at, Timestamp::from_unix_nanos(11));
    }
}
