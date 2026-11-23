//! A WASM processor guest's description of one branch's committed checkpoint.
//!
//! Layer: data plane.
//! - **Owns.** The scratch render of a committed checkpoint for a state inspection: instantiating
//!   the pinned module, initializing it with the branch's configuration, restoring the committed
//!   checkpoint, calling `nervix_describe_state`, validating the sections the guest returns and
//!   discarding the instance, on the node's extension workers under the processor's execution
//!   limits and the read's deadline, and the lifecycle diagnostic of every render that fails.
//! - **Depends on.** Compiled WASM processors, the committed checkpoints this node holds, the
//!   processor's execution plan and relay schemas, the domain clock, the Arrow IPC scanner and the
//!   bounded executor.
//! - **Must not know.** Session encoding, owner routing, inspection paging or presentation, and
//!   the instance that processes the branch's input, which a render never touches.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        observer,
        reason = "a state inspection renders committed checkpoints on scratch instances for \
                  operator reports"
    )
)]

use std::collections::BTreeSet;

use arrow_schema::DataType;
use error_stack::{Report, ResultExt as _};
use nervix_execution::{Cancellation, CpuClass, MemoryClass};
use nervix_models::{
    WasmGuestStateDescription, WasmGuestStateField, WasmGuestStateRender,
    WasmGuestStateRenderStage, WasmGuestStateSection, WasmStateEntryKey,
};
use nervix_wasm::{
    CompiledWasmProcessor, SavedStateRejection, WasmExecutionContext, WasmGuestOperation,
    WasmStateDescription,
};

use super::*;
use crate::runtime_schema::{IpcMessages, StreamEnding};

/// Why the host refuses what a guest described of its state, or why a render produced nothing to
/// validate. Guest-declared names are reported only once they are valid names, so a diagnostic
/// never repeats text the guest wrote outside them.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WasmStateDescriptionError {
    #[error("section {index} has a name that is not a valid name")]
    SectionName { index: usize },
    #[error("section '{section}' is described more than once")]
    RepeatedSection { section: FieldName },
    #[error("section '{section}' declares no field")]
    SectionWithoutFields { section: FieldName },
    #[error("section '{section}' field {index} has a name that is not a valid name")]
    FieldName { section: FieldName, index: usize },
    #[error("section '{section}' declares field '{field}' more than once")]
    RepeatedField {
        section: FieldName,
        field: FieldName,
    },
    #[error("section '{section}' field '{field}' declares a type that names no Nervix type")]
    FieldType {
        section: FieldName,
        field: FieldName,
    },
    #[error(
        "the rows of section '{section}' are not an Arrow IPC stream of the types Nervix reads"
    )]
    UnreadableRows { section: FieldName },
    #[error("the rows of section '{section}' are {batches} record batches instead of exactly one")]
    RowBatches { section: FieldName, batches: usize },
    #[error(
        "the rows of section '{section}' have {columns} columns, but it declares {fields} fields"
    )]
    RowColumns {
        section: FieldName,
        columns: usize,
        fields: usize,
    },
    #[error(
        "the column of section '{section}' field '{field}' is {actual:?} (nullable \
         {actual_nullable}), but the field declares {expected:?} (nullable {optional})"
    )]
    ColumnType {
        section: FieldName,
        field: FieldName,
        expected: DataType,
        optional: bool,
        actual: DataType,
        actual_nullable: bool,
    },
    #[error("the render did not finish within the {budget:?} the read left it")]
    Deadline { budget: Duration },
    #[error("the read stopped waiting for the render")]
    Cancelled,
    #[error("the node's extension workers did not run the render")]
    Execution,
    #[error("the render could not start its scheduler on an extension worker")]
    Scheduler,
    #[error("the domain clock has no execution time for the render")]
    Clock,
    #[error(
        "the description does not fit one read: it carries {sections} sections in {bytes} bytes, \
         where one read carries at most {max_sections} sections in {max_bytes} bytes"
    )]
    ExceedsRead {
        sections: usize,
        bytes: u64,
        max_sections: usize,
        max_bytes: u64,
    },
}

/// One branch's checkpoint as this node holds it, sampled for a state inspection, with what a
/// render of its committed checkpoint needs.
pub(crate) struct WasmCheckpointSample {
    /// The unit whose placement holds the checkpoint on this node.
    pub(crate) unit: Unit,
    pub(crate) checkpoint: WasmCheckpointInspection,
    pub(crate) source: WasmDescriptionSource,
}

/// The committed checkpoint of one branch, captured when its read sampled the branch: what a
/// render initializes and restores a scratch instance with.
pub(crate) struct WasmDescriptionSource {
    fingerprint: Option<BranchKeyFingerprint>,
    branch: Option<BranchKey>,
    generation: WasmStateGeneration,
    committed: StdArc<WasmGuestState>,
}

impl WasmCheckpointSample {
    fn of(unit: Unit, state: &ReplicatedWasmProcessorState) -> Self {
        let committed = state.restore_guest_state();
        let checkpoint = state.inspection();
        Self {
            unit,
            source: WasmDescriptionSource {
                fingerprint: checkpoint.branch,
                branch: state.placement.branch_key.clone(),
                generation: state.generation(),
                committed,
            },
            checkpoint,
        }
    }
}

impl WasmDescriptionSource {
    /// The identity of the entry this source renders the description of.
    pub(crate) const fn key(&self) -> WasmStateEntryKey {
        WasmStateEntryKey(self.fingerprint)
    }

    /// The committed revision a render restores; absent while the lifetime has committed nothing.
    fn revision(&self) -> Option<u64> {
        match self.committed.revision() {
            0 => None,
            revision => Some(revision),
        }
    }

    fn description(&self, render: WasmGuestStateRender) -> WasmGuestStateDescription {
        WasmGuestStateDescription {
            generation: self.generation,
            revision: self.revision().and_then(NonZeroU64::new),
            render,
        }
    }

    /// The description that replaces a render too large for any read: a failure of the state
    /// description, which carries no section, in place of what the guest described.
    pub(crate) fn exceeding_read(
        &self,
        plan: &WasmDescriptionPlan,
        measured: WasmDescriptionSize,
        read: WasmDescriptionSize,
    ) -> WasmGuestStateDescription {
        let refusal = Report::new(WasmStateDescriptionError::ExceedsRead {
            sections: measured.sections,
            bytes: measured.bytes,
            max_sections: read.sections,
            max_bytes: read.bytes,
        });
        let failure = plan.module(self).render_failure(
            refusal,
            WasmGuestOperation::StateDescription,
            self.revision(),
        );
        self.description(WasmGuestStateRender::Failed {
            stage: WasmGuestStateRenderStage::StateDescription,
            diagnostic: format!("{failure:#}"),
        })
    }
}

/// How many guest-state sections and bytes a description takes, or a read carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WasmDescriptionSize {
    pub(crate) sections: usize,
    pub(crate) bytes: u64,
}

/// What every render of one WASM processor's branches on this node shares: the pinned module, the
/// limits and relays the processor runs with, the domain's published routing and its clock.
pub(crate) struct WasmDescriptionPlan {
    domain: DomainName,
    processor: ModelName,
    resource: ResourceName,
    resource_version: u64,
    file: String,
    limits: nervix_models::WasmProcessorLimits,
    input_relay: RelayName,
    output_relays: Vec<RelayName>,
    routing: StdArc<DomainRoutingSnapshot>,
    clock: DomainClock,
}

impl WasmDescriptionPlan {
    fn module(&self, source: &WasmDescriptionSource) -> WasmBranchModule {
        WasmBranchModule {
            processor: self.processor.clone(),
            branch: source.branch.clone(),
            resource: ResourceId::new(
                self.domain.clone(),
                self.resource.clone(),
                self.resource_version,
            ),
            file: self.file.clone(),
        }
    }

    /// The configuration the live instance of `module`'s branch was initialized with, from the
    /// relay schemas the domain publishes.
    fn branch_init(
        &self,
        module: &WasmBranchModule,
    ) -> Result<nervix_wasm::WasmBranchInit, Report<DomainRoutingError>> {
        let input_schema =
            relay_schema_for_routing(&self.routing, &self.domain, &self.input_relay)?;
        let mut output_schemas = Vec::with_capacity(self.output_relays.len());
        for relay in &self.output_relays {
            let schema = relay_schema_for_routing(&self.routing, &self.domain, relay)?;
            output_schemas.push((relay.clone(), schema));
        }
        Ok(module.branch_init(&self.input_relay, &input_schema, &output_schemas))
    }
}

/// A render that failed at `stage`, with the complete lifecycle report of its failure.
struct WasmRenderFailure {
    stage: WasmGuestStateRenderStage,
    report: Report<WasmInstanceError>,
}

/// Everything one scratch render takes onto an extension worker.
struct WasmRenderJob {
    compiled: Arc<CompiledWasmProcessor>,
    limits: nervix_models::WasmProcessorLimits,
    init: nervix_wasm::WasmBranchInit,
    execution_now: Timestamp,
    committed: StdArc<WasmGuestState>,
    module: WasmBranchModule,
    revision: Option<u64>,
    budget: Duration,
}

impl WasmRenderJob {
    /// Runs the render on this extension worker, with a scheduler of its own that drives the
    /// guest's calls, until it finishes or the read's budget ends it.
    fn run(
        self,
        cancellation: &Cancellation,
    ) -> Result<Vec<WasmGuestStateSection>, WasmRenderFailure> {
        let module = self.module.clone();
        let revision = self.revision;
        let description_failure = |defect: WasmStateDescriptionError| WasmRenderFailure {
            stage: WasmGuestStateRenderStage::StateDescription,
            report: module.render_failure(
                Report::new(defect),
                WasmGuestOperation::StateDescription,
                revision,
            ),
        };
        if cancellation.is_cancelled() {
            return Err(description_failure(WasmStateDescriptionError::Cancelled));
        }
        let scheduler = match nervix_primitives::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
        {
            Ok(scheduler) => scheduler,
            Err(error) => {
                return Err(WasmRenderFailure {
                    stage: WasmGuestStateRenderStage::StateDescription,
                    report: module.render_failure(
                        Report::new(error).change_context(WasmStateDescriptionError::Scheduler),
                        WasmGuestOperation::StateDescription,
                        revision,
                    ),
                });
            }
        };
        let budget = self.budget;
        let rendered = scheduler.block_on(nervix_primitives::time::timeout(
            budget,
            self.render(cancellation),
        ));
        match rendered {
            Ok(result) => result,
            Err(_) => Err(description_failure(WasmStateDescriptionError::Deadline {
                budget,
            })),
        }
    }

    async fn render(
        self,
        cancellation: &Cancellation,
    ) -> Result<Vec<WasmGuestStateSection>, WasmRenderFailure> {
        let context = WasmExecutionContext::new(self.execution_now);
        let restored = self.committed.restorable();
        let mut restored_revision = None;
        if let Some(restored) = &restored {
            restored_revision = Some(restored.revision);
        }
        let instantiation = self
            .compiled
            .instantiate_branch(self.limits, self.init, context, None)
            .await;
        let mut guest = match instantiation {
            Ok(guest) => guest,
            Err(failure) => {
                // Instantiating a branch without saved state runs only the instantiation and the
                // initialization, so a failure is one or the other.
                let stage = if failure.current_context().operation()
                    == WasmGuestOperation::Initialization
                {
                    WasmGuestStateRenderStage::Initialization
                } else {
                    WasmGuestStateRenderStage::Instantiation
                };
                return Err(WasmRenderFailure {
                    stage,
                    report: self.module.guest_failure(failure, restored_revision),
                });
            }
        };
        if let Some(restored) = restored
            && let Err(failure) = guest.load_state_in_context(restored.bytes, context).await
        {
            let stage = match failure.current_context().saved_state_rejection() {
                Some(SavedStateRejection::SnapshotEnvelope) => {
                    WasmGuestStateRenderStage::SnapshotEnvelopeDecoding
                }
                Some(SavedStateRejection::ApplicationState) => {
                    WasmGuestStateRenderStage::ApplicationStateRestoration
                }
                None => WasmGuestStateRenderStage::StateRestore,
            };
            return Err(WasmRenderFailure {
                stage,
                report: self.module.guest_failure(failure, Some(restored.revision)),
            });
        }
        if cancellation.is_cancelled() {
            return Err(WasmRenderFailure {
                stage: WasmGuestStateRenderStage::StateDescription,
                report: self.module.render_failure(
                    Report::new(WasmStateDescriptionError::Cancelled),
                    WasmGuestOperation::StateDescription,
                    self.revision,
                ),
            });
        }
        let described = guest.describe_state_in_context(context).await;
        // The scratch instance is never used again: whatever it holds goes with it.
        drop(guest);
        let description = match described {
            Ok(description) => description,
            Err(failure) => {
                return Err(WasmRenderFailure {
                    stage: WasmGuestStateRenderStage::StateDescription,
                    report: self.module.guest_failure(failure, self.revision),
                });
            }
        };
        validated_sections(description).map_err(|defect| WasmRenderFailure {
            stage: WasmGuestStateRenderStage::StateDescription,
            report: self.module.render_failure(
                defect,
                WasmGuestOperation::StateDescription,
                self.revision,
            ),
        })
    }
}

/// The sections a guest described, once every name is a valid name, unique among its siblings,
/// every type names a Nervix type and every section's rows are exactly one batch of its fields.
fn validated_sections(
    description: WasmStateDescription,
) -> Result<Vec<WasmGuestStateSection>, Report<WasmStateDescriptionError>> {
    let mut sections = Vec::with_capacity(description.sections.len());
    let mut section_names = BTreeSet::new();
    for (index, section) in description.sections.into_iter().enumerate() {
        let name = FieldName::parse(&section.schema.name)
            .change_context(WasmStateDescriptionError::SectionName { index })?;
        if !section_names.insert(name.clone()) {
            return Err(Report::new(WasmStateDescriptionError::RepeatedSection {
                section: name,
            }));
        }
        if section.schema.fields.is_empty() {
            return Err(Report::new(
                WasmStateDescriptionError::SectionWithoutFields { section: name },
            ));
        }
        let mut fields = Vec::with_capacity(section.schema.fields.len());
        let mut field_names = BTreeSet::new();
        for (field_index, field) in section.schema.fields.iter().enumerate() {
            let field_name = FieldName::parse(&field.name).change_context(
                WasmStateDescriptionError::FieldName {
                    section: name.clone(),
                    index: field_index,
                },
            )?;
            if !field_names.insert(field_name.clone()) {
                return Err(Report::new(WasmStateDescriptionError::RepeatedField {
                    section: name,
                    field: field_name,
                }));
            }
            let ty = ParseAsType::try_from(&field.ty).change_context(
                WasmStateDescriptionError::FieldType {
                    section: name.clone(),
                    field: field_name.clone(),
                },
            )?;
            fields.push(WasmGuestStateField {
                name: field_name,
                ty,
                optional: field.optional,
            });
        }
        let rows = section_rows(&name, &fields, &section.arrow_ipc_batch)?;
        sections.push(WasmGuestStateSection { name, fields, rows });
    }
    Ok(sections)
}

/// How many rows a section's Arrow IPC stream holds, once it is exactly one readable batch whose
/// columns carry exactly the types and optionality of `fields`.
fn section_rows(
    section: &FieldName,
    fields: &[WasmGuestStateField],
    ipc: &[u8],
) -> Result<usize, Report<WasmStateDescriptionError>> {
    let unreadable = || WasmStateDescriptionError::UnreadableRows {
        section: section.clone(),
    };
    // Arrow's reader panics on a stream that declares what it does not expect, and the stream is
    // the guest's, so it is scanned before the reader reads any of it.
    let scanned = IpcMessages::ending(ipc, StreamEnding::MarkerOrEnd)
        .scan(None)
        .change_context_lazy(unreadable)?;
    if scanned.trailing != 0 {
        return Err(Report::new(unreadable()));
    }
    if scanned.record_batches != 1 {
        return Err(Report::new(WasmStateDescriptionError::RowBatches {
            section: section.clone(),
            batches: scanned.record_batches,
        }));
    }
    let reader = scanned.reader().change_context_lazy(unreadable)?;
    let schema = reader.schema();
    if schema.fields().len() != fields.len() {
        return Err(Report::new(WasmStateDescriptionError::RowColumns {
            section: section.clone(),
            columns: schema.fields().len(),
            fields: fields.len(),
        }));
    }
    for (column, field) in schema.fields().iter().zip(fields) {
        let expected = field.ty.arrow_data_type();
        if column.data_type() != &expected || column.is_nullable() != field.optional {
            return Err(Report::new(WasmStateDescriptionError::ColumnType {
                section: section.clone(),
                field: field.name.clone(),
                expected,
                optional: field.optional,
                actual: column.data_type().clone(),
                actual_nullable: column.is_nullable(),
            }));
        }
    }
    // Reading the batch checks it against its schema: Arrow refuses a null in a column the schema
    // declares non-nullable, which the comparison above holds to the field's optionality.
    let mut rows = 0;
    for batch in reader {
        rows = batch.change_context_lazy(unreadable)?.num_rows();
    }
    Ok(rows)
}

impl Runtime {
    /// The checkpoints of the current lifetimes of a WASM processor's branches on this node, in
    /// branch order, each with what a render of its committed checkpoint needs.
    pub(crate) fn sample_wasm_processor_checkpoints(
        &self,
        domain: &DomainName,
        processor: &ModelName,
    ) -> Vec<WasmCheckpointSample> {
        let mut samples = Vec::new();
        self.visit_wasm_processor_checkpoints(domain, processor, |unit, state| {
            samples.push(WasmCheckpointSample::of(unit, state));
        });
        samples.sort_by_key(|sample| sample.checkpoint.branch);
        samples
    }

    /// What every render of `processor`'s branches on this node shares, or `None` when this node
    /// does not run the processor.
    pub(crate) fn wasm_description_plan(
        &self,
        domain: &DomainName,
        processor: &ModelName,
    ) -> Option<WasmDescriptionPlan> {
        let execution = self.inner.executions.get(domain)?;
        let node = execution
            .revision
            .processors
            .processor(ModelKind::WasmProcessor, processor)?;
        let BranchedProcessorOperationSpec::WasmProcessor {
            output_routes,
            resource,
            resource_version,
            file,
            limits,
            ..
        } = &node.spec.operation
        else {
            return None;
        };
        let input_relay = node.spec.input_relays.first()?.clone();
        let mut output_relays = Vec::with_capacity(output_routes.routes.len());
        for route in output_routes.outputs() {
            output_relays.push(route.relay.clone());
        }
        Some(WasmDescriptionPlan {
            domain: domain.clone(),
            processor: processor.clone(),
            resource: resource.clone(),
            resource_version: *resource_version,
            file: file.clone(),
            limits: *limits,
            input_relay,
            output_relays,
            routing: execution.routing.shared().load_full(),
            clock: execution.domain_clock.clone(),
        })
    }

    /// The guest's description of `source`'s committed checkpoint, rendered on a scratch instance
    /// within `budget` and discarded. A render that fails reports its stage and diagnostic in the
    /// description and changes nothing else: the live instance, its checkpoints and the
    /// processor's error policy never see it.
    pub(crate) async fn render_wasm_state_description(
        &self,
        plan: &WasmDescriptionPlan,
        source: &WasmDescriptionSource,
        budget: Duration,
    ) -> WasmGuestStateDescription {
        let render = match self.render_wasm_state_sections(plan, source, budget).await {
            Ok(sections) => WasmGuestStateRender::Described { sections },
            Err(failure) => {
                tracing::debug!(
                    domain = plan.domain.as_str(),
                    processor = plan.processor.as_str(),
                    error = %format_args!("{:#}", failure.report),
                    "WASM state description render failed"
                );
                WasmGuestStateRender::Failed {
                    stage: failure.stage,
                    diagnostic: format!("{:#}", failure.report),
                }
            }
        };
        source.description(render)
    }

    async fn render_wasm_state_sections(
        &self,
        plan: &WasmDescriptionPlan,
        source: &WasmDescriptionSource,
        budget: Duration,
    ) -> Result<Vec<WasmGuestStateSection>, WasmRenderFailure> {
        let module = plan.module(source);
        let mut restored_revision = None;
        if let Some(restored) = source.committed.restorable() {
            restored_revision = Some(restored.revision);
        }
        let instantiation_failure = |report: Report<WasmInstanceError>| WasmRenderFailure {
            stage: WasmGuestStateRenderStage::Instantiation,
            report,
        };
        let compiled = self
            .compile_wasm_processor_module(
                &plan.domain,
                &plan.processor,
                &plan.resource,
                plan.resource_version,
                &plan.file,
            )
            .await
            .map_err(|report| WasmRenderFailure {
                stage: WasmGuestStateRenderStage::ModuleCompilation,
                report,
            })?;
        let init = plan.branch_init(&module).map_err(|error| {
            instantiation_failure(module.render_failure(
                error,
                WasmGuestOperation::Instantiation,
                restored_revision,
            ))
        })?;
        let execution_now = match plan.clock.snapshot() {
            Ok(snapshot) => snapshot.now(),
            Err(error) => {
                return Err(instantiation_failure(module.render_failure(
                    error.change_context(WasmStateDescriptionError::Clock),
                    WasmGuestOperation::Instantiation,
                    restored_revision,
                )));
            }
        };
        let compiled = compiled.compiled;
        // The render copies the committed checkpoint into the scratch instance, which is the data
        // it processes, so the checkpoint's size is what it charges, and at least one byte.
        let restored_bytes = u64::try_from(source.committed.bytes().len())
            .assured("a checkpoint held in memory has a length that fits u64")
            .max(1);
        let executor = self.executor();
        let reservation = match executor.try_reserve(MemoryClass::StateRead, restored_bytes) {
            Ok(reservation) => reservation,
            Err(error) => {
                return Err(instantiation_failure(module.render_failure(
                    error.change_context(WasmStateDescriptionError::Execution),
                    WasmGuestOperation::Instantiation,
                    restored_revision,
                )));
            }
        };
        let job = WasmRenderJob {
            compiled,
            limits: plan.limits,
            init,
            execution_now,
            committed: source.committed.clone(),
            module: module.clone(),
            revision: source.revision(),
            budget,
        };
        let rendered = executor
            .run_cpu(
                CpuClass::Extension,
                reservation,
                move |_reservation, cancellation| job.run(cancellation),
            )
            .await;
        match rendered {
            Ok(result) => result,
            Err(error) => Err(instantiation_failure(module.render_failure(
                error.change_context(WasmStateDescriptionError::Execution),
                WasmGuestOperation::Instantiation,
                restored_revision,
            ))),
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, UInt32Array};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{Field, Schema};
    use nervix_wasm::{
        WasmBranchInit, WasmProcessorField, WasmProcessorSchema, WasmProcessorType, WasmRuntime,
        WasmRuntimeConfig, WasmStateSection,
    };

    use super::*;

    /// What a failing fixture export reports on the global-error channel.
    const GUEST_REASON: &str = "the fixture guest refused";

    /// Long enough for every render that must finish, so only a render out of time fails by it.
    const BUDGET: Duration = Duration::from_secs(30);

    /// An Arrow IPC stream of `batches` copies of one batch of `columns` under `fields`.
    fn rows(fields: Vec<Field>, columns: Vec<ArrayRef>, batches: usize) -> Vec<u8> {
        let schema = StdArc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .assured("the fixture's columns match its fields");
        let mut bytes = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut bytes, &schema).assured("a writer over a buffer opens");
            for _ in 0..batches {
                writer
                    .write(&batch)
                    .assured("the batch matches the stream's schema");
            }
            writer.finish().assured("a writer over a buffer finishes");
        }
        bytes
    }

    fn field(name: &str, ty: WasmProcessorType, optional: bool) -> WasmProcessorField {
        WasmProcessorField {
            name: name.to_string(),
            ty,
            optional,
        }
    }

    fn section(name: &str, fields: Vec<WasmProcessorField>, rows: Vec<u8>) -> WasmStateSection {
        WasmStateSection {
            schema: WasmProcessorSchema {
                name: name.to_string(),
                fields,
            },
            arrow_ipc_batch: rows,
        }
    }

    /// A `counters` section of one required `count` field and one row.
    fn counters(name: &str) -> WasmStateSection {
        section(
            name,
            vec![field("count", WasmProcessorType::U32, false)],
            rows(
                vec![Field::new("count", DataType::UInt32, false)],
                vec![StdArc::new(UInt32Array::from(vec![7]))],
                1,
            ),
        )
    }

    /// The refusal of a description of `sections`, which must be refused.
    fn refusal(sections: Vec<WasmStateSection>) -> Report<WasmStateDescriptionError> {
        match validated_sections(WasmStateDescription { sections }) {
            Ok(sections) => panic!("an invalid description was accepted: {sections:?}"),
            Err(refusal) => refusal,
        }
    }

    fn name(raw: &str) -> FieldName {
        FieldName::parse(raw).assured("a literal name follows the name rules")
    }

    #[test]
    fn a_valid_description_reports_each_section_with_its_fields_and_rows() {
        let tenants = section(
            "tenants",
            vec![
                field("tenant", WasmProcessorType::String, false),
                field("note", WasmProcessorType::String, true),
            ],
            rows(
                vec![
                    Field::new("tenant", DataType::Utf8, false),
                    Field::new("note", DataType::Utf8, true),
                ],
                vec![
                    StdArc::new(StringArray::from(vec!["alpha", "beta"])),
                    StdArc::new(StringArray::from(vec![Some("first"), None])),
                ],
                1,
            ),
        );

        let sections = validated_sections(WasmStateDescription {
            sections: vec![counters("Counters"), tenants],
        })
        .assured("a valid description validates");

        assert_eq!(
            sections,
            vec![
                WasmGuestStateSection {
                    name: name("counters"),
                    fields: vec![WasmGuestStateField {
                        name: name("count"),
                        ty: ParseAsType::U32,
                        optional: false,
                    }],
                    rows: 1,
                },
                WasmGuestStateSection {
                    name: name("tenants"),
                    fields: vec![
                        WasmGuestStateField {
                            name: name("tenant"),
                            ty: ParseAsType::String,
                            optional: false,
                        },
                        WasmGuestStateField {
                            name: name("note"),
                            ty: ParseAsType::String,
                            optional: true,
                        },
                    ],
                    rows: 2,
                },
            ]
        );
        assert!(
            validated_sections(WasmStateDescription {
                sections: Vec::new()
            })
            .assured("a description of no state validates")
            .is_empty()
        );
    }

    #[test]
    fn a_description_refuses_invalid_and_repeated_names_and_sections_without_fields() {
        assert!(matches!(
            refusal(vec![counters("not a name")]).current_context(),
            WasmStateDescriptionError::SectionName { index: 0 }
        ));
        assert!(matches!(
            refusal(vec![counters("counters"), counters("COUNTERS")]).current_context(),
            WasmStateDescriptionError::RepeatedSection { section } if *section == name("counters")
        ));
        assert!(matches!(
            refusal(vec![section("empty", Vec::new(), Vec::new())]).current_context(),
            WasmStateDescriptionError::SectionWithoutFields { section } if *section == name("empty")
        ));
        let mut misnamed = counters("counters");
        misnamed.schema.fields[0].name = "not a name".to_string();
        assert!(matches!(
            refusal(vec![misnamed]).current_context(),
            WasmStateDescriptionError::FieldName { index: 0, .. }
        ));
        let mut repeated = counters("counters");
        repeated
            .schema
            .fields
            .push(field("COUNT", WasmProcessorType::U32, false));
        assert!(matches!(
            refusal(vec![repeated]).current_context(),
            WasmStateDescriptionError::RepeatedField { field, .. } if *field == name("count")
        ));
        let mut empty_array = counters("counters");
        empty_array.schema.fields[0].ty = WasmProcessorType::Vec {
            element: Box::new(WasmProcessorType::Array {
                element: Box::new(WasmProcessorType::U8),
                len: 0,
            }),
        };
        assert!(matches!(
            refusal(vec![empty_array]).current_context(),
            WasmStateDescriptionError::FieldType { field, .. } if *field == name("count")
        ));
    }

    #[test]
    fn a_description_refuses_rows_that_are_not_one_batch_of_exactly_its_fields() {
        let count = || vec![field("count", WasmProcessorType::U32, false)];
        let unreadable = refusal(vec![section("counters", count(), vec![0xff; 32])]);
        assert!(matches!(
            unreadable.current_context(),
            WasmStateDescriptionError::UnreadableRows { .. }
        ));
        assert!(matches!(
            refusal(vec![section("counters", count(), Vec::new())]).current_context(),
            WasmStateDescriptionError::UnreadableRows { .. }
        ));
        let two_batches = rows(
            vec![Field::new("count", DataType::UInt32, false)],
            vec![StdArc::new(UInt32Array::from(vec![7]))],
            2,
        );
        assert!(matches!(
            refusal(vec![section("counters", count(), two_batches)]).current_context(),
            WasmStateDescriptionError::RowBatches { batches: 2, .. }
        ));
        let no_batch = rows(
            vec![Field::new("count", DataType::UInt32, false)],
            vec![StdArc::new(UInt32Array::from(vec![7]))],
            0,
        );
        assert!(matches!(
            refusal(vec![section("counters", count(), no_batch)]).current_context(),
            WasmStateDescriptionError::RowBatches { batches: 0, .. }
        ));
        let mut extra_field = count();
        extra_field.push(field("note", WasmProcessorType::String, true));
        let one_column = rows(
            vec![Field::new("count", DataType::UInt32, false)],
            vec![StdArc::new(UInt32Array::from(vec![7]))],
            1,
        );
        assert!(matches!(
            refusal(vec![section("counters", extra_field, one_column)]).current_context(),
            WasmStateDescriptionError::RowColumns {
                columns: 1,
                fields: 2,
                ..
            }
        ));
        let signed = rows(
            vec![Field::new("count", DataType::Int32, false)],
            vec![StdArc::new(Int32Array::from(vec![7]))],
            1,
        );
        assert!(matches!(
            refusal(vec![section("counters", count(), signed)]).current_context(),
            WasmStateDescriptionError::ColumnType {
                expected: DataType::UInt32,
                optional: false,
                actual: DataType::Int32,
                actual_nullable: false,
                ..
            }
        ));
        let nullable = rows(
            vec![Field::new("count", DataType::UInt32, true)],
            vec![StdArc::new(UInt32Array::from(vec![Some(7)]))],
            1,
        );
        assert!(matches!(
            refusal(vec![section("counters", count(), nullable)]).current_context(),
            WasmStateDescriptionError::ColumnType {
                expected: DataType::UInt32,
                optional: false,
                actual: DataType::UInt32,
                actual_nullable: true,
                ..
            }
        ));
    }

    #[test]
    fn a_description_too_large_for_a_read_is_a_failed_state_description() {
        let module = counting_module();
        let refusal = Report::new(WasmStateDescriptionError::ExceedsRead {
            sections: 200,
            bytes: 4096,
            max_sections: 128,
            max_bytes: 1024,
        });

        let failure = module.render_failure(refusal, WasmGuestOperation::StateDescription, Some(3));

        assert_eq!(
            format!("{failure:#}"),
            "wasm processor 'counting_guest' state description failed (unbranched, resource \
             'counting_guest' version 1 file 'guest.wasm', export 'nervix_describe_state', saved \
             state revision 3): the description does not fit one read: it carries 200 sections in \
             4096 bytes, where one read carries at most 128 sections in 1024 bytes"
        );
    }

    /// The module of the unbranched `counting_guest` processor every render test describes.
    fn counting_module() -> WasmBranchModule {
        WasmBranchModule {
            processor: ModelName::parse("counting_guest").assured("a literal name"),
            branch: None,
            resource: ResourceId::new(
                DomainName::parse("events").assured("a literal domain"),
                ResourceName::parse("counting_guest").assured("a literal name"),
                1,
            ),
            file: "guest.wasm".to_string(),
        }
    }

    /// A guest module with every export a render instance resolves, whose `nervix_init`,
    /// `nervix_load_state` and `nervix_describe_state` run `init`, `load` and `describe`. The
    /// description export lends `message` from offset 8192, and the module has no description
    /// export at all when `describe` is `None`. `$refuse`, `$reject_envelope` and
    /// `$reject_application` report the guest's reason and return a failure, a snapshot envelope
    /// rejection and an application-state rejection.
    fn guest(init: &str, load: &str, describe: Option<&str>, message: &[u8]) -> String {
        let mut data = String::with_capacity(message.len() * 3);
        for byte in message {
            data.push_str(&format!("\\{byte:02x}"));
        }
        let mut describe_export = String::new();
        if let Some(describe) = describe {
            describe_export = format!(
                r#"(func (export "nervix_describe_state") (result i32)
                    i32.const 8192
                    global.set $read_ptr
                    {describe})"#
            );
        }
        format!(
            r#"
            (module
                (memory (export "memory") 1)
                (data (i32.const 2048) "{GUEST_REASON}")
                (data (i32.const 8192) "{data}")
                (global $reason_len (mut i32) (i32.const 0))
                (global $read_ptr (mut i32) (i32.const 1024))
                (func $fail (param $code i32) (result i32)
                    i32.const {reason_len}
                    global.set $reason_len
                    local.get $code)
                (func $refuse (result i32) (call $fail (i32.const -1)))
                (func $reject_envelope (result i32) (call $fail (i32.const {envelope})))
                (func $reject_application (result i32) (call $fail (i32.const {application})))
                (func (export "nervix_buffer_ptr") (result i32) global.get $read_ptr)
                (func (export "nervix_alloc") (param i32) (result i32)
                    i32.const 1024
                    global.set $read_ptr
                    i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) {init})
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_flush") (result i32) i32.const 0)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) {load})
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                {describe_export}
                (func (export "nervix_global_error_ptr") (result i32) i32.const 2048)
                (func (export "nervix_global_error_len") (result i32) global.get $reason_len)
                (func (export "nervix_clear_global_error") (result i32)
                    i32.const 0
                    global.set $reason_len
                    i32.const 0)
            )
            "#,
            reason_len = GUEST_REASON.len(),
            envelope = SavedStateRejection::SnapshotEnvelope.code(),
            application = SavedStateRejection::ApplicationState.code(),
        )
    }

    /// Renders `wat`'s guest on the executor's extension workers, as a read does, restoring
    /// `committed` when it holds saved state, within `budget`.
    async fn render(
        wat: String,
        committed: Option<(u64, Vec<u8>)>,
        budget: Duration,
    ) -> Result<Vec<WasmGuestStateSection>, WasmRenderFailure> {
        let executor = nervix_execution::Executor::default();
        let runtime = WasmRuntime::new(WasmRuntimeConfig::default())
            .assured("the default WASM runtime configures");
        let compiled = runtime
            .compile_processor(&executor, wat)
            .await
            .assured("the fixture module compiles");
        let mut state = WasmGuestState::saved_at(0, Vec::new());
        let mut revision = None;
        if let Some((saved_revision, bytes)) = committed {
            state = WasmGuestState::saved_at(saved_revision, bytes);
            revision = Some(saved_revision);
        }
        let job = WasmRenderJob {
            compiled: Arc::new(compiled),
            limits: nervix_models::WasmProcessorLimits {
                max_fuel: NonZeroU64::new(1 << 50).assured("a literal fuel limit is nonzero"),
                max_memory_bytes: NonZeroU64::new(64 << 20)
                    .assured("a literal memory limit is nonzero"),
                max_table_elements: 4_096,
            },
            init: WasmBranchInit {
                domain_name: "events".to_string(),
                domain_type: "runtime".to_string(),
                branch_key: None,
                input_schema: WasmProcessorSchema {
                    name: "counts".to_string(),
                    fields: vec![field("count", WasmProcessorType::U32, false)],
                },
                output_schemas: Vec::new(),
            },
            execution_now: Timestamp::from_unix_nanos(0),
            committed: StdArc::new(state),
            module: counting_module(),
            revision,
            budget,
        };
        let reservation = executor
            .try_reserve(MemoryClass::StateRead, 1)
            .assured("an idle executor admits one byte of state reads");
        executor
            .run_cpu(
                CpuClass::Extension,
                reservation,
                move |_reservation, cancellation| job.run(cancellation),
            )
            .await
            .assured("the extension workers run the render")
    }

    /// The sections of a render that must succeed.
    fn described(
        rendered: Result<Vec<WasmGuestStateSection>, WasmRenderFailure>,
    ) -> Vec<WasmGuestStateSection> {
        match rendered {
            Ok(sections) => sections,
            Err(failure) => panic!(
                "a render that must succeed failed at {:?}: {:#}",
                failure.stage, failure.report
            ),
        }
    }

    /// The failure of a render that must fail.
    fn failed(
        rendered: Result<Vec<WasmGuestStateSection>, WasmRenderFailure>,
    ) -> WasmRenderFailure {
        match rendered {
            Ok(sections) => panic!("a render that must fail described {sections:?}"),
            Err(failure) => failure,
        }
    }

    /// The failure of a render whose guest restores the committed checkpoint at revision 3 by
    /// running `load`.
    async fn restore_failure(load: &str) -> WasmRenderFailure {
        let saved = Some((3, b"saved".to_vec()));
        failed(
            render(
                guest("i32.const 0", load, Some("i32.const 0"), &[]),
                saved,
                BUDGET,
            )
            .await,
        )
    }

    #[nervix_primitives::test]
    async fn a_render_restores_the_committed_checkpoint_before_the_guest_describes_it() {
        let nothing_saved = guest("i32.const 0", "call $refuse", Some("i32.const 0"), &[]);
        assert!(described(render(nothing_saved, None, BUDGET).await).is_empty());

        let restoring = guest("i32.const 0", "i32.const 0", Some("i32.const 0"), &[]);
        let saved = Some((3, b"saved".to_vec()));
        assert!(described(render(restoring, saved, BUDGET).await).is_empty());
    }

    #[nervix_primitives::test]
    async fn a_render_that_cannot_bring_its_instance_up_fails_at_instantiation_or_initialization() {
        let without_export = guest("i32.const 0", "i32.const 0", None, &[]);
        let missing = failed(render(without_export, None, BUDGET).await);
        assert_eq!(missing.stage, WasmGuestStateRenderStage::Instantiation);
        let diagnostic = format!("{:#}", missing.report);
        assert!(diagnostic.contains("nervix_describe_state"), "{diagnostic}");

        let refusing = guest("call $refuse", "i32.const 0", Some("i32.const 0"), &[]);
        let refused = failed(render(refusing, None, BUDGET).await);
        assert_eq!(refused.stage, WasmGuestStateRenderStage::Initialization);
        let diagnostic = format!("{:#}", refused.report);
        assert!(diagnostic.contains(GUEST_REASON), "{diagnostic}");
    }

    #[nervix_primitives::test]
    async fn a_render_whose_restore_fails_reports_the_guest_verdict_as_its_stage() {
        let envelope = restore_failure("call $reject_envelope").await;
        assert_eq!(
            envelope.stage,
            WasmGuestStateRenderStage::SnapshotEnvelopeDecoding
        );
        let application = restore_failure("call $reject_application").await;
        assert_eq!(
            application.stage,
            WasmGuestStateRenderStage::ApplicationStateRestoration
        );
        let without_verdict = restore_failure("call $refuse").await;
        assert_eq!(
            without_verdict.stage,
            WasmGuestStateRenderStage::StateRestore
        );
        let diagnostic = format!("{:#}", without_verdict.report);
        assert!(
            diagnostic.contains("saved state revision 3"),
            "{diagnostic}"
        );
        assert!(diagnostic.contains(GUEST_REASON), "{diagnostic}");
    }

    #[nervix_primitives::test]
    async fn a_render_whose_guest_refuses_or_misdescribes_its_state_fails_at_the_state_description()
    {
        let refusing = guest("i32.const 0", "i32.const 0", Some("call $refuse"), &[]);
        let refused = failed(render(refusing, None, BUDGET).await);
        assert_eq!(refused.stage, WasmGuestStateRenderStage::StateDescription);
        let diagnostic = format!("{:#}", refused.report);
        assert!(diagnostic.contains(GUEST_REASON), "{diagnostic}");

        let misnamed = nervix_wasm_protocol::StateDescription {
            sections: vec![nervix_wasm_protocol::StateSection {
                schema: nervix_wasm_protocol::ProcessorSchema {
                    name: "not a name".to_string(),
                    fields: Vec::new(),
                },
                arrow_ipc_batch: Vec::new(),
            }],
        }
        .encode();
        let describe = format!("i32.const {}", misnamed.len());
        let misdescribing = guest("i32.const 0", "i32.const 0", Some(&describe), &misnamed);
        let refused_by_host = failed(render(misdescribing, None, BUDGET).await);
        assert_eq!(
            refused_by_host.stage,
            WasmGuestStateRenderStage::StateDescription
        );
        let diagnostic = format!("{:#}", refused_by_host.report);
        assert!(
            diagnostic.contains("section 0 has a name that is not a valid name"),
            "{diagnostic}"
        );
    }

    #[nervix_primitives::test]
    async fn a_render_out_of_time_fails_at_the_state_description_naming_its_budget() {
        // The guest's initialization never returns, so only the render's budget ends it.
        let spinning = guest(
            "(loop $spin (br $spin)) i32.const 0",
            "i32.const 0",
            Some("i32.const 0"),
            &[],
        );
        let failure = failed(render(spinning, None, Duration::ZERO).await);
        assert_eq!(failure.stage, WasmGuestStateRenderStage::StateDescription);
        let diagnostic = format!("{:#}", failure.report);
        assert!(
            diagnostic.contains("the render did not finish within the 0ns the read left it"),
            "{diagnostic}"
        );
    }
}
