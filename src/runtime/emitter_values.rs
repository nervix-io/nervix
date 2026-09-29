//! The host projection every row sink writes from.
//!
//! Layer: data plane.
//! - **Owns.** Binding one emitter's lowered `VALUES` mapping once, evaluating it once per batch
//!   into an Arrow batch of mapped columns, the rows each batch still has to write, handing a row
//!   sink every run of successive projected batches of one source relay and concrete branch in one
//!   write, and handing a row request sink each projected batch to prepare requests from, whose
//!   prepared requests the emitter retains and hands back until the sink answers for them.
//! - **Depends on.** The VM's compile and execute API, Arrow batches, the connector contract's row
//!   sink, row request sink and mapped-carrier value types, and the retained payloads and checked
//!   answers a write is resolved through.
//! - **Must not know.** Which external system consumes the mapped rows, how it divides a write into
//!   requests, or how it encodes them.

use async_trait::async_trait;
use nervix_connector::{
    MappedSinkCarrier, MappedSinkRows, RowRequestSink, RowSink, SinkAcknowledgements,
    SinkLifecycle, SinkRecordPosition,
};

use super::*;

/// A row sink and the host projection whose mapped columns it writes.
///
/// The mapping is evaluated here, once per batch, so the sink receives Arrow columns and the rows
/// it must write and never learns what produced them.
pub(in crate::runtime) struct MappedRowSink {
    sink: Box<dyn RowSink>,
    projection: MappedValuesProjection,
}

impl MappedRowSink {
    pub(in crate::runtime) fn new(
        sink: Box<dyn RowSink>,
        projection: MappedValuesProjection,
    ) -> Self {
        Self { sink, projection }
    }
}

#[async_trait]
impl EmitterSink for MappedRowSink {
    fn lifecycle(&self) -> &dyn SinkLifecycle {
        &*self.sink
    }

    fn lifecycle_mut(&mut self) -> &mut dyn SinkLifecycle {
        &mut *self.sink
    }

    /// Writes every buffered batch, projecting each once and handing the sink each run of
    /// successive batches of one source relay and concrete branch in one call.
    ///
    /// A row sink answers for each mapped row by its position, so a row it left unresolved is
    /// projected again by the next attempt and nothing is retained between attempts.
    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        publication: EmitterPublication<'_>,
    ) -> EmitterRuntimeResult<()> {
        let EmitterPublication { batches, .. } = publication;
        let acknowledgements = match self.sink.retains_acknowledgements() {
            true => DeliveredAcknowledgements::Sink,
            false => DeliveredAcknowledgements::Host,
        };
        let mut open_run: Option<ProjectedRun> = None;
        for batch_index in 0..batches.len() {
            tokio::task::consume_budget().await;
            let (source, mut projected) = {
                let batch = &batches[batch_index];
                let pending_rows = batch.pending_record_rows();
                // A batch whose rows a previous attempt already delivered has nothing left to map.
                if pending_rows.is_empty() {
                    continue;
                }
                let source = CarrierSource {
                    relay: batch.source_relay().clone(),
                    branch: batch.relay_batch().key.clone(),
                };
                let projected = self
                    .projection
                    .project(
                        batch_index,
                        batch.relay_batch(),
                        batch.execution_now(),
                        &pending_rows,
                    )
                    .await?;
                (source, projected)
            };
            let rejected = projected.take_rejected();
            finish_rejected_records(context, batches, rejected, MessageErrorOperation::Values)
                .await?;
            if projected.is_empty() {
                continue;
            }
            // One write never spans source relays or branches, so a batch of another one writes
            // what the open run holds first.
            let run = match open_run.take() {
                Some(mut run) if run.source == source => {
                    run.carriers.push(projected);
                    run
                }
                Some(run) => {
                    self.publish_run(context, batches, run, acknowledgements)
                        .await?;
                    ProjectedRun::open(source, projected)
                }
                None => ProjectedRun::open(source, projected),
            };
            open_run = Some(run);
        }
        match open_run {
            Some(run) => {
                self.publish_run(context, batches, run, acknowledgements)
                    .await
            }
            None => Ok(()),
        }
    }
}

impl MappedRowSink {
    /// Hands the sink one write of every carrier in `run`, then applies its answers.
    async fn publish_run(
        &mut self,
        context: &EmitterSinkContext,
        batches: &mut [EmitterPublishBatch],
        run: ProjectedRun,
        acknowledgements: DeliveredAcknowledgements,
    ) -> EmitterRuntimeResult<()> {
        let mut carriers = Vec::with_capacity(run.carriers.len());
        let mut rows = 0_usize;
        for projected in &run.carriers {
            // A sink that resolves acknowledgements on its own commit boundary takes the ones its
            // write carries, so the host stops owning them the moment the write accepts the rows.
            let retained = match acknowledgements {
                DeliveredAcknowledgements::Sink => {
                    let batch = &batches[projected.batch_index];
                    Some(SinkAcknowledgements::new(
                        batch.acks_for_rows(projected.selected_rows()),
                    ))
                }
                DeliveredAcknowledgements::Host => None,
            };
            rows = rows
                .checked_add(projected.selected_rows().len())
                .assured("the rows of one write are held in memory");
            carriers.push(projected.sink_carrier(retained));
        }
        let write = MappedSinkRows {
            target_columns: self.projection.target_columns(),
            carriers,
        };
        let outcome = self.sink.publish(write).await;
        let outcome = context.received_outcome(rows, outcome);
        RowAnswers::from(outcome)
            .apply(context, batches, acknowledgements)
            .await
    }
}

/// The source relay and concrete branch a buffered batch came from, which every batch of one write
/// shares. A relay has one fixed named branch declaration, so the relay and the concrete key
/// together identify the exact branch.
#[derive(PartialEq)]
struct CarrierSource {
    relay: RelayName,
    branch: Option<BranchKey>,
}

/// Successive projected batches of one source relay and concrete branch, which one write carries.
struct ProjectedRun {
    source: CarrierSource,
    carriers: Vec<ProjectedValueRows>,
}

impl ProjectedRun {
    fn open(source: CarrierSource, first: ProjectedValueRows) -> Self {
        Self {
            source,
            carriers: vec![first],
        }
    }
}

/// A row request sink and the host projection whose mapped columns it prepares requests from.
///
/// The mapping is evaluated here, once per batch, and the sink prepares each request once. The
/// emitter retains every prepared request with the rows it carries until the sink answers for it,
/// so a request whose outcome the emitter did not learn is sent again exactly as it was prepared,
/// and its rows are never mapped or prepared again.
pub(in crate::runtime) struct MappedRequestSink {
    sink: Box<dyn RowRequestSink>,
    projection: MappedValuesProjection,
}

impl MappedRequestSink {
    pub(in crate::runtime) fn new(
        sink: Box<dyn RowRequestSink>,
        projection: MappedValuesProjection,
    ) -> Self {
        Self { sink, projection }
    }
}

#[async_trait]
impl EmitterSink for MappedRequestSink {
    fn lifecycle(&self) -> &dyn SinkLifecycle {
        &*self.sink
    }

    fn lifecycle_mut(&mut self) -> &mut dyn SinkLifecycle {
        &mut *self.sink
    }

    /// Prepares requests from every row no retained request carries yet, one projection and one
    /// preparation per batch, and sends every retained request in one write: the ones earlier
    /// attempts prepared and the sink left unanswered, exactly as they were first sent, followed by
    /// the ones prepared now.
    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        publication: EmitterPublication<'_>,
    ) -> EmitterRuntimeResult<()> {
        let EmitterPublication {
            batches,
            row_requests,
            ..
        } = publication;
        for batch_index in 0..batches.len() {
            tokio::task::consume_budget().await;
            let mut projected = {
                let batch = &batches[batch_index];
                let pending_rows = batch.pending_record_rows();
                // A batch whose rows retained requests carry, or an earlier attempt resolved, has
                // nothing left to prepare.
                if pending_rows.is_empty() {
                    continue;
                }
                self.projection
                    .project(
                        batch_index,
                        batch.relay_batch(),
                        batch.execution_now(),
                        &pending_rows,
                    )
                    .await?
            };
            let rejected = projected.take_rejected();
            finish_rejected_records(context, batches, rejected, MessageErrorOperation::Values)
                .await?;
            if projected.is_empty() {
                continue;
            }
            // The host checks and retains what the sink prepared batch by batch, so each
            // preparation is handed exactly one carrier.
            let carrier = projected.sink_carrier(None);
            let occurred_at = carrier.occurred_at;
            let rows = MappedSinkRows {
                target_columns: self.projection.target_columns(),
                carriers: vec![carrier],
            };
            let preparation = self
                .sink
                .prepare(rows)
                .await
                .map_err(sink_publish_failure)?;
            let checked = CheckedPreparation::check(
                preparation,
                batch_index,
                projected.selected_rows(),
                occurred_at,
            )?;
            for request in checked.requests {
                row_requests.retain(request, batches)?;
            }
            finish_rejected_records(
                context,
                batches,
                checked.rejected,
                MessageErrorOperation::Publish,
            )
            .await?;
        }
        if row_requests.is_empty() {
            return Ok(());
        }
        let PreparedWrite { records, payloads } = row_requests.next_write();
        let request_count = records.len();
        let outcome = self.sink.publish(records).await;
        let outcome = context.received_outcome(request_count, outcome);
        row_requests
            .answers(batches, payloads, outcome)?
            .apply(context, batches, DeliveredAcknowledgements::Host)
            .await
    }
}

pub(in crate::runtime) struct CompiledSqlValuesProgram {
    program: Arc<VmCompiledProgram>,
    label: &'static str,
    error_sites: CompiledMessageErrorSites,
}

impl CompiledSqlValuesProgram {
    fn structured_side_error(
        &self,
        execution_now: Timestamp,
        reason: String,
        span: VmSpan,
    ) -> StructuredMessageError {
        let site = self.error_sites.get(&span);
        let operation = match site {
            Some(site) => site.operation,
            None => MessageErrorOperation::Values,
        };
        structured_message_error(
            execution_now,
            MessageErrorCode::Evaluation,
            reason,
            operation,
            site.and_then(|site| site.operation_index),
            site.map(|site| site.fields.iter().cloned())
                .into_iter()
                .flatten(),
        )
    }
}

fn compile_sql_values_program(
    label: &'static str,
    namespace: &'static str,
    domain: &DomainName,
    emitter: &EmitterName,
    mapping: &MappedValuesPlan,
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> error_stack::Result<CompiledSqlValuesProgram, RuntimeError> {
    let parsed = &mapping.program;
    let empty_sink_schema =
        StdArc::new(arrow_schema::Schema::new(Vec::<arrow_schema::Field>::new()));
    let infer_bindings = vec![
        VmCompileBinding::writeonly(namespace, empty_sink_schema),
        VmCompileBinding::readonly("input", input_schema.clone()),
        VmCompileBinding::readonly("message", input_schema.clone()),
    ];
    let inferred_fields = infer_vm_set_expr_types_for_bindings_with_udfs(
        parsed,
        infer_bindings,
        runtime_udf_signatures(udfs),
    )
    .map_err(|error| {
        let reason = format!(
            "{label} VALUES type inference failed for '{}': {}",
            emitter.as_str(),
            error.current_context().message
        );
        error.change_context(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason,
        })
    })?;
    let output_schema = StdArc::new(arrow_schema::Schema::new(
        inferred_fields
            .into_iter()
            .map(|inferred| {
                arrow_schema::Field::new(inferred.field, inferred.data_type, inferred.nullable)
            })
            .collect::<Vec<_>>(),
    ));
    let compile_bindings = vec![
        VmCompileBinding::writeonly(namespace, output_schema.clone()),
        VmCompileBinding::readonly("input", input_schema.clone()),
        VmCompileBinding::readonly("message", input_schema),
    ];
    let mut error_sites = compiled_message_error_sites(
        parsed,
        &vec![MessageErrorOperation::Values; parsed.inner.set.len()],
        None,
    )
    .map_err(|error| {
        let reason = format!(
            "{label} VALUES message-error metadata for '{}' is invalid: {error}",
            emitter.as_str()
        );
        error.change_context(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason,
        })
    })?;
    for site in error_sites.values_mut() {
        if site.operation != MessageErrorOperation::Values {
            continue;
        }
        let Some(index) = site.operation_index.map(|index| index.arch_into()) else {
            continue;
        };
        let Some(column) = mapping.columns.get(index) else {
            continue;
        };
        let internal_target = format!("{namespace}.c{index}");
        let external_target = format!("{namespace}.{column}");
        site.fields = SortedSet::from_unsorted(
            site.fields
                .iter()
                .map(|field| {
                    if field.as_str() == internal_target {
                        FieldPath::new(external_target.clone())
                    } else {
                        field.clone()
                    }
                })
                .collect(),
        );
    }
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        parsed,
        output_schema.clone(),
        VmSchemaSensitivity::default(),
        compile_bindings,
        runtime_udf_compile_options(
            udfs,
            VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                allow_sensitive_output: false,
                ..VmCompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        let reason = format!(
            "{label} VALUES compile failed for '{}': {}",
            emitter.as_str(),
            error.current_context().message
        );
        error.change_context(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason,
        })
    })?;
    Ok(CompiledSqlValuesProgram {
        program: Arc::new(compiled),
        label,
        error_sites,
    })
}

/// What one emitter's `VALUES` mapping is compiled from.
pub(in crate::runtime) struct MappedValuesProjectionInit<'a> {
    /// How this sink names itself in diagnostics, such as `ClickHouse`.
    pub(in crate::runtime) label: &'static str,
    /// The expression scope this sink's mapped columns are written through, such as `clickhouse`.
    pub(in crate::runtime) namespace: &'static str,
    pub(in crate::runtime) domain: &'a DomainName,
    pub(in crate::runtime) emitter: &'a EmitterName,
    pub(in crate::runtime) mapping: &'a MappedValuesPlan,
    pub(in crate::runtime) input_schema: StdArc<arrow_schema::Schema>,
    pub(in crate::runtime) udfs: Option<&'a UdfExecutor>,
}

/// One emitter's `VALUES` mapping, compiled once at start and evaluated once per batch.
///
/// The mapping is evaluated column by column: the VM writes one output column per mapped value,
/// and the projection hands those columns to the sink under their target names together with the
/// rows it must write. No row of the mapped batch is ever materialized as a scalar here.
pub(in crate::runtime) struct MappedValuesProjection {
    program: CompiledSqlValuesProgram,
    /// The mapped columns a row sink reads, named by their target columns in mapping order and
    /// nullable because a mapped expression may evaluate to a typed null.
    mapped_schema: StdArc<arrow_schema::Schema>,
    target_columns: Vec<String>,
}

impl MappedValuesProjection {
    pub(in crate::runtime) fn compile(
        init: MappedValuesProjectionInit<'_>,
    ) -> error_stack::Result<Self, RuntimeError> {
        let MappedValuesProjectionInit {
            label,
            namespace,
            domain,
            emitter,
            mapping,
            input_schema,
            udfs,
        } = init;
        let program = compile_sql_values_program(
            label,
            namespace,
            domain,
            emitter,
            mapping,
            input_schema,
            udfs,
        )?;
        let target_columns = mapping.columns.clone();
        let output_fields = program.program.output_schema.fields();
        if output_fields.len() != target_columns.len() {
            return Err(Report::new(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "{label} VALUES for '{}' produced {} columns for {} mappings",
                    emitter.as_str(),
                    output_fields.len(),
                    target_columns.len()
                ),
            }));
        }
        let fields = output_fields
            .iter()
            .zip(&target_columns)
            .map(|(field, column)| {
                arrow_schema::Field::new(column, field.data_type().clone(), true)
            })
            .collect::<Vec<_>>();
        Ok(Self {
            program,
            mapped_schema: StdArc::new(arrow_schema::Schema::new(fields)),
            target_columns,
        })
    }

    /// The mapped columns this projection produces, for a sink that validates their exact types
    /// before its first batch arrives.
    pub(in crate::runtime) fn mapped_schema(&self) -> &StdArc<arrow_schema::Schema> {
        &self.mapped_schema
    }

    /// The target column each mapped column is written to, in mapping order.
    pub(in crate::runtime) fn target_columns(&self) -> &[String] {
        &self.target_columns
    }

    /// Evaluates the mapping once for `batch` and selects the rows the sink still has to write.
    ///
    /// A row whose mapping failed carries a side error instead of a value, so it is rejected here
    /// with the structured message error its expression produced and never reaches the sink.
    pub(in crate::runtime) async fn project(
        &self,
        batch_index: usize,
        batch: &RelayRecordBatch,
        execution_now: Timestamp,
        pending_rows: &[usize],
    ) -> EmitterRuntimeResult<ProjectedValueRows> {
        let output = self.execute(batch, execution_now).await?;
        let mut selected_rows = Vec::with_capacity(pending_rows.len());
        let mut rejected = Vec::new();
        for row in pending_rows {
            let Some(side_error) = output.errors().row(*row).first() else {
                selected_rows.push(*row);
                continue;
            };
            let reason = format!(
                "{} VALUES side error {}: {} at {}",
                self.program.label,
                side_error.code().as_str(),
                side_error.reason,
                side_error.span
            );
            rejected.push(RejectedEmitterRecord {
                position: SinkRecordPosition {
                    batch_index,
                    row_index: *row,
                },
                reason: String::new(),
                structured_error: Some(self.program.structured_side_error(
                    execution_now,
                    reason,
                    side_error.span,
                )),
            });
        }
        let columns = output
            .columns()
            .iter()
            .map(VmTypedArray::to_array_ref)
            .collect::<Vec<_>>();
        let mapped =
            RecordBatch::try_new(self.mapped_schema.clone(), columns).map_err(|error| {
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "{} VALUES produced columns its mapped schema rejects: {error}",
                    self.program.label
                ))
            })?;
        Ok(ProjectedValueRows {
            batch_index,
            batch: mapped,
            selected_rows,
            rejected,
            execution_now,
        })
    }

    /// Runs the compiled mapping over one batch, producing one output column per mapped value.
    async fn execute(
        &self,
        batch: &RelayRecordBatch,
        execution_now: Timestamp,
    ) -> EmitterRuntimeResult<VmTypedBatch> {
        let side_inputs = HashMap::default();
        let lookup_columns = HashMap::default();
        let input = project_vm_input_batch(
            &self.program.program.input_schema,
            &VmInputProjectionSources {
                carrier: &batch.batch,
                namespace_batches: &[],
                strict_namespaces: &[],
                keys: &batch.keys,
                side_inputs: &side_inputs,
                ingest_metadata: None,
                lookup_columns: &lookup_columns,
                uninitialized: None,
            },
            None,
        )
        .map_err(|error| {
            error
                .change_context(EmitterRuntimeError::EncodeBatch)
                .attach_printable(format!(
                    "{} VALUES input projection failed",
                    self.program.label
                ))
        })?;
        let result = execute_program_with_selection_in_context(
            &self.program.program,
            &input,
            &VmExecutionContext {
                now: execution_now,
                injector: None,
            },
        )
        .await
        .map_err(|error| {
            error
                .change_context(EmitterRuntimeError::EncodeBatch)
                .attach_printable(format!("{} VALUES execution failed", self.program.label))
        })?;
        let row_count = batch.batch.batch().num_rows();
        if result.batch.row_count() != row_count {
            return Err(
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "{} VALUES produced {} rows for {} input records",
                    self.program.label,
                    result.batch.row_count(),
                    row_count
                )),
            );
        }
        Ok(result.batch)
    }
}

/// One batch of mapped columns, with the rows a row sink writes and the rows it never sees.
pub(in crate::runtime) struct ProjectedValueRows {
    batch_index: usize,
    batch: RecordBatch,
    selected_rows: Vec<usize>,
    /// The rows whose mapping failed, which the host reports instead of writing them.
    rejected: Vec<RejectedEmitterRecord>,
    execution_now: Timestamp,
}

impl ProjectedValueRows {
    pub(in crate::runtime) fn take_rejected(&mut self) -> Vec<RejectedEmitterRecord> {
        std::mem::take(&mut self.rejected)
    }

    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.selected_rows.is_empty()
    }

    /// The rows this write carries, which the host reads to hand their acknowledgements over to a
    /// sink that resolves them on its own commit boundary.
    pub(in crate::runtime) fn selected_rows(&self) -> &[usize] {
        &self.selected_rows
    }

    /// This batch as one carrier of a mapped-row write.
    pub(in crate::runtime) fn sink_carrier(
        &self,
        acknowledgements: Option<SinkAcknowledgements>,
    ) -> MappedSinkCarrier<'_> {
        MappedSinkCarrier {
            batch_index: self.batch_index,
            batch: &self.batch,
            selected_rows: &self.selected_rows,
            occurred_at: self.execution_now,
            acknowledgements,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use futures_util::FutureExt as _;
    use nervix_connector::{
        PerRecordOutcome, PreparedRowRequest, RowRequestPreparation, SinkPublishError,
        SinkPublishResult, SinkRecordId, SinkRowRequest,
    };
    use nervix_models::ClickHouseValueMapping;
    use parking_lot::Mutex;

    use super::*;
    use crate::{
        runtime::test_fixtures::{expression, input_schema, named, sink_context, test_schema},
        runtime_schema::test_runtime_row,
    };

    /// One carrier of a write a row sink received: its buffered batch and the rows it selected.
    #[derive(Debug, PartialEq, Eq)]
    struct RecordedCarrier {
        batch_index: usize,
        rows: Vec<usize>,
    }

    fn recorded(batch_index: usize, rows: &[usize]) -> RecordedCarrier {
        RecordedCarrier {
            batch_index,
            rows: rows.to_vec(),
        }
    }

    /// A row sink that delivers every row it is handed and records the carriers of each write.
    struct RecordingRowSink {
        writes: StdArc<parking_lot::Mutex<Vec<Vec<RecordedCarrier>>>>,
    }

    #[async_trait]
    impl SinkLifecycle for RecordingRowSink {}

    #[async_trait]
    impl RowSink for RecordingRowSink {
        async fn publish(
            &mut self,
            rows: MappedSinkRows<'_>,
        ) -> PerRecordOutcome<SinkRecordPosition> {
            let mut outcome = PerRecordOutcome::with_capacity(rows.member_count());
            for member in rows.members() {
                outcome.deliver(rows.position(member));
            }
            let mut write = Vec::new();
            for carrier in &rows.carriers {
                write.push(recorded(carrier.batch_index, carrier.selected_rows));
            }
            self.writes.lock().push(write);
            outcome
        }
    }

    fn source_batch(relay: &str, rows: i64, branch: Option<&str>) -> EmitterPublishBatch {
        let mut batch = test_batch(rows);
        batch.key = branch.map(|tenant| {
            BranchKey::from_fields([(
                named::<FieldName>("tenant"),
                RuntimeValue::String(tenant.to_string()),
            )])
            .expect("one string field makes a concrete branch key")
        });
        EmitterPublishBatch::from_input(named(relay), batch, Timestamp::from_unix_nanos(7))
    }

    /// Successive carriers of one relay and branch travel in one write, and a carrier of another
    /// relay or branch starts the next one, so a write never mixes sources.
    #[tokio::test]
    async fn a_write_carries_successive_carriers_of_one_relay_and_branch() {
        let writes = StdArc::new(parking_lot::Mutex::new(Vec::new()));
        let mut sink = MappedRowSink::new(
            Box::new(RecordingRowSink {
                writes: writes.clone(),
            }),
            test_projection(),
        );
        let mut batches = vec![
            source_batch("orders", 2, None),
            source_batch("orders", 1, None),
            source_batch("refunds", 1, None),
            source_batch("refunds", 2, Some("acme")),
            source_batch("refunds", 1, Some("acme")),
            source_batch("refunds", 1, Some("beta")),
            source_batch("orders", 1, None),
        ];

        sink.publish_batches(
            &sink_context(),
            EmitterPublication {
                batches: &mut batches,
                payloads: &mut PreparedPayloads::default(),
                requests: &mut PreparedPayloads::default(),
                row_requests: &mut PreparedPayloads::default(),
            },
        )
        .await
        .expect("every row is delivered");

        assert_eq!(
            *writes.lock(),
            vec![
                vec![recorded(0, &[0, 1]), recorded(1, &[0])],
                vec![recorded(2, &[0])],
                vec![recorded(3, &[0, 1]), recorded(4, &[0])],
                vec![recorded(5, &[0])],
                vec![recorded(6, &[0])],
            ]
        );
        assert!(
            batches
                .iter()
                .all(|batch| batch.resolved_rows().iter().all(|resolved| *resolved))
        );
    }

    fn mapping(column: &str, raw: &str) -> ClickHouseValueMapping {
        ClickHouseValueMapping {
            column: column.to_string(),
            expression: expression(raw),
        }
    }

    fn test_projection() -> MappedValuesProjection {
        let domain: DomainName = named("test_domain");
        let emitter: EmitterName = named("test_emitter");
        let schema = test_schema(&[("value", ParseAsType::I64), ("name", ParseAsType::String)]);
        let values = vec![
            mapping("id", "input.value"),
            mapping("label", "input.name"),
            mapping("doubled", "input.value * 2"),
        ];
        let mapping = MappedValuesPlan::decide(&emitter, "ClickHouse", "clickhouse", &values)
            .expect("the mapping should lower");

        MappedValuesProjection::compile(MappedValuesProjectionInit {
            label: "ClickHouse",
            namespace: "clickhouse",
            domain: &domain,
            emitter: &emitter,
            mapping: &mapping,
            input_schema: schema.arrow_schema(),
            udfs: None,
        })
        .expect("the test VALUES mapping should compile")
    }

    fn test_batch(rows: i64) -> RelayRecordBatch {
        let schema = test_schema(&[("value", ParseAsType::I64), ("name", ParseAsType::String)]);
        let messages = (0..rows)
            .map(|row| RelayMessage {
                key: None,
                record: test_runtime_row([
                    ("value".to_string(), RuntimeValue::I64(row)),
                    (
                        "name".to_string(),
                        RuntimeValue::String(format!("row-{row}")),
                    ),
                ]),
                acks: AckSet::empty(),
            })
            .collect();
        RelayRecordBatch::from_messages(schema, messages).expect("the test batch should build")
    }

    #[tokio::test]
    async fn mapped_columns_carry_every_selected_row_under_its_target_name() {
        let projection = test_projection();
        let batch = test_batch(3);

        let projected = projection
            .project(4, &batch, Timestamp::from_unix_nanos(7), &[0, 2])
            .await
            .expect("the mapping should project");

        let rows = projected.sink_carrier(None);
        assert_eq!(rows.batch_index, 4);
        assert_eq!(projection.target_columns(), ["id", "label", "doubled"]);
        assert_eq!(rows.selected_rows, [0, 2]);
        assert_eq!(rows.occurred_at, Timestamp::from_unix_nanos(7));
        assert_eq!(rows.batch.num_columns(), 3);
        assert_eq!(rows.batch.num_rows(), 3);
        let doubled = rows
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("the mapped column keeps its exact type");
        assert_eq!(doubled.values().as_ref(), [0, 2, 4]);
    }

    #[tokio::test]
    async fn a_mapping_that_fails_rejects_its_row_instead_of_selecting_it() {
        let domain: DomainName = named("test_domain");
        let emitter: EmitterName = named("test_emitter");
        let schema = test_schema(&[("value", ParseAsType::I64), ("name", ParseAsType::String)]);
        let values = vec![mapping("ratio", "100 / input.value")];
        let mapping = MappedValuesPlan::decide(&emitter, "ClickHouse", "clickhouse", &values)
            .expect("the mapping should lower");
        let projection = MappedValuesProjection::compile(MappedValuesProjectionInit {
            label: "ClickHouse",
            namespace: "clickhouse",
            domain: &domain,
            emitter: &emitter,
            mapping: &mapping,
            input_schema: schema.arrow_schema(),
            udfs: None,
        })
        .expect("the test VALUES mapping should compile");
        let batch = test_batch(3);

        let mut projected = projection
            .project(0, &batch, Timestamp::from_unix_nanos(7), &[0, 1, 2])
            .await
            .expect("the mapping should project");

        let rejected = projected.take_rejected();
        assert_eq!(projected.sink_carrier(None).selected_rows, [1, 2]);
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].position.row_index, 0);
        let error = rejected[0]
            .structured_error
            .as_ref()
            .expect("a failed mapping carries its structured message error");
        assert_eq!(error.operation, MessageErrorOperation::Values);
        assert!(
            error.message.contains("ClickHouse VALUES side error"),
            "unexpected side error message: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn projection_failure_keeps_the_schema_report_under_sink_context() {
        let projection = test_projection();
        let schema = test_schema(&[("other", ParseAsType::I64)]);
        let batch = RelayRecordBatch::from_messages(
            schema,
            vec![RelayMessage {
                key: None,
                record: test_runtime_row([("other".to_string(), RuntimeValue::I64(1))]),
                acks: AckSet::empty(),
            }],
        )
        .expect("the unrelated relay batch still has a valid schema");
        let report = projection
            .execute(&batch, Timestamp::from_unix_nanos(7))
            .await
            .expect_err("the VALUES program requires fields the relay batch lacks");
        assert!(matches!(
            report.current_context(),
            EmitterRuntimeError::EncodeBatch
        ));
        assert!(report.contains::<RuntimeSchemaError>());
        assert!(format!("{report:?}").contains("ClickHouse VALUES input projection failed"));
    }

    /// The projection evaluates one program and builds one Arrow batch, so its allocation count is
    /// a property of the batch and not of the rows inside it.
    #[tokio::test]
    async fn projecting_allocates_per_batch_and_not_per_row() {
        let projection = test_projection();
        let narrow = test_batch(8);
        let wide = test_batch(512);
        let narrow_rows = (0..8).collect::<Vec<_>>();
        let wide_rows = (0..512).collect::<Vec<_>>();
        let execution_now = Timestamp::from_unix_nanos(7);
        // The first projection resolves the lazily built parts of the program, so both counted
        // projections measure steady-state work.
        projection
            .project(0, &narrow, execution_now, &narrow_rows)
            .await
            .expect("the warm-up projection should succeed");

        // Each projection is polled exactly once, so no other task runs on this thread while the
        // counter is reading. Yielding first gives that poll a fresh scheduling budget.
        tokio::task::yield_now().await;
        let (narrow_allocations, narrow_projected) = alloc_count::alloc_count!({
            projection
                .project(0, &narrow, execution_now, &narrow_rows)
                .now_or_never()
        });
        tokio::task::yield_now().await;
        let (wide_allocations, wide_projected) = alloc_count::alloc_count!({
            projection
                .project(0, &wide, execution_now, &wide_rows)
                .now_or_never()
        });

        assert_eq!(
            narrow_projected
                .expect("projecting a batch never waits")
                .expect("the narrow projection should succeed")
                .sink_carrier(None)
                .selected_rows
                .len(),
            8
        );
        assert_eq!(
            wide_projected
                .expect("projecting a batch never waits")
                .expect("the wide projection should succeed")
                .sink_carrier(None)
                .selected_rows
                .len(),
            512
        );
        assert_eq!(
            (
                narrow_allocations.alloc_calls,
                narrow_allocations.realloc_calls
            ),
            (wide_allocations.alloc_calls, wide_allocations.realloc_calls),
            "projecting 64 times as many rows must cost the same allocations"
        );
    }

    /// How the scripted row request sink answers one write.
    enum RequestAnswer {
        /// The first request is delivered, and the sink fails before it answers for the rest.
        DeliverFirstThenFail,
        /// Every request of the write is delivered.
        DeliverAll,
    }

    /// One request the scripted sink was handed: its identity and its bytes.
    #[derive(Debug, PartialEq, Eq)]
    struct HandedRequest {
        id: usize,
        body: Vec<u8>,
    }

    impl HandedRequest {
        fn new(id: usize, body: &str) -> Self {
            Self {
                id,
                body: body.as_bytes().to_vec(),
            }
        }
    }

    /// A row request sink that prepares one request for each pair of rows, whose bytes name the
    /// rows and the preparation that made them, and answers every write from a script.
    struct ScriptedRequestSink {
        preparations: Arc<AtomicUsize>,
        writes: Arc<Mutex<Vec<Vec<HandedRequest>>>>,
        answers: VecDeque<RequestAnswer>,
    }

    impl SinkLifecycle for ScriptedRequestSink {}

    #[async_trait]
    impl RowRequestSink for ScriptedRequestSink {
        async fn prepare(
            &mut self,
            rows: MappedSinkRows<'_>,
        ) -> SinkPublishResult<RowRequestPreparation> {
            let preparation = self.preparations.fetch_add(1, Ordering::SeqCst);
            let mut prepared = RowRequestPreparation::default();
            for carrier in &rows.carriers {
                for pair in carrier.selected_rows.chunks(2) {
                    let mut members = Vec::with_capacity(pair.len());
                    for row in pair {
                        members.push(SinkRecordPosition {
                            batch_index: carrier.batch_index,
                            row_index: *row,
                        });
                    }
                    prepared.requests.push(PreparedRowRequest {
                        members,
                        body: format!("preparation {preparation} of {pair:?}").into_bytes(),
                    });
                }
            }
            Ok(prepared)
        }

        async fn publish(
            &mut self,
            requests: Vec<SinkRowRequest>,
        ) -> PerRecordOutcome<SinkRecordId> {
            self.writes.lock().push(
                requests
                    .iter()
                    .map(|request| HandedRequest {
                        id: request.id.index(),
                        body: request.body.clone(),
                    })
                    .collect(),
            );
            let mut outcome = PerRecordOutcome::with_capacity(requests.len());
            let answer = self
                .answers
                .pop_front()
                .expect("the test scripts an answer for every write it makes");
            match answer {
                RequestAnswer::DeliverFirstThenFail => {
                    outcome.deliver(requests[0].id);
                    outcome.fail(Report::new(SinkPublishError::Publish { sink: "scripted" }));
                }
                RequestAnswer::DeliverAll => {
                    for request in &requests {
                        outcome.deliver(request.id);
                    }
                }
            }
            outcome
        }
    }

    /// One buffered batch whose rows each carry an acknowledgement root of their own, and the
    /// completions of those roots in row order.
    fn acknowledged_batch(values: &[i64]) -> (EmitterPublishBatch, Vec<AckCompletion>) {
        let mut messages = Vec::with_capacity(values.len());
        let mut completions = Vec::with_capacity(values.len());
        for value in values {
            let (acks, completion) = AckSet::root();
            messages.push(RelayMessage {
                key: None,
                record: test_runtime_row([("value".to_string(), RuntimeValue::I64(*value))]),
                acks,
            });
            completions.push(completion);
        }
        let batch = RelayRecordBatch::from_messages(input_schema(), messages)
            .expect("the test rows match the emitter input schema");
        (
            EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(10)),
            completions,
        )
    }

    /// The projection an OTEL-like emitter maps its one input value through.
    fn value_projection() -> MappedValuesProjection {
        let domain: DomainName = named("test_domain");
        let emitter: EmitterName = named("test_emitter");
        let values = [mapping("value", "input.value")];
        let mapping = MappedValuesPlan::decide(&emitter, "OTEL", "otel", &values)
            .expect("the mapping should lower");
        MappedValuesProjection::compile(MappedValuesProjectionInit {
            label: "OTEL",
            namespace: "otel",
            domain: &domain,
            emitter: &emitter,
            mapping: &mapping,
            input_schema: input_schema().arrow_schema(),
            udfs: None,
        })
        .expect("the test VALUES mapping should compile")
    }

    /// A scripted row request sink paired with its projection, and what the test reads back from
    /// it: how many preparations it made, and every write it was handed.
    struct ScriptedRequests {
        sink: MappedRequestSink,
        preparations: Arc<AtomicUsize>,
        writes: Arc<Mutex<Vec<Vec<HandedRequest>>>>,
    }

    fn scripted_requests(answers: impl IntoIterator<Item = RequestAnswer>) -> ScriptedRequests {
        let preparations = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let sink = ScriptedRequestSink {
            preparations: preparations.clone(),
            writes: writes.clone(),
            answers: answers.into_iter().collect(),
        };
        ScriptedRequests {
            sink: MappedRequestSink::new(Box::new(sink), value_projection()),
            preparations,
            writes,
        }
    }

    #[tokio::test]
    async fn a_request_whose_outcome_is_unknown_is_sent_again_unchanged_without_preparing_it_again()
    {
        let context = sink_context();
        let ScriptedRequests {
            mut sink,
            preparations,
            writes,
        } = scripted_requests([
            RequestAnswer::DeliverFirstThenFail,
            RequestAnswer::DeliverAll,
        ]);
        let (first, first_completions) = acknowledged_batch(&[1, 2, 3]);
        let mut batches = vec![first];
        let mut row_requests = PreparedPayloads::default();

        let failed = sink
            .publish_batches(
                &context,
                EmitterPublication {
                    batches: &mut batches,
                    payloads: &mut PreparedPayloads::default(),
                    requests: &mut PreparedPayloads::default(),
                    row_requests: &mut row_requests,
                },
            )
            .await
            .expect_err("the sink failed before answering for its second request");
        assert!(emitter_publish_error_is_retryable(&failed));
        assert_eq!(batches[0].resolved_rows(), vec![true, true, false]);
        assert!(
            batches[0].pending_record_rows().is_empty(),
            "the unanswered request still carries its row, so no attempt prepares it again"
        );

        // A batch buffered after the failed attempt is prepared on its own, after the request the
        // emitter kept.
        let (second, second_completions) = acknowledged_batch(&[4]);
        batches.push(second);
        sink.publish_batches(
            &context,
            EmitterPublication {
                batches: &mut batches,
                payloads: &mut PreparedPayloads::default(),
                requests: &mut PreparedPayloads::default(),
                row_requests: &mut row_requests,
            },
        )
        .await
        .expect("the retry is delivered");

        assert_eq!(
            *writes.lock(),
            vec![
                vec![
                    HandedRequest::new(0, "preparation 0 of [0, 1]"),
                    HandedRequest::new(1, "preparation 0 of [2]"),
                ],
                vec![
                    HandedRequest::new(0, "preparation 0 of [2]"),
                    HandedRequest::new(1, "preparation 1 of [0]"),
                ],
            ],
            "the retry sends the kept request byte for byte, ahead of the one prepared after it"
        );
        assert_eq!(
            preparations.load(Ordering::SeqCst),
            2,
            "each batch is prepared once"
        );
        assert!(row_requests.is_empty());
        for completion in first_completions.into_iter().chain(second_completions) {
            assert_eq!(completion.wait().await, AckOutcome::Ack);
        }
    }

    /// A row request sink whose preparation answers for a row it was never handed.
    struct ForeignRowSink;

    impl SinkLifecycle for ForeignRowSink {}

    #[async_trait]
    impl RowRequestSink for ForeignRowSink {
        async fn prepare(
            &mut self,
            rows: MappedSinkRows<'_>,
        ) -> SinkPublishResult<RowRequestPreparation> {
            let carrier = rows
                .carriers
                .first()
                .expect("the host hands every preparation one carrier");
            let mut members = Vec::new();
            for row in carrier.selected_rows {
                members.push(SinkRecordPosition {
                    batch_index: carrier.batch_index,
                    row_index: *row,
                });
            }
            members.push(SinkRecordPosition {
                batch_index: carrier.batch_index,
                row_index: carrier.batch.num_rows(),
            });
            Ok(RowRequestPreparation {
                requests: vec![PreparedRowRequest {
                    members,
                    body: b"every row and one more".to_vec(),
                }],
                rejected: Vec::new(),
            })
        }

        async fn publish(
            &mut self,
            _requests: Vec<SinkRowRequest>,
        ) -> PerRecordOutcome<SinkRecordId> {
            panic!("a preparation that breaks its contract is never sent");
        }
    }

    #[tokio::test]
    async fn a_preparation_that_breaks_its_contract_keeps_nothing_and_is_not_retried() {
        let context = sink_context();
        let mut sink = MappedRequestSink::new(Box::new(ForeignRowSink), value_projection());
        let (batch, _completions) = acknowledged_batch(&[1, 2]);
        let mut batches = vec![batch];
        let mut row_requests = PreparedPayloads::default();

        let error = sink
            .publish_batches(
                &context,
                EmitterPublication {
                    batches: &mut batches,
                    payloads: &mut PreparedPayloads::default(),
                    requests: &mut PreparedPayloads::default(),
                    row_requests: &mut row_requests,
                },
            )
            .await
            .expect_err("the preparation names a row the write did not hand over");

        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::RowPreparation {
                batch_index: 0,
                violation: RowPreparationViolation::Unselected { batch: 0, row: 2 },
            }
        );
        assert!(!emitter_publish_error_is_retryable(&error));
        assert!(row_requests.is_empty());
        assert_eq!(batches[0].pending_record_rows(), vec![0, 1]);
    }

    #[test]
    fn mapped_value_decision_requires_mappings() {
        let emitter = EmitterName::parse("output").expect("valid emitter name");
        let errors =
            [("Iceberg", "iceberg"), ("ClickHouse", "clickhouse")].map(|(label, namespace)| {
                MappedValuesPlan::decide(&emitter, label, namespace, &[]).err()
            });
        for result in errors {
            let Some(error) = result else {
                panic!("empty VALUES mappings must fail before compilation")
            };
            assert!(
                error
                    .to_string()
                    .contains("requires at least one VALUES mapping")
            );
        }
    }

    #[test]
    fn sql_value_type_failure_keeps_emitter_and_vm_compile_context() {
        let domain: DomainName = named("test_domain");
        let emitter: EmitterName = named("test_emitter");
        let schema = test_schema(&[("value", ParseAsType::I64)]);
        let values = [mapping("external_id", "input.missing")];
        let mapping = MappedValuesPlan::decide(&emitter, "ClickHouse", "clickhouse", &values)
            .assured("the syntactically valid fixture mapping lowers before type checking");
        let report = MappedValuesProjection::compile(MappedValuesProjectionInit {
            label: "ClickHouse",
            namespace: "clickhouse",
            domain: &domain,
            emitter: &emitter,
            mapping: &mapping,
            input_schema: schema.arrow_schema(),
            udfs: None,
        })
        .err()
        .assured("the mapping refers to a field absent from the declared input schema");
        assert!(matches!(
            report.current_context(),
            RuntimeError::BuildDomainExecution { domain, reason }
                if domain == "test_domain"
                    && reason.contains("ClickHouse VALUES type inference failed")
                    && reason.contains("test_emitter")
                    && reason.contains("missing")
        ));
        assert!(report.contains::<nervix_vm::CompileError>());
    }
}
