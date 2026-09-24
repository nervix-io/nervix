//! The host projection every row sink writes from.
//!
//! Layer: data plane.
//! - **Owns.** Compiling one emitter's `VALUES` mapping once, evaluating it once per batch into an
//!   Arrow batch of mapped columns, the row selection and chunk ranges that batch is written in,
//!   and handing each projected batch to the row sink it is mapped for.
//! - **Depends on.** The VM's compile and execute API, Arrow batches and the connector contract's
//!   row sink and mapped-rows value type.
//! - **Must not know.** Which external system consumes the mapped rows, or how it encodes them.

use std::ops::Range;

use async_trait::async_trait;
use nervix_connector::{
    MappedSinkRows, RowSink, SinkAcknowledgements, SinkLifecycle, SinkRecordPosition,
};
use nervix_models::BatchMessageLimit;

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

    /// Writes every buffered batch, one projection and one virtual call per batch.
    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()> {
        let acknowledgements = match self.sink.retains_acknowledgements() {
            true => DeliveredAcknowledgements::Sink,
            false => DeliveredAcknowledgements::Host,
        };
        for batch_index in 0..batches.len() {
            tokio::task::consume_budget().await;
            let mut projected = {
                let batch = &batches[batch_index];
                let pending_rows = batch.pending_record_rows();
                // A batch whose rows a previous attempt already delivered has nothing left to map.
                if pending_rows.is_empty() {
                    continue;
                }
                self.projection
                    .project(
                        batch_index,
                        &batch.batch,
                        batch.execution_now,
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
            // A sink that resolves acknowledgements on its own commit boundary takes the ones its
            // write carries, so the host stops owning them the moment the write accepts the rows.
            let retained = match acknowledgements {
                DeliveredAcknowledgements::Sink => {
                    let batch = &batches[batch_index];
                    Some(SinkAcknowledgements::new(
                        batch.acks_for_rows(projected.selected_rows()),
                    ))
                }
                DeliveredAcknowledgements::Host => None,
            };
            let outcome = self.sink.publish(projected.rows(retained)).await;
            finish_record_sink_publish(context, batches, outcome, acknowledgements).await?;
        }
        Ok(())
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
    values: &[ClickHouseValueMapping],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledSqlValuesProgram, RuntimeError> {
    if values.is_empty() {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "{label} emitter '{}' requires at least one VALUES mapping",
                emitter.as_str()
            ),
        });
    }
    let mut assignments = Vec::with_capacity(values.len());
    for (index, mapping) in values.iter().enumerate() {
        let field = FieldName::parse(&format!("c{index}")).assured(
            "a generated name containing c followed by decimal digits is a valid field name",
        );
        assignments.push(nervix_models::Assignment {
            target: nervix_models::AssignmentTarget::bare(field),
            value: mapping.expression.clone(),
        });
    }
    let parsed = lower_route_construction(
        &nervix_models::RouteConstruction {
            assignments,
            ..nervix_models::RouteConstruction::default()
        },
        nervix_vm::SemanticScopePolicy::read_write("input", namespace),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "{label} VALUES for '{}' is invalid: {reason}",
            emitter.as_str()
        ),
    })?;
    let empty_sink_schema =
        StdArc::new(arrow_schema::Schema::new(Vec::<arrow_schema::Field>::new()));
    let infer_bindings = vec![
        VmCompileBinding::writeonly(namespace, empty_sink_schema),
        VmCompileBinding::readonly("input", input_schema.clone()),
        VmCompileBinding::readonly("message", input_schema.clone()),
    ];
    let inferred_fields = infer_vm_set_expr_types_for_bindings_with_udfs(
        &parsed,
        infer_bindings,
        runtime_udf_signatures(udfs),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "{label} VALUES type inference failed for '{}': {}",
            emitter.as_str(),
            error.message
        ),
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
        &parsed,
        &vec![MessageErrorOperation::Values; parsed.inner.set.len()],
        None,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "{label} VALUES message-error metadata for '{}' is invalid: {reason}",
            emitter.as_str()
        ),
    })?;
    for site in error_sites.values_mut() {
        if site.operation != MessageErrorOperation::Values {
            continue;
        }
        let Some(index) = site.operation_index.map(|index| index.arch_into()) else {
            continue;
        };
        let Some(mapping) = values.get(index) else {
            continue;
        };
        let internal_target = format!("{namespace}.c{index}");
        let external_target = format!("{namespace}.{}", mapping.column);
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
        &parsed,
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
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "{label} VALUES compile failed for '{}': {}",
            emitter.as_str(),
            error.message
        ),
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
    pub(in crate::runtime) values: &'a [ClickHouseValueMapping],
    pub(in crate::runtime) input_schema: StdArc<arrow_schema::Schema>,
    pub(in crate::runtime) udfs: Option<&'a UdfExecutor>,
    /// How many rows one write may carry, from the emitter's `BATCH MAX MESSAGES`. A sink that
    /// publishes a whole batch in one request declares none.
    pub(in crate::runtime) max_batch: Option<BatchMessageLimit>,
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
    max_rows: Option<NonZeroUsize>,
}

impl MappedValuesProjection {
    pub(in crate::runtime) fn compile(
        init: MappedValuesProjectionInit<'_>,
    ) -> Result<Self, RuntimeError> {
        let MappedValuesProjectionInit {
            label,
            namespace,
            domain,
            emitter,
            values,
            input_schema,
            udfs,
            max_batch,
        } = init;
        let program = compile_sql_values_program(
            label,
            namespace,
            domain,
            emitter,
            values,
            input_schema,
            udfs,
        )?;
        let target_columns = values
            .iter()
            .map(|mapping| mapping.column.clone())
            .collect::<Vec<_>>();
        let output_fields = program.program.output_schema.fields();
        if output_fields.len() != target_columns.len() {
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "{label} VALUES for '{}' produced {} columns for {} mappings",
                    emitter.as_str(),
                    output_fields.len(),
                    target_columns.len()
                ),
            });
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
            max_rows: max_batch.map(|limit| addressable_count(NonZeroU64::from(limit.get()))),
        })
    }

    /// The mapped columns this projection produces, for a sink that validates their exact types
    /// before its first batch arrives.
    pub(in crate::runtime) fn mapped_schema(&self) -> &StdArc<arrow_schema::Schema> {
        &self.mapped_schema
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
        let chunks = self.row_chunks(selected_rows.len());
        Ok(ProjectedValueRows {
            batch_index,
            batch: mapped,
            target_columns: self.target_columns.clone(),
            selected_rows,
            chunks,
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
        .map_err(|error| Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(error))?;
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
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "{} VALUES execution failed: {error}",
                self.program.label
            ))
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

    /// The ranges of selected rows one write may carry, from the emitter's `MAX BATCH`.
    ///
    /// The ranges are counted before they are built, so a batch costs one allocation here however
    /// many rows it selected.
    fn row_chunks(&self, selected: usize) -> Vec<Range<usize>> {
        if selected == 0 {
            return Vec::new();
        }
        // A sink that publishes a whole batch in one request writes every selected row at once.
        let max_rows = match self.max_rows {
            Some(max_rows) => max_rows.get(),
            None => selected,
        };
        let mut chunks = Vec::with_capacity(selected.div_ceil(max_rows));
        let mut start = 0;
        while start < selected {
            let end = start
                .checked_add(max_rows)
                .assured("a chunk starts inside a selection this node already holds in memory")
                .min(selected);
            chunks.push(start..end);
            start = end;
        }
        chunks
    }
}

/// One batch of mapped columns, with the rows a row sink writes and the rows it never sees.
pub(in crate::runtime) struct ProjectedValueRows {
    batch_index: usize,
    batch: RecordBatch,
    target_columns: Vec<String>,
    selected_rows: Vec<usize>,
    chunks: Vec<Range<usize>>,
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

    pub(in crate::runtime) fn rows(
        &self,
        acknowledgements: Option<SinkAcknowledgements>,
    ) -> MappedSinkRows<'_> {
        MappedSinkRows {
            batch_index: self.batch_index,
            batch: &self.batch,
            target_columns: &self.target_columns,
            selected_rows: &self.selected_rows,
            selected_row_chunks: &self.chunks,
            occurred_at: self.execution_now,
            acknowledgements,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt as _;

    use super::*;
    use crate::{
        runtime::test_fixtures::{expression, input_schema, named, test_schema},
        runtime_schema::test_runtime_row,
    };

    fn mapping(column: &str, raw: &str) -> ClickHouseValueMapping {
        ClickHouseValueMapping {
            column: column.to_string(),
            expression: expression(raw),
        }
    }

    fn test_projection(max_batch: Option<BatchMessageLimit>) -> MappedValuesProjection {
        let domain: DomainName = named("test_domain");
        let emitter: EmitterName = named("test_emitter");
        let schema = test_schema(&[("value", ParseAsType::I64), ("name", ParseAsType::String)]);
        let values = vec![
            mapping("id", "input.value"),
            mapping("label", "input.name"),
            mapping("doubled", "input.value * 2"),
        ];

        MappedValuesProjection::compile(MappedValuesProjectionInit {
            label: "ClickHouse",
            namespace: "clickhouse",
            domain: &domain,
            emitter: &emitter,
            values: &values,
            input_schema: schema.arrow_schema(),
            udfs: None,
            max_batch,
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
        let projection = test_projection(BatchMessageLimit::try_from(2u32).ok());
        let batch = test_batch(3);

        let projected = projection
            .project(4, &batch, Timestamp::from_unix_nanos(7), &[0, 2])
            .await
            .expect("the mapping should project");

        let rows = projected.rows(None);
        assert_eq!(rows.batch_index, 4);
        assert_eq!(rows.target_columns, ["id", "label", "doubled"]);
        assert_eq!(rows.selected_rows, [0, 2]);
        assert_eq!(rows.selected_row_chunks.len(), 1);
        assert_eq!(rows.selected_row_chunks.first(), Some(&(0..2)));
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
        let projection = MappedValuesProjection::compile(MappedValuesProjectionInit {
            label: "ClickHouse",
            namespace: "clickhouse",
            domain: &domain,
            emitter: &emitter,
            values: &values,
            input_schema: schema.arrow_schema(),
            udfs: None,
            max_batch: None,
        })
        .expect("the test VALUES mapping should compile");
        let batch = test_batch(3);

        let mut projected = projection
            .project(0, &batch, Timestamp::from_unix_nanos(7), &[0, 1, 2])
            .await
            .expect("the mapping should project");

        let rejected = projected.take_rejected();
        assert_eq!(projected.rows(None).selected_rows, [1, 2]);
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

    /// The projection evaluates one program and builds one Arrow batch, so its allocation count is
    /// a property of the batch and not of the rows inside it.
    #[tokio::test]
    async fn projecting_allocates_per_batch_and_not_per_row() {
        let projection = test_projection(BatchMessageLimit::try_from(4u32).ok());
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
                .rows(None)
                .selected_rows
                .len(),
            8
        );
        assert_eq!(
            wide_projected
                .expect("projecting a batch never waits")
                .expect("the wide projection should succeed")
                .rows(None)
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

    #[test]
    fn sql_value_compilers_reject_empty_mappings_before_compilation() {
        let domain = DomainName::parse("emitter_tests").expect("valid domain");
        let emitter = EmitterName::parse("output").expect("valid emitter name");
        let schema = input_schema().arrow_schema();

        let errors =
            [("Iceberg", "iceberg"), ("ClickHouse", "clickhouse")].map(|(label, namespace)| {
                MappedValuesProjection::compile(MappedValuesProjectionInit {
                    label,
                    namespace,
                    domain: &domain,
                    emitter: &emitter,
                    values: &[],
                    input_schema: schema.clone(),
                    udfs: None,
                    max_batch: None,
                })
                .err()
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
}
