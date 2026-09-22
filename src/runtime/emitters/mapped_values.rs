//! The host projection every row sink writes from.
//!
//! Layer: data plane.
//! - **Owns.** Compiling one emitter's `VALUES` mapping once, evaluating it once per batch into an
//!   Arrow batch of mapped columns, and the row selection and chunk ranges that batch is written
//!   in.
//! - **Depends on.** The VM's compile and execute API, Arrow batches and the connector contract's
//!   mapped-rows value type.
//! - **Must not know.** Which external system consumes the mapped rows, or how it encodes them.

use std::ops::Range;

use super::*;

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
    /// How many rows one write may carry, from the emitter's `MAX BATCH`. A sink that publishes a
    /// whole batch in one request declares none.
    pub(in crate::runtime) max_batch: Option<NonZeroU64>,
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
            max_rows: max_batch.map(addressable_count),
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

    pub(in crate::runtime) fn rows(&self) -> MappedSinkRows<'_> {
        MappedSinkRows {
            batch_index: self.batch_index,
            batch: &self.batch,
            target_columns: &self.target_columns,
            selected_rows: &self.selected_rows,
            selected_row_chunks: &self.chunks,
            occurred_at: self.execution_now,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt as _;

    use super::*;
    use crate::{
        runtime::test_fixtures::{expression, named, test_schema},
        runtime_schema::test_runtime_row,
    };

    fn mapping(column: &str, raw: &str) -> ClickHouseValueMapping {
        ClickHouseValueMapping {
            column: column.to_string(),
            expression: expression(raw),
        }
    }

    fn test_projection(max_batch: Option<NonZeroU64>) -> MappedValuesProjection {
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
        let projection = test_projection(NonZeroU64::new(2));
        let batch = test_batch(3);

        let projected = projection
            .project(4, &batch, Timestamp::from_unix_nanos(7), &[0, 2])
            .await
            .expect("the mapping should project");

        let rows = projected.rows();
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
        assert_eq!(projected.rows().selected_rows, [1, 2]);
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
        let projection = test_projection(NonZeroU64::new(4));
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
                .rows()
                .selected_rows
                .len(),
            8
        );
        assert_eq!(
            wide_projected
                .expect("projecting a batch never waits")
                .expect("the wide projection should succeed")
                .rows()
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
}
