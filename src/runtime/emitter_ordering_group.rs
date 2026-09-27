//! The ordering group the emitter host evaluates for every record it publishes.
//!
//! Layer: data plane.
//! - **Owns.** Compiling the ordering group an emitter's plan declares, evaluating it over each
//!   filtered source batch, carrying the result beside the batch through the emitter's route
//!   selection, and the reason a row has no group.
//! - **Depends on.** The emitter's start plan, the expression VM that evaluates a group expression,
//!   and the relay batches it runs over.
//! - **Must not know.** Which sink writes records under a group, what the external system does with
//!   it, or when the emitter publishes.
//!
//! A sink that orders records per group receives each record's group already evaluated, so the
//! expression that produced it, and the reason a record has none, both stay with the host.

use arrow_array::UInt64Array;

use super::{filter_map::ExecutedFilterMap, *};

/// The column a compiled ordering-group expression writes each row's group into.
const ORDERING_GROUP_FIELD: &str = "ordering_group";

/// How the host evaluates the ordering group of an emitter whose plan declares one.
#[derive(Debug, Clone)]
pub(super) enum CompiledOrderingGroup {
    /// Every record is published under its batch's concrete branch key.
    FromBranch,
    /// Every record is published under the `STRING` this program produces for its source row.
    Expression(CompiledProgramWithMaterializedInterest),
}

impl CompiledOrderingGroup {
    /// Compiles the group `declared` against the emitter's input schema.
    pub(super) fn compile(
        declared: &EmitterOrderingGroup,
        domain: &DomainName,
        emitter: &EmitterName,
        input: RuntimeVmSchema,
        context: RuntimeVmCompileContext<'_>,
    ) -> Result<Self, RuntimeError> {
        let expression = match declared {
            EmitterOrderingGroup::FromBranch => return Ok(Self::FromBranch),
            EmitterOrderingGroup::Expression(expression) => expression,
        };
        let field = FieldName::parse(ORDERING_GROUP_FIELD)
            .assured("this is a constant literal that satisfies the identifier grammar");
        let output_schema = StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            field.as_str(),
            ArrowDataType::Utf8,
            false,
        )]));
        let construction = RouteConstruction {
            assignments: vec![Assignment {
                target: nervix_models::AssignmentTarget::bare(field),
                value: expression.clone(),
            }],
            ..RouteConstruction::default()
        };
        let parsed =
            lower_transforming_route(&construction, input.schema.as_ref(), output_schema.as_ref())
                .map_err(|reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "ordering group expression for emitter '{}' is invalid: {reason}",
                        emitter.as_str()
                    ),
                })?;
        let error_sites =
            compiled_message_error_sites(&parsed, &[MessageErrorOperation::Set], None).map_err(
                |reason| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("{reason:#}"),
                },
            )?;
        let program = compile_emitter_filter_map_part(
            RuntimeCompileTarget {
                domain,
                identifier: &ModelName::from(emitter),
            },
            parsed,
            RuntimeVmSchemaPair {
                input: input.schema,
                input_sensitivity: input.sensitivity,
                output: output_schema,
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            true,
            error_sites,
            context,
        )?;
        Ok(Self::Expression(program))
    }

    /// The ordering group of every row of `batch`.
    ///
    /// Fails only when the group cannot be evaluated over the batch at all. A row whose own group
    /// cannot be evaluated keeps its reason, and the emitter rejects that row when it publishes.
    pub(super) async fn evaluate(
        &self,
        emitter: &EmitterName,
        batch: &RelayRecordBatch,
        execution_now: Timestamp,
        side_inputs: &HashMap<String, RuntimeValue>,
    ) -> PlannedGeneralResult<OrderingGroups> {
        let program = match self {
            Self::FromBranch => return Ok(OrderingGroups::from_branch(batch.key.as_ref())),
            Self::Expression(program) => program,
        };
        let executed = execute_filter_map_program_on_batch(
            "emitter",
            emitter,
            program,
            FilterMapBatchInputs {
                carrier: &batch.batch,
                namespace_batches: &[],
                keys: &batch.keys,
                side_inputs,
                ingest_metadata: None,
            },
            execution_now,
            batch.acks.clone(),
            None,
        )
        .await?;
        let groups = EvaluatedOrderingGroups::from_executed(emitter, batch, executed)?;
        Ok(OrderingGroups::Evaluated(groups))
    }
}

/// Why one row has no ordering group. The emitter rejects that row when it publishes it.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum OrderingGroupError {
    #[error("ordering group FROM BRANCH received an unbranched record")]
    UnbranchedRecord,
    #[error("ordering group expression omitted its input row")]
    OmittedInputRow,
    #[error("ordering group expression failed with {} at {span}", .code.as_str())]
    ExpressionFailed {
        code: nervix_vm::ErrorCode,
        span: VmSpan,
    },
    #[error("ordering group expression produced NULL")]
    Null,
}

/// The ordering group of every row of one emitter batch, as the host evaluated it.
#[derive(Debug, Clone)]
pub(super) enum OrderingGroups {
    /// Every row is published under the batch's concrete branch key.
    Branch(BranchKey),
    /// No row has a group, for the one reason every row shares.
    Unavailable(OrderingGroupError),
    /// Each row's own group, evaluated from the emitter's expression.
    Evaluated(EvaluatedOrderingGroups),
}

impl OrderingGroups {
    /// The groups of a batch whose records are published under their branch key.
    fn from_branch(key: Option<&BranchKey>) -> Self {
        match key {
            Some(key) => Self::Branch(key.clone()),
            None => Self::Unavailable(OrderingGroupError::UnbranchedRecord),
        }
    }

    /// The groups of `source_rows`, in that order: the rows an emitter route kept, each with the
    /// group its own source row produced.
    pub(super) fn select(&self, source_rows: &[usize]) -> EmitterRuntimeResult<Self> {
        match self {
            Self::Branch(_) | Self::Unavailable(_) => Ok(self.clone()),
            Self::Evaluated(groups) => {
                let selected = groups.select(source_rows)?;
                Ok(Self::Evaluated(selected))
            }
        }
    }

    /// How many rows these groups describe, absent when they describe every row alike.
    pub(super) fn row_count(&self) -> Option<usize> {
        match self {
            Self::Branch(_) | Self::Unavailable(_) => None,
            Self::Evaluated(groups) => Some(groups.groups.len()),
        }
    }

    /// The group row `row` is published under, or why it has none. `None` means these groups
    /// describe no such row.
    pub(super) fn group(&self, row: usize) -> Option<Result<&str, &OrderingGroupError>> {
        match self {
            Self::Branch(key) => Some(Ok(key.as_str())),
            Self::Unavailable(error) => Some(Err(error)),
            Self::Evaluated(groups) => groups.group(row),
        }
    }

    /// The bytes these groups hold beyond the batch they describe.
    pub(super) fn estimated_bytes(&self) -> u64 {
        match self {
            // The branch key is the one the batch already carries.
            Self::Branch(_) | Self::Unavailable(_) => 0,
            Self::Evaluated(groups) => groups.groups.values().len().arch_into(),
        }
    }
}

/// Each row's own ordering group, as one Arrow column beside the batch.
///
/// A row whose group could not be evaluated is null in `groups`, and `failures` holds its reason.
/// They are only written together, by [`EvaluatedOrderingGroupsBuilder`] and
/// [`Self::select`], so every null has exactly one reason and every reason one null.
#[derive(Debug, Clone)]
pub(super) struct EvaluatedOrderingGroups {
    groups: StringArray,
    failures: BTreeMap<usize, OrderingGroupError>,
}

impl EvaluatedOrderingGroups {
    /// The groups `executed` produced for the rows of `batch`.
    ///
    /// The program selects each input row it evaluated, so a row it left out, a row whose
    /// evaluation recorded an error, and a row it produced no value for are the rows without a
    /// group.
    fn from_executed(
        emitter: &EmitterName,
        batch: &RelayRecordBatch,
        executed: ExecutedFilterMap,
    ) -> PlannedGeneralResult<Self> {
        let evaluation_failed = |reason: String| {
            Report::new(PlannedGeneralError {
                acks: batch.acks.clone(),
                reason: format!(
                    "emitter '{}' ordering group expression {reason}",
                    emitter.as_str()
                ),
            })
        };
        let row_count = batch.batch.batch().num_rows();
        let evaluated = executed.batch;
        let Ok(column_index) = evaluated.schema().index_of(ORDERING_GROUP_FIELD) else {
            return Err(evaluation_failed(
                "produced no ordering group column".to_string(),
            ));
        };
        let column = evaluated.column(column_index).to_array_ref();
        let Some(values) = column.as_any().downcast_ref::<StringArray>() else {
            return Err(evaluation_failed(format!(
                "produced {}, expected STRING",
                column.data_type()
            )));
        };

        // A program that evaluated every row in order, without an error or a missing value,
        // produced exactly the column the groups are.
        if let nervix_vm::RowSelection::All(selected) = &executed.selected_rows
            && *selected == row_count
            && evaluated.errors().is_error_free()
            && values.null_count() == 0
        {
            return Ok(Self {
                groups: values.clone(),
                failures: BTreeMap::new(),
            });
        }

        let mut output_rows = vec![None; row_count];
        for (output_row, input_row) in executed.selected_rows.iter().enumerate() {
            let Some(slot) = output_rows.get_mut(input_row) else {
                return Err(evaluation_failed(format!(
                    "referenced missing input row {input_row}"
                )));
            };
            *slot = Some(output_row);
        }
        let mut builder = EvaluatedOrderingGroupsBuilder::with_capacity(row_count);
        for output_row in output_rows {
            let Some(output_row) = output_row else {
                builder.push_failure(OrderingGroupError::OmittedInputRow);
                continue;
            };
            if let Some(error) = evaluated.errors().row(output_row).first() {
                builder.push_failure(OrderingGroupError::ExpressionFailed {
                    code: error.code(),
                    span: error.span,
                });
                continue;
            }
            if values.is_null(output_row) {
                builder.push_failure(OrderingGroupError::Null);
                continue;
            }
            builder.push_group(values.value(output_row));
        }
        Ok(builder.finish())
    }

    /// The groups of `source_rows`, in that order.
    fn select(&self, source_rows: &[usize]) -> EmitterRuntimeResult<Self> {
        let row_count = self.groups.len();
        if source_rows.iter().copied().eq(0..row_count) {
            return Ok(self.clone());
        }
        let mut indices = Vec::with_capacity(source_rows.len());
        let mut failures = BTreeMap::new();
        for (output_row, source_row) in source_rows.iter().copied().enumerate() {
            if source_row >= row_count {
                return Err(Report::new(
                    EmitterRuntimeError::OrderingGroupRowOutOfBounds {
                        row: source_row,
                        row_count,
                    },
                ));
            }
            if let Some(failure) = self.failures.get(&source_row) {
                failures.insert(output_row, failure.clone());
            }
            let index: u64 = source_row.arch_into();
            indices.push(index);
        }
        let selected = take_arrow_array(&self.groups, &UInt64Array::from(indices), None)
            .map_err(|error| emitter_report(EmitterRuntimeError::SelectOrderingGroups, error))?;
        let groups = selected
            .as_any()
            .downcast_ref::<StringArray>()
            .assured("taking rows of a string column yields a string column")
            .clone();
        Ok(Self { groups, failures })
    }

    fn group(&self, row: usize) -> Option<Result<&str, &OrderingGroupError>> {
        if row >= self.groups.len() {
            return None;
        }
        if self.groups.is_valid(row) {
            return Some(Ok(self.groups.value(row)));
        }
        let failure = self
            .failures
            .get(&row)
            .assured("every null group is written together with the reason it has none");
        Some(Err(failure))
    }

    /// Groups built row by row, for tests that need a particular mix of groups and failures.
    #[cfg(test)]
    pub(super) fn from_rows<'a>(
        rows: impl IntoIterator<Item = Result<&'a str, OrderingGroupError>>,
    ) -> Self {
        let mut builder = EvaluatedOrderingGroupsBuilder::with_capacity(0);
        for row in rows {
            match row {
                Ok(group) => builder.push_group(group),
                Err(failure) => builder.push_failure(failure),
            }
        }
        builder.finish()
    }
}

/// Writes each row's group, or its null and the reason for it, in row order.
struct EvaluatedOrderingGroupsBuilder {
    groups: StringBuilder,
    failures: BTreeMap<usize, OrderingGroupError>,
}

impl EvaluatedOrderingGroupsBuilder {
    fn with_capacity(rows: usize) -> Self {
        Self {
            groups: StringBuilder::with_capacity(rows, 0),
            failures: BTreeMap::new(),
        }
    }

    fn push_group(&mut self, group: &str) {
        self.groups.append_value(group);
    }

    fn push_failure(&mut self, failure: OrderingGroupError) {
        self.failures.insert(self.groups.len(), failure);
        self.groups.append_null();
    }

    fn finish(mut self) -> EvaluatedOrderingGroups {
        EvaluatedOrderingGroups {
            groups: self.groups.finish(),
            failures: self.failures,
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::ParseAsType;

    use super::*;
    use crate::runtime_schema::test_runtime_row;

    fn compile_expression(
        group: &str,
        input_schema: &Arc<CompiledSchema>,
    ) -> CompiledOrderingGroup {
        CompiledOrderingGroup::compile(
            &EmitterOrderingGroup::Expression(expression(group)),
            &domain("default"),
            &named("ordered_notifications"),
            RuntimeVmSchema {
                schema: input_schema.arrow_schema(),
                sensitivity: VmSchemaSensitivity::default(),
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &ResolvedBranching::unbranched(),
                udfs: None,
            },
        )
        .expect("the ordering group expression must compile")
    }

    fn unbranched_batch(
        input_schema: Arc<CompiledSchema>,
        rows: Vec<Vec<(&str, RuntimeValue)>>,
    ) -> RelayRecordBatch {
        let messages = rows
            .into_iter()
            .map(|fields| {
                let (acks, _completion) = AckSet::root();
                let fields = fields
                    .into_iter()
                    .map(|(name, value)| (name.to_string(), value));
                RelayMessage {
                    key: None,
                    record: test_runtime_row(fields),
                    acks,
                }
            })
            .collect::<Vec<_>>();
        RelayRecordBatch::from_messages(input_schema, messages)
            .expect("the source batch must build")
    }

    async fn evaluate(group: &CompiledOrderingGroup, batch: &RelayRecordBatch) -> OrderingGroups {
        group
            .evaluate(
                &named("ordered_notifications"),
                batch,
                Timestamp::from_unix_nanos(1),
                &HashMap::default(),
            )
            .await
            .expect("the ordering group must evaluate over the batch")
    }

    fn rows(groups: &OrderingGroups, row_count: usize) -> Vec<Result<&str, &OrderingGroupError>> {
        (0..row_count)
            .map(|row| {
                groups
                    .group(row)
                    .expect("the groups must describe every row of the batch")
            })
            .collect()
    }

    #[tokio::test]
    async fn an_expression_group_is_evaluated_per_source_row_in_order() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("region", ParseAsType::String),
        ]);
        let group = compile_expression("concat(input.tenant, '-', input.region)", &input_schema);
        let batch = unbranched_batch(
            input_schema,
            [("acme", "us"), ("globex", "eu"), ("acme", "ap")]
                .into_iter()
                .map(|(tenant, region)| {
                    vec![
                        ("tenant", RuntimeValue::String(tenant.to_string())),
                        ("region", RuntimeValue::String(region.to_string())),
                    ]
                })
                .collect(),
        );

        let groups = evaluate(&group, &batch).await;

        assert_eq!(groups.row_count(), Some(3));
        assert_eq!(
            rows(&groups, 3),
            vec![Ok("acme-us"), Ok("globex-eu"), Ok("acme-ap")]
        );
        let group_bytes: u64 = "acme-usglobex-euacme-ap".len().arch_into();
        assert_eq!(groups.estimated_bytes(), group_bytes);
    }

    #[tokio::test]
    async fn an_expression_group_fails_only_the_row_whose_evaluation_failed() {
        let input_schema =
            test_schema(&[("left", ParseAsType::I64), ("divisor", ParseAsType::I64)]);
        let group = compile_expression("(input.left / input.divisor) AS STRING", &input_schema);
        let batch = unbranched_batch(
            input_schema,
            [(7, 2), (7, 0), (9, 3)]
                .into_iter()
                .map(|(left, divisor)| {
                    vec![
                        ("left", RuntimeValue::I64(left)),
                        ("divisor", RuntimeValue::I64(divisor)),
                    ]
                })
                .collect(),
        );

        let groups = evaluate(&group, &batch).await;

        let outcomes = rows(&groups, 3);
        let [divided, failed, last] = outcomes[..] else {
            panic!("every input row must produce an ordering group outcome: {outcomes:?}");
        };
        assert_eq!(divided, Ok("3"));
        assert_eq!(last, Ok("3"));
        let Err(OrderingGroupError::ExpressionFailed { code, .. }) = failed else {
            panic!("dividing by zero must fail only its own row: {failed:?}");
        };
        assert_eq!(*code, nervix_vm::ErrorCode::DivisionByZero);
        assert!(
            failed
                .map_err(|failure| failure.to_string())
                .is_err_and(|message| message
                    .starts_with("ordering group expression failed with division_by_zero at ")),
            "the failure names the code: {failed:?}"
        );
    }

    #[tokio::test]
    async fn a_branch_group_is_the_batch_key_and_an_unbranched_batch_has_none() {
        let input_schema = test_schema(&[("tenant", ParseAsType::String)]);
        let unbranched = unbranched_batch(
            input_schema.clone(),
            vec![vec![("tenant", RuntimeValue::String("acme".to_string()))]],
        );

        let groups = evaluate(&CompiledOrderingGroup::FromBranch, &unbranched).await;

        assert_eq!(groups.row_count(), None);
        assert_eq!(
            groups.group(0),
            Some(Err(&OrderingGroupError::UnbranchedRecord))
        );
        assert_eq!(groups.estimated_bytes(), 0);

        let key = string_branch_key("tenant", "acme");
        let mut branched = unbranched;
        branched.key = key.clone();
        let key = key.expect("a one-field branch key is concrete");

        let groups = evaluate(&CompiledOrderingGroup::FromBranch, &branched).await;

        assert_eq!(groups.group(0), Some(Ok(key.as_str())));
        let selected = groups
            .select(&[0, 0])
            .expect("a branch group selects any rows");
        assert_eq!(selected.group(1), Some(Ok(key.as_str())));
    }

    fn executed(
        field: arrow_schema::Field,
        column: VmTypedArray,
        selected_rows: nervix_vm::RowSelection,
    ) -> ExecutedFilterMap {
        let schema = StdArc::new(arrow_schema::Schema::new(vec![field]));
        ExecutedFilterMap {
            batch: VmTypedBatch::try_new(schema, vec![column])
                .expect("the program output batch must build"),
            selected_rows,
            invocations: Vec::new(),
            acks: Vec::new(),
        }
    }

    #[test]
    fn rows_the_program_gave_no_value_have_no_group() {
        let input_schema = test_schema(&[("tenant", ParseAsType::String)]);
        let batch = unbranched_batch(
            input_schema,
            ["acme", "globex", "initech"]
                .into_iter()
                .map(|tenant| vec![("tenant", RuntimeValue::String(tenant.to_string()))])
                .collect(),
        );
        let emitter = named("ordered_notifications");
        let nullable_group =
            arrow_schema::Field::new(ORDERING_GROUP_FIELD, ArrowDataType::Utf8, true);

        let groups = EvaluatedOrderingGroups::from_executed(
            &emitter,
            &batch,
            executed(
                nullable_group.clone(),
                VmTypedArray::Utf8(StringArray::from(vec![Some("initech"), None])),
                nervix_vm::RowSelection::Selected(vec![2, 0]),
            ),
        )
        .expect("a partial selection must still produce groups");
        let groups = OrderingGroups::Evaluated(groups);
        assert_eq!(
            rows(&groups, 3),
            vec![
                Err(&OrderingGroupError::Null),
                Err(&OrderingGroupError::OmittedInputRow),
                Ok("initech"),
            ]
        );

        for (field, column, selected_rows, reason) in [
            (
                arrow_schema::Field::new("unrelated", ArrowDataType::Utf8, false),
                VmTypedArray::Utf8(StringArray::from(vec!["acme"; 3])),
                nervix_vm::RowSelection::All(3),
                "produced no ordering group column",
            ),
            (
                arrow_schema::Field::new(ORDERING_GROUP_FIELD, ArrowDataType::Int64, false),
                VmTypedArray::Int64(arrow_array::Int64Array::from(vec![1_i64; 3])),
                nervix_vm::RowSelection::All(3),
                "produced Int64, expected STRING",
            ),
            (
                nullable_group.clone(),
                VmTypedArray::Utf8(StringArray::from(vec![Some("acme")])),
                nervix_vm::RowSelection::Selected(vec![3]),
                "referenced missing input row 3",
            ),
        ] {
            let error = EvaluatedOrderingGroups::from_executed(
                &emitter,
                &batch,
                executed(field, column, selected_rows),
            )
            .expect_err("an output the groups cannot be read from fails the batch");
            let error = error.current_context();
            assert_eq!(
                error.reason,
                format!("emitter 'ordered_notifications' ordering group expression {reason}")
            );
            assert_eq!(error.acks.len(), 3);
        }
    }

    #[test]
    fn selecting_rows_keeps_each_row_with_its_own_group_or_reason() {
        let groups = OrderingGroups::Evaluated(EvaluatedOrderingGroups::from_rows([
            Ok("alpha"),
            Err(OrderingGroupError::Null),
            Ok("beta"),
            Err(OrderingGroupError::OmittedInputRow),
        ]));

        let selected = groups
            .select(&[3, 2, 1])
            .expect("rows inside the groups must select");

        assert_eq!(selected.row_count(), Some(3));
        assert_eq!(
            rows(&selected, 3),
            vec![
                Err(&OrderingGroupError::OmittedInputRow),
                Ok("beta"),
                Err(&OrderingGroupError::Null),
            ]
        );
        assert_eq!(selected.group(3), None);

        let unchanged = groups
            .select(&[0, 1, 2, 3])
            .expect("every row in order must select");
        assert_eq!(rows(&unchanged, 4), rows(&groups, 4));

        let error = groups
            .select(&[1, 4])
            .expect_err("a row outside the groups cannot be selected");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::OrderingGroupRowOutOfBounds {
                row: 4,
                row_count: 4,
            }
        );
    }
}
