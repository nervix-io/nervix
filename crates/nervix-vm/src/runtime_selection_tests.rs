//! Tests for conditional arms executed over the rows they select.
//!
//! Layer: test harness.
//!
//! - **Owns.** Compiling conditional programs whose arms call injected functions, pattern
//!   matching, casts and float kernels, executing them over batches that select some, every or no
//!   row, and checking which rows each injected function is invoked for, which rows report errors,
//!   and that the results and errors land on the rows that selected them.
//! - **Depends on.** The VM compiler and runtime entry points.
//! - **Must not know.** How the runtime narrows operands or scatters results.

use std::sync::{Arc as StdArc, Mutex};

use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray,
    builder::{Int64Builder, ListBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema};
use nervix_models::Timestamp;

use super::{
    ExecutionContext, FunctionInjector, InjectedResult, RowSelection,
    execute_program_in_context_sync, execute_program_sync,
};
use crate::{
    CompileBinding, CompileOptions, CompiledProgram, ErrorCode, RowErrorMask, RuntimeError,
    SideError, SideErrorReason, TypedArray, TypedBatch, UdfParameter, UdfSignature, UdfSignatures,
    compile_program_with_options_for_bindings,
    ir::InstructionKind,
    program::{FunctionName, Span},
    regexp::{PatternSource, RegexpCall},
    semantics::BuiltinLowering,
    test_support::parse_program,
};

/// One invocation an arm made into the probe function: the rows it was made for, by identity in
/// the batch, and the values it was handed for them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProbeCall {
    rows: RowSelection,
    values: Vec<Option<i64>>,
}

/// An injected `udf::probe(value)` that adds one to every value, reports a per-row error for a
/// negative value, and records every call it receives.
#[derive(Debug, Default)]
struct ProbeInjector {
    calls: Mutex<Vec<ProbeCall>>,
}

impl ProbeInjector {
    fn calls(&self) -> Vec<ProbeCall> {
        self.calls
            .lock()
            .expect("the probe call log is only locked by the test thread")
            .clone()
    }
}

impl FunctionInjector for ProbeInjector {
    fn inject_with_context(
        &self,
        function: &FunctionName,
        arguments: &[TypedArray],
        rows: &RowSelection,
        span: Span,
        _now: Timestamp,
        prior_error_rows: RowErrorMask<'_>,
    ) -> Result<InjectedResult, RuntimeError> {
        assert_eq!(*function, FunctionName::Udf("probe".to_string()));
        let [TypedArray::Int64(values)] = arguments else {
            panic!("probe must receive one Int64 argument");
        };
        assert_eq!(
            values.len(),
            rows.len(),
            "an argument covers every row of the call"
        );
        assert_eq!(
            prior_error_rows.len(),
            rows.len(),
            "the prior error rows cover every row of the call"
        );
        self.calls
            .lock()
            .expect("the probe call log is only locked by the test thread")
            .push(ProbeCall {
                rows: rows.clone(),
                values: values.iter().collect(),
            });
        let mut output = Int64Builder::with_capacity(rows.len());
        let mut side_errors = Vec::new();
        for (row, value) in values.iter().enumerate() {
            match value {
                Some(value) if value < 0 => {
                    output.append_null();
                    side_errors.push((
                        row,
                        SideError {
                            reason: SideErrorReason::Injected {
                                code: ErrorCode::InvalidArgument,
                                message: "probe rejects a negative value".to_string(),
                            },
                            span,
                        },
                    ));
                }
                Some(value) => output.append_value(value + 1),
                None => output.append_null(),
            }
        }
        Ok(InjectedResult {
            output: TypedArray::Int64(output.finish()),
            side_errors,
        })
    }
}

/// An injected `read_headers(name)` that answers one header value per row, naming the row's
/// identity in the batch, and records the rows each call was made for.
#[derive(Debug, Default)]
struct ListingHeaderInjector {
    calls: Mutex<Vec<RowSelection>>,
}

impl FunctionInjector for ListingHeaderInjector {
    fn inject_with_context(
        &self,
        function: &FunctionName,
        arguments: &[TypedArray],
        rows: &RowSelection,
        _span: Span,
        _now: Timestamp,
        _prior_error_rows: RowErrorMask<'_>,
    ) -> Result<InjectedResult, RuntimeError> {
        assert_eq!(*function, FunctionName::ReadHeaders);
        let [TypedArray::Utf8(names)] = arguments else {
            panic!("read_headers must receive one Utf8 argument");
        };
        assert_eq!(names.len(), rows.len());
        self.calls
            .lock()
            .expect("the header call log is only locked by the test thread")
            .push(rows.clone());
        let field = StdArc::new(Field::new("item", DataType::Utf8, false));
        let mut builder = ListBuilder::new(StringBuilder::new()).with_field(field);
        for (row, name) in rows.iter().zip(names.iter()) {
            let name = name.expect("the header name literal is never null");
            builder.values().append_value(format!("{name}-{row}"));
            builder.append(true);
        }
        Ok(InjectedResult::success(TypedArray::Generic(StdArc::new(
            builder.finish(),
        ))))
    }
}

fn probe_signatures() -> UdfSignatures {
    let mut signatures = UdfSignatures::default();
    signatures.insert(
        "probe",
        UdfSignature {
            arguments: vec![UdfParameter {
                data_type: DataType::Int64,
                optional: true,
            }],
            return_type: DataType::Int64,
            return_optional: true,
            volatile: false,
        },
    );
    signatures
}

fn schema(fields: Vec<Field>) -> StdArc<Schema> {
    StdArc::new(Schema::new(fields))
}

fn compile(
    source: &str,
    input_schema: &StdArc<Schema>,
    outputs: Vec<Field>,
    options: CompileOptions,
) -> CompiledProgram {
    let output_schema = schema(
        input_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .chain(outputs)
            .collect(),
    );
    let parsed = parse_program(source).expect("the conditional program must parse");
    compile_program_with_options_for_bindings(
        &parsed,
        output_schema,
        [CompileBinding::writable("input", input_schema.clone())],
        options,
    )
    .unwrap_or_else(|error| panic!("`{source}` must compile: {error:?}"))
}

fn compile_with_probe(
    source: &str,
    input_schema: &StdArc<Schema>,
    outputs: Vec<Field>,
    probe: &StdArc<ProbeInjector>,
) -> CompiledProgram {
    let injector: Box<dyn FunctionInjector> = Box::new(StdArc::clone(probe));
    compile(
        source,
        input_schema,
        outputs,
        CompileOptions {
            udf_signatures: probe_signatures(),
            injector: Some(triomphe::Arc::new(injector)),
            ..CompileOptions::default()
        },
    )
}

impl FunctionInjector for StdArc<ProbeInjector> {
    fn inject_with_context(
        &self,
        function: &FunctionName,
        arguments: &[TypedArray],
        rows: &RowSelection,
        span: Span,
        now: Timestamp,
        prior_error_rows: RowErrorMask<'_>,
    ) -> Result<InjectedResult, RuntimeError> {
        self.as_ref()
            .inject_with_context(function, arguments, rows, span, now, prior_error_rows)
    }
}

impl FunctionInjector for StdArc<ListingHeaderInjector> {
    fn inject_with_context(
        &self,
        function: &FunctionName,
        arguments: &[TypedArray],
        rows: &RowSelection,
        span: Span,
        now: Timestamp,
        prior_error_rows: RowErrorMask<'_>,
    ) -> Result<InjectedResult, RuntimeError> {
        self.as_ref()
            .inject_with_context(function, arguments, rows, span, now, prior_error_rows)
    }
}

fn output_column<'a>(batch: &'a TypedBatch, name: &str) -> &'a TypedArray {
    let index = batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == name)
        .expect("the output column must exist");
    batch.column(index)
}

fn error_counts(batch: &TypedBatch) -> Vec<usize> {
    batch.errors().iter().map(<[SideError]>::len).collect()
}

fn flag_and_value_schema() -> StdArc<Schema> {
    schema(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("value", DataType::Int64, true),
    ])
}

fn flag_and_value_batch(flags: Vec<Option<bool>>, values: Vec<Option<i64>>) -> TypedBatch {
    TypedBatch::try_new(
        flag_and_value_schema(),
        vec![
            TypedArray::Boolean(BooleanArray::from(flags)),
            TypedArray::Int64(Int64Array::from(values)),
        ],
    )
    .expect("the batch must build")
}

#[test]
fn an_arm_invokes_its_injected_function_only_for_the_rows_it_selects() {
    let probe = StdArc::new(ProbeInjector::default());
    let compiled = compile_with_probe(
        "SET probed = CASE WHEN input.flag THEN udf::probe(input.value) ELSE 0 END",
        &flag_and_value_schema(),
        vec![Field::new("probed", DataType::Int64, true)],
        &probe,
    );
    let batch = flag_and_value_batch(
        vec![Some(true), Some(false), Some(true), None, Some(true)],
        vec![Some(1), Some(-5), Some(-2), Some(3), Some(4)],
    );

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        probe.calls(),
        [ProbeCall {
            rows: RowSelection::Selected(vec![0, 2, 4]),
            values: vec![Some(1), Some(-2), Some(4)],
        }],
        "the function sees the selected rows only, by identity and in batch order"
    );
    assert_eq!(
        output_column(&output, "probed"),
        &TypedArray::Int64(Int64Array::from(vec![
            Some(2),
            Some(0),
            None,
            Some(0),
            Some(5)
        ]))
    );
    assert_eq!(error_counts(&output), [0, 0, 1, 0, 0]);
    assert_eq!(
        output.errors().row(2)[0].code(),
        ErrorCode::InvalidArgument,
        "the failure lands on the row that selected the arm"
    );
}

#[test]
fn an_arm_no_row_selects_never_invokes_its_injected_function() {
    let probe = StdArc::new(ProbeInjector::default());
    let compiled = compile_with_probe(
        "SET probed = CASE WHEN input.flag THEN udf::probe(input.value) ELSE 0 END",
        &flag_and_value_schema(),
        vec![Field::new("probed", DataType::Int64, true)],
        &probe,
    );
    let batch = flag_and_value_batch(
        vec![Some(false), None, Some(false)],
        vec![Some(-1), Some(-2), Some(-3)],
    );

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert!(probe.calls().is_empty());
    assert_eq!(
        output_column(&output, "probed"),
        &TypedArray::Int64(Int64Array::from(vec![Some(0), Some(0), Some(0)]))
    );
    assert!(output.errors().is_error_free());
}

#[test]
fn an_arm_every_row_selects_invokes_its_injected_function_over_the_batch() {
    let probe = StdArc::new(ProbeInjector::default());
    let compiled = compile_with_probe(
        "SET probed = CASE WHEN input.flag THEN udf::probe(input.value) ELSE 0 END",
        &flag_and_value_schema(),
        vec![Field::new("probed", DataType::Int64, true)],
        &probe,
    );
    let batch = flag_and_value_batch(
        vec![Some(true), Some(true), Some(true)],
        vec![Some(1), None, Some(-3)],
    );

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        probe.calls(),
        [ProbeCall {
            rows: RowSelection::All(3),
            values: vec![Some(1), None, Some(-3)],
        }]
    );
    assert_eq!(
        output_column(&output, "probed"),
        &TypedArray::Int64(Int64Array::from(vec![Some(2), None, None]))
    );
    assert_eq!(error_counts(&output), [0, 0, 1]);
}

#[test]
fn nested_arms_select_the_rows_both_conditions_select() {
    let probe = StdArc::new(ProbeInjector::default());
    let input_schema = schema(vec![
        Field::new("outer", DataType::Boolean, true),
        Field::new("inner", DataType::Boolean, true),
        Field::new("value", DataType::Int64, true),
    ]);
    let compiled = compile_with_probe(
        "SET probed = CASE WHEN input.outer THEN CASE WHEN input.inner THEN \
         udf::probe(input.value) ELSE -1 END ELSE 0 END",
        &input_schema,
        vec![Field::new("probed", DataType::Int64, true)],
        &probe,
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Boolean(BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
            ])),
            TypedArray::Boolean(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                Some(true),
            ])),
            TypedArray::Int64(Int64Array::from(vec![
                Some(10),
                Some(-20),
                Some(-30),
                Some(40),
                Some(-50),
            ])),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        probe.calls(),
        [ProbeCall {
            rows: RowSelection::Selected(vec![0, 3]),
            values: vec![Some(10), Some(40)],
        }],
        "the inner arm sees only the rows the outer arm passed to it"
    );
    assert_eq!(
        output_column(&output, "probed"),
        &TypedArray::Int64(Int64Array::from(vec![
            Some(11),
            Some(-1),
            Some(0),
            Some(41),
            Some(0)
        ]))
    );
    assert!(output.errors().is_error_free());
}

#[test]
fn later_arm_conditions_run_only_for_rows_earlier_arms_leave_open() {
    let probe = StdArc::new(ProbeInjector::default());
    let input_schema = schema(vec![
        Field::new("first", DataType::Int64, true),
        Field::new("second", DataType::Int64, true),
    ]);
    let compiled = compile_with_probe(
        "SET chosen = CASE WHEN udf::probe(input.first) > 0 THEN 1 WHEN udf::probe(input.second) \
         > 0 THEN 2 ELSE 3 END",
        &input_schema,
        vec![Field::new("chosen", DataType::Int64, false)],
        &probe,
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Int64(Int64Array::from(vec![Some(0), Some(-3), Some(-1), None])),
            TypedArray::Int64(Int64Array::from(vec![Some(1), Some(2), Some(-4), Some(0)])),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        probe.calls(),
        [
            ProbeCall {
                rows: RowSelection::All(4),
                values: vec![Some(0), Some(-3), Some(-1), None],
            },
            ProbeCall {
                rows: RowSelection::Selected(vec![1, 2, 3]),
                values: vec![Some(2), Some(-4), Some(0)],
            },
        ],
        "the second condition sees only the rows the first arm did not answer"
    );
    // A row whose condition reported an error is null for the assignment, as any failed row is.
    assert_eq!(
        output_column(&output, "chosen"),
        &TypedArray::Int64(Int64Array::from(vec![Some(1), None, None, Some(2)]))
    );
    // The first condition ran for every row and the second for the three open rows, so the row
    // that failed in both carries two errors.
    assert_eq!(
        error_counts(&output),
        [0, 1, 2, 0],
        "a condition reports its failures on the open rows that evaluated it"
    );
}

#[test]
fn a_scalar_condition_selects_every_row_or_none() {
    let probe = StdArc::new(ProbeInjector::default());
    let compiled = compile_with_probe(
        "SET probed = CASE WHEN now() > ('2001-01-01T00:00:00Z' AS DATETIME) THEN \
         udf::probe(input.value) ELSE 0 END",
        &flag_and_value_schema(),
        vec![Field::new("probed", DataType::Int64, true)],
        &probe,
    );
    let batch = flag_and_value_batch(vec![None, None], vec![Some(1), Some(2)]);
    let epoch = ExecutionContext::new(Timestamp::from_unix_nanos(0));
    let later = ExecutionContext::new(Timestamp::from_unix_nanos(1_000_000_000_000_000_000));

    let before =
        execute_program_in_context_sync(&compiled, &batch, &epoch).expect("execution must succeed");
    assert!(
        probe.calls().is_empty(),
        "a false shared condition selects no row"
    );
    assert_eq!(
        output_column(&before.batch, "probed"),
        &TypedArray::Int64(Int64Array::from(vec![Some(0), Some(0)]))
    );

    let after =
        execute_program_in_context_sync(&compiled, &batch, &later).expect("execution must succeed");
    assert_eq!(
        probe.calls(),
        [ProbeCall {
            rows: RowSelection::All(2),
            values: vec![Some(1), Some(2)],
        }],
        "a true shared condition selects every row"
    );
    assert_eq!(
        output_column(&after.batch, "probed"),
        &TypedArray::Int64(Int64Array::from(vec![Some(2), Some(3)]))
    );
}

#[test]
fn an_arm_compiles_argument_patterns_only_for_the_rows_it_selects() {
    let input_schema = schema(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("pattern", DataType::Utf8, true),
    ]);
    let compiled = compile(
        "SET matched = CASE WHEN input.flag THEN regexp_like(input.text, input.pattern) ELSE \
         false END",
        &input_schema,
        vec![Field::new("matched", DataType::Boolean, true)],
        CompileOptions::default(),
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Boolean(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
            ])),
            TypedArray::Utf8(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
            ])),
            TypedArray::Utf8(StringArray::from(vec![
                Some("^a$"),
                Some("^b$"),
                Some("^x$"),
                Some("^d$"),
            ])),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "matched"),
        &TypedArray::Boolean(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(false)
        ]))
    );
    let mut caches = Vec::new();
    for instruction in &compiled.instructions {
        if let InstructionKind::Builtin {
            lowering:
                BuiltinLowering::Regexp(RegexpCall {
                    pattern: PatternSource::Argument(cache),
                    ..
                }),
            ..
        } = &instruction.kind
        {
            caches.push(cache);
        }
    }
    let [cache] = caches.as_slice() else {
        panic!("one call reads its pattern argument");
    };
    assert_eq!(
        cache.statistics().compiled,
        2,
        "only the patterns of the selected rows are compiled"
    );
}

#[test]
fn a_narrowed_cast_reports_only_the_failures_of_selected_rows() {
    let input_schema = schema(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("text", DataType::Utf8, true),
    ]);
    let compiled = compile(
        "SET parsed = CASE WHEN input.flag THEN (input.text AS I64) ELSE 0 END",
        &input_schema,
        vec![Field::new("parsed", DataType::Int64, true)],
        CompileOptions::default(),
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Boolean(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
            ])),
            TypedArray::Utf8(StringArray::from(vec![
                Some("7"),
                Some("x"),
                Some("y"),
                Some("8"),
            ])),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "parsed"),
        &TypedArray::Int64(Int64Array::from(vec![Some(7), Some(0), None, Some(0)]))
    );
    assert_eq!(error_counts(&output), [0, 0, 1, 0]);
    assert_eq!(output.errors().row(2)[0].code(), ErrorCode::CastFailed);
}

#[test]
fn a_narrowed_kernel_reads_a_scalar_operand_over_the_selected_rows() {
    let input_schema = schema(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("base", DataType::Float64, true),
    ]);
    let compiled = compile(
        "SET powered = CASE WHEN input.flag THEN pow(input.base, 2.0) ELSE 0.0 END",
        &input_schema,
        vec![Field::new("powered", DataType::Float64, true)],
        CompileOptions::default(),
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Boolean(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
            ])),
            TypedArray::Float64(Float64Array::from(vec![Some(3.0), Some(4.0), Some(5.0)])),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "powered"),
        &TypedArray::Float64(Float64Array::from(vec![Some(9.0), Some(0.0), Some(25.0)]))
    );
    assert!(output.errors().is_error_free());
}

#[test]
fn a_shared_arm_value_reports_its_failure_on_the_selected_rows_only() {
    let compiled = compile(
        "SET ratio = CASE WHEN input.flag THEN 1 / 0 ELSE input.value END",
        &flag_and_value_schema(),
        vec![Field::new("ratio", DataType::Int64, true)],
        CompileOptions::default(),
    );
    let batch = flag_and_value_batch(
        vec![Some(true), Some(false), None, Some(true)],
        vec![Some(1), Some(2), Some(3), Some(4)],
    );

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "ratio"),
        &TypedArray::Int64(Int64Array::from(vec![None, Some(2), Some(3), None]))
    );
    assert_eq!(error_counts(&output), [1, 0, 0, 1]);
}

#[test]
fn an_arm_scatters_list_results_and_yields_nulls_where_no_row_selects_it() {
    let headers = StdArc::new(ListingHeaderInjector::default());
    let injector: Box<dyn FunctionInjector> = Box::new(StdArc::clone(&headers));
    let input_schema = schema(vec![Field::new("flag", DataType::Boolean, true)]);
    // The list the header read yields is narrowed and scattered as a list; `first` reads it
    // back over the batch.
    let compiled = compile(
        "SET route = CASE WHEN input.flag THEN first(read_headers('route')) END",
        &input_schema,
        vec![Field::new("route", DataType::Utf8, true)],
        CompileOptions {
            allow_header_reads: true,
            injector: Some(triomphe::Arc::new(injector)),
            ..CompileOptions::default()
        },
    );

    let partial = TypedBatch::try_new(
        input_schema.clone(),
        vec![TypedArray::Boolean(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(true),
        ]))],
    )
    .expect("the batch must build");
    let output = execute_program_sync(&compiled, &partial).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "route"),
        &TypedArray::Utf8(StringArray::from(vec![
            None,
            Some("route-1"),
            Some("route-2")
        ])),
        "the header read names each selected row's identity"
    );

    let none = TypedBatch::try_new(
        input_schema,
        vec![TypedArray::Boolean(BooleanArray::from(vec![
            Some(false),
            None,
        ]))],
    )
    .expect("the batch must build");
    let output = execute_program_sync(&compiled, &none).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "route"),
        &TypedArray::Utf8(StringArray::new_null(2))
    );
    assert_eq!(
        *headers
            .calls
            .lock()
            .expect("the header call log is only locked by the test thread"),
        [RowSelection::Selected(vec![1, 2])],
        "the header read ran once, for the two selected rows of the first batch"
    );
}

#[test]
fn arms_that_hold_on_every_row_or_on_none_keep_first_match_order() {
    let input_schema = schema(vec![
        Field::new("first", DataType::Boolean, true),
        Field::new("second", DataType::Boolean, true),
        Field::new("value", DataType::Int64, true),
    ]);
    let compiled = compile(
        "SET chosen = CASE WHEN input.first THEN input.value WHEN input.second THEN 2 ELSE 3 END",
        &input_schema,
        vec![Field::new("chosen", DataType::Int64, true)],
        CompileOptions::default(),
    );
    let run = |first: Vec<Option<bool>>, second: Vec<Option<bool>>| {
        let batch = TypedBatch::try_new(
            input_schema.clone(),
            vec![
                TypedArray::Boolean(BooleanArray::from(first)),
                TypedArray::Boolean(BooleanArray::from(second)),
                TypedArray::Int64(Int64Array::from(vec![Some(10), None, Some(30)])),
            ],
        )
        .expect("the batch must build");
        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        output_column(&output, "chosen").clone()
    };

    assert_eq!(
        run(
            vec![Some(true), Some(true), Some(true)],
            vec![Some(true), Some(false), None]
        ),
        TypedArray::Int64(Int64Array::from(vec![Some(10), None, Some(30)])),
        "a first arm that holds on every row answers every row, nulls included"
    );
    assert_eq!(
        run(
            vec![Some(true), Some(false), None],
            vec![Some(true), Some(true), Some(true)]
        ),
        TypedArray::Int64(Int64Array::from(vec![Some(10), Some(2), Some(2)])),
        "an earlier arm still overrides a later arm that holds on every row"
    );
    assert_eq!(
        run(
            vec![Some(false), None, Some(false)],
            vec![Some(false), Some(true), None]
        ),
        TypedArray::Int64(Int64Array::from(vec![Some(3), Some(2), Some(3)])),
        "an arm that holds on no row leaves the rows to the later arms"
    );
    assert_eq!(
        run(
            vec![Some(false), None, Some(false)],
            vec![None, Some(false), Some(false)]
        ),
        TypedArray::Int64(Int64Array::from(vec![Some(3), Some(3), Some(3)])),
        "when no arm holds on any row, every row takes ELSE"
    );
}

#[test]
fn a_udf_call_repeated_outside_its_arm_is_made_for_every_row() {
    let probe = StdArc::new(ProbeInjector::default());
    let compiled = compile_with_probe(
        "SET probed = CASE WHEN input.flag THEN udf::probe(input.value) ELSE 0 END + \
         udf::probe(input.value)",
        &flag_and_value_schema(),
        vec![Field::new("probed", DataType::Int64, true)],
        &probe,
    );
    let batch = flag_and_value_batch(
        vec![Some(true), Some(false), Some(true)],
        vec![Some(1), Some(5), Some(2)],
    );

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "probed"),
        &TypedArray::Int64(Int64Array::from(vec![Some(4), Some(6), Some(6)])),
        "the call outside the arm answers the rows the arm did not select"
    );
    assert_eq!(
        probe.calls(),
        [
            ProbeCall {
                rows: RowSelection::Selected(vec![0, 2]),
                values: vec![Some(1), Some(2)],
            },
            ProbeCall {
                rows: RowSelection::All(3),
                values: vec![Some(1), Some(5), Some(2)],
            },
        ]
    );
}

#[test]
fn a_header_read_repeated_outside_its_arm_is_made_for_every_row() {
    let headers = StdArc::new(ListingHeaderInjector::default());
    let injector: Box<dyn FunctionInjector> = Box::new(StdArc::clone(&headers));
    let input_schema = schema(vec![Field::new("flag", DataType::Boolean, true)]);
    let compiled = compile(
        "SET route = coalesce(CASE WHEN input.flag THEN first(read_headers('route')) END, \
         first(read_headers('route')))",
        &input_schema,
        vec![Field::new("route", DataType::Utf8, true)],
        CompileOptions {
            allow_header_reads: true,
            injector: Some(triomphe::Arc::new(injector)),
            ..CompileOptions::default()
        },
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![TypedArray::Boolean(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
        ]))],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "route"),
        &TypedArray::Utf8(StringArray::from(vec!["route-0", "route-1", "route-2"])),
        "the read outside the arm answers the row the arm did not select"
    );
    assert_eq!(
        *headers
            .calls
            .lock()
            .expect("the header call log is only locked by the test thread"),
        [RowSelection::Selected(vec![0, 2]), RowSelection::All(3)]
    );
}
