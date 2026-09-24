//! Tests for casts that report a value they cannot convert and casts that yield a typed null for
//! it.
//!
//! Layer: test harness.
//!
//! - **Owns.** Converting every failure class the public documentation lists under both failure
//!   policies, and executing tolerant conversions beside the input nulls, operand failures,
//!   injected-function failures, required output fields and sensitive inputs they must leave to
//!   their own rules.
//! - **Depends on.** The VM compiler, the runtime's conversion kernel and its entry points.
//! - **Must not know.** How Arrow converts a value.

use std::sync::Arc as StdArc;

use arrow_array::{
    BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray, TimestampNanosecondArray, UInt8Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use nervix_models::Timestamp;

use super::{
    FunctionInjector, InjectedResult, RowSelection, cast_typed_array, execute_program_sync,
};
use crate::{
    CompileBinding, CompileError, CompileOptions, CompiledProgram, ErrorCode, InstructionKind,
    RegisterType, RowErrorMask, RowErrors, RuntimeError, SchemaSensitivity, SideError,
    SideErrorReason, TypedArray, TypedBatch, UdfParameter, UdfSignature, UdfSignatures,
    compile_program_with_options_for_bindings,
    compile_program_with_options_for_bindings_with_sensitivity,
    program::{BinaryOp, CastFailure, FunctionName, Span},
    test_support::parse_program,
};

/// The operation every conversion below is recorded against.
const SPAN: Span = Span { start: 0, end: 1 };

/// What converting one column yields under one failure policy.
struct Converted {
    values: TypedArray,
    errors: RowErrors,
}

fn convert(input: &TypedArray, target: RegisterType, on_failure: CastFailure) -> Converted {
    let mut errors = RowErrors::new(input.len());
    let values = cast_typed_array(input.clone(), target, on_failure, &mut errors, SPAN)
        .expect("every pair of scalar register types has a conversion kernel");
    Converted { values, errors }
}

/// The rows holding at least one error.
fn failed_rows(errors: &RowErrors) -> Vec<usize> {
    let mut rows = Vec::new();
    for (row, row_errors) in errors.iter().enumerate() {
        if !row_errors.is_empty() {
            rows.push(row);
        }
    }
    rows
}

fn datetimes(values: Vec<Option<i64>>) -> TypedArray {
    TypedArray::Datetime(TimestampNanosecondArray::from(values).with_timezone_utc())
}

fn texts(values: Vec<Option<&str>>) -> TypedArray {
    TypedArray::Utf8(StringArray::from(values))
}

/// One column converted to one type: the values both policies yield, and the rows a cast that
/// reports failures fails, which a tolerant cast turns into nulls instead.
struct Conversion {
    input: TypedArray,
    target: RegisterType,
    converted: TypedArray,
    failed_rows: Vec<usize>,
}

impl Conversion {
    fn check(&self) {
        let label = format!("{:?} to {}", self.input.data_type(), self.target);
        let strict = convert(&self.input, self.target, CastFailure::Error);
        let tolerant = convert(&self.input, self.target, CastFailure::Null);

        assert_eq!(strict.values, self.converted, "{label} under AS");
        assert_eq!(tolerant.values, self.converted, "{label} under TRY_CAST");
        assert_eq!(
            failed_rows(&strict.errors),
            self.failed_rows,
            "{label} fails"
        );
        for error in strict.errors.iter().flatten() {
            assert_eq!(
                error.reason,
                SideErrorReason::CastFailed {
                    target: self.target
                },
                "{label}"
            );
            assert_eq!(error.span, SPAN, "{label}");
        }
        assert!(
            tolerant.errors.is_error_free(),
            "{label} under TRY_CAST reports nothing"
        );
    }
}

#[test]
fn both_policies_convert_the_same_values_and_differ_only_on_the_values_that_fail() {
    let conversions = [
        Conversion {
            input: TypedArray::Int64(Int64Array::from(vec![Some(255), Some(256), Some(-1), None])),
            target: RegisterType::UInt8,
            converted: TypedArray::UInt8(UInt8Array::from(vec![Some(255), None, None, None])),
            failed_rows: vec![1, 2],
        },
        Conversion {
            input: TypedArray::UInt64(UInt64Array::from(vec![
                Some(9_223_372_036_854_775_807),
                Some(9_223_372_036_854_775_808),
            ])),
            target: RegisterType::Int64,
            converted: TypedArray::Int64(Int64Array::from(vec![Some(i64::MAX), None])),
            failed_rows: vec![1],
        },
        Conversion {
            input: TypedArray::UInt64(UInt64Array::from(vec![
                Some(9_223_372_036_854_775_807),
                Some(9_223_372_036_854_775_808),
            ])),
            target: RegisterType::Datetime,
            converted: datetimes(vec![Some(i64::MAX), None]),
            failed_rows: vec![1],
        },
        Conversion {
            input: TypedArray::Int64(Int64Array::from(vec![Some(i64::MIN), Some(-1)])),
            target: RegisterType::Datetime,
            converted: datetimes(vec![Some(i64::MIN), Some(-1)]),
            failed_rows: vec![],
        },
        Conversion {
            input: TypedArray::Float64(Float64Array::from(vec![
                Some(2.9),
                Some(-2.9),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(3e9),
            ])),
            target: RegisterType::Int32,
            converted: TypedArray::Int32(Int32Array::from(vec![
                Some(2),
                Some(-2),
                None,
                None,
                None,
            ])),
            failed_rows: vec![2, 3, 4],
        },
        Conversion {
            input: TypedArray::Float64(Float64Array::from(vec![
                Some(1.5),
                Some(f64::NAN),
                Some(f64::NEG_INFINITY),
                Some(1e19),
            ])),
            target: RegisterType::Datetime,
            converted: datetimes(vec![Some(1), None, None, None]),
            failed_rows: vec![1, 2, 3],
        },
        Conversion {
            input: texts(vec![
                Some("+12"),
                Some("-0"),
                Some(" 12"),
                Some("1e3"),
                Some("2.5"),
                Some(""),
                Some("40000"),
            ]),
            target: RegisterType::Int16,
            converted: TypedArray::Int16(Int16Array::from(vec![
                Some(12),
                Some(0),
                None,
                None,
                None,
                None,
                None,
            ])),
            failed_rows: vec![2, 3, 4, 5, 6],
        },
        Conversion {
            input: texts(vec![
                Some("1e3"),
                Some(".5"),
                Some("+1.5"),
                Some("INF"),
                Some("-Infinity"),
                Some("1e400"),
                Some(" 1"),
                Some("1,5"),
                Some(""),
            ]),
            target: RegisterType::Float64,
            converted: TypedArray::Float64(Float64Array::from(vec![
                Some(1000.0),
                Some(0.5),
                Some(1.5),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(f64::INFINITY),
                None,
                None,
                None,
            ])),
            failed_rows: vec![6, 7, 8],
        },
        Conversion {
            input: texts(vec![
                Some("tru"),
                Some("OFF"),
                Some(" yes "),
                Some("o"),
                Some("2"),
                Some(""),
            ]),
            target: RegisterType::Boolean,
            converted: TypedArray::Boolean(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
                None,
                None,
            ])),
            failed_rows: vec![3, 4, 5],
        },
        Conversion {
            input: texts(vec![
                Some("2024-02-29T12:00:00Z"),
                Some("2024-02-29 13:00:00+01:00"),
                Some("1677-09-21T00:12:43.145224192Z"),
                Some("2262-04-12T00:00:00Z"),
                Some("2024-02-29T12:00:00"),
                Some("yesterday"),
            ]),
            target: RegisterType::Datetime,
            converted: datetimes(vec![
                Some(1_709_208_000_000_000_000),
                Some(1_709_208_000_000_000_000),
                Some(i64::MIN),
                None,
                None,
                None,
            ]),
            failed_rows: vec![3, 4, 5],
        },
        Conversion {
            input: datetimes(vec![Some(0), Some(-1), Some(1_700_000_000_000_000_000)]),
            target: RegisterType::Int32,
            converted: TypedArray::Int32(Int32Array::from(vec![Some(0), Some(-1), None])),
            failed_rows: vec![2],
        },
        Conversion {
            input: datetimes(vec![Some(0), Some(-1)]),
            target: RegisterType::UInt64,
            converted: TypedArray::UInt64(UInt64Array::from(vec![Some(0), None])),
            failed_rows: vec![1],
        },
        Conversion {
            input: TypedArray::Boolean(BooleanArray::from(vec![Some(true), Some(false), None])),
            target: RegisterType::Datetime,
            converted: datetimes(vec![None, None, None]),
            failed_rows: vec![0, 1],
        },
        Conversion {
            input: datetimes(vec![Some(0), None]),
            target: RegisterType::Boolean,
            converted: TypedArray::Boolean(BooleanArray::from(vec![None, None])),
            failed_rows: vec![0],
        },
        Conversion {
            input: TypedArray::Float64(Float64Array::from(vec![
                Some(1e300),
                Some(-1e300),
                Some(0.5),
            ])),
            target: RegisterType::Float32,
            converted: TypedArray::Float32(Float32Array::from(vec![
                Some(f32::INFINITY),
                Some(f32::NEG_INFINITY),
                Some(0.5),
            ])),
            failed_rows: vec![],
        },
        Conversion {
            input: TypedArray::Float64(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(f64::NAN),
                Some(2.5),
            ])),
            target: RegisterType::Boolean,
            converted: TypedArray::Boolean(BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(true),
            ])),
            failed_rows: vec![],
        },
        Conversion {
            input: TypedArray::Boolean(BooleanArray::from(vec![Some(true), Some(false)])),
            target: RegisterType::Int8,
            converted: TypedArray::Int8(Int8Array::from(vec![Some(1), Some(0)])),
            failed_rows: vec![],
        },
        Conversion {
            input: TypedArray::Float64(Float64Array::from(vec![Some(1e21), Some(2.5)])),
            target: RegisterType::Utf8,
            converted: texts(vec![Some("1000000000000000000000"), Some("2.5")]),
            failed_rows: vec![],
        },
        Conversion {
            input: datetimes(vec![Some(0), Some(1_700_000_000_123_456_789)]),
            target: RegisterType::Utf8,
            converted: texts(vec![
                Some("1970-01-01T00:00:00+00:00"),
                Some("2023-11-14T22:13:20.123456789+00:00"),
            ]),
            failed_rows: vec![],
        },
    ];
    for conversion in &conversions {
        conversion.check();
    }
}

#[test]
fn text_reads_as_not_a_number_under_both_policies() {
    let input = texts(vec![Some("NaN"), Some("nan"), Some("-nan")]);
    for on_failure in [CastFailure::Error, CastFailure::Null] {
        let converted = convert(&input, RegisterType::Float64, on_failure);
        let TypedArray::Float64(values) = converted.values else {
            panic!("a conversion to F64 yields F64 values");
        };
        assert!(values.iter().all(|value| value.is_some_and(f64::is_nan)));
        assert!(converted.errors.is_error_free());
    }
}

fn schema(fields: Vec<Field>) -> StdArc<Schema> {
    StdArc::new(Schema::new(fields))
}

fn compile_with(
    source: &str,
    input_schema: &StdArc<Schema>,
    outputs: Vec<Field>,
    options: CompileOptions,
) -> Result<CompiledProgram, CompileError> {
    let output_schema = schema(
        input_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .chain(outputs)
            .collect(),
    );
    let parsed = parse_program(source).expect("the conversion program must parse");
    compile_program_with_options_for_bindings(
        &parsed,
        output_schema,
        [CompileBinding::writable("input", input_schema.clone())],
        options,
    )
}

fn compile(source: &str, input_schema: &StdArc<Schema>, outputs: Vec<Field>) -> CompiledProgram {
    compile_with(source, input_schema, outputs, CompileOptions::default())
        .unwrap_or_else(|error| panic!("`{source}` must compile: {error:?}"))
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

/// The operation the first instruction `matches` accepts belongs to.
fn instruction_span(
    compiled: &CompiledProgram,
    matches: impl Fn(&InstructionKind) -> bool,
) -> Span {
    let instruction = compiled
        .instructions
        .iter()
        .find(|instruction| matches(&instruction.kind))
        .expect("the program compiles the instruction");
    instruction.span
}

fn is_cast(kind: &InstructionKind, target: RegisterType, on_failure: CastFailure) -> bool {
    let InstructionKind::Cast {
        target: cast_target,
        on_failure: cast_failure,
        ..
    } = kind
    else {
        return false;
    };
    *cast_target == target && *cast_failure == on_failure
}

#[test]
fn a_tolerant_cast_yields_typed_nulls_where_a_strict_cast_reports_errors() {
    let input_schema = schema(vec![Field::new("text", DataType::Utf8, true)]);
    let compiled = compile(
        "SET tolerant = TRY_CAST(input.text AS I64), strict = input.text AS I64",
        &input_schema,
        vec![
            Field::new("tolerant", DataType::Int64, true),
            Field::new("strict", DataType::Int64, true),
        ],
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![texts(vec![
            Some("42"),
            None,
            Some("4x2"),
            Some("9223372036854775808"),
        ])],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    let converted = TypedArray::Int64(Int64Array::from(vec![Some(42), None, None, None]));
    assert_eq!(output_column(&output, "tolerant"), &converted);
    assert_eq!(output_column(&output, "strict"), &converted);
    assert_eq!(
        failed_rows(output.errors()),
        [2, 3],
        "a null input is not a failure, and only the strict cast reports one"
    );
    let strict_span = instruction_span(&compiled, |kind| {
        is_cast(kind, RegisterType::Int64, CastFailure::Error)
    });
    for error in output.errors().iter().flatten() {
        assert_eq!(error.code(), ErrorCode::CastFailed);
        assert_eq!(error.span, strict_span);
    }
}

#[test]
fn a_tolerant_cast_reports_every_failure_of_its_operand() {
    let input_schema = schema(vec![
        Field::new("divisor", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
    ]);
    let compiled = compile(
        "SET quotient = TRY_CAST(100 / input.divisor AS STRING), reparsed = TRY_CAST(input.text \
         AS I64 AS STRING), narrowed = TRY_CAST(input.text AS I64) AS U8",
        &input_schema,
        vec![
            Field::new("quotient", DataType::Utf8, true),
            Field::new("reparsed", DataType::Utf8, true),
            Field::new("narrowed", DataType::UInt8, true),
        ],
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Int64(Int64Array::from(vec![Some(4), Some(0), Some(4), Some(4)])),
            texts(vec![Some("7"), Some("7"), Some("x"), Some("300")]),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "quotient"),
        &texts(vec![Some("25"), None, Some("25"), Some("25")])
    );
    assert_eq!(
        output_column(&output, "narrowed"),
        &TypedArray::UInt8(UInt8Array::from(vec![Some(7), Some(7), None, None]))
    );
    let reasons = output
        .errors()
        .iter()
        .map(|errors| {
            errors
                .iter()
                .map(|error| error.reason.clone())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        reasons,
        [
            vec![],
            vec![SideErrorReason::DivisionByZero(
                crate::DivisionOperation::Division
            )],
            vec![SideErrorReason::CastFailed {
                target: RegisterType::Int64
            }],
            vec![SideErrorReason::CastFailed {
                target: RegisterType::UInt8
            }],
        ],
        "the division, the inner AS and the outer AS each report their own failure, and the \
         tolerant conversion of text that is not an integer reports none"
    );
    let spans = output
        .errors()
        .iter()
        .flatten()
        .map(|error| error.span)
        .collect::<Vec<_>>();
    let division = instruction_span(&compiled, |kind| {
        matches!(
            kind,
            InstructionKind::Binary {
                op: BinaryOp::Div,
                ..
            }
        )
    });
    let inner_cast = instruction_span(&compiled, |kind| {
        is_cast(kind, RegisterType::Int64, CastFailure::Error)
    });
    let outer_cast = instruction_span(&compiled, |kind| {
        is_cast(kind, RegisterType::UInt8, CastFailure::Error)
    });
    assert_eq!(
        spans,
        [division, inner_cast, outer_cast],
        "each failure keeps the operation it belongs to"
    );
    assert!(
        division != inner_cast && inner_cast != outer_cast,
        "the three assignments are distinct operations"
    );
}

#[test]
fn a_tolerant_cast_of_literals_converts_once_for_every_row() {
    let input_schema = schema(vec![Field::new("id", DataType::Int64, true)]);
    let compiled = compile(
        "SET parsed = TRY_CAST('12' AS I64), unparsed = TRY_CAST('x' AS I64)",
        &input_schema,
        vec![
            Field::new("parsed", DataType::Int64, true),
            Field::new("unparsed", DataType::Int64, true),
        ],
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![TypedArray::Int64(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
        ]))],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "parsed"),
        &TypedArray::Int64(Int64Array::from(vec![Some(12), Some(12), Some(12)]))
    );
    assert_eq!(
        output_column(&output, "unparsed"),
        &TypedArray::Int64(Int64Array::new_null(3))
    );
    assert!(output.errors().is_error_free());
}

/// An injected `udf::checked(value)` that returns its argument, reports a per-row error for a
/// negative one, and fails the whole batch for any value above 1000.
#[derive(Debug)]
struct CheckedInjector;

impl FunctionInjector for CheckedInjector {
    fn inject_with_context(
        &self,
        function: &FunctionName,
        arguments: &[TypedArray],
        rows: &RowSelection,
        span: Span,
        _now: Timestamp,
        _prior_error_rows: RowErrorMask<'_>,
    ) -> Result<InjectedResult, RuntimeError> {
        assert_eq!(*function, FunctionName::Udf("checked".to_string()));
        let [TypedArray::Int64(values)] = arguments else {
            panic!("checked must receive one Int64 argument");
        };
        assert_eq!(values.len(), rows.len());
        let mut side_errors = Vec::new();
        let mut output = Vec::with_capacity(values.len());
        for (row, value) in values.iter().enumerate() {
            let Some(value) = value else {
                output.push(None);
                continue;
            };
            if value > 1000 {
                return Err(RuntimeError::InjectedFunctionFailed {
                    function: function.as_str().to_string(),
                    message: "value above 1000".to_string(),
                });
            }
            if value < 0 {
                output.push(None);
                side_errors.push((
                    row,
                    SideError {
                        reason: SideErrorReason::Injected {
                            code: ErrorCode::InvalidArgument,
                            message: "checked rejects a negative value".to_string(),
                        },
                        span,
                    },
                ));
                continue;
            }
            output.push(Some(value));
        }
        Ok(InjectedResult {
            output: TypedArray::Int64(Int64Array::from(output)),
            side_errors,
        })
    }
}

fn checked_options() -> CompileOptions {
    let mut signatures = UdfSignatures::default();
    signatures.insert(
        "checked",
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
    let injector: Box<dyn FunctionInjector> = Box::new(CheckedInjector);
    CompileOptions {
        udf_signatures: signatures,
        injector: Some(triomphe::Arc::new(injector)),
        ..CompileOptions::default()
    }
}

#[test]
fn a_tolerant_cast_reports_the_failures_of_an_injected_function_in_its_operand() {
    let input_schema = schema(vec![Field::new("value", DataType::Int64, true)]);
    let compiled = compile_with(
        "SET text = TRY_CAST(udf::checked(input.value) AS STRING)",
        &input_schema,
        vec![Field::new("text", DataType::Utf8, true)],
        checked_options(),
    )
    .expect("the tolerant conversion of a UDF result must compile");

    let per_row = TypedBatch::try_new(
        input_schema.clone(),
        vec![TypedArray::Int64(Int64Array::from(vec![Some(7), Some(-1)]))],
    )
    .expect("the batch must build");
    let output = execute_program_sync(&compiled, &per_row).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "text"),
        &texts(vec![Some("7"), None])
    );
    assert_eq!(failed_rows(output.errors()), [1]);
    assert_eq!(output.errors().row(1)[0].code(), ErrorCode::InvalidArgument);

    let whole_batch = TypedBatch::try_new(
        input_schema,
        vec![TypedArray::Int64(Int64Array::from(vec![
            Some(7),
            Some(1001),
        ]))],
    )
    .expect("the batch must build");
    let error = execute_program_sync(&compiled, &whole_batch)
        .expect_err("a failure of the whole batch is not a conversion failure");
    assert!(
        matches!(error, RuntimeError::InjectedFunctionFailed { .. }),
        "{error}"
    );
}

#[test]
fn a_tolerant_cast_is_optional_and_initializes_a_required_field_only_through_a_non_null_expression()
{
    let input_schema = schema(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("number", DataType::Int64, false),
    ]);
    for source in [
        "SET amount = TRY_CAST(input.text AS I64)",
        // A conversion that can never fail is still optional.
        "SET amount = TRY_CAST(input.number AS I64)",
    ] {
        let error = compile_with(
            source,
            &input_schema,
            vec![Field::new("amount", DataType::Int64, false)],
            CompileOptions::default(),
        )
        .expect_err("a tolerant conversion may be null");
        assert_eq!(error.code, "null_for_required_field", "{source}");
    }

    let compiled = compile(
        "SET amount = coalesce(TRY_CAST(input.text AS I64), -1)",
        &input_schema,
        vec![Field::new("amount", DataType::Int64, false)],
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            texts(vec![Some("5"), Some("x")]),
            TypedArray::Int64(Int64Array::from(vec![Some(1), Some(2)])),
        ],
    )
    .expect("the batch must build");
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "amount"),
        &TypedArray::Int64(Int64Array::from(vec![Some(5), Some(-1)]))
    );
}

#[test]
fn a_tolerant_cast_keeps_its_exact_target_type_and_the_sensitivity_of_its_operand() {
    let input_schema = schema(vec![Field::new("secret", DataType::Utf8, true)]);
    let output_schema = |data_type: DataType| {
        schema(vec![
            Field::new("secret", DataType::Utf8, true),
            Field::new("amount", data_type, true),
        ])
    };
    let compile_sensitive = |source: &str, data_type: DataType| {
        let parsed = parse_program(source).expect("the conversion program must parse");
        compile_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            output_schema(data_type),
            SchemaSensitivity::from_sensitive_fields(["secret"]),
            [CompileBinding::writable("input", input_schema.clone())
                .with_sensitivity(SchemaSensitivity::from_sensitive_fields(["secret"]))],
            CompileOptions::default(),
        )
    };

    let leak = compile_sensitive(
        "SET amount = TRY_CAST(input.secret AS I64)",
        DataType::Int64,
    )
    .expect_err("a converted sensitive value stays sensitive");
    assert_eq!(leak.code, "sensitive_leak");

    compile_sensitive(
        "SET amount = leak_sensitive(TRY_CAST(input.secret AS I64))",
        DataType::Int64,
    )
    .expect("an explicit leak removes the sensitivity");

    let mismatch = compile_sensitive(
        "SET amount = leak_sensitive(TRY_CAST(input.secret AS I64))",
        DataType::Utf8,
    )
    .expect_err("the result has exactly the target type");
    assert_eq!(mismatch.code, "type_mismatch");
}

#[test]
fn a_tolerant_set_element_is_its_converted_value_or_a_null_the_set_rejects() {
    let input_schema = schema(vec![Field::new("number", DataType::Int64, true)]);
    let compiled = compile(
        "SET known = input.number IN (TRY_CAST('7' AS I64), 9)",
        &input_schema,
        vec![Field::new("known", DataType::Boolean, true)],
    );
    let batch = TypedBatch::try_new(
        input_schema.clone(),
        vec![TypedArray::Int64(Int64Array::from(vec![
            Some(7),
            Some(8),
            Some(9),
        ]))],
    )
    .expect("the batch must build");
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "known"),
        &TypedArray::Boolean(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true)
        ]))
    );

    let error = compile_with(
        "SET known = input.number IN (TRY_CAST('x' AS I64))",
        &input_schema,
        vec![Field::new("known", DataType::Boolean, true)],
        CompileOptions::default(),
    )
    .expect_err("an element that does not convert is a typed null");
    assert_eq!(error.code, "null_set_element");
}
