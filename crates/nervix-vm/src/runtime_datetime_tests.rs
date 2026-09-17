//! Tests for datetime builtins compiled from NSPL and executed over batches.
//!
//! Layer: test harness.
//!
//! - **Owns.** Compiling every datetime builtin from NSPL source, executing it over batches that
//!   hold nulls and failing rows, and checking the typed results, the per-row errors, the
//!   execution-local time an expression over `now()` observes, and the operand types checked when
//!   a program is compiled.
//! - **Depends on.** The VM compiler and runtime entry points.
//! - **Must not know.** How a datetime kernel traverses its Arrow buffers.

use std::{str::FromStr as _, sync::Arc as StdArc};

use arrow_array::{Int64Array, TimestampNanosecondArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use nervix_models::Timestamp;

use super::{ExecutionContext, execute_program_in_context_sync, execute_program_sync};
use crate::{
    CompileBinding, CompiledProgram, DatetimeOperation, ErrorCode, SideError, SideErrorReason,
    TypedArray, TypedBatch, compile_program_for_bindings,
    ir::InstructionKind,
    program::{DatePart, DatetimeFunction, FixedTimeUnit},
    semantics::BuiltinLowering,
    test_support::parse_program,
};

fn datetime_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
}

fn nanoseconds(instant: &str) -> i64 {
    Timestamp::from_str(instant)
        .expect("test instants are RFC 3339 values inside the DATETIME range")
        .unix_nanos()
}

fn compile(source: &str, inputs: Vec<Field>, outputs: Vec<Field>) -> CompiledProgram {
    let input_schema = StdArc::new(Schema::new(inputs));
    let output_schema = StdArc::new(Schema::new(
        input_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .chain(outputs)
            .collect::<Vec<_>>(),
    ));
    let parsed = parse_program(source).expect("the datetime program must parse");
    compile_program_for_bindings(
        &parsed,
        output_schema,
        [CompileBinding::writable("input", input_schema)],
    )
    .unwrap_or_else(|error| panic!("`{source}` must compile: {error:?}"))
}

fn compile_error(source: &str, inputs: Vec<Field>, outputs: Vec<Field>) -> String {
    let input_schema = StdArc::new(Schema::new(inputs));
    let output_schema = StdArc::new(Schema::new(
        input_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .chain(outputs)
            .collect::<Vec<_>>(),
    ));
    let parsed = parse_program(source).expect("the datetime program must parse");
    compile_program_for_bindings(
        &parsed,
        output_schema,
        [CompileBinding::writable("input", input_schema)],
    )
    .expect_err("the datetime program must be rejected")
    .message
}

fn column<'a>(batch: &'a TypedBatch, name: &str) -> &'a TypedArray {
    let index = batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == name)
        .expect("output column must exist");
    batch.column(index)
}

fn datetimes(batch: &TypedBatch, name: &str) -> Vec<Option<i64>> {
    let TypedArray::Datetime(values) = column(batch, name) else {
        panic!("{name} must be a DATETIME column");
    };
    values.iter().collect()
}

fn integers(batch: &TypedBatch, name: &str) -> Vec<Option<i64>> {
    let TypedArray::Int64(values) = column(batch, name) else {
        panic!("{name} must be an I64 column");
    };
    values.iter().collect()
}

#[test]
fn datetime_builtins_execute_over_a_batch_with_nulls_and_failing_rows() {
    let compiled = compile(
        "SET hour = date_part('hour', input.occurred_at), day_start = date_trunc('day', \
         input.occurred_at), quarter_hour = date_bin('minute', 15, input.occurred_at, \
         input.origin), shifted = date_add('day', input.amount, input.occurred_at), \
         elapsed_milliseconds = date_diff('millisecond', input.origin, input.occurred_at), \
         unix_seconds = to_unix('second', input.occurred_at), converted = from_unix('hour', \
         input.count)",
        vec![
            Field::new("occurred_at", datetime_type(), true),
            Field::new("origin", datetime_type(), true),
            Field::new("amount", DataType::Int64, true),
            Field::new("count", DataType::UInt32, true),
        ],
        vec![
            Field::new("hour", DataType::Int64, true),
            Field::new("day_start", datetime_type(), true),
            Field::new("quarter_hour", datetime_type(), true),
            Field::new("shifted", datetime_type(), true),
            Field::new("elapsed_milliseconds", DataType::Int64, true),
            Field::new("unix_seconds", DataType::Int64, true),
            Field::new("converted", datetime_type(), true),
        ],
    );
    let batch = TypedBatch::try_new(
        compiled.input_schema.clone(),
        vec![
            TypedArray::Datetime(
                TimestampNanosecondArray::from(vec![
                    Some(nanoseconds("2000-02-29T23:52:30.250Z")),
                    None,
                    Some(i64::MAX),
                ])
                .with_timezone_utc(),
            ),
            TypedArray::Datetime(
                TimestampNanosecondArray::from(vec![
                    Some(nanoseconds("2000-02-29T00:05:00Z")),
                    Some(nanoseconds("2000-02-29T00:05:00Z")),
                    Some(i64::MIN),
                ])
                .with_timezone_utc(),
            ),
            TypedArray::Int64(Int64Array::from(vec![Some(1), Some(1), Some(1)])),
            TypedArray::UInt32(UInt32Array::from(vec![Some(24), None, Some(0)])),
        ],
    )
    .expect("the datetime batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(integers(&output, "hour"), [Some(23), None, Some(23)]);
    assert_eq!(
        datetimes(&output, "day_start"),
        [
            Some(nanoseconds("2000-02-29T00:00:00Z")),
            None,
            Some(nanoseconds("2262-04-11T00:00:00Z")),
        ]
    );
    assert_eq!(
        datetimes(&output, "quarter_hour"),
        [
            Some(nanoseconds("2000-02-29T23:50:00Z")),
            None,
            Some(nanoseconds("2262-04-11T23:42:43.145224192Z")),
        ]
    );
    assert_eq!(
        datetimes(&output, "shifted"),
        [Some(nanoseconds("2000-03-01T23:52:30.250Z")), None, None]
    );
    assert_eq!(
        integers(&output, "elapsed_milliseconds"),
        [Some(85_650_250), None, Some(18_446_744_073_709)]
    );
    assert_eq!(
        integers(&output, "unix_seconds"),
        [Some(951_868_350), None, Some(9_223_372_036)]
    );
    assert_eq!(
        datetimes(&output, "converted"),
        [Some(nanoseconds("1970-01-02T00:00:00Z")), None, Some(0)]
    );

    assert!(output.errors().row(0).is_empty());
    assert!(output.errors().row(1).is_empty());
    let [failure] = output.errors().row(2) else {
        panic!(
            "the maximum instant fails only date_add, found {:?}",
            output.errors().row(2)
        );
    };
    assert_eq!(
        failure.reason,
        SideErrorReason::DatetimeOutOfRange(DatetimeOperation::DateAdd)
    );
    assert_eq!(failure.code(), ErrorCode::Overflow);
}

#[test]
fn datetime_failures_report_the_function_whose_result_left_its_range() {
    let compiled = compile(
        "SET truncated = date_trunc('second', input.occurred_at), converted = \
         from_unix('millisecond', input.count), elapsed = date_diff('nanosecond', input.origin, \
         input.occurred_at)",
        vec![
            Field::new("occurred_at", datetime_type(), true),
            Field::new("origin", datetime_type(), true),
            Field::new("count", DataType::Int64, true),
        ],
        vec![
            Field::new("truncated", datetime_type(), true),
            Field::new("converted", datetime_type(), true),
            Field::new("elapsed", DataType::Int64, true),
        ],
    );
    let batch = TypedBatch::try_new(
        compiled.input_schema.clone(),
        vec![
            TypedArray::Datetime(
                TimestampNanosecondArray::from(vec![i64::MIN]).with_timezone_utc(),
            ),
            TypedArray::Datetime(
                TimestampNanosecondArray::from(vec![i64::MAX]).with_timezone_utc(),
            ),
            TypedArray::Int64(Int64Array::from(vec![i64::MAX])),
        ],
    )
    .expect("the boundary batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    let reasons = output
        .errors()
        .row(0)
        .iter()
        .map(|error: &SideError| error.reason.to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        reasons,
        [
            "date_trunc result is outside the DATETIME range",
            "from_unix result is outside the DATETIME range",
            "date_diff result does not fit I64",
        ]
    );
    assert!(
        output
            .errors()
            .row(0)
            .iter()
            .all(|error| error.code() == ErrorCode::Overflow)
    );
    assert_eq!(datetimes(&output, "truncated"), [None]);
    assert_eq!(datetimes(&output, "converted"), [None]);
    assert_eq!(integers(&output, "elapsed"), [None]);
}

#[test]
fn datetime_calls_over_now_read_the_supplied_execution_time() {
    let compiled = compile(
        "SET execution_day = date_trunc('day', now()), execution_year = date_part('year', now()), \
         age_seconds = date_diff('second', input.occurred_at, now())",
        vec![Field::new("occurred_at", datetime_type(), false)],
        vec![
            Field::new("execution_day", datetime_type(), false),
            Field::new("execution_year", DataType::Int64, false),
            Field::new("age_seconds", DataType::Int64, false),
        ],
    );
    let batch = TypedBatch::try_new(
        compiled.input_schema.clone(),
        vec![TypedArray::Datetime(
            TimestampNanosecondArray::from(vec![nanoseconds("2000-02-29T11:59:00Z")])
                .with_timezone_utc(),
        )],
    )
    .expect("the execution-time batch must build");
    let context = ExecutionContext::new(
        Timestamp::from_str("2000-02-29T12:00:00Z").expect("the execution time is RFC 3339"),
    );

    let output = execute_program_in_context_sync(&compiled, &batch, &context)
        .expect("execution must succeed")
        .batch;

    assert_eq!(
        datetimes(&output, "execution_day"),
        [Some(nanoseconds("2000-02-29T00:00:00Z"))]
    );
    assert_eq!(integers(&output, "execution_year"), [Some(2000)]);
    assert_eq!(integers(&output, "age_seconds"), [Some(60)]);
}

#[test]
fn datetime_operand_types_are_checked_when_compiled() {
    let inputs = || {
        vec![
            Field::new("occurred_at", datetime_type(), true),
            Field::new("label", DataType::Utf8, true),
            Field::new("ratio", DataType::Float64, true),
        ]
    };
    assert_eq!(
        compile_error(
            "SET out = date_trunc('day', input.label)",
            inputs(),
            vec![Field::new("out", datetime_type(), true)],
        ),
        "function 'date_trunc' requires Datetime input, found Utf8"
    );
    assert_eq!(
        compile_error(
            "SET out = date_add('second', input.ratio, input.occurred_at)",
            inputs(),
            vec![Field::new("out", datetime_type(), true)],
        ),
        "function 'date_add' requires integer input, found Float64"
    );
    assert_eq!(
        compile_error(
            "SET out = from_unix('second', input.occurred_at)",
            inputs(),
            vec![Field::new("out", datetime_type(), true)],
        ),
        "function 'from_unix' requires integer input, found Datetime"
    );
    let mismatched = compile_error(
        "SET out = date_part('hour', input.occurred_at)",
        inputs(),
        vec![Field::new("out", datetime_type(), true)],
    );
    assert!(
        mismatched.contains("has expression type Int64"),
        "a date part is an I64: {mismatched}"
    );
}

#[test]
fn infallible_datetime_calls_are_shared_and_failing_calls_evaluate_per_occurrence() {
    let compiled = compile(
        "SET same_hour = date_part('hour', input.occurred_at) = date_part('hour', \
         input.occurred_at), same_shift = date_add('hour', input.amount, input.occurred_at) = \
         date_add('hour', input.amount, input.occurred_at)",
        vec![
            Field::new("occurred_at", datetime_type(), true),
            Field::new("amount", DataType::Int64, true),
        ],
        vec![
            Field::new("same_hour", DataType::Boolean, true),
            Field::new("same_shift", DataType::Boolean, true),
        ],
    );
    let count = |expected: DatetimeFunction| {
        compiled
            .instructions
            .iter()
            .filter(|instruction| {
                matches!(
                    instruction.kind,
                    InstructionKind::Builtin {
                        lowering: BuiltinLowering::Datetime(function),
                        ..
                    } if function == expected
                )
            })
            .count()
    };

    assert_eq!(count(DatetimeFunction::DatePart(DatePart::Hour)), 1);
    assert_eq!(count(DatetimeFunction::DateAdd(FixedTimeUnit::Hour)), 2);
}
