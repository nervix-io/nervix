//! Tests for datetime builtins compiled from NSPL and executed over batches.
//!
//! Layer: test harness.
//!
//! - **Owns.** Compiling every datetime builtin from NSPL source, including calendar units, time
//!   zones and formats, executing it over batches that hold nulls and failing rows, and checking
//!   the typed results, the per-row errors and their codes, the execution-local time an expression
//!   over `now()` observes, which calls are shared, and the operand types checked when a program is
//!   compiled.
//! - **Depends on.** The VM compiler and runtime entry points.
//! - **Must not know.** How a datetime kernel traverses its Arrow buffers.

use std::{str::FromStr as _, sync::Arc as StdArc};

use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use nervix_models::Timestamp;

use super::{ExecutionContext, execute_program_in_context_sync, execute_program_sync};
use crate::{
    CompileBinding, CompiledProgram, DatetimeOperation, ErrorCode, SideError, SideErrorReason,
    TypedArray, TypedBatch, compile_program_for_bindings,
    ir::InstructionKind,
    program::{
        DatePart, DatetimeFunction, DatetimeFunctionName, DatetimeUnit, FixedTimeUnit, Zone,
    },
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

fn strings(batch: &TypedBatch, name: &str) -> Vec<Option<String>> {
    let TypedArray::Utf8(values) = column(batch, name) else {
        panic!("{name} must be a STRING column");
    };
    values
        .iter()
        .map(|value| value.map(str::to_string))
        .collect()
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
                    &instruction.kind,
                    InstructionKind::Builtin {
                        lowering: BuiltinLowering::Datetime(function),
                        ..
                    } if *function == expected
                )
            })
            .count()
    };

    let hour = DatetimeFunction::DatePart {
        part: DatePart::Hour,
        zone: Zone::UTC,
    };
    let shift = DatetimeFunction::DateAdd {
        unit: DatetimeUnit::Fixed(FixedTimeUnit::Hour),
        zone: Zone::UTC,
    };
    assert_eq!(count(hour), 1);
    assert_eq!(count(shift), 2);
}

#[test]
fn calendar_zone_and_format_builtins_execute_over_a_batch_with_nulls_and_failing_rows() {
    let compiled = compile(
        "SET local_hour = date_part('hour', input.occurred_at, 'America/New_York'), month_start = \
         date_trunc('month', input.occurred_at, 'Europe/Berlin'), next_month = date_add('month', \
         input.months, input.occurred_at), months_elapsed = date_diff('month', input.origin, \
         input.occurred_at), label = format_datetime('%FT%T%:z %Z', input.occurred_at, \
         'America/New_York'), parsed = parse_datetime('%F %T', input.text, 'America/New_York')",
        vec![
            Field::new("occurred_at", datetime_type(), true),
            Field::new("origin", datetime_type(), true),
            Field::new("months", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
        ],
        vec![
            Field::new("local_hour", DataType::Int64, true),
            Field::new("month_start", datetime_type(), true),
            Field::new("next_month", datetime_type(), true),
            Field::new("months_elapsed", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("parsed", datetime_type(), true),
        ],
    );
    let batch = TypedBatch::try_new(
        compiled.input_schema.clone(),
        vec![
            TypedArray::Datetime(
                TimestampNanosecondArray::from(vec![
                    Some(nanoseconds("2024-01-31T23:30:00Z")),
                    None,
                    Some(nanoseconds("2262-03-31T12:00:00Z")),
                    Some(nanoseconds("2024-11-03T06:30:00Z")),
                ])
                .with_timezone_utc(),
            ),
            TypedArray::Datetime(
                TimestampNanosecondArray::from(vec![
                    Some(nanoseconds("2023-12-31T23:30:00Z")),
                    Some(nanoseconds("2023-12-31T23:30:00Z")),
                    Some(nanoseconds("2262-01-31T12:00:00Z")),
                    None,
                ])
                .with_timezone_utc(),
            ),
            TypedArray::Int64(Int64Array::from(vec![Some(1), Some(1), Some(1), Some(-1)])),
            TypedArray::Utf8(StringArray::from(vec![
                Some("2024-07-04 12:30:00"),
                Some("2024/07/04 12:30:00"),
                None,
                Some("2024-11-03 01:30:00"),
            ])),
        ],
    )
    .expect("the calendar batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        integers(&output, "local_hour"),
        [Some(18), None, Some(8), Some(1)]
    );
    assert_eq!(
        datetimes(&output, "month_start"),
        [
            Some(nanoseconds("2024-01-31T23:00:00Z")),
            None,
            Some(nanoseconds("2262-02-28T23:00:00Z")),
            Some(nanoseconds("2024-10-31T23:00:00Z")),
        ]
    );
    assert_eq!(
        datetimes(&output, "next_month"),
        [
            Some(nanoseconds("2024-02-29T23:30:00Z")),
            None,
            None,
            Some(nanoseconds("2024-10-03T06:30:00Z")),
        ]
    );
    assert_eq!(
        integers(&output, "months_elapsed"),
        [Some(1), None, Some(2), None]
    );
    assert_eq!(
        strings(&output, "label"),
        [
            Some("2024-01-31T18:30:00-05:00 EST".to_string()),
            None,
            Some("2262-03-31T08:00:00-04:00 EDT".to_string()),
            Some("2024-11-03T01:30:00-05:00 EST".to_string()),
        ]
    );
    assert_eq!(
        datetimes(&output, "parsed"),
        [Some(nanoseconds("2024-07-04T16:30:00Z")), None, None, None]
    );

    assert!(output.errors().row(0).is_empty());
    let described = |row: usize| {
        output
            .errors()
            .row(row)
            .iter()
            .map(|error| (error.code(), error.reason.to_string()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        described(1),
        [(
            ErrorCode::CastFailed,
            "parse_datetime input does not match its format at byte 4: expected '-'".to_string()
        )]
    );
    assert_eq!(
        described(2),
        [(
            ErrorCode::Overflow,
            "date_add result is outside the DATETIME range".to_string()
        )]
    );
    assert_eq!(
        described(3),
        [(
            ErrorCode::InvalidArgument,
            "parse_datetime local time is ambiguous in America/New_York".to_string()
        )]
    );
}

#[test]
fn parse_datetime_failures_report_their_kind_and_their_zone() {
    let compiled = compile(
        "SET strict = parse_datetime('%F %T', input.text, 'America/New_York'), offset = \
         parse_datetime('%F %T %z', input.logged)",
        vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("logged", DataType::Utf8, true),
        ],
        vec![
            Field::new("strict", datetime_type(), true),
            Field::new("offset", datetime_type(), true),
        ],
    );
    let batch = TypedBatch::try_new(
        compiled.input_schema.clone(),
        vec![
            TypedArray::Utf8(StringArray::from(vec![
                "2024-03-10 02:30:00",
                "2023-02-29 00:00:00",
                "2024-07-04 24:00:00",
                "2262-04-12 00:00:00",
            ])),
            TypedArray::Utf8(StringArray::from(vec![
                "2024-03-10 02:30:00 -0500",
                "2024-03-10 02:30:00 +0560",
                "2024-03-10 02:30:00 -0500 trailing",
                "2262-04-11 23:47:17 +0000",
            ])),
        ],
    )
    .expect("the parse batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    let described = |row: usize| {
        output
            .errors()
            .row(row)
            .iter()
            .map(|error| (error.code(), error.reason.to_string()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        described(0),
        [(
            ErrorCode::InvalidArgument,
            "parse_datetime local time does not exist in America/New_York".to_string()
        )]
    );
    assert_eq!(
        described(1),
        [
            (
                ErrorCode::CastFailed,
                "parse_datetime date does not exist".to_string()
            ),
            (
                ErrorCode::CastFailed,
                "parse_datetime UTC offset is out of range".to_string()
            ),
        ]
    );
    assert_eq!(
        described(2),
        [
            (
                ErrorCode::CastFailed,
                "parse_datetime hour is out of range".to_string()
            ),
            (
                ErrorCode::CastFailed,
                "parse_datetime input continues past its format at byte 25".to_string()
            ),
        ]
    );
    assert_eq!(
        described(3),
        [
            (
                ErrorCode::Overflow,
                "parse_datetime result is outside the DATETIME range".to_string()
            ),
            (
                ErrorCode::Overflow,
                "parse_datetime result is outside the DATETIME range".to_string()
            ),
        ]
    );
    assert_eq!(
        datetimes(&output, "offset"),
        [Some(nanoseconds("2024-03-10T07:30:00Z")), None, None, None]
    );
    let Some(SideError {
        reason: SideErrorReason::SkippedLocalTime { zone },
        ..
    }) = output.errors().row(0).first()
    else {
        panic!("a skipped local time names its zone");
    };
    assert_eq!(zone.to_string(), "America/New_York");
}

#[test]
fn calendar_calls_over_now_read_the_supplied_execution_time_in_their_zone() {
    let compiled = compile(
        "SET local_date = format_datetime('%F %Z', now(), 'Asia/Tokyo'), local_month = \
         date_trunc('month', now(), 'Pacific/Auckland'), local_year = date_part('year', now(), \
         'Pacific/Kiritimati')",
        vec![Field::new("id", DataType::Utf8, false)],
        vec![
            Field::new("local_date", DataType::Utf8, false),
            Field::new("local_month", datetime_type(), false),
            Field::new("local_year", DataType::Int64, false),
        ],
    );
    let batch = TypedBatch::try_new(
        compiled.input_schema.clone(),
        vec![TypedArray::Utf8(StringArray::from(vec!["only"]))],
    )
    .expect("the execution-time batch must build");
    let context = ExecutionContext::new(
        Timestamp::from_str("1999-12-31T12:00:00Z").expect("the execution time is RFC 3339"),
    );

    let output = execute_program_in_context_sync(&compiled, &batch, &context)
        .expect("execution must succeed")
        .batch;

    assert_eq!(
        strings(&output, "local_date"),
        [Some("1999-12-31 JST".to_string())]
    );
    assert_eq!(
        datetimes(&output, "local_month"),
        [Some(nanoseconds("1999-12-31T11:00:00Z"))]
    );
    assert_eq!(integers(&output, "local_year"), [Some(2000)]);
}

#[test]
fn calendar_zone_and_format_operand_types_are_checked_when_compiled() {
    let inputs = || {
        vec![
            Field::new("occurred_at", datetime_type(), true),
            Field::new("label", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]
    };
    assert_eq!(
        compile_error(
            "SET out = parse_datetime('%F', input.occurred_at, 'UTC')",
            inputs(),
            vec![Field::new("out", datetime_type(), true)],
        ),
        "function 'parse_datetime' requires Utf8 input, found Datetime"
    );
    assert_eq!(
        compile_error(
            "SET out = format_datetime('%F', input.label)",
            inputs(),
            vec![Field::new("out", DataType::Utf8, true)],
        ),
        "function 'format_datetime' requires Datetime input, found Utf8"
    );
    assert_eq!(
        compile_error(
            "SET out = date_add('month', input.amount, input.occurred_at, 'Europe/Berlin')",
            inputs(),
            vec![Field::new("out", datetime_type(), true)],
        ),
        "function 'date_add' requires integer input, found Float64"
    );
    let mismatched = compile_error(
        "SET out = format_datetime('%F', input.occurred_at)",
        inputs(),
        vec![Field::new("out", datetime_type(), true)],
    );
    assert!(
        mismatched.contains("has expression type Utf8"),
        "a formatted datetime is a STRING: {mismatched}"
    );
}

#[test]
fn formatting_is_shared_and_parsing_is_evaluated_at_each_occurrence() {
    let compiled = compile(
        "SET same_label = format_datetime('%F', input.occurred_at, 'Europe/Berlin') = \
         format_datetime('%F', input.occurred_at, 'Europe/Berlin'), same_instant = \
         parse_datetime('%F', input.label, 'UTC') = parse_datetime('%F', input.label, 'UTC'), \
         other_zone = format_datetime('%F', input.occurred_at, 'Europe/Paris') = \
         format_datetime('%F', input.occurred_at, 'Europe/Berlin')",
        vec![
            Field::new("occurred_at", datetime_type(), true),
            Field::new("label", DataType::Utf8, true),
        ],
        vec![
            Field::new("same_label", DataType::Boolean, true),
            Field::new("same_instant", DataType::Boolean, true),
            Field::new("other_zone", DataType::Boolean, true),
        ],
    );
    let count = |name: DatetimeFunctionName| {
        compiled
            .instructions
            .iter()
            .filter(|instruction| {
                matches!(
                    &instruction.kind,
                    InstructionKind::Builtin {
                        lowering: BuiltinLowering::Datetime(function),
                        ..
                    } if function.name() == name
                )
            })
            .count()
    };
    // The two formattings in Berlin that one assignment compares are shared, the parses are not,
    // and a formatting in another zone is another computation.
    assert_eq!(count(DatetimeFunctionName::FormatDatetime), 3);
    assert_eq!(count(DatetimeFunctionName::ParseDatetime), 2);
}
