//! Execution tests for `DURATION` values: the literal, comparison, the conditional and
//! type-generic forms, sets, ranges and extrema, the list functions, the operators a duration
//! takes alone and with a `DATETIME`, the conversions to and from whole units, and the
//! conversions and operators a duration does not take.
//!
//! Layer: test harness.
//!
//! - **Owns.** The value and the null of every row a `DURATION` expression computes from Plexus
//!   source, and the statement refusals of what a duration does not take.
//! - **Depends on.** The Plexus parser, the VM frontend, compiler and runtime entry points.
//! - **Must not know.** How the kernels traverse their Arrow buffers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use arrow_array::{
    DurationNanosecondArray, Float64Array, Int32Array, Int64Array, ListArray,
    TimestampNanosecondArray, UInt64Array,
    builder::{DurationNanosecondBuilder, ListBuilder},
    cast::AsArray as _,
    types::DurationNanosecondType,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use nervix_primitives::sync::StdArc;

use super::{DatetimeExecution, execute_datetime, execute_program_sync};
use crate::{
    CompileBinding, CompiledProgram, ErrorCode, RowErrors, TypedArray, TypedBatch,
    compile_program_for_bindings,
    ir::InstructionKind,
    program::{DatetimeFunction, DurationUnit, Span, UnitRounding},
    test_support::parse_program,
};

const MILLISECOND: i64 = 1_000_000;
const SECOND: i64 = 1_000_000_000;
const MINUTE: i64 = 60 * SECOND;
const HOUR: i64 = 60 * MINUTE;
const DAY: i64 = 24 * HOUR;

fn duration() -> DataType {
    DataType::Duration(TimeUnit::Nanosecond)
}

fn durations() -> DataType {
    DataType::List(StdArc::new(Field::new("item", duration(), false)))
}

fn field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

/// Compiles `source` over `input` into `outputs`, or returns the message of the stage that
/// refused it: lowering, which reads the Plexus, or compilation, which types it.
fn compile(
    source: &str,
    input: &StdArc<Schema>,
    outputs: Vec<Field>,
) -> Result<CompiledProgram, String> {
    let program = parse_program(source)?;
    let mut fields = input
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend(outputs);
    compile_program_for_bindings(
        &program,
        StdArc::new(Schema::new(fields)),
        [CompileBinding::writable("input", input.clone())],
    )
    .map_err(|error| format!("{error:#}"))
}

fn column<'a>(batch: &'a TypedBatch, name: &str) -> &'a TypedArray {
    let index = batch
        .schema()
        .index_of(name)
        .expect("the output column must exist");
    batch.column(index)
}

fn lengths(batch: &TypedBatch, name: &str) -> Vec<Option<i64>> {
    let TypedArray::Duration(values) = column(batch, name) else {
        panic!("{name} must be a DURATION column");
    };
    values.iter().collect()
}

fn flags(batch: &TypedBatch, name: &str) -> Vec<Option<bool>> {
    let TypedArray::Boolean(values) = column(batch, name) else {
        panic!("{name} must be a BOOL column");
    };
    values.iter().collect()
}

/// The elements of each row of a list column, or `None` for a null row.
fn listed(batch: &TypedBatch, name: &str) -> Vec<Option<Vec<i64>>> {
    let array = column(batch, name).to_array_ref();
    let rows = match array.data_type() {
        DataType::List(_) => array.as_list::<i32>().iter().collect::<Vec<_>>(),
        DataType::FixedSizeList(_, _) => array.as_fixed_size_list().iter().collect::<Vec<_>>(),
        other => panic!("{name} must be a list column, found {other:?}"),
    };
    rows.into_iter()
        .map(|row| {
            row.map(|values| {
                values
                    .as_primitive::<DurationNanosecondType>()
                    .values()
                    .to_vec()
            })
        })
        .collect()
}

fn wait_batch(input: &StdArc<Schema>) -> TypedBatch {
    TypedBatch::try_new(
        input.clone(),
        vec![TypedArray::Duration(DurationNanosecondArray::from(vec![
            Some(30 * SECOND),
            Some(90 * MINUTE),
            None,
            Some(250 * MILLISECOND),
        ]))],
    )
    .expect("the batch must build")
}

#[test]
fn duration_literals_compare_and_select_like_every_ordered_scalar() {
    let input = StdArc::new(Schema::new(vec![field("wait", duration())]));
    let compiled = compile(
        "SET literal = 1h 30m, fraction = 1.5s, longer = input.wait > 1m, equal = input.wait = \
         30s, unequal = input.wait != 250ms, at_most = input.wait <= 30s, differs = input.wait IS \
         DISTINCT FROM 30s, chosen = CASE WHEN input.wait < 1s THEN 1s ELSE input.wait END, \
         matched = CASE input.wait WHEN 30s THEN 1d ELSE 0s END, fallback = coalesce(input.wait, \
         5s), unless = nullif(input.wait, 30s), within = input.wait BETWEEN 1s AND 1h, listed = \
         input.wait IN (30s, 250ms), outside = input.wait NOT IN (), most = greatest(input.wait, \
         1m), fewest = least(input.wait, 1m), clamped = clamp(input.wait, 1s, 1h), conditional = \
         IF input.wait > 1h THEN 1d ELSE 0s END, unchanged = input.wait AS DURATION",
        &input,
        vec![
            field("literal", duration()),
            field("fraction", duration()),
            field("longer", DataType::Boolean),
            field("equal", DataType::Boolean),
            field("unequal", DataType::Boolean),
            field("at_most", DataType::Boolean),
            field("differs", DataType::Boolean),
            field("chosen", duration()),
            field("matched", duration()),
            field("fallback", duration()),
            field("unless", duration()),
            field("within", DataType::Boolean),
            field("listed", DataType::Boolean),
            field("outside", DataType::Boolean),
            field("most", duration()),
            field("fewest", duration()),
            field("clamped", duration()),
            field("conditional", duration()),
            field("unchanged", duration()),
        ],
    )
    .expect("every form takes a DURATION");
    let output =
        execute_program_sync(&compiled, &wait_batch(&input)).expect("execution must succeed");

    let thirty = Some(30 * SECOND);
    let ninety_minutes = Some(90 * MINUTE);
    let quarter = Some(250 * MILLISECOND);
    assert_eq!(lengths(&output, "literal"), [ninety_minutes; 4]);
    assert_eq!(lengths(&output, "fraction"), [Some(1_500 * MILLISECOND); 4]);
    assert_eq!(
        flags(&output, "longer"),
        [Some(false), Some(true), None, Some(false)]
    );
    assert_eq!(
        flags(&output, "equal"),
        [Some(true), Some(false), None, Some(false)]
    );
    assert_eq!(
        flags(&output, "unequal"),
        [Some(true), Some(true), None, Some(false)]
    );
    assert_eq!(
        flags(&output, "at_most"),
        [Some(true), Some(false), None, Some(true)]
    );
    assert_eq!(
        flags(&output, "differs"),
        [Some(false), Some(true), Some(true), Some(true)]
    );
    assert_eq!(
        lengths(&output, "chosen"),
        [thirty, ninety_minutes, None, Some(SECOND)]
    );
    assert_eq!(
        lengths(&output, "matched"),
        [Some(DAY), Some(0), Some(0), Some(0)]
    );
    assert_eq!(
        lengths(&output, "fallback"),
        [thirty, ninety_minutes, Some(5 * SECOND), quarter]
    );
    assert_eq!(
        lengths(&output, "unless"),
        [None, ninety_minutes, None, quarter]
    );
    assert_eq!(
        flags(&output, "within"),
        [Some(true), Some(false), None, Some(false)]
    );
    assert_eq!(
        flags(&output, "listed"),
        [Some(true), Some(false), None, Some(true)]
    );
    assert_eq!(flags(&output, "outside"), [Some(true); 4]);
    assert_eq!(
        lengths(&output, "most"),
        [Some(MINUTE), ninety_minutes, Some(MINUTE), Some(MINUTE)]
    );
    assert_eq!(
        lengths(&output, "fewest"),
        [thirty, Some(MINUTE), Some(MINUTE), quarter]
    );
    assert_eq!(
        lengths(&output, "clamped"),
        [thirty, Some(HOUR), None, Some(SECOND)]
    );
    assert_eq!(
        lengths(&output, "conditional"),
        [Some(0), Some(DAY), Some(0), Some(0)]
    );
    assert_eq!(
        lengths(&output, "unchanged"),
        [thirty, ninety_minutes, None, quarter]
    );
}

fn list_batch(input: &StdArc<Schema>) -> TypedBatch {
    let mut builder = ListBuilder::new(DurationNanosecondBuilder::new()).with_field(Field::new(
        "item",
        duration(),
        false,
    ));
    builder
        .values()
        .append_slice(&[SECOND, 1_500 * MILLISECOND, 2 * MINUTE]);
    builder.append(true);
    builder.append(true);
    builder.append(false);
    let waits: ListArray = builder.finish();
    TypedBatch::try_new(input.clone(), vec![TypedArray::Generic(StdArc::new(waits))])
        .expect("the batch must build")
}

#[test]
fn duration_lists_take_the_type_generic_list_functions() {
    let input = StdArc::new(Schema::new(vec![field("waits", durations())]));
    let compiled = compile(
        "SET counted = count(input.waits), head = first(input.waits), tail = last(input.waits), \
         second = nth(input.waits, 1), has = contains(input.waits, 1500ms), later = \
         slice(input.waits, 1, 2), joined = concat(input.waits, vec(3s)), lowest = \
         min(input.waits), highest = max(input.waits), longest_at = list_argmax(input.waits), \
         fixed = [1d, 36h]",
        &input,
        vec![
            field("counted", DataType::Int64),
            field("head", duration()),
            field("tail", duration()),
            field("second", duration()),
            field("has", DataType::Boolean),
            field("later", durations()),
            field("joined", durations()),
            field("lowest", duration()),
            field("highest", duration()),
            field("longest_at", DataType::Int64),
            field(
                "fixed",
                DataType::FixedSizeList(StdArc::new(Field::new("item", duration(), false)), 2),
            ),
        ],
    )
    .expect("every list function takes DURATION elements");
    let output =
        execute_program_sync(&compiled, &list_batch(&input)).expect("execution must succeed");

    let TypedArray::Int64(counted) = column(&output, "counted") else {
        panic!("counted must be an I64 column");
    };
    assert_eq!(
        counted.iter().collect::<Vec<_>>(),
        Int64Array::from(vec![Some(3), Some(0), None])
            .iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(lengths(&output, "head"), [Some(SECOND), None, None]);
    assert_eq!(lengths(&output, "tail"), [Some(2 * MINUTE), None, None]);
    assert_eq!(
        lengths(&output, "second"),
        [Some(1_500 * MILLISECOND), None, None]
    );
    assert_eq!(flags(&output, "has"), [Some(true), Some(false), None]);
    assert_eq!(
        listed(&output, "later"),
        [
            Some(vec![1_500 * MILLISECOND, 2 * MINUTE]),
            Some(Vec::new()),
            None
        ]
    );
    assert_eq!(
        listed(&output, "joined"),
        [
            Some(vec![SECOND, 1_500 * MILLISECOND, 2 * MINUTE, 3 * SECOND]),
            Some(vec![3 * SECOND]),
            None
        ]
    );
    assert_eq!(lengths(&output, "lowest"), [Some(SECOND), None, None]);
    assert_eq!(lengths(&output, "highest"), [Some(2 * MINUTE), None, None]);
    let TypedArray::Int64(longest_at) = column(&output, "longest_at") else {
        panic!("longest_at must be an I64 column");
    };
    assert_eq!(longest_at.iter().collect::<Vec<_>>(), [Some(2), None, None]);
    let fixed = Some(vec![DAY, 36 * HOUR]);
    assert_eq!(
        listed(&output, "fixed"),
        [fixed.clone(), fixed.clone(), fixed]
    );
}

#[test]
fn a_duration_converts_to_no_other_type() {
    let input = StdArc::new(Schema::new(vec![
        field("wait", duration()),
        field("waits", durations()),
        field("count", DataType::Int64),
        field("text", DataType::Utf8),
    ]));
    for (source, output) in [
        ("SET out = input.wait AS I64", DataType::Int64),
        ("SET out = TRY_CAST(input.wait AS STRING)", DataType::Utf8),
        ("SET out = input.count AS DURATION", duration()),
        ("SET out = TRY_CAST(input.text AS DURATION)", duration()),
        ("SET out = 30 AS DURATION", duration()),
        (
            "SET out = input.waits AS VEC<I64>",
            DataType::List(StdArc::new(Field::new("item", DataType::Int64, false))),
        ),
    ] {
        let refusal = compile(source, &input, vec![field("out", output)])
            .expect_err("no conversion joins a DURATION and another type");
        assert!(
            refusal.contains(
                "a DURATION converts to no other type, and no other type converts to a DURATION"
            ),
            "{source}: {refusal}"
        );
    }
}

fn datetime() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
}

fn instants(batch: &TypedBatch, name: &str) -> Vec<Option<i64>> {
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

/// The message of every error each row reports, in the order the row reports them.
fn row_messages(batch: &TypedBatch) -> Vec<Vec<String>> {
    let mut rows = Vec::with_capacity(batch.row_count());
    for row in 0..batch.row_count() {
        let mut messages = Vec::new();
        for error in batch.errors().row(row) {
            assert_eq!(error.code(), ErrorCode::Overflow, "{error:?}");
            messages.push(error.reason.to_string());
        }
        rows.push(messages);
    }
    rows
}

fn timed_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        field("wait", duration()),
        field("started", datetime()),
        field("finished", datetime()),
    ]))
}

/// One row of the timed batch, in nanoseconds.
struct Timed {
    wait: Option<i64>,
    started: Option<i64>,
    finished: Option<i64>,
}

fn timed_batch(input: &StdArc<Schema>, rows: &[Timed]) -> TypedBatch {
    let waits = rows.iter().map(|row| row.wait).collect::<Vec<_>>();
    let started = rows.iter().map(|row| row.started).collect::<Vec<_>>();
    let finished = rows.iter().map(|row| row.finished).collect::<Vec<_>>();
    TypedBatch::try_new(
        input.clone(),
        vec![
            TypedArray::Duration(DurationNanosecondArray::from(waits)),
            TypedArray::Datetime(TimestampNanosecondArray::from(started).with_timezone_utc()),
            TypedArray::Datetime(TimestampNanosecondArray::from(finished).with_timezone_utc()),
        ],
    )
    .expect("the batch must build")
}

#[test]
fn durations_add_subtract_and_negate_and_move_and_measure_instants() {
    let input = timed_schema();
    let compiled = compile(
        "SET total = input.wait + 1s, spare = input.wait - 1m, reversed = -input.wait, later = \
         input.started + input.wait, also_later = input.wait + input.started, earlier = \
         input.started - input.wait, took = input.finished - input.started, back = input.started \
         - input.finished, negated_literal = -30s, constant = 10s + 5s, listed = input.wait IN \
         (-30s, 30s)",
        &input,
        vec![
            field("total", duration()),
            field("spare", duration()),
            field("reversed", duration()),
            field("later", datetime()),
            field("also_later", datetime()),
            field("earlier", datetime()),
            field("took", duration()),
            field("back", duration()),
            field("negated_literal", duration()),
            field("constant", duration()),
            field("listed", DataType::Boolean),
        ],
    )
    .expect("every form of duration arithmetic compiles");
    let start = 1_709_208_000 * SECOND;
    let batch = timed_batch(
        &input,
        &[
            Timed {
                wait: Some(30 * SECOND),
                started: Some(start),
                finished: Some(start + 90 * SECOND),
            },
            Timed {
                wait: Some(-30 * SECOND),
                started: Some(start),
                finished: Some(start - MINUTE),
            },
            Timed {
                wait: None,
                started: Some(start),
                finished: None,
            },
        ],
    );
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        lengths(&output, "total"),
        [Some(31 * SECOND), Some(-29 * SECOND), None]
    );
    assert_eq!(
        lengths(&output, "spare"),
        [Some(-30 * SECOND), Some(-90 * SECOND), None]
    );
    assert_eq!(
        lengths(&output, "reversed"),
        [Some(-30 * SECOND), Some(30 * SECOND), None]
    );
    assert_eq!(
        instants(&output, "later"),
        [Some(start + 30 * SECOND), Some(start - 30 * SECOND), None]
    );
    assert_eq!(instants(&output, "also_later"), instants(&output, "later"));
    assert_eq!(
        instants(&output, "earlier"),
        [Some(start - 30 * SECOND), Some(start + 30 * SECOND), None]
    );
    assert_eq!(
        lengths(&output, "took"),
        [Some(90 * SECOND), Some(-MINUTE), None]
    );
    assert_eq!(
        lengths(&output, "back"),
        [Some(-90 * SECOND), Some(MINUTE), None]
    );
    assert_eq!(lengths(&output, "negated_literal"), [Some(-30 * SECOND); 3]);
    assert_eq!(lengths(&output, "constant"), [Some(15 * SECOND); 3]);
    assert_eq!(flags(&output, "listed"), [Some(true), Some(true), None]);
    assert_eq!(row_messages(&output), vec![Vec::<String>::new(); 3]);
}

#[test]
fn a_result_outside_its_type_fails_only_its_own_row() {
    let input = timed_schema();
    let compiled = compile(
        "SET total = input.wait + 1ns, spare = input.wait - 1ns, reversed = -input.wait, later = \
         input.started + input.wait, earlier = input.started - input.wait, took = input.finished \
         - input.started",
        &input,
        vec![
            field("total", duration()),
            field("spare", duration()),
            field("reversed", duration()),
            field("later", datetime()),
            field("earlier", datetime()),
            field("took", duration()),
        ],
    )
    .expect("every form of duration arithmetic compiles");
    let batch = timed_batch(
        &input,
        &[
            // The longest DURATION: one more nanosecond overflows, and so does the instant it moves.
            Timed {
                wait: Some(i64::MAX),
                started: Some(1),
                finished: Some(0),
            },
            // The shortest DURATION: its negation is one nanosecond longer than the longest, and
            // the epoch moved back by it lies past the last instant.
            Timed {
                wait: Some(i64::MIN),
                started: Some(0),
                finished: Some(0),
            },
            // Two instants further apart than a DURATION holds.
            Timed {
                wait: Some(0),
                started: Some(i64::MIN),
                finished: Some(i64::MAX),
            },
        ],
    );
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        row_messages(&output),
        [
            vec![
                "duration addition result does not fit a DURATION".to_string(),
                "datetime addition result is outside the DATETIME range".to_string(),
            ],
            vec![
                "duration subtraction result does not fit a DURATION".to_string(),
                "duration negation result does not fit a DURATION".to_string(),
                "datetime subtraction result is outside the DATETIME range".to_string(),
            ],
            vec!["datetime subtraction result does not fit a DURATION".to_string()],
        ]
    );
    assert_eq!(
        lengths(&output, "total"),
        [None, Some(i64::MIN + 1), Some(1)]
    );
    assert_eq!(
        lengths(&output, "spare"),
        [Some(i64::MAX - 1), None, Some(-1)]
    );
    assert_eq!(
        lengths(&output, "reversed"),
        [Some(-i64::MAX), None, Some(0)]
    );
    assert_eq!(
        instants(&output, "later"),
        [None, Some(i64::MIN), Some(i64::MIN)]
    );
    assert_eq!(
        instants(&output, "earlier"),
        [Some(1 - i64::MAX), None, Some(i64::MIN)]
    );
    assert_eq!(lengths(&output, "took"), [Some(-1), Some(0), None]);
}

#[test]
fn constant_duration_arithmetic_folds_and_one_that_overflows_fails_every_row() {
    let input = StdArc::new(Schema::new(vec![field("wait", duration())]));
    let folded = compile(
        "SET out = 10s + 5s - -1m, whole = duration_ceil('second', 1500ms), length = \
         to_duration('minute', 90)",
        &input,
        vec![
            field("out", duration()),
            field("whole", DataType::Int64),
            field("length", duration()),
        ],
    )
    .expect("constant duration arithmetic compiles");
    let computed = folded
        .instructions
        .iter()
        .filter(|instruction| {
            matches!(
                instruction.kind,
                InstructionKind::Binary { .. }
                    | InstructionKind::Unary { .. }
                    | InstructionKind::Builtin { .. }
            )
        })
        .count();
    assert_eq!(computed, 0, "every constant folds to a literal");
    let output =
        execute_program_sync(&folded, &wait_batch(&input)).expect("execution must succeed");
    assert_eq!(lengths(&output, "out"), [Some(75 * SECOND); 4]);
    assert_eq!(integers(&output, "whole"), [Some(2); 4]);
    assert_eq!(lengths(&output, "length"), [Some(90 * MINUTE); 4]);

    // A constant whose result leaves its type does not fold: it executes, and every row that
    // evaluates it reports the failure.
    let overflowing = compile(
        "SET out = 106751d + 106751d, negated = -to_duration('nanosecond', -9223372036854775807 - \
         1), length = to_duration('day', 106752)",
        &input,
        vec![
            field("out", duration()),
            field("negated", duration()),
            field("length", duration()),
        ],
    )
    .expect("an overflowing constant still compiles");
    let output =
        execute_program_sync(&overflowing, &wait_batch(&input)).expect("execution must succeed");
    let expected = vec![
        "duration addition result does not fit a DURATION".to_string(),
        "duration negation result does not fit a DURATION".to_string(),
        "to_duration result does not fit a DURATION".to_string(),
    ];
    assert_eq!(row_messages(&output), vec![expected; 4]);
    assert_eq!(lengths(&output, "out"), [None; 4]);
    assert_eq!(lengths(&output, "length"), [None; 4]);
}

#[test]
fn a_conditional_arm_reports_duration_failures_only_for_the_rows_it_selects() {
    let input = StdArc::new(Schema::new(vec![field("wait", duration())]));
    let compiled = compile(
        "SET out = CASE WHEN input.wait < 2d THEN input.wait + 106751d ELSE -input.wait END",
        &input,
        vec![field("out", duration())],
    )
    .expect("a conditional over durations compiles");
    let batch = TypedBatch::try_new(
        input.clone(),
        vec![TypedArray::Duration(DurationNanosecondArray::from(vec![
            Some(30 * SECOND),
            Some(DAY),
            // The THEN arm would overflow for this row, which the arm does not select.
            Some(106_751 * DAY),
            None,
        ]))],
    )
    .expect("the batch must build");
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        lengths(&output, "out"),
        [
            Some(106_751 * DAY + 30 * SECOND),
            None,
            Some(-106_751 * DAY),
            None
        ]
    );
    assert_eq!(
        row_messages(&output),
        [
            Vec::new(),
            vec!["duration addition result does not fit a DURATION".to_string()],
            Vec::new(),
            Vec::new(),
        ]
    );
}

#[test]
fn the_duration_conversions_round_as_their_names_say() {
    let input = StdArc::new(Schema::new(vec![
        field("wait", duration()),
        field("small", DataType::Int32),
        field("large", DataType::UInt64),
        field("at", datetime()),
    ]));
    let compiled = compile(
        "SET floor = duration_floor('second', input.wait), ceil = duration_ceil('SECOND', \
         input.wait), round = duration_round('second', input.wait), minutes = \
         duration_round('minute', input.wait), from_small = to_duration('millisecond', \
         input.small), from_large = to_duration('nanosecond', input.large), binned = \
         date_bin(15m, input.at, from_unix('second', 0))",
        &input,
        vec![
            field("floor", DataType::Int64),
            field("ceil", DataType::Int64),
            field("round", DataType::Int64),
            field("minutes", DataType::Int64),
            field("from_small", duration()),
            field("from_large", duration()),
            field("binned", datetime()),
        ],
    )
    .expect("every duration conversion compiles");
    let start = 1_709_208_000 * SECOND;
    let batch = TypedBatch::try_new(
        input.clone(),
        vec![
            TypedArray::Duration(DurationNanosecondArray::from(vec![
                Some(90_400 * MILLISECOND),
                Some(-87_900 * MILLISECOND),
                Some(1_500 * MILLISECOND),
                Some(-1_500 * MILLISECOND),
                None,
            ])),
            TypedArray::Int32(Int32Array::from(vec![
                Some(-1_500),
                Some(i32::MAX),
                Some(0),
                None,
                Some(1),
            ])),
            TypedArray::UInt64(UInt64Array::from(vec![
                Some(0),
                Some(9_223_372_036_854_775_807),
                Some(9_223_372_036_854_775_808),
                None,
                Some(u64::MAX),
            ])),
            TypedArray::Datetime(
                TimestampNanosecondArray::from(vec![
                    Some(start),
                    Some(start + 8 * MINUTE),
                    None,
                    Some(start + 15 * MINUTE),
                    Some(start - 1),
                ])
                .with_timezone_utc(),
            ),
        ],
    )
    .expect("the batch must build");
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        integers(&output, "floor"),
        [Some(90), Some(-88), Some(1), Some(-2), None]
    );
    assert_eq!(
        integers(&output, "ceil"),
        [Some(91), Some(-87), Some(2), Some(-1), None]
    );
    // A value halfway between two whole units rounds away from zero, as `round` does.
    assert_eq!(
        integers(&output, "round"),
        [Some(90), Some(-88), Some(2), Some(-2), None]
    );
    assert_eq!(
        integers(&output, "minutes"),
        [Some(2), Some(-1), Some(0), Some(0), None]
    );
    assert_eq!(
        lengths(&output, "from_small"),
        [
            Some(-1_500 * MILLISECOND),
            Some(i64::from(i32::MAX) * MILLISECOND),
            Some(0),
            None,
            Some(MILLISECOND),
        ]
    );
    assert_eq!(
        lengths(&output, "from_large"),
        [Some(0), Some(i64::MAX), None, None, None]
    );
    // Bins fifteen minutes wide from the epoch: an instant on a boundary starts its own bin, and
    // one a nanosecond before it belongs to the bin before.
    assert_eq!(
        instants(&output, "binned"),
        [
            Some(start),
            Some(start),
            None,
            Some(start + 15 * MINUTE),
            Some(start - 15 * MINUTE),
        ]
    );
    assert_eq!(
        row_messages(&output),
        [
            Vec::<String>::new(),
            Vec::new(),
            vec!["to_duration result does not fit a DURATION".to_string()],
            Vec::new(),
            vec!["to_duration result does not fit a DURATION".to_string()],
        ]
    );
}

#[test]
fn a_duration_takes_only_its_own_operators_and_conversions() {
    let input = StdArc::new(Schema::new(vec![
        field("wait", duration()),
        field("at", datetime()),
        field("count", DataType::Int64),
        field("ratio", DataType::Float64),
        field("unit", DataType::Utf8),
    ]));
    for (source, output, expected) in [
        (
            "SET out = input.wait * 2",
            duration(),
            "operator Mul is not valid for Duration(Nanosecond) and Int64",
        ),
        (
            "SET out = 2m * 3m",
            duration(),
            "operator Mul is not valid for Duration(Nanosecond)",
        ),
        (
            "SET out = input.wait / input.wait",
            duration(),
            "operator Div is not valid for Duration(Nanosecond)",
        ),
        (
            "SET out = input.wait % 1s",
            duration(),
            "operator Rem is not valid for Duration(Nanosecond)",
        ),
        (
            "SET out = input.at + input.at",
            datetime(),
            "operator Add is not valid for Timestamp(Nanosecond",
        ),
        (
            "SET out = input.wait - input.at",
            datetime(),
            "operator Sub is not valid for Duration(Nanosecond) and Timestamp(Nanosecond",
        ),
        (
            "SET out = input.count + input.at",
            datetime(),
            "operator Add is not valid for Int64 and Timestamp(Nanosecond",
        ),
        (
            "SET out = -input.at",
            datetime(),
            "operator Neg is not valid for Timestamp",
        ),
        (
            "SET out = input.count + 1s",
            duration(),
            "operator Add is not valid for Int64 and Duration(Nanosecond)",
        ),
        (
            "SET out = duration_floor('week', input.wait)",
            DataType::Int64,
            "function 'duration_floor' does not accept time unit 'week'; expected one of \
             nanosecond, microsecond, millisecond, second, minute, hour, day",
        ),
        (
            "SET out = duration_ceil(input.unit, input.wait)",
            DataType::Int64,
            "function 'duration_ceil' requires its time unit to be a STRING literal",
        ),
        (
            "SET out = duration_round('second', input.count)",
            DataType::Int64,
            "function 'duration_round' requires Duration input, found Int64",
        ),
        (
            "SET out = duration_floor('second', input.at)",
            DataType::Int64,
            "function 'duration_floor' requires Duration input, found Datetime",
        ),
        (
            "SET out = duration_ceil('second', input.ratio)",
            DataType::Int64,
            "function 'duration_ceil' requires Duration input, found Float64",
        ),
        (
            "SET out = to_duration('second', input.ratio)",
            duration(),
            "function 'to_duration' requires integer input, found Float64",
        ),
        (
            "SET out = to_duration('month', input.count)",
            duration(),
            "function 'to_duration' does not accept time unit 'month'",
        ),
        (
            "SET out = to_duration('second')",
            duration(),
            "function 'to_duration' expects 2 arguments, found 1",
        ),
        (
            "SET out = date_bin(0s, input.at, input.at)",
            datetime(),
            "function 'date_bin' requires a positive width, found 0s",
        ),
        (
            "SET out = date_bin(-5m, input.at, input.at)",
            datetime(),
            "function 'date_bin' requires a positive width, found -5m",
        ),
        (
            "SET out = date_bin(input.wait, input.at, input.at)",
            datetime(),
            "function 'date_bin' requires its width to be a DURATION literal",
        ),
        (
            "SET out = date_bin(input.at, input.at)",
            datetime(),
            "function 'date_bin' expects 3 or 4 arguments, found 2",
        ),
    ] {
        let refusal = compile(source, &input, vec![field("out", output)])
            .expect_err("the statement is refused");
        assert!(refusal.contains(expected), "{source}: {refusal}");
    }
}
#[test]
fn json_value_reads_no_duration() {
    let input = StdArc::new(Schema::new(vec![field("document", DataType::Utf8)]));
    for (source, output) in [
        (
            "SET out = JSON_VALUE(input.document, '$.wait' AS DURATION)",
            duration(),
        ),
        (
            "SET out = TRY_JSON_VALUE(input.document, '$.waits' AS VEC<DURATION>)",
            durations(),
        ),
    ] {
        let refusal = compile(source, &input, vec![field("out", output)])
            .expect_err("no JSON value is a duration");
        assert!(
            refusal.contains("cannot read DURATION, which no JSON value is"),
            "{source}: {refusal}"
        );
    }
}

#[test]
fn a_duration_literal_names_only_units_of_a_fixed_length() {
    let input = StdArc::new(Schema::new(vec![field("wait", duration())]));
    for (source, expected) in [
        ("SET out = 1y", "duration unit 'y' has no fixed length"),
        ("SET out = 2M", "duration unit 'M' has no fixed length"),
        (
            "SET out = 1w",
            "a DURATION literal takes the units ns, us, ms, s, m, h and d",
        ),
        (
            "SET out = 5min",
            "duration unit 'min' is not written in a literal",
        ),
        ("SET out = 106752d", "does not fit a DURATION"),
        ("SET out = 1.5ns", "invalid duration literal '1.5ns'"),
    ] {
        let refusal = compile(source, &input, vec![field("out", duration())])
            .expect_err("the literal is refused");
        assert!(refusal.contains(expected), "{source}: {refusal}");
    }
    let unit_at_the_end = compile(
        "SET out = 106751d 23h 47m 16s 854ms 775us 807ns",
        &input,
        vec![field("out", duration())],
    )
    .expect("the longest DURATION is a literal");
    let output = execute_program_sync(&unit_at_the_end, &wait_batch(&input))
        .expect("execution must succeed");
    assert_eq!(lengths(&output, "out"), [Some(i64::MAX); 4]);
}

#[test]
fn a_number_no_unit_follows_stays_a_number() {
    let input = StdArc::new(Schema::new(vec![field("wait", duration())]));
    let compiled = compile(
        "SET out = 5 > 3",
        &input,
        vec![field("out", DataType::Boolean)],
    )
    .expect("a number without a unit is an I64 literal");
    let output =
        execute_program_sync(&compiled, &wait_batch(&input)).expect("execution must succeed");
    assert_eq!(flags(&output, "out"), [Some(true); 4]);
}

#[test]
fn a_duration_kernel_refuses_operands_its_signature_does_not_take() {
    let span = Span { start: 0, end: 0 };
    let mut row_errors = RowErrors::new(1);
    let ratios = TypedArray::Float64(Float64Array::from(vec![1.5]));
    let to_seconds = DatetimeFunction::ToDuration(DurationUnit::Second);
    let execution = execute_datetime(
        &to_seconds,
        std::slice::from_ref(&ratios),
        &mut row_errors,
        span,
    );
    assert!(
        matches!(execution, DatetimeExecution::UnsupportedInputs),
        "a float is no count of units"
    );
    let whole_seconds = DatetimeFunction::DurationUnits {
        unit: DurationUnit::Second,
        rounding: UnitRounding::Ceil,
    };
    let execution = execute_datetime(&whole_seconds, &[ratios], &mut row_errors, span);
    assert!(
        matches!(execution, DatetimeExecution::UnsupportedInputs),
        "a float is no duration"
    );
    assert!(row_errors.first().is_none());
}
