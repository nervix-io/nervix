//! Execution tests for `DURATION` values: the literal, comparison, the conditional and
//! type-generic forms, sets, ranges and extrema, the list functions, and the conversions and
//! operators a duration does not take.
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
    DurationNanosecondArray, Int64Array, ListArray,
    builder::{DurationNanosecondBuilder, ListBuilder},
    cast::AsArray as _,
    types::DurationNanosecondType,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use nervix_primitives::sync::StdArc;

use super::execute_program_sync;
use crate::{
    CompileBinding, CompiledProgram, TypedArray, TypedBatch, compile_program_for_bindings,
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

#[test]
fn a_duration_takes_no_arithmetic_and_no_negation() {
    let input = StdArc::new(Schema::new(vec![field("wait", duration())]));
    for (source, expected) in [
        (
            "SET out = input.wait + 1s",
            "operator Add is not valid for Duration(Nanosecond)",
        ),
        (
            "SET out = 2m * 3m",
            "operator Mul is not valid for Duration(Nanosecond)",
        ),
        ("SET out = -input.wait", "Duration(Nanosecond)"),
    ] {
        let refusal = compile(source, &input, vec![field("out", duration())])
            .expect_err("a duration takes no arithmetic yet");
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
