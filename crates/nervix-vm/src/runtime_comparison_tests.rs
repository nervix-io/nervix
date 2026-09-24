//! Execution tests for membership, ranges, null-safe equality and the scalar extrema.
//!
//! Layer: test harness.
//!
//! - **Owns.** The value, the null and the per-row error of every row `IN`, `BETWEEN`,
//!   `IS [NOT] DISTINCT FROM`, `greatest`, `least` and `clamp` compute from NSPL source, in a route
//!   and in a read-only predicate.
//! - **Depends on.** The NSPL parser, the VM frontend, compiler and runtime entry points.
//! - **Must not know.** How the kernels traverse their Arrow buffers.

use std::sync::Arc as StdArc;

use arrow_array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nervix_models::Timestamp;

use super::{execute_predicate_in_context, execute_program_sync};
use crate::{
    CompileBinding, CompiledProgram, ExecutionContext, PredicateCompileOptions,
    SemanticScopePolicy, SideErrorReason, TypedArray, TypedBatch,
    compile_predicate_with_options_for_bindings, compile_program_for_bindings,
    extremum::ClampBoundsDefect, lower_expression, test_support::parse_program,
};

fn compile(source: &str, input: &StdArc<Schema>, outputs: Vec<Field>) -> CompiledProgram {
    let program = parse_program(source).expect("the program must parse");
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
    .expect("the program must compile")
}

fn column<'a>(batch: &'a TypedBatch, name: &str) -> &'a TypedArray {
    let index = batch
        .schema()
        .index_of(name)
        .expect("the output column must exist");
    batch.column(index)
}

fn flags(batch: &TypedBatch, name: &str) -> Vec<Option<bool>> {
    let TypedArray::Boolean(values) = column(batch, name) else {
        panic!("{name} must be a BOOL column");
    };
    values.iter().collect()
}

fn floats(batch: &TypedBatch, name: &str) -> Vec<Option<f64>> {
    let TypedArray::Float64(values) = column(batch, name) else {
        panic!("{name} must be an F64 column");
    };
    values.iter().collect()
}

fn boolean(name: &str) -> Field {
    Field::new(name, DataType::Boolean, true)
}

#[test]
fn membership_decides_equality_as_the_comparison_operator_does() {
    let input = StdArc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, true),
        Field::new("priority", DataType::Int32, true),
        Field::new("reading", DataType::Float64, true),
        Field::new("stamp", DataType::Utf8, true),
        Field::new("code", DataType::Int64, true),
    ]));
    let catalogue = (1000..1040)
        .map(|code| code.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let compiled = compile(
        &format!(
            "SET open = input.status IN ('open', 'held', 'open'), settled = input.status NOT IN \
             ('open', 'held'), nothing = input.status IN (), anything = input.status NOT IN (), \
             urgent = input.priority IN (1 AS I32, -1 AS I32), zero = input.reading IN (0.0, \
             'nan' AS F64), day = input.stamp AS DATETIME IN ('2026-01-01T00:00:00Z' AS \
             DATETIME), catalogued = input.code IN ({catalogue})"
        ),
        &input,
        vec![
            boolean("open"),
            boolean("settled"),
            boolean("nothing"),
            boolean("anything"),
            boolean("urgent"),
            boolean("zero"),
            boolean("day"),
            boolean("catalogued"),
        ],
    );
    let batch = TypedBatch::try_new(
        input,
        vec![
            TypedArray::Utf8(StringArray::from(vec![
                Some("open"),
                Some("closed"),
                None,
                Some("held"),
            ])),
            TypedArray::Int32(Int32Array::from(vec![Some(1), Some(-1), None, Some(2)])),
            TypedArray::Float64(Float64Array::from(vec![
                Some(-0.0),
                Some(f64::NAN),
                None,
                Some(0.0),
            ])),
            TypedArray::Utf8(StringArray::from(vec![
                Some("2026-01-01T00:00:00Z"),
                Some("2026-01-02T00:00:00Z"),
                None,
                Some("2026-01-01T00:00:00Z"),
            ])),
            TypedArray::Int64(Int64Array::from(vec![
                Some(1003),
                Some(42),
                None,
                Some(1039),
            ])),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        flags(&output, "open"),
        [Some(true), Some(false), None, Some(true)]
    );
    assert_eq!(
        flags(&output, "settled"),
        [Some(false), Some(true), None, Some(false)]
    );
    assert_eq!(flags(&output, "nothing"), [Some(false); 4]);
    assert_eq!(flags(&output, "anything"), [Some(true); 4]);
    assert_eq!(
        flags(&output, "urgent"),
        [Some(true), Some(true), None, Some(false)]
    );
    assert_eq!(
        flags(&output, "zero"),
        [Some(true), Some(false), None, Some(true)]
    );
    assert_eq!(
        flags(&output, "day"),
        [Some(true), Some(false), None, Some(true)]
    );
    assert_eq!(
        flags(&output, "catalogued"),
        [Some(true), Some(false), None, Some(true)]
    );
    assert!(output.errors().is_error_free());
}

#[test]
fn between_is_the_conjunction_of_both_comparisons() {
    let input = StdArc::new(Schema::new(vec![
        Field::new("value", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("text", DataType::Utf8, false),
    ]));
    let compiled = compile(
        "SET inside = input.value BETWEEN input.low AND input.high, outside = input.value NOT \
         BETWEEN input.low AND input.high, lexical = input.text BETWEEN 'b' AND 'd'",
        &input,
        vec![boolean("inside"), boolean("outside"), boolean("lexical")],
    );
    let batch = TypedBatch::try_new(
        input,
        vec![
            TypedArray::Float64(Float64Array::from(vec![
                Some(1.0),
                Some(2.0),
                Some(3.0),
                Some(1.5),
                Some(f64::NAN),
                Some(5.0),
                Some(-0.0),
                None,
            ])),
            TypedArray::Float64(Float64Array::from(vec![
                Some(1.0),
                Some(1.0),
                None,
                None,
                Some(0.0),
                Some(10.0),
                Some(0.0),
                Some(0.0),
            ])),
            TypedArray::Float64(Float64Array::from(vec![
                Some(2.0),
                Some(2.0),
                Some(2.0),
                Some(2.0),
                Some(10.0),
                Some(0.0),
                Some(0.0),
                Some(1.0),
            ])),
            TypedArray::Utf8(StringArray::from(vec![
                "a", "b", "c", "d", "e", "bz", "D", "dd",
            ])),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    // Both bounds are inclusive; a null bound decides nothing unless the other comparison is
    // false; NaN lies in no range; an inverted range holds nothing; and `-0.0` equals `0.0`.
    assert_eq!(
        flags(&output, "inside"),
        [
            Some(true),
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(true),
            None
        ]
    );
    assert_eq!(
        flags(&output, "outside"),
        [
            Some(false),
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true),
            Some(false),
            None
        ]
    );
    assert_eq!(
        flags(&output, "lexical"),
        [
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false)
        ]
    );
}

#[test]
fn a_range_operand_that_fails_reports_its_failure_once() {
    let input = StdArc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let compiled = compile(
        "SET inside = input.value * 2 BETWEEN 0 AND 10",
        &input,
        vec![boolean("inside")],
    );
    let batch = TypedBatch::try_new(
        input,
        vec![TypedArray::Int64(Int64Array::from(vec![3, i64::MAX]))],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(flags(&output, "inside")[0], Some(true));
    assert!(output.errors().row(0).is_empty());
    assert_eq!(output.errors().row(1).len(), 1);
}

#[test]
fn distinctness_is_equality_under_which_nulls_are_equal() {
    let input = StdArc::new(Schema::new(vec![
        Field::new("left", DataType::Float64, true),
        Field::new("right", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let compiled = compile(
        "SET differs = input.left IS DISTINCT FROM input.right, same = input.left IS NOT DISTINCT \
         FROM input.right, named = input.name IS NOT DISTINCT FROM 'eu', itself = input.name IS \
         DISTINCT FROM input.name",
        &input,
        vec![
            Field::new("differs", DataType::Boolean, false),
            Field::new("same", DataType::Boolean, false),
            Field::new("named", DataType::Boolean, false),
            Field::new("itself", DataType::Boolean, false),
        ],
    );
    let batch = TypedBatch::try_new(
        input,
        vec![
            TypedArray::Float64(Float64Array::from(vec![
                Some(1.0),
                Some(1.0),
                None,
                None,
                Some(f64::NAN),
                Some(-0.0),
            ])),
            TypedArray::Float64(Float64Array::from(vec![
                Some(1.0),
                Some(2.0),
                None,
                Some(1.0),
                Some(f64::NAN),
                Some(0.0),
            ])),
            TypedArray::Utf8(StringArray::from(vec![
                Some("eu"),
                None,
                Some("us"),
                None,
                Some("eu"),
                Some("x"),
            ])),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    // NaN equals no value under `=`, NaN included, so it is distinct from itself, while the two
    // zeros are equal and so not distinct.
    assert_eq!(
        flags(&output, "differs"),
        [
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(false)
        ]
    );
    assert_eq!(
        flags(&output, "same"),
        [
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true)
        ]
    );
    assert_eq!(
        flags(&output, "named"),
        [
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(false)
        ]
    );
    assert_eq!(flags(&output, "itself"), [Some(false); 6]);
}

#[test]
fn extrema_skip_nulls_and_clamp_fails_only_the_rows_it_evaluates() {
    let input = StdArc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
        Field::new("low", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("flag", DataType::Boolean, false),
    ]));
    let compiled = compile(
        "SET top = greatest(input.a, input.b), bottom = least(input.a, input.b, 0.0), clamped = \
         clamp(input.a, input.low, input.high), guarded = IF input.flag THEN clamp(input.a, \
         input.low, input.high) ELSE 0.0 END",
        &input,
        vec![
            Field::new("top", DataType::Float64, true),
            Field::new("bottom", DataType::Float64, false),
            Field::new("clamped", DataType::Float64, true),
            Field::new("guarded", DataType::Float64, true),
        ],
    );
    let batch = TypedBatch::try_new(
        input,
        vec![
            TypedArray::Float64(Float64Array::from(vec![
                Some(5.0),
                None,
                Some(20.0),
                Some(3.0),
            ])),
            TypedArray::Float64(Float64Array::from(vec![
                None,
                None,
                Some(f64::NAN),
                Some(4.0),
            ])),
            TypedArray::Float64(Float64Array::from(vec![0.0, 0.0, 10.0, f64::NAN])),
            TypedArray::Float64(Float64Array::from(vec![10.0, 10.0, 0.0, 1.0])),
            TypedArray::Boolean(BooleanArray::from(vec![true, true, false, true])),
        ],
    )
    .expect("the batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    let top = floats(&output, "top");
    assert_eq!(top[..2], [Some(5.0), None]);
    assert!(top[2].is_some_and(f64::is_nan), "NaN is above every value");
    assert_eq!(top[3], Some(4.0));
    assert_eq!(
        floats(&output, "bottom"),
        [Some(0.0), Some(0.0), Some(0.0), Some(0.0)]
    );
    assert_eq!(floats(&output, "clamped"), [Some(5.0), None, None, None]);
    assert_eq!(
        floats(&output, "guarded"),
        [Some(5.0), None, Some(0.0), None]
    );

    let errors = output.errors();
    assert!(errors.row(0).is_empty());
    assert!(errors.row(1).is_empty());
    // The unselected arm reports nothing, so only the unconditional clamp fails row 2.
    let inverted = errors.row(2);
    assert_eq!(inverted.len(), 1);
    assert_eq!(
        inverted[0].reason,
        SideErrorReason::InvalidClampBounds(ClampBoundsDefect::LowerAboveUpper)
    );
    let nan_bound = errors.row(3);
    assert_eq!(nan_bound.len(), 2);
    assert!(nan_bound.iter().all(|error| {
        error.reason == SideErrorReason::InvalidClampBounds(ClampBoundsDefect::NanBound)
    }));
    assert_eq!(nan_bound[0].code().as_str(), "invalid_argument");
    assert_eq!(nan_bound[0].reason.to_string(), "clamp bound is NaN");
}

#[tokio::test(flavor = "current_thread")]
async fn read_only_predicates_select_rows_with_the_new_tests() {
    let input = StdArc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, false),
        Field::new("carrier", DataType::Utf8, true),
        Field::new("preferred", DataType::Utf8, true),
    ]));
    let predicate = nervix_nspl::parse_expression(
        "input.status IN ('open', 'held') AND input.weight BETWEEN 1.0 AND 50.0 AND input.carrier \
         IS DISTINCT FROM input.preferred AND greatest(input.weight, 10.0) < 40.0",
    )
    .expect("the predicate must parse");
    let predicate = lower_expression(&predicate, SemanticScopePolicy::read_only("input"))
        .expect("the predicate must lower");
    let compiled = compile_predicate_with_options_for_bindings(
        &predicate,
        [CompileBinding::readonly("input", input.clone())],
        PredicateCompileOptions::default(),
    )
    .expect("the predicate must compile");
    let batch = TypedBatch::try_new(
        input,
        vec![
            TypedArray::Utf8(StringArray::from(vec![
                "open", "closed", "held", "open", "held",
            ])),
            TypedArray::Float64(Float64Array::from(vec![5.0, 5.0, 60.0, 5.0, 30.0])),
            TypedArray::Utf8(StringArray::from(vec![
                Some("dhl"),
                Some("dhl"),
                Some("dhl"),
                Some("ups"),
                None,
            ])),
            TypedArray::Utf8(StringArray::from(vec![
                Some("ups"),
                Some("ups"),
                Some("ups"),
                Some("ups"),
                Some("ups"),
            ])),
        ],
    )
    .expect("the batch must build");

    let result = execute_predicate_in_context(
        &compiled,
        &batch,
        &ExecutionContext::new(Timestamp::from_unix_nanos(0)),
    )
    .await
    .expect("the predicate must execute");

    assert_eq!(result.selected_rows().iter().collect::<Vec<_>>(), [0, 4]);
    assert!(result.errors().is_error_free());
}
