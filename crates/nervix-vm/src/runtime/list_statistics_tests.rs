//! Tests of the statistics over one list, through their kernels over Arrow child buffers and
//! through compiled programs.
//!
//! Layer: test harness.
//!
//! - **Owns.** The values, nulls, non-finite failures, orders and identities of the list
//!   statistics over fixed, ragged, empty, null and sliced lists, their typing and refusals when a
//!   program is compiled, and their property against an independent reference.
//! - **Depends on.** The VM compiler, its runtime, and Arrow list arrays.
//! - **Must not know.** Graphs, routes, or how a row's error reaches an error policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use arrow_array::{
    ArrayRef, FixedSizeListArray, Float64Array, Int64Array, ListArray,
    builder::{
        BinaryBuilder, BooleanBuilder, ListBuilder, StringBuilder, TimestampNanosecondBuilder,
    },
    types::{Float32Type, Float64Type, Int64Type},
};
use arrow_schema::{Field, Schema};
use nervix_primitives::sync::StdArc;

use super::*;
use crate::{
    CompileBinding, CompileError, CompiledProgram, ErrorCode, TypedBatch,
    compile_program_for_bindings,
    ir::InstructionKind,
    list_statistics::ListPercentile,
    runtime::{ListExtremum as ValueExtremum, execute_list_extremum, execute_program_sync},
    semantics::BuiltinLowering,
    test_support::parse_program,
};

const SPAN: Span = Span { start: 0, end: 1 };

const VAR_POP: ListStatistic = ListStatistic::Dispersion(ListDispersion::VarPop);
const VAR_SAMP: ListStatistic = ListStatistic::Dispersion(ListDispersion::VarSamp);
const STDDEV_POP: ListStatistic = ListStatistic::Dispersion(ListDispersion::StddevPop);
const STDDEV_SAMP: ListStatistic = ListStatistic::Dispersion(ListDispersion::StddevSamp);
const MEDIAN: ListStatistic = ListStatistic::Quantile(ListQuantile::Median);
const ARGMIN: ListStatistic = ListStatistic::ExtremeIndex(ListExtremum::Least);
const ARGMAX: ListStatistic = ListStatistic::ExtremeIndex(ListExtremum::Greatest);

fn quantile(percentile: f64) -> ListStatistic {
    let percentile = ListPercentile::new(percentile).verified("each test percentile is in range");
    ListStatistic::Quantile(ListQuantile::Percentile(percentile))
}

/// Lists of required `F64` elements, as a `VEC<F64>` field declares them.
fn required_float_lists(rows: &[Option<&[f64]>]) -> ArrayRef {
    let mut lengths = Vec::with_capacity(rows.len());
    let mut validity = Vec::with_capacity(rows.len());
    let mut values = Vec::new();
    for row in rows {
        let elements = row.unwrap_or(&[]);
        lengths.push(elements.len());
        validity.push(row.is_some());
        values.extend_from_slice(elements);
    }
    StdArc::new(
        ListArray::try_new(
            StdArc::new(Field::new("item", DataType::Float64, false)),
            arrow_buffer::OffsetBuffer::from_lengths(lengths),
            StdArc::new(Float64Array::from(values)),
            Some(arrow_buffer::NullBuffer::from(validity)),
        )
        .verified("the offsets cover exactly the generated values"),
    )
}

/// Arrow's cells for lists of `T`: each row's present values, or `None` for a null list.
fn list_cells<T: Copy>(
    rows: impl IntoIterator<Item = Option<impl AsRef<[T]>>>,
) -> Vec<Option<Vec<Option<T>>>> {
    let mut cells = Vec::new();
    for row in rows {
        let Some(values) = row else {
            cells.push(None);
            continue;
        };
        let mut cell = Vec::with_capacity(values.as_ref().len());
        for value in values.as_ref() {
            cell.push(Some(*value));
        }
        cells.push(Some(cell));
    }
    cells
}

fn float_lists(rows: &[Option<&[f64]>]) -> ArrayRef {
    StdArc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
        list_cells(rows.iter().copied()),
    ))
}

fn integer_arrays(rows: &[Option<[i64; 4]>]) -> ArrayRef {
    StdArc::new(FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
        list_cells(rows.iter().copied()),
        4,
    ))
}

/// The statistic of every row's list, with the errors its rows reported.
fn run(statistic: ListStatistic, lists: &ArrayRef) -> (TypedArray, RowErrors) {
    let input = TypedArray::Generic(lists.clone());
    let mut errors = RowErrors::new(lists.len());
    let output = execute_list_statistic(statistic, &input, &mut errors, SPAN)
        .verified("every test list holds elements the statistic reads");
    (output, errors)
}

fn floats(statistic: ListStatistic, lists: &ArrayRef) -> Vec<Option<f64>> {
    let (output, errors) = run(statistic, lists);
    for row in 0..lists.len() {
        assert!(
            errors.row(row).is_empty(),
            "row {row}: {:?}",
            errors.row(row)
        );
    }
    let TypedArray::Float64(output) = output else {
        panic!("a variance, deviation or quantile yields F64");
    };
    output.iter().collect()
}

fn positions(statistic: ListStatistic, lists: &ArrayRef) -> Vec<Option<i64>> {
    let (output, errors) = run(statistic, lists);
    for row in 0..lists.len() {
        assert!(errors.row(row).is_empty(), "an index never fails a row");
    }
    let TypedArray::Int64(output) = output else {
        panic!("an index yields I64");
    };
    output.iter().collect()
}

#[test]
fn dispersions_center_each_list_and_leave_too_short_lists_null() {
    let lists = float_lists(&[
        Some(&[5.0, 2.0, 9.0, 4.0, 4.0, 7.0, 4.0, 13.0]),
        Some(&[7.5]),
        Some(&[]),
        None,
        // A small spread beside a large mean: a difference of sums of squares would lose it.
        Some(&[1e9 + 1.0, 1e9 + 2.0, 1e9 + 3.0]),
    ]);
    assert_eq!(
        floats(VAR_POP, &lists),
        [Some(11.0), Some(0.0), None, None, Some(2.0 / 3.0)]
    );
    assert_eq!(
        floats(VAR_SAMP, &lists),
        [Some(88.0 / 7.0), None, None, None, Some(1.0)]
    );
    assert_eq!(
        floats(STDDEV_POP, &lists),
        [
            Some(11.0_f64.sqrt()),
            Some(0.0),
            None,
            None,
            Some((2.0_f64 / 3.0).sqrt())
        ]
    );
    assert_eq!(
        floats(STDDEV_SAMP, &lists),
        [Some((88.0_f64 / 7.0).sqrt()), None, None, None, Some(1.0)]
    );
}

#[test]
fn integer_elements_read_as_their_nearest_f64() {
    let lists = integer_arrays(&[Some([10, 30, 20, 40]), Some([-3, -3, -3, -3]), None]);
    assert_eq!(
        floats(VAR_SAMP, &lists),
        [Some(500.0 / 3.0), Some(0.0), None]
    );
    assert_eq!(floats(MEDIAN, &lists), [Some(25.0), Some(-3.0), None]);
    assert_eq!(
        floats(quantile(62.5), &lists),
        [Some(28.75), Some(-3.0), None]
    );
    assert_eq!(positions(ARGMAX, &lists), [Some(3), Some(0), None]);

    // The largest integers round to their nearest F64, so equal values keep no spread.
    let unsigned: ArrayRef = StdArc::new(ListArray::from_iter_primitive::<
        arrow_array::types::UInt64Type,
        _,
        _,
    >([Some(vec![Some(u64::MAX), Some(u64::MAX - 1)])]));
    assert_eq!(floats(VAR_POP, &unsigned), [Some(0.0)]);
    assert_eq!(floats(MEDIAN, &unsigned), [Some(u64::MAX.approx_into())]);
}

#[test]
fn a_non_finite_element_or_result_fails_only_its_row() {
    let lists = float_lists(&[
        Some(&[1.0, f64::NAN]),
        Some(&[1.0, 2.0]),
        Some(&[f64::NEG_INFINITY]),
        Some(&[f64::MAX, -f64::MAX]),
    ]);
    for statistic in [VAR_POP, VAR_SAMP, STDDEV_POP, STDDEV_SAMP] {
        let (output, errors) = run(statistic, &lists);
        let TypedArray::Float64(output) = output else {
            panic!("a dispersion yields F64");
        };
        let name = match statistic {
            ListStatistic::Dispersion(dispersion) => dispersion.float_statistic(),
            ListStatistic::Quantile(_) | ListStatistic::ExtremeIndex(_) => {
                panic!("only dispersions are checked here")
            }
        };
        assert!(output.is_null(0) && output.is_null(2) && output.is_null(3));
        assert!(output.is_valid(1));
        for row in [0, 2] {
            assert_eq!(
                errors.row(row)[0].reason,
                SideErrorReason::NonFiniteListElement(name)
            );
            assert_eq!(errors.row(row)[0].code(), ErrorCode::InvalidArgument);
        }
        assert!(errors.row(1).is_empty());
        assert_eq!(
            errors.row(3)[0].reason,
            SideErrorReason::NonFiniteResult(FloatOperation::ListStatistic(name))
        );
    }

    let (output, errors) = run(MEDIAN, &lists);
    let TypedArray::Float64(output) = output else {
        panic!("a median yields F64");
    };
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::NonFiniteListElement(FloatListStatistic::ListMedian)
    );
    assert_eq!(
        errors.row(2)[0].reason.to_string(),
        "list_median input contains a non-finite element"
    );
    assert_eq!(output.value(1), 1.5);
    // The two extremes interpolate to their midpoint without overflowing.
    assert_eq!(output.value(3), 0.0);
    assert!(errors.row(3).is_empty());
}

#[test]
fn quantiles_interpolate_between_whole_ranks_without_reordering_the_column() {
    let lists = float_lists(&[
        Some(&[5.0, 2.0, 9.0, 4.0, 4.0, 7.0, 4.0, 13.0]),
        Some(&[]),
        None,
    ]);
    let before = lists.to_data();
    assert_eq!(floats(MEDIAN, &lists), [Some(4.5), None, None]);
    assert_eq!(floats(quantile(0.0), &lists), [Some(2.0), None, None]);
    assert_eq!(floats(quantile(25.0), &lists), [Some(4.0), None, None]);
    assert_eq!(floats(quantile(62.5), &lists), [Some(5.75), None, None]);
    assert_eq!(floats(quantile(100.0), &lists), [Some(13.0), None, None]);
    assert_eq!(
        lists.to_data(),
        before,
        "selection works on a copy of each list"
    );
}

#[test]
fn quantiles_order_signed_zeros_by_the_total_order() {
    for order in [[0.0, -0.0], [-0.0, 0.0]] {
        let lists = float_lists(&[Some(&order)]);
        let lowest = floats(quantile(0.0), &lists)[0].verified("a two-value list has a quantile");
        let highest =
            floats(quantile(100.0), &lists)[0].verified("a two-value list has a quantile");
        assert_eq!(lowest.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(highest.to_bits(), 0.0_f64.to_bits());
    }
}

#[test]
fn extreme_positions_keep_the_first_of_equal_elements() {
    let lists = float_lists(&[
        Some(&[3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 9.0]),
        Some(&[2.0]),
        Some(&[]),
        None,
    ]);
    assert_eq!(positions(ARGMIN, &lists), [Some(1), Some(0), None, None]);
    assert_eq!(positions(ARGMAX, &lists), [Some(5), Some(0), None, None]);
}

#[test]
fn extreme_positions_order_floats_as_min_and_max_do() {
    let negative_nan = f64::from_bits(f64::NAN.to_bits() | (1 << 63));
    let lists = float_lists(&[
        Some(&[0.0, -0.0]),
        Some(&[1.0, f64::NAN, f64::INFINITY]),
        Some(&[f64::NEG_INFINITY, negative_nan, -1.0]),
    ]);
    assert_eq!(positions(ARGMIN, &lists), [Some(1), Some(0), Some(1)]);
    assert_eq!(positions(ARGMAX, &lists), [Some(0), Some(1), Some(2)]);
    assert_identities(&lists);
}

#[test]
fn extreme_positions_order_bools_strings_and_datetimes() {
    let mut flags = ListBuilder::new(BooleanBuilder::new());
    for row in [vec![true, false, false], vec![false, true, true]] {
        for value in row {
            flags.values().append_value(value);
        }
        flags.append(true);
    }
    let flags: ArrayRef = StdArc::new(flags.finish());
    assert_eq!(positions(ARGMIN, &flags), [Some(1), Some(0)]);
    assert_eq!(positions(ARGMAX, &flags), [Some(0), Some(1)]);

    let mut labels = ListBuilder::new(StringBuilder::new());
    for value in ["beta", "alpha", "é", "zeta", "alpha"] {
        labels.values().append_value(value);
    }
    labels.append(true);
    let labels: ArrayRef = StdArc::new(labels.finish());
    // STRING orders by its bytes, so the two-byte `é` follows every ASCII letter.
    assert_eq!(positions(ARGMIN, &labels), [Some(1)]);
    assert_eq!(positions(ARGMAX, &labels), [Some(2)]);

    let mut stamps = ListBuilder::new(TimestampNanosecondBuilder::new().with_timezone("+00:00"));
    for value in [30, -10, 20, -10] {
        stamps.values().append_value(value);
    }
    stamps.append(true);
    let stamps: ArrayRef = StdArc::new(stamps.finish());
    assert_eq!(positions(ARGMIN, &stamps), [Some(1)]);
    assert_eq!(positions(ARGMAX, &stamps), [Some(0)]);
    for lists in [&flags, &labels, &stamps] {
        assert_identities(lists);
    }
}

#[test]
fn statistics_skip_missing_children_and_read_sliced_lists() {
    let nullable: ArrayRef = StdArc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(1.0), None, Some(3.0)]),
        Some(vec![None]),
    ]));
    assert_eq!(floats(VAR_POP, &nullable), [Some(1.0), None]);
    assert_eq!(floats(MEDIAN, &nullable), [Some(2.0), None]);
    assert_eq!(positions(ARGMAX, &nullable), [Some(2), None]);

    let lists = float_lists(&[
        Some(&[100.0]),
        Some(&[1.0, 2.0, 6.0]),
        None,
        Some(&[4.0, 4.0]),
    ]);
    let sliced = lists.slice(1, 3);
    assert_eq!(floats(MEDIAN, &sliced), [Some(2.0), None, Some(4.0)]);
    assert_eq!(
        floats(VAR_POP, &sliced),
        [Some(14.0 / 3.0), None, Some(0.0)]
    );
    assert_eq!(positions(ARGMIN, &sliced), [Some(0), None, Some(0)]);

    let fixed = integer_arrays(&[Some([9, 9, 9, 9]), Some([4, 1, 3, 2]), None]);
    let sliced = fixed.slice(1, 2);
    assert_eq!(floats(MEDIAN, &sliced), [Some(2.5), None]);
    assert_eq!(positions(ARGMIN, &sliced), [Some(1), None]);
}

#[test]
fn float32_elements_widen_to_f64_and_fail_on_their_own_non_finite_values() {
    let lists: ArrayRef = StdArc::new(ListArray::from_iter_primitive::<Float32Type, _, _>([
        Some(vec![Some(1.5_f32), Some(2.5)]),
        Some(vec![Some(f32::NAN), Some(1.0)]),
        Some(vec![Some(-0.0_f32), Some(0.0)]),
    ]));
    let (variances, errors) = run(VAR_POP, &lists);
    let TypedArray::Float64(variances) = variances else {
        panic!("a variance yields F64");
    };
    assert_eq!(variances.value(0), 0.25);
    assert!(variances.is_null(1));
    assert_eq!(
        errors.row(1)[0].reason,
        SideErrorReason::NonFiniteListElement(FloatListStatistic::ListVarPop)
    );
    assert_eq!(variances.value(2), 0.0);
    assert!(errors.row(0).is_empty() && errors.row(2).is_empty());

    let (medians, errors) = run(MEDIAN, &lists);
    let TypedArray::Float64(medians) = medians else {
        panic!("a median yields F64");
    };
    assert_eq!(medians.value(0), 2.0);
    assert!(medians.is_null(1));
    assert_eq!(errors.row(1)[0].code(), ErrorCode::InvalidArgument);

    // A positive NaN orders above every value and negative zero below positive zero.
    assert_eq!(positions(ARGMIN, &lists), [Some(0), Some(1), Some(0)]);
    assert_eq!(positions(ARGMAX, &lists), [Some(1), Some(0), Some(1)]);
    assert_identities(&lists);
}

#[test]
fn kernels_refuse_elements_their_statements_never_compile_for() {
    let mut labels = ListBuilder::new(StringBuilder::new());
    labels.values().append_value("a");
    labels.append(true);
    let labels = TypedArray::Generic(StdArc::new(labels.finish()));
    for (statistic, operation) in [(VAR_POP, "list_var_pop"), (MEDIAN, "list_median")] {
        let mut errors = RowErrors::new(1);
        let refused = execute_list_statistic(statistic, &labels, &mut errors, SPAN)
            .expect_err("a STRING list has no numeric value to read");
        assert!(
            matches!(
                refused.current_context(),
                RuntimeError::CollectionNonNumericElement { operation: refused, .. }
                    if *refused == operation
            ),
            "{refused:?}"
        );
    }

    let mut digests = ListBuilder::new(BinaryBuilder::new());
    digests.values().append_value(b"a");
    digests.append(true);
    let digests = TypedArray::Generic(StdArc::new(digests.finish()));
    let mut errors = RowErrors::new(1);
    let refused = execute_list_statistic(ARGMIN, &digests, &mut errors, SPAN)
        .expect_err("BYTES values have no order the list functions use");
    assert!(
        matches!(
            refused.current_context(),
            RuntimeError::CollectionUnorderedElement {
                operation: "list_argmin",
                ..
            }
        ),
        "{refused:?}"
    );
}

/// `nth(list, list_argmin(list))` is `min(list)` and the same holds for the greatest element,
/// compared by bits so signed zeros and NaN payloads count.
fn assert_identities(lists: &ArrayRef) {
    let input = TypedArray::Generic(lists.clone());
    for (statistic, extremum) in [(ARGMIN, ValueExtremum::Min), (ARGMAX, ValueExtremum::Max)] {
        let located = positions(statistic, lists);
        let selected = execute_list_extremum(&input, extremum)
            .verified("min and max read every ordered element type");
        let selected = selected.to_array_ref();
        let list = ListColumn::from_typed(&input).verified("the input is a list");
        for (row, position) in located.into_iter().enumerate() {
            let Some(position) = position else {
                assert!(selected.is_null(row));
                continue;
            };
            let start = list.value_range(row).start;
            let index =
                start + usize::try_from(position).verified("a located position is non-negative");
            let located_value = list.values().slice(index, 1).to_data();
            let selected_value = selected.slice(row, 1).to_data();
            assert_eq!(located_value, selected_value, "row {row}");
        }
    }
}

fn values_schema(element: DataType) -> StdArc<Schema> {
    let item = StdArc::new(Field::new("item", element, false));
    StdArc::new(Schema::new(vec![
        Field::new("values", DataType::List(item), true),
        Field::new("index", DataType::Int64, true),
    ]))
}

fn compile(
    expression: &str,
    element: DataType,
    result_type: DataType,
) -> error_stack::Result<CompiledProgram, CompileError> {
    let program =
        parse_program(&format!("SET result = {expression}")).verified("each compiled case lowers");
    let input = values_schema(element);
    let mut fields = input
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("result", result_type, true));
    compile_program_for_bindings(
        &program,
        StdArc::new(Schema::new(fields)),
        [CompileBinding::writable("input", input)],
    )
}

#[test]
fn calls_return_optional_f64_statistics_and_i64_positions() {
    for expression in [
        "list_var_pop(input.values)",
        "list_var_samp(input.values)",
        "LIST_STDDEV_POP(input.values)",
        "list_stddev_samp(input.values)",
        "list_median(input.values)",
        "list_quantile(input.values, 99.5)",
        "list_quantile(input.values, 0)",
    ] {
        let program = compile(expression, DataType::Int16, DataType::Float64);
        assert!(program.is_ok(), "{expression}: {program:?}");
    }
    for element in [
        DataType::Float32,
        DataType::Boolean,
        DataType::Utf8,
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+00:00".into())),
    ] {
        for expression in ["list_argmin(input.values)", "list_argmax(input.values)"] {
            let program = compile(expression, element.clone(), DataType::Int64);
            assert!(
                program.is_ok(),
                "{expression} over {element:?}: {program:?}"
            );
        }
    }
    let wrong = compile(
        "list_var_pop(input.values)",
        DataType::Int64,
        DataType::Int64,
    )
    .expect_err("a statistic is an F64, not its element type");
    assert_eq!(wrong.current_context().code(), "type_mismatch");
}

#[test]
fn calls_refuse_elements_they_cannot_read() {
    let nested =
        DataType::FixedSizeList(StdArc::new(Field::new("item", DataType::Float64, false)), 2);
    for (expression, element, message) in [
        (
            "list_var_pop(input.values)",
            nested,
            "function 'list_var_pop' requires ARRAY or VEC elements of a scalar type",
        ),
        (
            "list_median(input.values)",
            DataType::Utf8,
            "function 'list_median' requires numeric input, found Utf8",
        ),
        (
            "list_stddev_samp(input.values)",
            DataType::Boolean,
            "function 'list_stddev_samp' requires numeric input, found Boolean",
        ),
        (
            "list_argmax(input.values)",
            DataType::Binary,
            "function 'list_argmax' requires ordered scalar elements, found Binary",
        ),
    ] {
        let refused = compile(expression, element, DataType::Float64)
            .expect_err("the element type is refused when the program compiles");
        let refusal = &refused.current_context().message;
        assert!(refusal.starts_with(message), "{expression}: {refusal}");
    }
}

#[test]
fn a_quantile_reads_its_percentile_literal_when_the_call_is_lowered() {
    for (expression, message) in [
        (
            "list_quantile(input.values, 101)",
            "function 'list_quantile' requires a percentile from 0 to 100, found 101",
        ),
        (
            "list_quantile(input.values, 100.5)",
            "function 'list_quantile' requires a percentile from 0 to 100, found 100.5",
        ),
        (
            "list_quantile(input.values, input.index)",
            "function 'list_quantile' requires its percentile to be a numeric literal",
        ),
        (
            "list_quantile(input.values, -1)",
            "function 'list_quantile' requires its percentile to be a numeric literal",
        ),
        (
            "list_quantile(input.values)",
            "function 'list_quantile' expects 2 arguments, found 1",
        ),
        (
            "list_median(input.values, 50)",
            "function 'list_median' expects 1 arguments, found 2",
        ),
    ] {
        let refused = parse_program(&format!("SET result = {expression}"))
            .expect_err("the call is refused when it is lowered");
        assert!(refused.contains(message), "{expression}: {refused}");
    }
}

/// Compiles `SET result = <expression>` over a VEC<F64> and runs it over `lists`, returning the
/// results and the errors of every row.
fn execute(
    expression: &str,
    lists: &ArrayRef,
    indexes: &[Option<i64>],
) -> (Float64Array, RowErrors) {
    let program = compile(expression, DataType::Float64, DataType::Float64)
        .verified("each executed case compiles");
    let batch = TypedBatch::try_new(
        values_schema(DataType::Float64),
        vec![
            TypedArray::Generic(lists.clone()),
            TypedArray::Int64(Int64Array::from(indexes.to_vec())),
        ],
    )
    .verified("the columns match the schema");
    let output = execute_program_sync(&program, &batch).verified("the program executes");
    let result = output
        .schema()
        .index_of("result")
        .verified("the output schema declares the result field");
    let TypedArray::Float64(results) = output.column(result) else {
        panic!("the result field is F64");
    };
    (results.clone(), output.errors().clone())
}

#[test]
fn a_conditional_arm_reports_only_the_failures_of_rows_it_selects() {
    let lists = required_float_lists(&[
        Some(&[1.0, f64::NAN]),
        Some(&[1.0, 3.0]),
        Some(&[f64::INFINITY]),
    ]);
    let indexes = [Some(0), Some(1), Some(1)];
    for (statistic, selected) in [
        ("list_var_pop(input.values)", 1.0),
        ("list_quantile(input.values, 50)", 2.0),
    ] {
        let expression = format!("CASE WHEN input.index > 0 THEN {statistic} END");
        let (results, errors) = execute(&expression, &lists, &indexes);
        assert!(results.is_null(0));
        assert!(
            errors.row(0).is_empty(),
            "{statistic}: an unselected row reports nothing"
        );
        assert_eq!(results.value(1), selected, "{statistic}");
        assert!(errors.row(1).is_empty());
        assert!(results.is_null(2));
        assert_eq!(
            errors.row(2)[0].code(),
            ErrorCode::InvalidArgument,
            "{statistic}"
        );
    }
}

#[test]
fn the_median_is_the_quantile_at_fifty_and_no_call_is_folded() {
    let lists = required_float_lists(&[Some(&[4.0, 1.0, 3.0, 2.0]), Some(&[]), None]);
    let indexes = [None, None, None];
    let (median, _) = execute("list_median(input.values)", &lists, &indexes);
    let (fiftieth, _) = execute("list_quantile(input.values, 50)", &lists, &indexes);
    assert_eq!(median, fiftieth);
    assert_eq!(median.value(0), 2.5);

    let program = compile(
        "list_var_pop([1.0, 2.0])",
        DataType::Float64,
        DataType::Float64,
    )
    .verified("a statistic of constants compiles");
    let runs_the_kernel = program.instructions.iter().any(|instruction| {
        matches!(
            &instruction.kind,
            InstructionKind::Builtin {
                lowering: BuiltinLowering::ListStatistic(_),
                ..
            }
        )
    });
    assert!(
        runs_the_kernel,
        "a list statistic of constants is computed, not folded"
    );
}

// --- The registered property -------------------------------------------------------------------

#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
enum SpecialValue {
    PositiveZero,
    NegativeZero,
    Nan,
    NegativeNan,
    Infinity,
    NegativeInfinity,
}

/// A value the total order and the finiteness checks treat specially.
impl SpecialValue {
    fn value(self) -> f64 {
        match self {
            Self::PositiveZero => 0.0,
            Self::NegativeZero => -0.0,
            Self::Nan => f64::NAN,
            Self::NegativeNan => -f64::NAN,
            Self::Infinity => f64::INFINITY,
            Self::NegativeInfinity => f64::NEG_INFINITY,
        }
    }
}

/// One generated row: a null list, or a list of finite integers with at most one special value.
/// The integers are `I16`, so every centered difference from a list's mean is exact and the
/// dispersion tolerance below bounds only the rounding of the squares and their sums.
#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedRow {
    null: bool,
    values: Vec<i16>,
    special: Option<(u8, SpecialValue)>,
}

impl GeneratedRow {
    fn values(&self, width: Option<usize>) -> Option<Vec<f64>> {
        if self.null {
            return None;
        }
        let mut values = self
            .values
            .iter()
            .map(|value| f64::from(*value))
            .collect::<Vec<_>>();
        if let Some(width) = width {
            values.resize(width, 0.0);
        }
        if let Some((at, special)) = self.special
            && !values.is_empty()
        {
            let at = usize::from(at) % values.len();
            values[at] = special.value();
        }
        Some(values)
    }
}

/// A generated batch of lists: its rows, whether they are fixed `ARRAY`s of one width, the
/// percentile its quantile selects, and the leading rows a slice of the batch skips.
#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedLists {
    rows: Vec<GeneratedRow>,
    fixed_width: Option<u8>,
    percentile: u16,
    skipped: u8,
}

impl GeneratedLists {
    /// The rows, each null or a list, and the Arrow column holding them after the skipped rows.
    fn column(&self) -> (Vec<Option<Vec<f64>>>, ArrayRef) {
        let width = self.fixed_width.map(|width| usize::from(width % 8) + 1);
        let mut rows = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            rows.push(row.values(width));
        }
        let cells = list_cells(rows.iter().map(Option::as_deref));
        let column: ArrayRef = match width {
            Some(width) => StdArc::new(
                FixedSizeListArray::from_iter_primitive::<Float64Type, _, _>(
                    cells,
                    i32::try_from(width).verified("a generated width is at most eight"),
                ),
            ),
            None => StdArc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(cells)),
        };
        let skipped = usize::from(self.skipped).min(rows.len());
        let kept = rows.len() - skipped;
        (rows[skipped..].to_vec(), column.slice(skipped, kept))
    }

    fn percentile(&self) -> f64 {
        f64::from(self.percentile % 1001) / 10.0
    }
}

/// The variance of `values` from their exact integer sums, rounded once.
fn exact_variance(values: &[f64], estimator: nervix_simd_kernels::Estimator) -> Option<f64> {
    let count = i128::try_from(values.len()).verified("a generated list is short");
    let mut sum = 0_i128;
    let mut squares = 0_i128;
    for &value in values {
        let value = value
            .checked_approx_into::<i64>()
            .verified("the values are generated integers");
        sum += i128::from(value);
        squares += i128::from(value) * i128::from(value);
    }
    let centered = count * squares - sum * sum;
    let divisor = match estimator {
        nervix_simd_kernels::Estimator::Population => count * count,
        nervix_simd_kernels::Estimator::Sample => count * (count - 1),
    };
    if divisor == 0 {
        return None;
    }
    Some(centered.approx_into::<f64>() / divisor.approx_into::<f64>())
}

/// The quantile of `values` from a full sort by the total order, at the rank the window
/// quantiles interpolate at.
fn sorted_quantile(values: &[f64], percentile: f64) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = QuantileRank::new(percentile, u64::try_from(sorted.len()).verified("short"))?;
    let lower = sorted[rank
        .lower()
        .checked_approx_into::<usize>()
        .verified("in range")];
    let upper = sorted[rank
        .upper()
        .checked_approx_into::<usize>()
        .verified("in range")];
    if rank.lower() == rank.upper() {
        return Some(lower);
    }
    Some(rank.interpolate(lower, upper))
}

/// The first position of the least or greatest value by the total order, from a full scan.
fn scanned_position(values: &[f64], extremum: ListExtremum) -> Option<i64> {
    let mut best: Option<usize> = None;
    for (position, value) in values.iter().enumerate() {
        let replaces = match best {
            Some(current) => extremum.replaces(value.total_cmp(&values[current])),
            None => true,
        };
        if replaces {
            best = Some(position);
        }
    }
    best.map(|position| i64::try_from(position).verified("a generated list is short"))
}

#[test]
fn bolero_list_statistics_match_a_reference_over_fixed_ragged_sliced_and_null_lists() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .with_type::<GeneratedLists>()
        .for_each(|generated| {
            let (rows, column) = generated.column();
            let percentile = generated.percentile();
            let statistics = [
                VAR_POP,
                VAR_SAMP,
                STDDEV_POP,
                STDDEV_SAMP,
                MEDIAN,
                quantile(percentile),
            ];
            for statistic in statistics {
                let (output, errors) = run(statistic, &column);
                let TypedArray::Float64(output) = output else {
                    panic!("a floating-point statistic yields F64");
                };
                for (row, values) in rows.iter().enumerate() {
                    let Some(values) = values else {
                        assert!(output.is_null(row) && errors.row(row).is_empty());
                        continue;
                    };
                    if values.iter().any(|value| !value.is_finite()) {
                        assert!(output.is_null(row), "{statistic:?} row {row}");
                        assert_eq!(errors.row(row)[0].code(), ErrorCode::InvalidArgument);
                        continue;
                    }
                    assert!(errors.row(row).is_empty(), "{statistic:?} row {row}");
                    let expected = reference(statistic, values);
                    let actual = output.is_valid(row).then(|| output.value(row));
                    assert_statistic(statistic, values.len(), expected, actual);
                }
            }
            for extremum in [ListExtremum::Least, ListExtremum::Greatest] {
                let located = positions(ListStatistic::ExtremeIndex(extremum), &column);
                for (row, values) in rows.iter().enumerate() {
                    let expected = values
                        .as_ref()
                        .and_then(|values| scanned_position(values, extremum));
                    assert_eq!(located[row], expected, "{extremum:?} row {row}");
                }
            }
            assert_identities(&column);
        });
}

fn reference(statistic: ListStatistic, values: &[f64]) -> Option<f64> {
    match statistic {
        ListStatistic::Dispersion(dispersion) => {
            let variance = exact_variance(values, dispersion.estimator())?;
            Some(dispersion.from_variance(variance))
        }
        ListStatistic::Quantile(quantile) => sorted_quantile(values, quantile.percentile().value()),
        ListStatistic::ExtremeIndex(_) => None.verified("indexes are checked by position"),
    }
}

/// A dispersion agrees with the exact variance within `(count + 8)` units of the last place of
/// its own magnitude, and exactly when every value is equal; a quantile agrees by bits.
fn assert_statistic(
    statistic: ListStatistic,
    count: usize,
    expected: Option<f64>,
    actual: Option<f64>,
) {
    let (Some(expected), Some(actual)) = (expected, actual) else {
        assert_eq!(expected.is_some(), actual.is_some(), "{statistic:?}");
        return;
    };
    match statistic {
        ListStatistic::Dispersion(_) => {
            let ulps = (count + 8).approx_into::<f64>();
            let tolerance = ulps * f64::EPSILON * expected.abs();
            assert!(
                (actual - expected).abs() <= tolerance,
                "{statistic:?}: {actual} differs from {expected} by more than {tolerance}"
            );
        }
        ListStatistic::Quantile(_) | ListStatistic::ExtremeIndex(_) => {
            assert_eq!(actual.to_bits(), expected.to_bits(), "{statistic:?}");
        }
    }
}
