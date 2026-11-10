//! Tests of the conversions a cast accepts and of the list conversion kernel.
//!
//! Layer: test harness.
//!
//! - **Owns.** Which pairs of types a cast accepts or refuses and why, and the kernel's results,
//!   row failures and buffer reuse over fixed, ragged, empty, sliced and null lists.
//! - **Depends on.** The cast module, Arrow arrays and the scalar conversion kernel.
//! - **Must not know.** Programs, registers or how a caller handles a failed row.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array,
    ListArray, StringArray, TimestampNanosecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef, TimeUnit};
use nervix_primitives::sync::StdArc;

use super::*;
use crate::semantics::ArmExecution;

/// The operation every conversion below is recorded against.
const SPAN: Span = Span { start: 3, end: 9 };

fn item(data_type: DataType) -> FieldRef {
    StdArc::new(Field::new("item", data_type, false))
}

fn vec_type(element: DataType) -> DataType {
    DataType::List(item(element))
}

fn array_type(element: DataType, size: i32) -> DataType {
    DataType::FixedSizeList(item(element), size)
}

fn datetime_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
}

/// A `VEC` column over `elements`, whose rows hold `lengths` elements each, null where `valid`
/// says so.
fn vec_column(elements: ArrayRef, lengths: &[usize], valid: Option<&[bool]>) -> ArrayRef {
    let nulls = valid.map(|valid| NullBuffer::from(valid.to_vec()));
    let list = ListArray::try_new(
        item(elements.data_type().clone()),
        OffsetBuffer::from_lengths(lengths.iter().copied()),
        elements,
        nulls,
    )
    .assured("the test lengths cover exactly the test elements");
    StdArc::new(list)
}

/// An `ARRAY` column of `size` over `elements`, null where `valid` says so.
fn array_column(elements: ArrayRef, size: i32, valid: Option<&[bool]>) -> ArrayRef {
    let nulls = valid.map(|valid| NullBuffer::from(valid.to_vec()));
    let list =
        FixedSizeListArray::try_new(item(elements.data_type().clone()), size, elements, nulls)
            .assured("the test elements fill every test value of the array");
    StdArc::new(list)
}

fn integers(values: &[i64]) -> ArrayRef {
    StdArc::new(Int64Array::from(values.to_vec()))
}

fn texts(values: &[&str]) -> ArrayRef {
    StdArc::new(StringArray::from(values.to_vec()))
}

fn plan(value: &DataType, target: &DataType) -> ListCast {
    let CastTarget::List(plan) = CastTarget::between(value, target)
        .unwrap_or_else(|refusal| panic!("{value} AS {target} converts, refused: {refusal}"))
    else {
        panic!("{value} AS {target} is a list conversion");
    };
    plan
}

/// What converting one column yields under one failure policy.
struct Converted {
    output: ArrayRef,
    errors: RowErrors,
}

impl Converted {
    /// The reason recorded for each row, `None` for a row that recorded none.
    fn reasons(&self) -> Vec<Option<SideErrorReason>> {
        let mut reasons = Vec::with_capacity(self.errors.row_count());
        for row in self.errors.iter() {
            match row {
                [] => reasons.push(None),
                [error] => {
                    assert_eq!(error.span, SPAN, "a row error names the conversion's span");
                    reasons.push(Some(error.reason.clone()));
                }
                errors => panic!("a row records at most one conversion failure, found {errors:?}"),
            }
        }
        reasons
    }
}

fn convert(plan: &ListCast, input: &ArrayRef, on_failure: CastFailure) -> Converted {
    let mut errors = RowErrors::new(input.len());
    let output = plan
        .convert(
            TypedArray::Generic(StdArc::clone(input)),
            on_failure,
            &mut errors,
            SPAN,
        )
        .assured("a column of the plan's type converts");
    let TypedArray::Generic(output) = output else {
        panic!("a list conversion yields a list column");
    };
    Converted { output, errors }
}

/// Converts `input` under both policies and checks that both yield `expected`, with `failures`
/// recorded only by `AS`.
fn assert_converts(
    value: &DataType,
    target: &DataType,
    input: &ArrayRef,
    expected: &ArrayRef,
    failures: &[Option<SideErrorReason>],
) {
    let plan = plan(value, target);
    let strict = convert(&plan, input, CastFailure::Error);
    let tolerant = convert(&plan, input, CastFailure::Null);
    for (label, converted) in [("AS", &strict), ("TRY_CAST", &tolerant)] {
        assert_eq!(
            converted.output.data_type(),
            target,
            "{label} yields the target type"
        );
        assert_eq!(
            converted.output.as_ref(),
            expected.as_ref(),
            "{label} converts {value} to {target}"
        );
        converted
            .output
            .to_data()
            .validate_full()
            .assured("a converted column is a valid Arrow array");
        assert_eq!(
            null_elements(&converted.output),
            0,
            "{label} leaves no null element beneath a list, a null list's included"
        );
    }
    assert_eq!(strict.reasons(), failures, "AS reports the failed rows");
    assert!(
        tolerant.errors.is_error_free(),
        "TRY_CAST reports nothing for the rows it nulls"
    );
}

/// The null elements every level beneath the outermost holds, where a reader of lists refuses one
/// even beneath a null list.
fn null_elements(column: &ArrayRef) -> usize {
    let mut nulls = 0;
    let mut elements = StdArc::clone(column);
    loop {
        elements = match elements.data_type() {
            DataType::List(_) => StdArc::clone(as_vec(&elements).values()),
            DataType::FixedSizeList(_, _) => StdArc::clone(as_array(&elements).values()),
            _ => return nulls,
        };
        nulls += elements.null_count();
    }
}

fn element_failure(target: RegisterType) -> Option<SideErrorReason> {
    Some(SideErrorReason::ListElementCastFailed { target })
}

fn length_failure(expected: usize, found: usize) -> Option<SideErrorReason> {
    Some(SideErrorReason::ListCastLengthMismatch { expected, found })
}

#[test]
fn a_list_converts_only_to_a_list_type_nested_as_deep_with_arrays_of_its_length() {
    let integer_vec = vec_type(DataType::Int64);
    let integer_pair = array_type(DataType::Int64, 2);
    let integer_grid = array_type(array_type(DataType::Int64, 3), 2);
    for (value, target, refusal, message) in [
        (
            integer_vec.clone(),
            DataType::Utf8,
            CastRefusal::ListToScalar,
            "an ARRAY or VEC value converts only to an ARRAY or VEC type",
        ),
        (
            array_type(DataType::Int64, 1),
            DataType::Int64,
            CastRefusal::ListToScalar,
            "an ARRAY or VEC value converts only to an ARRAY or VEC type",
        ),
        (
            DataType::Utf8,
            vec_type(DataType::Utf8),
            CastRefusal::ScalarToList,
            "only an ARRAY or VEC value converts to an ARRAY or VEC type",
        ),
        (
            integer_vec.clone(),
            vec_type(vec_type(DataType::Int64)),
            CastRefusal::Depth {
                value: 1,
                target: 2,
            },
            "an ARRAY or VEC value nested 1 deep converts only to a type nested 1 deep, found one \
             nested 2 deep",
        ),
        (
            integer_grid.clone(),
            vec_type(DataType::Int64),
            CastRefusal::Depth {
                value: 2,
                target: 1,
            },
            "an ARRAY or VEC value nested 2 deep converts only to a type nested 2 deep, found one \
             nested 1 deep",
        ),
        (
            integer_pair.clone(),
            array_type(DataType::Int64, 3),
            CastRefusal::ArrayLength {
                value: 2,
                target: 3,
            },
            "an ARRAY of length 2 never converts to an ARRAY of length 3",
        ),
        (
            integer_grid.clone(),
            array_type(array_type(DataType::Int64, 4), 2),
            CastRefusal::ArrayLength {
                value: 3,
                target: 4,
            },
            "an ARRAY of length 3 never converts to an ARRAY of length 4",
        ),
        (
            vec_type(DataType::Binary),
            vec_type(DataType::Utf8),
            CastRefusal::Bytes,
            "BYTES conversions require bytes_from_utf8, bytes_to_utf8, base64 or hex functions",
        ),
        (
            integer_vec.clone(),
            array_type(DataType::Binary, 2),
            CastRefusal::Bytes,
            "BYTES conversions require bytes_from_utf8, bytes_to_utf8, base64 or hex functions",
        ),
    ] {
        assert_eq!(
            CastTarget::between(&value, &target),
            Err(refusal.clone()),
            "{value} AS {target}"
        );
        assert_eq!(refusal.to_string(), message);
    }

    // A container converts whatever its elements are, and the elements of `BYTES` lists convert
    // to `BYTES` only.
    for (value, target) in [
        (integer_vec.clone(), integer_vec.clone()),
        (integer_pair.clone(), integer_vec.clone()),
        (integer_vec.clone(), integer_pair.clone()),
        (integer_grid.clone(), vec_type(vec_type(DataType::Float32))),
        (vec_type(DataType::Binary), array_type(DataType::Binary, 4)),
        (vec_type(DataType::Utf8), vec_type(datetime_type())),
        (vec_type(DataType::Boolean), vec_type(datetime_type())),
    ] {
        let converted = CastTarget::between(&value, &target);
        assert!(
            matches!(converted, Ok(CastTarget::List(_))),
            "{value} AS {target}: {converted:?}"
        );
    }
}

#[test]
fn a_scalar_conversion_keeps_its_bytes_and_type_refusals() {
    assert_eq!(
        CastTarget::between(&DataType::Int64, &DataType::Utf8),
        Ok(CastTarget::Scalar(RegisterType::Utf8))
    );
    assert_eq!(
        CastTarget::between(&DataType::Binary, &DataType::Binary),
        Ok(CastTarget::Scalar(RegisterType::Binary))
    );
    for (value, target) in [
        (DataType::Binary, DataType::Utf8),
        (DataType::Utf8, DataType::Binary),
    ] {
        assert_eq!(
            CastTarget::between(&value, &target),
            Err(CastRefusal::Bytes)
        );
    }
    let refusal = CastTarget::between(&DataType::Float16, &DataType::Int64)
        .expect_err("the VM holds no register for a half-precision float");
    assert_eq!(
        refusal.to_string(),
        "casts are only implemented for supported nervix VM types, found Float16 AS Int64"
    );
}

#[test]
fn the_arm_executing_a_list_conversion_follows_its_element_conversion() {
    let text = plan(&vec_type(DataType::Utf8), &vec_type(DataType::Int64));
    let formatted = plan(&vec_type(DataType::Float64), &vec_type(DataType::Utf8));
    let numeric = plan(&vec_type(DataType::Int64), &vec_type(DataType::Float32));
    let container = plan(&vec_type(DataType::Utf8), &array_type(DataType::Utf8, 2));
    for (plan, execution) in [
        (text, ArmExecution::SelectedRows),
        (formatted, ArmExecution::SelectedRows),
        (numeric, ArmExecution::WholeBatch),
        (container, ArmExecution::WholeBatch),
    ] {
        assert_eq!(
            CastTarget::List(plan).arm_execution(RegisterType::Generic),
            execution
        );
    }
}

#[test]
fn an_element_conversion_keeps_the_offsets_and_validity_of_every_level() {
    let input = vec_column(
        integers(&[1, 2, 3, -4, 5]),
        &[3, 0, 0, 2],
        Some(&[true, true, false, true]),
    );
    let expected = vec_column(
        StdArc::new(Float32Array::from(vec![1.0, 2.0, 3.0, -4.0, 5.0])),
        &[3, 0, 0, 2],
        Some(&[true, true, false, true]),
    );
    let target = vec_type(DataType::Float32);
    assert_converts(
        &vec_type(DataType::Int64),
        &target,
        &input,
        &expected,
        &[None, None, None, None],
    );

    let converted = convert(
        &plan(&vec_type(DataType::Int64), &target),
        &input,
        CastFailure::Error,
    );
    let (input, output) = (as_vec(&input), as_vec(&converted.output));
    assert_eq!(
        output.offsets().inner().inner().as_ptr(),
        input.offsets().inner().inner().as_ptr(),
        "the converted VEC reuses the input's offsets"
    );
    assert_eq!(
        output.nulls().map(|nulls| nulls.buffer().as_ptr()),
        input.nulls().map(|nulls| nulls.buffer().as_ptr()),
        "the converted VEC reuses the input's validity"
    );

    let grid = array_column(
        array_column(integers(&[1, 2, 3, 4, 0, 0, 0, 0]), 2, None),
        2,
        Some(&[true, false]),
    );
    let expected = array_column(
        array_column(
            StdArc::new(Float64Array::from(vec![
                1.0, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0,
            ])),
            2,
            None,
        ),
        2,
        Some(&[true, false]),
    );
    assert_converts(
        &array_type(array_type(DataType::Int64, 2), 2),
        &array_type(array_type(DataType::Float64, 2), 2),
        &grid,
        &expected,
        &[None, None],
    );
}

fn as_vec(column: &ArrayRef) -> &ListArray {
    column
        .as_any()
        .downcast_ref::<ListArray>()
        .verified("the test column is a VEC")
}

fn as_array(column: &ArrayRef) -> &FixedSizeListArray {
    column
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .verified("the test column is an ARRAY")
}

/// The innermost `I64` elements of a column nested `depth` deep.
fn integer_elements(column: &ArrayRef, depth: usize) -> Int64Array {
    let mut elements = StdArc::clone(column);
    for _ in 0..depth {
        elements = match elements.data_type() {
            DataType::List(_) => StdArc::clone(as_vec(&elements).values()),
            _ => StdArc::clone(as_array(&elements).values()),
        };
    }
    elements
        .as_any()
        .downcast_ref::<Int64Array>()
        .verified("the test elements are I64")
        .clone()
}

#[test]
fn a_container_conversion_reuses_the_element_values() {
    let elements = integers(&[1, 2, 3, 4, 5, 6, 7, 8]);
    let grid = array_column(array_column(StdArc::clone(&elements), 2, None), 2, None);
    let rows = vec_column(
        vec_column(StdArc::clone(&elements), &[2, 2, 2, 2], None),
        &[2, 2],
        None,
    );
    let grid_type = array_type(array_type(DataType::Int64, 2), 2);
    let rows_type = vec_type(vec_type(DataType::Int64));
    assert_converts(&grid_type, &rows_type, &grid, &rows, &[None, None]);
    assert_converts(&rows_type, &grid_type, &rows, &grid, &[None, None]);
    for (value, target, input) in [
        (&grid_type, &rows_type, &grid),
        (&rows_type, &grid_type, &rows),
    ] {
        let converted = convert(&plan(value, target), input, CastFailure::Error);
        assert_eq!(
            integer_elements(&converted.output, 2).values().as_ptr(),
            integer_elements(input, 2).values().as_ptr(),
            "{value} AS {target} reuses the element values"
        );
    }

    // A VEC that holds a sliced range of its elements becomes an ARRAY over the same values.
    let sliced = vec_column(StdArc::clone(&elements), &[1, 3, 3, 1], None).slice(1, 2);
    let expected = array_column(integers(&[2, 3, 4, 5, 6, 7]), 3, None);
    let triple = array_type(DataType::Int64, 3);
    assert_converts(
        &vec_type(DataType::Int64),
        &triple,
        &sliced,
        &expected,
        &[None, None],
    );
    let converted = convert(
        &plan(&vec_type(DataType::Int64), &triple),
        &sliced,
        CastFailure::Error,
    );
    // The VEC's first visible element is its second, where the ARRAY's elements begin.
    assert_eq!(
        integer_elements(&converted.output, 1).values().as_ptr(),
        integer_elements(&sliced, 1).values()[1..].as_ptr(),
        "the ARRAY slices the VEC's element values"
    );
}

#[test]
fn a_vec_of_another_length_fails_its_row_under_as_and_nulls_it_under_try_cast() {
    let input = vec_column(
        integers(&[1, 2, 3, 4, 5, 6, 7, 8, 9]),
        &[3, 2, 0, 0, 3, 1],
        Some(&[true, true, true, false, true, true]),
    );
    let expected = array_column(
        integers(&[1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 7, 8, 0, 0, 0]),
        3,
        Some(&[true, false, false, false, true, false]),
    );
    assert_converts(
        &vec_type(DataType::Int64),
        &array_type(DataType::Int64, 3),
        &input,
        &expected,
        &[
            None,
            length_failure(3, 2),
            length_failure(3, 0),
            None,
            None,
            length_failure(3, 1),
        ],
    );
}

#[test]
fn an_element_that_does_not_convert_fails_its_whole_list() {
    let input = vec_column(
        texts(&["7", "8", "1", "x", "-2", "300"]),
        &[2, 0, 2, 1, 1],
        Some(&[true, true, true, false, true]),
    );
    let expected = vec_column(
        integers(&[7, 8, 300]),
        &[2, 0, 0, 0, 1],
        Some(&[true, true, false, false, true]),
    );
    assert_converts(
        &vec_type(DataType::Utf8),
        &vec_type(DataType::Int64),
        &input,
        &expected,
        &[None, None, element_failure(RegisterType::Int64), None, None],
    );

    // An element converts by the scalar rule of its pair, which converts no BOOL to a DATETIME,
    // so every list that holds one fails and an empty list still converts.
    let flags = vec_column(
        StdArc::new(BooleanArray::from(vec![true, false])),
        &[0, 2],
        None,
    );
    let expected = vec_column(
        StdArc::new(TimestampNanosecondArray::from(Vec::<i64>::new()).with_timezone_utc()),
        &[0, 0],
        Some(&[true, false]),
    );
    assert_converts(
        &vec_type(DataType::Boolean),
        &vec_type(datetime_type()),
        &flags,
        &expected,
        &[None, element_failure(RegisterType::Datetime)],
    );
}

#[test]
fn a_null_row_never_fails_whatever_its_hidden_elements_hold() {
    // The null row hides an element that does not convert, which a VEC cannot hold as a null, so
    // the converted null row holds no element at all.
    let input = vec_column(
        texts(&["1", "x", "y", "2"]),
        &[1, 2, 1],
        Some(&[true, false, true]),
    );
    let expected = vec_column(integers(&[1, 2]), &[1, 0, 1], Some(&[true, false, true]));
    assert_converts(
        &vec_type(DataType::Utf8),
        &vec_type(DataType::Int64),
        &input,
        &expected,
        &[None, None, None],
    );

    // A null ARRAY may hide null elements, which the VEC it becomes cannot hold.
    let elements: ArrayRef = StdArc::new(Int64Array::from(vec![
        Some(1),
        Some(2),
        None,
        None,
        Some(5),
        Some(6),
    ]));
    let pairs = array_column(elements, 2, Some(&[true, false, true]));
    let expected = vec_column(
        integers(&[1, 2, 5, 6]),
        &[2, 0, 2],
        Some(&[true, false, true]),
    );
    assert_converts(
        &array_type(DataType::Int64, 2),
        &vec_type(DataType::Int64),
        &pairs,
        &expected,
        &[None, None, None],
    );
}

#[test]
fn a_failure_anywhere_in_a_nested_list_fails_its_outermost_row() {
    let rows_type = vec_type(vec_type(DataType::Int64));
    let grid_type = array_type(array_type(DataType::Int64, 2), 2);
    let rows = vec_column(
        vec_column(integers(&[1, 2, 3, 4, 5, 6, 7, 8, 9]), &[2, 2, 2, 3], None),
        &[2, 2],
        None,
    );
    let expected = array_column(
        array_column(
            integers(&[1, 2, 3, 4, 0, 0, 0, 0]),
            2,
            Some(&[true, true, false, false]),
        ),
        2,
        Some(&[true, false]),
    );
    assert_converts(
        &rows_type,
        &grid_type,
        &rows,
        &expected,
        &[None, length_failure(2, 3)],
    );

    let words = vec_column(
        vec_column(texts(&["1", "2", "three", "4"]), &[1, 1, 2], None),
        &[2, 1],
        None,
    );
    let expected = vec_column(
        vec_column(integers(&[1, 2]), &[1, 1], None),
        &[2, 0],
        Some(&[true, false]),
    );
    assert_converts(
        &vec_type(vec_type(DataType::Utf8)),
        &rows_type,
        &words,
        &expected,
        &[None, element_failure(RegisterType::Int64)],
    );
}

#[test]
fn a_sliced_column_converts_only_the_rows_it_holds() {
    let elements = texts(&["0", "1", "x", "3", "4", "5"]);
    let input = vec_column(elements, &[1, 2, 1, 2], None).slice(2, 2);
    let expected = vec_column(integers(&[3, 4, 5]), &[1, 2], None);
    assert_converts(
        &vec_type(DataType::Utf8),
        &vec_type(DataType::Int64),
        &input,
        &expected,
        &[None, None],
    );

    let pairs = array_column(integers(&[1, 2, 3, 4, 5, 6]), 2, None).slice(1, 2);
    let expected = vec_column(
        StdArc::new(Float64Array::from(vec![3.0, 4.0, 5.0, 6.0])),
        &[2, 2],
        None,
    );
    assert_converts(
        &array_type(DataType::Int64, 2),
        &vec_type(DataType::Float64),
        &pairs,
        &expected,
        &[None, None],
    );
}

#[test]
fn an_identity_conversion_returns_its_input() {
    let input = vec_column(integers(&[1, 2]), &[2, 0], Some(&[true, false]));
    let converted = convert(
        &plan(&vec_type(DataType::Int64), &vec_type(DataType::Int64)),
        &input,
        CastFailure::Error,
    );
    assert!(StdArc::ptr_eq(&converted.output, &input));
    assert!(converted.errors.is_error_free());
}

#[test]
fn a_column_of_another_type_is_a_typed_runtime_error() {
    let plan = plan(&vec_type(DataType::Int64), &vec_type(DataType::Float64));
    let input = vec_column(texts(&["1"]), &[1], None);
    let mut errors = RowErrors::new(1);
    let error = plan
        .convert(
            TypedArray::Generic(input),
            CastFailure::Error,
            &mut errors,
            SPAN,
        )
        .expect_err("a column of another type is not the plan's");
    assert!(
        matches!(
            error.current_context(),
            RuntimeError::CollectionTypeMismatch {
                operation: "list conversion",
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn an_array_type_of_no_element_is_refused_as_unsupported() {
    for (value, target) in [
        (
            array_type(DataType::Int64, 0),
            array_type(DataType::Int64, 0),
        ),
        (vec_type(DataType::Int64), array_type(DataType::Int64, -1)),
        (array_type(DataType::Int64, 0), vec_type(DataType::Int64)),
    ] {
        let refusal = CastTarget::between(&value, &target)
            .expect_err("no Nervix type holds an ARRAY of no element");
        assert_eq!(
            refusal,
            CastRefusal::Unsupported {
                value: value.clone(),
                target: target.clone(),
            }
        );
    }
}

#[test]
fn an_element_failure_beside_null_elements_hidden_by_a_null_array_fails_only_its_row() {
    let elements: ArrayRef = StdArc::new(StringArray::from(vec![
        Some("1"),
        Some("2"),
        None,
        None,
        Some("x"),
        Some("4"),
    ]));
    let pairs = array_column(elements, 2, Some(&[true, false, true]));
    let expected = array_column(
        integers(&[1, 2, 0, 0, 0, 0]),
        2,
        Some(&[true, false, false]),
    );
    assert_converts(
        &array_type(DataType::Utf8, 2),
        &array_type(DataType::Int64, 2),
        &pairs,
        &expected,
        &[None, None, element_failure(RegisterType::Int64)],
    );
}

#[test]
fn a_vec_of_arrays_converts_its_elements_on_the_input_layout() {
    let input = vec_column(
        array_column(integers(&[1, 2, 3, 4, 5, 6]), 2, None),
        &[2, 0, 1],
        Some(&[true, false, true]),
    );
    let expected = vec_column(
        array_column(
            StdArc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])),
            2,
            None,
        ),
        &[2, 0, 1],
        Some(&[true, false, true]),
    );
    let value = vec_type(array_type(DataType::Int64, 2));
    let target = vec_type(array_type(DataType::Float64, 2));
    assert_converts(&value, &target, &input, &expected, &[None, None, None]);
    let converted = convert(&plan(&value, &target), &input, CastFailure::Error);
    assert_eq!(
        as_vec(&converted.output).offsets().inner().inner().as_ptr(),
        as_vec(&input).offsets().inner().inner().as_ptr(),
        "the converted VEC of ARRAYs keeps the input's offsets"
    );
}

#[test]
fn null_elements_take_the_empty_value_of_their_type() {
    let datetimes: ArrayRef =
        StdArc::new(TimestampNanosecondArray::from(vec![Some(7), None]).with_timezone_utc());
    let placeholders: [(ArrayRef, ArrayRef); 4] = [
        (
            StdArc::new(Int64Array::from(vec![Some(7), None])),
            StdArc::new(Int64Array::from(vec![7, 0])),
        ),
        (
            StdArc::new(StringArray::from(vec![None, Some("seven")])),
            StdArc::new(StringArray::from(vec!["", "seven"])),
        ),
        (
            StdArc::new(BooleanArray::from(vec![Some(true), None])),
            StdArc::new(BooleanArray::from(vec![true, false])),
        ),
        (
            datetimes,
            StdArc::new(TimestampNanosecondArray::from(vec![7, 0]).with_timezone_utc()),
        ),
    ];
    for (elements, expected) in placeholders {
        let replaced =
            without_null_elements(elements).assured("every scalar type has an empty value");
        assert_eq!(replaced.as_ref(), expected.as_ref());
        assert_eq!(replaced.null_count(), 0);
    }

    // Elements without a null, with or without a validity buffer, are returned as they are.
    let unmasked = integers(&[1, 2]);
    let masked: ArrayRef = StdArc::new(Int64Array::new(
        vec![1, 2].into(),
        Some(NullBuffer::new_valid(2)),
    ));
    for elements in [unmasked, masked] {
        let replaced = without_null_elements(StdArc::clone(&elements))
            .assured("elements without a null need no placeholder");
        assert!(StdArc::ptr_eq(&replaced, &elements));
    }
}

#[test]
fn offsets_beyond_what_a_list_holds_are_a_typed_refusal() {
    let largest = usize::try_from(i32::MAX).assured("i32::MAX is a valid usize");
    let overflowing = largest
        .checked_add(1)
        .assured("one past i32::MAX is a valid usize");
    for lengths in [vec![largest, 1], vec![overflowing]] {
        let error = offsets_from_lengths(lengths).expect_err("the offsets exceed an i32");
        assert!(
            matches!(
                error.current_context(),
                RuntimeError::CollectionTooLarge {
                    operation: "list conversion",
                    limit: CollectionLimit::VectorOffsets,
                }
            ),
            "{error:?}"
        );
    }
    let offsets = offsets_from_lengths([2, 0, 3]).assured("small lengths fit an i32");
    assert_eq!(offsets.inner().as_ref(), [0, 2, 2, 5]);
}
