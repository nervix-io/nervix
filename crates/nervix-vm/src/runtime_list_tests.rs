//! Direct tests of collection kernels over Arrow child buffers.
//!
//! Layer: test harness.
//!
//! - **Owns.** Buffer identity, scalar equality, empty/null behavior, and row error assertions.
//! - **Depends on.** The VM runtime and Arrow arrays.
//! - **Must not know.** Server graph ownership or connector behavior.

use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray,
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use super::*;
use crate::ErrorCode;

#[test]
fn construction_and_identity_slice_reuse_child_buffers() {
    let source = Int64Array::from(vec![Some(2), Some(3), None]);
    let input = TypedArray::Int64(source.clone());
    let array = ListConstruction::Fixed
        .construct(&[input], 3)
        .verified("three input values provide one item in each of three rows");
    let TypedArray::Generic(array) = array else {
        panic!("construction must yield a list");
    };
    let fixed = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .verified("the fixed constructor creates a fixed-size list");
    let child = fixed
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .verified("the constructor retains the input's I64 child type");
    assert_eq!(child.values().as_ptr(), source.values().as_ptr());
    assert!(fixed.is_null(2));

    let values: ArrayRef = StdArc::new(Int64Array::from(vec![2, 3, 4]));
    let field = StdArc::new(Field::new("item", DataType::Int64, false));
    let list = ListArray::try_new(
        field,
        OffsetBuffer::from_lengths([2, 1, 0]),
        values.clone(),
        None,
    )
    .verified("the offsets cover exactly the three child values");
    let input = TypedArray::Generic(StdArc::new(list));
    let starts = Int64Array::from(vec![0, 0, 0]);
    let lengths = Int64Array::from(vec![3, 3, 3]);
    let sliced = execute_list_slice(
        &input,
        CountOperand::Int64(Operand::Column(&starts)),
        CountOperand::Int64(Operand::Column(&lengths)),
    )
    .verified("the whole-list slice is valid");
    let TypedArray::Generic(sliced) = sliced else {
        panic!("slice must yield a list");
    };
    let TypedArray::Generic(original) = &input else {
        panic!("input must be a list");
    };
    assert!(StdArc::ptr_eq(&sliced, original));
    let concatenated = execute_list_concat(std::slice::from_ref(&input))
        .verified("one compatible list is a valid concatenation");
    let TypedArray::Generic(concatenated) = concatenated else {
        panic!("concat must yield a list");
    };
    assert!(StdArc::ptr_eq(&concatenated, original));

    let empty = ListConstruction::empty_vector(&original.data_type().clone(), 3)
        .verified("the list type supplies the empty vector's element type");
    let TypedArray::Generic(empty) = empty else {
        panic!("empty vector must be a list");
    };
    let empty = empty
        .as_any()
        .downcast_ref::<ListArray>()
        .verified("vec() creates a variable-length list");
    assert_eq!(empty.len(), 3);
    assert_eq!(empty.values().len(), 0);
}

#[test]
fn a_null_constructor_element_nulls_the_container_through_concat() {
    let first = TypedArray::Int64(Int64Array::from(vec![Some(1), Some(2)]));
    let second = TypedArray::Int64(Int64Array::from(vec![Some(3), None]));
    let array = ListConstruction::Fixed
        .construct(&[first, second], 2)
        .verified("matching I64 columns form fixed pairs");
    let TypedArray::Generic(ref values) = array else {
        panic!("construction creates an ARRAY");
    };
    let values = values
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .verified("ARRAY construction uses fixed-size lists");
    assert!(!values.is_null(0));
    assert!(values.is_null(1));

    let joined = execute_list_concat(&[array.clone(), array])
        .verified("null fixed arrays can be concatenated");
    let TypedArray::Generic(joined) = joined else {
        panic!("concat keeps ARRAY shape");
    };
    let joined = joined
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .verified("two fixed arrays concatenate to a fixed array");
    assert_eq!(joined.value_length(), 4);
    assert!(joined.is_null(1));
}

#[test]
fn vector_constructor_preserves_rows_and_rejects_invalid_columns() {
    let first = TypedArray::Int64(Int64Array::from(vec![Some(1), Some(2)]));
    let second = TypedArray::Int64(Int64Array::from(vec![Some(3), None]));
    let vector = ListConstruction::Variable
        .construct(&[first.clone(), second.clone()], 2)
        .verified("equal-length I64 columns form a vector");
    let TypedArray::Generic(vector) = vector else {
        panic!("vec constructs a list");
    };
    let vector = vector
        .as_any()
        .downcast_ref::<ListArray>()
        .verified("vec constructs a variable-length list");
    assert_eq!(vector.value_offsets(), &[0, 2, 4]);
    assert!(!vector.is_null(0));
    assert!(vector.is_null(1));
    let child = vector
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .verified("I64 columns keep their element type");
    assert_eq!(child.values().as_ref(), &[1, 3, 2, 0]);

    assert!(ListConstruction::Variable.construct(&[], 2).is_err());
    assert!(
        ListConstruction::Variable
            .construct(
                &[
                    first.clone(),
                    TypedArray::Float64(Float64Array::from(vec![1.0, 2.0]))
                ],
                2
            )
            .is_err()
    );
    assert!(
        ListConstruction::Variable
            .construct(&[first, TypedArray::Int64(Int64Array::from(vec![1]))], 2)
            .is_err()
    );
    assert!(ListConstruction::empty_vector(&DataType::Int64, 2).is_err());
}

#[test]
fn nullable_child_extrema_skip_missing_values_without_losing_empty_rows() {
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![None, Some(5), Some(2)]),
        Some(vec![None]),
        Some(vec![]),
        None,
    ]);
    let input = TypedArray::Generic(StdArc::new(lists));
    let minimum = execute_list_extremum(&input, ListExtremum::Min)
        .verified("nullable I64 children support minimum");
    let maximum = execute_list_extremum(&input, ListExtremum::Max)
        .verified("nullable I64 children support maximum");
    let TypedArray::Int64(minimum) = minimum else {
        panic!("minimum keeps I64 elements");
    };
    let TypedArray::Int64(maximum) = maximum else {
        panic!("maximum keeps I64 elements");
    };
    assert_eq!(
        minimum.iter().collect::<Vec<_>>(),
        vec![Some(2), None, None, None]
    );
    assert_eq!(
        maximum.iter().collect::<Vec<_>>(),
        vec![Some(5), None, None, None]
    );
}

#[test]
fn numeric_vector_dispatch_preserves_integer_dot_types_and_float_results() {
    macro_rules! check_type {
        ($arrow_type:ty, $variant:ident, $one:expr, $two:expr, $five:expr) => {{
            let list = TypedArray::Generic(StdArc::new(ListArray::from_iter_primitive::<
                $arrow_type,
                _,
                _,
            >([Some(vec![
                Some($one),
                Some($two),
            ])])));
            let mut errors = RowErrors::new(1);
            let dot = execute_list_dot(&list, &list, &mut errors, (0..1).into())
                .verified("equal numeric vectors support dot");
            let TypedArray::$variant(dot) = dot else {
                panic!("integer dot keeps the element type");
            };
            assert_eq!(dot.value(0), $five);
            let mean = execute_list_mean(&list, &mut errors, (0..1).into())
                .verified("numeric vectors support mean");
            let TypedArray::Float64(mean) = mean else {
                panic!("mean produces F64");
            };
            assert_eq!(mean.value(0), 1.5);
            let distance = execute_list_distance(&list, &list, &mut errors, (0..1).into())
                .verified("matching numeric vectors support distance");
            let TypedArray::Float64(distance) = distance else {
                panic!("distance produces F64");
            };
            assert_eq!(distance.value(0), 0.0);
            assert!(errors.row(0).is_empty());
        }};
    }
    check_type!(UInt8Type, UInt8, 1_u8, 2_u8, 5_u8);
    check_type!(Int8Type, Int8, 1_i8, 2_i8, 5_i8);
    check_type!(UInt16Type, UInt16, 1_u16, 2_u16, 5_u16);
    check_type!(Int16Type, Int16, 1_i16, 2_i16, 5_i16);
    check_type!(UInt32Type, UInt32, 1_u32, 2_u32, 5_u32);
    check_type!(Int32Type, Int32, 1_i32, 2_i32, 5_i32);
    check_type!(UInt64Type, UInt64, 1_u64, 2_u64, 5_u64);
    check_type!(Int64Type, Int64, 1_i64, 2_i64, 5_i64);
    check_type!(Float32Type, Float32, 1.0_f32, 2.0_f32, 5.0_f32);
}

#[test]
fn membership_uses_ieee_float_equality_over_child_values() {
    let values = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(f64::NAN)]),
        Some(vec![Some(-0.0)]),
        Some(vec![]),
        None,
    ]);
    let input = TypedArray::Generic(StdArc::new(values));
    let needles = TypedArray::Float64(Float64Array::from(vec![
        Some(f64::NAN),
        Some(0.0),
        Some(1.0),
        Some(1.0),
    ]));
    let contains = execute_list_contains(&input, &needles)
        .verified("the needle type matches the list element type");
    assert_eq!(
        contains.iter().collect::<Vec<_>>(),
        vec![Some(false), Some(true), Some(false), None]
    );
    let other = TypedArray::Generic(StdArc::new(ListArray::from_iter_primitive::<
        Float64Type,
        _,
        _,
    >([
        Some(vec![Some(f64::NAN)]),
        Some(vec![Some(0.0)]),
        Some(vec![Some(1.0)]),
        Some(vec![]),
    ])));
    let overlap = execute_list_overlap(&input, &other)
        .verified("the lists have the same element type and row count");
    assert_eq!(
        overlap.iter().collect::<Vec<_>>(),
        vec![Some(false), Some(true), Some(false), None]
    );
}

#[test]
fn slice_clips_bounds_and_keeps_null_containers() {
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(4)]),
        None,
    ]);
    let input = TypedArray::Generic(StdArc::new(lists));
    let starts = Int64Array::from(vec![-2, 5, 0]);
    let lengths = Int64Array::from(vec![2, -1, 1]);
    let sliced = execute_list_slice(
        &input,
        CountOperand::Int64(Operand::Column(&starts)),
        CountOperand::Int64(Operand::Column(&lengths)),
    )
    .verified("signed bounds are valid slice operands");
    let TypedArray::Generic(sliced) = sliced else {
        panic!("slice yields a vector");
    };
    let sliced = sliced
        .as_any()
        .downcast_ref::<ListArray>()
        .verified("slice always yields a variable-length list");
    assert_eq!(
        sliced
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .verified("I64 list elements stay I64")
            .values()
            .as_ref(),
        &[1, 2]
    );
    assert_eq!(sliced.value(1).len(), 0);
    assert!(sliced.is_null(2));
}

#[test]
fn fixed_extrema_compare_whole_child_lanes_and_keep_container_nulls() {
    let field = StdArc::new(Field::new("item", DataType::Int64, false));
    let values: ArrayRef = StdArc::new(Int64Array::from(vec![5, 2, 1, 7, 8, 4]));
    let lists = FixedSizeListArray::try_new(
        field,
        2,
        values,
        Some(NullBuffer::from(vec![true, true, false])),
    )
    .verified("six child values form three fixed pairs");
    let input = TypedArray::Generic(StdArc::new(lists));
    let smallest =
        execute_list_extremum(&input, ListExtremum::Min).verified("I64 pairs are ordered");
    let greatest =
        execute_list_extremum(&input, ListExtremum::Max).verified("I64 pairs are ordered");
    let TypedArray::Int64(smallest) = smallest else {
        panic!("minimum keeps the I64 type");
    };
    let TypedArray::Int64(greatest) = greatest else {
        panic!("maximum keeps the I64 type");
    };
    assert_eq!(
        smallest.iter().collect::<Vec<_>>(),
        vec![Some(2), Some(1), None]
    );
    assert_eq!(
        greatest.iter().collect::<Vec<_>>(),
        vec![Some(5), Some(7), None]
    );
}

#[test]
fn fixed_float_dot_multiplies_child_columns_and_respects_parent_nulls() {
    let field = StdArc::new(Field::new("item", DataType::Float64, false));
    let left_values: ArrayRef = StdArc::new(Float64Array::from(vec![
        1.0,
        2.0,
        3.0,
        4.0,
        f64::MAX,
        2.0,
        5.0,
        6.0,
    ]));
    let right_values: ArrayRef = StdArc::new(Float64Array::from(vec![
        3.0, 4.0, 1.0, 2.0, 2.0, 2.0, 1.0, 1.0,
    ]));
    let parent_nulls = Some(NullBuffer::from(vec![true, true, true, false]));
    let left = FixedSizeListArray::try_new(field.clone(), 2, left_values, parent_nulls.clone())
        .verified("eight float values form four fixed pairs");
    let right = FixedSizeListArray::try_new(field, 2, right_values, parent_nulls)
        .verified("eight float values form four fixed pairs");
    let mut errors = RowErrors::new(4);
    let dot = execute_list_dot(
        &TypedArray::Generic(StdArc::new(left)),
        &TypedArray::Generic(StdArc::new(right)),
        &mut errors,
        (0..1).into(),
    )
    .verified("the fixed float children have equal lengths and types");
    let TypedArray::Float64(dot) = dot else {
        panic!("F64 dot returns F64");
    };
    assert_eq!(
        dot.iter().collect::<Vec<_>>(),
        vec![Some(11.0), Some(11.0), None, None]
    );
    assert!(errors.row(0).is_empty());
    assert!(errors.row(1).is_empty());
    assert_eq!(errors.row(2)[0].code(), ErrorCode::InvalidArgument);
    assert!(errors.row(3).is_empty());
}

#[test]
fn numeric_lists_keep_empty_null_overflow_and_dimension_contracts() {
    let left = TypedArray::Generic(StdArc::new(
        ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(2), Some(3)]),
            Some(vec![]),
            Some(vec![Some(i64::MAX), Some(1)]),
            Some(vec![Some(1), Some(2)]),
            None,
        ]),
    ));
    let right = TypedArray::Generic(StdArc::new(
        ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(3), Some(4)]),
            Some(vec![]),
            Some(vec![Some(2), Some(2)]),
            Some(vec![Some(3)]),
            Some(vec![Some(1)]),
        ]),
    ));
    let span: Span = (0..1).into();
    let mut errors = RowErrors::new(5);
    let dot = execute_list_dot(&left, &right, &mut errors, span)
        .verified("both list columns have I64 elements");
    let TypedArray::Int64(dot) = dot else {
        panic!("I64 dot returns I64");
    };
    assert_eq!(
        dot.iter().collect::<Vec<_>>(),
        vec![Some(18), Some(0), None, None, None]
    );
    assert_eq!(errors.row(2)[0].code(), ErrorCode::Overflow);
    assert_eq!(
        errors.row(3)[0].reason,
        SideErrorReason::VectorLengthMismatch { left: 2, right: 1 }
    );
    assert!(errors.row(4).is_empty());

    let mut errors = RowErrors::new(5);
    let mean =
        execute_list_mean(&left, &mut errors, span).verified("I64 list elements are numeric");
    let TypedArray::Float64(mean) = mean else {
        panic!("mean returns F64");
    };
    assert_eq!(mean.value(0), 2.5);
    assert!(mean.is_null(1));
    assert!(mean.is_null(4));

    let mut errors = RowErrors::new(5);
    let distance = execute_list_distance(&left, &right, &mut errors, span)
        .verified("both list columns have I64 elements");
    let TypedArray::Float64(distance) = distance else {
        panic!("distance returns F64");
    };
    assert!((distance.value(0) - 2.0_f64.sqrt()).abs() < 1e-12);
    assert_eq!(distance.value(1), 0.0);
    assert!(distance.is_null(3));
    assert_eq!(errors.row(3)[0].code(), ErrorCode::InvalidArgument);
}

#[test]
fn non_finite_vector_results_report_only_the_affected_row() {
    let left = TypedArray::Generic(StdArc::new(ListArray::from_iter_primitive::<
        Float64Type,
        _,
        _,
    >([
        Some(vec![Some(f64::MAX), Some(f64::MAX)]),
        Some(vec![Some(1.0), Some(2.0)]),
    ])));
    let right = TypedArray::Generic(StdArc::new(ListArray::from_iter_primitive::<
        Float64Type,
        _,
        _,
    >([
        Some(vec![Some(-f64::MAX), Some(0.0)]),
        Some(vec![Some(3.0), Some(4.0)]),
    ])));
    let span: Span = (0..1).into();

    let mut mean_errors = RowErrors::new(2);
    let mean =
        execute_list_mean(&left, &mut mean_errors, span).verified("F64 vectors support mean");
    let TypedArray::Float64(mean) = mean else {
        panic!("mean yields F64");
    };
    assert!(mean.is_null(0));
    assert_eq!(mean.value(1), 1.5);
    assert_eq!(mean_errors.row(0)[0].code(), ErrorCode::InvalidArgument);
    assert!(mean_errors.row(1).is_empty());

    let mut dot_errors = RowErrors::new(2);
    let dot = execute_list_dot(&left, &right, &mut dot_errors, span)
        .verified("equal-length F64 vectors support dot");
    let TypedArray::Float64(dot) = dot else {
        panic!("F64 dot yields F64");
    };
    assert!(dot.is_null(0));
    assert_eq!(dot.value(1), 11.0);
    assert_eq!(dot_errors.row(0)[0].code(), ErrorCode::InvalidArgument);
    assert!(dot_errors.row(1).is_empty());

    let mut distance_errors = RowErrors::new(2);
    let distance = execute_list_distance(&left, &right, &mut distance_errors, span)
        .verified("equal-length F64 vectors support distance");
    let TypedArray::Float64(distance) = distance else {
        panic!("distance yields F64");
    };
    assert!(distance.is_null(0));
    assert_eq!(distance.value(1), 2.0_f64.sqrt() * 2.0);
    assert_eq!(distance_errors.row(0)[0].code(), ErrorCode::InvalidArgument);
    assert!(distance_errors.row(1).is_empty());
}
