//! List shape and nested-access production-owner regressions.
//!
//! Layer: test harness.
//!
//! - **Owns.** Shape, value, validity, sliced-buffer and allocation-identity assertions.
//! - **Depends on.** The production column kernels, compiler and Arrow arrays.
//! - **Must not know.** Cluster scheduling or connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the production list shape kernels")
)]

use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray, types::Int64Type};
use arrow_schema::Schema;

use super::*;
use crate::{
    CompileBinding, TypedBatch, compile_program_for_bindings,
    program::ListShapeFunctionName,
    runtime::{ListItem, execute_list_item, execute_program_sync},
    test_support::parse_program,
};

pub(super) fn fixed_type(element: DataType, width: i32) -> DataType {
    DataType::FixedSizeList(StdArc::new(Field::new("item", element, false)), width)
}

pub(super) fn fixed(values: ArrayRef, width: i32, nulls: Option<NullBuffer>) -> ArrayRef {
    StdArc::new(
        FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", values.data_type().clone(), false)),
            width,
            values,
            nulls,
        )
        .verified("the fixture provides complete fixed rows and masks null children"),
    )
}

pub(super) fn reshape(input: &TypedArray, dimensions: Vec<i64>) -> TypedArray {
    let dimensions = ArrayDimensions::try_from(dimensions)
        .verified("fixture dimensions are positive and have a bounded product");
    ListShapeFunction::Reshape(dimensions)
        .execute(input)
        .verified("the fixture's element count matches the requested shape")
}

pub(super) fn leaf(array: &ArrayRef) -> &ArrayRef {
    if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        return leaf(list.values());
    }
    array
}

/// An independent logical oracle: visit only valid containers, then compare every scalar's bits.
pub(super) fn logical_rows(array: &ArrayRef) -> Vec<Option<Vec<u64>>> {
    let mut rows = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            rows.push(None);
        } else {
            let mut bits = Vec::new();
            collect_bits(array.as_ref(), row, &mut bits);
            rows.push(Some(bits));
        }
    }
    rows
}

fn collect_bits(array: &dyn Array, row: usize, bits: &mut Vec<u64>) {
    assert!(
        array.is_valid(row),
        "every element of a valid list is required"
    );
    if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let value = list.value(row);
        for index in 0..value.len() {
            collect_bits(value.as_ref(), index, bits);
        }
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let value = list.value(row);
        for index in 0..value.len() {
            collect_bits(value.as_ref(), index, bits);
        }
    } else {
        let floats = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .verified("the logical oracle's leaves are F64");
        bits.push(floats.value(row).to_bits());
    }
}

fn as_generic(input: &TypedArray) -> &ArrayRef {
    let TypedArray::Generic(values) = input else {
        panic!("shape operations retain an Arrow list column");
    };
    values
}

#[test]
fn list_shape_reshape_and_fixed_flatten_reuse_sliced_values_and_nulls() {
    let source: ArrayRef = StdArc::new(Float64Array::from(vec![
        Some(-1.0),
        Some(-2.0),
        Some(-3.0),
        Some(-4.0),
        Some(-5.0),
        Some(-6.0),
        Some(1.0),
        Some(-0.0),
        Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        Some(4.0),
        Some(5.0),
        Some(6.0),
        None,
        None,
        None,
        None,
        None,
        None,
        Some(7.0),
        Some(8.0),
        Some(9.0),
        Some(10.0),
        Some(11.0),
        Some(12.0),
    ]));
    let root_nulls = NullBuffer::from(vec![true, true, false, true]);
    let inner = fixed(source, 3, Some(root_nulls.expand(2)));
    let matrix = fixed(inner, 2, Some(root_nulls)).slice(1, 3);
    let input = TypedArray::Generic(matrix.clone());
    let reshaped = reshape(&input, vec![1, 3, 2]);
    let flattened = ListShapeFunction::Flatten
        .execute(&input)
        .verified("two fixed levels flatten");
    for result in [&reshaped, &flattened] {
        let result = as_generic(result);
        assert_eq!(logical_rows(result), logical_rows(&matrix));
        let original = leaf(&matrix)
            .as_any()
            .downcast_ref::<Float64Array>()
            .verified("fixture leaves are F64");
        let changed = leaf(result)
            .as_any()
            .downcast_ref::<Float64Array>()
            .verified("shaping preserves F64 leaves");
        assert_eq!(original.values().as_ptr(), changed.values().as_ptr());
        assert_eq!(result.nulls(), matrix.nulls());
    }
    assert_eq!(
        reshaped.data_type(),
        fixed_type(fixed_type(fixed_type(DataType::Float64, 2), 3), 1)
    );
    assert_eq!(flattened.data_type(), fixed_type(DataType::Float64, 6));
    let back = reshape(&reshaped, vec![2, 3]);
    assert_eq!(logical_rows(as_generic(&back)), logical_rows(&matrix));
}

#[test]
fn list_shape_transpose_exchanges_only_the_outer_axes_in_sliced_rank_three_arrays() {
    let source: ArrayRef = StdArc::new(Float64Array::from_iter_values((0_i32..48).map(f64::from)));
    let third = fixed(source, 2, None);
    let second = fixed(third, 3, None);
    let original = fixed(
        second,
        2,
        Some(NullBuffer::from(vec![true, true, false, true])),
    )
    .slice(1, 3);
    let input = TypedArray::Generic(original.clone());
    let transposed = ListShapeFunction::Transpose
        .execute(&input)
        .verified("the fixture has three fixed axes");
    assert_eq!(
        transposed.data_type(),
        fixed_type(fixed_type(fixed_type(DataType::Float64, 2), 2), 3)
    );
    let expected_first = [
        12.0_f64, 13.0, 18.0, 19.0, 14.0, 15.0, 20.0, 21.0, 16.0, 17.0, 22.0, 23.0,
    ];
    let rows = logical_rows(as_generic(&transposed));
    assert_eq!(
        rows[0],
        Some(expected_first.into_iter().map(f64::to_bits).collect())
    );
    assert_eq!(rows[1], None);
    let back = ListShapeFunction::Transpose
        .execute(&transposed)
        .verified("transposing leaves two or more fixed axes");
    assert_eq!(back.data_type(), input.data_type());
    assert_eq!(logical_rows(as_generic(&back)), logical_rows(&original));
}

#[test]
fn list_shape_flatten_composes_variable_offsets_without_copying_values() {
    let values: ArrayRef = StdArc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    let inner = ListArray::try_new(
        StdArc::new(Field::new("item", DataType::Float64, false)),
        OffsetBuffer::from_lengths([1, 2, 0, 2]),
        values.clone(),
        None,
    )
    .verified("the inner offsets cover five values");
    let outer = ListArray::try_new(
        StdArc::new(Field::new("item", inner.data_type().clone(), false)),
        OffsetBuffer::from_lengths([1, 2, 0, 1]),
        StdArc::new(inner),
        Some(NullBuffer::from(vec![true, true, false, true])),
    )
    .verified("the outer offsets cover four inner lists")
    .slice(1, 3);
    let input = TypedArray::Generic(StdArc::new(outer));
    let flattened = ListShapeFunction::Flatten
        .execute(&input)
        .verified("two variable levels flatten");
    assert_eq!(
        logical_rows(as_generic(&flattened)),
        vec![
            Some(vec![2.0_f64.to_bits(), 3.0_f64.to_bits()]),
            None,
            Some(vec![4.0_f64.to_bits(), 5.0_f64.to_bits()]),
        ]
    );
    let list = as_generic(&flattened)
        .as_any()
        .downcast_ref::<ListArray>()
        .verified("variable flatten yields a VEC");
    assert!(StdArc::ptr_eq(list.values(), &values));
    assert_eq!(list.value_offsets(), &[1, 3, 3, 5]);
}

#[test]
fn list_shape_flatten_handles_both_mixed_fixed_and_variable_levels() {
    let scalar: ArrayRef = StdArc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
    let inner = fixed(scalar.clone(), 2, None);
    let outer = ListArray::try_new(
        StdArc::new(Field::new("item", inner.data_type().clone(), false)),
        OffsetBuffer::from_lengths([2, 0]),
        inner,
        None,
    )
    .verified("the VEC contains two fixed arrays then an empty list");
    let output = ListShapeFunction::Flatten
        .execute(&TypedArray::Generic(StdArc::new(outer)))
        .verified("VEC of ARRAY flattens");
    assert_eq!(
        logical_rows(as_generic(&output)),
        vec![
            Some(vec![
                1.0_f64.to_bits(),
                2.0_f64.to_bits(),
                3.0_f64.to_bits(),
                4.0_f64.to_bits()
            ]),
            Some(vec![]),
        ]
    );
    let inner = ListArray::try_new(
        StdArc::new(Field::new("item", DataType::Float64, false)),
        OffsetBuffer::from_lengths([1, 1, 0, 2]),
        scalar,
        None,
    )
    .verified("four variable rows cover the scalar child");
    let outer = fixed(StdArc::new(inner), 2, None);
    let output = ListShapeFunction::Flatten
        .execute(&TypedArray::Generic(outer))
        .verified("ARRAY of VEC flattens");
    assert_eq!(
        logical_rows(as_generic(&output)),
        vec![
            Some(vec![1.0_f64.to_bits(), 2.0_f64.to_bits()]),
            Some(vec![3.0_f64.to_bits(), 4.0_f64.to_bits()]),
        ]
    );
}

#[test]
fn list_shape_variable_flatten_omits_null_children_hidden_by_null_containers() {
    let values: ArrayRef = StdArc::new(Float64Array::from(vec![Some(1.0), Some(2.0), None, None]));
    let inner = fixed(values, 2, Some(NullBuffer::from(vec![true, false])));
    let outer = ListArray::try_new(
        StdArc::new(Field::new("item", inner.data_type().clone(), true)),
        OffsetBuffer::from_lengths([1, 1]),
        inner,
        Some(NullBuffer::from(vec![true, false])),
    )
    .verified("the nullable Arrow child is hidden by the null outer container");
    let output = ListShapeFunction::Flatten
        .execute(&TypedArray::Generic(StdArc::new(outer)))
        .verified("null containers do not expose physical null children");
    let list = as_generic(&output)
        .as_any()
        .downcast_ref::<ListArray>()
        .verified("a variable level yields a VEC");
    assert_eq!(list.values().len(), 2);
    assert_eq!(list.values().null_count(), 0);
    assert_eq!(
        logical_rows(as_generic(&output)),
        vec![Some(vec![1.0_f64.to_bits(), 2.0_f64.to_bits()]), None,]
    );
}

#[test]
fn list_shape_flatten_removes_only_one_level_and_handles_text() {
    let text: ArrayRef = StdArc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let deep = fixed(text.clone(), 2, None);
    let inner = fixed(deep, 1, None);
    let outer = TypedArray::Generic(fixed(inner, 2, None));
    let output = ListShapeFunction::Flatten
        .execute(&outer)
        .verified("fixed text arrays flatten one level");
    assert_eq!(
        output.data_type(),
        fixed_type(fixed_type(DataType::Utf8, 2), 2)
    );
    assert!(StdArc::ptr_eq(leaf(as_generic(&output)), &text));
}

#[test]
fn list_shape_nested_access_selects_fixed_and_variable_elements() {
    let inner = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        Some(vec![]),
        Some(vec![Some(3)]),
    ]);
    let variable = ListArray::try_new(
        StdArc::new(Field::new("item", inner.data_type().clone(), false)),
        OffsetBuffer::from_lengths([2, 0, 0, 1]),
        StdArc::new(inner),
        Some(NullBuffer::from(vec![true, false, true, true])),
    )
    .verified("nested VEC fixture has a null and an empty outer list");
    let input = TypedArray::Generic(StdArc::new(variable));
    for item in [ListItem::First, ListItem::Last] {
        let result = execute_list_item(&input, item).verified("nested VEC access is supported");
        let list = as_generic(&result)
            .as_any()
            .downcast_ref::<ListArray>()
            .verified("access returns an optional inner VEC");
        assert!(list.is_null(1));
        assert!(list.is_null(2));
        assert_eq!(
            list.value(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .verified("the nested scalar type stays I64")
                .value(0),
            3
        );
    }
    let child: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]));
    let inner = fixed(child, 2, None);
    let outer = TypedArray::Generic(fixed(inner, 2, None));
    let indices = Int64Array::from(vec![Some(1), None]);
    let result = execute_list_item(
        &outer,
        ListItem::Nth(crate::count::CountOperand::Int64(
            crate::operand::Operand::Column(&indices),
        )),
    )
    .verified("nth selects optional fixed elements");
    let list = as_generic(&result)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .verified("access returns an optional inner ARRAY");
    assert_eq!(
        list.value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .verified("the scalar type stays I64")
            .values()
            .as_ref(),
        &[3, 4]
    );
    assert!(list.is_null(1));
}

#[test]
fn list_shape_calls_execute_under_scalar_conditional_arms() {
    let values: ArrayRef = StdArc::new(Float64Array::from_iter_values((0_i32..18).map(f64::from)));
    let matrix = fixed(
        fixed(values, 3, None),
        2,
        Some(NullBuffer::from(vec![true, true, false])),
    );
    let input_schema = StdArc::new(Schema::new(vec![
        Field::new("matrix", matrix.data_type().clone(), true),
        Field::new("choose", DataType::Boolean, false),
    ]));
    let output_schema = StdArc::new(Schema::new(vec![
        input_schema.field(0).clone(),
        input_schema.field(1).clone(),
        Field::new("result", DataType::Int64, true),
    ]));
    let program = parse_program(
        "SET result = IF input.choose THEN count(list_flatten(input.matrix)) ELSE \
         count(first(list_transpose(input.matrix))) END",
    )
    .verified("the program uses supported shape calls");
    let compiled = compile_program_for_bindings(
        &program,
        output_schema,
        [CompileBinding::writable("input", input_schema.clone())],
    )
    .verified("both conditional arms return I64");
    let input = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Generic(matrix),
            TypedArray::Boolean(BooleanArray::from(vec![true, false, false])),
        ],
    )
    .verified("the input columns have three rows");
    let output = execute_program_sync(&compiled, &input).verified("pure shape calls execute");
    assert!(output.errors().is_error_free());
    let TypedArray::Int64(result) = &output.columns()[2] else {
        panic!("result is I64");
    };
    assert_eq!(result.value(0), 6);
    assert_eq!(result.value(1), 2);
    assert!(result.is_null(2));
}

#[test]
fn list_shape_names_join_the_ordinary_completion_catalog() {
    let names = crate::program::FunctionName::ordinary_completion_names();
    for name in ["list_reshape", "list_flatten", "list_transpose"] {
        assert!(names.binary_search(&name.to_string()).is_ok());
        assert_eq!(
            name.to_ascii_uppercase()
                .parse::<ListShapeFunctionName>()
                .verified("shape names are case insensitive")
                .to_string(),
            name
        );
    }
}
