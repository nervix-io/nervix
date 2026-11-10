//! Deterministic checks of list shape, validity and scalar arithmetic equivalence.
//!
//! Layer: test harness.
//!
//! - **Owns.** Sliced and nested child-buffer results and message-local failure assertions.
//! - **Depends on.** The production list arithmetic owner and Arrow constructors.
//! - **Must not know.** Server graph or transport state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests list arithmetic owners")
)]

use super::*;

fn vector(values: Vec<i64>, lengths: &[usize], nulls: Option<NullBuffer>) -> TypedArray {
    let field = StdArc::new(Field::new("item", DataType::Int64, false));
    let array = ListArray::new(
        field,
        OffsetBuffer::from_lengths(lengths.iter().copied()),
        StdArc::new(Int64Array::from(values)),
        nulls,
    );
    TypedArray::Generic(StdArc::new(array))
}

fn fixed(values: Vec<i64>, width: i32, nulls: Option<NullBuffer>) -> TypedArray {
    let field = StdArc::new(Field::new("item", DataType::Int64, false));
    TypedArray::Generic(StdArc::new(FixedSizeListArray::new(
        field,
        width,
        StdArc::new(Int64Array::from(values)),
        nulls,
    )))
}

fn child(input: &TypedArray) -> &Int64Array {
    let TypedArray::Generic(array) = input else {
        panic!("arithmetic returns a list");
    };
    let values = match array.data_type() {
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .verified("list backing")
            .values(),
        DataType::FixedSizeList(_, _) => array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .verified("fixed-list backing")
            .values(),
        _ => panic!("arithmetic returns a list"),
    };
    values
        .as_any()
        .downcast_ref::<Int64Array>()
        .verified("the fixture has I64 leaves")
}

#[test]
fn fixed_list_arithmetic_keeps_one_error_per_message_and_masks_null_parents() {
    let left = fixed(
        vec![6, -4, i64::MAX, i64::MAX, i64::MAX, i64::MAX, 8, 4],
        2,
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let right = fixed(vec![2, -2, 1, 2, 1, 2, 2, 2], 2, None);
    let mut errors = RowErrors::new(4);
    let result = pair(Arithmetic::Add, &left, &right, &mut errors, (0..1).into())
        .verified("fixed list addition executes");
    assert_eq!(&child(&result).values()[..2], &[8, -6]);
    assert_eq!(&child(&result).values()[6..], &[10, 6]);
    assert_eq!(errors.row(1).len(), 1);
    assert_eq!(
        errors.row(1)[0].reason,
        SideErrorReason::IntegerOverflow(IntegerOperation::Addition)
    );
    assert!(errors.row(0).is_empty());
    assert!(errors.row(2).is_empty());
    assert!(errors.row(3).is_empty());
    assert!(result.as_array().is_null(1));
    assert!(result.as_array().is_null(2));
    assert_eq!(child(&result).null_count(), 0);
}

#[test]
fn ragged_list_arithmetic_reuses_offsets_and_isolates_lengths_and_division_failures() {
    let left = vector(vec![6, -4, 1, 2, 6, 8, 9, 12], &[2, 0, 2, 2, 2], None);
    let right = vector(vec![2, -2, 1, 0, 0, 3, 3], &[2, 0, 1, 2, 2], None);
    let mut errors = RowErrors::new(5);
    let result = pair(Arithmetic::Div, &left, &right, &mut errors, (0..1).into())
        .verified("ragged list division executes");
    assert_eq!(&child(&result).values()[..2], &[3, 2]);
    assert_eq!(&child(&result).values()[6..], &[3, 4]);
    assert_eq!(errors.row(2).len(), 1);
    assert_eq!(
        errors.row(2)[0].reason,
        SideErrorReason::VectorLengthMismatch { left: 2, right: 1 }
    );
    assert_eq!(errors.row(3).len(), 1);
    assert_eq!(
        errors.row(3)[0].reason,
        SideErrorReason::DivisionByZero(crate::DivisionOperation::Division)
    );
    assert!(errors.row(1).is_empty());
    let TypedArray::Generic(original) = &left else {
        panic!("list fixture");
    };
    let TypedArray::Generic(result) = result else {
        panic!("list result");
    };
    let original = original
        .as_any()
        .downcast_ref::<ListArray>()
        .verified("list fixture");
    let result = result
        .as_any()
        .downcast_ref::<ListArray>()
        .verified("list result");
    assert_eq!(result.offsets().as_ptr(), original.offsets().as_ptr());
    assert_eq!(result.values().null_count(), 0);
}

#[test]
fn sliced_lists_and_scalar_columns_keep_their_active_spans() {
    let left = vector(vec![i64::MIN, 6, -4, 8, 4, i64::MAX], &[1, 2, 2, 1], None);
    let TypedArray::Generic(array) = left else {
        panic!("list fixture");
    };
    let left = TypedArray::Generic(array.slice(1, 2));
    let scalars = Int64Array::from(vec![Some(2), None]);
    let mut errors = RowErrors::new(2);
    let result = scalar(
        Arithmetic::Mul,
        &left,
        Operand::Column(&scalars),
        &mut errors,
        (0..1).into(),
    )
    .verified("a scalar column scales the sliced rows");
    assert!(errors.is_error_free());
    assert_eq!(&child(&result).values()[..2], &[12, -8]);
    assert_eq!(child(&result).len(), 4);
    assert!(result.as_array().is_null(1));
    let TypedArray::Generic(result) = result else {
        panic!("list result");
    };
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<ListArray>()
            .verified("list result")
            .value_offsets(),
        &[0, 2, 4]
    );
}

#[test]
fn nested_fixed_arrays_keep_their_shape_and_use_the_exact_numeric_scalar() {
    let inner_field = StdArc::new(Field::new("item", DataType::Int64, false));
    let inner = FixedSizeListArray::new(
        inner_field,
        2,
        StdArc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
        None,
    );
    let outer_field = StdArc::new(Field::new("item", inner.data_type().clone(), false));
    let outer = FixedSizeListArray::new(
        outer_field,
        2,
        StdArc::new(inner),
        Some(NullBuffer::from(vec![true, false])),
    );
    let input = TypedArray::Generic(StdArc::new(outer));
    let scalar_value = Int64Array::from(vec![2]);
    let mut errors = RowErrors::new(2);
    let result = scalar(
        Arithmetic::Mul,
        &input,
        Operand::Scalar(&scalar_value),
        &mut errors,
        (0..1).into(),
    )
    .verified("a numeric scalar scales every fixed dimension");
    assert_eq!(result.data_type(), input.data_type());
    assert!(errors.is_error_free());
    let shape = Shape::new(&result).verified("the output remains a nested list");
    assert_eq!(
        shape.numbers::<Int64Type>().values().as_ref(),
        &[2, 4, 6, 8, 10, 12, 14, 16]
    );
    assert!(result.as_array().is_null(1));
}

#[test]
fn absolute_value_and_non_finite_results_follow_scalar_failures() {
    let input = fixed(vec![i64::MIN, i64::MIN, -3, 4], 2, None);
    let mut errors = RowErrors::new(2);
    let result =
        absolute(&input, &mut errors, (0..1).into()).verified("integer absolute value executes");
    assert_eq!(errors.row(0).len(), 1);
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::IntegerOverflow(IntegerOperation::AbsoluteValue)
    );
    assert_eq!(&child(&result).values()[2..], &[3, 4]);
    let field = StdArc::new(Field::new("item", DataType::Float64, false));
    let input = TypedArray::Generic(StdArc::new(FixedSizeListArray::new(
        field,
        2,
        StdArc::new(Float64Array::from(vec![f64::MAX, f64::NAN, -0.0, -2.0])),
        None,
    )));
    let scale = Float64Array::from(vec![2.0]);
    let mut errors = RowErrors::new(2);
    let result = scalar(
        Arithmetic::Mul,
        &input,
        Operand::Scalar(&scale),
        &mut errors,
        (0..1).into(),
    )
    .verified("float scaling executes");
    assert_eq!(errors.row(0).len(), 1);
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::NonFiniteResult(FloatOperation::Arithmetic)
    );
    assert!(errors.row(1).is_empty());
    let shape = Shape::new(&result).verified("float scaling preserves lists");
    assert_eq!(
        shape.numbers::<Float64Type>().value(2).to_bits(),
        (-0.0_f64).to_bits()
    );
}

#[test]
fn list_arithmetic_signatures_preserve_types_and_refuse_implicit_changes() {
    use crate::{builtin_signature, program::FunctionName};
    for numeric in [
        DataType::UInt8,
        DataType::Int8,
        DataType::UInt16,
        DataType::Int16,
        DataType::UInt32,
        DataType::Int32,
        DataType::UInt64,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ] {
        let field = StdArc::new(Field::new("item", numeric.clone(), false));
        let array = DataType::FixedSizeList(field.clone(), 2);
        let vector = DataType::List(field);
        for function in [
            FunctionName::ListAdd,
            FunctionName::ListSub,
            FunctionName::ListMul,
            FunctionName::ListDiv,
        ] {
            assert_eq!(
                builtin_signature(&function, &[array.clone(), array.clone()], 0..1)
                    .verified("matching fixed numeric arrays are valid"),
                array
            );
            assert_eq!(
                builtin_signature(&function, &[array.clone(), vector.clone()], 0..1)
                    .verified("a vector operand gives a vector result"),
                vector
            );
            assert_eq!(
                builtin_signature(&function, &[vector.clone(), array.clone()], 0..1)
                    .verified("mixed containers preserve the element type"),
                vector
            );
        }
        for function in [
            FunctionName::ListAddScalar,
            FunctionName::ListSubScalar,
            FunctionName::ListMulScalar,
            FunctionName::ListDivScalar,
        ] {
            assert_eq!(
                builtin_signature(&function, &[array.clone(), numeric.clone()], 0..1)
                    .verified("an exact numeric scalar is valid"),
                array
            );
            assert_eq!(
                builtin_signature(&function, &[vector.clone(), numeric.clone()], 0..1)
                    .verified("an exact numeric scalar is valid"),
                vector
            );
        }
        assert_eq!(
            builtin_signature(&FunctionName::ListAbs, std::slice::from_ref(&array), 0..1)
                .verified("numeric absolute value is valid"),
            array
        );
    }
    let integer = StdArc::new(Field::new("item", DataType::Int64, false));
    let fixed = DataType::FixedSizeList(integer.clone(), 2);
    let vector = DataType::List(integer.clone());
    let wider = DataType::FixedSizeList(integer, 3);
    let floats = DataType::List(StdArc::new(Field::new("item", DataType::Float64, false)));
    let nested = DataType::List(StdArc::new(Field::new("item", vector.clone(), false)));
    for arguments in [
        vec![fixed, wider],
        vec![vector.clone(), floats],
        vec![vector, DataType::Int64],
        vec![nested.clone(), nested],
        vec![],
    ] {
        assert!(
            builtin_signature(&FunctionName::ListAdd, &arguments, 0..1).is_err(),
            "{arguments:?}"
        );
    }
    let nullable = DataType::List(StdArc::new(Field::new("item", DataType::Int64, true)));
    let required = DataType::List(StdArc::new(Field::new("item", DataType::Int64, false)));
    let nullable_inner = DataType::List(StdArc::new(Field::new(
        "item",
        DataType::FixedSizeList(StdArc::new(Field::new("item", DataType::Int64, true)), 2),
        false,
    )));
    for (function, arguments) in [
        (FunctionName::ListAbs, vec![nullable.clone()]),
        (FunctionName::ListAdd, vec![required, nullable]),
        (
            FunctionName::ListAddScalar,
            vec![nullable_inner, DataType::Int64],
        ),
    ] {
        assert!(
            builtin_signature(&function, &arguments, 0..1).is_err(),
            "{function:?}: {arguments:?}"
        );
    }
}

#[test]
fn list_arithmetic_preserves_empty_vectors_with_wide_fixed_shapes() {
    let mut element = DataType::Int64;
    for _ in 0..3 {
        element =
            DataType::FixedSizeList(StdArc::new(Field::new("item", element, false)), i32::MAX);
    }
    let field = StdArc::new(Field::new("item", element.clone(), false));
    let input = TypedArray::Generic(StdArc::new(ListArray::new(
        field,
        OffsetBuffer::from_lengths([0]),
        new_empty_array(&element),
        None,
    )));
    let mut errors = RowErrors::new(1);
    let result = absolute(&input, &mut errors, (0..1).into())
        .verified("an empty VEC evaluates no numeric element, regardless of its fixed shape");
    assert_eq!(result.data_type(), input.data_type());
    assert!(errors.is_error_free());
    let shape = Shape::new(&result).verified("the result remains a list");
    assert_eq!(shape.list.len(), 1);
    assert_eq!(shape.list.value_range(0).len(), 0);
    assert_eq!(shape.leaf.len(), 0);
}

#[test]
fn list_arithmetic_vector_offsets_refuse_lengths_the_container_cannot_represent() {
    let maximum = usize::try_from(i32::MAX).assured("the Arrow offset limit fits usize");
    let valid = Shape::fixed_offsets(maximum, 1).verified("the largest Arrow offset is valid");
    assert_eq!(valid.as_ref(), &[0, i32::MAX]);
    let Err(error) = Shape::fixed_offsets(maximum, 2) else {
        panic!("two such rows exceed a VEC's offsets");
    };
    assert!(matches!(
        error.current_context(),
        RuntimeError::CollectionTooLarge {
            limit: CollectionLimit::VectorOffsets,
            ..
        }
    ));
    let Err(error) = Shape::fixed_offsets(usize::MAX, 2) else {
        panic!("the item count must fit addressable memory");
    };
    assert!(matches!(
        error.current_context(),
        RuntimeError::CollectionTooLarge {
            limit: CollectionLimit::AddressableLength,
            ..
        }
    ));
}

#[test]
fn compiled_list_arithmetic_dispatches_each_explicit_operation() {
    use arrow_schema::Schema;

    use crate::{
        CompileBinding, TypedBatch, compile_program_for_bindings, test_support::parse_program,
    };

    let items = fixed(vec![-8, 6], 2, None);
    let data_type = items.data_type();
    let source = StdArc::new(Schema::new(vec![
        Field::new("items", data_type.clone(), false),
        Field::new("other", data_type.clone(), false),
        Field::new("scale", DataType::Int64, false),
    ]));
    let cases = [
        ("added", "list_add(input.items, input.other)", [-6, 3]),
        ("subtracted", "list_sub(input.items, input.other)", [-10, 9]),
        (
            "multiplied",
            "list_mul(input.items, input.other)",
            [-16, -18],
        ),
        ("divided", "list_div(input.items, input.other)", [-4, -2]),
        (
            "shifted",
            "list_add_scalar(input.items, input.scale)",
            [-6, 8],
        ),
        (
            "adjusted",
            "list_sub_scalar(input.items, input.scale)",
            [-10, 4],
        ),
        (
            "scaled",
            "list_mul_scalar(input.items, input.scale)",
            [-16, 12],
        ),
        (
            "reduced",
            "list_div_scalar(input.items, input.scale)",
            [-4, 3],
        ),
        ("magnitude", "list_abs(input.items)", [8, 6]),
    ];
    let assignments = cases
        .iter()
        .map(|(name, expression, _)| format!("{name} = {expression}"))
        .collect::<Vec<_>>()
        .join(", ");
    let program = parse_program(&format!("SET {assignments}"))
        .verified("all explicit names are valid Plexus calls");
    let output = StdArc::new(Schema::new(
        cases
            .iter()
            .map(|(name, _, _)| Field::new(*name, data_type.clone(), false))
            .collect::<Vec<_>>(),
    ));
    let compiled = compile_program_for_bindings(
        &program,
        output,
        [CompileBinding::writable("input", source.clone())],
    )
    .verified("each explicit operation accepts the exact types");
    let batch = TypedBatch::try_new(
        source,
        vec![
            items,
            fixed(vec![2, -3], 2, None),
            TypedArray::Int64(Int64Array::from(vec![2])),
        ],
    )
    .verified("the single-row fixture matches its schema");
    let result = super::super::execute_program_sync(&compiled, &batch)
        .verified("compiled list arithmetic executes");
    assert!(result.errors().row(0).is_empty());
    for (index, (name, _, expected)) in cases.iter().enumerate() {
        assert_eq!(
            child(result.column(index)).values().as_ref(),
            expected,
            "{name}"
        );
    }
}

#[test]
fn list_arithmetic_in_a_case_arm_does_not_report_unselected_failures() {
    use arrow_schema::Schema;

    use crate::{
        CompileBinding, TypedBatch, compile_program_for_bindings, test_support::parse_program,
    };
    let source = StdArc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let output = StdArc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Int64,
        true,
    )]));
    let program = parse_program(
        "SET result = CASE WHEN input.value > 0 THEN first(list_div_scalar(vec(input.value, \
         input.value), 0)) ELSE input.value END",
    )
    .verified("the conditional is valid Plexus");
    let compiled = compile_program_for_bindings(
        &program,
        output,
        [CompileBinding::writable("input", source.clone())],
    )
    .verified("the explicit list call has an exact numeric signature");
    let batch = TypedBatch::try_new(
        source,
        vec![TypedArray::Int64(Int64Array::from(vec![-2, 3, -4]))],
    )
    .verified("the fixture matches its schema");
    let result =
        super::super::execute_program_sync(&compiled, &batch).verified("the conditional executes");
    assert!(result.errors().row(0).is_empty());
    assert_eq!(result.errors().row(1).len(), 1);
    assert!(result.errors().row(2).is_empty());
}
