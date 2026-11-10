//! Typed collection signatures at the VM compiler boundary.
//!
//! Layer: test harness.
//!
//! - **Owns.** Collection result shapes and exact element-type diagnostics.
//! - **Depends on.** The VM compiler and semantic program parser.
//! - **Must not know.** Runtime Arrow kernel implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use arrow_schema::{DataType, Field, Schema};
use nervix_primitives::sync::StdArc;

use super::*;
use crate::test_support::parse_program;

fn source_schema() -> StdArc<Schema> {
    let integer_item = StdArc::new(Field::new("item", DataType::Int64, false));
    let float_item = StdArc::new(Field::new("item", DataType::Float64, false));
    StdArc::new(Schema::new(vec![
        Field::new("left", DataType::List(integer_item.clone()), true),
        Field::new("right", DataType::FixedSizeList(integer_item, 2), true),
        Field::new("other", DataType::List(float_item), true),
        Field::new("index", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "matrix",
            DataType::FixedSizeList(
                StdArc::new(Field::new(
                    "item",
                    DataType::FixedSizeList(
                        StdArc::new(Field::new("item", DataType::Int64, false)),
                        3,
                    ),
                    false,
                )),
                2,
            ),
            true,
        ),
    ]))
}

#[test]
fn list_shape_calls_resolve_dimensions_and_nested_access_when_compiled() {
    let item = StdArc::new(Field::new("item", DataType::Int64, false));
    let row = DataType::FixedSizeList(item.clone(), 3);
    let transposed = DataType::FixedSizeList(
        StdArc::new(Field::new(
            "item",
            DataType::FixedSizeList(item.clone(), 2),
            false,
        )),
        3,
    );
    for (expression, result_type) in [
        (
            "list_reshape(input.matrix, 6)",
            DataType::FixedSizeList(item.clone(), 6),
        ),
        (
            "list_flatten(input.matrix)",
            DataType::FixedSizeList(item, 6),
        ),
        ("list_transpose(input.matrix)", transposed),
        ("first(input.matrix)", row.clone()),
        ("last(input.matrix)", row.clone()),
        ("nth(input.matrix, 1)", row),
    ] {
        let result = compile_assignment(expression, result_type);
        assert!(result.is_ok(), "{expression}: {result:?}");
    }
}

#[test]
fn list_shape_calls_refuse_invalid_input_shapes_when_compiled() {
    for expression in [
        "list_reshape(input.matrix, 5)",
        "list_reshape(input.left, 2)",
        "list_transpose(input.left)",
        "list_transpose(input.right)",
        "list_flatten(input.right)",
        "list_flatten(input.index)",
        "list_flatten()",
        "list_flatten(input.matrix, input.matrix)",
        "list_transpose(input.matrix, input.matrix)",
    ] {
        let error = compile_assignment(expression, DataType::Int64)
            .expect_err("invalid list shapes cannot become an executable program");
        assert_eq!(
            error.current_context().code(),
            "unsupported_function",
            "{expression}: {error:#}"
        );
    }
}

#[test]
fn list_shape_dimensions_must_be_positive_integer_literals() {
    for expression in [
        "list_reshape()",
        "list_reshape(input.matrix)",
        "list_reshape(input.matrix, 0)",
        "list_reshape(input.matrix, -1)",
        "list_reshape(input.matrix, 2147483648)",
        "list_reshape(input.matrix, 2.0)",
        "list_reshape(input.matrix, input.index)",
        "list_reshape(input.matrix, 2 + 1)",
        "list_reshape(input.matrix, 2147483647, 2147483647, 2147483647)",
    ] {
        let error = parse_program(&format!("SET result = {expression}"))
            .expect_err("dimensions are resolved before an expression reaches type inference");
        assert!(error.contains("list_reshape"), "{expression}: {error}");
    }
}

fn compile_assignment(
    expression: &str,
    result_type: DataType,
) -> error_stack::Result<CompiledProgram, CompileError> {
    let program = parse_program(&format!("SET result = {expression}"))
        .verified("each case is syntactically valid Plexus");
    let input = source_schema();
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
fn collection_calls_have_exact_shapes_and_result_types() {
    let item = StdArc::new(Field::new("item", DataType::Int64, false));
    let fixed_two = DataType::FixedSizeList(item.clone(), 2);
    let fixed_four = DataType::FixedSizeList(item.clone(), 4);
    let vector = DataType::List(item);
    for (expression, result_type) in [
        ("[input.index, input.index]", fixed_two.clone()),
        ("array(input.index, input.index)", fixed_two.clone()),
        ("vec(input.index)", vector.clone()),
        ("vec()", vector.clone()),
        ("concat(input.left, input.right)", vector.clone()),
        ("concat(input.right, input.right)", fixed_four),
        ("slice(input.right, 0, 1)", vector),
        ("contains(input.left, input.index)", DataType::Boolean),
        ("overlap(input.left, input.right)", DataType::Boolean),
        ("min(input.left)", DataType::Int64),
        ("max(input.right)", DataType::Int64),
        ("mean(input.left)", DataType::Float64),
        ("dot(input.left, input.right)", DataType::Int64),
        ("distance(input.left, input.right)", DataType::Float64),
    ] {
        let result = compile_assignment(expression, result_type);
        assert!(result.is_ok(), "{expression}: {result:?}");
    }
}

#[test]
fn collection_calls_reject_implicit_element_casts_and_invalid_indices() {
    let rejected = [
        ("[input.index, input.name]", "type_mismatch"),
        ("vec(input.index, input.name)", "type_mismatch"),
        ("contains(input.left, input.name)", "type_mismatch"),
        ("overlap(input.left, input.other)", "type_mismatch"),
        ("dot(input.left, input.other)", "type_mismatch"),
        ("slice(input.left, input.name, 1)", "unsupported_function"),
        ("mean(input.name)", "unsupported_function"),
        ("vec()", "type_mismatch"),
    ];
    for (expression, code) in rejected {
        let error = compile_assignment(expression, DataType::Int64)
            .expect_err("collection operands outside their signatures must be refused");
        assert_eq!(
            error.current_context().code(),
            code,
            "{expression}: {error}"
        );
    }
}
