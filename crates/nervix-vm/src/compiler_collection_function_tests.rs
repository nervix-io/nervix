//! Typed collection signatures at the VM compiler boundary.
//!
//! Layer: test harness.
//!
//! - **Owns.** Collection result shapes and exact element-type diagnostics.
//! - **Depends on.** The VM compiler and semantic program parser.
//! - **Must not know.** Runtime Arrow kernel implementation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use super::*;
use crate::test_support::parse_program;

fn source_schema() -> Arc<Schema> {
    let integer_item = Arc::new(Field::new("item", DataType::Int64, false));
    let float_item = Arc::new(Field::new("item", DataType::Float64, false));
    Arc::new(Schema::new(vec![
        Field::new("left", DataType::List(integer_item.clone()), true),
        Field::new("right", DataType::FixedSizeList(integer_item, 2), true),
        Field::new("other", DataType::List(float_item), true),
        Field::new("index", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn compile_assignment(
    expression: &str,
    result_type: DataType,
) -> Result<CompiledProgram, CompileError> {
    let program = parse_program(&format!("SET result = {expression}"))
        .verified("each case is syntactically valid NSPL");
    let input = source_schema();
    let mut fields = input
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("result", result_type, true));
    compile_program_for_bindings(
        &program,
        Arc::new(Schema::new(fields)),
        [CompileBinding::writable("input", input)],
    )
}

#[test]
fn collection_calls_have_exact_shapes_and_result_types() {
    let item = Arc::new(Field::new("item", DataType::Int64, false));
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
        assert_eq!(error.code, code, "{expression}: {error}");
    }
}
