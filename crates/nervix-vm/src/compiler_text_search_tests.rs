//! Typed text-search signatures and constant-set lowering.
//!
//! Layer: test harness.
//!
//! - **Owns.** Positive and negative compilation cases for public string-search calls.
//! - **Depends on.** The VM compiler and semantic NSPL expression parser.
//! - **Must not know.** Runtime Arrow buffer layout.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use super::*;
use crate::{test_support::parse_program, text_search::ContainsAnyCall};

fn compile_assignment(
    expression: &str,
    result_type: DataType,
) -> Result<CompiledProgram, CompileError> {
    let input = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("pattern", DataType::Utf8, true),
        Field::new("number", DataType::Int64, true),
    ]));
    let program = parse_program(&format!("SET result = {expression}"))
        .verified("each case has valid expression syntax");
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
fn text_search_calls_have_exact_result_types() {
    let strings = DataType::List(Field::new("item", DataType::Utf8, false).into());
    for (expression, output) in [
        ("octet_length(input.text)", DataType::Int64),
        ("concat_ws('-', input.text, input.pattern)", DataType::Utf8),
        ("split(input.text, ',')", strings),
        ("join(vec(input.text), ',')", DataType::Utf8),
        ("like(input.text, input.pattern)", DataType::Boolean),
        ("ilike(input.text, input.pattern)", DataType::Boolean),
        ("contains_any(input.text, vec('a', 'b'))", DataType::Boolean),
        (
            "regexp_extract(input.text, '(a)', input.number)",
            DataType::Utf8,
        ),
        ("normalize_nfc(input.text)", DataType::Utf8),
    ] {
        let compiled = compile_assignment(expression, output);
        assert!(compiled.is_ok(), "{expression}: {compiled:?}");
    }
}

#[test]
fn text_search_rejects_wrong_types_and_arity() {
    for expression in [
        "octet_length(input.number)",
        "concat_ws('-', input.text, input.number)",
        "join(input.text, '-')",
        "like(input.text, input.number)",
        "contains_any(input.text, vec(input.number))",
        "regexp_extract(input.text, '(a)', input.text)",
        "normalize_nfc(input.number)",
    ] {
        let error = compile_assignment(expression, DataType::Utf8)
            .err()
            .verified("wrong argument contracts must be rejected");
        assert_ne!(error.code, "unknown_function", "{expression}");
    }
}

#[test]
fn literal_multi_pattern_sets_are_prepared_once() {
    let constant = compile_assignment(
        "contains_any(input.text, vec('alpha', 'beta'))",
        DataType::Boolean,
    )
    .verified("literal pattern set has a valid signature");
    let dynamic = compile_assignment(
        "contains_any(input.text, vec(input.pattern, 'beta'))",
        DataType::Boolean,
    )
    .verified("dynamic pattern set has a valid signature");
    let constant_call = constant
        .instructions
        .iter()
        .find_map(|instruction| {
            if let InstructionKind::Builtin {
                lowering: BuiltinLowering::ContainsAny(call),
                inputs,
                ..
            } = &instruction.kind
            {
                Some((call, inputs.len()))
            } else {
                None
            }
        })
        .verified("the constant call lowers to a builtin");
    assert!(matches!(
        constant_call.0,
        ContainsAnyCall::Constant {
            matcher: Some(_),
            ..
        }
    ));
    assert_eq!(constant_call.1, 1);
    let dynamic_call = dynamic
        .instructions
        .iter()
        .find_map(|instruction| {
            if let InstructionKind::Builtin {
                lowering: BuiltinLowering::ContainsAny(call),
                inputs,
                ..
            } = &instruction.kind
            {
                Some((call, inputs.len()))
            } else {
                None
            }
        })
        .verified("the dynamic call lowers to a builtin");
    assert!(matches!(dynamic_call.0, ContainsAnyCall::Dynamic));
    assert_eq!(dynamic_call.1, 2);
}
