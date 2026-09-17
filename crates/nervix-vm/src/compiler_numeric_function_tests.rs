//! Compiler tests for the numeric classification, math and bit builtins.
//!
//! Layer: test harness.
//!
//! - **Owns.** The accepted argument types and result widths of `sign`, `trunc`, `round` with a
//!   count of digits, `sin`, `atan2`, `log2`, `radians`, `degrees`, `is_nan`, `is_finite`,
//!   `is_infinite`, the bitwise functions, the shifts and `bit_count`, the diagnostics outside those
//!   signatures, and which of their calls over literals fold.
//! - **Depends on.** The VM compiler and its constant folding.
//! - **Must not know.** How execution evaluates a call.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use super::*;
use crate::test_support::parse_program;

fn operand_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("u8", DataType::UInt8, true),
        Field::new("i8", DataType::Int8, true),
        Field::new("u16", DataType::UInt16, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("u64", DataType::UInt64, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn compile_assignment(
    expression: &str,
    output_type: DataType,
) -> Result<CompiledProgram, CompileError> {
    let program = parse_program(&format!("SET out = {expression}")).expect("must parse");
    let input = operand_schema();
    let mut fields = input
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("out", output_type, true));
    compile_program_for_bindings(
        &program,
        Arc::new(Schema::new(fields)),
        [CompileBinding::writable("input", input)],
    )
}

#[test]
fn every_new_builtin_has_its_declared_result_width() {
    let signatures = [
        ("sign(input.u8)", DataType::UInt8),
        ("sign(input.i64)", DataType::Int64),
        ("sign(input.f32)", DataType::Float32),
        ("trunc(input.i16)", DataType::Int16),
        ("trunc(input.f32)", DataType::Float32),
        ("round(input.f32, input.u8)", DataType::Float32),
        ("round(input.i32, -2)", DataType::Int32),
        ("round(input.f64, input.u64)", DataType::Float64),
        ("sin(input.i16)", DataType::Float64),
        ("atan2(input.i32, input.f32)", DataType::Float64),
        ("log2(input.u64)", DataType::Float64),
        ("radians(input.f32)", DataType::Float64),
        ("degrees(input.i64)", DataType::Float64),
        ("is_nan(input.f32)", DataType::Boolean),
        ("is_finite(input.f64)", DataType::Boolean),
        ("is_infinite(input.f64)", DataType::Boolean),
        ("bitwise_and(input.u16, input.u16)", DataType::UInt16),
        ("bitwise_or(input.i8, input.i8)", DataType::Int8),
        ("bitwise_xor(input.i64, 3)", DataType::Int64),
        ("bitwise_not(input.u64)", DataType::UInt64),
        ("bit_count(input.u16)", DataType::Int64),
        ("shift_left(input.u16, input.i8)", DataType::UInt16),
        ("shift_right(input.i64, input.u64)", DataType::Int64),
        ("BITWISE_AND(input.i32, input.i32)", DataType::Int32),
    ];
    for (expression, output_type) in signatures {
        let compiled = compile_assignment(expression, output_type.clone());
        assert!(
            compiled.is_ok(),
            "`{expression}` must compile to {output_type:?}: {compiled:?}"
        );
    }
}

#[test]
fn calls_outside_a_signature_are_rejected_when_the_statement_is_applied() {
    let rejected = [
        (
            "is_nan(input.i64)",
            "unsupported_function",
            "function 'is_nan' requires floating-point input",
        ),
        (
            "is_infinite(input.u8)",
            "unsupported_function",
            "requires floating-point input",
        ),
        (
            "bitwise_and(input.i64, input.i32)",
            "type_mismatch",
            "function 'bitwise_and' requires matching operand types",
        ),
        (
            "bitwise_or(input.f64, input.f64)",
            "unsupported_function",
            "function 'bitwise_or' requires integer input",
        ),
        (
            "bitwise_not(input.f32)",
            "unsupported_function",
            "requires integer input",
        ),
        (
            "bit_count(input.f64)",
            "unsupported_function",
            "requires integer input",
        ),
        (
            "shift_left(input.i64, input.f64)",
            "unsupported_function",
            "function 'shift_left' requires integer input",
        ),
        (
            "shift_right(input.f64, 1)",
            "unsupported_function",
            "requires integer input",
        ),
        (
            "round(input.f64, input.f64)",
            "unsupported_function",
            "function 'round' requires integer input",
        ),
        (
            "round(input.f64, 1, 2)",
            "unknown_function",
            "unknown function 'round' with arity 3",
        ),
        (
            "atan2(input.f64)",
            "unknown_function",
            "unknown function 'atan2' with arity 1",
        ),
        (
            "sign(input.name)",
            "unsupported_function",
            "function 'sign' requires numeric input",
        ),
        (
            "log2(input.name)",
            "unsupported_function",
            "requires numeric input",
        ),
    ];
    for (expression, code, message) in rejected {
        let error = compile_assignment(expression, DataType::Int64)
            .expect_err("a call outside its signature must be rejected");
        assert_eq!(error.code, code, "`{expression}`: {error}");
        assert!(
            error.message.contains(message),
            "`{expression}` reported `{}`",
            error.message
        );
    }
}

#[test]
fn a_result_must_match_the_exact_type_of_its_destination() {
    let error = compile_assignment("bit_count(input.u8)", DataType::UInt8)
        .expect_err("bit_count returns I64");
    assert_eq!(error.code, "type_mismatch");

    let error = compile_assignment("sign(input.i16)", DataType::Int64)
        .expect_err("sign returns its operand's type");
    assert_eq!(error.code, "type_mismatch");
}

#[test]
fn calls_that_cannot_fail_fold_over_literals_and_calls_that_can_fail_do_not() {
    let program = parse_program(
        "SET masked = bitwise_and(12, 10), inverted = bitwise_not(0), ones = bit_count(255), \
         finite = is_finite(1.5), shifted = shift_left(1, 3), signed = sign(2.5), truncated = \
         trunc(2.5), rounded = round(2.675, 2)",
    )
    .expect("must parse");
    let input = Arc::new(Schema::new(Vec::<Field>::new()));
    let output = Arc::new(Schema::new(vec![
        Field::new("masked", DataType::Int64, true),
        Field::new("inverted", DataType::Int64, true),
        Field::new("ones", DataType::Int64, true),
        Field::new("finite", DataType::Boolean, true),
        Field::new("shifted", DataType::Int64, true),
        Field::new("signed", DataType::Float64, true),
        Field::new("truncated", DataType::Float64, true),
        Field::new("rounded", DataType::Float64, true),
    ]));

    let compiled =
        compile_program_for_bindings(&program, output, [CompileBinding::writable("input", input)])
            .expect("must compile");

    let builtins = compiled
        .instructions
        .iter()
        .filter_map(|instruction| match &instruction.kind {
            InstructionKind::Builtin { lowering, .. } => Some(lowering.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        builtins,
        [
            BuiltinLowering::ShiftLeft,
            BuiltinLowering::Sign,
            BuiltinLowering::Trunc,
            BuiltinLowering::Round,
        ]
    );
}

#[test]
fn folding_applies_the_value_contracts_execution_applies() {
    let integer = |value| FoldedValue::NonNull(ScalarValue::Int64(value));
    let float = |value| FoldedValue::NonNull(ScalarValue::Float64(value));
    let boolean = |value| Some(FoldedValue::NonNull(ScalarValue::Boolean(value)));

    assert_eq!(
        fold_builtin_call(&FunctionName::BitwiseAnd, &[integer(12), integer(10)]),
        Some(integer(8))
    );
    assert_eq!(
        fold_builtin_call(&FunctionName::BitwiseOr, &[integer(12), integer(10)]),
        Some(integer(14))
    );
    assert_eq!(
        fold_builtin_call(&FunctionName::BitwiseXor, &[integer(-1), integer(i64::MIN)]),
        Some(integer(i64::MAX))
    );
    assert_eq!(
        fold_builtin_call(&FunctionName::BitwiseNot, &[integer(i64::MAX)]),
        Some(integer(i64::MIN))
    );
    assert_eq!(
        fold_builtin_call(&FunctionName::BitCount, &[integer(i64::MIN)]),
        Some(integer(1))
    );
    assert_eq!(
        fold_builtin_call(&FunctionName::IsNan, &[float(f64::NAN)]),
        boolean(true)
    );
    assert_eq!(
        fold_builtin_call(&FunctionName::IsFinite, &[float(f64::NAN)]),
        boolean(false)
    );
    assert_eq!(
        fold_builtin_call(&FunctionName::IsInfinite, &[float(f64::NEG_INFINITY)]),
        boolean(true)
    );
    // A call over an argument of another shape declines to fold instead of guessing.
    assert_eq!(
        fold_builtin_call(&FunctionName::BitwiseAnd, &[integer(1)]),
        None
    );
    assert_eq!(fold_builtin_call(&FunctionName::IsNan, &[integer(1)]), None);
    assert_eq!(
        fold_builtin_call(&FunctionName::ShiftLeft, &[integer(1), integer(3)]),
        None
    );
}
