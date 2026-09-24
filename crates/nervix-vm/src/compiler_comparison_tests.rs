//! Compiler tests for membership, ranges, null-safe equality and the scalar extrema.
//!
//! Layer: test harness.
//!
//! - **Owns.** The accepted operand types, result types, nullability and sensitivity of `IN`,
//!   `BETWEEN`, `IS [NOT] DISTINCT FROM`, `greatest`, `least` and `clamp`, the diagnostics outside
//!   those signatures, and the set an `IN` prepares once when its program is compiled.
//! - **Depends on.** The VM frontend and compiler.
//! - **Must not know.** How execution evaluates a test.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use super::*;
use crate::test_support::parse_program;

fn operand_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("priority", DataType::Int32, false),
        Field::new("small", DataType::UInt8, false),
        Field::new("weight", DataType::Float64, false),
        Field::new("optional_weight", DataType::Float64, true),
        Field::new("active", DataType::Boolean, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("secret", DataType::Float64, false),
    ]))
}

fn compile_assignment(
    expression: &str,
    output_type: DataType,
    output_nullable: bool,
) -> Result<CompiledProgram, CompileError> {
    let program = parse_program(&format!("SET out = {expression}")).expect("must parse");
    let input = operand_schema();
    let mut fields = input
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("out", output_type, output_nullable));
    compile_program_for_bindings_with_sensitivity(
        &program,
        Arc::new(Schema::new(fields)),
        SchemaSensitivity::from_sensitive_fields(["secret"]),
        [CompileBinding::writable("input", input)
            .with_sensitivity(SchemaSensitivity::from_sensitive_fields(["secret"]))],
    )
}

fn failure(expression: &str, output_type: DataType) -> CompileError {
    compile_assignment(expression, output_type, true).expect_err("the expression must be rejected")
}

fn memberships(compiled: &CompiledProgram) -> Vec<&MembershipSet> {
    compiled
        .instructions
        .iter()
        .filter_map(|instruction| match &instruction.kind {
            InstructionKind::Builtin {
                lowering: BuiltinLowering::Membership(set),
                ..
            } => Some(set),
            _ => None,
        })
        .collect()
}

#[test]
fn every_test_is_boolean_and_every_extremum_keeps_its_operand_type() {
    let signatures = [
        ("input.status IN ('open', 'held')", DataType::Boolean),
        (
            "input.priority NOT IN (1 AS I32, -2 AS I32)",
            DataType::Boolean,
        ),
        ("input.weight BETWEEN 1.0 AND 2.0", DataType::Boolean),
        ("input.status NOT BETWEEN 'a' AND 'm'", DataType::Boolean),
        ("input.region IS DISTINCT FROM 'eu'", DataType::Boolean),
        ("input.active IS NOT DISTINCT FROM FALSE", DataType::Boolean),
        (
            "greatest(input.weight, 1.5, input.optional_weight)",
            DataType::Float64,
        ),
        ("least(input.status, 'm')", DataType::Utf8),
        ("greatest(input.active, FALSE)", DataType::Boolean),
        ("clamp(input.priority, 0 AS I32, 9 AS I32)", DataType::Int32),
        ("GREATEST(input.small)", DataType::UInt8),
    ];
    for (expression, output_type) in signatures {
        compile_assignment(expression, output_type.clone(), true)
            .unwrap_or_else(|error| panic!("{expression} must compile as {output_type}: {error}"));
    }
}

#[test]
fn nullability_follows_each_test_and_extremum() {
    let required = [
        // A required operand is always tested, and no value is an element of an empty set.
        ("input.status IN ('open')", DataType::Boolean),
        ("input.region IN ()", DataType::Boolean),
        ("input.region NOT IN ()", DataType::Boolean),
        ("input.weight BETWEEN 1.0 AND 2.0", DataType::Boolean),
        // Null-safe equality is never null.
        (
            "input.region IS DISTINCT FROM input.region",
            DataType::Boolean,
        ),
        (
            "input.optional_weight IS NOT DISTINCT FROM 1.0",
            DataType::Boolean,
        ),
        // One required argument makes an extremum required, since null arguments are skipped.
        (
            "greatest(input.optional_weight, input.weight)",
            DataType::Float64,
        ),
        ("least(input.optional_weight, 0.0)", DataType::Float64),
        ("clamp(input.weight, 0.0, 1.0)", DataType::Float64),
    ];
    for (expression, output_type) in required {
        if let Err(error) = compile_assignment(expression, output_type, false) {
            panic!("{expression} must be required: {error}");
        }
    }

    let optional = [
        ("input.region IN ('eu')", DataType::Boolean),
        ("input.region NOT IN ('eu')", DataType::Boolean),
        (
            "input.optional_weight BETWEEN 1.0 AND 2.0",
            DataType::Boolean,
        ),
        (
            "input.weight BETWEEN input.optional_weight AND 2.0",
            DataType::Boolean,
        ),
        (
            "greatest(input.optional_weight, input.optional_weight)",
            DataType::Float64,
        ),
        (
            "clamp(input.weight, input.optional_weight, 1.0)",
            DataType::Float64,
        ),
    ];
    for (expression, output_type) in optional {
        let error = compile_assignment(expression, output_type, false)
            .expect_err("a nullable result must not fill a required field");
        assert_eq!(error.code, "null_for_required_field", "{expression}");
    }
}

#[test]
fn operands_outside_each_signature_are_rejected_when_the_program_is_compiled() {
    let cases = [
        (
            "input.priority IN (1, 2)",
            "type_mismatch",
            "IN set element 1 has type Int64, but the operand has type Int32",
        ),
        (
            "input.status IN ('open', NULL)",
            "null_set_element",
            "IN set element 2 is NULL",
        ),
        (
            "input.status IN ('open', input.status)",
            "non_constant_set_element",
            "IN set element 2 is not a constant",
        ),
        (
            "input.status IN (upper('open'))",
            "non_constant_set_element",
            "IN set element 1 is not a constant",
        ),
        (
            "input.small IN (255 AS U8, 300 AS U8)",
            "invalid_set_element",
            "IN set element 2 cannot be evaluated: cannot cast value to UInt8",
        ),
        (
            "input.status IN ('x' AS I64)",
            "type_mismatch",
            "IN set element 1 has type Int64, but the operand has type Utf8",
        ),
        (
            "input.tags IN ()",
            "unsupported_membership",
            "IN is not valid for List",
        ),
        (
            "input.active BETWEEN FALSE AND TRUE",
            "unsupported_range",
            "BETWEEN is not valid for Boolean",
        ),
        (
            "input.weight BETWEEN 1 AND 2",
            "type_mismatch",
            "BETWEEN requires the operand and both bounds to have one exact type, found Float64, \
             Int64 and Int64",
        ),
        (
            "input.region IS DISTINCT FROM 1",
            "type_mismatch",
            "binary operator IsDistinctFrom requires matching operand types, found Utf8 and Int64",
        ),
        (
            "input.tags IS NOT DISTINCT FROM input.tags",
            "unsupported_binary",
            "operator IsNotDistinctFrom is not valid for List",
        ),
        (
            "greatest(input.weight, input.priority)",
            "type_mismatch",
            "function 'greatest' requires matching operand types, found Float64 and Int32",
        ),
        (
            "least(input.tags)",
            "unsupported_function",
            "function 'least' requires numeric, BOOL, STRING or DATETIME input, found Generic",
        ),
        (
            "greatest()",
            "unknown_function",
            "unknown function 'greatest' with arity 0",
        ),
        (
            "clamp(input.active, FALSE, TRUE)",
            "unsupported_function",
            "function 'clamp' requires numeric, STRING or DATETIME input, found Boolean",
        ),
        (
            "clamp(input.weight, 0.0)",
            "unknown_function",
            "unknown function 'clamp' with arity 2",
        ),
    ];
    for (expression, code, message) in cases {
        let error = failure(expression, DataType::Boolean);
        assert_eq!(error.code, code, "{expression}: {}", error.message);
        assert!(
            error.message.starts_with(message),
            "{expression}: expected {message:?}, found {:?}",
            error.message
        );
    }
}

#[test]
fn every_test_and_extremum_keeps_the_sensitivity_of_its_operands() {
    for (expression, output_type) in [
        ("input.secret IN (1.0, 2.0)", DataType::Boolean),
        ("input.secret NOT BETWEEN 1.0 AND 2.0", DataType::Boolean),
        (
            "input.weight BETWEEN input.secret AND 2.0",
            DataType::Boolean,
        ),
        ("input.secret IS DISTINCT FROM 1.0", DataType::Boolean),
        ("greatest(input.weight, input.secret)", DataType::Float64),
        ("least(input.secret)", DataType::Float64),
        ("clamp(input.weight, 0.0, input.secret)", DataType::Float64),
    ] {
        let error = failure(expression, output_type);
        assert_eq!(error.code, "sensitive_leak", "{expression}");
    }
    compile_assignment(
        "leak_sensitive(input.secret) IN (1.0)",
        DataType::Boolean,
        true,
    )
    .expect("an explicit leak removes the sensitivity");
}

#[test]
fn a_set_is_evaluated_once_and_shared_by_every_identical_test() {
    let program = parse_program(
        "SET first = input.priority IN (-1 AS I32, 2 AS I32, 2 AS I32) OR input.priority IN (-1 \
         AS I32, 2 AS I32, 2 AS I32), third = input.priority NOT IN (7 AS I32)",
    )
    .expect("must parse");
    let input = operand_schema();
    let mut fields = input
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    for name in ["first", "third"] {
        fields.push(Field::new(name, DataType::Boolean, false));
    }
    let compiled = compile_program_for_bindings(
        &program,
        Arc::new(Schema::new(fields)),
        [CompileBinding::writable("input", input)],
    )
    .expect("the program must compile");

    let sets = memberships(&compiled);
    assert_eq!(sets.len(), 2, "identical tests share one instruction");
    let expected = MembershipSet::prepare(
        RegisterType::Int32,
        &[
            TypedArray::Int32(arrow_array::Int32Array::from(vec![-1])),
            TypedArray::Int32(arrow_array::Int32Array::from(vec![2])),
        ],
    )
    .expect("the elements are I32 values");
    assert_eq!(sets[0], &expected);
    assert!(
        compiled.instructions.iter().any(|instruction| matches!(
            instruction.kind,
            InstructionKind::Unary {
                op: UnaryOp::Not,
                ..
            }
        )),
        "NOT IN negates the membership test"
    );
    let clone = compiled.clone();
    assert!(memberships(&clone)[0].shares_elements_with(sets[0]));
}
