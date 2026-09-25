//! Tests for JSON extraction compiled into programs and executed over batches.
//!
//! Layer: test harness.
//!
//! - **Owns.** Sharing one scan among the extractions a program makes from one document column,
//!   confining a scan to the rows an arm selects, a document every row shares, the errors each
//!   extraction reports against its own assignment, and the type, nullability and sensitivity
//!   contract of an extraction's result.
//! - **Depends on.** The VM frontend, compiler and runtime entry points.
//! - **Must not know.** How a document is parsed or a value converted.

use std::sync::Arc as StdArc;

use arrow_array::{Array, BooleanArray, Int64Array, ListArray, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::execute_program_sync;
use crate::{
    CompileBinding, CompileError, CompileOptions, CompiledProgram, InstructionKind, JsonDefect,
    JsonKind, JsonOperation, JsonPlace, JsonTarget, SchemaSensitivity, SideErrorReason, TypedArray,
    TypedBatch, compile_program_with_options_for_bindings,
    compile_program_with_options_for_bindings_with_sensitivity, test_support::parse_program,
};

fn schema(fields: Vec<Field>) -> StdArc<Schema> {
    StdArc::new(Schema::new(fields))
}

fn document_schema() -> StdArc<Schema> {
    schema(vec![
        Field::new("kind", DataType::Utf8, true),
        Field::new("doc", DataType::Utf8, true),
    ])
}

fn compile_with(
    source: &str,
    input_schema: &StdArc<Schema>,
    outputs: Vec<Field>,
) -> Result<CompiledProgram, CompileError> {
    let output_schema = schema(
        input_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .chain(outputs)
            .collect(),
    );
    let parsed = parse_program(source).expect("the extraction program must parse");
    compile_program_with_options_for_bindings(
        &parsed,
        output_schema,
        [CompileBinding::writable("input", input_schema.clone())],
        CompileOptions::default(),
    )
}

fn compile(source: &str, input_schema: &StdArc<Schema>, outputs: Vec<Field>) -> CompiledProgram {
    compile_with(source, input_schema, outputs)
        .unwrap_or_else(|error| panic!("`{source}` must compile: {error:?}"))
}

fn documents(kinds: Vec<Option<&str>>, docs: Vec<Option<&str>>) -> TypedBatch {
    TypedBatch::try_new(
        document_schema(),
        vec![
            TypedArray::Utf8(StringArray::from(kinds)),
            TypedArray::Utf8(StringArray::from(docs)),
        ],
    )
    .expect("the batch must build")
}

fn output_column<'a>(batch: &'a TypedBatch, name: &str) -> &'a TypedArray {
    let index = batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == name)
        .expect("the output column must exist");
    batch.column(index)
}

/// How many instructions of the program `matches` accepts.
fn count_instructions(
    compiled: &CompiledProgram,
    matches: impl Fn(&InstructionKind) -> bool,
) -> usize {
    let mut count = 0;
    for instruction in &compiled.instructions {
        if matches(&instruction.kind) {
            count += 1;
        }
    }
    count
}

fn is_scan(kind: &InstructionKind) -> bool {
    matches!(kind, InstructionKind::JsonScan { .. })
}

/// The JSON defects recorded for `row`, in the order they were recorded.
fn json_defects(batch: &TypedBatch, row: usize) -> Vec<JsonDefect> {
    let mut defects = Vec::new();
    for error in batch.errors().row(row) {
        if let SideErrorReason::Json(defect) = &error.reason {
            defects.push(defect.clone());
        }
    }
    defects
}

#[test]
fn every_extraction_from_one_document_column_shares_one_scan() {
    let compiled = compile(
        "SET count = JSON_VALUE(input.doc, '$.count' AS I64), label = JSON_VALUE(input.doc, \
         '$.label' AS STRING), count_read = NOT is_null(TRY_JSON_VALUE(input.doc, '$.count' AS \
         I64)), labelled = JSON_EXISTS(input.doc, '$.label'), tags = JSON_VALUE(input.doc, \
         '$.tags' AS VEC<I64>) WHERE JSON_EXISTS(input.doc, '$.keep')",
        &document_schema(),
        vec![
            Field::new("count", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("count_read", DataType::Boolean, true),
            Field::new("labelled", DataType::Boolean, true),
            Field::new(
                "tags",
                JsonTarget::Vec(triomphe::Arc::new(JsonTarget::Int64)).data_type(),
                true,
            ),
        ],
    );
    assert_eq!(
        count_instructions(&compiled, is_scan),
        1,
        "{:#?}",
        compiled.instructions
    );
    let batch = documents(
        vec![None, None, None],
        vec![
            Some(r#"{"count":7,"label":"x","tags":[1,2],"keep":true}"#),
            Some(r#"{"count":"7","label":"y","keep":null}"#),
            Some(r#"{"count":1}"#),
        ],
    );

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output.row_count(),
        2,
        "the filter keeps the documents holding a keep member"
    );
    assert_eq!(
        output_column(&output, "count"),
        &TypedArray::Int64(Int64Array::from(vec![Some(7), None]))
    );
    assert_eq!(
        output_column(&output, "label"),
        &TypedArray::Utf8(StringArray::from(vec!["x", "y"])),
        "a defect of one extraction leaves the others of the same document untouched"
    );
    assert_eq!(
        output_column(&output, "count_read"),
        &TypedArray::Boolean(BooleanArray::from(vec![true, false]))
    );
    assert_eq!(
        output_column(&output, "labelled"),
        &TypedArray::Boolean(BooleanArray::from(vec![true, true]))
    );
    let TypedArray::Generic(tags) = output_column(&output, "tags") else {
        panic!("a VEC result is a generic column");
    };
    let tags = tags
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("VEC<I64> is a list");
    assert_eq!(tags.value_offsets(), &[0, 2, 2]);
    assert!(tags.is_null(1));
    assert!(output.errors().row(0).is_empty());
    assert_eq!(
        json_defects(&output, 1),
        [JsonDefect::TypeMismatch {
            path: triomphe::Arc::new(
                nervix_models::JsonPath::parse("$.count").expect("the path is valid")
            ),
            place: JsonPlace::Value,
            found: JsonKind::String,
            expected: triomphe::Arc::new(JsonTarget::Int64),
        }]
    );
}

#[test]
fn each_extraction_reports_its_failure_against_its_own_assignment() {
    let compiled = compile(
        "SET first = JSON_VALUE(input.doc, '$.a' AS I64), second = JSON_VALUE(input.doc, '$.b' AS \
         I64), repeated = JSON_VALUE(input.doc, '$.a' AS I64) + JSON_VALUE(input.doc, '$.a' AS \
         I64)",
        &document_schema(),
        vec![
            Field::new("first", DataType::Int64, true),
            Field::new("second", DataType::Int64, true),
            Field::new("repeated", DataType::Int64, true),
        ],
    );
    let mut answered = 0;
    for instruction in &compiled.instructions {
        if let InstructionKind::JsonScan { outputs, .. } = &instruction.kind {
            answered += outputs.len();
        }
    }
    assert_eq!(
        answered, 3,
        "the extraction one assignment repeats is answered once, and again for another assignment"
    );
    let batch = documents(vec![None], vec![Some(r#"{"a":1,"b":"two"}"#)]);

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "first"),
        &TypedArray::Int64(Int64Array::from(vec![1]))
    );
    assert_eq!(
        output_column(&output, "repeated"),
        &TypedArray::Int64(Int64Array::from(vec![2]))
    );
    assert_eq!(
        output_column(&output, "second"),
        &TypedArray::Int64(Int64Array::from(vec![None]))
    );
    let [error] = output.errors().row(0) else {
        panic!("only the second assignment fails");
    };
    assert_eq!(
        error.span,
        crate::program::Span { start: 2, end: 3 },
        "the failure belongs to the second assignment"
    );
}

#[test]
fn an_arm_parses_only_the_documents_of_the_rows_it_selects() {
    let compiled = compile(
        "SET number = CASE WHEN input.kind = 'json' THEN JSON_VALUE(input.doc, '$.n' AS I64) ELSE \
         -1 END, outside = TRY_JSON_VALUE(input.doc, '$.n' AS I64)",
        &document_schema(),
        vec![
            Field::new("number", DataType::Int64, true),
            Field::new("outside", DataType::Int64, true),
        ],
    );
    assert_eq!(
        count_instructions(&compiled, is_scan),
        2,
        "a scan under an arm answers only that arm's rows, so it is not shared outside it"
    );
    let batch = documents(
        vec![Some("json"), Some("other"), Some("json"), Some("other")],
        vec![
            Some(r#"{"n":5}"#),
            Some("not json"),
            Some(r#"{"n":"#),
            Some(r#"{"n":8}"#),
        ],
    );

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "number"),
        &TypedArray::Int64(Int64Array::from(vec![Some(5), Some(-1), None, Some(-1)]))
    );
    assert_eq!(
        output_column(&output, "outside"),
        &TypedArray::Int64(Int64Array::from(vec![Some(5), None, None, Some(8)]))
    );
    assert!(
        output.errors().row(1).is_empty(),
        "a document the arm does not select is never read by it"
    );
    assert_eq!(
        json_defects(&output, 2),
        [JsonDefect::Malformed(JsonOperation::JsonValue)]
    );
    assert!(output.errors().row(3).is_empty());

    let none_selected = documents(vec![Some("other")], vec![Some("not json")]);
    let output = execute_program_sync(&compiled, &none_selected).expect("execution must succeed");
    assert!(output.errors().is_error_free());
}

#[test]
fn a_document_every_row_shares_is_read_once_for_all_of_them() {
    let compiled = compile(
        "SET shared = JSON_VALUE('{\"n\":3}', '$.n' AS I64), broken = CASE WHEN input.kind = 'x' \
         THEN JSON_EXISTS('{', '$') ELSE FALSE END",
        &document_schema(),
        vec![
            Field::new("shared", DataType::Int64, true),
            Field::new("broken", DataType::Boolean, true),
        ],
    );
    let batch = documents(
        vec![Some("x"), Some("y"), Some("x")],
        vec![None, None, None],
    );

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    assert_eq!(
        output_column(&output, "shared"),
        &TypedArray::Int64(Int64Array::from(vec![3, 3, 3]))
    );
    assert_eq!(
        json_defects(&output, 0),
        [JsonDefect::Malformed(JsonOperation::JsonExists)]
    );
    assert!(
        output.errors().row(1).is_empty(),
        "an unselected row does not share the failure"
    );
    assert_eq!(
        json_defects(&output, 2),
        [JsonDefect::Malformed(JsonOperation::JsonExists)]
    );
}

#[test]
fn an_extraction_keeps_the_type_nullability_and_sensitivity_contract() {
    let rejected = |source: &str, outputs: Vec<Field>| {
        let input_schema = schema(vec![
            Field::new("doc", DataType::Utf8, false),
            Field::new("number", DataType::Int64, true),
        ]);
        compile_with(source, &input_schema, outputs).expect_err("the statement must be rejected")
    };

    let required = rejected(
        "SET amount = JSON_VALUE(input.doc, '$.a' AS I64)",
        vec![Field::new("amount", DataType::Int64, false)],
    );
    assert_eq!(required.code, "null_for_required_field");
    compile(
        "SET present = JSON_EXISTS(input.doc, '$.a')",
        &schema(vec![Field::new("doc", DataType::Utf8, false)]),
        vec![Field::new("present", DataType::Boolean, false)],
    );

    let mistyped = rejected(
        "SET amount = JSON_VALUE(input.doc, '$.a' AS I64)",
        vec![Field::new("amount", DataType::Utf8, true)],
    );
    assert_eq!(mistyped.code, "type_mismatch");

    let not_text = rejected(
        "SET amount = JSON_VALUE(input.number, '$.a' AS I64)",
        vec![Field::new("amount", DataType::Int64, true)],
    );
    assert_eq!(not_text.code, "type_mismatch");
    assert_eq!(
        not_text.message,
        "JSON_VALUE document must be STRING, found Int64"
    );

    let unreadable = parse_program("SET amount = JSON_VALUE(input.doc, '$.a' AS DATETIME)")
        .expect_err("no JSON value is a DATETIME");
    assert!(
        unreadable.contains("JSON_VALUE cannot read DATETIME"),
        "{unreadable}"
    );

    let input_schema = schema(vec![Field::new("secret", DataType::Utf8, true)]);
    let compile_sensitive = |source: &str| {
        let parsed = parse_program(source).expect("the extraction program must parse");
        compile_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            schema(vec![
                Field::new("secret", DataType::Utf8, true),
                Field::new("amount", DataType::Int64, true),
            ]),
            SchemaSensitivity::from_sensitive_fields(["secret"]),
            [CompileBinding::writable("input", input_schema.clone())
                .with_sensitivity(SchemaSensitivity::from_sensitive_fields(["secret"]))],
            CompileOptions::default(),
        )
    };
    let leak = compile_sensitive("SET amount = TRY_JSON_VALUE(input.secret, '$.a' AS I64)")
        .expect_err("a value read from a sensitive document stays sensitive");
    assert_eq!(leak.code, "sensitive_leak");
    compile_sensitive("SET amount = leak_sensitive(JSON_VALUE(input.secret, '$.a' AS I64))")
        .expect("an explicit leak removes the sensitivity");
}
