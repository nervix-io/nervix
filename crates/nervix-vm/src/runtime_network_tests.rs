//! Tests for the IP address, network and URL builtins compiled and executed as programs.
//!
//! Layer: test harness.
//!
//! - **Owns.** Compiling programs that parse, format, mask and test addresses and read URL
//!   components, executing them over batches, and checking their values, nulls and errors, and
//!   the decisions compilation makes for them: a constant network parsed once, an invalid one
//!   rejected, argument types, the components that can be missing, folded and shared validity
//!   tests, conditional arms, sensitivity, and literal arguments evaluated once per batch.
//! - **Depends on.** The VM compiler and runtime entry points.
//! - **Must not know.** How the kernels parse text or walk columns.

use std::sync::Arc as StdArc;

use arrow_array::{Array, BinaryArray, BooleanArray, Int64Array, ListArray, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::execute_program_sync;
use crate::{
    CompileBinding, CompileError, CompileOptions, CompiledProgram, SchemaSensitivity,
    SideErrorReason, TypedArray, TypedBatch, compile_program_with_options_for_bindings,
    compile_program_with_options_for_bindings_with_sensitivity,
    error::IpOperation,
    ip_address::{NetworkDefect, NetworkSource},
    ir::InstructionKind,
    semantics::BuiltinLowering,
    test_support::parse_program,
};

fn schema(fields: Vec<Field>) -> StdArc<Schema> {
    StdArc::new(Schema::new(fields))
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
    let parsed = parse_program(source).expect("the network program must parse");
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

fn output_column<'a>(batch: &'a TypedBatch, name: &str) -> &'a TypedArray {
    let index = batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == name)
        .expect("the output column must exist");
    batch.column(index)
}

/// The builtins the program runs, with how many registers each reads.
fn builtins(compiled: &CompiledProgram) -> Vec<(BuiltinLowering, usize)> {
    let mut builtins = Vec::new();
    for instruction in &compiled.instructions {
        if let InstructionKind::Builtin {
            lowering, inputs, ..
        } = &instruction.kind
        {
            builtins.push((lowering.clone(), inputs.len()));
        }
    }
    builtins
}

fn texts(values: Vec<Option<&str>>) -> TypedArray {
    TypedArray::Utf8(StringArray::from(values))
}

fn address_schema() -> StdArc<Schema> {
    schema(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("network", DataType::Utf8, true),
    ])
}

fn address_batch(addresses: Vec<Option<&str>>, networks: Vec<Option<&str>>) -> TypedBatch {
    TypedBatch::try_new(address_schema(), vec![texts(addresses), texts(networks)])
        .expect("the address batch must build")
}

#[test]
fn a_constant_network_is_parsed_once_and_a_row_network_is_read_for_each_row() {
    let compiled = compile(
        "SET inside = ip_in_network(ip_from_string(input.text), '10.0.0.0/8'), unique_local = \
         ip_in_network(ip_from_string(input.text), upper('fd00::/8')), named = \
         ip_in_network(ip_from_string(input.text), input.network)",
        &address_schema(),
        vec![
            Field::new("inside", DataType::Boolean, true),
            Field::new("unique_local", DataType::Boolean, true),
            Field::new("named", DataType::Boolean, true),
        ],
    );
    let mut constants = 0;
    let mut arguments = 0;
    for (lowering, inputs) in builtins(&compiled) {
        match lowering {
            BuiltinLowering::IpInNetwork(NetworkSource::Constant(_)) => {
                assert_eq!(inputs, 1, "a constant network reads only the address");
                constants += 1;
            }
            BuiltinLowering::IpInNetwork(NetworkSource::Argument) => {
                assert_eq!(inputs, 2, "a row network reads the address and the network");
                arguments += 1;
            }
            _ => {}
        }
    }
    assert_eq!((constants, arguments), (2, 1));

    let batch = address_batch(
        vec![Some("10.1.2.3"), Some("fd12::1"), Some("192.0.2.1"), None],
        vec![
            Some("192.0.2.0/24"),
            Some("fd00::/8"),
            Some("192.0.2.0/24"),
            Some("10.0.0.0/8"),
        ],
    );
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert!(output.errors().is_error_free());
    assert_eq!(
        output_column(&output, "inside"),
        &TypedArray::Boolean(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            None
        ]))
    );
    assert_eq!(
        output_column(&output, "unique_local"),
        &TypedArray::Boolean(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            None
        ]))
    );
    assert_eq!(
        output_column(&output, "named"),
        &TypedArray::Boolean(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(true),
            None
        ]))
    );
}

#[test]
fn a_constant_that_is_no_network_rejects_the_program() {
    let cases = [
        (
            "10.0.0.1/8",
            "function 'ip_in_network' network '10.0.0.1/8' has host bits set past its prefix \
             length",
        ),
        (
            "10.0.0.0/33",
            "function 'ip_in_network' network '10.0.0.0/33' prefix length must be 0 to 32 for an \
             IPv4 network",
        ),
        (
            "10.0.0.0",
            "function 'ip_in_network' network '10.0.0.0' is not written as address/prefix",
        ),
    ];
    for (network, message) in cases {
        let source = format!("SET inside = ip_in_network(ip_from_string(input.text), '{network}')");
        let error = compile_with(
            &source,
            &address_schema(),
            vec![Field::new("inside", DataType::Boolean, true)],
        )
        .expect_err("an invalid constant network rejects the program");
        assert_eq!(error.code, "invalid_ip_network", "{network}");
        assert_eq!(error.message, message);
    }
}

#[test]
fn arguments_keep_their_exact_types() {
    let input_schema = schema(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("address", DataType::Binary, true),
        Field::new("ratio", DataType::Float64, true),
    ]);
    let cases = [
        (
            "SET out = ip_in_network(input.text, '10.0.0.0/8')",
            DataType::Boolean,
            "function 'ip_in_network' requires BYTES input, found Utf8",
        ),
        (
            "SET out = ip_in_network(input.address, input.address)",
            DataType::Boolean,
            "function 'ip_in_network' requires Utf8 input, found Binary",
        ),
        (
            "SET out = ip_trunc(input.address, input.ratio)",
            DataType::Binary,
            "function 'ip_trunc' requires integer input, found Float64",
        ),
        (
            "SET out = url_host(input.address)",
            DataType::Utf8,
            "function 'url_host' requires Utf8 input, found Binary",
        ),
        (
            "SET out = ip_to_string(input.text)",
            DataType::Utf8,
            "function 'ip_to_string' requires BYTES input, found Utf8",
        ),
        (
            "SET out = ip_from_string(input.address)",
            DataType::Binary,
            "function 'ip_from_string' requires Utf8 input, found Binary",
        ),
    ];
    for (source, output_type, message) in cases {
        let error = compile_with(
            source,
            &input_schema,
            vec![Field::new("out", output_type, true)],
        )
        .expect_err("a mistyped argument rejects the program");
        assert_eq!(error.code, "unsupported_function", "{source}");
        assert_eq!(error.message, message, "{source}");
    }

    let error = compile_with(
        "SET out = url_query_values(input.text, 'tag')",
        &input_schema,
        vec![Field::new("out", DataType::Utf8, true)],
    )
    .expect_err("url_query_values returns a VEC<STRING>");
    assert_eq!(error.code, "type_mismatch");
}

#[test]
fn components_a_url_can_lack_are_optional_and_the_others_follow_their_input() {
    let input_schema = schema(vec![Field::new("url", DataType::Utf8, false)]);
    for source in [
        "SET out = url_host(input.url)",
        "SET out = url_query(input.url)",
        "SET out = url_fragment(input.url)",
        "SET out = url_query_value(input.url, 'q')",
    ] {
        let error = compile_with(
            source,
            &input_schema,
            vec![Field::new("out", DataType::Utf8, false)],
        )
        .expect_err("a component a URL can lack is optional");
        assert_eq!(error.code, "null_for_required_field", "{source}");
    }
    let error = compile_with(
        "SET out = url_port(input.url)",
        &input_schema,
        vec![Field::new("out", DataType::Int64, false)],
    )
    .expect_err("a URL can lack a port");
    assert_eq!(error.code, "null_for_required_field");

    for source in [
        "SET out = url_scheme(input.url)",
        "SET out = url_path(input.url)",
        "SET out = url_decode(input.url)",
    ] {
        compile(
            source,
            &input_schema,
            vec![Field::new("out", DataType::Utf8, false)],
        );
    }
    compile(
        "SET out = url_query_values(input.url, 'tag')",
        &input_schema,
        vec![Field::new(
            "out",
            DataType::List(Field::new("item", DataType::Utf8, false).into()),
            false,
        )],
    );
    compile(
        "SET out = is_url(input.url)",
        &input_schema,
        vec![Field::new("out", DataType::Boolean, false)],
    );
}

#[test]
fn validity_tests_fold_over_literals_and_are_shared_over_columns() {
    let compiled = compile(
        "SET url_literal = is_url('https://example.com/'), address_literal = \
         is_ip_address('10.0.0.256'), first = is_ip_address(input.text), second = \
         is_ip_address(input.text) AND NOT is_ip_address(input.text)",
        &address_schema(),
        vec![
            Field::new("url_literal", DataType::Boolean, true),
            Field::new("address_literal", DataType::Boolean, true),
            Field::new("first", DataType::Boolean, true),
            Field::new("second", DataType::Boolean, true),
        ],
    );
    let tests = builtins(&compiled)
        .into_iter()
        .filter(|(lowering, _)| {
            matches!(
                lowering,
                BuiltinLowering::IsIpAddress | BuiltinLowering::IsUrl
            )
        })
        .count();
    // Each assignment starts a new cache generation, so `first` and `second` compute their own
    // test, and the two occurrences inside `second` share one.
    assert_eq!(
        tests, 2,
        "literal tests fold, and one test of a column is shared within an expression"
    );

    let batch = address_batch(vec![Some("192.0.2.1"), Some("nope"), None], vec![None; 3]);
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "url_literal"),
        &TypedArray::Boolean(BooleanArray::from(vec![true, true, true]))
    );
    assert_eq!(
        output_column(&output, "address_literal"),
        &TypedArray::Boolean(BooleanArray::from(vec![false, false, false]))
    );
    assert_eq!(
        output_column(&output, "first"),
        &TypedArray::Boolean(BooleanArray::from(vec![Some(true), Some(false), None]))
    );
    assert_eq!(
        output_column(&output, "second"),
        &TypedArray::Boolean(BooleanArray::from(vec![Some(false), Some(false), None]))
    );
}

#[test]
fn each_strict_parse_reports_its_own_error() {
    let compiled = compile(
        "SET first = ip_from_string(input.text), second = ip_to_string(ip_from_string(input.text))",
        &address_schema(),
        vec![
            Field::new("first", DataType::Binary, true),
            Field::new("second", DataType::Utf8, true),
        ],
    );
    let parses = builtins(&compiled)
        .into_iter()
        .filter(|(lowering, _)| matches!(lowering, BuiltinLowering::IpFromString))
        .count();
    assert_eq!(parses, 2, "a parse that can fail is not shared");

    let batch = address_batch(vec![Some("::1"), Some("bad")], vec![None, None]);
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "first"),
        &TypedArray::Binary(BinaryArray::from(vec![
            Some(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1][..]),
            None
        ]))
    );
    assert_eq!(
        output_column(&output, "second"),
        &texts(vec![Some("::1"), None])
    );
    assert!(output.errors().row(0).is_empty());
    let reasons = output
        .errors()
        .row(1)
        .iter()
        .map(|error| error.reason.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        reasons,
        [
            SideErrorReason::UnreadableIpAddress,
            SideErrorReason::UnreadableIpAddress
        ]
    );
}

#[test]
fn a_conditional_arm_parses_and_masks_only_the_rows_it_selects() {
    let compiled = compile(
        "SET parsed = CASE WHEN is_ip_address(input.text) THEN \
         ip_to_string(ip_trunc(ip_from_string(input.text), 24)) END, host = CASE WHEN \
         starts_with(input.network, 'http') THEN url_host(input.network) ELSE 'none' END",
        &address_schema(),
        vec![
            Field::new("parsed", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ],
    );
    let batch = address_batch(
        vec![Some("192.0.2.77"), Some("not an address"), None],
        vec![
            Some("https://Example.com/"),
            Some("mailto:x@y"),
            Some("http://"),
        ],
    );
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "parsed"),
        &texts(vec![Some("192.0.2.0"), None, None])
    );
    assert_eq!(
        output_column(&output, "host"),
        &texts(vec![Some("example.com"), Some("none"), None])
    );
    assert!(output.errors().row(0).is_empty());
    assert!(output.errors().row(1).is_empty());
    assert_eq!(
        output.errors().row(2)[0].reason.to_string(),
        "url_host input is not an absolute URL: empty host"
    );
}

#[test]
fn every_url_component_is_read_from_a_column() {
    let input_schema = schema(vec![Field::new("url", DataType::Utf8, true)]);
    let compiled = compile(
        "SET scheme = url_scheme(input.url), host = url_host(input.url), port = \
         url_port(input.url), path = url_path(input.url), query = url_query(input.url), fragment \
         = url_fragment(input.url), decoded = url_decode(url_path(input.url)), valid = \
         is_url(input.url)",
        &input_schema,
        vec![
            Field::new("scheme", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("port", DataType::Int64, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("query", DataType::Utf8, true),
            Field::new("fragment", DataType::Utf8, true),
            Field::new("decoded", DataType::Utf8, true),
            Field::new("valid", DataType::Boolean, true),
        ],
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![texts(vec![
            Some("WSS://Chat.Example:9443/room%201?since=5#latest"),
            Some("mailto:ops@example.com"),
            None,
        ])],
    )
    .expect("the URL batch must build");
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert!(output.errors().is_error_free());
    assert_eq!(
        output_column(&output, "scheme"),
        &texts(vec![Some("wss"), Some("mailto"), None])
    );
    assert_eq!(
        output_column(&output, "host"),
        &texts(vec![Some("chat.example"), None, None])
    );
    assert_eq!(
        output_column(&output, "port"),
        &TypedArray::Int64(Int64Array::from(vec![Some(9443), None, None]))
    );
    assert_eq!(
        output_column(&output, "path"),
        &texts(vec![Some("/room%201"), Some("ops@example.com"), None])
    );
    assert_eq!(
        output_column(&output, "query"),
        &texts(vec![Some("since=5"), None, None])
    );
    assert_eq!(
        output_column(&output, "fragment"),
        &texts(vec![Some("latest"), None, None])
    );
    assert_eq!(
        output_column(&output, "decoded"),
        &texts(vec![Some("/room 1"), Some("ops@example.com"), None])
    );
    assert_eq!(
        output_column(&output, "valid"),
        &TypedArray::Boolean(BooleanArray::from(vec![Some(true), Some(true), None]))
    );
}

#[test]
fn query_values_inside_an_arm_are_read_for_the_selected_rows_only() {
    let compiled = compile(
        "SET tag_count = CASE WHEN input.network = 'read' THEN count(url_query_values(input.text, \
         'tag')) ELSE -1 END",
        &address_schema(),
        vec![Field::new("tag_count", DataType::Int64, true)],
    );
    let batch = address_batch(
        vec![
            Some("https://example.com/?tag=a&tag=b"),
            Some("not a url"),
            Some("https://example.com/"),
        ],
        vec![Some("read"), Some("skip"), Some("read")],
    );
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert!(output.errors().is_error_free());
    assert_eq!(
        output_column(&output, "tag_count"),
        &TypedArray::Int64(Int64Array::from(vec![2, -1, 0]))
    );

    // No row selects the arm, so its list is computed over the batch and its errors dropped.
    let batch = address_batch(
        vec![Some("not a url"), Some("https://example.com/?tag=a")],
        vec![Some("skip"), Some("skip")],
    );
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert!(output.errors().is_error_free());
    assert_eq!(
        output_column(&output, "tag_count"),
        &TypedArray::Int64(Int64Array::from(vec![-1, -1]))
    );
}

#[test]
fn literal_arguments_are_computed_once_and_their_failure_fails_every_row() {
    let compiled = compile(
        "SET text = ip_to_string(ip_from_string('192.0.2.1')), \
         path = url_path('https://example.com/a b'), \
         family = ip_family(ip_from_string('::1'))",
        &address_schema(),
        vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("family", DataType::Int64, true),
        ],
    );
    let batch = address_batch(vec![None, None, None], vec![None, None, None]);
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert!(output.errors().is_error_free());
    assert_eq!(
        output_column(&output, "text"),
        &texts(vec![Some("192.0.2.1"); 3])
    );
    assert_eq!(
        output_column(&output, "path"),
        &texts(vec![Some("/a%20b"); 3])
    );
    assert_eq!(
        output_column(&output, "family"),
        &TypedArray::Int64(Int64Array::from(vec![6, 6, 6]))
    );

    let failing = compile(
        "SET parsed = ip_from_string('192.0.2.300')",
        &address_schema(),
        vec![Field::new("parsed", DataType::Binary, true)],
    );
    let output = execute_program_sync(&failing, &batch).expect("execution must succeed");
    assert_eq!(output_column(&output, "parsed").null_count(), 3);
    for row in 0..3 {
        assert_eq!(
            output.errors().row(row)[0].reason,
            SideErrorReason::UnreadableIpAddress,
            "row {row}"
        );
    }
}

#[test]
fn query_values_fill_a_vec_of_strings() {
    let input_schema = schema(vec![Field::new("url", DataType::Utf8, true)]);
    let list = DataType::List(Field::new("item", DataType::Utf8, false).into());
    let compiled = compile(
        "SET tags = url_query_values(input.url, 'tag'), first = url_query_value(input.url, 'tag')",
        &input_schema,
        vec![
            Field::new("tags", list, true),
            Field::new("first", DataType::Utf8, true),
        ],
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![texts(vec![
            Some("https://example.com/?tag=a&tag=b+c&tag=d%26e"),
            Some("https://example.com/"),
            Some("https://example.com/?tag=%ZZ"),
            None,
        ])],
    )
    .expect("the URL batch must build");
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    let TypedArray::Generic(tags) = output_column(&output, "tags") else {
        panic!("url_query_values produces a list column");
    };
    let tags = tags
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("a VEC<STRING> column is a list array");
    let row = tags.value(0);
    let row = row
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("the list holds STRING values");
    assert_eq!(
        row.iter().collect::<Vec<_>>(),
        [Some("a"), Some("b c"), Some("d&e")]
    );
    assert_eq!(tags.value(1).len(), 0);
    assert!(tags.is_null(2));
    assert!(tags.is_null(3));
    assert_eq!(
        output_column(&output, "first"),
        &texts(vec![Some("a"), None, None, None])
    );
    assert_eq!(
        output.errors().row(2)[0].reason.to_string(),
        "url_query_values input is not valid percent-encoded UTF-8"
    );
}

#[test]
fn address_bytes_that_hold_no_address_fail_their_row() {
    let input_schema = schema(vec![Field::new("address", DataType::Binary, true)]);
    let compiled = compile(
        "SET family = ip_family(input.address), unmapped = ip_unmap(input.address)",
        &input_schema,
        vec![
            Field::new("family", DataType::Int64, true),
            Field::new("unmapped", DataType::Binary, true),
        ],
    );
    let batch = TypedBatch::try_new(
        input_schema,
        vec![TypedArray::Binary(BinaryArray::from(vec![
            Some(&[10, 0, 0, 1][..]),
            Some(&[10, 0, 0][..]),
        ]))],
    )
    .expect("the address batch must build");
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "family"),
        &TypedArray::Int64(Int64Array::from(vec![Some(4), None]))
    );
    assert_eq!(
        output.errors().row(1)[0].reason,
        SideErrorReason::NotAnIpAddress(IpOperation::IpFamily)
    );
    assert_eq!(
        output.errors().row(1)[1].reason,
        SideErrorReason::NotAnIpAddress(IpOperation::IpUnmap)
    );
}

#[test]
fn a_row_network_that_is_no_network_fails_its_row() {
    let compiled = compile(
        "SET named = ip_in_network(ip_from_string(input.text), input.network)",
        &address_schema(),
        vec![Field::new("named", DataType::Boolean, true)],
    );
    let batch = address_batch(
        vec![Some("10.0.0.1"), Some("10.0.0.1")],
        vec![Some("10.0.0.0/8"), Some("10.0.0.1/8")],
    );
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert_eq!(
        output_column(&output, "named"),
        &TypedArray::Boolean(BooleanArray::from(vec![Some(true), None]))
    );
    assert_eq!(
        output.errors().row(1)[0].reason,
        SideErrorReason::InvalidIpNetwork(NetworkDefect::HostBitsSet)
    );
}

#[test]
fn every_result_keeps_the_sensitivity_of_its_arguments() {
    let input_schema = schema(vec![Field::new("secret", DataType::Utf8, true)]);
    let compile_sensitive = |source: &str, data_type: DataType| {
        let parsed = parse_program(source).expect("the network program must parse");
        compile_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            schema(vec![
                Field::new("secret", DataType::Utf8, true),
                Field::new("out", data_type, true),
            ]),
            SchemaSensitivity::from_sensitive_fields(["secret"]),
            [CompileBinding::writable("input", input_schema.clone())
                .with_sensitivity(SchemaSensitivity::from_sensitive_fields(["secret"]))],
            CompileOptions::default(),
        )
    };
    for (source, data_type) in [
        (
            "SET out = ip_in_network(ip_from_string(input.secret), '10.0.0.0/8')",
            DataType::Boolean,
        ),
        (
            "SET out = ip_family(ip_from_string(input.secret))",
            DataType::Int64,
        ),
        ("SET out = url_host(input.secret)", DataType::Utf8),
        ("SET out = url_port(input.secret)", DataType::Int64),
        ("SET out = is_url(input.secret)", DataType::Boolean),
        (
            "SET out = url_query_value('https://example.com/?q=1', input.secret)",
            DataType::Utf8,
        ),
    ] {
        let error = compile_sensitive(source, data_type.clone())
            .expect_err("a result read from a sensitive value stays sensitive");
        assert_eq!(error.code, "sensitive_leak", "{source}");
    }
    compile_sensitive(
        "SET out = leak_sensitive(url_host(input.secret))",
        DataType::Utf8,
    )
    .expect("an explicit leak removes the sensitivity");
}
