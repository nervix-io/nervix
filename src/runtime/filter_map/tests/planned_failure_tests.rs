use super::*;

/// A branch program that keys each output row by the quotient of its `amount` and `divisor`.
fn quotient_branch_program(schema: &Arc<CompiledSchema>) -> CompiledBranchProgram {
    let branch_schema = test_schema(&[("bucket", ParseAsType::I64)]);
    let lowered = nervix_vm::lower_branch_construction(
        &construction("SET bucket = message.amount / message.divisor").assignments,
        branch_schema.arrow_schema().as_ref(),
        schema.arrow_schema().as_ref(),
        schema.arrow_schema().as_ref(),
    )
    .expect("the quotient is the I64 the branch schema declares");
    let vm_schema = |schema: &Arc<CompiledSchema>| RuntimeVmSchema {
        schema: schema.arrow_schema(),
        sensitivity: VmSchemaSensitivity::default(),
    };
    bind_output_branch_program(
        &named("bucket_source"),
        &lowered,
        vm_schema(schema),
        vm_schema(schema),
        vm_schema(&branch_schema),
        RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &ResolvedBranching::unbranched(),
            udfs: None,
        },
    )
    .expect("the branch program binds to the schemas it was lowered for")
}

fn readings(schema: &Arc<CompiledSchema>, rows: &[(i64, i64)]) -> RelayRecordBatch {
    let messages = rows
        .iter()
        .map(|(amount, divisor)| RelayMessage {
            key: None,
            record: test_runtime_row([
                ("amount".to_string(), RuntimeValue::I64(*amount)),
                ("divisor".to_string(), RuntimeValue::I64(*divisor)),
            ]),
            acks: AckSet::root().0,
        })
        .collect();
    RelayRecordBatch::from_messages(schema.clone(), messages).expect("the readings batch builds")
}

/// A row whose branch key cannot be computed keeps that as its own typed outcome, and the rows
/// around it keep their keys.
#[nervix_primitives::test]
async fn a_branch_key_that_fails_for_one_row_leaves_the_other_rows_their_keys() {
    let schema = test_schema(&[("amount", ParseAsType::I64), ("divisor", ParseAsType::I64)]);
    let program = quotient_branch_program(&schema);
    let batch = readings(&schema, &[(10, 5), (4, 0), (9, 3)]);

    let outcomes = evaluate_output_branch_program(
        ProgramRun {
            executor: &Executor::default(),
            now: Timestamp::from_unix_nanos(1),
        },
        &program,
        &batch.batch,
        &batch.batch,
        &batch.keys,
        &HashMap::default(),
    )
    .await
    .expect("the branch program runs over the batch");

    let [divided, failed, last] = &outcomes[..] else {
        panic!("every input row has a branch outcome: {outcomes:?}");
    };
    assert_eq!(
        divided.as_ref().map(BranchKey::as_str),
        Ok(r#"{"bucket":2}"#)
    );
    assert_eq!(last.as_ref().map(BranchKey::as_str), Ok(r#"{"bucket":3}"#));
    let Err(BranchRowError::Set(error)) = failed else {
        panic!("dividing by zero fails only its own row: {failed:?}");
    };
    assert_eq!(error.code(), nervix_vm::ErrorCode::DivisionByZero);
    let message = BranchRowError::Set(error.clone()).to_string();
    assert!(
        message
            .starts_with("branch SET failed with division_by_zero: integer division by zero at "),
        "{message}"
    );
}

/// Branch construction checks that the batch it keys and its sidecars describe the same rows.
#[nervix_primitives::test]
async fn branch_construction_refuses_inputs_and_keys_of_another_row_count() {
    let schema = test_schema(&[("amount", ParseAsType::I64), ("divisor", ParseAsType::I64)]);
    let program = quotient_branch_program(&schema);
    let one_row = readings(&schema, &[(10, 5)]);
    let two_rows = readings(&schema, &[(10, 5), (9, 3)]);
    let run = ProgramRun {
        executor: &Executor::default(),
        now: Timestamp::from_unix_nanos(1),
    };

    let error = evaluate_output_branch_program(
        run,
        &program,
        &one_row.batch,
        &two_rows.batch,
        &two_rows.keys,
        &HashMap::default(),
    )
    .await
    .expect_err("one input row cannot key two output rows");
    assert_eq!(
        error.current_context(),
        &PlannedGeneralError::SidecarRowCount {
            operation: MessageErrorOperation::BranchSet,
            sidecar: PlannedSidecar::Input,
            expected: 2,
            found: 1,
        }
    );
    assert_eq!(
        error.to_string(),
        "branch construction received 1 input rows for 2 records"
    );

    let error = evaluate_output_branch_program(
        run,
        &program,
        &two_rows.batch,
        &two_rows.batch,
        &one_row.keys,
        &HashMap::default(),
    )
    .await
    .expect_err("one key cannot describe two rows");
    assert_eq!(
        error.current_context(),
        &PlannedGeneralError::SidecarRowCount {
            operation: MessageErrorOperation::BranchSet,
            sidecar: PlannedSidecar::BranchKeys,
            expected: 2,
            found: 1,
        }
    );
}

/// A program run that cannot even prepare its input fails the whole batch, and hands back the
/// acknowledgement of every message it was given for the error policy to resolve.
#[nervix_primitives::test]
async fn a_failed_program_run_hands_back_every_acknowledgement() {
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let program = validation_filter_map_program(&schema);
    let row = test_runtime_row([("value".to_string(), RuntimeValue::I64(7))]);
    let carrier = row.one_row_batch();
    let (acks, _completion) = AckSet::root();

    let failure = execute_filter_map_program_on_batch(
        ProgramRun {
            executor: &Executor::default(),
            now: Timestamp::from_unix_nanos(1),
        },
        &program,
        FilterMapBatchInputs {
            carrier: &carrier,
            namespace_batches: &[],
            keys: &[],
            side_inputs: &HashMap::default(),
            ingest_metadata: None,
        },
        vec![acks],
        None,
    )
    .await
    .err()
    .expect("a batch without its branch keys cannot be projected");

    assert_eq!(failure.acks.len(), 1);
    assert_eq!(
        failure.error.current_context(),
        &PlannedGeneralError::PrepareInput {
            operation: MessageErrorOperation::Set,
        }
    );
    assert!(failure.error.contains::<RuntimeSchemaError>());
    assert!(
        format!("{:#}", failure.error).starts_with("failed to prepare FILTER-MAP input batch: "),
        "{:#}",
        failure.error
    );
}

/// A planned batch whose input cannot be prepared fails as a whole, names the program by its
/// clause, and hands back the acknowledgement of every message it held.
#[nervix_primitives::test]
async fn a_planned_batch_that_cannot_prepare_its_input_hands_back_its_acknowledgements() {
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let program = validation_filter_map_program(&schema);
    let messages = [7, 8]
        .into_iter()
        .map(|value| RelayMessage {
            key: None,
            record: test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]),
            acks: AckSet::root().0,
        })
        .collect();
    let mut batch =
        RelayRecordBatch::from_messages(schema, messages).expect("the two-row batch builds");
    // Branch keys that describe no row cannot be projected into the program's input.
    batch.keys.clear();

    let failure = plan_filter_map_messages(
        ProgramRun {
            executor: &Executor::default(),
            now: Timestamp::from_unix_nanos(1),
        },
        "junction",
        named::<ModelName>("validate_filter_map"),
        MessageErrorOperation::FilterWhere,
        &program,
        batch,
        &HashMap::default(),
    )
    .await
    .err()
    .expect("a batch without its branch keys cannot be planned");

    assert_eq!(failure.acks.len(), 2);
    assert_eq!(
        failure.error.current_context(),
        &PlannedGeneralError::PrepareInput {
            operation: MessageErrorOperation::FilterWhere,
        }
    );
    assert!(failure.error.contains::<RuntimeSchemaError>());
    assert!(
        format!("{:#}", failure.error).starts_with("failed to prepare FILTER WHERE input batch: "),
        "{:#}",
        failure.error
    );
}

#[test]
fn header_invocations_name_why_a_row_has_no_headers() {
    let write_header =
        |names: Vec<Option<&str>>, values: Vec<Option<&str>>| nervix_vm::FunctionInvocation {
            function: FunctionName::WriteHeader,
            arguments: vec![
                VmTypedArray::Utf8(StringArray::from(names)),
                VmTypedArray::Utf8(StringArray::from(values)),
            ],
            span: VmSpan { start: 0, end: 1 },
        };
    let headers = [write_header(
        vec![Some("tenant"), None],
        vec![Some("acme"), Some("beta")],
    )];

    assert_eq!(
        emitter_headers_from_invocations(&headers, 0, Some(HeaderValues::Text))
            .expect("the first row writes its header"),
        EmitterHeaders::Text(vec![("tenant".to_string(), "acme".to_string())])
    );
    let cases = [
        (
            emitter_headers_from_invocations(&headers, 1, Some(HeaderValues::Text)),
            EmitterHeaderError::NullArgument,
        ),
        (
            emitter_headers_from_invocations(&headers, 2, Some(HeaderValues::Text)),
            EmitterHeaderError::MissingRow { row: 2 },
        ),
        (
            emitter_headers_from_invocations(
                &[nervix_vm::FunctionInvocation {
                    function: FunctionName::Lower,
                    arguments: Vec::new(),
                    span: VmSpan { start: 0, end: 1 },
                }],
                0,
                Some(HeaderValues::Text),
            ),
            EmitterHeaderError::UnsupportedInvocation {
                function: FunctionName::Lower,
            },
        ),
        (
            emitter_headers_from_invocations(
                &[nervix_vm::FunctionInvocation {
                    function: FunctionName::WriteHeader,
                    arguments: vec![VmTypedArray::Utf8(StringArray::from(vec!["tenant"]))],
                    span: VmSpan { start: 0, end: 1 },
                }],
                0,
                Some(HeaderValues::Text),
            ),
            EmitterHeaderError::ArgumentTypes,
        ),
        (
            emitter_headers_from_invocations(
                &[nervix_vm::FunctionInvocation {
                    function: FunctionName::WriteHeader,
                    arguments: vec![
                        VmTypedArray::Utf8(StringArray::from(vec!["user"])),
                        VmTypedArray::Int64(arrow_array::Int64Array::from(vec![7])),
                    ],
                    span: VmSpan { start: 0, end: 1 },
                }],
                0,
                Some(HeaderValues::Text),
            ),
            EmitterHeaderError::ArgumentTypes,
        ),
    ];
    for (outcome, expected) in cases {
        let error = outcome.expect_err("the row has no headers");
        assert_eq!(error.current_context(), &expected);
    }
}

/// Typed header values an emitter entity declares: a STRING, an I64 and BYTES.
const TYPED_HEADER_VALUES: &[nervix_models::HeaderValueType] = &[
    nervix_models::HeaderValueType {
        ty: nervix_models::PropertyType::String,
        carried_as: "String",
    },
    nervix_models::HeaderValueType {
        ty: nervix_models::PropertyType::I64,
        carried_as: "Number",
    },
    nervix_models::HeaderValueType {
        ty: nervix_models::PropertyType::Bytes,
        carried_as: "Binary",
    },
];

/// A `write_header` invocation writing `name` for every row with the values `values`.
fn typed_write_header(name: &str, values: VmTypedArray) -> nervix_vm::FunctionInvocation {
    let names = vec![Some(name); values.len()];
    nervix_vm::FunctionInvocation {
        function: FunctionName::WriteHeader,
        arguments: vec![VmTypedArray::Utf8(StringArray::from(names)), values],
        span: VmSpan { start: 0, end: 1 },
    }
}

#[test]
fn typed_header_invocations_give_each_row_its_values_in_their_exact_types() {
    let headers = [
        typed_write_header(
            "tenant",
            VmTypedArray::Utf8(StringArray::from(vec![Some("acme"), Some("beta")])),
        ),
        typed_write_header(
            "user",
            VmTypedArray::Int64(arrow_array::Int64Array::from(vec![Some(7), None])),
        ),
        typed_write_header(
            "digest",
            VmTypedArray::Binary(arrow_array::BinaryArray::from(vec![
                Some(&[0x00, 0xff][..]),
                Some(&[0x10][..]),
            ])),
        ),
    ];
    let declared = Some(HeaderValues::Typed(TYPED_HEADER_VALUES));

    assert_eq!(
        emitter_headers_from_invocations(&headers, 0, declared)
            .expect("the first row writes every header"),
        EmitterHeaders::Typed(vec![
            TypedRecordHeader {
                name: "tenant".to_string(),
                value: PropertyValue::String("acme".to_string()),
            },
            TypedRecordHeader {
                name: "user".to_string(),
                value: PropertyValue::I64(7),
            },
            TypedRecordHeader {
                name: "digest".to_string(),
                value: PropertyValue::Bytes(vec![0x00, 0xff]),
            },
        ])
    );
    let null_value = emitter_headers_from_invocations(&headers, 1, declared)
        .expect_err("the second row's user is NULL");
    assert_eq!(
        null_value.current_context(),
        &EmitterHeaderError::NullArgument
    );
    let float = [typed_write_header(
        "ratio",
        VmTypedArray::Float64(arrow_array::Float64Array::from(vec![0.5])),
    )];
    let undeclared = emitter_headers_from_invocations(&float, 0, declared)
        .expect_err("no header value is a float");
    assert_eq!(
        undeclared.current_context(),
        &EmitterHeaderError::ArgumentTypes
    );
}

#[test]
fn every_typed_register_becomes_the_property_value_of_its_exact_type() {
    let cases = [
        (
            VmTypedArray::UInt8(arrow_array::UInt8Array::from(vec![8])),
            PropertyValue::U8(8),
        ),
        (
            VmTypedArray::Int8(arrow_array::Int8Array::from(vec![-8])),
            PropertyValue::I8(-8),
        ),
        (
            VmTypedArray::UInt16(arrow_array::UInt16Array::from(vec![16])),
            PropertyValue::U16(16),
        ),
        (
            VmTypedArray::Int16(arrow_array::Int16Array::from(vec![-16])),
            PropertyValue::I16(-16),
        ),
        (
            VmTypedArray::UInt32(arrow_array::UInt32Array::from(vec![32])),
            PropertyValue::U32(32),
        ),
        (
            VmTypedArray::Int32(arrow_array::Int32Array::from(vec![-32])),
            PropertyValue::I32(-32),
        ),
        (
            VmTypedArray::UInt64(arrow_array::UInt64Array::from(vec![u64::MAX])),
            PropertyValue::U64(u64::MAX),
        ),
        (
            VmTypedArray::Boolean(arrow_array::BooleanArray::from(vec![true])),
            PropertyValue::Bool(true),
        ),
        (
            VmTypedArray::Datetime(arrow_array::TimestampNanosecondArray::from(vec![42])),
            PropertyValue::Datetime(Timestamp::from_unix_nanos(42)),
        ),
    ];
    let declared = Some(HeaderValues::Typed(TYPED_HEADER_VALUES));

    for (values, expected) in cases {
        let headers = [typed_write_header("value", values)];
        let written = emitter_headers_from_invocations(&headers, 0, declared)
            .expect("a non-null value of a typed register is a header value");
        assert_eq!(
            written,
            EmitterHeaders::Typed(vec![TypedRecordHeader {
                name: "value".to_string(),
                value: expected,
            }])
        );
    }
}

#[test]
fn a_planned_failure_names_the_program_by_its_clause() {
    for (operation, name) in [
        (MessageErrorOperation::SourceWhere, "FROM WHERE"),
        (MessageErrorOperation::FilterWhere, "FILTER WHERE"),
        (MessageErrorOperation::RouteWhere, "ROUTE WHERE"),
        (MessageErrorOperation::Set, "FILTER-MAP"),
        (MessageErrorOperation::BranchSet, "branch construction"),
        (MessageErrorOperation::Inferencer, "inferencer"),
    ] {
        assert_eq!(ProgramName::of(&operation).to_string(), name);
    }
}
