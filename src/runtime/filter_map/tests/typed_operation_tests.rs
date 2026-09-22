use super::*;

#[tokio::test]
async fn filter_evaluation_uses_the_typed_operation_when_the_error_site_is_unmapped() {
    let input_schema = test_schema(&[
        ("amount", ParseAsType::I64),
        ("denominator", ParseAsType::I64),
    ]);
    let output_schema = test_schema(&[("amount", ParseAsType::I64)]);
    let mut program = compile_processor_output_filter_map_program(
        RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &named("route_filter"),
        },
        &[named("amounts")],
        &named("filtered_amounts"),
        &construction("SET amount = input.amount / input.denominator"),
        RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        None,
        RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &ResolvedBranching::unbranched(),
            udfs: None,
        },
    )
    .expect("the route filter must compile")
    .expect("the route filter must produce a program");
    program.error_sites.clear();
    let batch = RelayRecordBatch::from_messages(
        input_schema,
        vec![RelayMessage {
            key: None,
            record: test_runtime_row([
                ("amount".to_string(), RuntimeValue::I64(10)),
                ("denominator".to_string(), RuntimeValue::I64(0)),
            ]),
            acks: AckSet::empty(),
        }],
    )
    .expect("the source record must build");

    let plan = plan_filter_map_messages(
        "processor",
        &named::<ModelName>("route_filter"),
        MessageErrorOperation::RouteWhere,
        &program,
        batch,
        Timestamp::from_unix_nanos(1),
        &HashMap::default(),
    )
    .await
    .expect("the invalid output must become a planned message error");

    let [error] = plan.message_errors.as_slice() else {
        panic!("expected exactly one planned message error");
    };
    assert_eq!(error.error.operation, MessageErrorOperation::RouteWhere);
}

#[tokio::test]
async fn filter_predicate_evaluation_error_becomes_a_planned_message_error() {
    let input_schema = test_schema(&[
        ("amount", ParseAsType::I64),
        ("denominator", ParseAsType::I64),
    ]);
    let program = compile_scoped_filter_program(
        RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &named("input_filter"),
        },
        Some(&expression("input.amount / input.denominator > 0")),
        RuntimeVmSchema {
            schema: input_schema.arrow_schema(),
            sensitivity: VmSchemaSensitivity::default(),
        },
        MessageErrorOperation::FilterWhere,
        RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &ResolvedBranching::unbranched(),
            udfs: None,
        },
        RuntimeFilterScope::Source {
            namespace: "input",
            allow_header_reads: false,
            allow_metadata: false,
        },
    )
    .expect("the input filter must compile")
    .expect("the input filter must produce a program");
    let batch = RelayRecordBatch::from_messages(
        input_schema,
        vec![RelayMessage {
            key: None,
            record: test_runtime_row([
                ("amount".to_string(), RuntimeValue::I64(10)),
                ("denominator".to_string(), RuntimeValue::I64(0)),
            ]),
            acks: AckSet::empty(),
        }],
    )
    .expect("the source record must build");

    let plan = plan_filter_map_messages(
        "processor",
        &named::<ModelName>("input_filter"),
        MessageErrorOperation::FilterWhere,
        &program,
        batch,
        Timestamp::from_unix_nanos(1),
        &HashMap::default(),
    )
    .await
    .expect("the predicate failure must become a planned message error");

    assert!(plan.batch.is_none());
    let [error] = plan.message_errors.as_slice() else {
        panic!("expected exactly one planned message error");
    };
    assert_eq!(error.error.operation, MessageErrorOperation::FilterWhere);
}
