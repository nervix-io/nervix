use ahash::HashMap;
use nervix_models::{
    AckMode, CreateEmitter, CreateSchema, EmitterEntity, ErrorPolicies, FieldPath, KafkaEmitter,
    KafkaTopicEmitter, MessageErrorOperation, ModelName, ParseAsType, ProcessorInputs, RetryPolicy,
    Timestamp, ZeroMqEmitter, ZeroMqPushEmitter,
};
use nervix_primitives::sync::Arc;
use nonzero_ext::nonzero;
use ordered_float::OrderedFloat;

use super::*;
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{
        RuntimeRecordBatch, RuntimeRow, RuntimeValue, RuntimeValueKind, compile_schema,
        test_runtime_row,
    },
};

fn validation_filter_map_program(
    schema: &Arc<CompiledSchema>,
) -> CompiledProgramWithMaterializedInterest {
    compile_processor_output_filter_map_program(
        &named("validate_filter_map"),
        &[named("input_records")],
        &named("output_records"),
        &construction("INHERIT ALL"),
        RuntimeVmSchemaPair {
            input: schema.arrow_schema(),
            input_sensitivity: VmSchemaSensitivity::default(),
            output: schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        None,
        RuntimeVmCompileContext {
            settings: nervix_vm::VmSettings::default(),
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &ResolvedBranching::unbranched(),
            udfs: None,
        },
    )
    .verified("the test construction is valid for the matching schemas")
    .verified("INHERIT ALL requires a filter-map program")
}

#[nervix_primitives::test]
async fn filter_map_batch_validates_every_sidecar_length() {
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let program = validation_filter_map_program(&schema);
    let row = test_runtime_row([("value".to_string(), RuntimeValue::I64(7))]);
    let carrier = row.one_row_batch();
    let empty_carrier = carrier
        .slice(0, 0)
        .verified("a zero-length slice starts within the one-row batch");
    let metadata = RecordMetadataColumns::from_rows([row.metadata().clone()]);
    let empty_metadata = RecordMetadataColumns::from_rows([]);
    let keys = [None];
    let side_inputs = HashMap::default();
    let outcomes = evaluate_filter_map_on_batch(
        &Executor::default(),
        &SharedRemoteAckTiming::default(),
        &program,
        FilterMapOutcomeInputs {
            carrier: &empty_carrier,
            record_metadata: &empty_metadata,
            keys: &[],
            filter_map_metadata: None,
            side_inputs: &side_inputs,
        },
        Timestamp::from_unix_nanos(1),
    )
    .await
    .verified("empty batches do not require sidecar rows");
    assert!(outcomes.is_empty());

    let error = evaluate_filter_map_on_batch(
        &Executor::default(),
        &SharedRemoteAckTiming::default(),
        &program,
        FilterMapOutcomeInputs {
            carrier: &carrier,
            record_metadata: &empty_metadata,
            keys: &keys,
            filter_map_metadata: None,
            side_inputs: &side_inputs,
        },
        Timestamp::from_unix_nanos(1),
    )
    .await
    .err()
    .verified("the metadata sidecar is intentionally one row short");
    assert_eq!(
        error.current_context(),
        &PlannedGeneralError::SidecarRowCount {
            operation: MessageErrorOperation::Set,
            sidecar: PlannedSidecar::RuntimeMetadata,
            expected: 1,
            found: 0,
        }
    );
    assert_eq!(
        error.to_string(),
        "FILTER-MAP received 0 runtime metadata rows for 1 records"
    );

    let error = evaluate_filter_map_on_batch(
        &Executor::default(),
        &SharedRemoteAckTiming::default(),
        &program,
        FilterMapOutcomeInputs {
            carrier: &carrier,
            record_metadata: &metadata,
            keys: &[],
            filter_map_metadata: None,
            side_inputs: &side_inputs,
        },
        Timestamp::from_unix_nanos(1),
    )
    .await
    .err()
    .verified("the key sidecar is intentionally one row short");
    assert_eq!(
        error.current_context(),
        &PlannedGeneralError::SidecarRowCount {
            operation: MessageErrorOperation::Set,
            sidecar: PlannedSidecar::BranchKeys,
            expected: 1,
            found: 0,
        }
    );

    let empty_ingest_metadata = ingest_metadata_for_test(IngestMetadataProjection::none(), &[]);
    let error = evaluate_filter_map_on_batch(
        &Executor::default(),
        &SharedRemoteAckTiming::default(),
        &program,
        FilterMapOutcomeInputs {
            carrier: &carrier,
            record_metadata: &metadata,
            keys: &keys,
            filter_map_metadata: Some(&empty_ingest_metadata),
            side_inputs: &side_inputs,
        },
        Timestamp::from_unix_nanos(1),
    )
    .await
    .err()
    .verified("the ingest-metadata sidecar is intentionally one row short");
    assert_eq!(
        error.current_context(),
        &PlannedGeneralError::SidecarRowCount {
            operation: MessageErrorOperation::Set,
            sidecar: PlannedSidecar::IngestMetadata,
            expected: 1,
            found: 0,
        }
    );
}

#[nervix_primitives::test]
async fn accepted_work_route_keeps_every_row_while_the_data_class_is_full() {
    let executor = single_worker_executor();
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let program = validation_filter_map_program(&schema);
    let carrier = schema
        .batch_from_test_rows(
            (0..1025).map(|value| [("value".to_owned(), RuntimeValue::I64(value))]),
        )
        .assured("the large batch has the route's input schema");
    let now = Timestamp::from_unix_nanos(1);
    let metadata = RecordMetadataColumns::from_rows((0..1025).map(|_| {
        crate::runtime_schema::RuntimeRecordMetadata::from_ingested_at_watermarks(now, now)
    }));
    let keys = vec![None; 1025];
    let side_inputs = HashMap::default();
    let outcomes = accepted_work_after_cpu_room(
        &executor,
        nervix_execution::CpuClass::Data,
        evaluate_filter_map_on_batch(
            &executor,
            &SharedRemoteAckTiming::default(),
            &program,
            FilterMapOutcomeInputs {
                carrier: &carrier,
                record_metadata: &metadata,
                keys: &keys,
                filter_map_metadata: None,
                side_inputs: &side_inputs,
            },
            now,
        ),
    )
    .await
    .assured("a full worker class cannot fail an accepted route");
    assert_eq!(outcomes.len(), 1025);
    for (value, outcome) in outcomes.into_iter().enumerate() {
        let SingleRecordFilterMapOutcome::Output(row) = outcome else {
            panic!("every input row passes the identity route");
        };
        assert_eq!(
            row_value(&row, "value"),
            Some(RuntimeValue::I64(
                i64::try_from(value).assured("the test value fits I64")
            ))
        );
    }
}

#[test]
fn scalar_appenders_preserve_typed_mismatch_and_range_errors() {
    let wrong_value = RuntimeValue::String("not a scalar match".to_string());

    let field = arrow_schema::Field::new("f32", ArrowDataType::Float32, false);
    let error = append_filter_map_f32(&mut Float32Builder::new(), &wrong_value, &field)
        .err()
        .verified("a STRING cannot be appended to an F32 builder");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::RuntimeValueTypeMismatch {
            expected: ParseAsType::F32,
            found: RuntimeValueKind::String,
            ..
        }
    ));

    let field = arrow_schema::Field::new("f64", ArrowDataType::Float64, false);
    let error = append_filter_map_f64(&mut Float64Builder::new(), &wrong_value, &field)
        .err()
        .verified("a STRING cannot be appended to an F64 builder");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::RuntimeValueTypeMismatch {
            expected: ParseAsType::F64,
            found: RuntimeValueKind::String,
            ..
        }
    ));

    let field = arrow_schema::Field::new("active", ArrowDataType::Boolean, false);
    let error = append_filter_map_bool(&mut BooleanBuilder::new(), &wrong_value, &field)
        .err()
        .verified("a STRING cannot be appended to a BOOL builder");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::RuntimeValueTypeMismatch {
            expected: ParseAsType::Bool,
            found: RuntimeValueKind::String,
            ..
        }
    ));

    let field = arrow_schema::Field::new("text", ArrowDataType::Utf8, false);
    let integer = RuntimeValue::I64(7);
    let error = append_filter_map_string(&mut StringBuilder::new(), &integer, &field)
        .err()
        .verified("an I64 cannot be appended to a STRING builder");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::RuntimeValueTypeMismatch {
            expected: ParseAsType::String,
            found: RuntimeValueKind::I64,
            ..
        }
    ));

    let datetime_type =
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+00:00".into()));
    let field = arrow_schema::Field::new("occurred_at", datetime_type, false);
    let error =
        append_filter_map_datetime(&mut TimestampNanosecondBuilder::new(), &wrong_value, &field)
            .err()
            .verified("a STRING cannot be appended to a DATETIME builder");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::RuntimeValueTypeMismatch {
            expected: ParseAsType::Datetime,
            found: RuntimeValueKind::String,
            ..
        }
    ));

    let outside_nanos = RuntimeValue::Datetime(
        chrono::FixedOffset::east_opt(0)
            .verified("zero is a valid UTC offset")
            .with_ymd_and_hms(3000, 1, 1, 0, 0, 0)
            .single()
            .verified("the test date is valid in the proleptic Gregorian calendar"),
    );
    let error = append_filter_map_datetime(
        &mut TimestampNanosecondBuilder::new(),
        &outside_nanos,
        &field,
    )
    .err()
    .verified("year 3000 is outside the signed nanosecond timestamp range");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::RuntimeValueOutOfRange {
            expected: ParseAsType::Datetime,
            ..
        }
    ));
}

#[test]
fn nested_appenders_preserve_typed_builder_and_shape_errors() {
    let item = StdArc::new(arrow_schema::Field::new(
        "item",
        ArrowDataType::UInt8,
        false,
    ));
    let list_type = ArrowDataType::List(item.clone());
    let list_field = arrow_schema::Field::new("items", list_type.clone(), false);
    let mut wrong_builder = BooleanBuilder::new();
    let error = append_filter_map_nested_value(&mut wrong_builder, &list_type, None, &list_field)
        .err()
        .verified("a BOOL builder cannot accept an Arrow LIST");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::ArrowBuilderTypeMismatch { .. }
    ));

    let mut list_builder = make_builder(&list_type, 1);
    let wrong_value = RuntimeValue::Bool(true);
    let error = append_filter_map_nested_value(
        list_builder.as_mut(),
        &list_type,
        Some(&wrong_value),
        &list_field,
    )
    .err()
    .verified("a BOOL value cannot initialize a runtime VEC");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::RuntimeValueTypeMismatch {
            expected: ParseAsType::Vec { .. },
            found: RuntimeValueKind::Bool,
            ..
        }
    ));

    let fixed_type = ArrowDataType::FixedSizeList(item.clone(), 2);
    let fixed_field = arrow_schema::Field::new("pair", fixed_type.clone(), false);
    let mut wrong_builder = BooleanBuilder::new();
    let error = append_filter_map_nested_value(&mut wrong_builder, &fixed_type, None, &fixed_field)
        .err()
        .verified("a BOOL builder cannot accept an Arrow fixed-size list");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::ArrowBuilderTypeMismatch { .. }
    ));

    let mut fixed_builder = make_builder(&fixed_type, 1);
    let short_value = RuntimeValue::Array(vec![RuntimeValue::U8(1)]);
    let error = append_filter_map_nested_value(
        fixed_builder.as_mut(),
        &fixed_type,
        Some(&short_value),
        &fixed_field,
    )
    .err()
    .verified("one value cannot initialize a two-value runtime ARRAY");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::RuntimeArrayLengthMismatch { found: 1, .. }
    ));

    let mut fixed_builder = make_builder(&fixed_type, 1);
    let error = append_filter_map_nested_value(
        fixed_builder.as_mut(),
        &fixed_type,
        Some(&wrong_value),
        &fixed_field,
    )
    .err()
    .verified("a BOOL value cannot initialize a runtime ARRAY");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::RuntimeValueTypeMismatch {
            expected: ParseAsType::Array { .. },
            found: RuntimeValueKind::Bool,
            ..
        }
    ));

    let zero_type = ArrowDataType::FixedSizeList(item.clone(), 0);
    let zero_field = arrow_schema::Field::new("empty", zero_type.clone(), false);
    let mut zero_builder = make_builder(&zero_type, 1);
    let error =
        append_filter_map_nested_value(zero_builder.as_mut(), &zero_type, None, &zero_field)
            .err()
            .verified("a zero-length fixed-size list is not a runtime ARRAY");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::EmptyFixedSizeList { len: 0 }
    ));

    let negative_type = ArrowDataType::FixedSizeList(item.clone(), -1);
    let negative_field = arrow_schema::Field::new("negative", negative_type.clone(), false);
    let values_builder = make_builder(&ArrowDataType::UInt8, 0);
    let mut negative_builder: Box<dyn ArrayBuilder> =
        Box::new(FixedSizeListBuilder::with_capacity(values_builder, -1, 0).with_field(item));
    let error = append_filter_map_nested_value(
        negative_builder.as_mut(),
        &negative_type,
        None,
        &negative_field,
    )
    .err()
    .verified("a negative fixed-size list length is not a runtime ARRAY");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::EmptyFixedSizeList { len: -1 }
    ));

    let unsupported_type = ArrowDataType::Binary;
    let unsupported_field = arrow_schema::Field::new("binary", unsupported_type.clone(), false);
    let mut builder = BooleanBuilder::new();
    let error =
        append_filter_map_nested_value(&mut builder, &unsupported_type, None, &unsupported_field)
            .err()
            .verified("binary runtime values have no scalar representation");
    assert!(matches!(
        error.current_context(),
        RuntimeSchemaError::UnsupportedArrowType {
            data_type: ArrowDataType::Binary
        }
    ));
}

#[nervix_primitives::test]
async fn filter_map_can_read_branch_namespace() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
        ("branch_tenant", ParseAsType::String),
    ]);
    let branching = test_branching(&[("tenant", ParseAsType::String)]);
    let program = compile_processor_output_filter_map_program(
        &named("project_notifications"),
        &[named("notifications")],
        &named("projected_notifications"),
        &construction(
            "INHERIT ALL SET branch_tenant = branch.tenant, amount = amount + 1 WHERE \
             branch.tenant = output.tenant",
        ),
        RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: VmSchemaSensitivity::default(),
            output: input_schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        None,
        RuntimeVmCompileContext {
            settings: nervix_vm::VmSettings::default(),
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &branching,
            udfs: None,
        },
    )
    .expect("filter-map should compile")
    .expect("program should exist");

    let (acks, _completion) = AckSet::root();
    let batch = RelayRecordBatch::from_messages(
        input_schema,
        vec![RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("amount".to_string(), RuntimeValue::I64(7)),
                (
                    "branch_tenant".to_string(),
                    RuntimeValue::String("".to_string()),
                ),
            ]),
            acks,
        }],
    )
    .expect("batch should build");

    let plan = plan_filter_map_messages(
        ProgramRun {
            executor: &Executor::default(),
            now: Timestamp::from_unix_nanos(1),
            ack_timing: &SharedRemoteAckTiming::default(),
        },
        "deduplicator",
        &named::<ModelName>("project_notifications"),
        MessageErrorOperation::Set,
        &program,
        batch,
        &HashMap::default(),
    )
    .await
    .expect("filter-map planning should succeed");

    assert!(
        plan.message_errors.is_empty(),
        "projection should not produce message errors: {:?}",
        plan.message_errors
            .iter()
            .map(|error| error.error.message.as_str())
            .collect::<Vec<_>>()
    );

    let messages = plan
        .batch
        .expect("filter-map should produce a batch")
        .try_into_messages()
        .expect("filter-map batch should convert to messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(
        row_value(&messages[0].record, "branch_tenant"),
        Some(RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(
        row_value(&messages[0].record, "amount"),
        Some(RuntimeValue::I64(8))
    );
}

#[nervix_primitives::test]
async fn projection_can_read_branch_namespace() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("amount", ParseAsType::I64),
    ]);
    let output_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
        ("branch_tenant", ParseAsType::String),
    ]);
    let branching = test_branching(&[("tenant", ParseAsType::String)]);
    let program = compile_processor_output_filter_map_program(
        &named("project_notifications"),
        &[named("notifications")],
        &named("projected_notifications"),
        &construction(
            "INHERIT tenant, amount SET branch_tenant = branch.tenant, amount = amount + 1 WHERE \
             branch.tenant = output.tenant",
        ),
        RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        None,
        RuntimeVmCompileContext {
            settings: nervix_vm::VmSettings::default(),
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &branching,
            udfs: None,
        },
    )
    .expect("filter-map should compile")
    .expect("program should exist");

    let (acks, _completion) = AckSet::root();
    let batch = RelayRecordBatch::from_messages(
        input_schema,
        vec![RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("active".to_string(), RuntimeValue::Bool(true)),
                ("amount".to_string(), RuntimeValue::I64(7)),
            ]),
            acks,
        }],
    )
    .expect("batch should build");

    let plan = plan_filter_map_messages(
        ProgramRun {
            executor: &Executor::default(),
            now: Timestamp::from_unix_nanos(1),
            ack_timing: &SharedRemoteAckTiming::default(),
        },
        "processor",
        &named::<ModelName>("project_notifications"),
        MessageErrorOperation::Set,
        &program,
        batch,
        &HashMap::default(),
    )
    .await
    .expect("filter-map planning should succeed");

    assert!(
        plan.message_errors.is_empty(),
        "projection should not produce message errors: {:?}",
        plan.message_errors
            .iter()
            .map(|error| error.error.message.as_str())
            .collect::<Vec<_>>()
    );

    let messages = plan
        .batch
        .expect("filter-map should produce a batch")
        .try_into_messages()
        .expect("filter-map batch should convert to messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(
        row_value(&messages[0].record, "branch_tenant"),
        Some(RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(
        row_value(&messages[0].record, "amount"),
        Some(RuntimeValue::I64(8))
    );
    assert_eq!(
        messages[0].key.as_ref(),
        string_branch_key("tenant", "acme").as_ref()
    );
}

#[nervix_primitives::test]
async fn inherit_all_preserves_fixed_size_array_values_through_the_vm() {
    let schema = test_schema(&[(
        "vector",
        ParseAsType::Array {
            element: Box::new(ParseAsType::F32),
            len: nonzero!(2u32),
        },
    )]);
    let program = compile_processor_output_filter_map_program(
        &named("copy_vectors"),
        &[named("vectors")],
        &named("copied_vectors"),
        &construction("INHERIT ALL"),
        RuntimeVmSchemaPair {
            input: schema.arrow_schema(),
            input_sensitivity: VmSchemaSensitivity::default(),
            output: schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        None,
        RuntimeVmCompileContext {
            settings: nervix_vm::VmSettings::default(),
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &ResolvedBranching::unbranched(),
            udfs: None,
        },
    )
    .expect("array inheritance should compile")
    .expect("INHERIT ALL should produce a VM program");
    let expected = RuntimeValue::Array(vec![
        RuntimeValue::F32(1.25.into()),
        RuntimeValue::F32((-2.5).into()),
    ]);
    let batch = RelayRecordBatch::from_messages(
        schema,
        vec![RelayMessage {
            key: None,
            record: test_runtime_row([("vector".to_string(), expected.clone())]),
            acks: AckSet::empty(),
        }],
    )
    .expect("array input batch should build");

    let plan = plan_filter_map_messages(
        ProgramRun {
            executor: &Executor::default(),
            now: Timestamp::from_unix_nanos(1),
            ack_timing: &SharedRemoteAckTiming::default(),
        },
        "junction",
        &named::<ModelName>("copy_vectors"),
        MessageErrorOperation::Set,
        &program,
        batch,
        &HashMap::default(),
    )
    .await
    .expect("array inheritance should execute");

    assert!(plan.message_errors.is_empty());
    let output = plan.batch.expect("array output batch should exist");
    let record = output
        .runtime_row(0)
        .expect("array output row should materialize at the test boundary");
    assert_eq!(row_value(&record, "vector"), Some(expected));
}

#[nervix_primitives::test]
async fn ordered_set_error_reports_operation_index_and_previous_partial_value() {
    let input_schema = test_schema(&[
        ("amount", ParseAsType::I64),
        ("denominator", ParseAsType::I64),
    ]);
    let output_schema = test_schema(&[("amount", ParseAsType::I64)]);
    let program = compile_processor_output_filter_map_program(
        &named("calculate_amount"),
        &[named("amounts")],
        &named("calculated_amounts"),
        &construction("SET amount = input.amount, amount = amount / input.denominator"),
        RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        None,
        RuntimeVmCompileContext {
            settings: nervix_vm::VmSettings::default(),
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &ResolvedBranching::unbranched(),
            udfs: None,
        },
    )
    .expect("ordered SET should compile")
    .expect("ordered SET should produce a program");
    let batch = RelayRecordBatch::from_messages(
        input_schema,
        vec![
            RelayMessage {
                key: None,
                record: test_runtime_row([
                    ("amount".to_string(), RuntimeValue::I64(7)),
                    ("denominator".to_string(), RuntimeValue::I64(0)),
                ]),
                acks: AckSet::empty(),
            },
            RelayMessage {
                key: None,
                record: test_runtime_row([
                    ("amount".to_string(), RuntimeValue::I64(10)),
                    ("denominator".to_string(), RuntimeValue::I64(2)),
                ]),
                acks: AckSet::empty(),
            },
        ],
    )
    .expect("input batch should build");

    let plan = plan_filter_map_messages(
        ProgramRun {
            executor: &Executor::default(),
            now: Timestamp::from_unix_nanos(1),
            ack_timing: &SharedRemoteAckTiming::default(),
        },
        "junction",
        &named::<ModelName>("calculate_amount"),
        MessageErrorOperation::Set,
        &program,
        batch,
        &HashMap::default(),
    )
    .await
    .expect("a side error is a planned message error");

    let successful = plan
        .batch
        .as_ref()
        .expect("the successful row should remain in the output batch");
    assert_eq!(successful.message_count(), 1);
    let successful_record = successful
        .runtime_row(0)
        .expect("successful output row should materialize at the test boundary");
    assert_eq!(
        row_value(&successful_record, "amount"),
        Some(RuntimeValue::I64(5))
    );
    let [error] = plan.message_errors.as_slice() else {
        panic!("expected exactly one planned message error");
    };
    assert_eq!(error.error.class, MessageErrorClass::EVALUATION);
    assert_eq!(error.error.operation, MessageErrorOperation::Set);
    assert_eq!(error.error.operation_index, Some(1));
    assert_eq!(
        error
            .error
            .fields
            .iter()
            .map(FieldPath::as_str)
            .collect::<Vec<_>>(),
        vec!["input.denominator", "output.amount"]
    );
    assert_eq!(
        error.partial_output.as_ref().and_then(|output| output
            .value(0, "amount")
            .expect("partial output is readable")),
        Some(RuntimeValue::I64(7)),
        "partial output: {:?}; error: {}",
        error.partial_output,
        error.error.message
    );
}

#[path = "filter_map/tests/planned_failure_tests.rs"]
mod planned_failure_tests;
#[path = "filter_map/tests/typed_operation_tests.rs"]
mod typed_operation_tests;

#[test]
fn filter_map_rejects_missing_branch_key() {
    let schema = test_schema(&[("tenant", ParseAsType::String)]);
    let branching = test_branching(&[("region", ParseAsType::String)]);
    let error = compile_processor_output_filter_map_program(
        &named("project_notifications"),
        &[named("notifications")],
        &named("projected_notifications"),
        &construction("INHERIT ALL WHERE branch.tenant = output.tenant"),
        RuntimeVmSchemaPair {
            input: schema.arrow_schema(),
            input_sensitivity: VmSchemaSensitivity::default(),
            output: schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        None,
        RuntimeVmCompileContext {
            settings: nervix_vm::VmSettings::default(),
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &branching,
            udfs: None,
        },
    )
    .expect_err("branch namespace must reject missing keys");
    let error = format!("{error:#}");

    assert!(
        error.contains("branch.tenant") || error.contains("tenant"),
        "expected missing branch key error, got {error}"
    );
}

#[nervix_primitives::test]
async fn emitter_invocations_run_after_set_for_selected_rows_and_append_headers() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("raw", ParseAsType::String),
        ("active", ParseAsType::Bool),
    ]);
    let output_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("normalized", ParseAsType::String),
    ]);
    let emitter = CreateEmitter {
        labels: Default::default(),
        instances: nervix_models::EmitterInstances::ONE,
        name: named("kafka_notifications"),
        from: ProcessorInputs::single(named("notifications")),
        body: nervix_models::EmitterBody::Codec {
            codec: named("notification_codec"),
        },
        envelope: Box::new(nervix_models::EmitterEnvelope::default()),
        connector: Box::new(EmitterEntity::Kafka(KafkaEmitter::Topic(
            KafkaTopicEmitter {
                mode: nervix_models::KafkaTopicPublishingMode::NoAck {
                    retry_policy: nervix_models::EmitterRetryPolicy {
                        pacing: RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                        give_up_after: None,
                    },
                },
                client: named("kafka_main"),
                topic: named("notifications_out"),
            },
        ))),
        batch: None,
        flush_policy: FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string(),
        },
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        construction: construction(
            "INHERIT tenant SET normalized = lower(input.raw) WHERE input.active INVOKE \
             write_header(lower(\"TENANT\"),
                 input.tenant), write_header(\"route\", output.normalized), \
             write_header(\"route\", \"second\")",
        ),
        materialized_state: Vec::new(),
    };
    let route = EmitterExecutionPlan::route(
        &emitter,
        input_schema.arrow_schema().as_ref(),
        output_schema.arrow_schema().as_ref(),
    )
    .expect("emitter route must lower");
    let program = compile_emitter_filter_map_program(
        &emitter.name,
        route.as_ref(),
        RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        RuntimeVmCompileContext {
            settings: nervix_vm::VmSettings::default(),
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &ResolvedBranching::unbranched(),
            udfs: None,
        },
    )
    .expect("emitter filter-map must compile")
    .expect("program must exist");
    let mut unsupported_emitter = emitter.clone();
    *unsupported_emitter.connector =
        EmitterEntity::ZeroMq(ZeroMqEmitter::Push(ZeroMqPushEmitter {
            mode: nervix_models::ZeroMqPushPublishingMode::NoAck {
                retry_policy: nervix_models::EmitterRetryPolicy {
                    pacing: nervix_models::RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                    give_up_after: None,
                },
            },
            client: named("zeromq_main"),
        }));
    let error = EmitterExecutionPlan::route(
        &unsupported_emitter,
        input_schema.arrow_schema().as_ref(),
        output_schema.arrow_schema().as_ref(),
    )
    .expect_err("ZeroMQ emitters must reject write_header");
    assert!(error.to_string().contains("ZEROMQ PUSH emitter"));
    let messages = [true, false]
        .into_iter()
        .map(|active| {
            let (acks, _completion) = AckSet::root();
            RelayMessage {
                key: None,
                record: test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    (
                        "raw".to_string(),
                        RuntimeValue::String("FAST-LANE".to_string()),
                    ),
                    ("active".to_string(), RuntimeValue::Bool(active)),
                ]),
                acks,
            }
        })
        .collect::<Vec<_>>();
    let batch = RelayRecordBatch::from_messages(input_schema, messages).expect("batch must build");

    let plan = plan_emitter_filter_map_batch(
        &Executor::default(),
        &SharedRemoteAckTiming::default(),
        &emitter.name,
        &program,
        batch,
        Timestamp::from_unix_nanos(1),
        &HashMap::default(),
    )
    .await
    .expect("emitter filter-map must execute");

    let output = plan
        .batch
        .expect("selected emitter output must remain a batch");
    assert_eq!(output.batch.batch().num_rows(), 1);
    let output_record = output
        .runtime_row(0)
        .expect("test may inspect the selected output row");
    assert_eq!(plan.source_rows, vec![0]);
    assert_eq!(
        row_value(&output_record, "normalized"),
        Some(RuntimeValue::String("fast-lane".to_string()))
    );
    assert_eq!(
        plan.headers,
        Some(vec![EmitterHeaders::Text(vec![
            ("tenant".to_string(), "acme".to_string()),
            ("route".to_string(), "fast-lane".to_string()),
            ("route".to_string(), "second".to_string()),
        ])])
    );
}

#[nervix_primitives::test]
async fn subscription_predicate_reports_a_typed_evaluation_error() {
    use crate::runtime::subscription_predicate::SubscriptionPredicateExecutionError;

    let schema = test_schema(&[("left", ParseAsType::I64), ("divisor", ParseAsType::I64)]);
    // `coalesce` still selects the row whose division failed, and a selected row that recorded
    // an error fails the predicate instead of being delivered.
    let where_clause = expression("coalesce(input.left / input.divisor, 1) > 0");
    let predicate = compile_subscription_predicate(
        &named("ratio_subscription"),
        &where_clause,
        SubscriptionPredicateCompileContext::new(
            schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            None,
        ),
    )
    .expect("subscription predicate must compile");
    let record = test_runtime_row([
        ("left".to_string(), RuntimeValue::I64(7)),
        ("divisor".to_string(), RuntimeValue::I64(0)),
    ]);

    let error = execute_subscription_predicate_on_record(
        &Executor::default(),
        &predicate,
        &record,
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect_err("a selected row that divided by zero must fail the predicate");

    let SubscriptionPredicateExecutionError::Evaluation { reason, .. } = error.current_context()
    else {
        panic!("unexpected subscription predicate error: {error:#}");
    };
    assert_eq!(
        reason,
        &nervix_vm::SideErrorReason::DivisionByZero(nervix_vm::DivisionOperation::Division)
    );
    assert!(
        error.current_context().to_string().starts_with(
            "predicate evaluation error division_by_zero: integer division by zero at "
        )
    );
}

#[nervix_primitives::test]
async fn subscription_predicate_evaluates_only_selected_arrow_row() {
    let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::U32)]);
    let where_clause = expression("input.value = (3 AS U32)");
    let predicate = compile_subscription_predicate(
        &named("selected_row_subscription"),
        &where_clause,
        SubscriptionPredicateCompileContext::new(
            schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            None,
        ),
    )
    .expect("subscription predicate must compile");
    let rows = [
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("value".to_string(), RuntimeValue::U32(1)),
        ]),
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("value".to_string(), RuntimeValue::U32(3)),
        ]),
    ];
    let row_batches = rows
        .iter()
        .map(RuntimeRow::one_row_batch)
        .collect::<Vec<_>>();
    let batch = RuntimeRecordBatch::concat(&row_batches.iter().collect::<Vec<_>>())
        .expect("multi-row Arrow batch should build");
    let selected = RuntimeRow::new(Arc::new(batch), 1, rows[1].metadata().clone())
        .expect("second Arrow row should be addressable");

    let selected = execute_subscription_predicate_on_record(
        &Executor::default(),
        &predicate,
        &selected,
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("selected row predicate must execute");

    assert!(selected);
}

#[nervix_primitives::test]
async fn filter_map_internal_types_roundtrip_matches_http_logic_fixture() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("u8", ParseAsType::U8),
        ("i8", ParseAsType::I8),
        ("u16", ParseAsType::U16),
        ("i16", ParseAsType::I16),
        ("u32", ParseAsType::U32),
        ("i32", ParseAsType::I32),
        ("u64", ParseAsType::U64),
        ("i64", ParseAsType::I64),
        ("f32", ParseAsType::F32),
        ("f64", ParseAsType::F64),
        ("occurred_at", ParseAsType::Datetime),
        ("raw", ParseAsType::String),
    ]);
    let output_schema = Arc::new(compile_schema(&CreateSchema {
        labels: Default::default(),
        name: named("logic_output"),
        fields: vec![
            nervix_models::SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("u8_next"),
                ty: ParseAsType::U8,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("i8_abs"),
                ty: ParseAsType::I8,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("u16_keep"),
                ty: ParseAsType::U16,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("i16_prev"),
                ty: ParseAsType::I16,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("u32_same"),
                ty: ParseAsType::U32,
                optional: true,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("i32_neg"),
                ty: ParseAsType::I32,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("u64_next"),
                ty: ParseAsType::U64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("i64_keep"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("f32_next"),
                ty: ParseAsType::F32,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("f64_keep"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("bool_copy"),
                ty: ParseAsType::Bool,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("occurred_text"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: named("occurred_copy"),
                ty: ParseAsType::Datetime,
                optional: false,
                sensitive: false,
            },
        ],
    }));
    let program = bind_ingestor_route_for_test(
        &named::<ModelName>("logic_ingestor"),
        IngestorMetadataDeclaration::NONE,
        true,
        &construction(
            "INHERIT tenant SET u8_next = input.u8 + (1 AS U8), i8_abs = abs(input.i8), u16_keep \
             = coalesce(input.u16, (0 AS U16)), i16_prev = input.i16 - (1 AS I16), u32_same = \
             coalesce(nullif(input.u32, (999 AS U32)), (0 AS U32)), i32_neg = -input.i32, \
             u64_next = input.u64 + (2 AS U64), i64_keep = input.i64, f32_next = input.f32 + (1.5 \
             AS F32), f64_keep = input.f64, bool_copy = input.active, occurred_text = \
             input.occurred_at AS STRING, occurred_copy = (input.occurred_at AS STRING) AS \
             DATETIME WHERE input.active AND input.occurred_at > ('2026-04-07T00:00:00Z' AS \
             DATETIME)",
        ),
        RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        RuntimeVmCompileContext {
            settings: nervix_vm::VmSettings::default(),
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &ResolvedBranching::unbranched(),
            udfs: None,
        },
    )
    .expect("filter-map must compile");

    let record = test_runtime_row([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        ("active".to_string(), RuntimeValue::Bool(true)),
        ("u8".to_string(), RuntimeValue::U8(5)),
        ("i8".to_string(), RuntimeValue::I8(-7)),
        ("u16".to_string(), RuntimeValue::U16(9)),
        ("i16".to_string(), RuntimeValue::I16(12)),
        ("u32".to_string(), RuntimeValue::U32(42)),
        ("i32".to_string(), RuntimeValue::I32(-11)),
        ("u64".to_string(), RuntimeValue::U64(100)),
        ("i64".to_string(), RuntimeValue::I64(-64)),
        ("f32".to_string(), RuntimeValue::F32(OrderedFloat(2.5))),
        ("f64".to_string(), RuntimeValue::F64(OrderedFloat(7.25))),
        (
            "occurred_at".to_string(),
            RuntimeValue::Datetime(
                chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z")
                    .expect("valid timestamp"),
            ),
        ),
        (
            "raw".to_string(),
            RuntimeValue::String("ignored".to_string()),
        ),
    ]);

    let output =
        execute_filter_map_for_test(&program, record, None, None, Timestamp::from_unix_nanos(1))
            .await
            .expect("filter-map must execute")
            .expect("record must not be filtered out");

    assert_eq!(
        row_value(&output, "tenant"),
        Some(RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(row_value(&output, "u8_next"), Some(RuntimeValue::U8(6)));
    assert_eq!(row_value(&output, "i8_abs"), Some(RuntimeValue::I8(7)));
    assert_eq!(row_value(&output, "u16_keep"), Some(RuntimeValue::U16(9)));
    assert_eq!(row_value(&output, "i16_prev"), Some(RuntimeValue::I16(11)));
    assert_eq!(row_value(&output, "u32_same"), Some(RuntimeValue::U32(42)));
    assert_eq!(row_value(&output, "i32_neg"), Some(RuntimeValue::I32(11)));
    assert_eq!(row_value(&output, "u64_next"), Some(RuntimeValue::U64(102)));
    assert_eq!(row_value(&output, "i64_keep"), Some(RuntimeValue::I64(-64)));
    assert_eq!(
        row_value(&output, "f32_next"),
        Some(RuntimeValue::F32(OrderedFloat(4.0)))
    );
    assert_eq!(
        row_value(&output, "f64_keep"),
        Some(RuntimeValue::F64(OrderedFloat(7.25)))
    );
    assert_eq!(
        row_value(&output, "bool_copy"),
        Some(RuntimeValue::Bool(true))
    );
    assert_eq!(
        row_value(&output, "occurred_text"),
        Some(RuntimeValue::String(
            "2026-04-07T12:34:56+00:00".to_string()
        ))
    );
    assert_eq!(
        row_value(&output, "occurred_copy"),
        Some(RuntimeValue::Datetime(
            chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z").expect("valid timestamp"),
        ))
    );
}

#[nervix_primitives::test]
async fn large_vm_batches_preserve_results_through_public_vm_api() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("sequence", ParseAsType::U32),
        ("payload", ParseAsType::String),
    ]);
    let program = compile_reorderer_program(
        &named("order_notifications"),
        &[named("incoming_notifications")],
        &[expression("input.sequence")],
        input_schema.arrow_schema(),
        None,
        nervix_vm::VmSettings::default(),
    )
    .expect("reorderer key program should compile");
    let records = (0..=VmInlineRowLimit::DEFAULT)
        .map(|sequence| {
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                (
                    "sequence".to_string(),
                    RuntimeValue::U32(
                        u32::try_from(sequence)
                            .assured("the VM inline row limit fits a u32 test field"),
                    ),
                ),
                (
                    "payload".to_string(),
                    RuntimeValue::String(format!("payload-{sequence}")),
                ),
            ])
        })
        .collect::<Vec<_>>();
    let input = vm_input_from_test_rows(&records, &program.program.input_schema)
        .expect("VM input batch should build");

    let output = execute_program_with_selection_in_context(
        &Executor::default(),
        &program.program,
        &input,
        &VmExecutionContext {
            now: Timestamp::from_unix_nanos(1),
            injector: None,
        },
    )
    .await
    .expect("large VM batch should execute");

    assert_eq!(output.batch.row_count(), VmInlineRowLimit::DEFAULT + 1);
}
