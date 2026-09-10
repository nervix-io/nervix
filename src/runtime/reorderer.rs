use super::*;

pub(super) fn reorder_key_part(array: &VmTypedArray, row: usize) -> ReorderKeyPart {
    match array {
        VmTypedArray::UInt8(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::UInt64(u64::from(array.value(row)))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::UInt16(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::UInt64(u64::from(array.value(row)))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::UInt32(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::UInt64(u64::from(array.value(row)))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::UInt64(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::UInt64(array.value(row))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Int8(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Int64(i64::from(array.value(row)))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Int16(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Int64(i64::from(array.value(row)))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Int32(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Int64(i64::from(array.value(row)))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Int64(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Int64(array.value(row))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Float32(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Float64(OrderedFloat(f64::from(array.value(row))))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Float64(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Float64(OrderedFloat(array.value(row)))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Boolean(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Boolean(array.value(row))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Utf8(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Utf8(array.value(row).to_string())
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Datetime(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Datetime(array.value(row))
            } else {
                ReorderKeyPart::Null
            }
        }
        VmTypedArray::Generic(_) => ReorderKeyPart::Null,
        VmTypedArray::Uninitialized { .. } => ReorderKeyPart::Null,
    }
}

pub(super) struct ReordererFlushContext<'a> {
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) input_relays: &'a [RelayName],
    pub(super) materialized_state: &'a [nervix_models::MaterializedStateDependency],
    pub(super) execution_now: Timestamp,
}

pub(super) async fn flush_branch_reorderer_output(
    context: ReordererFlushContext<'_>,
    output_buffer: &mut ReordererOutputBuffer,
    output_index: usize,
) {
    let graph = context.graph;
    let node_kind = context.node_kind;
    let processor = context.processor;
    let error_policies = context.error_policies;
    let output_routes = context.output_routes;
    let input_relays = context.input_relays;
    let materialized_state = context.materialized_state;
    let execution_now = context.execution_now;
    let branch = context.branch;
    output_routes.routes[output_index].clear_flush_deadline();

    if output_buffer.is_empty() {
        return;
    }
    let batch = match output_buffer.take_ordered_batch() {
        Ok(batch) => batch,
        Err(failure) => {
            let failure = *failure;
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                failure.batches.iter().flat_map(|batch| batch.acks.iter()),
                format!(
                    "reorderer '{}' failed to order buffered Arrow batches: {}",
                    processor.as_str(),
                    failure.error
                ),
            );
            output_routes.routes[output_index].clear_flush_deadline();
            return;
        }
    };
    if let Some(acks) = dispatch_processor_output(
        ProcessorOutputDispatchContext {
            graph,
            branch,
            node_kind,
            source_kind: ModelKind::Reorderer,
            processor,
            error_policies,
            input_relays,
            filter_source: ProcessorOutputFilterSource::InputRelays,
            materialized_state: ProcessorMaterializedState::ResolvedAtDispatch(materialized_state),
            execution_now,
        },
        output_routes,
        batch,
        output_index,
    )
    .await
    {
        for ack in acks {
            ack.ack_success();
        }
    }
    output_routes.routes[output_index].clear_flush_deadline();
}

#[cfg(test)]
mod tests {
    use nervix_models::{ParseAsType, Timestamp};
    use tokio::time::{Duration, timeout};
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime_ack::{AckOutcome, AckSet},
        runtime_schema::{RuntimeRecordMetadata, RuntimeValue, test_runtime_row},
    };
    #[tokio::test]
    async fn reorderer_buffer_applies_one_columnar_permutation_to_batches_and_sidecars() {
        /// One row fed into the reorderer buffer: the sequence it carries, the watermark it arrived
        /// with, and the ACKs the reordered output must keep aligned with it.
        struct ReorderedRow {
            sequence: u32,
            ingested_at: i64,
            acks: AckSet,
        }

        let schema = test_schema(&[("sequence", ParseAsType::U32)]);
        let batch = |rows: Vec<ReorderedRow>| {
            RelayRecordBatch::from_messages(
                schema.clone(),
                rows.into_iter()
                    .map(|row| RelayMessage {
                        key: None,
                        record: test_runtime_row([(
                            "sequence".to_string(),
                            RuntimeValue::U32(row.sequence),
                        )])
                        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(row.ingested_at)),
                        acks: row.acks,
                    })
                    .collect(),
            )
            .expect("test relay batch should build")
        };
        let (first_ordered_acks, first_ordered_completion) = AckSet::root();
        let mut buffer = ReordererOutputBuffer::default();
        buffer.push(
            batch(vec![
                ReorderedRow {
                    sequence: 3,
                    ingested_at: 300,
                    acks: AckSet::empty(),
                },
                ReorderedRow {
                    sequence: 1,
                    ingested_at: 100,
                    acks: first_ordered_acks,
                },
            ]),
            Arc::new(vec![
                ReordererRowOrder {
                    key: vec![ReorderKeyPart::UInt64(3)],
                    arrival_sequence: 0,
                },
                ReordererRowOrder {
                    key: vec![ReorderKeyPart::UInt64(1)],
                    arrival_sequence: 1,
                },
            ]),
            Timestamp::from_unix_nanos(10),
        );
        buffer.push(
            batch(vec![
                ReorderedRow {
                    sequence: 2,
                    ingested_at: 200,
                    acks: AckSet::empty(),
                },
                ReorderedRow {
                    sequence: 1,
                    ingested_at: 101,
                    acks: AckSet::empty(),
                },
            ]),
            Arc::new(vec![
                ReordererRowOrder {
                    key: vec![ReorderKeyPart::UInt64(2)],
                    arrival_sequence: 2,
                },
                ReordererRowOrder {
                    key: vec![ReorderKeyPart::UInt64(1)],
                    arrival_sequence: 3,
                },
            ]),
            Timestamp::from_unix_nanos(20),
        );

        let ordered = buffer
            .take_ordered_batch()
            .expect("buffered Arrow batches should reorder");

        let sequences = (0..ordered.batch.batch().num_rows())
            .map(|row| {
                ordered
                    .batch
                    .value(row, "sequence")
                    .expect("sequence should decode")
                    .expect("sequence should be initialized")
            })
            .collect::<Vec<_>>();
        let watermarks = ordered
            .metadata
            .iter()
            .map(RuntimeRecordMetadata::ingested_at_low_watermark)
            .collect::<Vec<_>>();

        assert_eq!(
            sequences,
            [
                RuntimeValue::U32(1),
                RuntimeValue::U32(1),
                RuntimeValue::U32(2),
                RuntimeValue::U32(3),
            ]
        );
        assert_eq!(
            watermarks,
            [
                Timestamp::from_unix_nanos(100),
                Timestamp::from_unix_nanos(101),
                Timestamp::from_unix_nanos(200),
                Timestamp::from_unix_nanos(300),
            ]
        );
        assert_eq!(ordered.acks.len(), 4);
        ordered.acks[0].ack_success();
        assert_eq!(
            timeout(Duration::from_secs(1), first_ordered_completion.wait())
                .await
                .expect("the first reordered row should retain its source ACK"),
            AckOutcome::Ack
        );
        assert!(buffer.is_empty());
        assert_eq!(buffer.estimated_bytes(), 0);
    }

    #[tokio::test]
    async fn reorderer_key_program_evaluates_direct_u32_field() {
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
        )
        .expect("reorderer key program should compile");
        let records = vec![
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("sequence".to_string(), RuntimeValue::U32(3)),
                (
                    "payload".to_string(),
                    RuntimeValue::String("third".to_string()),
                ),
            ]),
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("sequence".to_string(), RuntimeValue::U32(1)),
                (
                    "payload".to_string(),
                    RuntimeValue::String("first".to_string()),
                ),
            ]),
        ];
        let input = vm_input_from_test_rows(&records, &program.program.input_schema)
            .expect("VM input batch should build");
        let output = execute_program_with_selection_in_context(
            &program.program,
            &input,
            &VmExecutionContext {
                now: Timestamp::from_unix_nanos(1),
                injector: None,
            },
        )
        .await
        .expect("reorderer key program should execute");

        assert_eq!(program.key_count, 1);
        assert_eq!(
            program.key_column_offset,
            output
                .batch
                .columns()
                .len()
                .checked_sub(1)
                .expect("the generated batch must hold at least one column")
        );
        assert_eq!(
            reorder_key_part(output.batch.column(program.key_column_offset), 0),
            ReorderKeyPart::UInt64(3)
        );
        assert_eq!(
            reorder_key_part(output.batch.column(program.key_column_offset), 1),
            ReorderKeyPart::UInt64(1)
        );
    }
}
