use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder, UInt8Builder,
    UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_schema::{DataType, TimeUnit};

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
        VmTypedArray::Binary(array) => {
            if array.is_valid(row) {
                ReorderKeyPart::Bytes(array.value(row).to_vec())
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

/// A key part that is not a value of the column type its key expression produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum ReorderKeyColumnError {
    #[error("a key part does not have the type its key expression produces")]
    PartType,
}

impl ReorderKeyPart {
    /// One typed key column of `data_type` holding `parts`, every part the type's own normalization.
    pub(super) fn typed_column<'a>(
        data_type: &DataType,
        parts: impl Iterator<Item = &'a ReorderKeyPart>,
        rows: usize,
    ) -> error_stack::Result<ArrayRef, ReorderKeyColumnError> {
        let mismatch = || Report::new(ReorderKeyColumnError::PartType);
        macro_rules! integers {
            ($builder:ty, $variant:ident, $integer:ty) => {{
                let mut builder = <$builder>::with_capacity(rows);
                for part in parts {
                    match part {
                        ReorderKeyPart::Null => builder.append_null(),
                        ReorderKeyPart::$variant(value) => {
                            let narrowed = <$integer>::try_from(*value).map_err(|_| mismatch())?;
                            builder.append_value(narrowed);
                        }
                        _ => return Err(mismatch()),
                    }
                }
                let array: ArrayRef = StdArc::new(builder.finish());
                Ok(array)
            }};
        }
        match data_type {
            DataType::UInt8 => integers!(UInt8Builder, UInt64, u8),
            DataType::UInt16 => integers!(UInt16Builder, UInt64, u16),
            DataType::UInt32 => integers!(UInt32Builder, UInt64, u32),
            DataType::UInt64 => integers!(UInt64Builder, UInt64, u64),
            DataType::Int8 => integers!(Int8Builder, Int64, i8),
            DataType::Int16 => integers!(Int16Builder, Int64, i16),
            DataType::Int32 => integers!(Int32Builder, Int64, i32),
            DataType::Int64 => integers!(Int64Builder, Int64, i64),
            DataType::Float32 => {
                let mut builder = Float32Builder::with_capacity(rows);
                for part in parts {
                    match part {
                        ReorderKeyPart::Null => builder.append_null(),
                        ReorderKeyPart::Float64(OrderedFloat(value)) => {
                            // A single-precision key part was widened exactly, so narrowing it back
                            // recovers the same value; anything else was never this column's part.
                            let value = *value;
                            let narrowed: f32 = value.approx_into();
                            let widened = f64::from(narrowed);
                            if widened.to_bits() != value.to_bits()
                                && !(widened.is_nan() && value.is_nan())
                            {
                                return Err(mismatch());
                            }
                            builder.append_value(narrowed);
                        }
                        _ => return Err(mismatch()),
                    }
                }
                let array: ArrayRef = StdArc::new(builder.finish());
                Ok(array)
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(rows);
                for part in parts {
                    match part {
                        ReorderKeyPart::Null => builder.append_null(),
                        ReorderKeyPart::Float64(OrderedFloat(value)) => {
                            builder.append_value(*value)
                        }
                        _ => return Err(mismatch()),
                    }
                }
                let array: ArrayRef = StdArc::new(builder.finish());
                Ok(array)
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(rows);
                for part in parts {
                    match part {
                        ReorderKeyPart::Null => builder.append_null(),
                        ReorderKeyPart::Boolean(value) => builder.append_value(*value),
                        _ => return Err(mismatch()),
                    }
                }
                let array: ArrayRef = StdArc::new(builder.finish());
                Ok(array)
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(rows, 0);
                for part in parts {
                    match part {
                        ReorderKeyPart::Null => builder.append_null(),
                        ReorderKeyPart::Utf8(value) => builder.append_value(value),
                        _ => return Err(mismatch()),
                    }
                }
                let array: ArrayRef = StdArc::new(builder.finish());
                Ok(array)
            }
            DataType::Binary => {
                let mut builder = BinaryBuilder::with_capacity(rows, 0);
                for part in parts {
                    match part {
                        ReorderKeyPart::Null => builder.append_null(),
                        ReorderKeyPart::Bytes(value) => builder.append_value(value),
                        _ => return Err(mismatch()),
                    }
                }
                let array: ArrayRef = StdArc::new(builder.finish());
                Ok(array)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) => {
                let mut builder = TimestampNanosecondBuilder::with_capacity(rows);
                for part in parts {
                    match part {
                        ReorderKeyPart::Null => builder.append_null(),
                        ReorderKeyPart::Datetime(value) => builder.append_value(*value),
                        _ => return Err(mismatch()),
                    }
                }
                let array: ArrayRef = StdArc::new(builder.finish().with_timezone(zone.clone()));
                Ok(array)
            }
            // Every other type reaches a key as a null part, so only nulls fit its column.
            other => {
                for part in parts {
                    if *part != ReorderKeyPart::Null {
                        return Err(mismatch());
                    }
                }
                Ok(new_null_array(other, rows))
            }
        }
    }
}

pub(super) struct ReordererFlushContext<'a> {
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) materialized_state: &'a [nervix_models::MaterializedStateDependency],
    pub(super) execution_now: Timestamp,
}

pub(super) async fn flush_branch_reorderer_output(
    context: ReordererFlushContext<'_>,
    output_buffer: &mut ReordererOutputBuffer,
    output_index: usize,
) {
    let node_kind = context.node_kind;
    let processor = context.processor;
    let error_policies = context.error_policies;
    let output_routes = context.output_routes;
    let materialized_state = context.materialized_state;
    let execution_now = context.execution_now;
    let branch = context.branch;
    output_routes.routes[output_index].clear_flush_timer();

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
                &failure.error,
            );
            output_routes.routes[output_index].clear_flush_timer();
            return;
        }
    };
    if let Some(acks) = dispatch_processor_output(
        ProcessorOutputDispatchContext {
            branch,
            node_kind,
            entity_kind: ModelKind::Reorderer,
            processor,
            error_policies,
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
    output_routes.routes[output_index].clear_flush_timer();
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::time::Duration;

    use arrow_array::{
        BinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
        cast::AsArray as _,
        types::{Float32Type, Float64Type},
    };
    use nervix_models::{ParseAsType, Timestamp};
    use nervix_primitives::{sync::Arc, time::timeout};

    use super::*;
    use crate::{
        runtime::processors::ReordererOutputBatchError,
        runtime_ack::{AckOutcome, AckSet},
        runtime_schema::{RuntimeValue, test_runtime_row},
    };

    #[test]
    fn binary_sort_keys_use_octet_order_and_keep_null_distinct() {
        let values = VmTypedArray::Binary(BinaryArray::from(vec![
            Some(&[0, 255][..]),
            Some(&[1][..]),
            None,
        ]));
        assert_eq!(
            reorder_key_part(&values, 0),
            ReorderKeyPart::Bytes(vec![0, 255])
        );
        assert_eq!(reorder_key_part(&values, 2), ReorderKeyPart::Null);
        assert!(reorder_key_part(&values, 0) < reorder_key_part(&values, 1));
    }

    /// The key column of `expected`'s type built from the parts read from each of its rows.
    fn typed_column_of_parts(expected: &ArrayRef) -> ArrayRef {
        let typed = VmTypedArray::try_from_array_ref(StdArc::clone(expected))
            .assured("every fixture column has a key type");
        let rows = expected.len();
        let mut parts = Vec::with_capacity(rows);
        for row in 0..rows {
            parts.push(reorder_key_part(&typed, row));
        }
        ReorderKeyPart::typed_column(expected.data_type(), parts.iter(), rows)
            .assured("every part was read from a column of this type")
    }

    #[test]
    fn typed_key_columns_rebuild_every_key_type_from_the_parts_read_from_it() {
        let datetime: ArrayRef = StdArc::new(
            TimestampNanosecondArray::from(vec![Some(i64::MIN), Some(0), None])
                .with_timezone("+00:00"),
        );
        assert_eq!(
            datetime.data_type(),
            &ParseAsType::Datetime.arrow_data_type()
        );
        let exact: Vec<ArrayRef> = vec![
            StdArc::new(UInt8Array::from(vec![Some(0), Some(u8::MAX), None])),
            StdArc::new(UInt16Array::from(vec![Some(0), Some(u16::MAX), None])),
            StdArc::new(UInt32Array::from(vec![Some(0), Some(u32::MAX), None])),
            StdArc::new(UInt64Array::from(vec![Some(0), Some(u64::MAX), None])),
            StdArc::new(Int8Array::from(vec![Some(i8::MIN), Some(i8::MAX), None])),
            StdArc::new(Int16Array::from(vec![Some(i16::MIN), Some(i16::MAX), None])),
            StdArc::new(Int32Array::from(vec![Some(i32::MIN), Some(i32::MAX), None])),
            StdArc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
            StdArc::new(BooleanArray::from(vec![Some(false), Some(true), None])),
            StdArc::new(StringArray::from(vec![Some(""), Some("east"), None])),
            StdArc::new(BinaryArray::from(vec![
                Some(&[][..]),
                Some(&[0, 255][..]),
                None,
            ])),
            datetime,
        ];
        for expected in &exact {
            let column = typed_column_of_parts(expected);
            assert_eq!(&*column, &**expected, "{}", expected.data_type());
        }

        // Floats compare by their bits, so negative zero, NaN and the extremes come back exactly.
        let single: ArrayRef = StdArc::new(Float32Array::from(vec![
            Some(-0.0),
            Some(f32::NAN),
            Some(f32::MIN_POSITIVE),
            Some(f32::MAX),
            None,
        ]));
        let column = typed_column_of_parts(&single);
        let restored: Vec<Option<u32>> = column
            .as_primitive::<Float32Type>()
            .iter()
            .map(|value| value.map(f32::to_bits))
            .collect();
        let original: Vec<Option<u32>> = single
            .as_primitive::<Float32Type>()
            .iter()
            .map(|value| value.map(f32::to_bits))
            .collect();
        assert_eq!(restored, original);
        let double: ArrayRef = StdArc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(f64::NAN),
            Some(f64::MIN),
            None,
        ]));
        let column = typed_column_of_parts(&double);
        let restored: Vec<Option<u64>> = column
            .as_primitive::<Float64Type>()
            .iter()
            .map(|value| value.map(f64::to_bits))
            .collect();
        let original: Vec<Option<u64>> = double
            .as_primitive::<Float64Type>()
            .iter()
            .map(|value| value.map(f64::to_bits))
            .collect();
        assert_eq!(restored, original);
    }

    #[test]
    fn a_key_part_outside_its_column_type_is_refused_and_untyped_keys_hold_only_nulls() {
        let list = ParseAsType::Vec {
            element: Box::new(ParseAsType::I64),
        }
        .arrow_data_type();
        let refused = [
            (
                DataType::UInt8,
                ReorderKeyPart::UInt64(u64::from(u8::MAX) + 1),
            ),
            (
                DataType::Int16,
                ReorderKeyPart::Int64(i64::from(i16::MIN) - 1),
            ),
            (DataType::Int64, ReorderKeyPart::UInt64(1)),
            (
                DataType::Float32,
                ReorderKeyPart::Float64(OrderedFloat(0.1)),
            ),
            (DataType::Float64, ReorderKeyPart::Int64(1)),
            (DataType::Boolean, ReorderKeyPart::Utf8("true".to_string())),
            (DataType::Utf8, ReorderKeyPart::Bytes(b"east".to_vec())),
            (DataType::Binary, ReorderKeyPart::Utf8("east".to_string())),
            (
                ParseAsType::Datetime.arrow_data_type(),
                ReorderKeyPart::Int64(0),
            ),
            (list.clone(), ReorderKeyPart::Int64(1)),
        ];
        for (data_type, part) in &refused {
            let error = ReorderKeyPart::typed_column(data_type, std::iter::once(part), 1)
                .err()
                .assured("the part is not a value of this column's type");
            assert_eq!(
                *error.current_context(),
                ReorderKeyColumnError::PartType,
                "{data_type}"
            );
        }

        let nulls = [ReorderKeyPart::Null, ReorderKeyPart::Null];
        let column = ReorderKeyPart::typed_column(&list, nulls.iter(), nulls.len())
            .assured("a null part fits every key column");
        assert_eq!(column.data_type(), &list);
        assert_eq!(column.null_count(), 2);
    }
    #[nervix_primitives::test]
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
            .rows()
            .map(|metadata| metadata.ingested_at_low_watermark())
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

    /// Ordering keys that do not pair with a batch's Arrow rows can only come from a defect above
    /// the buffer. The flush must name the first batch that disagrees and hand every buffered
    /// batch back, because the caller resolves the ACKs those batches carry.
    #[nervix_primitives::test]
    async fn reorderer_buffer_reports_the_first_ordering_key_mismatch_and_returns_every_batch() {
        let schema = test_schema(&[("sequence", ParseAsType::U32)]);
        let batch = |sequence: u32, acks: AckSet| {
            RelayRecordBatch::from_messages(
                schema.clone(),
                vec![RelayMessage {
                    key: None,
                    record: test_runtime_row([(
                        "sequence".to_string(),
                        RuntimeValue::U32(sequence),
                    )])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(sequence.into())),
                    acks,
                }],
            )
            .expect("test relay batch should build")
        };
        let ordering_keys = |count: u64| {
            Arc::new(
                (0..count)
                    .map(|sequence| ReordererRowOrder {
                        key: vec![ReorderKeyPart::UInt64(sequence)],
                        arrival_sequence: sequence,
                    })
                    .collect::<Vec<_>>(),
            )
        };
        let (first_acks, first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        let mut buffer = ReordererOutputBuffer::default();
        // One Arrow row against two ordering keys: the first disagreement in buffer order.
        buffer.push(
            batch(1, first_acks),
            ordering_keys(2),
            Timestamp::from_unix_nanos(10),
        );
        // A second disagreement, which the flush must not report over the one buffered before it.
        buffer.push(
            batch(2, second_acks),
            ordering_keys(3),
            Timestamp::from_unix_nanos(20),
        );

        let failure = *buffer
            .take_ordered_batch()
            .expect_err("ordering keys that do not pair with the Arrow rows must not order");

        assert!(matches!(
            failure.error.current_context(),
            ReordererOutputBatchError::OrderingKeyCount {
                arrow_rows: 1,
                ordering_keys: 2,
            }
        ));
        assert_eq!(failure.batches.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.estimated_bytes(), 0);

        for returned in &failure.batches {
            for ack in &returned.acks {
                ack.ack_success();
            }
        }
        assert_eq!(
            timeout(Duration::from_secs(1), first_completion.wait())
                .await
                .expect("the first buffered batch should come back carrying its ACK"),
            AckOutcome::Ack
        );
        assert_eq!(
            timeout(Duration::from_secs(1), second_completion.wait())
                .await
                .expect("the second buffered batch should come back carrying its ACK"),
            AckOutcome::Ack
        );
    }

    #[test]
    fn reorderer_buffer_returns_batches_that_cannot_be_concatenated() {
        let sequence_schema = test_schema(&[("sequence", ParseAsType::U32)]);
        let other_schema = test_schema(&[("other_sequence", ParseAsType::U32)]);
        let batch = |schema, field: &str| {
            RelayRecordBatch::from_messages(
                schema,
                vec![RelayMessage {
                    key: None,
                    record: test_runtime_row([(field.to_string(), RuntimeValue::U32(1))]),
                    acks: AckSet::empty(),
                }],
            )
            .expect("each fixture row must match its own schema")
        };
        let order = || {
            Arc::new(vec![ReordererRowOrder {
                key: vec![ReorderKeyPart::UInt64(1)],
                arrival_sequence: 0,
            }])
        };
        let mut buffer = ReordererOutputBuffer::default();
        buffer.push(
            batch(sequence_schema, "sequence"),
            order(),
            Timestamp::from_unix_nanos(10),
        );
        buffer.push(
            batch(other_schema, "other_sequence"),
            order(),
            Timestamp::from_unix_nanos(20),
        );

        let failure = *buffer
            .take_ordered_batch()
            .expect_err("reorderer batches with different schemas cannot concatenate");

        assert!(matches!(
            failure.error.current_context(),
            ReordererOutputBatchError::Concatenate
        ));
        assert!(
            failure
                .error
                .downcast_ref::<crate::runtime::relay_batch::RelayRecordBatchError>()
                .is_some(),
            "the relay batch failure must stay beneath the reorderer's own context"
        );
        assert_eq!(failure.batches.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.estimated_bytes(), 0);
    }

    #[nervix_primitives::test]
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
            nervix_vm::VmSettings::default(),
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
            &Executor::default(),
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
