//! SDK Arrow input/output conversions preserve logical columns and callback order.
//!
//! Layer: test harness.
//! - **Owns.** Bounded nullable/nested columns, complete Arrow equality and ordered emission checks.
//! - **Depends on.** Production SDK envelopes, Arrow builders and current message generators.
//! - **Must not know.** Host scheduling, checkpoint publication or live ACK ownership.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{Field, Schema};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::WasmValues;
use nervix_wasm_protocol::{
    AckSidecar, AckToken, Envelope, OutputColumnRef, OutputRow, ProcessorType, RoutedOutput,
};

use crate::{BranchContext, GuestContext, InputBatch, OutputEnvelope, ProcessorTypeArrow};

struct Columns<'a>(WasmValues<'a>);

impl Columns<'_> {
    fn array(&mut self, ty: &ProcessorType, rows: usize, optional: bool) -> ArrayRef {
        let validity: Vec<bool> = (0..rows)
            .map(|_| !optional || self.0.entropy().flag())
            .collect();
        macro_rules! scalar {
            ($array:ty, $value:expr) => {{
                let values: Vec<_> = validity
                    .iter()
                    .map(|valid| if *valid { Some($value) } else { None })
                    .collect();
                Arc::new(<$array>::from(values))
            }};
        }
        match ty {
            ProcessorType::U8 => scalar!(UInt8Array, self.0.entropy().byte()),
            ProcessorType::I8 => scalar!(Int8Array, i8::from_le_bytes([self.0.entropy().byte()])),
            ProcessorType::U16 => scalar!(
                UInt16Array,
                u16::from_le_bytes(std::array::from_fn(|_| self.0.entropy().byte()))
            ),
            ProcessorType::I16 => scalar!(
                Int16Array,
                i16::from_le_bytes(std::array::from_fn(|_| self.0.entropy().byte()))
            ),
            ProcessorType::U32 => scalar!(
                UInt32Array,
                u32::from_le_bytes(std::array::from_fn(|_| self.0.entropy().byte()))
            ),
            ProcessorType::I32 => scalar!(
                Int32Array,
                i32::from_le_bytes(std::array::from_fn(|_| self.0.entropy().byte()))
            ),
            ProcessorType::U64 => scalar!(UInt64Array, self.0.entropy().any_u64()),
            ProcessorType::I64 => scalar!(Int64Array, self.0.entropy().any_i64()),
            ProcessorType::Bool => scalar!(BooleanArray, self.0.entropy().flag()),
            ProcessorType::String => scalar!(
                StringArray,
                self.0
                    .entropy()
                    .pick(["", "\0", "東京🙂", "value"])
                    .to_string()
            ),
            ProcessorType::Bytes => {
                let values: Vec<Option<Vec<u8>>> = validity
                    .iter()
                    .map(|valid| if *valid { Some(self.0.bytes()) } else { None })
                    .collect();
                Arc::new(BinaryArray::from_iter(
                    values.iter().map(|value| value.as_deref()),
                ))
            }
            ProcessorType::Datetime => {
                let values: Vec<_> = validity
                    .iter()
                    .map(|valid| {
                        if *valid {
                            Some(self.0.entropy().any_i64())
                        } else {
                            None
                        }
                    })
                    .collect();
                Arc::new(TimestampNanosecondArray::from(values).with_timezone("+00:00"))
            }
            ProcessorType::F32 => {
                let bits = u32::from_le_bytes(std::array::from_fn(|_| self.0.entropy().byte()));
                scalar!(
                    Float32Array,
                    f32::from_bits(self.0.entropy().pick([
                        0,
                        0x8000_0000,
                        0x7fc0_1234,
                        0x7f80_0000,
                        0xff80_0000,
                        bits,
                    ]))
                )
            }
            ProcessorType::F64 => {
                let bits = self.0.entropy().any_u64();
                scalar!(
                    Float64Array,
                    f64::from_bits(self.0.entropy().pick([
                        0,
                        0x8000_0000_0000_0000,
                        0x7ff8_0000_0000_1234,
                        0x7ff0_0000_0000_0000,
                        0xfff0_0000_0000_0000,
                        bits,
                    ]))
                )
            }
            ProcessorType::Array { element, len } => {
                let width = usize::try_from(*len).assured("test widths are at most three");
                let values = self.array(
                    element,
                    rows.checked_mul(width)
                        .assured("eight rows times three fits"),
                    false,
                );
                let field = Arc::new(Field::new("item", values.data_type().clone(), false));
                Arc::new(
                    FixedSizeListArray::try_new(
                        field,
                        i32::try_from(*len).assured("test width fits i32"),
                        values,
                        Some(NullBuffer::from(validity)),
                    )
                    .assured("the child length equals rows times width"),
                )
            }
            ProcessorType::Vec { element } => {
                let mut offsets = vec![0_i32];
                let mut length = 0_i32;
                for _ in 0..rows {
                    length += i32::try_from(self.0.entropy().count(3))
                        .assured("list width is at most three");
                    offsets.push(length);
                }
                let values = self.array(
                    element,
                    usize::try_from(length).assured("list length is nonnegative"),
                    false,
                );
                let field = Arc::new(Field::new("item", values.data_type().clone(), false));
                Arc::new(
                    ListArray::try_new(
                        field,
                        OffsetBuffer::new(offsets.into()),
                        values,
                        Some(NullBuffer::from(validity)),
                    )
                    .assured("offsets are monotonic and end at the child length"),
                )
            }
        }
    }

    fn batch(&mut self, rows: usize) -> RecordBatch {
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for kind in 0..16 {
            let ty = match kind {
                14 => ProcessorType::Array {
                    element: Box::new(ProcessorType::Vec {
                        element: Box::new(WasmValues::scalar(self.0.entropy().byte() % 14)),
                    }),
                    len: 3,
                },
                15 => ProcessorType::Vec {
                    element: Box::new(ProcessorType::Array {
                        element: Box::new(WasmValues::scalar(self.0.entropy().byte() % 14)),
                        len: 2,
                    }),
                },
                _ => WasmValues::scalar(kind),
            };
            let optional = self.0.entropy().flag();
            let column = self.array(&ty, rows, optional);
            assert_eq!(
                ty.arrow_data_type().assured("bounded type maps to Arrow"),
                *column.data_type()
            );
            fields.push(Field::new(
                format!("column_{kind}"),
                column.data_type().clone(),
                optional,
            ));
            columns.push(column);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .assured("every column has the requested row count")
    }

    fn ipc(batches: &[RecordBatch]) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &batches[0].schema())
            .assured("a generated schema is IPC representable");
        for batch in batches {
            writer
                .write(batch)
                .assured("valid generated columns encode");
        }
        writer.finish().assured("the in-memory writer completes");
        drop(writer);
        bytes
    }
}

#[test]
fn bolero_sdk_input_preserves_complete_arrow_batches_and_sidecars() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut columns = Columns(WasmValues::new(bytes));
            let rows = columns.0.entropy().count(8);
            let batch = columns.batch(rows);
            let batches = vec![
                batch.clone(),
                batch.slice(0, rows / 2),
                batch.slice(rows / 2, rows - rows / 2),
            ];
            let ipc = Columns::ipc(&batches);
            let mut acks = columns.0.sidecar();
            acks.rows = (0..rows * 2)
                .map(|_| OutputRow {
                    tokens: columns.0.tokens(),
                    source_token: if columns.0.entropy().flag() {
                        Some(AckToken(columns.0.entropy().any_u64()))
                    } else {
                        None
                    },
                })
                .collect();
            let input = InputBatch::from_envelope_bytes(
                Envelope::Input {
                    arrow_ipc_batch: ipc.clone(),
                    acks: acks.clone(),
                }
                .encode(),
            )
            .assured("valid protocol and Arrow input decode");
            assert_eq!(input.arrow_ipc(), ipc);
            assert_eq!(input.acks(), &acks);
            assert_eq!(
                input.row_count(),
                u64::try_from(rows * 2).assured("the row budget fits u64")
            );
            // Arrow equality compares schemas, logical validity and value bits, including NaN payloads.
            assert_eq!(input.batches(), batches);
            let retained = input.clone();
            drop(input);
            assert_eq!(retained.batches(), batches);
            assert_eq!(retained.acks(), &acks);
            assert_eq!(retained.arrow_ipc(), ipc);
        });
}

#[test]
fn bolero_sdk_output_preserves_shared_columns_routes_and_emit_order() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut columns = Columns(WasmValues::new(bytes));
            let rows = columns.0.entropy().count(8);
            let batch = columns.batch(rows);
            let mut expected_fields = Vec::new();
            for field in batch.schema().fields() {
                expected_fields.push(Field::new(
                    "",
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            }
            let expected = RecordBatch::try_new(
                Arc::new(Schema::new(expected_fields)),
                batch.columns().to_vec(),
            )
            .assured("the generated pool preserves each column and its nullability");
            let branch = BranchContext::from(columns.0.branch_init());
            let mut pending_emit = Vec::new();
            let mut global_error = Vec::new();
            let mut error_state = None;
            let mut all_outputs = Vec::new();
            {
                let mut ctx = GuestContext {
                    branch: &branch,
                    pending_emit: &mut pending_emit,
                    global_error: &mut global_error,
                    error_state: &mut error_state,
                };
                for emission in 0..3 {
                    let mut output = OutputEnvelope::new();
                    let mut refs = Vec::new();
                    for (column, field) in batch.columns().iter().zip(batch.schema().fields()) {
                        refs.push(OutputColumnRef::Generated {
                            column_index: output
                                .add_generated_column(column.clone(), field.is_nullable()),
                        });
                    }
                    refs.extend([
                        OutputColumnRef::Input { column_index: 0 },
                        OutputColumnRef::Uninitialized,
                    ]);
                    let mut routes = Vec::new();
                    for route in 0..2 {
                        let acks = AckSidecar {
                            rows: (0..rows)
                                .map(|row| OutputRow {
                                    tokens: vec![AckToken(
                                        u64::try_from(row).assured("row index fits u64"),
                                    )],
                                    source_token: None,
                                })
                                .collect(),
                            ..columns.0.sidecar()
                        };
                        let routed = RoutedOutput {
                            output_relay: format!("emit_{emission}_route_{route}"),
                            columns: refs.clone(),
                            acks,
                        };
                        output.add_route(
                            routed.output_relay.clone(),
                            routed.columns.clone(),
                            routed.acks.clone(),
                        );
                        routes.push(routed);
                    }
                    ctx.emit(output)
                        .assured("the generated pool and routes encode");
                    all_outputs.push(routes);
                }
            }
            assert_eq!(pending_emit.len(), all_outputs.len());
            for (encoded, routes) in pending_emit.iter().zip(all_outputs) {
                let Envelope::Output {
                    generated_arrow_ipc_batch,
                    outputs,
                } = Envelope::decode(encoded).assured("an emitted SDK envelope verifies")
                else {
                    panic!("the SDK emitted output");
                };
                assert_eq!(outputs, routes);
                let batches = StreamReader::try_new(generated_arrow_ipc_batch.as_slice(), None)
                    .assured("the generated pool is IPC")
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .assured("every generated batch decodes");
                assert_eq!(batches.as_slice(), std::slice::from_ref(&expected));
            }
            let mut empty = OutputEnvelope::new();
            empty.add_route("empty", vec![], AckSidecar::default());
            assert_eq!(
                Envelope::decode(&empty.encode().assured("an empty pool encodes"))
                    .assured("an empty pool decodes"),
                Envelope::Output {
                    generated_arrow_ipc_batch: vec![],
                    outputs: vec![RoutedOutput {
                        output_relay: "empty".into(),
                        columns: vec![],
                        acks: AckSidecar::default(),
                    }]
                }
            );
        });
}
