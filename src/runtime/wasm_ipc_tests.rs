//! Generated Arrow pools whose declared lengths must stay within the guest's bytes.
//!
//! Layer: test harness.
//! - **Owns.** Current generated pools, deliberate framing damage and typed refusal assertions.
//! - **Depends on.** The production WASM output validator and Arrow's current message schema.
//! - **Must not know.** Guest execution, branch scheduling or checkpoint publication.

use arrow_array::Int32Array;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::ParseAsType;
use nervix_primitives::sync::{Arc, StdArc};
use nervix_wasm::{WasmOutputColumnRef, WasmOutputRow};

use super::{WasmMaterializedOutput, WasmOutputError};
use crate::runtime::{
    WasmAckMap,
    test_fixtures::{test_schema, validate_wasm_test_outputs, wasm_guest_column, wasm_test_generated_output},
};
use crate::runtime_schema::CompiledSchema;

struct GeneratedPool {
    schema: Arc<CompiledSchema>,
    bytes: Vec<u8>,
    rows: usize,
}

impl GeneratedPool {
    fn new() -> Self {
        Self::with_values(vec![42])
    }

    fn with_values(values: Vec<i32>) -> Self {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let rows = values.len();
        let bytes = wasm_guest_column(
            schema.arrow_schema().field(0).clone(),
            StdArc::new(Int32Array::from(values)),
        );
        Self { schema, bytes, rows }
    }

    fn declaring(mut self, length: i64) -> Self {
        let schema_length = usize::try_from(i32::from_le_bytes(
            self.bytes[4..8].try_into().assured("a frame word has four bytes"),
        )).assured("the writer's schema metadata length is positive");
        let prefix = 8_usize.checked_add(schema_length).assured("the small schema fits in memory");
        assert_eq!(&self.bytes[prefix..prefix + 4], &[0xff; 4]);
        let metadata = prefix.checked_add(8).assured("the small prefix fits in memory");
        let metadata_length = usize::try_from(i32::from_le_bytes(
            self.bytes[prefix + 4..metadata].try_into().assured("a frame word has four bytes"),
        )).assured("the writer's batch metadata length is positive");
        let end = metadata.checked_add(metadata_length).assured("the small message fits in memory");
        let message = arrow_ipc::root_as_message(&self.bytes[metadata..end])
            .assured("the writer produced valid batch metadata");
        assert_eq!(message.header_type(), arrow_ipc::MessageHeader::RecordBatch);
        let field = message._tab.vtable().get(arrow_ipc::Message::VT_BODYLENGTH);
        assert_ne!(field, 0, "this nonempty batch declares a body length");
        let table = metadata.checked_add(message._tab.loc()).assured("the table is within the small message");
        let start = table.checked_add(usize::from(field)).assured("the field is within the small message");
        let end = start.checked_add(8).assured("the field has eight bytes");
        self.bytes[start..end].copy_from_slice(&length.to_le_bytes());
        self
    }

    fn with_unsupported_schema_bit_width(mut self) -> Self {
        let metadata_length = usize::try_from(i32::from_le_bytes(
            self.bytes[4..8].try_into().assured("the schema frame word is complete"),
        ))
        .assured("the written schema has a nonnegative metadata length");
        let end = 8_usize
            .checked_add(metadata_length)
            .assured("the small schema metadata fits in memory");
        let message = arrow_ipc::root_as_message(&self.bytes[8..end])
            .assured("the writer produced valid schema metadata");
        let schema = message.header_as_schema().assured("the first message is the schema");
        let fields = schema.fields().assured("the written schema contains fields");
        assert!(!fields.is_empty());
        let integer = fields.get(0).type_as_int().assured("the only field is I32");
        let field = integer._tab.vtable().get(arrow_ipc::Int::VT_BITWIDTH);
        assert_ne!(field, 0, "the I32 field writes its bit width");
        let table = 8_usize
            .checked_add(integer._tab.loc())
            .assured("the small metadata table fits in memory");
        let start = table
            .checked_add(usize::from(field))
            .assured("the bit-width slot fits in memory");
        let end = start.checked_add(4).assured("the bit-width word fits in memory");
        self.bytes[start..end].copy_from_slice(&24_i32.to_le_bytes());
        self
    }

    fn validate(&self) -> Result<Vec<WasmMaterializedOutput>, Report<WasmOutputError>> {
        validate_wasm_test_outputs(
            &self.schema,
            &self.schema,
            &WasmAckMap::default(),
            vec![wasm_test_generated_output(
                self.bytes.clone(),
                vec![WasmOutputColumnRef::generated(0)],
                vec![WasmOutputRow::default(); self.rows],
            )],
        )
    }
}

#[test]
fn wasm_generated_ipc_refuses_a_declared_body_larger_than_its_bytes() {
    for length in [1_i64 << 60, -1_i64] {
        let report = GeneratedPool::new().declaring(length).validate()
            .expect_err("a body outside the supplied bytes must return a typed error");
        assert!(matches!(report.current_context(), WasmOutputError::InvalidGeneratedArrowIpc { .. }));
    }
}

#[test]
fn wasm_generated_ipc_requires_its_end_marker() {
    let mut pool = GeneratedPool::new();
    pool.bytes.truncate(pool.bytes.len().checked_sub(8).assured("the stream has its end marker"));
    let report = pool.validate().expect_err("a stream without its end marker must return a typed error");
    assert!(matches!(report.current_context(), WasmOutputError::InvalidGeneratedArrowIpc { .. }));
}

#[test]
fn wasm_generated_ipc_refuses_a_schema_arrow_cannot_convert() {
    let report = GeneratedPool::new()
        .with_unsupported_schema_bit_width()
        .validate()
        .expect_err("unsupported schema metadata must return a typed error");
    assert!(matches!(
        report.current_context(),
        WasmOutputError::InvalidGeneratedArrowIpc { .. }
    ));
}

#[test]
fn bolero_wasm_generated_ipc_restores_every_i32_value() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(32)
        .for_each(|input: &[u8]| {
            let mut values = Vec::new();
            for chunk in input.chunks(4).take(8) {
                let mut bytes = [0_u8; 4];
                bytes[..chunk.len()].copy_from_slice(chunk);
                values.push(i32::from_le_bytes(bytes));
            }
            if values.is_empty() {
                values.push(0);
            }
            let pool = GeneratedPool::with_values(values.clone());
            let outputs = pool.validate().assured("the current writer's generated pool validates");
            assert_eq!(outputs.len(), 1);
            let output = &outputs[0];
            assert_eq!(output.output_route_index, 0);
            assert_eq!(output.batch.batch().schema(), pool.schema.arrow_schema());
            assert_eq!(output.batch.batch().num_rows(), values.len());
            assert_eq!(output.acks.rows.len(), values.len());
            assert_eq!(output.batch.batch().num_columns(), 1);
            let column = output.batch.batch().column(0).as_any().downcast_ref::<Int32Array>()
                .assured("the current pool's only field is I32");
            assert_eq!(column, &Int32Array::from(values));
        });
}

#[test]
fn bolero_malformed_wasm_generated_ipc_is_refused_typed() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|input: &[u8]| {
            let choice = match input.first() {
                Some(byte) => byte % 5,
                None => 0,
            };
            let mut pool = GeneratedPool::new();
            match choice {
                0 => pool = pool.declaring(1_i64 << 60),
                1 => pool = pool.declaring(-1),
                2 => {
                    let end = pool.bytes.len().checked_sub(8).assured("the writer appends EOS");
                    pool.bytes.truncate(end);
                }
                3 => pool.bytes.extend_from_slice(input),
                _ => {
                    let length = usize::try_from(i32::from_le_bytes(
                        pool.bytes[4..8].try_into().assured("the schema frame is complete"),
                    )).assured("the schema metadata has a nonnegative length");
                    let offset = match input.get(1) {
                        Some(byte) => usize::from(*byte) % length,
                        None => 0,
                    };
                    let bit = match input.get(2) {
                        Some(byte) => 1_u8 << (byte % 8),
                        None => 1,
                    };
                    pool.bytes[8 + offset] ^= bit;
                }
            }
            match pool.validate() {
                Ok(outputs) => {
                    assert_eq!(outputs.len(), 1);
                    let batch = outputs[0].batch.batch();
                    assert_eq!(batch.num_rows(), 1);
                    assert_eq!(batch.num_columns(), 1);
                    let column = batch.column(0).as_any().downcast_ref::<Int32Array>()
                        .assured("a validated generated column has the current I32 schema");
                    assert_eq!(column, &Int32Array::from(vec![42]));
                }
                Err(report) => {
                    assert!(matches!(
                        report.current_context(),
                        WasmOutputError::InvalidGeneratedArrowIpc { .. }
                            | WasmOutputError::GeneratedRecordBatchCount { .. }
                            | WasmOutputError::GeneratedColumnTypeMismatch { .. }
                            | WasmOutputError::GeneratedColumnRowCountMismatch { .. }
                    ), "malformed generated IPC has a content error: {report:?}");
                }
            }
        });
}
