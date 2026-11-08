//! Canonical emitter-batch decoding, independent of the bytes' origin.
//!
//! Layer: test harness.
//! - **Owns.** Exact schema, batch count, row count and typed Arrow cause assertions.
//! - **Depends on.** The production decoder, field vocabulary and maintained Arrow writer.
//! - **Must not know.** Transport, host bindings or server execution.

use arrow_array::{RecordBatch, UInt32Array};
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ParseAsType, SchemaField};
use nervix_primitives::sync::StdArc;

use super::field;
use crate::{EmitterBatchDecodeError, EmitterDelivery};

fn fields() -> Vec<SchemaField> {
    vec![field("id", ParseAsType::U32)]
}

fn stream(batches: usize) -> Bytes {
    let schema = StdArc::new(SchemaField::arrow_schema(&fields()));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![StdArc::new(UInt32Array::from(vec![7]))],
    )
    .assured("the only U32 column conforms to the declared schema");
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema)
        .assured("an in-memory stream accepts the declared schema");
    for _ in 0..batches {
        writer.write(&batch).assured("the batch matches its stream");
    }
    writer.finish().assured("an in-memory stream can finish");
    Bytes::from(bytes)
}

#[test]
fn the_decoder_preserves_a_canonical_batch_and_its_exact_values() {
    let bytes = stream(1);
    let batch = EmitterDelivery::decode_record_batch(&fields(), &bytes, 1)
        .assured("one matching batch decodes");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch.schema().as_ref(),
        &SchemaField::arrow_schema(&fields())
    );
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .assured("the declared U32 field has a UInt32Array");
    assert_eq!(ids.values().as_ref(), &[7]);
}

#[test]
fn the_declared_arrow_schema_and_the_one_batch_row_count_are_enforced() {
    let bytes = stream(1);
    let expected = fields();
    let mut renamed = expected.clone();
    renamed[0] = field("other_id", ParseAsType::U32);
    let mut retyped = expected.clone();
    retyped[0].ty = ParseAsType::I32;
    let mut nullable = expected.clone();
    nullable[0].optional = true;
    for fields in [renamed, retyped, nullable] {
        let error = EmitterDelivery::decode_record_batch(&fields, &bytes, 1)
            .expect_err("names, types and nullability are part of the exact Arrow schema");
        assert!(matches!(
            error.current_context(),
            EmitterBatchDecodeError::SchemaMismatch
        ));
    }
    let missing = EmitterDelivery::decode_record_batch(&expected, &stream(0), 0)
        .expect_err("a schema-only stream contains no batch");
    assert!(matches!(
        missing.current_context(),
        EmitterBatchDecodeError::MissingBatch
    ));
    let extra = EmitterDelivery::decode_record_batch(&expected, &stream(2), 2)
        .expect_err("two batches cannot stand in for one batch");
    assert!(matches!(
        extra.current_context(),
        EmitterBatchDecodeError::ExtraBatch
    ));
    let count = EmitterDelivery::decode_record_batch(&expected, &bytes, 2)
        .expect_err("the declared member count must match");
    assert!(matches!(
        count.current_context(),
        EmitterBatchDecodeError::RowCount {
            expected: 2,
            actual: 1,
        }
    ));
}

#[test]
fn a_malformed_stream_keeps_its_typed_arrow_cause() {
    let bytes = Bytes::from_static(&[0, 1, 2]);
    let error = EmitterDelivery::decode_record_batch(&fields(), &bytes, 1)
        .expect_err("truncated IPC cannot open a stream");
    assert!(matches!(
        error.current_context(),
        EmitterBatchDecodeError::OpenStream
    ));
    assert!(error.contains::<arrow_schema::ArrowError>());
}

#[test]
fn an_unreadable_first_or_later_batch_keeps_its_typed_arrow_cause() {
    for batches in [0, 1] {
        let mut bytes = stream(batches).to_vec();
        // The maintained writer finishes with an eight-byte end marker. Replace that marker
        // with an incomplete next message, after either the schema or the first complete batch.
        bytes.truncate(
            bytes
                .len()
                .checked_sub(8)
                .assured("the writer emitted its end marker"),
        );
        bytes.extend_from_slice(&[255, 255, 255, 255, 64, 0, 0, 0, 1, 2, 3]);
        let error = EmitterDelivery::decode_record_batch(&fields(), &Bytes::from(bytes), 1)
            .expect_err("the incomplete batch message cannot be read");
        assert!(matches!(
            error.current_context(),
            EmitterBatchDecodeError::ReadBatch
        ));
        assert!(error.contains::<arrow_schema::ArrowError>());
    }
}
