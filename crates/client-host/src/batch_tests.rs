//! The batch owner's local-input classification and canonical column preservation.
//!
//! Layer: test harness.
//! - **Owns.** Deterministic batch-input and complete typed-cause regressions.
//! - **Depends on.** The production batch builder, decoder and shared field vocabulary.
//! - **Must not know.** The server, JNI or a binding's exception classes.

use bytes::Bytes;
use meticulous::ResultExt as _;
use nervix_models::{FieldName, ParseAsType, SchemaField};

use crate::{Batch, BatchBuilder, FailureKind, Schema};

fn schema() -> Schema {
    Schema::of_fields(vec![SchemaField {
        name: FieldName::parse("id").assured("id is a valid field-name literal"),
        ty: ParseAsType::U32,
        optional: false,
        sensitive: false,
    }])
}

#[test]
fn invalid_ipc_is_a_local_argument_failure_with_the_decoders_complete_cause() {
    let Err(error) = Batch::decode(Bytes::from_static(&[0, 1, 2]), schema(), 1) else {
        panic!("a truncated IPC stream cannot construct a batch");
    };
    assert_eq!(error.kind(), FailureKind::InvalidArgument);
    assert!(error.message().contains("Arrow IPC stream"));
    assert!(error.message().contains("Expected schema message"));
    assert_eq!(error.execution_reference(), None);
    assert_eq!(error.open_refusal(), None);
}

#[test]
fn a_canonical_batch_keeps_its_columns_and_a_wrong_row_count_is_an_argument_failure() {
    let schema = schema();
    let expected = 7u32.to_ne_bytes();
    let mut builder = BatchBuilder::new(schema.clone(), 1);
    builder
        .fixed(0, 0, &expected)
        .assured("the only U32 field takes exactly four bytes for its one row");
    let batch = builder.finish().assured("the only field is initialized");
    let ipc = Bytes::copy_from_slice(batch.ipc().assured("the validated batch can be encoded"));
    let decoded = Batch::decode(ipc.clone(), schema.clone(), 1)
        .assured("the canonical stream has its original schema and one row");
    let mut actual = [0; 4];
    decoded
        .fixed(0, 0, &mut actual)
        .assured("the only U32 field copies exactly four bytes");
    assert_eq!(actual, expected);
    assert_eq!(decoded.ipc().assured("the decoded stream is retained"), ipc);
    let Err(error) = Batch::decode(ipc, schema, 2) else {
        panic!("the caller's row count must match the canonical stream");
    };
    assert_eq!(error.kind(), FailureKind::InvalidArgument);
}
