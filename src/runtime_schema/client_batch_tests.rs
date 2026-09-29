//! A client batch is accepted only as one canonical, uncompressed Arrow IPC stream of exactly the
//! ingestor's schema within its limits, and every other body is refused with the defect it has.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc as StdArc,
};

use arrow_array::{
    ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray, UInt64Array,
    builder::StringDictionaryBuilder, types::Int32Type,
};
use arrow_ipc::{
    BodyCompression, BodyCompressionArgs, BodyCompressionMethod, CompressionType, DictionaryBatch,
    DictionaryBatchArgs, Message, MessageArgs, MessageHeader, MetadataVersion,
    RecordBatch as IpcRecordBatch, RecordBatchArgs, root_as_message,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Executor, MemoryClass, Reservation};
use nervix_models::{
    ClientBatchDefect, CreateSchema, FieldName, ParseAsType, SchemaField, SchemaName,
};

use super::{ClientBatchError, ClientBatchLimits, ClientSchemaDifference};
use crate::runtime_schema::{CompiledSchema, compile_schema};

/// The ingestor schema the tests submit against: a required unsigned identifier and an optional
/// string.
fn schema() -> CompiledSchema {
    compile_schema(&CreateSchema {
        name: SchemaName::parse("order").assured("a literal schema name"),
        fields: vec![
            SchemaField {
                name: FieldName::parse("id").assured("a literal field name"),
                ty: ParseAsType::U64,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: FieldName::parse("note").assured("a literal field name"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: true,
            },
        ],
    })
}

fn arrow_schema() -> StdArc<Schema> {
    schema().arrow_schema()
}

fn batch_of(schema: StdArc<Schema>, ids: &[u64]) -> RecordBatch {
    let id: ArrayRef = StdArc::new(UInt64Array::from(ids.to_vec()));
    let notes = ids
        .iter()
        .map(|id| {
            if id % 2 == 0 {
                Some(format!("note {id}"))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let note: ArrayRef = StdArc::new(StringArray::from(notes));
    RecordBatch::try_new(schema, vec![id, note]).assured("the columns match the schema")
}

/// Writes `batches` as one stream of `schema`.
fn stream(schema: &Schema, batches: &[RecordBatch]) -> Vec<u8> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema).assured("an in-memory writer opens");
    for batch in batches {
        writer
            .write(batch)
            .assured("an in-memory writer takes a batch");
    }
    writer.finish().assured("an in-memory writer finishes");
    writer
        .into_inner()
        .assured("a finished writer yields its buffer")
}

fn limits() -> ClientBatchLimits {
    ClientBatchLimits {
        max_bytes: NonZeroU64::new(4 * 1024 * 1024).assured("a literal non-zero limit"),
        max_rows: NonZeroUsize::new(65_536).assured("a literal non-zero limit"),
    }
}

async fn decode(body: Vec<u8>, limits: ClientBatchLimits) -> Result<RecordBatch, ClientBatchError> {
    let executor = Executor::default();
    match schema()
        .decode_client_batch(&executor, Bytes::from(body), limits)
        .await
    {
        Ok(batch) => Ok(batch.batch().clone()),
        Err(error) => Err(error.current_context().clone()),
    }
}

fn defect(result: Result<RecordBatch, ClientBatchError>) -> ClientBatchDefect {
    match result {
        Ok(batch) => panic!("the body decoded as a batch of {} rows", batch.num_rows()),
        Err(error) => error
            .defect()
            .unwrap_or_else(|| panic!("the refusal names a defect of the batch: {error}")),
    }
}

/// The framed messages of a stream, each with its prefix, metadata and body, in order. The last is
/// the end-of-stream marker.
fn messages(stream: &[u8]) -> Vec<Vec<u8>> {
    let mut messages = Vec::new();
    let mut offset = 0;
    loop {
        let length_bytes: [u8; 4] = stream[offset + 4..offset + 8]
            .try_into()
            .assured("a stream frames every message with a length");
        let length = usize::try_from(i32::from_le_bytes(length_bytes)).assured("a positive length");
        if length == 0 {
            messages.push(stream[offset..offset + 8].to_vec());
            return messages;
        }
        let metadata = &stream[offset + 8..offset + 8 + length];
        let message = root_as_message(metadata).assured("a written message parses");
        let body = usize::try_from(message.bodyLength()).assured("a written body length fits");
        let end = offset + 8 + length + body;
        messages.push(stream[offset..end].to_vec());
        offset = end;
    }
}

/// Frames a finished message and its body as a stream message.
fn framed(message: &[u8], body: &[u8]) -> Vec<u8> {
    let padded = message.len().div_ceil(8) * 8;
    let mut framed = vec![0xff; 4];
    framed.extend_from_slice(
        &i32::try_from(padded)
            .assured("a small message")
            .to_le_bytes(),
    );
    framed.extend_from_slice(message);
    framed.resize(8 + padded, 0);
    framed.extend_from_slice(body);
    framed
}

#[nervix_primitives::test]
async fn one_canonical_batch_of_the_schema_is_decoded_with_its_rows() {
    let batch = batch_of(arrow_schema(), &[1, 2, 3]);
    let decoded = decode(
        stream(&arrow_schema(), std::slice::from_ref(&batch)),
        limits(),
    )
    .await
    .assured("a canonical batch decodes");
    assert_eq!(decoded, batch);

    let empty = batch_of(arrow_schema(), &[]);
    let decoded = decode(stream(&arrow_schema(), &[empty]), limits())
        .await
        .assured("an empty batch is still one batch");
    assert_eq!(decoded.num_rows(), 0);
}

#[nervix_primitives::test]
async fn another_schema_is_refused_with_its_first_difference() {
    let field =
        |name: &str, data_type: DataType, nullable: bool| Field::new(name, data_type, nullable);
    /// A schema a batch was written in, the first difference from the ingestor's schema, and how
    /// the refusal describes it.
    struct Case {
        submitted: Schema,
        difference: ClientSchemaDifference,
        message: &'static str,
    }
    let cases = [
        Case {
            submitted: Schema::new(vec![field("id", DataType::UInt64, false)]),
            difference: ClientSchemaDifference::FieldCount {
                expected: 2,
                found: 1,
            },
            message: "it has 1 fields where the ingestor's schema has 2",
        },
        Case {
            submitted: Schema::new(vec![
                field("id", DataType::UInt64, false),
                field("remark", DataType::Utf8, true),
            ]),
            difference: ClientSchemaDifference::FieldName {
                index: 1,
                expected: "note".to_string(),
                found: "remark".to_string(),
            },
            message: "field 1 is 'remark' where the ingestor's schema has 'note'",
        },
        Case {
            submitted: Schema::new(vec![
                field("id", DataType::Int64, false),
                field("note", DataType::Utf8, true),
            ]),
            difference: ClientSchemaDifference::FieldType {
                field: "id".to_string(),
                expected: DataType::UInt64,
                found: DataType::Int64,
            },
            message: "field 'id' is Int64 where the ingestor's schema has UInt64",
        },
        Case {
            submitted: Schema::new(vec![
                field("id", DataType::UInt64, true),
                field("note", DataType::Utf8, true),
            ]),
            difference: ClientSchemaDifference::FieldNullability {
                field: "id".to_string(),
                expected_nullable: false,
            },
            message: "field 'id' is nullable where the ingestor's schema declares it required",
        },
        Case {
            submitted: Schema::new(vec![
                field("id", DataType::UInt64, false),
                field("note", DataType::Utf8, false),
            ]),
            difference: ClientSchemaDifference::FieldNullability {
                field: "note".to_string(),
                expected_nullable: true,
            },
            message: "field 'note' is not nullable where the ingestor's schema declares it \
                      optional",
        },
        Case {
            submitted: Schema::new_with_metadata(
                vec![
                    field("id", DataType::UInt64, false),
                    field("note", DataType::Utf8, true),
                ],
                [("origin".to_string(), "sensor".to_string())]
                    .into_iter()
                    .collect(),
            ),
            difference: ClientSchemaDifference::Metadata,
            message: "it carries schema or field metadata, which the ingestor's schema does not",
        },
    ];
    for Case {
        submitted,
        difference,
        message,
    } in cases
    {
        let submitted = StdArc::new(submitted);
        let mut columns = Vec::new();
        for field in submitted.fields() {
            let column: ArrayRef = match field.data_type() {
                DataType::UInt64 => StdArc::new(UInt64Array::from(vec![1])),
                DataType::Int64 => StdArc::new(Int64Array::from(vec![1])),
                _ => StdArc::new(StringArray::from(vec![Some("n")])),
            };
            columns.push(column);
        }
        let batch = RecordBatch::try_new(submitted.clone(), columns).assured("matching columns");
        let refused = decode(stream(&submitted, &[batch]), limits()).await;
        let Err(ClientBatchError::SchemaMismatch { difference: found }) = refused else {
            panic!("{difference} is refused as a schema mismatch, not {refused:?}");
        };
        assert_eq!(found, difference);
        assert_eq!(found.to_string(), message);
    }
}

#[nervix_primitives::test]
async fn a_stream_without_exactly_one_batch_is_refused() {
    let none = stream(&arrow_schema(), &[]);
    assert!(matches!(
        decode(none, limits()).await,
        Err(ClientBatchError::NotOneBatch { batches: 0 })
    ));
    let two = stream(
        &arrow_schema(),
        &[
            batch_of(arrow_schema(), &[1]),
            batch_of(arrow_schema(), &[2]),
        ],
    );
    assert!(matches!(
        decode(two, limits()).await,
        Err(ClientBatchError::NotOneBatch { batches: 2 })
    ));
}

#[nervix_primitives::test]
async fn rows_and_bytes_beyond_the_limits_are_refused_before_decoding() {
    let three = stream(&arrow_schema(), &[batch_of(arrow_schema(), &[1, 2, 3])]);
    let two_rows = ClientBatchLimits {
        max_rows: NonZeroUsize::new(2).assured("a literal non-zero limit"),
        ..limits()
    };
    assert!(matches!(
        decode(three.clone(), two_rows).await,
        Err(ClientBatchError::TooManyRows { rows: 3, limit: 2 })
    ));
    assert_eq!(
        defect(decode(three.clone(), two_rows).await),
        ClientBatchDefect::TooManyRows
    );
    let size = u64::try_from(three.len()).assured("a small stream");
    let smaller = ClientBatchLimits {
        max_bytes: NonZeroU64::new(size - 1).assured("a stream has more than one byte"),
        ..limits()
    };
    assert_eq!(
        defect(decode(three, smaller).await),
        ClientBatchDefect::TooLarge
    );
}

#[nervix_primitives::test]
async fn a_body_that_is_not_one_canonical_stream_is_malformed() {
    let canonical = stream(&arrow_schema(), &[batch_of(arrow_schema(), &[1, 2])]);
    let mut truncated = canonical.clone();
    truncated.pop();
    let mut without_end = canonical.clone();
    without_end.truncate(canonical.len() - 8);
    let mut trailing = canonical.clone();
    trailing.push(0);
    let legacy = {
        let options = IpcWriteOptions::try_new(8, true, MetadataVersion::V4)
            .assured("the legacy format is a valid option set");
        let mut writer = StreamWriter::try_new_with_options(Vec::new(), &arrow_schema(), options)
            .assured("an in-memory writer opens");
        writer
            .write(&batch_of(arrow_schema(), &[1]))
            .assured("an in-memory writer takes a batch");
        writer.finish().assured("an in-memory writer finishes");
        writer
            .into_inner()
            .assured("a finished writer yields its buffer")
    };
    let batch_first = {
        let parts = messages(&canonical);
        let mut reordered = parts[1].clone();
        reordered.extend_from_slice(&parts[0]);
        reordered.extend_from_slice(&parts[2]);
        reordered
    };
    for body in [
        b"not an arrow stream".to_vec(),
        truncated,
        without_end,
        trailing,
        legacy,
        batch_first,
    ] {
        assert_eq!(
            defect(decode(body, limits()).await),
            ClientBatchDefect::Malformed
        );
    }
}

#[nervix_primitives::test]
async fn a_compressed_batch_is_refused() {
    let canonical = stream(&arrow_schema(), &[batch_of(arrow_schema(), &[1])]);
    let parts = messages(&canonical);
    let mut builder = FlatBufferBuilder::new();
    let compression = BodyCompression::create(
        &mut builder,
        &BodyCompressionArgs {
            codec: CompressionType::ZSTD,
            method: BodyCompressionMethod::BUFFER,
        },
    );
    let batch = IpcRecordBatch::create(
        &mut builder,
        &RecordBatchArgs {
            length: 1,
            compression: Some(compression),
            ..RecordBatchArgs::default()
        },
    );
    let message = Message::create(
        &mut builder,
        &MessageArgs {
            version: MetadataVersion::V5,
            header_type: MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: 0,
            custom_metadata: None,
        },
    );
    builder.finish(message, None);
    let mut body = parts[0].clone();
    body.extend_from_slice(&framed(builder.finished_data(), &[]));
    body.extend_from_slice(&parts[2]);
    assert!(matches!(
        decode(body.clone(), limits()).await,
        Err(ClientBatchError::Compressed)
    ));
    assert_eq!(
        defect(decode(body, limits()).await),
        ClientBatchDefect::Compressed
    );
}

/// A framed record batch message without a body, declaring `rows` rows, or with no record batch
/// header at all for `None`, and the body length `body_length`.
fn record_batch_message(rows: Option<i64>, body_length: i64) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let header = match rows {
        Some(length) => {
            let batch = IpcRecordBatch::create(
                &mut builder,
                &RecordBatchArgs {
                    length,
                    ..RecordBatchArgs::default()
                },
            );
            Some(batch.as_union_value())
        }
        None => None,
    };
    let message = Message::create(
        &mut builder,
        &MessageArgs {
            version: MetadataVersion::V5,
            header_type: MessageHeader::RecordBatch,
            header,
            bodyLength: body_length,
            custom_metadata: None,
        },
    );
    builder.finish(message, None);
    framed(builder.finished_data(), &[])
}

#[nervix_primitives::test]
async fn a_stream_whose_framing_or_headers_break_the_format_names_what_is_wrong() {
    let canonical = stream(&arrow_schema(), &[batch_of(arrow_schema(), &[1])]);
    let parts = messages(&canonical);
    let end_of_stream = parts[2].clone();
    let between_schema_and_end = |message: Vec<u8>| {
        let mut body = parts[0].clone();
        body.extend_from_slice(&message);
        body.extend_from_slice(&end_of_stream);
        body
    };
    let mut negative_metadata_length = vec![0xff; 4];
    negative_metadata_length.extend_from_slice(&(-8_i32).to_le_bytes());
    let cases = [
        (
            end_of_stream.clone(),
            "the stream ends before its schema message",
        ),
        (
            negative_metadata_length,
            "a message declares a negative metadata length",
        ),
        (
            between_schema_and_end(record_batch_message(Some(-1), 0)),
            "a record batch declares a negative row count",
        ),
        (
            between_schema_and_end(record_batch_message(Some(1), -8)),
            "a message declares a body length outside this body",
        ),
    ];
    for (body, expected) in cases {
        let refused = decode(body, limits()).await;
        let Err(ClientBatchError::Malformed { reason }) = refused else {
            panic!("a body is malformed because {expected}, not {refused:?}");
        };
        assert_eq!(reason, expected);
    }
    // The message verifier refuses a record batch message whose header is missing.
    assert_eq!(
        defect(
            decode(
                between_schema_and_end(record_batch_message(None, 0)),
                limits()
            )
            .await
        ),
        ClientBatchDefect::Malformed
    );
}

#[nervix_primitives::test]
async fn a_dictionary_message_is_unexpected() {
    let canonical = stream(&arrow_schema(), &[batch_of(arrow_schema(), &[1])]);
    let parts = messages(&canonical);
    let mut builder = FlatBufferBuilder::new();
    let data = IpcRecordBatch::create(
        &mut builder,
        &RecordBatchArgs {
            length: 0,
            ..RecordBatchArgs::default()
        },
    );
    let dictionary = DictionaryBatch::create(
        &mut builder,
        &DictionaryBatchArgs {
            id: 0,
            data: Some(data),
            isDelta: false,
        },
    );
    let message = Message::create(
        &mut builder,
        &MessageArgs {
            version: MetadataVersion::V5,
            header_type: MessageHeader::DictionaryBatch,
            header: Some(dictionary.as_union_value()),
            bodyLength: 0,
            custom_metadata: None,
        },
    );
    builder.finish(message, None);
    let mut body = parts[0].clone();
    body.extend_from_slice(&framed(builder.finished_data(), &[]));
    body.extend_from_slice(&parts[1]);
    body.extend_from_slice(&parts[2]);
    assert!(matches!(
        decode(body, limits()).await,
        Err(ClientBatchError::UnexpectedMessage {
            kind: "DictionaryBatch"
        })
    ));

    // A dictionary-encoded column is refused too: its stream carries a dictionary message.
    let dictionary_schema = StdArc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "note",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));
    let mut notes = StringDictionaryBuilder::<Int32Type>::new();
    notes.append_value("n");
    let columns: Vec<ArrayRef> = vec![
        StdArc::new(UInt64Array::from(vec![1])),
        StdArc::new(notes.finish()),
    ];
    let batch =
        RecordBatch::try_new(dictionary_schema.clone(), columns).assured("matching columns");
    assert_eq!(
        defect(decode(stream(&dictionary_schema, &[batch]), limits()).await),
        ClientBatchDefect::UnexpectedMessage
    );
}

#[nervix_primitives::test]
async fn columns_that_break_their_type_are_invalid_data() {
    // The record batch of a binary column carries bytes that are not UTF-8; framed under the
    // string schema, its columns do not satisfy their declared type.
    let binary_schema = StdArc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("note", DataType::Binary, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        StdArc::new(UInt64Array::from(vec![1])),
        StdArc::new(BinaryArray::from(vec![Some(&[0xff_u8, 0xfe][..])])),
    ];
    let binary = RecordBatch::try_new(binary_schema.clone(), columns).assured("matching columns");
    let binary = messages(&stream(&binary_schema, &[binary]));
    let string = messages(&stream(&arrow_schema(), &[batch_of(arrow_schema(), &[1])]));
    let mut body = string[0].clone();
    body.extend_from_slice(&binary[1]);
    body.extend_from_slice(&string[2]);
    assert_eq!(
        defect(decode(body, limits()).await),
        ClientBatchDefect::InvalidData
    );
}

#[nervix_primitives::test]
async fn a_node_without_relay_memory_is_busy_rather_than_refusing_the_batch() {
    let executor = Executor::default();
    let mut held = Vec::<Reservation>::new();
    while let Ok(reservation) = executor.try_reserve(MemoryClass::Relay, 1024 * 1024) {
        held.push(reservation);
    }
    let body = stream(&arrow_schema(), &[batch_of(arrow_schema(), &[1])]);
    let refused = schema()
        .decode_client_batch(&executor, Bytes::from(body), limits())
        .await;
    let Err(error) = refused else {
        panic!("a node without relay memory decodes nothing");
    };
    assert!(matches!(error.current_context(), ClientBatchError::Busy));
    assert_eq!(error.current_context().defect(), None);
    drop(held);
}
