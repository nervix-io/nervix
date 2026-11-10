//! Batch containers: one payload carrying several records in the codec's own format.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The member value each record contributes to a batch, the container every format
//!   publishes a batch in, the `ON EMITTING BATCH` transformation that may replace that container,
//!   and encoding one candidate's container under the emitter's `MAX SIZE`.
//! - **Depends on.** The compiled codec, its jaq programs, and the bounded writer.
//! - **Must not know.** Which records a candidate holds and why, how a candidate that did not fit
//!   is divided, or where the payload is published. The emitter host chooses the members; this
//!   module encodes what it is given and answers.
//!
//! A member value is produced once per record and is exactly what the single-record path would
//! have encoded. Formats whose container is a concatenation of its members — schemaful JSON, CBOR
//! and Avro, and a protobuf batch message whose one field repeats the member message — keep each
//! member as its own encoded bytes, so a re-encoding after a subdivision only frames them again.
//! The jaq-native formats and a protobuf codec with a batch transformation keep the member's jaq
//! output instead, because their container is written, or transformed, as one value. These values
//! exist only while one buffered batch is being encoded; the retained records stay Arrow rows.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{
    io::{self, Write as _},
    num::NonZeroUsize,
};

use apache_avro::{Schema as AvroSchema, write_avro_datum_ref};
use ciborium_ll::{Encoder as CborEncoder, Header as CborHeader};
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_bounded_write::{BoundedWrite, BoundedWriter};
use nervix_jaq::{JaqNativeFormat, JaqProgramError};
use nervix_models::PayloadSizeLimit;
use prost::encoding::{WireType, encode_key, encode_varint};
use serde_json::{
    Map as JsonMap, Value as JsonValue,
    ser::{CompactFormatter, Formatter as _},
};

use super::{
    ArrowCodecRow, CodecContractError, CodecError, CompiledCodec, CompiledCodecBatchEncoder,
    CompiledWireSchema, PayloadLimitExceeded, encode_protobuf_payload, run_jaq_transformation,
    syslog::SyslogBatchMember,
};

/// The key a TOML batch holds its members under, and the root element an XML batch holds them in.
///
/// Neither format has a top-level sequence: a TOML document is a table and an XML document has one
/// root element.
const BATCH_CONTAINER_NAME: &str = "batch";

/// What one record contributes to the container of the batch that carries it.
///
/// Opaque outside this module: the host only asks whether two members may share a container.
#[derive(Debug, Clone)]
pub(crate) struct BatchMember(MemberValue);

#[derive(Debug, Clone)]
enum MemberValue {
    /// The record's complete encoding, framed as it is into the container.
    Encoded(Vec<u8>),
    /// The value the codec's `ON EMITTING` transformation produced for the record.
    Value(JsonValue),
    /// The RFC 5424 message the record encodes to on its own.
    Syslog(SyslogBatchMember),
}

impl BatchMember {
    /// Whether this member and `other` may share one container.
    ///
    /// Only a syslog frame carries a header of its own, so only syslog members can disagree about
    /// what their container says once for all of them.
    pub(crate) fn shares_container_with(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (MemberValue::Syslog(member), MemberValue::Syslog(other)) => {
                member.shares_frame_with(other)
            }
            (MemberValue::Encoded(_) | MemberValue::Value(_) | MemberValue::Syslog(_), _) => true,
        }
    }
}

/// What preparing one record as a batch member produced.
#[derive(Debug)]
pub(crate) enum BatchMemberEncoding {
    Member(BatchMember),
    /// The record's own encoding already exceeds `MAX SIZE`, and every container of it holds that
    /// encoding whole, so no batch can carry it.
    Oversize(PayloadLimitExceeded),
}

/// One candidate's container encoded under a payload size limit.
#[derive(Debug)]
pub(crate) enum BoundedBatchEncoding {
    /// The complete encoding, whose length is its exact size.
    Encoded(Vec<u8>),
    /// The encoding reached the limit and was abandoned there.
    Oversize(PayloadLimitExceeded),
}

/// Why a candidate's container could not be produced.
///
/// These describe the candidate rather than any one member, so every member of it shares the
/// failure. None of them quotes a payload value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BatchContainerError {
    #[error("ON EMITTING BATCH produced no output")]
    NoOutput,
    #[error("ON EMITTING BATCH produced more than one output")]
    MultipleOutputs,
    #[error("ON EMITTING BATCH evaluation failed")]
    Evaluation,
    #[error("cannot write the batch as {encoding}")]
    Unwritable { encoding: &'static str },
}

impl From<&JaqProgramError> for BatchContainerError {
    fn from(error: &JaqProgramError) -> Self {
        match error {
            JaqProgramError::NoOutput => Self::NoOutput,
            JaqProgramError::MultipleOutputs => Self::MultipleOutputs,
            JaqProgramError::Compile { .. }
            | JaqProgramError::Eval { .. }
            | JaqProgramError::BinaryStringNotJson
            | JaqProgramError::InvalidJsonText { .. }
            | JaqProgramError::JsonObjectKeyType { .. }
            | JaqProgramError::InvalidJsonNumber { .. } => Self::Evaluation,
        }
    }
}

impl CompiledCodecBatchEncoder<'_> {
    /// Prepares row `row_index` as a batch member, held to `limit` where the member's own encoding
    /// is part of every container that carries it.
    ///
    /// A failure here is the record's own and rejects it alone, exactly as a failed single-record
    /// encoding does.
    pub(crate) fn batch_member(
        &self,
        row_index: usize,
        limit: PayloadSizeLimit,
    ) -> error_stack::Result<BatchMemberEncoding, CodecError> {
        let codec = self.codec;
        match &codec.wire_schema {
            CompiledWireSchema::Json(_)
            | CompiledWireSchema::Cbor(_)
            | CompiledWireSchema::Avro(_) => self.encoded_member(row_index, limit),
            CompiledWireSchema::Protobuf(protobuf) => {
                if protobuf.transformations.on_emitting_batch.is_none() {
                    return self.encoded_member(row_index, limit);
                }
                let value = self.emitting_value(row_index)?;
                Ok(BatchMemberEncoding::Member(BatchMember(
                    MemberValue::Value(value),
                )))
            }
            CompiledWireSchema::JaqNative(native) => {
                let value = self.emitting_value(row_index)?;
                if native.transformations.on_emitting_batch.is_none() {
                    // Without a batch transformation the member is written into the container as
                    // it is, so it must be a value the format can write.
                    native
                        .format
                        .write_value_into(&value, &mut io::sink())
                        .map_err(|error| {
                            Report::new(CodecError::JaqNativeEncode {
                                codec: codec.name.as_str().to_string(),
                                format: native.format.name(),
                                reason: error.to_string(),
                                report: error,
                            })
                        })?;
                }
                Ok(BatchMemberEncoding::Member(BatchMember(
                    MemberValue::Value(value),
                )))
            }
            CompiledWireSchema::Syslog(_) => {
                self.check_row(row_index)?;
                let row = ArrowCodecRow::new(codec, self.batch, row_index);
                let member = SyslogBatchMember::from_row(&row)?;
                if member.len() > limit_bytes(limit).get() {
                    return Ok(BatchMemberEncoding::Oversize(
                        codec.payload_limit_exceeded(limit),
                    ));
                }
                Ok(BatchMemberEncoding::Member(BatchMember(
                    MemberValue::Syslog(member),
                )))
            }
        }
    }

    fn encoded_member(
        &self,
        row_index: usize,
        limit: PayloadSizeLimit,
    ) -> error_stack::Result<BatchMemberEncoding, CodecError> {
        match self.encode_row_within(row_index, limit)? {
            super::BoundedRowEncoding::Encoded(payload) => Ok(BatchMemberEncoding::Member(
                BatchMember(MemberValue::Encoded(payload)),
            )),
            super::BoundedRowEncoding::Oversize(exceeded) => {
                Ok(BatchMemberEncoding::Oversize(exceeded))
            }
        }
    }

    /// The value the codec's `ON EMITTING` transformation produces for row `row_index`.
    fn emitting_value(&self, row_index: usize) -> error_stack::Result<JsonValue, CodecError> {
        let codec = self.codec;
        self.check_row(row_index)?;
        let Some(program) = codec.on_emitting() else {
            return Err(codec.contract_violation(CodecContractError::OnEmittingRequired));
        };
        let row = ArrowCodecRow::new(codec, self.batch, row_index);
        run_jaq_transformation(codec, program, row.to_json_value()?)
    }
}

impl CompiledCodec {
    /// Encodes the container of a batch holding `members`, in order, under `limit`.
    ///
    /// The encoding is abandoned at the first write that would take it past the limit, so an
    /// oversize container is never built in full. `members` is never empty: an empty batch is
    /// never published.
    pub(crate) fn encode_batch_within(
        &self,
        members: &[&BatchMember],
        limit: PayloadSizeLimit,
    ) -> error_stack::Result<BoundedBatchEncoding, BatchContainerError> {
        let written = BoundedWriter::write_with(limit_bytes(limit), |writer| {
            self.write_batch(members, writer)
        })?;
        match written {
            BoundedWrite::Complete(payload) => Ok(BoundedBatchEncoding::Encoded(payload)),
            BoundedWrite::LimitReached => Ok(BoundedBatchEncoding::Oversize(
                self.payload_limit_exceeded(limit),
            )),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the external JSON, CBOR, Avro, Arrow and payload writer \
                                   interfaces encode one admitted batch")
    )]
    fn write_batch(
        &self,
        members: &[&BatchMember],
        output: &mut BoundedWriter,
    ) -> error_stack::Result<(), BatchContainerError> {
        let encoding = self.wire_schema.encoding_name();
        let unwritable = |cause: io::Error| {
            Report::new(cause).change_context(BatchContainerError::Unwritable { encoding })
        };
        match &self.wire_schema {
            CompiledWireSchema::Json(_) => {
                // A compact array, whose brackets and separators serde_json's formatter writes.
                // Each member is already a complete JSON text, so its bytes go between them as
                // they are, neither parsed nor checked: the formatter's raw-fragment write would
                // place them the same way, but it takes a `str`, which costs a UTF-8 check.
                let mut formatter = CompactFormatter;
                formatter.begin_array(output).map_err(unwritable)?;
                for (index, member) in encoded_members(members, encoding)?.enumerate() {
                    let first = index == 0;
                    formatter
                        .begin_array_value(output, first)
                        .map_err(unwritable)?;
                    output.write_all(member).map_err(unwritable)?;
                    formatter.end_array_value(output).map_err(unwritable)?;
                }
                formatter.end_array(output).map_err(unwritable)
            }
            CompiledWireSchema::Cbor(_) => {
                // A definite-length array head, which ciborium writes in its shortest form, then
                // the members.
                CborEncoder::from(&mut *output)
                    .push(CborHeader::Array(Some(members.len())))
                    .map_err(unwritable)?;
                for member in encoded_members(members, encoding)? {
                    output.write_all(member).map_err(unwritable)?;
                }
                Ok(())
            }
            CompiledWireSchema::Avro(_) => {
                // One array block: the item count as an Avro long, the items, then the zero count
                // that ends the array. apache-avro writes each count as a long datum.
                let unwritable_count = |cause: apache_avro::Error| {
                    Report::new(cause).change_context(BatchContainerError::Unwritable { encoding })
                };
                let count = i64::try_from(members.len())
                    .assured("a batch holds at most 65,536 members, which fits an Avro long");
                write_avro_datum_ref(&AvroSchema::Long, &count, output)
                    .map_err(unwritable_count)?;
                for member in encoded_members(members, encoding)? {
                    output.write_all(member).map_err(unwritable)?;
                }
                write_avro_datum_ref(&AvroSchema::Long, &0_i64, output)
                    .map_err(unwritable_count)?;
                Ok(())
            }
            CompiledWireSchema::Protobuf(protobuf) => {
                let batch_message = protobuf
                    .batch_message
                    .as_ref()
                    .ok_or(BatchContainerError::Unwritable { encoding })?;
                if let Some(program) = protobuf.transformations.on_emitting_batch.as_deref() {
                    let value = run_batch_transformation(program, members)?;
                    let encoded = encode_protobuf_payload(batch_message, &value)
                        .change_context(BatchContainerError::Unwritable { encoding })?;
                    return output.write_all(&encoded).map_err(unwritable);
                }
                // The batch message's one field repeats the member message, so its encoding is
                // each member's bytes behind that field's key and length.
                let field = batch_message
                    .fields()
                    .next()
                    .ok_or(BatchContainerError::Unwritable { encoding })?;
                for member in encoded_members(members, encoding)? {
                    let mut framing = Vec::new();
                    encode_key(field.number(), WireType::LengthDelimited, &mut framing);
                    let length = u64::try_from(member.len()).assured(
                        "Nervix builds for 64-bit targets only, where u64 holds every usize",
                    );
                    encode_varint(length, &mut framing);
                    output.write_all(&framing).map_err(unwritable)?;
                    output.write_all(member).map_err(unwritable)?;
                }
                Ok(())
            }
            CompiledWireSchema::JaqNative(native) => {
                let value = match native.transformations.on_emitting_batch.as_deref() {
                    Some(program) => run_batch_transformation(program, members)?,
                    None => native_container(native.format, member_values(members, encoding)?),
                };
                native
                    .format
                    .write_value_into(&value, output)
                    .map_err(|cause| {
                        cause.change_context(BatchContainerError::Unwritable { encoding })
                    })
            }
            CompiledWireSchema::Syslog(_) => {
                let mut syslog_members = Vec::with_capacity(members.len());
                for member in members.iter().copied() {
                    let MemberValue::Syslog(member) = &member.0 else {
                        return Err(Report::new(BatchContainerError::Unwritable { encoding }));
                    };
                    syslog_members.push(member);
                }
                SyslogBatchMember::write_frame(&syslog_members, output).map_err(unwritable)
            }
        }
    }

    fn payload_limit_exceeded(&self, limit: PayloadSizeLimit) -> PayloadLimitExceeded {
        PayloadLimitExceeded {
            codec: self.name.clone(),
            encoding: self.wire_schema.encoding_name(),
            limit,
        }
    }

    fn on_emitting(&self) -> Option<&nervix_jaq::CompiledJaqProgram> {
        match &self.wire_schema {
            CompiledWireSchema::JaqNative(native) => native.transformations.on_emitting.as_deref(),
            CompiledWireSchema::Protobuf(protobuf) => {
                protobuf.transformations.on_emitting.as_deref()
            }
            CompiledWireSchema::Json(_)
            | CompiledWireSchema::Cbor(_)
            | CompiledWireSchema::Avro(_)
            | CompiledWireSchema::Syslog(_) => None,
        }
    }
}

fn limit_bytes(limit: PayloadSizeLimit) -> NonZeroUsize {
    NonZeroUsize::try_from(limit.bytes())
        .assured("Nervix builds for 64-bit targets only, where usize holds every u64")
}

/// The encoded bytes of `members`, which a concatenating container frames in order.
fn encoded_members<'a>(
    members: &[&'a BatchMember],
    encoding: &'static str,
) -> error_stack::Result<impl Iterator<Item = &'a [u8]>, BatchContainerError> {
    let mut encoded = Vec::with_capacity(members.len());
    for member in members.iter().copied() {
        let MemberValue::Encoded(bytes) = &member.0 else {
            return Err(Report::new(BatchContainerError::Unwritable { encoding }));
        };
        encoded.push(bytes.as_slice());
    }
    Ok(encoded.into_iter())
}

/// The values of `members`, as the array a container or a batch transformation receives.
fn member_values(
    members: &[&BatchMember],
    encoding: &'static str,
) -> error_stack::Result<Vec<JsonValue>, BatchContainerError> {
    let mut values = Vec::with_capacity(members.len());
    for member in members.iter().copied() {
        let MemberValue::Value(value) = &member.0 else {
            return Err(Report::new(BatchContainerError::Unwritable { encoding }));
        };
        values.push(value.clone());
    }
    Ok(values)
}

/// Runs a batch transformation over the array of `members`' values and requires exactly one
/// output.
fn run_batch_transformation(
    program: &nervix_jaq::CompiledJaqProgram,
    members: &[&BatchMember],
) -> error_stack::Result<JsonValue, BatchContainerError> {
    let mut values = Vec::with_capacity(members.len());
    for member in members.iter().copied() {
        let MemberValue::Value(value) = &member.0 else {
            return Err(Report::new(BatchContainerError::Evaluation));
        };
        values.push(value.clone());
    }
    {
        let mapping_input = program.run_single(JsonValue::Array(values));
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let context = BatchContainerError::from(error.current_context());
                error.change_context(context)
            }),
        }
    }
}

/// The default container of a jaq-native format: an array for the formats with a top-level
/// sequence, a single `batch` key for TOML and a single `batch` root element for XML.
fn native_container(format: JaqNativeFormat, values: Vec<JsonValue>) -> JsonValue {
    match format {
        JaqNativeFormat::Toml => {
            let mut table = JsonMap::new();
            table.insert(BATCH_CONTAINER_NAME.to_string(), JsonValue::Array(values));
            JsonValue::Object(table)
        }
        JaqNativeFormat::Xml => {
            // jaq reads and writes an XML element as an object holding its tag in `t` and its
            // children in `c`.
            let mut element = JsonMap::new();
            element.insert(
                "t".to_string(),
                JsonValue::String(BATCH_CONTAINER_NAME.to_string()),
            );
            element.insert("c".to_string(), JsonValue::Array(values));
            JsonValue::Object(element)
        }
        JaqNativeFormat::Json
        | JaqNativeFormat::Yaml
        | JaqNativeFormat::Cbor
        | JaqNativeFormat::Raw => JsonValue::Array(values),
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::{Schema as AvroSchema, from_avro_datum, types::Value as AvroValue};
    use nervix_models::{
        AvroType, ByteSizeUnit, CborType, CodecJaqFormat, CodecJaqTransformations,
        CodecProtobufConfig, CodecWireFormat, CreateCodec, CreateSchema, CreateWireSchema,
        JsonType, ParseAsType, ResolvedCodecWireFormat, SchemaField, WireSchemaField,
    };
    use nervix_primitives::sync::Arc;
    use prost::Message as _;
    use prost_reflect::DynamicMessage;
    use serde_json::json;

    use super::*;
    use crate::runtime_schema::{
        ProtobufCodecDescriptors, ProtobufDescriptorPool, RuntimeRecordBatch, RuntimeRow,
        RuntimeValue, compile_codec_spec_with_protobuf, compile_schema, test_runtime_row,
    };

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn limit(bytes: u64) -> PayloadSizeLimit {
        PayloadSizeLimit::new(
            std::num::NonZeroU64::new(bytes).expect("a positive limit"),
            ByteSizeUnit::B,
        )
        .expect("a byte count fits 64 bits")
    }

    fn event_schema() -> CreateSchema {
        CreateSchema {
            labels: Default::default(),
            name: named("event"),
            fields: vec![
                SchemaField {
                    name: named("seq"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("note"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
            ],
        }
    }

    fn event(seq: i64, note: &str) -> RuntimeRow {
        test_runtime_row([
            ("seq".to_string(), RuntimeValue::I64(seq)),
            ("note".to_string(), RuntimeValue::String(note.to_string())),
        ])
    }

    fn events() -> RuntimeRecordBatch {
        let rows = [event(1, "a"), event(2, "b"), event(3, "c")];
        let batches = rows
            .iter()
            .map(RuntimeRow::one_row_batch)
            .collect::<Vec<_>>();
        RuntimeRecordBatch::concat(&batches.iter().collect::<Vec<_>>())
            .expect("rows should concatenate")
    }

    fn event_wire<T>(seq: T, note: T) -> CreateWireSchema<T> {
        CreateWireSchema {
            labels: Default::default(),
            name: named("event_wire"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("seq"),
                    ty: seq,
                    optional: false,
                },
                WireSchemaField {
                    name: named("note"),
                    ty: note,
                    optional: false,
                },
            ],
        }
    }

    fn avro_wire() -> CreateWireSchema<AvroType> {
        CreateWireSchema {
            labels: Default::default(),
            name: named("event_avro"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("seq"),
                    ty: AvroType::Long,
                    optional: false,
                },
                WireSchemaField {
                    name: named("note"),
                    ty: AvroType::String,
                    optional: false,
                },
            ],
        }
    }

    fn codec_named(name: &str, wire_format: CodecWireFormat) -> CreateCodec {
        CreateCodec {
            labels: Default::default(),
            name: named(name),
            wire_format,
            schema: named("event"),
            encoding_rules: Vec::new(),
        }
    }

    fn transformations(
        on_emitting: &str,
        on_emitting_batch: Option<&str>,
    ) -> CodecJaqTransformations {
        CodecJaqTransformations {
            on_ingestion: None,
            on_emitting: Some(on_emitting.to_string()),
            on_emitting_batch: on_emitting_batch.map(str::to_string),
        }
    }

    fn compile(
        codec: &CreateCodec,
        wire_format: ResolvedCodecWireFormat<'_>,
        descriptors: Option<ProtobufCodecDescriptors>,
    ) -> Arc<CompiledCodec> {
        compile_codec_spec_with_protobuf(
            &codec.name,
            &codec.encoding_rules,
            Arc::new(compile_schema(&event_schema())),
            wire_format,
            descriptors,
        )
        .expect("codec should compile")
    }

    fn jaq_codec(
        format: CodecJaqFormat,
        on_emitting: &str,
        on_emitting_batch: Option<&str>,
    ) -> Arc<CompiledCodec> {
        let codec = codec_named(
            "event_codec",
            CodecWireFormat::JaqNative {
                format,
                transformations: transformations(on_emitting, on_emitting_batch),
            },
        );
        let transformations = transformations(on_emitting, on_emitting_batch);
        compile(
            &codec,
            ResolvedCodecWireFormat::JaqNative {
                format,
                transformations: &transformations,
            },
            None,
        )
    }

    fn members_of(
        codec: &CompiledCodec,
        batch: &RuntimeRecordBatch,
        limit: PayloadSizeLimit,
    ) -> Vec<BatchMember> {
        let encoder = codec
            .batch_encoder(batch)
            .expect("the batch fits the codec");
        (0..batch.batch().num_rows())
            .map(|row| match encoder.batch_member(row, limit) {
                Ok(BatchMemberEncoding::Member(member)) => member,
                other => panic!("row {row} should be a member, got {other:?}"),
            })
            .collect()
    }

    fn container(codec: &CompiledCodec) -> Result<Vec<u8>, BatchContainerError> {
        let batch = events();
        let members = members_of(codec, &batch, limit(4096));
        let references = members.iter().collect::<Vec<_>>();
        let encoded = codec
            .encode_batch_within(&references, limit(4096))
            .map_err(|error| *error.current_context())?;
        match encoded {
            BoundedBatchEncoding::Encoded(payload) => Ok(payload),
            BoundedBatchEncoding::Oversize(exceeded) => panic!("unexpected {exceeded}"),
        }
    }

    fn event_values() -> JsonValue {
        json!([{"seq": 1, "note": "a"}, {"seq": 2, "note": "b"}, {"seq": 3, "note": "c"}])
    }

    #[test]
    fn a_wire_json_batch_is_a_compact_array_of_the_members() {
        let codec = codec_named(
            "event_codec",
            CodecWireFormat::Json {
                wire_schema: named("event_wire"),
            },
        );
        let wire = event_wire(JsonType::Integer, JsonType::String);
        let codec = compile(&codec, ResolvedCodecWireFormat::Json((&wire).into()), None);

        let payload = container(&codec).expect("the container should encode");

        assert_eq!(
            String::from_utf8(payload).expect("JSON is text"),
            r#"[{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"c"}]"#
        );
    }

    #[test]
    fn a_wire_cbor_batch_is_a_definite_length_array_of_the_members() {
        let codec = codec_named(
            "event_codec",
            CodecWireFormat::Cbor {
                wire_schema: named("event_wire"),
            },
        );
        let wire = event_wire(CborType::Integer, CborType::Text);
        let codec = compile(&codec, ResolvedCodecWireFormat::Cbor((&wire).into()), None);

        let payload = container(&codec).expect("the container should encode");

        assert_eq!(payload.first(), Some(&0x83));
        let decoded: JsonValue =
            ciborium::from_reader(payload.as_slice()).expect("the container is one CBOR item");
        assert_eq!(decoded, event_values());
    }

    #[test]
    fn a_wire_avro_batch_is_one_array_datum_of_the_record_schema() {
        let codec = codec_named(
            "event_codec",
            CodecWireFormat::Avro {
                wire_schema: named("event_avro"),
            },
        );
        let wire = avro_wire();
        let codec = compile(&codec, ResolvedCodecWireFormat::Avro((&wire).into()), None);

        let payload = container(&codec).expect("the container should encode");

        assert_eq!(
            payload,
            vec![
                0x06, 0x02, 0x02, b'a', 0x04, 0x02, b'b', 0x06, 0x02, b'c', 0x00
            ]
        );
        let schema = AvroSchema::parse_str(
            r#"{"type": "array", "items": {"type": "record", "name": "event", "fields": [
                {"name": "seq", "type": "long"}, {"name": "note", "type": "string"}]}}"#,
        )
        .expect("the array schema parses");
        let decoded = from_avro_datum(&schema, &mut payload.as_slice(), None)
            .expect("the container is one array datum");
        let AvroValue::Array(items) = decoded else {
            panic!("an array datum decodes to an array");
        };
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn jaq_native_batches_use_each_format_container() {
        for (format, expected, read_back) in [
            (
                CodecJaqFormat::Json,
                r#"[{"seq": 1, "note": "a"}, {"seq": 2, "note": "b"}, {"seq": 3, "note": "c"}]"#,
                event_values(),
            ),
            (
                CodecJaqFormat::Yaml,
                r#"[{"seq": 1, "note": "a"}, {"seq": 2, "note": "b"}, {"seq": 3, "note": "c"}]"#,
                event_values(),
            ),
            (
                CodecJaqFormat::Toml,
                "[[batch]]\nseq = 1\nnote = \"a\"\n\n[[batch]]\nseq = 2\nnote = \
                 \"b\"\n\n[[batch]]\nseq = 3\nnote = \"c\"\n",
                json!({"batch": event_values()}),
            ),
        ] {
            let codec = jaq_codec(format, ".", None);
            let payload = container(&codec).expect("the container should encode");
            let decoded = JaqNativeFormat::from(format)
                .read_single_value(&payload)
                .expect("the container reads back as one value");
            assert_eq!(String::from_utf8_lossy(&payload), expected, "{format:?}");
            assert_eq!(decoded, read_back, "{format:?}");
        }

        let codec = jaq_codec(CodecJaqFormat::Cbor, ".", None);
        let payload = container(&codec).expect("the container should encode");
        assert_eq!(payload.first(), Some(&0x83));
        let decoded = JaqNativeFormat::Cbor
            .read_single_value(&payload)
            .expect("the container reads back as one CBOR item");
        assert_eq!(decoded, event_values());
    }

    #[test]
    fn a_jaq_native_xml_batch_is_one_batch_root_element() {
        let codec = jaq_codec(
            CodecJaqFormat::Xml,
            r#"{t: "event", a: {seq: (.seq | tostring), note: .note}}"#,
            None,
        );

        let payload = container(&codec).expect("the container should encode");

        assert_eq!(
            String::from_utf8_lossy(&payload),
            r#"<batch><event seq="1" note="a"/><event seq="2" note="b"/><event seq="3" note="c"/></batch>"#
        );
        let decoded = JaqNativeFormat::Xml
            .read_single_value(&payload)
            .expect("the container reads back as one root element");
        assert_eq!(decoded.get("t"), Some(&json!("batch")));
    }

    #[test]
    fn a_batch_transformation_replaces_the_container() {
        for (program, expected) in [
            (
                ".",
                r#"[{"seq": 1, "note": "a"}, {"seq": 2, "note": "b"}, {"seq": 3, "note": "c"}]"#,
            ),
            ("map(.seq)", "[1, 2, 3]"),
            (
                "{count: length, records: map(.note)}",
                r#"{"count": 3, "records": ["a", "b", "c"]}"#,
            ),
        ] {
            let codec = jaq_codec(CodecJaqFormat::Json, ".", Some(program));
            let payload = container(&codec).expect("the transformed container should encode");
            assert_eq!(
                String::from_utf8(payload).expect("JSON is text"),
                expected,
                "{program}"
            );
        }
    }

    #[test]
    fn batch_transformation_failures_are_typed_and_quote_no_value() {
        for (format, program, expected) in [
            (CodecJaqFormat::Json, "empty", BatchContainerError::NoOutput),
            (
                CodecJaqFormat::Json,
                ".[]",
                BatchContainerError::MultipleOutputs,
            ),
            (
                CodecJaqFormat::Json,
                r#"error("secret")"#,
                BatchContainerError::Evaluation,
            ),
            (
                CodecJaqFormat::Toml,
                ".",
                BatchContainerError::Unwritable { encoding: "TOML" },
            ),
        ] {
            let codec = jaq_codec(format, ".", Some(program));
            assert_eq!(container(&codec).err(), Some(expected), "{program}");
        }
    }

    #[test]
    fn a_member_the_format_cannot_write_fails_alone() {
        let codec = jaq_codec(CodecJaqFormat::Toml, ".seq", None);
        let batch = events();
        let encoder = codec
            .batch_encoder(&batch)
            .expect("the batch fits the codec");

        let error = encoder
            .batch_member(0, limit(4096))
            .expect_err("a TOML document must be a table");

        assert!(matches!(
            error.current_context(),
            CodecError::JaqNativeEncode { format: "TOML", .. }
        ));
    }

    #[test]
    fn an_encoding_that_reaches_the_limit_is_oversize() {
        let codec = jaq_codec(CodecJaqFormat::Json, ".", None);
        let batch = events();
        let members = members_of(&codec, &batch, limit(4096));
        let references = members.iter().collect::<Vec<_>>();

        let encoded = codec
            .encode_batch_within(&references, limit(10))
            .expect("reaching the limit is an outcome");

        let BoundedBatchEncoding::Oversize(exceeded) = encoded else {
            panic!("a 3-member array does not fit 10 bytes");
        };
        assert_eq!(
            exceeded.to_string(),
            "codec 'event_codec' JSON payload exceeds MAX SIZE 10B"
        );
    }

    #[test]
    fn a_wire_member_whose_own_encoding_exceeds_the_limit_is_oversize() {
        let codec = codec_named(
            "event_codec",
            CodecWireFormat::Json {
                wire_schema: named("event_wire"),
            },
        );
        let wire = event_wire(JsonType::Integer, JsonType::String);
        let codec = compile(&codec, ResolvedCodecWireFormat::Json((&wire).into()), None);
        let batch = events();
        let encoder = codec
            .batch_encoder(&batch)
            .expect("the batch fits the codec");

        let member = encoder
            .batch_member(0, limit(8))
            .expect("reaching the limit is an outcome");

        assert!(matches!(member, BatchMemberEncoding::Oversize(_)));
    }

    fn protobuf_descriptors() -> ProtobufCodecDescriptors {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let proto_path = dir.path().join("event.proto");
        std::fs::write(
            &proto_path,
            r#"
                syntax = "proto3";
                package nervix.test;
                message Event { int64 seq = 1; string note = 2; }
                message EventBatch { repeated Event events = 1; }
                message EventEnvelope { uint32 count = 1; repeated Event records = 2; }
            "#,
        )
        .expect("proto file should be written");
        let file_descriptor_set =
            protox::compile([proto_path], [dir.path()]).expect("proto should compile");
        let pool = ProtobufDescriptorPool::from_file_descriptor_set(file_descriptor_set)
            .expect("descriptor pool should be built");
        ProtobufCodecDescriptors {
            message: pool.message("nervix.test.Event").expect("Event exists"),
            batch_message: Some(
                pool.message("nervix.test.EventBatch")
                    .expect("EventBatch exists"),
            ),
        }
    }

    fn protobuf_codec(batch_message: &str, on_emitting_batch: Option<&str>) -> Arc<CompiledCodec> {
        let config = CodecProtobufConfig {
            resource: named("proto_bundle"),
            resource_version: 1,
            config: Vec::new(),
            message: "nervix.test.Event".to_string(),
            batch_message: Some(batch_message.to_string()),
            transformations: transformations("{seq: .seq, note: .note}", on_emitting_batch),
        };
        let codec = codec_named("event_codec", CodecWireFormat::Protobuf(config.clone()));
        let mut descriptors = protobuf_descriptors();
        let pool_batch = descriptors
            .batch_message
            .as_ref()
            .expect("EventBatch exists")
            .parent_pool()
            .get_message_by_name(batch_message)
            .expect("the batch message exists");
        descriptors.batch_message = Some(pool_batch);
        compile(
            &codec,
            ResolvedCodecWireFormat::Protobuf(&config),
            Some(descriptors),
        )
    }

    #[test]
    fn a_protobuf_batch_fills_the_batch_message_repeated_field() {
        let codec = protobuf_codec("nervix.test.EventBatch", None);

        let payload = container(&codec).expect("the container should encode");

        let descriptors = protobuf_descriptors();
        let batch_message = descriptors.batch_message.expect("EventBatch exists");
        let decoded = DynamicMessage::decode(batch_message, payload.as_slice())
            .expect("the payload is one EventBatch");
        let events = decoded
            .get_field_by_name("events")
            .expect("events is declared");
        assert_eq!(events.as_list().map(<[_]>::len), Some(3));
        assert_eq!(decoded.encode_to_vec(), payload);
    }

    #[test]
    fn a_protobuf_batch_transformation_builds_any_batch_message() {
        let codec = protobuf_codec(
            "nervix.test.EventEnvelope",
            Some("{count: length, records: .}"),
        );

        let payload = container(&codec).expect("the container should encode");

        let descriptors = protobuf_descriptors();
        let envelope = descriptors
            .message
            .parent_pool()
            .get_message_by_name("nervix.test.EventEnvelope")
            .expect("EventEnvelope exists");
        let decoded = DynamicMessage::decode(envelope, payload.as_slice())
            .expect("the payload is one EventEnvelope");
        assert_eq!(
            decoded
                .get_field_by_name("count")
                .and_then(|count| count.as_u32()),
            Some(3)
        );
    }
    #[test]
    fn a_syslog_batch_is_one_frame_whose_message_is_the_members_messages() {
        let schema = CreateSchema {
            labels: Default::default(),
            name: named("syslog_event"),
            fields: [
                ("facility", ParseAsType::U8),
                ("severity", ParseAsType::U8),
                ("timestamp", ParseAsType::Datetime),
                ("hostname", ParseAsType::String),
                ("message", ParseAsType::String),
            ]
            .into_iter()
            .map(|(name, ty)| SchemaField {
                name: named(name),
                ty,
                optional: name == "hostname" || name == "timestamp",
                sensitive: false,
            })
            .collect(),
        };
        let codec: CreateCodec = CreateCodec {
            labels: Default::default(),
            name: named("syslog_codec"),
            wire_format: CodecWireFormat::Syslog,
            schema: named("syslog_event"),
            encoding_rules: Vec::new(),
        };
        let compiled_schema = Arc::new(compile_schema(&schema));
        let codec = compile_codec_spec_with_protobuf(
            &codec.name,
            &codec.encoding_rules,
            compiled_schema.clone(),
            ResolvedCodecWireFormat::Syslog,
            None,
        )
        .expect("the SYSLOG codec should compile");
        let arrow_schema = compiled_schema.arrow_schema();
        let columns: Vec<arrow_array::ArrayRef> = vec![
            nervix_primitives::sync::StdArc::new(arrow_array::UInt8Array::from(vec![16, 16, 16])),
            nervix_primitives::sync::StdArc::new(arrow_array::UInt8Array::from(vec![6, 6, 3])),
            nervix_primitives::sync::StdArc::new(
                arrow_array::TimestampNanosecondArray::from(vec![
                    946_684_800_000_000_000,
                    946_684_801_000_000_000,
                    946_684_802_000_000_000,
                ])
                .with_timezone("+00:00"),
            ),
            nervix_primitives::sync::StdArc::new(arrow_array::StringArray::from(vec!["app-01"; 3])),
            nervix_primitives::sync::StdArc::new(arrow_array::StringArray::from(vec![
                "order accepted",
                "say \"hi\"",
                "order failed",
            ])),
        ];
        let batch = RuntimeRecordBatch::from_record_batch(
            arrow_schema.clone(),
            arrow_array::RecordBatch::try_new(arrow_schema, columns)
                .expect("the columns match the schema"),
        )
        .expect("the batch matches the schema");

        let members = members_of(&codec, &batch, limit(4096));
        let [accepted, quoted, failed] = members.as_slice() else {
            panic!("three rows make three members");
        };
        assert!(accepted.shares_container_with(quoted));
        assert!(!accepted.shares_container_with(failed));
        let encoded = codec
            .encode_batch_within(&[accepted, quoted], limit(4096))
            .expect("the frame should encode");

        let BoundedBatchEncoding::Encoded(payload) = encoded else {
            panic!("the frame fits 4096 bytes");
        };
        assert_eq!(
            String::from_utf8(payload).expect("syslog is text"),
            r#"<134>1 2000-01-01T00:00:00Z app-01 - - - - ["<134>1 2000-01-01T00:00:00Z app-01 - - - - order accepted","<134>1 2000-01-01T00:00:01Z app-01 - - - - say \"hi\""]"#
        );
        let member = codec
            .batch_encoder(&batch)
            .expect("the batch fits the codec")
            .batch_member(0, limit(30))
            .expect("reaching the limit is an outcome");
        assert!(matches!(member, BatchMemberEncoding::Oversize(_)));
    }

    fn json_event_codec() -> Arc<CompiledCodec> {
        let codec = codec_named(
            "event_codec",
            CodecWireFormat::Json {
                wire_schema: named("event_wire"),
            },
        );
        let wire = event_wire(JsonType::Integer, JsonType::String);
        compile(&codec, ResolvedCodecWireFormat::Json((&wire).into()), None)
    }

    fn cbor_event_codec() -> Arc<CompiledCodec> {
        let codec = codec_named(
            "event_codec",
            CodecWireFormat::Cbor {
                wire_schema: named("event_wire"),
            },
        );
        let wire = event_wire(CborType::Integer, CborType::Text);
        compile(&codec, ResolvedCodecWireFormat::Cbor((&wire).into()), None)
    }

    fn avro_event_codec() -> Arc<CompiledCodec> {
        let codec = codec_named(
            "event_codec",
            CodecWireFormat::Avro {
                wire_schema: named("event_avro"),
            },
        );
        let wire = avro_wire();
        compile(&codec, ResolvedCodecWireFormat::Avro((&wire).into()), None)
    }

    /// A container of the three events' members repeated to a count, beside the members' own
    /// encodings concatenated in the same order.
    struct RepeatedContainer {
        members: Vec<u8>,
        payload: Vec<u8>,
    }

    /// The container `codec` writes for `count` members, the three events repeated in order,
    /// under a limit no container of a batch reaches.
    fn repeated_container(codec: &CompiledCodec, count: usize) -> RepeatedContainer {
        let batch = events();
        let distinct = members_of(codec, &batch, limit(4096));
        let mut run = Vec::with_capacity(count);
        let mut members = Vec::new();
        for index in 0..count {
            let member = &distinct[index % distinct.len()];
            let MemberValue::Encoded(bytes) = &member.0 else {
                panic!("a schemaful member is its record's own encoding");
            };
            members.extend_from_slice(bytes);
            run.push(member);
        }
        let encoded = codec
            .encode_batch_within(&run, limit(1 << 24))
            .expect("a container of encoded members has no evaluation to fail");
        let BoundedBatchEncoding::Encoded(payload) = encoded else {
            panic!("{count} members fit 16 MiB");
        };
        RepeatedContainer { members, payload }
    }

    /// RFC 8949 §3 and §4.1: the array head is major type 4 with the count as its argument in its
    /// preferred serialization, inline below 24 and otherwise the shortest of one, two, four or
    /// eight big-endian bytes. A batch holds at most 65,536 members, the first count that needs
    /// four.
    #[test]
    fn a_cbor_container_heads_its_members_with_the_shortest_array_argument() {
        let codec = cbor_event_codec();
        for (count, head) in [
            (1, vec![0x81]),
            (23, vec![0x97]),
            (24, vec![0x98, 24]),
            (255, vec![0x98, 0xff]),
            (256, vec![0x99, 0x01, 0x00]),
            (65_535, vec![0x99, 0xff, 0xff]),
            (65_536, vec![0x9a, 0x00, 0x01, 0x00, 0x00]),
        ] {
            let container = repeated_container(&codec, count);

            let mut expected = head;
            expected.extend_from_slice(&container.members);
            assert_eq!(container.payload, expected, "{count} members");
        }
    }

    /// The Avro specification's binary encoding: an array is blocks of a long count and that many
    /// items, ended by a zero count, and a long is zig-zag encoded seven bits at a time, low group
    /// first. The container is one block, so its count needs a second byte from 64 members.
    #[test]
    fn an_avro_container_is_one_block_counted_by_a_zig_zag_long() {
        let codec = avro_event_codec();
        for (count, block_count) in [
            (1, vec![0x02]),
            (3, vec![0x06]),
            (63, vec![0x7e]),
            (64, vec![0x80, 0x01]),
            (65_536, vec![0x80, 0x80, 0x08]),
        ] {
            let container = repeated_container(&codec, count);

            let mut expected = block_count;
            expected.extend_from_slice(&container.members);
            expected.push(0x00);
            assert_eq!(container.payload, expected, "{count} members");
        }
    }

    /// A limit that ends inside a container's count refuses the container as a whole, however the
    /// count is written: the payload is never cut after a partial count.
    #[test]
    fn a_limit_inside_the_container_count_is_oversize() {
        for (codec, count) in [(cbor_event_codec(), 24), (avro_event_codec(), 64)] {
            let batch = events();
            let distinct = members_of(&codec, &batch, limit(4096));
            let mut run = Vec::with_capacity(count);
            for index in 0..count {
                run.push(&distinct[index % distinct.len()]);
            }

            let encoded = codec
                .encode_batch_within(&run, limit(1))
                .expect("reaching the limit is an outcome");

            let BoundedBatchEncoding::Oversize(exceeded) = encoded else {
                panic!("a two-byte count does not fit one byte");
            };
            assert_eq!(exceeded.limit, limit(1), "{count} members");
        }
    }

    /// RFC 8259 §5: an array is `[`, its values separated by `,`, then `]`, and the container adds
    /// no insignificant whitespace. One member takes no separator, and the batch maximum takes one
    /// between every two members.
    #[test]
    fn a_wire_json_container_separates_its_members_with_one_comma_each() {
        let codec = json_event_codec();
        let batch = events();
        let distinct = members_of(&codec, &batch, limit(4096));
        for count in [1, 2, 3, 65_536] {
            let mut run = Vec::with_capacity(count);
            let mut expected = vec![b'['];
            for index in 0..count {
                let member = &distinct[index % distinct.len()];
                let MemberValue::Encoded(bytes) = &member.0 else {
                    panic!("a schemaful member is its record's own encoding");
                };
                if index > 0 {
                    expected.push(b',');
                }
                expected.extend_from_slice(bytes);
                run.push(member);
            }
            expected.push(b']');

            let encoded = codec
                .encode_batch_within(&run, limit(1 << 24))
                .expect("a container of encoded members has no evaluation to fail");

            let BoundedBatchEncoding::Encoded(payload) = encoded else {
                panic!("{count} members fit 16 MiB");
            };
            assert_eq!(payload, expected, "{count} members");
        }
    }

    /// The bounded writer refuses each write of the array whole, so a limit that ends inside the
    /// first member, on the separator after it or on the closing bracket makes the container
    /// oversize instead of cutting it, and exactly the container's length completes it.
    #[test]
    fn a_limit_on_a_json_member_separator_or_closing_bracket_is_oversize() {
        let codec = json_event_codec();
        let batch = events();
        let distinct = members_of(&codec, &batch, limit(4096));
        let run = distinct.iter().collect::<Vec<_>>();
        let MemberValue::Encoded(first) = &distinct[0].0 else {
            panic!("a schemaful member is its record's own encoding");
        };
        let complete = container(&codec).expect("the container should encode");
        let length = u64::try_from(complete.len()).expect("a container's length fits u64");
        let through_first = u64::try_from(first.len() + 1).expect("a member's length fits u64");

        for refused in [1, through_first, length - 1] {
            let encoded = codec
                .encode_batch_within(&run, limit(refused))
                .expect("reaching the limit is an outcome");

            let BoundedBatchEncoding::Oversize(exceeded) = encoded else {
                panic!("a {length}-byte container does not fit {refused} bytes");
            };
            assert_eq!(exceeded.limit, limit(refused));
        }
        let encoded = codec
            .encode_batch_within(&run, limit(length))
            .expect("a container of encoded members has no evaluation to fail");
        let BoundedBatchEncoding::Encoded(payload) = encoded else {
            panic!("a {length}-byte container fits {length} bytes");
        };
        assert_eq!(payload, complete);
    }

    /// The container frames each member's bytes exactly as its record's encoding produced them. It
    /// neither parses, validates nor re-encodes a member, which keeps the framing after a
    /// subdivision to the punctuation alone.
    #[test]
    fn a_json_container_writes_its_members_without_reading_them() {
        let codec = json_event_codec();
        let unread = BatchMember(MemberValue::Encoded(b"\xff{\"seq\":".to_vec()));
        let text = BatchMember(MemberValue::Encoded(b"\"a\"".to_vec()));

        let encoded = codec
            .encode_batch_within(&[&unread, &text], limit(4096))
            .expect("a container of encoded members has no evaluation to fail");

        let BoundedBatchEncoding::Encoded(payload) = encoded else {
            panic!("two short members fit 4096 bytes");
        };
        assert_eq!(payload, b"[\xff{\"seq\":,\"a\"]".to_vec());
    }

    #[test]
    fn batch_transformation_failures_classify_without_the_program_reason() {
        assert_eq!(
            BatchContainerError::from(&JaqProgramError::NoOutput),
            BatchContainerError::NoOutput
        );
        assert_eq!(
            BatchContainerError::from(&JaqProgramError::MultipleOutputs),
            BatchContainerError::MultipleOutputs
        );
        assert_eq!(
            BatchContainerError::from(&JaqProgramError::Eval {
                reason: "cannot index number with \"secret\"".to_string()
            }),
            BatchContainerError::Evaluation
        );
        assert_eq!(
            BatchContainerError::Evaluation.to_string(),
            "ON EMITTING BATCH evaluation failed"
        );
    }
}
