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

use std::{
    io::{self, Write as _},
    num::NonZeroUsize,
};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_bounded_write::{BoundedWrite, BoundedWriter};
use nervix_jaq::{JaqNativeFormat, JaqProgramError};
use nervix_models::PayloadSizeLimit;
use prost::encoding::{WireType, encode_key, encode_varint};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::{
    ArrowCodecRow, CodecError, CompiledCodec, CompiledCodecBatchEncoder, CompiledWireSchema,
    PayloadLimitExceeded, encode_protobuf_payload, run_jaq_transformation,
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
                            })
                        })?;
                }
                Ok(BatchMemberEncoding::Member(BatchMember(
                    MemberValue::Value(value),
                )))
            }
            CompiledWireSchema::Syslog => {
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
            return Err(Report::new(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec used for encoding must declare ON EMITTING transformation"
                    .to_string(),
            }));
        };
        let row = ArrowCodecRow::new(codec, self.batch, row_index);
        Ok(run_jaq_transformation(
            codec,
            program,
            row.to_json_value()?,
        )?)
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
    ) -> Result<BoundedBatchEncoding, BatchContainerError> {
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

    fn write_batch(
        &self,
        members: &[&BatchMember],
        output: &mut BoundedWriter,
    ) -> Result<(), BatchContainerError> {
        let encoding = self.wire_schema.encoding_name();
        let unwritable = |_: io::Error| BatchContainerError::Unwritable { encoding };
        match &self.wire_schema {
            CompiledWireSchema::Json(_) => {
                output.write_all(b"[").map_err(unwritable)?;
                for (index, member) in encoded_members(members, encoding)?.enumerate() {
                    if index > 0 {
                        output.write_all(b",").map_err(unwritable)?;
                    }
                    output.write_all(member).map_err(unwritable)?;
                }
                output.write_all(b"]").map_err(unwritable)
            }
            CompiledWireSchema::Cbor(_) => {
                output
                    .write_all(&cbor_array_header(members.len()))
                    .map_err(unwritable)?;
                for member in encoded_members(members, encoding)? {
                    output.write_all(member).map_err(unwritable)?;
                }
                Ok(())
            }
            CompiledWireSchema::Avro(_) => {
                // One array block: the item count as an Avro long, the items, then the zero count
                // that ends the array.
                let count = i64::try_from(members.len())
                    .assured("a batch holds at most 65,536 members, which fits an Avro long");
                output.write_all(&avro_long(count)).map_err(unwritable)?;
                for member in encoded_members(members, encoding)? {
                    output.write_all(member).map_err(unwritable)?;
                }
                output.write_all(&avro_long(0)).map_err(unwritable)
            }
            CompiledWireSchema::Protobuf(protobuf) => {
                let batch_message = protobuf
                    .batch_message
                    .as_ref()
                    .ok_or(BatchContainerError::Unwritable { encoding })?;
                if let Some(program) = protobuf.transformations.on_emitting_batch.as_deref() {
                    let value = run_batch_transformation(program, members)?;
                    let encoded = encode_protobuf_payload(batch_message, &value)
                        .map_err(|_| BatchContainerError::Unwritable { encoding })?;
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
                    .map_err(|_| BatchContainerError::Unwritable { encoding })
            }
            CompiledWireSchema::Syslog => {
                let mut syslog_members = Vec::with_capacity(members.len());
                for member in members.iter().copied() {
                    let MemberValue::Syslog(member) = &member.0 else {
                        return Err(BatchContainerError::Unwritable { encoding });
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
            | CompiledWireSchema::Syslog => None,
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
) -> Result<impl Iterator<Item = &'a [u8]>, BatchContainerError> {
    let mut encoded = Vec::with_capacity(members.len());
    for member in members.iter().copied() {
        let MemberValue::Encoded(bytes) = &member.0 else {
            return Err(BatchContainerError::Unwritable { encoding });
        };
        encoded.push(bytes.as_slice());
    }
    Ok(encoded.into_iter())
}

/// The values of `members`, as the array a container or a batch transformation receives.
fn member_values(
    members: &[&BatchMember],
    encoding: &'static str,
) -> Result<Vec<JsonValue>, BatchContainerError> {
    let mut values = Vec::with_capacity(members.len());
    for member in members.iter().copied() {
        let MemberValue::Value(value) = &member.0 else {
            return Err(BatchContainerError::Unwritable { encoding });
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
) -> Result<JsonValue, BatchContainerError> {
    let mut values = Vec::with_capacity(members.len());
    for member in members.iter().copied() {
        let MemberValue::Value(value) = &member.0 else {
            return Err(BatchContainerError::Evaluation);
        };
        values.push(value.clone());
    }
    program
        .run_single(JsonValue::Array(values))
        .map_err(|error| BatchContainerError::from(&error))
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

/// The head of a definite-length CBOR array of `count` items: major type 4 with the count as its
/// argument, in the shortest form RFC 8949 allows.
fn cbor_array_header(count: usize) -> Vec<u8> {
    const MAJOR_ARRAY: u8 = 0x80;
    if let Ok(count) = u8::try_from(count)
        && count < 24
    {
        return vec![MAJOR_ARRAY | count];
    }
    if let Ok(count) = u8::try_from(count) {
        return vec![MAJOR_ARRAY | 24, count];
    }
    if let Ok(count) = u16::try_from(count) {
        let mut header = vec![MAJOR_ARRAY | 25];
        header.extend_from_slice(&count.to_be_bytes());
        return header;
    }
    if let Ok(count) = u32::try_from(count) {
        let mut header = vec![MAJOR_ARRAY | 26];
        header.extend_from_slice(&count.to_be_bytes());
        return header;
    }
    let count = u64::try_from(count)
        .assured("Nervix builds for 64-bit targets only, where u64 holds every usize");
    let mut header = vec![MAJOR_ARRAY | 27];
    header.extend_from_slice(&count.to_be_bytes());
    header
}

/// An Avro `long`: the zig-zag encoding of `value` as a variable-length integer.
fn avro_long(value: i64) -> Vec<u8> {
    // Zig-zag maps a signed value onto an unsigned one so small magnitudes stay short; the shifts
    // and the exclusive or are the encoding itself.
    let mut zigzag = ((value << 1) ^ (value >> 63)).cast_unsigned();
    let mut encoded = Vec::new();
    loop {
        let byte = u8::try_from(zigzag & 0x7f).assured("the value is masked to seven bits");
        zigzag >>= 7;
        if zigzag == 0 {
            encoded.push(byte);
            return encoded;
        }
        encoded.push(byte | 0x80);
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::{Schema as AvroSchema, from_avro_datum, types::Value as AvroValue};
    use nervix_models::{
        AvroType, ByteSizeUnit, CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig,
        CodecWireFormat, CreateCodec, CreateSchema, CreateWireSchema, JsonType, ParseAsType,
        ResolvedCodecWireFormat, SchemaField, WireSchemaField,
    };
    use prost::Message as _;
    use prost_reflect::DynamicMessage;
    use serde_json::json;
    use triomphe::Arc;

    use super::*;
    use crate::runtime_schema::{
        ProtobufCodecDescriptors, ProtobufDescriptorPool, RuntimeRecordBatch, RuntimeRow,
        RuntimeValue, compile_codec_with_protobuf, compile_schema, test_runtime_row,
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

    fn json_wire() -> CreateWireSchema<JsonType> {
        CreateWireSchema {
            name: named("event_wire"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("seq"),
                    ty: JsonType::Integer,
                    optional: false,
                },
                WireSchemaField {
                    name: named("note"),
                    ty: JsonType::String,
                    optional: false,
                },
            ],
        }
    }

    fn avro_wire() -> CreateWireSchema<AvroType> {
        CreateWireSchema {
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
        compile_codec_with_protobuf(
            codec,
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
        match codec.encode_batch_within(&references, limit(4096))? {
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
        let wire = json_wire();
        let codec = compile(&codec, ResolvedCodecWireFormat::Json(&wire), None);

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
        let wire = json_wire();
        let codec = compile(&codec, ResolvedCodecWireFormat::Cbor(&wire), None);

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
        let codec = compile(&codec, ResolvedCodecWireFormat::Avro(&wire), None);

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
                "[{seq: 1, note: a}, {seq: 2, note: b}, {seq: 3, note: c}]\n",
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
        let wire = json_wire();
        let codec = compile(&codec, ResolvedCodecWireFormat::Json(&wire), None);
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
            name: named("syslog_event"),
            fields: [
                ("facility", ParseAsType::U8),
                ("severity", ParseAsType::U8),
                ("hostname", ParseAsType::String),
                ("message", ParseAsType::String),
            ]
            .into_iter()
            .map(|(name, ty)| SchemaField {
                name: named(name),
                ty,
                optional: name == "hostname",
                sensitive: false,
            })
            .collect(),
        };
        let codec = CreateCodec {
            name: named("syslog_codec"),
            wire_format: CodecWireFormat::Syslog,
            schema: named("syslog_event"),
            encoding_rules: Vec::new(),
        };
        let compiled_schema = Arc::new(compile_schema(&schema));
        let codec = compile_codec_with_protobuf(
            &codec,
            compiled_schema.clone(),
            ResolvedCodecWireFormat::Syslog,
            None,
        )
        .expect("the SYSLOG codec should compile");
        let arrow_schema = compiled_schema.arrow_schema();
        let columns: Vec<arrow_array::ArrayRef> = vec![
            std::sync::Arc::new(arrow_array::UInt8Array::from(vec![16, 16, 16])),
            std::sync::Arc::new(arrow_array::UInt8Array::from(vec![6, 6, 3])),
            std::sync::Arc::new(arrow_array::StringArray::from(vec!["app-01"; 3])),
            std::sync::Arc::new(arrow_array::StringArray::from(vec![
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
            r#"<134>1 - app-01 - - - - ["<134>1 - app-01 - - - - order accepted","<134>1 - app-01 - - - - say \"hi\""]"#
        );
        let member = codec
            .batch_encoder(&batch)
            .expect("the batch fits the codec")
            .batch_member(0, limit(30))
            .expect("reaching the limit is an outcome");
        assert!(matches!(member, BatchMemberEncoding::Oversize(_)));
    }

    #[test]
    fn cbor_array_headers_use_the_shortest_argument() {
        assert_eq!(cbor_array_header(1), vec![0x81]);
        assert_eq!(cbor_array_header(23), vec![0x97]);
        assert_eq!(cbor_array_header(24), vec![0x98, 24]);
        assert_eq!(cbor_array_header(255), vec![0x98, 0xff]);
        assert_eq!(cbor_array_header(256), vec![0x99, 0x01, 0x00]);
        assert_eq!(
            cbor_array_header(65_536),
            vec![0x9a, 0x00, 0x01, 0x00, 0x00]
        );
    }

    #[test]
    fn avro_longs_are_zig_zag_varints() {
        assert_eq!(avro_long(0), vec![0x00]);
        assert_eq!(avro_long(1), vec![0x02]);
        assert_eq!(avro_long(3), vec![0x06]);
        assert_eq!(avro_long(64), vec![0x80, 0x01]);
        assert_eq!(avro_long(65_536), vec![0x80, 0x80, 0x08]);
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
